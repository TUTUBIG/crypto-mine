package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	UniV3 = "uniSwapV3"
)

type PairInfo struct {
	ChainId        string `json:"chain_id"`
	Protocol       string `json:"protocol"`
	PoolAddress    string `json:"pool_address"`
	PoolName       string `json:"pool_name"`
	Token0Address  string `json:"token_0_address"`
	Token0Symbol   string `json:"token_0_symbol"`
	Token0Decimals uint8  `json:"token_0_decimals"`
	Token1Address  string `json:"token_1_address"`
	Token1Symbol   string `json:"token_1_symbol"`
	Token1Decimals uint8  `json:"token_1_decimals"`
	Skip           bool   `json:"skip"`
}

func (pi *PairInfo) ID() string {
	return GeneratePairInfoId(pi.ChainId, pi.Protocol, pi.PoolAddress)
}

func GeneratePairInfoId(chainId, protocolName, poolAddress string) string {
	return fmt.Sprintf("%s-%s-%s", chainId, protocolName, strings.TrimPrefix(strings.ToLower(poolAddress), "0x"))
}

type TokenInfo struct {
	ChainId        string  `json:"chain_id"`
	TokenAddress   string  `json:"token_address"`
	TokenSymbol    string  `json:"token_symbol"`
	TokenName      string  `json:"token_name"`
	Decimals       uint8   `json:"decimals"`
	IconUrl        string  `json:"icon_url"`
	DailyVolumeUSD float64 `json:"daily_volume_usd"`
	IsSpecial      bool    `json:"is_special"`
	OnlyCache      bool
}

func (ti *TokenInfo) ID() string {
	return GenerateTokenId(ti.ChainId, ti.TokenAddress)
}

func GenerateTokenId(chainId, tokenAddress string) string {
	return fmt.Sprintf("%s-%s", chainId, strings.TrimPrefix(strings.ToLower(tokenAddress), "0x"))
}

type PoolInfo struct {
	Pairs        map[string]*PairInfo
	db           *CloudflareD1
	fetchDone    bool
	tokens       map[string]*TokenInfo
	locker       sync.RWMutex
	fetchDoneMu  sync.RWMutex
	loadWaitChan chan struct{} // Signal when initial load is complete
}

func (pi *PoolInfo) AddPool(pool *PairInfo) {
	pi.locker.Lock()
	defer pi.locker.Unlock()
	slog.Info("Add new pool", "id", pool.ID())
	pi.Pairs[pool.ID()] = pool
	_, err := pi.db.Insert(pool)
	if err != nil {
		slog.Error("Failed to insert pool into database", "err", err, "pool id", pool.ID())
		return
	}
}

func (pi *PoolInfo) AsyncLoadPools() {
	go func() {
		defer func() {
			pi.setFetchDone(true)
			close(pi.loadWaitChan)
			slog.Info("Fetch pools done", "pools", len(pi.Pairs))
		}()
		pageIndex := 1
		pageSize := 100
		for {
			pairs := make([]*PairInfo, 0)
			err := pi.db.List(pageIndex, pageSize, &pairs)
			if err != nil {
				slog.Error("Failed to load pools from database", "err", err, "page", pageIndex)
				// Don't panic, just log and continue
				return
			}
			if len(pairs) == 0 {
				return
			}
			pi.locker.Lock()
			for _, pair := range pairs {
				pi.Pairs[pair.ID()] = pair
			}
			pi.locker.Unlock()
			if len(pairs) < pageSize {
				return
			}
			pageIndex++
		}
	}()
	slog.Info("Swap pools loading asynchronously...")
	// Wait for initial load with timeout
	select {
	case <-pi.loadWaitChan:
		slog.Info("Initial pool loading completed")
	case <-time.After(30 * time.Second):
		slog.Warn("Pool loading timeout after 30s, continuing with partial data")
	}
}

func (pi *PoolInfo) LoadSinglePool(chainId, protocolName, poolAddress string) *PairInfo {
	var pool *PairInfo
	err := pi.db.Get(chainId, protocolName, poolAddress, &pool)
	if err != nil {
		slog.Error("Failed to load pools from database", "err", err, "pool", poolAddress, "chainId", chainId, "protocolName", protocolName)
		return nil
	}
	return pool
}

func (pi *PoolInfo) FindPool(chainId, protocolName, poolAddress string) *PairInfo {
	poolID := GeneratePairInfoId(chainId, protocolName, poolAddress)

	// Try read lock first for hot path
	pi.locker.RLock()
	p, found := pi.Pairs[poolID]
	pi.locker.RUnlock()

	if found {
		return p
	}

	// Not in cache, try loading from DB if initial fetch is done
	if !pi.isFetchDone() {
		p = pi.LoadSinglePool(chainId, protocolName, poolAddress)
		if p != nil {
			pi.locker.Lock()
			pi.Pairs[poolID] = p
			pi.locker.Unlock()
		}
	}
	return p
}

func (pi *PoolInfo) AddToken(token *TokenInfo) {
	pi.locker.Lock()
	defer pi.locker.Unlock()
	token.OnlyCache = true
	pi.tokens[token.ID()] = token
}

func (pi *PoolInfo) StoreToken(token *TokenInfo) {

	slog.Info("Add new token", "token symbol", token.TokenSymbol, "token address", token.TokenAddress)

	_, err := pi.db.InsertToken(token)
	if err != nil {
		slog.Error("Failed to insert token into database", "err", err, "token address", token.TokenAddress)
		return
	}
}

func (pi *PoolInfo) FindToken(chainId, tokenAddress string) *TokenInfo {
	tokenID := GenerateTokenId(chainId, tokenAddress)

	// Try read lock first for hot path
	pi.locker.RLock()
	t, found := pi.tokens[tokenID]
	pi.locker.RUnlock()

	if found {
		return t
	}

	// Not in cache, try loading from DB if initial fetch is done
	if !pi.isFetchDone() {
		t = pi.LoadSingleToken(chainId, tokenAddress)
		if t != nil {
			t.OnlyCache = false
			pi.locker.Lock()
			pi.tokens[tokenID] = t
			pi.locker.Unlock()
		}
	}
	return t
}

func (pi *PoolInfo) LoadSingleToken(chainId, tokenAddress string) *TokenInfo {
	var token TokenInfo
	err := pi.db.GetToken(chainId, tokenAddress, &token)
	if err != nil {
		if errors.Is(err, NotfoundError) {
			return nil
		}
		slog.Error("Failed to load token from database", "err", err, "chain_id", chainId, "token_address", tokenAddress)
		return nil
	}
	return &token
}

func (pi *PoolInfo) AsyncLoadTokens() {
	tokenLoadChan := make(chan struct{})
	go func() {
		defer func() {
			pi.setFetchDone(true)
			close(tokenLoadChan)
			slog.Info("Tokens loading done", "total", len(pi.tokens))
		}()
		pageIndex := 1
		pageSize := 100
		for {
			token := make([]*TokenInfo, 0)
			err := pi.db.ListToken(pageIndex, pageSize, &token)
			if err != nil {
				slog.Error("Failed to load tokens from database", "err", err, "page", pageIndex)
				// Don't panic, just log and continue
				return
			}
			if len(token) == 0 {
				return
			}
			pi.locker.Lock()
			for _, t := range token {
				pi.tokens[t.ID()] = t
			}
			pi.locker.Unlock()
			if len(token) < pageSize {
				return
			}
			pageIndex++
		}
	}()
	slog.Info("Tokens info loading asynchronously...")
	// Wait for initial load with timeout
	select {
	case <-tokenLoadChan:
		slog.Info("Initial token loading completed")
	case <-time.After(30 * time.Second):
		slog.Warn("Token loading timeout after 30s, continuing with partial data")
	}
}

func NewPoolInfo(db *CloudflareD1) *PoolInfo {
	return &PoolInfo{
		db:           db,
		Pairs:        make(map[string]*PairInfo),
		tokens:       make(map[string]*TokenInfo),
		loadWaitChan: make(chan struct{}),
	}
}

// Thread-safe getter/setter for fetchDone
func (pi *PoolInfo) isFetchDone() bool {
	pi.fetchDoneMu.RLock()
	defer pi.fetchDoneMu.RUnlock()
	return pi.fetchDone
}

func (pi *PoolInfo) setFetchDone(done bool) {
	pi.fetchDoneMu.Lock()
	defer pi.fetchDoneMu.Unlock()
	pi.fetchDone = done
}

// GetAllTokens returns all tokens from the cache
func (pi *PoolInfo) GetAllTokens() []*TokenInfo {
	pi.locker.RLock()
	defer pi.locker.RUnlock()

	tokens := make([]*TokenInfo, 0, len(pi.tokens))
	for _, token := range pi.tokens {
		tokens = append(tokens, token)
	}

	return tokens
}
