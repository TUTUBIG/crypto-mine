package storage

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const (
	UniV3 = "uniSwapV3"
)

type PairInfo struct {
	ChainId           string `json:"chain_id"`
	Protocol          string `json:"protocol"`
	PoolAddress       string `json:"pool_address"`
	PoolName          string `json:"pool_name"`
	CostTokenAddress  string `json:"cost_token_address"`
	CostTokenSymbol   string `json:"cost_token_symbol"`
	CostTokenDecimals int64  `json:"cost_token_decimals"`
	GetTokenAddress   string `json:"get_token_address"`
	GetTokenSymbol    string `json:"get_token_symbol"`
	GetTokenDecimals  int64  `json:"get_token_decimals"`
	Skip              bool   `json:"skip"`
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
	OnlyCache      bool
}

func (ti *TokenInfo) ID() string {
	return GenerateTokenId(ti.ChainId, ti.TokenAddress)
}

func GenerateTokenId(chainId, tokenAddress string) string {
	return fmt.Sprintf("%s-%s", chainId, strings.TrimPrefix("0x", strings.ToLower(tokenAddress)))
}

type PoolInfo struct {
	Pairs     map[string]*PairInfo
	db        *CloudflareD1
	fetchDone bool
	tokens    map[string]*TokenInfo
	locker    sync.Mutex
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
			pi.fetchDone = true
		}()
		pageIndex := 1
		pageSize := 300
		for {
			pairs := make([]*PairInfo, 0)
			err := pi.db.List(pageIndex, pageSize, &pairs)
			if err != nil {
				slog.Error("Failed to load pools from database", "err", err, "page", pageIndex)
				panic(err)
			}
			if len(pairs) == 0 {
				return
			}
			for _, pair := range pairs {
				pi.Pairs[pair.ID()] = pair
			}
			if len(pairs) < pageSize {
				return
			}
			pageIndex++
		}
	}()
	slog.Info("Swap pools loading... Waiting for 10 seconds")
	time.Sleep(10 * time.Second)
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
	p, f := pi.Pairs[GeneratePairInfoId(chainId, protocolName, poolAddress)]
	if !f && !pi.fetchDone {
		p = pi.LoadSinglePool(chainId, protocolName, poolAddress)
		if p != nil {
			pi.Pairs[GeneratePairInfoId(chainId, protocolName, poolAddress)] = p
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

func (pi *PoolInfo) FindToken(chainId string, tokenAddress common.Address) *TokenInfo {
	t, f := pi.tokens[GenerateTokenId(chainId, tokenAddress.Hex())]
	if !f && !pi.fetchDone {
		t = pi.LoadSingleToken(tokenAddress.Hex())
		if t != nil {
			t.OnlyCache = false
			pi.tokens[GenerateTokenId(chainId, tokenAddress.Hex())] = t
		}
	}
	return t
}

func (pi *PoolInfo) LoadSingleToken(tokenAddress string) *TokenInfo {
	var token *TokenInfo
	err := pi.db.GetToken(tokenAddress, token)
	if err != nil {
		slog.Error("Failed to load token from database", "err", err, "token address", tokenAddress)
		return nil
	}
	return token
}

func (pi *PoolInfo) AsyncLoadTokens() {
	go func() {
		defer func() {
			pi.fetchDone = true
		}()
		pageIndex := 1
		pageSize := 300
		for {
			token := make([]*TokenInfo, 0)
			err := pi.db.ListToken(pageIndex, pageSize, &token)
			if err != nil {
				slog.Error("Failed to load pools from database", "err", err, "page", pageIndex)
				panic(err)
			}
			if len(token) == 0 {
				return
			}
			for _, t := range token {
				pi.tokens[t.ID()] = t
			}
			if len(token) < pageSize {
				return
			}
			pageIndex++
		}
	}()
	slog.Info("Tokens Info loading... Waiting for 10 seconds")
	time.Sleep(10 * time.Second)
}

func NewPoolInfo(db *CloudflareD1) *PoolInfo {
	return &PoolInfo{db: db, Pairs: make(map[string]*PairInfo), tokens: make(map[string]*TokenInfo)}
}
