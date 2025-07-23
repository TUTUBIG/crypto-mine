package storage

import (
	"fmt"
	"log/slog"
	"strings"
)

const (
	UniV3 = "uniSwapV3"
)

type PairInfo struct {
	ChainId           string
	Protocol          string
	PoolAddress       string
	PoolName          string
	CostTokenAddress  string
	CostTokenSymbol   string
	CostTokenDecimals int64
	GetTokenAddress   string
	GetTokenSymbol    string
	GetTokenDecimals  int64
}

func (pi *PairInfo) ID() string {
	return generatePairInfoId(pi.ChainId, pi.Protocol, pi.PoolAddress)
}

func generatePairInfoId(chainId, protocolName, poolAddress string) string {
	return fmt.Sprintf("%s-%s-%s", chainId, protocolName, strings.TrimPrefix(poolAddress, "0x"))
}

type PoolInfo struct {
	Pairs map[string]*PairInfo
	db    *CloudflareD1
}

func (pi *PoolInfo) AddPool(pool *PairInfo) {
	pi.Pairs[pool.ID()] = pool
	_, err := pi.db.Insert(pool)
	if err != nil {
		slog.Error("Failed to insert pool into database", "err", err, "pool id", pool.ID())
		return
	}
}

func (pi *PoolInfo) LoadPools() {
	pageIndex := 1
	pageSize := 100
	for {
		pairs := make([]*PairInfo, 0)
		// TODO: get chain id and protocol
		err := pi.db.List("chain id", "protocol", pageIndex, pageSize, &pairs)
		if err != nil {
			slog.Error("Failed to load pools from database", "err", err, "page", pageIndex)
			return
		}
		if len(pairs) == 0 {
			break
		}
		for _, pair := range pairs {
			pi.Pairs[pair.ID()] = pair
		}
		if len(pairs) < pageSize {
			break
		}
		pageIndex++
	}
}

func (pi *PoolInfo) FindPool(chainId, protocolName, poolAddress string) *PairInfo {
	return pi.Pairs[generatePairInfoId(chainId, protocolName, poolAddress)]
}

func NewPoolInfo(db *CloudflareD1) *PoolInfo {
	return &PoolInfo{db: db, Pairs: make(map[string]*PairInfo)}
}
