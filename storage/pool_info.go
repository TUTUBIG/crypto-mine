package storage

import (
	"database/sql"
	"fmt"
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
	db    *sql.DB
	Pairs map[string]*PairInfo
}

func (pi *PoolInfo) AddPool(pool *PairInfo) {
	pi.Pairs[pool.ID()] = pool
	pi.db.Exec(``)
}

func (pi *PoolInfo) LoadPools() {
	pi.db.Query(``)
}

func (pi *PoolInfo) FindPool(chainId, protocolName, poolAddress string) *PairInfo {
	return pi.Pairs[generatePairInfoId(chainId, protocolName, poolAddress)]
}

func NewPoolInfo(db *sql.DB) *PoolInfo {
	return &PoolInfo{db: db, Pairs: make(map[string]*PairInfo)}
}
