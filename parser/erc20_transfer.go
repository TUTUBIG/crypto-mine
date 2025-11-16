package parser

import (
	"context"
	"crypto-mine/storage"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// ERC20 Transfer event signature: Transfer(address indexed from, address indexed to, uint256 value)
// keccak256("Transfer(address,address,uint256)") = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
const erc20TransferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// ERC20 ABI for Transfer event
const erc20TransferABI = `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]`

// TransferHistory represents a single ERC20 transfer event
type TransferHistory struct {
	ChainID      string    `json:"chain_id"`
	TokenAddress string    `json:"token_address"`
	From         string    `json:"from"`
	To           string    `json:"to"`
	Value        string    `json:"value"`         // Raw value as string (to preserve precision)
	ValueDecimal float64   `json:"value_decimal"` // Human-readable value
	Decimals     uint8     `json:"decimals"`
	BlockNumber  uint64    `json:"block_number"`
	TxHash       string    `json:"tx_hash"`
	TxIndex      uint      `json:"tx_index"`
	LogIndex     uint      `json:"log_index"`
	Timestamp    time.Time `json:"timestamp"`
}

// ERC20TransferMonitor handles monitoring and storing ERC20 transfer events
type ERC20TransferMonitor struct {
	chainID       string
	ethClient     *ethclient.Client
	kvStorage     storage.KVDriver
	transferABI   *abi.ABI
	ctx           context.Context
	transfers     chan *TransferHistory
	shutdown      chan struct{}
	tokenDecimals map[string]uint8 // Cache for token decimals
}

// NewERC20TransferMonitor creates a new ERC20 transfer monitor
func NewERC20TransferMonitor(chainID string, ethClient *ethclient.Client, kvStorage storage.KVDriver) (*ERC20TransferMonitor, error) {
	ctx := context.Background()

	// Parse ERC20 Transfer ABI
	transferABI, err := abi.JSON(strings.NewReader(erc20TransferABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ERC20 Transfer ABI: %w", err)
	}

	return &ERC20TransferMonitor{
		chainID:       chainID,
		ethClient:     ethClient,
		kvStorage:     kvStorage,
		transferABI:   &transferABI,
		ctx:           ctx,
		transfers:     make(chan *TransferHistory, 10000),
		shutdown:      make(chan struct{}),
		tokenDecimals: make(map[string]uint8),
	}, nil
}

// GetTransferTopic returns the ERC20 Transfer event topic hash
func GetTransferTopic() common.Hash {
	return common.HexToHash(erc20TransferTopic)
}

// IsTransferEvent checks if a log is an ERC20 Transfer event
func IsTransferEvent(log *types.Log) bool {
	if len(log.Topics) < 3 {
		return false
	}
	return log.Topics[0].Hex() == erc20TransferTopic
}

// ParseTransferEvent parses a Transfer event from a log
func (m *ERC20TransferMonitor) ParseTransferEvent(log *types.Log) (*TransferHistory, error) {
	if !IsTransferEvent(log) {
		return nil, fmt.Errorf("not a Transfer event")
	}

	// Transfer event has 3 topics: [Transfer signature, from (indexed), to (indexed)]
	// and 1 data field: value (uint256)
	if len(log.Topics) != 3 {
		return nil, fmt.Errorf("invalid Transfer event: expected 3 topics, got %d", len(log.Topics))
	}

	// Extract from address (topic[1])
	from := common.BytesToAddress(log.Topics[1].Bytes())

	// Extract to address (topic[2])
	to := common.BytesToAddress(log.Topics[2].Bytes())

	// Extract value from data (topic[3] doesn't exist, value is in Data field)
	var transferEvent struct {
		Value *big.Int
	}

	if err := m.transferABI.UnpackIntoInterface(&transferEvent, "Transfer", log.Data); err != nil {
		return nil, fmt.Errorf("failed to unpack Transfer event data: %w", err)
	}

	// Get token address from log address
	tokenAddress := log.Address.Hex()

	// Get token decimals (with caching)
	decimals, err := m.getTokenDecimals(tokenAddress)
	if err != nil {
		slog.Warn("Failed to get token decimals, using 18 as default",
			"token", tokenAddress, "error", err)
		decimals = 18 // Default to 18 decimals
	}

	// Calculate decimal value
	valueDecimal := new(big.Float).Quo(
		new(big.Float).SetInt(transferEvent.Value),
		new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)),
	)
	valueDecimalFloat, _ := valueDecimal.Float64()

	// Get block timestamp
	block, err := m.ethClient.BlockByNumber(m.ctx, big.NewInt(int64(log.BlockNumber)))
	if err != nil {
		slog.Warn("Failed to get block for timestamp",
			"block", log.BlockNumber, "error", err)
		// Use current time as fallback
		block = nil
	}

	var timestamp time.Time
	if block != nil {
		timestamp = time.Unix(int64(block.Time()), 0)
	} else {
		timestamp = time.Now()
	}

	return &TransferHistory{
		ChainID:      m.chainID,
		TokenAddress: tokenAddress,
		From:         from.Hex(),
		To:           to.Hex(),
		Value:        transferEvent.Value.String(),
		ValueDecimal: valueDecimalFloat,
		Decimals:     decimals,
		BlockNumber:  log.BlockNumber,
		TxHash:       log.TxHash.Hex(),
		TxIndex:      log.TxIndex,
		LogIndex:     log.Index,
		Timestamp:    timestamp,
	}, nil
}

// getTokenDecimals gets token decimals with caching
func (m *ERC20TransferMonitor) getTokenDecimals(tokenAddress string) (uint8, error) {
	// Check cache first
	if decimals, ok := m.tokenDecimals[strings.ToLower(tokenAddress)]; ok {
		return decimals, nil
	}

	// Fetch from blockchain
	decimals, err := fetchTokenDecimals(m.ctx, m.ethClient, common.HexToAddress(tokenAddress))
	if err != nil {
		return 0, err
	}

	// Cache the result
	m.tokenDecimals[strings.ToLower(tokenAddress)] = decimals
	return decimals, nil
}

// fetchTokenDecimals fetches token decimals from the blockchain
func fetchTokenDecimals(ctx context.Context, client *ethclient.Client, tokenAddress common.Address) (uint8, error) {
	// Use the existing erc20ABI from evm.go
	if erc20ABI == nil {
		return 18, fmt.Errorf("ERC20 ABI not initialized")
	}

	// Pack the decimals() call
	data, err := erc20ABI.Pack("decimals")
	if err != nil {
		return 0, fmt.Errorf("failed to pack decimals call: %w", err)
	}

	// Call the contract
	msg := ethereum.CallMsg{
		To:   &tokenAddress,
		Data: data,
	}

	result, err := client.CallContract(ctx, msg, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to call decimals: %w", err)
	}

	// Unpack the result
	var decimals uint8
	if err := erc20ABI.UnpackIntoInterface(&decimals, "decimals", result); err != nil {
		return 0, fmt.Errorf("failed to unpack decimals result: %w", err)
	}

	return decimals, nil
}

// StoreTransferHistory stores a transfer history to KV storage
func (m *ERC20TransferMonitor) StoreTransferHistory(transfer *TransferHistory) error {
	if transfer == nil {
		return fmt.Errorf("transfer history is nil")
	}

	// Generate KV key: transfer-history-{chainId}-{tokenAddress}
	tokenAddressLower := strings.ToLower(strings.TrimPrefix(transfer.TokenAddress, "0x"))
	kvKey := fmt.Sprintf("/transfer/history-%s-%s", m.chainID, tokenAddressLower)

	// Load existing transfer history
	existingData, err := m.kvStorage.Load(kvKey)
	var transfers []*TransferHistory

	if err == nil && existingData != nil {
		// Parse existing data
		if err := json.Unmarshal(existingData, &transfers); err != nil {
			slog.Warn("Failed to parse existing transfer history, starting fresh",
				"error", err, "key", kvKey)
			transfers = []*TransferHistory{}
		}
	} else {
		transfers = []*TransferHistory{}
	}

	// Append new transfer
	transfers = append(transfers, transfer)

	// Keep only last 10000 transfers to avoid KV size limits
	const maxTransfers = 10000
	if len(transfers) > maxTransfers {
		transfers = transfers[len(transfers)-maxTransfers:]
	}

	// Marshal and store
	data, err := json.Marshal(transfers)
	if err != nil {
		return fmt.Errorf("failed to marshal transfer history: %w", err)
	}

	if err := m.kvStorage.Store(kvKey, data); err != nil {
		return fmt.Errorf("failed to store transfer history: %w", err)
	}

	return nil
}

// GetTransferHistory retrieves transfer history for a token
func (m *ERC20TransferMonitor) GetTransferHistory(tokenAddress string, limit int) ([]*TransferHistory, error) {
	tokenAddressLower := strings.ToLower(strings.TrimPrefix(tokenAddress, "0x"))
	kvKey := fmt.Sprintf("transfer-history-%s-%s", m.chainID, tokenAddressLower)

	data, err := m.kvStorage.Load(kvKey)
	if err != nil {
		return nil, err
	}

	var transfers []*TransferHistory
	if err := json.Unmarshal(data, &transfers); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transfer history: %w", err)
	}

	// Apply limit if specified
	if limit > 0 && limit < len(transfers) {
		// Return the last N transfers (most recent)
		transfers = transfers[len(transfers)-limit:]
	}

	return transfers, nil
}

// Start starts the transfer monitor worker
func (m *ERC20TransferMonitor) Start() {
	go func() {
		for {
			select {
			case <-m.shutdown:
				slog.Info("ERC20 transfer monitor shutting down", "chain_id", m.chainID)
				return
			case transfer := <-m.transfers:
				if err := m.StoreTransferHistory(transfer); err != nil {
					slog.Error("Failed to store transfer history",
						"error", err,
						"token", transfer.TokenAddress,
						"tx", transfer.TxHash)
				} else {
					slog.Debug("Stored transfer history",
						"token", transfer.TokenAddress,
						"from", transfer.From,
						"to", transfer.To,
						"value", transfer.ValueDecimal)
				}
			}
		}
	}()
}

// ProcessLog processes a log and extracts Transfer events
func (m *ERC20TransferMonitor) ProcessLog(log *types.Log) {
	if !IsTransferEvent(log) {
		return
	}

	transfer, err := m.ParseTransferEvent(log)
	if err != nil {
		slog.Warn("Failed to parse Transfer event",
			"error", err,
			"tx", log.TxHash.Hex(),
			"log_index", log.Index)
		return
	}

	// Send to processing channel
	select {
	case m.transfers <- transfer:
	case <-m.shutdown:
		return
	}
}

// Stop stops the transfer monitor
func (m *ERC20TransferMonitor) Stop() {
	close(m.shutdown)
}
