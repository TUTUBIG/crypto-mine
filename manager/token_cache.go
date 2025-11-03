package manager

import (
	"crypto-mine/orm"
	"crypto-mine/storage"
	"fmt"
	"sync"
	"time"

	"log/slog"
)

// TokenIDCache caches the mapping between token identifiers (chainId+address) and database token IDs
type TokenIDCache struct {
	ormClient     *orm.Client
	cache         map[string]int // tokenId (string) -> database token ID (int)
	cacheMutex    sync.RWMutex
	refreshTicker *time.Ticker
	stopChan      chan struct{}
	logger        *slog.Logger
}

// NewTokenIDCache creates a new token ID cache
func NewTokenIDCache(ormClient *orm.Client, refreshInterval time.Duration) *TokenIDCache {
	return &TokenIDCache{
		ormClient:     ormClient,
		cache:         make(map[string]int),
		refreshTicker: time.NewTicker(refreshInterval),
		stopChan:      make(chan struct{}),
		logger:        slog.Default(),
	}
}

// SetLogger sets a custom logger
func (c *TokenIDCache) SetLogger(logger *slog.Logger) {
	c.logger = logger
}

// Start starts the refresh goroutine
func (c *TokenIDCache) Start() {
	c.logger.Info("Starting token ID cache")
	go c.refreshLoop()
}

// Stop stops the refresh goroutine
func (c *TokenIDCache) Stop() {
	c.logger.Info("Stopping token ID cache")
	c.refreshTicker.Stop()
	close(c.stopChan)
}

// refreshLoop periodically refreshes the token ID cache
func (c *TokenIDCache) refreshLoop() {
	for {
		select {
		case <-c.refreshTicker.C:
			c.refresh()
		case <-c.stopChan:
			return
		}
	}
}

// refresh loads all tokens from database and updates cache
func (c *TokenIDCache) refresh() {
	c.logger.Debug("Refreshing token ID cache")

	// Fetch all tokens from database
	sql := `SELECT id, chain_id, token_address FROM tokens`
	results, err := c.ormClient.ExecuteSQL(sql)
	if err != nil {
		c.logger.Error("Failed to refresh token ID cache", "error", err)
		return
	}

	// Build new cache
	newCache := make(map[string]int)
	for _, row := range results {
		chainID, ok1 := row["chain_id"].(string)
		tokenAddress, ok2 := row["token_address"].(string)
		tokenID, ok3 := row["id"].(float64)

		if ok1 && ok2 && ok3 {
			// Generate token ID string (same format as GenerateTokenId)
			tokenIdStr := storage.GenerateTokenId(chainID, tokenAddress)
			newCache[tokenIdStr] = int(tokenID)
		}
	}

	// Update cache
	c.cacheMutex.Lock()
	c.cache = newCache
	c.cacheMutex.Unlock()

	c.logger.Info("Token ID cache refreshed", "count", len(newCache))
}

// GetDatabaseTokenID gets the database token ID for a token identifier string
func (c *TokenIDCache) GetDatabaseTokenID(tokenIdStr string) (int, bool) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	tokenID, exists := c.cache[tokenIdStr]
	return tokenID, exists
}

// GetDatabaseTokenIDFromParts gets the database token ID from chainId and tokenAddress
func (c *TokenIDCache) GetDatabaseTokenIDFromParts(chainId, tokenAddress string) (int, bool) {
	tokenIdStr := storage.GenerateTokenId(chainId, tokenAddress)
	return c.GetDatabaseTokenID(tokenIdStr)
}

// GetOrFetchDatabaseTokenID gets the database token ID, fetching from database if not in cache
func (c *TokenIDCache) GetOrFetchDatabaseTokenID(chainId, tokenAddress string) (int, error) {
	// First check cache
	if tokenID, exists := c.GetDatabaseTokenIDFromParts(chainId, tokenAddress); exists {
		return tokenID, nil
	}

	// Not in cache, fetch from database
	sql := `SELECT id FROM tokens WHERE chain_id = ? AND token_address = ?`
	result, err := c.ormClient.ExecuteSQLSingle(sql, chainId, tokenAddress)
	if err != nil {
		return 0, fmt.Errorf("token not found in database: %w", err)
	}

	tokenID, ok := result["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid token ID in database response")
	}

	dbTokenID := int(tokenID)

	// Update cache
	c.cacheMutex.Lock()
	tokenIdStr := storage.GenerateTokenId(chainId, tokenAddress)
	c.cache[tokenIdStr] = dbTokenID
	c.cacheMutex.Unlock()

	return dbTokenID, nil
}
