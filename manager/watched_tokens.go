package manager

import (
	"crypto-mine/orm"
	"crypto-mine/orm/models"
	"crypto-mine/storage"
	"sync"
	"time"

	"log/slog"
)

// WatchedTokenManager manages watched tokens in memory
type WatchedTokenManager struct {
	ORMClient        *orm.Client                       // Exported so AlertManager can access it
	db               *orm.DB                           // GORM-like DB instance
	tokens           map[string][]*models.WatchedToken // tokenId (chainId+address) -> []WatchedToken
	tokensMutex      sync.RWMutex
	refreshInterval  time.Duration
	removalInterval  time.Duration // Interval for removal cron job
	refreshLimit     int           // Maximum number of records to fetch per refresh
	removalLimit     int           // Maximum number of records to process per removal cycle
	lastFetchedID    int           // Last ID fetched from database
	lastFetchedMutex sync.RWMutex
	stopChan         chan struct{}
	logger           *slog.Logger
}

// NewWatchedTokenManager creates a new watched token manager
func NewWatchedTokenManager(ormClient *orm.Client, refreshInterval time.Duration) *WatchedTokenManager {
	return &WatchedTokenManager{
		ORMClient:       ormClient,
		db:              orm.NewDB(ormClient),
		tokens:          make(map[string][]*models.WatchedToken),
		refreshInterval: refreshInterval,
		removalInterval: 30 * time.Second, // Default: check every 30 seconds
		refreshLimit:    100,              // Default: fetch 100 records per refresh
		removalLimit:    50,               // Default: process 50 records per removal cycle
		lastFetchedID:   0,                // Start from 0 (will fetch all initially)
		stopChan:        make(chan struct{}),
		logger:          slog.Default(),
	}
}

// SetRefreshLimit sets the maximum number of records to fetch per refresh
func (m *WatchedTokenManager) SetRefreshLimit(limit int) {
	if limit > 0 {
		m.refreshLimit = limit
	}
}

// SetLogger sets a custom logger
func (m *WatchedTokenManager) SetLogger(logger *slog.Logger) {
	m.logger = logger
}

// Start starts the refresh goroutine
func (m *WatchedTokenManager) Start() error {
	m.logger.Info("Starting watched token manager")

	// Initial refresh
	if err := m.Refresh(); err != nil {
		m.logger.Error("Failed to refresh watched tokens on startup", "error", err)
		return err
	}

	// Start periodic refresh
	go m.refreshLoop()

	// Start removal cron job
	go m.removalLoop()

	return nil
}

// Stop stops the refresh goroutine
func (m *WatchedTokenManager) Stop() {
	m.logger.Info("Stopping watched token manager")
	close(m.stopChan)
}

// Refresh fetches active watched tokens from the database incrementally and updates memory
// It fetches from the last fetched ID with a limit to control data size
func (m *WatchedTokenManager) Refresh() error {
	m.logger.Debug("Refreshing watched tokens from database")

	// Get the last fetched ID from memory
	m.lastFetchedMutex.RLock()
	lastFetchedID := m.lastFetchedID
	m.lastFetchedMutex.RUnlock()

	// If no last fetched ID, get the maximum ID from memory to start from there
	if lastFetchedID == 0 {
		m.tokensMutex.RLock()
		maxID := m.getMaxIDInMemory()
		m.tokensMutex.RUnlock()
		lastFetchedID = maxID
	}

	// Build SQL query with incremental fetching
	// Fetch from the last ID (exclusive) with limit
	sql := `
		SELECT
			wt.id,
			wt.user_id,
			wt.token_id,
			wt.notes,
			wt.interval_1m,
			wt.interval_5m,
			wt.interval_15m,
			wt.interval_1h,
			wt.alert_active,
			wt.record_refresh,
			wt.created_at,
			t.chain_id,
			t.token_address,
			t.token_symbol,
			t.token_name,
			t.decimals,
			t.icon_url
		FROM user_watched_tokens wt
		INNER JOIN tokens t ON wt.token_id = t.id
		WHERE wt.alert_active = 1 AND wt.record_refresh = 0 AND wt.id > ?
		ORDER BY wt.id ASC
		LIMIT ?
	`

	results, err := m.ORMClient.ExecuteSQL(sql, lastFetchedID, m.refreshLimit)
	if err != nil {
		m.logger.Error("Failed to fetch active watched tokens", "error", err)
		return err
	}

	if len(results) == 0 {
		m.logger.Debug("No new watched tokens to fetch", "last_fetched_id", lastFetchedID)
		return nil
	}

	// Convert results to WatchedToken models using cached reflection
	watchedTokens := make([]models.WatchedToken, 0, len(results))
	var maxID int
	for _, row := range results {
		wt := models.WatchedToken{}
		if err := orm.ConvertRowToModel(row, &wt); err != nil {
			m.logger.Warn("Failed to convert row to WatchedToken", "error", err)
			continue
		}
		watchedTokens = append(watchedTokens, wt)

		// Track the maximum ID in this batch
		if wt.ID > maxID {
			maxID = wt.ID
		}
	}

	m.logger.Debug("Fetched watched tokens from database",
		"count", len(watchedTokens),
		"last_fetched_id", lastFetchedID,
		"max_id", maxID)

	// Update in-memory map incrementally
	m.tokensMutex.Lock()
	for i := range watchedTokens {
		token := &watchedTokens[i]
		tokenIdStr := storage.GenerateTokenId(token.ChainID, token.TokenAddress)

		if m.tokens[tokenIdStr] == nil {
			m.tokens[tokenIdStr] = make([]*models.WatchedToken, 0)
		}

		// Add new
		m.tokens[tokenIdStr] = append(m.tokens[tokenIdStr], token)

	}
	m.tokensMutex.Unlock()

	// Update last fetched ID
	if maxID > 0 {
		m.lastFetchedMutex.Lock()
		m.lastFetchedID = maxID
		m.lastFetchedMutex.Unlock()
	}

	m.logger.Info("Updated watched tokens in memory",
		"fetched_count", len(watchedTokens),
		"last_fetched_id", maxID)

	return nil
}

// getMaxIDInMemory returns the maximum ID from all watched tokens in memory
func (m *WatchedTokenManager) getMaxIDInMemory() int {
	maxID := 0
	for _, tokens := range m.tokens {
		for _, token := range tokens {
			if token.ID > maxID {
				maxID = token.ID
			}
		}
	}
	return maxID
}

// refreshLoop periodically refreshes the watched tokens
func (m *WatchedTokenManager) refreshLoop() {
	ticker := time.NewTicker(m.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.Refresh(); err != nil {
				m.logger.Error("Failed to refresh watched tokens", "error", err)
			}
		case <-m.stopChan:
			return
		}
	}
}

// SyncRecordRefreshTokens syncs tokens with record_refresh = true
// This cron job finds records that need to be synced (record_refresh = true),
// adds them to memory if alert_active = true, removes them if alert_active = false,
// then sets record_refresh = false
func (m *WatchedTokenManager) SyncRecordRefreshTokens() error {
	m.logger.Debug("Syncing records with record_refresh flag")

	// Query for tokens that need to be synced (record_refresh = true)
	sql := `
		SELECT
			wt.id,
			wt.user_id,
			wt.token_id,
			wt.notes,
			wt.interval_1m,
			wt.interval_5m,
			wt.interval_15m,
			wt.interval_1h,
			wt.alert_active,
			wt.created_at,
			t.chain_id,
			t.token_address,
			t.token_symbol,
			t.token_name,
			t.decimals,
			t.icon_url
		FROM user_watched_tokens wt
		INNER JOIN tokens t ON wt.token_id = t.id
		WHERE wt.record_refresh = 1
		LIMIT ?
	`

	results, err := m.ORMClient.ExecuteSQL(sql, m.removalLimit)
	if err != nil {
		m.logger.Error("Failed to fetch records with record_refresh flag", "error", err)
		return err
	}

	if len(results) == 0 {
		m.logger.Debug("No records found with record_refresh flag")
		return nil
	}

	// Convert results to WatchedToken models
	watchedTokens := make([]models.WatchedToken, 0, len(results))
	idsToUpdate := make([]int, 0, len(results))
	for _, row := range results {
		wt := models.WatchedToken{}
		if err := orm.ConvertRowToModel(row, &wt); err != nil {
			m.logger.Warn("Failed to convert row to WatchedToken", "error", err)
			continue
		}
		watchedTokens = append(watchedTokens, wt)
		idsToUpdate = append(idsToUpdate, wt.ID)
	}

	m.logger.Debug("Found records to sync", "count", len(watchedTokens))

	// Sync with memory: add if alert_active = true, remove if alert_active = false
	m.tokensMutex.Lock()
	addedCount := 0
	removedCount := 0
	for i := range watchedTokens {
		token := &watchedTokens[i]
		tokenIdStr := storage.GenerateTokenId(token.ChainID, token.TokenAddress)

		if token.AlertActive {
			// Add or update in memory
			if m.tokens[tokenIdStr] == nil {
				m.tokens[tokenIdStr] = make([]*models.WatchedToken, 0)
			}

			// Check if already exists and update, or add new
			found := false
			for j, existing := range m.tokens[tokenIdStr] {
				if existing.ID == token.ID {
					// Update existing
					m.tokens[tokenIdStr][j] = token
					found = true
					break
				}
			}
			if !found {
				// Add new
				m.tokens[tokenIdStr] = append(m.tokens[tokenIdStr], token)
				addedCount++
			} else {
				addedCount++ // Count as updated
			}
		} else {
			// Remove from memory
			if tokens, exists := m.tokens[tokenIdStr]; exists {
				for j, existing := range tokens {
					if existing.ID == token.ID {
						// Remove from slice
						m.tokens[tokenIdStr] = append(tokens[:j], tokens[j+1:]...)
						removedCount++

						// If slice is empty, remove the tokenIdStr entry
						if len(m.tokens[tokenIdStr]) == 0 {
							delete(m.tokens, tokenIdStr)
						}
						break
					}
				}
			}
		}
	}
	m.tokensMutex.Unlock()

	// Update record_refresh = false for all processed records
	if len(idsToUpdate) > 0 {
		// Build batch update query
		updateSQL := `UPDATE user_watched_tokens SET record_refresh = 0 WHERE id IN (`
		params := make([]interface{}, len(idsToUpdate))
		for i, id := range idsToUpdate {
			if i > 0 {
				updateSQL += ","
			}
			updateSQL += "?"
			params[i] = id
		}
		updateSQL += ")"

		_, err := m.ORMClient.ExecuteSQLUpdate(updateSQL, params...)
		if err != nil {
			m.logger.Warn("Failed to update record_refresh flag", "error", err)
		}
	}

	m.logger.Info("Synced records with record_refresh flag",
		"total_count", len(watchedTokens),
		"added_count", addedCount,
		"removed_count", removedCount)

	return nil
}

// removalLoop periodically syncs tokens with record_refresh flag
func (m *WatchedTokenManager) removalLoop() {
	ticker := time.NewTicker(m.removalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.SyncRecordRefreshTokens(); err != nil {
				m.logger.Error("Failed to sync record_refresh tokens", "error", err)
			}
		case <-m.stopChan:
			return
		}
	}
}

// GetWatchedTokensForToken returns all watched tokens for a specific token (chainId + tokenAddress)
func (m *WatchedTokenManager) GetWatchedTokensForToken(chainID, tokenAddress string) []*models.WatchedToken {
	m.tokensMutex.RLock()
	defer m.tokensMutex.RUnlock()

	tokenIdStr := storage.GenerateTokenId(chainID, tokenAddress)
	if tokens, exists := m.tokens[tokenIdStr]; exists {
		// Return a copy to avoid race conditions
		result := make([]*models.WatchedToken, len(tokens))
		copy(result, tokens)
		return result
	}

	return []*models.WatchedToken{}
}

// GetWatchedTokenCount returns the total number of watched tokens
func (m *WatchedTokenManager) GetWatchedTokenCount() int {
	m.tokensMutex.RLock()
	defer m.tokensMutex.RUnlock()

	total := 0
	for _, tokens := range m.tokens {
		total += len(tokens)
	}

	return total
}
