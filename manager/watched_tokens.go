package manager

import (
	"crypto-mine/orm"
	"crypto-mine/orm/models"
	"sync"
	"time"

	"log/slog"
)

// convertRowToWatchedToken converts a database row to WatchedToken model
func convertRowToWatchedToken(row map[string]interface{}) models.WatchedToken {
	wt := models.WatchedToken{}

	// Required fields
	if id, ok := row["id"].(float64); ok {
		wt.ID = int(id)
	}
	if userID, ok := row["user_id"].(float64); ok {
		wt.UserID = int(userID)
	}
	if tokenID, ok := row["token_id"].(float64); ok {
		wt.TokenID = int(tokenID)
	}
	if alertActive, ok := row["alert_active"].(bool); ok {
		wt.AlertActive = alertActive
	}
	if createdAt, ok := row["created_at"].(string); ok {
		wt.CreatedAt = createdAt
	}

	// Optional fields
	if notes, ok := row["notes"].(string); ok && notes != "" {
		wt.Notes = &notes
	}
	if interval1m, ok := row["interval_1m"].(float64); ok {
		wt.Interval1m = &interval1m
	}
	if interval5m, ok := row["interval_5m"].(float64); ok {
		wt.Interval5m = &interval5m
	}
	if interval15m, ok := row["interval_15m"].(float64); ok {
		wt.Interval15m = &interval15m
	}
	if interval1h, ok := row["interval_1h"].(float64); ok {
		wt.Interval1h = &interval1h
	}

	// Token details from JOIN
	if chainID, ok := row["chain_id"].(string); ok {
		wt.ChainID = chainID
	}
	if tokenAddress, ok := row["token_address"].(string); ok {
		wt.TokenAddress = tokenAddress
	}
	if tokenSymbol, ok := row["token_symbol"].(string); ok {
		wt.TokenSymbol = tokenSymbol
	}
	if tokenName, ok := row["token_name"].(string); ok {
		wt.TokenName = tokenName
	}
	if decimals, ok := row["decimals"].(float64); ok {
		wt.Decimals = int(decimals)
	}
	if iconURL, ok := row["icon_url"].(string); ok && iconURL != "" {
		wt.IconURL = &iconURL
	}

	return wt
}

// WatchedTokenManager manages watched tokens in memory
type WatchedTokenManager struct {
	ORMClient       *orm.Client                    // Exported so AlertManager can access it
	tokens          map[int][]*models.WatchedToken // tokenID -> []WatchedToken
	tokensMutex     sync.RWMutex
	refreshInterval time.Duration
	stopChan        chan struct{}
	logger          *slog.Logger
}

// NewWatchedTokenManager creates a new watched token manager
func NewWatchedTokenManager(ormClient *orm.Client, refreshInterval time.Duration) *WatchedTokenManager {
	return &WatchedTokenManager{
		ORMClient:       ormClient,
		tokens:          make(map[int][]*models.WatchedToken),
		refreshInterval: refreshInterval,
		stopChan:        make(chan struct{}),
		logger:          slog.Default(),
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

	return nil
}

// Stop stops the refresh goroutine
func (m *WatchedTokenManager) Stop() {
	m.logger.Info("Stopping watched token manager")
	close(m.stopChan)
}

// Refresh fetches all active watched tokens from the database and updates memory
func (m *WatchedTokenManager) Refresh() error {
	m.logger.Debug("Refreshing watched tokens from database")

	// Use SQL query instead of specific API endpoint
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
		WHERE wt.alert_active = 1
		ORDER BY wt.created_at DESC
	`

	results, err := m.ORMClient.ExecuteSQL(sql)
	if err != nil {
		m.logger.Error("Failed to fetch active watched tokens", "error", err)
		return err
	}

	// Convert results to WatchedToken models
	watchedTokens := make([]models.WatchedToken, 0, len(results))
	for _, row := range results {
		wt := convertRowToWatchedToken(row)
		watchedTokens = append(watchedTokens, wt)
	}

	m.logger.Info("Fetched watched tokens from database", "count", len(watchedTokens))

	// Build new map structure
	newTokens := make(map[int][]*models.WatchedToken)
	for i := range watchedTokens {
		token := &watchedTokens[i]
		tokenID := token.TokenID

		if newTokens[tokenID] == nil {
			newTokens[tokenID] = make([]*models.WatchedToken, 0)
		}
		newTokens[tokenID] = append(newTokens[tokenID], token)
	}

	// Update in-memory map
	m.tokensMutex.Lock()
	m.tokens = newTokens
	m.tokensMutex.Unlock()

	m.logger.Info("Updated watched tokens in memory",
		"total_tokens", len(newTokens),
		"total_watches", len(watchedTokens))

	return nil
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

// GetWatchedTokensForTokenID returns all watched tokens for a specific token ID
func (m *WatchedTokenManager) GetWatchedTokensForTokenID(tokenID int) []*models.WatchedToken {
	m.tokensMutex.RLock()
	defer m.tokensMutex.RUnlock()

	if tokens, exists := m.tokens[tokenID]; exists {
		// Return a copy to avoid race conditions
		result := make([]*models.WatchedToken, len(tokens))
		copy(result, tokens)
		return result
	}

	return []*models.WatchedToken{}
}

// GetAllWatchedTokenIDs returns all token IDs that are being watched
func (m *WatchedTokenManager) GetAllWatchedTokenIDs() []int {
	m.tokensMutex.RLock()
	defer m.tokensMutex.RUnlock()

	tokenIDs := make([]int, 0, len(m.tokens))
	for tokenID := range m.tokens {
		tokenIDs = append(tokenIDs, tokenID)
	}

	return tokenIDs
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
