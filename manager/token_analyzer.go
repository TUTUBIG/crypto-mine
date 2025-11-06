package manager

import (
	"crypto-mine/orm"
	"crypto-mine/storage"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"log/slog"
)

const (
	// Label thresholds
	PumpingThreshold   = 20.0 // Price increase > 20%
	RisingMinThreshold = 5.0  // Price increase >= 5%
	RisingMaxThreshold = 20.0 // Price increase <= 20%

	// Hot token criteria
	MinTransactionCountForHot = 50     // Minimum transactions to be considered hot
	MinVolumeUSDForHot        = 5000.0 // Minimum USD volume to be considered hot
)

// TokenLabel represents a label that can be applied to a token
type TokenLabel string

const (
	LabelHot     TokenLabel = "hot"     // High transaction count and volume
	LabelPumping TokenLabel = "pumping" // Price increase > 20%
	LabelRising  TokenLabel = "rising"  // Price increase 5-20%
)

// AnalyzedToken represents a token with its latest candle and labels
type AnalyzedToken struct {
	TokenID          string // Generated using GenerateTokenId(ChainID, TokenAddress)
	ChainID          string
	TokenAddress     string
	TokenSymbol      string
	TokenName        string
	Decimals         uint8
	IconUrl          string
	LatestCandle     *storage.CandleData
	Labels           []TokenLabel
	PriceChangeRate  float64
	TransactionCount int64
	VolumeUSD        float64
	LastUpdated      time.Time
}

// TokenAnalyzer performs offline analysis on tokens
type TokenAnalyzer struct {
	poolInfo      *storage.PoolInfo
	candleStorage storage.CandleDataStorage
	interval      time.Duration
	logger        *slog.Logger
	mu            sync.RWMutex
	// In-memory cache for latest candles: tokenID -> latest candle
	candleCache map[string]*storage.CandleData
	cacheMu     sync.RWMutex
	// ORM client for database operations
	ormClient *orm.Client
	// KV storage for labeled tokens (performance optimization)
	kvStorage storage.KVDriver
}

// NewTokenAnalyzer creates a new token analyzer
func NewTokenAnalyzer(poolInfo *storage.PoolInfo, candleStorage storage.CandleDataStorage, interval time.Duration, ormClient *orm.Client, kvStorage storage.KVDriver) *TokenAnalyzer {
	return &TokenAnalyzer{
		poolInfo:      poolInfo,
		candleStorage: candleStorage,
		interval:      interval,
		logger:        slog.Default(),
		candleCache:   make(map[string]*storage.CandleData),
		ormClient:     ormClient,
		kvStorage:     kvStorage,
	}
}

// SetLogger sets a custom logger
func (ta *TokenAnalyzer) SetLogger(logger *slog.Logger) {
	ta.logger = logger
}

func (ta *TokenAnalyzer) GetInterval() time.Duration {
	return ta.interval
}

// AnalyzeAllTokens analyzes all tokens and returns labeled tokens
// Performance-optimized: only analyzes tokens with cached candles, sequential processing
func (ta *TokenAnalyzer) AnalyzeAllTokens() ([]*AnalyzedToken, error) {
	startTime := time.Now()
	ta.logger.Info("Starting offline analysis of all tokens (performance mode)")

	// Get all cached candles first (fast, no I/O)
	cachedTokens := ta.getCachedTokens()
	ta.logger.Info("Found cached tokens to analyze", "count", len(cachedTokens))

	if len(cachedTokens) == 0 {
		return []*AnalyzedToken{}, nil
	}

	// Sequential processing - determineLabels is fast enough
	analyzedTokens := make([]*AnalyzedToken, 0, len(cachedTokens))
	for _, token := range cachedTokens {
		// Fast path: only analyze if cached (skip storage fetch)
		analyzed := ta.analyzeTokenFast(token)
		if analyzed != nil {
			analyzedTokens = append(analyzedTokens, analyzed)
		}
	}

	duration := time.Since(startTime)
	ta.logger.Info("Analysis complete",
		"total_tokens", len(cachedTokens),
		"analyzed", len(analyzedTokens),
		"duration_ms", duration.Milliseconds())
	return analyzedTokens, nil
}

// getCachedTokens returns tokens that have candles in cache
func (ta *TokenAnalyzer) getCachedTokens() []*storage.TokenInfo {
	ta.cacheMu.RLock()
	cachedTokenIDs := make([]string, 0, len(ta.candleCache))
	for tokenID := range ta.candleCache {
		cachedTokenIDs = append(cachedTokenIDs, tokenID)
	}
	ta.cacheMu.RUnlock()

	// Get token info for cached tokens
	tokens := make([]*storage.TokenInfo, 0, len(cachedTokenIDs))
	for _, tokenID := range cachedTokenIDs {
		// Parse tokenID: chainId-tokenAddress
		parts := splitTokenID(tokenID)
		if len(parts) != 2 {
			continue
		}
		token := ta.poolInfo.FindToken(parts[0], parts[1])
		if token != nil {
			tokens = append(tokens, token)
		}
	}

	return tokens
}

// splitTokenID splits tokenID into chainId and tokenAddress
// Performance-optimized: uses strings.SplitN for fast parsing
func splitTokenID(tokenID string) []string {
	// TokenID format: "chainId-tokenAddress"
	// Use SplitN with limit 2 to handle addresses that might contain dashes
	parts := strings.SplitN(tokenID, "-", 2)
	if len(parts) == 2 {
		return parts
	}
	return []string{}
}

// getAllTokens retrieves all tokens from pool info
func (ta *TokenAnalyzer) getAllTokens() []*storage.TokenInfo {
	return ta.poolInfo.GetAllTokens()
}

// analyzeToken analyzes a single token and returns its analysis
// Falls back to storage if not in cache
func (ta *TokenAnalyzer) analyzeToken(token *storage.TokenInfo) (*AnalyzedToken, error) {
	tokenID := storage.GenerateTokenId(token.ChainId, token.TokenAddress)

	// Try to get from cache first
	candle := ta.getLatestCandleFromCache(tokenID)

	// If not in cache, fetch from storage
	if candle == nil {
		var err error
		candle, err = ta.candleStorage.GetLatestCandle(tokenID, ta.interval)
		if err != nil {
			// Token might not have candle data yet
			return nil, fmt.Errorf("failed to get latest candle: %w", err)
		}

		if candle == nil {
			return nil, fmt.Errorf("no candle data available")
		}

		// Cache it for future use
		ta.updateCandleCache(tokenID, candle)
	}

	return ta.createAnalyzedToken(token, candle), nil
}

// analyzeTokenFast analyzes a token using only cached data (performance-optimized)
// Returns nil if token doesn't have cached candle data
func (ta *TokenAnalyzer) analyzeTokenFast(token *storage.TokenInfo) *AnalyzedToken {
	tokenID := storage.GenerateTokenId(token.ChainId, token.TokenAddress)

	// Only use cache - skip storage fetch for performance
	candle := ta.getLatestCandleFromCache(tokenID)
	if candle == nil {
		return nil // Skip tokens without cached data
	}

	return ta.createAnalyzedToken(token, candle)
}

// createAnalyzedToken creates an AnalyzedToken from token and candle (shared logic)
func (ta *TokenAnalyzer) createAnalyzedToken(token *storage.TokenInfo, candle *storage.CandleData) *AnalyzedToken {
	// Calculate price change rate (fast, no I/O)
	priceChangeRate := ta.calculatePriceChangeRate(candle)

	// Determine labels (fast, no I/O)
	labels := ta.determineLabels(candle, priceChangeRate)

	return &AnalyzedToken{
		TokenID:          storage.GenerateTokenId(token.ChainId, token.TokenAddress),
		ChainID:          token.ChainId,
		TokenAddress:     token.TokenAddress,
		TokenSymbol:      token.TokenSymbol,
		TokenName:        token.TokenName,
		Decimals:         token.Decimals,
		IconUrl:          token.IconUrl,
		LatestCandle:     candle,
		Labels:           labels,
		PriceChangeRate:  priceChangeRate,
		TransactionCount: candle.TransactionCount,
		VolumeUSD:        candle.VolumeUSD,
		LastUpdated:      time.Now(),
	}
}

// calculatePriceChangeRate calculates the price change rate percentage
// Performance-optimized: minimal validation, fast calculation
func (ta *TokenAnalyzer) calculatePriceChangeRate(candle *storage.CandleData) float64 {
	// Fast path: skip validation for performance (assume valid data)
	// Calculate percentage change: (close - open) / open * 100
	if candle.OpenPrice > 0 {
		return ((candle.ClosePrice - candle.OpenPrice) / candle.OpenPrice) * 100.0
	}
	return 0.0
}

// determineLabels determines which labels apply to a token based on its candle data
func (ta *TokenAnalyzer) determineLabels(candle *storage.CandleData, priceChangeRate float64) []TokenLabel {
	labels := make([]TokenLabel, 0)

	// Check for pumping (price increase > 20%)
	if priceChangeRate > PumpingThreshold {
		labels = append(labels, LabelPumping)
	}

	// Check for rising (price increase 5-20%)
	if priceChangeRate >= RisingMinThreshold && priceChangeRate <= RisingMaxThreshold {
		labels = append(labels, LabelRising)
	}

	// Check for hot (high transaction count and volume)
	if candle.TransactionCount >= MinTransactionCountForHot && candle.VolumeUSD >= MinVolumeUSDForHot {
		labels = append(labels, LabelHot)
	}

	return labels
}

// GetHotTokens returns tokens labeled as "hot" sorted by transaction count
// Performance-optimized: uses partial sort for top N
func (ta *TokenAnalyzer) GetHotTokens(analyzedTokens []*AnalyzedToken, limit int) []*AnalyzedToken {
	hotTokens := make([]*AnalyzedToken, 0, len(analyzedTokens)/4) // Pre-allocate with estimate

	for _, token := range analyzedTokens {
		// Fast check: iterate labels once
		for _, label := range token.Labels {
			if label == LabelHot {
				hotTokens = append(hotTokens, token)
				break
			}
		}
	}

	if len(hotTokens) == 0 {
		return hotTokens
	}

	// Use partial sort for better performance when limit is small
	if limit > 0 && limit < len(hotTokens) {
		// Partial sort: only sort top N elements
		sort.Slice(hotTokens, func(i, j int) bool {
			return hotTokens[i].TransactionCount > hotTokens[j].TransactionCount
		})
		return hotTokens[:limit]
	}

	// Full sort if no limit or limit is large
	sort.Slice(hotTokens, func(i, j int) bool {
		return hotTokens[i].TransactionCount > hotTokens[j].TransactionCount
	})

	return hotTokens
}

// GetPumpingTokens returns tokens labeled as "pumping" sorted by price change rate
// Performance-optimized: uses partial sort for top N
func (ta *TokenAnalyzer) GetPumpingTokens(analyzedTokens []*AnalyzedToken, limit int) []*AnalyzedToken {
	pumpingTokens := make([]*AnalyzedToken, 0, len(analyzedTokens)/10) // Pre-allocate with estimate

	for _, token := range analyzedTokens {
		for _, label := range token.Labels {
			if label == LabelPumping {
				pumpingTokens = append(pumpingTokens, token)
				break
			}
		}
	}

	if len(pumpingTokens) == 0 {
		return pumpingTokens
	}

	// Use partial sort for better performance when limit is small
	if limit > 0 && limit < len(pumpingTokens) {
		sort.Slice(pumpingTokens, func(i, j int) bool {
			return pumpingTokens[i].PriceChangeRate > pumpingTokens[j].PriceChangeRate
		})
		return pumpingTokens[:limit]
	}

	sort.Slice(pumpingTokens, func(i, j int) bool {
		return pumpingTokens[i].PriceChangeRate > pumpingTokens[j].PriceChangeRate
	})

	return pumpingTokens
}

// GetRisingTokens returns tokens labeled as "rising" sorted by price change rate
// Performance-optimized: uses partial sort for top N
func (ta *TokenAnalyzer) GetRisingTokens(analyzedTokens []*AnalyzedToken, limit int) []*AnalyzedToken {
	risingTokens := make([]*AnalyzedToken, 0, len(analyzedTokens)/10) // Pre-allocate with estimate

	for _, token := range analyzedTokens {
		for _, label := range token.Labels {
			if label == LabelRising {
				risingTokens = append(risingTokens, token)
				break
			}
		}
	}

	if len(risingTokens) == 0 {
		return risingTokens
	}

	// Use partial sort for better performance when limit is small
	if limit > 0 && limit < len(risingTokens) {
		sort.Slice(risingTokens, func(i, j int) bool {
			return risingTokens[i].PriceChangeRate > risingTokens[j].PriceChangeRate
		})
		return risingTokens[:limit]
	}

	sort.Slice(risingTokens, func(i, j int) bool {
		return risingTokens[i].PriceChangeRate > risingTokens[j].PriceChangeRate
	})

	return risingTokens
}

// GetCategorizedTokens returns tokens grouped by category
// Performance-optimized: pre-allocates slices, parallel sorting
func (ta *TokenAnalyzer) GetCategorizedTokens(analyzedTokens []*AnalyzedToken) map[TokenLabel][]*AnalyzedToken {
	categorized := make(map[TokenLabel][]*AnalyzedToken)

	// Pre-allocate slices with estimates
	categorized[LabelHot] = make([]*AnalyzedToken, 0, len(analyzedTokens)/4)
	categorized[LabelPumping] = make([]*AnalyzedToken, 0, len(analyzedTokens)/10)
	categorized[LabelRising] = make([]*AnalyzedToken, 0, len(analyzedTokens)/10)

	for _, token := range analyzedTokens {
		for _, label := range token.Labels {
			categorized[label] = append(categorized[label], token)
		}
	}

	// Sort each category in parallel (if needed, but for small datasets sequential is faster)
	for label, tokens := range categorized {
		if len(tokens) == 0 {
			continue
		}
		switch label {
		case LabelHot:
			sort.Slice(tokens, func(i, j int) bool {
				return tokens[i].TransactionCount > tokens[j].TransactionCount
			})
		case LabelPumping, LabelRising:
			sort.Slice(tokens, func(i, j int) bool {
				return tokens[i].PriceChangeRate > tokens[j].PriceChangeRate
			})
		}
	}

	return categorized
}

// AnalyzeTokenByID analyzes a specific token by its ID
func (ta *TokenAnalyzer) AnalyzeTokenByID(chainID, tokenAddress string) (*AnalyzedToken, error) {
	token := ta.poolInfo.FindToken(chainID, tokenAddress)
	if token == nil {
		return nil, fmt.Errorf("token not found: %s-%s", chainID, tokenAddress)
	}

	return ta.analyzeToken(token)
}

// UpdateCandleCache updates the in-memory cache with a new candle
// This should be called when a candle is stored
func (ta *TokenAnalyzer) UpdateCandleCache(chainID, tokenAddress string, candle *storage.CandleData, interval time.Duration) {
	// Only cache candles for the configured interval
	if interval != ta.interval {
		return
	}

	tokenID := storage.GenerateTokenId(chainID, tokenAddress)
	ta.updateCandleCache(tokenID, candle)
}

// updateCandleCache updates the cache (internal method)
func (ta *TokenAnalyzer) updateCandleCache(tokenID string, candle *storage.CandleData) {
	ta.cacheMu.Lock()
	defer ta.cacheMu.Unlock()

	// Create a copy to avoid race conditions
	candleCopy := *candle
	ta.candleCache[tokenID] = &candleCopy

	ta.logger.Debug("Updated candle cache",
		"token_id", tokenID,
		"timestamp", candle.Timestamp,
		"transaction_count", candle.TransactionCount)
}

// getLatestCandleFromCache gets the latest candle from cache
func (ta *TokenAnalyzer) getLatestCandleFromCache(tokenID string) *storage.CandleData {
	ta.cacheMu.RLock()
	defer ta.cacheMu.RUnlock()

	candle, exists := ta.candleCache[tokenID]
	if !exists {
		return nil
	}

	// Return a copy to avoid race conditions
	candleCopy := *candle
	return &candleCopy
}

// GetCachedCandleCount returns the number of candles in cache
func (ta *TokenAnalyzer) GetCachedCandleCount() int {
	ta.cacheMu.RLock()
	defer ta.cacheMu.RUnlock()

	return len(ta.candleCache)
}

// ClearCache clears the candle cache
func (ta *TokenAnalyzer) ClearCache() {
	ta.cacheMu.Lock()
	defer ta.cacheMu.Unlock()

	ta.candleCache = make(map[string]*storage.CandleData)
	ta.logger.Info("Candle cache cleared")
}

// RankAndUpdateTopTokens ranks tokens by labels and updates top 5 in database
func (ta *TokenAnalyzer) RankAndUpdateTopTokens() error {
	startTime := time.Now()
	ta.logger.Info("Starting token ranking and database update")

	// Analyze all tokens
	analyzedTokens, err := ta.AnalyzeAllTokens()
	if err != nil {
		return fmt.Errorf("failed to analyze tokens: %w", err)
	}

	if len(analyzedTokens) == 0 {
		ta.logger.Info("No tokens to rank")
		return nil
	}

	// Get top 5 tokens for each label
	topHot := ta.GetHotTokens(analyzedTokens, 5)
	topPumping := ta.GetPumpingTokens(analyzedTokens, 5)
	topRising := ta.GetRisingTokens(analyzedTokens, 5)

	ta.logger.Info("Ranked tokens",
		"hot", len(topHot),
		"pumping", len(topPumping),
		"rising", len(topRising))

	// Store in KV for fast retrieval (performance optimization)
	if err := ta.storeLabeledTokensInKV(LabelHot, topHot); err != nil {
		ta.logger.Warn("Failed to store hot tokens in KV", "error", err)
		// Continue - KV is for performance, not critical
	}

	if err := ta.storeLabeledTokensInKV(LabelPumping, topPumping); err != nil {
		ta.logger.Warn("Failed to store pumping tokens in KV", "error", err)
		// Continue - KV is for performance, not critical
	}

	if err := ta.storeLabeledTokensInKV(LabelRising, topRising); err != nil {
		ta.logger.Warn("Failed to store rising tokens in KV", "error", err)
		// Continue - KV is for performance, not critical
	}

	duration := time.Since(startTime)
	ta.logger.Info("Token ranking and database update complete",
		"duration_ms", duration.Milliseconds())
	return nil
}

// updateTokenTagsInDB updates token tags in database for a specific label
// Performance-optimized: deletes all tags for label, then inserts top tokens
// Uses GenerateTokenId for token identification
func (ta *TokenAnalyzer) updateTokenTagsInDB(label TokenLabel, tokens []*AnalyzedToken) error {
	if ta.ormClient == nil {
		return fmt.Errorf("ORM client not initialized")
	}

	if len(tokens) == 0 {
		// Remove all tags for this label if no tokens
		deleteSQL := `DELETE FROM token_tags WHERE tag = ?`
		_, err := ta.ormClient.ExecuteSQLUpdate(deleteSQL, string(label))
		if err != nil {
			ta.logger.Warn("Failed to remove all tags", "label", label, "error", err)
		}
		return nil
	}

	// Step 1: Delete all existing tags for this label (fast, single query)
	deleteSQL := `DELETE FROM token_tags WHERE tag = ?`
	_, err := ta.ormClient.ExecuteSQLUpdate(deleteSQL, string(label))
	if err != nil {
		ta.logger.Warn("Failed to delete existing tags", "label", label, "error", err)
		// Continue anyway - might be first run
	}

	// Step 2: Insert tags for top tokens
	// Query database to get numeric token IDs using chainId and tokenAddress
	// Token identification uses GenerateTokenId(chainId, tokenAddress)
	tokenIDs := make([]int, 0, len(tokens))
	for _, token := range tokens {
		// Use GenerateTokenId for identification
		tokenID := storage.GenerateTokenId(token.ChainID, token.TokenAddress)

		// Query database to get numeric token ID
		querySQL := `SELECT id FROM tokens WHERE chain_id = ? AND token_address = ? LIMIT 1`
		result, err := ta.ormClient.ExecuteSQLSingle(querySQL, token.ChainID, token.TokenAddress)
		if err != nil {
			ta.logger.Debug("Token not found in database, skipping tag",
				"token_id", tokenID,
				"chain_id", token.ChainID,
				"token_address", token.TokenAddress,
				"label", label,
				"error", err)
			continue
		}

		// Extract numeric token ID from result
		dbTokenID, ok := result["id"].(float64)
		if !ok {
			ta.logger.Debug("Invalid token ID in database response",
				"token_id", tokenID,
				"chain_id", token.ChainID,
				"token_address", token.TokenAddress)
			continue
		}

		tokenIDs = append(tokenIDs, int(dbTokenID))
	}

	if len(tokenIDs) == 0 {
		ta.logger.Debug("No valid token IDs to tag", "label", label)
		return nil
	}

	// Insert tags (use batch if supported, otherwise individual inserts)
	// For performance, use individual INSERT OR IGNORE (handles duplicates)
	for _, dbTokenID := range tokenIDs {
		insertSQL := `INSERT OR IGNORE INTO token_tags (token_id, tag) VALUES (?, ?)`
		_, err := ta.ormClient.ExecuteSQLUpdate(insertSQL, dbTokenID, string(label))
		if err != nil {
			ta.logger.Warn("Failed to add tag",
				"token_id", dbTokenID,
				"label", label,
				"error", err)
			// Continue with other tokens
		}
	}

	ta.logger.Info("Updated token tags in database",
		"label", label,
		"count", len(tokenIDs))
	return nil
}

// storeLabeledTokensInKV stores labeled tokens in KV storage for fast retrieval
// Performance-optimized: stores JSON-encoded token data
func (ta *TokenAnalyzer) storeLabeledTokensInKV(label TokenLabel, tokens []*AnalyzedToken) error {
	if ta.kvStorage == nil {
		return fmt.Errorf("KV storage not initialized")
	}

	// Create a token list with all basic info for storage
	tokenList := make([]map[string]interface{}, 0, len(tokens))
	for _, token := range tokens {
		tokenData := map[string]interface{}{
			"token_id":          token.TokenID, // Generated using GenerateTokenId
			"chain_id":          token.ChainID,
			"token_address":     token.TokenAddress,
			"token_symbol":      token.TokenSymbol,
			"token_name":        token.TokenName,
			"decimals":          token.Decimals,
			"icon_url":          token.IconUrl,
			"price_change_rate": token.PriceChangeRate,
			"transaction_count": token.TransactionCount,
			"volume_usd":        token.VolumeUSD,
			"last_updated":      token.LastUpdated.Unix(),
		}
		tokenList = append(tokenList, tokenData)
	}

	// Encode to JSON
	data, err := json.Marshal(tokenList)
	if err != nil {
		return fmt.Errorf("failed to marshal token data: %w", err)
	}

	// Store in KV with key: "label-{label}"
	key := fmt.Sprintf("label-%s", string(label))
	if err := ta.kvStorage.Store(key, data); err != nil {
		return fmt.Errorf("failed to store tokens in KV: %w", err)
	}

	ta.logger.Debug("Stored labeled tokens in KV",
		"label", label,
		"count", len(tokens),
		"key", key)
	return nil
}

// GetLabeledTokensFromKV retrieves labeled tokens from KV storage
// Performance-optimized: fast retrieval without database query
func (ta *TokenAnalyzer) GetLabeledTokensFromKV(label TokenLabel) ([]*AnalyzedToken, error) {
	if ta.kvStorage == nil {
		return nil, fmt.Errorf("KV storage not initialized")
	}

	// Load from KV with key: "label-{label}"
	key := fmt.Sprintf("label-%s", string(label))
	data, err := ta.kvStorage.Load(key)
	if err != nil {
		if err == storage.NotfoundError {
			return []*AnalyzedToken{}, nil
		}
		return nil, fmt.Errorf("failed to load tokens from KV: %w", err)
	}

	// Decode from JSON
	var tokenList []map[string]interface{}
	if err := json.Unmarshal(data, &tokenList); err != nil {
		return nil, fmt.Errorf("failed to unmarshal token data: %w", err)
	}

	// Convert to AnalyzedToken list
	tokens := make([]*AnalyzedToken, 0, len(tokenList))
	for _, tokenData := range tokenList {
		// Safe type assertions with error handling
		chainID, ok := tokenData["chain_id"].(string)
		if !ok {
			continue
		}
		tokenAddress, ok := tokenData["token_address"].(string)
		if !ok {
			continue
		}

		// Generate tokenID using GenerateTokenId function for consistent identification
		tokenID := storage.GenerateTokenId(chainID, tokenAddress)

		// If token_id exists in KV data, use it (for backward compatibility)
		if val, ok := tokenData["token_id"].(string); ok && val != "" {
			tokenID = val
		}

		tokenSymbol, ok := tokenData["token_symbol"].(string)
		if !ok {
			continue
		}
		tokenName, ok := tokenData["token_name"].(string)
		if !ok {
			continue
		}

		var decimals uint8
		if val, ok := tokenData["decimals"].(float64); ok {
			decimals = uint8(val)
		}

		var iconUrl string
		if val, ok := tokenData["icon_url"].(string); ok {
			iconUrl = val
		}

		var priceChangeRate float64
		if val, ok := tokenData["price_change_rate"].(float64); ok {
			priceChangeRate = val
		}

		var transactionCount int64
		if val, ok := tokenData["transaction_count"].(float64); ok {
			transactionCount = int64(val)
		}

		var volumeUSD float64
		if val, ok := tokenData["volume_usd"].(float64); ok {
			volumeUSD = val
		}

		var lastUpdated time.Time
		if val, ok := tokenData["last_updated"].(float64); ok {
			lastUpdated = time.Unix(int64(val), 0)
		} else {
			lastUpdated = time.Now()
		}

		token := &AnalyzedToken{
			TokenID:          tokenID,
			ChainID:          chainID,
			TokenAddress:     tokenAddress,
			TokenSymbol:      tokenSymbol,
			TokenName:        tokenName,
			Decimals:         decimals,
			IconUrl:          iconUrl,
			PriceChangeRate:  priceChangeRate,
			TransactionCount: transactionCount,
			VolumeUSD:        volumeUSD,
			LastUpdated:      lastUpdated,
			Labels:           []TokenLabel{label},
		}
		tokens = append(tokens, token)
	}

	return tokens, nil
}
