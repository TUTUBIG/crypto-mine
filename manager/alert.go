package manager

import (
	"crypto-mine/orm"
	"crypto-mine/orm/models"
	"crypto-mine/queue"
	"crypto-mine/storage"
	"fmt"
	"sync"
	"time"

	"log/slog"
)

// Candle represents a price candle
type Candle struct {
	ChainID      string    // Chain ID
	TokenAddress string    // Token address
	Timestamp    time.Time // Candle timestamp
	Open         float64   // Opening price
	High         float64   // High price
	Low          float64   // Low price
	Close        float64   // Closing price
	Volume       float64   // Volume
	Interval     string    // Interval: "1m", "5m", "15m", "1h"
}

// PriceChange represents a price change for alert checking
type PriceChange struct {
	ChainID       string
	TokenAddress  string
	Interval      string // "1m", "5m", "15m", "1h"
	ChangePercent float64
	OldPrice      float64
	NewPrice      float64
	Timestamp     time.Time
}

// AlertManager manages price alerts
type AlertManager struct {
	watchedTokenManager *WatchedTokenManager
	userCacheManager    *UserCacheManager
	messageQueue        *queue.MessageQueue
	db                  *orm.DB                       // GORM-like DB instance
	candles             map[string]map[string]*Candle // tokenId (chainId+address) -> interval -> Candle
	candlesMutex        sync.RWMutex
	logger              *slog.Logger
}

// NewAlertManager creates a new alert manager
func NewAlertManager(watchedTokenManager *WatchedTokenManager, userCacheManager *UserCacheManager, messageQueue *queue.MessageQueue) *AlertManager {
	return &AlertManager{
		watchedTokenManager: watchedTokenManager,
		userCacheManager:    userCacheManager,
		messageQueue:        messageQueue,
		db:                  orm.NewDB(watchedTokenManager.ORMClient),
		candles:             make(map[string]map[string]*Candle),
		logger:              slog.Default(),
	}
}

// SetLogger sets a custom logger
func (m *AlertManager) SetLogger(logger *slog.Logger) {
	m.logger = logger
}

// ProcessCandle processes a new candle and checks for alerts
func (m *AlertManager) ProcessCandle(candle *Candle) error {
	m.logger.Debug("Processing candle",
		"chain_id", candle.ChainID,
		"token_address", candle.TokenAddress,
		"interval", candle.Interval,
		"close", candle.Close,
		"timestamp", candle.Timestamp)

	// Generate token ID string for caching
	tokenIdStr := storage.GenerateTokenId(candle.ChainID, candle.TokenAddress)

	// Store the candle
	m.candlesMutex.Lock()
	if m.candles[tokenIdStr] == nil {
		m.candles[tokenIdStr] = make(map[string]*Candle)
	}
	m.candles[tokenIdStr][candle.Interval] = candle
	m.candlesMutex.Unlock()

	// Get watched tokens for this token
	watchedTokens := m.watchedTokenManager.GetWatchedTokensForToken(candle.ChainID, candle.TokenAddress)
	if len(watchedTokens) == 0 {
		return nil // No watched tokens for this token
	}

	// Check for price changes based on interval
	priceChange := m.calculatePriceChange(candle)
	if priceChange == nil {
		return nil // Invalid candle data
	}

	// Check each watched token for alert conditions
	for _, watchedToken := range watchedTokens {
		if !watchedToken.AlertActive {
			continue // Skip inactive alerts
		}

		threshold := m.getThresholdForInterval(watchedToken, candle.Interval)
		if threshold == nil {
			continue // No threshold set for this interval
		}

		// Check if price change exceeds threshold
		changeAbs := priceChange.ChangePercent
		if changeAbs < 0 {
			changeAbs = -changeAbs
		}

		if changeAbs >= *threshold {
			// Trigger alert
			m.triggerAlert(watchedToken, priceChange)
		}
	}

	return nil
}

// calculatePriceChange calculates the price change percentage for a candle
// using Open and Close prices: (close - open) / open * 100
func (m *AlertManager) calculatePriceChange(candle *Candle) *PriceChange {
	// Validate candle data
	if candle == nil || candle.Open <= 0 {
		return nil
	}

	// Calculate price change percentage: (close - open) / open * 100
	changePercent := ((candle.Close - candle.Open) / candle.Open) * 100

	return &PriceChange{
		ChainID:       candle.ChainID,
		TokenAddress:  candle.TokenAddress,
		Interval:      candle.Interval,
		ChangePercent: changePercent,
		OldPrice:      candle.Open,
		NewPrice:      candle.Close,
		Timestamp:     candle.Timestamp,
	}
}

// getThresholdForInterval returns the threshold for a specific interval
func (m *AlertManager) getThresholdForInterval(watchedToken *models.WatchedToken, interval string) *float64 {
	switch interval {
	case "1m":
		return watchedToken.Interval1m
	case "5m":
		return watchedToken.Interval5m
	case "15m":
		return watchedToken.Interval15m
	case "1h":
		return watchedToken.Interval1h
	default:
		return nil
	}
}

// triggerAlert triggers an alert for a watched token
func (m *AlertManager) triggerAlert(watchedToken *models.WatchedToken, priceChange *PriceChange) {
	m.logger.Info("Alert triggered",
		"user_id", watchedToken.UserID,
		"chain_id", priceChange.ChainID,
		"token_address", priceChange.TokenAddress,
		"token_symbol", watchedToken.TokenSymbol,
		"interval", priceChange.Interval,
		"change_percent", priceChange.ChangePercent,
		"old_price", priceChange.OldPrice,
		"new_price", priceChange.NewPrice)

	// Get notification preferences from cache
	userData := m.userCacheManager.GetUserWithPreferences(watchedToken.UserID)
	if userData == nil || userData.Preferences == nil {
		m.logger.Error("User preferences not found in cache", "user_id", watchedToken.UserID)
		return
	}

	prefs := userData.Preferences

	// Create alert message
	message := m.buildAlertMessage(watchedToken, priceChange)

	// Queue email alert if enabled
	if prefs.EmailEnabled && prefs.Email != nil && *prefs.Email != "" {
		emailMsg := &queue.EmailMessage{
			To:      *prefs.Email,
			Subject: fmt.Sprintf("Price Alert: %s", watchedToken.TokenSymbol),
			Body:    message,
		}
		if err := m.messageQueue.PushEmail(emailMsg); err != nil {
			m.logger.Error("Failed to queue email alert", "error", err)
		}
	}

	// Queue telegram alert if enabled
	if prefs.TelegramEnabled && prefs.TelegramID != nil && *prefs.TelegramID != "" {
		telegramMsg := &queue.TelegramMessage{
			ChatID: *prefs.TelegramID,
			Text:   message,
		}
		if err := m.messageQueue.PushTelegram(telegramMsg); err != nil {
			m.logger.Error("Failed to queue telegram alert", "error", err)
		}
	}
}

// buildAlertMessage builds the alert message text
func (m *AlertManager) buildAlertMessage(watchedToken *models.WatchedToken, priceChange *PriceChange) string {
	direction := "⬆️"
	if priceChange.ChangePercent < 0 {
		direction = "⬇️"
	}

	return fmt.Sprintf(
		"🚨 Price Alert\n\n"+
			"Token: %s (%s)\n"+
			"Interval: %s\n"+
			"Change: %s%.2f%%\n"+
			"Price: $%.6f → $%.6f\n"+
			"Time: %s",
		watchedToken.TokenSymbol,
		watchedToken.TokenName,
		priceChange.Interval,
		direction,
		priceChange.ChangePercent,
		priceChange.OldPrice,
		priceChange.NewPrice,
		priceChange.Timestamp.Format("2006-01-02 15:04:05"),
	)
}
