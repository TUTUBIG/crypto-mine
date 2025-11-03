package manager

import (
	"crypto-mine/orm/models"
	"crypto-mine/queue"
	"fmt"
	"sync"
	"time"

	"log/slog"
)

// convertRowToUser converts a database row to User model
func convertRowToUser(row map[string]interface{}) *models.User {
	user := &models.User{}

	if id, ok := row["id"].(float64); ok {
		user.ID = int(id)
	}
	if email, ok := row["email"].(string); ok && email != "" {
		user.Email = &email
	}
	if telegramID, ok := row["telegram_id"].(string); ok && telegramID != "" {
		user.TelegramID = &telegramID
	}
	if telegramUsername, ok := row["telegram_username"].(string); ok && telegramUsername != "" {
		user.TelegramUsername = &telegramUsername
	}
	if fullName, ok := row["full_name"].(string); ok && fullName != "" {
		user.FullName = &fullName
	}
	if avatarURL, ok := row["avatar_url"].(string); ok && avatarURL != "" {
		user.AvatarURL = &avatarURL
	}
	if botStarted, ok := row["bot_started"].(bool); ok {
		user.BotStarted = botStarted
	}
	if botStartedAt, ok := row["bot_started_at"].(string); ok && botStartedAt != "" {
		user.BotStartedAt = &botStartedAt
	}
	if createdAt, ok := row["created_at"].(string); ok {
		user.CreatedAt = createdAt
	}
	if updatedAt, ok := row["updated_at"].(string); ok {
		user.UpdatedAt = updatedAt
	}

	return user
}

// convertRowToNotificationPreferences converts a database row to NotificationPreferences model
func convertRowToNotificationPreferences(row map[string]interface{}) *models.NotificationPreferences {
	prefs := &models.NotificationPreferences{}

	if userID, ok := row["user_id"].(float64); ok {
		prefs.UserID = int(userID)
	}
	if emailEnabled, ok := row["email_enabled"].(bool); ok {
		prefs.EmailEnabled = emailEnabled
	} else {
		prefs.EmailEnabled = true // Default
	}
	if telegramEnabled, ok := row["telegram_enabled"].(bool); ok {
		prefs.TelegramEnabled = telegramEnabled
	} else {
		prefs.TelegramEnabled = false // Default
	}

	return prefs
}

// Candle represents a price candle
type Candle struct {
	TokenID   int       // Token ID in database
	Timestamp time.Time // Candle timestamp
	Open      float64   // Opening price
	High      float64   // High price
	Low       float64   // Low price
	Close     float64   // Closing price
	Volume    float64   // Volume
	Interval  string    // Interval: "1m", "5m", "15m", "1h"
}

// PriceChange represents a price change for alert checking
type PriceChange struct {
	TokenID       int
	Interval      string // "1m", "5m", "15m", "1h"
	ChangePercent float64
	OldPrice      float64
	NewPrice      float64
	Timestamp     time.Time
}

// AlertManager manages price alerts
type AlertManager struct {
	watchedTokenManager *WatchedTokenManager
	messageQueue        *queue.MessageQueue
	candles             map[int]map[string]*Candle // tokenID -> interval -> Candle
	candlesMutex        sync.RWMutex
	logger              *slog.Logger
}

// NewAlertManager creates a new alert manager
func NewAlertManager(watchedTokenManager *WatchedTokenManager, messageQueue *queue.MessageQueue) *AlertManager {
	return &AlertManager{
		watchedTokenManager: watchedTokenManager,
		messageQueue:        messageQueue,
		candles:             make(map[int]map[string]*Candle),
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
		"token_id", candle.TokenID,
		"interval", candle.Interval,
		"close", candle.Close,
		"timestamp", candle.Timestamp)

	// Store the candle
	m.candlesMutex.Lock()
	if m.candles[candle.TokenID] == nil {
		m.candles[candle.TokenID] = make(map[string]*Candle)
	}
	m.candles[candle.TokenID][candle.Interval] = candle
	m.candlesMutex.Unlock()

	// Get watched tokens for this token ID
	watchedTokens := m.watchedTokenManager.GetWatchedTokensForTokenID(candle.TokenID)
	if len(watchedTokens) == 0 {
		return nil // No watched tokens for this token
	}

	// Check for price changes based on interval
	priceChange := m.calculatePriceChange(candle)
	if priceChange == nil {
		return nil // No previous candle to compare
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
func (m *AlertManager) calculatePriceChange(candle *Candle) *PriceChange {
	m.candlesMutex.RLock()
	defer m.candlesMutex.RUnlock()

	// Get previous candle for the same interval
	tokenCandles, exists := m.candles[candle.TokenID]
	if !exists {
		return nil
	}

	prevCandle, exists := tokenCandles[candle.Interval]
	if !exists {
		return nil
	}

	// Calculate price change percentage
	oldPrice := prevCandle.Close
	newPrice := candle.Close
	changePercent := ((newPrice - oldPrice) / oldPrice) * 100

	return &PriceChange{
		TokenID:       candle.TokenID,
		Interval:      candle.Interval,
		ChangePercent: changePercent,
		OldPrice:      oldPrice,
		NewPrice:      newPrice,
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
		"token_id", priceChange.TokenID,
		"token_symbol", watchedToken.TokenSymbol,
		"interval", priceChange.Interval,
		"change_percent", priceChange.ChangePercent,
		"old_price", priceChange.OldPrice,
		"new_price", priceChange.NewPrice)

	// Get user and notification preferences using SQL
	userSQL := `SELECT * FROM users WHERE id = ?`
	userRow, err := m.watchedTokenManager.ORMClient.ExecuteSQLSingle(userSQL, watchedToken.UserID)
	if err != nil {
		m.logger.Error("Failed to get user for alert", "user_id", watchedToken.UserID, "error", err)
		return
	}
	user := convertRowToUser(userRow)

	prefsSQL := `SELECT * FROM notification_preferences WHERE user_id = ?`
	prefsRow, err := m.watchedTokenManager.ORMClient.ExecuteSQLSingle(prefsSQL, watchedToken.UserID)
	if err != nil {
		// Use defaults if preferences not found
		m.logger.Debug("Notification preferences not found, using defaults", "user_id", watchedToken.UserID)
		prefsRow = map[string]interface{}{
			"user_id":          watchedToken.UserID,
			"email_enabled":    true,
			"telegram_enabled": false,
		}
	}
	prefs := convertRowToNotificationPreferences(prefsRow)

	// Create alert message
	message := m.buildAlertMessage(watchedToken, priceChange)

	// Queue email alert if enabled
	if prefs.EmailEnabled && user.Email != nil && *user.Email != "" {
		emailMsg := &queue.EmailMessage{
			To:      *user.Email,
			Subject: fmt.Sprintf("Price Alert: %s", watchedToken.TokenSymbol),
			Body:    message,
		}
		if err := m.messageQueue.PushEmail(emailMsg); err != nil {
			m.logger.Error("Failed to queue email alert", "error", err)
		}
	}

	// Queue telegram alert if enabled
	if prefs.TelegramEnabled && user.TelegramID != nil && *user.TelegramID != "" {
		telegramMsg := &queue.TelegramMessage{
			ChatID: *user.TelegramID,
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
