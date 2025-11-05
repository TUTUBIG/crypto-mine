package models

// User represents a user in the system
type User struct {
	ID               int     `json:"id"`
	Email            *string `json:"email,omitempty"`
	TelegramID       *string `json:"telegram_id,omitempty"`
	TelegramUsername *string `json:"telegram_username,omitempty"`
	GoogleID         *string `json:"google_id,omitempty"`
	AppleID          *string `json:"apple_id,omitempty"`
	XID              *string `json:"x_id,omitempty"`
	FullName         *string `json:"full_name,omitempty"`
	AvatarURL        *string `json:"avatar_url,omitempty"`
	BotStarted       bool    `json:"bot_started"`
	BotStartedAt     *string `json:"bot_started_at,omitempty"`
	CreatedAt        string  `json:"created_at"`
	UpdatedAt        string  `json:"updated_at"`
	LastLoginAt      *string `json:"last_login_at,omitempty"`
}

// Token represents a token in the system
type Token struct {
	ID              int      `json:"id"`
	ChainID         string   `json:"chain_id"`
	TokenAddress    string   `json:"token_address"`
	TokenSymbol     string   `json:"token_symbol"`
	TokenName       string   `json:"token_name"`
	Decimals        int      `json:"decimals"`
	IconURL         *string  `json:"icon_url,omitempty"`
	DailyVolumeUSD  *float64 `json:"daily_volume_usd,omitempty"`
	VolumeUpdatedAt *string  `json:"volume_updated_at,omitempty"`
	CreatedAt       string   `json:"created_at"`
	UpdatedAt       string   `json:"updated_at"`
}

// WatchedToken represents a watched token with alert settings
type WatchedToken struct {
	ID            int      `json:"id"`
	UserID        int      `json:"user_id"`
	TokenID       int      `json:"token_id"`
	Notes         *string  `json:"notes,omitempty"`
	Interval1m    *float64 `json:"interval_1m,omitempty"`  // Price change threshold for 1 minute (percentage)
	Interval5m    *float64 `json:"interval_5m,omitempty"`  // Price change threshold for 5 minutes (percentage)
	Interval15m   *float64 `json:"interval_15m,omitempty"` // Price change threshold for 15 minutes (percentage)
	Interval1h    *float64 `json:"interval_1h,omitempty"`  // Price change threshold for 1 hour (percentage)
	AlertActive   bool     `json:"alert_active"`
	RecordRefresh bool     `json:"record_refresh"` // Flag to indicate this record needs to be synced with memory
	CreatedAt     string   `json:"created_at"`

	// Token details (from JOIN)
	ChainID      string  `json:"chain_id"`
	TokenAddress string  `json:"token_address"`
	TokenSymbol  string  `json:"token_symbol"`
	TokenName    string  `json:"token_name"`
	Decimals     int     `json:"decimals"`
	IconURL      *string `json:"icon_url,omitempty"`
}

// NotificationPreferences represents user notification preferences
type NotificationPreferences struct {
	ID              int     `json:"id"`
	UserID          int     `json:"user_id"`
	Email           *string `json:"email,omitempty"`
	TelegramID      *string `json:"telegram_id,omitempty"`
	EmailEnabled    bool    `json:"email_enabled"`
	TelegramEnabled bool    `json:"telegram_enabled"`
	RecordRefresh   bool    `json:"record_refresh"` // Flag to indicate this record needs to be synced with memory
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
}
