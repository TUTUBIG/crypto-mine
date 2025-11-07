package orm

import (
	"testing"

	"crypto-mine/orm/models"
)

func TestConvertRowToModel_User(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]interface{}
		wantErr bool
		check   func(*models.User) bool
	}{
		{
			name: "basic user conversion",
			row: map[string]interface{}{
				"id":                1,
				"email":             "test@example.com",
				"telegram_id":       "123456",
				"telegram_username": "testuser",
				"bot_started":       1,
				"created_at":        "2024-01-01T00:00:00Z",
				"updated_at":        "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(u *models.User) bool {
				return u.ID == 1 &&
					u.Email != nil && *u.Email == "test@example.com" &&
					u.TelegramID != nil && *u.TelegramID == "123456" &&
					u.TelegramUsername != nil && *u.TelegramUsername == "testuser" &&
					u.BotStarted == true &&
					u.CreatedAt == "2024-01-01T00:00:00Z"
			},
		},
		{
			name: "user with nil optional fields",
			row: map[string]interface{}{
				"id":          2,
				"bot_started": 0,
				"created_at":  "2024-01-01T00:00:00Z",
				"updated_at":  "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(u *models.User) bool {
				return u.ID == 2 &&
					u.Email == nil &&
					u.TelegramID == nil &&
					u.BotStarted == false
			},
		},
		{
			name: "user with boolean from uint8",
			row: map[string]interface{}{
				"id":          3,
				"bot_started": uint8(1),
				"created_at":  "2024-01-01T00:00:00Z",
				"updated_at":  "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(u *models.User) bool {
				return u.ID == 3 && u.BotStarted == true
			},
		},
		{
			name: "user with boolean from uint64",
			row: map[string]interface{}{
				"id":          4,
				"bot_started": uint64(0),
				"created_at":  "2024-01-01T00:00:00Z",
				"updated_at":  "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(u *models.User) bool {
				return u.ID == 4 && u.BotStarted == false
			},
		},
		{
			name: "user with boolean from float64",
			row: map[string]interface{}{
				"id":          5,
				"bot_started": float64(1.0),
				"created_at":  "2024-01-01T00:00:00Z",
				"updated_at":  "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(u *models.User) bool {
				return u.ID == 5 && u.BotStarted == true
			},
		},
		{
			name: "user with empty string pointer",
			row: map[string]interface{}{
				"id":          6,
				"email":       "",
				"bot_started": false,
				"created_at":  "2024-01-01T00:00:00Z",
				"updated_at":  "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(u *models.User) bool {
				return u.ID == 6 && u.Email == nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var user models.User
			err := ConvertRowToModel(tt.row, &user)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertRowToModel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.check(&user) {
				t.Errorf("ConvertRowToModel() result validation failed")
			}
		})
	}
}

func TestConvertRowToModel_Token(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]interface{}
		wantErr bool
		check   func(*models.Token) bool
	}{
		{
			name: "token with float64 pointer",
			row: map[string]interface{}{
				"id":               1,
				"chain_id":         "ethereum",
				"token_address":    "0x123",
				"token_symbol":     "ETH",
				"token_name":       "Ethereum",
				"decimals":         18,
				"daily_volume_usd": float64(1000000.5),
				"created_at":       "2024-01-01T00:00:00Z",
				"updated_at":       "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(t *models.Token) bool {
				return t.ID == 1 &&
					t.ChainID == "ethereum" &&
					t.DailyVolumeUSD != nil &&
					*t.DailyVolumeUSD == 1000000.5
			},
		},
		{
			name: "token with int64 volume",
			row: map[string]interface{}{
				"id":               2,
				"chain_id":         "ethereum",
				"token_address":    "0x456",
				"token_symbol":     "BTC",
				"token_name":       "Bitcoin",
				"decimals":         8,
				"daily_volume_usd": int64(2000000),
				"created_at":       "2024-01-01T00:00:00Z",
				"updated_at":       "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(t *models.Token) bool {
				return t.ID == 2 &&
					t.DailyVolumeUSD != nil &&
					*t.DailyVolumeUSD == 2000000.0
			},
		},
		{
			name: "token with uint64 volume",
			row: map[string]interface{}{
				"id":               3,
				"chain_id":         "ethereum",
				"token_address":    "0x789",
				"token_symbol":     "USDT",
				"token_name":       "Tether",
				"decimals":         6,
				"daily_volume_usd": uint64(3000000),
				"created_at":       "2024-01-01T00:00:00Z",
				"updated_at":       "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(t *models.Token) bool {
				return t.ID == 3 &&
					t.DailyVolumeUSD != nil &&
					*t.DailyVolumeUSD == 3000000.0
			},
		},
		{
			name: "token with nil volume",
			row: map[string]interface{}{
				"id":            4,
				"chain_id":      "ethereum",
				"token_address": "0xabc",
				"token_symbol":  "USDC",
				"token_name":    "USD Coin",
				"decimals":      6,
				"created_at":    "2024-01-01T00:00:00Z",
				"updated_at":    "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(t *models.Token) bool {
				return t.ID == 4 && t.DailyVolumeUSD == nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var token models.Token
			err := ConvertRowToModel(tt.row, &token)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertRowToModel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.check(&token) {
				t.Errorf("ConvertRowToModel() result validation failed")
			}
		})
	}
}

func TestConvertRowToModel_NotificationPreferences(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]interface{}
		wantErr bool
		check   func(*models.NotificationPreferences) bool
	}{
		{
			name: "notification preferences with uint8 booleans",
			row: map[string]interface{}{
				"id":               1,
				"user_id":          100,
				"email_enabled":    uint8(1),
				"telegram_enabled": uint8(0),
				"record_refresh":   uint8(1),
				"created_at":       "2024-01-01T00:00:00Z",
				"updated_at":       "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(np *models.NotificationPreferences) bool {
				return np.ID == 1 &&
					np.UserID == 100 &&
					np.EmailEnabled == true &&
					np.TelegramEnabled == false &&
					np.RecordRefresh == true
			},
		},
		{
			name: "notification preferences with int booleans",
			row: map[string]interface{}{
				"id":               2,
				"user_id":          200,
				"email_enabled":    int(0),
				"telegram_enabled": int(1),
				"record_refresh":   int(0),
				"created_at":       "2024-01-01T00:00:00Z",
				"updated_at":       "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(np *models.NotificationPreferences) bool {
				return np.ID == 2 &&
					np.UserID == 200 &&
					np.EmailEnabled == false &&
					np.TelegramEnabled == true &&
					np.RecordRefresh == false
			},
		},
		{
			name: "notification preferences with float64 booleans",
			row: map[string]interface{}{
				"id":               3,
				"user_id":          300,
				"email_enabled":    float64(1.0),
				"telegram_enabled": float64(0.0),
				"record_refresh":   float64(1.0),
				"created_at":       "2024-01-01T00:00:00Z",
				"updated_at":       "2024-01-01T00:00:00Z",
			},
			wantErr: false,
			check: func(np *models.NotificationPreferences) bool {
				return np.ID == 3 &&
					np.UserID == 300 &&
					np.EmailEnabled == true &&
					np.TelegramEnabled == false &&
					np.RecordRefresh == true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var prefs models.NotificationPreferences
			err := ConvertRowToModel(tt.row, &prefs)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertRowToModel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.check(&prefs) {
				t.Errorf("ConvertRowToModel() result validation failed")
			}
		})
	}
}

func TestConvertRowToModel_ErrorCases(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]interface{}
		target  interface{}
		wantErr bool
	}{
		{
			name:    "nil row",
			row:     nil,
			target:  &models.User{},
			wantErr: true,
		},
		{
			name:    "non-pointer target",
			row:     map[string]interface{}{"id": 1},
			target:  models.User{},
			wantErr: true,
		},
		{
			name:    "nil target",
			row:     map[string]interface{}{"id": 1},
			target:  (*models.User)(nil),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ConvertRowToModel(tt.row, tt.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertRowToModel() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConvertRowToModel_UnknownFields(t *testing.T) {
	// Unknown fields should be silently ignored
	row := map[string]interface{}{
		"id":            1,
		"unknown_field": "should be ignored",
		"bot_started":   true,
		"created_at":    "2024-01-01T00:00:00Z",
		"updated_at":    "2024-01-01T00:00:00Z",
	}

	var user models.User
	err := ConvertRowToModel(row, &user)
	if err != nil {
		t.Errorf("ConvertRowToModel() error = %v, expected no error for unknown fields", err)
	}
	if user.ID != 1 {
		t.Errorf("ConvertRowToModel() ID = %d, want 1", user.ID)
	}
}

func TestConvertRowToModel_TypeConversions(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]interface{}
		check   func(*models.User) bool
		wantErr bool
	}{
		{
			name: "int from int64",
			row: map[string]interface{}{
				"id":          int64(123),
				"bot_started": false,
				"created_at":  "2024-01-01T00:00:00Z",
				"updated_at":  "2024-01-01T00:00:00Z",
			},
			check: func(u *models.User) bool {
				return u.ID == 123
			},
			wantErr: false,
		},
		{
			name: "int from uint64",
			row: map[string]interface{}{
				"id":          uint64(456),
				"bot_started": false,
				"created_at":  "2024-01-01T00:00:00Z",
				"updated_at":  "2024-01-01T00:00:00Z",
			},
			check: func(u *models.User) bool {
				return u.ID == 456
			},
			wantErr: false,
		},
		{
			name: "int from float64",
			row: map[string]interface{}{
				"id":          float64(789.0),
				"bot_started": false,
				"created_at":  "2024-01-01T00:00:00Z",
				"updated_at":  "2024-01-01T00:00:00Z",
			},
			check: func(u *models.User) bool {
				return u.ID == 789
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var user models.User
			err := ConvertRowToModel(tt.row, &user)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertRowToModel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.check(&user) {
				t.Errorf("ConvertRowToModel() result validation failed")
			}
		})
	}
}

// Benchmark tests
func BenchmarkConvertRowToModel_User(b *testing.B) {
	row := map[string]interface{}{
		"id":                1,
		"email":             "test@example.com",
		"telegram_id":       "123456",
		"telegram_username": "testuser",
		"bot_started":       1,
		"created_at":        "2024-01-01T00:00:00Z",
		"updated_at":        "2024-01-01T00:00:00Z",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var user models.User
		_ = ConvertRowToModel(row, &user)
	}
}
