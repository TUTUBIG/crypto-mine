package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration
type Config struct {
	// Cloudflare settings
	CFAccount   string
	CFNamespace string
	CFAPIKey    string
	WorkerHost  string
	WorkerToken string

	// Chain settings
	RPCEndpoint string
	WSEndpoint  string
	EthPrice    float64

	// Business logic settings
	MinVolumeUSD      float64
	PricePrecision    float64
	CandleInterval    time.Duration
	PollInterval      time.Duration // Long polling interval for fetching logs
	BufferSize        int
	LoadTimeout       time.Duration
	HTTPClientTimeout time.Duration

	// Alert system settings
	AlertRefreshInterval time.Duration // How often to refresh watched tokens from database
	EmailQPS             int           // Emails per second limit
	TelegramQPS          int           // Telegram messages per second limit

	// SMTP settings
	SMTPHost     string // SMTP server host
	SMTPPort     int    // SMTP server port
	SMTPUsername string // SMTP username
	SMTPPassword string // SMTP password
	SMTPFrom     string // From email address
	SMTPFromName string // From name

	// Telegram settings
	TelegramBotToken string // Telegram Bot API token

	// Debug settings
	Debug bool
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() *Config {
	cfg := &Config{
		// Cloudflare settings
		CFAccount:   getEnv("cf_account", "8dac6dbd68790fa6deec035c5b9551b9"),
		CFNamespace: getEnv("cf_namespace", "d425adfc89ad4e9080629fb317a60f1b"),
		CFAPIKey:    getEnv("cf_api_key", "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV"),
		WorkerHost:  getEnv("worker_host", "https://crypto-pump.bigtutu.workers.dev"),
		WorkerToken: getEnv("worker_token", "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV"),

		// Chain settings
		RPCEndpoint: getEnv("rpc_endpoint", "https://eth-mainnet.g.alchemy.com/v2/5Bi_GkXdNyVCeJqq6uACT"),
		WSEndpoint:  getEnv("ws_endpoint", "wss://eth-mainnet.g.alchemy.com/v2/5Bi_GkXdNyVCeJqq6uACT"),
		EthPrice:    getEnvFloat("eth_price", 4035.2),

		// Business logic settings - as per design document
		MinVolumeUSD:      getEnvFloat("min_volume_usd", 1000.0), // $1000 minimum as specified
		PricePrecision:    getEnvFloat("price_precision", 10000.0),
		CandleInterval:    getEnvDuration("candle_interval", 5*time.Minute),
		PollInterval:      getEnvDuration("poll_interval", 12*time.Second), // Long polling interval for fetching logs
		BufferSize:        getEnvInt("buffer_size", 10000),                 // Increased for better throughput
		LoadTimeout:       getEnvDuration("load_timeout", 30*time.Second),
		HTTPClientTimeout: getEnvDuration("http_timeout", 10*time.Second),

		// Alert system settings
		AlertRefreshInterval: getEnvDuration("alert_refresh_interval", 5*time.Minute),
		EmailQPS:             getEnvInt("email_qps", 1),     // 1 emails per second
		TelegramQPS:          getEnvInt("telegram_qps", 20), // 20 telegram messages per second

		// SMTP settings
		SMTPHost:     getEnv("smtp_host", ""),
		SMTPPort:     getEnvInt("smtp_port", 587),
		SMTPUsername: getEnv("smtp_username", ""),
		SMTPPassword: getEnv("smtp_password", ""),
		SMTPFrom:     getEnv("smtp_from", ""),
		SMTPFromName: getEnv("smtp_from_name", "Crypto Price Alert"),

		// Telegram settings
		TelegramBotToken: getEnv("telegram_bot_token", ""),

		// Debug settings
		Debug: getEnv("debug", "true") == "true",
	}

	return cfg
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
