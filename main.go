package main

import (
	"crypto-mine/config"
	"crypto-mine/email"
	"crypto-mine/manager"
	"crypto-mine/orm"
	"crypto-mine/parser"
	"crypto-mine/queue"
	"crypto-mine/storage"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	tgbotapi "github.com/TUTUBIG/telegram-bot-api/v5"
	"github.com/ethereum/go-ethereum/common"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Validate required configuration
	if err := validateConfig(cfg); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Set log level based on debug flag
	if cfg.Debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Info("Debug mode enabled")
	} else {
		slog.SetLogLoggerLevel(slog.LevelInfo)
	}

	// Initialize storage systems
	kvDriver := storage.NewCloudflareKV(cfg.CFAccount, cfg.CFNamespace, cfg.CFAPIKey)
	candleStorage := storage.NewCandleChartKVStorage(kvDriver)
	candleStorage.SetIntervalModNumbers(storage.IntervalModNumbers)
	cloudflareWorker := storage.NewCloudflareWorker(cfg.WorkerHost, cfg.WorkerToken)
	cloudflareD1 := storage.NewCloudflareD1(cloudflareWorker)
	poolStorage := storage.NewPoolInfo(cloudflareD1)

	// Set up special token check function for candle storage
	candleStorage.SetIsSpecialTokenFunc(func(tokenID string) bool {
		// Parse tokenID: chainId-tokenAddress
		parts := strings.Split(tokenID, "-")
		if len(parts) != 2 {
			return false
		}
		token := poolStorage.FindToken(parts[0], parts[1])
		return token != nil && token.IsSpecial
	})

	// Load pools and tokens asynchronously
	poolStorage.AsyncLoadPools()
	poolStorage.AsyncLoadTokens()

	// Initialize ORM client for alert system
	ormClient := orm.NewClient(cfg.WorkerHost, cfg.WorkerToken)
	ormClient.SetLogger(slog.Default())

	// Initialize alert system components
	watchedTokenManager := manager.NewWatchedTokenManager(ormClient, cfg.AlertRefreshInterval)
	watchedTokenManager.SetLogger(slog.Default())
	if err := watchedTokenManager.Start(); err != nil {
		log.Fatalf("Failed to start watched token manager: %v", err)
	}
	defer watchedTokenManager.Stop()

	// Initialize user cache manager
	userCacheManager := manager.NewUserCacheManager(ormClient, cfg.AlertRefreshInterval)
	userCacheManager.SetLogger(slog.Default())
	if err := userCacheManager.Start(); err != nil {
		log.Fatalf("Failed to start user cache manager: %v", err)
	}
	defer userCacheManager.Stop()

	// Initialize message queue
	messageQueue := queue.NewMessageQueue(cfg.EmailQPS, cfg.TelegramQPS)
	messageQueue.SetLogger(slog.Default())

	// Set up email sender using SMTP
	if cfg.SMTPHost != "" && cfg.SMTPFrom != "" {
		emailClient := email.NewSMTPClient(&email.SMTPConfig{
			Host:     cfg.SMTPHost,
			Port:     cfg.SMTPPort,
			Username: cfg.SMTPUsername,
			Password: cfg.SMTPPassword,
			From:     cfg.SMTPFrom,
			FromName: cfg.SMTPFromName,
			Logger:   slog.Default(),
		})
		messageQueue.SetEmailSender(func(msg *queue.EmailMessage) error {
			return emailClient.SendEmailWithTLS(msg.To, msg.Subject, msg.Body)
		})
		slog.Info("SMTP email sender configured", "host", cfg.SMTPHost, "port", cfg.SMTPPort, "from", cfg.SMTPFrom)
	} else {
		// Fallback to placeholder if SMTP is not configured
		messageQueue.SetEmailSender(func(msg *queue.EmailMessage) error {
			slog.Warn("SMTP not configured, email not sent", "to", msg.To, "subject", msg.Subject)
			return nil
		})
		slog.Warn("SMTP not configured, email sending disabled")
	}

	// Set up telegram sender using Telegram Bot API
	if cfg.TelegramBotToken != "" {
		bot, err := tgbotapi.NewBotAPI(cfg.TelegramBotToken)
		if err != nil {
			slog.Error("Failed to create Telegram bot", "error", err)
			// Fallback to placeholder
			messageQueue.SetTelegramSender(func(msg *queue.TelegramMessage) error {
				slog.Warn("Telegram bot not configured, message not sent", "chat_id", msg.ChatID, "text", msg.Text)
				return nil
			})
		} else {
			bot.Debug = cfg.Debug
			slog.Info("Telegram bot initialized", "username", bot.Self.UserName)
			messageQueue.SetTelegramSender(func(msg *queue.TelegramMessage) error {
				// Parse chat ID (could be int64 or string)
				chatID, err := parseChatID(msg.ChatID)
				if err != nil {
					return fmt.Errorf("invalid chat ID: %w", err)
				}
				telegramMsg := tgbotapi.NewMessage(chatID, msg.Text)
				telegramMsg.ParseMode = tgbotapi.ModeHTML
				_, err = bot.Send(telegramMsg)
				if err != nil {
					return fmt.Errorf("failed to send Telegram message: %w", err)
				}
				return nil
			})
		}
	} else {
		// Fallback to placeholder if Telegram bot token is not configured
		messageQueue.SetTelegramSender(func(msg *queue.TelegramMessage) error {
			slog.Warn("Telegram bot token not configured, message not sent", "chat_id", msg.ChatID, "text", msg.Text)
			return nil
		})
		slog.Warn("Telegram bot token not configured, Telegram sending disabled")
	}

	messageQueue.Start()
	defer messageQueue.Stop()

	// Initialize alert manager
	alertManager := manager.NewAlertManager(watchedTokenManager, userCacheManager, messageQueue)
	alertManager.SetLogger(slog.Default())

	// Initialize token analyzer for offline analysis
	tokenAnalyzer := manager.NewTokenAnalyzer(poolStorage, candleStorage, cfg.CandleInterval, ormClient, kvDriver)
	tokenAnalyzer.SetLogger(slog.Default())

	// Setup candle chart with configurable interval and alert integration
	minuteChart := storage.NewIntervalCandleChart(cfg.CandleInterval, candleStorage)
	candleChart := storage.NewCandleChart(cloudflareWorker).RegisterIntervalCandle(minuteChart)

	// Set up special token check function for rate limiting realtime price updates
	candleChart.SetIsSpecialTokenFunc(func(tokenID string) bool {
		// Parse tokenID: chainId-tokenAddress
		parts := strings.Split(tokenID, "-")
		if len(parts) != 2 {
			return false
		}
		token := poolStorage.FindToken(parts[0], parts[1])
		return token != nil && token.IsSpecial
	})

	// Hook alert manager and token analyzer into candle storage
	setupAlertHook(candleChart, alertManager)
	setupTokenAnalyzerHook(candleChart, tokenAnalyzer)

	candleChart.StartAggregateCandleData()

	// Initialize DEX protocols
	uniSwapV3 := parser.NewUniSwapV3()

	// Create EVM chain monitor
	ethChain, err := parser.NewEVMChain(poolStorage, candleChart, cfg.RPCEndpoint, kvDriver, cfg.PollInterval)
	if err != nil {
		log.Fatalf("Failed to create EVM chain: %v", err)
	}

	// Start buffer monitoring goroutine
	go monitorBuffers(ethChain)

	// Configure stablecoins and native wrapper
	ethPriceStr := strconv.FormatFloat(cfg.EthPrice, 'f', -1, 64)
	ethChain.RegisterProtocol(uniSwapV3).RegisterStableCoin(
		config.EthWrapper,
		ethPriceStr,
		[]common.Address{
			config.EvmTetherUSDT,
			config.EvmUSDC,
			config.EvmUSD1,
			config.EvmUSDe,
		},
	)

	// Create and start engine
	engine := parser.NewEVMEngine().RegisterChain(ethChain)
	if err := engine.Start(); err != nil {
		log.Fatalf("Failed to start engine: %v", err)
	}

	slog.Info("Crypto mining engine started successfully",
		"chain_id", ethChain.ChainID(),
		"min_volume_usd", cfg.MinVolumeUSD,
		"candle_interval", cfg.CandleInterval)

	// Start periodic token ranking and tagging (every 5 minutes)
	go startTokenRanking(tokenAnalyzer)

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	slog.Info("Received shutdown signal, stopping engine...")

	// Stop the engine gracefully
	engine.Stop()

	// Flush all pending candles to KV before shutdown
	if err := candleStorage.Shutdown(); err != nil {
		slog.Error("Failed to shutdown candle storage", "error", err)
	}

	slog.Info("Engine stopped successfully")
}

// validateConfig checks if all required configuration values are set
func validateConfig(cfg *config.Config) error {
	if cfg.CFAccount == "" {
		return fmt.Errorf("cf_account is required")
	}
	if cfg.CFNamespace == "" {
		return fmt.Errorf("cf_namespace is required")
	}
	if cfg.CFAPIKey == "" {
		return fmt.Errorf("cf_api_key is required")
	}
	if cfg.WorkerHost == "" {
		return fmt.Errorf("worker_host is required")
	}
	if cfg.RPCEndpoint == "" {
		return fmt.Errorf("rpc_endpoint is required")
	}

	return nil
}

// monitorBuffers periodically logs buffer status
func monitorBuffers(chain *parser.EVMChain) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		chain.PrintBuffer()
	}
}

// setupAlertHook hooks the alert manager into the candle storage process
func setupAlertHook(candleChart *storage.CandleChart, alertManager *manager.AlertManager) {
	candleChart.SetCandleStoredCallback(func(chainId, tokenAddress string, candleData *storage.CandleData, interval time.Duration) {
		// Convert interval to string format
		intervalStr := formatInterval(interval)

		// Convert CandleData to manager.Candle
		alertCandle := &manager.Candle{
			ChainID:      chainId,
			TokenAddress: tokenAddress,
			Timestamp:    candleData.Timestamp,
			Open:         candleData.OpenPrice,
			High:         candleData.HighPrice,
			Low:          candleData.LowPrice,
			Close:        candleData.ClosePrice,
			Volume:       candleData.VolumeUSD,
			Interval:     intervalStr,
		}

		// Process candle for alerts
		if err := alertManager.ProcessCandle(alertCandle); err != nil {
			slog.Error("Failed to process candle for alerts",
				"chain_id", chainId,
				"token_address", tokenAddress,
				"error", err)
		}
	})
}

// setupTokenAnalyzerHook hooks the token analyzer into the candle storage process
func setupTokenAnalyzerHook(candleChart *storage.CandleChart, tokenAnalyzer *manager.TokenAnalyzer) {
	// Store the original callback
	originalCallback := candleChart.GetCandleStoredCallback()

	// Set a new callback that calls both the original and token analyzer
	candleChart.SetCandleStoredCallback(func(chainId, tokenAddress string, candleData *storage.CandleData, interval time.Duration) {
		// Call original callback if it exists
		if originalCallback != nil {
			originalCallback(chainId, tokenAddress, candleData, interval)
		}

		// Update token analyzer cache
		tokenAnalyzer.UpdateCandleCache(chainId, tokenAddress, candleData, interval)
	})
}

// parseChatID parses a chat ID string to int64
// Telegram chat IDs can be strings or int64, but the API expects int64
func parseChatID(chatIDStr string) (int64, error) {
	// Try to parse as int64
	chatID, err := strconv.ParseInt(chatIDStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid chat ID format: %s", chatIDStr)
	}
	return chatID, nil
}

// formatInterval converts a time.Duration to interval string format
func formatInterval(interval time.Duration) string {
	switch interval {
	case time.Minute:
		return "1m"
	case 5 * time.Minute:
		return "5m"
	case 15 * time.Minute:
		return "15m"
	case time.Hour:
		return "1h"
	default:
		// For custom intervals, convert to approximate format
		minutes := int(interval.Minutes())
		if minutes < 60 {
			return fmt.Sprintf("%dm", minutes)
		}
		hours := minutes / 60
		return fmt.Sprintf("%dh", hours)
	}
}

// startTokenRanking starts a periodic task to rank tokens and update tags in database
func startTokenRanking(tokenAnalyzer *manager.TokenAnalyzer) {
	// Run every 5 minutes
	ticker := time.NewTicker(tokenAnalyzer.GetInterval())
	defer ticker.Stop()

	// Run periodically
	for range ticker.C {
		if err := tokenAnalyzer.RankAndUpdateTopTokens(); err != nil {
			slog.Error("Failed to rank and update tokens", "error", err)
		}
	}
}
