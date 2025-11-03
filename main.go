package main

import (
	"crypto-mine/config"
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
	"syscall"
	"time"

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
	candleStorage := storage.NewCandleChartKVStorage(
		storage.NewCloudflareKV(cfg.CFAccount, cfg.CFNamespace, cfg.CFAPIKey))
	cloudflareWorker := storage.NewCloudflareWorker(cfg.WorkerHost, cfg.WorkerToken)
	cloudflareD1 := storage.NewCloudflareD1(cloudflareWorker)
	poolStorage := storage.NewPoolInfo(cloudflareD1)

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

	// Initialize token ID cache
	tokenIDCache := manager.NewTokenIDCache(ormClient, 10*time.Minute)
	tokenIDCache.SetLogger(slog.Default())
	tokenIDCache.Start()
	defer tokenIDCache.Stop()

	// Initialize message queue
	messageQueue := queue.NewMessageQueue(cfg.EmailQPS, cfg.TelegramQPS)
	messageQueue.SetLogger(slog.Default())

	// Set up email sender (placeholder - implement your email service here)
	messageQueue.SetEmailSender(func(msg *queue.EmailMessage) error {
		slog.Info("Email alert", "to", msg.To, "subject", msg.Subject)
		// TODO: Implement email sending (SMTP, SendGrid, etc.)
		return nil
	})

	// Set up telegram sender (placeholder - implement your telegram service here)
	messageQueue.SetTelegramSender(func(msg *queue.TelegramMessage) error {
		slog.Info("Telegram alert", "chat_id", msg.ChatID, "text", msg.Text)
		// TODO: Implement Telegram sending (Telegram Bot API)
		return nil
	})

	messageQueue.Start()
	defer messageQueue.Stop()

	// Initialize alert manager
	alertManager := manager.NewAlertManager(watchedTokenManager, messageQueue)
	alertManager.SetLogger(slog.Default())

	// Setup candle chart with configurable interval and alert integration
	minuteChart := storage.NewIntervalCandleChart(cfg.CandleInterval, candleStorage)
	candleChart := storage.NewCandleChart(cloudflareWorker).RegisterIntervalCandle(minuteChart)

	// Hook alert manager into candle storage
	setupAlertHook(candleChart, alertManager, tokenIDCache)

	candleChart.StartAggregateCandleData()

	// Initialize DEX protocols
	uniSwapV3 := parser.NewUniSwapV3()

	// Create EVM chain monitor
	ethChain, err := parser.NewEVMChain(poolStorage, candleChart, cfg.RPCEndpoint, cfg.WSEndpoint)
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

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	slog.Info("Received shutdown signal, stopping engine...")

	// Stop the engine gracefully
	engine.Stop()

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
	if cfg.WSEndpoint == "" {
		return fmt.Errorf("ws_endpoint is required")
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
func setupAlertHook(candleChart *storage.CandleChart, alertManager *manager.AlertManager, tokenIDCache *manager.TokenIDCache) {
	candleChart.SetCandleStoredCallback(func(chainId, tokenAddress string, candleData *storage.CandleData, interval time.Duration) {
		// Get database token ID
		dbTokenID, err := tokenIDCache.GetOrFetchDatabaseTokenID(chainId, tokenAddress)
		if err != nil {
			// Token not in database, skip alert processing
			slog.Debug("Token not found in database, skipping alert",
				"chain_id", chainId,
				"token_address", tokenAddress,
				"error", err)
			return
		}

		// Convert interval to string format
		intervalStr := formatInterval(interval)

		// Convert CandleData to manager.Candle
		alertCandle := &manager.Candle{
			TokenID:   dbTokenID,
			Timestamp: candleData.Timestamp,
			Open:      candleData.OpenPrice,
			High:      candleData.HighPrice,
			Low:       candleData.LowPrice,
			Close:     candleData.ClosePrice,
			Volume:    candleData.VolumeUSD,
			Interval:  intervalStr,
		}

		// Process candle for alerts
		if err := alertManager.ProcessCandle(alertCandle); err != nil {
			slog.Error("Failed to process candle for alerts",
				"token_id", dbTokenID,
				"error", err)
		}
	})
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
