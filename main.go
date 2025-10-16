package main

import (
	"crypto-mine/config"
	"crypto-mine/parser"
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

	// Set log level based on debug flag
	if cfg.Debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Info("Debug mode enabled")
	} else {
		slog.SetLogLoggerLevel(slog.LevelInfo)
	}

	// Validate required configuration
	if err := validateConfig(cfg); err != nil {
		log.Fatalf("Configuration error: %v", err)
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

	// Setup candle chart with configurable interval
	minuteChart := storage.NewIntervalCandleChart(cfg.CandleInterval, candleStorage)
	candleChart := storage.NewCandleChart(cloudflareWorker).RegisterIntervalCandle(minuteChart)
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
