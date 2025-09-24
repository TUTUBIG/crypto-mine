package main

import (
	"crypto-mine/config"
	"crypto-mine/parser"
	"crypto-mine/storage"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func init() {
	_ = os.Setenv("cf_account", "8dac6dbd68790fa6deec035c5b9551b9")
	_ = os.Setenv("cf_namespace", "ccf6622667da4486a4d5b1b2823116b6")
	_ = os.Setenv("cf_api_key", "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV")
	_ = os.Setenv("worker_host", "http://localhost:8787")
	_ = os.Setenv("worker_token", "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV")
}
func main() {
	candleStorage := storage.NewCandleChartKVStorage(storage.NewCloudflareKV(os.Getenv("cf_account"), os.Getenv("cf_namespace"), os.Getenv("cf_api_key")))
	poolStorage := storage.NewPoolInfo(storage.NewCloudflareD1())
	poolStorage.AsyncLoadPools()
	poolStorage.AsyncLoadTokens()

	minuteChart := storage.NewIntervalCandleChart(time.Minute, candleStorage)
	candleChart := storage.NewCandleChart().RegisterIntervalCandle(minuteChart)

	candleChart.StartAggregateCandleData()

	uniSwapV3 := parser.NewUniSwapV3()

	ethChain, err := parser.NewEVMChain(poolStorage, candleChart, "https://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b", "wss://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b")
	if err != nil {
		log.Fatal("Failed to create EVM chain:", err)
	}
	ethChain.RegisterProtocol(uniSwapV3).RegisterStableCoin(config.EthWrapper, []common.Address{config.EvmTetherUSDT, config.EvmUSDC})

	engine := parser.NewEVMEngine().RegisterChain(ethChain)

	if err := engine.Start(); err != nil {
		log.Fatal("Failed to start engine:", err)
	}

	// Monitor Ctrl-C (interrupt) signal and stop the engine gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	fmt.Println("Received interrupt signal, stopping engine...")
	engine.Stop()
}
