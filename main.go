package main

import (
	"crypto-mine/parser"
	"crypto-mine/storage"
	"os"
	"time"
)

func main() {
	candleStorage := storage.NewCandleChartKVStorage(storage.NewCloudflareKV(os.Getenv("cf_account"), os.Getenv("cf_namespace"), os.Getenv("cf_api_key")))
	poolStorage := storage.NewPoolInfo(storage.NewCloudflareD1())
	ethChain := parser.NewEVMChain(poolStorage, "https://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b", "wss://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b")

	minuteChart := storage.NewIntervalCandleChart(time.Minute, candleStorage)
	candleChart := storage.NewCandleChart().RegisterIntervalCandle(minuteChart)
	candleChart.StartAggregateCandleData()

	engine := parser.NewEVMEngine().RegisterChain(ethChain)
	engine.Start()
}
