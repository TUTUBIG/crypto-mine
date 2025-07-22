package main

import (
	"crypto-mine/parser"
	"crypto-mine/storage"
	"time"
)

func main() {
	poolInfo := storage.NewPoolInfo(nil)
	ethChain := parser.NewEVMChain(poolInfo, "https://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b", "wss://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b")

	minuteChart := storage.NewIntervalCandleChart(time.Minute, &storage.TemporaryStorageCandleChart{})
	candleChart := storage.NewCandleChart().RegisterIntervalCandle(minuteChart)
	candleChart.StartAggregateCandleData()

	engine := parser.NewEVMEngine().RegisterChain(ethChain)
	engine.Start()

}
