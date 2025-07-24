package main

import (
	"crypto-mine/parser"
	"crypto-mine/storage"
	"fmt"
	"os"
	"time"
)

func main() {
	candleStorage := storage.NewCandleChartKVStorage(storage.NewCloudflareKV(os.Getenv("cf_account"), os.Getenv("cf_namespace"), os.Getenv("cf_api_key")))
	poolStorage := storage.NewPoolInfo(storage.NewCloudflareD1())

	minuteChart := storage.NewIntervalCandleChart(time.Minute, candleStorage)
	candleChart := storage.NewCandleChart().RegisterIntervalCandle(minuteChart)
	candleChart.StartAggregateCandleData()

	uniSwapV3 := parser.NewUniSwapV3()

	fmt.Println("dddd")
	ethChain := parser.NewEVMChain(poolStorage, candleChart, "https://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b", "wss://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b")
	ethChain.RegisterProtocol(uniSwapV3)

	engine := parser.NewEVMEngine().RegisterChain(ethChain)
	fmt.Println("aaa")

	engine.Start()

}
