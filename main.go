package main

import (
	"crypto-mine/parser"
	"crypto-mine/storage"
	"database/sql"
	"log"
	"log/slog"
	"os"
	"time"
)

func main() {
	// Database connection
	db, err := sql.Open("postgres", "host=localhost port=5432 user=username password=password dbname=crypto_mine sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			slog.Error("Failed to close database connection", "error", err)
		}
	}(db)

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	candleStorage := storage.NewCandleChartKVStorage(storage.NewCloudflareKV(os.Getenv("cf_account"), os.Getenv("cf_namespace"), os.Getenv("cf_api_key")))
	poolStorage := storage.NewPoolInfo(db)
	ethChain := parser.NewEVMChain(poolStorage, "https://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b", "wss://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b")

	minuteChart := storage.NewIntervalCandleChart(time.Minute, candleStorage)
	candleChart := storage.NewCandleChart().RegisterIntervalCandle(minuteChart)
	candleChart.StartAggregateCandleData()

	engine := parser.NewEVMEngine().RegisterChain(ethChain)
	engine.Start()
}
