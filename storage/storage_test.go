package storage

import (
	"encoding/base64"
	"net/http"
	"testing"
	"time"
)

const (
	workerHost  = "crypto-pump.bigtutu.workers.dev"
	workerToken = "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV"
)

var n = time.Now()

func TestCandleDataList_Base64Encode(t *testing.T) {
	cl := make(CandleDataList, 0)
	cl = append(cl, CandleData{
		OpenPrice:  0.1,
		ClosePrice: 0.2,
		HighPrice:  1,
	})
	cl = append(cl, CandleData{
		OpenPrice:  1,
		ClosePrice: 2,
		HighPrice:  10,
		LowPrice:   0,
		Timestamp:  n,
	})
	data := cl.ToBytes()
	t.Log(base64.StdEncoding.EncodeToString(data))
}

func TestCandleDataList_Base64Decode(t *testing.T) {
	cl := make(CandleDataList, 0)
	cl = append(cl, CandleData{
		OpenPrice:  0.1,
		ClosePrice: 0.2,
		HighPrice:  1,
	})
	cl = append(cl, CandleData{
		OpenPrice:  1,
		ClosePrice: 2,
		HighPrice:  10,
		LowPrice:   0,
		Timestamp:  n,
	})
	data := cl.ToBytes()
	data, _ = base64.StdEncoding.DecodeString("AQAAABLz8GgAAAAAx7q4jd0X+0CGONbFECX7QMDsnjxsL/tAx7q4jd0X+0A07u8OVdTvQCdEJyyvcydBAAAAAAAAAAA=")

	cl = make(CandleDataList, 0)
	if err := cl.FromBytes(data); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", cl)
}

func TestCandleData_Base64Encode(t *testing.T) {
	candle := CandleData{
		OpenPrice:  0.1,
		ClosePrice: 0.2,
		HighPrice:  1,
		LowPrice:   0,
		Timestamp:  n,
	}

	t.Log(base64.StdEncoding.EncodeToString(candle.ToBytes()))
}

func TestCandleData_ToBytes(t *testing.T) {
	candle := CandleData{
		OpenPrice:  0.1,
		ClosePrice: 0.2,
		HighPrice:  1,
		LowPrice:   0,
		Timestamp:  n,
	}
	if err := candle.FromBytes(candle.ToBytes()); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", candle)
}

func TestNewCloudflareKV_Store(t *testing.T) {
	ckv := NewCloudflareKV("8dac6dbd68790fa6deec035c5b9551b9", "ccf6622667da4486a4d5b1b2823116b6", "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV")
	if err := ckv.Store("test", []byte("hello world")); err != nil {
		t.Fatal(err)
	}
}

func TestCloudflareKV_Load(t *testing.T) {
	ckv := NewCloudflareKV("8dac6dbd68790fa6deec035c5b9551b9", "ccf6622667da4486a4d5b1b2823116b6", "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV")
	if err := ckv.Store("test", []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	value, err := ckv.Load("test")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(value))
}

func TestCloudflareDurable_Publish(t *testing.T) {
	d := NewCloudflareDurable(&CloudflareWorker{
		baseURL:    "https://" + workerHost,
		token:      workerToken,
		httpClient: http.DefaultClient,
	})
	data := &RealtimeTradeData{
		chainId:      "1",
		tokenAddress: "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
		TradeTime:    time.Unix(1760242850, 0),
		Price:        116000,
		Amount:       0.001,
		AmountUSD:    116,
		Native:       false,
	}
	dq, e := data.ToBytes()
	if e != nil {
		t.Fatal(e)
	}
	ok, err := d.Publish(GenerateTokenId(data.chainId, data.tokenAddress), dq)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(ok)
}

func TestCandleChartKVStorage_Store(t *testing.T) {
	candleStorage := NewCandleChartKVStorage(NewCloudflareKV("8dac6dbd68790fa6deec035c5b9551b9", "ccf6622667da4486a4d5b1b2823116b6", "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV"))
	if err := candleStorage.Store(GenerateTokenId("1", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"), time.Minute, &CandleData{
		OpenPrice:  116800,
		ClosePrice: 117400,
		HighPrice:  119100,
		LowPrice:   106700,
		Volume:     11,
		VolumeUSD:  139700,
		Timestamp:  n,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCandleChartKVStorage_Load(t *testing.T) {
	candleStorage := NewCandleChartKVStorage(NewCloudflareKV("8dac6dbd68790fa6deec035c5b9551b9", "ccf6622667da4486a4d5b1b2823116b6", "ROHMxlZqCV-cNnQtHUsJUoBRASjVgZigU8vDL3YV"))
	candle, err := candleStorage.GetLatestCandle(GenerateTokenId("1", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", candle)

	candles, err := candleStorage.GetRecentCandles(GenerateTokenId("1", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", candles)
}

func TestGenerateCandleKVKey(t *testing.T) {
	// Fixed datetime: 2025-Nov-11 18:15 UTC
	fixedTime := time.Date(2025, time.November, 11, 18, 15, 0, 0, time.UTC)

	modNumbers := make(map[time.Duration]int)
	modNumbers[time.Minute] = 1
	modNumbers[time.Minute*5] = 5
	modNumbers[time.Hour] = 12
	modNumbers[time.Hour*24] = 288

	tokenID := "1-0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

	// Test case 1: 1 minute interval (mod 1)
	key1 := GenerateCandleKVKey(tokenID, time.Minute, fixedTime, modNumbers)
	// 2025-315-1: year=2025, yearDay=315, 315%1=0
	expected1 := "60-2025-315-" + tokenID
	if key1 != expected1 {
		t.Errorf("Expected key %s, got %s", expected1, key1)
	}
	t.Logf("Nov 11 18:15, 2025, 1min interval: %s", key1)

	// Test case 2: 5 minute interval (mod 5)
	key2 := GenerateCandleKVKey(tokenID, 5*time.Minute, fixedTime, modNumbers)
	// 2025-315-5: year=2025, yearDay=315, 315%5=0
	expected2 := "300-2025-63-" + tokenID
	if key2 != expected2 {
		t.Errorf("Expected key %s, got %s", expected2, key2)
	}
	t.Logf("Nov 11 18:15, 2025, 5min interval: %s", key2)

	// Test case 3: 1 hour interval (mod 12)
	key3 := GenerateCandleKVKey(tokenID, time.Hour, fixedTime, modNumbers)
	// 2025-315-12: year=2025, yearDay=315, 315%12=3
	expected3 := "3600-2025-26-" + tokenID
	if key3 != expected3 {
		t.Errorf("Expected key %s, got %s", expected3, key3)
	}
	t.Logf("Nov 11 18:15, 2025, 1hour interval: %s", key3)

	// Test case 4: 24 hour interval (mod 288)
	key4 := GenerateCandleKVKey(tokenID, 24*time.Hour, fixedTime, modNumbers)
	// 2025-315-288: year=2025, yearDay=315, 315%288=27
	expected4 := "86400-2025-1-" + tokenID
	if key4 != expected4 {
		t.Errorf("Expected key %s, got %s", expected4, key4)
	}
	t.Logf("Nov 11 18:15, 2025, 24hour interval: %s", key4)

	// Test case 4: one-week interval (mod 0)
	key5 := GenerateCandleKVKey(tokenID, 24*time.Hour*7, fixedTime, modNumbers)
	// 2025-315-288: year=2025, yearDay=315, 315%288=27
	expected5 := "604800-2025-" + tokenID
	if key5 != expected5 {
		t.Errorf("Expected key %s, got %s", expected5, key5)
	}
	t.Logf("Nov 11 18:15, 2025, 1-week interval: %s", key5)
}
