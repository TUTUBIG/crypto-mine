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
