package storage

import (
	"encoding/base64"
	"os"
	"testing"
	"time"
)

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
		Timestamp:  0,
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
		Timestamp:  0,
	})
	data := cl.ToBytes()
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
		Timestamp:  0,
	}

	t.Log(base64.StdEncoding.EncodeToString(candle.ToBytes()))
}

func TestCandleData_ToBytes(t *testing.T) {
	candle := CandleData{
		OpenPrice:  0.1,
		ClosePrice: 0.2,
		HighPrice:  1,
		LowPrice:   0,
		Timestamp:  0,
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
	_ = os.Setenv("worker_host", "http://localhost:8787")
	d := NewCloudflareDurable()
	n := time.Now()
	data := &RealtimeTradeData{
		poolID:    "12345",
		AmountIn:  1000,
		AmountOut: 500,
		TradeTime: &n,
	}
	dq, e := data.ToBytes()
	if e != nil {
		t.Fatal(e)
	}
	ok, err := d.Publish(data.poolID, dq)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(ok)
}
