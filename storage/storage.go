package storage

import (
	"fmt"
	"log/slog"
	"sort"
	"time"
	"unsafe"
)

const minimumInterval = time.Minute

type CandleData struct {
	OpenPrice  float64
	ClosePrice float64
	HighPrice  float64
	LowPrice   float64
	VolumeIn   float64
	VolumeOut  float64
	Timestamp  int64 // second
}

func (cd *CandleData) refresh(amountIn, amountOut float64) bool {
	price := amountIn / amountOut

	cd.ClosePrice = price
	if price > cd.HighPrice {
		cd.HighPrice = price
	}
	if price < cd.LowPrice {
		cd.LowPrice = price
	}

	cd.VolumeIn += amountIn
	cd.VolumeOut += amountOut
	return true
}

func (cd *CandleData) ToBytes() []byte {
	// Create a single-item slice and use the list encoder
	list := CandleDataList{*cd}
	return list.ToBytes()
}

func (cd *CandleData) FromBytes(data []byte) error {
	// Decode using the list decoder and extract the first item
	var list CandleDataList
	if err := list.FromBytes(data); err != nil {
		return err
	}

	if len(list) == 0 {
		return fmt.Errorf("no candle data found in encoded string")
	}

	*cd = list[0]
	return nil
}

type CandleDataList []CandleData

func (cdl *CandleDataList) ToBytes() []byte {
	if len(*cdl) == 0 {
		return nil
	}

	// Calculate total size needed: each candle needs 7 float64s + 1 int64 = 8 * 8 = 64 bytes
	// Plus 4 bytes for the count at the beginning
	totalSize := 4 + len(*cdl)*64
	data := make([]byte, totalSize)

	// Write the count of candles (4 bytes)
	data[0] = byte(len(*cdl))
	data[1] = byte(len(*cdl) >> 8)
	data[2] = byte(len(*cdl) >> 16)
	data[3] = byte(len(*cdl) >> 24)

	offset := 4
	for _, candle := range *cdl {
		// Convert each float64/int64 to bytes (8 bytes each)
		// Timestamp (int64)
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(candle.Timestamp >> (i * 8))
		}
		offset += 8

		// OpenPrice (float64)
		openBits := *(*uint64)(unsafe.Pointer(&candle.OpenPrice))
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(openBits >> (i * 8))
		}
		offset += 8

		// ClosePrice (float64)
		closeBits := *(*uint64)(unsafe.Pointer(&candle.ClosePrice))
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(closeBits >> (i * 8))
		}
		offset += 8

		// HighPrice (float64)
		highBits := *(*uint64)(unsafe.Pointer(&candle.HighPrice))
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(highBits >> (i * 8))
		}
		offset += 8

		// LowPrice (float64)
		lowBits := *(*uint64)(unsafe.Pointer(&candle.LowPrice))
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(lowBits >> (i * 8))
		}
		offset += 8

		// VolumeIn (float64)
		volInBits := *(*uint64)(unsafe.Pointer(&candle.VolumeIn))
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(volInBits >> (i * 8))
		}
		offset += 8

		// VolumeOut (float64)
		volOutBits := *(*uint64)(unsafe.Pointer(&candle.VolumeOut))
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(volOutBits >> (i * 8))
		}
		offset += 8
	}

	return data
}

func (cdl *CandleDataList) FromBytes(data []byte) error {
	if data == nil {
		*cdl = CandleDataList{}
		return nil
	}

	if len(data) < 4 {
		return fmt.Errorf("invalid data: too short")
	}

	// Read count of candles
	count := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24

	expectedSize := 4 + count*64
	if len(data) != expectedSize {
		return fmt.Errorf("invalid data size: expected %d, got %d", expectedSize, len(data))
	}

	// Create slice with correct capacity
	candles := make(CandleDataList, count)

	offset := 4
	for i := 0; i < count; i++ {
		candle := &candles[i]

		// Read Timestamp (int64)
		candle.Timestamp = int64(data[offset]) | int64(data[offset+1])<<8 | int64(data[offset+2])<<16 | int64(data[offset+3])<<24 |
			int64(data[offset+4])<<32 | int64(data[offset+5])<<40 | int64(data[offset+6])<<48 | int64(data[offset+7])<<56
		offset += 8

		// Read OpenPrice (float64)
		openBits := uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
			uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 | uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
		candle.OpenPrice = *(*float64)(unsafe.Pointer(&openBits))
		offset += 8

		// Read ClosePrice (float64)
		closeBits := uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
			uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 | uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
		candle.ClosePrice = *(*float64)(unsafe.Pointer(&closeBits))
		offset += 8

		// Read HighPrice (float64)
		highBits := uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
			uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 | uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
		candle.HighPrice = *(*float64)(unsafe.Pointer(&highBits))
		offset += 8

		// Read LowPrice (float64)
		lowBits := uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
			uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 | uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
		candle.LowPrice = *(*float64)(unsafe.Pointer(&lowBits))
		offset += 8

		// Read VolumeIn (float64)
		volInBits := uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
			uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 | uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
		candle.VolumeIn = *(*float64)(unsafe.Pointer(&volInBits))
		offset += 8

		// Read VolumeOut (float64)
		volOutBits := uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
			uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 | uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
		candle.VolumeOut = *(*float64)(unsafe.Pointer(&volOutBits))
		offset += 8
	}

	*cdl = candles
	return nil
}

type RealtimeTradeData struct {
	TradeTime *time.Time
	AmountIn  float64
	AmountOut float64
}

type IntervalCandleChart struct {
	interval           time.Duration
	lastStartTimestamp *time.Time
	currentCandle      *CandleData
	storage            CandleDataStorage
	pairID             string // chainId-protocol-poolAddress
}

func NewIntervalCandleChart(interval time.Duration, storage CandleDataStorage) *IntervalCandleChart {
	if interval < minimumInterval {
		panic("interval too small")
	}

	startTimestampMinute := time.Now().Truncate(minimumInterval)
	return &IntervalCandleChart{
		interval:           interval,
		lastStartTimestamp: &startTimestampMinute,
		storage:            storage,
		currentCandle: &CandleData{
			OpenPrice:  0,
			ClosePrice: 0,
			HighPrice:  0,
			LowPrice:   0,
			VolumeIn:   0,
			VolumeOut:  0,
			Timestamp:  time.Now().Unix(),
		},
	}
}

type CandleDataStorage interface {
	Store(pairID string, interval time.Duration, candle *CandleData) error
	GetCandles(pairID string, interval time.Duration, startTime, endTime int64, limit int) ([]*CandleData, error)
	GetLatestCandle(pairID string, interval time.Duration) (*CandleData, error)
}

type CandleChartKVStorage struct {
	cache  CandleDataList
	engine KVDriver
}

func (cs *CandleChartKVStorage) Store(pairID string, interval time.Duration, candle *CandleData) error {
	if candle == nil {
		return fmt.Errorf("candle data cannot be nil")
	}

	//TODO If there exist some lost candles, use 0 to take the position.Or front side should handle this gap.
	cs.cache = append(cs.cache, *candle)

	// Compose the key: pairID-interval-date
	// Store whole candle, client will fetch this data only when the first time loading chart.
	key := fmt.Sprintf("%s-%d-%s", pairID, int(interval.Seconds()), time.Unix(candle.Timestamp, 0).UTC().Format(time.DateOnly))
	if err := cs.engine.Store(key, cs.cache.ToBytes()); err != nil {
		return err
	}

	// Store the latest candle, this is designed from pooling the latest candle from client side.
	currentKey := fmt.Sprintf("%s-%d-current", pairID, int(interval.Seconds()))
	if err := cs.engine.Store(currentKey, candle.ToBytes()); err != nil {
		return err
	}

	return nil
}

func (cs *CandleChartKVStorage) GetCandles(pairID string, interval time.Duration, startTime, endTime int64, limit int) ([]*CandleData, error) {
	return nil, nil
}

func (cs *CandleChartKVStorage) GetLatestCandle(pairID string, interval time.Duration) (*CandleData, error) {

	return nil, nil
}

// Additional utility methods
func (cs *CandleChartKVStorage) GetCandlesByTimeRange(pairID string, interval time.Duration, startTime, endTime time.Time) ([]*CandleData, error) {
	return cs.GetCandles(pairID, interval, startTime.Unix(), endTime.Unix(), 0)
}

func (cs *CandleChartKVStorage) GetRecentCandles(pairID string, interval time.Duration, count int) ([]*CandleData, error) {
	now := time.Now().Unix()
	past := now - int64(interval.Seconds()*float64(count*2))
	return cs.GetCandles(pairID, interval, past, now, count)
}

func NewCandleChartKVStorage(engine KVDriver) *CandleChartKVStorage {
	return &CandleChartKVStorage{
		cache:  make(CandleDataList, 0),
		engine: engine,
	}
}

type CandleChart struct {
	data            <-chan *RealtimeTradeData
	intervalCandles []*IntervalCandleChart
}

func NewCandleChart() *CandleChart {
	return &CandleChart{
		data:            make(chan *RealtimeTradeData, 1000),
		intervalCandles: make([]*IntervalCandleChart, 0),
	}
}

func (cc *CandleChart) RegisterIntervalCandle(ic *IntervalCandleChart) *CandleChart {
	cc.intervalCandles = append(cc.intervalCandles, ic)
	return cc
}

func (cc *CandleChart) StartAggregateCandleData() {
	// Sort intervalCandles with interval, smaller to bigger
	sort.Slice(cc.intervalCandles, func(i, j int) bool {
		return cc.intervalCandles[i].interval < cc.intervalCandles[j].interval
	})

	for tradeData := range cc.data {
		// Process each interval candle
		for _, candle := range cc.intervalCandles {
			// Calculate the start time for the current interval
			if candle.lastStartTimestamp.Add(candle.interval).After(*tradeData.TradeTime) {
				if candle.currentCandle == nil {
					price := tradeData.AmountIn / tradeData.AmountOut
					candle.currentCandle = &CandleData{
						OpenPrice:  price,
						ClosePrice: price,
						HighPrice:  price,
						LowPrice:   price,
						VolumeIn:   tradeData.AmountIn,
						VolumeOut:  tradeData.AmountOut,
						Timestamp:  tradeData.TradeTime.Unix(),
					}
				} else {
					candle.currentCandle.refresh(tradeData.AmountIn, tradeData.AmountOut)
				}
				// Skip for larger intervals if it is before the smaller one
				break
			} else {
				if err := candle.storage.Store("", candle.interval, candle.currentCandle); err != nil {
					slog.Error("store candle error", "error", err.Error(), "candle", candle.currentCandle)
					break
				}
				price := tradeData.AmountIn / tradeData.AmountOut
				newCandleData := CandleData{
					OpenPrice:  price,
					ClosePrice: price,
					HighPrice:  price,
					LowPrice:   price,
					VolumeIn:   tradeData.AmountIn,
					VolumeOut:  tradeData.AmountOut,
					Timestamp:  tradeData.TradeTime.Unix(),
				}
				candle.currentCandle = &newCandleData
			}
		}
	}
}
