package storage

import (
	"errors"
	"fmt"
	"log"
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
	Volume     float64
	VolumeUSD  float64
	Timestamp  time.Time
}

func (cd *CandleData) refresh(amount, amountUSD, price float64) bool {

	cd.ClosePrice = price
	if price > cd.HighPrice {
		cd.HighPrice = price
	}
	if price < cd.LowPrice {
		cd.LowPrice = price
	}

	cd.VolumeUSD += amountUSD
	cd.Volume += amount
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
			data[offset+i] = byte(candle.Timestamp.Unix() >> (i * 8))
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

		// VolumeUSD (float64)
		volInBits := *(*uint64)(unsafe.Pointer(&candle.VolumeUSD))
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(volInBits >> (i * 8))
		}
		offset += 8

		// Volume (float64)
		volOutBits := *(*uint64)(unsafe.Pointer(&candle.Volume))
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
		timestamp := int64(data[offset]) | int64(data[offset+1])<<8 | int64(data[offset+2])<<16 | int64(data[offset+3])<<24 |
			int64(data[offset+4])<<32 | int64(data[offset+5])<<40 | int64(data[offset+6])<<48 | int64(data[offset+7])<<56
		offset += 8

		candle.Timestamp = time.Unix(timestamp, 0)

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
		candle.VolumeUSD = *(*float64)(unsafe.Pointer(&volInBits))
		offset += 8

		// Read VolumeOut (float64)
		volOutBits := uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
			uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 | uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
		candle.Volume = *(*float64)(unsafe.Pointer(&volOutBits))
		offset += 8
	}

	*cdl = candles
	return nil
}

type RealtimeTradeData struct {
	chainId      string
	tokenAddress string
	TradeTime    time.Time
	Price        float64
	Amount       float64
	AmountUSD    float64
	Native       bool
}

func (rd *RealtimeTradeData) ToBytes() ([]byte, error) {
	if rd == nil {
		return nil, fmt.Errorf("RealtimeTradeData is nil")
	}

	buf := make([]byte, 0, 8+8+8) // time (int64) + float64 + float64

	// Serialize TradeTime as UnixNano (int64), or 0 if nil
	ts := rd.TradeTime.Unix()

	timeBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		timeBytes[i] = byte(ts >> (8 * i))
	}
	buf = append(buf, timeBytes...)

	// Serialize Amount0 (float64)
	amountInBits := *(*uint64)(unsafe.Pointer(&rd.AmountUSD))
	for i := 0; i < 8; i++ {
		buf = append(buf, byte(amountInBits>>(8*i)))
	}

	// Serialize Amount1 (float64)
	amountOutBits := *(*uint64)(unsafe.Pointer(&rd.Amount))
	for i := 0; i < 8; i++ {
		buf = append(buf, byte(amountOutBits>>(8*i)))
	}

	return buf, nil
}

type IntervalCandleChart struct {
	interval       time.Duration
	currentCandles map[string]*CandleData
	storage        CandleDataStorage
}

func NewIntervalCandleChart(interval time.Duration, storage CandleDataStorage) *IntervalCandleChart {
	if interval < minimumInterval {
		log.Fatal("interval too small")
	}

	return &IntervalCandleChart{
		interval:       interval,
		storage:        storage,
		currentCandles: make(map[string]*CandleData),
	}
}

type CandleDataStorage interface {
	Store(tokenID string, interval time.Duration, candle *CandleData) error
	GetCandles(tokenID string, interval time.Duration, startTime, endTime int64, limit int) ([]*CandleData, error)
	GetLatestCandle(tokenID string, interval time.Duration) (*CandleData, error)
}

type CandleChartKVStorage struct {
	cache  map[string]CandleDataList
	engine KVDriver
}

func (cs *CandleChartKVStorage) Store(tokenID string, interval time.Duration, candle *CandleData) error {
	if candle == nil {
		return fmt.Errorf("candle data cannot be nil")
	}

	// Compose the key: tokenID-interval-date
	// Store whole candle, client will fetch this data only when the first time loading chart.
	key := fmt.Sprintf("%s-%d-%s", tokenID, int(interval.Seconds()), candle.Timestamp.UTC().Format(time.DateOnly))

	cache, ok := cs.cache[key]
	if !ok {
		cache = make(CandleDataList, 0)
		cs.cache[key] = cache
	}

	if len(cache) == 0 {
		data, err := cs.engine.Load(key)
		if err != nil {
			if !errors.Is(err, NotfoundError) {
				return err
			}
			// empty
		} else {
			candles := make(CandleDataList, 0)
			if err := candles.FromBytes(data); err != nil {
				return err
			}

			cache = candles
		}
	}

	cache = append(cache, *candle)

	if err := cs.engine.Store(key, cache.ToBytes()); err != nil {
		return err
	}

	// Store the latest candle, this is designed from polling the latest candle from client side.
	currentKey := fmt.Sprintf("%s-%d-current", tokenID, int(interval.Seconds()))
	if err := cs.engine.Store(currentKey, candle.ToBytes()); err != nil {
		return err
	}

	return nil
}

func (cs *CandleChartKVStorage) GetCandles(tokenID string, interval time.Duration, startTime, endTime int64, limit int) ([]*CandleData, error) {
	return nil, nil
}

func (cs *CandleChartKVStorage) GetLatestCandle(tokenID string, interval time.Duration) (*CandleData, error) {
	currentKey := fmt.Sprintf("%s-%d-current", tokenID, int(interval.Seconds()))
	data, err := cs.engine.Load(currentKey)
	if err != nil {
		return nil, err
	}
	cd := new(CandleData)
	if err := cd.FromBytes(data); err != nil {
		return nil, err
	}
	return cd, nil
}

func (cs *CandleChartKVStorage) GetCandlesByTimeRange(tokenID string, interval time.Duration, startTime, endTime time.Time) ([]*CandleData, error) {
	return cs.GetCandles(tokenID, interval, startTime.Unix(), endTime.Unix(), 0)
}

func (cs *CandleChartKVStorage) GetRecentCandles(tokenID string, interval time.Duration) (CandleDataList, error) {
	key := fmt.Sprintf("%s-%d-%s", tokenID, int(interval.Seconds()), time.Now().UTC().Format(time.DateOnly))

	data, err := cs.engine.Load(key)
	if err != nil {
		if !errors.Is(err, NotfoundError) {
			return nil, err
		}
	}
	candles := make(CandleDataList, 0)
	if err := candles.FromBytes(data); err != nil {
		return nil, err
	}

	fmt.Printf("%+v %s", candles, key)

	return candles, nil

}

func NewCandleChartKVStorage(engine KVDriver) *CandleChartKVStorage {
	return &CandleChartKVStorage{
		cache:  make(map[string]CandleDataList),
		engine: engine,
	}
}

type CandleChart struct {
	data            chan *RealtimeTradeData
	intervalCandles []*IntervalCandleChart
	publisher       *CloudflareDurable
}

func NewCandleChart() *CandleChart {
	return &CandleChart{
		data:            make(chan *RealtimeTradeData, 1000),
		intervalCandles: make([]*IntervalCandleChart, 0),
		publisher:       NewCloudflareDurable(),
	}
}

func (cc *CandleChart) RegisterIntervalCandle(ic *IntervalCandleChart) *CandleChart {
	cc.intervalCandles = append(cc.intervalCandles, ic)
	return cc
}

func (cc *CandleChart) AddCandle(chainId, tokenAddress string, tradeTime time.Time, amountUSD, amountToken, price float64) error {
	cc.data <- &RealtimeTradeData{
		chainId:      chainId,
		tokenAddress: tokenAddress,
		TradeTime:    tradeTime,
		AmountUSD:    amountUSD,
		Amount:       amountToken,
		Price:        price,
	}
	return nil
}

func (cc *CandleChart) StartAggregateCandleData() {
	// Sort intervalCandles with interval, smaller to bigger
	sort.Slice(cc.intervalCandles, func(i, j int) bool {
		return cc.intervalCandles[i].interval < cc.intervalCandles[j].interval
	})

	go func() {
		for tradeData := range cc.data {
			// Publish realtime data
			data, err := tradeData.ToBytes()
			if err != nil {
				slog.Error("publish realtime trade data to cloudflare api failed", "error", err)
			}
			if _, err := cc.publisher.Publish(GenerateTokenId(tradeData.chainId, tradeData.tokenAddress), data); err != nil {
				slog.Error("publish realtime trade data to cloudflare api failed", "error", err)
			}
			// Process each interval candle, ordered with time frame
			for _, candle := range cc.intervalCandles {
				currentCandle, found := candle.currentCandles[GenerateTokenId(tradeData.chainId, tradeData.tokenAddress)]
				if !found {
					candle.currentCandles[GenerateTokenId(tradeData.chainId, tradeData.tokenAddress)] = &CandleData{
						OpenPrice:  tradeData.Price,
						ClosePrice: tradeData.Price,
						HighPrice:  tradeData.Price,
						LowPrice:   tradeData.Price,
						VolumeUSD:  tradeData.AmountUSD,
						Volume:     tradeData.Amount,
						Timestamp:  tradeData.TradeTime,
					}
					continue
				}

				// Calculate the start time for the current interval, refresh the current candle if this price is still in the current time frame
				if currentCandle.Timestamp.Add(candle.interval).After(tradeData.TradeTime) {
					slog.Debug("refresh candle", "token", tradeData.tokenAddress, "price", tradeData.Price, "time", tradeData.TradeTime.Format(time.RFC3339))
					currentCandle.refresh(tradeData.AmountUSD, tradeData.Amount, tradeData.Price)
					// Skip for larger intervals if it is before the smaller one
					break
				} else {
					// freeze the past candle and create a new one
					slog.Debug("store candle", "token", tradeData.tokenAddress, "candle", currentCandle)
					if err := candle.storage.Store(GenerateTokenId(tradeData.chainId, tradeData.tokenAddress), candle.interval, currentCandle); err != nil {
						slog.Error("store candle error", "error", err.Error(), "candle", currentCandle)
						break
					}
					candle.currentCandles[GenerateTokenId(tradeData.chainId, tradeData.tokenAddress)] = &CandleData{
						OpenPrice:  tradeData.Price,
						ClosePrice: tradeData.Price,
						HighPrice:  tradeData.Price,
						LowPrice:   tradeData.Price,
						VolumeUSD:  tradeData.AmountUSD,
						Volume:     tradeData.Amount,
						Timestamp:  tradeData.TradeTime,
					}
				}
			}
		}
	}()
}
