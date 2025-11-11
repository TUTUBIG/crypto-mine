package storage

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
	"unsafe"
)

const (
	minimumInterval = time.Minute
	// VolumeUSDPrecision is the precision for volume USD calculations (1 decimal place)
	VolumeUSDPrecision = 10.0
)

var (
	// IntervalModNumbers maps time intervals to their mod numbers for KV key allocation
	// Default: 1 minute = 1, 5 minutes = 5, 1 hour = 60
	IntervalModNumbers = map[time.Duration]int{
		time.Minute:        1,
		5 * time.Minute:    5,
		time.Hour:          60,
		24 * time.Hour:     24 * 60,
		7 * 24 * time.Hour: 7 * 24 * 60,
	}
)

type CandleData struct {
	OpenPrice        float64
	ClosePrice       float64
	HighPrice        float64
	LowPrice         float64
	Volume           float64
	VolumeUSD        float64
	Timestamp        time.Time
	TransactionCount int64
}

// truncVolumeUSD truncates volume USD to 1 decimal place
func truncVolumeUSD(volumeUSD float64) float64 {
	return math.Round(volumeUSD*VolumeUSDPrecision) / VolumeUSDPrecision
}

func (cd *CandleData) refresh(amount, amountUSD, price float64) bool {

	cd.ClosePrice = price
	if price > cd.HighPrice {
		cd.HighPrice = price
	}
	if price < cd.LowPrice {
		cd.LowPrice = price
	}

	cd.VolumeUSD = truncVolumeUSD(cd.VolumeUSD + amountUSD)
	cd.Volume += amount
	cd.TransactionCount++
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

	// Calculate total size needed: each candle needs 6 float64s + 2 int64s = 8 * 8 = 64 bytes
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

		// TransactionCount (int64)
		for i := 0; i < 8; i++ {
			data[offset+i] = byte(candle.TransactionCount >> (i * 8))
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

	// Calculate expected size: 4 bytes for count + count * 64 bytes per candle
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

		// Read TransactionCount (int64)
		transactionCount := int64(data[offset]) | int64(data[offset+1])<<8 | int64(data[offset+2])<<16 | int64(data[offset+3])<<24 |
			int64(data[offset+4])<<32 | int64(data[offset+5])<<40 | int64(data[offset+6])<<48 | int64(data[offset+7])<<56
		candle.TransactionCount = transactionCount
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
	cache              map[string]CandleDataList
	engine             KVDriver
	pendingRegularKeys map[string]CandleDataList // Batched candles for regular tokens (key -> candles)
	isSpecialToken     func(tokenID string) bool // Function to check if token is special
	mu                 sync.RWMutex              // Mutex for thread-safe operations
	shutdownChan       chan struct{}             // Channel to signal shutdown
	flushDone          chan struct{}             // Channel to signal flush completion
	intervalModNumbers map[time.Duration]int     // Custom mod numbers per interval (optional)
}

// SetIntervalModNumbers allows configuring custom mod numbers for different intervals
func (cs *CandleChartKVStorage) SetIntervalModNumbers(modNumbers map[time.Duration]int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.intervalModNumbers = modNumbers
}

// getModNumber returns the mod number for a given interval
func (cs *CandleChartKVStorage) getModNumber(interval time.Duration) int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	// Check custom mod numbers first
	if cs.intervalModNumbers != nil {
		if mod, ok := cs.intervalModNumbers[interval]; ok {
			return mod
		}
	}

	// Fall back to default mod numbers
	if mod, ok := IntervalModNumbers[interval]; ok {
		return mod
	}

	// Default: use interval in minutes as mod number
	return int(interval.Minutes())
}

// getYearDay calculates the day of year (1-365/366)
func getYearDay(timestamp time.Time) int {
	return timestamp.UTC().YearDay()
}

// GenerateCandleKVKey generates a KV key for a candle based on the new allocation logic:
// 1. Get year and year day (day of year, 1-365/366)
// 2. Get mod number based on interval
// 3. Key format: {year}-{year_day % mod_number}-{tokenId}
func GenerateCandleKVKey(tokenID string, interval time.Duration, timestamp time.Time, modNumbers map[time.Duration]int) string {
	// Get year and year day
	year := timestamp.UTC().Year()
	yearDay := getYearDay(timestamp)

	// Get mod number for this interval
	var modNumber int
	if modNumbers != nil {
		if mod, ok := modNumbers[interval]; ok {
			modNumber = mod
		}
	}

	if modNumber == 0 {
		return fmt.Sprintf("%d-%d-%s", int64(interval.Seconds()), year, tokenID)
	}

	// Calculate: year_day % mod_number
	modResult := yearDay / modNumber

	// Generate key: {year}-{year_day % mod_number}-{tokenId}
	key := fmt.Sprintf("%d-%d-%d-%s", int64(interval.Seconds()), year, modResult, tokenID)

	return key
}

func (cs *CandleChartKVStorage) Store(tokenID string, interval time.Duration, candle *CandleData) error {
	if candle == nil {
		return fmt.Errorf("candle data cannot be nil")
	}

	// Generate KV key using new allocation logic
	key := GenerateCandleKVKey(tokenID, interval, candle.Timestamp, cs.intervalModNumbers)

	// Check if token is special
	isSpecial := cs.isSpecialToken != nil && cs.isSpecialToken(tokenID)

	if isSpecial {
		// Special tokens: store immediately
		return cs.storeImmediately(key, candle)
	}

	// Regular tokens: batch in memory
	cs.mu.Lock()
	if cs.pendingRegularKeys == nil {
		cs.pendingRegularKeys = make(map[string]CandleDataList)
	}

	// Get or create pending candles list for this key
	pending, exists := cs.pendingRegularKeys[key]
	if !exists {
		pending = make(CandleDataList, 0)
	}
	pending = append(pending, *candle)
	cs.pendingRegularKeys[key] = pending
	cs.mu.Unlock()

	return nil
}

// storeImmediately stores a candle to KV immediately (for special tokens)
func (cs *CandleChartKVStorage) storeImmediately(key string, candle *CandleData) error {
	cs.mu.Lock()
	cache, ok := cs.cache[key]
	if !ok {
		cache = make(CandleDataList, 0)
		cs.cache[key] = cache
	}
	cs.mu.Unlock()

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

	return cs.engine.Store(key, cache.ToBytes())
}

// FlushPendingCandles flushes all pending regular token candles to KV
func (cs *CandleChartKVStorage) FlushPendingCandles() error {
	cs.mu.Lock()
	if len(cs.pendingRegularKeys) == 0 {
		cs.mu.Unlock()
		return nil
	}

	// Copy pending keys to avoid holding lock during KV operations
	pendingCopy := make(map[string]CandleDataList)
	for k, v := range cs.pendingRegularKeys {
		pendingCopy[k] = v
	}
	cs.pendingRegularKeys = make(map[string]CandleDataList)
	cs.mu.Unlock()

	// Flush each pending key to KV
	for key, candles := range pendingCopy {
		// Load existing candles from KV
		data, err := cs.engine.Load(key)
		existingCandles := make(CandleDataList, 0)
		if err == nil {
			if err := existingCandles.FromBytes(data); err == nil {
				// Merge with existing candles (append new candles)
				existingCandles = append(existingCandles, candles...)
			} else {
				// Failed to decode existing candles, use pending candles only
				existingCandles = candles
			}
		} else if errors.Is(err, NotfoundError) {
			// No existing candles, use pending candles
			existingCandles = candles
		} else {
			// Error loading, skip this key
			slog.Error("Failed to load existing candles for flush", "key", key, "error", err)
			continue
		}

		// Store merged candles to KV
		slog.Debug("Flushed pending candles for key", "key", key)
		if err := cs.engine.Store(key, existingCandles.ToBytes()); err != nil {
			slog.Error("Failed to flush candles to KV", "key", key, "error", err)
			continue
		}

		// Update cache
		cs.mu.Lock()
		cs.cache[key] = existingCandles
		cs.mu.Unlock()
	}

	slog.Info("Flushed pending candles to KV", "count", len(pendingCopy))
	return nil
}

func (cs *CandleChartKVStorage) GetCandles(_ string, _ time.Duration, _, _ int64, _ int) ([]*CandleData, error) {
	return nil, fmt.Errorf("GetCandles not implemented")
}

func (cs *CandleChartKVStorage) GetLatestCandle(tokenID string, interval time.Duration) (*CandleData, error) {
	currentKey := fmt.Sprintf("%d-current-%s", int(interval.Seconds()), tokenID)
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
	key := fmt.Sprintf("%d-%s-%s", int(interval.Seconds()), time.Now().UTC().Format(time.DateOnly), tokenID)

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
	cs := &CandleChartKVStorage{
		cache:              make(map[string]CandleDataList),
		engine:             engine,
		pendingRegularKeys: make(map[string]CandleDataList),
		shutdownChan:       make(chan struct{}),
		flushDone:          make(chan struct{}),
	}

	// Start hourly flush goroutine for regular tokens
	go cs.startHourlyFlush()

	return cs
}

// SetIsSpecialTokenFunc sets the function to check if a token is special
func (cs *CandleChartKVStorage) SetIsSpecialTokenFunc(fn func(tokenID string) bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.isSpecialToken = fn
}

// startHourlyFlush starts a goroutine that flushes pending candles every hour
func (cs *CandleChartKVStorage) startHourlyFlush() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := cs.FlushPendingCandles(); err != nil {
				slog.Error("Failed to flush pending candles", "error", err)
			}
		case <-cs.shutdownChan:
			// Final flush on shutdown
			if err := cs.FlushPendingCandles(); err != nil {
				slog.Error("Failed to flush pending candles on shutdown", "error", err)
			}
			close(cs.flushDone)
			return
		}
	}
}

// Shutdown gracefully shuts down the storage, flushing all pending candles
func (cs *CandleChartKVStorage) Shutdown() error {
	slog.Info("Shutting down CandleChartKVStorage, flushing all pending candles...")
	close(cs.shutdownChan)

	// Wait for flush to complete (with timeout)
	select {
	case <-cs.flushDone:
		slog.Info("All pending candles flushed successfully")
	case <-time.After(5 * time.Minute):
		slog.Warn("Flush timeout, some candles may not have been flushed")
	}

	return nil
}

// CandleStoredCallback is called when a candle is stored (completed)
type CandleStoredCallback func(chainId, tokenAddress string, candle *CandleData, interval time.Duration)

type CandleChart struct {
	data            chan *RealtimeTradeData
	intervalCandles []*IntervalCandleChart
	publisher       *CloudflareDurable
	onCandleStored  CandleStoredCallback // Callback for when a candle is stored
}

func NewCandleChart(worker *CloudflareWorker) *CandleChart {
	return &CandleChart{
		data:            make(chan *RealtimeTradeData, 1000),
		intervalCandles: make([]*IntervalCandleChart, 0),
		publisher:       NewCloudflareDurable(worker),
	}
}

func (cc *CandleChart) RegisterIntervalCandle(ic *IntervalCandleChart) *CandleChart {
	cc.intervalCandles = append(cc.intervalCandles, ic)
	return cc
}

// SetCandleStoredCallback sets a callback that will be called when a candle is stored
func (cc *CandleChart) SetCandleStoredCallback(callback CandleStoredCallback) {
	cc.onCandleStored = callback
}

// GetCandleStoredCallback gets the current callback
func (cc *CandleChart) GetCandleStoredCallback() CandleStoredCallback {
	return cc.onCandleStored
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
			// Process each interval candle, ordered with time frame
			for _, candle := range cc.intervalCandles {
				currentCandle, found := candle.currentCandles[GenerateTokenId(tradeData.chainId, tradeData.tokenAddress)]
				if !found {
					candle.currentCandles[GenerateTokenId(tradeData.chainId, tradeData.tokenAddress)] = &CandleData{
						OpenPrice:        tradeData.Price,
						ClosePrice:       tradeData.Price,
						HighPrice:        tradeData.Price,
						LowPrice:         tradeData.Price,
						VolumeUSD:        truncVolumeUSD(tradeData.AmountUSD),
						Volume:           tradeData.Amount,
						Timestamp:        tradeData.TradeTime,
						TransactionCount: 1,
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

					// Trigger alert callback if registered and with real time price
					if cc.onCandleStored != nil && time.Now().Sub(tradeData.TradeTime) < time.Minute {
						cc.onCandleStored(tradeData.chainId, tradeData.tokenAddress, currentCandle, candle.interval)
					}

					tokenIdStr := GenerateTokenId(tradeData.chainId, tradeData.tokenAddress)
					if err := candle.storage.Store(tokenIdStr, candle.interval, currentCandle); err != nil {
						slog.Error("store candle error", "error", err.Error(), "candle", currentCandle)
						break
					}

					candle.currentCandles[GenerateTokenId(tradeData.chainId, tradeData.tokenAddress)] = &CandleData{
						OpenPrice:        tradeData.Price,
						ClosePrice:       tradeData.Price,
						HighPrice:        tradeData.Price,
						LowPrice:         tradeData.Price,
						VolumeUSD:        truncVolumeUSD(tradeData.AmountUSD),
						Volume:           tradeData.Amount,
						Timestamp:        tradeData.TradeTime,
						TransactionCount: 1,
					}
				}
			}

			// Publish realtime data
			data, err := tradeData.ToBytes()
			if err != nil {
				slog.Error("publish realtime trade data to cloudflare api failed", "error", err)
			}
			if _, err := cc.publisher.Publish(GenerateTokenId(tradeData.chainId, tradeData.tokenAddress), data); err != nil {
				slog.Error("publish realtime trade data to cloudflare api failed", "error", err)
			}
		}
	}()
}
