package storage

import (
	"github.com/pkg/errors"
	"log/slog"
	"sort"
	"time"
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
}

func NewIntervalCandleChart(interval time.Duration, storage CandleDataStorage) *IntervalCandleChart {
	if interval < minimumInterval {
		panic("interval too small")
	}

	startTimestampMinute := time.Now().Truncate(time.Minute)
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
	Store(candle *CandleData) error
}

type TemporaryStorageCandleChart struct{}

func (ts *TemporaryStorageCandleChart) Store(candle *CandleData) error {
	return errors.New("not implemented")
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
				if err := candle.storage.Store(candle.currentCandle); err != nil {
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
