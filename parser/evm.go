package parser

import (
	"context"
	"crypto-mine/storage"
	"fmt"
	"log"
	"log/slog"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	// MinVolumeUSD is the minimum trading volume required before storing token to DB
	// Set to $1000 as per design specification
	MinVolumeUSD = 1000.0

	// PricePrecision is the precision for price calculations (4 decimal places)
	PricePrecision = 1e4
)

type EvmEngine struct {
	filter ethereum.FilterQuery
	chains []*EVMChain
	ctx    context.Context
	cancel context.CancelFunc
}

func NewEVMEngine() *EvmEngine {
	ctx, cancel := context.WithCancel(context.Background())
	// For live WebSocket subscriptions, don't specify BlockHash, FromBlock, or ToBlock
	// This creates a subscription for new blocks only
	filter := ethereum.FilterQuery{
		Addresses: []common.Address{}, // Empty slice instead of nil
		Topics: [][]common.Hash{
			{
				common.HexToHash(swapV3Topic1),
				//common.HexToHash(swapV3Topic2),
			},
		},
	}
	return &EvmEngine{
		filter: filter,
		chains: make([]*EVMChain, 0),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (ee *EvmEngine) RegisterChain(chain *EVMChain) *EvmEngine {
	ee.chains = append(ee.chains, chain)
	return ee
}

func (ee *EvmEngine) Start() error {
	for _, chain := range ee.chains {
		if err := chain.StartPumpLogs(ee.filter); err != nil {
			return fmt.Errorf("failed to start pump logs for chain %s: %w", chain.chainId, err)
		}
		chain.StartAssemblyLine()
	}
	return nil
}

func (ee *EvmEngine) Stop() {
	ee.cancel() // Cancel context first
	for _, chain := range ee.chains {
		chain.Stop()
	}
}

type EVMChain struct {
	chainId                  string
	ctx                      context.Context
	cancel                   context.CancelFunc
	shutdown                 chan struct{}
	endpoint                 string
	wsEndpoint               string
	ethClient                *ethclient.Client
	wsClient                 *ethclient.Client
	subscriber               ethereum.Subscription
	logs                     chan types.Log
	trades                   chan *TradeInfo
	poolInfo                 *storage.PoolInfo
	protocols                []DEXProtocol
	candleChart              *storage.CandleChart
	stableCoins              []common.Address
	nativeCoinWrapper        common.Address
	realtimeNativeTokenPrice float64
	tokens                   map[common.Address]*storage.TokenInfo
}

func NewEVMChain(pi *storage.PoolInfo, cc *storage.CandleChart, rpc, ws string) (*EVMChain, error) {
	ctx, cancel := context.WithCancel(context.Background())

	rpcClient, err := ethclient.Dial(rpc)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to RPC endpoint: %w", err)
	}

	wsClient, err := ethclient.Dial(ws)
	if err != nil {
		cancel()
		rpcClient.Close()
		return nil, fmt.Errorf("failed to connect to WebSocket endpoint: %w", err)
	}

	chainId, err := rpcClient.ChainID(ctx)
	if err != nil {
		cancel()
		rpcClient.Close()
		wsClient.Close()
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	return &EVMChain{
		chainId:     chainId.String(),
		ctx:         ctx,
		cancel:      cancel,
		poolInfo:    pi,
		candleChart: cc,
		endpoint:    rpc,
		wsEndpoint:  ws,
		logs:        make(chan types.Log, 10000), // Increased buffer size
		trades:      make(chan *TradeInfo, 10000),
		shutdown:    make(chan struct{}),
		ethClient:   rpcClient,
		wsClient:    wsClient,
	}, nil
}

func (e *EVMChain) PrintBuffer() {
	slog.Info("EVM chain", "chainId", e.chainId, "buffer", len(e.logs), "trades", len(e.trades))
}

func (e *EVMChain) ChainID() string {
	return e.chainId
}

func (e *EVMChain) RegisterStableCoin(nativeWrapper common.Address, nativePrice string, stableCoins []common.Address) *EVMChain {
	e.stableCoins = append(e.stableCoins, stableCoins...)
	e.nativeCoinWrapper = nativeWrapper
	if nativePrice != "" {
		np, err := strconv.ParseFloat(nativePrice, 64)
		if err != nil {
			panic(err)
		}
		e.realtimeNativeTokenPrice = np
	}
	return e
}

func (e *EVMChain) RegisterProtocol(protocol DEXProtocol) *EVMChain {
	e.protocols = append(e.protocols, protocol)
	return e
}

func (e *EVMChain) StartPumpLogs(filter ethereum.FilterQuery) error {
	sub, err := e.wsClient.SubscribeFilterLogs(e.ctx, filter, e.logs)
	if err != nil {
		return fmt.Errorf("failed to subscribe to filter logs: %w", err)
	}
	slog.Info("start monitoring pump logs", "chain id", e.chainId, "topics", filter.Topics)
	e.subscriber = sub

	// Monitor subscription errors
	go func() {
		select {
		case err := <-sub.Err():
			if err != nil {
				slog.Error("subscription error", "chain", e.chainId, "error", err)
			}
		case <-e.ctx.Done():
			return
		}
	}()

	return nil
}

func (e *EVMChain) StartAssemblyLine() {
	// handle protocols
	go func() {
		for {
			select {
			case <-e.shutdown:
				slog.Info("assembly lines shutting down", "network", e.chainId, "worker", "protocol")
				return
			case l := <-e.logs:
				for _, protocol := range e.protocols {
					if !protocol.HasTopic(l.Topics[0]) {
						continue
					} else {
						if err := e.handleProtocol(&l, protocol); err != nil {
							slog.Error("Failed to extract trade info", "err", err, "tx hash", l.TxHash.Hex())
						}
						break
					}
				}
			}
		}
	}()

	// handle market data
	go func() {
		for {
			select {
			case <-e.shutdown:
				slog.Info("assembly lines shutting down", "network", e.chainId, "worker", "trad info")
				return
			case trade := <-e.trades:
				if err := e.handleTradeInfo(trade); err != nil {
					slog.Error("Failed to extract trade info", "err", err, "trade info", trade)
				}
			}
		}
	}()
}

func (e *EVMChain) handleProtocol(log *types.Log, protocol DEXProtocol) error {
	tradeInfo, err := protocol.ExtractTradeInfo(log)
	if err != nil {
		return err
	}
	e.trades <- tradeInfo
	return nil
}

func TransferTokenAmount(amount0, amount1 *big.Int, token0Decimals, token1Decimals uint8) (float64, float64, error) {
	// Validate inputs
	if amount0 == nil || amount1 == nil {
		return 0, 0, fmt.Errorf("amounts cannot be nil")
	}
	if amount0.Cmp(big.NewInt(0)) <= 0 {
		return 0, 0, fmt.Errorf("amount0 must be positive")
	}
	if amount1.Cmp(big.NewInt(0)) <= 0 {
		return 0, 0, fmt.Errorf("amount1 must be positive")
	}
	// uint8 is always >= 0, no need to check for negative
	if token0Decimals > 77 || token1Decimals > 77 {
		return 0, 0, fmt.Errorf("decimals too large (max 77)")
	}

	amount0Float := new(big.Float).SetInt(amount0)
	amount1Float := new(big.Float).SetInt(amount1)

	// Adjust for decimals
	token0Decimals10 := new(big.Float).SetFloat64(math.Pow10(int(token0Decimals)))
	token1Decimals10 := new(big.Float).SetFloat64(math.Pow10(int(token1Decimals)))

	amount0Adjusted, _ := new(big.Float).Quo(amount0Float, token0Decimals10).Float64()
	amount1Adjusted, _ := new(big.Float).Quo(amount1Float, token1Decimals10).Float64()

	return amount0Adjusted, amount1Adjusted, nil

	//scale := math.Pow10(8)
	//return math.Round(amountInAdjusted*scale) / scale, math.Round(amountOutAdjusted*scale) / scale, nil
}

func (e *EVMChain) handleTradeInfo(trade *TradeInfo) error {
	protocolName := trade.Protocol.GetProtocolName()
	poolInfo := e.poolInfo.FindPool(e.chainId, protocolName, trade.PoolAddress.Hex())
	if poolInfo == nil {
		// get pair info through pool address
		pi, err := trade.Protocol.GetPoolInfo(e.ethClient, trade.PoolAddress)
		if err != nil {
			return err
		}
		poolInfo = &storage.PairInfo{
			ChainId:        e.chainId,
			Protocol:       protocolName,
			PoolAddress:    trade.PoolAddress.Hex(),
			PoolName:       pi.PoolName,
			Token0Address:  pi.Token0Address,
			Token0Symbol:   pi.Token0Symbol,
			Token0Decimals: pi.Token0Decimals,
			Token1Address:  pi.Token1Address,
			Token1Symbol:   pi.Token1Symbol,
			Token1Decimals: pi.Token1Decimals,
		}

		// check if it is a standard tokens pair
		if !e.standardTokenHelper([2]common.Address{common.HexToAddress(poolInfo.Token0Address), common.HexToAddress(poolInfo.Token1Address)}) {
			slog.Debug("Skip pool", "pool name", poolInfo.PoolName)
			poolInfo.Skip = true
		}

		e.poolInfo.AddPool(poolInfo)
	}

	if poolInfo.Skip {
		return nil
	}

	// Calculate price with decimals
	amount0, amount1, err := TransferTokenAmount(trade.Amount0, trade.Amount1, poolInfo.Token0Decimals, poolInfo.Token1Decimals)
	if err != nil {
		return err
	}

	if amount0 == 0 || amount1 == 0 {
		return fmt.Errorf("invalid trade amount  %s - %s", trade.Amount0, trade.Amount1)
	}

	// Calculate token price based on stable coins or native wrapper
	priceResult := e.calculateTokenPrice(poolInfo, amount0, amount1)
	if priceResult == nil {
		// Skip this trade - not a valid pair
		return nil
	}

	tokenAddress := priceResult.tokenAddress
	tokenPrice := truncAmount(priceResult.tokenPrice)
	tokenAmount := priceResult.tokenAmount
	usd := priceResult.usd

	// treat native wrapper token as a normal one
	if common.HexToAddress(tokenAddress).Cmp(e.nativeCoinWrapper) == 0 {
		e.realtimeNativeTokenPrice = tokenPrice
	}

	return e.candleChart.AddCandle(e.chainId, tokenAddress, trade.TradeTime, usd, tokenAmount, tokenPrice)
}

func truncAmount(amount float64) float64 {
	return math.Max(float64(int64(amount*PricePrecision))/PricePrecision, 1/PricePrecision)
}

// priceCalculationResult holds the result of token price calculation
type priceCalculationResult struct {
	tokenAddress string
	tokenPrice   float64
	tokenAmount  float64
	usd          float64
}

// calculateTokenPrice determines token price based on stablecoins or native wrapper
// Returns nil if the trading pair is not supported (not paired with stablecoin or native wrapper)
func (e *EVMChain) calculateTokenPrice(poolInfo *storage.PairInfo, amount0, amount1 float64) *priceCalculationResult {
	// First, check if either token is a stablecoin
	for _, stableCoin := range e.stableCoins {
		if stableCoin.Cmp(common.HexToAddress(poolInfo.Token0Address)) == 0 {
			// Token0 is stablecoin, Token1 is the target token
			tokenPrice := handleStableCoin(e.poolInfo, e.chainId, poolInfo.Token1Symbol, poolInfo.Token1Address, amount0, amount1, poolInfo.Token1Decimals)
			if tokenPrice < 0 {
				return nil // Volume too low
			}
			return &priceCalculationResult{
				tokenAddress: poolInfo.Token1Address,
				tokenPrice:   tokenPrice,
				tokenAmount:  amount1,
				usd:          amount0,
			}
		}

		if stableCoin.Cmp(common.HexToAddress(poolInfo.Token1Address)) == 0 {
			// Token1 is stablecoin, Token0 is the target token
			tokenPrice := handleStableCoin(e.poolInfo, e.chainId, poolInfo.Token0Symbol, poolInfo.Token0Address, amount1, amount0, poolInfo.Token0Decimals)
			if tokenPrice < 0 {
				return nil // Volume too low
			}
			return &priceCalculationResult{
				tokenAddress: poolInfo.Token0Address,
				tokenPrice:   tokenPrice,
				tokenAmount:  amount0,
				usd:          amount1,
			}
		}
	}

	// No stablecoin found, check if paired with native wrapper token
	if e.realtimeNativeTokenPrice == 0 {
		slog.Debug("Skip trade - native token price not available",
			"token0", poolInfo.Token0Symbol, "token1", poolInfo.Token1Symbol)
		return nil
	}

	if common.HexToAddress(poolInfo.Token0Address).Cmp(e.nativeCoinWrapper) == 0 {
		// Token0 is native wrapper, Token1 is the target token
		return &priceCalculationResult{
			tokenAddress: poolInfo.Token1Address,
			tokenPrice:   amount0 / amount1 * e.realtimeNativeTokenPrice,
			tokenAmount:  amount1,
			usd:          amount0 * e.realtimeNativeTokenPrice,
		}
	}

	if common.HexToAddress(poolInfo.Token1Address).Cmp(e.nativeCoinWrapper) == 0 {
		// Token1 is native wrapper, Token0 is the target token
		return &priceCalculationResult{
			tokenAddress: poolInfo.Token0Address,
			tokenPrice:   amount1 / amount0 * e.realtimeNativeTokenPrice,
			tokenAmount:  amount0,
			usd:          amount1 * e.realtimeNativeTokenPrice,
		}
	}

	// Neither stablecoin nor native wrapper - skip this trade
	return nil
}

func handleStableCoin(pool *storage.PoolInfo, chainId, coinSymbol, coinAddress string, usdAmount, coinAmount float64, coinDecimals uint8) float64 {
	token := pool.FindToken(chainId, coinAddress)
	if token == nil {
		// New token - add to cache
		pool.AddToken(&storage.TokenInfo{
			ChainId:        chainId,
			TokenAddress:   coinAddress,
			DailyVolumeUSD: usdAmount,
			TokenName:      coinSymbol,
			TokenSymbol:    coinSymbol,
			Decimals:       coinDecimals,
		})
		// Volume too low, need to accumulate more
		return -1
	}

	if token.OnlyCache {
		// Token exists in cache only, accumulate volume
		token.DailyVolumeUSD += usdAmount
		if token.DailyVolumeUSD >= MinVolumeUSD {
			// Volume threshold reached - store to DB
			pool.StoreToken(token)
			token.OnlyCache = false
			slog.Info("Token volume threshold reached",
				"symbol", coinSymbol,
				"address", coinAddress,
				"volume", token.DailyVolumeUSD)
			return usdAmount / coinAmount
		}
		// Still accumulating
		return -1
	}

	// Token already in DB, return price
	return usdAmount / coinAmount
}

/*
	func fetchTokenInfo(c *ethclient.Client, tokenAddress common.Address) (name, symbol string, decimals uint8) {
		tokenABI, err := abi.JSON(strings.NewReader(erc20TokenABI))
		if err != nil {
			log.Printf("Failed to parse token ABI: %v", err)
			return
		}

		// Get token name
		data, err := tokenABI.Pack("name")
		if err != nil {
			log.Printf("Failed to pack name data: %v", err)
			return
		}

		msg := ethereum.CallMsg{
			To:   &tokenAddress,
			Data: data,
		}

		result, err := c.CallContract(context.Background(), msg, nil)
		if err != nil {
			log.Printf("Failed to call name method: %v", err)
			return
		}

		err = tokenABI.UnpackIntoInterface(&name, "name", result)
		if err != nil {
			log.Printf("Failed to unpack name result: %v", err)
			return
		}

		// Get token symbol
		data, err = tokenABI.Pack("symbol")
		if err != nil {
			log.Printf("Failed to pack symbol data: %v", err)
			return
		}

		msg.Data = data
		result, err = c.CallContract(context.Background(), msg, nil)
		if err != nil {
			log.Printf("Failed to call symbol method: %v", err)
			return
		}

		err = tokenABI.UnpackIntoInterface(&symbol, "symbol", result)
		if err != nil {
			log.Printf("Failed to unpack symbol result: %v", err)
			return
		}

		// Get token decimals
		data, err = tokenABI.Pack("decimals")
		if err != nil {
			log.Printf("Failed to pack decimals data: %v", err)
			return
		}

		msg.Data = data
		result, err = c.CallContract(context.Background(), msg, nil)
		if err != nil {
			log.Printf("Failed to call decimals method: %v", err)
			return
		}

		err = tokenABI.UnpackIntoInterface(&decimals, "decimals", result)
		if err != nil {
			log.Printf("Failed to unpack decimals result: %v", err)
			return
		}

		slog.Debug("Token info fetched", "address", tokenAddress, "name", name, "symbol", symbol, "decimals", decimals)
		return
	}
*/
func (e *EVMChain) standardTokenHelper(tokenPair [2]common.Address) bool {
	for _, t := range tokenPair {
		if e.nativeCoinWrapper.Cmp(t) == 0 {
			return true
		}
	}

	for _, t := range tokenPair {
		for _, s := range e.stableCoins {
			if t.Cmp(s) == 0 {
				return true
			}
		}
	}

	return false
}

func (e *EVMChain) Stop() {
	e.cancel() // Cancel context
	if e.subscriber != nil {
		e.subscriber.Unsubscribe()
	}
	close(e.shutdown)

	// Close clients
	if e.ethClient != nil {
		e.ethClient.Close()
	}
	if e.wsClient != nil {
		e.wsClient.Close()
	}
}

type TradeInfo struct {
	Protocol    DEXProtocol
	PoolAddress *common.Address
	Amount0     *big.Int
	Amount1     *big.Int
	TradeTime   time.Time
}

type DEXProtocol interface {
	GetProtocolName() string
	HasTopic(common.Hash) bool
	ExtractTradeInfo(log *types.Log) (*TradeInfo, error)
	GetPoolInfo(client *ethclient.Client, poolAddress *common.Address) (*storage.PairInfo, error)
}

const (
	swapV3Topic1 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
	// swapV3Topic2 = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"
)

const (
	swapV3PoolABI = `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"CollectProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid1","type":"uint256"}],"name":"Flash","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextOld","type":"uint16"},{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextNew","type":"uint16"}],"name":"IncreaseObservationCardinalityNext","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Initialize","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"feeProtocol0Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol0New","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1New","type":"uint8"}],"name":"SetFeeProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collect","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collectProtocol","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal0X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal1X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"flash","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"}],"name":"increaseObservationCardinalityNext","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxLiquidityPerTick","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"mint","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"observations","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint128","name":"liquidity","type":"uint128"},{"internalType":"uint256","name":"feeGrowthInside0LastX128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthInside1LastX128","type":"uint256"},{"internalType":"uint128","name":"tokensOwed0","type":"uint128"},{"internalType":"uint128","name":"tokensOwed1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFees","outputs":[{"internalType":"uint128","name":"token0","type":"uint128"},{"internalType":"uint128","name":"token1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8","name":"feeProtocol0","type":"uint8"},{"internalType":"uint8","name":"feeProtocol1","type":"uint8"}],"name":"setFeeProtocol","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"}],"name":"snapshotCumulativesInside","outputs":[{"internalType":"int56","name":"tickCumulativeInside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityInsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsInside","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bool","name":"zeroForOne","type":"bool"},{"internalType":"int256","name":"amountSpecified","type":"int256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[{"internalType":"int256","name":"amount0","type":"int256"},{"internalType":"int256","name":"amount1","type":"int256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"int16","name":"","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickDistance","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`

	erc20TokenABI = `[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}]`
)

var erc20ABI *abi.ABI

func init() {
	a, err := abi.JSON(strings.NewReader(erc20TokenABI))
	if err != nil {
		log.Fatal(err)
	}
	erc20ABI = &a
}

type UniSwapV3 struct {
	swapTopic   []common.Hash
	swapPoolABI *abi.ABI
}

func NewUniSwapV3() *UniSwapV3 {
	a, err := abi.JSON(strings.NewReader(swapV3PoolABI))
	if err != nil {
		log.Fatal(err)
	}
	return &UniSwapV3{
		swapTopic: []common.Hash{
			common.HexToHash(swapV3Topic1),
			//	common.HexToHash(swapV3Topic2),
		},
		swapPoolABI: &a,
	}
}

func (u3 *UniSwapV3) GetProtocolName() string {
	return storage.UniV3
}

func (u3 *UniSwapV3) HasTopic(topic common.Hash) bool {
	for _, st := range u3.swapTopic {
		if topic.Cmp(st) == 0 {
			return true
		}
	}

	return false
}

func (u3 *UniSwapV3) GetPoolInfo(client *ethclient.Client, poolAddress *common.Address) (*storage.PairInfo, error) {
	ctx := context.Background()

	// Step 1: Get token addresses from pool
	token0Address, token1Address, err := u3.getPoolTokens(ctx, client, poolAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get pool tokens: %w", err)
	}

	// Step 2: Get token info for both tokens concurrently
	type tokenResult struct {
		symbol   string
		decimals uint8
		err      error
	}

	token0Chan := make(chan tokenResult, 1)
	token1Chan := make(chan tokenResult, 1)

	// Fetch token0 info
	go func() {
		symbol, decimals, err := u3.getTokenInfo(ctx, client, token0Address)
		token0Chan <- tokenResult{symbol: symbol, decimals: decimals, err: err}
	}()

	// Fetch token1 info
	go func() {
		symbol, decimals, err := u3.getTokenInfo(ctx, client, token1Address)
		token1Chan <- tokenResult{symbol: symbol, decimals: decimals, err: err}
	}()

	// Wait for both results
	token0Result := <-token0Chan
	token1Result := <-token1Chan

	if token0Result.err != nil {
		return nil, fmt.Errorf("failed to get token0 info: %w", token0Result.err)
	}
	if token1Result.err != nil {
		return nil, fmt.Errorf("failed to get token1 info: %w", token1Result.err)
	}

	return &storage.PairInfo{
		Protocol:       storage.UniV3,
		PoolAddress:    poolAddress.Hex(),
		PoolName:       fmt.Sprintf("%s-%s", token0Result.symbol, token1Result.symbol),
		Token0Address:  token0Address.Hex(),
		Token0Symbol:   token0Result.symbol,
		Token0Decimals: token0Result.decimals,
		Token1Address:  token1Address.Hex(),
		Token1Symbol:   token1Result.symbol,
		Token1Decimals: token1Result.decimals,
	}, nil
}

// getPoolTokens retrieves token0 and token1 addresses from a pool
func (u3 *UniSwapV3) getPoolTokens(ctx context.Context, client *ethclient.Client, poolAddress *common.Address) (common.Address, common.Address, error) {
	// Get token0
	data, err := u3.swapPoolABI.Pack("token0")
	if err != nil {
		return common.Address{}, common.Address{}, err
	}

	token0AddressBytes, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   poolAddress,
		Data: data,
	}, nil)
	if err != nil {
		return common.Address{}, common.Address{}, err
	}

	var token0Address common.Address
	if err := u3.swapPoolABI.UnpackIntoInterface(&token0Address, "token0", token0AddressBytes); err != nil {
		return common.Address{}, common.Address{}, err
	}

	// Get token1
	data, err = u3.swapPoolABI.Pack("token1")
	if err != nil {
		return common.Address{}, common.Address{}, err
	}

	token1AddressBytes, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   poolAddress,
		Data: data,
	}, nil)
	if err != nil {
		return common.Address{}, common.Address{}, err
	}

	var token1Address common.Address
	if err := u3.swapPoolABI.UnpackIntoInterface(&token1Address, "token1", token1AddressBytes); err != nil {
		return common.Address{}, common.Address{}, err
	}

	return token0Address, token1Address, nil
}

// getTokenInfo retrieves symbol and decimals for a token address
func (u3 *UniSwapV3) getTokenInfo(ctx context.Context, client *ethclient.Client, tokenAddress common.Address) (string, uint8, error) {
	// Get symbol
	symbolData, err := erc20ABI.Pack("symbol")
	if err != nil {
		return "", 0, err
	}

	symbolBytes, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &tokenAddress,
		Data: symbolData,
	}, nil)
	if err != nil {
		return "", 0, err
	}

	var symbol string
	if err := erc20ABI.UnpackIntoInterface(&symbol, "symbol", symbolBytes); err != nil {
		// Fallback for non-standard tokens
		symbol = string(symbolBytes)
	}

	// Get decimals
	decimalsData, err := erc20ABI.Pack("decimals")
	if err != nil {
		return "", 0, err
	}

	decimalsBytes, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &tokenAddress,
		Data: decimalsData,
	}, nil)
	if err != nil {
		return "", 0, err
	}

	var decimals uint8
	if err := erc20ABI.UnpackIntoInterface(&decimals, "decimals", decimalsBytes); err != nil {
		return "", 0, err
	}

	return symbol, decimals, nil
}

func (u3 *UniSwapV3) ExtractTradeInfo(log *types.Log) (*TradeInfo, error) {
	if len(log.Topics) != 3 {
		return nil, fmt.Errorf("invalid swap topic length %d", len(log.Topics))
	}

	var hit bool
	for _, topic := range u3.swapTopic {
		if log.Topics[0].Cmp(topic) == 0 {
			hit = true
		}
	}
	if !hit {
		return nil, fmt.Errorf("unknown swap topic %s", log.Topics[0])
	}

	swapInfo, err := u3.swapPoolABI.Unpack("Swap", log.Data)
	if err != nil {
		return nil, err
	}

	// In UniSwap V3, amount0 and amount1 can be positive or negative
	// One will be positive (tokens received) and one negative (tokens given)
	var amount0, amount1 *big.Int
	var ok bool

	if amount0, ok = swapInfo[0].(*big.Int); !ok {
		return nil, fmt.Errorf("invalid amount0 type in swap data")
	}
	if amount1, ok = swapInfo[1].(*big.Int); !ok {
		return nil, fmt.Errorf("invalid amount1 type in swap data")
	}

	if amount0 == nil || amount1 == nil {
		return nil, fmt.Errorf("failed to extract valid swap amounts from log %s", log.TxHash.Hex())
	}

	//TODO: Replace with block time, can't get this data from logs subscription
	t := time.Now()
	return &TradeInfo{
		Protocol:    u3,
		TradeTime:   t,
		PoolAddress: &log.Address,
		Amount0:     amount0.Abs(amount0),
		Amount1:     amount1.Abs(amount1),
	}, nil
}
