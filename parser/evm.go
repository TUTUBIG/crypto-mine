package parser

import (
	"context"
	"crypto-mine/storage"
	"fmt"
	"log"
	"log/slog"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type EvmEngine struct {
	filter ethereum.FilterQuery
	chains []*EVMChain
}

func NewEVMEngine() *EvmEngine {
	filter := ethereum.FilterQuery{
		BlockHash: nil,
		FromBlock: nil,
		ToBlock:   nil,
		Addresses: nil,
		Topics: [][]common.Hash{
			{
				common.HexToHash(swapV3Topic1),
				common.HexToHash(swapV3Topic1),
			},
		},
	}
	return &EvmEngine{
		filter: filter,
		chains: make([]*EVMChain, 0),
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
	for _, chain := range ee.chains {
		chain.Stop()
	}
}

type EVMChain struct {
	chainId     string
	shutdown    chan struct{}
	endpoint    string
	wsEndpoint  string
	ethClient   *ethclient.Client
	wsClient    *ethclient.Client
	subscriber  ethereum.Subscription
	logs        chan types.Log
	trades      chan *TradeInfo
	poolInfo    *storage.PoolInfo
	protocols   []DEXProtocol
	candleChart *storage.CandleChart
}

func NewEVMChain(pi *storage.PoolInfo, cc *storage.CandleChart, rpc, ws string) (*EVMChain, error) {
	rpcClient, err := ethclient.Dial(rpc)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC endpoint: %w", err)
	}

	wsClient, err := ethclient.Dial(ws)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to WebSocket endpoint: %w", err)
	}

	chainId, err := rpcClient.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	return &EVMChain{
		chainId:     chainId.String(),
		poolInfo:    pi,
		candleChart: cc,
		endpoint:    rpc,
		wsEndpoint:  ws,
		logs:        make(chan types.Log, 1000),
		trades:      make(chan *TradeInfo, 1000),
		shutdown:    make(chan struct{}),
		ethClient:   rpcClient,
		wsClient:    wsClient,
	}, nil
}

func (e *EVMChain) RegisterProtocol(protocol DEXProtocol) *EVMChain {
	e.protocols = append(e.protocols, protocol)
	return e
}

func (e *EVMChain) StartPumpLogs(filter ethereum.FilterQuery) error {
	sub, err := e.wsClient.SubscribeFilterLogs(context.Background(), filter, e.logs)
	if err != nil {
		return fmt.Errorf("failed to subscribe to filter logs: %w", err)
	}
	slog.Info("start monitoring pump logs", "chain id", e.chainId, "topics", filter.Topics)
	e.subscriber = sub
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

func TransferTokenAmount(amountIn, amountOut *big.Int, tokenInDecimals, tokenOutDecimals int64) (float64, float64, error) {
	// Validate inputs
	if amountIn == nil || amountOut == nil {
		return 0, 0, fmt.Errorf("amounts cannot be nil")
	}
	if amountIn.Cmp(big.NewInt(0)) <= 0 {
		return 0, 0, fmt.Errorf("amountIn must be positive")
	}
	if amountOut.Cmp(big.NewInt(0)) <= 0 {
		return 0, 0, fmt.Errorf("amountOut must be positive")
	}
	if tokenInDecimals < 0 || tokenOutDecimals < 0 {
		return 0, 0, fmt.Errorf("decimals cannot be negative")
	}

	amountInFloat := new(big.Float).SetInt(amountIn)
	amountOutFloat := new(big.Float).SetInt(amountOut)

	// Adjust for decimals
	costTokenDecimals10 := new(big.Float).SetFloat64(math.Pow10(int(tokenInDecimals)))
	getTokenDecimals10 := new(big.Float).SetFloat64(math.Pow10(int(tokenOutDecimals)))

	amountInAdjusted, _ := new(big.Float).Quo(amountInFloat, costTokenDecimals10).Float64()
	amountOutAdjusted, _ := new(big.Float).Quo(amountOutFloat, getTokenDecimals10).Float64()

	return amountInAdjusted, amountOutAdjusted, nil

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
			ChainId:           e.chainId,
			Protocol:          protocolName,
			PoolAddress:       trade.PoolAddress.Hex(),
			PoolName:          pi.PoolName,
			CostTokenAddress:  pi.CostTokenAddress,
			CostTokenSymbol:   pi.CostTokenSymbol,
			CostTokenDecimals: pi.CostTokenDecimals,
			GetTokenAddress:   pi.GetTokenAddress,
			GetTokenSymbol:    pi.GetTokenSymbol,
			GetTokenDecimals:  pi.GetTokenDecimals,
		}
		e.poolInfo.AddPool(poolInfo)
	}

	// Calculate price with decimals
	costAmount, getAmount, err := TransferTokenAmount(trade.AmountIn, trade.AmountOut, poolInfo.CostTokenDecimals, poolInfo.GetTokenDecimals)
	if err != nil {
		return err
	}

	if costAmount == 0 || getAmount == 0 {
		return fmt.Errorf("invalid trade amount in %s out %s", trade.AmountIn, trade.AmountOut)
	}

	return e.candleChart.AddCandle(storage.GeneratePairInfoId(e.chainId, protocolName, strings.TrimPrefix(trade.PoolAddress.Hex(), "0x")), trade.TradeTime, costAmount, getAmount)
}

func (e *EVMChain) Stop() {
	e.subscriber.Unsubscribe()
	close(e.shutdown)
}

type TradeInfo struct {
	Protocol    DEXProtocol
	PoolAddress *common.Address
	AmountIn    *big.Int
	AmountOut   *big.Int
	TradeTime   *time.Time
}

type DEXProtocol interface {
	GetProtocolName() string
	HasTopic(common.Hash) bool
	ExtractTradeInfo(log *types.Log) (*TradeInfo, error)
	GetPoolInfo(client *ethclient.Client, poolAddress *common.Address) (*storage.PairInfo, error)
}

const (
	swapV3Topic1 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
	swapV3Topic2 = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"
)

const (
	swapV3PoolABI = `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"CollectProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid1","type":"uint256"}],"name":"Flash","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextOld","type":"uint16"},{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextNew","type":"uint16"}],"name":"IncreaseObservationCardinalityNext","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Initialize","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"feeProtocol0Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol0New","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1New","type":"uint8"}],"name":"SetFeeProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collect","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collectProtocol","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal0X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal1X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"flash","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"}],"name":"increaseObservationCardinalityNext","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxLiquidityPerTick","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"mint","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"observations","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint128","name":"liquidity","type":"uint128"},{"internalType":"uint256","name":"feeGrowthInside0LastX128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthInside1LastX128","type":"uint256"},{"internalType":"uint128","name":"tokensOwed0","type":"uint128"},{"internalType":"uint128","name":"tokensOwed1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFees","outputs":[{"internalType":"uint128","name":"token0","type":"uint128"},{"internalType":"uint128","name":"token1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8","name":"feeProtocol0","type":"uint8"},{"internalType":"uint8","name":"feeProtocol1","type":"uint8"}],"name":"setFeeProtocol","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"}],"name":"snapshotCumulativesInside","outputs":[{"internalType":"int56","name":"tickCumulativeInside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityInsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsInside","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bool","name":"zeroForOne","type":"bool"},{"internalType":"int256","name":"amountSpecified","type":"int256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[{"internalType":"int256","name":"amount0","type":"int256"},{"internalType":"int256","name":"amount1","type":"int256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"int16","name":"","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickDistance","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`

	erc20TokenABI = `[{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}]`
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
			common.HexToHash(swapV3Topic2),
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
	// Call contract to get token0 and token1 from swapPoolABI
	// Pack the data for the "token0" function call
	data, err := u3.swapPoolABI.Pack("token0")
	if err != nil {
		return nil, err
	}
	// Call the contract to get token0 address
	token0AddressBytes, err := client.CallContract(context.Background(), ethereum.CallMsg{
		To:   poolAddress,
		Data: data,
	}, nil)
	if err != nil {
		return nil, err
	}

	var token0Address common.Address
	err = u3.swapPoolABI.UnpackIntoInterface(&token0Address, "token0", token0AddressBytes)
	if err != nil {
		return nil, err
	}

	data, err = u3.swapPoolABI.Pack("token1")
	if err != nil {
		return nil, err
	}
	token1AddressBytes, err := client.CallContract(context.Background(), ethereum.CallMsg{
		To:   poolAddress,
		Data: data,
	}, nil)
	if err != nil {
		return nil, err
	}

	var token1Address common.Address
	err = u3.swapPoolABI.UnpackIntoInterface(&token1Address, "token1", token1AddressBytes)
	if err != nil {
		return nil, err
	}

	// token0 symbol
	data, err = erc20ABI.Pack("symbol")
	if err != nil {
		return nil, err
	}
	token0SymbolBytes, err := client.CallContract(context.Background(), ethereum.CallMsg{
		To:   &token0Address,
		Data: data,
	}, nil)
	if err != nil {
		return nil, err
	}
	var token0Symbol string
	err = erc20ABI.UnpackIntoInterface(&token0Symbol, "symbol", token0SymbolBytes)
	if err != nil {
		// fallback: try to decode as string directly
		token0Symbol = string(token0SymbolBytes)
	}

	// token0 decimals
	data, err = erc20ABI.Pack("decimals")
	if err != nil {
		return nil, err
	}
	token0DecimalsBytes, err := client.CallContract(context.Background(), ethereum.CallMsg{
		To:   &token0Address,
		Data: data,
	}, nil)
	if err != nil {
		return nil, err
	}
	var token0Decimals uint8
	err = erc20ABI.UnpackIntoInterface(&token0Decimals, "decimals", token0DecimalsBytes)
	if err != nil {
		return nil, err
	}

	// token1 symbol
	data, err = erc20ABI.Pack("symbol")
	if err != nil {
		return nil, err
	}
	token1SymbolBytes, err := client.CallContract(context.Background(), ethereum.CallMsg{
		To:   &token1Address,
		Data: data,
	}, nil)
	if err != nil {
		return nil, err
	}
	var token1Symbol string
	err = erc20ABI.UnpackIntoInterface(&token1Symbol, "symbol", token1SymbolBytes)
	if err != nil {
		token1Symbol = string(token1SymbolBytes)
	}

	// token1 decimals
	data, err = erc20ABI.Pack("decimals")
	if err != nil {
		return nil, err
	}
	token1DecimalsBytes, err := client.CallContract(context.Background(), ethereum.CallMsg{
		To:   &token1Address,
		Data: data,
	}, nil)
	if err != nil {
		return nil, err
	}
	var token1Decimals uint8
	err = erc20ABI.UnpackIntoInterface(&token1Decimals, "decimals", token1DecimalsBytes)
	if err != nil {
		return nil, err
	}

	return &storage.PairInfo{
		Protocol:          storage.UniV3,
		PoolAddress:       poolAddress.Hex(),
		PoolName:          fmt.Sprintf("%s-%s", token0Symbol, token1Symbol),
		CostTokenAddress:  token0Address.Hex(),
		CostTokenSymbol:   token0Symbol,
		CostTokenDecimals: int64(token0Decimals),
		GetTokenAddress:   token1Address.Hex(),
		GetTokenSymbol:    token1Symbol,
		GetTokenDecimals:  int64(token1Decimals),
	}, nil
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

	var amountCost, amountGet *big.Int

	// Determine which amount is input (negative) and which is output (positive)
	if amount0.Cmp(big.NewInt(0)) < 0 && amount1.Cmp(big.NewInt(0)) > 0 {
		// amount0 is negative (input), amount1 is positive (output)
		amountCost = new(big.Int).Abs(amount0)
		amountGet = amount1
	} else if amount1.Cmp(big.NewInt(0)) < 0 && amount0.Cmp(big.NewInt(0)) > 0 {
		// amount1 is negative (input), amount0 is positive (output)
		amountCost = new(big.Int).Abs(amount1)
		amountGet = amount0
	} else {
		return nil, fmt.Errorf("invalid swap amounts: amount0=%s, amount1=%s", amount0.String(), amount1.String())
	}

	if amountCost == nil || amountGet == nil {
		return nil, fmt.Errorf("failed to extract valid swap amounts from log %s", log.TxHash.Hex())
	}

	//TODO: Replace with block time, can't get this data from logs subscription
	t := time.Now()
	return &TradeInfo{
		Protocol:    u3,
		TradeTime:   &t,
		PoolAddress: &log.Address,
		AmountIn:    amountCost,
		AmountOut:   amountGet,
	}, nil
}
