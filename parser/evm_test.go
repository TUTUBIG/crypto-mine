package parser

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"math/big"
	"testing"
)

func TestCalculatePriceWithDecimals(t *testing.T) {
	tests := []struct {
		name              string
		amountIn          *big.Int
		amountOut         *big.Int
		costTokenDecimals int64
		getTokenDecimals  int64
		expectedPrice     float64
		expectError       bool
		errorMessage      string
	}{
		{
			name:              "Basic price calculation - same decimals",
			amountIn:          big.NewInt(1000000000000000000), // 1 token with 18 decimals
			amountOut:         big.NewInt(2000000000000000000), // 2 tokens with 18 decimals
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     0.5, // 1/2 = 0.5
			expectError:       false,
		},
		{
			name:              "Different decimals - 18 to 6",
			amountIn:          big.NewInt(1000000000000000000), // 1 token with 18 decimals
			amountOut:         big.NewInt(1000000),             // 1 token with 6 decimals
			costTokenDecimals: 18,
			getTokenDecimals:  6,
			expectedPrice:     1.0, // 1/1 = 1.0
			expectError:       false,
		},
		{
			name:              "Different decimals - 6 to 18",
			amountIn:          big.NewInt(1000000),             // 1 token with 6 decimals
			amountOut:         big.NewInt(1000000000000000000), // 1 token with 18 decimals
			costTokenDecimals: 6,
			getTokenDecimals:  18,
			expectedPrice:     1.0, // 1/1 = 1.0
			expectError:       false,
		},
		{
			name:              "Large amounts",
			amountIn:          big.NewInt(1000000000000000000), // 1 token with 18 decimals
			amountOut:         big.NewInt(100000000000000000),  // 0.1 token with 18 decimals
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     10.0, // 1/0.1 = 10.0
			expectError:       false,
		},
		{
			name:              "Small price - high precision",
			amountIn:          big.NewInt(1000000000000000),    // 0.001 token with 18 decimals
			amountOut:         big.NewInt(1000000000000000000), // 1 token with 18 decimals
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     0.001, // 0.001/1 = 0.001
			expectError:       false,
		},
		{
			name:              "Zero decimals",
			amountIn:          big.NewInt(10), // 10 tokens
			amountOut:         big.NewInt(5),  // 5 tokens
			costTokenDecimals: 0,
			getTokenDecimals:  0,
			expectedPrice:     2.0, // 10/5 = 2.0
			expectError:       false,
		},
		// Error cases
		{
			name:              "Nil amountIn",
			amountIn:          nil,
			amountOut:         big.NewInt(1000000000000000000),
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     0,
			expectError:       true,
			errorMessage:      "amounts cannot be nil",
		},
		{
			name:              "Nil amountOut",
			amountIn:          big.NewInt(1000000000000000000),
			amountOut:         nil,
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     0,
			expectError:       true,
			errorMessage:      "amounts cannot be nil",
		},
		{
			name:              "Zero amountIn",
			amountIn:          big.NewInt(0),
			amountOut:         big.NewInt(1000000000000000000),
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     0,
			expectError:       true,
			errorMessage:      "amountIn must be positive",
		},
		{
			name:              "Negative amountIn",
			amountIn:          big.NewInt(-1000000000000000000),
			amountOut:         big.NewInt(1000000000000000000),
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     0,
			expectError:       true,
			errorMessage:      "amountIn must be positive",
		},
		{
			name:              "Zero amountOut",
			amountIn:          big.NewInt(1000000000000000000),
			amountOut:         big.NewInt(0),
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     0,
			expectError:       true,
			errorMessage:      "amountOut must be positive",
		},
		{
			name:              "Negative amountOut",
			amountIn:          big.NewInt(1000000000000000000),
			amountOut:         big.NewInt(-1000000000000000000),
			costTokenDecimals: 18,
			getTokenDecimals:  18,
			expectedPrice:     0,
			expectError:       true,
			errorMessage:      "amountOut must be positive",
		},
		{
			name:              "Negative cost token decimals",
			amountIn:          big.NewInt(1000000000000000000),
			amountOut:         big.NewInt(1000000000000000000),
			costTokenDecimals: -1,
			getTokenDecimals:  18,
			expectedPrice:     0,
			expectError:       true,
			errorMessage:      "decimals cannot be negative",
		},
		{
			name:              "Negative get token decimals",
			amountIn:          big.NewInt(1000000000000000000),
			amountOut:         big.NewInt(1000000000000000000),
			costTokenDecimals: 18,
			getTokenDecimals:  -1,
			expectedPrice:     0,
			expectError:       true,
			errorMessage:      "decimals cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			price, err := CalculatePriceWithDecimals(tt.amountIn, tt.amountOut, tt.costTokenDecimals, tt.getTokenDecimals)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if err.Error() != tt.errorMessage {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMessage, err.Error())
				}
				if price != tt.expectedPrice {
					t.Errorf("Expected price %f when error occurs, got %f", tt.expectedPrice, price)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if price != tt.expectedPrice {
					t.Errorf("Expected price %f, got %f", tt.expectedPrice, price)
				}
			}
		})
	}
}

func TestCalculatePriceWithDecimals_RealWorldScenarios(t *testing.T) {
	tests := []struct {
		name              string
		amountIn          string // Using string to handle large numbers
		amountOut         string
		costTokenDecimals int64
		getTokenDecimals  int64
		expectedPrice     float64
		tolerance         float64 // For floating point comparison
	}{
		{
			name:              "ETH/USDC swap - 1 ETH for 2000 USDC",
			amountIn:          "1000000000000000000", // 1 ETH (18 decimals)
			amountOut:         "2000000000",          // 2000 USDC (6 decimals)
			costTokenDecimals: 18,
			getTokenDecimals:  6,
			expectedPrice:     0.0005, // 1 ETH / 2000 USDC = 0.0005 ETH per USDC
			tolerance:         0.000001,
		},
		{
			name:              "BTC/ETH swap - 0.1 BTC for 2.5 ETH",
			amountIn:          "10000000",            // 0.1 BTC (8 decimals)
			amountOut:         "2500000000000000000", // 2.5 ETH (18 decimals)
			costTokenDecimals: 8,
			getTokenDecimals:  18,
			expectedPrice:     0.04, // 0.1 BTC / 2.5 ETH = 0.04 BTC per ETH
			tolerance:         0.000001,
		},
		{
			name:              "USDT/USDC swap - 1000 USDT for 999.5 USDC",
			amountIn:          "1000000000", // 1000 USDT (6 decimals)
			amountOut:         "999500000",  // 999.5 USDC (6 decimals)
			costTokenDecimals: 6,
			getTokenDecimals:  6,
			expectedPrice:     1.0005002, // 1000 / 999.5 ≈ 1.0005002
			tolerance:         0.0000001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			amountIn, ok := new(big.Int).SetString(tt.amountIn, 10)
			if !ok {
				t.Fatalf("Failed to parse amountIn: %s", tt.amountIn)
			}

			amountOut, ok := new(big.Int).SetString(tt.amountOut, 10)
			if !ok {
				t.Fatalf("Failed to parse amountOut: %s", tt.amountOut)
			}

			price, err := CalculatePriceWithDecimals(amountIn, amountOut, tt.costTokenDecimals, tt.getTokenDecimals)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			diff := price - tt.expectedPrice
			if diff < 0 {
				diff = -diff
			}

			if diff > tt.tolerance {
				t.Errorf("Expected price %f (±%f), got %f", tt.expectedPrice, tt.tolerance, price)
			}
		})
	}
}

// Benchmark test to ensure the function performs well
func BenchmarkCalculatePriceWithDecimals(b *testing.B) {
	amountIn := big.NewInt(1000000000000000000)  // 1 token with 18 decimals
	amountOut := big.NewInt(2000000000000000000) // 2 tokens with 18 decimals

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CalculatePriceWithDecimals(amountIn, amountOut, 18, 18)
	}
}

func TestUniSwapV3_GetPoolInfo(t *testing.T) {
	p := NewUniSwapV3()
	c, e := ethclient.Dial("https://capable-tiniest-knowledge.quiknode.pro/326754df17ae865cf46d044db09213ce7e2ec23b")
	if e != nil {
		t.Fatal(e)
	}
	poolAddress := common.HexToAddress("0x7d60220578D99dcE0F0F59cB1368787B8dBB312B")
	pi, e := p.GetPoolInfo(c, &poolAddress)
	if e != nil {
		t.Fatal(e)
	}
	t.Log(pi)
}
