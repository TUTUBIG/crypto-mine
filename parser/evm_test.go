package parser

import (
	"math/big"
	"testing"
)

func TestCalculatePriceWithDecimals(t *testing.T) {
	eth := big.NewInt(1e18)
	ethS := 18
	usd := big.NewInt(1e9)
	usdS := 6
	price, err := CalculatePriceWithDecimals(usd, eth, int64(usdS), int64(ethS))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(price)
}
