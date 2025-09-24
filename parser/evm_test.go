package parser

import (
	"math/big"
	"testing"
)

func TestCalculateDecimals(t *testing.T) {
	eth := big.NewInt(1e18)
	ethS := 18
	usd := big.NewInt(1e9)
	usdS := 6
	u, e, err := TransferTokenAmount(usd, eth, uint8(usdS), uint8(ethS))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(u, e)
}

func TestTruncAmount(t *testing.T) {
	var tokenPrice = 0.2469616644025134
	t.Log("tokenPrice", truncAmount(tokenPrice))
}
