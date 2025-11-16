package storage

import (
	"crypto-mine/orm"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// BloomFilterManager manages bloom filters for addresses and tokens
type BloomFilterManager struct {
	addressFilters  map[string]*bloom.BloomFilter // chainId -> filter
	ormClient       *orm.Client
	locker          sync.RWMutex
	refreshInterval time.Duration
	stopChan        chan struct{}
}

// NewBloomFilterManager creates a new bloom filter manager
func NewBloomFilterManager(ormClient *orm.Client, refreshInterval time.Duration) *BloomFilterManager {
	return &BloomFilterManager{
		addressFilters:  make(map[string]*bloom.BloomFilter),
		ormClient:       ormClient,
		refreshInterval: refreshInterval,
		stopChan:        make(chan struct{}),
	}
}

// Start starts the refresh goroutine for address bloom filters
func (bfm *BloomFilterManager) Start() {
	go bfm.refreshAddressFiltersLoop()
}

// Stop stops the refresh goroutine
func (bfm *BloomFilterManager) Stop() {
	close(bfm.stopChan)
}

// refreshAddressFiltersLoop refreshes address bloom filters every minute
func (bfm *BloomFilterManager) refreshAddressFiltersLoop() {
	ticker := time.NewTicker(bfm.refreshInterval)
	defer ticker.Stop()

	// Initial load
	bfm.refreshAllAddressFilters()

	for {
		select {
		case <-bfm.stopChan:
			slog.Info("Bloom filter refresh loop stopped")
			return
		case <-ticker.C:
			bfm.refreshAllAddressFilters()
		}
	}
}

// refreshAllAddressFilters refreshes address bloom filters for all chains
func (bfm *BloomFilterManager) refreshAllAddressFilters() {
	// For now, we'll refresh for known chains. In a real implementation,
	// you might want to track which chains are active.
	// Common chain IDs: "1" (Ethereum), "137" (Polygon), etc.
	chains := []string{"1"} // Add more chains as needed

	for _, chainId := range chains {
		if err := bfm.refreshAddressFilter(chainId); err != nil {
			slog.Error("Failed to refresh address bloom filter", "chain_id", chainId, "error", err)
		}
	}
}

// refreshAddressFilter fetches and updates the address bloom filter for a chain
func (bfm *BloomFilterManager) refreshAddressFilter(chainId string) error {
	// Fetch bloom filter from API
	resp, err := bfm.ormClient.GetAddressBloomFilter(chainId)
	if err != nil {
		return fmt.Errorf("failed to fetch bloom filter: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			slog.Error("failed to close response body", "err", err)
		}
	}(resp.Body)

	if resp.StatusCode != 200 {
		return fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var apiResponse struct {
		Success    bool    `json:"success"`
		ChainID    string  `json:"chain_id"`
		FilterType string  `json:"filter_type"`
		FilterData []uint8 `json:"filter_data"`
		Error      string  `json:"error,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !apiResponse.Success {
		return fmt.Errorf("API returned error: %s", apiResponse.Error)
	}

	// Deserialize bloom filter from stored binary data
	filter := bloom.NewWithEstimates(8192, 0.01) // Match the size used in AddressBloomManager
	if len(apiResponse.FilterData) > 0 {
		// Convert []uint8 to []byte
		filterData := make([]byte, len(apiResponse.FilterData))
		for i, v := range apiResponse.FilterData {
			filterData[i] = byte(v)
		}
		// Deserialize using UnmarshalBinary
		if err := filter.UnmarshalBinary(filterData); err != nil {
			slog.Warn("Failed to deserialize bloom filter, creating new one", "error", err, "chain_id", chainId)
			// Create a new filter if deserialization fails
			filter = bloom.NewWithEstimates(8192, 0.01)
		}
	} else {
		// No data, create empty filter
		filter = bloom.NewWithEstimates(8192, 0.01)
	}

	bfm.locker.Lock()
	bfm.addressFilters[chainId] = filter
	bfm.locker.Unlock()

	slog.Info("Refreshed address bloom filter", "chain_id", chainId)
	return nil
}

// CheckAddress checks if an address is in the bloom filter for a chain
func (bfm *BloomFilterManager) CheckAddress(chainId, address string) bool {
	bfm.locker.RLock()
	filter, exists := bfm.addressFilters[chainId]
	bfm.locker.RUnlock()

	if !exists {
		return false
	}

	// Normalize address (lowercase, remove 0x prefix)
	normalized := strings.ToLower(strings.TrimPrefix(address, "0x"))
	return filter.Test([]byte(normalized))
}

// SetAddressFilter sets the address filter for a chain (used by AddressBloomManager)
func (bfm *BloomFilterManager) SetAddressFilter(chainId string, filter *bloom.BloomFilter) {
	bfm.locker.Lock()
	defer bfm.locker.Unlock()
	bfm.addressFilters[chainId] = filter
}

// GetAddressFilter gets the address filter for a chain (used by AddressBloomManager)
func (bfm *BloomFilterManager) GetAddressFilter(chainId string) *bloom.BloomFilter {
	bfm.locker.RLock()
	defer bfm.locker.RUnlock()
	return bfm.addressFilters[chainId]
}
