package manager

import (
	"crypto-mine/orm"
	"crypto-mine/storage"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// AddressBloomManager manages address bloom filters
type AddressBloomManager struct {
	ORMClient       *orm.Client
	bloomFilterMgr  *storage.BloomFilterManager
	refreshInterval time.Duration
	refreshLimit    int // Maximum number of addresses to process per refresh
	stopChan        chan struct{}
	logger          *slog.Logger
}

// NewAddressBloomManager creates a new address bloom manager
func NewAddressBloomManager(ormClient *orm.Client, bloomFilterMgr *storage.BloomFilterManager, refreshInterval time.Duration) *AddressBloomManager {
	return &AddressBloomManager{
		ORMClient:       ormClient,
		bloomFilterMgr:  bloomFilterMgr,
		refreshInterval: refreshInterval,
		refreshLimit:    100, // Default: process 100 addresses per refresh
		stopChan:        make(chan struct{}),
		logger:          slog.Default(),
	}
}

// SetRefreshLimit sets the maximum number of addresses to process per refresh
func (m *AddressBloomManager) SetRefreshLimit(limit int) {
	if limit > 0 {
		m.refreshLimit = limit
	}
}

// SetLogger sets a custom logger
func (m *AddressBloomManager) SetLogger(logger *slog.Logger) {
	m.logger = logger
}

// Start starts the refresh goroutine
func (m *AddressBloomManager) Start() error {
	m.logger.Info("Starting address bloom manager")

	// Initial refresh
	if err := m.RefreshUnrefreshedAddresses(); err != nil {
		m.logger.Error("Failed to refresh address bloom filters on startup", "error", err)
		return err
	}

	// Start periodic refresh
	go m.refreshLoop()

	return nil
}

// Stop stops the refresh goroutine
func (m *AddressBloomManager) Stop() {
	m.logger.Info("Stopping address bloom manager")
	close(m.stopChan)
}

// RefreshUnrefreshedAddresses fetches unrefreshed addresses, updates bloom filters, and marks them as refreshed
func (m *AddressBloomManager) RefreshUnrefreshedAddresses() error {
	m.logger.Debug("Refreshing address bloom filters")

	// Fetch unrefreshed addresses grouped by chain_id
	sql := `
		SELECT id, chain_id, address
		FROM registered_addresses
		WHERE bloom_refreshed = 0 OR bloom_refreshed = false
		ORDER BY chain_id, id ASC
		LIMIT ?
	`

	results, err := m.ORMClient.ExecuteSQL(sql, m.refreshLimit)
	if err != nil {
		m.logger.Error("Failed to fetch unrefreshed addresses", "error", err)
		return err
	}

	if len(results) == 0 {
		m.logger.Debug("No unrefreshed addresses to process")
		return nil
	}

	// Group addresses by chain_id
	chainAddresses := make(map[string][]string)
	addressIDs := make(map[string][]int) // chain_id -> []address_ids

	for _, row := range results {
		id, ok := row["id"]
		if !ok {
			continue
		}
		addressID := 0
		switch v := id.(type) {
		case int:
			addressID = v
		case int64:
			addressID = int(v)
		case float64:
			addressID = int(v)
		}

		chainID, ok := row["chain_id"].(string)
		if !ok {
			continue
		}

		address, ok := row["address"].(string)
		if !ok {
			continue
		}

		// Normalize address (lowercase, remove 0x prefix)
		normalizedAddress := strings.ToLower(strings.TrimPrefix(address, "0x"))

		if chainAddresses[chainID] == nil {
			chainAddresses[chainID] = make([]string, 0)
			addressIDs[chainID] = make([]int, 0)
		}
		chainAddresses[chainID] = append(chainAddresses[chainID], normalizedAddress)
		addressIDs[chainID] = append(addressIDs[chainID], addressID)
	}

	m.logger.Debug("Grouped unrefreshed addresses by chain",
		"chain_count", len(chainAddresses),
		"total_addresses", len(results))

	// Process each chain
	for chainID, addresses := range chainAddresses {
		if err := m.refreshBloomFilterForChain(chainID, addresses, addressIDs[chainID]); err != nil {
			m.logger.Error("Failed to refresh bloom filter for chain",
				"chain_id", chainID,
				"error", err)
			// Continue with other chains even if one fails
			continue
		}
	}

	return nil
}

// refreshBloomFilterForChain refreshes the bloom filter for a specific chain
func (m *AddressBloomManager) refreshBloomFilterForChain(chainID string, newAddresses []string, addressIDs []int) error {
	m.logger.Debug("Refreshing bloom filter for chain",
		"chain_id", chainID,
		"new_address_count", len(newAddresses))

	// Get existing filter from in-memory cache or rebuild from refreshed addresses
	filter, err := m.getOrCreateFilter(chainID)
	if err != nil {
		return fmt.Errorf("failed to get or create filter: %w", err)
	}

	// Add only new addresses to the existing filter
	for _, addr := range newAddresses {
		filter.Add([]byte(addr))
	}

	// Store updated bloom filter to database
	if err := m.storeBloomFilterToDB(chainID, filter); err != nil {
		return fmt.Errorf("failed to store bloom filter: %w", err)
	}

	// Update in-memory filter
	m.bloomFilterMgr.SetAddressFilter(chainID, filter)

	// Mark addresses as refreshed
	if len(addressIDs) > 0 {
		updateSQL := `UPDATE registered_addresses SET bloom_refreshed = true WHERE id IN (`
		params := make([]interface{}, len(addressIDs))
		for i, id := range addressIDs {
			if i > 0 {
				updateSQL += ","
			}
			updateSQL += "?"
			params[i] = id
		}
		updateSQL += ")"

		_, err := m.ORMClient.ExecuteSQLUpdate(updateSQL, params...)
		if err != nil {
			m.logger.Warn("Failed to mark addresses as refreshed",
				"chain_id", chainID,
				"error", err)
			return fmt.Errorf("failed to mark addresses as refreshed: %w", err)
		}
	}

	m.logger.Info("Refreshed bloom filter for chain",
		"chain_id", chainID,
		"new_address_count", len(newAddresses))

	return nil
}

// getOrCreateFilter gets the existing filter from in-memory cache, database, or creates a new one
func (m *AddressBloomManager) getOrCreateFilter(chainID string) (*bloom.BloomFilter, error) {
	// First, try to get from in-memory cache (BloomFilterManager)
	if m.bloomFilterMgr != nil {
		filter := m.bloomFilterMgr.GetAddressFilter(chainID)
		if filter != nil {
			m.logger.Debug("Using existing filter from memory cache", "chain_id", chainID)
			return filter, nil
		}
	}

	// Second, try to load from database
	filter, err := m.loadFilterFromDB(chainID)
	if err == nil && filter != nil {
		m.logger.Debug("Loaded filter from database", "chain_id", chainID)
		// Update in-memory cache
		if m.bloomFilterMgr != nil {
			m.bloomFilterMgr.SetAddressFilter(chainID, filter)
		}
		return filter, nil
	}
	if err != nil {
		m.logger.Debug("Failed to load filter from database, will create new one", "chain_id", chainID, "error", err)
	}

	// If not in memory or database, create a new empty filter
	m.logger.Debug("Creating new filter", "chain_id", chainID)
	return bloom.NewWithEstimates(8192, 0.01), nil
}

// loadFilterFromDB loads the bloom filter from database
func (m *AddressBloomManager) loadFilterFromDB(chainID string) (*bloom.BloomFilter, error) {
	sql := `SELECT filter_data FROM bloom_filters WHERE chain_id = ? AND filter_type = ?`
	results, err := m.ORMClient.ExecuteSQL(sql, chainID, "address")
	if err != nil {
		return nil, fmt.Errorf("failed to query database: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no filter found in database")
	}

	// Get filter_data from result
	filterDataRaw, ok := results[0]["filter_data"]
	if !ok {
		return nil, fmt.Errorf("filter_data not found in result")
	}

	// Convert to []byte
	var filterData []byte
	switch v := filterDataRaw.(type) {
	case []byte:
		filterData = v
	case string:
		filterData = []byte(v)
	case []interface{}:
		// Convert []interface{} to []byte
		filterData = make([]byte, len(v))
		for i, val := range v {
			switch b := val.(type) {
			case byte:
				filterData[i] = b
			case int:
				filterData[i] = byte(b)
			case float64:
				filterData[i] = byte(b)
			default:
				return nil, fmt.Errorf("unexpected type in filter_data: %T", val)
			}
		}
	default:
		return nil, fmt.Errorf("unexpected filter_data type: %T", filterDataRaw)
	}

	if len(filterData) == 0 {
		return nil, fmt.Errorf("filter_data is empty")
	}

	// Deserialize bloom filter
	filter := bloom.NewWithEstimates(8192, 0.01)
	if err := filter.UnmarshalBinary(filterData); err != nil {
		return nil, fmt.Errorf("failed to deserialize bloom filter: %w", err)
	}

	return filter, nil
}

// rebuildFilterFromRefreshedAddresses rebuilds the bloom filter from only refreshed addresses
// Uses fixed size matching backend SimpleBloomFilter: 1024 * 8 = 8192 bits
func (m *AddressBloomManager) rebuildFilterFromRefreshedAddresses(chainID string) (*bloom.BloomFilter, error) {
	sql := `SELECT address FROM registered_addresses WHERE chain_id = ? AND (bloom_refreshed = 1 OR bloom_refreshed = true)`
	results, err := m.ORMClient.ExecuteSQL(sql, chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch refreshed addresses: %w", err)
	}

	// Use fixed size matching backend SimpleBloomFilter: 1024 * 8 bits = 8192 bits
	// Backend uses 3 hash functions, Go library will use appropriate number based on false positive rate
	filter := bloom.NewWithEstimates(8192, 0.01) // 8192 capacity, 1% false positive rate
	for _, row := range results {
		address, ok := row["address"].(string)
		if !ok {
			continue
		}
		// Normalize address
		normalized := strings.ToLower(strings.TrimPrefix(address, "0x"))
		filter.Add([]byte(normalized))
	}

	m.logger.Debug("Rebuilt filter from refreshed addresses",
		"chain_id", chainID,
		"address_count", len(results))

	return filter, nil
}

// storeBloomFilterToDB stores the serialized bloom filter to database
func (m *AddressBloomManager) storeBloomFilterToDB(chainID string, filter *bloom.BloomFilter) error {
	// Serialize the bloom filter to bytes
	filterData, err := filter.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to serialize bloom filter: %w", err)
	}

	sql := `
		INSERT INTO bloom_filters (chain_id, filter_type, filter_data, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(chain_id, filter_type) DO UPDATE SET
			filter_data = excluded.filter_data,
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = m.ORMClient.ExecuteSQLUpdate(sql, chainID, "address", filterData)
	return err
}

// refreshLoop periodically refreshes address bloom filters
func (m *AddressBloomManager) refreshLoop() {
	ticker := time.NewTicker(m.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.RefreshUnrefreshedAddresses(); err != nil {
				m.logger.Error("Failed to refresh address bloom filters", "error", err)
			}
		case <-m.stopChan:
			return
		}
	}
}
