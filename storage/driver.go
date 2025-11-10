package storage

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var NotfoundError = errors.New("not found")

type KVDriver interface {
	Store(key string, value []byte) error
	Load(key string) ([]byte, error)
}

type CloudflareKV struct {
	AccountID   string
	NamespaceID string
	APIToken    string
	BaseURL     string
	httpClient  *http.Client
}

func NewCloudflareKV(accountID, namespaceID, apiToken string) *CloudflareKV {
	return &CloudflareKV{
		AccountID:   accountID,
		NamespaceID: namespaceID,
		APIToken:    apiToken,
		BaseURL:     "https://api.cloudflare.com/client/v4",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *CloudflareKV) Store(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Cloudflare KV API endpoint for storing values
	url := fmt.Sprintf("%s/accounts/%s/storage/kv/namespaces/%s/values/%s",
		c.BaseURL, c.AccountID, c.NamespaceID, key)

	return putKVHelper(c.httpClient, c.APIToken, url, value)
}

func (c *CloudflareKV) Load(key string) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Cloudflare KV API endpoint for retrieving values
	url := fmt.Sprintf("%s/accounts/%s/storage/kv/namespaces/%s/values/%s",
		c.BaseURL, c.AccountID, c.NamespaceID, key)

	// Create GET request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set required headers
	req.Header.Set("Authorization", "Bearer "+c.APIToken)

	// Execute the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			slog.Error("failed to close response body", "error", err, "url", url)
		}
	}(resp.Body)

	// Handle different response codes
	switch resp.StatusCode {
	case http.StatusOK:
		// Key found, read the value
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body: %w", err)
		}

		return base64.StdEncoding.DecodeString(string(body))

	case http.StatusNotFound:
		// Key not found
		return nil, NotfoundError

	default:
		// Other error
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cloudflare KV load failed with status %d: %s", resp.StatusCode, string(body))
	}
}

// StoreWithTTL stores a key-value pair with a time-to-live (TTL) in seconds
func (c *CloudflareKV) StoreWithTTL(key string, value []byte, ttlSeconds int) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Cloudflare KV API endpoint with TTL parameter
	url := fmt.Sprintf("%s/accounts/%s/storage/kv/namespaces/%s/values/%s?expiration_ttl=%d",
		c.BaseURL, c.AccountID, c.NamespaceID, key, ttlSeconds)

	return putKVHelper(c.httpClient, c.APIToken, url, value)
}

func putKVHelper(httpClient *http.Client, apiKey, url string, value []byte) error {
	// Create PUT request with the value as body
	req, err := http.NewRequest("PUT", url, strings.NewReader(base64.StdEncoding.EncodeToString(value)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set required headers
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "text/plain")

	// Execute the request
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			slog.Error("failed to close response body", "error", err, "url", url)
		}
	}(resp.Body)

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cloudflare KV store with TTL failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Delete removes a key from the KV store
func (c *CloudflareKV) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Cloudflare KV API endpoint for deleting values
	url := fmt.Sprintf("%s/accounts/%s/storage/kv/namespaces/%s/values/%s",
		c.BaseURL, c.AccountID, c.NamespaceID, key)

	// Create DELETE request
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set required headers
	req.Header.Set("Authorization", "Bearer "+c.APIToken)

	// Execute the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			slog.Error("failed to close response body", "error", err, "url", url)
		}
	}(resp.Body)

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cloudflare KV delete failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Exists checks if a key exists in the KV store
func (c *CloudflareKV) Exists(key string) (bool, error) {
	_, err := c.Load(key)
	if err != nil {
		if errors.Is(err, NotfoundError) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

type CloudflareWorker struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func NewCloudflareWorker(baseURL string, token string) *CloudflareWorker {
	return &CloudflareWorker{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Timeout: time.Second * 10,
		},
	}
}

type CloudflareD1 struct {
	*CloudflareWorker
}

func NewCloudflareD1(worker *CloudflareWorker) *CloudflareD1 {
	return &CloudflareD1{
		worker,
	}
}

func (c *CloudflareD1) Insert(object interface{}) (bool, error) {
	data, err := json.Marshal(object)
	if err != nil {
		return false, fmt.Errorf("failed to marshal object: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/pools/add", bytes.NewBuffer(data))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("remote API returned status %d: %s", resp.StatusCode, string(body))
	}

	return true, nil
}

func (c *CloudflareD1) InsertToken(object interface{}) (bool, error) {
	data, err := json.Marshal(object)
	if err != nil {
		return false, fmt.Errorf("failed to marshal object: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/tokens", bytes.NewBuffer(data))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("remote API returned status %d: %s", resp.StatusCode, string(body))
	}

	return true, nil
}

func (c *CloudflareD1) List(pageIndex, pageSize int, object interface{}) error {
	// Check if object is a pointer
	if reflect.TypeOf(object).Kind() != reflect.Ptr {
		return fmt.Errorf("object must be a pointer")
	}

	// Use SQL query via /db/query endpoint
	offset := (pageIndex - 1) * pageSize
	sql := `SELECT id, chain_id, protocol, pool_address, pool_name, token_0_address, token_0_symbol, token_0_decimals, token_1_address, token_1_symbol, token_1_decimals FROM pool_info ORDER BY created_at DESC LIMIT ? OFFSET ?`

	requestBody := map[string]interface{}{
		"sql":    sql,
		"params": []interface{}{pageSize, offset},
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/db/query", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("X-API-Key", c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote API returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var apiResponse struct {
		Success bool                     `json:"success"`
		Data    []map[string]interface{} `json:"data,omitempty"`
		Error   string                   `json:"error,omitempty"`
	}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&apiResponse); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !apiResponse.Success {
		return fmt.Errorf("API returned error: %s", apiResponse.Error)
	}

	// Convert results to slice of PairInfo
	objValue := reflect.ValueOf(object).Elem()
	sliceType := objValue.Type().Elem() // Get element type (e.g., PairInfo)

	for _, row := range apiResponse.Data {
		pairInfo := PairInfo{
			ChainId:        getString(row["chain_id"]),
			Protocol:       getString(row["protocol"]),
			PoolAddress:    getString(row["pool_address"]),
			PoolName:       getString(row["pool_name"]),
			Token0Address:  getString(row["token_0_address"]),
			Token0Symbol:   getString(row["token_0_symbol"]),
			Token0Decimals: getUint8(row["token_0_decimals"]),
			Token1Address:  getString(row["token_1_address"]),
			Token1Symbol:   getString(row["token_1_symbol"]),
			Token1Decimals: getUint8(row["token_1_decimals"]),
		}

		// Create new element and append to slice
		if sliceType.Kind() == reflect.Ptr {
			// If slice element is a pointer, create pointer to PairInfo
			elemValue := reflect.New(reflect.TypeOf(pairInfo))
			elemValue.Elem().Set(reflect.ValueOf(pairInfo))
			objValue.Set(reflect.Append(objValue, elemValue))
		} else {
			// If slice element is a value, append directly
			objValue.Set(reflect.Append(objValue, reflect.ValueOf(pairInfo)))
		}
	}

	return nil
}

func (c *CloudflareD1) ListToken(pageIndex, pageSize int, object interface{}) error {
	// Check if object is a pointer
	if reflect.TypeOf(object).Kind() != reflect.Ptr {
		return fmt.Errorf("object must be a pointer")
	}

	// Use SQL query via /db/query endpoint
	offset := (pageIndex - 1) * pageSize
	sql := `SELECT id, chain_id, token_address, token_symbol, token_name, decimals, icon_url, daily_volume_usd, volume_updated_at, is_special, created_at, updated_at FROM tokens ORDER BY created_at DESC LIMIT ? OFFSET ?`

	requestBody := map[string]interface{}{
		"sql":    sql,
		"params": []interface{}{pageSize, offset},
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/db/query", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("X-API-Key", c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote API returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var apiResponse struct {
		Success bool                     `json:"success"`
		Data    []map[string]interface{} `json:"data,omitempty"`
		Error   string                   `json:"error,omitempty"`
	}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&apiResponse); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !apiResponse.Success {
		return fmt.Errorf("API returned error: %s", apiResponse.Error)
	}

	// Convert results to slice of TokenInfo
	objValue := reflect.ValueOf(object).Elem()
	sliceType := objValue.Type().Elem() // Get element type (e.g., TokenInfo)

	for _, row := range apiResponse.Data {
		tokenInfo := TokenInfo{
			ChainId:        getString(row["chain_id"]),
			TokenAddress:   getString(row["token_address"]),
			TokenSymbol:    getString(row["token_symbol"]),
			TokenName:      getString(row["token_name"]),
			Decimals:       getUint8(row["decimals"]),
			IconUrl:        getString(row["icon_url"]),
			DailyVolumeUSD: getFloat64(row["daily_volume_usd"]),
			IsSpecial:      getBool(row["is_special"]),
			OnlyCache:      false,
		}

		// Create new element and append to slice
		if sliceType.Kind() == reflect.Ptr {
			// If slice element is a pointer, create pointer to TokenInfo
			elemValue := reflect.New(reflect.TypeOf(tokenInfo))
			elemValue.Elem().Set(reflect.ValueOf(tokenInfo))
			objValue.Set(reflect.Append(objValue, elemValue))
		} else {
			// If slice element is a value, append directly
			objValue.Set(reflect.Append(objValue, reflect.ValueOf(tokenInfo)))
		}
	}

	return nil
}

func (c *CloudflareD1) Get(chainId, protocolName, poolAddress string, object interface{}) error {
	// Check if object is a pointer
	if reflect.TypeOf(object).Kind() != reflect.Ptr {
		return fmt.Errorf("object must be a pointer")
	}

	url := fmt.Sprintf("%s/pool?chain_id=%s&protocol_name=%s&pool_address=%s", c.baseURL, chainId, protocolName, poolAddress)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.token)

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote API returned status %d: %s", resp.StatusCode, string(body))
	}

	// TODO: Could be null
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(object); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

func (c *CloudflareD1) GetToken(chainId, tokenAddress string, object interface{}) error {
	// Check if object is a pointer
	if reflect.TypeOf(object).Kind() != reflect.Ptr {
		return fmt.Errorf("object must be a pointer")
	}

	// Use SQL query via /db/query endpoint
	sql := `SELECT id, chain_id, token_address, token_symbol, token_name, decimals, icon_url, daily_volume_usd, volume_updated_at, is_special, created_at, updated_at FROM tokens WHERE chain_id = ? AND token_address = ? LIMIT 1`

	requestBody := map[string]interface{}{
		"sql":    sql,
		"params": []interface{}{chainId, tokenAddress},
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/db/query", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.token)

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote API returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var apiResponse struct {
		Success bool                     `json:"success"`
		Data    []map[string]interface{} `json:"data,omitempty"`
		Error   string                   `json:"error,omitempty"`
	}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&apiResponse); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !apiResponse.Success {
		return fmt.Errorf("API returned error: %s", apiResponse.Error)
	}

	if len(apiResponse.Data) == 0 {
		return NotfoundError
	}

	// Convert map to struct
	result := apiResponse.Data[0]

	// Map database fields to TokenInfo struct
	tokenInfo := TokenInfo{
		ChainId:        getString(result["chain_id"]),
		TokenAddress:   getString(result["token_address"]),
		TokenSymbol:    getString(result["token_symbol"]),
		TokenName:      getString(result["token_name"]),
		Decimals:       getUint8(result["decimals"]),
		IconUrl:        getString(result["icon_url"]),
		DailyVolumeUSD: getFloat64(result["daily_volume_usd"]),
		IsSpecial:      getBool(result["is_special"]),
		OnlyCache:      false,
	}

	// Set the object value using reflection
	// object is a pointer, so we need to get the element and set it
	objValue := reflect.ValueOf(object).Elem()
	objValue.Set(reflect.ValueOf(tokenInfo))

	return nil
}

// Helper functions to safely extract values from map[string]interface{}
func getString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func getUint8(v interface{}) uint8 {
	if v == nil {
		return 0
	}
	if f, ok := v.(float64); ok {
		return uint8(f)
	}
	return 0
}

func getFloat64(v interface{}) float64 {
	if v == nil {
		return 0
	}
	if f, ok := v.(float64); ok {
		return f
	}
	if s, ok := v.(string); ok {
		// Try to parse string as float
		if parsed, err := strconv.ParseFloat(s, 64); err == nil {
			return parsed
		}
	}
	return 0
}

func getBool(v interface{}) bool {
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	// SQLite returns 0/1 as float64
	if f, ok := v.(float64); ok {
		return f != 0
	}
	return false
}

type CloudflareDurable struct {
	*CloudflareWorker
}

func NewCloudflareDurable(worker *CloudflareWorker) *CloudflareDurable {
	return &CloudflareDurable{
		worker,
	}
}

func (c *CloudflareDurable) Publish(tokenID string, data []byte) (bool, error) {
	req, err := http.NewRequest("POST", c.baseURL+"/publish", bytes.NewBuffer(data))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/binary")
	req.Header.Set("X-API-Key", c.token)
	req.Header.Set("Customized-Token-ID", tokenID)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("remote API returned status %d: %s", resp.StatusCode, string(body))
	}

	return true, nil
}
