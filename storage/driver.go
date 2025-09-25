package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
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

	// Create PUT request with the value as body
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(value))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set required headers
	req.Header.Set("Authorization", "Bearer "+c.APIToken)
	req.Header.Set("Content-Type", "text/plain")

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
		return fmt.Errorf("cloudflare KV store failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
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
		return body, nil

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

	// Create PUT request with the value as body
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(value))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set required headers
	req.Header.Set("Authorization", "Bearer "+c.APIToken)
	req.Header.Set("Content-Type", "text/plain")

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

type CloudflareD1 struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func NewCloudflareD1() *CloudflareD1 {
	return &CloudflareD1{
		baseURL: os.Getenv("worker_host"),
		token:   os.Getenv("worker_token"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
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
	// TODO: Check if object is a pointer

	url := fmt.Sprintf("%s/pools?page=%d&pageSize=%d", c.baseURL, pageIndex, pageSize)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
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

	if err := json.NewDecoder(resp.Body).Decode(object); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

func (c *CloudflareD1) ListToken(pageIndex, pageSize int, object interface{}) error {
	// TODO: Check if object is a pointer

	url := fmt.Sprintf("%s/tokens?page=%d&pageSize=%d", c.baseURL, pageIndex, pageSize)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
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

	if err := json.NewDecoder(resp.Body).Decode(object); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

func (c *CloudflareD1) Get(chainId, protocolName, poolAddress string, object interface{}) error {
	// TODO: Check if object is a pointer

	url := fmt.Sprintf("%s/pool?chain_id=%s&protocol_name=%s&pool_address=%s", c.baseURL, chainId, protocolName, poolAddress)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
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

func (c *CloudflareD1) GetToken(tokenAddress string, object interface{}) error {
	// TODO: Check if object is a pointer

	url := fmt.Sprintf("%s/token?address=%s", c.baseURL, tokenAddress)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
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

type CloudflareDurable struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func NewCloudflareDurable() *CloudflareDurable {
	return &CloudflareDurable{
		baseURL: os.Getenv("worker_host"),
		token:   os.Getenv("worker_token"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *CloudflareDurable) Publish(tokenID string, data []byte) (bool, error) {
	req, err := http.NewRequest("POST", c.baseURL+"/publish", bytes.NewBuffer(data))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/binary")
	req.Header.Set("Authorization", "Bearer "+c.token)
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
