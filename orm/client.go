package orm

import (
	"bytes"
	"crypto-mine/orm/models"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"log/slog"
)

// Client is the ORM client that communicates with the crypto-pump API
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	logger     *slog.Logger
}

// NewClient creates a new ORM client
func NewClient(baseURL string, apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: slog.Default(),
	}
}

// SetLogger sets a custom logger for the client
func (c *Client) SetLogger(logger *slog.Logger) {
	c.logger = logger
}

// doRequest performs an HTTP request with authentication
func (c *Client) doRequest(method, endpoint string, body interface{}) (*http.Response, error) {
	url := c.baseURL + endpoint

	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	return resp, nil
}

// parseResponse parses the API response
func (c *Client) parseResponse(resp *http.Response, target interface{}) error {
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	if target == nil {
		return nil
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	var apiResponse struct {
		Success bool        `json:"success"`
		Data    interface{} `json:"data"`
		Error   string      `json:"error,omitempty"`
	}

	if err := json.Unmarshal(bodyBytes, &apiResponse); err != nil {
		return fmt.Errorf("failed to parse API response: %w", err)
	}

	if !apiResponse.Success {
		return fmt.Errorf("API returned error: %s", apiResponse.Error)
	}

	// Marshal data back to JSON and unmarshal into target
	dataBytes, err := json.Marshal(apiResponse.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal response data: %w", err)
	}

	if err := json.Unmarshal(dataBytes, target); err != nil {
		return fmt.Errorf("failed to unmarshal response data: %w", err)
	}

	return nil
}

// ExecuteSQL executes a SQL query and returns the results
func (c *Client) ExecuteSQL(sql string, params ...interface{}) ([]map[string]interface{}, error) {
	requestBody := map[string]interface{}{
		"sql":    sql,
		"params": params,
	}

	resp, err := c.doRequest("POST", "/db/query", requestBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var apiResponse struct {
		Success bool                     `json:"success"`
		Data    []map[string]interface{} `json:"data,omitempty"`
		Meta    interface{}              `json:"meta,omitempty"`
		Error   string                   `json:"error,omitempty"`
		Message string                   `json:"message,omitempty"`
	}

	if err := json.Unmarshal(bodyBytes, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse API response: %w", err)
	}

	if !apiResponse.Success {
		return nil, fmt.Errorf("API returned error: %s", apiResponse.Error)
	}

	return apiResponse.Data, nil
}

// ExecuteSQLSingle executes a SQL query and returns the first result
func (c *Client) ExecuteSQLSingle(sql string, params ...interface{}) (map[string]interface{}, error) {
	results, err := c.ExecuteSQL(sql, params...)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no results found")
	}

	return results[0], nil
}

// ExecuteSQLUpdate executes an INSERT, UPDATE, or DELETE query and returns metadata
func (c *Client) ExecuteSQLUpdate(sql string, params ...interface{}) (map[string]interface{}, error) {
	requestBody := map[string]interface{}{
		"sql":    sql,
		"params": params,
	}

	resp, err := c.doRequest("POST", "/db/query", requestBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var apiResponse struct {
		Success bool        `json:"success"`
		Meta    interface{} `json:"meta,omitempty"`
		Error   string      `json:"error,omitempty"`
		Message string      `json:"message,omitempty"`
	}

	if err := json.Unmarshal(bodyBytes, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse API response: %w", err)
	}

	if !apiResponse.Success {
		return nil, fmt.Errorf("API returned error: %s", apiResponse.Error)
	}

	metaMap, ok := apiResponse.Meta.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}, nil
	}

	return metaMap, nil
}

// SQLBatchQuery represents a single query in a batch
type SQLBatchQuery struct {
	SQL    string
	Params []interface{}
}

// ExecuteSQLBatch executes multiple SQL queries and returns results for each
func (c *Client) ExecuteSQLBatch(queries []SQLBatchQuery) ([]map[string]interface{}, error) {
	requestQueries := make([]map[string]interface{}, len(queries))
	for i, q := range queries {
		requestQueries[i] = map[string]interface{}{
			"sql":    q.SQL,
			"params": q.Params,
		}
	}

	requestBody := map[string]interface{}{
		"queries": requestQueries,
	}

	resp, err := c.doRequest("POST", "/db/query/batch", requestBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var apiResponse struct {
		Success bool                     `json:"success"`
		Results []map[string]interface{} `json:"results,omitempty"`
		Error   string                   `json:"error,omitempty"`
		Message string                   `json:"message,omitempty"`
	}

	if err := json.Unmarshal(bodyBytes, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse API response: %w", err)
	}

	if !apiResponse.Success {
		return nil, fmt.Errorf("API returned error: %s", apiResponse.Error)
	}

	return apiResponse.Results, nil
}

// GetActiveWatchedTokens retrieves all active watched tokens
func (c *Client) GetActiveWatchedTokens() ([]models.WatchedToken, error) {
	resp, err := c.doRequest("GET", "/db/watched-tokens/active", nil)
	if err != nil {
		return nil, err
	}

	var apiResponse struct {
		Success bool                  `json:"success"`
		Data    []models.WatchedToken `json:"data"`
		Count   int                   `json:"count"`
		Error   string                `json:"error,omitempty"`
	}

	if err := c.parseResponse(resp, &apiResponse); err != nil {
		return nil, err
	}

	return apiResponse.Data, nil
}

// GetWatchedTokensByTokenID retrieves watched tokens for a specific token ID
func (c *Client) GetWatchedTokensByTokenID(tokenID int) ([]models.WatchedToken, error) {
	endpoint := fmt.Sprintf("/db/watched-tokens/token/%d", tokenID)
	resp, err := c.doRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	var apiResponse struct {
		Success bool                  `json:"success"`
		Data    []models.WatchedToken `json:"data"`
		Count   int                   `json:"count"`
		Error   string                `json:"error,omitempty"`
	}

	if err := c.parseResponse(resp, &apiResponse); err != nil {
		return nil, err
	}

	return apiResponse.Data, nil
}

// GetUser retrieves a user by ID
func (c *Client) GetUser(userID int) (*models.User, error) {
	endpoint := fmt.Sprintf("/db/users/%d", userID)
	resp, err := c.doRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	var user models.User
	if err := c.parseResponse(resp, &user); err != nil {
		return nil, err
	}

	return &user, nil
}

// GetUserByTelegramID retrieves a user by Telegram ID
func (c *Client) GetUserByTelegramID(telegramID string) (*models.User, error) {
	endpoint := fmt.Sprintf("/db/users/telegram/%s", telegramID)
	resp, err := c.doRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	var user models.User
	if err := c.parseResponse(resp, &user); err != nil {
		return nil, err
	}

	return &user, nil
}

// GetNotificationPreferences retrieves notification preferences for a user
func (c *Client) GetNotificationPreferences(userID int) (*models.NotificationPreferences, error) {
	endpoint := fmt.Sprintf("/db/users/%d/notification-preferences", userID)
	resp, err := c.doRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	var prefs models.NotificationPreferences
	if err := c.parseResponse(resp, &prefs); err != nil {
		return nil, err
	}

	return &prefs, nil
}

// GetToken retrieves a token by chain ID and address
func (c *Client) GetToken(chainID, tokenAddress string) (*models.Token, error) {
	endpoint := fmt.Sprintf("/db/tokens/%s/%s", chainID, tokenAddress)
	resp, err := c.doRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	var token models.Token
	if err := c.parseResponse(resp, &token); err != nil {
		return nil, err
	}

	return &token, nil
}

// GetTokenByID retrieves a token by ID
func (c *Client) GetTokenByID(tokenID int) (*models.Token, error) {
	endpoint := fmt.Sprintf("/db/tokens/id/%d", tokenID)
	resp, err := c.doRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	var token models.Token
	if err := c.parseResponse(resp, &token); err != nil {
		return nil, err
	}

	return &token, nil
}
