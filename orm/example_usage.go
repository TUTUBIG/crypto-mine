package orm

// Example usage of the ORM client with SQL queries
//
// This demonstrates how to use the ORM client with SQL queries generated
// by Go ORM libraries like GORM, sqlx, or custom query builders.

import (
	"crypto-mine/orm/models"
	"fmt"
)

// Example using direct SQL queries with the ORM client
func ExampleSQLUsage(client *Client) {
	// Example 1: SELECT query
	sql := `SELECT id, user_id, token_id, interval_1m, interval_5m, interval_15m, interval_1h, alert_active
			FROM user_watched_tokens
			WHERE alert_active = 1`

	results, err := client.ExecuteSQL(sql)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Process results
	for _, row := range results {
		// row is a map[string]interface{}
		fmt.Printf("Watched Token ID: %v\n", row["id"])
	}

	// Example 2: SELECT with parameters
	sql2 := `SELECT * FROM user_watched_tokens WHERE user_id = ? AND token_id = ?`
	results2, err := client.ExecuteSQL(sql2, 123, 456)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Example 3: SELECT single result
	sql3 := `SELECT * FROM users WHERE id = ?`
	user, err := client.ExecuteSQLSingle(sql3, 123)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("User: %+v\n", user)

	// Example 4: INSERT query
	insertSQL := `INSERT INTO user_watched_tokens (user_id, token_id, interval_1m, alert_active)
				  VALUES (?, ?, ?, ?)`
	meta, err := client.ExecuteSQLUpdate(insertSQL, 123, 456, 5.0, true)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Insert successful, last_row_id: %v\n", meta["last_row_id"])

	// Example 5: UPDATE query
	updateSQL := `UPDATE user_watched_tokens 
				  SET interval_1m = ?, alert_active = ?
				  WHERE id = ?`
	_, err = client.ExecuteSQLUpdate(updateSQL, 10.0, false, 789)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Example 6: Batch queries
	batchQueries := []SQLBatchQuery{
		{
			SQL:    `SELECT * FROM users WHERE id = ?`,
			Params: []interface{}{123},
		},
		{
			SQL:    `SELECT * FROM notification_preferences WHERE user_id = ?`,
			Params: []interface{}{123},
		},
		{
			SQL:    `UPDATE users SET last_login_at = CURRENT_TIMESTAMP WHERE id = ?`,
			Params: []interface{}{123},
		},
	}

	batchResults, err := client.ExecuteSQLBatch(batchQueries)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for i, result := range batchResults {
		fmt.Printf("Batch query %d result: %+v\n", i, result)
	}
}

// Example using with GORM-style queries (if you build SQL manually)
func ExampleWithGORMStyle(client *Client, userID int, tokenID int) {
	// Build SQL query like GORM would
	sql := fmt.Sprintf(`
		SELECT 
			wt.id,
			wt.user_id,
			wt.token_id,
			wt.notes,
			wt.interval_1m,
			wt.interval_5m,
			wt.interval_15m,
			wt.interval_1h,
			wt.alert_active,
			wt.created_at,
			t.chain_id,
			t.token_address,
			t.token_symbol,
			t.token_name,
			t.decimals,
			t.icon_url
		FROM user_watched_tokens wt
		INNER JOIN tokens t ON wt.token_id = t.id
		WHERE wt.user_id = ? AND wt.token_id = ? AND wt.alert_active = 1
	`)

	results, err := client.ExecuteSQL(sql, userID, tokenID)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Convert results to models
	for _, row := range results {
		watchedToken := &models.WatchedToken{
			ID:           int(row["id"].(float64)),
			UserID:       int(row["user_id"].(float64)),
			TokenID:      int(row["token_id"].(float64)),
			AlertActive:  row["alert_active"].(bool),
			ChainID:      row["chain_id"].(string),
			TokenAddress: row["token_address"].(string),
			TokenSymbol:  row["token_symbol"].(string),
			TokenName:    row["token_name"].(string),
			Decimals:     int(row["decimals"].(float64)),
		}

		// Handle optional fields
		if notes, ok := row["notes"].(string); ok && notes != "" {
			watchedToken.Notes = &notes
		}
		if interval1m, ok := row["interval_1m"].(float64); ok {
			watchedToken.Interval1m = &interval1m
		}

		fmt.Printf("Watched Token: %+v\n", watchedToken)
	}
}
