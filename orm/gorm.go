package orm

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"log/slog"
)

// DB represents a GORM-like database session
type DB struct {
	client *Client
	logger *slog.Logger
}

// NewDB creates a new GORM-like database session
func NewDB(client *Client) *DB {
	return &DB{
		client: client,
		logger: slog.Default(),
	}
}

// SetLogger sets a custom logger
func (db *DB) SetLogger(logger *slog.Logger) {
	db.logger = logger
}

// Model specifies the model to use for the query
func (db *DB) Model(value interface{}) *Query {
	return &Query{
		db:        db,
		model:     value,
		modelType: reflect.TypeOf(value),
		where:     []whereCondition{},
		order:     []string{},
		limitVal:  0,
		offsetVal: 0,
		logger:    db.logger,
	}
}

// Query represents a GORM-like query builder
type Query struct {
	db        *DB
	model     interface{}
	modelType reflect.Type
	tableName string
	where     []whereCondition
	order     []string
	limitVal  int
	offsetVal int
	preload   []string
	logger    *slog.Logger
}

// whereCondition represents a WHERE clause condition
type whereCondition struct {
	column   string
	operator string
	value    interface{}
}

// Where adds a WHERE condition to the query
// Supports: Where("id", 1), Where("id = ?", 1), Where("id > ?", 1), etc.
func (q *Query) Where(query interface{}, args ...interface{}) *Query {
	if str, ok := query.(string); ok {
		if len(args) == 0 {
			// Direct SQL condition without placeholders: Where("id = 1")
			// Not recommended but supported
			q.where = append(q.where, whereCondition{
				column:   str,
				operator: "",
				value:    nil,
			})
		} else if len(args) == 1 {
			// Parse condition with operator
			str = strings.TrimSpace(str)
			operators := []string{"=", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "IN"}

			var column string
			var operator string = "="
			var value interface{} = args[0]

			// Check if it contains an operator
			found := false
			for _, op := range operators {
				if strings.Contains(str, " "+op+" ") || strings.Contains(str, op+" ?") {
					parts := strings.Split(str, op)
					if len(parts) == 2 {
						column = strings.TrimSpace(parts[0])
						operator = op
						found = true
						break
					}
				}
			}

			if !found {
				// Simple equality: Where("id", 1)
				column = str
			}

			q.where = append(q.where, whereCondition{
				column:   column,
				operator: operator,
				value:    value,
			})
		} else {
			// Multiple args: Where("id IN ?", []int{1,2,3})
			// For now, treat as single condition
			q.where = append(q.where, whereCondition{
				column:   str,
				operator: "=",
				value:    args[0],
			})
		}
	}
	return q
}

// Order specifies ORDER BY clause
func (q *Query) Order(value string) *Query {
	q.order = append(q.order, value)
	return q
}

// Limit specifies LIMIT clause
func (q *Query) Limit(limit int) *Query {
	q.limitVal = limit
	return q
}

// Offset specifies OFFSET clause
func (q *Query) Offset(offset int) *Query {
	q.offsetVal = offset
	return q
}

// Preload specifies relations to preload (for future JOIN support)
func (q *Query) Preload(associations ...string) *Query {
	q.preload = append(q.preload, associations...)
	return q
}

// Find executes the query and returns all matching records
func (q *Query) Find(dest interface{}) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	sliceValue := destValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	// Build SQL query
	sql, params := q.buildSelectSQL()

	// Execute query
	results, err := q.db.client.ExecuteSQL(sql, params...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Convert results to models
	elemType := sliceValue.Type().Elem()
	for _, row := range results {
		// Create new element of the correct type
		var elemValue reflect.Value
		if elemType.Kind() == reflect.Ptr {
			// If slice element is a pointer, create new instance
			elemValue = reflect.New(elemType.Elem())
			if err := ConvertRowToModel(row, elemValue.Interface()); err != nil {
				q.logger.Warn("Failed to convert row to model", "error", err)
				continue
			}
			sliceValue.Set(reflect.Append(sliceValue, elemValue))
		} else {
			// If slice element is a value, create pointer then dereference
			elemPtr := reflect.New(elemType)
			if err := ConvertRowToModel(row, elemPtr.Interface()); err != nil {
				q.logger.Warn("Failed to convert row to model", "error", err)
				continue
			}
			sliceValue.Set(reflect.Append(sliceValue, elemPtr.Elem()))
		}
	}

	return nil
}

// First executes the query and returns the first matching record
func (q *Query) First(dest interface{}) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer")
	}

	// Add LIMIT 1
	q.limitVal = 1

	// Build SQL query
	sql, params := q.buildSelectSQL()

	// Execute query
	result, err := q.db.client.ExecuteSQLSingle(sql, params...)
	if err != nil {
		return fmt.Errorf("record not found: %w", err)
	}

	// Convert result to model
	return ConvertRowToModel(result, dest)
}

// Create inserts a new record
func (q *Query) Create(value interface{}) error {
	// Build INSERT SQL
	sql, params := q.buildInsertSQL(value)

	// Execute query
	meta, err := q.db.client.ExecuteSQLUpdate(sql, params...)
	if err != nil {
		return fmt.Errorf("failed to create record: %w", err)
	}

	// Set ID if returned
	if lastRowID, ok := meta["last_row_id"].(float64); ok {
		valueValue := reflect.ValueOf(value)
		if valueValue.Kind() == reflect.Ptr {
			valueValue = valueValue.Elem()
		}
		if idField := valueValue.FieldByName("ID"); idField.IsValid() && idField.CanSet() {
			idField.SetInt(int64(lastRowID))
		}
	}

	return nil
}

// Update updates records matching the query
func (q *Query) Update(column string, value interface{}) error {
	// Build UPDATE SQL
	sql, params := q.buildUpdateSQL(column, value)

	// Execute query
	_, err := q.db.client.ExecuteSQLUpdate(sql, params...)
	if err != nil {
		return fmt.Errorf("failed to update record: %w", err)
	}

	return nil
}

// Updates updates multiple columns
func (q *Query) Updates(values map[string]interface{}) error {
	// Build UPDATE SQL
	sql, params := q.buildUpdatesSQL(values)

	// Execute query
	_, err := q.db.client.ExecuteSQLUpdate(sql, params...)
	if err != nil {
		return fmt.Errorf("failed to update records: %w", err)
	}

	return nil
}

// Delete deletes records matching the query
func (q *Query) Delete() error {
	// Build DELETE SQL
	sql, params := q.buildDeleteSQL()

	// Execute query
	_, err := q.db.client.ExecuteSQLUpdate(sql, params...)
	if err != nil {
		return fmt.Errorf("failed to delete records: %w", err)
	}

	return nil
}

// Count returns the count of matching records
func (q *Query) Count(count *int64) error {
	// Build COUNT SQL
	sql, params := q.buildCountSQL()

	// Execute query
	result, err := q.db.client.ExecuteSQLSingle(sql, params...)
	if err != nil {
		return fmt.Errorf("failed to count records: %w", err)
	}

	// Extract count
	if countVal, ok := result["count"].(float64); ok {
		*count = int64(countVal)
	} else {
		return fmt.Errorf("invalid count result")
	}

	return nil
}

// buildSelectSQL builds a SELECT SQL query
func (q *Query) buildSelectSQL() (string, []interface{}) {
	tableName := q.getTableName()
	sql := fmt.Sprintf("SELECT * FROM %s", tableName)
	params := []interface{}{}

	// Add WHERE clause
	if len(q.where) > 0 {
		whereParts := []string{}
		for _, cond := range q.where {
			if cond.operator == "" {
				// Direct SQL condition (not recommended but supported)
				whereParts = append(whereParts, cond.column)
			} else {
				whereParts = append(whereParts, fmt.Sprintf("%s %s ?", cond.column, cond.operator))
				params = append(params, cond.value)
			}
		}
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	// Add ORDER BY clause
	if len(q.order) > 0 {
		sql += " ORDER BY " + strings.Join(q.order, ", ")
	}

	// Add LIMIT clause
	if q.limitVal > 0 {
		sql += fmt.Sprintf(" LIMIT %d", q.limitVal)
	}

	// Add OFFSET clause
	if q.offsetVal > 0 {
		sql += fmt.Sprintf(" OFFSET %d", q.offsetVal)
	}

	return sql, params
}

// buildInsertSQL builds an INSERT SQL query using cached reflection info
func (q *Query) buildInsertSQL(value interface{}) (string, []interface{}) {
	tableName := q.getTableName()
	valueValue := reflect.ValueOf(value)
	if valueValue.Kind() == reflect.Ptr {
		valueValue = valueValue.Elem()
	}
	valueType := valueValue.Type()

	// Get cached field info for better performance
	fieldInfo := globalCache.getFieldInfo(valueType)
	if fieldInfo == nil {
		// Fallback to non-cached approach
		return q.buildInsertSQLFallback(value, tableName)
	}

	columns := []string{}
	placeholders := []string{}
	params := []interface{}{}

	// Iterate through cached field info
	for i := 0; i < valueType.NumField(); i++ {
		fieldValue := valueValue.Field(i)
		fieldType := fieldInfo.fieldTypes[i]

		// Skip zero values for optional fields
		if fieldType.Kind() == reflect.Ptr && fieldValue.IsNil() {
			continue
		}

		// Skip ID field if it's zero (auto-increment)
		if fieldInfo.fieldNames[i] == "ID" && fieldValue.Kind() == reflect.Int {
			if fieldValue.Int() == 0 {
				continue
			}
		}

		columnName := fieldInfo.fieldToColumn[fieldInfo.fieldNames[i]]
		columns = append(columns, columnName)
		placeholders = append(placeholders, "?")

		// Get field value
		if fieldValue.Kind() == reflect.Ptr {
			params = append(params, fieldValue.Elem().Interface())
		} else {
			params = append(params, fieldValue.Interface())
		}
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	return sql, params
}

// buildInsertSQLFallback is a fallback implementation without cache
func (q *Query) buildInsertSQLFallback(value interface{}, tableName string) (string, []interface{}) {
	valueValue := reflect.ValueOf(value)
	if valueValue.Kind() == reflect.Ptr {
		valueValue = valueValue.Elem()
	}
	valueType := valueValue.Type()

	columns := []string{}
	placeholders := []string{}
	params := []interface{}{}

	for i := 0; i < valueType.NumField(); i++ {
		field := valueType.Field(i)
		fieldValue := valueValue.Field(i)

		// Skip zero values for optional fields
		if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
			continue
		}

		columnName := toSnakeCase(field.Name)
		columns = append(columns, columnName)
		placeholders = append(placeholders, "?")

		// Get field value
		if fieldValue.Kind() == reflect.Ptr {
			params = append(params, fieldValue.Elem().Interface())
		} else {
			params = append(params, fieldValue.Interface())
		}
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	return sql, params
}

// buildUpdateSQL builds an UPDATE SQL query for a single column
func (q *Query) buildUpdateSQL(column string, value interface{}) (string, []interface{}) {
	tableName := q.getTableName()
	sql := fmt.Sprintf("UPDATE %s SET %s = ?", tableName, column)
	params := []interface{}{value}

	// Add WHERE clause
	if len(q.where) > 0 {
		whereParts := []string{}
		for _, cond := range q.where {
			if cond.operator == "" {
				// Direct SQL condition (not recommended but supported)
				whereParts = append(whereParts, cond.column)
			} else {
				whereParts = append(whereParts, fmt.Sprintf("%s %s ?", cond.column, cond.operator))
				params = append(params, cond.value)
			}
		}
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	return sql, params
}

// buildUpdatesSQL builds an UPDATE SQL query for multiple columns
func (q *Query) buildUpdatesSQL(values map[string]interface{}) (string, []interface{}) {
	tableName := q.getTableName()
	setParts := []string{}
	params := []interface{}{}

	for column, value := range values {
		setParts = append(setParts, fmt.Sprintf("%s = ?", column))
		params = append(params, value)
	}

	sql := fmt.Sprintf("UPDATE %s SET %s", tableName, strings.Join(setParts, ", "))

	// Add WHERE clause
	if len(q.where) > 0 {
		whereParts := []string{}
		for _, cond := range q.where {
			if cond.operator == "" {
				// Direct SQL condition (not recommended but supported)
				whereParts = append(whereParts, cond.column)
			} else {
				whereParts = append(whereParts, fmt.Sprintf("%s %s ?", cond.column, cond.operator))
				params = append(params, cond.value)
			}
		}
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	return sql, params
}

// buildDeleteSQL builds a DELETE SQL query
func (q *Query) buildDeleteSQL() (string, []interface{}) {
	tableName := q.getTableName()
	sql := fmt.Sprintf("DELETE FROM %s", tableName)
	params := []interface{}{}

	// Add WHERE clause
	if len(q.where) > 0 {
		whereParts := []string{}
		for _, cond := range q.where {
			if cond.operator == "" {
				// Direct SQL condition (not recommended but supported)
				whereParts = append(whereParts, cond.column)
			} else {
				whereParts = append(whereParts, fmt.Sprintf("%s %s ?", cond.column, cond.operator))
				params = append(params, cond.value)
			}
		}
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	return sql, params
}

// buildCountSQL builds a COUNT SQL query
func (q *Query) buildCountSQL() (string, []interface{}) {
	tableName := q.getTableName()
	sql := fmt.Sprintf("SELECT COUNT(*) as count FROM %s", tableName)
	params := []interface{}{}

	// Add WHERE clause
	if len(q.where) > 0 {
		whereParts := []string{}
		for _, cond := range q.where {
			if cond.operator == "" {
				// Direct SQL condition (not recommended but supported)
				whereParts = append(whereParts, cond.column)
			} else {
				whereParts = append(whereParts, fmt.Sprintf("%s %s ?", cond.column, cond.operator))
				params = append(params, cond.value)
			}
		}
		sql += " WHERE " + strings.Join(whereParts, " AND ")
	}

	return sql, params
}

// getTableName gets the table name from the model using cached reflection info
func (q *Query) getTableName() string {
	if q.tableName != "" {
		return q.tableName
	}

	// Get table name from cached reflection info
	if q.modelType != nil {
		// Handle pointer types
		t := q.modelType
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		return globalCache.getTableName(t)
	}

	return "unknown"
}

// Table specifies the table name explicitly
func (q *Query) Table(name string) *Query {
	q.tableName = name
	return q
}

// Transaction represents a database transaction (for future implementation)
type Transaction struct {
	db *DB
}

// Begin begins a transaction
func (db *DB) Begin() (*Transaction, error) {
	// For now, return a transaction-like object
	// Full transaction support would require backend support
	return &Transaction{db: db}, nil
}

// Commit commits the transaction
func (tx *Transaction) Commit() error {
	// Placeholder for future implementation
	return nil
}

// Rollback rolls back the transaction
func (tx *Transaction) Rollback() error {
	// Placeholder for future implementation
	return nil
}

// Now returns the current time (useful for timestamps)
func Now() time.Time {
	return time.Now()
}
