package orm

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// reflectCache caches reflection metadata for types
type reflectCache struct {
	mu sync.RWMutex

	// Table name cache: type -> table name
	tableNames map[reflect.Type]string

	// Field info cache: type -> field info
	fieldInfo map[reflect.Type]*typeFieldInfo
}

// typeFieldInfo contains cached field information for a type
type typeFieldInfo struct {
	// Field name (PascalCase) -> database column name (snake_case)
	fieldToColumn map[string]string
	// Database column name (snake_case) -> field index
	columnToField map[string]int
	// Field index -> field type
	fieldTypes map[int]reflect.Type
	// Field index -> field name
	fieldNames map[int]string
}

var globalCache = &reflectCache{
	tableNames: make(map[reflect.Type]string),
	fieldInfo:  make(map[reflect.Type]*typeFieldInfo),
}

// getTableName returns the cached table name for a type
func (c *reflectCache) getTableName(t reflect.Type) string {
	// Handle pointer types
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Check cache
	c.mu.RLock()
	if name, ok := c.tableNames[t]; ok {
		c.mu.RUnlock()
		return name
	}
	c.mu.RUnlock()

	// Compute and cache
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if name, ok := c.tableNames[t]; ok {
		return name
	}

	// Compute table name
	name := c.computeTableName(t)
	c.tableNames[t] = name
	return name
}

// computeTableName computes the table name for a type
func (c *reflectCache) computeTableName(t reflect.Type) string {
	typeName := t.Name()

	// Special case mappings
	tableNameMap := map[string]string{
		"User":                    "users",
		"Token":                   "tokens",
		"WatchedToken":            "user_watched_tokens",
		"NotificationPreferences": "notification_preferences",
	}

	if mapped, ok := tableNameMap[typeName]; ok {
		return mapped
	}

	// Default: Convert to snake_case and pluralize
	tableName := toSnakeCase(typeName)
	// Simple pluralization (add 's')
	if !strings.HasSuffix(tableName, "s") {
		tableName += "s"
	}
	return tableName
}

// getFieldInfo returns cached field information for a type
func (c *reflectCache) getFieldInfo(t reflect.Type) *typeFieldInfo {
	// Handle pointer types
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil
	}

	// Check cache
	c.mu.RLock()
	if info, ok := c.fieldInfo[t]; ok {
		c.mu.RUnlock()
		return info
	}
	c.mu.RUnlock()

	// Compute and cache
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if info, ok := c.fieldInfo[t]; ok {
		return info
	}

	// Compute field info
	info := c.computeFieldInfo(t)
	c.fieldInfo[t] = info
	return info
}

// computeFieldInfo computes field information for a type
func (c *reflectCache) computeFieldInfo(t reflect.Type) *typeFieldInfo {
	info := &typeFieldInfo{
		fieldToColumn: make(map[string]string),
		columnToField: make(map[string]int),
		fieldTypes:    make(map[int]reflect.Type),
		fieldNames:    make(map[int]string),
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldName := field.Name
		columnName := toSnakeCase(fieldName)

		info.fieldToColumn[fieldName] = columnName
		info.columnToField[columnName] = i
		info.fieldTypes[i] = field.Type
		info.fieldNames[i] = fieldName
	}

	return info
}

// getColumnName returns the database column name for a field
func (c *reflectCache) getColumnName(t reflect.Type, fieldName string) (string, error) {
	info := c.getFieldInfo(t)
	if info == nil {
		return "", fmt.Errorf("invalid type: %v", t)
	}

	if column, ok := info.fieldToColumn[fieldName]; ok {
		return column, nil
	}

	return "", fmt.Errorf("field %s not found in type %v", fieldName, t)
}

// getFieldIndex returns the field index for a database column name
func (c *reflectCache) getFieldIndex(t reflect.Type, columnName string) (int, error) {
	info := c.getFieldInfo(t)
	if info == nil {
		return -1, fmt.Errorf("invalid type: %v", t)
	}

	if index, ok := info.columnToField[columnName]; ok {
		return index, nil
	}

	return -1, fmt.Errorf("column %s not found in type %v", columnName, t)
}

// getFieldType returns the field type for a field index
func (c *reflectCache) getFieldType(t reflect.Type, fieldIndex int) (reflect.Type, error) {
	info := c.getFieldInfo(t)
	if info == nil {
		return nil, fmt.Errorf("invalid type: %v", t)
	}

	if fieldType, ok := info.fieldTypes[fieldIndex]; ok {
		return fieldType, nil
	}

	return nil, fmt.Errorf("field index %d not found in type %v", fieldIndex, t)
}

// clearCache clears all cached reflection metadata (useful for testing)
func (c *reflectCache) clearCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tableNames = make(map[reflect.Type]string)
	c.fieldInfo = make(map[reflect.Type]*typeFieldInfo)
}
