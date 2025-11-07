package orm

import (
	"fmt"
	"reflect"
	"strings"
)

// convertRowToModel converts a database row (map[string]interface{}) to a struct model
// using reflection with cached reflection metadata for better performance.
// The target parameter should be a pointer to the model struct.
func convertRowToModel(row map[string]interface{}, target interface{}) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr {
		return fmt.Errorf("target must be a pointer")
	}

	targetElem := targetValue.Elem()
	targetType := targetElem.Type()

	// If target is a pointer to a struct, dereference it
	if targetElem.Kind() == reflect.Ptr {
		if targetElem.IsNil() {
			targetElem.Set(reflect.New(targetElem.Type().Elem()))
		}
		targetElem = targetElem.Elem()
		targetType = targetElem.Type()
	}

	if targetElem.Kind() != reflect.Struct {
		return fmt.Errorf("target must be a struct or pointer to struct")
	}

	// Get cached field info
	fieldInfo := globalCache.getFieldInfo(targetType)
	if fieldInfo == nil {
		return fmt.Errorf("failed to get field info for type %v", targetType)
	}

	// Set field values from row using cached field info
	for dbKey, dbValue := range row {
		fieldIndex, exists := fieldInfo.columnToField[dbKey]
		if !exists {
			continue // Skip unknown fields
		}

		fieldValue := targetElem.Field(fieldIndex)
		fieldType := fieldInfo.fieldTypes[fieldIndex]

		if err := setFieldValue(fieldValue, fieldType, dbValue); err != nil {
			// Return error instead of silently ignoring it
			return fmt.Errorf("failed to set field %s (column %s): %w", fieldInfo.fieldNames[fieldIndex], dbKey, err)
		}
	}

	return nil
}

// setFieldValue sets a field value with type conversion
func setFieldValue(fieldValue reflect.Value, fieldType reflect.Type, dbValue interface{}) error {
	if !fieldValue.CanSet() {
		return fmt.Errorf("field cannot be set")
	}

	if dbValue == nil {
		// Set zero value for nil
		if fieldType.Kind() == reflect.Ptr {
			fieldValue.Set(reflect.Zero(fieldType))
		} else {
			// For non-pointer fields, set to zero value
			fieldValue.Set(reflect.Zero(fieldType))
		}
		return nil
	}

	dbValueType := reflect.TypeOf(dbValue)
	fieldKind := fieldType.Kind()

	// Handle pointer types
	if fieldKind == reflect.Ptr {
		elemType := fieldType.Elem()

		// For string pointers, only set if non-empty
		if elemType.Kind() == reflect.String {
			if strVal, ok := convertToString(dbValue); ok {
				if strVal != "" {
					if fieldValue.IsNil() {
						fieldValue.Set(reflect.New(elemType))
					}
					fieldValue.Elem().SetString(strVal)
					return nil
				} else {
					// Empty string - set to nil
					fieldValue.Set(reflect.Zero(fieldType))
					return nil
				}
			}
		}

		// For other pointer types, set the value
		if fieldValue.IsNil() {
			fieldValue.Set(reflect.New(elemType))
		}
		return setFieldValue(fieldValue.Elem(), elemType, dbValue)
	}

	// Direct type matches
	if dbValueType.AssignableTo(fieldType) {
		fieldValue.Set(reflect.ValueOf(dbValue))
		return nil
	}

	// Type conversions
	switch fieldKind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if intVal, ok := convertToInt(dbValue); ok {
			fieldValue.SetInt(int64(intVal))
			return nil
		}

	case reflect.String:
		if strVal, ok := convertToString(dbValue); ok {
			fieldValue.SetString(strVal)
			return nil
		}

	case reflect.Bool:
		if boolVal, ok := convertToBool(dbValue); ok {
			fieldValue.SetBool(boolVal)
			return nil
		}

	case reflect.Float32, reflect.Float64:
		if floatVal, ok := convertToFloat(dbValue); ok {
			fieldValue.SetFloat(floatVal)
			return nil
		}
	}

	return fmt.Errorf("cannot convert %T to %s", dbValue, fieldType)
}

// convertToInt converts various types to int
func convertToInt(val interface{}) (int, bool) {
	switch v := val.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float32:
		return int(v), true
	case float64:
		return int(v), true
	case uint:
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		return int(v), true
	case uint64:
		return int(v), true
	default:
		return 0, false
	}
}

// convertToString converts various types to string
func convertToString(val interface{}) (string, bool) {
	switch v := val.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case int:
		return fmt.Sprintf("%d", v), true
	case int8:
		return fmt.Sprintf("%d", v), true
	case int16:
		return fmt.Sprintf("%d", v), true
	case int32:
		return fmt.Sprintf("%d", v), true
	case int64:
		return fmt.Sprintf("%d", v), true
	case uint:
		return fmt.Sprintf("%d", v), true
	case uint8:
		return fmt.Sprintf("%d", v), true
	case uint16:
		return fmt.Sprintf("%d", v), true
	case uint32:
		return fmt.Sprintf("%d", v), true
	case uint64:
		return fmt.Sprintf("%d", v), true
	case float32:
		return fmt.Sprintf("%g", v), true
	case float64:
		return fmt.Sprintf("%g", v), true
	case bool:
		if v {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}

// convertToBool converts various types to bool
func convertToBool(val interface{}) (bool, bool) {
	switch v := val.(type) {
	case bool:
		return v, true
	case int:
		return v != 0, true
	case int8:
		return v != 0, true
	case int16:
		return v != 0, true
	case int32:
		return v != 0, true
	case int64:
		return v != 0, true
	case uint:
		return v != 0, true
	case uint8:
		return v != 0, true
	case uint16:
		return v != 0, true
	case uint32:
		return v != 0, true
	case uint64:
		return v != 0, true
	case float32:
		return v != 0, true
	case float64:
		return v != 0, true
	case string:
		// Handle string representations of booleans
		switch v {
		case "true", "1", "yes", "on":
			return true, true
		case "false", "0", "no", "off", "":
			return false, true
		}
		return false, false
	default:
		return false, false
	}
}

// convertToFloat converts various types to float64
func convertToFloat(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

// toSnakeCase converts PascalCase to snake_case
func toSnakeCase(s string) string {
	if len(s) == 0 {
		return s
	}

	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// ConvertRowToModel is the public API for converting a database row to a model
// target should be a pointer to the model struct
func ConvertRowToModel(row map[string]interface{}, target interface{}) error {
	return convertRowToModel(row, target)
}
