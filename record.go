package archery

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// RecordWrapper provides methods to apply array operations to Arrow Records
type RecordWrapper struct {
	record arrow.Record
	mem    memory.Allocator
}

// NewRecordWrapper creates a new RecordWrapper for the given record
func NewRecordWrapper(record arrow.Record, mem memory.Allocator) *RecordWrapper {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	return &RecordWrapper{
		record: record,
		mem:    mem,
	}
}

// Record returns the underlying Arrow Record
func (rw *RecordWrapper) Record() arrow.Record {
	return rw.record
}

// ColumnNames returns the names of all columns in the record
func (rw *RecordWrapper) ColumnNames() []string {
	schema := rw.record.Schema()
	names := make([]string, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		names[i] = schema.Field(i).Name
	}
	return names
}

// Column returns the array for the specified column name
func (rw *RecordWrapper) Column(name string) (arrow.Array, error) {
	schema := rw.record.Schema()
	for i := 0; i < schema.NumFields(); i++ {
		if schema.Field(i).Name == name {
			return rw.record.Column(i), nil
		}
	}
	return nil, fmt.Errorf("column not found: %s", name)
}

// ColumnByIndex returns the array for the specified column index
func (rw *RecordWrapper) ColumnByIndex(i int) (arrow.Array, error) {
	if i < 0 || i >= int(rw.record.NumCols()) {
		return nil, fmt.Errorf("column index out of range: %d", i)
	}
	return rw.record.Column(i), nil
}

// FilterByMask filters a record using a boolean mask
// Returns a new record with only the rows where the mask is true
func (rw *RecordWrapper) FilterByMask(ctx context.Context, mask *array.Boolean) (arrow.Record, error) {
	if mask.Len() != int(rw.record.NumRows()) {
		return nil, fmt.Errorf("mask length (%d) does not match record row count (%d)", mask.Len(), rw.record.NumRows())
	}

	// Create a new schema with the same fields
	schema := rw.record.Schema()

	// Filter each column
	cols := make([]arrow.Array, rw.record.NumCols())
	for i := 0; i < int(rw.record.NumCols()); i++ {
		col := rw.record.Column(i)
		filtered, err := FilterByMask(ctx, col, mask)
		if err != nil {
			// Clean up already created arrays
			for j := 0; j < i; j++ {
				cols[j].Release()
			}
			return nil, fmt.Errorf("error filtering column %d: %w", i, err)
		}
		cols[i] = filtered
	}

	// Count true values in the mask
	trueCount := 0
	for i := 0; i < mask.Len(); i++ {
		if mask.Value(i) {
			trueCount++
		}
	}

	// Create a new record batch
	result := array.NewRecord(schema, cols, int64(trueCount))

	// Release the filtered columns (the record keeps a reference)
	for _, col := range cols {
		col.Release()
	}

	return result, nil
}

// FilterRows filters a record based on a predicate function
// The predicate function takes a row index and returns true if the row should be included
func (rw *RecordWrapper) FilterRows(ctx context.Context, predicate func(int) bool) (arrow.Record, error) {
	// Create a boolean mask
	mask := array.NewBooleanBuilder(rw.mem)
	defer mask.Release()

	for i := 0; i < int(rw.record.NumRows()); i++ {
		mask.Append(predicate(i))
	}

	boolMask := mask.NewBooleanArray()
	defer boolMask.Release()

	return rw.FilterByMask(ctx, boolMask)
}

// FilterRowsByColumn filters a record based on a condition applied to a specific column
// Returns a new record with only the rows where the condition is true
func (rw *RecordWrapper) FilterRowsByColumn(ctx context.Context, columnName string, condition func(arrow.Array, int) bool) (arrow.Record, error) {
	col, err := rw.Column(columnName)
	if err != nil {
		return nil, err
	}

	return rw.FilterRows(ctx, func(i int) bool {
		return condition(col, i)
	})
}

// Example conditions for FilterRowsByColumn
// These can be used with FilterRowsByColumn to filter records based on common conditions

// GreaterThan returns a condition function that checks if a value is greater than the specified value
func GreaterThan(value interface{}) func(arrow.Array, int) bool {
	return func(arr arrow.Array, i int) bool {
		if arr.IsNull(i) {
			return false
		}

		switch arr.DataType().ID() {
		case arrow.INT64:
			intArr := arr.(*array.Int64)
			intValue, ok := value.(int64)
			if !ok {
				// Try to convert
				if intValueFloat, okFloat := value.(float64); okFloat {
					intValue = int64(intValueFloat)
				} else if intValueInt, okInt := value.(int); okInt {
					intValue = int64(intValueInt)
				} else {
					return false
				}
			}
			return intArr.Value(i) > intValue
		case arrow.FLOAT64:
			floatArr := arr.(*array.Float64)
			floatValue, ok := value.(float64)
			if !ok {
				// Try to convert
				if floatValueInt, okInt := value.(int64); okInt {
					floatValue = float64(floatValueInt)
				} else if floatValueInt, okInt := value.(int); okInt {
					floatValue = float64(floatValueInt)
				} else {
					return false
				}
			}
			return floatArr.Value(i) > floatValue
		case arrow.STRING:
			strArr := arr.(*array.String)
			strValue, ok := value.(string)
			if !ok {
				return false
			}
			return strArr.Value(i) > strValue
		default:
			return false
		}
	}
}

// LessThan returns a condition function that checks if a value is less than the specified value
func LessThan(value interface{}) func(arrow.Array, int) bool {
	return func(arr arrow.Array, i int) bool {
		if arr.IsNull(i) {
			return false
		}

		switch arr.DataType().ID() {
		case arrow.INT64:
			intArr := arr.(*array.Int64)
			intValue, ok := value.(int64)
			if !ok {
				// Try to convert
				if intValueFloat, okFloat := value.(float64); okFloat {
					intValue = int64(intValueFloat)
				} else if intValueInt, okInt := value.(int); okInt {
					intValue = int64(intValueInt)
				} else {
					return false
				}
			}
			return intArr.Value(i) < intValue
		case arrow.FLOAT64:
			floatArr := arr.(*array.Float64)
			floatValue, ok := value.(float64)
			if !ok {
				// Try to convert
				if floatValueInt, okInt := value.(int64); okInt {
					floatValue = float64(floatValueInt)
				} else if floatValueInt, okInt := value.(int); okInt {
					floatValue = float64(floatValueInt)
				} else {
					return false
				}
			}
			return floatArr.Value(i) < floatValue
		case arrow.STRING:
			strArr := arr.(*array.String)
			strValue, ok := value.(string)
			if !ok {
				return false
			}
			return strArr.Value(i) < strValue
		default:
			return false
		}
	}
}

// Equal returns a condition function that checks if a value is equal to the specified value
func Equal(value interface{}) func(arrow.Array, int) bool {
	return func(arr arrow.Array, i int) bool {
		if arr.IsNull(i) {
			return false
		}

		switch arr.DataType().ID() {
		case arrow.INT64:
			intArr := arr.(*array.Int64)
			intValue, ok := value.(int64)
			if !ok {
				// Try to convert
				if intValueFloat, okFloat := value.(float64); okFloat {
					intValue = int64(intValueFloat)
				} else if intValueInt, okInt := value.(int); okInt {
					intValue = int64(intValueInt)
				} else {
					return false
				}
			}
			return intArr.Value(i) == intValue
		case arrow.FLOAT64:
			floatArr := arr.(*array.Float64)
			floatValue, ok := value.(float64)
			if !ok {
				// Try to convert
				if floatValueInt, okInt := value.(int64); okInt {
					floatValue = float64(floatValueInt)
				} else if floatValueInt, okInt := value.(int); okInt {
					floatValue = float64(floatValueInt)
				} else {
					return false
				}
			}
			return floatArr.Value(i) == floatValue
		case arrow.STRING:
			strArr := arr.(*array.String)
			strValue, ok := value.(string)
			if !ok {
				return false
			}
			return strArr.Value(i) == strValue
		case arrow.BOOL:
			boolArr := arr.(*array.Boolean)
			boolValue, ok := value.(bool)
			if !ok {
				return false
			}
			return boolArr.Value(i) == boolValue
		default:
			return false
		}
	}
}

// Between returns a condition function that checks if a value is between the specified lower and upper bounds (inclusive)
func Between(lower, upper interface{}) func(arrow.Array, int) bool {
	return func(arr arrow.Array, i int) bool {
		if arr.IsNull(i) {
			return false
		}

		switch arr.DataType().ID() {
		case arrow.INT64:
			intArr := arr.(*array.Int64)
			lowerInt, okLower := lower.(int64)
			if !okLower {
				// Try to convert
				if lowerFloat, okFloat := lower.(float64); okFloat {
					lowerInt = int64(lowerFloat)
				} else if lowerIntVal, okInt := lower.(int); okInt {
					lowerInt = int64(lowerIntVal)
				} else {
					return false
				}
			}

			upperInt, okUpper := upper.(int64)
			if !okUpper {
				// Try to convert
				if upperFloat, okFloat := upper.(float64); okFloat {
					upperInt = int64(upperFloat)
				} else if upperIntVal, okInt := upper.(int); okInt {
					upperInt = int64(upperIntVal)
				} else {
					return false
				}
			}

			val := intArr.Value(i)
			return val >= lowerInt && val <= upperInt
		case arrow.FLOAT64:
			floatArr := arr.(*array.Float64)
			lowerFloat, okLower := lower.(float64)
			if !okLower {
				// Try to convert
				if lowerInt, okInt := lower.(int64); okInt {
					lowerFloat = float64(lowerInt)
				} else if lowerIntVal, okInt := lower.(int); okInt {
					lowerFloat = float64(lowerIntVal)
				} else {
					return false
				}
			}

			upperFloat, okUpper := upper.(float64)
			if !okUpper {
				// Try to convert
				if upperInt, okInt := upper.(int64); okInt {
					upperFloat = float64(upperInt)
				} else if upperIntVal, okInt := upper.(int); okInt {
					upperFloat = float64(upperIntVal)
				} else {
					return false
				}
			}

			val := floatArr.Value(i)
			return val >= lowerFloat && val <= upperFloat
		case arrow.STRING:
			strArr := arr.(*array.String)
			lowerStr, okLower := lower.(string)
			if !okLower {
				return false
			}

			upperStr, okUpper := upper.(string)
			if !okUpper {
				return false
			}

			val := strArr.Value(i)
			return val >= lowerStr && val <= upperStr
		default:
			return false
		}
	}
}

// SortRecord sorts a record by the specified column
// Returns a new record with rows sorted according to the column values
func (rw *RecordWrapper) SortRecord(ctx context.Context, columnName string, order SortOrder) (arrow.Record, error) {
	// Get the column to sort by
	col, err := rw.Column(columnName)
	if err != nil {
		return nil, err
	}

	// Get the sort indices
	indices, err := SortIndicesWithOrder(ctx, col, order)
	if err != nil {
		return nil, err
	}
	defer indices.Release()

	// Create a new schema with the same fields
	schema := rw.record.Schema()

	// Take each column using the indices
	cols := make([]arrow.Array, rw.record.NumCols())
	for i := 0; i < int(rw.record.NumCols()); i++ {
		col := rw.record.Column(i)
		taken, err := TakeWithIndices(ctx, col, indices)
		if err != nil {
			// Clean up already created arrays
			for j := 0; j < i; j++ {
				cols[j].Release()
			}
			return nil, fmt.Errorf("error taking column %d: %w", i, err)
		}
		cols[i] = taken
	}

	// Create a new record batch
	result := array.NewRecord(schema, cols, rw.record.NumRows())

	// Release the taken columns (the record keeps a reference)
	for _, col := range cols {
		col.Release()
	}

	return result, nil
}

// AggregateColumn applies an aggregation function to a column
// The aggregation function should take an array and return a scalar value
func (rw *RecordWrapper) AggregateColumn(ctx context.Context, columnName string,
	aggregator func(context.Context, arrow.Array) (interface{}, error)) (interface{}, error) {

	col, err := rw.Column(columnName)
	if err != nil {
		return nil, err
	}

	return aggregator(ctx, col)
}

// Common aggregation functions for use with AggregateColumn

// SumAggregator returns an aggregator function that calculates the sum of a column
func SumAggregator() func(context.Context, arrow.Array) (interface{}, error) {
	return func(ctx context.Context, arr arrow.Array) (interface{}, error) {
		result, err := Sum(ctx, arr)
		if err != nil {
			return nil, err
		}

		switch result.DataType().ID() {
		case arrow.INT64:
			return result.(*scalar.Int64).Value, nil
		case arrow.FLOAT64:
			return result.(*scalar.Float64).Value, nil
		default:
			return nil, fmt.Errorf("unsupported data type for sum: %s", result.DataType().Name())
		}
	}
}

// MeanAggregator returns an aggregator function that calculates the mean of a column
func MeanAggregator() func(context.Context, arrow.Array) (interface{}, error) {
	return func(ctx context.Context, arr arrow.Array) (interface{}, error) {
		result, err := Mean(ctx, arr)
		if err != nil {
			return nil, err
		}

		return result.(*scalar.Float64).Value, nil
	}
}

// MinAggregator returns an aggregator function that finds the minimum value in a column
func MinAggregator() func(context.Context, arrow.Array) (interface{}, error) {
	return func(ctx context.Context, arr arrow.Array) (interface{}, error) {
		result, err := Min(ctx, arr)
		if err != nil {
			return nil, err
		}

		switch result.DataType().ID() {
		case arrow.INT64:
			return result.(*scalar.Int64).Value, nil
		case arrow.FLOAT64:
			return result.(*scalar.Float64).Value, nil
		case arrow.STRING:
			return result.(*scalar.String).Value, nil
		default:
			return nil, fmt.Errorf("unsupported data type for min: %s", result.DataType().Name())
		}
	}
}

// MaxAggregator returns an aggregator function that finds the maximum value in a column
func MaxAggregator() func(context.Context, arrow.Array) (interface{}, error) {
	return func(ctx context.Context, arr arrow.Array) (interface{}, error) {
		result, err := Max(ctx, arr)
		if err != nil {
			return nil, err
		}

		switch result.DataType().ID() {
		case arrow.INT64:
			return result.(*scalar.Int64).Value, nil
		case arrow.FLOAT64:
			return result.(*scalar.Float64).Value, nil
		case arrow.STRING:
			return result.(*scalar.String).Value, nil
		default:
			return nil, fmt.Errorf("unsupported data type for max: %s", result.DataType().Name())
		}
	}
}

// GroupBy groups a record by one or more columns and applies aggregation functions to other columns
// Returns a new record with one row per group and the aggregated values
type GroupByResult struct {
	Keys       map[string]arrow.Array
	Aggregates map[string]arrow.Array
}

// GroupBy groups a record by the specified key columns and applies aggregation functions to the value columns
func (rw *RecordWrapper) GroupBy(ctx context.Context, keyColumns []string,
	aggregations map[string]func(context.Context, arrow.Array) (interface{}, error)) (*GroupByResult, error) {

	// This is a simplified implementation that doesn't handle all edge cases
	// A full implementation would be more complex

	// Get the key columns
	keyArrays := make([]arrow.Array, len(keyColumns))
	for i, colName := range keyColumns {
		col, err := rw.Column(colName)
		if err != nil {
			return nil, err
		}
		keyArrays[i] = col
	}

	// Create a map of group keys to row indices
	type GroupKey struct {
		values []interface{}
	}

	groupMap := make(map[string][]int)

	// For each row, create a group key and add the row index to the group
	for rowIdx := 0; rowIdx < int(rw.record.NumRows()); rowIdx++ {
		// Create a key for this row
		keyValues := make([]string, len(keyArrays))

		for i, keyArray := range keyArrays {
			if keyArray.IsNull(rowIdx) {
				keyValues[i] = "NULL"
				continue
			}

			switch keyArray.DataType().ID() {
			case arrow.INT64:
				keyValues[i] = fmt.Sprintf("%d", keyArray.(*array.Int64).Value(rowIdx))
			case arrow.FLOAT64:
				keyValues[i] = fmt.Sprintf("%f", keyArray.(*array.Float64).Value(rowIdx))
			case arrow.STRING:
				keyValues[i] = keyArray.(*array.String).Value(rowIdx)
			case arrow.BOOL:
				keyValues[i] = fmt.Sprintf("%t", keyArray.(*array.Boolean).Value(rowIdx))
			default:
				return nil, fmt.Errorf("unsupported key column data type: %s", keyArray.DataType().Name())
			}
		}

		// Create a string key
		key := fmt.Sprintf("%v", keyValues)

		// Add this row to the group
		groupMap[key] = append(groupMap[key], rowIdx)
	}

	// Now we have groups of row indices
	// For each group, we need to apply the aggregation functions

	// First, create builders for the key columns
	keyBuilders := make(map[string]array.Builder)
	for _, colName := range keyColumns {
		col, err := rw.Column(colName)
		if err != nil {
			return nil, err
		}

		switch col.DataType().ID() {
		case arrow.INT64:
			keyBuilders[colName] = array.NewInt64Builder(rw.mem)
		case arrow.FLOAT64:
			keyBuilders[colName] = array.NewFloat64Builder(rw.mem)
		case arrow.STRING:
			keyBuilders[colName] = array.NewStringBuilder(rw.mem)
		case arrow.BOOL:
			keyBuilders[colName] = array.NewBooleanBuilder(rw.mem)
		default:
			return nil, fmt.Errorf("unsupported key column data type: %s", col.DataType().Name())
		}
	}

	// Create builders for the aggregation columns - always use Float64 for aggregation results
	// since most aggregation functions return floating point values
	aggBuilders := make(map[string]array.Builder)
	for colName := range aggregations {
		aggBuilders[colName] = array.NewFloat64Builder(rw.mem)
	}

	// For each group, apply the aggregations
	for _, rowIndices := range groupMap {
		// For each key column, add the value from the first row in the group
		firstRowIdx := rowIndices[0]

		for _, colName := range keyColumns {
			col, err := rw.Column(colName)
			if err != nil {
				return nil, err
			}

			builder := keyBuilders[colName]

			if col.IsNull(firstRowIdx) {
				builder.AppendNull()
				continue
			}

			switch col.DataType().ID() {
			case arrow.INT64:
				builder.(*array.Int64Builder).Append(col.(*array.Int64).Value(firstRowIdx))
			case arrow.FLOAT64:
				builder.(*array.Float64Builder).Append(col.(*array.Float64).Value(firstRowIdx))
			case arrow.STRING:
				builder.(*array.StringBuilder).Append(col.(*array.String).Value(firstRowIdx))
			case arrow.BOOL:
				builder.(*array.BooleanBuilder).Append(col.(*array.Boolean).Value(firstRowIdx))
			}
		}

		// For each aggregation column, create a slice of the column for this group
		// and apply the aggregation function
		for colName, aggFunc := range aggregations {
			col, err := rw.Column(colName)
			if err != nil {
				return nil, err
			}

			// Create a slice of the column for this group
			sliceBuilder := array.NewBuilder(rw.mem, col.DataType())
			defer sliceBuilder.Release()

			for _, rowIdx := range rowIndices {
				if col.IsNull(rowIdx) {
					sliceBuilder.AppendNull()
					continue
				}

				switch col.DataType().ID() {
				case arrow.INT64:
					sliceBuilder.(*array.Int64Builder).Append(col.(*array.Int64).Value(rowIdx))
				case arrow.FLOAT64:
					sliceBuilder.(*array.Float64Builder).Append(col.(*array.Float64).Value(rowIdx))
				case arrow.STRING:
					sliceBuilder.(*array.StringBuilder).Append(col.(*array.String).Value(rowIdx))
				case arrow.BOOL:
					sliceBuilder.(*array.BooleanBuilder).Append(col.(*array.Boolean).Value(rowIdx))
				}
			}

			slice := sliceBuilder.NewArray()
			defer slice.Release()

			// Apply the aggregation function
			aggResult, err := aggFunc(ctx, slice)
			if err != nil {
				return nil, err
			}

			// Add the result to the builder - always convert to float64 for consistency
			builder := aggBuilders[colName]
			floatBuilder := builder.(*array.Float64Builder)

			switch v := aggResult.(type) {
			case int64:
				floatBuilder.Append(float64(v))
			case float64:
				floatBuilder.Append(v)
			case string:
				// Try to parse as float
				var f float64
				_, err := fmt.Sscanf(v, "%f", &f)
				if err != nil {
					return nil, fmt.Errorf("cannot convert string to float64: %s", v)
				}
				floatBuilder.Append(f)
			default:
				return nil, fmt.Errorf("unsupported aggregation result type: %T", aggResult)
			}
		}
	}

	// Build the final arrays
	keyArraysResult := make(map[string]arrow.Array)
	for colName, builder := range keyBuilders {
		keyArraysResult[colName] = builder.NewArray()
		builder.Release()
	}

	aggArraysResult := make(map[string]arrow.Array)
	for colName, builder := range aggBuilders {
		aggArraysResult[colName] = builder.NewArray()
		builder.Release()
	}

	return &GroupByResult{
		Keys:       keyArraysResult,
		Aggregates: aggArraysResult,
	}, nil
}

// ToRecord converts a GroupByResult to a Record
func (gr *GroupByResult) ToRecord(mem memory.Allocator) arrow.Record {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	// Create a schema
	fields := make([]arrow.Field, 0, len(gr.Keys)+len(gr.Aggregates))

	// Add key fields
	for name, arr := range gr.Keys {
		fields = append(fields, arrow.Field{Name: name, Type: arr.DataType()})
	}

	// Add aggregate fields
	for name, arr := range gr.Aggregates {
		fields = append(fields, arrow.Field{Name: name + "_agg", Type: arr.DataType()})
	}

	schema := arrow.NewSchema(fields, nil)

	// Create arrays
	arrays := make([]arrow.Array, 0, len(gr.Keys)+len(gr.Aggregates))

	// Add key arrays
	for _, field := range fields {
		if arr, ok := gr.Keys[field.Name]; ok {
			arrays = append(arrays, arr)
		} else if arr, ok := gr.Aggregates[field.Name[:len(field.Name)-4]]; ok {
			arrays = append(arrays, arr)
		}
	}

	// Get the number of rows
	var numRows int64
	if len(arrays) > 0 {
		numRows = int64(arrays[0].Len())
	}

	// Create the record
	return array.NewRecord(schema, arrays, numRows)
}

// Release releases all arrays in the GroupByResult
func (gr *GroupByResult) Release() {
	for _, arr := range gr.Keys {
		arr.Release()
	}

	for _, arr := range gr.Aggregates {
		arr.Release()
	}
}
