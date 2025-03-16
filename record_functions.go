package archery

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FilterRecordByMask filters a record using a boolean mask
// Returns a new record with only the rows where the mask is true
func FilterRecordByMask(ctx context.Context, record arrow.Record, mask *array.Boolean, mem memory.Allocator) (arrow.Record, error) {
	wrapper := NewRecordWrapper(record, mem)
	return wrapper.FilterByMask(ctx, mask)
}

// FilterRecordRows filters a record based on a predicate function
// The predicate function takes a row index and returns true if the row should be included
func FilterRecordRows(ctx context.Context, record arrow.Record, predicate func(int) bool, mem memory.Allocator) (arrow.Record, error) {
	wrapper := NewRecordWrapper(record, mem)
	return wrapper.FilterRows(ctx, predicate)
}

// FilterRecordByColumn filters a record based on a condition applied to a specific column
// Returns a new record with only the rows where the condition is true
func FilterRecordByColumn(ctx context.Context, record arrow.Record, columnName string,
	condition func(arrow.Array, int) bool, mem memory.Allocator) (arrow.Record, error) {
	wrapper := NewRecordWrapper(record, mem)
	return wrapper.FilterRowsByColumn(ctx, columnName, condition)
}

// SortRecordByColumn sorts a record by the specified column
// Returns a new record with rows sorted according to the column values
func SortRecordByColumn(ctx context.Context, record arrow.Record, columnName string,
	order SortOrder, mem memory.Allocator) (arrow.Record, error) {
	wrapper := NewRecordWrapper(record, mem)
	return wrapper.SortRecord(ctx, columnName, order)
}

// AggregateRecordColumn applies an aggregation function to a column in a record
// The aggregation function should take an array and return a scalar value
func AggregateRecordColumn(ctx context.Context, record arrow.Record, columnName string,
	aggregator func(context.Context, arrow.Array) (interface{}, error), mem memory.Allocator) (interface{}, error) {
	wrapper := NewRecordWrapper(record, mem)
	return wrapper.AggregateColumn(ctx, columnName, aggregator)
}

// GroupByRecord groups a record by the specified key columns and applies aggregation functions to the value columns
func GroupByRecord(ctx context.Context, record arrow.Record, keyColumns []string,
	aggregations map[string]func(context.Context, arrow.Array) (interface{}, error), mem memory.Allocator) (*GroupByResult, error) {
	wrapper := NewRecordWrapper(record, mem)
	return wrapper.GroupBy(ctx, keyColumns, aggregations)
}

// FilterRecordGreaterThan filters a record to include only rows where the specified column is greater than the value
func FilterRecordGreaterThan(ctx context.Context, record arrow.Record, columnName string,
	value interface{}, mem memory.Allocator) (arrow.Record, error) {
	return FilterRecordByColumn(ctx, record, columnName, GreaterThan(value), mem)
}

// FilterRecordLessThan filters a record to include only rows where the specified column is less than the value
func FilterRecordLessThan(ctx context.Context, record arrow.Record, columnName string,
	value interface{}, mem memory.Allocator) (arrow.Record, error) {
	return FilterRecordByColumn(ctx, record, columnName, LessThan(value), mem)
}

// FilterRecordEqual filters a record to include only rows where the specified column equals the value
func FilterRecordEqual(ctx context.Context, record arrow.Record, columnName string,
	value interface{}, mem memory.Allocator) (arrow.Record, error) {
	return FilterRecordByColumn(ctx, record, columnName, Equal(value), mem)
}

// FilterRecordBetween filters a record to include only rows where the specified column is between lower and upper (inclusive)
func FilterRecordBetween(ctx context.Context, record arrow.Record, columnName string,
	lower, upper interface{}, mem memory.Allocator) (arrow.Record, error) {
	return FilterRecordByColumn(ctx, record, columnName, Between(lower, upper), mem)
}

// SumRecordColumn calculates the sum of values in the specified column
func SumRecordColumn(ctx context.Context, record arrow.Record, columnName string, mem memory.Allocator) (interface{}, error) {
	return AggregateRecordColumn(ctx, record, columnName, SumAggregator(), mem)
}

// MeanRecordColumn calculates the mean of values in the specified column
func MeanRecordColumn(ctx context.Context, record arrow.Record, columnName string, mem memory.Allocator) (float64, error) {
	result, err := AggregateRecordColumn(ctx, record, columnName, MeanAggregator(), mem)
	if err != nil {
		return 0, err
	}
	return result.(float64), nil
}

// MinRecordColumn finds the minimum value in the specified column
func MinRecordColumn(ctx context.Context, record arrow.Record, columnName string, mem memory.Allocator) (interface{}, error) {
	return AggregateRecordColumn(ctx, record, columnName, MinAggregator(), mem)
}

// MaxRecordColumn finds the maximum value in the specified column
func MaxRecordColumn(ctx context.Context, record arrow.Record, columnName string, mem memory.Allocator) (interface{}, error) {
	return AggregateRecordColumn(ctx, record, columnName, MaxAggregator(), mem)
}

// GetRecordColumn returns the array for the specified column name
func GetRecordColumn(record arrow.Record, columnName string) (arrow.Array, error) {
	schema := record.Schema()
	for i := 0; i < schema.NumFields(); i++ {
		if schema.Field(i).Name == columnName {
			return record.Column(i), nil
		}
	}
	return nil, fmt.Errorf("column not found: %s", columnName)
}
