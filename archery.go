// Package archery provides a simple and idiomatic Go API for Apache Arrow compute operations.
package archery

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// SortOrder specifies the order for sorting operations
type SortOrder int

const (
	// Ascending sort order (smallest to largest)
	Ascending SortOrder = iota
	// Descending sort order (largest to smallest)
	Descending
)

// ReleaseArray safely releases an array if it's not nil
func ReleaseArray(arr arrow.Array) {
	if arr != nil {
		arr.Release()
	}
}

// ReleaseRecord safely releases a record if it's not nil
func ReleaseRecord(rec arrow.Record) {
	if rec != nil {
		rec.Release()
	}
}

// ReplaceRecordColumn replaces a column in the record batch and returns a new record
func ReplaceRecordColumn(rec arrow.Record, colIndex int, newCol arrow.Array) arrow.Record {
	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		if i == colIndex {
			cols[i] = newCol
		} else {
			col := rec.Column(i)
			col.Retain() // Ensure it doesn't get released
			cols[i] = col
		}
	}

	// Create a new record with the replaced column
	newRecord := array.NewRecord(rec.Schema(), cols, rec.NumRows())

	// Release the column arrays (the record keeps a reference)
	for _, col := range cols {
		col.Release()
	}

	return newRecord
}

// ReplaceRecordColumnByName replaces a column in the record batch by name and returns a new record
func ReplaceRecordColumnByName(rec arrow.Record, colName string, newCol arrow.Array) (arrow.Record, error) {
	schema := rec.Schema()

	// Find the column index
	colIndex := -1
	for i, field := range schema.Fields() {
		if field.Name == colName {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return nil, fmt.Errorf("column not found: %s", colName)
	}

	return ReplaceRecordColumn(rec, colIndex, newCol), nil
}

// GetColumn returns a column from a record batch by name
func GetColumn(rec arrow.Record, name string) (arrow.Array, error) {
	schema := rec.Schema()
	for i, field := range schema.Fields() {
		if field.Name == name {
			col := rec.Column(i)
			col.Retain() // The caller is responsible for releasing
			return col, nil
		}
	}
	return nil, fmt.Errorf("column not found: %s", name)
}

// GetColumnIndex returns the index of a column in a record batch by name
func GetColumnIndex(rec arrow.Record, name string) (int, error) {
	schema := rec.Schema()
	for i, field := range schema.Fields() {
		if field.Name == name {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column not found: %s", name)
}

// ColumnNames returns the names of all columns in the record
func ColumnNames(rec arrow.Record) []string {
	schema := rec.Schema()
	names := make([]string, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		names[i] = schema.Field(i).Name
	}
	return names
}

// Internal utility functions

// callFunction is a helper to call Arrow compute functions
func callFunction(ctx context.Context, funcName string, args ...arrow.Array) (arrow.Array, error) {
	// Convert arrays to datums
	datums := make([]compute.Datum, len(args))
	for i, arr := range args {
		datums[i] = compute.NewDatum(arr)
	}

	// Call the function
	result, err := compute.CallFunction(ctx, funcName, nil, datums...)
	if err != nil {
		return nil, fmt.Errorf("failed to call %s: %w", funcName, err)
	}

	// Convert result back to array using the datumToArray function from arithmetic.go
	if result.Kind() == compute.KindArray {
		return result.(*compute.ArrayDatum).MakeArray(), nil
	} else if result.Kind() == compute.KindChunked {
		chunked := result.(*compute.ChunkedDatum).Value
		if len(chunked.Chunks()) > 0 {
			return chunked.Chunks()[0], nil
		}
	}
	return nil, nil
}
