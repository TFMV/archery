// Package arrow provides utility functions for working with the Apache Arrow compute package.
package archery

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/compute"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// CreateInt64Array creates an Int64Array with the given values and validity.
func CreateInt64Array(mem memory.Allocator, values []int64, validity []bool) *array.Int64 {
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	if validity == nil {
		builder.AppendValues(values, nil)
	} else {
		builder.AppendValues(values, validity)
	}

	return builder.NewInt64Array()
}

// CreateFloat64Array creates a Float64Array with the given values and validity.
func CreateFloat64Array(mem memory.Allocator, values []float64, validity []bool) *array.Float64 {
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	if validity == nil {
		builder.AppendValues(values, nil)
	} else {
		builder.AppendValues(values, validity)
	}

	return builder.NewFloat64Array()
}

// CreateBooleanArray creates a BooleanArray with the given values and validity.
func CreateBooleanArray(mem memory.Allocator, values []bool, validity []bool) *array.Boolean {
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	if validity == nil {
		builder.AppendValues(values, nil)
	} else {
		builder.AppendValues(values, validity)
	}

	return builder.NewBooleanArray()
}

// CreateStringArray creates a StringArray with the given values and validity.
func CreateStringArray(mem memory.Allocator, values []string, validity []bool) *array.String {
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	if validity == nil {
		builder.AppendValues(values, nil)
	} else {
		builder.AppendValues(values, validity)
	}

	return builder.NewStringArray()
}

// ExtractInt64Values extracts the values from an Int64Array into a slice.
// Null values are represented as nil in the returned slice.
func ExtractInt64Values(arr *array.Int64) []*int64 {
	result := make([]*int64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			result[i] = nil
		} else {
			val := arr.Value(i)
			result[i] = &val
		}
	}
	return result
}

// ExtractFloat64Values extracts the values from a Float64Array into a slice.
// Null values are represented as nil in the returned slice.
func ExtractFloat64Values(arr *array.Float64) []*float64 {
	result := make([]*float64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			result[i] = nil
		} else {
			val := arr.Value(i)
			result[i] = &val
		}
	}
	return result
}

// ExtractBooleanValues extracts the values from a BooleanArray into a slice.
// Null values are represented as nil in the returned slice.
func ExtractBooleanValues(arr *array.Boolean) []*bool {
	result := make([]*bool, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			result[i] = nil
		} else {
			val := arr.Value(i)
			result[i] = &val
		}
	}
	return result
}

// ExtractStringValues extracts the values from a StringArray into a slice.
// Null values are represented as nil in the returned slice.
func ExtractStringValues(arr *array.String) []*string {
	result := make([]*string, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			result[i] = nil
		} else {
			val := arr.Value(i)
			result[i] = &val
		}
	}
	return result
}

// DatumToArray converts a compute.Datum to an arrow.Array.
// Returns nil if the conversion is not possible.
func DatumToArray(datum compute.Datum) arrow.Array {
	if datum == nil {
		return nil
	}

	switch datum.Kind() {
	case compute.KindArray:
		arr := datum.(*compute.ArrayDatum).Value
		return array.MakeFromData(arr)
	case compute.KindChunked:
		// For simplicity, we only return the first chunk
		chunked := datum.(*compute.ChunkedDatum).Value
		if len(chunked.Chunks()) > 0 {
			return chunked.Chunks()[0]
		}
	}
	return nil
}

// DatumToInt64Array converts a compute.Datum to an *array.Int64.
// Returns nil if the conversion is not possible.
func DatumToInt64Array(datum compute.Datum) *array.Int64 {
	arr := DatumToArray(datum)
	if arr == nil {
		return nil
	}

	if int64Arr, ok := arr.(*array.Int64); ok {
		return int64Arr
	}
	return nil
}

// DatumToFloat64Array converts a compute.Datum to an *array.Float64.
// Returns nil if the conversion is not possible.
func DatumToFloat64Array(datum compute.Datum) *array.Float64 {
	arr := DatumToArray(datum)
	if arr == nil {
		return nil
	}

	if float64Arr, ok := arr.(*array.Float64); ok {
		return float64Arr
	}
	return nil
}

// DatumToBooleanArray converts a compute.Datum to an *array.Boolean.
// Returns nil if the conversion is not possible.
func DatumToBooleanArray(datum compute.Datum) *array.Boolean {
	arr := DatumToArray(datum)
	if arr == nil {
		return nil
	}

	if boolArr, ok := arr.(*array.Boolean); ok {
		return boolArr
	}
	return nil
}

// DatumToStringArray converts a compute.Datum to an *array.String.
// Returns nil if the conversion is not possible.
func DatumToStringArray(datum compute.Datum) *array.String {
	arr := DatumToArray(datum)
	if arr == nil {
		return nil
	}

	if strArr, ok := arr.(*array.String); ok {
		return strArr
	}
	return nil
}
