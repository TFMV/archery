package archery

import (
	"context"
	"sort"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/arrow/scalar"
)

// SortOrder represents the order in which elements should be sorted.
type SortOrder int

const (
	// Ascending order (smallest to largest)
	Ascending SortOrder = iota
	// Descending order (largest to smallest)
	Descending
)

// SortIndicesOptions implements compute.FunctionOptions for the sort_indices function
type SortIndicesOptions struct {
	Descending bool
}

// TypeName implements the compute.FunctionOptions interface
func (o *SortIndicesOptions) TypeName() string {
	return "sort_indices"
}

// SortIndicesWithOrder returns the indices that would sort the array according to the specified order.
// Returns an Int64Array containing the indices and any error encountered.
func SortIndicesWithOrder(ctx context.Context, arr arrow.Array, order SortOrder) (*array.Int64, error) {
	// Since the sort_indices function is not available, we'll implement it manually
	length := arr.Len()
	indices := make([]int64, length)

	// Initialize indices
	for i := 0; i < length; i++ {
		indices[i] = int64(i)
	}

	// Sort indices based on array values
	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if int64Arr.IsNull(int(indices[i])) {
				return !int64Arr.IsNull(int(indices[j]))
			}
			if int64Arr.IsNull(int(indices[j])) {
				return false
			}

			// Sort based on order
			if order == Ascending {
				return int64Arr.Value(int(indices[i])) < int64Arr.Value(int(indices[j]))
			}
			return int64Arr.Value(int(indices[i])) > int64Arr.Value(int(indices[j]))
		})
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if float64Arr.IsNull(int(indices[i])) {
				return !float64Arr.IsNull(int(indices[j]))
			}
			if float64Arr.IsNull(int(indices[j])) {
				return false
			}

			// Sort based on order
			if order == Ascending {
				return float64Arr.Value(int(indices[i])) < float64Arr.Value(int(indices[j]))
			}
			return float64Arr.Value(int(indices[i])) > float64Arr.Value(int(indices[j]))
		})
	case arrow.STRING:
		stringArr := arr.(*array.String)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if stringArr.IsNull(int(indices[i])) {
				return !stringArr.IsNull(int(indices[j]))
			}
			if stringArr.IsNull(int(indices[j])) {
				return false
			}

			// Sort based on order
			if order == Ascending {
				return stringArr.Value(int(indices[i])) < stringArr.Value(int(indices[j]))
			}
			return stringArr.Value(int(indices[i])) > stringArr.Value(int(indices[j]))
		})
	default:
		return nil, arrow.ErrNotImplemented
	}

	// Create an Int64Array from the indices
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	builder.AppendValues(indices, nil)
	return builder.NewInt64Array(), nil
}

// Sort returns a new array with the elements sorted.
// Returns a new array of the same type as the input and any error encountered.
func Sort(ctx context.Context, arr arrow.Array, order SortOrder) (arrow.Array, error) {
	// First get the sorted indices
	indices, err := SortIndicesWithOrder(ctx, arr, order)
	if err != nil {
		return nil, err
	}
	defer indices.Release()

	// Use the indices to create a new sorted array
	return TakeWithIndices(ctx, arr, indices)
}

// TakeWithIndices returns a new array with elements taken from the input array at the specified indices.
// Returns a new array of the same type as the input and any error encountered.
func TakeWithIndices(ctx context.Context, arr arrow.Array, indices *array.Int64) (arrow.Array, error) {
	// Since the take function is not available, we'll implement it manually
	length := indices.Len()

	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		defer builder.Release()

		for i := 0; i < length; i++ {
			idx := int(indices.Value(i))
			if idx < 0 || idx >= int64Arr.Len() {
				return nil, arrow.ErrIndex
			}

			if int64Arr.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(int64Arr.Value(idx))
			}
		}

		return builder.NewInt64Array(), nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer builder.Release()

		for i := 0; i < length; i++ {
			idx := int(indices.Value(i))
			if idx < 0 || idx >= float64Arr.Len() {
				return nil, arrow.ErrIndex
			}

			if float64Arr.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(float64Arr.Value(idx))
			}
		}

		return builder.NewFloat64Array(), nil
	case arrow.STRING:
		stringArr := arr.(*array.String)
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		defer builder.Release()

		for i := 0; i < length; i++ {
			idx := int(indices.Value(i))
			if idx < 0 || idx >= stringArr.Len() {
				return nil, arrow.ErrIndex
			}

			if stringArr.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(stringArr.Value(idx))
			}
		}

		return builder.NewStringArray(), nil
	default:
		return nil, arrow.ErrNotImplemented
	}
}

// NthElement returns the nth element of the sorted array.
// Returns the scalar value and any error encountered.
func NthElement(ctx context.Context, arr arrow.Array, n int64, order SortOrder) (scalar.Scalar, error) {
	// First get the sorted indices
	indices, err := SortIndicesWithOrder(ctx, arr, order)
	if err != nil {
		return nil, err
	}
	defer indices.Release()

	// Check if n is within bounds
	if n < 0 || n >= int64(indices.Len()) {
		return nil, arrow.ErrIndex
	}

	// Get the index of the nth element
	idx := int(indices.Value(int(n)))

	// Extract the scalar value
	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		if int64Arr.IsNull(idx) {
			return scalar.NewInt64Scalar(0), nil
		}
		return scalar.NewInt64Scalar(int64Arr.Value(idx)), nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		if float64Arr.IsNull(idx) {
			return scalar.NewFloat64Scalar(0), nil
		}
		return scalar.NewFloat64Scalar(float64Arr.Value(idx)), nil
	case arrow.STRING:
		stringArr := arr.(*array.String)
		if stringArr.IsNull(idx) {
			return scalar.NewStringScalar(""), nil
		}
		return scalar.NewStringScalar(stringArr.Value(idx)), nil
	default:
		return nil, arrow.ErrNotImplemented
	}
}

// Rank returns an array with the rank of each element in the input array.
// Returns a new Int64Array with the ranks and any error encountered.
func Rank(ctx context.Context, arr arrow.Array, order SortOrder) (arrow.Array, error) {
	// First get the sorted indices
	indices, err := SortIndicesWithOrder(ctx, arr, order)
	if err != nil {
		return nil, err
	}
	defer indices.Release()

	// Create a new array to hold the ranks
	length := arr.Len()
	ranks := make([]int64, length)

	// Assign ranks based on the sorted indices
	for i := 0; i < indices.Len(); i++ {
		idx := int(indices.Value(i))
		ranks[idx] = int64(i)
	}

	// Create an Int64Array from the ranks
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	builder.AppendValues(ranks, nil)
	return builder.NewInt64Array(), nil
}

// UniqueValues returns a new array with duplicate elements removed.
// Returns a new array of the same type as the input and any error encountered.
func UniqueValues(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	// Since the unique function is not available, we'll implement it manually
	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		seen := make(map[int64]bool)
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		defer builder.Release()

		for i := 0; i < int64Arr.Len(); i++ {
			if int64Arr.IsNull(i) {
				// Only append the first null value
				if !seen[0] {
					builder.AppendNull()
					seen[0] = true
				}
			} else {
				val := int64Arr.Value(i)
				if !seen[val] {
					builder.Append(val)
					seen[val] = true
				}
			}
		}

		return builder.NewInt64Array(), nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		seen := make(map[float64]bool)
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer builder.Release()

		for i := 0; i < float64Arr.Len(); i++ {
			if float64Arr.IsNull(i) {
				// Only append the first null value
				if !seen[0] {
					builder.AppendNull()
					seen[0] = true
				}
			} else {
				val := float64Arr.Value(i)
				if !seen[val] {
					builder.Append(val)
					seen[val] = true
				}
			}
		}

		return builder.NewFloat64Array(), nil
	case arrow.STRING:
		stringArr := arr.(*array.String)
		seen := make(map[string]bool)
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		defer builder.Release()

		for i := 0; i < stringArr.Len(); i++ {
			if stringArr.IsNull(i) {
				// Only append the first null value
				if !seen[""] {
					builder.AppendNull()
					seen[""] = true
				}
			} else {
				val := stringArr.Value(i)
				if !seen[val] {
					builder.Append(val)
					seen[val] = true
				}
			}
		}

		return builder.NewStringArray(), nil
	default:
		return nil, arrow.ErrNotImplemented
	}
}
