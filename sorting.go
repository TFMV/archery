package archery

import (
	"context"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ARRAY OPERATIONS

// Sort returns a sorted copy of the input array
func Sort(ctx context.Context, input arrow.Array, order SortOrder) (arrow.Array, error) {
	// Get sort indices
	indices, err := SortIndices(ctx, input, order)
	if err != nil {
		return nil, err
	}
	defer indices.Release()

	// Use take to reorder the input array
	return TakeWithIndices(ctx, input, indices)
}

// SortIndices returns the indices that would sort the input array
func SortIndices(ctx context.Context, input arrow.Array, order SortOrder) (arrow.Array, error) {
	// Implement sort_indices manually since the function is not available
	length := input.Len()
	indices := make([]int64, length)

	// Initialize indices
	for i := 0; i < length; i++ {
		indices[i] = int64(i)
	}

	// Sort indices based on array values
	switch input.DataType().ID() {
	case arrow.BOOL:
		boolArr := input.(*array.Boolean)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if boolArr.IsNull(int(indices[i])) {
				return true
			}
			if boolArr.IsNull(int(indices[j])) {
				return false
			}
			// Compare values
			if order == Ascending {
				return !boolArr.Value(int(indices[i])) && boolArr.Value(int(indices[j]))
			}
			return boolArr.Value(int(indices[i])) && !boolArr.Value(int(indices[j]))
		})
	case arrow.INT8:
		int8Arr := input.(*array.Int8)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if int8Arr.IsNull(int(indices[i])) {
				return true
			}
			if int8Arr.IsNull(int(indices[j])) {
				return false
			}
			// Compare values
			if order == Ascending {
				return int8Arr.Value(int(indices[i])) < int8Arr.Value(int(indices[j]))
			}
			return int8Arr.Value(int(indices[i])) > int8Arr.Value(int(indices[j]))
		})
	case arrow.INT16:
		int16Arr := input.(*array.Int16)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if int16Arr.IsNull(int(indices[i])) {
				return true
			}
			if int16Arr.IsNull(int(indices[j])) {
				return false
			}
			// Compare values
			if order == Ascending {
				return int16Arr.Value(int(indices[i])) < int16Arr.Value(int(indices[j]))
			}
			return int16Arr.Value(int(indices[i])) > int16Arr.Value(int(indices[j]))
		})
	case arrow.INT32:
		int32Arr := input.(*array.Int32)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if int32Arr.IsNull(int(indices[i])) {
				return true
			}
			if int32Arr.IsNull(int(indices[j])) {
				return false
			}
			// Compare values
			if order == Ascending {
				return int32Arr.Value(int(indices[i])) < int32Arr.Value(int(indices[j]))
			}
			return int32Arr.Value(int(indices[i])) > int32Arr.Value(int(indices[j]))
		})
	case arrow.INT64:
		int64Arr := input.(*array.Int64)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if int64Arr.IsNull(int(indices[i])) {
				return true
			}
			if int64Arr.IsNull(int(indices[j])) {
				return false
			}
			// Compare values
			if order == Ascending {
				return int64Arr.Value(int(indices[i])) < int64Arr.Value(int(indices[j]))
			}
			return int64Arr.Value(int(indices[i])) > int64Arr.Value(int(indices[j]))
		})
	case arrow.FLOAT32:
		float32Arr := input.(*array.Float32)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if float32Arr.IsNull(int(indices[i])) {
				return true
			}
			if float32Arr.IsNull(int(indices[j])) {
				return false
			}
			// Compare values
			if order == Ascending {
				return float32Arr.Value(int(indices[i])) < float32Arr.Value(int(indices[j]))
			}
			return float32Arr.Value(int(indices[i])) > float32Arr.Value(int(indices[j]))
		})
	case arrow.FLOAT64:
		float64Arr := input.(*array.Float64)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if float64Arr.IsNull(int(indices[i])) {
				return true
			}
			if float64Arr.IsNull(int(indices[j])) {
				return false
			}
			// Compare values
			if order == Ascending {
				return float64Arr.Value(int(indices[i])) < float64Arr.Value(int(indices[j]))
			}
			return float64Arr.Value(int(indices[i])) > float64Arr.Value(int(indices[j]))
		})
	case arrow.STRING:
		stringArr := input.(*array.String)
		sort.SliceStable(indices, func(i, j int) bool {
			// Handle nulls - nulls come first
			if stringArr.IsNull(int(indices[i])) {
				return true
			}
			if stringArr.IsNull(int(indices[j])) {
				return false
			}
			// Compare values
			if order == Ascending {
				return stringArr.Value(int(indices[i])) < stringArr.Value(int(indices[j]))
			}
			return stringArr.Value(int(indices[i])) > stringArr.Value(int(indices[j]))
		})
	default:
		return nil, fmt.Errorf("sorting not implemented for type %s", input.DataType())
	}

	// Create an Int64Array from the sorted indices
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(indices, nil)
	return builder.NewArray(), nil
}

// TakeWithIndices reorders elements of the array according to the indices
func TakeWithIndices(ctx context.Context, input arrow.Array, indices arrow.Array) (arrow.Array, error) {
	// Implement take manually
	indicesArr, ok := indices.(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("indices must be an Int64Array")
	}

	length := indicesArr.Len()

	// Create a builder of the appropriate type
	var builder array.Builder
	switch input.DataType().ID() {
	case arrow.BOOL:
		builder = array.NewBooleanBuilder(memory.DefaultAllocator)
	case arrow.INT8:
		builder = array.NewInt8Builder(memory.DefaultAllocator)
	case arrow.INT16:
		builder = array.NewInt16Builder(memory.DefaultAllocator)
	case arrow.INT32:
		builder = array.NewInt32Builder(memory.DefaultAllocator)
	case arrow.INT64:
		builder = array.NewInt64Builder(memory.DefaultAllocator)
	case arrow.UINT8:
		builder = array.NewUint8Builder(memory.DefaultAllocator)
	case arrow.UINT16:
		builder = array.NewUint16Builder(memory.DefaultAllocator)
	case arrow.UINT32:
		builder = array.NewUint32Builder(memory.DefaultAllocator)
	case arrow.UINT64:
		builder = array.NewUint64Builder(memory.DefaultAllocator)
	case arrow.FLOAT32:
		builder = array.NewFloat32Builder(memory.DefaultAllocator)
	case arrow.FLOAT64:
		builder = array.NewFloat64Builder(memory.DefaultAllocator)
	case arrow.STRING:
		builder = array.NewStringBuilder(memory.DefaultAllocator)
	default:
		return nil, fmt.Errorf("take not implemented for type %s", input.DataType())
	}
	defer builder.Release()

	// Append values according to indices
	for i := 0; i < length; i++ {
		idx := int(indicesArr.Value(i))
		if idx < 0 || idx >= input.Len() {
			return nil, fmt.Errorf("index out of bounds: %d", idx)
		}

		if input.IsNull(idx) {
			builder.AppendNull()
			continue
		}

		switch arr := input.(type) {
		case *array.Boolean:
			builder.(*array.BooleanBuilder).Append(arr.Value(idx))
		case *array.Int8:
			builder.(*array.Int8Builder).Append(arr.Value(idx))
		case *array.Int16:
			builder.(*array.Int16Builder).Append(arr.Value(idx))
		case *array.Int32:
			builder.(*array.Int32Builder).Append(arr.Value(idx))
		case *array.Int64:
			builder.(*array.Int64Builder).Append(arr.Value(idx))
		case *array.Uint8:
			builder.(*array.Uint8Builder).Append(arr.Value(idx))
		case *array.Uint16:
			builder.(*array.Uint16Builder).Append(arr.Value(idx))
		case *array.Uint32:
			builder.(*array.Uint32Builder).Append(arr.Value(idx))
		case *array.Uint64:
			builder.(*array.Uint64Builder).Append(arr.Value(idx))
		case *array.Float32:
			builder.(*array.Float32Builder).Append(arr.Value(idx))
		case *array.Float64:
			builder.(*array.Float64Builder).Append(arr.Value(idx))
		case *array.String:
			builder.(*array.StringBuilder).Append(arr.Value(idx))
		}
	}

	return builder.NewArray(), nil
}

// NthElement returns the nth element in sorted order
func NthElement(ctx context.Context, input arrow.Array, n int64, order SortOrder) (interface{}, error) {
	// Check if n is in range
	if n < 0 || n >= int64(input.Len()) {
		return nil, fmt.Errorf("index %d out of range (0-%d)", n, input.Len()-1)
	}

	// Sort the indices
	indices, err := SortIndices(ctx, input, order)
	if err != nil {
		return nil, err
	}
	defer indices.Release()

	// Get the nth index
	nthIndex := indices.(*array.Int64).Value(int(n))

	// Handle null values
	if input.IsNull(int(nthIndex)) {
		return nil, nil
	}

	// Extract the value based on the type
	switch arr := input.(type) {
	case *array.Boolean:
		return arr.Value(int(nthIndex)), nil
	case *array.Int8:
		return arr.Value(int(nthIndex)), nil
	case *array.Int16:
		return arr.Value(int(nthIndex)), nil
	case *array.Int32:
		return arr.Value(int(nthIndex)), nil
	case *array.Int64:
		return arr.Value(int(nthIndex)), nil
	case *array.Uint8:
		return arr.Value(int(nthIndex)), nil
	case *array.Uint16:
		return arr.Value(int(nthIndex)), nil
	case *array.Uint32:
		return arr.Value(int(nthIndex)), nil
	case *array.Uint64:
		return arr.Value(int(nthIndex)), nil
	case *array.Float32:
		return arr.Value(int(nthIndex)), nil
	case *array.Float64:
		return arr.Value(int(nthIndex)), nil
	case *array.String:
		return arr.Value(int(nthIndex)), nil
	default:
		return nil, fmt.Errorf("unsupported array type: %T", input)
	}
}

// Rank returns the rank of each element in the array
func Rank(ctx context.Context, input arrow.Array, order SortOrder) (arrow.Array, error) {
	// Get sort indices
	sortIndices, err := SortIndices(ctx, input, order)
	if err != nil {
		return nil, err
	}
	defer sortIndices.Release()

	// Create array builder for ranks
	length := input.Len()
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.Reserve(length)

	// Map from indices back to ranks
	ranks := make([]int64, length)
	indicesArr := sortIndices.(*array.Int64)

	// Extract indices
	for i := 0; i < length; i++ {
		index := indicesArr.Value(i)
		ranks[index] = int64(i)
	}

	// Build the rank array
	builder.AppendValues(ranks, nil)
	return builder.NewArray(), nil
}

// UniqueValues returns the unique values in the array
func UniqueValues(ctx context.Context, input arrow.Array) (arrow.Array, error) {
	// Implement unique manually
	switch input.DataType().ID() {
	case arrow.BOOL:
		boolArr := input.(*array.Boolean)
		hasTrue := false
		hasFalse := false
		hasNull := false

		for i := 0; i < boolArr.Len(); i++ {
			if boolArr.IsNull(i) {
				hasNull = true
			} else if boolArr.Value(i) {
				hasTrue = true
			} else {
				hasFalse = true
			}
		}

		builder := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer builder.Release()

		if hasNull {
			builder.AppendNull()
		}
		if hasFalse {
			builder.Append(false)
		}
		if hasTrue {
			builder.Append(true)
		}

		return builder.NewArray(), nil
	case arrow.INT64:
		int64Arr := input.(*array.Int64)
		uniqueMap := make(map[int64]bool)
		hasNull := false

		for i := 0; i < int64Arr.Len(); i++ {
			if int64Arr.IsNull(i) {
				hasNull = true
			} else {
				uniqueMap[int64Arr.Value(i)] = true
			}
		}

		uniqueValues := make([]int64, 0, len(uniqueMap))
		for val := range uniqueMap {
			uniqueValues = append(uniqueValues, val)
		}
		sort.Slice(uniqueValues, func(i, j int) bool {
			return uniqueValues[i] < uniqueValues[j]
		})

		builder := array.NewInt64Builder(memory.DefaultAllocator)
		defer builder.Release()

		if hasNull {
			builder.AppendNull()
		}
		builder.AppendValues(uniqueValues, nil)

		return builder.NewArray(), nil
	case arrow.FLOAT64:
		float64Arr := input.(*array.Float64)
		uniqueMap := make(map[float64]bool)
		hasNull := false

		for i := 0; i < float64Arr.Len(); i++ {
			if float64Arr.IsNull(i) {
				hasNull = true
			} else {
				uniqueMap[float64Arr.Value(i)] = true
			}
		}

		uniqueValues := make([]float64, 0, len(uniqueMap))
		for val := range uniqueMap {
			uniqueValues = append(uniqueValues, val)
		}
		sort.Float64s(uniqueValues)

		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer builder.Release()

		if hasNull {
			builder.AppendNull()
		}
		builder.AppendValues(uniqueValues, nil)

		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("unique not implemented for type %s", input.DataType())
	}
}

// CountValues returns the unique values and their counts in the array
func CountValues(ctx context.Context, input arrow.Array) (values arrow.Array, counts arrow.Array, err error) {
	// Implement value_counts manually
	switch input.DataType().ID() {
	case arrow.BOOL:
		boolArr := input.(*array.Boolean)
		trueCount := 0
		falseCount := 0
		nullCount := 0

		for i := 0; i < boolArr.Len(); i++ {
			if boolArr.IsNull(i) {
				nullCount++
			} else if boolArr.Value(i) {
				trueCount++
			} else {
				falseCount++
			}
		}

		// Build values array
		valBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer valBuilder.Release()

		// Build counts array
		countBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		defer countBuilder.Release()

		if nullCount > 0 {
			valBuilder.AppendNull()
			countBuilder.Append(int64(nullCount))
		}
		if falseCount > 0 {
			valBuilder.Append(false)
			countBuilder.Append(int64(falseCount))
		}
		if trueCount > 0 {
			valBuilder.Append(true)
			countBuilder.Append(int64(trueCount))
		}

		return valBuilder.NewArray(), countBuilder.NewArray(), nil
	case arrow.INT64:
		int64Arr := input.(*array.Int64)
		countMap := make(map[int64]int64)
		nullCount := int64(0)

		for i := 0; i < int64Arr.Len(); i++ {
			if int64Arr.IsNull(i) {
				nullCount++
			} else {
				countMap[int64Arr.Value(i)]++
			}
		}

		// Extract unique values and their counts
		uniqueValues := make([]int64, 0, len(countMap))
		for val := range countMap {
			uniqueValues = append(uniqueValues, val)
		}
		sort.Slice(uniqueValues, func(i, j int) bool {
			return uniqueValues[i] < uniqueValues[j]
		})

		// Build values array
		valBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		defer valBuilder.Release()

		// Build counts array
		countBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		defer countBuilder.Release()

		if nullCount > 0 {
			valBuilder.AppendNull()
			countBuilder.Append(nullCount)
		}
		for _, val := range uniqueValues {
			valBuilder.Append(val)
			countBuilder.Append(countMap[val])
		}

		return valBuilder.NewArray(), countBuilder.NewArray(), nil
	case arrow.FLOAT64:
		float64Arr := input.(*array.Float64)
		countMap := make(map[float64]int64)
		nullCount := int64(0)

		for i := 0; i < float64Arr.Len(); i++ {
			if float64Arr.IsNull(i) {
				nullCount++
			} else {
				countMap[float64Arr.Value(i)]++
			}
		}

		// Extract unique values and their counts
		uniqueValues := make([]float64, 0, len(countMap))
		for val := range countMap {
			uniqueValues = append(uniqueValues, val)
		}
		sort.Float64s(uniqueValues)

		// Build values array
		valBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer valBuilder.Release()

		// Build counts array
		countBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		defer countBuilder.Release()

		if nullCount > 0 {
			valBuilder.AppendNull()
			countBuilder.Append(nullCount)
		}
		for _, val := range uniqueValues {
			valBuilder.Append(val)
			countBuilder.Append(countMap[val])
		}

		return valBuilder.NewArray(), countBuilder.NewArray(), nil
	default:
		return nil, nil, fmt.Errorf("value_counts not implemented for type %s", input.DataType())
	}
}

// RECORD OPERATIONS

// SortRecord sorts a record by one or more columns
func SortRecord(ctx context.Context, input arrow.Record, sortCols []string, sortOrders []SortOrder) (arrow.Record, error) {
	if len(sortCols) == 0 {
		return nil, fmt.Errorf("no sort columns specified")
	}

	if len(sortCols) != len(sortOrders) {
		return nil, fmt.Errorf("number of sort columns (%d) does not match number of sort orders (%d)",
			len(sortCols), len(sortOrders))
	}

	// For now we'll implement a simpler version using just the first sort column
	// Multi-column sorting can be added later with more complex logic
	colName := sortCols[0]
	order := sortOrders[0]

	// Get column by name
	col, err := GetColumn(input, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	// Get sort indices
	indices, err := SortIndices(ctx, col, order)
	if err != nil {
		ReleaseArray(col)
		return nil, err
	}
	defer indices.Release()

	// Create new record with sorted columns
	cols := make([]arrow.Array, input.NumCols())
	for i := 0; i < int(input.NumCols()); i++ {
		col := input.Column(i)
		sorted, err := TakeWithIndices(ctx, col, indices)
		if err != nil {
			// Clean up already created columns
			for j := 0; j < i; j++ {
				cols[j].Release()
			}
			return nil, fmt.Errorf("error sorting column %d: %w", i, err)
		}
		cols[i] = sorted
	}

	// Create new record
	schema := input.Schema()
	result_record := array.NewRecord(schema, cols, int64(cols[0].Len()))

	// Release the columns (record takes ownership)
	for _, col := range cols {
		col.Release()
	}

	return result_record, nil
}

// SortRecordByColumn sorts a record by a single column
func SortRecordByColumn(ctx context.Context, input arrow.Record, colName string, order SortOrder) (arrow.Record, error) {
	return SortRecord(ctx, input, []string{colName}, []SortOrder{order})
}
