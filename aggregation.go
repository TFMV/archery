package archery

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ARRAY AGGREGATION OPERATIONS

// Sum returns the sum of all elements in the array
func Sum(ctx context.Context, input arrow.Array) (interface{}, error) {
	// Implement sum manually since the compute function is not available
	switch input.DataType().ID() {
	case arrow.BOOL:
		boolArr := input.(*array.Boolean)
		var sum int64
		for i := 0; i < boolArr.Len(); i++ {
			if !boolArr.IsNull(i) {
				if boolArr.Value(i) {
					sum++
				}
			}
		}
		return sum, nil
	case arrow.INT8:
		int8Arr := input.(*array.Int8)
		var sum int64
		for i := 0; i < int8Arr.Len(); i++ {
			if !int8Arr.IsNull(i) {
				sum += int64(int8Arr.Value(i))
			}
		}
		return sum, nil
	case arrow.INT16:
		int16Arr := input.(*array.Int16)
		var sum int64
		for i := 0; i < int16Arr.Len(); i++ {
			if !int16Arr.IsNull(i) {
				sum += int64(int16Arr.Value(i))
			}
		}
		return sum, nil
	case arrow.INT32:
		int32Arr := input.(*array.Int32)
		var sum int64
		for i := 0; i < int32Arr.Len(); i++ {
			if !int32Arr.IsNull(i) {
				sum += int64(int32Arr.Value(i))
			}
		}
		return sum, nil
	case arrow.INT64:
		int64Arr := input.(*array.Int64)
		var sum int64
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				sum += int64Arr.Value(i)
			}
		}
		return sum, nil
	case arrow.UINT8:
		uint8Arr := input.(*array.Uint8)
		var sum uint64
		for i := 0; i < uint8Arr.Len(); i++ {
			if !uint8Arr.IsNull(i) {
				sum += uint64(uint8Arr.Value(i))
			}
		}
		return sum, nil
	case arrow.UINT16:
		uint16Arr := input.(*array.Uint16)
		var sum uint64
		for i := 0; i < uint16Arr.Len(); i++ {
			if !uint16Arr.IsNull(i) {
				sum += uint64(uint16Arr.Value(i))
			}
		}
		return sum, nil
	case arrow.UINT32:
		uint32Arr := input.(*array.Uint32)
		var sum uint64
		for i := 0; i < uint32Arr.Len(); i++ {
			if !uint32Arr.IsNull(i) {
				sum += uint64(uint32Arr.Value(i))
			}
		}
		return sum, nil
	case arrow.UINT64:
		uint64Arr := input.(*array.Uint64)
		var sum uint64
		for i := 0; i < uint64Arr.Len(); i++ {
			if !uint64Arr.IsNull(i) {
				sum += uint64Arr.Value(i)
			}
		}
		return sum, nil
	case arrow.FLOAT32:
		float32Arr := input.(*array.Float32)
		var sum float64
		for i := 0; i < float32Arr.Len(); i++ {
			if !float32Arr.IsNull(i) {
				sum += float64(float32Arr.Value(i))
			}
		}
		return sum, nil
	case arrow.FLOAT64:
		float64Arr := input.(*array.Float64)
		var sum float64
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				sum += float64Arr.Value(i)
			}
		}
		return sum, nil
	default:
		return nil, fmt.Errorf("sum not implemented for type %s", input.DataType())
	}
}

// Mean returns the mean of all elements in the array
func Mean(ctx context.Context, input arrow.Array) (float64, error) {
	// Implement mean manually
	if input.Len() == 0 || input.Len() == input.NullN() {
		return 0, nil
	}

	switch input.DataType().ID() {
	case arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64:

		sum, err := Sum(ctx, input)
		if err != nil {
			return 0, err
		}

		count := float64(input.Len() - input.NullN())

		switch v := sum.(type) {
		case int64:
			return float64(v) / count, nil
		case uint64:
			return float64(v) / count, nil
		case float64:
			return v / count, nil
		default:
			return 0, fmt.Errorf("unexpected sum type: %T", sum)
		}
	default:
		return 0, fmt.Errorf("mean not implemented for type %s", input.DataType())
	}
}

// Min returns the minimum value in the array
func Min(ctx context.Context, input arrow.Array) (interface{}, error) {
	// Implement min manually
	if input.Len() == 0 || input.Len() == input.NullN() {
		return nil, nil
	}

	switch input.DataType().ID() {
	case arrow.BOOL:
		boolArr := input.(*array.Boolean)
		// Find first non-null value
		var min bool
		found := false
		for i := 0; i < boolArr.Len(); i++ {
			if !boolArr.IsNull(i) {
				min = boolArr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// If we found a false, that's the minimum
		if !min {
			return false, nil
		}
		// Otherwise, check if there are any false values
		for i := 0; i < boolArr.Len(); i++ {
			if !boolArr.IsNull(i) && !boolArr.Value(i) {
				return false, nil
			}
		}
		return true, nil
	case arrow.INT8:
		int8Arr := input.(*array.Int8)
		// Find first non-null value
		var min int8
		found := false
		for i := 0; i < int8Arr.Len(); i++ {
			if !int8Arr.IsNull(i) {
				min = int8Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find minimum
		for i := 0; i < int8Arr.Len(); i++ {
			if !int8Arr.IsNull(i) && int8Arr.Value(i) < min {
				min = int8Arr.Value(i)
			}
		}
		return min, nil
	case arrow.INT16:
		int16Arr := input.(*array.Int16)
		// Find first non-null value
		var min int16
		found := false
		for i := 0; i < int16Arr.Len(); i++ {
			if !int16Arr.IsNull(i) {
				min = int16Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find minimum
		for i := 0; i < int16Arr.Len(); i++ {
			if !int16Arr.IsNull(i) && int16Arr.Value(i) < min {
				min = int16Arr.Value(i)
			}
		}
		return min, nil
	case arrow.INT32:
		int32Arr := input.(*array.Int32)
		// Find first non-null value
		var min int32
		found := false
		for i := 0; i < int32Arr.Len(); i++ {
			if !int32Arr.IsNull(i) {
				min = int32Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find minimum
		for i := 0; i < int32Arr.Len(); i++ {
			if !int32Arr.IsNull(i) && int32Arr.Value(i) < min {
				min = int32Arr.Value(i)
			}
		}
		return min, nil
	case arrow.INT64:
		int64Arr := input.(*array.Int64)
		// Find first non-null value
		var min int64
		found := false
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				min = int64Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find minimum
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) && int64Arr.Value(i) < min {
				min = int64Arr.Value(i)
			}
		}
		return min, nil
	case arrow.FLOAT64:
		float64Arr := input.(*array.Float64)
		// Find first non-null value
		var min float64
		found := false
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				min = float64Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find minimum
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) && float64Arr.Value(i) < min {
				min = float64Arr.Value(i)
			}
		}
		return min, nil
	default:
		return nil, fmt.Errorf("min not implemented for type %s", input.DataType())
	}
}

// Max returns the maximum value in the array
func Max(ctx context.Context, input arrow.Array) (interface{}, error) {
	// Implement max manually
	if input.Len() == 0 || input.Len() == input.NullN() {
		return nil, nil
	}

	switch input.DataType().ID() {
	case arrow.BOOL:
		boolArr := input.(*array.Boolean)
		// Find first non-null value
		var max bool
		found := false
		for i := 0; i < boolArr.Len(); i++ {
			if !boolArr.IsNull(i) {
				max = boolArr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// If we found a true, that's the maximum
		if max {
			return true, nil
		}
		// Otherwise, check if there are any true values
		for i := 0; i < boolArr.Len(); i++ {
			if !boolArr.IsNull(i) && boolArr.Value(i) {
				return true, nil
			}
		}
		return false, nil
	case arrow.INT8:
		int8Arr := input.(*array.Int8)
		// Find first non-null value
		var max int8
		found := false
		for i := 0; i < int8Arr.Len(); i++ {
			if !int8Arr.IsNull(i) {
				max = int8Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find maximum
		for i := 0; i < int8Arr.Len(); i++ {
			if !int8Arr.IsNull(i) && int8Arr.Value(i) > max {
				max = int8Arr.Value(i)
			}
		}
		return max, nil
	case arrow.INT16:
		int16Arr := input.(*array.Int16)
		// Find first non-null value
		var max int16
		found := false
		for i := 0; i < int16Arr.Len(); i++ {
			if !int16Arr.IsNull(i) {
				max = int16Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find maximum
		for i := 0; i < int16Arr.Len(); i++ {
			if !int16Arr.IsNull(i) && int16Arr.Value(i) > max {
				max = int16Arr.Value(i)
			}
		}
		return max, nil
	case arrow.INT32:
		int32Arr := input.(*array.Int32)
		// Find first non-null value
		var max int32
		found := false
		for i := 0; i < int32Arr.Len(); i++ {
			if !int32Arr.IsNull(i) {
				max = int32Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find maximum
		for i := 0; i < int32Arr.Len(); i++ {
			if !int32Arr.IsNull(i) && int32Arr.Value(i) > max {
				max = int32Arr.Value(i)
			}
		}
		return max, nil
	case arrow.INT64:
		int64Arr := input.(*array.Int64)
		// Find first non-null value
		var max int64
		found := false
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				max = int64Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find maximum
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) && int64Arr.Value(i) > max {
				max = int64Arr.Value(i)
			}
		}
		return max, nil
	case arrow.FLOAT64:
		float64Arr := input.(*array.Float64)
		// Find first non-null value
		var max float64
		found := false
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				max = float64Arr.Value(i)
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
		// Find maximum
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) && float64Arr.Value(i) > max {
				max = float64Arr.Value(i)
			}
		}
		return max, nil
	default:
		return nil, fmt.Errorf("max not implemented for type %s", input.DataType())
	}
}

// Mode returns the most common value in the array
func Mode(ctx context.Context, input arrow.Array) (interface{}, error) {
	// Implement mode manually
	if input.Len() == 0 || input.Len() == input.NullN() {
		return nil, nil
	}

	// For simplicity, we'll implement mode for a few common types
	switch input.DataType().ID() {
	case arrow.BOOL:
		boolArr := input.(*array.Boolean)
		trueCount := 0
		falseCount := 0
		for i := 0; i < boolArr.Len(); i++ {
			if !boolArr.IsNull(i) {
				if boolArr.Value(i) {
					trueCount++
				} else {
					falseCount++
				}
			}
		}
		if trueCount > falseCount {
			return true, nil
		}
		return false, nil
	case arrow.INT64:
		int64Arr := input.(*array.Int64)
		counts := make(map[int64]int)
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				counts[int64Arr.Value(i)]++
			}
		}
		var mode int64
		maxCount := 0
		for val, count := range counts {
			if count > maxCount {
				maxCount = count
				mode = val
			}
		}
		return mode, nil
	case arrow.FLOAT64:
		float64Arr := input.(*array.Float64)
		counts := make(map[float64]int)
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				counts[float64Arr.Value(i)]++
			}
		}
		var mode float64
		maxCount := 0
		for val, count := range counts {
			if count > maxCount {
				maxCount = count
				mode = val
			}
		}
		return mode, nil
	default:
		return nil, fmt.Errorf("mode not implemented for type %s", input.DataType())
	}
}

// Variance returns the variance of the array
func Variance(ctx context.Context, input arrow.Array) (float64, error) {
	// Implement variance manually
	if input.Len() == 0 || input.Len() == input.NullN() {
		return 0, nil
	}

	// Calculate mean first
	mean, err := Mean(ctx, input)
	if err != nil {
		return 0, err
	}

	var sumSquaredDiff float64
	var count float64

	switch input.DataType().ID() {
	case arrow.INT8:
		int8Arr := input.(*array.Int8)
		for i := 0; i < int8Arr.Len(); i++ {
			if !int8Arr.IsNull(i) {
				diff := float64(int8Arr.Value(i)) - mean
				sumSquaredDiff += diff * diff
				count++
			}
		}
	case arrow.INT16:
		int16Arr := input.(*array.Int16)
		for i := 0; i < int16Arr.Len(); i++ {
			if !int16Arr.IsNull(i) {
				diff := float64(int16Arr.Value(i)) - mean
				sumSquaredDiff += diff * diff
				count++
			}
		}
	case arrow.INT32:
		int32Arr := input.(*array.Int32)
		for i := 0; i < int32Arr.Len(); i++ {
			if !int32Arr.IsNull(i) {
				diff := float64(int32Arr.Value(i)) - mean
				sumSquaredDiff += diff * diff
				count++
			}
		}
	case arrow.INT64:
		int64Arr := input.(*array.Int64)
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				diff := float64(int64Arr.Value(i)) - mean
				sumSquaredDiff += diff * diff
				count++
			}
		}
	case arrow.FLOAT32:
		float32Arr := input.(*array.Float32)
		for i := 0; i < float32Arr.Len(); i++ {
			if !float32Arr.IsNull(i) {
				diff := float64(float32Arr.Value(i)) - mean
				sumSquaredDiff += diff * diff
				count++
			}
		}
	case arrow.FLOAT64:
		float64Arr := input.(*array.Float64)
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				diff := float64Arr.Value(i) - mean
				sumSquaredDiff += diff * diff
				count++
			}
		}
	default:
		return 0, fmt.Errorf("variance not implemented for type %s", input.DataType())
	}

	if count <= 1 {
		return 0, nil
	}

	// Use population variance (divide by count)
	return sumSquaredDiff / count, nil
}

// StandardDeviation returns the standard deviation of the array
func StandardDeviation(ctx context.Context, input arrow.Array) (float64, error) {
	// Calculate variance first
	variance, err := Variance(ctx, input)
	if err != nil {
		return 0, err
	}

	// Take square root of variance
	return math.Sqrt(variance), nil
}

// Count returns the number of non-null elements in the array
func Count(ctx context.Context, input arrow.Array) (int64, error) {
	// This is simply the length minus the null count
	return int64(input.Len() - input.NullN()), nil
}

// CountNull returns the number of null elements in the array
func CountNull(ctx context.Context, input arrow.Array) int64 {
	return int64(input.NullN())
}

// Any returns true if any element in the boolean array is true
func Any(ctx context.Context, input arrow.Array) (bool, error) {
	if input.DataType().ID() != arrow.BOOL {
		return false, fmt.Errorf("any operation only supported on boolean arrays")
	}

	boolArr := input.(*array.Boolean)
	for i := 0; i < boolArr.Len(); i++ {
		if !boolArr.IsNull(i) && boolArr.Value(i) {
			return true, nil
		}
	}
	return false, nil
}

// All returns true if all elements in the boolean array are true
func All(ctx context.Context, input arrow.Array) (bool, error) {
	if input.DataType().ID() != arrow.BOOL {
		return false, fmt.Errorf("all operation only supported on boolean arrays")
	}

	boolArr := input.(*array.Boolean)
	for i := 0; i < boolArr.Len(); i++ {
		if !boolArr.IsNull(i) && !boolArr.Value(i) {
			return false, nil
		}
	}
	return true, nil
}

// RECORD OPERATIONS

// SumColumn returns the sum of a column in a record batch
func SumColumn(ctx context.Context, rec arrow.Record, colName string) (interface{}, error) {
	col, err := GetColumn(rec, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	return Sum(ctx, col)
}

// MeanColumn returns the mean of a column in a record batch
func MeanColumn(ctx context.Context, rec arrow.Record, colName string) (float64, error) {
	col, err := GetColumn(rec, colName)
	if err != nil {
		return 0, err
	}
	defer ReleaseArray(col)

	return Mean(ctx, col)
}

// MinColumn returns the minimum value in a column
func MinColumn(ctx context.Context, rec arrow.Record, colName string) (interface{}, error) {
	col, err := GetColumn(rec, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	return Min(ctx, col)
}

// MaxColumn returns the maximum value in a column
func MaxColumn(ctx context.Context, rec arrow.Record, colName string) (interface{}, error) {
	col, err := GetColumn(rec, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	return Max(ctx, col)
}

// VarianceColumn returns the variance of a column
func VarianceColumn(ctx context.Context, rec arrow.Record, colName string) (float64, error) {
	col, err := GetColumn(rec, colName)
	if err != nil {
		return 0, err
	}
	defer ReleaseArray(col)

	return Variance(ctx, col)
}

// StandardDeviationColumn returns the standard deviation of a column
func StandardDeviationColumn(ctx context.Context, rec arrow.Record, colName string) (float64, error) {
	col, err := GetColumn(rec, colName)
	if err != nil {
		return 0, err
	}
	defer ReleaseArray(col)

	return StandardDeviation(ctx, col)
}

// CountColumn returns the number of non-null elements in a column
func CountColumn(ctx context.Context, rec arrow.Record, colName string) (int64, error) {
	col, err := GetColumn(rec, colName)
	if err != nil {
		return 0, err
	}
	defer ReleaseArray(col)

	return Count(ctx, col)
}
