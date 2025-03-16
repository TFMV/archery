package archery

import (
	"context"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// Sum calculates the sum of all elements in an array.
// Returns the resulting scalar and any error encountered.
func Sum(ctx context.Context, arr arrow.Array) (scalar.Scalar, error) {
	// Since the sum function is not available, we'll implement it manually
	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		var sum int64
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				sum += int64Arr.Value(i)
			}
		}
		return scalar.NewInt64Scalar(sum), nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		var sum float64
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				sum += float64Arr.Value(i)
			}
		}
		return scalar.NewFloat64Scalar(sum), nil
	default:
		return nil, arrow.ErrNotImplemented
	}
}

// Mean calculates the arithmetic mean of all elements in an array.
// Returns the resulting scalar and any error encountered.
func Mean(ctx context.Context, arr arrow.Array) (scalar.Scalar, error) {
	// Since the mean function is not available, we'll implement it manually
	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		var sum int64
		var count int64
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				sum += int64Arr.Value(i)
				count++
			}
		}
		if count == 0 {
			return nil, arrow.ErrInvalid
		}
		return scalar.NewFloat64Scalar(float64(sum) / float64(count)), nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		var sum float64
		var count int64
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				sum += float64Arr.Value(i)
				count++
			}
		}
		if count == 0 {
			return nil, arrow.ErrInvalid
		}
		return scalar.NewFloat64Scalar(sum / float64(count)), nil
	default:
		return nil, arrow.ErrNotImplemented
	}
}

// Min finds the minimum value in an array.
// Returns the resulting scalar and any error encountered.
func Min(ctx context.Context, arr arrow.Array) (scalar.Scalar, error) {
	// Since the min function is not available, we'll implement it manually
	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		if int64Arr.Len() == 0 {
			return nil, arrow.ErrInvalid
		}
		var min int64
		var found bool
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				if !found {
					min = int64Arr.Value(i)
					found = true
				} else if int64Arr.Value(i) < min {
					min = int64Arr.Value(i)
				}
			}
		}
		if !found {
			return nil, arrow.ErrInvalid
		}
		return scalar.NewInt64Scalar(min), nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		if float64Arr.Len() == 0 {
			return nil, arrow.ErrInvalid
		}
		var min float64
		var found bool
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				if !found {
					min = float64Arr.Value(i)
					found = true
				} else if float64Arr.Value(i) < min {
					min = float64Arr.Value(i)
				}
			}
		}
		if !found {
			return nil, arrow.ErrInvalid
		}
		return scalar.NewFloat64Scalar(min), nil
	default:
		return nil, arrow.ErrNotImplemented
	}
}

// Max finds the maximum value in an array.
// Returns the resulting scalar and any error encountered.
func Max(ctx context.Context, arr arrow.Array) (scalar.Scalar, error) {
	// Since the max function is not available, we'll implement it manually
	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		if int64Arr.Len() == 0 {
			return nil, arrow.ErrInvalid
		}
		var max int64
		var found bool
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				if !found {
					max = int64Arr.Value(i)
					found = true
				} else if int64Arr.Value(i) > max {
					max = int64Arr.Value(i)
				}
			}
		}
		if !found {
			return nil, arrow.ErrInvalid
		}
		return scalar.NewInt64Scalar(max), nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		if float64Arr.Len() == 0 {
			return nil, arrow.ErrInvalid
		}
		var max float64
		var found bool
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				if !found {
					max = float64Arr.Value(i)
					found = true
				} else if float64Arr.Value(i) > max {
					max = float64Arr.Value(i)
				}
			}
		}
		if !found {
			return nil, arrow.ErrInvalid
		}
		return scalar.NewFloat64Scalar(max), nil
	default:
		return nil, arrow.ErrNotImplemented
	}
}

// MinMaxResult contains the min and max scalars.
type MinMaxResult struct {
	Min scalar.Scalar
	Max scalar.Scalar
}

// MinMax finds both the minimum and maximum values in an array.
// Returns a struct containing the min and max scalars, and any error encountered.
func MinMax(ctx context.Context, arr arrow.Array) (*MinMaxResult, error) {
	// Since the min_max function is not available, we'll implement it manually
	min, err := Min(ctx, arr)
	if err != nil {
		return nil, err
	}

	max, err := Max(ctx, arr)
	if err != nil {
		return nil, err
	}

	return &MinMaxResult{
		Min: min,
		Max: max,
	}, nil
}

// Count counts the number of elements in an array.
// Returns the count as an int64 and any error encountered.
func Count(ctx context.Context, arr arrow.Array) (int64, error) {
	// Since the count function is not available, we'll implement it manually
	return int64(arr.Len()), nil
}

// CountNonNull counts the number of non-null elements in an array.
// Returns the count as an int64 and any error encountered.
func CountNonNull(ctx context.Context, arr arrow.Array) (int64, error) {
	// Since the count_non_null function is not available, we'll implement it manually
	return int64(arr.Len() - arr.NullN()), nil
}

// Variance calculates the variance of the elements in an array.
// Returns the variance as a float64 and any error encountered.
func Variance(ctx context.Context, arr arrow.Array) (float64, error) {
	// Since the variance function is not available, we'll implement it manually
	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		var sum int64
		var sumSquares int64
		var count int64
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				val := int64Arr.Value(i)
				sum += val
				sumSquares += val * val
				count++
			}
		}
		if count <= 1 {
			return 0, arrow.ErrInvalid
		}
		mean := float64(sum) / float64(count)
		return float64(sumSquares)/float64(count) - mean*mean, nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		var sum float64
		var sumSquares float64
		var count int64
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				val := float64Arr.Value(i)
				sum += val
				sumSquares += val * val
				count++
			}
		}
		if count <= 1 {
			return 0, arrow.ErrInvalid
		}
		mean := sum / float64(count)
		return sumSquares/float64(count) - mean*mean, nil
	default:
		return 0, arrow.ErrNotImplemented
	}
}

// StandardDeviation calculates the standard deviation of the elements in an array.
// Returns the standard deviation as a float64 and any error encountered.
func StandardDeviation(ctx context.Context, arr arrow.Array) (float64, error) {
	// Since the stddev function is not available, we'll implement it manually
	variance, err := Variance(ctx, arr)
	if err != nil {
		return 0, err
	}
	return math.Sqrt(variance), nil
}

// QuantileOpts implements compute.FunctionOptions for the quantile function
type QuantileOpts struct {
	Interpolation string
	Quantiles     []float64
}

// TypeName implements the compute.FunctionOptions interface
func (o *QuantileOpts) TypeName() string {
	return "quantile"
}

// Quantile calculates the quantile of the elements in an array.
// The quantile parameter should be between 0 and 1.
// Returns the quantile value and any error encountered.
func Quantile(ctx context.Context, arr arrow.Array, q float64) (scalar.Scalar, error) {
	// Since the quantile function is not available, we'll implement it manually
	if q < 0 || q > 1 {
		return nil, arrow.ErrInvalid
	}

	switch arr.DataType().ID() {
	case arrow.INT64:
		int64Arr := arr.(*array.Int64)
		if int64Arr.Len() == 0 {
			return nil, arrow.ErrInvalid
		}

		// Extract non-null values
		var values []int64
		for i := 0; i < int64Arr.Len(); i++ {
			if !int64Arr.IsNull(i) {
				values = append(values, int64Arr.Value(i))
			}
		}
		if len(values) == 0 {
			return nil, arrow.ErrInvalid
		}

		// Sort the values
		sort := func(values []int64) {
			for i := 0; i < len(values); i++ {
				for j := i + 1; j < len(values); j++ {
					if values[i] > values[j] {
						values[i], values[j] = values[j], values[i]
					}
				}
			}
		}
		sort(values)

		// Calculate the index
		index := q * float64(len(values)-1)
		if index == float64(int(index)) {
			// Exact index
			return scalar.NewInt64Scalar(values[int(index)]), nil
		}

		// Interpolate
		lower := int(math.Floor(index))
		upper := int(math.Ceil(index))
		fraction := index - float64(lower)

		lowerVal := values[lower]
		upperVal := values[upper]

		interpolated := float64(lowerVal) + fraction*float64(upperVal-lowerVal)
		return scalar.NewFloat64Scalar(interpolated), nil
	case arrow.FLOAT64:
		float64Arr := arr.(*array.Float64)
		if float64Arr.Len() == 0 {
			return nil, arrow.ErrInvalid
		}

		// Extract non-null values
		var values []float64
		for i := 0; i < float64Arr.Len(); i++ {
			if !float64Arr.IsNull(i) {
				values = append(values, float64Arr.Value(i))
			}
		}
		if len(values) == 0 {
			return nil, arrow.ErrInvalid
		}

		// Sort the values
		sort := func(values []float64) {
			for i := 0; i < len(values); i++ {
				for j := i + 1; j < len(values); j++ {
					if values[i] > values[j] {
						values[i], values[j] = values[j], values[i]
					}
				}
			}
		}
		sort(values)

		// Calculate the index
		index := q * float64(len(values)-1)
		if index == float64(int(index)) {
			// Exact index
			return scalar.NewFloat64Scalar(values[int(index)]), nil
		}

		// Interpolate
		lower := int(math.Floor(index))
		upper := int(math.Ceil(index))
		fraction := index - float64(lower)

		lowerVal := values[lower]
		upperVal := values[upper]

		interpolated := lowerVal + fraction*(upperVal-lowerVal)
		return scalar.NewFloat64Scalar(interpolated), nil
	default:
		return nil, arrow.ErrNotImplemented
	}
}

// Median calculates the median of the elements in an array.
// Returns the median value and any error encountered.
func Median(ctx context.Context, arr arrow.Array) (scalar.Scalar, error) {
	return Quantile(ctx, arr, 0.5)
}
