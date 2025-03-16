package archery

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// FilterByMask filters an array using a boolean mask.
// Returns the filtered array and any error encountered.
func FilterByMask(ctx context.Context, arr arrow.Array, mask *array.Boolean) (arrow.Array, error) {
	// The filter function is available in the compute package
	result, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(arr), compute.NewDatum(mask))
	if err != nil {
		return nil, err
	}

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// FilterGreaterThan filters an array to include only elements greater than the specified value.
// Returns the filtered array and any error encountered.
func FilterGreaterThan(ctx context.Context, arr arrow.Array, value interface{}) (arrow.Array, error) {
	// Create a scalar from the value
	var scalarValue scalar.Scalar
	var err error

	switch arr.DataType().ID() {
	case arrow.INT64:
		intValue, ok := value.(int64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewInt64Scalar(intValue)
	case arrow.FLOAT64:
		floatValue, ok := value.(float64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewFloat64Scalar(floatValue)
	case arrow.STRING:
		strValue, ok := value.(string)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewStringScalar(strValue)
	default:
		return nil, arrow.ErrNotImplemented
	}

	// Use the greater function to create a boolean mask
	result, err := compute.CallFunction(ctx, "greater", nil, compute.NewDatum(arr), compute.NewDatum(scalarValue))
	if err != nil {
		return nil, err
	}

	// Get the boolean mask
	mask := result.(*compute.ArrayDatum).MakeArray()
	defer mask.Release()

	// Use the mask to filter the array
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterGreaterEqual filters an array to include only elements greater than or equal to the specified value.
// Returns the filtered array and any error encountered.
func FilterGreaterEqual(ctx context.Context, arr arrow.Array, value interface{}) (arrow.Array, error) {
	// Create a scalar from the value
	var scalarValue scalar.Scalar
	var err error

	switch v := value.(type) {
	case int:
		scalarValue = scalar.NewInt64Scalar(int64(v))
	case int64:
		scalarValue = scalar.NewInt64Scalar(v)
	case float64:
		scalarValue = scalar.NewFloat64Scalar(v)
	default:
		return nil, arrow.ErrInvalid
	}

	// Create a boolean mask using the greater_equal function
	maskResult, err := compute.CallFunction(ctx, "greater_equal", nil, compute.NewDatum(arr), compute.NewDatum(scalarValue))
	if err != nil {
		return nil, err
	}

	// Convert the result to a boolean array
	mask := DatumToArray(maskResult)
	defer mask.Release()

	// Filter the array using the mask
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterLessThan filters an array to include only elements less than the specified value.
// Returns the filtered array and any error encountered.
func FilterLessThan(ctx context.Context, arr arrow.Array, value interface{}) (arrow.Array, error) {
	// Create a scalar from the value
	var scalarValue scalar.Scalar
	var err error

	switch arr.DataType().ID() {
	case arrow.INT64:
		intValue, ok := value.(int64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewInt64Scalar(intValue)
	case arrow.FLOAT64:
		floatValue, ok := value.(float64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewFloat64Scalar(floatValue)
	case arrow.STRING:
		strValue, ok := value.(string)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewStringScalar(strValue)
	default:
		return nil, arrow.ErrNotImplemented
	}

	// Use the less function to create a boolean mask
	result, err := compute.CallFunction(ctx, "less", nil, compute.NewDatum(arr), compute.NewDatum(scalarValue))
	if err != nil {
		return nil, err
	}

	// Get the boolean mask
	mask := result.(*compute.ArrayDatum).MakeArray()
	defer mask.Release()

	// Use the mask to filter the array
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterLessEqual filters an array to include only elements less than or equal to the specified value.
// Returns the filtered array and any error encountered.
func FilterLessEqual(ctx context.Context, arr arrow.Array, value interface{}) (arrow.Array, error) {
	// Create a scalar from the value
	var scalarValue scalar.Scalar
	var err error

	switch v := value.(type) {
	case int:
		scalarValue = scalar.NewInt64Scalar(int64(v))
	case int64:
		scalarValue = scalar.NewInt64Scalar(v)
	case float64:
		scalarValue = scalar.NewFloat64Scalar(v)
	default:
		return nil, arrow.ErrInvalid
	}

	// Create a boolean mask using the less_equal function
	maskResult, err := compute.CallFunction(ctx, "less_equal", nil, compute.NewDatum(arr), compute.NewDatum(scalarValue))
	if err != nil {
		return nil, err
	}

	// Convert the result to a boolean array
	mask := DatumToArray(maskResult)
	defer mask.Release()

	// Filter the array using the mask
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterEqual filters an array to include only elements equal to the specified value.
// Returns the filtered array and any error encountered.
func FilterEqual(ctx context.Context, arr arrow.Array, value interface{}) (arrow.Array, error) {
	// Create a scalar from the value
	var scalarValue scalar.Scalar
	var err error

	switch arr.DataType().ID() {
	case arrow.INT64:
		intValue, ok := value.(int64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewInt64Scalar(intValue)
	case arrow.FLOAT64:
		floatValue, ok := value.(float64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewFloat64Scalar(floatValue)
	case arrow.STRING:
		strValue, ok := value.(string)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewStringScalar(strValue)
	default:
		return nil, arrow.ErrNotImplemented
	}

	// Use the equal function to create a boolean mask
	result, err := compute.CallFunction(ctx, "equal", nil, compute.NewDatum(arr), compute.NewDatum(scalarValue))
	if err != nil {
		return nil, err
	}

	// Get the boolean mask
	mask := result.(*compute.ArrayDatum).MakeArray()
	defer mask.Release()

	// Use the mask to filter the array
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterNotEqual filters an array to include only elements not equal to the specified value.
// Returns the filtered array and any error encountered.
func FilterNotEqual(ctx context.Context, arr arrow.Array, value interface{}) (arrow.Array, error) {
	// Create a scalar from the value
	var scalarValue scalar.Scalar
	var err error

	switch arr.DataType().ID() {
	case arrow.INT64:
		intValue, ok := value.(int64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewInt64Scalar(intValue)
	case arrow.FLOAT64:
		floatValue, ok := value.(float64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewFloat64Scalar(floatValue)
	case arrow.STRING:
		strValue, ok := value.(string)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		scalarValue = scalar.NewStringScalar(strValue)
	default:
		return nil, arrow.ErrNotImplemented
	}

	// Use the not_equal function to create a boolean mask
	result, err := compute.CallFunction(ctx, "not_equal", nil, compute.NewDatum(arr), compute.NewDatum(scalarValue))
	if err != nil {
		return nil, err
	}

	// Get the boolean mask
	mask := result.(*compute.ArrayDatum).MakeArray()
	defer mask.Release()

	// Use the mask to filter the array
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterIsMultipleOf filters an array to include only elements that are multiples of the specified value.
// Returns the filtered array and any error encountered.
func FilterIsMultipleOf(ctx context.Context, arr arrow.Array, value interface{}) (arrow.Array, error) {
	// Since modulo is not available, we'll implement this using division and multiplication
	var scalarValue scalar.Scalar

	switch arr.DataType().ID() {
	case arrow.INT64:
		intValue, ok := value.(int64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		if intValue == 0 {
			return nil, arrow.ErrInvalid // Division by zero
		}
		scalarValue = scalar.NewInt64Scalar(intValue)
	case arrow.FLOAT64:
		floatValue, ok := value.(float64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		if floatValue == 0 {
			return nil, arrow.ErrInvalid // Division by zero
		}
		scalarValue = scalar.NewFloat64Scalar(floatValue)
	default:
		return nil, arrow.ErrNotImplemented
	}

	// Divide the array by the value
	divResult, err := compute.CallFunction(ctx, "divide", nil, compute.NewDatum(arr), compute.NewDatum(scalarValue))
	if err != nil {
		return nil, err
	}

	// Floor the result to get the integer division
	floorResult, err := compute.CallFunction(ctx, "floor", nil, compute.NewDatum(divResult))
	if err != nil {
		return nil, err
	}

	// Multiply by the original value to get multiples
	multResult, err := compute.CallFunction(ctx, "multiply", nil, compute.NewDatum(floorResult), compute.NewDatum(scalarValue))
	if err != nil {
		return nil, err
	}

	// Check if the result equals the original array
	equalResult, err := compute.CallFunction(ctx, "equal", nil, compute.NewDatum(arr), compute.NewDatum(multResult))
	if err != nil {
		return nil, err
	}

	// Get the boolean mask
	mask := equalResult.(*compute.ArrayDatum).MakeArray()
	defer mask.Release()

	// Use the mask to filter the array
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterBetween filters an array to include only elements between the specified lower and upper bounds (inclusive).
// Returns the filtered array and any error encountered.
func FilterBetween(ctx context.Context, arr arrow.Array, lower, upper interface{}) (arrow.Array, error) {
	// Create scalars from the values
	var lowerScalar, upperScalar scalar.Scalar

	switch arr.DataType().ID() {
	case arrow.INT64:
		lowerInt, ok := lower.(int64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		upperInt, ok := upper.(int64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		lowerScalar = scalar.NewInt64Scalar(lowerInt)
		upperScalar = scalar.NewInt64Scalar(upperInt)
	case arrow.FLOAT64:
		lowerFloat, ok := lower.(float64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		upperFloat, ok := upper.(float64)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		lowerScalar = scalar.NewFloat64Scalar(lowerFloat)
		upperScalar = scalar.NewFloat64Scalar(upperFloat)
	case arrow.STRING:
		lowerStr, ok := lower.(string)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		upperStr, ok := upper.(string)
		if !ok {
			return nil, arrow.ErrInvalid
		}
		lowerScalar = scalar.NewStringScalar(lowerStr)
		upperScalar = scalar.NewStringScalar(upperStr)
	default:
		return nil, arrow.ErrNotImplemented
	}

	// Use the greater_equal function to create a lower bound mask
	lowerResult, err := compute.CallFunction(ctx, "greater_equal", nil, compute.NewDatum(arr), compute.NewDatum(lowerScalar))
	if err != nil {
		return nil, err
	}

	// Use the less_equal function to create an upper bound mask
	upperResult, err := compute.CallFunction(ctx, "less_equal", nil, compute.NewDatum(arr), compute.NewDatum(upperScalar))
	if err != nil {
		return nil, err
	}

	// Use the and function to combine the masks
	andResult, err := compute.CallFunction(ctx, "and", nil, compute.NewDatum(lowerResult), compute.NewDatum(upperResult))
	if err != nil {
		return nil, err
	}

	// Get the boolean mask
	mask := andResult.(*compute.ArrayDatum).MakeArray()
	defer mask.Release()

	// Use the mask to filter the array
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterIn filters an array to include only elements that are in the specified values array.
// Returns the filtered array and any error encountered.
func FilterIn(ctx context.Context, arr arrow.Array, values arrow.Array) (arrow.Array, error) {
	// Use the is_in function to create a boolean mask
	result, err := compute.CallFunction(ctx, "is_in", nil, compute.NewDatum(arr), compute.NewDatum(values))
	if err != nil {
		return nil, err
	}

	// Get the boolean mask
	mask := result.(*compute.ArrayDatum).MakeArray()
	defer mask.Release()

	// Use the mask to filter the array
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// FilterNotNull filters an array to include only non-null elements.
// Returns the filtered array and any error encountered.
func FilterNotNull(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	// Use the is_valid function to create a boolean mask
	result, err := compute.CallFunction(ctx, "is_valid", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}

	// Get the boolean mask
	mask := result.(*compute.ArrayDatum).MakeArray()
	defer mask.Release()

	// Use the mask to filter the array
	return FilterByMask(ctx, arr, mask.(*array.Boolean))
}

// CreateBooleanMask creates a boolean mask from a function that evaluates each element.
// The function should take an index and return a boolean value.
// Returns the boolean mask array.
func CreateBooleanMask(ctx context.Context, arr arrow.Array, fn func(int) bool) *array.Boolean {
	builder := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(fn(i))
		}
	}

	return builder.NewBooleanArray()
}

// Take selects elements from an array based on the provided indices.
// Returns the resulting array and any error encountered.
func Take(ctx context.Context, arr arrow.Array, indices arrow.Array) (arrow.Array, error) {
	opts := compute.TakeOptions{BoundsCheck: true}
	result, err := compute.Take(ctx, opts, compute.NewDatum(arr), compute.NewDatum(indices))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Unique returns the unique values in an array.
// Returns the resulting array and any error encountered.
func Unique(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "unique", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}
