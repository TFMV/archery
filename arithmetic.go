package archery

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// Add performs element-wise addition of two arrays.
// Returns the resulting array and any error encountered.
func Add(ctx context.Context, left, right arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "add", nil, compute.NewDatum(left), compute.NewDatum(right))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// AddScalar performs element-wise addition of an array and a scalar.
// Returns the resulting array and any error encountered.
func AddScalar(ctx context.Context, arr arrow.Array, val interface{}) (arrow.Array, error) {
	opts := compute.ArithmeticOptions{}
	var s scalar.Scalar

	switch arr.DataType().ID() {
	case arrow.INT64:
		if v, ok := val.(int64); ok {
			s = scalar.NewInt64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	case arrow.FLOAT64:
		if v, ok := val.(float64); ok {
			s = scalar.NewFloat64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	default:
		return nil, arrow.ErrNotImplemented
	}

	result, err := compute.Add(ctx, opts, compute.NewDatum(arr), compute.NewDatum(s))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Subtract performs element-wise subtraction of two arrays.
// Returns the resulting array and any error encountered.
func Subtract(ctx context.Context, left, right arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "subtract", nil, compute.NewDatum(left), compute.NewDatum(right))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// SubtractScalar performs element-wise subtraction of an array and a scalar.
// Returns the resulting array and any error encountered.
func SubtractScalar(ctx context.Context, arr arrow.Array, val interface{}) (arrow.Array, error) {
	opts := compute.ArithmeticOptions{}
	var s scalar.Scalar

	switch arr.DataType().ID() {
	case arrow.INT64:
		if v, ok := val.(int64); ok {
			s = scalar.NewInt64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	case arrow.FLOAT64:
		if v, ok := val.(float64); ok {
			s = scalar.NewFloat64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	default:
		return nil, arrow.ErrNotImplemented
	}

	result, err := compute.Subtract(ctx, opts, compute.NewDatum(arr), compute.NewDatum(s))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Multiply performs element-wise multiplication of two arrays.
// Returns the resulting array and any error encountered.
func Multiply(ctx context.Context, left, right arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "multiply", nil, compute.NewDatum(left), compute.NewDatum(right))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// MultiplyScalar performs element-wise multiplication of an array and a scalar.
// Returns the resulting array and any error encountered.
func MultiplyScalar(ctx context.Context, arr arrow.Array, val interface{}) (arrow.Array, error) {
	opts := compute.ArithmeticOptions{}
	var s scalar.Scalar

	switch arr.DataType().ID() {
	case arrow.INT64:
		if v, ok := val.(int64); ok {
			s = scalar.NewInt64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	case arrow.FLOAT64:
		if v, ok := val.(float64); ok {
			s = scalar.NewFloat64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	default:
		return nil, arrow.ErrNotImplemented
	}

	result, err := compute.Multiply(ctx, opts, compute.NewDatum(arr), compute.NewDatum(s))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Divide performs element-wise division of two arrays.
// Returns the resulting array and any error encountered.
func Divide(ctx context.Context, left, right arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "divide", nil, compute.NewDatum(left), compute.NewDatum(right))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// DivideScalar performs element-wise division of an array and a scalar.
// Returns the resulting array and any error encountered.
func DivideScalar(ctx context.Context, arr arrow.Array, val interface{}) (arrow.Array, error) {
	opts := compute.ArithmeticOptions{}
	var s scalar.Scalar

	switch arr.DataType().ID() {
	case arrow.INT64:
		if v, ok := val.(int64); ok {
			s = scalar.NewInt64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	case arrow.FLOAT64:
		if v, ok := val.(float64); ok {
			s = scalar.NewFloat64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	default:
		return nil, arrow.ErrNotImplemented
	}

	result, err := compute.Divide(ctx, opts, compute.NewDatum(arr), compute.NewDatum(s))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Power raises each element in the base array to the power of the corresponding element in the exponent array.
// Returns the resulting array and any error encountered.
func Power(ctx context.Context, base, exponent arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "power", nil, compute.NewDatum(base), compute.NewDatum(exponent))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// PowerScalar raises each element in the array to the power of the scalar value.
// Returns the resulting array and any error encountered.
func PowerScalar(ctx context.Context, arr arrow.Array, exponent interface{}) (arrow.Array, error) {
	opts := compute.ArithmeticOptions{}
	var s scalar.Scalar

	switch arr.DataType().ID() {
	case arrow.INT64:
		if v, ok := exponent.(int64); ok {
			s = scalar.NewInt64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	case arrow.FLOAT64:
		if v, ok := exponent.(float64); ok {
			s = scalar.NewFloat64Scalar(v)
		} else {
			return nil, arrow.ErrInvalid
		}
	default:
		return nil, arrow.ErrNotImplemented
	}

	result, err := compute.Power(ctx, opts, compute.NewDatum(arr), compute.NewDatum(s))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// AbsoluteValue computes the absolute value of each element in the array.
// Returns the resulting array and any error encountered.
func AbsoluteValue(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "absolute_value", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Negate computes the negation of each element in the array.
// Returns the resulting array and any error encountered.
func Negate(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "negate", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Sign returns the sign of each element in the array (-1, 0, or 1).
// Returns the resulting array and any error encountered.
func Sign(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.Sign(ctx, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Sin computes the sine of each element in the array.
// Returns the resulting array and any error encountered.
func Sin(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "sin", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Cos computes the cosine of each element in the array.
// Returns the resulting array and any error encountered.
func Cos(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "cos", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Tan computes the tangent of each element in the array.
// Returns the resulting array and any error encountered.
func Tan(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "tan", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Asin computes the arcsine of each element in the array.
// Returns the resulting array and any error encountered.
func Asin(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "asin", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Acos computes the arccosine of each element in the array.
// Returns the resulting array and any error encountered.
func Acos(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "acos", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Atan computes the arctangent of each element in the array.
// Returns the resulting array and any error encountered.
func Atan(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "atan", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Ln computes the natural logarithm of each element in the array.
// Returns the resulting array and any error encountered.
func Ln(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "ln", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Log10 computes the base-10 logarithm of each element in the array.
// Returns the resulting array and any error encountered.
func Log10(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "log10", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Log2 computes the base-2 logarithm of each element in the array.
// Returns the resulting array and any error encountered.
func Log2(ctx context.Context, arr arrow.Array) (arrow.Array, error) {
	result, err := compute.CallFunction(ctx, "log2", nil, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}

// Round rounds each element in the array to the specified number of decimal places.
// Returns the resulting array and any error encountered.
func Round(ctx context.Context, arr arrow.Array, nDigits int64) (arrow.Array, error) {
	opts := compute.RoundOptions{NDigits: nDigits}
	result, err := compute.Round(ctx, opts, compute.NewDatum(arr))
	if err != nil {
		return nil, err
	}
	return DatumToArray(result), nil
}
