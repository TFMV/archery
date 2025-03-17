package archery

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// ARRAY FILTERING OPERATIONS

// Filter returns a new array with only elements where the mask is true
func Filter(ctx context.Context, input arrow.Array, mask arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "filter", input, mask)
}

// IsNull returns a mask array indicating which elements are null
func IsNull(ctx context.Context, input arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "is_null", input)
}

// IsValid returns a mask array indicating which elements are not null
func IsValid(ctx context.Context, input arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "is_valid", input)
}

// Equal returns a mask array indicating which elements are equal
func Equal(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "equal", a, b)
}

// NotEqual returns a mask array indicating which elements are not equal
func NotEqual(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "not_equal", a, b)
}

// Greater returns a mask array indicating which elements of a are greater than b
func Greater(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "greater", a, b)
}

// GreaterEqual returns a mask array indicating which elements of a are greater than or equal to b
func GreaterEqual(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "greater_equal", a, b)
}

// Less returns a mask array indicating which elements of a are less than b
func Less(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "less", a, b)
}

// LessEqual returns a mask array indicating which elements of a are less than or equal to b
func LessEqual(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "less_equal", a, b)
}

// And performs logical AND operation on two boolean arrays
func And(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "and", a, b)
}

// Or performs logical OR operation on two boolean arrays
func Or(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "or", a, b)
}

// Xor performs logical XOR operation on two boolean arrays
func Xor(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "xor", a, b)
}

// Invert performs logical NOT operation on a boolean array
func Invert(ctx context.Context, input arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "invert", input)
}

// SCALAR COMPARISON OPERATIONS

// EqualScalar returns a mask array indicating which elements are equal to the scalar value
func EqualScalar(ctx context.Context, input arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, input.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	result, err := compute.CallFunction(ctx, "equal", nil, compute.NewDatum(input), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to compare with scalar: %w", err)
	}

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// NotEqualScalar returns a mask array indicating which elements are not equal to the scalar value
func NotEqualScalar(ctx context.Context, input arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, input.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	result, err := compute.CallFunction(ctx, "not_equal", nil, compute.NewDatum(input), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to compare with scalar: %w", err)
	}

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// GreaterScalar returns a mask array indicating which elements are greater than the scalar value
func GreaterScalar(ctx context.Context, input arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, input.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	result, err := compute.CallFunction(ctx, "greater", nil, compute.NewDatum(input), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to compare with scalar: %w", err)
	}

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// GreaterEqualScalar returns a mask array indicating which elements are greater than or equal to the scalar value
func GreaterEqualScalar(ctx context.Context, input arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, input.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	result, err := compute.CallFunction(ctx, "greater_equal", nil, compute.NewDatum(input), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to compare with scalar: %w", err)
	}

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// LessScalar returns a mask array indicating which elements are less than the scalar value
func LessScalar(ctx context.Context, input arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, input.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	result, err := compute.CallFunction(ctx, "less", nil, compute.NewDatum(input), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to compare with scalar: %w", err)
	}

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// LessEqualScalar returns a mask array indicating which elements are less than or equal to the scalar value
func LessEqualScalar(ctx context.Context, input arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, input.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	result, err := compute.CallFunction(ctx, "less_equal", nil, compute.NewDatum(input), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to compare with scalar: %w", err)
	}

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// RECORD OPERATIONS

// FilterRecord returns a new record with only rows where the mask is true
func FilterRecord(ctx context.Context, input arrow.Record, mask arrow.Array) (arrow.Record, error) {
	// Check mask length
	if int64(mask.Len()) != input.NumRows() {
		return nil, fmt.Errorf("mask length (%d) does not match record rows (%d)",
			mask.Len(), input.NumRows())
	}

	// Filter each column
	cols := make([]arrow.Array, input.NumCols())
	for i := 0; i < int(input.NumCols()); i++ {
		col := input.Column(i)
		filtered, err := Filter(ctx, col, mask)
		if err != nil {
			// Clean up already created columns
			for j := 0; j < i; j++ {
				cols[j].Release()
			}
			return nil, fmt.Errorf("error filtering column %d: %w", i, err)
		}
		cols[i] = filtered
	}

	// Create new record batch
	schema := input.Schema()
	result := array.NewRecord(schema, cols, int64(cols[0].Len()))

	// Release the columns (record takes ownership)
	for _, col := range cols {
		col.Release()
	}

	return result, nil
}

// FilterRecordByColumn returns a new record with only rows where the condition on the column is true
func FilterRecordByColumn(ctx context.Context, input arrow.Record, colName string, condition arrow.Array) (arrow.Record, error) {
	// Get column by name
	col, err := GetColumn(input, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	// Apply filtering to all columns
	return FilterRecord(ctx, input, condition)
}

// FilterRecordByColumnValue returns a new record with only rows where the column equals the given value
func FilterRecordByColumnValue(ctx context.Context, input arrow.Record, colName string, val interface{}) (arrow.Record, error) {
	// Get column by name
	col, err := GetColumn(input, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	// Create mask for filtering
	mask, err := EqualScalar(ctx, col, val)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(mask)

	// Apply filtering
	return FilterRecord(ctx, input, mask)
}

// FilterRecordByColumnRange returns a new record with only rows where the column value is in the given range
func FilterRecordByColumnRange(ctx context.Context, input arrow.Record, colName string, min, max interface{}) (arrow.Record, error) {
	// Get column by name
	col, err := GetColumn(input, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	// Create lower bound mask
	lowerMask, err := GreaterEqualScalar(ctx, col, min)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(lowerMask)

	// Create upper bound mask
	upperMask, err := LessEqualScalar(ctx, col, max)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(upperMask)

	// Combine masks
	combinedMask, err := And(ctx, lowerMask, upperMask)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(combinedMask)

	// Apply filtering
	return FilterRecord(ctx, input, combinedMask)
}
