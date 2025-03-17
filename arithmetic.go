package archery

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// ARRAY OPERATIONS

// Add performs element-wise addition of two arrays
func Add(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "add", a, b)
}

// Subtract performs element-wise subtraction of two arrays
func Subtract(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "subtract", a, b)
}

// Multiply performs element-wise multiplication of two arrays
func Multiply(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "multiply", a, b)
}

// Divide performs element-wise division of two arrays
func Divide(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "divide", a, b)
}

// Power raises each element in first array to the power of the corresponding element in second array
func Power(ctx context.Context, a, b arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "power", a, b)
}

// Abs calculates the absolute value of each element in an array
func Abs(ctx context.Context, a arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "abs", a)
}

// Negate negates each element in an array
func Negate(ctx context.Context, a arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "negate", a)
}

// Sqrt calculates the square root of each element in an array
func Sqrt(ctx context.Context, a arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "sqrt", a)
}

// Sign returns the sign of each element (-1, 0, or 1)
func Sign(ctx context.Context, a arrow.Array) (arrow.Array, error) {
	return callFunction(ctx, "sign", a)
}

// SCALAR OPERATIONS

// AddScalar adds a scalar value to each element of an array
func AddScalar(ctx context.Context, a arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, a.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	opts := compute.ArithmeticOptions{}
	result, err := compute.Add(ctx, opts, compute.NewDatum(a), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to add scalar: %w", err)
	}

	return datumToArray(result), nil
}

// SubtractScalar subtracts a scalar value from each element of an array
func SubtractScalar(ctx context.Context, a arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, a.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	opts := compute.ArithmeticOptions{}
	result, err := compute.Subtract(ctx, opts, compute.NewDatum(a), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to subtract scalar: %w", err)
	}

	return datumToArray(result), nil
}

// MultiplyScalar multiplies each element of an array by a scalar value
func MultiplyScalar(ctx context.Context, a arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, a.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	opts := compute.ArithmeticOptions{}
	result, err := compute.Multiply(ctx, opts, compute.NewDatum(a), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to multiply by scalar: %w", err)
	}

	return datumToArray(result), nil
}

// DivideScalar divides each element of an array by a scalar value
func DivideScalar(ctx context.Context, a arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, a.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	opts := compute.ArithmeticOptions{}
	result, err := compute.Divide(ctx, opts, compute.NewDatum(a), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to divide by scalar: %w", err)
	}

	return datumToArray(result), nil
}

// PowerScalar raises each element of an array to a scalar power
func PowerScalar(ctx context.Context, a arrow.Array, val interface{}) (arrow.Array, error) {
	// Convert the scalar value to an Arrow scalar
	sc, err := toArrowScalar(val, a.DataType())
	if err != nil {
		return nil, fmt.Errorf("failed to convert scalar: %w", err)
	}

	// Call the function
	opts := compute.ArithmeticOptions{}
	result, err := compute.Power(ctx, opts, compute.NewDatum(a), compute.NewDatum(sc))
	if err != nil {
		return nil, fmt.Errorf("failed to raise to power: %w", err)
	}

	return datumToArray(result), nil
}

// Helper function to convert a datum to an array
func datumToArray(datum compute.Datum) arrow.Array {
	if datum == nil {
		return nil
	}

	switch datum.Kind() {
	case compute.KindArray:
		return datum.(*compute.ArrayDatum).MakeArray()
	case compute.KindChunked:
		// For simplicity, we only return the first chunk
		chunked := datum.(*compute.ChunkedDatum).Value
		if len(chunked.Chunks()) > 0 {
			return chunked.Chunks()[0]
		}
	}
	return nil
}

// RECORD OPERATIONS

// AddColumns adds corresponding columns from two record batches
func AddColumns(ctx context.Context, a, b arrow.Record, colName string) (arrow.Record, error) {
	// Get columns by name
	colA, err := GetColumn(a, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(colA)

	colB, err := GetColumn(b, colName)
	if err != nil {
		ReleaseArray(colA)
		return nil, err
	}
	defer ReleaseArray(colB)

	// Add the columns
	result, err := Add(ctx, colA, colB)
	if err != nil {
		return nil, err
	}

	// Replace column in record a
	newRecord, err := ReplaceRecordColumnByName(a, colName, result)
	if err != nil {
		ReleaseArray(result)
		return nil, err
	}

	// The new record now owns the result array, so we don't need to release it
	return newRecord, nil
}

// SubtractColumns subtracts corresponding columns from two record batches
func SubtractColumns(ctx context.Context, a, b arrow.Record, colName string) (arrow.Record, error) {
	// Get columns by name
	colA, err := GetColumn(a, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(colA)

	colB, err := GetColumn(b, colName)
	if err != nil {
		ReleaseArray(colA)
		return nil, err
	}
	defer ReleaseArray(colB)

	// Subtract the columns
	result, err := Subtract(ctx, colA, colB)
	if err != nil {
		return nil, err
	}

	// Replace column in record a
	newRecord, err := ReplaceRecordColumnByName(a, colName, result)
	if err != nil {
		ReleaseArray(result)
		return nil, err
	}

	// The new record now owns the result array, so we don't need to release it
	return newRecord, nil
}

// MultiplyColumns multiplies corresponding columns from two record batches
func MultiplyColumns(ctx context.Context, a, b arrow.Record, colName string) (arrow.Record, error) {
	// Get columns by name
	colA, err := GetColumn(a, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(colA)

	colB, err := GetColumn(b, colName)
	if err != nil {
		ReleaseArray(colA)
		return nil, err
	}
	defer ReleaseArray(colB)

	// Multiply the columns
	result, err := Multiply(ctx, colA, colB)
	if err != nil {
		return nil, err
	}

	// Replace column in record a
	newRecord, err := ReplaceRecordColumnByName(a, colName, result)
	if err != nil {
		ReleaseArray(result)
		return nil, err
	}

	// The new record now owns the result array, so we don't need to release it
	return newRecord, nil
}

// DivideColumns divides corresponding columns from two record batches
func DivideColumns(ctx context.Context, a, b arrow.Record, colName string) (arrow.Record, error) {
	// Get columns by name
	colA, err := GetColumn(a, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(colA)

	colB, err := GetColumn(b, colName)
	if err != nil {
		ReleaseArray(colA)
		return nil, err
	}
	defer ReleaseArray(colB)

	// Divide the columns
	result, err := Divide(ctx, colA, colB)
	if err != nil {
		return nil, err
	}

	// Replace column in record a
	newRecord, err := ReplaceRecordColumnByName(a, colName, result)
	if err != nil {
		ReleaseArray(result)
		return nil, err
	}

	// The new record now owns the result array, so we don't need to release it
	return newRecord, nil
}

// AddColumnScalar adds a scalar to a column in a record batch
func AddColumnScalar(ctx context.Context, rec arrow.Record, colName string, val interface{}) (arrow.Record, error) {
	// Get column by name
	col, err := GetColumn(rec, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	// Add scalar to column
	result, err := AddScalar(ctx, col, val)
	if err != nil {
		return nil, err
	}

	// Replace column in record
	newRecord, err := ReplaceRecordColumnByName(rec, colName, result)
	if err != nil {
		ReleaseArray(result)
		return nil, err
	}

	// The new record now owns the result array, so we don't need to release it
	return newRecord, nil
}

// SubtractColumnScalar subtracts a scalar from a column in a record batch
func SubtractColumnScalar(ctx context.Context, rec arrow.Record, colName string, val interface{}) (arrow.Record, error) {
	// Get column by name
	col, err := GetColumn(rec, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	// Subtract scalar from column
	result, err := SubtractScalar(ctx, col, val)
	if err != nil {
		return nil, err
	}

	// Replace column in record
	newRecord, err := ReplaceRecordColumnByName(rec, colName, result)
	if err != nil {
		ReleaseArray(result)
		return nil, err
	}

	// The new record now owns the result array, so we don't need to release it
	return newRecord, nil
}

// MultiplyColumnScalar multiplies a column in a record batch by a scalar
func MultiplyColumnScalar(ctx context.Context, rec arrow.Record, colName string, val interface{}) (arrow.Record, error) {
	// Get column by name
	col, err := GetColumn(rec, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	// Multiply column by scalar
	result, err := MultiplyScalar(ctx, col, val)
	if err != nil {
		return nil, err
	}

	// Replace column in record
	newRecord, err := ReplaceRecordColumnByName(rec, colName, result)
	if err != nil {
		ReleaseArray(result)
		return nil, err
	}

	// The new record now owns the result array, so we don't need to release it
	return newRecord, nil
}

// DivideColumnScalar divides a column in a record batch by a scalar
func DivideColumnScalar(ctx context.Context, rec arrow.Record, colName string, val interface{}) (arrow.Record, error) {
	// Get column by name
	col, err := GetColumn(rec, colName)
	if err != nil {
		return nil, err
	}
	defer ReleaseArray(col)

	// Divide column by scalar
	result, err := DivideScalar(ctx, col, val)
	if err != nil {
		return nil, err
	}

	// Replace column in record
	newRecord, err := ReplaceRecordColumnByName(rec, colName, result)
	if err != nil {
		ReleaseArray(result)
		return nil, err
	}

	// The new record now owns the result array, so we don't need to release it
	return newRecord, nil
}

// toArrowScalar converts a Go value to an Arrow scalar of the specified type
func toArrowScalar(value interface{}, dataType arrow.DataType) (scalar.Scalar, error) {
	// Handle nil values
	if value == nil {
		return scalar.MakeNullScalar(dataType), nil
	}

	// Convert the value based on the target data type
	switch dataType.ID() {
	case arrow.BOOL:
		if val, ok := value.(bool); ok {
			return scalar.NewBooleanScalar(val), nil
		}
	case arrow.INT8:
		if val, ok := value.(int8); ok {
			return scalar.NewInt8Scalar(val), nil
		} else if val, ok := value.(int); ok && val >= -128 && val <= 127 {
			return scalar.NewInt8Scalar(int8(val)), nil
		}
	case arrow.INT16:
		if val, ok := value.(int16); ok {
			return scalar.NewInt16Scalar(val), nil
		} else if val, ok := value.(int); ok && val >= -32768 && val <= 32767 {
			return scalar.NewInt16Scalar(int16(val)), nil
		}
	case arrow.INT32:
		if val, ok := value.(int32); ok {
			return scalar.NewInt32Scalar(val), nil
		} else if val, ok := value.(int); ok && val >= -2147483648 && val <= 2147483647 {
			return scalar.NewInt32Scalar(int32(val)), nil
		}
	case arrow.INT64:
		if val, ok := value.(int64); ok {
			return scalar.NewInt64Scalar(val), nil
		} else if val, ok := value.(int); ok {
			return scalar.NewInt64Scalar(int64(val)), nil
		}
	case arrow.UINT8:
		if val, ok := value.(uint8); ok {
			return scalar.NewUint8Scalar(val), nil
		} else if val, ok := value.(int); ok && val >= 0 && val <= 255 {
			return scalar.NewUint8Scalar(uint8(val)), nil
		}
	case arrow.UINT16:
		if val, ok := value.(uint16); ok {
			return scalar.NewUint16Scalar(val), nil
		} else if val, ok := value.(int); ok && val >= 0 && val <= 65535 {
			return scalar.NewUint16Scalar(uint16(val)), nil
		}
	case arrow.UINT32:
		if val, ok := value.(uint32); ok {
			return scalar.NewUint32Scalar(val), nil
		} else if val, ok := value.(int); ok && val >= 0 && val <= 4294967295 {
			return scalar.NewUint32Scalar(uint32(val)), nil
		}
	case arrow.UINT64:
		if val, ok := value.(uint64); ok {
			return scalar.NewUint64Scalar(val), nil
		} else if val, ok := value.(int); ok && val >= 0 {
			return scalar.NewUint64Scalar(uint64(val)), nil
		}
	case arrow.FLOAT32:
		if val, ok := value.(float32); ok {
			return scalar.NewFloat32Scalar(val), nil
		} else if val, ok := value.(float64); ok && val >= -3.40282346638529e+38 && val <= 3.40282346638529e+38 {
			return scalar.NewFloat32Scalar(float32(val)), nil
		} else if val, ok := value.(int); ok {
			return scalar.NewFloat32Scalar(float32(val)), nil
		}
	case arrow.FLOAT64:
		if val, ok := value.(float64); ok {
			return scalar.NewFloat64Scalar(val), nil
		} else if val, ok := value.(float32); ok {
			return scalar.NewFloat64Scalar(float64(val)), nil
		} else if val, ok := value.(int); ok {
			return scalar.NewFloat64Scalar(float64(val)), nil
		}
	case arrow.STRING:
		if val, ok := value.(string); ok {
			return scalar.NewStringScalar(val), nil
		} else if val, ok := value.([]byte); ok {
			return scalar.NewStringScalar(string(val)), nil
		}
	case arrow.BINARY:
		if val, ok := value.([]byte); ok {
			buf := memory.NewBufferBytes(val)
			return scalar.NewBinaryScalar(buf, dataType), nil
		} else if val, ok := value.(string); ok {
			buf := memory.NewBufferBytes([]byte(val))
			return scalar.NewBinaryScalar(buf, dataType), nil
		}
	}

	return nil, fmt.Errorf("cannot convert %T to Arrow scalar of type %s", value, dataType)
}
