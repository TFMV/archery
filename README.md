# Archery: A Go Library for Apache Arrow Compute Operations

Archery provides an API for working with Apache Arrow compute operations in Go.
It simplifies common tasks by providing direct functions that operate on Arrow arrays and records.

## Features

- **Functional Design**: Simple, idiomatic Go functions that work directly with Arrow arrays and records.
- **Full Type Support**: Works with all standard Arrow data types.
- **Memory Management**: Careful handling of Arrow memory to prevent leaks.
- **Comprehensive Operations**:
  - Arithmetic functions (add, subtract, multiply, divide, etc.)
  - Filtering and comparison operations
  - Aggregation functions (sum, mean, min, max, etc.)
  - Sorting operations
  - Anomaly detection using z-scores

## Installation

```bash
go get github.com/TFMV/archery
```

Requires Go 1.19+ and Apache Arrow Go v18+.

## Usage

### Basic Array Operations

```go
import (
    "context"
    "fmt"
    
    "github.com/TFMV/archery"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    // Create a new array
    builder := array.NewFloat64Builder(memory.DefaultAllocator)
    defer builder.Release()
    
    builder.AppendValues([]float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
    arr := builder.NewFloat64Array()
    defer arr.Release()
    
    ctx := context.Background()
    
    // Add 10 to each element
    result, err := archery.AddScalar(ctx, arr, 10.0)
    if err != nil {
        panic(err)
    }
    defer archery.ReleaseArray(result)
    
    // Display the result
    for i := 0; i < result.Len(); i++ {
        fmt.Println(result.(*array.Float64).Value(i))  // 11.0, 12.0, 13.0, 14.0, 15.0
    }
}
```

### Working with Records

```go
import (
    "context"
    "fmt"
    
    "github.com/TFMV/archery"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    // Create sample data
    builder := array.NewFloat64Builder(memory.DefaultAllocator)
    defer builder.Release()
    
    builder.AppendValues([]float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
    values := builder.NewFloat64Array()
    defer values.Release()
    
    // Create another column
    builder.Reset()
    builder.AppendValues([]float64{10.0, 20.0, 30.0, 40.0, 50.0}, nil)
    moreValues := builder.NewFloat64Array()
    defer moreValues.Release()
    
    // Create a schema
    fields := []arrow.Field{
        {Name: "values", Type: arrow.PrimitiveTypes.Float64},
        {Name: "more_values", Type: arrow.PrimitiveTypes.Float64},
    }
    schema := arrow.NewSchema(fields, nil)
    
    // Create the record
    columns := []arrow.Array{values, moreValues}
    record := array.NewRecord(schema, columns, int64(values.Len()))
    defer archery.ReleaseRecord(record)
    
    ctx := context.Background()
    
    // Calculate sum of a column
    sum, err := archery.SumColumn(ctx, record, "values")
    if err != nil {
        panic(err)
    }
    fmt.Printf("Sum of values: %v\n", sum)  // 15.0
    
    // Get a column by name
    col, err := archery.GetColumn(record, "values")
    if err != nil {
        panic(err)
    }
    defer col.Release()
    
    // Filter rows where values > 2
    mask, err := archery.GreaterScalar(ctx, col, 2.0)
    if err != nil {
        panic(err)
    }
    defer archery.ReleaseArray(mask)
    
    filtered, err := archery.FilterRecord(ctx, record, mask)
    if err != nil {
        panic(err)
    }
    defer archery.ReleaseRecord(filtered)
    
    fmt.Printf("Filtered record has %d rows\n", filtered.NumRows())  // 3
}
```

### Filtering and Null Handling

```go
import (
    "context"
    "fmt"
    
    "github.com/TFMV/archery"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    // Create a test array with nulls
    builder := array.NewInt64Builder(memory.DefaultAllocator)
    defer builder.Release()
    
    // Add values with some nulls
    builder.AppendValues([]int64{1, 2, 3}, []bool{true, false, true})
    builder.AppendNull()
    builder.AppendValues([]int64{5, 6}, []bool{true, true})
    
    arr := builder.NewInt64Array()
    defer arr.Release()
    
    // Create a mask for non-null values
    maskBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
    defer maskBuilder.Release()
    
    for i := 0; i < arr.Len(); i++ {
        maskBuilder.Append(!arr.IsNull(i))
    }
    
    mask := maskBuilder.NewBooleanArray()
    defer mask.Release()
    
    // Apply the mask to drop nulls
    ctx := context.Background()
    nonNullArr, err := archery.Filter(ctx, arr, mask)
    if err != nil {
        panic(err)
    }
    defer archery.ReleaseArray(nonNullArr)
    
    // Print the non-null values
    fmt.Println("Non-null values:")
    for i := 0; i < nonNullArr.Len(); i++ {
        fmt.Printf("%d ", nonNullArr.(*array.Int64).Value(i))
    }
    // Output: 1 3 5 6
    
    // Count nulls
    nullCount := archery.CountNull(ctx, arr)
    fmt.Printf("Null count: %d\n", nullCount)
    // Output: 2
}
```

## Available Functions

### Arithmetic Operations

- `Add(ctx, a, b arrow.Array) (arrow.Array, error)`
- `Subtract(ctx, a, b arrow.Array) (arrow.Array, error)`
- `Multiply(ctx, a, b arrow.Array) (arrow.Array, error)`
- `Divide(ctx, a, b arrow.Array) (arrow.Array, error)`
- `Power(ctx, a, b arrow.Array) (arrow.Array, error)`
- `AddScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `SubtractScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `MultiplyScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `DivideScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `PowerScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `Abs(ctx, arr arrow.Array) (arrow.Array, error)`
- `Negate(ctx, arr arrow.Array) (arrow.Array, error)`
- `Sqrt(ctx, arr arrow.Array) (arrow.Array, error)`
- `Sign(ctx, arr arrow.Array) (arrow.Array, error)`

### Aggregation Operations

- `Sum(ctx, arr arrow.Array) (interface{}, error)`
- `Mean(ctx, arr arrow.Array) (float64, error)`
- `Min(ctx, arr arrow.Array) (interface{}, error)`
- `Max(ctx, arr arrow.Array) (interface{}, error)`
- `Variance(ctx, arr arrow.Array) (float64, error)`
- `StandardDeviation(ctx, arr arrow.Array) (float64, error)`
- `Count(ctx, arr arrow.Array) (int64, error)`
- `CountNull(ctx, arr arrow.Array) int64`
- `Mode(ctx, arr arrow.Array) (interface{}, error)`
- `Any(ctx, arr arrow.Array) (bool, error)` - For boolean arrays
- `All(ctx, arr arrow.Array) (bool, error)` - For boolean arrays

### Filtering and Comparison Operations

- `Filter(ctx, input arrow.Array, mask arrow.Array) (arrow.Array, error)`
- `IsNull(ctx, arr arrow.Array) (arrow.Array, error)` - Returns boolean mask
- `IsValid(ctx, arr arrow.Array) (arrow.Array, error)` - Returns boolean mask
- `Equal(ctx, a, b arrow.Array) (arrow.Array, error)` - Returns boolean mask
- `NotEqual(ctx, a, b arrow.Array) (arrow.Array, error)` - Returns boolean mask
- `Greater(ctx, a, b arrow.Array) (arrow.Array, error)` - Returns boolean mask
- `GreaterEqual(ctx, a, b arrow.Array) (arrow.Array, error)` - Returns boolean mask
- `Less(ctx, a, b arrow.Array) (arrow.Array, error)` - Returns boolean mask
- `LessEqual(ctx, a, b arrow.Array) (arrow.Array, error)` - Returns boolean mask
- `And(ctx, a, b arrow.Array) (arrow.Array, error)` - Boolean AND
- `Or(ctx, a, b arrow.Array) (arrow.Array, error)` - Boolean OR
- `Xor(ctx, a, b arrow.Array) (arrow.Array, error)` - Boolean XOR
- `EqualScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `NotEqualScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `GreaterScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `GreaterEqualScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `LessScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`
- `LessEqualScalar(ctx, arr arrow.Array, value interface{}) (arrow.Array, error)`

### Sorting Operations

- `Sort(ctx, arr arrow.Array, order SortOrder) (arrow.Array, error)`
- `SortIndices(ctx, arr arrow.Array, order SortOrder) (arrow.Array, error)`
- `TakeWithIndices(ctx, arr, indices arrow.Array) (arrow.Array, error)`
- `NthElement(ctx, arr arrow.Array, n int64, order SortOrder) (interface{}, error)`
- `Rank(ctx, arr arrow.Array, order SortOrder) (arrow.Array, error)`
- `UniqueValues(ctx, arr arrow.Array) (arrow.Array, error)`
- `CountValues(ctx, arr arrow.Array) (values arrow.Array, counts arrow.Array, err error)`

### Record Operations

- `FilterRecord(ctx, rec arrow.Record, mask arrow.Array) (arrow.Record, error)`
- `FilterRecordByColumn(ctx, rec arrow.Record, colName string, condition arrow.Array) (arrow.Record, error)`
- `FilterRecordByColumnValue(ctx, rec arrow.Record, colName string, value interface{}) (arrow.Record, error)`
- `FilterRecordByColumnRange(ctx, rec arrow.Record, colName string, min, max interface{}) (arrow.Record, error)`
- `SortRecord(ctx, rec arrow.Record, sortCols []string, sortOrders []SortOrder) (arrow.Record, error)`
- `SortRecordByColumn(ctx, rec arrow.Record, colName string, order SortOrder) (arrow.Record, error)`
- `SumColumn(ctx, rec arrow.Record, colName string) (interface{}, error)`
- `MeanColumn(ctx, rec arrow.Record, colName string) (float64, error)`
- `MinColumn(ctx, rec arrow.Record, colName string) (interface{}, error)`
- `MaxColumn(ctx, rec arrow.Record, colName string) (interface{}, error)`
- `VarianceColumn(ctx, rec arrow.Record, colName string) (float64, error)`
- `StandardDeviationColumn(ctx, rec arrow.Record, colName string) (float64, error)`
- `CountColumn(ctx, rec arrow.Record, colName string) (int64, error)`

### Utility Functions

- `ReleaseArray(arr arrow.Array)`
- `ReleaseRecord(rec arrow.Record)`
- `GetColumn(rec arrow.Record, name string) (arrow.Array, error)`
- `GetColumnIndex(rec arrow.Record, name string) (int, error)`
- `ColumnNames(rec arrow.Record) []string`
- `ReplaceRecordColumn(rec arrow.Record, colIndex int, newCol arrow.Array) arrow.Record`
- `ReplaceRecordColumnByName(rec arrow.Record, colName string, newCol arrow.Array) (arrow.Record, error)`

## Implementation Details

Archery implements many functions that are not available in the core Arrow Go library. These include:

1. **Aggregation Functions**: Functions like `Sum`, `Mean`, `Min`, `Max`, `Variance`, `StandardDeviation`, etc. are implemented manually to provide functionality that's missing from the Arrow compute module.

2. **Sorting Functions**: Functions like `Sort`, `SortIndices`, `UniqueValues`, etc. are implemented manually since the Arrow Go library doesn't provide these compute functions.

3. **Memory Management**: Archery provides careful memory management with functions like `ReleaseArray` and `ReleaseRecord` to prevent memory leaks when working with Arrow data structures.

## Examples

The library includes comprehensive testable examples for all major functionality.

## Missing Functionality

For details on functionality that is currently missing from the Arrow Go library and implemented manually in Archery, see the [MISSING_FUNCTIONALITY.md](MISSING_FUNCTIONALITY.md) file.

## License

[MIT](LICENSE)
