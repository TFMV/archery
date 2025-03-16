# Archery: Apache Arrow Compute Library for Go

Archery is a Go library that provides a user-friendly interface to the Apache Arrow compute package. It simplifies working with Arrow arrays and compute operations by providing a set of helper functions for common tasks.

## Features

- **Arithmetic Operations**: Add, subtract, multiply, divide, and more operations on Arrow arrays.
- **Filtering Operations**: Filter arrays based on various conditions like greater than, less than, equal to, etc.
- **Aggregation Operations**: Calculate sum, mean, min, max, standard deviation, and other statistics on Arrow arrays.
- **Sorting Operations**: Sort arrays, get sort indices, find nth elements, and calculate ranks.
- **Comparison Operations**: Compare arrays for equality, inequality, greater than, less than, etc.
- **Record Operations**: Apply array operations to Arrow Records, including filtering, sorting, aggregation, and grouping.

## Installation

```bash
go get github.com/TFMV/archery
```

## Available Functions

### Arithmetic Functions

- `add`, `subtract`, `multiply`, `divide`
- `power`, `sqrt`, `sign`, `negate`, `abs`

### Filtering Functions

- `FilterByMask`, `FilterGreaterThan`, `FilterLessThan`
- `FilterEqual`, `FilterNotEqual`, `FilterBetween`
- `FilterIsMultipleOf`, `FilterIn`, `FilterNotNull`

### Aggregation Functions

- `Sum`, `Mean`, `Min`, `Max`, `MinMax`
- `Count`, `CountNonNull`, `Variance`, `StandardDeviation`
- `Quantile`, `Median`

### Sorting Functions

- `Sort`, `SortIndicesWithOrder`, `TakeWithIndices`
- `NthElement`, `Rank`, `UniqueValues`

### Comparison Functions

- `equal`, `not_equal`, `greater`, `less`
- `greater_equal`, `less_equal`, `and`, `or`, `not`

### Record Operations

- `RecordWrapper`: A wrapper for Arrow Records that provides methods to apply array operations to records
- `FilterByMask`, `FilterRows`, `FilterRowsByColumn`: Filter records based on conditions
- `SortRecord`: Sort records by a specified column
- `AggregateColumn`: Apply aggregation functions to columns
- `GroupBy`: Group records by one or more columns and apply aggregation functions

## Usage Examples

### Working with Arrays

```go
// Create an array
builder := array.NewFloat64Builder(memory.DefaultAllocator)
builder.AppendValues([]float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
arr := builder.NewFloat64Array()
defer arr.Release()

// Perform arithmetic operation
ctx := context.Background()
result, err := archery.Add(ctx, arr, arr)
if err != nil {
    log.Fatal(err)
}
defer result.Release()

// Filter array
filtered, err := archery.FilterGreaterThan(ctx, arr, 3.0)
if err != nil {
    log.Fatal(err)
}
defer filtered.Release()

// Calculate aggregation
sum, err := archery.Sum(ctx, arr)
if err != nil {
    log.Fatal(err)
}
```

### Working with Records

```go
// Create a record
schema := arrow.NewSchema(
    []arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "name", Type: arrow.BinaryTypes.String},
        {Name: "score", Type: arrow.PrimitiveTypes.Float64},
    },
    nil,
)

// Create arrays for the record
idArray := ... // Int64Array
nameArray := ... // StringArray
scoreArray := ... // Float64Array

// Create the record
record := array.NewRecord(schema, []arrow.Array{idArray, nameArray, scoreArray}, 5)
defer record.Release()

// Create a RecordWrapper
wrapper := archery.NewRecordWrapper(record, memory.DefaultAllocator)

// Filter records where score > 80
ctx := context.Background()
filtered, err := wrapper.FilterRowsByColumn(ctx, "score", archery.GreaterThan(80.0))
if err != nil {
    log.Fatal(err)
}
defer filtered.Release()

// Sort records by score in descending order
sorted, err := wrapper.SortRecord(ctx, "score", archery.Descending)
if err != nil {
    log.Fatal(err)
}
defer sorted.Release()

// Calculate mean score
mean, err := wrapper.AggregateColumn(ctx, "score", archery.MeanAggregator())
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Mean score: %.2f\n", mean)

// Group by category and calculate mean scores
groupByResult, err := wrapper.GroupBy(ctx, []string{"category"}, map[string]func(context.Context, arrow.Array) (interface{}, error){
    "score": archery.MeanAggregator(),
})
if err != nil {
    log.Fatal(err)
}
defer groupByResult.Release()

// Convert the result to a record
groupedRecord := groupByResult.ToRecord(memory.DefaultAllocator)
defer groupedRecord.Release()
```

## License

[MIT](LICENSE)
