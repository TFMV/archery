# Archery: Apache Arrow Compute Library for Go

Archery is a Go library that provides a user-friendly interface to the Apache Arrow compute package. It simplifies working with Arrow arrays and compute operations by providing a set of helper functions for common tasks.

## Features

- **Arithmetic Operations**: Add, subtract, multiply, divide, and more operations on Arrow arrays.
- **Filtering Operations**: Filter arrays based on various conditions like greater than, less than, equal to, etc.
- **Aggregation Operations**: Calculate sum, mean, min, max, standard deviation, and other statistics on Arrow arrays.
- **Sorting Operations**: Sort arrays, get sort indices, find nth elements, and calculate ranks.
- **Comparison Operations**: Compare arrays for equality, inequality, greater than, less than, etc.

## Installation

```bash
go get github.com/apache/arrow/go/v18/arrow
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

## License

[MIT](LICENSE)
