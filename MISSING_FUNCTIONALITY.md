# Missing Functionality in arrow-go/v18

This document outlines functionality that is currently missing from the Apache Arrow Go implementation (arrow-go/v18) that would be beneficial to add to the core library. The Archery library has implemented these functions manually, but they would be better suited as part of the core Arrow compute functionality.

## Compute Functions

### Aggregation Functions

The following aggregation functions are missing from the Arrow compute module:

- `sum`: Calculate the sum of elements in an array
- `mean`: Calculate the mean of elements in an array
- `min`: Find the minimum value in an array
- `max`: Find the maximum value in an array
- `mode`: Find the most common value in an array
- `variance`: Calculate the variance of elements in an array
- `standard_deviation`: Calculate the standard deviation of elements in an array
- `count`: Count non-null elements in an array
- `count_null`: Count null elements in an array
- `any`: Check if any element in a boolean array is true
- `all`: Check if all elements in a boolean array are true

### Sorting Functions

The following sorting functions are missing from the Arrow compute module:

- `sort_indices`: Return indices that would sort an array
- `sort`: Return a sorted copy of an array
- `take`: Reorder elements of an array according to indices
- `nth_element`: Return the nth element in sorted order
- `rank`: Return the rank of each element in an array
- `unique`: Return unique values in an array
- `value_counts`: Return unique values and their counts in an array

## Implementation Details

### Aggregation Functions

These functions should support all numeric types (integers, floating point) and in some cases boolean types. They should handle null values appropriately.

Example signature for `sum`:

```go
func Sum(ctx context.Context, input arrow.Array) (compute.Datum, error)
```

### Sorting Functions

These functions should support all comparable types (numeric, string, boolean) and handle null values appropriately. They should also support both ascending and descending sort orders.

Example signature for `sort_indices`:

```go
func SortIndices(ctx context.Context, input arrow.Array, options *compute.SortOptions) (compute.Datum, error)
```

## Benefits of Core Implementation

1. **Performance**: Native implementations in the core library can leverage optimizations not available at the Go level.
2. **Consistency**: Core implementations ensure consistent behavior across all Arrow language bindings.
3. **Maintenance**: Centralized implementations reduce duplication of effort and bugs.
4. **Vectorization**: Core implementations can take advantage of SIMD instructions and other hardware acceleration.

## Current Workarounds

In the Archery library, we've implemented these functions manually using Go's standard library and iterating through Arrow arrays. While functional, these implementations:

1. Are likely less performant than native implementations would be
2. May not handle edge cases as robustly
3. Require more maintenance as Arrow evolves
4. Cannot take advantage of hardware acceleration

## Priority Additions

If prioritizing which functions to add first, we suggest:

1. `sum`, `mean`, `min`, `max` - Basic statistical operations
2. `sort_indices`, `sort` - Fundamental sorting operations
3. `take` - Essential for many data manipulation tasks
4. `unique`, `value_counts` - Common data analysis operations

## Conclusion

Adding these compute functions to the core arrow-go library would significantly enhance its utility for data processing and analysis tasks, bringing it closer to feature parity with other Arrow implementations like pyarrow.
