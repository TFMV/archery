# Missing Functionality in arrow-go/v18

This document outlines functionality that was missing from the Apache Arrow Go implementation (arrow-go/v18). Recent releases have added many of these capabilities and Archery now delegates to the Arrow compute module whenever possible.

## Compute Functions

### Aggregation Functions

The following aggregation functions were previously unavailable but are now provided by Arrow compute and used by Archery:

- `sum`
- `mean`
- `min`
- `max`
- `variance`
- `standard_deviation`
- `count`
- `count_null`
- `any`
- `all`
- `mode` (still missing)

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
