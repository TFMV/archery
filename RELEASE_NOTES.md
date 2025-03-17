# Archery v0.4.0 Release Notes

## Overview

Archery v0.4.0 represents a major overhaul of the library, introducing a completely new API design and implementation approach. This version focuses on providing a more idiomatic, reliable, and maintainable Go interface for Apache Arrow operations.

## Key Changes

### New API Design

- **Functional API**: Redesigned the entire API to follow a functional programming style, making operations more composable and easier to understand.
- **Type Safety**: Improved type handling across all operations with proper error reporting.
- **Memory Management**: Added explicit memory management helpers like `ReleaseArray` and `ReleaseRecord` to simplify resource cleanup.
- **Context Support**: All operations now accept a context parameter for better cancellation and timeout support.

### Manual Implementations

- **Independence from Missing Compute Functions**: Implemented several functions that are missing from the arrow-go/v18 compute module:
  - **Sorting Operations**: Manually implemented `Sort`, `SortIndices`, `TakeWithIndices`, `NthElement`, `Rank`, `UniqueValues`, and `CountValues` functions
  - **Aggregation Operations**: Manually implemented `Sum`, `Mean`, `Min`, `Max`, `Variance`, `StandardDeviation`, `Count`, and `CountNull` functions
  - **Other Operations**: Added manual implementations for various utility functions to ensure consistent behavior
  - See `MISSING_FUNCTIONALITY.md` for a detailed list of functions that were manually implemented due to their absence in arrow-go/v18

- **Comprehensive Type Support**: Added support for all major Arrow data types (BOOL, INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, STRING).
- **Null Handling**: Improved handling of null values across all operations.

### Operations

#### Arithmetic Operations

- Scalar operations: `AddScalar`, `SubtractScalar`, `MultiplyScalar`, `DivideScalar`
- Array operations: `Add`, `Subtract`, `Multiply`, `Divide`
- Mathematical functions: `Sqrt`, `Abs`, `Negate`

#### Aggregation Operations

- Basic statistics: `Sum`, `Mean`, `Min`, `Max`
- Advanced statistics: `Variance`, `StandardDeviation`
- Counting: `Count`, `CountNull`

#### Filtering Operations

- Comparison filters: `Equal`, `NotEqual`, `Greater`, `GreaterEqual`, `Less`, `LessEqual`
- Logical operations: `And`, `Or`, `Not`
- Array filtering: `Filter`, `FilterRecord`

#### Sorting Operations

- Array sorting: `Sort`, `SortIndices`, `TakeWithIndices`
- Element selection: `NthElement`, `Rank`
- Unique values: `UniqueValues`, `CountValues`

#### Record Operations

- Column manipulation: `GetColumn`, `ReplaceRecordColumn`, `ReplaceRecordColumnByName`
- Record filtering: `FilterRecord`
- Record sorting: `SortRecordByColumn`

### New Testing Approach

- **Example-Based Testing**: Introduced a comprehensive suite of example-based tests that serve dual purposes:
  - Documentation: Examples show how to use each function in real-world scenarios
  - Validation: Examples verify that functions produce expected results

- **Modular Test Organization**: Reorganized tests into separate files by functionality:
  - `arithmetic_test.go`: Tests for arithmetic operations
  - `aggregation_test.go`: Tests for aggregation functions
  - `filtering_test.go`: Tests for filtering operations
  - `sorting_test.go`: Tests for sorting operations

- **Improved Test Coverage**: Added tests for edge cases, null handling, and various data types.

## Breaking Changes

- The entire API has been redesigned, requiring code updates for existing users.
- Removed dependency on Arrow compute functions, now using manual implementations.
- Changed function signatures to include context parameters.
- Modified return types for better error handling.

## Examples

The library now includes a comprehensive set of examples:

- Basic example (`examples/basic_example.go`) demonstrating all major functionality
- Example tests showing usage patterns for each function

## Future Directions

- Further performance optimizations
- Additional operations and data type support
- Integration with Arrow Flight and other Arrow ecosystem components

## Requirements

- Go 1.18 or later
- github.com/apache/arrow-go/v18
