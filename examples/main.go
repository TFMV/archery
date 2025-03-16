package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/compute"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/arrow/scalar"
)

func main() {
	// Create a context
	ctx := context.Background()

	// Create a memory allocator
	mem := memory.NewGoAllocator()

	fmt.Println("=== Apache Arrow Compute Examples ===")

	// Demonstrate arithmetic operations
	fmt.Println("\n=== Arithmetic Operations ===")
	demonstrateArithmetic(ctx, mem)

	// Demonstrate filtering operations
	fmt.Println("\n=== Filtering Operations ===")
	demonstrateFiltering(ctx, mem)

	// Demonstrate comparison operations
	fmt.Println("\n=== Comparison Operations ===")
	demonstrateComparison(ctx, mem)

	// Demonstrate available compute functions
	fmt.Println("\n=== Available Compute Functions ===")
	demonstrateAvailableFunctions(ctx)
}

func demonstrateArithmetic(ctx context.Context, mem memory.Allocator) {
	// Create an Int64 array
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewInt64Array()
	defer arr.Release()

	fmt.Println("Original array:", formatArray(arr))

	// Addition with scalar
	addScalar := scalar.NewInt64Scalar(10)
	addResult, err := compute.CallFunction(ctx, "add", nil, compute.NewDatum(arr), compute.NewDatum(addScalar))
	if err != nil {
		fmt.Printf("Error in add: %v\n", err)
	} else {
		addArr := addResult.(*compute.ArrayDatum).MakeArray()
		defer addArr.Release()
		fmt.Println("Add 10:", formatArray(addArr))
	}

	// Subtraction with scalar
	subScalar := scalar.NewInt64Scalar(1)
	subResult, err := compute.CallFunction(ctx, "subtract", nil, compute.NewDatum(arr), compute.NewDatum(subScalar))
	if err != nil {
		fmt.Printf("Error in subtract: %v\n", err)
	} else {
		subArr := subResult.(*compute.ArrayDatum).MakeArray()
		defer subArr.Release()
		fmt.Println("Subtract 1:", formatArray(subArr))
	}

	// Multiplication with scalar
	mulScalar := scalar.NewInt64Scalar(2)
	mulResult, err := compute.CallFunction(ctx, "multiply", nil, compute.NewDatum(arr), compute.NewDatum(mulScalar))
	if err != nil {
		fmt.Printf("Error in multiply: %v\n", err)
	} else {
		mulArr := mulResult.(*compute.ArrayDatum).MakeArray()
		defer mulArr.Release()
		fmt.Println("Multiply by 2:", formatArray(mulArr))
	}

	// Division with scalar
	divScalar := scalar.NewInt64Scalar(2)
	divResult, err := compute.CallFunction(ctx, "divide", nil, compute.NewDatum(arr), compute.NewDatum(divScalar))
	if err != nil {
		fmt.Printf("Error in divide: %v\n", err)
	} else {
		divArr := divResult.(*compute.ArrayDatum).MakeArray()
		defer divArr.Release()
		fmt.Println("Divide by 2:", formatArray(divArr))
	}
}

func demonstrateFiltering(ctx context.Context, mem memory.Allocator) {
	// Create an Int64 array
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	builder.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	arr := builder.NewInt64Array()
	defer arr.Release()

	fmt.Println("Original array:", formatArray(arr))

	// Filter greater than
	greaterScalar := scalar.NewInt64Scalar(5)
	greaterResult, err := compute.CallFunction(ctx, "greater", nil, compute.NewDatum(arr), compute.NewDatum(greaterScalar))
	if err != nil {
		fmt.Printf("Error in greater: %v\n", err)
	} else {
		greaterMask := greaterResult.(*compute.ArrayDatum).MakeArray()
		defer greaterMask.Release()

		filterResult, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(arr), compute.NewDatum(greaterMask))
		if err != nil {
			fmt.Printf("Error in filter: %v\n", err)
		} else {
			filterArr := filterResult.(*compute.ArrayDatum).MakeArray()
			defer filterArr.Release()
			fmt.Println("Filter > 5:", formatArray(filterArr))
		}
	}

	// Filter less than
	lessScalar := scalar.NewInt64Scalar(5)
	lessResult, err := compute.CallFunction(ctx, "less", nil, compute.NewDatum(arr), compute.NewDatum(lessScalar))
	if err != nil {
		fmt.Printf("Error in less: %v\n", err)
	} else {
		lessMask := lessResult.(*compute.ArrayDatum).MakeArray()
		defer lessMask.Release()

		filterResult, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(arr), compute.NewDatum(lessMask))
		if err != nil {
			fmt.Printf("Error in filter: %v\n", err)
		} else {
			filterArr := filterResult.(*compute.ArrayDatum).MakeArray()
			defer filterArr.Release()
			fmt.Println("Filter < 5:", formatArray(filterArr))
		}
	}

	// Filter equal
	equalScalar := scalar.NewInt64Scalar(5)
	equalResult, err := compute.CallFunction(ctx, "equal", nil, compute.NewDatum(arr), compute.NewDatum(equalScalar))
	if err != nil {
		fmt.Printf("Error in equal: %v\n", err)
	} else {
		equalMask := equalResult.(*compute.ArrayDatum).MakeArray()
		defer equalMask.Release()

		filterResult, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(arr), compute.NewDatum(equalMask))
		if err != nil {
			fmt.Printf("Error in filter: %v\n", err)
		} else {
			filterArr := filterResult.(*compute.ArrayDatum).MakeArray()
			defer filterArr.Release()
			fmt.Println("Filter == 5:", formatArray(filterArr))
		}
	}

	// Filter between (using AND)
	lowerScalar := scalar.NewInt64Scalar(3)
	upperScalar := scalar.NewInt64Scalar(7)

	lowerResult, err := compute.CallFunction(ctx, "greater_equal", nil, compute.NewDatum(arr), compute.NewDatum(lowerScalar))
	if err != nil {
		fmt.Printf("Error in greater_equal: %v\n", err)
		return
	}

	upperResult, err := compute.CallFunction(ctx, "less_equal", nil, compute.NewDatum(arr), compute.NewDatum(upperScalar))
	if err != nil {
		fmt.Printf("Error in less_equal: %v\n", err)
		return
	}

	andResult, err := compute.CallFunction(ctx, "and", nil, compute.NewDatum(lowerResult), compute.NewDatum(upperResult))
	if err != nil {
		fmt.Printf("Error in and: %v\n", err)
		return
	}

	betweenMask := andResult.(*compute.ArrayDatum).MakeArray()
	defer betweenMask.Release()

	filterResult, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(arr), compute.NewDatum(betweenMask))
	if err != nil {
		fmt.Printf("Error in filter: %v\n", err)
	} else {
		filterArr := filterResult.(*compute.ArrayDatum).MakeArray()
		defer filterArr.Release()
		fmt.Println("Filter between 3 and 7:", formatArray(filterArr))
	}
}

func demonstrateComparison(ctx context.Context, mem memory.Allocator) {
	// Create two Int64 arrays
	builder1 := array.NewInt64Builder(mem)
	defer builder1.Release()

	builder1.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	arr1 := builder1.NewInt64Array()
	defer arr1.Release()

	builder2 := array.NewInt64Builder(mem)
	defer builder2.Release()

	builder2.AppendValues([]int64{1, 3, 3, 0, 5}, nil)
	arr2 := builder2.NewInt64Array()
	defer arr2.Release()

	fmt.Println("Array 1:", formatArray(arr1))
	fmt.Println("Array 2:", formatArray(arr2))

	// Equal
	equalResult, err := compute.CallFunction(ctx, "equal", nil, compute.NewDatum(arr1), compute.NewDatum(arr2))
	if err != nil {
		fmt.Printf("Error in equal: %v\n", err)
	} else {
		equalArr := equalResult.(*compute.ArrayDatum).MakeArray()
		defer equalArr.Release()
		fmt.Println("Equal:", formatArray(equalArr))
	}

	// Not equal
	notEqualResult, err := compute.CallFunction(ctx, "not_equal", nil, compute.NewDatum(arr1), compute.NewDatum(arr2))
	if err != nil {
		fmt.Printf("Error in not_equal: %v\n", err)
	} else {
		notEqualArr := notEqualResult.(*compute.ArrayDatum).MakeArray()
		defer notEqualArr.Release()
		fmt.Println("Not equal:", formatArray(notEqualArr))
	}

	// Greater
	greaterResult, err := compute.CallFunction(ctx, "greater", nil, compute.NewDatum(arr1), compute.NewDatum(arr2))
	if err != nil {
		fmt.Printf("Error in greater: %v\n", err)
	} else {
		greaterArr := greaterResult.(*compute.ArrayDatum).MakeArray()
		defer greaterArr.Release()
		fmt.Println("Greater:", formatArray(greaterArr))
	}

	// Less
	lessResult, err := compute.CallFunction(ctx, "less", nil, compute.NewDatum(arr1), compute.NewDatum(arr2))
	if err != nil {
		fmt.Printf("Error in less: %v\n", err)
	} else {
		lessArr := lessResult.(*compute.ArrayDatum).MakeArray()
		defer lessArr.Release()
		fmt.Println("Less:", formatArray(lessArr))
	}
}

func demonstrateAvailableFunctions(ctx context.Context) {
	// List all available functions
	fmt.Println("Available functions:")

	// Check for arithmetic functions
	fmt.Println("\nArithmetic functions:")
	checkFunctions(ctx, []string{
		"add", "subtract", "multiply", "divide",
		"power", "sqrt", "sign", "negate",
		"abs", "absolute_value", "round", "floor", "ceil", "modulo",
	})

	// Check for aggregation functions
	fmt.Println("\nAggregation functions:")
	checkFunctions(ctx, []string{
		"sum", "mean", "min", "max", "min_max",
		"count", "count_non_null", "variance", "stddev",
	})

	// Check for sorting functions
	fmt.Println("\nSorting functions:")
	checkFunctions(ctx, []string{
		"sort_indices", "sort", "rank", "nth_element",
	})

	// Check for filtering functions
	fmt.Println("\nFiltering functions:")
	checkFunctions(ctx, []string{
		"filter", "take", "unique", "is_in", "is_valid",
	})

	// Check for comparison functions
	fmt.Println("\nComparison functions:")
	checkFunctions(ctx, []string{
		"equal", "not_equal", "greater", "greater_equal",
		"less", "less_equal", "and", "or", "not",
	})
}

// Helper function to check if functions are available
func checkFunctions(ctx context.Context, functions []string) {
	for _, fn := range functions {
		// Try to call the function with empty arguments to see if it exists
		_, err := compute.CallFunction(ctx, fn, nil)
		if err != nil {
			// Check if the error is due to invalid arguments (function exists) or function not found
			if err.Error() == "arrow/compute: function not found" {
				fmt.Printf("✗ %s\n", fn)
			} else {
				fmt.Printf("✓ %s\n", fn)
			}
		} else {
			fmt.Printf("✓ %s\n", fn)
		}
	}
}

// Helper function to format an array as a string
func formatArray(arr arrow.Array) string {
	var result string
	result = "["

	for i := 0; i < arr.Len(); i++ {
		if i > 0 {
			result += ", "
		}

		if arr.IsNull(i) {
			result += "null"
		} else {
			switch arr.DataType().ID() {
			case arrow.INT64:
				result += fmt.Sprintf("%d", arr.(*array.Int64).Value(i))
			case arrow.FLOAT64:
				result += fmt.Sprintf("%f", arr.(*array.Float64).Value(i))
			case arrow.BOOL:
				result += fmt.Sprintf("%t", arr.(*array.Boolean).Value(i))
			case arrow.STRING:
				result += fmt.Sprintf("\"%s\"", arr.(*array.String).Value(i))
			default:
				result += "?"
			}
		}
	}

	result += "]"
	return result
}
