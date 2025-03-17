package archery_test

import (
	"context"
	"fmt"

	"github.com/TFMV/archery"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func Example_addScalar() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Add a scalar
	ctx := context.Background()
	result, err := archery.AddScalar(ctx, arr, float64(10))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(result)

	// Print the result
	fmt.Println("Add 10:")
	for i := 0; i < result.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%.1f", result.(*array.Float64).Value(i))
	}
	fmt.Println()

	// Output:
	// Add 10:
	// 11.0 12.0 13.0 14.0 15.0
}

func Example_multiplyScalar() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Multiply by a scalar
	ctx := context.Background()
	result, err := archery.MultiplyScalar(ctx, arr, float64(2))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(result)

	// Print the result
	fmt.Println("Multiply by 2:")
	for i := 0; i < result.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%.1f", result.(*array.Float64).Value(i))
	}
	fmt.Println()

	// Output:
	// Multiply by 2:
	// 2.0 4.0 6.0 8.0 10.0
}

func Example_sqrt() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 4, 9, 16, 25}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Calculate square root
	ctx := context.Background()
	result, err := archery.Sqrt(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(result)

	// Print the result
	fmt.Println("Square root:")
	for i := 0; i < result.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%.1f", result.(*array.Float64).Value(i))
	}
	fmt.Println()

	// Output:
	// Square root:
	// 1.0 2.0 3.0 4.0 5.0
}

func Example_add() {
	// Create two test arrays
	builder1 := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder1.Release()

	builder1.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr1 := builder1.NewFloat64Array()
	defer arr1.Release()

	builder2 := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder2.Release()
	builder2.AppendValues([]float64{10, 20, 30, 40, 50}, nil)
	arr2 := builder2.NewFloat64Array()
	defer arr2.Release()

	// Add arrays
	ctx := context.Background()
	result, err := archery.Add(ctx, arr1, arr2)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(result)

	// Print the result
	fmt.Println("Add arrays:")
	for i := 0; i < result.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%.1f", result.(*array.Float64).Value(i))
	}
	fmt.Println()

	// Output:
	// Add arrays:
	// 11.0 22.0 33.0 44.0 55.0
}

func Example_abs() {
	// Create a test array with negative values
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{-1, 2, -3, 4, -5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Calculate absolute values
	ctx := context.Background()
	result, err := archery.Abs(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(result)

	// Print the result
	fmt.Println("Absolute values:")
	for i := 0; i < result.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%.1f", result.(*array.Float64).Value(i))
	}
	fmt.Println()

	// Output:
	// Absolute values:
	// 1.0 2.0 3.0 4.0 5.0
}
