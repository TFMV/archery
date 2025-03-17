package archery_test

import (
	"context"
	"fmt"

	"github.com/TFMV/archery"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func Example_sum() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Calculate sum
	ctx := context.Background()
	sum, err := archery.Sum(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Print the result
	fmt.Printf("Sum: %.1f\n", sum)

	// Output:
	// Sum: 15.0
}

func Example_mean() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Calculate mean
	ctx := context.Background()
	mean, err := archery.Mean(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Print the result
	fmt.Printf("Mean: %.1f\n", mean)

	// Output:
	// Mean: 3.0
}

func Example_minMax() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Calculate min and max
	ctx := context.Background()
	min, err := archery.Min(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	max, err := archery.Max(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Print the results
	fmt.Printf("Min: %.1f\n", min)
	fmt.Printf("Max: %.1f\n", max)

	// Output:
	// Min: 1.0
	// Max: 5.0
}

func Example_variance() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Calculate variance
	ctx := context.Background()
	variance, err := archery.Variance(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Print the result
	fmt.Printf("Variance: %.1f\n", variance)

	// Output:
	// Variance: 2.0
}

func Example_standardDeviation() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Calculate standard deviation
	ctx := context.Background()
	stdDev, err := archery.StandardDeviation(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Print the result (rounded to 1 decimal place)
	fmt.Printf("Standard Deviation: %.1f\n", stdDev)

	// Output:
	// Standard Deviation: 1.4
}

func Example_count() {
	// Create a test array with some null values
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, []bool{true, true, false, true, true})
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Count non-null values
	ctx := context.Background()
	count, err := archery.Count(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Count null values
	nullCount := archery.CountNull(ctx, arr)

	// Print the results
	fmt.Printf("Count: %d\n", count)
	fmt.Printf("Null Count: %d\n", nullCount)

	// Output:
	// Count: 4
	// Null Count: 1
}
