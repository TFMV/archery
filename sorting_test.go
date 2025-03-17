package archery_test

import (
	"context"
	"fmt"

	"github.com/TFMV/archery"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func Example_sort() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{5, 3, 1, 4, 2}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Sort the array in ascending order
	ctx := context.Background()
	sorted, err := archery.Sort(ctx, arr, archery.Ascending)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(sorted)

	// Print the sorted array
	fmt.Println("Sorted (ascending):")
	for i := 0; i < sorted.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%.1f", sorted.(*array.Float64).Value(i))
	}
	fmt.Println()

	// Output:
	// Sorted (ascending):
	// 1.0 2.0 3.0 4.0 5.0
}

func Example_sortDescending() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{5, 3, 1, 4, 2}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Sort the array in descending order
	ctx := context.Background()
	sorted, err := archery.Sort(ctx, arr, archery.Descending)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(sorted)

	// Print the sorted array
	fmt.Println("Sorted (descending):")
	for i := 0; i < sorted.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%.1f", sorted.(*array.Float64).Value(i))
	}
	fmt.Println()

	// Output:
	// Sorted (descending):
	// 5.0 4.0 3.0 2.0 1.0
}

func Example_sortIndices() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{5, 3, 1, 4, 2}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Get sort indices
	ctx := context.Background()
	indices, err := archery.SortIndices(ctx, arr, archery.Ascending)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(indices)

	// Print the indices
	fmt.Println("Sort indices:")
	for i := 0; i < indices.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%d", indices.(*array.Int64).Value(i))
	}
	fmt.Println()

	// Output:
	// Sort indices:
	// 2 4 1 3 0
}

func Example_uniqueValues() {
	// Create a test array with duplicates
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 2, 3, 1, 4, 5, 5}, nil)
	arr := builder.NewInt64Array()
	defer arr.Release()

	// Get unique values
	ctx := context.Background()
	unique, err := archery.UniqueValues(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(unique)

	// Print the unique values
	fmt.Println("Unique values:")
	for i := 0; i < unique.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%d", unique.(*array.Int64).Value(i))
	}
	fmt.Println()

	// Output:
	// Unique values:
	// 1 2 3 4 5
}

func Example_countValues() {
	// Create a test array with duplicates
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 2, 3, 1, 4, 5, 5}, nil)
	arr := builder.NewInt64Array()
	defer arr.Release()

	// Count values
	ctx := context.Background()
	values, counts, err := archery.CountValues(ctx, arr)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(values)
	defer archery.ReleaseArray(counts)

	// Print the values and counts
	fmt.Println("Value counts:")
	for i := 0; i < values.Len(); i++ {
		fmt.Printf("%d: %d\n", values.(*array.Int64).Value(i), counts.(*array.Int64).Value(i))
	}

	// Output:
	// Value counts:
	// 1: 2
	// 2: 2
	// 3: 1
	// 4: 1
	// 5: 2
}

func Example_nthElement() {
	// Create a test array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{5, 3, 1, 4, 2}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	// Get the 2nd element (0-indexed) in sorted order
	ctx := context.Background()
	element, err := archery.NthElement(ctx, arr, 2, archery.Ascending)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Print the result
	fmt.Printf("3rd smallest element: %.1f\n", element)

	// Output:
	// 3rd smallest element: 3.0
}
