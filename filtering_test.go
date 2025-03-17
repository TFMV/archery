package archery_test

import (
	"context"
	"fmt"

	"github.com/TFMV/archery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func Example_filter() {
	// Create a test array
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	arr := builder.NewInt64Array()
	defer arr.Release()

	// Create a mask for values > 5
	ctx := context.Background()
	mask, err := archery.GreaterScalar(ctx, arr, int64(5))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(mask)

	// Apply the mask
	filtered, err := archery.Filter(ctx, arr, mask)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(filtered)

	// Print the filtered array
	fmt.Println("Values > 5:")
	for i := 0; i < filtered.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%d", filtered.(*array.Int64).Value(i))
	}
	fmt.Println()

	// Output:
	// Values > 5:
	// 6 7 8 9 10
}

func Example_filterRange() {
	// Create a test array
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	arr := builder.NewInt64Array()
	defer arr.Release()

	// Create masks for 3 <= x <= 7
	ctx := context.Background()
	lowerMask, err := archery.GreaterEqualScalar(ctx, arr, int64(3))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(lowerMask)

	upperMask, err := archery.LessEqualScalar(ctx, arr, int64(7))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(upperMask)

	// Combine masks
	combinedMask, err := archery.And(ctx, lowerMask, upperMask)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(combinedMask)

	// Apply the combined mask
	filtered, err := archery.Filter(ctx, arr, combinedMask)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(filtered)

	// Print the filtered array
	fmt.Println("Values between 3 and 7:")
	for i := 0; i < filtered.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%d", filtered.(*array.Int64).Value(i))
	}
	fmt.Println()

	// Output:
	// Values between 3 and 7:
	// 3 4 5 6 7
}

func Example_isIn() {
	// Create a test array
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	arr := builder.NewInt64Array()
	defer arr.Release()

	// Create a set of values to check against
	setValues := []int64{2, 4, 6, 8, 10}

	// Create a mask for values in the set
	ctx := context.Background()

	// Create individual masks for each value in the set
	masks := make([]arrow.Array, len(setValues))
	for i, val := range setValues {
		mask, err := archery.EqualScalar(ctx, arr, val)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		masks[i] = mask
	}

	// Combine all masks with OR
	var combinedMask arrow.Array
	if len(masks) > 0 {
		combinedMask = masks[0]
		for i := 1; i < len(masks); i++ {
			newMask, err := archery.Or(ctx, combinedMask, masks[i])
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			combinedMask.Release()
			masks[i].Release()
			combinedMask = newMask
		}
	}
	defer archery.ReleaseArray(combinedMask)

	// Apply the mask
	filtered, err := archery.Filter(ctx, arr, combinedMask)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(filtered)

	// Print the filtered array
	fmt.Println("Values in set [2, 4, 6, 8, 10]:")
	for i := 0; i < filtered.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%d", filtered.(*array.Int64).Value(i))
	}
	fmt.Println()

	// Output:
	// Values in set [2, 4, 6, 8, 10]:
	// 2 4 6 8 10
}

func Example_nullFiltering() {
	// Create a test array with nulls
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	// Add values with some nulls
	builder.AppendValues([]int64{1, 2, 3}, []bool{true, false, true})
	builder.AppendNull()
	builder.AppendValues([]int64{5, 6}, []bool{true, true})

	arr := builder.NewInt64Array()
	defer arr.Release()

	// Create a manual mask for non-null values
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
		fmt.Println("Error:", err)
		return
	}
	defer archery.ReleaseArray(nonNullArr)

	// Print the non-null values
	fmt.Println("Non-null values:")
	for i := 0; i < nonNullArr.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%d", nonNullArr.(*array.Int64).Value(i))
	}
	fmt.Println()

	// Count nulls
	nullCount := archery.CountNull(ctx, arr)
	fmt.Printf("Null count: %d\n", nullCount)

	// Output:
	// Non-null values:
	// 1 3 5 6
	// Null count: 2
}
