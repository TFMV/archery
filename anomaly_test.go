package archery_test

import (
	"context"
	"fmt"

	"github.com/TFMV/archery"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func Example_detectAnomalies() {
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewFloat64Array()
	defer arr.Release()

	ctx := context.Background()
	res, err := archery.DetectAnomalies(ctx, arr, 1.0)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer res.Release()

	fmt.Println("Mask:")
	for i := 0; i < res.Mask.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		if res.Mask.Value(i) {
			fmt.Print("1")
		} else {
			fmt.Print("0")
		}
	}
	fmt.Println()

	fmt.Println("Z-scores:")
	for i := 0; i < res.Zscore.Len(); i++ {
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("%.1f", res.Zscore.Value(i))
	}
	fmt.Println()

	// Output:
	// Mask:
	// 1 0 0 0 1
	// Z-scores:
	// -1.4 -0.7 0.0 0.7 1.4
}
