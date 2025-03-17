package main

import (
	"context"
	"fmt"
	"os"

	"github.com/TFMV/archery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// This example demonstrates the basic functionality of the Archery library.
// It shows how to create Arrow arrays and perform various operations on them.

func main() {
	// Create a context for all operations
	ctx := context.Background()

	// Create a memory allocator (DefaultAllocator is recommended for most use cases)
	mem := memory.DefaultAllocator

	// Create and populate a sample Float64 array
	fmt.Println("=== Creating Arrow Arrays ===")
	numArray := createSampleArray(mem)
	defer archery.ReleaseArray(numArray)
	printArray("Original array", numArray)

	// Demonstrate arithmetic operations
	fmt.Println("\n=== Arithmetic Operations ===")
	if err := demonstrateArithmetic(ctx, numArray); err != nil {
		fmt.Printf("Error in arithmetic operations: %v\n", err)
		os.Exit(1)
	}

	// Demonstrate aggregation operations
	fmt.Println("\n=== Aggregation Operations ===")
	if err := demonstrateAggregation(ctx, numArray); err != nil {
		fmt.Printf("Error in aggregation operations: %v\n", err)
		os.Exit(1)
	}

	// Demonstrate filtering operations
	fmt.Println("\n=== Filtering Operations ===")
	if err := demonstrateFiltering(ctx, numArray); err != nil {
		fmt.Printf("Error in filtering operations: %v\n", err)
		os.Exit(1)
	}

	// Demonstrate sorting operations
	fmt.Println("\n=== Sorting Operations ===")
	if err := demonstrateSorting(ctx, numArray); err != nil {
		fmt.Printf("Error in sorting operations: %v\n", err)
		os.Exit(1)
	}

	// Demonstrate record operations
	fmt.Println("\n=== Record Operations ===")
	if err := demonstrateRecords(ctx, numArray); err != nil {
		fmt.Printf("Error in record operations: %v\n", err)
		os.Exit(1)
	}

	// Demonstrate null handling
	fmt.Println("\n=== Null Handling ===")
	if err := demonstrateNullHandling(ctx, mem); err != nil {
		fmt.Printf("Error in null handling: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nAll operations completed successfully!")
}

// createSampleArray creates a sample Float64 array with values [1.0, 2.0, 3.0, 4.0, 5.0]
func createSampleArray(mem memory.Allocator) arrow.Array {
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	// Add data
	builder.AppendValues([]float64{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
	return builder.NewFloat64Array()
}

// demonstrateArithmetic shows basic arithmetic operations on arrays
func demonstrateArithmetic(ctx context.Context, numArray arrow.Array) error {
	// Add a scalar
	result, err := archery.AddScalar(ctx, numArray, 10.0)
	if err != nil {
		return fmt.Errorf("adding scalar: %w", err)
	}
	defer archery.ReleaseArray(result)
	printArray("Add 10 to each element", result)

	// Multiply by 2
	result2, err := archery.MultiplyScalar(ctx, numArray, 2.0)
	if err != nil {
		return fmt.Errorf("multiplying by scalar: %w", err)
	}
	defer archery.ReleaseArray(result2)
	printArray("Multiply each element by 2", result2)

	// Take square root
	result3, err := archery.Sqrt(ctx, numArray)
	if err != nil {
		return fmt.Errorf("calculating square root: %w", err)
	}
	defer archery.ReleaseArray(result3)
	printArray("Square root of each element", result3)

	// Add two arrays
	result4, err := archery.Add(ctx, numArray, result2)
	if err != nil {
		return fmt.Errorf("adding arrays: %w", err)
	}
	defer archery.ReleaseArray(result4)
	printArray("Original + Doubled", result4)

	// Calculate absolute values
	// Create array with negative values
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{-1.0, 2.0, -3.0, 4.0, -5.0}, nil)
	negArray := builder.NewFloat64Array()
	defer archery.ReleaseArray(negArray)

	absResult, err := archery.Abs(ctx, negArray)
	if err != nil {
		return fmt.Errorf("calculating absolute values: %w", err)
	}
	defer archery.ReleaseArray(absResult)
	printArray("Absolute values of [-1, 2, -3, 4, -5]", absResult)

	return nil
}

// demonstrateAggregation shows aggregation operations on arrays
func demonstrateAggregation(ctx context.Context, numArray arrow.Array) error {
	// Calculate sum
	sum, err := archery.Sum(ctx, numArray)
	if err != nil {
		return fmt.Errorf("calculating sum: %w", err)
	}
	fmt.Printf("Sum: %.1f\n", sum)

	// Calculate mean
	mean, err := archery.Mean(ctx, numArray)
	if err != nil {
		return fmt.Errorf("calculating mean: %w", err)
	}
	fmt.Printf("Mean: %.1f\n", mean)

	// Calculate min and max
	min, err := archery.Min(ctx, numArray)
	if err != nil {
		return fmt.Errorf("calculating min: %w", err)
	}
	fmt.Printf("Min: %.1f\n", min)

	max, err := archery.Max(ctx, numArray)
	if err != nil {
		return fmt.Errorf("calculating max: %w", err)
	}
	fmt.Printf("Max: %.1f\n", max)

	// Calculate variance and standard deviation
	variance, err := archery.Variance(ctx, numArray)
	if err != nil {
		return fmt.Errorf("calculating variance: %w", err)
	}
	fmt.Printf("Variance: %.1f\n", variance)

	stdDev, err := archery.StandardDeviation(ctx, numArray)
	if err != nil {
		return fmt.Errorf("calculating standard deviation: %w", err)
	}
	fmt.Printf("Standard Deviation: %.1f\n", stdDev)

	return nil
}

// demonstrateFiltering shows filtering operations on arrays
func demonstrateFiltering(ctx context.Context, numArray arrow.Array) error {
	// Filter values greater than 2
	mask, err := archery.GreaterScalar(ctx, numArray, 2.0)
	if err != nil {
		return fmt.Errorf("creating mask: %w", err)
	}
	defer archery.ReleaseArray(mask)

	filtered, err := archery.Filter(ctx, numArray, mask)
	if err != nil {
		return fmt.Errorf("filtering: %w", err)
	}
	defer archery.ReleaseArray(filtered)
	printArray("Values greater than 2", filtered)

	// Filter values between 2 and 4 (inclusive)
	lowerMask, err := archery.GreaterEqualScalar(ctx, numArray, 2.0)
	if err != nil {
		return fmt.Errorf("creating lower mask: %w", err)
	}
	defer archery.ReleaseArray(lowerMask)

	upperMask, err := archery.LessEqualScalar(ctx, numArray, 4.0)
	if err != nil {
		return fmt.Errorf("creating upper mask: %w", err)
	}
	defer archery.ReleaseArray(upperMask)

	combinedMask, err := archery.And(ctx, lowerMask, upperMask)
	if err != nil {
		return fmt.Errorf("combining masks: %w", err)
	}
	defer archery.ReleaseArray(combinedMask)

	rangeFiltered, err := archery.Filter(ctx, numArray, combinedMask)
	if err != nil {
		return fmt.Errorf("range filtering: %w", err)
	}
	defer archery.ReleaseArray(rangeFiltered)
	printArray("Values between 2 and 4 (inclusive)", rangeFiltered)

	return nil
}

// demonstrateSorting shows sorting operations on arrays
func demonstrateSorting(ctx context.Context, numArray arrow.Array) error {
	// Create an unsorted array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{5.0, 3.0, 1.0, 4.0, 2.0}, nil)
	unsortedArray := builder.NewFloat64Array()
	defer archery.ReleaseArray(unsortedArray)
	printArray("Unsorted array", unsortedArray)

	// Sort in ascending order
	sortedAsc, err := archery.Sort(ctx, unsortedArray, archery.Ascending)
	if err != nil {
		return fmt.Errorf("sorting ascending: %w", err)
	}
	defer archery.ReleaseArray(sortedAsc)
	printArray("Sorted in ascending order", sortedAsc)

	// Sort in descending order
	sortedDesc, err := archery.Sort(ctx, unsortedArray, archery.Descending)
	if err != nil {
		return fmt.Errorf("sorting descending: %w", err)
	}
	defer archery.ReleaseArray(sortedDesc)
	printArray("Sorted in descending order", sortedDesc)

	// Get sort indices
	indices, err := archery.SortIndices(ctx, unsortedArray, archery.Ascending)
	if err != nil {
		return fmt.Errorf("getting sort indices: %w", err)
	}
	defer archery.ReleaseArray(indices)
	printArray("Sort indices (ascending)", indices)

	// Create array with duplicates for unique values demo
	dupBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	defer dupBuilder.Release()
	dupBuilder.AppendValues([]int64{1, 2, 2, 3, 1, 4, 5, 5}, nil)
	dupArray := dupBuilder.NewInt64Array()
	defer archery.ReleaseArray(dupArray)
	printArray("Array with duplicates", dupArray)

	// Get unique values
	unique, err := archery.UniqueValues(ctx, dupArray)
	if err != nil {
		return fmt.Errorf("getting unique values: %w", err)
	}
	defer archery.ReleaseArray(unique)
	printArray("Unique values", unique)

	// Count values
	values, counts, err := archery.CountValues(ctx, dupArray)
	if err != nil {
		return fmt.Errorf("counting values: %w", err)
	}
	defer archery.ReleaseArray(values)
	defer archery.ReleaseArray(counts)

	fmt.Println("Value counts:")
	for i := 0; i < values.Len(); i++ {
		fmt.Printf("  %d: %d\n", values.(*array.Int64).Value(i), counts.(*array.Int64).Value(i))
	}

	return nil
}

// demonstrateRecords shows operations on Arrow record batches
func demonstrateRecords(ctx context.Context, numArray arrow.Array) error {
	// Create a second column (doubled values)
	doubled, err := archery.MultiplyScalar(ctx, numArray, 2.0)
	if err != nil {
		return fmt.Errorf("creating doubled column: %w", err)
	}
	defer archery.ReleaseArray(doubled)

	// Create a schema
	fields := []arrow.Field{
		{Name: "values", Type: arrow.PrimitiveTypes.Float64},
		{Name: "doubled", Type: arrow.PrimitiveTypes.Float64},
	}
	schema := arrow.NewSchema(fields, nil)

	// Create columns
	columns := []arrow.Array{numArray, doubled}

	// Create a record batch
	record := array.NewRecord(schema, columns, int64(numArray.Len()))
	defer archery.ReleaseRecord(record)

	// Print record info
	fmt.Printf("Record has %d rows and %d columns\n", record.NumRows(), record.NumCols())
	fmt.Printf("Column names: %v\n", archery.ColumnNames(record))

	// Calculate sum of a column
	colSum, err := archery.SumColumn(ctx, record, "values")
	if err != nil {
		return fmt.Errorf("calculating column sum: %w", err)
	}
	fmt.Printf("Sum of 'values' column: %.1f\n", colSum)

	// Sort record by a column
	sortedRecord, err := archery.SortRecordByColumn(ctx, record, "values", archery.Ascending)
	if err != nil {
		return fmt.Errorf("sorting record: %w", err)
	}
	defer archery.ReleaseRecord(sortedRecord)
	fmt.Println("Record sorted by 'values' column (ascending)")

	// Filter record by condition
	mask, err := archery.GreaterScalar(ctx, numArray, 2.0)
	if err != nil {
		return fmt.Errorf("creating filter mask: %w", err)
	}
	defer archery.ReleaseArray(mask)

	filteredRecord, err := archery.FilterRecord(ctx, record, mask)
	if err != nil {
		return fmt.Errorf("filtering record: %w", err)
	}
	defer archery.ReleaseRecord(filteredRecord)
	fmt.Printf("Filtered record (values > 2) has %d rows\n", filteredRecord.NumRows())

	return nil
}

// demonstrateNullHandling shows how to work with null values in arrays
func demonstrateNullHandling(ctx context.Context, mem memory.Allocator) error {
	// Create an array with some null values
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	// Add values with nulls (true = valid, false = null)
	builder.AppendValues([]float64{1.0, 2.0, 3.0, 4.0, 5.0}, []bool{true, false, true, false, true})
	arrWithNulls := builder.NewFloat64Array()
	defer archery.ReleaseArray(arrWithNulls)

	fmt.Printf("Array with nulls (length: %d, null count: %d)\n", arrWithNulls.Len(), arrWithNulls.NullN())

	// Print array with nulls
	fmt.Print("Values: [")
	for i := 0; i < arrWithNulls.Len(); i++ {
		if i > 0 {
			fmt.Print(", ")
		}
		if arrWithNulls.IsNull(i) {
			fmt.Print("null")
		} else {
			fmt.Printf("%.1f", arrWithNulls.Value(i))
		}
	}
	fmt.Println("]")

	// Create a mask for non-null values
	maskBuilder := array.NewBooleanBuilder(mem)
	defer maskBuilder.Release()

	for i := 0; i < arrWithNulls.Len(); i++ {
		maskBuilder.Append(!arrWithNulls.IsNull(i))
	}

	mask := maskBuilder.NewBooleanArray()
	defer mask.Release()

	// Filter out nulls
	nonNullArr, err := archery.Filter(ctx, arrWithNulls, mask)
	if err != nil {
		return fmt.Errorf("filtering nulls: %w", err)
	}
	defer archery.ReleaseArray(nonNullArr)

	printArray("Non-null values", nonNullArr)
	fmt.Printf("Null count: %d\n", archery.CountNull(ctx, arrWithNulls))

	return nil
}

// Helper function to print an array
func printArray(label string, arr arrow.Array) {
	fmt.Printf("%s: [", label)
	for i := 0; i < arr.Len(); i++ {
		if i > 0 {
			fmt.Print(", ")
		}
		if arr.IsNull(i) {
			fmt.Print("null")
			continue
		}

		switch a := arr.(type) {
		case *array.Float64:
			fmt.Printf("%.1f", a.Value(i))
		case *array.Int64:
			fmt.Printf("%d", a.Value(i))
		case *array.Boolean:
			fmt.Printf("%t", a.Value(i))
		default:
			fmt.Print("?")
		}
	}
	fmt.Println("]")
}
