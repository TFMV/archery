package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/TFMV/archery"
)

func main() {
	// Create a memory allocator
	mem := memory.NewGoAllocator()

	// Create a schema for our record
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int64},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Create builders for our columns
	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()

	ageBuilder := array.NewInt64Builder(mem)
	defer ageBuilder.Release()

	scoreBuilder := array.NewFloat64Builder(mem)
	defer scoreBuilder.Release()

	// Add data to the builders
	idBuilder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie", "Dave", "Eve"}, nil)
	ageBuilder.AppendValues([]int64{25, 30, 35, 40, 45}, nil)
	scoreBuilder.AppendValues([]float64{90.5, 85.0, 95.5, 75.0, 80.5}, nil)

	// Create arrays from the builders
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	nameArray := nameBuilder.NewArray()
	defer nameArray.Release()

	ageArray := ageBuilder.NewArray()
	defer ageArray.Release()

	scoreArray := scoreBuilder.NewArray()
	defer scoreArray.Release()

	// Create a record batch
	record := array.NewRecord(schema, []arrow.Array{idArray, nameArray, ageArray, scoreArray}, 5)
	defer record.Release()

	fmt.Println("Original Record:")
	printRecord(record)

	// Create a RecordWrapper
	wrapper := archery.NewRecordWrapper(record, mem)

	// Demonstrate filtering
	demonstrateFiltering(wrapper)

	// Demonstrate sorting
	demonstrateSorting(wrapper)

	// Demonstrate aggregation
	demonstrateAggregation(wrapper)

	// Demonstrate grouping
	demonstrateGroupBy()
}

func printRecord(record arrow.Record) {
	schema := record.Schema()
	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	// Print header
	for i := 0; i < schema.NumFields(); i++ {
		fmt.Printf("| %-10s ", schema.Field(i).Name)
	}
	fmt.Println("|")

	// Print separator
	for i := 0; i < schema.NumFields(); i++ {
		fmt.Printf("| %-10s ", "----------")
	}
	fmt.Println("|")

	// Print data
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		for colIdx := 0; colIdx < numCols; colIdx++ {
			col := record.Column(colIdx)
			if col.IsNull(rowIdx) {
				fmt.Printf("| %-10s ", "NULL")
				continue
			}

			switch col.DataType().ID() {
			case arrow.INT64:
				fmt.Printf("| %-10d ", col.(*array.Int64).Value(rowIdx))
			case arrow.FLOAT64:
				fmt.Printf("| %-10.2f ", col.(*array.Float64).Value(rowIdx))
			case arrow.STRING:
				fmt.Printf("| %-10s ", col.(*array.String).Value(rowIdx))
			case arrow.BOOL:
				fmt.Printf("| %-10t ", col.(*array.Boolean).Value(rowIdx))
			default:
				fmt.Printf("| %-10s ", "???")
			}
		}
		fmt.Println("|")
	}
	fmt.Println()
}

func demonstrateFiltering(wrapper *archery.RecordWrapper) {
	ctx := context.Background()

	// Filter records where age > 30
	fmt.Println("Filtering records where age > 30:")
	filteredByAge, err := wrapper.FilterRowsByColumn(ctx, "age", archery.GreaterThan(int64(30)))
	if err != nil {
		log.Fatalf("Error filtering by age: %v", err)
	}
	defer filteredByAge.Release()
	printRecord(filteredByAge)

	// Filter records where score is between 80 and 90
	fmt.Println("Filtering records where score is between 80 and 90:")
	filteredByScore, err := wrapper.FilterRowsByColumn(ctx, "score", archery.Between(80.0, 90.0))
	if err != nil {
		log.Fatalf("Error filtering by score: %v", err)
	}
	defer filteredByScore.Release()
	printRecord(filteredByScore)

	// Filter records with custom predicate (even IDs)
	fmt.Println("Filtering records with even IDs:")
	filteredByCustom, err := wrapper.FilterRowsByColumn(ctx, "id", func(arr arrow.Array, i int) bool {
		idArr := arr.(*array.Int64)
		return idArr.Value(i)%2 == 0
	})
	if err != nil {
		log.Fatalf("Error filtering by custom predicate: %v", err)
	}
	defer filteredByCustom.Release()
	printRecord(filteredByCustom)
}

func demonstrateSorting(wrapper *archery.RecordWrapper) {
	ctx := context.Background()

	// Sort by age in ascending order
	fmt.Println("Sorting by age (ascending):")
	sortedByAge, err := wrapper.SortRecord(ctx, "age", archery.Ascending)
	if err != nil {
		log.Fatalf("Error sorting by age: %v", err)
	}
	defer sortedByAge.Release()
	printRecord(sortedByAge)

	// Sort by score in descending order
	fmt.Println("Sorting by score (descending):")
	sortedByScore, err := wrapper.SortRecord(ctx, "score", archery.Descending)
	if err != nil {
		log.Fatalf("Error sorting by score: %v", err)
	}
	defer sortedByScore.Release()
	printRecord(sortedByScore)
}

func demonstrateAggregation(wrapper *archery.RecordWrapper) {
	ctx := context.Background()

	// Calculate sum of ages
	ageSum, err := wrapper.AggregateColumn(ctx, "age", archery.SumAggregator())
	if err != nil {
		log.Fatalf("Error calculating sum of ages: %v", err)
	}
	fmt.Printf("Sum of ages: %v\n", ageSum)

	// Calculate mean of scores
	scoreMean, err := wrapper.AggregateColumn(ctx, "score", archery.MeanAggregator())
	if err != nil {
		log.Fatalf("Error calculating mean of scores: %v", err)
	}
	fmt.Printf("Mean of scores: %.2f\n", scoreMean)

	// Calculate min and max ages
	ageMin, err := wrapper.AggregateColumn(ctx, "age", archery.MinAggregator())
	if err != nil {
		log.Fatalf("Error calculating min age: %v", err)
	}
	fmt.Printf("Min age: %v\n", ageMin)

	ageMax, err := wrapper.AggregateColumn(ctx, "age", archery.MaxAggregator())
	if err != nil {
		log.Fatalf("Error calculating max age: %v", err)
	}
	fmt.Printf("Max age: %v\n", ageMax)
	fmt.Println()
}

func demonstrateGroupBy() {
	// Create a memory allocator
	mem := memory.NewGoAllocator()

	// Create a schema for our record
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int64},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64},
			{Name: "category", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Create builders for our columns
	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()

	ageBuilder := array.NewInt64Builder(mem)
	defer ageBuilder.Release()

	scoreBuilder := array.NewFloat64Builder(mem)
	defer scoreBuilder.Release()

	categoryBuilder := array.NewStringBuilder(mem)
	defer categoryBuilder.Release()

	// Add data to the builders
	idBuilder.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8}, nil)
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Heidi"}, nil)
	ageBuilder.AppendValues([]int64{25, 30, 35, 40, 45, 25, 30, 35}, nil)
	scoreBuilder.AppendValues([]float64{90.5, 85.0, 95.5, 75.0, 80.5, 92.0, 88.0, 79.0}, nil)
	categoryBuilder.AppendValues([]string{"A", "B", "A", "B", "A", "B", "A", "B"}, nil)

	// Create arrays from the builders
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	nameArray := nameBuilder.NewArray()
	defer nameArray.Release()

	ageArray := ageBuilder.NewArray()
	defer ageArray.Release()

	scoreArray := scoreBuilder.NewArray()
	defer scoreArray.Release()

	categoryArray := categoryBuilder.NewArray()
	defer categoryArray.Release()

	// Create a record batch
	record := array.NewRecord(schema, []arrow.Array{idArray, nameArray, ageArray, scoreArray, categoryArray}, 8)
	defer record.Release()

	fmt.Println("Record with categories:")
	printRecord(record)

	// Create a RecordWrapper
	wrapper := archery.NewRecordWrapper(record, mem)

	// Group by category and calculate mean age and score
	fmt.Println("Grouping by category and calculating mean age and score:")
	ctx := context.Background()
	groupByResult, err := wrapper.GroupBy(ctx, []string{"category"}, map[string]func(context.Context, arrow.Array) (interface{}, error){
		"age":   archery.MeanAggregator(),
		"score": archery.MeanAggregator(),
	})
	if err != nil {
		log.Fatalf("Error grouping by category: %v", err)
	}
	defer groupByResult.Release()

	// Convert the result to a record
	groupedRecord := groupByResult.ToRecord(mem)
	defer groupedRecord.Release()

	printRecord(groupedRecord)
}
