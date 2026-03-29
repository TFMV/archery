package benchmarks

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/TFMV/archery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var benchCtx = context.Background()

func buildInt64Array(size int) *array.Int64 {
	pool := memory.NewGoAllocator()
	b := array.NewInt64Builder(pool)
	defer b.Release()

	vals := make([]int64, size)
	valid := make([]bool, size)
	for i := 0; i < size; i++ {
		vals[i] = int64(i % 1024)
		valid[i] = i%10 != 0 // ~10% nulls
	}
	b.AppendValues(vals, valid)
	return b.NewInt64Array()
}

func buildFloat64Array(size int) *array.Float64 {
	pool := memory.NewGoAllocator()
	b := array.NewFloat64Builder(pool)
	defer b.Release()

	vals := make([]float64, size)
	valid := make([]bool, size)
	for i := 0; i < size; i++ {
		vals[i] = math.Sin(float64(i)) * 1000
		valid[i] = i%10 != 0 // ~10% nulls
	}
	b.AppendValues(vals, valid)
	return b.NewFloat64Array()
}

func buildBoolMask(size int) *array.Boolean {
	pool := memory.NewGoAllocator()
	b := array.NewBooleanBuilder(pool)
	defer b.Release()

	vals := make([]bool, size)
	valid := make([]bool, size)
	for i := 0; i < size; i++ {
		vals[i] = i%3 == 0
		valid[i] = true
	}
	b.AppendValues(vals, valid)
	return b.NewBooleanArray()
}

func benchmarkSizes() []int {
	return []int{100_000, 1_000_000}
}

func benchmarkTypes() []struct {
	name string
	dt   arrow.Type
} {
	return []struct {
		name string
		dt   arrow.Type
	}{
		{name: "int64", dt: arrow.INT64},
		{name: "float64", dt: arrow.FLOAT64},
	}
}

func BenchmarkAddScalar(b *testing.B) {
	for _, size := range benchmarkSizes() {
		for _, typ := range benchmarkTypes() {
			name := fmt.Sprintf("%s/n=%d", typ.name, size)
			b.Run(name, func(b *testing.B) {
				var arr arrow.Array
				scalar := any(int64(3))
				if typ.dt == arrow.INT64 {
					a := buildInt64Array(size)
					defer a.Release()
					arr = a
				} else {
					a := buildFloat64Array(size)
					defer a.Release()
					arr = a
					scalar = float64(3.5)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := archery.AddScalar(benchCtx, arr, scalar)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}

func BenchmarkMultiplyScalar(b *testing.B) {
	for _, size := range benchmarkSizes() {
		for _, typ := range benchmarkTypes() {
			name := fmt.Sprintf("%s/n=%d", typ.name, size)
			b.Run(name, func(b *testing.B) {
				var arr arrow.Array
				scalar := any(int64(7))
				if typ.dt == arrow.INT64 {
					a := buildInt64Array(size)
					defer a.Release()
					arr = a
				} else {
					a := buildFloat64Array(size)
					defer a.Release()
					arr = a
					scalar = float64(7.25)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := archery.MultiplyScalar(benchCtx, arr, scalar)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}

func BenchmarkSum(b *testing.B) {
	benchmarkAggregate(b, "sum", func(arr arrow.Array) error {
		_, err := archery.Sum(benchCtx, arr)
		return err
	})
}

func BenchmarkMean(b *testing.B) {
	benchmarkAggregate(b, "mean", func(arr arrow.Array) error {
		_, err := archery.Mean(benchCtx, arr)
		return err
	})
}

func BenchmarkMin(b *testing.B) {
	benchmarkAggregate(b, "min", func(arr arrow.Array) error {
		_, err := archery.Min(benchCtx, arr)
		return err
	})
}

func BenchmarkMax(b *testing.B) {
	benchmarkAggregate(b, "max", func(arr arrow.Array) error {
		_, err := archery.Max(benchCtx, arr)
		return err
	})
}

func BenchmarkVariance(b *testing.B) {
	benchmarkAggregate(b, "variance", func(arr arrow.Array) error {
		_, err := archery.Variance(benchCtx, arr)
		return err
	})
}

func benchmarkAggregate(b *testing.B, _ string, fn func(arr arrow.Array) error) {
	for _, size := range benchmarkSizes() {
		for _, typ := range benchmarkTypes() {
			name := fmt.Sprintf("%s/n=%d", typ.name, size)
			b.Run(name, func(b *testing.B) {
				var arr arrow.Array
				if typ.dt == arrow.INT64 {
					a := buildInt64Array(size)
					defer a.Release()
					arr = a
				} else {
					a := buildFloat64Array(size)
					defer a.Release()
					arr = a
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := fn(arr); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkGreaterScalar(b *testing.B) {
	for _, size := range benchmarkSizes() {
		for _, typ := range benchmarkTypes() {
			name := fmt.Sprintf("%s/n=%d", typ.name, size)
			b.Run(name, func(b *testing.B) {
				var arr arrow.Array
				scalar := any(int64(128))
				if typ.dt == arrow.INT64 {
					a := buildInt64Array(size)
					defer a.Release()
					arr = a
				} else {
					a := buildFloat64Array(size)
					defer a.Release()
					arr = a
					scalar = float64(64)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := archery.GreaterScalar(benchCtx, arr, scalar)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}

func BenchmarkEqual(b *testing.B) {
	for _, size := range benchmarkSizes() {
		for _, typ := range benchmarkTypes() {
			name := fmt.Sprintf("%s/n=%d", typ.name, size)
			b.Run(name, func(b *testing.B) {
				var left, right arrow.Array
				if typ.dt == arrow.INT64 {
					a := buildInt64Array(size)
					defer a.Release()
					left = a
					c := buildInt64Array(size)
					defer c.Release()
					right = c
				} else {
					a := buildFloat64Array(size)
					defer a.Release()
					left = a
					c := buildFloat64Array(size)
					defer c.Release()
					right = c
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := archery.Equal(benchCtx, left, right)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}

func BenchmarkFilter(b *testing.B) {
	for _, size := range benchmarkSizes() {
		for _, typ := range benchmarkTypes() {
			name := fmt.Sprintf("%s/n=%d", typ.name, size)
			b.Run(name, func(b *testing.B) {
				var input arrow.Array
				if typ.dt == arrow.INT64 {
					a := buildInt64Array(size)
					defer a.Release()
					input = a
				} else {
					a := buildFloat64Array(size)
					defer a.Release()
					input = a
				}
				mask := buildBoolMask(size)
				defer mask.Release()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := archery.Filter(benchCtx, input, mask)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
		}
	}
}
