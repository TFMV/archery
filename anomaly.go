package archery

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// AnomalyResult holds mask and z-scores for anomalies.
type AnomalyResult struct {
	Mask   *array.Boolean
	Zscore *array.Float64
}

// Release frees memory associated with the AnomalyResult.
func (r *AnomalyResult) Release() {
	if r.Mask != nil {
		r.Mask.Release()
	}
	if r.Zscore != nil {
		r.Zscore.Release()
	}
}

// computeMeanAndVariance calculates mean and variance for a Float64 array.
// TODO(archery): replace with compute.mean when supported
func computeMeanAndVariance(col *array.Float64) (mean, variance float64) {
	var sum, sumsq float64
	var count int
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Value(i)
		sum += v
		sumsq += v * v
		count++
	}
	if count == 0 {
		return 0, 0
	}
	mean = sum / float64(count)
	// Population variance: sum of squared differences from mean
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		diff := col.Value(i) - mean
		variance += diff * diff
	}
	variance /= float64(count)
	return
}

// DetectAnomalies computes z-scores and a boolean mask using Arrow compute functions.
func DetectAnomalies(ctx context.Context, col arrow.Array, threshold float64) (*AnomalyResult, error) {
	floatCol, ok := col.(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("input must be Float64 array, got %T", col)
	}

	mean, variance := computeMeanAndVariance(floatCol)

	meanScalar := scalar.NewFloat64Scalar(mean)
	varianceScalar := scalar.NewFloat64Scalar(variance)

	stdDevRes, err := compute.CallFunction(ctx, "sqrt", nil, compute.NewDatum(varianceScalar))
	if err != nil {
		return nil, fmt.Errorf("sqrt computation: %w", err)
	}
	defer stdDevRes.Release()

	stdDev := stdDevRes.(*compute.ScalarDatum).Value.(*scalar.Float64).Value
	stdDevScalar := scalar.NewFloat64Scalar(stdDev)

	diffRes, err := compute.CallFunction(ctx, "subtract", nil, compute.NewDatum(col), compute.NewDatum(meanScalar))
	if err != nil {
		return nil, fmt.Errorf("subtract computation: %w", err)
	}
	defer diffRes.Release()

	zRes, err := compute.CallFunction(ctx, "divide", nil, diffRes, compute.NewDatum(stdDevScalar))
	if err != nil {
		return nil, fmt.Errorf("divide computation: %w", err)
	}
	defer zRes.Release()

	absRes, err := compute.CallFunction(ctx, "abs", nil, zRes)
	if err != nil {
		return nil, fmt.Errorf("abs computation: %w", err)
	}
	defer absRes.Release()

	zArr := zRes.(*compute.ArrayDatum).MakeArray().(*array.Float64)

	threshScalar := scalar.NewFloat64Scalar(threshold)
	compRes, err := compute.CallFunction(ctx, "greater_equal", nil, absRes, compute.NewDatum(threshScalar))
	if err != nil {
		zArr.Release()
		return nil, fmt.Errorf("threshold comparison: %w", err)
	}
	defer compRes.Release()

	maskArr := compRes.(*compute.ArrayDatum).MakeArray().(*array.Boolean)

	return &AnomalyResult{Mask: maskArr, Zscore: zArr}, nil
}
