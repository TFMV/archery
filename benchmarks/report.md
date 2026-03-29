# SIMD Evaluation Report (archery)

## Status

This repository now contains the benchmark harness required for the controlled experiment. Running the full empirical phases requires a Go 1.26 toolchain with `GOEXPERIMENT=simd` enabled.

In this execution environment, the Go 1.26 toolchain download from `proxy.golang.org` is blocked, so numerical benchmark/profile outputs are not yet available.

## Phase 1 — Function classification

### Category A — Keep Arrow compute (do not replace)

These operations currently dispatch to Arrow compute kernels and should remain compute-first:

- Arithmetic array ops: `Add`, `Subtract`, `Multiply`, `Divide`, `Power`, `Abs`, `Negate`, `Sqrt`, `Sign`.
- Scalar arithmetic: `AddScalar`, `SubtractScalar`, `MultiplyScalar`, `DivideScalar`, `PowerScalar`.
- Filter/comparison wrappers: `Filter`, `Equal`, `NotEqual`, `Greater`, `GreaterEqual`, `Less`, `LessEqual`, `And`, `Or`, `Xor`, `Invert`.
- Scalar comparisons: `EqualScalar`, `NotEqualScalar`, `GreaterScalar`, `GreaterEqualScalar`, `LessScalar`, `LessEqualScalar`.

### Category B — SIMD candidates (manual loops)

These use manual loops over Arrow arrays with null checks and are eligible for SIMD experiments:

- Aggregations in `aggregation.go`: `Sum`, `Mean` (via `Sum` + scalar finalization), `Min`, `Max`, `Variance`.
- Additional future candidates: any new manual array scans added for custom filtering not backed by Arrow compute.

## Phase 0 / 3 benchmark commands

Baseline:

```bash
go test ./benchmarks -bench=. -benchmem > benchmarks/baseline.txt
go test ./benchmarks -bench=AddScalar -cpuprofile=benchmarks/addscalar_baseline.prof
go test ./benchmarks -bench=Sum -cpuprofile=benchmarks/sum_baseline.prof
go test ./benchmarks -bench=GreaterScalar -cpuprofile=benchmarks/filter_baseline.prof
```

SIMD-enabled:

```bash
export GOEXPERIMENT=simd
go test ./benchmarks -bench=. -benchmem > benchmarks/simd.txt
go test ./benchmarks -bench=AddScalar -cpuprofile=benchmarks/addscalar_simd.prof
go test ./benchmarks -bench=Sum -cpuprofile=benchmarks/sum_simd.prof
go test ./benchmarks -bench=GreaterScalar -cpuprofile=benchmarks/filter_simd.prof
```

## Benchmark table

Fill after running the commands above:

| Operation | Baseline ns/op | SIMD ns/op | Speedup |
|---|---:|---:|---:|
| AddScalar (int64, 1e5) | TBD | TBD | TBD |
| AddScalar (int64, 1e6) | TBD | TBD | TBD |
| AddScalar (float64, 1e5) | TBD | TBD | TBD |
| AddScalar (float64, 1e6) | TBD | TBD | TBD |
| MultiplyScalar (int64, 1e5) | TBD | TBD | TBD |
| MultiplyScalar (int64, 1e6) | TBD | TBD | TBD |
| MultiplyScalar (float64, 1e5) | TBD | TBD | TBD |
| MultiplyScalar (float64, 1e6) | TBD | TBD | TBD |
| Sum (int64, 1e5) | TBD | TBD | TBD |
| Sum (int64, 1e6) | TBD | TBD | TBD |
| Sum (float64, 1e5) | TBD | TBD | TBD |
| Sum (float64, 1e6) | TBD | TBD | TBD |
| Mean (int64, 1e5) | TBD | TBD | TBD |
| Mean (int64, 1e6) | TBD | TBD | TBD |
| Mean (float64, 1e5) | TBD | TBD | TBD |
| Mean (float64, 1e6) | TBD | TBD | TBD |
| Min (int64, 1e5) | TBD | TBD | TBD |
| Min (int64, 1e6) | TBD | TBD | TBD |
| Min (float64, 1e5) | TBD | TBD | TBD |
| Min (float64, 1e6) | TBD | TBD | TBD |
| Max (int64, 1e5) | TBD | TBD | TBD |
| Max (int64, 1e6) | TBD | TBD | TBD |
| Max (float64, 1e5) | TBD | TBD | TBD |
| Max (float64, 1e6) | TBD | TBD | TBD |
| Variance (int64, 1e5) | TBD | TBD | TBD |
| Variance (int64, 1e6) | TBD | TBD | TBD |
| Variance (float64, 1e5) | TBD | TBD | TBD |
| Variance (float64, 1e6) | TBD | TBD | TBD |
| GreaterScalar (int64, 1e5) | TBD | TBD | TBD |
| GreaterScalar (int64, 1e6) | TBD | TBD | TBD |
| GreaterScalar (float64, 1e5) | TBD | TBD | TBD |
| GreaterScalar (float64, 1e6) | TBD | TBD | TBD |
| Equal (int64, 1e5) | TBD | TBD | TBD |
| Equal (int64, 1e6) | TBD | TBD | TBD |
| Equal (float64, 1e5) | TBD | TBD | TBD |
| Equal (float64, 1e6) | TBD | TBD | TBD |
| Filter (int64, 1e5) | TBD | TBD | TBD |
| Filter (int64, 1e6) | TBD | TBD | TBD |
| Filter (float64, 1e5) | TBD | TBD | TBD |
| Filter (float64, 1e6) | TBD | TBD | TBD |

## CPU profile comparison checklist

When profiles are captured, evaluate:

1. Hotspot shifts from scalar per-element loops to vector chunks.
2. Branch reductions (especially null checks).
3. Whether kernels become memory-bandwidth bound.
4. Whether bitmap/null handling dominates vector math costs.

## Findings template

### SIMD wins
- Fill with operations/sizes where speedup is stable and significant.

### SIMD neutral
- Fill where speedup is small or noisy.

### SIMD regressions
- Fill where SIMD loses to scalar/compute.

## Root-cause analysis template

- **Memory bandwidth limits:**
- **Null bitmap overhead:**
- **Branch misprediction:**
- **Vector underutilization/tail costs:**
- **Compute-kernel overhead vs raw loops:**

## Complexity cost

- `unsafe.Pointer` and buffer-level access increase correctness risk.
- Build-tag fragmentation (`amd64 && goexperiment.simd`) increases test matrix size.
- Debugging SIMD codepaths is harder than scalar loops.
- Optional SIMD kernels should stay isolated behind internal interfaces with mandatory scalar fallback.

## Final decision matrix (to complete with data)

Keep SIMD for:
- TBD based on repeatable benchmark wins.

Do NOT use SIMD for:
- Arrow compute-backed operations where compute remains faster or simpler.
- Manual loops where null handling + memory traffic erase vector gains.

## Final question answer (current)

At this point, **there is not yet empirical evidence in this environment** to claim SIMD should bypass Arrow compute generally. The architecture should remain compute-first, with SIMD considered only for manual-loop Category B functions once reproducible benchmark/profile data is collected.
