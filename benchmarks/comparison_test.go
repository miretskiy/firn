package benchmarks

import (
	"testing"

	"github.com/miretskiy/turbo-polars/pkg/polars"
)

// Benchmark scenarios demonstrating turbo-polars performance characteristics

func BenchmarkTurboPolarsOperations(b *testing.B) {
	b.Run("PureCGOOverhead", func(b *testing.B) {
		// Pure CGO overhead - no Rust work at all
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			polars.NoopCGOCall()
		}
	})

	b.Run("EmptyDataFrame", func(b *testing.B) {
		// True baseline: Just CGO overhead with minimal work
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df := polars.NewDataFrame()
			_, err := df.Execute()
			if err != nil {
				b.Fatal(err)
			}
			df.Release()
		}
	})

	b.Run("ReadCSV", func(b *testing.B) {
		// CSV reading: CGO overhead + parsing work
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df := polars.ReadCSV("datasets/iris.csv")
			_, err := df.Execute()
			if err != nil {
				b.Fatal(err)
			}
			df.Release()
		}
	})

	b.Run("SimpleFilter", func(b *testing.B) {
		// ReadCSV + Filter: 2 operations batched into 1 CGO call
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df := polars.ReadCSV("datasets/iris.csv").
				Filter(polars.Col("petal.length").Gt(polars.Lit(1.0)))
			_, err := df.Execute()
			if err != nil {
				b.Fatal(err)
			}
			df.Release()
		}
	})

	b.Run("ComplexChain", func(b *testing.B) {
		// ReadCSV + 2 Filters + Select: 4 operations batched into 1 CGO call
		// This demonstrates our key architectural advantage
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			df := polars.ReadCSV("datasets/iris.csv").
				Filter(polars.Col("petal.length").Gt(polars.Lit(1.0))).
				Filter(polars.Col("sepal.length").Lt(polars.Lit(6.0))).
				Select("variety", "petal.length", "sepal.length")
			_, err := df.Execute()
			if err != nil {
				b.Fatal(err)
			}
			df.Release()
		}
	})
}

func BenchmarkExpressionConstruction(b *testing.B) {
	// Test pure expression building overhead (no I/O, no Polars execution)
	// This tests our expression stack machine approach
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Complex expression: Col("a").Gt(&Lit(3)).Eq(&Col("b").Lt(&Lit(1.5)))
		col1 := polars.Col("petal.length")
		lit1 := polars.Lit(3.0)
		col2 := polars.Col("petal.width") 
		lit2 := polars.Lit(1.5)
		
		// Build step by step since we can't take address of method results
		ltExpr := col2.Lt(lit2)
		_ = col1.Gt(lit1).Eq(ltExpr)
		// No need to release - expressions don't allocate heap memory
	}
}
