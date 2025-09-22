package polars

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBasicOperations demonstrates core DataFrame operations with golden test outputs
func TestBasicOperations(t *testing.T) {
	t.Run("ReadCSV", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: shows complete sample dataset structure
		expected := `shape: (7, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Alice   ┆ 25  ┆ 50000  ┆ Engineering │
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Diana   ┆ 28  ┆ 55000  ┆ Sales       │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   │
│ Grace   ┆ 27  ┆ 52000  ┆ Sales       │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("Select", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Select("name", "salary").Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: column selection
		expected := `shape: (7, 2)
┌─────────┬────────┐
│ name    ┆ salary │
│ ---     ┆ ---    │
│ str     ┆ i64    │
╞═════════╪════════╡
│ Alice   ┆ 50000  │
│ Bob     ┆ 60000  │
│ Charlie ┆ 70000  │
│ Diana   ┆ 55000  │
│ Eve     ┆ 65000  │
│ Frank   ┆ 58000  │
│ Grace   ┆ 52000  │
└─────────┴────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("Filter", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Filter(Col("department").Eq(Lit("Engineering"))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: filtering by department
		expected := `shape: (3, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Alice   ┆ 25  ┆ 50000  ┆ Engineering │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("Count", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Count().Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: count shows total rows
		expected := `shape: (1, 1)
┌───────┐
│ count │
│ ---   │
│ u32   │
╞═══════╡
│ 7     │
└───────┘`

		require.Equal(t, expected, result.String())
	})
}

// TestExpressions demonstrates expression operations with clear examples
func TestExpressions(t *testing.T) {
	t.Run("ArithmeticAndComparison", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Filter: salary * 2 > 120000 (should match Charlie and Eve)
		result, err := df.Filter(Col("salary").Mul(Lit(2)).Gt(Lit(120000))).Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (2, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("BooleanLogic", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Filter: age > 30 AND department = "Engineering" (should match Charlie and Eve)
		result, err := df.Filter(
			Col("age").Gt(Lit(30)).And(Col("department").Eq(Lit("Engineering"))),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (2, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("WithColumns", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.WithColumns(
			Col("salary").Mul(Lit(2)).Alias("double_salary"),
			Col("age").Add(Lit(10)).Alias("age_plus_10"),
		).Select("name", "double_salary", "age_plus_10").Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: computed columns
		expected := `shape: (7, 3)
┌─────────┬───────────────┬─────────────┐
│ name    ┆ double_salary ┆ age_plus_10 │
│ ---     ┆ ---           ┆ ---         │
│ str     ┆ i64           ┆ i64         │
╞═════════╪═══════════════╪═════════════╡
│ Alice   ┆ 100000        ┆ 35          │
│ Bob     ┆ 120000        ┆ 40          │
│ Charlie ┆ 140000        ┆ 45          │
│ Diana   ┆ 110000        ┆ 38          │
│ Eve     ┆ 130000        ┆ 42          │
│ Frank   ┆ 116000        ┆ 39          │
│ Grace   ┆ 104000        ┆ 37          │
└─────────┴───────────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("StringOperations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.SelectExpr(
			Col("name").Alias("name"),
			Col("name").StrLen().Alias("name_length"),
			Col("name").StrToUppercase().Alias("name_upper"),
			Col("name").StrContains("a").Alias("contains_a"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: string operations
		output := result.String()
		require.Contains(t, output, "name_length")
		require.Contains(t, output, "name_upper")
		require.Contains(t, output, "contains_a")
		require.Contains(t, output, "ALICE") // uppercase transformation
		require.Contains(t, output, "true")  // boolean result
		require.Contains(t, output, "false") // boolean result
	})
}

// TestAggregations demonstrates GroupBy and aggregation operations
func TestAggregations(t *testing.T) {
	t.Run("BasicAggregations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.SelectExpr(
			Col("salary").Sum().Alias("total_salary"),
			Col("age").Mean().Alias("avg_age"),
			Col("salary").Min().Alias("min_salary"),
			Col("salary").Max().Alias("max_salary"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: basic aggregations
		output := result.String()
		require.Contains(t, output, "total_salary")
		require.Contains(t, output, "avg_age")
		require.Contains(t, output, "min_salary")
		require.Contains(t, output, "max_salary")
		require.Contains(t, output, "410000") // total salary
		require.Contains(t, output, "50000")  // min salary
		require.Contains(t, output, "70000")  // max salary
	})

	t.Run("GroupByAggregation", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.GroupBy("department").
			Agg(Col("salary").Mean().Alias("avg_salary")).
			Sort([]string{"avg_salary"}).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: group by with aggregation
		expected := `shape: (3, 2)
┌─────────────┬──────────────┐
│ department  ┆ avg_salary   │
│ ---         ┆ ---          │
│ str         ┆ f64          │
╞═════════════╪══════════════╡
│ Sales       ┆ 53500.0      │
│ Marketing   ┆ 59000.0      │
│ Engineering ┆ 61666.666667 │
└─────────────┴──────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("MultipleAggregations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.GroupBy("department").
			Agg(
				Col("salary").Mean().Alias("avg_salary"),
				Col("name").Count().Alias("employee_count"),
				Col("age").Max().Alias("max_age"),
			).
			Sort([]string{"avg_salary"}).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: multiple aggregations
		expected := `shape: (3, 4)
┌─────────────┬──────────────┬────────────────┬─────────┐
│ department  ┆ avg_salary   ┆ employee_count ┆ max_age │
│ ---         ┆ ---          ┆ ---            ┆ ---     │
│ str         ┆ f64          ┆ u32            ┆ i64     │
╞═════════════╪══════════════╪════════════════╪═════════╡
│ Sales       ┆ 53500.0      ┆ 2              ┆ 28      │
│ Marketing   ┆ 59000.0      ┆ 2              ┆ 30      │
│ Engineering ┆ 61666.666667 ┆ 3              ┆ 35      │
└─────────────┴──────────────┴────────────────┴─────────┘`

		require.Equal(t, expected, result.String())
	})
}

// TestAdvancedFeatures demonstrates sorting, limiting, and SQL operations
func TestAdvancedFeatures(t *testing.T) {
	t.Run("SortAndLimit", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Sort([]string{"salary"}).Limit(3).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: sort by salary (ascending) and limit to top 3
		expected := `shape: (3, 4)
┌───────┬─────┬────────┬─────────────┐
│ name  ┆ age ┆ salary ┆ department  │
│ ---   ┆ --- ┆ ---    ┆ ---         │
│ str   ┆ i64 ┆ i64    ┆ str         │
╞═══════╪═════╪════════╪═════════════╡
│ Alice ┆ 25  ┆ 50000  ┆ Engineering │
│ Grace ┆ 27  ┆ 52000  ┆ Sales       │
│ Diana ┆ 28  ┆ 55000  ┆ Sales       │
└───────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("NewSortByAPI", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.SortBy([]SortField{
			Desc("salary"), // Highest salary first
		}).Limit(2).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: sort by salary descending, limit to top 2
		expected := `shape: (2, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("SQLQuery", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Query("SELECT name, salary FROM df WHERE salary > 60000 ORDER BY salary DESC").Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: SQL query (salary > 60000 excludes Bob who has exactly 60000)
		expected := `shape: (2, 2)
┌─────────┬────────┐
│ name    ┆ salary │
│ ---     ┆ ---    │
│ str     ┆ i64    │
╞═════════╪════════╡
│ Charlie ┆ 70000  │
│ Eve     ┆ 65000  │
└─────────┴────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("Concatenation", func(t *testing.T) {
		// Load the same file twice to test concatenation
		df1, err := ReadCSV("../../testdata/sample.csv").Collect()
		require.NoError(t, err)
		defer df1.Release()

		df2, err := ReadCSV("../../testdata/sample.csv").Collect()
		require.NoError(t, err)
		defer df2.Release()

		// Concatenate and limit to show structure
		result, err := Concat(df1, df2).Limit(10).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: concatenated DataFrame (showing first 10 rows)
		output := result.String()
		require.Contains(t, output, "shape: (10, 4)")
		require.Contains(t, output, "Alice") // Should appear twice
		
		// Verify we have 14 total rows when not limited
		fullResult, err := Concat(df1, df2).Collect()
		require.NoError(t, err)
		defer fullResult.Release()
		
		height, err := fullResult.Height()
		require.NoError(t, err)
		require.Equal(t, 14, height) // 7 + 7 = 14 rows
	})
}

// TestPerformanceBenchmarks - Important benchmark tests for large datasets
func TestPerformanceBenchmarks(t *testing.T) {
	t.Run("Count10MRows", func(t *testing.T) {
		// Test with 10M rows using glob pattern (10 files * 1M each)
		df := ReadCSV("../../testdata/weather_data_part_*.csv")
		
		start := time.Now()
		result, err := df.Count().Collect()
		elapsed := time.Since(start)
		
		require.NoError(t, err)
		defer result.Release()

		// Golden test: count should show 10,000,000 rows
		expected := `shape: (1, 1)
┌──────────┐
│ count    │
│ ---      │
│ u32      │
╞══════════╡
│ 10000000 │
└──────────┘`

		require.Equal(t, expected, result.String())
		
		// Performance logging
		rowsPerSecond := float64(10_000_000) / elapsed.Seconds()
		t.Logf("10M row count completed in %v (%.2f million rows/second)", elapsed, rowsPerSecond/1_000_000)
	})

	t.Run("Filter10MRowsWithComplexLogic", func(t *testing.T) {
		// Test complex filtering on 10M rows: extreme temperatures AND high pressure
		df := ReadCSV("../../testdata/weather_data_part_*.csv")
		
		start := time.Now()
		result, err := df.Filter(
			Col("high_temp").Gt(Lit(40)).Or(Col("high_temp").Lt(Lit(-40))).And(Col("pressure").Gt(Lit(1000))),
		).Count().Collect()
		elapsed := time.Since(start)
		
		require.NoError(t, err)
		defer result.Release()

		// Should return some results (exact count varies due to randomness)
		output := result.String()
		require.Contains(t, output, "count")
		require.Contains(t, output, "u32")
		
		// Performance logging
		rowsPerSecond := float64(10_000_000) / elapsed.Seconds()
		t.Logf("10M row complex filter completed in %v (%.2f million rows/second)", elapsed, rowsPerSecond/1_000_000)
		t.Logf("Filter result: %s", output)
	})

	t.Run("Count100MRowsWithAggregation", func(t *testing.T) {
		// Load all 10 files from scripts/testdata (100M rows total) using glob pattern
		df := ReadCSVWithOptions("../../scripts/testdata/weather_data_part_*.csv", true, true)

		// Test complex aggregation on 100M rows
		start := time.Now()
		result, err := df.Filter(
			Col("high_temp").Gt(Lit(35)).Or(Col("low_temp").Lt(Lit(-35))),
		).SelectExpr(
			Col("city").Count().Alias("extreme_temp_count"),
			Col("low_temp").Min().Alias("min_temp"),
			Col("high_temp").Max().Alias("max_temp"),
			Col("pressure").Mean().Alias("avg_pressure"),
		).Collect()
		elapsed := time.Since(start)

		require.NoError(t, err)
		defer result.Release()

		// Golden test: verify aggregation structure
		output := result.String()
		require.Contains(t, output, "extreme_temp_count")
		require.Contains(t, output, "min_temp")
		require.Contains(t, output, "max_temp")
		require.Contains(t, output, "avg_pressure")

		// Performance logging
		rowsPerSecond := float64(100_000_000) / elapsed.Seconds()
		t.Logf("100M row filter + aggregation completed in %v (%.2f million rows/second)", elapsed, rowsPerSecond/1_000_000)
		t.Logf("Result: %s", output)
		
		// Verify we got meaningful results
		require.Contains(t, output, "shape: (1, 4)") // Should be 1 row with 4 aggregated columns
	})

	t.Run("Count100MRowsFullScan", func(t *testing.T) {
		// Test with filter that matches nothing (impossible temperatures)
		df := ReadCSVWithOptions("../../scripts/testdata/weather_data_part_*.csv", true, true)

		start := time.Now()
		result, err := df.Filter(
			Col("high_temp").Gt(Lit(60)).Or(Col("low_temp").Lt(Lit(-60))), // Impossible range
		).Count().Collect()
		elapsed := time.Since(start)

		require.NoError(t, err)
		defer result.Release()

		// Should have 0 matches but still return count result
		expected := `shape: (1, 1)
┌───────┐
│ count │
│ ---   │
│ u32   │
╞═══════╡
│ 0     │
└───────┘`

		require.Equal(t, expected, result.String())

		// Performance logging
		rowsPerSecond := float64(100_000_000) / elapsed.Seconds()
		t.Logf("100M row full scan (no matches) completed in %v (%.2f million rows/second)", elapsed, rowsPerSecond/1_000_000)
	})
}

// TestErrorHandling demonstrates proper error handling
func TestErrorHandling(t *testing.T) {
	t.Run("InvalidSQL", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		_, err := df.Query("INVALID SQL SYNTAX").Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "polars error")
	})

	t.Run("AggWithoutGroupBy", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		_, err := df.Agg(Col("salary").Mean()).Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Agg() can only be called on LazyGroupBy")
	})

	t.Run("SortOnGroupBy", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		_, err := df.GroupBy("department").Sort([]string{"salary"}).Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Cannot call sort() on LazyGroupBy")
		require.Contains(t, err.Error(), "Call agg() first to resolve grouping")
	})
}
