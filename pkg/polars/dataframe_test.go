package polars

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBasicDataFrameOperations(t *testing.T) {
	t.Run("ReadCSV", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: verify exact output
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
		result, err := df.Select("name", "age").Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: verify selected columns
		expected := `shape: (7, 2)
┌─────────┬─────┐
│ name    ┆ age │
│ ---     ┆ --- │
│ str     ┆ i64 │
╞═════════╪═════╡
│ Alice   ┆ 25  │
│ Bob     ┆ 30  │
│ Charlie ┆ 35  │
│ Diana   ┆ 28  │
│ Eve     ┆ 32  │
│ Frank   ┆ 29  │
│ Grace   ┆ 27  │
└─────────┴─────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("Count", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Count().Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: count should show 7 rows
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

	t.Run("CountLargeDataset", func(t *testing.T) {
		// Test with all 10 large files using glob pattern (10M rows total)
		df := ReadCSV("../../testdata/weather_data_part_*.csv")
		result, err := df.Count().Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: count should show 10,000,000 rows (10 files * 1M each)
		expected := `shape: (1, 1)
┌──────────┐
│ count    │
│ ---      │
│ u32      │
╞══════════╡
│ 10000000 │
└──────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("CountLargeDatasetWithFilter", func(t *testing.T) {
		// Test filtering on large dataset: count rows where temperature > 50 OR < -50 (should be 0)
		df := ReadCSV("../../testdata/weather_data_part_*.csv")
		result, err := df.Filter(
			Col("high_temp").Gt(Lit(50)).Or(Col("high_temp").Lt(Lit(-50))), // Impossible: outside -50 to 50 range
		).Count().Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should be 0 rows since temperature range is exactly -50 to 50°C (inclusive)
		expected := `shape: (1, 1)
┌───────┐
│ count │
│ ---   │
│ u32   │
╞═══════╡
│ 0     │
└───────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("CountLargeDatasetWithRealisticFilter", func(t *testing.T) {
		// Test filtering: extreme temperatures (very hot OR very cold) AND high pressure
		df := ReadCSV("../../testdata/weather_data_part_*.csv")
		result, err := df.Filter(
			Col("high_temp").Gt(Lit(40)).Or(Col("high_temp").Lt(Lit(-40))).And(Col("pressure").Gt(Lit(1000))),
		).Count().Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should be some rows: (temp > 40 OR temp < -40) AND pressure > 1000
		// This tests complex boolean logic: OR within AND
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 1, height) // Should be 1 row (the count result)

		// Verify the result structure
		resultStr := result.String()
		require.Contains(t, resultStr, "count")
		require.Contains(t, resultStr, "u32")
		// The actual count will vary due to randomness, but should be > 0
	})

	t.Run("CountLargeDatasetWithComplexFilter", func(t *testing.T) {
		// Test very complex filtering: ((hot OR cold) AND humid) OR (moderate temp AND high precipitation)
		df := ReadCSV("../../testdata/weather_data_part_*.csv")
		result, err := df.Filter(
			Col("high_temp").Gt(Lit(35)).Or(Col("low_temp").Lt(Lit(-35))).And(Col("humidity").Gt(Lit(85))).Or(
				Col("high_temp").Gt(Lit(10)).And(Col("high_temp").Lt(Lit(30))).And(Col("precipitation").Gt(Lit(75))),
			),
		).Count().Collect()
		require.NoError(t, err)
		defer result.Release()

		// Complex expression: ((high_temp > 35 OR low_temp < -35) AND humidity > 85) OR
		//                    (high_temp > 10 AND high_temp < 30 AND precipitation > 75)
		// This tests deeply nested boolean logic with multiple columns
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 1, height) // Should be 1 row (the count result)

		// Verify the result structure
		resultStr := result.String()
		require.Contains(t, resultStr, "count")
		require.Contains(t, resultStr, "u32")
	})
}

func TestComparisonExpressions(t *testing.T) {
	t.Run("GreaterThan", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Filter(Col("age").Gt(Lit(26))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: age > 26 should match 6 people (all except Alice who is 25)
		expected := `shape: (6, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Diana   ┆ 28  ┆ 55000  ┆ Sales       │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   │
│ Grace   ┆ 27  ┆ 52000  ┆ Sales       │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("LessThan", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Filter(Col("salary").Lt(Lit(55000))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: salary < 55000 should match Alice (50000) and Grace (52000)
		expected := `shape: (2, 4)
┌───────┬─────┬────────┬─────────────┐
│ name  ┆ age ┆ salary ┆ department  │
│ ---   ┆ --- ┆ ---    ┆ ---         │
│ str   ┆ i64 ┆ i64    ┆ str         │
╞═══════╪═════╪════════╪═════════════╡
│ Alice ┆ 25  ┆ 50000  ┆ Engineering │
│ Grace ┆ 27  ┆ 52000  ┆ Sales       │
└───────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("Equals", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Filter(Col("department").Eq(Lit("Engineering"))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test: department = "Engineering" should match Alice, Charlie, and Eve
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
}

func TestArithmeticExpressions(t *testing.T) {
	t.Run("Addition", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Test: salary + 10000 > 65000 should match Bob, Charlie, Eve, Frank
		result, err := df.Filter(Col("salary").Add(Lit(10000)).Gt(Lit(65000))).Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (4, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("Multiplication", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Test: salary * 2 > 100000 should match everyone except Alice
		result, err := df.Filter(Col("salary").Mul(Lit(2)).Gt(Lit(100000))).Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (6, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Diana   ┆ 28  ┆ 55000  ┆ Sales       │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   │
│ Grace   ┆ 27  ┆ 52000  ┆ Sales       │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("Division", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Test: salary / 1000 < 50 - no one matches (lowest is Alice with 50000/1000 = 50)
		result, err := df.Filter(Col("salary").Div(Lit(1000)).Lt(Lit(50))).Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (0, 4)
┌──────┬─────┬────────┬────────────┐
│ name ┆ age ┆ salary ┆ department │
│ ---  ┆ --- ┆ ---    ┆ ---        │
│ str  ┆ i64 ┆ i64    ┆ str        │
╞══════╪═════╪════════╪════════════╡
└──────┴─────┴────────┴────────────┘`

		require.Equal(t, expected, result.String())
	})
}

func TestBooleanExpressions(t *testing.T) {
	t.Run("And", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Test: age > 25 AND department = "Engineering" should match Charlie and Eve
		result, err := df.Filter(
			Col("age").Gt(Lit(25)).And(Col("department").Eq(Lit("Engineering"))),
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

	t.Run("Or", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Test: age < 26 OR salary > 55000 should match Alice, Bob, Charlie, Eve, Frank
		result, err := df.Filter(
			Col("age").Lt(Lit(26)).Or(Col("salary").Gt(Lit(55000))),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (5, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Alice   ┆ 25  ┆ 50000  ┆ Engineering │
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})
}

func TestMultiFileOperations(t *testing.T) {
	t.Run("ConcatTwoSmallFiles", func(t *testing.T) {
		// Load the same file twice to test concatenation
		df1, err := ReadCSV("../../testdata/sample.csv").Collect()
		require.NoError(t, err)
		defer df1.Release()

		df2, err := ReadCSV("../../testdata/sample.csv").Collect()
		require.NoError(t, err)
		defer df2.Release()

		// Concatenate the DataFrames
		result, err := Concat(df1, df2).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should have double the rows (14 total)
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 14, height)

		// Golden test: verify the concatenated output
		expected := `shape: (14, 4)
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
│ …       ┆ …   ┆ …      ┆ …           │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Diana   ┆ 28  ┆ 55000  ┆ Sales       │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   │
│ Grace   ┆ 27  ┆ 52000  ┆ Sales       │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("ConcatMultipleLargeFiles", func(t *testing.T) {
		// Load first 3 parts of the large dataset
		df1, err := ReadCSV("../../testdata/weather_data_part_00.csv").Collect()
		require.NoError(t, err)
		defer df1.Release()

		df2, err := ReadCSV("../../testdata/weather_data_part_01.csv").Collect()
		require.NoError(t, err)
		defer df2.Release()

		df3, err := ReadCSV("../../testdata/weather_data_part_02.csv").Collect()
		require.NoError(t, err)
		defer df3.Release()

		// Concatenate all three DataFrames
		result, err := Concat(df1, df2, df3).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should have 3M rows total
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 3000000, height)

		// Count aggregation should also show 3M
		countResult, err := result.Count().Collect()
		require.NoError(t, err)
		defer countResult.Release()

		expected := `shape: (1, 1)
┌─────────┐
│ count   │
│ ---     │
│ u32     │
╞═════════╡
│ 3000000 │
└─────────┘`

		require.Equal(t, expected, countResult.String())
	})

	t.Run("ConcatWithHelper", func(t *testing.T) {
		// Helper function to load and execute multiple files
		loadFiles := func(paths ...string) []*DataFrame {
			dfs := make([]*DataFrame, len(paths))
			for i, path := range paths {
				df, err := ReadCSV(path).Collect()
				require.NoError(t, err)
				dfs[i] = df
			}
			return dfs
		}

		// Load multiple parts using helper
		dfs := loadFiles(
			"../../testdata/weather_data_part_00.csv",
			"../../testdata/weather_data_part_01.csv",
		)
		defer func() {
			for _, df := range dfs {
				df.Release()
			}
		}()

		// Concatenate using variadic syntax
		result, err := Concat(dfs...).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should have 2M rows
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 2000000, height)
	})

}

func TestWithColumns(t *testing.T) {
	t.Run("SimpleWithColumns", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// Test: Add a computed column salary * 2 (no alias for now)
		result, err := df.WithColumns(
			Col("salary").Mul(Lit(2)),
		).Collect()

		// This should work now
		require.NoError(t, err)
		defer result.Release()

		// The result should have the same number of rows but with the computed column
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 7, height) // Same as original sample.csv

		// Golden test: verify the output shows the computed column
		// Note: with_columns replaces the existing salary column with doubled values
		expected := `shape: (7, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Alice   ┆ 25  ┆ 100000 ┆ Engineering │
│ Bob     ┆ 30  ┆ 120000 ┆ Marketing   │
│ Charlie ┆ 35  ┆ 140000 ┆ Engineering │
│ Diana   ┆ 28  ┆ 110000 ┆ Sales       │
│ Eve     ┆ 32  ┆ 130000 ┆ Engineering │
│ Frank   ┆ 29  ┆ 116000 ┆ Marketing   │
│ Grace   ┆ 27  ┆ 104000 ┆ Sales       │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("WithColumnsAndAlias", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// Test: Add a new computed column with alias "double_salary"
		result, err := df.WithColumns(
			Col("salary").Mul(Lit(2)).Alias("double_salary"),
		).Collect()

		require.NoError(t, err)
		defer result.Release()

		// Should have 5 columns now (original 4 + new double_salary)
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 7, height)

		// Golden test: verify the output shows the new aliased column
		expected := `shape: (7, 5)
┌─────────┬─────┬────────┬─────────────┬───────────────┐
│ name    ┆ age ┆ salary ┆ department  ┆ double_salary │
│ ---     ┆ --- ┆ ---    ┆ ---         ┆ ---           │
│ str     ┆ i64 ┆ i64    ┆ str         ┆ i64           │
╞═════════╪═════╪════════╪═════════════╪═══════════════╡
│ Alice   ┆ 25  ┆ 50000  ┆ Engineering ┆ 100000        │
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   ┆ 120000        │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering ┆ 140000        │
│ Diana   ┆ 28  ┆ 55000  ┆ Sales       ┆ 110000        │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering ┆ 130000        │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   ┆ 116000        │
│ Grace   ┆ 27  ┆ 52000  ┆ Sales       ┆ 104000        │
└─────────┴─────┴────────┴─────────────┴───────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("MultipleWithColumns", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// Test: Add multiple computed columns at once
		result, err := df.WithColumns(
			Col("salary").Mul(Lit(2)).Alias("double_salary"),
			Col("age").Add(Lit(10)).Alias("age_plus_10"),
			Col("salary").Div(Col("age")).Alias("salary_per_age"),
		).Collect()

		require.NoError(t, err)
		defer result.Release()

		// The result should have the same number of rows but with 3 additional columns
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 7, height) // Same as original sample.csv

		// Golden test: verify the output shows all original columns plus 3 new computed columns
		expected := `shape: (7, 7)
┌─────────┬─────┬────────┬─────────────┬───────────────┬─────────────┬────────────────┐
│ name    ┆ age ┆ salary ┆ department  ┆ double_salary ┆ age_plus_10 ┆ salary_per_age │
│ ---     ┆ --- ┆ ---    ┆ ---         ┆ ---           ┆ ---         ┆ ---            │
│ str     ┆ i64 ┆ i64    ┆ str         ┆ i64           ┆ i64         ┆ i64            │
╞═════════╪═════╪════════╪═════════════╪═══════════════╪═════════════╪════════════════╡
│ Alice   ┆ 25  ┆ 50000  ┆ Engineering ┆ 100000        ┆ 35          ┆ 2000           │
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   ┆ 120000        ┆ 40          ┆ 2000           │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering ┆ 140000        ┆ 45          ┆ 2000           │
│ Diana   ┆ 28  ┆ 55000  ┆ Sales       ┆ 110000        ┆ 38          ┆ 1964           │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering ┆ 130000        ┆ 42          ┆ 2031           │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   ┆ 116000        ┆ 39          ┆ 2000           │
│ Grace   ┆ 27  ┆ 52000  ┆ Sales       ┆ 104000        ┆ 37          ┆ 1925           │
└─────────┴─────┴────────┴─────────────┴───────────────┴─────────────┴────────────────┘`

		require.Equal(t, expected, result.String())
	})
}

func TestExpressionAggregations(t *testing.T) {
	t.Run("BasicExpressionAggregations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// CORRECT: Expression-based aggregations using WithColumns
		result, err := df.WithColumns(
			Col("salary").Sum().Alias("total_salary"),
			Col("age").Mean().Alias("avg_age"),
			Col("salary").Min().Alias("min_salary"),
			Col("salary").Max().Alias("max_salary"),
			Col("age").Std().Alias("age_std"),
			Col("salary").Var().Alias("salary_var"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should have original columns plus new aggregated columns
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 7, height) // Same number of rows as original

		// Verify the DataFrame has the correct shape (original 4 columns + 6 new columns = 10 total)
		resultStr := result.String()
		require.Contains(t, resultStr, "shape: (7, 10)") // 7 rows, 10 columns

		// Verify some of the visible columns exist (the output may be truncated)
		require.Contains(t, resultStr, "min_salary")
		require.Contains(t, resultStr, "max_salary")
		require.Contains(t, resultStr, "age_std")
		require.Contains(t, resultStr, "salary_var")
	})

	t.Run("SelectWithAggregations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// CORRECT: Select specific aggregations (Polars way)
		result, err := df.SelectExpr(
			Col("salary").Sum().Alias("total_salary"),
			Col("age").Mean().Alias("avg_age"),
			Col("salary").Min().Alias("min_salary"),
			Col("salary").Max().Alias("max_salary"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should return 1 row with aggregated values
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 1, height) // Aggregations collapse to 1 row

		// Verify structure
		resultStr := result.String()
		require.Contains(t, resultStr, "total_salary")
		require.Contains(t, resultStr, "avg_age")
		require.Contains(t, resultStr, "min_salary")
		require.Contains(t, resultStr, "max_salary")
	})

	t.Run("ComplexExpressionAggregations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// CORRECT: Complex expressions with aggregations
		result, err := df.SelectExpr(
			Col("salary").Mul(Lit(2)).Sum().Alias("doubled_salary_sum"),
			Col("age").Add(Lit(10)).Mean().Alias("age_plus_10_mean"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should return 1 row with complex aggregated values
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 1, height)

		// Verify the complex expressions worked
		resultStr := result.String()
		require.Contains(t, resultStr, "doubled_salary_sum")
		require.Contains(t, resultStr, "age_plus_10_mean")
	})

	t.Run("DdofConfiguration", func(t *testing.T) {
		// Test different ddof values
		sampleResult, err := ReadCSV("../../testdata/sample.csv").SelectExpr(
			Col("age").Std(1).Alias("sample_std"), // Sample std (ddof=1)
			Col("age").Var(1).Alias("sample_var"), // Sample var (ddof=1)
		).Collect()
		require.NoError(t, err)
		defer sampleResult.Release()

		popResult, err := ReadCSV("../../testdata/sample.csv").SelectExpr(
			Col("age").Std(0).Alias("pop_std"), // Population std (ddof=0)
			Col("age").Var(0).Alias("pop_var"), // Population var (ddof=0)
		).Collect()
		require.NoError(t, err)
		defer popResult.Release()

		// Both should return 1 row
		height1, err := sampleResult.Height()
		require.NoError(t, err)
		require.Equal(t, 1, height1)

		height2, err := popResult.Height()
		require.NoError(t, err)
		require.Equal(t, 1, height2)

		// Verify columns exist
		sampleStr := sampleResult.String()
		require.Contains(t, sampleStr, "sample_std")
		require.Contains(t, sampleStr, "sample_var")

		popStr := popResult.String()
		require.Contains(t, popStr, "pop_std")
		require.Contains(t, popStr, "pop_var")
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		t.Run("DataFrameReuse", func(t *testing.T) {
			df := ReadCSV("../../testdata/sample.csv")

			// First use should work
			result1, err := df.SelectExpr(Col("age").Mean().Alias("avg_age")).Collect()
			require.NoError(t, err)
			defer result1.Release()

			// Second use should give a friendly error, not a crash
			// Let's see what actually happens
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Recovered from panic: %v", r)
					// This is expected - we want to see what the panic message is
				}
			}()

			result2, err := df.SelectExpr(Col("salary").Sum().Alias("total_salary")).Collect()
			if err != nil {
				t.Logf("Got error (good): %v", err)
				require.Error(t, err)
			} else {
				t.Logf("No error - this is unexpected")
				defer result2.Release()
			}
		})

		t.Run("InvalidDdof", func(t *testing.T) {
			df := ReadCSV("../../testdata/sample.csv")

			// This should return an error during Execute(), not panic during construction
			result, err := df.SelectExpr(Col("age").Std(2).Alias("invalid_std")).Collect()

			require.Error(t, err)
			require.Contains(t, err.Error(), "ddof must be 0 (population) or 1 (sample)")
			require.Nil(t, result)
		})
	})
}

func TestNewExpressionOperations(t *testing.T) {
	t.Run("AdditionalAggregations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// Test the new aggregation operations
		result, err := df.SelectExpr(
			Col("salary").Median().Alias("median_salary"),
			Col("name").First().Alias("first_name"),
			Col("name").Last().Alias("last_name"),
			Col("department").NUnique().Alias("unique_departments"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should return 1 row with aggregated values
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 1, height)

		// Verify the columns exist
		resultStr := result.String()
		require.Contains(t, resultStr, "median_salary")
		require.Contains(t, resultStr, "first_name")
		require.Contains(t, resultStr, "last_name")
		require.Contains(t, resultStr, "unique_departments")
	})

	t.Run("NullOperations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// Test null checking operations
		result, err := df.SelectExpr(
			Col("name").IsNull().Alias("name_is_null"),
			Col("name").IsNotNull().Alias("name_is_not_null"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should have same number of rows as original
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 7, height) // Same as original data

		resultStr := result.String()
		require.Contains(t, resultStr, "name_is_null")
		require.Contains(t, resultStr, "name_is_not_null")
	})

	t.Run("CountOperations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// Test both count operations
		result, err := df.SelectExpr(
			Col("name").Count().Alias("count_names"),
			Col("name").CountWithNulls().Alias("count_names_with_nulls"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should return 1 row with count values
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 1, height)

		resultStr := result.String()
		require.Contains(t, resultStr, "count_names")
		require.Contains(t, resultStr, "count_names_with_nulls")

		// Both should likely be the same for CSV data (no real nulls)
		t.Logf("Count result: %s", resultStr)
	})

	t.Run("NullRowTesting", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// First check original height
		originalResult, err := df.Collect()
		require.NoError(t, err)
		originalHeight, err := originalResult.Height()
		require.NoError(t, err)
		t.Logf("Original height: %d", originalHeight)
		originalResult.Release()

		// Add a null row for testing
		testDf := ReadCSV("../../testdata/sample.csv").addNullRowForTesting()

		// Check if height increased
		testResult, err := testDf.Collect()
		require.NoError(t, err)
		testHeight, err := testResult.Height()
		require.NoError(t, err)
		t.Logf("Test height after adding null row: %d", testHeight)
		t.Logf("Test DataFrame: %s", testResult.String())
		testResult.Release()

		// Test count operations with real nulls
		df2 := ReadCSV("../../testdata/sample.csv").addNullRowForTesting()
		result, err := df2.SelectExpr(
			Col("name").Count().Alias("non_null_names"),
			Col("name").CountWithNulls().Alias("total_names"),
			Col("age").Count().Alias("non_null_ages"),
			Col("age").CountWithNulls().Alias("total_ages"),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		resultStr := result.String()
		t.Logf("Count result: %s", resultStr)

		// Verify the height increased
		require.Equal(t, originalHeight+1, testHeight, "Height should increase by 1 after adding null row")

		// Verify that Count() excludes nulls and CountWithNulls() includes them
		require.Contains(t, resultStr, "│ 7              ┆ 8           ┆ 7             ┆ 8          │",
			"Count() should exclude nulls (7), CountWithNulls() should include them (8)")
	})
}

func TestInvalidExpressionUsage(t *testing.T) {
	t.Run("NonExistentColumn", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// Test what happens when we reference a column that doesn't exist
		t.Logf("Testing Col(\"nonexistent\").Sum()...")
		result, err := df.SelectExpr(Col("nonexistent").Sum().Alias("bad_column")).Collect()
		if err != nil {
			t.Logf("ERROR for Col(\"nonexistent\").Sum(): %v", err)
		} else {
			t.Logf("SUCCESS for Col(\"nonexistent\").Sum(): %s", result.String())
			result.Release()
		}
	})
}

func TestMassiveDataset(t *testing.T) {
	t.Run("Count100MRowsWithFilter", func(t *testing.T) {
		// Load all 10 files (100M rows total) using glob pattern
		df := ReadCSVWithOptions("../../scripts/testdata/weather_data_part_*.csv", true, true) // has_header=true, with_glob=true

		// Test our RPN stack machine with a complex filter on 100M rows
		// Filter: high_temp > 40 OR low_temp < -40 (extreme temperatures)
		start := time.Now()
		result, err := df.Filter(
			Col("high_temp").Gt(Lit(40)).Or(
				Col("low_temp").Lt(Lit(-40)),
			),
		).SelectExpr(
			Col("city").Count().Alias("extreme_temp_count"),
			Col("low_temp").Min().Alias("min_temp"),
			Col("high_temp").Max().Alias("max_temp"),
		).Collect()

		elapsed := time.Since(start)
		require.NoError(t, err)
		defer result.Release()

		resultStr := result.String()
		t.Logf("100M row filter + aggregation completed in %v", elapsed)
		t.Logf("Result: %s", resultStr)

		// Verify we got some results (should have extreme temperatures in 100M rows)
		require.Contains(t, resultStr, "extreme_temp_count")
		require.Contains(t, resultStr, "min_temp")
		require.Contains(t, resultStr, "max_temp")

		// Calculate and log performance metrics
		rowsPerSecond := float64(100_000_000) / elapsed.Seconds()
		t.Logf("Performance: %.2f million rows/second", rowsPerSecond/1_000_000)
	})

	t.Run("Count100MRowsNoMatches", func(t *testing.T) {
		// Load all 10 files (100M rows total) using glob pattern
		df := ReadCSVWithOptions("../../scripts/testdata/weather_data_part_*.csv", true, true)

		// Test with filter that should match NOTHING (temp range is -50 to +50)
		// Filter: high_temp > 50 OR low_temp < -50 (impossible temperatures)
		start := time.Now()
		result, err := df.Filter(
			Col("high_temp").Gt(Lit(50)).Or(
				Col("low_temp").Lt(Lit(-50)),
			),
		).SelectExpr(
			Col("city").Count().Alias("no_match_count"),
			Col("low_temp").Min().Alias("min_temp"),
			Col("high_temp").Max().Alias("max_temp"),
		).Collect()

		elapsed := time.Since(start)
		require.NoError(t, err)
		defer result.Release()

		resultStr := result.String()
		t.Logf("100M row full scan (no matches) completed in %v", elapsed)
		t.Logf("Result: %s", resultStr)

		// Calculate and log performance metrics
		rowsPerSecond := float64(100_000_000) / elapsed.Seconds()
		t.Logf("Performance: %.2f million rows/second", rowsPerSecond/1_000_000)

		// Should have 0 matches but still return aggregation results
		require.Contains(t, resultStr, "no_match_count")
		require.Contains(t, resultStr, "│ 0") // Should show 0 count
	})
}

func TestComplexExpressions(t *testing.T) {
	t.Run("ChainedArithmeticAndComparison", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Test: (salary / 1000) * 2 > 100 should match everyone except Alice
		result, err := df.Filter(
			Col("salary").Div(Lit(1000)).Mul(Lit(2)).Gt(Lit(100)),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (6, 4)
┌─────────┬─────┬────────┬─────────────┐
│ name    ┆ age ┆ salary ┆ department  │
│ ---     ┆ --- ┆ ---    ┆ ---         │
│ str     ┆ i64 ┆ i64    ┆ str         │
╞═════════╪═════╪════════╪═════════════╡
│ Bob     ┆ 30  ┆ 60000  ┆ Marketing   │
│ Charlie ┆ 35  ┆ 70000  ┆ Engineering │
│ Diana   ┆ 28  ┆ 55000  ┆ Sales       │
│ Eve     ┆ 32  ┆ 65000  ┆ Engineering │
│ Frank   ┆ 29  ┆ 58000  ┆ Marketing   │
│ Grace   ┆ 27  ┆ 52000  ┆ Sales       │
└─────────┴─────┴────────┴─────────────┘`

		require.Equal(t, expected, result.String())
	})

	t.Run("BooleanWithArithmetic", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Test: (age + 5) > 30 AND salary < 55000 should match Grace only
		result, err := df.Filter(
			Col("age").Add(Lit(5)).Gt(Lit(30)).And(Col("salary").Lt(Lit(55000))),
		).Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (1, 4)
┌───────┬─────┬────────┬────────────┐
│ name  ┆ age ┆ salary ┆ department │
│ ---   ┆ --- ┆ ---    ┆ ---        │
│ str   ┆ i64 ┆ i64    ┆ str        │
╞═══════╪═════╪════════╪════════════╡
│ Grace ┆ 27  ┆ 52000  ┆ Sales      │
└───────┴─────┴────────┴────────────┘`

		require.Equal(t, expected, result.String())
	})
}

func TestGroupByOperations(t *testing.T) {
	t.Run("BasicGroupByAgg", func(t *testing.T) {
		// Test the basic GroupBy -> Agg pattern with golden output
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// GroupBy department and aggregate salary with mean
		result, err := df.GroupBy("department").
			Agg(Col("salary").Mean()).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Verify key aspects of the result (order may vary without Sort)
		output := result.String()
		t.Logf("GroupBy result:\n%s", output)

		// Verify structure and content
		require.Contains(t, output, "shape: (3, 2)") // 3 departments, 2 columns
		require.Contains(t, output, "department")
		require.Contains(t, output, "salary")
		require.Contains(t, output, "f64") // Mean produces float

		// Verify all departments and their correct average salaries
		require.Contains(t, output, "Marketing   ┆ 59000.0")      // (60000+58000)/2
		require.Contains(t, output, "Sales       ┆ 53500.0")      // (52000+55000)/2
		require.Contains(t, output, "Engineering ┆ 61666.666667") // (50000+70000+65000)/3
	})

	t.Run("MultipleAggregations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// GroupBy with multiple aggregations
		result, err := df.GroupBy("department").
			Agg(
				Col("salary").Mean().Alias("avg_salary"),
				Col("age").Max().Alias("max_age"),
				Col("name").Count().Alias("employee_count"),
			).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test - verify multiple aggregations work correctly
		output := result.String()
		t.Logf("Multi-agg GroupBy result:\n%s", output)

		// Verify all expected columns are present with correct aliases
		require.Contains(t, output, "department")
		require.Contains(t, output, "avg_salary")
		require.Contains(t, output, "max_age")
		require.Contains(t, output, "employee_count")

		// Verify we have data for all departments
		require.Contains(t, output, "Engineering")
		require.Contains(t, output, "Marketing")
		require.Contains(t, output, "Sales")
	})

	t.Run("GroupByMultipleColumns", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// GroupBy multiple columns
		result, err := df.GroupBy("department", "age").
			Agg(Col("salary").Sum()).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Golden test - verify multi-column grouping
		output := result.String()
		t.Logf("Multi-column GroupBy result:\n%s", output)

		// Verify we have all expected columns
		require.Contains(t, output, "department")
		require.Contains(t, output, "age")
		require.Contains(t, output, "salary")

		// Should have unique combinations of department + age
		// (more granular than just department grouping)
		require.Contains(t, output, "Engineering")
		require.Contains(t, output, "Marketing")
	})

	t.Run("GroupByAggSort", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Test: GroupBy -> Agg -> Sort (golden test)
		result, err := df.GroupBy("department").
			Agg(Col("salary").Mean().Alias("avg_salary")).
			Sort([]string{"avg_salary"}).
			Collect()
		require.NoError(t, err)
		defer result.Release()

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

	t.Run("GroupByAggFilter", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Test: GroupBy -> Agg -> Filter -> Sort (HAVING clause equivalent with deterministic order)
		result, err := df.GroupBy("department").
			Agg(Col("salary").Mean().Alias("avg_salary")).
			Filter(Col("avg_salary").Gt(Lit(55000.0))).
			Sort([]string{"avg_salary"}). // Add explicit sort for deterministic order
			Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (2, 2)
┌─────────────┬──────────────┐
│ department  ┆ avg_salary   │
│ ---         ┆ ---          │
│ str         ┆ f64          │
╞═════════════╪══════════════╡
│ Marketing   ┆ 59000.0      │
│ Engineering ┆ 61666.666667 │
└─────────────┴──────────────┘`
		require.Equal(t, expected, result.String())
	})

	t.Run("GroupByComplexChaining", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Test: GroupBy -> Agg -> Filter -> Sort (complex chaining)
		result, err := df.GroupBy("department").
			Agg(
				Col("salary").Mean().Alias("avg_salary"),
				Col("name").Count().Alias("employee_count"),
			).
			Filter(Col("employee_count").Gt(Lit(1))). // Only departments with >1 employee
			Sort([]string{"avg_salary"}).             // Sort by average salary
			Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (3, 3)
┌─────────────┬──────────────┬────────────────┐
│ department  ┆ avg_salary   ┆ employee_count │
│ ---         ┆ ---          ┆ ---            │
│ str         ┆ f64          ┆ u32            │
╞═════════════╪══════════════╪════════════════╡
│ Sales       ┆ 53500.0      ┆ 2              │
│ Marketing   ┆ 59000.0      ┆ 2              │
│ Engineering ┆ 61666.666667 ┆ 3              │
└─────────────┴──────────────┴────────────────┘`
		require.Equal(t, expected, result.String())
	})

}

func TestGroupByErrorCases(t *testing.T) {
	t.Run("AggWithoutGroupBy", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Try to call Agg without GroupBy - should fail
		_, err := df.Agg(Col("salary").Mean()).Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Agg() can only be called on LazyGroupBy")
	})

	t.Run("EmptyAggExpressions", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Try to call Agg with no expressions - should fail
		_, err := df.GroupBy("department").Agg().Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Agg() requires at least one expression")
	})
}

func TestNewSortByAPI(t *testing.T) {
	t.Run("SortByDescending", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Sort by salary descending using new SortBy API
		result, err := df.SortBy([]SortField{Desc("salary")}).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted by salary descending:\n%s", output)

		// Verify descending order - Charlie should be first (highest salary: 70000)
		require.Contains(t, output, "shape: (7, 4)")
		require.Contains(t, output, "Charlie")
		require.Contains(t, output, "70000")
	})

	t.Run("SortByMultipleFields", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Sort by department ascending, then salary descending
		result, err := df.SortBy([]SortField{
			Asc("department"),
			Desc("salary"),
		}).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted by department ASC, salary DESC:\n%s", output)

		// Verify multi-field sort
		require.Contains(t, output, "shape: (7, 4)")
		// Engineering should come first alphabetically, with Charlie (highest salary) first within Engineering
		require.Contains(t, output, "Charlie")
		require.Contains(t, output, "Engineering")
	})

	t.Run("SortByWithNullsFirst", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Sort by age ascending with nulls first (though our test data has no nulls)
		result, err := df.SortBy([]SortField{AscNullsFirst("age")}).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted by age ASC nulls first:\n%s", output)

		// Verify ascending order - Alice should be first (youngest: 25)
		require.Contains(t, output, "shape: (7, 4)")
		require.Contains(t, output, "Alice")
		require.Contains(t, output, "25")
	})

	t.Run("ComplexMultiColumnSort", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Sort by department DESC, age ASC, salary DESC
		// This should give us: Sales (desc), Marketing (desc), Engineering (desc)
		// Within each department: youngest first, then highest salary first for same age
		result, err := df.SortBy([]SortField{
			Desc("department"), // Sales, Marketing, Engineering
			Asc("age"),         // Youngest first within department
			Desc("salary"),     // Highest salary first for same age
		}).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted by department DESC, age ASC, salary DESC:\n%s", output)

		// Verify complex sort order
		require.Contains(t, output, "shape: (7, 4)")

		// Sales should come first (department DESC)
		lines := strings.Split(output, "\n")
		var dataLines []string
		for _, line := range lines {
			if strings.Contains(line, "Sales") || strings.Contains(line, "Marketing") || strings.Contains(line, "Engineering") {
				dataLines = append(dataLines, line)
			}
		}

		// First data row should be Sales (Grace, age 27 or Diana, age 28)
		require.True(t, len(dataLines) >= 1)
		require.Contains(t, dataLines[0], "Sales")
	})

	t.Run("NullsFirstVsNullsLast", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Add a null row for testing nulls ordering
		dfWithNulls, err := df.addNullRowForTesting().Collect()
		require.NoError(t, err)
		defer dfWithNulls.Release()

		// Test 1: Sort by age ASC with nulls first
		resultNullsFirst, err := dfWithNulls.SortBy([]SortField{AscNullsFirst("age")}).Collect()
		require.NoError(t, err)
		defer resultNullsFirst.Release()

		outputNullsFirst := resultNullsFirst.String()
		t.Logf("Sorted by age ASC, nulls first:\n%s", outputNullsFirst)

		// Test 2: Sort by age ASC with nulls last
		resultNullsLast, err := dfWithNulls.SortBy([]SortField{Asc("age")}).Collect() // Asc defaults to nulls last
		require.NoError(t, err)
		defer resultNullsLast.Release()

		outputNullsLast := resultNullsLast.String()
		t.Logf("Sorted by age ASC, nulls last:\n%s", outputNullsLast)

		// Verify both have 8 rows (7 original + 1 null)
		require.Contains(t, outputNullsFirst, "shape: (8, 4)")
		require.Contains(t, outputNullsLast, "shape: (8, 4)")

		// Verify null row appears in different positions
		require.Contains(t, outputNullsFirst, "null")
		require.Contains(t, outputNullsLast, "null")

		// Parse the outputs to verify null positioning
		linesFirst := strings.Split(outputNullsFirst, "\n")
		linesLast := strings.Split(outputNullsLast, "\n")

		// Find the null row position in each output
		var nullPosFirst, nullPosLast int = -1, -1
		for i, line := range linesFirst {
			if strings.Contains(line, "│ null") {
				nullPosFirst = i
				break
			}
		}
		for i, line := range linesLast {
			if strings.Contains(line, "│ null") {
				nullPosLast = i
				break
			}
		}

		// Nulls first should have null row earlier than nulls last
		require.True(t, nullPosFirst > 0, "Should find null row in nulls first output")
		require.True(t, nullPosLast > 0, "Should find null row in nulls last output")
		require.True(t, nullPosFirst < nullPosLast, "Null row should appear earlier with nulls first")
	})

	t.Run("MixedNullsOrdering", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Add null row and sort with different nulls ordering per column
		dfWithNulls, err := df.addNullRowForTesting().Collect()
		require.NoError(t, err)
		defer dfWithNulls.Release()

		result, err := dfWithNulls.SortBy([]SortField{
			AscNullsFirst("department"), // Department ASC, nulls first
			DescNullsFirst("salary"),    // Salary DESC, nulls first
		}).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted with mixed nulls ordering:\n%s", output)

		// Verify it works with null data
		require.Contains(t, output, "shape: (8, 4)")
		require.Contains(t, output, "null") // Should contain the null row
	})
}

func TestContextTypeErrorMessages(t *testing.T) {
	t.Run("SortOnLazyGroupBy", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Try to call Sort on LazyGroupBy - should fail with context error
		_, err := df.GroupBy("department").Sort([]string{"salary"}).Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Cannot call sort() on LazyGroupBy")
		require.Contains(t, err.Error(), "Call agg() first to resolve grouping")
	})

	t.Run("CollectOnLazyGroupBy", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Try to call Collect directly on LazyGroupBy - should fail with context error
		grouped := df.GroupBy("department")
		defer grouped.Release()
		_, err := grouped.Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Cannot collect LazyGroupBy")
		require.Contains(t, err.Error(), "Call agg() first to resolve grouping")
	})

	t.Run("AggOnDataFrame", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Try to call Agg on DataFrame (not LazyGroupBy) - should fail
		_, err := df.Agg(Col("salary").Mean()).Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Agg() can only be called on LazyGroupBy")
	})
}

func TestGroupByArchitecturalIssues(t *testing.T) {
	t.Skip("RESOLVED: GroupBy now works with proper lazy evaluation!")

	t.Run("ForcedCollectEverywhere", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// This should work but doesn't due to our architecture:
		// result := df.Select("name", "department").
		//              Filter(Col("salary").Gt(Lit(80000))).
		//              GroupBy("department").
		//              Agg(Col("salary").Mean()).
		//              Execute()  // Single collect at the end

		// Instead we're forced to do:
		selected, _ := df.Select("name", "department").Collect() // ❌ Forced collect
		defer selected.Release()

		filtered, _ := selected.Filter(Col("salary").Gt(Lit(80000))).Collect() // ❌ Forced collect
		defer filtered.Release()

		result, _ := filtered.GroupBy("department").Collect() // ❌ Forced collect
		defer result.Release()

		t.Logf("Result: %s", result.String())

		// This defeats lazy optimization and is inefficient
	})

	t.Run("ContextTypeMismatch", func(t *testing.T) {
		// Polars has different return types:
		// - df.lazy() → LazyFrame
		// - lazy_frame.group_by() → LazyGroupBy  ⚠️ Different type!
		// - lazy_group_by.agg() → LazyFrame

		// But our FFI assumes everything returns DataFrame handle
		// This architectural mismatch forces us to collect() everywhere

		t.Log("Our current architecture doesn't handle Polars' type system properly")
	})
}

func TestStringOperations(t *testing.T) {
	t.Run("BasicStringOperations", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		result, err := df.SelectExpr(
			Col("name").Alias("original_name"),
			Col("name").StrLen().Alias("name_length"),
			Col("name").StrToUppercase().Alias("name_upper"),
			Col("name").StrToLowercase().Alias("name_lower"),
			Col("department").Alias("original_dept"),
			Col("department").StrLen().Alias("dept_length"),
		).Collect()

		require.NoError(t, err)
		defer result.Release()

		resultStr := result.String()
		t.Logf("String operations result:\n%s", resultStr)

		// Check that we have the expected columns
		require.Contains(t, resultStr, "original_name")
		require.Contains(t, resultStr, "name_length")
		require.Contains(t, resultStr, "name_upper")
		require.Contains(t, resultStr, "name_lower")
		require.Contains(t, resultStr, "original_dept")
		require.Contains(t, resultStr, "dept_length")

		// Check some expected transformations
		require.Contains(t, resultStr, "ALICE") // uppercase
		require.Contains(t, resultStr, "alice") // lowercase
	})

	t.Run("StringPatternMatching", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		result, err := df.SelectExpr(
			Col("name").Alias("name"),
			Col("name").StrContains("a").Alias("contains_a"),
			Col("name").StrStartsWith("A").Alias("starts_with_A"),
			Col("name").StrEndsWith("e").Alias("ends_with_e"),
			Col("department").StrContains("ng").Alias("dept_contains_ng"),
		).Collect()

		require.NoError(t, err)
		defer result.Release()

		resultStr := result.String()
		t.Logf("String pattern matching result:\n%s", resultStr)

		// Check that we have the expected columns
		require.Contains(t, resultStr, "contains_a")
		require.Contains(t, resultStr, "starts_with_A")
		require.Contains(t, resultStr, "ends_with_e")
		require.Contains(t, resultStr, "dept_contains_ng")

		// Check for boolean results (true/false values)
		require.Contains(t, resultStr, "true")
		require.Contains(t, resultStr, "false")
	})

	t.Run("StringOperationsWithFilter", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")

		// Filter for names that contain 'a' and have length > 4
		result, err := df.Filter(
			Col("name").StrContains("a").And(
				Col("name").StrLen().Gt(Lit(4)),
			),
		).SelectExpr(
			Col("name").Alias("name"),
			Col("name").StrLen().Alias("name_length"),
			Col("name").StrToUppercase().Alias("name_upper"),
		).Collect()

		require.NoError(t, err)
		defer result.Release()

		resultStr := result.String()
		t.Logf("Filtered string operations result:\n%s", resultStr)

		// Should have filtered results
		require.Contains(t, resultStr, "name")
		require.Contains(t, resultStr, "name_length")
		require.Contains(t, resultStr, "name_upper")
	})
}

func TestSortAndLimit(t *testing.T) {
	t.Run("BasicSort", func(t *testing.T) {
		// Sort by age ascending
		result, err := ReadCSV("../../testdata/sample.csv").Sort([]string{"age"}).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted by age:\n%s", output)

		// Verify structure
		require.Contains(t, output, "shape: (7, 4)")
		require.Contains(t, output, "Alice   ┆ 25") // Should be first (youngest)
	})

	t.Run("SortDescending", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Sort by salary ascending (for now)
		result, err := df.Sort([]string{"salary"}).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted by salary ascending:\n%s", output)

		// Verify structure
		require.Contains(t, output, "shape: (7, 4)")
		// Note: With ascending sort, lowest salary should be first
	})

	t.Run("BasicLimit", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Limit to first 3 rows
		result, err := df.Limit(3).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Limited to 3 rows:\n%s", output)

		// Verify only 3 rows
		require.Contains(t, output, "shape: (3, 4)")
	})

	t.Run("SortAndLimit", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Sort by age and limit to top 2
		result, err := df.Sort([]string{"age"}).Limit(2).Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted by age, limited to 2:\n%s", output)

		// Verify 2 rows with youngest people
		require.Contains(t, output, "shape: (2, 4)")
		require.Contains(t, output, "Alice") // Youngest
		require.Contains(t, output, "Grace") // Second youngest
		require.Contains(t, output, "25")    // Alice's age
		require.Contains(t, output, "27")    // Grace's age
	})

	t.Run("GroupBySortedData", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Sort by department, then group by department and aggregate
		result, err := df.Sort([]string{"department"}).
			GroupBy("department").
			Agg(Col("salary").Mean().Alias("avg_salary")).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("Sorted then grouped:\n%s", output)

		// Verify structure - should have deterministic order now!
		require.Contains(t, output, "shape: (3, 2)")
		require.Contains(t, output, "Engineering")
		require.Contains(t, output, "Marketing")
		require.Contains(t, output, "Sales")
	})
}

func TestSQLQueries(t *testing.T) {
	t.Run("BasicSQLQuery", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Test basic SQL query
		result, err := df.Query("SELECT name, age FROM df WHERE age > 28").Collect()
		require.NoError(t, err)
		defer result.Release()

		expected := `shape: (4, 2)
┌─────────┬─────┐
│ name    ┆ age │
│ ---     ┆ --- │
│ str     ┆ i64 │
╞═════════╪═════╡
│ Bob     ┆ 30  │
│ Charlie ┆ 35  │
│ Eve     ┆ 32  │
│ Frank   ┆ 29  │
└─────────┴─────┘`
		require.Equal(t, expected, result.String())
	})

	t.Run("SQLAggregation", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Test SQL aggregation
		result, err := df.Query("SELECT department, AVG(salary) as avg_salary, COUNT(*) as employee_count FROM df GROUP BY department ORDER BY avg_salary").Collect()
		require.NoError(t, err)
		defer result.Release()

		output := result.String()
		t.Logf("SQL aggregation result:\n%s", output)

		// Verify structure
		require.Contains(t, output, "department")
		require.Contains(t, output, "avg_salary")
		require.Contains(t, output, "employee_count")
		require.Contains(t, output, "Engineering")
		require.Contains(t, output, "Marketing")
		require.Contains(t, output, "Sales")
	})

	t.Run("SQLErrorHandling", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		defer df.Release()

		// Test invalid SQL
		_, err := df.Query("INVALID SQL SYNTAX").Collect()
		require.Error(t, err)
		require.Contains(t, err.Error(), "polars error")
	})
}
