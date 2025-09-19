package polars

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasicDataFrameOperations(t *testing.T) {
	t.Run("ReadCSV", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Execute()
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
		result, err := df.Select("name", "age").Execute()
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
		result, err := df.Count().Execute()
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
		result, err := df.Count().Execute()
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
}

func TestComparisonExpressions(t *testing.T) {
	t.Run("GreaterThan", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		result, err := df.Filter(Col("age").Gt(Lit(26))).Execute()
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
		result, err := df.Filter(Col("salary").Lt(Lit(55000))).Execute()
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
		result, err := df.Filter(Col("department").Eq(Lit("Engineering"))).Execute()
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
		result, err := df.Filter(Col("salary").Add(Lit(10000)).Gt(Lit(65000))).Execute()
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
		result, err := df.Filter(Col("salary").Mul(Lit(2)).Gt(Lit(100000))).Execute()
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
		result, err := df.Filter(Col("salary").Div(Lit(1000)).Lt(Lit(50))).Execute()
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
		).Execute()
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
		).Execute()
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
		df1, err := ReadCSV("../../testdata/sample.csv").Execute()
		require.NoError(t, err)
		defer df1.Release()

		df2, err := ReadCSV("../../testdata/sample.csv").Execute()
		require.NoError(t, err)
		defer df2.Release()

		// Concatenate the DataFrames
		result, err := Concat(df1, df2).Execute()
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
		df1, err := ReadCSV("../../testdata/weather_data_part_00.csv").Execute()
		require.NoError(t, err)
		defer df1.Release()

		df2, err := ReadCSV("../../testdata/weather_data_part_01.csv").Execute()
		require.NoError(t, err)
		defer df2.Release()

		df3, err := ReadCSV("../../testdata/weather_data_part_02.csv").Execute()
		require.NoError(t, err)
		defer df3.Release()

		// Concatenate all three DataFrames
		result, err := Concat(df1, df2, df3).Execute()
		require.NoError(t, err)
		defer result.Release()

		// Should have 3M rows total
		height, err := result.Height()
		require.NoError(t, err)
		require.Equal(t, 3000000, height)

		// Count aggregation should also show 3M
		countResult, err := result.Count().Execute()
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
				df, err := ReadCSV(path).Execute()
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
		result, err := Concat(dfs...).Execute()
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
		).Execute()

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
		).Execute()

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
		).Execute()

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

func TestComplexExpressions(t *testing.T) {
	t.Run("ChainedArithmeticAndComparison", func(t *testing.T) {
		df := ReadCSV("../../testdata/sample.csv")
		// Test: (salary / 1000) * 2 > 100 should match everyone except Alice
		result, err := df.Filter(
			Col("salary").Div(Lit(1000)).Mul(Lit(2)).Gt(Lit(100)),
		).Execute()
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
		).Execute()
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
