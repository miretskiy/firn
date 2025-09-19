package polars

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDataFrame(t *testing.T) {
	df := NewDataFrame()
	defer df.Release()

	// Should be lazy - no handle yet
	require.Equal(t, Handle(0), df.handle, "Expected zero handle before Execute()")

	// Execute to materialize
	executed, err := df.Execute()
	require.NoError(t, err)

	// Should now have a handle
	require.NotEqual(t, Handle(0), executed.handle, "Expected non-zero handle after Execute()")
}

func TestReadCSV(t *testing.T) {
	// Read directly from testdata
	csvFile := "../../testdata/sample.csv"

	df := ReadCSV(csvFile)
	defer df.Release()

	// Should be lazy - no handle yet
	require.Equal(t, Handle(0), df.handle, "Expected zero handle before Execute()")

	// Execute to materialize
	executed, err := df.Execute()
	require.NoError(t, err)

	// Should now have a handle
	require.NotEqual(t, Handle(0), executed.handle, "Expected non-zero handle after Execute()")
}

func TestReadCSVNonExistentFile(t *testing.T) {
	df := ReadCSV("/non/existent/file.csv") // ReadCSV is lazy, no error yet
	defer df.Release()

	// Error should happen on Execute()
	_, err := df.Execute()
	require.Error(t, err, "Expected error when executing with non-existent file")

	// Check that it's a polars Error
	polarsErr, ok := err.(*Error)
	require.True(t, ok, "Expected polars.Error, got %T", err)
	require.NotEqual(t, 0, polarsErr.Code, "Expected non-zero error code")
	require.NotEmpty(t, polarsErr.Message, "Expected non-empty error message")
}

func TestDataFrameRelease(t *testing.T) {
	df := NewDataFrame()

	// Execute first to get a handle
	executed, err := df.Execute()
	require.NoError(t, err)
	require.NotEqual(t, Handle(0), executed.handle, "Expected non-zero handle after Execute()")

	// Release
	err = executed.Release()
	require.NoError(t, err)

	// Should be marked as released (handle == 0)
	require.Equal(t, Handle(0), executed.handle, "DataFrame should be marked as released (handle should be 0)")

	// Double release should be safe
	err = executed.Release()
	require.NoError(t, err)
}

func TestDataFrameString(t *testing.T) {
	df := NewDataFrame()
	defer df.Release()

	// Before execution - should show lazy state
	str := df.String()
	require.Equal(t, "DataFrame{lazy: 1 ops}", str)
	t.Logf("DataFrame string (lazy): %s", str)

	// After execution - should show Polars formatted output
	executed, err := df.Execute()
	require.NoError(t, err)
	defer executed.Release()

	str = executed.String()
	expectedExecuted := `shape: (0, 0)
┌┐
╞╡
└┘`
	require.Equal(t, expectedExecuted, str)
	t.Logf("DataFrame string (executed): %s", str)

	// After release - handle is cleared, so it shows empty state
	executed.Release()
	str = executed.String()
	require.Equal(t, "DataFrame{empty}", str)
}

func TestDataFrameOperationsWithLargeData(t *testing.T) {
	csvFile := "../../testdata/large_sample.csv"

	t.Run("ReadCSV_Works", func(t *testing.T) {
		df := ReadCSV(csvFile)
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()

		// Verify it contains expected shape and data
		output := executed.String()
		require.Contains(t, output, "shape: (26, 7)")
		require.Contains(t, output, "Alice Johnson")
		require.Contains(t, output, "Engineering")
		t.Logf("ReadCSV output:\n%s", output)
	})

	t.Run("Filter_HighSalary_Works", func(t *testing.T) {
		df := ReadCSV(csvFile).Filter(Col("salary").Gt(Lit(70000)))
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()

		// Should have fewer rows (only high salary employees)
		output := executed.String()
		require.Contains(t, output, "Alice Johnson")   // 75000 > 70000
		require.Contains(t, output, "Charlie Brown")   // 85000 > 70000
		require.NotContains(t, output, "Diana Prince") // 60000 < 70000
		t.Logf("Filter high salary output:\n%s", output)
	})

	t.Run("Select_Columns_Works", func(t *testing.T) {
		df := ReadCSV(csvFile).Select("name", "department", "salary")
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()

		// Should only have 3 columns
		output := executed.String()
		require.Contains(t, output, "shape: (26, 3)")
		require.Contains(t, output, "name")
		require.Contains(t, output, "department")
		require.Contains(t, output, "salary")
		require.NotContains(t, output, "age") // Should not have age column
		t.Logf("Select columns output:\n%s", output)
	})

	t.Run("Complex_Chain_Works", func(t *testing.T) {
		df := ReadCSV(csvFile).
			Filter(Col("department").Eq(Lit("Engineering"))).
			Filter(Col("age").Gt(Lit(30))).
			Select("name", "age", "salary")
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()

		// Should have only senior engineers
		output := executed.String()
		require.Contains(t, output, "shape:")           // Has some results
		require.Contains(t, output, "Charlie Brown")    // 35, Engineering
		require.NotContains(t, output, "Alice Johnson") // 25 < 30
		require.NotContains(t, output, "Bob Smith")     // Marketing, not Engineering
		t.Logf("Complex chain output:\n%s", output)
	})
}

func BenchmarkNewDataFrame(b *testing.B) {
	for i := 0; i < b.N; i++ {
		df := NewDataFrame()
		df.Release()
	}
}

func TestSelect(t *testing.T) {
	// Read CSV and select specific columns
	csvFile := "../../testdata/sample.csv"

	df := ReadCSV(csvFile).Select("name", "age")
	defer df.Release()

	// Should be lazy - no handle yet
	require.Equal(t, Handle(0), df.handle, "Expected zero handle before Execute()")
	require.Equal(t, 2, len(df.operations), "Expected 2 operations: ReadCSV + Select")

	// Execute to materialize
	executed, err := df.Execute()
	require.NoError(t, err)

	// Should now have a handle
	require.NotEqual(t, Handle(0), executed.handle, "Expected non-zero handle after Execute()")
}

func TestExecuteChaining(t *testing.T) {
	// Test that we can chain operations after Execute()
	csvFile := "../../testdata/sample.csv"

	// First execution
	df := ReadCSV(csvFile)
	executed1, err := df.Execute()
	require.NoError(t, err)
	require.NotEqual(t, Handle(0), executed1.handle, "Should have handle after first Execute()")
	defer executed1.Release()

	// Chain more operations on the executed DataFrame
	chained := executed1.Select("name")
	require.Equal(t, executed1.handle, chained.handle, "Should retain handle when chaining")
	require.Equal(t, 1, len(chained.operations), "Should have 1 new operation")

	// Second execution
	executed2, err := chained.Execute()
	require.NoError(t, err)
	require.NotEqual(t, Handle(0), executed2.handle, "Should have handle after second Execute()")
	require.Equal(t, 0, len(executed2.operations), "Operations should be cleared after Execute()")

	// The handle might be different (new DataFrame from select operation)
	// That's fine - Polars operations are immutable
}

func TestLiteralTypes(t *testing.T) {
	// Test our new centralized Literal abstraction with different value types
	csvFile := "../../testdata/sample.csv"

	// Test Int64 literal
	t.Run("Int64Literal", func(t *testing.T) {
		df := ReadCSV(csvFile).Filter(Col("age").Gt(Lit(int64(25))))
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
		require.NotEqual(t, Handle(0), executed.handle)
	})

	// Test regular int (should convert to int64)
	t.Run("IntLiteral", func(t *testing.T) {
		df := ReadCSV(csvFile).Filter(Col("age").Gt(Lit(25)))
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
		require.NotEqual(t, Handle(0), executed.handle)
	})

	// Test Float64 literal
	t.Run("Float64Literal", func(t *testing.T) {
		df := ReadCSV(csvFile).Filter(Col("age").Gt(Lit(25.5)))
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
		require.NotEqual(t, Handle(0), executed.handle)
	})

	// Test String literal
	t.Run("StringLiteral", func(t *testing.T) {
		df := ReadCSV(csvFile).Filter(Col("name").Eq(Lit("Alice")))
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
		require.NotEqual(t, Handle(0), executed.handle)
	})

	// Test Bool literal - create a simple DataFrame with boolean column
	t.Run("BoolLiteral", func(t *testing.T) {
		// For this test, we'll just verify the literal creation works
		// even if the column doesn't exist - the error will be about the column, not the literal
		df := ReadCSV(csvFile).Filter(Col("nonexistent_bool_column").Eq(Lit(true)))
		defer df.Release()

		// This should fail with "column not found", not with literal conversion error
		_, err := df.Execute()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unable to find column")
		// The important thing is that the boolean literal was created successfully
	})

	// Test fallback to string for unknown types
	t.Run("UnknownTypeLiteral", func(t *testing.T) {
		type CustomType struct{ Value int }
		custom := CustomType{Value: 42}

		df := ReadCSV(csvFile).Filter(Col("name").Eq(Lit(custom))) // Should convert to string "{42}"
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
		require.NotEqual(t, Handle(0), executed.handle)
	})
}

func TestComparisonOperations(t *testing.T) {
	// Test all comparison operations with the new Literal abstraction
	csvFile := "../../testdata/sample.csv"

	t.Run("GreaterThan", func(t *testing.T) {
		df := ReadCSV(csvFile).Filter(Col("age").Gt(Lit(25)))
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
	})

	t.Run("LessThan", func(t *testing.T) {
		df := ReadCSV(csvFile).Filter(Col("age").Lt(Lit(50)))
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
	})

	t.Run("Equals", func(t *testing.T) {
		df := ReadCSV(csvFile).Filter(Col("name").Eq(Lit("Alice")))
		defer df.Release()

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
	})

	// Test chaining multiple filters
	t.Run("ChainedComparisons", func(t *testing.T) {
		df := ReadCSV(csvFile).
			Filter(Col("age").Gt(Lit(20))).
			Filter(Col("age").Lt(Lit(60))).
			Filter(Col("department").Eq(Lit("Engineering")))
		defer df.Release()

		require.Equal(t, 4, len(df.operations), "Should have 4 operations: ReadCSV + 3 filters")

		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()
		require.Equal(t, 0, len(executed.operations), "Operations should be cleared after Execute()")
	})
}

func TestExpressionAPI(t *testing.T) {
	// Test our new expression API - Col("age").Gt(Lit(25))

	t.Run("SimpleExpression", func(t *testing.T) {
		// Build expression: Col("age").Gt(Lit(25))
		expr := Col("age").Gt(Lit(25))

		// Verify the expression has the right number of operations
		require.Equal(t, 3, len(expr.ops), "Should have 3 operations: Col, Lit, Gt")

		// Verify operation types by checking function pointers
		require.NotEqual(t, uintptr(0), expr.ops[0].funcPtr, "First op should have function pointer")
		require.NotEqual(t, uintptr(0), expr.ops[1].funcPtr, "Second op should have function pointer")
		require.NotEqual(t, uintptr(0), expr.ops[2].funcPtr, "Third op should have function pointer")
	})

	t.Run("ChainedExpression", func(t *testing.T) {
		// Build more complex expression: Col("age").Gt(Lit(25)).Eq(Col("active"))
		expr := Col("age").Gt(Lit(25)).Eq(Col("status"))

		// Should have: Col("age"), Lit(25), Gt, Col("status"), Eq = 5 operations
		require.Equal(t, 5, len(expr.ops), "Should have 5 operations")
	})

	t.Run("LiteralTypes", func(t *testing.T) {
		// Test different literal types
		intExpr := Lit(42)
		require.Equal(t, 1, len(intExpr.ops))

		floatExpr := Lit(3.14)
		require.Equal(t, 1, len(floatExpr.ops))

		stringExpr := Lit("hello")
		require.Equal(t, 1, len(stringExpr.ops))

		boolExpr := Lit(true)
		require.Equal(t, 1, len(boolExpr.ops))
	})
}

func TestEndToEndExpressions(t *testing.T) {
	// End-to-end test: CSV -> Expression Filter -> Execute
	csvFile := "../../testdata/sample.csv"

	t.Run("SimpleFilter", func(t *testing.T) {
		// Test: ReadCSV -> Filter(Col("age").Gt(Lit(25))) -> Execute
		filter := Col("age").Gt(Lit(25))

		df := ReadCSV(csvFile).Filter(filter)
		defer df.Release()

		// Verify the filter expression was consumed
		require.Equal(t, 0, len(filter.ops), "Expression should be consumed by Filter()")

		// Verify DataFrame has the right operations
		require.Equal(t, 2, len(df.operations), "Should have ReadCSV + Filter operations")

		// Execute the operations
		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()

		require.NotEqual(t, Handle(0), executed.handle, "Should have valid handle after execution")

		// Inspect the results
		csv, err := executed.ToCsv()
		require.NoError(t, err)
		require.NotEmpty(t, csv, "CSV output should not be empty")

		// Verify the filter worked - should only have rows where age > 25
		// Original data: Alice(25), Bob(30), Charlie(35), Diana(28), Eve(32), Frank(29), Grace(27)
		// Expected: Bob(30), Charlie(35), Diana(28), Eve(32), Frank(29), Grace(27)
		t.Logf("Filtered CSV (age > 25):\n%s", csv)
		require.Contains(t, csv, "Bob", "Should contain Bob (age 30)")
		require.Contains(t, csv, "Charlie", "Should contain Charlie (age 35)")
		require.NotContains(t, csv, "Alice", "Should not contain Alice (age 25)")
	})

	t.Run("ComplexFilter", func(t *testing.T) {
		// Test: ReadCSV -> Filter(Col("age").Gt(Lit(25)).Eq(Col("salary").Lt(Lit(60000)))) -> Execute
		filter := Col("age").Gt(Lit(25)).Eq(Col("salary").Lt(Lit(60000)))

		df := ReadCSV(csvFile).Filter(filter).Select("name", "age")
		defer df.Release()

		// Execute the operations
		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()

		require.NotEqual(t, Handle(0), executed.handle, "Should have valid handle after execution")
	})

	t.Run("ChainedOperations", func(t *testing.T) {
		// Test: ReadCSV -> Filter -> Select -> Execute
		ageFilter := Col("age").Gt(Lit(30))

		df := ReadCSV(csvFile).
			Filter(ageFilter).
			Select("name", "age", "salary")
		defer df.Release()

		// Should have 3 operations: ReadCSV, Filter, Select
		require.Equal(t, 3, len(df.operations), "Should have 3 operations")

		// Execute
		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()

		require.NotEqual(t, Handle(0), executed.handle, "Should have valid handle")
		require.Equal(t, 0, len(executed.operations), "Operations should be cleared after Execute")

		// Inspect the results
		csv, err := executed.ToCsv()
		require.NoError(t, err)
		require.NotEmpty(t, csv, "CSV output should not be empty")

		// Should only have people with age > 30: Bob(30), Charlie(35), Eve(32)
		// But age > 30 means Charlie(35) and Eve(32) only
		t.Logf("Filtered and selected CSV (age > 30, name/age/salary only):\n%s", csv)
		require.Contains(t, csv, "Charlie", "Should contain Charlie (age 35)")
		require.Contains(t, csv, "Eve", "Should contain Eve (age 32)")
		require.NotContains(t, csv, "Bob", "Should not contain Bob (age 30, not > 30)")
		require.NotContains(t, csv, "department", "Should not contain department column (not selected)")
	})

	t.Run("MultipleFilters", func(t *testing.T) {
		// Test multiple separate filters
		ageFilter := Col("age").Gt(Lit(25))
		salaryFilter := Col("salary").Lt(Lit(65000))

		df := ReadCSV(csvFile).
			Filter(ageFilter).
			Filter(salaryFilter)
		defer df.Release()

		// Should have 3 operations: ReadCSV, Filter, Filter
		require.Equal(t, 3, len(df.operations), "Should have 3 operations")

		// Execute
		executed, err := df.Execute()
		require.NoError(t, err)
		defer executed.Release()

		require.NotEqual(t, Handle(0), executed.handle, "Should have valid handle")
	})
}

// Test move semantics and error cases
func TestExpressionMoveSemantics(t *testing.T) {
	t.Run("BasicMoveSemantics", func(t *testing.T) {
		// Create expressions
		col := Col("age")
		lit := Lit(25)

		// Use them in a comparison - this should consume both
		gtExpr := col.Gt(lit)

		// Verify the expressions are consumed
		require.Equal(t, 0, len(col.ops), "col should be consumed after Gt()")
		require.Equal(t, 0, len(lit.ops), "lit should be consumed after Gt()")
		require.Greater(t, len(gtExpr.ops), 0, "result expression should not be consumed")
	})

	t.Run("ErrorOnReuse", func(t *testing.T) {
		col := Col("age")
		lit := Lit(25)

		// First use - should work
		_ = col.Gt(lit)

		// Second use - should panic
		require.Panics(t, func() {
			col.Lt(Lit(30)) // col is already consumed
		}, "should panic when reusing consumed expression")

		require.Panics(t, func() {
			lit.Eq(Lit(25)) // lit is already consumed
		}, "should panic when reusing consumed expression")
	})

	t.Run("CloneForReuse", func(t *testing.T) {
		col := Col("age")

		// Clone for reuse (simple value copy with Go)
		colClone := col

		// Use original
		gtExpr := col.Gt(Lit(25))
		require.Equal(t, 0, len(col.ops), "original should be consumed")

		// Use clone - should work
		ltExpr := colClone.Lt(Lit(30))
		require.Equal(t, 0, len(colClone.ops), "clone should be consumed after use")

		// Both results should be valid
		require.Greater(t, len(gtExpr.ops), 0, "gtExpr should not be consumed")
		require.Greater(t, len(ltExpr.ops), 0, "ltExpr should not be consumed")
	})

	t.Run("ValueSemanticsReuse", func(t *testing.T) {
		col := Col("age")

		// With value semantics, we can reuse expressions naturally
		gtExpr1 := col.Gt(Lit(25))
		gtExpr2 := col.Gt(Lit(30)) // This should work fine now

		// Both expressions should be valid
		require.Greater(t, len(gtExpr1.ops), 0, "gtExpr1 should be valid")
		require.Greater(t, len(gtExpr2.ops), 0, "gtExpr2 should be valid")
	})

	t.Run("EndToEndWithMoveSemantics", func(t *testing.T) {
		// Test that expressions work correctly in DataFrame operations
		df := ReadCSV("../../testdata/large_sample.csv")

		// Create expression with move semantics
		col := Col("age")
		filterExpr := col.Gt(Lit(25))

		// Use in filter - should consume the expression
		filtered := df.Filter(filterExpr)
		require.Equal(t, 0, len(filterExpr.ops), "filter expression should be consumed")

		// Execute and verify
		executed, err := filtered.Execute()
		require.NoError(t, err)
		require.NoError(t, executed.Release())
	})
}

func BenchmarkReadCSV(b *testing.B) {
	// Read directly from testdata
	csvFile := "../../testdata/small.csv"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df := ReadCSV(csvFile)
		df.Release()
	}
}
