package polars

import (
	"testing"
)

func TestCastOperations(t *testing.T) {
	// Test basic cast functionality
	t.Run("Cast integer to float", func(t *testing.T) {
		// Create a simple expression that casts an integer column to float
		expr := Col("age").Cast(Float64)
		
		// Verify the expression has the expected number of operations
		if expr.countOps() != 2 { // Column + Cast
			t.Errorf("Expected 2 operations, got %d", expr.countOps())
		}
	})
	
	t.Run("Cast with strict mode", func(t *testing.T) {
		// Test strict casting
		expr := Col("price").CastStrict(Int32, true)
		
		// Verify the expression has the expected number of operations
		if expr.countOps() != 2 { // Column + Cast
			t.Errorf("Expected 2 operations, got %d", expr.countOps())
		}
	})
	
	t.Run("Cast with options", func(t *testing.T) {
		// Test casting with full options
		expr := Col("value").CastWithOptions(Float32, false, true)
		
		// Verify the expression has the expected number of operations
		if expr.countOps() != 2 { // Column + Cast
			t.Errorf("Expected 2 operations, got %d", expr.countOps())
		}
	})
	
	t.Run("DataType constants", func(t *testing.T) {
		// Test that DataType constants have expected values
		testCases := []struct {
			name     string
			dataType DataType
			expected uint32
		}{
			{"Int32", Int32, 0x0000_0003},
			{"Float64", Float64, 0x0001_0002},
			{"String", String, 0x0002_0001},
			{"Boolean", Boolean, 0x0004_0001},
			{"Date", Date, 0x0003_0001},
		}
		
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if uint32(tc.dataType) != tc.expected {
					t.Errorf("Expected %s to have value 0x%08X, got 0x%08X", 
						tc.name, tc.expected, uint32(tc.dataType))
				}
			})
		}
	})
	
	t.Run("Chained operations with cast", func(t *testing.T) {
		// Test chaining cast with other operations
		expr := Col("salary").Cast(Float64).Mul(Lit(1.1)).Alias("bonus_salary")
		
		// Should have: Column + Cast + Literal + Mul + Alias = 5 operations
		if expr.countOps() != 5 {
			t.Errorf("Expected 5 operations, got %d", expr.countOps())
		}
	})
}
