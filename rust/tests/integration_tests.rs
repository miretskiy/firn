use turbo_polars::*;
use polars::prelude::*;

#[test]
fn test_column_name_preservation() {
    // Test that col("salary") * lit(2) replaces the "salary" column
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }.unwrap();

    let result = df.lazy().with_columns([col("salary") * lit(2)]).collect().unwrap();
    
    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(0), Some(100000));
    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(1), Some(120000));
}

#[test]
fn test_column_name_with_alias() {
    // Test that (col("salary") * lit(2)).alias("double_salary") creates a new column
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }.unwrap();

    let result = df.lazy()
        .with_columns([(col("salary") * lit(2)).alias("double_salary")])
        .collect()
        .unwrap();
    
    // Original salary column should be unchanged
    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(0), Some(50000));
    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(1), Some(60000));
    
    // New double_salary column should exist
    assert_eq!(result.column("double_salary").unwrap().i32().unwrap().get(0), Some(100000));
    assert_eq!(result.column("double_salary").unwrap().i32().unwrap().get(1), Some(120000));
}

#[test]
fn test_expr_stack_machine_with_columns() {
    // Test the expression stack machine with a non-aliased expression
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }.unwrap();

    let mut stack: Vec<Expr> = Vec::new();
    
    // Simulate: col("salary")
    stack.push(col("salary"));
    
    // Simulate: lit(2)
    stack.push(lit(2));
    
    // Simulate: multiply operation (pops 2, pushes 1)
    let right = stack.pop().unwrap();
    let left = stack.pop().unwrap();
    stack.push(left * right);
    
    assert_eq!(stack.len(), 1);
    
    let expr = stack.pop().unwrap();
    let result = df.lazy().with_columns([expr]).collect().unwrap();
    
    // Should replace the salary column
    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(0), Some(100000));
    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(1), Some(120000));
}

#[test]
fn test_expr_stack_machine_with_alias() {
    // Test the expression stack machine with an aliased expression
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }.unwrap();

    let mut stack: Vec<Expr> = Vec::new();
    
    // Simulate: col("salary")
    stack.push(col("salary"));
    
    // Simulate: lit(1000)
    stack.push(lit(1000));
    
    // Simulate: add operation (pops 2, pushes 1)
    let right = stack.pop().unwrap();
    let left = stack.pop().unwrap();
    let expr = left + right;
    
    // Simulate: alias operation
    let aliased_expr = expr.alias("salary_bonus");
    stack.push(aliased_expr);
    
    assert_eq!(stack.len(), 1);
    
    let expr = stack.pop().unwrap();
    let result = df.lazy().with_columns([expr]).collect().unwrap();
    
    // Original salary should be unchanged
    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(0), Some(50000));
    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(1), Some(60000));
    
    // New salary_bonus column should exist
    assert_eq!(result.column("salary_bonus").unwrap().i32().unwrap().get(0), Some(51000));
    assert_eq!(result.column("salary_bonus").unwrap().i32().unwrap().get(1), Some(61000));
}

#[test]
fn test_execution_context() {
    // Test that ExecutionContext can pass the expression stack
    let mut stack: Vec<Expr> = Vec::new();
    
    let context = ExecutionContext {
        expr_stack: &mut stack as *mut Vec<Expr>,
        operation_args: 0, // No args needed for this test
    };
    
    // Verify we can access the stack through the context
    let stack_ref = unsafe { &mut *context.expr_stack };
    stack_ref.push(col("test"));
    
    assert_eq!(stack.len(), 1);
}

#[test]
fn test_read_csv_args() {
    
    // Create a test CSV file
    let csv_content = "name,age,salary\nAlice,25,50000\nBob,30,60000\n";
    std::fs::write("test_sample.csv", csv_content).unwrap();
    
    // Test ReadCsvArgs initialization
    let path_bytes = b"test_sample.csv\0";
    let raw_str = RawStr {
        data: path_bytes.as_ptr() as *const std::os::raw::c_char,
        len: path_bytes.len() - 1, // Exclude null terminator
    };
    
    let args = ReadCsvArgs {
        path: raw_str,
        has_header: true,
        with_glob: false,
    };
    
    // Verify we can read the path
    let path_str = unsafe { args.path.as_str() }.unwrap();
    assert_eq!(path_str, "test_sample.csv");
    
    // Clean up
    std::fs::remove_file("test_sample.csv").unwrap();
}
