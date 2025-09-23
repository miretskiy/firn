use polars::prelude::*;
use firn::*;

#[test]
fn test_basic_functionality() {
    // Test basic expression stack operations
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }
    .unwrap();

    // Test column name preservation (no alias)
    let result = df
        .clone()
        .lazy()
        .with_columns([col("salary") * lit(2)])
        .collect()
        .unwrap();

    assert_eq!(result.column("salary").unwrap().i32().unwrap().get(0), Some(100000));

    // Test column name with alias
    let result2 = df
        .lazy()
        .with_columns([(col("salary") * lit(2)).alias("double_salary")])
        .collect()
        .unwrap();

    assert_eq!(result2.column("salary").unwrap().i32().unwrap().get(0), Some(50000)); // Original unchanged
    assert_eq!(result2.column("double_salary").unwrap().i32().unwrap().get(0), Some(100000)); // New column
}

#[test]
fn test_sql_expressions() {
    let df = df! {
        "name" => ["Alice", "Bob", "Charlie"],
        "salary" => [50000, 60000, 70000],
        "bonus" => [5000, 6000, 7000],
    }
    .unwrap();

    // Test basic SQL expressions
    let result = df
        .clone()
        .lazy()
        .filter(polars_sql::sql_expr("salary > 55000").unwrap())
        .collect()
        .unwrap();

    assert_eq!(result.height(), 2); // Bob and Charlie

    // Test complex SQL with functions
    let result2 = df
        .lazy()
        .with_columns([
            polars_sql::sql_expr("salary * 1.1").unwrap().alias("boosted_salary"),
            polars_sql::sql_expr("(salary + bonus) / 12").unwrap().alias("monthly_total"),
        ])
        .collect()
        .unwrap();

    assert_eq!(result2.width(), 5); // original 3 + 2 new columns
    
    // Verify calculations
    let boosted_col = result2.column("boosted_salary").unwrap();
    let boosted_val = if let Ok(f64_col) = boosted_col.f64() {
        f64_col.get(0).unwrap()
    } else {
        boosted_col.i32().unwrap().get(0).unwrap() as f64
    };
    assert!((boosted_val - 55000.0).abs() < 0.001);
}

#[test]
fn test_execution_context() {
    // Test that ExecutionContext can pass the expression stack
    let mut stack: Vec<Expr> = Vec::new();

    let context = ExecutionContext {
        expr_stack: &mut stack as *mut Vec<Expr>,
        operation_args: 0,
    };

    // Verify we can access the stack through the context
    let stack_ref = unsafe { &mut *context.expr_stack };
    stack_ref.push(col("test"));

    assert_eq!(stack.len(), 1);
}

#[test]
fn test_read_csv_args() {
    // Test ReadCsvArgs initialization
    let path_bytes = b"test_sample.csv\0";
    let raw_str = RawStr {
        data: path_bytes.as_ptr() as *const std::os::raw::c_char,
        len: path_bytes.len() - 1,
    };

    let args = ReadCsvArgs {
        path: raw_str,
        has_header: true,
        with_glob: false,
    };

    // Verify we can read the path
    let path_str = unsafe { args.path.as_str() }.unwrap();
    assert_eq!(path_str, "test_sample.csv");
}
