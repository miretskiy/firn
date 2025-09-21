use polars::prelude::*;
use turbo_polars::*;

#[test]
fn test_column_name_preservation() {
    // Test that col("salary") * lit(2) replaces the "salary" column
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }
    .unwrap();

    let result = df
        .lazy()
        .with_columns([col("salary") * lit(2)])
        .collect()
        .unwrap();

    assert_eq!(
        result.column("salary").unwrap().i32().unwrap().get(0),
        Some(100000)
    );
    assert_eq!(
        result.column("salary").unwrap().i32().unwrap().get(1),
        Some(120000)
    );
}

#[test]
fn test_column_name_with_alias() {
    // Test that (col("salary") * lit(2)).alias("double_salary") creates a new column
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }
    .unwrap();

    let result = df
        .lazy()
        .with_columns([(col("salary") * lit(2)).alias("double_salary")])
        .collect()
        .unwrap();

    // Original salary column should be unchanged
    assert_eq!(
        result.column("salary").unwrap().i32().unwrap().get(0),
        Some(50000)
    );
    assert_eq!(
        result.column("salary").unwrap().i32().unwrap().get(1),
        Some(60000)
    );

    // New double_salary column should exist
    assert_eq!(
        result
            .column("double_salary")
            .unwrap()
            .i32()
            .unwrap()
            .get(0),
        Some(100000)
    );
    assert_eq!(
        result
            .column("double_salary")
            .unwrap()
            .i32()
            .unwrap()
            .get(1),
        Some(120000)
    );
}

#[test]
fn test_expr_stack_machine_with_columns() {
    // Test the expression stack machine with a non-aliased expression
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }
    .unwrap();

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
    assert_eq!(
        result.column("salary").unwrap().i32().unwrap().get(0),
        Some(100000)
    );
    assert_eq!(
        result.column("salary").unwrap().i32().unwrap().get(1),
        Some(120000)
    );
}

#[test]
fn test_expr_stack_machine_with_alias() {
    // Test the expression stack machine with an aliased expression
    let df = df! {
        "name" => ["Alice", "Bob"],
        "salary" => [50000, 60000],
    }
    .unwrap();

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
    assert_eq!(
        result.column("salary").unwrap().i32().unwrap().get(0),
        Some(50000)
    );
    assert_eq!(
        result.column("salary").unwrap().i32().unwrap().get(1),
        Some(60000)
    );

    // New salary_bonus column should exist
    assert_eq!(
        result.column("salary_bonus").unwrap().i32().unwrap().get(0),
        Some(51000)
    );
    assert_eq!(
        result.column("salary_bonus").unwrap().i32().unwrap().get(1),
        Some(61000)
    );
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

#[test]
fn test_string_operations_basic() {
    // Test basic string operations: len, to_lowercase, to_uppercase
    let df = df! {
        "name" => ["Alice", "BOB", "Charlie"],
        "department" => ["Engineering", "SALES", "marketing"],
    }
    .unwrap();

    // Test string length
    let result = df
        .clone()
        .lazy()
        .with_columns([
            col("name").str().len_chars().alias("name_len"),
            col("department").str().len_chars().alias("dept_len"),
        ])
        .collect()
        .unwrap();

    assert_eq!(
        result.column("name_len").unwrap().u32().unwrap().get(0),
        Some(5)
    ); // "Alice"
    assert_eq!(
        result.column("name_len").unwrap().u32().unwrap().get(1),
        Some(3)
    ); // "BOB"
    assert_eq!(
        result.column("name_len").unwrap().u32().unwrap().get(2),
        Some(7)
    ); // "Charlie"

    assert_eq!(
        result.column("dept_len").unwrap().u32().unwrap().get(0),
        Some(11)
    ); // "Engineering"
    assert_eq!(
        result.column("dept_len").unwrap().u32().unwrap().get(1),
        Some(5)
    ); // "SALES"
    assert_eq!(
        result.column("dept_len").unwrap().u32().unwrap().get(2),
        Some(9)
    ); // "marketing"

    // Test to_lowercase
    let result = df
        .clone()
        .lazy()
        .with_columns([
            col("name").str().to_lowercase().alias("name_lower"),
            col("department").str().to_lowercase().alias("dept_lower"),
        ])
        .collect()
        .unwrap();

    assert_eq!(
        result.column("name_lower").unwrap().str().unwrap().get(0),
        Some("alice")
    );
    assert_eq!(
        result.column("name_lower").unwrap().str().unwrap().get(1),
        Some("bob")
    );
    assert_eq!(
        result.column("name_lower").unwrap().str().unwrap().get(2),
        Some("charlie")
    );

    assert_eq!(
        result.column("dept_lower").unwrap().str().unwrap().get(0),
        Some("engineering")
    );
    assert_eq!(
        result.column("dept_lower").unwrap().str().unwrap().get(1),
        Some("sales")
    );
    assert_eq!(
        result.column("dept_lower").unwrap().str().unwrap().get(2),
        Some("marketing")
    );

    // Test to_uppercase
    let result = df
        .clone()
        .lazy()
        .with_columns([
            col("name").str().to_uppercase().alias("name_upper"),
            col("department").str().to_uppercase().alias("dept_upper"),
        ])
        .collect()
        .unwrap();

    assert_eq!(
        result.column("name_upper").unwrap().str().unwrap().get(0),
        Some("ALICE")
    );
    assert_eq!(
        result.column("name_upper").unwrap().str().unwrap().get(1),
        Some("BOB")
    );
    assert_eq!(
        result.column("name_upper").unwrap().str().unwrap().get(2),
        Some("CHARLIE")
    );

    assert_eq!(
        result.column("dept_upper").unwrap().str().unwrap().get(0),
        Some("ENGINEERING")
    );
    assert_eq!(
        result.column("dept_upper").unwrap().str().unwrap().get(1),
        Some("SALES")
    );
    assert_eq!(
        result.column("dept_upper").unwrap().str().unwrap().get(2),
        Some("MARKETING")
    );
}

#[test]
fn test_string_operations_pattern_matching() {
    // Test pattern matching operations: contains, starts_with, ends_with
    let df = df! {
        "name" => ["Alice", "Bob", "Charlie", "David"],
        "email" => ["alice@example.com", "bob@test.org", "charlie@example.com", "david@company.net"],
    }.unwrap();

    // Test contains
    let result = df
        .clone()
        .lazy()
        .with_columns([
            col("name")
                .str()
                .contains_literal(lit("a"))
                .alias("contains_a"),
            col("email")
                .str()
                .contains_literal(lit("example"))
                .alias("contains_example"),
        ])
        .collect()
        .unwrap();

    assert_eq!(
        result.column("contains_a").unwrap().bool().unwrap().get(0),
        Some(false)
    ); // "Alice" - no lowercase 'a'
    assert_eq!(
        result.column("contains_a").unwrap().bool().unwrap().get(1),
        Some(false)
    ); // "Bob"
    assert_eq!(
        result.column("contains_a").unwrap().bool().unwrap().get(2),
        Some(true)
    ); // "Charlie"
    assert_eq!(
        result.column("contains_a").unwrap().bool().unwrap().get(3),
        Some(true)
    ); // "David"

    assert_eq!(
        result
            .column("contains_example")
            .unwrap()
            .bool()
            .unwrap()
            .get(0),
        Some(true)
    ); // alice@example.com
    assert_eq!(
        result
            .column("contains_example")
            .unwrap()
            .bool()
            .unwrap()
            .get(1),
        Some(false)
    ); // bob@test.org
    assert_eq!(
        result
            .column("contains_example")
            .unwrap()
            .bool()
            .unwrap()
            .get(2),
        Some(true)
    ); // charlie@example.com
    assert_eq!(
        result
            .column("contains_example")
            .unwrap()
            .bool()
            .unwrap()
            .get(3),
        Some(false)
    ); // david@company.net

    // Test starts_with
    let result = df
        .clone()
        .lazy()
        .with_columns([
            col("name")
                .str()
                .starts_with(lit("A"))
                .alias("starts_with_A"),
            col("email")
                .str()
                .starts_with(lit("alice"))
                .alias("starts_with_alice"),
        ])
        .collect()
        .unwrap();

    assert_eq!(
        result
            .column("starts_with_A")
            .unwrap()
            .bool()
            .unwrap()
            .get(0),
        Some(true)
    ); // "Alice"
    assert_eq!(
        result
            .column("starts_with_A")
            .unwrap()
            .bool()
            .unwrap()
            .get(1),
        Some(false)
    ); // "Bob"
    assert_eq!(
        result
            .column("starts_with_A")
            .unwrap()
            .bool()
            .unwrap()
            .get(2),
        Some(false)
    ); // "Charlie"
    assert_eq!(
        result
            .column("starts_with_A")
            .unwrap()
            .bool()
            .unwrap()
            .get(3),
        Some(false)
    ); // "David"

    assert_eq!(
        result
            .column("starts_with_alice")
            .unwrap()
            .bool()
            .unwrap()
            .get(0),
        Some(true)
    ); // alice@example.com
    assert_eq!(
        result
            .column("starts_with_alice")
            .unwrap()
            .bool()
            .unwrap()
            .get(1),
        Some(false)
    ); // bob@test.org
    assert_eq!(
        result
            .column("starts_with_alice")
            .unwrap()
            .bool()
            .unwrap()
            .get(2),
        Some(false)
    ); // charlie@example.com
    assert_eq!(
        result
            .column("starts_with_alice")
            .unwrap()
            .bool()
            .unwrap()
            .get(3),
        Some(false)
    ); // david@company.net

    // Test ends_with
    let result = df
        .clone()
        .lazy()
        .with_columns([
            col("name").str().ends_with(lit("e")).alias("ends_with_e"),
            col("email")
                .str()
                .ends_with(lit(".com"))
                .alias("ends_with_com"),
        ])
        .collect()
        .unwrap();

    assert_eq!(
        result.column("ends_with_e").unwrap().bool().unwrap().get(0),
        Some(true)
    ); // "Alice"
    assert_eq!(
        result.column("ends_with_e").unwrap().bool().unwrap().get(1),
        Some(false)
    ); // "Bob"
    assert_eq!(
        result.column("ends_with_e").unwrap().bool().unwrap().get(2),
        Some(true)
    ); // "Charlie"
    assert_eq!(
        result.column("ends_with_e").unwrap().bool().unwrap().get(3),
        Some(false)
    ); // "David"

    assert_eq!(
        result
            .column("ends_with_com")
            .unwrap()
            .bool()
            .unwrap()
            .get(0),
        Some(true)
    ); // alice@example.com
    assert_eq!(
        result
            .column("ends_with_com")
            .unwrap()
            .bool()
            .unwrap()
            .get(1),
        Some(false)
    ); // bob@test.org
    assert_eq!(
        result
            .column("ends_with_com")
            .unwrap()
            .bool()
            .unwrap()
            .get(2),
        Some(true)
    ); // charlie@example.com
    assert_eq!(
        result
            .column("ends_with_com")
            .unwrap()
            .bool()
            .unwrap()
            .get(3),
        Some(false)
    ); // david@company.net
}

#[test]
fn test_string_args_struct() {
    // Test StringArgs struct functionality
    let pattern_bytes = b"test_pattern\0";
    let raw_str = RawStr {
        data: pattern_bytes.as_ptr() as *const std::os::raw::c_char,
        len: pattern_bytes.len() - 1, // Exclude null terminator
    };

    let args = StringArgs { pattern: raw_str };

    // Verify we can read the pattern
    let pattern_str = unsafe { args.pattern.as_str() }.unwrap();
    assert_eq!(pattern_str, "test_pattern");
}

#[test]
fn test_dispatch_select_with_context_types() {
    // Create test DataFrame
    let df = df! {
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [25, 30, 35],
        "salary" => [50000, 60000, 70000],
    }
    .unwrap();

    // Test 1: DataFrame context -> should return LazyFrame
    let df_handle = PolarsHandle::new(
        Box::into_raw(Box::new(df.clone())) as usize,
        ContextType::DataFrame,
    );

    // Create SelectArgs
    let columns = vec!["name".to_string(), "age".to_string()];
    let column_ptrs: Vec<_> = columns
        .iter()
        .map(|s| {
            let bytes = s.as_bytes();
            RawStr {
                data: bytes.as_ptr() as *const std::os::raw::c_char,
                len: bytes.len(),
            }
        })
        .collect();

    let select_args = SelectArgs {
        columns: column_ptrs.as_ptr() as *mut RawStr,
        column_count: columns.len(), // Fix: use usize, not i32
    };

    let context = ExecutionContext {
        expr_stack: std::ptr::null_mut(), // Not needed for select
        operation_args: &select_args as *const SelectArgs as usize,
    };

    // Save handle value for cleanup before move
    let df_raw_handle = df_handle.handle;

    let result = dispatch_select(df_handle, &context as *const ExecutionContext as usize);

    // Should succeed and return LazyFrame context
    assert_eq!(result.error_code, 0);
    assert_eq!(
        result.polars_handle.get_context_type(),
        Some(ContextType::LazyFrame)
    );

    // Test 2: LazyGroupBy context -> should return error
    let group_by_handle = PolarsHandle::new(
        0x1234, // Dummy handle - we won't dereference it
        ContextType::LazyGroupBy,
    );

    let result = dispatch_select(
        group_by_handle,
        &context as *const ExecutionContext as usize,
    );

    // Should fail with appropriate error
    assert_ne!(result.error_code, 0);

    // Clean up the DataFrame handle
    unsafe {
        let _ = Box::from_raw(df_raw_handle as *mut DataFrame);
    }
}

#[test]
fn test_dispatch_collect_with_context_types() {
    // Create test LazyFrame
    let df = df! {
        "name" => ["Alice", "Bob"],
        "age" => [25, 30],
    }.unwrap();
    
    let lazy_frame = df.lazy().select([col("name")]);
    
    // Test 1: LazyFrame context -> should materialize to DataFrame
    let lazy_handle = PolarsHandle::new(
        Box::into_raw(Box::new(lazy_frame)) as usize,
        ContextType::LazyFrame,
    );
    
    let lazy_raw_handle = lazy_handle.handle;
    let result = dispatch_collect(lazy_handle, 0); // No args needed
    
    // Should succeed and return DataFrame context
    assert_eq!(result.error_code, 0);
    assert_eq!(result.polars_handle.get_context_type(), Some(ContextType::DataFrame));
    
    // Test 2: LazyGroupBy context -> should return error
    let group_by_handle = PolarsHandle::new(
        0x1234, // Dummy handle
        ContextType::LazyGroupBy,
    );
    
    let result = dispatch_collect(group_by_handle, 0);
    
    // Should fail with appropriate error
    assert_ne!(result.error_code, 0);
    
    // Clean up
    unsafe {
        let _ = Box::from_raw(lazy_raw_handle as *mut LazyFrame);
        let _ = Box::from_raw(result.polars_handle.handle as *mut DataFrame);
    }
}

#[test]
fn test_end_to_end_lazy_evaluation() {
    // Test: DataFrame -> Select (LazyFrame) -> Collect (DataFrame)
    let df = df! {
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [25, 30, 35],
        "salary" => [50000, 60000, 70000],
    }.unwrap();

    // Step 1: DataFrame -> Select -> LazyFrame
    let df_handle = PolarsHandle::new(
        Box::into_raw(Box::new(df.clone())) as usize,
        ContextType::DataFrame,
    );
    
    let columns = vec!["name".to_string(), "age".to_string()];
    let column_ptrs: Vec<_> = columns
        .iter()
        .map(|s| {
            let bytes = s.as_bytes();
            RawStr {
                data: bytes.as_ptr() as *const std::os::raw::c_char,
                len: bytes.len(),
            }
        })
        .collect();
    
    let select_args = SelectArgs {
        columns: column_ptrs.as_ptr() as *mut RawStr,
        column_count: columns.len(),
    };
    
    let context = ExecutionContext {
        expr_stack: std::ptr::null_mut(),
        operation_args: &select_args as *const SelectArgs as usize,
    };
    
    let df_raw_handle = df_handle.handle;
    let select_result = dispatch_select(df_handle, &context as *const ExecutionContext as usize);
    
    // Should return LazyFrame
    assert_eq!(select_result.error_code, 0);
    assert_eq!(select_result.polars_handle.get_context_type(), Some(ContextType::LazyFrame));
    
    // Step 2: LazyFrame -> Collect -> DataFrame
    let lazy_raw_handle = select_result.polars_handle.handle;
    let collect_result = dispatch_collect(select_result.polars_handle, 0);
    
    // Should return DataFrame
    assert_eq!(collect_result.error_code, 0);
    assert_eq!(collect_result.polars_handle.get_context_type(), Some(ContextType::DataFrame));
    
    // Verify the result has the right columns
    let result_df = unsafe { &*(collect_result.polars_handle.handle as *const DataFrame) };
    assert_eq!(result_df.get_column_names(), vec!["name", "age"]);
    assert_eq!(result_df.height(), 3);
    
    // Clean up
    unsafe {
        let _ = Box::from_raw(df_raw_handle as *mut DataFrame);
        let _ = Box::from_raw(lazy_raw_handle as *mut LazyFrame);
        let _ = Box::from_raw(collect_result.polars_handle.handle as *mut DataFrame);
    }
}
