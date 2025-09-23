use polars::prelude::{Expr, col, lit, df, JoinArgs as PolarJoinArgs, JoinType as PolarJoinType, IntoLazy};
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

#[test]
fn test_basic_join_functionality() {
    // Test basic join functionality using Polars directly
    let left_df = df! {
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
    }
    .unwrap();

    let right_df = df! {
        "id" => [1, 2, 4],
        "age" => [25, 30, 35],
    }
    .unwrap();

    // Test inner join
    let result = left_df
        .lazy()
        .join(
            right_df.lazy(),
            [col("id")],
            [col("id")],
            PolarJoinArgs::new(PolarJoinType::Inner),
        )
        .collect()
        .unwrap();

    // Should have 2 rows (id 1 and 2 match)
    assert_eq!(result.height(), 2);
    assert_eq!(result.width(), 3); // id, name, age

    // Verify the data
    let ids = result.column("id").unwrap().i32().unwrap();
    assert_eq!(ids.get(0), Some(1));
    assert_eq!(ids.get(1), Some(2));

    let names = result.column("name").unwrap().str().unwrap();
    assert_eq!(names.get(0), Some("Alice"));
    assert_eq!(names.get(1), Some("Bob"));

    let ages = result.column("age").unwrap().i32().unwrap();
    assert_eq!(ages.get(0), Some(25));
    assert_eq!(ages.get(1), Some(30));
}

#[test]
fn test_self_join_scenario() {
    // Test the critical self-join scenario
    let df = df! {
        "id" => [1, 2, 3],
        "value" => [10, 20, 30],
    }
    .unwrap();

    // Self-join: join df with itself on id
    let result = df
        .clone()
        .lazy()
        .join(
            df.lazy(),
            [col("id")],
            [col("id")],
            PolarJoinArgs::new(PolarJoinType::Inner).with_suffix(Some("_right".into())),
        )
        .collect()
        .unwrap();

    // Should have 3 rows (all ids match with themselves)
    assert_eq!(result.height(), 3);
    assert_eq!(result.width(), 3); // id, value, value_right

    // Verify the data - all values should be duplicated
    let ids = result.column("id").unwrap().i32().unwrap();
    let values = result.column("value").unwrap().i32().unwrap();
    let values_right = result.column("value_right").unwrap().i32().unwrap();

    for i in 0..3 {
        assert_eq!(ids.get(i), Some(i as i32 + 1));
        assert_eq!(values.get(i), Some((i as i32 + 1) * 10));
        assert_eq!(values_right.get(i), Some((i as i32 + 1) * 10));
    }
}

#[test]
fn test_join_types() {
    let left_df = df! {
        "id" => [1, 2, 3],
        "left_val" => ["A", "B", "C"],
    }
    .unwrap();

    let right_df = df! {
        "id" => [2, 3, 4],
        "right_val" => ["X", "Y", "Z"],
    }
    .unwrap();

    // Test Left Join
    let left_result = left_df
        .clone()
        .lazy()
        .join(
            right_df.clone().lazy(),
            [col("id")],
            [col("id")],
            PolarJoinArgs::new(PolarJoinType::Left),
        )
        .collect()
        .unwrap();

    // Should have 3 rows (all from left)
    assert_eq!(left_result.height(), 3);
    assert_eq!(left_result.width(), 3); // id, left_val, right_val

    // Test Right Join
    let right_result = left_df
        .clone()
        .lazy()
        .join(
            right_df.clone().lazy(),
            [col("id")],
            [col("id")],
            PolarJoinArgs::new(PolarJoinType::Right),
        )
        .collect()
        .unwrap();

    // Debug: let's see what we actually got for right join
    println!("Right join result: {}", right_result);
    println!("Right join height: {}, width: {}", right_result.height(), right_result.width());

    // Test Full Join
    let full_result = left_df
        .lazy()
        .join(
            right_df.lazy(),
            [col("id")],
            [col("id")],
            PolarJoinArgs::new(PolarJoinType::Full),
        )
        .collect()
        .unwrap();

    // Debug: let's see what we actually got for full join
    println!("Full join result: {}", full_result);
    println!("Full join height: {}, width: {}", full_result.height(), full_result.width());
    
    // Basic sanity checks - verify we got the expected results
    assert_eq!(full_result.height(), 4); // Should have 4 rows (union of both sides)
    assert_eq!(full_result.width(), 4); // id, left_val, id_right, right_val (no coalescing by default)
    
    // The full join should contain all unique IDs from both sides
    // Left: [1,2,3], Right: [2,3,4] -> Full join should have rows for [1,2,3,4]
}
