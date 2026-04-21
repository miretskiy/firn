//! Constant / stable expression evaluation.
//!
//! Lets the Go caller evaluate a SQL expression like `"1 + 2"`, `"pi()"`, or
//! `"now() + interval '1 hour'"` and get back a single typed scalar value.
//!
//! Stable functions (`now()` for v1) read from an `EvalState` whose fields are
//! atomics updated between evaluations by the Go side.
//!
//! Design notes:
//! - We wrap each user expression as `SELECT <expr> AS __r`. polars-sql handles
//!   FROMless SELECT via `DataFrame::empty().lazy()` (see polars-sql
//!   `context.rs:703-704`), so no dummy table is needed.
//! - Zero-argument UDFs via the polars `FunctionRegistry` don't work in polars
//!   0.51 — `AnonymousFunction` schema inference rejects empty inputs at
//!   `polars-plan/src/plans/aexpr/schema.rs:238`. Built-in nullary functions
//!   like `pi()` sidestep this by returning `lit(...)` directly
//!   (`polars-plan/src/dsl/mod.rs:691`).
//! - To support `now()` without an upstream patch, we do a narrow, quote-aware
//!   textual rewrite of `now()` -> `TIMESTAMP '<iso-8601>'` before calling
//!   polars-sql. The rewrite only looks at identifier tokens outside string
//!   literals, so `'now()'` inside a SQL string stays untouched.
//! - The `FunctionRegistry` scaffolding is kept because it still works for
//!   UDFs that take at least one argument.

use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use polars::prelude::*;
use polars_plan::dsl::UserDefinedFunction;
use polars_sql::function_registry::FunctionRegistry;
use polars_sql::SQLContext;

use crate::{RawStr, ERROR_INVALID_UTF8, ERROR_POLARS_OPERATION};

// Scalar kind tags mirrored on the Go side.
pub const SCALAR_NULL: u8 = 0;
pub const SCALAR_INT64: u8 = 1;
pub const SCALAR_FLOAT64: u8 = 2;
pub const SCALAR_BOOL: u8 = 3;
pub const SCALAR_STRING: u8 = 4;
pub const SCALAR_DATETIME_NS: u8 = 5;
pub const SCALAR_DURATION_NS: u8 = 6;
pub const SCALAR_DATE_DAYS: u8 = 7;

/// FFI return type for scalar evaluation.
#[repr(C)]
pub struct ScalarResult {
    pub kind: u8,
    pub int_val: i64,
    pub float_val: f64,
    pub bool_val: bool,
    pub str_data: *mut c_char, // heap-allocated C string; Go must free via firn_free_cstring
    pub str_len: usize,
    pub error_code: c_int,
    pub error_message: *mut c_char,
}

impl ScalarResult {
    fn ok_null() -> Self {
        Self {
            kind: SCALAR_NULL,
            int_val: 0,
            float_val: 0.0,
            bool_val: false,
            str_data: ptr::null_mut(),
            str_len: 0,
            error_code: 0,
            error_message: ptr::null_mut(),
        }
    }

    fn ok_int(kind: u8, v: i64) -> Self {
        Self { kind, int_val: v, ..Self::ok_null() }
    }

    fn ok_float(v: f64) -> Self {
        Self { kind: SCALAR_FLOAT64, float_val: v, ..Self::ok_null() }
    }

    fn ok_bool(v: bool) -> Self {
        Self { kind: SCALAR_BOOL, bool_val: v, ..Self::ok_null() }
    }

    fn ok_string(v: &str) -> Self {
        let bytes = v.as_bytes();
        let len = bytes.len();
        let cstr = match CString::new(bytes) {
            Ok(c) => c,
            Err(_) => return Self::error(ERROR_POLARS_OPERATION, "string contains NUL byte"),
        };
        Self {
            kind: SCALAR_STRING,
            str_data: cstr.into_raw(),
            str_len: len,
            ..Self::ok_null()
        }
    }

    fn error(code: c_int, msg: &str) -> Self {
        let cmsg = CString::new(msg)
            .unwrap_or_else(|_| CString::new("invalid error message").unwrap());
        Self {
            kind: SCALAR_NULL,
            int_val: 0,
            float_val: 0.0,
            bool_val: false,
            str_data: ptr::null_mut(),
            str_len: 0,
            error_code: code,
            error_message: cmsg.into_raw(),
        }
    }
}

/// Shared mutable state driving stable functions. Every field is an atomic so
/// Go-side setters are lock-free and readers see a consistent value with a
/// single acquire load.
pub struct EvalState {
    /// Current timestamp as nanoseconds since Unix epoch. Returned by `now()`.
    pub now_ns: AtomicI64,
}

impl EvalState {
    fn new() -> Self {
        Self { now_ns: AtomicI64::new(0) }
    }
}

/// A `FunctionRegistry` for user-registered multi-arg UDFs. Zero-arg stable
/// functions are *not* handled here — see the textual rewrite in
/// [`EvalContext::rewrite_stable_fns`].
#[derive(Default)]
struct EvalRegistry {
    udfs: HashMap<String, UserDefinedFunction>,
}

impl FunctionRegistry for EvalRegistry {
    fn register(&mut self, name: &str, fun: UserDefinedFunction) -> PolarsResult<()> {
        self.udfs.insert(name.to_lowercase(), fun);
        Ok(())
    }
    fn get_udf(&self, name: &str) -> PolarsResult<Option<UserDefinedFunction>> {
        Ok(self.udfs.get(&name.to_lowercase()).cloned())
    }
    fn contains(&self, name: &str) -> bool {
        self.udfs.contains_key(&name.to_lowercase())
    }
}

/// Reusable evaluation context. Built once on the Go side per logical
/// "context", then Go mutates state (`SetNow`) and calls `Eval` many times.
pub struct EvalContext {
    state: Arc<EvalState>,
    sql_ctx: SQLContext,
}

impl EvalContext {
    fn new() -> Self {
        let state = Arc::new(EvalState::new());
        let sql_ctx = SQLContext::new().with_function_registry(Arc::new(EvalRegistry::default()));
        Self { state, sql_ctx }
    }

    /// Rewrite well-known zero-arg stable functions as inline literals.
    /// Currently handles only `now()`; extend by adding more arms below.
    ///
    /// `CAST(<int> AS TIMESTAMP(9))` preserves nanosecond precision;
    /// `TIMESTAMP '...'` literals in polars-sql drop to microseconds.
    fn rewrite_stable_fns(&self, sql: &str) -> String {
        let now_ns = self.state.now_ns.load(Ordering::Acquire);
        let replacement = format!("CAST({} AS TIMESTAMP(9))", now_ns);
        replace_zero_arg_call(sql, "now", &replacement)
    }

    fn eval_sql(&mut self, sql: &str) -> PolarsResult<ScalarResult> {
        let rewritten = self.rewrite_stable_fns(sql);
        let query = format!("SELECT {} AS __r", rewritten);
        let lf = self.sql_ctx.execute(&query)?;
        let df = lf.collect()?;
        extract_single_scalar(&df)
    }
}

/// Case-insensitive, quote-aware rewrite of `name()` (possibly with whitespace
/// between the identifier and parens, and between the parens) to `replacement`.
/// Leaves identifiers inside `'...'`, `"..."`, or backtick-quoted strings alone.
fn replace_zero_arg_call(sql: &str, name: &str, replacement: &str) -> String {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let name_lower = name.to_ascii_lowercase();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;

    while i < n {
        let c = bytes[i];

        // Pass quoted literals through verbatim.
        if c == b'\'' || c == b'"' || c == b'`' {
            let quote = c;
            out.push(c as char);
            i += 1;
            while i < n {
                let d = bytes[i];
                out.push(d as char);
                i += 1;
                if d == quote {
                    // SQL doubled-quote escape stays in the quoted region.
                    if i < n && bytes[i] == quote {
                        out.push(quote as char);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        if is_ident_start(c) {
            let start = i;
            while i < n && is_ident_cont(bytes[i]) {
                i += 1;
            }
            let ident = &sql[start..i];
            if ident.eq_ignore_ascii_case(&name_lower) {
                // Look for an immediately-following `(` `)` (whitespace OK).
                let mut j = i;
                while j < n && (bytes[j] as char).is_whitespace() {
                    j += 1;
                }
                if j < n && bytes[j] == b'(' {
                    let mut k = j + 1;
                    while k < n && (bytes[k] as char).is_whitespace() {
                        k += 1;
                    }
                    if k < n && bytes[k] == b')' {
                        out.push_str(replacement);
                        i = k + 1;
                        continue;
                    }
                }
            }
            out.push_str(ident);
            continue;
        }

        out.push(c as char);
        i += 1;
    }

    out
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}

fn is_ident_cont(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn extract_single_scalar(df: &DataFrame) -> PolarsResult<ScalarResult> {
    let cols = df.get_columns();
    if cols.len() != 1 {
        polars_bail!(ComputeError: "expected 1 column in eval result, got {}", cols.len());
    }
    let col = &cols[0];
    match col.len() {
        0 => Ok(ScalarResult::ok_null()),
        1 => Ok(anyvalue_to_scalar(col.get(0)?)),
        n => polars_bail!(ComputeError: "expected scalar result (1 row), got {} rows", n),
    }
}

fn anyvalue_to_scalar(av: AnyValue<'_>) -> ScalarResult {
    match av {
        AnyValue::Null => ScalarResult::ok_null(),
        AnyValue::Boolean(b) => ScalarResult::ok_bool(b),
        AnyValue::Int8(v) => ScalarResult::ok_int(SCALAR_INT64, v as i64),
        AnyValue::Int16(v) => ScalarResult::ok_int(SCALAR_INT64, v as i64),
        AnyValue::Int32(v) => ScalarResult::ok_int(SCALAR_INT64, v as i64),
        AnyValue::Int64(v) => ScalarResult::ok_int(SCALAR_INT64, v),
        AnyValue::UInt8(v) => ScalarResult::ok_int(SCALAR_INT64, v as i64),
        AnyValue::UInt16(v) => ScalarResult::ok_int(SCALAR_INT64, v as i64),
        AnyValue::UInt32(v) => ScalarResult::ok_int(SCALAR_INT64, v as i64),
        // UInt64 loses sign when > i64::MAX; acceptable for v1.
        AnyValue::UInt64(v) => ScalarResult::ok_int(SCALAR_INT64, v as i64),
        AnyValue::Float32(v) => ScalarResult::ok_float(v as f64),
        AnyValue::Float64(v) => ScalarResult::ok_float(v),
        AnyValue::String(s) => ScalarResult::ok_string(s),
        AnyValue::StringOwned(s) => ScalarResult::ok_string(s.as_str()),
        AnyValue::Date(days) => ScalarResult::ok_int(SCALAR_DATE_DAYS, days as i64),
        AnyValue::Datetime(n, tu, _tz) => {
            ScalarResult::ok_int(SCALAR_DATETIME_NS, datetime_to_ns(n, tu))
        }
        AnyValue::DatetimeOwned(n, tu, _tz) => {
            ScalarResult::ok_int(SCALAR_DATETIME_NS, datetime_to_ns(n, tu))
        }
        AnyValue::Duration(n, tu) => {
            ScalarResult::ok_int(SCALAR_DURATION_NS, datetime_to_ns(n, tu))
        }
        other => ScalarResult::error(
            ERROR_POLARS_OPERATION,
            &format!("unsupported scalar type: {:?}", other.dtype()),
        ),
    }
}

fn datetime_to_ns(n: i64, tu: TimeUnit) -> i64 {
    match tu {
        TimeUnit::Nanoseconds => n,
        TimeUnit::Microseconds => n.saturating_mul(1_000),
        TimeUnit::Milliseconds => n.saturating_mul(1_000_000),
    }
}

// ---------- FFI surface ----------

/// Create a new evaluation context. Returns an opaque handle; caller must call
/// `firn_eval_ctx_free` to release.
#[no_mangle]
pub extern "C" fn firn_eval_ctx_new() -> usize {
    let ctx = Box::new(EvalContext::new());
    Box::into_raw(ctx) as usize
}

/// Release an evaluation context. No-op if `handle` is 0.
#[no_mangle]
pub extern "C" fn firn_eval_ctx_free(handle: usize) {
    if handle == 0 {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle as *mut EvalContext));
    }
}

/// Set the `now_ns` (nanoseconds since Unix epoch) value visible to `now()`.
#[no_mangle]
pub extern "C" fn firn_eval_ctx_set_now(handle: usize, ns: i64) -> c_int {
    if handle == 0 {
        return ERROR_POLARS_OPERATION;
    }
    let ctx = unsafe { &*(handle as *const EvalContext) };
    ctx.state.now_ns.store(ns, Ordering::Release);
    0
}

/// Evaluate the SQL expression text and return a single scalar.
#[no_mangle]
pub extern "C" fn firn_eval_ctx_eval_sql(handle: usize, sql: RawStr) -> ScalarResult {
    if handle == 0 {
        return ScalarResult::error(ERROR_POLARS_OPERATION, "null eval context handle");
    }
    let sql_str = match unsafe { sql.as_str() } {
        Ok(s) => s,
        Err(_) => return ScalarResult::error(ERROR_INVALID_UTF8, "invalid UTF-8 in SQL text"),
    };
    let ctx = unsafe { &mut *(handle as *mut EvalContext) };
    match ctx.eval_sql(sql_str) {
        Ok(r) => r,
        Err(e) => ScalarResult::error(ERROR_POLARS_OPERATION, &e.to_string()),
    }
}

/// Free a C string previously returned via `ScalarResult.str_data` or
/// `ScalarResult.error_message`.
#[no_mangle]
pub extern "C" fn firn_free_cstring(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(ctx: &mut EvalContext, sql: &str) -> ScalarResult {
        match ctx.eval_sql(sql) {
            Ok(r) => r,
            Err(e) => panic!("eval('{}') failed: {}", sql, e),
        }
    }

    #[test]
    fn eval_const_int() {
        let mut ctx = EvalContext::new();
        let r = eval(&mut ctx, "1 + 2");
        assert_eq!(r.kind, SCALAR_INT64);
        assert_eq!(r.int_val, 3);
    }

    #[test]
    fn eval_const_float() {
        let mut ctx = EvalContext::new();
        let r = eval(&mut ctx, "4.5");
        assert_eq!(r.kind, SCALAR_FLOAT64);
        assert_eq!(r.float_val, 4.5);
    }

    #[test]
    fn eval_pi() {
        let mut ctx = EvalContext::new();
        let r = eval(&mut ctx, "pi()");
        assert_eq!(r.kind, SCALAR_FLOAT64);
        assert!((r.float_val - std::f64::consts::PI).abs() < 1e-12);
    }

    #[test]
    fn eval_now_respects_context() {
        let mut ctx = EvalContext::new();
        let t0: i64 = 1_700_000_000_123_456_789;
        ctx.state.now_ns.store(t0, Ordering::Release);
        let r = eval(&mut ctx, "now()");
        assert_eq!(r.kind, SCALAR_DATETIME_NS);
        assert_eq!(r.int_val, t0);
    }

    #[test]
    fn eval_now_plus_interval() {
        let mut ctx = EvalContext::new();
        let t0: i64 = 1_700_000_000_000_000_000;
        ctx.state.now_ns.store(t0, Ordering::Release);
        let r = eval(&mut ctx, "now() + interval '1 hour'");
        assert_eq!(r.kind, SCALAR_DATETIME_NS);
        assert_eq!(r.int_val, t0 + 3_600_000_000_000);
    }

    #[test]
    fn set_now_between_evals() {
        let mut ctx = EvalContext::new();
        ctx.state.now_ns.store(1_000_000_000, Ordering::Release);
        assert_eq!(eval(&mut ctx, "now()").int_val, 1_000_000_000);
        ctx.state.now_ns.store(2_000_000_000, Ordering::Release);
        assert_eq!(eval(&mut ctx, "now()").int_val, 2_000_000_000);
    }

    #[test]
    fn rewrite_preserves_quoted_strings() {
        let out = replace_zero_arg_call("SELECT 'now()' || now()", "now", "X");
        assert_eq!(out, "SELECT 'now()' || X");
    }

    #[test]
    fn rewrite_case_insensitive() {
        let out = replace_zero_arg_call("NOW() + Now() + now()", "now", "X");
        assert_eq!(out, "X + X + X");
    }

    #[test]
    fn rewrite_ignores_partial_match() {
        let out = replace_zero_arg_call("snowman() + know()", "now", "X");
        assert_eq!(out, "snowman() + know()");
    }

    #[test]
    fn eval_string_literal() {
        let mut ctx = EvalContext::new();
        let r = eval(&mut ctx, "'hello'");
        assert_eq!(r.kind, SCALAR_STRING);
        let bytes = unsafe {
            std::slice::from_raw_parts(r.str_data as *const u8, r.str_len)
        };
        assert_eq!(std::str::from_utf8(bytes).unwrap(), "hello");
        firn_free_cstring(r.str_data);
    }
}
