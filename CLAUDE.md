# firn — Claude Code notes

Go bindings over Polars (Rust) designed for use inside a **discrete-event simulator**. Performance and determinism are first-class concerns.

## Layout

- `polars/` — Go layer. Public API (`DataFrame`, `ExprNode`, `Col/Lit/SqlExpr`, ...), CGO bridge (`dataframe_<goos>_<goarch>.go`), and the C header `firn.h`. All operations are lazy `Operation` records executed in a single FFI call (`Collect`/`Execute`).
- `rust/src/` — Rust library `firn` (staticlib) wrapping `polars` 0.44 and `polars-sql`.
  - `lib.rs` — FFI primitives (`RawStr`, `FfiResult`, `PolarsHandle`).
  - `opcodes.rs` — `OpCode` enum; **must stay in sync with `polars/opcodes.go`**.
  - `execution.rs` — `execute_operations` FFI entry point; dispatches opcodes against an expression stack + handle/context.
  - `expr.rs` — RPN stack machine for expressions (column, literal, arithmetic, comparisons, aggregates, `when/then/otherwise`, `over`, `cast`, SQL expression parsing via `polars_sql::sql_expr`).
  - `dataframe.rs` — DataFrame ops (select, filter, group_by, agg, sort, join, limit, collect, sql query).
  - `types.rs` — Argument structs mirrored in `firn.h`; bit-packed `DataType` codec.
  - `io.rs` — CSV / Parquet readers.
- `lib/` — Pre-built static libraries (`libfirn_<goos>_<goarch>.a`) linked by CGO.
- `internal/cgo`, `internal/ffi` — CGO helpers.
- `cmd/schema_inspector/` — CLI for inspecting schemas.
- `benchmarks/`, `testdata/`, `docs/` — as named.

## Architecture invariants

- **Batch, don't chatter.** Each Go builder method (`Filter`, `WithColumns`, `Col("x").Add(Lit(1))`, …) only appends to an `[]Operation`. CGO is crossed **once** per `Collect()`/`Execute()` call.
- **RPN expression stack.** Expressions are encoded as a flat sequence of ops; Rust maintains a `Vec<Expr>` stack. Binary ops pop two, push one. The whole `DataFrame` op stream and the expression ops share the same `Operation` vector and the same stack (expressions are pushed by `OpExpr*` ops, consumed by `OpFilterExpr` / `OpWithColumn` / `OpSelectExpr` / `OpAgg`).
- **Lazy args.** `Operation.args` is a `func() unsafe.Pointer`. The closure captures Go values and only allocates the C arg struct at FFI time, so Go strings stay alive without copying.
- **Move semantics on `*ExprNode`.** Once an `ExprNode` is consumed (`consumeOps`), it's set to `nil`. Don't reuse expressions after passing them to `WithColumns`, `Filter`, `Then`, `Otherwise`, etc.
- **Opcodes are wire format.** Every change in `rust/src/opcodes.rs::OpCode` needs a matching change in `polars/opcodes.go`.
- **Context tracking.** `PolarsHandle` carries a `ContextType` (DataFrame / LazyFrame / LazyGroupBy). Operations encode what they return; invalid transitions (e.g. select on LazyGroupBy) are rejected in Rust.
- **Polars is immutable.** Each op returns a new boxed handle; the Go layer releases the old `handle` after a successful `execute()`.

## Simulator discipline

- This is a **discrete-event simulator** and flight-sim for real workflows. Simulation runs must be **fully deterministic** and **fast** — hours/days/months of wall-clock time processed in seconds, single-threaded.
- Do not introduce wall-clock dependencies (`time.Now()`, OS randomness, non-deterministic map iteration) into simulation paths. If Polars provides something non-deterministic (e.g. `now()`, `random()`), it must be injected from simulator context, not pulled from the OS. See the "stable expression evaluation" design work for the pattern.
- Tests must be reproducible: same inputs → same outputs, byte-for-byte where feasible (the existing tests assert on full formatted `DataFrame` output).

## Build & test

```bash
# Full build
bazel build //rust:all //polars:all

# Go-only test loop (use CGO_LDFLAGS to silence macOS SDK warnings)
CGO_LDFLAGS="-w" go test -v ./polars
CGO_LDFLAGS="-w" go test -v -run TestBasicOperations ./polars

# Benchmarks
cd benchmarks && CGO_LDFLAGS="-w" go test -bench=. -benchmem

# Bazel tests
bazel test //polars:all
```

Rust nightly is required (see `BAZEL.md`). Static libraries under `lib/` are checked in; rebuild via `scripts/build_rust.sh` or `bazel build //rust:firn_static`.

## Conventions

- Go style: fluent builders return `*ExprNode` / `*DataFrame`. Errors are carried inside an `Operation{err: ...}` and surfaced at `execute()` time with the frame index. Prefer `appendErrOp`/`errOpf` over panicking.
- No `fmt.Println` in library code. Use Go test goldens (full rendered DataFrame strings, see `dataframe_test.go`).
- When adding an op: (1) add `OpCode` in both `rust/src/opcodes.rs` and `polars/opcodes.go`; (2) declare the arg struct in `firn.h` + `rust/src/types.rs`; (3) implement the dispatch in `rust/src/expr.rs` or `dataframe.rs`; (4) wire the Go builder in `polars/expr.go` or `polars/dataframe.go`; (5) add a golden test.
