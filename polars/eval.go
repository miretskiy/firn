package polars

/*
#include "firn.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"time"
	"unsafe"
)

// ValueKind identifies the typed content of a Value returned by Eval.
type ValueKind uint8

const (
	KindNull       ValueKind = C.SCALAR_NULL
	KindInt64      ValueKind = C.SCALAR_INT64
	KindFloat64    ValueKind = C.SCALAR_FLOAT64
	KindBool       ValueKind = C.SCALAR_BOOL
	KindString     ValueKind = C.SCALAR_STRING
	KindDatetimeNs ValueKind = C.SCALAR_DATETIME_NS
	KindDurationNs ValueKind = C.SCALAR_DURATION_NS
	KindDateDays   ValueKind = C.SCALAR_DATE_DAYS
)

func (k ValueKind) String() string {
	switch k {
	case KindNull:
		return "null"
	case KindInt64:
		return "int64"
	case KindFloat64:
		return "float64"
	case KindBool:
		return "bool"
	case KindString:
		return "string"
	case KindDatetimeNs:
		return "datetime"
	case KindDurationNs:
		return "duration"
	case KindDateDays:
		return "date"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(k))
	}
}

// Value is the typed result of evaluating a constant or stable SQL expression.
// Use the typed accessors (Int64, Float64, Bool, Str, Time, Duration, Date) —
// each returns (value, ok) where ok is false on kind mismatch.
type Value struct {
	Kind ValueKind
	i    int64
	f    float64
	b    bool
	s    string
}

func (v Value) IsNull() bool { return v.Kind == KindNull }

func (v Value) Int64() (int64, bool) {
	if v.Kind == KindInt64 {
		return v.i, true
	}
	return 0, false
}

func (v Value) Float64() (float64, bool) {
	if v.Kind == KindFloat64 {
		return v.f, true
	}
	return 0, false
}

func (v Value) Bool() (bool, bool) {
	if v.Kind == KindBool {
		return v.b, true
	}
	return false, false
}

func (v Value) Str() (string, bool) {
	if v.Kind == KindString {
		return v.s, true
	}
	return "", false
}

// Time returns the datetime as a time.Time in UTC. Datetimes are stored as
// nanoseconds since Unix epoch; dates are midnight UTC.
func (v Value) Time() (time.Time, bool) {
	switch v.Kind {
	case KindDatetimeNs:
		return time.Unix(0, v.i).UTC(), true
	case KindDateDays:
		return time.Unix(v.i*86400, 0).UTC(), true
	}
	return time.Time{}, false
}

func (v Value) Duration() (time.Duration, bool) {
	if v.Kind == KindDurationNs {
		return time.Duration(v.i), true
	}
	return 0, false
}

// String implements fmt.Stringer using the typed representation.
func (v Value) String() string {
	switch v.Kind {
	case KindNull:
		return "NULL"
	case KindInt64:
		return fmt.Sprintf("%d", v.i)
	case KindFloat64:
		return fmt.Sprintf("%g", v.f)
	case KindBool:
		return fmt.Sprintf("%t", v.b)
	case KindString:
		return v.s
	case KindDatetimeNs:
		return time.Unix(0, v.i).UTC().Format(time.RFC3339Nano)
	case KindDurationNs:
		return time.Duration(v.i).String()
	case KindDateDays:
		return time.Unix(v.i*86400, 0).UTC().Format("2006-01-02")
	}
	return fmt.Sprintf("Value(kind=%s)", v.Kind)
}

// EvalContext evaluates constant and stable SQL expressions against a
// caller-controlled set of stable-function values (currently just `now()`).
//
// Lifecycle: create with NewEvalContext, feed expressions via Eval, adjust
// state with SetNow, and call Close when done. A finalizer is installed as a
// safety net but callers should still Close explicitly to free the Rust-side
// resources promptly.
type EvalContext struct {
	handle C.uintptr_t
}

// NewEvalContext creates a fresh evaluation context. `now()` starts at the
// Unix epoch; call SetNow before evaluating expressions that use it.
func NewEvalContext() *EvalContext {
	ctx := &EvalContext{handle: C.firn_eval_ctx_new()}
	runtime.SetFinalizer(ctx, func(c *EvalContext) { _ = c.Close() })
	return ctx
}

// Close releases the Rust-side context. Safe to call multiple times.
func (c *EvalContext) Close() error {
	if c == nil || c.handle == 0 {
		return nil
	}
	C.firn_eval_ctx_free(c.handle)
	c.handle = 0
	runtime.SetFinalizer(c, nil)
	return nil
}

// SetNow updates the value returned by `now()` in subsequent evals.
func (c *EvalContext) SetNow(t time.Time) error {
	if c == nil || c.handle == 0 {
		return errors.New("eval context is closed")
	}
	ns := t.UnixNano()
	if code := C.firn_eval_ctx_set_now(c.handle, C.int64_t(ns)); code != 0 {
		return fmt.Errorf("firn_eval_ctx_set_now returned %d", int(code))
	}
	return nil
}

// Eval evaluates a SQL expression (not a full statement) and returns a typed
// scalar. Example inputs: "1 + 2", "pi()", "now() + interval '1 hour'".
func (c *EvalContext) Eval(sqlExpr string) (Value, error) {
	if c == nil || c.handle == 0 {
		return Value{}, errors.New("eval context is closed")
	}
	res := C.firn_eval_ctx_eval_sql(c.handle, makeRawStr(sqlExpr))
	defer freeScalarResult(&res)

	if res.error_code != 0 {
		return Value{}, &Error{
			Code:    int(res.error_code),
			Message: C.GoString(res.error_message),
		}
	}
	return scalarResultToValue(&res), nil
}

// scalarResultToValue copies the C struct contents into a Go Value. Strings
// are copied via C.GoStringN so the Go value is independent of the C buffer
// (which freeScalarResult releases on return).
func scalarResultToValue(r *C.ScalarResult) Value {
	v := Value{Kind: ValueKind(r.kind)}
	switch v.Kind {
	case KindInt64, KindDatetimeNs, KindDurationNs, KindDateDays:
		v.i = int64(r.int_val)
	case KindFloat64:
		v.f = float64(r.float_val)
	case KindBool:
		v.b = bool(r.bool_val)
	case KindString:
		if r.str_data != nil && r.str_len > 0 {
			v.s = C.GoStringN(r.str_data, C.int(r.str_len))
		}
	}
	return v
}

func freeScalarResult(r *C.ScalarResult) {
	if r.str_data != nil {
		C.firn_free_cstring(r.str_data)
		r.str_data = nil
	}
	if r.error_message != nil {
		C.firn_free_cstring(r.error_message)
		r.error_message = nil
	}
}

// silence unused-import diagnostics if something gets refactored out
var _ = unsafe.Pointer(nil)
