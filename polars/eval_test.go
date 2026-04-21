package polars

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEvalContext_Constants(t *testing.T) {
	ctx := NewEvalContext()
	defer ctx.Close()

	t.Run("int arithmetic", func(t *testing.T) {
		v, err := ctx.Eval("1 + 2")
		require.NoError(t, err)
		got, ok := v.Int64()
		require.True(t, ok, "kind=%s", v.Kind)
		require.Equal(t, int64(3), got)
	})

	t.Run("float literal", func(t *testing.T) {
		v, err := ctx.Eval("4.5")
		require.NoError(t, err)
		got, ok := v.Float64()
		require.True(t, ok, "kind=%s", v.Kind)
		require.Equal(t, 4.5, got)
	})

	t.Run("pi()", func(t *testing.T) {
		v, err := ctx.Eval("pi()")
		require.NoError(t, err)
		got, ok := v.Float64()
		require.True(t, ok)
		require.InDelta(t, math.Pi, got, 1e-12)
	})

	t.Run("bool", func(t *testing.T) {
		v, err := ctx.Eval("true AND (1 < 2)")
		require.NoError(t, err)
		got, ok := v.Bool()
		require.True(t, ok, "kind=%s", v.Kind)
		require.True(t, got)
	})

	t.Run("string", func(t *testing.T) {
		v, err := ctx.Eval("'hello'")
		require.NoError(t, err)
		got, ok := v.Str()
		require.True(t, ok, "kind=%s", v.Kind)
		require.Equal(t, "hello", got)
	})
}

func TestEvalContext_Now(t *testing.T) {
	ctx := NewEvalContext()
	defer ctx.Close()

	anchor := time.Date(2024, 3, 15, 12, 30, 45, 123_456_789, time.UTC)
	require.NoError(t, ctx.SetNow(anchor))

	t.Run("now() reads the set value", func(t *testing.T) {
		v, err := ctx.Eval("now()")
		require.NoError(t, err)
		require.Equal(t, KindDatetimeNs, v.Kind)
		got, ok := v.Time()
		require.True(t, ok)
		require.True(t, got.Equal(anchor), "got %s, want %s", got, anchor)
	})

	t.Run("now() + interval '1 hour'", func(t *testing.T) {
		v, err := ctx.Eval("now() + interval '1 hour'")
		require.NoError(t, err)
		got, ok := v.Time()
		require.True(t, ok)
		require.True(t, got.Equal(anchor.Add(time.Hour)), "got %s, want %s", got, anchor.Add(time.Hour))
	})

	t.Run("SetNow between evals", func(t *testing.T) {
		later := anchor.Add(5 * time.Minute)
		require.NoError(t, ctx.SetNow(later))
		v, err := ctx.Eval("now()")
		require.NoError(t, err)
		got, _ := v.Time()
		require.True(t, got.Equal(later), "got %s, want %s", got, later)
	})
}

func TestEvalContext_QuotedNowIsNotRewritten(t *testing.T) {
	ctx := NewEvalContext()
	defer ctx.Close()

	// The literal text 'now()' inside a SQL string should survive the rewrite.
	v, err := ctx.Eval("'now()'")
	require.NoError(t, err)
	s, ok := v.Str()
	require.True(t, ok)
	require.Equal(t, "now()", s)
}

func TestEvalContext_Errors(t *testing.T) {
	ctx := NewEvalContext()
	defer ctx.Close()

	// An unknown SQL function surfaces as a polars error, not a panic.
	_, err := ctx.Eval("no_such_function()")
	require.Error(t, err)

	// After Close, eval returns a Go error rather than dereferencing a null handle.
	require.NoError(t, ctx.Close())
	_, err = ctx.Eval("1")
	require.Error(t, err)
}
