package record_test

import (
	"testing"
	"time"

	"syncer/internal/record"
)

func TestRowGetString(t *testing.T) {
	r := record.Row{
		"name":  "alice",
		"bytes": []byte("hello"),
		"num":   42,
		"nil":   nil,
	}

	cases := []struct {
		field string
		want  string
	}{
		{"name", "alice"},
		{"bytes", "hello"},
		{"num", "42"},
		{"nil", ""},
		{"missing", ""},
	}

	for _, c := range cases {
		got := r.GetString(c.field)
		if got != c.want {
			t.Errorf("GetString(%q) = %q, want %q", c.field, got, c.want)
		}
	}
}

func TestRowGetInt64(t *testing.T) {
	r := record.Row{
		"i64": int64(100),
		"i32": int32(200),
		"f64": float64(300.9),
		"u64": uint64(400),
	}

	if r.GetInt64("i64") != 100 {
		t.Error("i64 failed")
	}
	if r.GetInt64("i32") != 200 {
		t.Error("i32 failed")
	}
	if r.GetInt64("f64") != 300 {
		t.Error("f64 failed")
	}
	if r.GetInt64("u64") != 400 {
		t.Error("u64 failed")
	}
	if r.GetInt64("missing") != 0 {
		t.Error("missing failed")
	}
}

func TestRowGetTime(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r := record.Row{
		"ts":  now,
		"str": "2024-01-15 10:30:00",
		"bad": "not-a-time",
	}

	if got := r.GetTime("ts"); !got.Equal(now) {
		t.Errorf("ts: got %v want %v", got, now)
	}
	if got := r.GetTime("str"); got.IsZero() {
		t.Error("str: expected non-zero time")
	}
	if got := r.GetTime("bad"); !got.IsZero() {
		t.Error("bad: expected zero time")
	}
}

func TestRowClone(t *testing.T) {
	original := record.Row{
		"id":   int64(1),
		"data": []byte("hello"),
	}

	clone := original.Clone()

	// mutate the original bytes — clone must be unaffected
	original["data"].([]byte)[0] = 'X'

	if clone["data"].([]byte)[0] != 'h' {
		t.Error("Clone did not deep-copy []byte")
	}
}

func TestPool(t *testing.T) {
	p := record.NewPool(8)

	r := p.Get()
	r["id"] = 1
	r["name"] = "test"

	p.Put(r)

	// after Put, the row should be cleared
	r2 := p.Get()
	if len(r2) != 0 {
		t.Errorf("expected empty row after Put, got %d fields", len(r2))
	}
}

func TestBatchToColumnSlices(t *testing.T) {
	batch := record.Batch{
		record.Row{"id": int64(1), "name": "alice"},
		record.Row{"id": int64(2), "name": "bob"},
	}

	cols := []string{"id", "name"}
	slices := batch.ToColumnSlices(cols, nil)

	if len(slices) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(slices))
	}
	if slices[0][0] != int64(1) || slices[0][1] != "alice" {
		t.Errorf("row 0 wrong: %v", slices[0])
	}
	if slices[1][0] != int64(2) || slices[1][1] != "bob" {
		t.Errorf("row 1 wrong: %v", slices[1])
	}
}