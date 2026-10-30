package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// baseGetStore counts the downloads of one object key.
type baseGetStore struct {
	*FakeStore
	key  atomic.Value // string
	gets atomic.Int64
}

func (s *baseGetStore) note(key string) {
	if k, _ := s.key.Load().(string); k != "" && k == key {
		s.gets.Add(1)
	}
}

func (s *baseGetStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	s.note(key)
	return s.FakeStore.GetObject(ctx, key)
}

func (s *baseGetStore) GetObjectTo(ctx context.Context, key string, w io.Writer) error {
	s.note(key)
	return s.FakeStore.GetObjectTo(ctx, key, w)
}

// SQLITE_NOMEM while a takeover opens the tenant is memory pressure, not a
// broken local cache. The claim fails as out_of_memory (507) and counts
// once. It does not download the base and chain a second time, and the
// manifest does not change. When memory is available again, the takeover
// recovers every row.
func TestTakeoverNoMemIsOutOfMemory(t *testing.T) {
	fs := &baseGetStore{FakeStore: NewFakeStore()}
	o := deltaTestOpts()
	m := newReadTestManager(t, fs.FakeStore, o, &Metrics{})
	ten := newDeltaTenant(t, m, 20)
	mustTx(t, m, "t1", "c", "more", insertStmt(21, "x"))
	if !checkpointNow(ten) {
		t.Fatal("delta checkpoint failed")
	}
	before, _, err := fs.GetManifest(context.Background(), "t1")
	if err != nil || len(before.Deltas) == 0 {
		t.Fatalf("no delta chain: %+v %v", before, err)
	}
	fs.key.Store(before.BaseKey)
	m.KillLease()

	met := &Metrics{}
	m2 := NewManager(fs, "fake", "a", "standard", t.TempDir(), o, 1000, met, ControllerOpts{})
	t.Cleanup(m2.ShutdownAll)
	srv := httptest.NewServer(NewServer(m2, nil).Handler())
	t.Cleanup(srv.Close)

	b, _ := json.Marshal(Statement{SQL: "SELECT COUNT(*) FROM kv"})
	restore := withHeapLimitAt(t, 1)
	resp, err := http.Post(srv.URL+"/v1/t/t1/query", "application/json", bytes.NewReader(b))
	restore()
	if err != nil {
		t.Fatal(err)
	}
	var ae apiError
	json.NewDecoder(resp.Body).Decode(&ae)
	resp.Body.Close()
	if resp.StatusCode != 507 || ae.Code != "out_of_memory" {
		t.Fatalf("takeover NOMEM: got %d %+v", resp.StatusCode, ae)
	}
	if n := met.OutOfMemory.Load(); n != 1 {
		t.Fatalf("out-of-memory count %d, want 1", n)
	}
	if n := fs.gets.Load(); n != 1 {
		t.Fatalf("base downloaded %d times, want 1", n)
	}
	after, _, _ := fs.GetManifest(context.Background(), "t1")
	if after.Gen != before.Gen || after.Epoch != before.Epoch || after.BaseKey != before.BaseKey {
		t.Fatalf("manifest changed: before %+v, after %+v", before, after)
	}
	if m2.LocalTenant("t1") != nil {
		t.Fatal("failed takeover registered the tenant")
	}

	// the direct API gives the same classified error
	restore = withHeapLimitAt(t, 1)
	_, err = m2.Acquire(context.Background(), "t1", false)
	restore()
	if !errors.Is(err, ErrOutOfMemory) || !isNoMem(err) {
		t.Fatalf("Acquire: %v", err)
	}

	if n := countRows(t, m2, "t1"); n != 21 {
		t.Fatalf("rows after recovery: %d, want 21", n)
	}
}
