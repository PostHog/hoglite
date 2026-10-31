package hoglite

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FakeStore is an in-memory Backend with S3 semantics: strong consistency,
// atomic PUT, ETag-based conditions. For unit tests and local development.
type FakeStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	etags   map[string]string
	leases  map[string]int64
	counter atomic.Int64

	Latency time.Duration // injected per-op delay

	// Fault hooks (tests).
	FailPutSegment func(key string) bool
	// FailCAS fails the next n manifest CAS calls with a transient error.
	FailCAS func() bool
	// SucceedButReportError simulates an S3 PUT that lands server-side but
	// whose client sees a failure (timeout after commit). Called before
	// storing; returning true stores the object AND returns an error.
	LatePutSegment func(key string) bool
	// FailPutBase fails a base-image PUT (checkpoint upload) without
	// storing it.
	FailPutBase func(key string) bool
	// CorruptObject flips a byte in the named object, once.
	CorruptObject func(key string) bool
}

func NewFakeStore() *FakeStore {
	return &FakeStore{objects: map[string][]byte{}, etags: map[string]string{}, leases: map[string]int64{}}
}

func (f *FakeStore) delay() {
	if f.Latency > 0 {
		time.Sleep(f.Latency)
	}
}

func (f *FakeStore) newEtag() string {
	return fmt.Sprintf("\"%d\"", f.counter.Add(1))
}

func (f *FakeStore) manifestKey(tid string) string { return manifestKey("fake", tid) }

func (f *FakeStore) GetManifest(ctx context.Context, tid string) (*Manifest, string, error) {
	f.delay()
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[f.manifestKey(tid)]
	if !ok {
		return nil, "", nil
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, "", err
	}
	return &m, f.etags[f.manifestKey(tid)], nil
}

func (f *FakeStore) PutManifestCAS(ctx context.Context, tid string, m *Manifest, etag string) (string, error) {
	f.delay()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailCAS != nil && f.FailCAS() {
		return "", fmt.Errorf("injected transient cas failure")
	}
	k := f.manifestKey(tid)
	cur, exists := f.etags[k]
	if etag == "" {
		if exists {
			return "", ErrCAS
		}
	} else if !exists || cur != etag {
		return "", ErrCAS
	}
	b, _ := json.Marshal(m)
	f.objects[k] = b
	f.etags[k] = f.newEtag()
	return f.etags[k], nil
}

func (f *FakeStore) PutSegment(ctx context.Context, key string, data []byte) error {
	f.delay()
	if f.FailPutSegment != nil && f.FailPutSegment(key) {
		return fmt.Errorf("injected segment failure: %s", key)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.objects[key] = cp
	if f.LatePutSegment != nil && f.LatePutSegment(key) {
		// landed, but the client hears an error
		return fmt.Errorf("injected post-commit timeout: %s", key)
	}
	return nil
}

func (f *FakeStore) PutBase(ctx context.Context, key string, r io.Reader, size int64) error {
	f.delay()
	if f.FailPutBase != nil && f.FailPutBase(key) {
		return fmt.Errorf("injected base failure: %s", key)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = b
	return nil
}

func (f *FakeStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	f.delay()
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

func (f *FakeStore) GetObjectTo(ctx context.Context, key string, w io.Writer) error {
	f.delay()
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	if !ok {
		return ErrNotFound
	}
	_, err := w.Write(b)
	return err
}

func (f *FakeStore) ListSegments(ctx context.Context, tid, iid string, gen int64) ([]SegMeta, error) {
	f.delay()
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := walPrefix("fake", tid, iid, gen)
	var out []SegMeta
	for k, v := range f.objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(k, prefix), "/")
		if len(parts) != 2 {
			continue
		}
		epoch, err1 := strconv.ParseInt(parts[0], 10, 64)
		off, err2 := strconv.ParseInt(strings.TrimSuffix(parts[1], ".seg"), 16, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, SegMeta{Key: k, Epoch: epoch, Offset: off, Size: int64(len(v))})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Offset != out[j].Offset {
			return out[i].Offset < out[j].Offset
		}
		return out[i].Epoch > out[j].Epoch
	})
	return out, nil
}

func (f *FakeStore) DeleteObjects(ctx context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.objects, k)
		delete(f.etags, k)
	}
	return nil
}

func (f *FakeStore) DeleteTenant(ctx context.Context, tid string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := tenantPrefix("fake", tid) + "/"
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			delete(f.objects, k)
			delete(f.etags, k)
		}
	}
	return nil
}

func (f *FakeStore) ListTenantIDs(ctx context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]bool{}
	for k := range f.objects {
		if strings.HasPrefix(k, "fake/") {
			rest := strings.TrimPrefix(k, "fake/")
			// <hh>/t/<tid>/...
			if i := strings.Index(rest, "/t/"); i > 0 {
				id := rest[i+3:]
				if j := strings.Index(id, "/"); j > 0 {
					seen[id[:j]] = true
				}
			}
		}
	}
	var ids []string
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (f *FakeStore) Usage(ctx context.Context, prefix string) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n, b int64
	for k, v := range f.objects {
		if strings.HasPrefix(k, "fake/"+strings.Trim(prefix, "/")) {
			n++
			b += int64(len(v))
		}
	}
	return n, b, nil
}

func (f *FakeStore) WriteFormatMarker(ctx context.Context, version string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects["fake/FORMAT"]; !ok {
		f.objects["fake/FORMAT"] = []byte(version)
	}
	return nil
}

func (f *FakeStore) ReadFormatMarker(ctx context.Context) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects["fake/FORMAT"]
	return string(b), ok, nil
}

func (f *FakeStore) PutPodLease(ctx context.Context, podid string, untilMs int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases[podid] = untilMs
	return nil
}

func (f *FakeStore) GetPodLease(ctx context.Context, podid string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.leases[podid]
	return u, ok, nil
}

// Corrupt flips one byte in the stored object (test hook for the parser
// as firewall).
func (f *FakeStore) Corrupt(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b, ok := f.objects[key]; ok && len(b) > 100 {
		cp := make([]byte, len(b))
		copy(cp, b)
		cp[100] ^= 0xFF
		f.objects[key] = cp
	}
}

// ListPrefix implements Backend.
func (f *FakeStore) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

// ObjectKeys is a test helper.
func (f *FakeStore) ObjectKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.objects {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
