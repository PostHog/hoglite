package hoglite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAcquireWaitRespectsContext(t *testing.T) {
	for _, tc := range []struct {
		name     string
		create   bool
		deadline bool
	}{
		{name: "canceled-read"},
		{name: "canceled-create", create: true},
		{name: "deadline-read", deadline: true},
		{name: "deadline-create", create: true, deadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := NewFakeStore()
			hs := newCASHoldStore(fs, "t1", true)
			hs.armed.Store(true)
			met := &Metrics{}
			m := NewManager(hs, "fake", "a", "standard", t.TempDir(), testOpts(), 1000, met, ControllerOpts{})
			release := sync.OnceFunc(func() { close(hs.release) })
			type result struct {
				tenant *Tenant
				err    error
			}
			var wg sync.WaitGroup
			acquire := func(ctx context.Context, create bool) <-chan result {
				done := make(chan result, 1)
				wg.Add(1)
				go func() {
					defer wg.Done()
					tenant, err := m.Acquire(ctx, "t1", create)
					done <- result{tenant, err}
				}()
				return done
			}
			t.Cleanup(func() {
				release()
				done := make(chan struct{})
				go func() { wg.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("acquire calls do not finish after the leader resumes")
				}
				m.ShutdownAll()
			})

			leader := acquire(context.Background(), true)
			select {
			case <-hs.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the leader does not reach the create CAS")
			}
			m.mu.Lock()
			opening := m.opening["t1"]
			m.mu.Unlock()
			if opening == nil {
				t.Fatal("the leader has no opening entry")
			}

			ctx, cancel := context.WithCancel(context.Background())
			var want error = context.Canceled
			if tc.deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			waiter := acquire(ctx, tc.create)
			healthy := acquire(context.Background(), !tc.create)
			if !tc.deadline {
				cancel()
			}
			select {
			case got := <-waiter:
				if got.tenant != nil || !errors.Is(got.err, want) {
					t.Errorf("canceled waiter: tenant %p, error %v (want nil, %v)", got.tenant, got.err, want)
				}
			case <-time.After(time.Second):
				t.Error("the waiter stays blocked after its context ends")
			}
			m.mu.Lock()
			current := m.opening["t1"]
			m.mu.Unlock()
			if current != opening {
				t.Error("the canceled waiter changes the leader's opening entry")
			}
			select {
			case got := <-healthy:
				t.Fatalf("the healthy waiter finishes before the leader resumes: %v", got.err)
			default:
			}

			release()
			var opened *Tenant
			select {
			case got := <-leader:
				if got.err != nil || got.tenant == nil {
					t.Fatalf("the leader returns %p, %v", got.tenant, got.err)
				}
				opened = got.tenant
			case <-time.After(5 * time.Second):
				t.Fatal("the leader does not finish")
			}
			select {
			case got := <-healthy:
				if got.err != nil || got.tenant != opened {
					t.Errorf("healthy waiter: tenant %p, error %v (want %p, nil)", got.tenant, got.err, opened)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the healthy waiter does not finish")
			}
			if got := met.Acquires.Load(); got != 1 {
				t.Errorf("acquire count is %d, want 1", got)
			}
		})
	}
}
