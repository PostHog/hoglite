package hoglite

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// A read pinned at a basis that is not durable yet is released exactly
// when ITS basis becomes durable: not before (it would expose an applied
// but not durable commit), and not only once a later, unrelated segment
// lands (it would stall behind writes it never saw). The rule holds for
// every completed outcome, in all three read shapes: a /query, a
// read-only /tx, and a read-only /tx whose guard fails. A guard
// diagnostic ("scalar 1 != 0") reveals the snapshot as much as a row does.
func TestReadReleasesAtOwnDurableBasis(t *testing.T) {
	for _, mode := range []string{"query", "tx", "guard"} {
		t.Run(mode, func(t *testing.T) { testReadReleasesAtOwnBasis(t, mode) })
	}
}

func testReadReleasesAtOwnBasis(t *testing.T, mode string) {
	fs := NewFakeStore()
	m := newTestManager(t, fs, "a")
	mustCreate(t, m, "t1")
	makeSchema(t, m, "t1")
	ten, err := m.Acquire(context.Background(), "t1", false)
	if err != nil {
		t.Fatal(err)
	}

	// Hold the next two segment PUTs, each behind its own gate.
	gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
	started := make(chan int, 2)
	var calls atomic.Int32
	fs.FailPutSegment = func(string) bool {
		if i := int(calls.Add(1)) - 1; i < len(gates) {
			started <- i
			<-gates[i]
		}
		return false
	}
	t.Cleanup(func() {
		for _, g := range gates {
			select {
			case <-g:
			default:
				close(g)
			}
		}
	})
	writes := make(chan error, 2)
	write := func(k int64) {
		_, _, err := ten.SubmitTx(TxRequest{Client: "c", TxID: fmt.Sprint(k), Stmts: []Statement{insertStmt(k, "v")}})
		writes <- err
	}
	awaitPut := func(want int) {
		t.Helper()
		select {
		case i := <-started:
			if i != want {
				t.Fatalf("segment PUT %d started, want %d", i, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("segment PUT %d never started", want)
		}
	}

	// w1 applies; its segment is held, so the basis below is not durable.
	go write(1)
	awaitPut(0)

	hook, pinned, gate := blockingHook()
	ten.readHook = hook
	type outcome struct {
		res QueryResult
		err error
	}
	read := make(chan outcome, 1)
	go func() {
		st := Statement{SQL: `SELECT count(*) FROM kv`}
		if mode == "query" {
			res, err := ten.SubmitQuery(st)
			read <- outcome{res, err}
			return
		}
		if mode == "guard" {
			want := int64(0)
			st.ExpectScalarInt = &want
		}
		results, _, err := ten.SubmitTx(TxRequest{Stmts: []Statement{st}})
		var res QueryResult
		if len(results) > 0 {
			res = results[0]
		}
		read <- outcome{res, err}
	}()
	waitPinned(t, pinned) // basis = after w1

	// w2 applies after the pin; its segment is held as well.
	go write(2)
	awaitPut(1)

	close(gate) // the worker runs on its snapshot only now
	select {
	case o := <-read:
		t.Fatalf("read released over a non-durable basis: %+v", o)
	case <-time.After(200 * time.Millisecond):
	}

	close(gates[0]) // w1, the read's basis, becomes durable; w2 does not
	select {
	case o := <-read:
		if mode == "guard" {
			var ef *ExpectFailedError
			if !errors.As(o.err, &ef) || ef.What != "scalar 1 != 0" {
				t.Fatalf("guard on the pinned snapshot: %v", o.err)
			}
			break
		}
		if o.err != nil {
			t.Fatal(o.err)
		}
		if n := o.res.Rows[0][0].(int64); n != 1 {
			t.Fatalf("read saw %d rows: its snapshot leaked the later w2", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read waited for a later, unrelated segment")
	}

	close(gates[1])
	for range 2 {
		if err := <-writes; err != nil {
			t.Fatal(err)
		}
	}
}
