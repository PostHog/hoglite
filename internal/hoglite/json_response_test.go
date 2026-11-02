package hoglite

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestJSONResponseEncodingFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value float64
	}{
		{"positive_infinity", math.Inf(1)},
		{"negative_infinity", math.Inf(-1)},
		{"nan", math.NaN()},
	} {
		for _, wrapped := range []bool{false, true} {
			name := tc.name + "/direct"
			if wrapped {
				name = tc.name + "/wrapped"
			}
			t.Run(name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				body := map[string]any{"value": tc.value}
				if wrapped {
					s := &Server{}
					s.wrap(func(http.ResponseWriter, *http.Request) (int, any) {
						return http.StatusOK, body
					})(rec, httptest.NewRequest(http.MethodGet, "/", nil))
				} else {
					writeJSON(rec, http.StatusOK, body)
				}
				if rec.Code != http.StatusInternalServerError {
					t.Fatalf("status = %d, want 500. Body = %q", rec.Code, rec.Body.String())
				}
				if got := rec.Header().Get("Content-Type"); got != "application/json" {
					t.Fatalf("content type = %q", got)
				}
				var out apiError
				if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
					t.Fatalf("invalid JSON: %v. Body = %q", err, rec.Body.String())
				}
				if !strings.Contains(out.Error, "encode response") {
					t.Fatalf("error = %q", out.Error)
				}
			})
		}
	}
}

type failedJSONWriter struct {
	header http.Header
	codes  []int
	writes int
}

func (w *failedJSONWriter) Header() http.Header { return w.header }
func (w *failedJSONWriter) WriteHeader(code int) {
	w.codes = append(w.codes, code)
}
func (w *failedJSONWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, io.ErrClosedPipe
}

func TestJSONResponseWriteFailure(t *testing.T) {
	w := &failedJSONWriter{header: http.Header{}}
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
	if len(w.codes) != 1 || w.codes[0] != http.StatusAccepted || w.writes != 1 {
		t.Fatalf("codes = %v, writes = %d. Do not append an error after a failed write", w.codes, w.writes)
	}
}

func TestJSONResponsePreservesStatusAndBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		body any
		want string
	}{
		{"success", 202, map[string]bool{"ok": true}, "{\"ok\":true}\n"},
		{"error", 422, apiError{Error: "refused", Code: CodeRefused}, "{\"error\":\"refused\",\"code\":\"refused\"}\n"},
		{"empty", 204, nil, ""},
		{"written", 0, nil, "proxied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			s := &Server{}
			s.wrap(func(w http.ResponseWriter, _ *http.Request) (int, any) {
				if tc.name == "written" {
					w.WriteHeader(201)
					_, _ = io.WriteString(w, "proxied")
				}
				return tc.code, tc.body
			})(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			wantCode := tc.code
			if wantCode == 0 {
				wantCode = 201
			}
			if rec.Code != wantCode || rec.Body.String() != tc.want {
				t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestNonFiniteQueryResults(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	for _, m := range c.mgrs {
		t.Cleanup(m.ShutdownAll)
	}
	c.seedTenant(t, "a", "numbers")

	for _, replica := range []string{"a", "b"} {
		for _, sql := range []string{"SELECT 1e999", "SELECT -1e999"} {
			t.Run(replica+"/"+sql, func(t *testing.T) {
				code, out, _ := c.do(t, replica, "POST", "/v1/t/numbers/query", Statement{SQL: sql})
				assertNonFiniteRefusal(t, code, out, 409)
				for _, interior := range []bool{false, true} {
					code, out, _ = c.do(t, replica, "POST", "/v1/t/numbers/tx", TxRequest{
						Client: "json", TxID: "read", Interior: interior,
						Stmts: []Statement{{SQL: sql}},
					})
					assertNonFiniteRefusal(t, code, out, 422)
				}
				code, answers := c.batch(t, replica, []map[string]any{
					{"tenant": "numbers", "sql": "SELECT 42"},
					{"tenant": "numbers", "sql": sql},
					{"tenant": "numbers", "sql": "SELECT 43"},
				})
				if code != 200 || len(answers) != 3 {
					t.Fatalf("batch: %d, %v", code, answers)
				}
				for i, want := range []float64{42, 0, 43} {
					var body map[string]any
					if err := json.Unmarshal(answers[i].Body, &body); err != nil {
						t.Fatalf("item %d: invalid JSON: %v", i, err)
					}
					if i == 1 {
						assertNonFiniteRefusal(t, answers[i].Status, body, 409)
					} else if answers[i].Status != 200 || body["rows"].([]any)[0].([]any)[0] != want {
						t.Fatalf("item %d: %d, %v", i, answers[i].Status, body)
					}
				}
			})
		}
	}

	// Finite numbers keep their types. Explicit text casts remain available.
	code, out, _ := c.do(t, "a", "POST", "/v1/t/numbers/query", Statement{
		SQL: "SELECT NULL, 1, 1.25, 1e308, CAST(1e999 AS TEXT), CAST(-1e999 AS TEXT), sqrt(-1)",
	})
	if code != 200 {
		t.Fatalf("representable query: %d, %v", code, out)
	}
	row := out["rows"].([]any)[0].([]any)
	for i, want := range []any{nil, float64(1), 1.25, 1e308, "Inf", "-Inf", nil} {
		if row[i] != want {
			t.Fatalf("column %d = %v, want %v", i, row[i], want)
		}
	}
}

func assertNonFiniteRefusal(t *testing.T, code int, out map[string]any, wantCode int) {
	t.Helper()
	msg, _ := out["error"].(string)
	if code != wantCode || out["code"] != CodeRefused || !strings.Contains(msg, "non-finite") {
		t.Fatalf("status = %d, body = %v. Want %d refused with a non-finite error", code, out, wantCode)
	}
}

func TestNonFiniteWriteResultsRollBack(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	for _, m := range c.mgrs {
		t.Cleanup(m.ShutdownAll)
	}
	if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/numbers", map[string]any{}); code != 200 {
		t.Fatalf("create: %d", code)
	}
	code, out, _ := c.do(t, "a", "POST", "/v1/t/numbers/tx", TxRequest{
		Client: "json", TxID: "setup", Stmts: []Statement{
			{SQL: "CREATE TABLE counter(v INTEGER)"},
			{SQL: "INSERT INTO counter VALUES(0)"},
			{SQL: "CREATE TABLE samples(v REAL)"},
			{SQL: "INSERT INTO samples VALUES(1e999), (-1e999)"},
		},
	})
	if code != 200 {
		t.Fatalf("setup: %d, %v", code, out)
	}
	var committed float64
	for _, replica := range []string{"a", "b"} {
		for i, sql := range []string{
			"SELECT 1e999",
			"SELECT -1e999",
			"UPDATE counter SET v = v + 1 RETURNING 1e999",
			"UPDATE counter SET v = v + 1 RETURNING -1e999",
		} {
			t.Run(replica+"/"+sql, func(t *testing.T) {
				req := TxRequest{Client: replica, TxID: strconv.Itoa(i), Stmts: []Statement{
					{SQL: "UPDATE counter SET v = v + 1"},
					{SQL: sql},
				}}
				badCode, badOut, _ := c.do(t, replica, "POST", "/v1/t/numbers/tx", req)
				code, out, _ := c.do(t, replica, "POST", "/v1/t/numbers/query", Statement{SQL: "SELECT v FROM counter"})
				if code != 200 || out["rows"].([]any)[0].([]any)[0] != committed {
					t.Fatalf("write does not roll back: %d, %v. Want %v", code, out, committed)
				}
				assertNonFiniteRefusal(t, badCode, badOut, 422)

				// The refused write must leave no receipt for this key.
				req.Stmts = []Statement{{SQL: "UPDATE counter SET v = v + 1 RETURNING v"}}
				committed++
				for _, wantDup := range []bool{false, true} {
					code, out, _ = c.do(t, replica, "POST", "/v1/t/numbers/tx", req)
					if code != 200 || out["dup"] != wantDup {
						t.Fatalf("retry: %d, %v. Want dup=%v", code, out, wantDup)
					}
					result := out["results"].([]any)[0].(map[string]any)
					if result["rows"].([]any)[0].([]any)[0] != committed {
						t.Fatalf("retry result = %v, want %v", result, committed)
					}
				}
			})
		}
	}
	// Storage still accepts infinity when the result contains no infinity.
	code, out, _ = c.do(t, "b", "POST", "/v1/t/numbers/query", Statement{
		SQL: "SELECT CAST(v AS TEXT) FROM samples ORDER BY v",
	})
	if code != 200 {
		t.Fatalf("stored infinity: %d, %v", code, out)
	}
	rows := out["rows"].([]any)
	if len(rows) != 2 || rows[0].([]any)[0] != "-Inf" || rows[1].([]any)[0] != "Inf" {
		t.Fatalf("stored infinity: %v", rows)
	}
}
