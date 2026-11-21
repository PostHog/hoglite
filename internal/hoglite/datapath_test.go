package hoglite

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	testDataPath  = "s3://hoglake-dev/catalogs/demo"
	otherDataPath = "s3://hoglake-dev/catalogs/other"
)

func createBody(dataPath string) map[string]any {
	body := map[string]any{}
	if dataPath != "" {
		body["data_path"] = dataPath
	}
	return body
}

func wantDataPath(t *testing.T, what string, hdr http.Header, want string) {
	t.Helper()
	if got := hdr.Get(DataPathHeader); got != want {
		t.Fatalf("%s: %s = %q, want %q", what, DataPathHeader, got, want)
	}
	if len(hdr.Values(DataPathHeader)) > 1 {
		t.Fatalf("%s: %s sent %d times", what, DataPathHeader, len(hdr.Values(DataPathHeader)))
	}
}

func wantMismatch(t *testing.T, what string, code int, out map[string]any) {
	t.Helper()
	if code != 409 || out["code"] != CodeDataPathMismatch {
		t.Fatalf("%s: %d %v, want 409 %s", what, code, out, CodeDataPathMismatch)
	}
}

// The create stores the data_path in the manifest, and every tenant-scoped
// route repeats it, at the owner and through a non-owner's proxy. A create
// of the existing tenant with the same value, or none, is idempotent; one
// with another value is refused at every point that answers "existing".
func TestDataPathCreateStoresAndServes(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	code, out, hdr := c.do(t, "a", "PUT", "/v1/tenants/dp", createBody(testDataPath))
	if code != 200 || out["data_path"] != testDataPath || out["existing"] != nil {
		t.Fatalf("create: %d %v", code, out)
	}
	wantDataPath(t, "create", hdr, testDataPath)
	mf := mustManifest(t, c.fs, "dp")
	if mf.DataPath != testDataPath {
		t.Fatalf("manifest data_path = %q", mf.DataPath)
	}

	tx := map[string]any{"client": "c", "txid": "1", "statements": []map[string]any{
		{"sql": `CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)`}, {"sql": `INSERT INTO kv VALUES(1, 'x')`},
	}}
	code, _, hdr = c.do(t, "a", "POST", "/v1/t/dp/tx", tx)
	if code != 200 {
		t.Fatalf("tx: %d", code)
	}
	wantDataPath(t, "tx at owner", hdr, testDataPath)
	readTx := map[string]any{"client": "c", "txid": "r", "statements": []map[string]any{{"sql": `SELECT v FROM kv`}}}
	code, _, hdr = c.do(t, "b", "POST", "/v1/t/dp/tx", readTx)
	if code != 200 {
		t.Fatalf("read tx via b: %d", code)
	}
	wantDataPath(t, "tx via proxy", hdr, testDataPath)
	q := map[string]any{"sql": `SELECT COUNT(*) FROM kv`}
	code, _, hdr = c.do(t, "b", "POST", "/v1/t/dp/query", q)
	if code != 200 {
		t.Fatalf("query via b: %d", code)
	}
	wantDataPath(t, "query via proxy", hdr, testDataPath)
	if c.mgrs["b"].metrics.Proxied.Load() == 0 {
		t.Fatal("b did not proxy")
	}
	// a failed query still carries the header: it names the tenant, not
	// the outcome
	code, _, hdr = c.do(t, "a", "POST", "/v1/t/dp/query", map[string]any{"sql": `SELECT * FROM nope`})
	if code == 200 {
		t.Fatal("query of a missing table succeeded")
	}
	wantDataPath(t, "failed query", hdr, testDataPath)
	for _, replica := range []string{"a", "b"} {
		code, out, hdr = c.do(t, replica, "GET", "/v1/t/dp/status", nil)
		if code != 200 {
			t.Fatalf("status via %s: %d", replica, code)
		}
		wantDataPath(t, "status via "+replica, hdr, testDataPath)
		if m, _ := out["manifest"].(map[string]any); m["data_path"] != testDataPath {
			t.Fatalf("status manifest via %s: %v", replica, out["manifest"])
		}
	}

	// idempotent creates: the same value, or none, at the owner and at a
	// non-owner (which answers from the manifest after the owner confirms)
	for _, tc := range []struct{ replica, path string }{
		{"a", testDataPath}, {"a", ""}, {"b", testDataPath}, {"b", ""},
	} {
		code, out, hdr = c.do(t, tc.replica, "PUT", "/v1/tenants/dp", createBody(tc.path))
		if code != 200 || out["data_path"] != testDataPath {
			t.Fatalf("recreate via %s with %q: %d %v", tc.replica, tc.path, code, out)
		}
		if tc.replica == "b" && out["existing"] != true {
			t.Fatalf("recreate via b: %v, want existing", out)
		}
		wantDataPath(t, fmt.Sprintf("recreate via %s with %q", tc.replica, tc.path), hdr, testDataPath)
	}
	// another value is a mismatch everywhere, and moves nothing
	for _, replica := range []string{"a", "b"} {
		start := time.Now()
		code, out, _ = c.do(t, replica, "PUT", "/v1/tenants/dp", createBody(otherDataPath))
		wantMismatch(t, "recreate via "+replica+" with another value", code, out)
		if time.Since(start) > promptCreate {
			t.Fatalf("mismatch via %s took %v: it waited a lease out", replica, time.Since(start))
		}
	}
	if mf := mustManifest(t, c.fs, "dp"); mf.DataPath != testDataPath || mf.OwnerAddr != "a" {
		t.Fatalf("manifest after mismatches: %+v", mf)
	}
	code, out, _ = c.do(t, "a", "POST", "/v1/t/dp/query", q)
	if code != 200 || out["rows"].([]any)[0].([]any)[0].(float64) != 1 {
		t.Fatalf("data after mismatches: %d %v", code, out)
	}
}

// Every manifest CAS copies the current manifest, so a release (ownership
// cleared), a reclass and a takeover by another replica all keep the
// data_path, and the new owner serves it.
func TestDataPathSurvivesReleaseAndTakeover(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dp", createBody(testDataPath)); code != 200 {
		t.Fatalf("create: %d", code)
	}
	tx := map[string]any{"client": "c", "txid": "1", "statements": []map[string]any{
		{"sql": `CREATE TABLE kv(k INTEGER PRIMARY KEY)`},
	}}
	if code, _, _ := c.do(t, "a", "POST", "/v1/t/dp/tx", tx); code != 200 {
		t.Fatalf("tx: %d", code)
	}
	code, _, hdr := c.do(t, "a", "POST", "/v1/t/dp/release", nil)
	if code != 200 {
		t.Fatalf("release: %d", code)
	}
	wantDataPath(t, "release", hdr, testDataPath)
	mf := mustManifest(t, c.fs, "dp")
	if mf.Owner != "" || mf.DataPath != testDataPath {
		t.Fatalf("manifest after release: %+v", mf)
	}
	// b takes the free tenant over: its Tenant copy comes from the manifest
	code, _, hdr = c.do(t, "b", "POST", "/v1/t/dp/query", map[string]any{"sql": `SELECT COUNT(*) FROM kv`})
	if code != 200 || hdr.Get("x-hw-owner") != c.mgrs["b"].ID() {
		t.Fatalf("query via b after release: %d owner=%q", code, hdr.Get("x-hw-owner"))
	}
	wantDataPath(t, "query at new owner", hdr, testDataPath)
	// a create that lands on the new owner, with the same value, is fine
	code, out, hdr := c.do(t, "b", "PUT", "/v1/tenants/dp", createBody(testDataPath))
	if code != 200 || out["data_path"] != testDataPath {
		t.Fatalf("recreate at new owner: %d %v", code, out)
	}
	wantDataPath(t, "recreate at new owner", hdr, testDataPath)
	// a reclass release rewrites the manifest too
	if code, _, _ := c.do(t, "b", "POST", "/v1/t/dp/release?class=whale", nil); code != 200 {
		t.Fatalf("reclass: %d", code)
	}
	if mf := mustManifest(t, c.fs, "dp"); mf.Class != "whale" || mf.DataPath != testDataPath {
		t.Fatalf("manifest after reclass: %+v", mf)
	}
	// the tenant is gone after a delete; a create may then choose another
	// data_path (the delete is the move)
	code, _, hdr = c.do(t, "a", "DELETE", "/v1/tenants/dp", nil)
	if code != 200 {
		t.Fatalf("delete: %d", code)
	}
	wantDataPath(t, "delete", hdr, testDataPath)
	code, out, hdr = c.do(t, "a", "PUT", "/v1/tenants/dp", createBody(otherDataPath))
	if code != 200 || out["data_path"] != otherDataPath {
		t.Fatalf("create after delete: %d %v", code, out)
	}
	wantDataPath(t, "create after delete", hdr, otherDataPath)
}

// A create that loses to an owner that then dies takes the tenant over
// and must answer from the stored data_path, not the body's.
func TestDataPathAfterOwnerCrash(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dp", createBody(testDataPath)); code != 200 {
		t.Fatal("create")
	}
	c.do(t, "a", "POST", "/v1/t/dp/tx", map[string]any{"client": "c", "txid": "s",
		"statements": []map[string]any{{"sql": `CREATE TABLE kv(k INTEGER PRIMARY KEY)`}}})
	c.servers["a"].Close()
	delete(c.servers, "a")
	c.mgrs["a"].KillLease()
	// the lease is still live in the manifest: the create on b waits it
	// out, takes over, and then compares the body with the stored value
	code, out, _ := c.do(t, "b", "PUT", "/v1/tenants/dp", createBody(otherDataPath))
	wantMismatch(t, "create with another value after the owner crashed", code, out)
	code, _, hdr := c.do(t, "b", "POST", "/v1/t/dp/query", map[string]any{"sql": `SELECT COUNT(*) FROM kv`})
	if code != 200 || hdr.Get("x-hw-owner") != c.mgrs["b"].ID() {
		t.Fatalf("query after takeover: %d owner=%q", code, hdr.Get("x-hw-owner"))
	}
	wantDataPath(t, "query after takeover", hdr, testDataPath)
}

// A tenant created without a data_path sends no header, and cannot take
// one later: the field is set at create or never.
func TestDataPathLegacyTenant(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	code, out, hdr := c.do(t, "a", "PUT", "/v1/tenants/legacy", createBody(""))
	if code != 200 || out["data_path"] != nil {
		t.Fatalf("create: %d %v", code, out)
	}
	wantDataPath(t, "create", hdr, "")
	if mf := mustManifest(t, c.fs, "legacy"); mf.DataPath != "" {
		t.Fatalf("manifest: %+v", mf)
	}
	for _, replica := range []string{"a", "b"} {
		code, _, hdr = c.do(t, replica, "POST", "/v1/t/legacy/query", map[string]any{"sql": `SELECT 1`})
		if code != 200 {
			t.Fatalf("query via %s: %d", replica, code)
		}
		wantDataPath(t, "query via "+replica, hdr, "")
		code, out, hdr = c.do(t, replica, "GET", "/v1/t/legacy/status", nil)
		if code != 200 {
			t.Fatalf("status via %s: %d", replica, code)
		}
		wantDataPath(t, "status via "+replica, hdr, "")
		if m, _ := out["manifest"].(map[string]any); m["data_path"] != nil {
			t.Fatalf("status manifest via %s: %v", replica, out["manifest"])
		}
		code, out, _ = c.do(t, replica, "PUT", "/v1/tenants/legacy", createBody(testDataPath))
		wantMismatch(t, "give a legacy tenant a data_path via "+replica, code, out)
		code, out, _ = c.do(t, replica, "PUT", "/v1/tenants/legacy", createBody(""))
		if code != 200 || out["data_path"] != nil {
			t.Fatalf("recreate without via %s: %d %v", replica, code, out)
		}
	}
}

func TestDataPathValidation(t *testing.T) {
	long := "s3://b/" + strings.Repeat("x", MaxDataPathLength)
	for _, tc := range []struct {
		path string
		ok   bool
	}{
		{"s3://bucket", true},
		{"s3://bucket/", true},
		{"s3://bucket/prefix", true},
		{"s3://bucket/a/b/c/", true},
		{"s3://bucket/a//b", true}, // hoglake accepts an empty segment here too
		{"s3://bucket/café", true},
		{"s3://b/" + strings.Repeat("x", MaxDataPathLength-len("s3://b/")), true},
		{"", false},
		{long, false},
		{"S3://bucket/prefix", false},
		{"s3:/bucket", false},
		{"s3://", false},
		{"s3:///prefix", false},
		{"gs://bucket/prefix", false},
		{"bucket/prefix", false},
		{"s3://bucket/./x", false},
		{"s3://bucket/../x", false},
		{"s3://bucket/x/..", false},
		{"s3://bucket/x/.", false},
		{"s3://bucket/a b", false},
		{"s3://bucket/a\tb", false},
		{"s3://bucket/a\nb", false},
		{"s3://bucket/a\x00b", false},
		{"s3://bucket/\xff", false},
	} {
		err := validDataPath(tc.path)
		if (err == nil) != tc.ok {
			t.Errorf("validDataPath(%q) = %v, want ok=%v", tc.path, err, tc.ok)
		}
	}
	c := newTestCluster(t, "a")
	for _, bad := range []string{"gs://bucket/x", "s3://bucket/../x", "s3://"} {
		code, out, _ := c.do(t, "a", "PUT", "/v1/tenants/bad", createBody(bad))
		if code != 400 || out["code"] != CodeInvalidDataPath {
			t.Fatalf("create with %q: %d %v, want 400 %s", bad, code, out, CodeInvalidDataPath)
		}
	}
	if mf, _, _ := c.fs.GetManifest(context.Background(), "bad"); mf != nil {
		t.Fatal("a refused create made a tenant")
	}
	// a data_path that is present but not a non-empty string, or a body
	// that does not decode, is refused: it used to be ignored, and made a
	// tenant without a data_path that could never gain one
	for _, tc := range []struct {
		name, body string
		code       string
	}{
		{"numeric", `{"data_path": 7}`, CodeInvalidDataPath},
		{"null", `{"data_path": null}`, CodeInvalidDataPath},
		{"empty", `{"data_path": ""}`, CodeInvalidDataPath},
		{"object", `{"data_path": {"s3": "x"}}`, CodeInvalidDataPath},
		{"non-string class", `{"class": 1}`, ""},
		{"truncated", `{"data_path": "s3://b/x"`, ""},
		{"array body", `["s3://b/x"]`, ""},
		{"text body", `not json`, ""},
		{"null body", `null`, ""},
		{"trailing text", `{"data_path": "s3://b/x"} garbage`, ""},
		{"two objects", `{"data_path": "s3://b/x"} {}`, ""},
	} {
		req, _ := http.NewRequest("PUT", c.servers["a"].URL+"/v1/tenants/badbody", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode != 400 || out["code"] != tc.code && !(tc.code == "" && out["code"] == nil) {
			t.Fatalf("create with a %s body %s: %d %v, want 400 %q", tc.name, tc.body, resp.StatusCode, out, tc.code)
		}
	}
	if mf, _, _ := c.fs.GetManifest(context.Background(), "badbody"); mf != nil {
		t.Fatal("a refused body made a tenant")
	}
	// an empty body, an empty object and a body with only a class are the
	// "no claim" forms
	for _, body := range []string{"", "  ", "{}", `{"class":"standard"}`} {
		req, _ := http.NewRequest("PUT", c.servers["a"].URL+"/v1/tenants/noclaim", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("create with body %q: %d", body, resp.StatusCode)
		}
	}
}

// A create body that does not arrive whole is refused, not read as empty:
// before, a connection cut before the body created the tenant without the
// data_path the body carried, and such a tenant can never gain one. The
// client announces a body and half-closes without sending it, so the
// server's read ends early (the bytes that did arrive, none, parse as a
// no-claim body) and its answer still has a way back.
func TestCreateCutBodyRefused(t *testing.T) {
	c := newTestCluster(t, "a")
	u, _ := url.Parse(c.servers["a"].URL)
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "PUT /v1/tenants/cut HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n",
		u.Host, len(fmt.Sprintf(`{"data_path":%q}`, testDataPath)))
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != 400 || !strings.Contains(fmt.Sprint(out["error"]), "create body") {
		t.Fatalf("cut body: %d %v, want 400 naming the body", resp.StatusCode, out)
	}
	if mf, _, _ := c.fs.GetManifest(context.Background(), "cut"); mf != nil {
		t.Fatalf("a cut body made a tenant: %+v", mf)
	}
}

func TestParseCreateBody(t *testing.T) {
	for _, tc := range []struct {
		raw      string
		path     string
		class    string
		code     int
		codeName string
	}{
		{"", "", "", 0, ""},
		{"{}", "", "", 0, ""},
		{`{"class":"whale"}`, "", "whale", 0, ""},
		{`{"data_path":"s3://b/x"}`, "s3://b/x", "", 0, ""},
		// a key hoglite does not know is a 400, not a no-claim: a misspelled
		// data_path would otherwise create a tenant that can never gain one
		{`{"data_path":"s3://b/x","other":1}`, "", "", 400, ""},
		{`{"dataPath":"s3://b/x"}`, "", "", 400, ""},
		{`{"datapath":"s3://b/x"}`, "", "", 400, ""},
		{`{"data-path":"s3://b/x"}`, "", "", 400, ""},
		// keys match exactly, as on hoglake's JVM side (Jackson): a known
		// key in another case is an unknown key, never folded onto it
		{`{"Class":"whale"}`, "", "", 400, ""},
		{`{"DATA_PATH":"s3://b/x"}`, "", "", 400, ""},
		{`{"Data_Path":"s3://b/x"}`, "", "", 400, ""},
		// a repeated key takes its last value, as Jackson does, so both
		// sides read one body the same way
		{`{"data_path":"s3://b/x","data_path":"s3://b/y"}`, "s3://b/y", "", 0, ""},
		{`{"class":null}`, "", "", 400, ""},
		{`{"data_path":""}`, "", "", 400, CodeInvalidDataPath},
		{`{"data_path":null}`, "", "", 400, CodeInvalidDataPath},
		{`{"data_path":7}`, "", "", 400, CodeInvalidDataPath},
		{`{"data_path":["s3://b/x"]}`, "", "", 400, CodeInvalidDataPath},
		{`{"class":7}`, "", "", 400, ""},
		{`{`, "", "", 400, ""},
		{`[]`, "", "", 400, ""},
		{`"s3://b/x"`, "", "", 400, ""},
		// the body is empty or exactly one object with nothing after it:
		// a bare null is not an object with no fields, and a second value
		// or text after the object is not ignored
		{" \n{}\n ", "", "", 0, ""},
		{`null`, "", "", 400, ""},
		{`true`, "", "", 400, ""},
		{`7`, "", "", 400, ""},
		{`{"data_path":"s3://b/x"} garbage`, "", "", 400, ""},
		{`{"data_path":"s3://b/x"} {"data_path":"s3://b/y"}`, "", "", 400, ""},
		{`{"data_path":"s3://b/x"}}`, "", "", 400, ""},
		{`{"data_path":"s3://b/x"},`, "", "", 400, ""},
		{`{} null`, "", "", 400, ""},
	} {
		body, code, perr := parseCreateBody([]byte(tc.raw))
		if code != tc.code || perr.Code != tc.codeName || (code == 0 && (body.DataPath != tc.path || body.Class != tc.class)) {
			t.Errorf("parseCreateBody(%q) = %+v, %d, %+v; want path %q class %q %d %q", tc.raw, body, code, perr, tc.path, tc.class, tc.code, tc.codeName)
		}
	}
}

// A data_path inside hoglite's own storage, or above it, is refused: a
// catalog there could register hoglite's objects and have hoglake delete
// them. The reserved roots are the backend's (StorageRoots).
func TestDataPathReservedStorage(t *testing.T) {
	c := newTestCluster(t, "a")
	for _, bad := range []string{
		FakeStorageRoot, FakeStorageRoot + "/", FakeStorageRoot + "/00/t/x", "s3://fake-bucket",
	} {
		code, out, _ := c.do(t, "a", "PUT", "/v1/tenants/reserved", createBody(bad))
		if code != 400 || out["code"] != CodeInvalidDataPath || !strings.Contains(out["error"].(string), "hoglite's own storage") {
			t.Fatalf("create with %q: %d %v, want 400 %s naming hoglite's storage", bad, code, out, CodeInvalidDataPath)
		}
	}
	if mf, _, _ := c.fs.GetManifest(context.Background(), "reserved"); mf != nil {
		t.Fatal("a refused create made a tenant")
	}
	// a sibling prefix in the same bucket, and a prefix that merely starts
	// with the same characters, are not hoglite's storage
	for _, ok := range []string{"s3://fake-bucket/other", "s3://fake-bucket/fakes", "s3://fake-bucket2/fake"} {
		code, out, _ := c.do(t, "a", "PUT", "/v1/tenants/near-"+TenantShard(ok), createBody(ok))
		if code != 200 || out["data_path"] != ok {
			t.Fatalf("create with %q: %d %v", ok, code, out)
		}
	}
}

func TestStoreStorageRoots(t *testing.T) {
	one := NewStore(nil, "ctl", "/hl/")
	if got := one.StorageRoots(); len(got) != 1 || got[0] != "s3://ctl/hl" {
		t.Fatalf("one plane: %v", got)
	}
	split := NewStore(nil, "ctl", "hl").WithDataPlane(nil, "data-express")
	if got := split.StorageRoots(); len(got) != 2 || got[0] != "s3://ctl/hl" || got[1] != "s3://data-express/hl" {
		t.Fatalf("split planes: %v", got)
	}
	if err := reservedDataPath("s3://data-express/hl/x", split.StorageRoots()); err == nil {
		t.Fatal("data plane root not reserved")
	}
	if err := reservedDataPath("s3://data-express/other", split.StorageRoots()); err != nil {
		t.Fatal(err)
	}
}

func TestDataPathsOverlap(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"s3://b/x", "s3://b/x", true},
		{"s3://b/x", "s3://b/x/", true},
		{"s3://b/x/", "s3://b/x", true},
		{"s3://b/x", "s3://b/x/y", true},
		{"s3://b/x/y/z", "s3://b/x", true},
		{"s3://b", "s3://b/x", true},
		{"s3://b/", "s3://b/x/y", true},
		{"s3://b/x", "s3://b/xy", false},
		{"s3://b/xy", "s3://b/x", false},
		{"s3://b/x", "s3://c/x", false},
		{"s3://b", "s3://bb/x", false},
	} {
		if got := dataPathsOverlap(tc.a, tc.b); got != tc.want {
			t.Errorf("dataPathsOverlap(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// A create refuses a data_path that overlaps another tenant's, in either
// direction and whatever the trailing slash; a disjoint one, a sibling
// whose name merely extends the other, and the tenant's own idempotent
// recreate pass. Tenants without a data_path take no part.
func TestDataPathOverlapAcrossTenants(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	for tid, path := range map[string]string{"one": testDataPath, "legacy": ""} {
		if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/"+tid, createBody(path)); code != 200 {
			t.Fatalf("create %s: %d", tid, code)
		}
	}
	for i, bad := range []string{
		testDataPath, testDataPath + "/", testDataPath + "/sub", "s3://hoglake-dev/catalogs", "s3://hoglake-dev/catalogs/", "s3://hoglake-dev",
	} {
		replica := []string{"a", "b"}[i%2]
		start := time.Now()
		code, out, _ := c.do(t, replica, "PUT", "/v1/tenants/two", createBody(bad))
		if code != 409 || out["code"] != CodeDataPathOverlap {
			t.Fatalf("create two with %q via %s: %d %v, want 409 %s", bad, replica, code, out, CodeDataPathOverlap)
		}
		if msg := out["error"].(string); !strings.Contains(msg, "tenant one") || !strings.Contains(msg, testDataPath) {
			t.Fatalf("overlap error does not name the other tenant and its path: %q", msg)
		}
		if time.Since(start) > promptCreate {
			t.Fatalf("overlap refusal took %v", time.Since(start))
		}
	}
	if mf, _, _ := c.fs.GetManifest(context.Background(), "two"); mf != nil {
		t.Fatal("a refused create made a tenant")
	}
	for tid, ok := range map[string]string{"two": testDataPath + "2", "three": "s3://hoglake-dev/catalogs/demo-2", "four": "s3://other-bucket/catalogs/demo"} {
		code, out, _ := c.do(t, "b", "PUT", "/v1/tenants/"+tid, createBody(ok))
		if code != 200 || out["data_path"] != ok {
			t.Fatalf("create %s with %q: %d %v", tid, ok, code, out)
		}
	}
	// the tenant's own path is not an overlap with itself: idempotent
	// recreates at the owner and at a non-owner still answer 200
	for _, replica := range []string{"a", "b"} {
		code, out, _ := c.do(t, replica, "PUT", "/v1/tenants/one", createBody(testDataPath))
		if code != 200 || out["data_path"] != testDataPath {
			t.Fatalf("recreate one via %s: %d %v", replica, code, out)
		}
	}
	// a tenant without a data_path can still be created on any backend
	// that has tenants with one, and never blocks a path
	if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/legacy2", createBody("")); code != 200 {
		t.Fatalf("create legacy2: %d", code)
	}
	// the scan picks the lowest tenant id when several overlap
	otherTid, otherPath, err := otherDataPathOverlap(context.Background(), c.fs, "new", "s3://hoglake-dev/catalogs")
	if err != nil || otherTid != "one" || otherPath != testDataPath {
		t.Fatalf("scan: %q %q %v", otherTid, otherPath, err)
	}
	if otherTid, _, err := otherDataPathOverlap(context.Background(), c.fs, "one", testDataPath); err != nil || otherTid != "" {
		t.Fatalf("scan excluding self: %q %v", otherTid, err)
	}
	// a scan whose caller is gone is unfinished, not "no overlap"
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	slow := &slowReadStore{Backend: c.fs}
	if otherTid, _, err := otherDataPathOverlap(gone, slow, "new", "s3://elsewhere/x"); err == nil || otherTid != "" {
		t.Fatalf("scan under a cancelled context: %q %v, want an error", otherTid, err)
	}
}

// slowReadStore fails manifest reads whose context has ended, as S3 does.
type slowReadStore struct{ Backend }

func (s *slowReadStore) GetManifest(ctx context.Context, tid string) (*Manifest, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	return s.Backend.GetManifest(ctx, tid)
}

// failReadStore fails every manifest read of one tenant: the overlap scan
// cannot finish, and the create must refuse rather than skip the check.
type failReadStore struct {
	Backend
	tid string
}

func (s *failReadStore) GetManifest(ctx context.Context, tid string) (*Manifest, string, error) {
	if tid == s.tid {
		return nil, "", errors.New("injected manifest read failure")
	}
	return s.Backend.GetManifest(ctx, tid)
}

func TestDataPathOverlapScanFailureRefuses(t *testing.T) {
	fs := NewFakeStore()
	c := newRaceCluster(t, fs, fs, "a")
	if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/one", createBody(testDataPath)); code != 200 {
		t.Fatal("create one")
	}
	broken := newRaceCluster(t, &failReadStore{Backend: fs, tid: "one"}, fs, "x")
	code, out, _ := broken.do(t, "x", "PUT", "/v1/tenants/two", createBody(otherDataPath))
	if code != 503 || !strings.Contains(out["error"].(string), "overlap check") {
		t.Fatalf("create with an unreadable manifest in the scan: %d %v, want 503", code, out)
	}
	if mf, _, _ := fs.GetManifest(context.Background(), "two"); mf != nil {
		t.Fatal("an unchecked create made a tenant")
	}
	// without a data_path there is nothing to check
	if code, _, _ := broken.do(t, "x", "PUT", "/v1/tenants/three", createBody("")); code != 200 {
		t.Fatalf("create without data_path: %d", code)
	}
}

// newClassCluster is newTestCluster with a class per replica.
func newClassCluster(t *testing.T, classes map[string]string) *testCluster {
	t.Helper()
	c := &testCluster{fs: NewFakeStore(), servers: map[string]*httptest.Server{}, mgrs: map[string]*Manager{}}
	urls := map[string]string{}
	for id, class := range classes {
		c.mgrs[id] = NewManager(c.fs, "fake", id, class, t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{})
		c.servers[id] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			NewServer(c.mgrs[id], urls).Handler().ServeHTTP(w, r)
		}))
		t.Cleanup(c.servers[id].Close)
	}
	for id, srv := range c.servers {
		urls[id] = srv.URL
	}
	return c
}

// A create that lands on a replica of another class answers from the
// manifest (acquire's ErrWrongClass): same value or none is "existing",
// another value is the mismatch.
func TestDataPathMismatchAtWrongClass(t *testing.T) {
	c := newClassCluster(t, map[string]string{"a": "standard", "w": "whale"})
	if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dp", createBody(testDataPath)); code != 200 {
		t.Fatal("create")
	}
	// a create leaves Class empty (any class may take the tenant); the
	// reclass release pins it
	if code, _, _ := c.do(t, "a", "POST", "/v1/t/dp/release?class=standard", nil); code != 200 {
		t.Fatal("reclass")
	}
	if _, err := c.mgrs["w"].Acquire(context.Background(), "dp", false); !errors.Is(err, ErrWrongClass) {
		t.Fatalf("whale acquire: %v, want ErrWrongClass", err)
	}
	code, out, _ := c.do(t, "w", "PUT", "/v1/tenants/dp", createBody(otherDataPath))
	wantMismatch(t, "create with another value at the wrong class", code, out)
	for _, path := range []string{testDataPath, ""} {
		code, out, hdr := c.do(t, "w", "PUT", "/v1/tenants/dp", createBody(path))
		if code != 200 || out["existing"] != true || out["data_path"] != testDataPath {
			t.Fatalf("recreate with %q at the wrong class: %d %v", path, code, out)
		}
		wantDataPath(t, "recreate at the wrong class", hdr, testDataPath)
	}
	if mf := mustManifest(t, c.fs, "dp"); mf.DataPath != testDataPath || mf.Class != "standard" {
		t.Fatalf("manifest: %+v", mf)
	}
}

// Two replicas create the same tenant at once with different data_paths:
// the create CAS picks the winner, and the loser, whose acquire answers
// ErrBusy for the live winner, gets the mismatch from the manifest, not
// a 503 from the winner refusing to confirm its body.
func TestDataPathMismatchAtCreateRace(t *testing.T) {
	fs := NewFakeStore()
	bs := newCreateBarrierStore(fs, "dp", 2)
	c := newRaceCluster(t, bs, fs, "a", "b")
	for _, m := range c.mgrs {
		t.Cleanup(m.ShutdownAll)
	}
	type reply struct {
		id   string
		code int
		out  map[string]any
	}
	replies := make(chan reply, 2)
	paths := map[string]string{"a": testDataPath, "b": otherDataPath}
	for id, path := range paths {
		go func() {
			code, out, _ := c.do(t, id, "PUT", "/v1/tenants/dp", createBody(path))
			replies <- reply{id, code, out}
		}()
	}
	got := map[string]reply{}
	for range 2 {
		r := <-replies
		got[r.id] = r
	}
	mf := mustManifest(t, fs, "dp")
	winner := mf.OwnerAddr
	if mf.DataPath != paths[winner] {
		t.Fatalf("manifest %+v does not carry the winner's data_path %q", mf, paths[winner])
	}
	for id, r := range got {
		if id == winner {
			if r.code != 200 || r.out["data_path"] != paths[id] {
				t.Fatalf("winner %s: %d %v", id, r.code, r.out)
			}
			continue
		}
		wantMismatch(t, "loser "+id, r.code, r.out)
	}
}

// peerCreatesOnReadStore runs a registered action the first time a replica
// reads "no manifest" for a tenant, and still answers that stale read: the
// action is a peer's create, so the replica's own create meets a live
// owner only inside acquire.
type peerCreatesOnReadStore struct {
	Backend
	mu      sync.Mutex
	onFirst map[string]func()
}

func (s *peerCreatesOnReadStore) arm(tid string, create func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.onFirst == nil {
		s.onFirst = map[string]func(){}
	}
	s.onFirst[tid] = create
}

func (s *peerCreatesOnReadStore) GetManifest(ctx context.Context, tid string) (*Manifest, string, error) {
	mf, etag, err := s.Backend.GetManifest(ctx, tid)
	if err == nil && mf == nil {
		s.mu.Lock()
		create := s.onFirst[tid]
		delete(s.onFirst, tid)
		s.mu.Unlock()
		if create != nil {
			create()
		}
	}
	return mf, etag, err
}

// A peer creates the tenant after createAtLiveOwner read no manifest.
// acquire then sees a live owner and asks the server to confirm it: the
// mismatch is decided from the manifest there, at once. Before, the
// confirmation carried the body to the owner, which answered 409, read as
// a denial, and the create waited the owner's lease out.
func TestDataPathMismatchWhenPeerCreatesDuringAcquire(t *testing.T) {
	fs := NewFakeStore()
	ps := &peerCreatesOnReadStore{Backend: fs}
	mgrs := map[string]*Manager{
		"a": NewManager(fs, "fake", "a", "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{}),
		"b": NewManager(ps, "fake", "b", "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{}),
	}
	c := &testCluster{fs: fs, servers: map[string]*httptest.Server{}, mgrs: mgrs}
	peers := map[string]string{}
	for id := range mgrs {
		c.servers[id] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			NewServer(mgrs[id], peers).Handler().ServeHTTP(w, r)
		}))
		t.Cleanup(c.servers[id].Close)
		t.Cleanup(mgrs[id].ShutdownAll)
	}
	for id, srv := range c.servers {
		peers[id] = srv.URL
	}
	ps.arm("dp", func() {
		if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dp", createBody(testDataPath)); code != 200 {
			t.Errorf("peer create: %d", code)
		}
	})
	start := time.Now()
	code, out, _ := c.do(t, "b", "PUT", "/v1/tenants/dp", createBody(otherDataPath))
	wantMismatch(t, "create against a peer that created meanwhile", code, out)
	if time.Since(start) > promptCreate {
		t.Fatalf("mismatch took %v: it waited the owner's lease out", time.Since(start))
	}
	if mf := mustManifest(t, fs, "dp"); mf.DataPath != testDataPath || mf.OwnerAddr != "a" {
		t.Fatalf("manifest: %+v", mf)
	}
	// the same race with the same value is an idempotent "existing"
	ps.arm("dp2", func() {
		if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dp2", createBody(testDataPath+"-2")); code != 200 {
			t.Errorf("peer create: %d", code)
		}
	})
	code, out, hdr := c.do(t, "b", "PUT", "/v1/tenants/dp2", createBody(testDataPath+"-2"))
	if code != 200 || out["existing"] != true {
		t.Fatalf("same-value race: %d %v", code, out)
	}
	wantDataPath(t, "same-value race", hdr, testDataPath+"-2")
}

// The data_path flag in Manifest.Format. A manifest with a data_path
// carries the flag, through every CAS; a build before the flag compares
// the whole Format against its highest codec and refuses it; this build
// refuses a manifest whose flag and field disagree, or that carries a
// flag it does not know. Tenants without a data_path are unchanged.
func TestManifestFormatFlags(t *testing.T) {
	plain := &Manifest{Format: manifestFormatChain}
	if manifestFormat(manifestFormatChain, plain) != manifestFormatChain || manifestCodec(plain) != manifestFormatChain {
		t.Fatal("a manifest without data_path gains bits")
	}
	if err := checkManifestFormat("t", plain); err != nil {
		t.Fatal(err)
	}
	for _, codec := range []int{manifestFormatRaw, manifestFormatZstd, manifestFormatChain} {
		mf := &Manifest{DataPath: testDataPath}
		mf.Format = manifestFormat(codec, mf)
		if manifestCodec(mf) != codec || mf.Format&manifestFlagDataPath == 0 {
			t.Fatalf("codec %d with data_path: format %d", codec, mf.Format)
		}
		if err := checkManifestFormat("t", mf); err != nil {
			t.Fatalf("codec %d with data_path: %v", codec, err)
		}
		// the gate of every earlier build (checkManifestFormatMax against
		// its highest codec) refuses the flagged manifest
		for _, max := range []int{manifestFormatZstd, manifestFormatChain} {
			if err := checkManifestFormatMax("t", mf, max); !errors.Is(err, ErrUnsupportedFormat) {
				t.Fatalf("older gate (max %d) accepted format %d: %v", max, mf.Format, err)
			}
		}
	}
	for name, mf := range map[string]*Manifest{
		"flag without field":   {Format: manifestFormatChain | manifestFlagDataPath},
		"field without flag":   {Format: manifestFormatChain, DataPath: testDataPath},
		"unknown flag":         {Format: manifestFormatChain | 1<<9},
		"unknown flag + known": {Format: manifestFormatChain | manifestFlagDataPath | 1<<12, DataPath: testDataPath},
		"newer codec":          {Format: maxManifestCodec + 1},
		"newer codec + flag":   {Format: (maxManifestCodec + 1) | manifestFlagDataPath, DataPath: testDataPath},
	} {
		if err := checkManifestFormat("t", mf); !errors.Is(err, ErrUnsupportedFormat) {
			t.Errorf("%s (format %d) accepted: %v", name, mf.Format, err)
		}
	}
}

// Every reader of Manifest.Format masks the flags off before it compares
// codecs: a data_path tenant in a raw generation materializes raw, and one
// in a chain generation still plans deltas and applies the chain at
// takeover.
func TestDataPathCodecsMasked(t *testing.T) {
	for name, tc := range map[string]struct {
		mut   func(*TenantOpts)
		codec int
		delta bool
	}{
		"raw":   {rawOpts, manifestFormatRaw, false},
		"chain": {func(o *TenantOpts) { o.DeltaMaxRatio = 1000 }, manifestFormatChain, true},
	} {
		t.Run(name, func(t *testing.T) {
			opts := testOpts()
			tc.mut(&opts)
			c := newTestClusterOpts(t, opts, "a", "b")
			if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dp", createBody(testDataPath)); code != 200 {
				t.Fatal("create")
			}
			tx := map[string]any{"client": "c", "txid": "1", "statements": []map[string]any{
				{"sql": `CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)`}, {"sql": `INSERT INTO kv VALUES(1, 'x')`},
			}}
			if code, _, _ := c.do(t, "a", "POST", "/v1/t/dp/tx", tx); code != 200 {
				t.Fatal("tx")
			}
			if code, _, _ := c.do(t, "a", "POST", "/v1/t/dp/release", nil); code != 200 {
				t.Fatal("release")
			}
			mf := mustManifest(t, c.fs, "dp")
			if manifestCodec(mf) != tc.codec || mf.Format&manifestFlagDataPath == 0 || (len(mf.Deltas) > 0) != tc.delta {
				t.Fatalf("manifest after release: format %d deltas %d, want codec %d with the flag, deltas=%v", mf.Format, len(mf.Deltas), tc.codec, tc.delta)
			}
			code, out, hdr := c.do(t, "b", "POST", "/v1/t/dp/query", map[string]any{"sql": `SELECT v FROM kv`})
			if code != 200 || out["rows"].([]any)[0].([]any)[0] != "x" {
				t.Fatalf("takeover of a %s generation: %d %v", name, code, out)
			}
			wantDataPath(t, "query after takeover", hdr, testDataPath)
		})
	}
}

// A tenant with a data_path is refused by a build from before the field,
// so a mixed-version pool or a rollback never rewrites its manifest without
// the field: the manifest keeps the flag through create, checkpoint,
// release and takeover, the older build's acquire and sweeper leave it
// alone, and a tenant without a data_path stays readable by that build.
// Replica "old" emulates the build with page deltas but before manifest
// flags (s2ReadForTest): its gate compares the whole Format against codec
// 2, and it would decode a manifest without DataPath and write it back
// without the field.
func TestDataPathRefusedByOlderBuild(t *testing.T) {
	c := newTestCluster(t, "a", "b", "old")
	old := c.mgrs["old"]
	old.s2ReadForTest = true
	if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/dp", createBody(testDataPath)); code != 200 {
		t.Fatal("create dp")
	}
	if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/plain", createBody("")); code != 200 {
		t.Fatal("create plain")
	}
	flagged := func(what string) *Manifest {
		t.Helper()
		mf := mustManifest(t, c.fs, "dp")
		if mf.Format&manifestFlagDataPath == 0 || mf.DataPath != testDataPath || manifestCodec(mf) > maxManifestCodec {
			t.Fatalf("%s: manifest %+v lacks the data_path flag", what, mf)
		}
		return mf
	}
	created := flagged("after create")
	for _, tid := range []string{"dp", "plain"} {
		tx := map[string]any{"client": "c", "txid": "1", "statements": []map[string]any{
			{"sql": `CREATE TABLE kv(k INTEGER PRIMARY KEY)`}, {"sql": `INSERT INTO kv VALUES(1)`},
		}}
		if code, _, _ := c.do(t, "a", "POST", "/v1/t/"+tid+"/tx", tx); code != 200 {
			t.Fatalf("tx %s: %d", tid, code)
		}
		// the release checkpoints the write: a new generation, whose codec
		// the upload decides, and the flag with it
		if code, _, _ := c.do(t, "a", "POST", "/v1/t/"+tid+"/release", nil); code != 200 {
			t.Fatalf("release %s: %d", tid, code)
		}
	}
	before := flagged("after checkpoint and release")
	if before.Owner != "" || before.Gen <= created.Gen {
		t.Fatalf("release did not checkpoint: %+v -> %+v", created, before)
	}
	ctx := context.Background()
	if _, err := old.Acquire(ctx, "dp", false); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("older build acquired a data_path tenant: %v", err)
	}
	if after := mustManifest(t, c.fs, "dp"); !manifestEqual(before, after) || after.DataPath != testDataPath {
		t.Fatalf("older build moved the manifest: %+v -> %+v", before, after)
	}
	// an orphan the sweeper of the older build would otherwise collect
	orphan := segKey("fake", "dp", "deaddead", 1, 1, 0)
	c.fs.PutSegment(ctx, orphan, []byte("x"))
	NewSweeper(old, c.fs, "fake", testOpts(), &Metrics{}).sweepTenant(ctx, "dp")
	if _, err := c.fs.GetObject(ctx, orphan); err != nil {
		t.Fatal("older build's sweeper touched a data_path tenant")
	}
	// the tenant without a data_path is the older build's to take
	if n := countRows(t, old, "plain"); n != 1 {
		t.Fatalf("older build reads the plain tenant: %d rows", n)
	}
	if err := old.Release("plain", ""); err != nil {
		t.Fatal(err)
	}
	// this build takes the data_path tenant over, serves the data_path,
	// and its claim keeps the flag
	code, _, hdr := c.do(t, "b", "POST", "/v1/t/dp/query", map[string]any{"sql": `SELECT COUNT(*) FROM kv`})
	if code != 200 {
		t.Fatalf("query at new owner: %d", code)
	}
	wantDataPath(t, "query at new owner", hdr, testDataPath)
	if mf := flagged("after takeover"); mf.OwnerAddr != "b" {
		t.Fatalf("takeover: %+v", mf)
	}
	// through HTTP, the older build refuses the tenant without claiming it
	if code, _, _ := c.do(t, "b", "POST", "/v1/t/dp/release", nil); code != 200 {
		t.Fatalf("release: %d", code)
	}
	code, out, _ := c.do(t, "old", "POST", "/v1/t/dp/query", map[string]any{"sql": `SELECT 1`})
	if code == 200 || !strings.Contains(out["error"].(string), "newer storage format") {
		t.Fatalf("older build served a data_path tenant: %d %v", code, out)
	}
	if mf := flagged("after the older build's refusal"); mf.Owner != "" {
		t.Fatalf("older build claimed the tenant: %+v", mf)
	}
}

// Each batch item carries its tenant's data_path, from the local answer
// and from a peer's sub-batch; the batch header is set only when the
// items resolve to one tenant and one value.
func TestDataPathQueryBatch(t *testing.T) {
	c := newTestCluster(t, "a", "b")
	for tid, path := range map[string]string{"one": testDataPath, "two": otherDataPath, "legacy": ""} {
		if code, _, _ := c.do(t, "a", "PUT", "/v1/tenants/"+tid, createBody(path)); code != 200 {
			t.Fatalf("create %s: %d", tid, code)
		}
	}
	item := func(tid string) map[string]any { return map[string]any{"tenant": tid, "sql": "SELECT 1"} }
	type answer struct {
		Status   int    `json:"status"`
		DataPath string `json:"data_path"`
	}
	results := func(out map[string]any) []answer {
		b, _ := json.Marshal(out["results"])
		var got []answer
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("results: %v: %s", err, b)
		}
		return got
	}
	for _, replica := range []string{"a", "b"} {
		// one tenant: header and items agree
		code, out, hdr := c.do(t, replica, "POST", "/v1/query-batch", map[string]any{"queries": []any{item("one"), item("one")}})
		if code != 200 {
			t.Fatalf("batch via %s: %d %v", replica, code, out)
		}
		wantDataPath(t, "single-tenant batch via "+replica, hdr, testDataPath)
		for i, a := range results(out) {
			if a.Status != 200 || a.DataPath != testDataPath {
				t.Fatalf("batch via %s item %d: %+v", replica, i, a)
			}
		}
		// several tenants: no header, each item has its own
		code, out, hdr = c.do(t, replica, "POST", "/v1/query-batch", map[string]any{"queries": []any{item("one"), item("two"), item("legacy"), item("missing")}})
		if code != 200 {
			t.Fatalf("mixed batch via %s: %d %v", replica, code, out)
		}
		wantDataPath(t, "mixed batch via "+replica, hdr, "")
		got := results(out)
		want := []answer{{200, testDataPath}, {200, otherDataPath}, {200, ""}, {404, ""}}
		if len(got) != len(want) {
			t.Fatalf("mixed batch via %s: %+v", replica, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("mixed batch via %s item %d: %+v, want %+v", replica, i, got[i], want[i])
			}
		}
	}
	if c.mgrs["b"].metrics.Proxied.Load() == 0 {
		t.Fatal("b forwarded no sub-batch")
	}
	// one tenant whose items disagree on the outcome: the header follows
	// the tenant, not the outcome
	code, out, hdr := c.do(t, "b", "POST", "/v1/query-batch", map[string]any{"queries": []any{item("one"), map[string]any{"tenant": "one", "sql": "SELECT * FROM nope"}}})
	if code != 200 {
		t.Fatalf("batch with a failing item: %d %v", code, out)
	}
	wantDataPath(t, "batch with a failing item", hdr, testDataPath)
	if got := results(out); got[1].Status == 200 || got[1].DataPath != testDataPath {
		t.Fatalf("failing item: %+v", got[1])
	}
}

// Only operator authority creates tenants, so only an operator sets a
// data_path; a write token cannot create (and so cannot set one), and a
// read token gets the header on its reads.
func TestDataPathCreateNeedsOperator(t *testing.T) {
	f := newAuthFixture(t)
	fs := NewFakeStore()
	peers := map[string]string{}
	servers := map[string]*httptest.Server{}
	for _, id := range []string{"a", "b"} {
		m := NewManager(fs, "fake", id, "standard", t.TempDir(), testOpts(), 1000, &Metrics{}, ControllerOpts{})
		s := httptest.NewServer(NewServer(m, peers).WithAuth(f.auth).Handler())
		servers[id], peers[id] = s, s.URL
		t.Cleanup(m.ShutdownAll)
		t.Cleanup(s.Close)
	}
	do := func(replica, method, path, token, body string) (int, map[string]any, http.Header) {
		t.Helper()
		r, _ := http.NewRequest(method, servers[replica].URL+path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out, resp.Header
	}
	operator := f.token(t, func(c jwt.MapClaims) {
		c["ten"], c["cat"] = "operator", map[string][]string{"catalog": {"admin"}}
	})
	write := f.token(t, nil)
	read := f.token(t, func(c jwt.MapClaims) { c["cat"] = map[string][]string{"catalog": {"read"}} })
	body := fmt.Sprintf(`{"data_path":%q}`, testDataPath)
	if code, _, _ := do("a", "PUT", "/v1/tenants/opaque_1", write, body); code != 403 {
		t.Fatalf("write token created a tenant: %d", code)
	}
	if mf, _, _ := fs.GetManifest(context.Background(), "opaque_1"); mf != nil {
		t.Fatal("write token made a manifest")
	}
	code, out, hdr := do("a", "PUT", "/v1/tenants/opaque_1", operator, body)
	if code != 200 || out["data_path"] != testDataPath {
		t.Fatalf("operator create: %d %v", code, out)
	}
	wantDataPath(t, "operator create", hdr, testDataPath)
	// the owner confirmation of an idempotent create carries the operator
	// token and the body; a mismatch is decided at the non-owner
	code, out, _ = do("b", "PUT", "/v1/tenants/opaque_1", operator, fmt.Sprintf(`{"data_path":%q}`, otherDataPath))
	wantMismatch(t, "operator recreate with another value via b", code, out)
	code, out, hdr = do("b", "PUT", "/v1/tenants/opaque_1", operator, body)
	if code != 200 || out["existing"] != true {
		t.Fatalf("operator recreate via b: %d %v", code, out)
	}
	wantDataPath(t, "operator recreate via b", hdr, testDataPath)
	code, _, hdr = do("b", "POST", "/v1/t/opaque_1/query", read, `{"sql":"SELECT 1"}`)
	if code != 200 {
		t.Fatalf("read via b: %d", code)
	}
	wantDataPath(t, "read via proxy", hdr, testDataPath)
	code, _, hdr = do("b", "POST", "/v1/query-batch", read, `{"queries":[{"tenant":"opaque_1","sql":"SELECT 1"}]}`)
	if code != 200 {
		t.Fatalf("batch via b: %d", code)
	}
	wantDataPath(t, "batch via proxy", hdr, testDataPath)
}

func TestManifestEqualSeesDataPath(t *testing.T) {
	a := &Manifest{Epoch: 1, Gen: 1, Instance: "i", DataPath: testDataPath}
	b := *a
	if !manifestEqual(a, &b) {
		t.Fatal("equal manifests differ")
	}
	b.DataPath = otherDataPath
	if manifestEqual(a, &b) {
		t.Fatal("data_path change not seen")
	}
}
