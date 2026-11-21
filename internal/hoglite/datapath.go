package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// The tenant data_path (Manifest.DataPath).
//
// A hoglake catalog's S3 prefix lives in the catalog's own `meta` row, which
// any write-scoped caller can rewrite with raw SQL. hoglake must never let
// that row decide where it deletes or writes under its S3 identity, so the
// create of the tenant stores the prefix here too, in the manifest: only
// hoglite writes the manifest, by CAS, and only an operator can create a
// tenant. Every tenant-scoped response then repeats the stored value in the
// X-Hoglite-Data-Path header, from the owner's in-memory copy, so a client
// gets the trusted value with the response it already waits for.
//
// The value is immutable: a create of an existing tenant that names a
// different data_path (a different value, or a value where the tenant has
// none) answers 409 data_path_mismatch. A create that omits the field makes
// no claim and keeps the current idempotent answer. A tenant that must move
// is deleted and created again.
//
// The manifest marks the field with manifestFlagDataPath in Format
// (store.go), so a build from before the field refuses the tenant instead
// of erasing the field on its next CAS.
//
// A data_path is refused when it overlaps hoglite's own storage (400) or
// the data_path of another tenant of this backend (409 data_path_overlap):
// hoglake's cleanup deletes under a catalog's data_path, and two catalogs
// on one prefix let one drain delete the other's objects (hoglake #666).

// DataPathHeader carries Manifest.DataPath on every tenant-scoped response
// of a tenant that has one. No header means the tenant has no stored
// data_path (created before the field existed), never an empty one.
const DataPathHeader = "X-Hoglite-Data-Path"

// MaxDataPathLength bounds the stored value. It matches hoglake's catalog
// bound, so a data_path hoglake accepts is one hoglite accepts.
const MaxDataPathLength = 512

// Codes of the create refusals that concern the data_path.
const (
	// CodeInvalidDataPath: the data_path in the body is not an
	// s3://<bucket>[/<prefix>] URI by the rules of validDataPath (400).
	CodeInvalidDataPath = "invalid_data_path"
	// CodeDataPathMismatch: the tenant exists with a different data_path,
	// or with none, and the body names one (409).
	CodeDataPathMismatch = "data_path_mismatch"
	// CodeDataPathOverlap: another tenant of this backend has a data_path
	// equal to, above or below the one in the body (409).
	CodeDataPathOverlap = "data_path_overlap"
)

// createRequest is the decoded body of PUT /v1/tenants/{tid}. DataPath is ""
// when the field is absent, which makes no claim.
type createRequest struct {
	Class    string
	DataPath string
}

// parseCreateBody decodes a create body: empty (whitespace only), or
// exactly one JSON object with nothing after it. Anything else is a 400: a
// body that is not an object (`null`, an array, a string), a second value
// or trailing text after the object (`{...} garbage`, `{...}{...}`), a
// class that is not a string, a data_path that is present but not a
// non-empty string (null included), or a key other than class and data_path
// (a misspelled `dataPath` would otherwise make no claim, silently). Key
// matching follows hoglake's JSON reading (Jackson defaults on the JVM
// server), not encoding/json's: names are matched exactly, so `Class` or
// `DATA_PATH` is an unknown key and a 400, never folded onto a known one;
// a repeated key takes its last value, as Jackson does, so the two sides
// read one body the same way. A
// decode error used to be ignored, so `"data_path": 7` or `""` created a
// tenant without a data_path, which can never gain one; and json.Decoder,
// left to itself, reads a `null` body as an object with no fields and stops
// at the first value, so a body with a typo after the object would have
// made a claim its sender never saw checked. The data_path's shape
// (validDataPath) is the caller's check.
func parseCreateBody(raw []byte) (createRequest, int, apiError) {
	var body createRequest
	if len(bytes.TrimSpace(raw)) == 0 {
		return body, 0, apiError{}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	var obj json.RawMessage
	if err := dec.Decode(&obj); err != nil {
		return body, 400, apiError{Error: "create body: " + err.Error()}
	}
	if obj = bytes.TrimSpace(obj); len(obj) == 0 || obj[0] != '{' {
		return body, 400, apiError{Error: "create body must be a JSON object"}
	}
	if _, err := dec.Token(); err != io.EOF {
		return body, 400, apiError{Error: "create body must be one JSON object with nothing after it"}
	}
	// A map decodes keys exactly (encoding/json folds case only into struct
	// fields) and keeps the last of a repeated key.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(obj, &fields); err != nil {
		return body, 400, apiError{Error: "create body: " + err.Error()}
	}
	for key := range fields {
		if key != "class" && key != "data_path" {
			return body, 400, apiError{Error: fmt.Sprintf("create body: unknown field %q", key)}
		}
	}
	if raw, ok := fields["class"]; ok {
		if err := json.Unmarshal(raw, &body.Class); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return body, 400, apiError{Error: "create body: class must be a string"}
		}
	}
	if raw, ok := fields["data_path"]; ok {
		if err := json.Unmarshal(raw, &body.DataPath); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return body, 400, apiError{Error: "data_path must be a string", Code: CodeInvalidDataPath}
		}
		if body.DataPath == "" {
			return body, 400, apiError{Error: "data_path must not be empty; omit it to make no claim", Code: CodeInvalidDataPath}
		}
	}
	return body, 0, apiError{}
}

// dataPathDir is a data_path as the directory it names: one trailing
// slash, so a prefix test cannot match "s3://b/ab" against "s3://b/a".
func dataPathDir(p string) string { return strings.TrimRight(p, "/") + "/" }

// dataPathsOverlap reports whether two data_paths name the same prefix or
// one lies under the other, by hoglake's rule (CatalogService.createCatalog):
// trailing slashes aside, either is a prefix of the other.
func dataPathsOverlap(a, b string) bool {
	da, db := dataPathDir(a), dataPathDir(b)
	return strings.HasPrefix(da, db) || strings.HasPrefix(db, da)
}

// reservedDataPath refuses a data_path that overlaps hoglite's own storage
// (Backend.StorageRoots: the control plane and, when separate, the data
// plane). A catalog there could register hoglite's manifests and objects
// as its own files and have hoglake delete them; a data_path above the
// root contains them all.
func reservedDataPath(p string, roots []string) error {
	for _, root := range roots {
		if dataPathsOverlap(p, root) {
			return fmt.Errorf("data_path %q overlaps hoglite's own storage %q", p, root)
		}
	}
	return nil
}

// overlapScanWidth bounds the manifest reads in flight during one overlap
// scan.
const overlapScanWidth = 16

// otherDataPathOverlap finds the tenant of this backend, other than tid,
// whose data_path overlaps p; the lowest tenant id when several do. It
// reads every other tenant's manifest: hoglite keeps no index of
// data_paths, and the manifests are the only place the values live. The
// cost is one listing (256 shard LISTs on S3, listShardWidth at a time,
// see Store.ListTenantIDs: well under a second even on an empty backend)
// plus one GET per tenant, overlapScanWidth at a time, paid once per
// create that names a data_path (not by an idempotent recreate of an
// existing tenant, which handleCreate answers before the scan). At 10k
// tenants that is several seconds against S3; a create is an operator
// action, and the alternative is a catalog that can delete another's data.
//
// Two concurrent creates of different tenants with overlapping paths can
// both scan before either manifest exists, and both succeed: hoglite has
// no lock across tenants. The window is the length of a create; hoglake
// serializes catalog creates for this in PostgreSQL and should do the same
// in front of this backend.
func otherDataPathOverlap(ctx context.Context, store Backend, tid, p string) (otherTid, otherPath string, err error) {
	tids, err := store.ListTenantIDs(ctx)
	if err != nil {
		return "", "", err
	}
	sort.Strings(tids)
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		slot = make(chan struct{}, overlapScanWidth)
	)
	for _, other := range tids {
		if other == tid || other == sweeperLockTenant {
			continue
		}
		slot <- struct{}{} // every goroutine releases its slot, so this returns
		mu.Lock()
		stop := err != nil || otherTid != "" && otherTid < other
		mu.Unlock()
		if stop {
			<-slot
			break // a lower hit or a failure already decides the answer
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slot }()
			mf, _, gerr := store.GetManifest(ctx, other)
			mu.Lock()
			defer mu.Unlock()
			if gerr != nil {
				if err == nil && ctx.Err() == nil {
					err = fmt.Errorf("read manifest of %s: %w", other, gerr)
					cancel()
				}
				return
			}
			if mf == nil || mf.DataPath == "" || !dataPathsOverlap(p, mf.DataPath) {
				return
			}
			if otherTid == "" || other < otherTid {
				otherTid, otherPath = other, mf.DataPath
			}
		}()
	}
	wg.Wait()
	if err == nil {
		// a read that failed because the caller's context ended records
		// no error above (ctx.Err() was set): an unfinished scan must not
		// read as "no overlap"
		err = parent.Err()
	}
	if err != nil {
		return "", "", err
	}
	return otherTid, otherPath, nil
}

// dataPathOverlap is the 409 of a create whose data_path another tenant
// owns. It names that tenant and its path: only an operator can create a
// tenant, and an operator reads every manifest through /status anyway.
func dataPathOverlap(tid, want, otherTid, otherPath string) (int, any) {
	return http.StatusConflict, apiError{
		Error: fmt.Sprintf("tenant %s cannot take data_path %q: it overlaps tenant %s (data_path %q); tenant data_paths must be disjoint", tid, want, otherTid, otherPath),
		Code:  CodeDataPathOverlap,
	}
}

// validDataPath checks the shape of a data_path. The rules are hoglake's
// (CatalogService.validateDataPath), so the two sides agree on what a
// data_path is: `s3://<bucket>[/<prefix>]`, non-empty bucket, at most
// MaxDataPathLength bytes, no whitespace or control characters, no `.` or
// `..` segment. A bucket root (`s3://bucket`) is legal. The value is stored
// as given: no normalization, so the header repeats what the create sent.
func validDataPath(p string) error {
	if p == "" {
		return errors.New("data_path must not be empty")
	}
	if len(p) > MaxDataPathLength {
		return fmt.Errorf("data_path is %d bytes, over the maximum %d", len(p), MaxDataPathLength)
	}
	if !utf8.ValidString(p) {
		return errors.New("data_path must be valid UTF-8")
	}
	for _, r := range p {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return errors.New("data_path must not contain whitespace or control characters")
		}
	}
	rest, ok := strings.CutPrefix(p, "s3://")
	if !ok {
		return errors.New("data_path must be an s3://<bucket>[/<prefix>] URI")
	}
	bucket, _, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return errors.New("data_path must be s3://<bucket>[/<prefix>] with a non-empty bucket")
	}
	for _, seg := range strings.Split(rest, "/") {
		if seg == "." || seg == ".." {
			return errors.New("data_path must not contain '.' or '..' segments")
		}
	}
	return nil
}

// dataPathConflict reports whether a create that names want may answer
// for a tenant whose stored data_path is have. An omitted want ("") makes
// no claim.
func dataPathConflict(have, want string) bool {
	return want != "" && have != want
}

// dataPathMismatch is the 409 of a create that names a data_path the tenant
// does not have.
func dataPathMismatch(tid, have, want string) (int, any) {
	msg := fmt.Sprintf("tenant %s has data_path %q, not %q; the data_path of a tenant never changes", tid, have, want)
	if have == "" {
		msg = fmt.Sprintf("tenant %s has no data_path and cannot take %q; the data_path of a tenant never changes", tid, want)
	}
	return http.StatusConflict, apiError{Error: msg, Code: CodeDataPathMismatch}
}

// setDataPath puts the stored data_path on a tenant-scoped response. An
// empty value sets nothing: the header's absence IS "no stored data_path".
func setDataPath(w http.ResponseWriter, dataPath string) {
	if dataPath != "" {
		w.Header().Set(DataPathHeader, dataPath)
	}
}

// withDataPath adds data_path to a response body when the tenant has one.
func withDataPath(body map[string]any, dataPath string) map[string]any {
	if dataPath != "" {
		body["data_path"] = dataPath
	}
	return body
}
