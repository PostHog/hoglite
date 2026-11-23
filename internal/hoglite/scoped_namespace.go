package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	scopedNamespaceTargetLimit   = 4 << 10
	scopedNamespaceTokenLimit    = 64 << 10
	scopedNamespaceResponseLimit = 4 << 10
	scopedNamespaceCeiling       = 5 * time.Second
)

type scopedNamespaceTarget struct {
	tenant, catalog, namespace string
	path                       string
}

type scopedNamespaceDTO struct {
	ID          int64                      `json:"namespace_id"`
	Name        string                     `json:"name"`
	Containment scopedNamespaceContainment `json:"containment"`
}

type scopedNamespaceContainment struct {
	Field string `json:"field"`
	Value int64  `json:"value"`
}

// Dispatch before ServeMux can clean an ambiguous target or redirect it.
func (s *Server) scopedNamespaceRoutes(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.URL.Path, "/", 5)
		if len(parts) == 5 && parts[0] == "" && parts[1] == "v1" && parts[2] == "t" {
			for _, segment := range strings.Split(parts[4], "/") {
				if segment == "" || segment == "." {
					continue // These aliases must reach strict path validation.
				}
				if segment == "metadata" {
					s.serveScopedNamespace(w, r)
					return
				}
				break
			}
		}
		next.ServeHTTP(w, r)
	})
}

func parseScopedNamespaceTarget(r *http.Request) (scopedNamespaceTarget, bool) {
	target, ok := parseScopedMetadataTarget(r)
	if !ok || target.kind != scopedMetadataKindNamespace {
		return scopedNamespaceTarget{}, false
	}
	return target.namespaceTarget(), true
}

func decodeScopedSegment(raw string) (string, bool) {
	v, err := url.PathUnescape(raw)
	if err != nil || v == "" || v == "." || v == ".." ||
		!utf8.ValidString(v) || strings.ContainsAny(v, "/\\%") ||
		strings.IndexFunc(v, unicode.IsControl) >= 0 ||
		scopedNamespaceSegment(v) != raw {
		return "", false
	}
	return v, true
}

func scopedNamespaceSegment(value string) string {
	const hex = "0123456789ABCDEF"
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		if 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z' || '0' <= b && b <= '9' || strings.ContainsRune("-._~", rune(b)) {
			out.WriteByte(b)
			continue
		}
		out.WriteByte('%')
		out.WriteByte(hex[b>>4])
		out.WriteByte(hex[b&15])
	}
	return out.String()
}

func scopedMetadataReply(w http.ResponseWriter, status int, body any, limit int) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	b, err := json.Marshal(body)
	if err != nil || len(b) > limit {
		status, b = http.StatusServiceUnavailable, []byte(`{"code":"unavailable"}`)
	}
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func scopedNamespaceReply(w http.ResponseWriter, status int, body any) {
	scopedMetadataReply(w, status, body, scopedNamespaceResponseLimit)
}

func scopedNamespaceRefusal(w http.ResponseWriter, status int) {
	code := "unavailable"
	switch status {
	case http.StatusBadRequest:
		code = "invalid_request"
	case http.StatusUnauthorized:
		code = "unauthorized"
	case http.StatusForbidden:
		code = "forbidden"
	case http.StatusNotFound:
		code = "not_found"
	}
	scopedNamespaceReply(w, status, map[string]string{"code": code})
}

func (s *Server) serveScopedNamespace(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), scopedNamespaceCeiling)
	defer cancel()
	r = r.WithContext(ctx)
	target, ok := parseScopedMetadataTarget(r)
	if !ok {
		scopedNamespaceRefusal(w, http.StatusBadRequest)
		return
	}
	if target.kind != scopedMetadataKindNamespace && !s.auth.readSessionEnabled() {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		scopedNamespaceRefusal(w, http.StatusUnauthorized)
		return
	}
	fields := strings.Fields(values[0])
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") ||
		len(fields[1]) > scopedNamespaceTokenLimit {
		scopedNamespaceRefusal(w, http.StatusUnauthorized)
		return
	}
	if s.auth.readSessionEnabled() {
		rsa, err := s.auth.verifyReadSession(ctx, fields[1], target.tenant, target.catalog)
		if err != nil {
			status := http.StatusUnauthorized
			if ctx.Err() != nil {
				status = http.StatusServiceUnavailable
			} else if errors.Is(err, errUnsupportedAuthority) {
				status = http.StatusNotFound
			}
			scopedNamespaceRefusal(w, status)
			return
		}
		if ctx.Err() != nil {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		t := s.mgr.LocalTenant(target.tenant)
		if t == nil {
			// Serve only an already loaded tenant on this owner. Never discover a peer or acquire.
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
			return
		}
		getManifest := func(readCtx context.Context) (*Manifest, error) {
			return s.readSessionManifest(readCtx, target.tenant, rsa.creds)
		}
		switch target.kind {
		case scopedMetadataKindNamespace:
			s.readScopedNamespace(w, r, target.namespaceTarget(), rsa.authority, t, getManifest)
		case scopedMetadataKindTable:
			s.readScopedTable(w, r, target, rsa, t, getManifest)
		case scopedMetadataKindScan:
			s.readScopedScan(w, r, target, rsa, t, getManifest)
		default:
			scopedNamespaceRefusal(w, http.StatusBadRequest)
		}
		return
	}
	nsTarget := target.namespaceTarget()
	a, err := s.auth.verifyScopedNamespace(ctx, fields[1], nsTarget.tenant, nsTarget.catalog)
	if err != nil {
		status := http.StatusUnauthorized
		if ctx.Err() != nil {
			status = http.StatusServiceUnavailable
		} else if errors.Is(err, errUnsupportedAuthority) {
			status = http.StatusNotFound
		}
		scopedNamespaceRefusal(w, status)
		return
	}
	if ctx.Err() != nil {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	if t := s.mgr.LocalTenant(nsTarget.tenant); t != nil {
		getManifest := func(readCtx context.Context) (*Manifest, error) {
			mf, _, err := s.mgr.store.GetManifest(readCtx, nsTarget.tenant)
			return mf, err
		}
		s.readScopedNamespace(w, r, nsTarget, a, t, getManifest)
		return
	}
	s.forwardScopedNamespace(w, r, nsTarget, a, values[0])
}

// Use the immutable claim identity, not a cached owner or manifest generation.
func (s *Server) scopedNamespaceOwned(ctx context.Context, tid string, t *Tenant, getManifest func(context.Context) (*Manifest, error)) int {
	if ctx.Err() != nil || !t.claimed || t.self != s.mgr.ID() ||
		!t.Alive() || t.leaseExpiring() || s.mgr.LocalTenant(tid) != t {
		return http.StatusServiceUnavailable
	}
	mf, err := getManifest(ctx)
	if err != nil {
		if errors.Is(err, errReadSessionExpired) {
			return http.StatusUnauthorized
		}
		return http.StatusServiceUnavailable
	}
	if mf != nil && mf.Owner == t.self &&
		mf.Epoch == t.claimEpoch && mf.Instance == t.claimInst &&
		ctx.Err() == nil && t.Alive() && !t.leaseExpiring() &&
		s.mgr.LocalTenant(tid) == t {
		return 0
	}
	return http.StatusServiceUnavailable
}

// The fixed join reads the catalog, live row, and mark in one durable view.
const scopedNamespaceSQL = `SELECT n.id, n.name, n.contained_field, n.contained_value
FROM meta AS m JOIN namespaces AS n ON 1 = 1
WHERE m.id = 1 AND m.name = ? COLLATE BINARY AND m.dropping = 0
AND n.name = ? COLLATE BINARY AND n.dropped_snapshot IS NULL
LIMIT 2`

func (s *Server) readScopedNamespace(w http.ResponseWriter, r *http.Request, target scopedNamespaceTarget, a *scopedNamespaceAuthority, t *Tenant, getManifest func(context.Context) (*Manifest, error)) {
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	result, err := t.SubmitQueryCtx(r.Context(), Statement{
		SQL: scopedNamespaceSQL, Args: []any{target.catalog, target.namespace},
	})
	if status := s.scopedNamespaceOwned(r.Context(), target.tenant, t, getManifest); status != 0 {
		scopedNamespaceRefusal(w, status)
		return
	}
	if err != nil {
		// A cold schema supplies no authority. Never initialize it here.
		if strings.Contains(err.Error(), "no such table: meta") ||
			strings.Contains(err.Error(), "no such table: namespaces") {
			scopedNamespaceRefusal(w, http.StatusNotFound)
		} else {
			scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		}
		return
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 4 {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	row := result.Rows[0]
	id, idOK := row[0].(int64)
	name, nameOK := row[1].(string)
	field, fieldOK := row[2].(string)
	value, valueOK := row[3].(int64)
	if !idOK || !nameOK || !fieldOK || !valueOK || id <= 0 ||
		name != target.namespace || field == "" || value <= 0 || !a.permits(field, value) {
		scopedNamespaceRefusal(w, http.StatusNotFound)
		return
	}
	// Construct the public DTO only after the mark grants access.
	scopedNamespaceReply(w, http.StatusOK, scopedNamespaceDTO{
		ID: id, Name: name, Containment: scopedNamespaceContainment{Field: field, Value: value},
	})
}

func (s *Server) forwardScopedNamespace(w http.ResponseWriter, r *http.Request, target scopedNamespaceTarget, a *scopedNamespaceAuthority, bearer string) {
	if r.Header.Get("x-hw-noproxy") != "" {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	mf, _, err := s.mgr.store.GetManifest(r.Context(), target.tenant)
	if err != nil || mf == nil || mf.Owner == "" || mf.Owner == s.mgr.ID() ||
		(mf.Class != "" && mf.Class != s.mgr.class) {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	base, ok := s.peers[ownerAddr(mf)]
	if !ok {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	live, _, err := s.mgr.ownerLeaseLive(r.Context(), mf.Owner)
	if err != nil || !live {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	u, err := url.Parse(base)
	if err != nil || !absoluteHTTPURL(base) || u.RawQuery != "" ||
		(u.Path != "" && u.Path != "/") {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, strings.TrimSuffix(base, "/")+target.path, nil)
	if err != nil {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	req.Header.Set("Authorization", bearer)
	req.Header.Set("x-hw-noproxy", "1")
	req.Header.Set("User-Agent", "")
	client := *s.client
	client.Timeout = scopedNamespaceCeiling
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if client.Transport == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.DisableKeepAlives = true
		transport.DisableCompression = true
		transport.ForceAttemptHTTP2 = false
		client.Transport = transport
	}
	resp, err := client.Do(req)
	if err != nil {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, scopedNamespaceResponseLimit+1))
	media, _, mediaErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || len(raw) > scopedNamespaceResponseLimit ||
		r.Context().Err() != nil || mediaErr != nil || media != "application/json" ||
		strictJSON(raw) != nil {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusUnauthorized {
		var refusal map[string]string
		expected := "not_found"
		if resp.StatusCode == http.StatusUnauthorized {
			expected = "unauthorized"
		}
		if json.Unmarshal(raw, &refusal) == nil && len(refusal) == 1 && refusal["code"] == expected {
			scopedNamespaceRefusal(w, resp.StatusCode)
			return
		}
	}
	if resp.StatusCode != http.StatusOK {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	dto, ok := decodeScopedNamespaceDTO(raw, target.namespace, a)
	if !ok {
		scopedNamespaceRefusal(w, http.StatusServiceUnavailable)
		return
	}
	scopedNamespaceReply(w, http.StatusOK, dto)
}

func decodeScopedNamespaceDTO(raw []byte, namespace string, a *scopedNamespaceAuthority) (scopedNamespaceDTO, bool) {
	var dto scopedNamespaceDTO
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || len(object) != 3 ||
		object["namespace_id"] == nil || object["name"] == nil || object["containment"] == nil {
		return dto, false
	}
	var mark map[string]json.RawMessage
	if json.Unmarshal(object["containment"], &mark) != nil || len(mark) != 2 ||
		mark["field"] == nil || mark["value"] == nil {
		return dto, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&dto) != nil || dto.ID <= 0 || dto.Name != namespace ||
		dto.Containment.Field == "" || dto.Containment.Value <= 0 ||
		!a.permits(dto.Containment.Field, dto.Containment.Value) {
		return scopedNamespaceDTO{}, false
	}
	return dto, true
}
