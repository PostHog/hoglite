package hoglite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Manifest is the single coordination point for a tenant. Updated only by
// compare-and-swap on the S3 ETag.
type Manifest struct {
	Epoch        int64  `json:"epoch"`
	Gen          int64  `json:"gen"`
	Instance     string `json:"instance"`             // random per tenant incarnation; scopes all object keys
	Owner        string `json:"owner"`                // unique per process boot (fencing identity)
	OwnerAddr    string `json:"owner_addr,omitempty"` // routable address of the owner process
	OwnerUntilMs int64  `json:"owner_until_ms"`
	BaseKey      string `json:"base_key"`
	Class        string `json:"class"`
	UpdatedMs    int64  `json:"updated_ms"`
	// Format is the encoding of EVERY object of the current generation
	// (base and segments; see codec.go). The checkpoint that publishes a
	// generation sets it, and it does not change within the generation.
	// A build refuses a tenant whose Format is above maxManifestFormat
	// instead of misreading it. Absent (0) means raw objects only, as
	// written by builds before compression.
	Format int `json:"format,omitempty"`
	// BaseSize is the stored size of the base object (format 2 only). The
	// checkpoint uses it for the rebase rule, and recovery checks it.
	BaseSize int64 `json:"base_size,omitempty"`
	// Deltas is the ordered page-delta chain on top of the base (format 2
	// only, see delta.go). State at the start of the generation = base,
	// then every delta in order.
	Deltas []DeltaRef `json:"deltas,omitempty"`
}

// DeltaRef names one page delta of the chain and its stored size.
type DeltaRef struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

// Manifest formats. Bump maxManifestFormat with any change that an older
// reader would misread (new object types, new codecs).
const (
	manifestFormatRaw  = 0 // raw base image and raw WAL segments
	manifestFormatZstd = 1 // base and every segment are one zstd frame each (see codec.go)
	// base + page-delta chain + segments, every object one zstd frame, and
	// base and deltas under ckpt/ (see delta.go)
	manifestFormatChain = 2
	maxManifestFormat   = manifestFormatChain
)

// manifestObjects returns the base and delta keys a manifest names. These
// objects are live as long as the manifest is current.
func manifestObjects(m *Manifest) []string {
	out := make([]string, 0, 1+len(m.Deltas))
	if m.BaseKey != "" {
		out = append(out, m.BaseKey)
	}
	for _, d := range m.Deltas {
		out = append(out, d.Key)
	}
	return out
}

// manifestNames reports whether key is the base or a delta of m.
func manifestNames(m *Manifest, key string) bool {
	for _, k := range manifestObjects(m) {
		if k == key {
			return true
		}
	}
	return false
}

// ErrUnsupportedFormat means the tenant was written by a newer build whose
// object format this build cannot read. Refusing is the only safe answer:
// a claim would publish a base built from a misread history.
var ErrUnsupportedFormat = errors.New("tenant uses a newer storage format")

// checkManifestFormat refuses manifests from a newer format.
func checkManifestFormat(tid string, m *Manifest) error {
	return checkManifestFormatMax(tid, m, maxManifestFormat)
}

func checkManifestFormatMax(tid string, m *Manifest, max int) error {
	if m.Format > max {
		return fmt.Errorf("%w: %s has format %d, this build reads up to %d", ErrUnsupportedFormat, tid, m.Format, max)
	}
	return nil
}

type Store struct {
	cli    *s3.Client // control plane: manifest CAS (needs conditional writes)
	bucket string
	prefix string // e.g. "test1" -> keys under test1/t/<tid>/...

	// data plane: base images and WAL segments. May point at an S3 Express
	// directory bucket for lower commit latency. No conditional writes are
	// used on this plane, so Express suffices.
	dataCli    *s3.Client
	dataBucket string

	// lease plane: the pod heartbeat. Never shares a connection pool with
	// anything, so a manifest or segment storm cannot starve the fence.
	leaseCli *s3.Client
}

func NewStore(cli *s3.Client, bucket, prefix string) *Store {
	return &Store{cli: cli, bucket: bucket, prefix: strings.Trim(prefix, "/"), dataCli: cli, dataBucket: bucket, leaseCli: cli}
}

func (s *Store) WithDataPlane(cli *s3.Client, bucket string) *Store {
	s.dataCli = cli
	s.dataBucket = bucket
	return s
}

// WithLeasePlane gives the pod lease its own client (own conn pool).
func (s *Store) WithLeasePlane(cli *s3.Client) *Store {
	s.leaseCli = cli
	return s
}

func (s *Store) key(parts ...string) string {
	all := append([]string{s.prefix}, parts...)
	return strings.Join(all, "/")
}

// TenantShard returns the two-hex-digit shard for a tenant id. S3
// partitions by key range, so spreading tenants across 256 leading
// directories lets partitioning scale with load instead of pinning
// everything under one "t/" range.
func TenantShard(tid string) string {
	h := fnv.New32a()
	h.Write([]byte(tid))
	return fmt.Sprintf("%02x", h.Sum32()%256)
}

// tenantPrefix is the shared root of every object a tenant owns.
func tenantPrefix(prefix, tid string) string {
	return prefix + "/" + TenantShard(tid) + "/t/" + tid
}

// Package-level key builders (shared with Manager/Tenant which only know
// the prefix, not the concrete store). All per-tenant objects live under
// <hh>/t/<tid>/i/<instance>/ so a recreated tenant never shares key space
// with its zombie predecessor.
func manifestKey(prefix, tid string) string { return tenantPrefix(prefix, tid) + "/manifest" }

// base keys carry a per-attempt nonce: two racing acquirers pick the same
// gen+epoch but never the same object, so a loser's upload can never
// overwrite the winner's base under the shared key.
func baseKey(prefix, tid, iid string, gen, epoch int64) string {
	return fmt.Sprintf("%s/i/%s/base/%06d-%06d.db", tenantPrefix(prefix, tid), iid, gen, epoch)
}
func baseKeyAttempt(prefix, tid, iid string, gen, epoch int64, attempt string) string {
	return fmt.Sprintf("%s/i/%s/base/%06d-%06d-%s.db", tenantPrefix(prefix, tid), iid, gen, epoch, attempt)
}

// ckptKey names a base (kind "base") or a page delta (kind "delta") of a
// chain generation. They live under ckpt/, not base/: builds before
// format 2 parse the gen of base/ keys and delete every base of an older
// gen, but a chain base stays live while later gens add deltas. Those
// builds skip keys under ckpt/. attempt is the claim nonce ("" outside
// claims), as for base keys.
func ckptKey(prefix, tid, iid string, gen, epoch int64, attempt, kind string) string {
	k := fmt.Sprintf("%s/i/%s/ckpt/%06d-%06d", tenantPrefix(prefix, tid), iid, gen, epoch)
	if attempt != "" {
		k += "-" + attempt
	}
	return k + "." + kind
}
func segKey(prefix, tid, iid string, gen, epoch, offset int64) string {
	return fmt.Sprintf("%s/i/%s/wal/%06d/%06d/%016x.seg", tenantPrefix(prefix, tid), iid, gen, epoch, offset)
}
func walPrefix(prefix, tid, iid string, gen int64) string {
	return fmt.Sprintf("%s/i/%s/wal/%06d/", tenantPrefix(prefix, tid), iid, gen)
}

func (s *Store) manifestKey(tid string) string { return manifestKey(s.prefix, tid) }

// tenantObjectPrefix is the shared root for sweeping and deleting one tenant.
func (s *Store) tenantObjectPrefix(tid string) string { return tenantPrefix(s.prefix, tid) }

var ErrNotFound = errors.New("not found")
var ErrCAS = errors.New("cas failed")

// Backend abstracts durable object storage so unit tests can run against an
// in-memory implementation with S3 semantics (strong consistency, CAS via
// ETag conditions).
type Backend interface {
	GetManifest(ctx context.Context, tid string) (*Manifest, string, error)
	PutManifestCAS(ctx context.Context, tid string, m *Manifest, etag string) (string, error)
	PutSegment(ctx context.Context, key string, data []byte) error
	PutBase(ctx context.Context, key string, r io.Reader, size int64) error
	GetObject(ctx context.Context, key string) ([]byte, error)
	// GetObjectTo streams an object into w without buffering it in memory.
	// Base images can exceed pod memory (16M-file tenant = 2.3 GiB);
	// io.ReadAll on those OOMKills the pod.
	GetObjectTo(ctx context.Context, key string, w io.Writer) error
	ListSegments(ctx context.Context, tid, iid string, gen int64) ([]SegMeta, error)
	DeleteObjects(ctx context.Context, keys ...string) error
	ListPrefix(ctx context.Context, prefix string) ([]string, error)
	DeleteTenant(ctx context.Context, tid string) error
	ListTenantIDs(ctx context.Context) ([]string, error)
	Usage(ctx context.Context, prefix string) (int64, int64, error)
	PutPodLease(ctx context.Context, podid string, untilMs int64) error
	GetPodLease(ctx context.Context, podid string) (untilMs int64, found bool, err error)
	// format marker: guards against mixing layout versions on one prefix
	WriteFormatMarker(ctx context.Context, version string) error
	ReadFormatMarker(ctx context.Context) (string, bool, error)
}

func isNotFound(err error) bool {
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		c := apiErr.ErrorCode()
		return c == "NoSuchKey" || c == "NotFound" || c == "404"
	}
	return false
}

func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "ConditionalRequestConflict"
	}
	return false
}

// withRetry retries transient S3 errors (SlowDown/429/503) with backoff.
func withRetry[T any](fn func() (T, error)) (T, error) {
	var zero T
	for attempt := 0; attempt < 6; attempt++ {
		v, err := fn()
		if err == nil {
			return v, nil
		}
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) {
			return zero, err
		}
		code := apiErr.ErrorCode()
		if code != "SlowDown" && code != "ServiceUnavailable" && code != "TooManyRequestsException" && code != "503" && code != "429" {
			return zero, err
		}
		time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
	}
	return zero, errors.New("s3: retries exhausted")
}

// GetManifest returns (nil, "", nil) when the tenant does not exist.
func (s *Store) GetManifest(ctx context.Context, tid string) (*Manifest, string, error) {
	out, err := withRetry(func() (*s3.GetObjectOutput, error) {
		return s.cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket), Key: aws.String(s.manifestKey(tid)),
		})
	})
	if err != nil {
		if isNotFound(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, "", fmt.Errorf("manifest corrupt for %s: %w", tid, err)
	}
	return &m, aws.ToString(out.ETag), nil
}

// PutManifestCAS writes m. etag=="" means create-only (If-None-Match: *).
// On success it returns the ETag of the new manifest object.
func (s *Store) PutManifestCAS(ctx context.Context, tid string, m *Manifest, etag string) (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.manifestKey(tid)),
		Body: bytes.NewReader(b), ContentType: aws.String("application/json"),
	}
	if etag == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(etag)
	}
	out, err := s.cli.PutObject(ctx, in)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", ErrCAS
		}
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// podLeaseKey is one object per pod boot. Only the owning pod writes it,
// so no conditional write is needed; readers treat a stale until_ms as
// expired.
func podLeaseKey(prefix, podid string) string { return prefix + "/lease/" + podid }

type podLease struct {
	UntilMs int64 `json:"until_ms"`
}

// FormatVersion is the storage layout this build serves. Bump on any
// incompatible layout change (key schema, lease model). A pod refuses a
// prefix whose marker names a different version, so a rolling deploy
// across layouts fails loudly instead of split-braining.
const FormatVersion = "2"

func formatKey(prefix string) string { return prefix + "/FORMAT" }

// WriteFormatMarker creates the marker if absent (If-None-Match: *).
func (s *Store) WriteFormatMarker(ctx context.Context, version string) error {
	_, err := s.cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(formatKey(s.prefix)),
		Body: strings.NewReader(version), IfNoneMatch: aws.String("*"),
	})
	if err == nil {
		return nil
	}
	if !isPreconditionFailed(err) {
		return err
	}
	// lost the create race: whoever won wrote SOME version. Verify it is
	// ours, otherwise this prefix belongs to a different build.
	v, found, rerr := s.ReadFormatMarker(ctx)
	if rerr != nil {
		return rerr
	}
	if !found || v != version {
		return fmt.Errorf("format marker conflict: prefix has %q, we serve %q", v, version)
	}
	return nil
}

// ReadFormatMarker returns (version, found, err).
func (s *Store) ReadFormatMarker(ctx context.Context) (string, bool, error) {
	out, err := s.cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(formatKey(s.prefix)),
	})
	if err != nil {
		if isNotFound(err) {
			return "", false, nil
		}
		return "", false, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return "", false, err
	}
	return strings.TrimSpace(string(b)), true, nil
}

// PutPodLease heartbeats this pod's lease (control plane).
func (s *Store) PutPodLease(ctx context.Context, podid string, untilMs int64) error {
	b, _ := json.Marshal(podLease{UntilMs: untilMs})
	_, err := s.leaseCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(podLeaseKey(s.prefix, podid)),
		Body: bytes.NewReader(b), ContentType: aws.String("application/json"),
	})
	return err
}

// GetPodLease reads another pod's lease. found=false means the pod never
// heartbeated (or its lease was swept): treat as expired.
func (s *Store) GetPodLease(ctx context.Context, podid string) (int64, bool, error) {
	out, err := withRetry(func() (*s3.GetObjectOutput, error) {
		return s.cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket), Key: aws.String(podLeaseKey(s.prefix, podid)),
		})
	})
	if err != nil {
		if isNotFound(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return 0, false, err
	}
	var pl podLease
	if err := json.Unmarshal(b, &pl); err != nil {
		return 0, false, fmt.Errorf("pod lease corrupt for %s: %w", podid, err)
	}
	return pl.UntilMs, true, nil
}

// PutSegment uploads one WAL segment. Segments are immutable; no conditions.
func (s *Store) PutSegment(ctx context.Context, key string, data []byte) error {
	_, err := s.dataCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.dataBucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})
	return err
}

func (s *Store) PutBase(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.dataCli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.dataBucket), Key: aws.String(key), Body: r, ContentLength: aws.Int64(size),
	})
	return err
}

// GetObject fetches data-plane objects (base images, segments). Manifests
// are read via GetManifest on the control plane.
func (s *Store) GetObject(ctx context.Context, key string) ([]byte, error) {
	var buf bytes.Buffer
	if err := s.GetObjectTo(ctx, key, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Store) GetObjectTo(ctx context.Context, key string, w io.Writer) error {
	out, err := withRetry(func() (*s3.GetObjectOutput, error) {
		return s.dataCli.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.dataBucket), Key: aws.String(key)})
	})
	if err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return err
	}
	defer out.Body.Close()
	_, err = io.Copy(w, out.Body)
	return err
}

type SegMeta struct {
	Key    string
	Epoch  int64
	Offset int64 // byte offset of the segment in the RAW WAL
	// Size is the STORED object size from LIST. For a compressed segment it
	// is not the raw WAL length: never use it for contiguity. Recovery uses
	// the decoded length instead.
	Size int64
}

// ListSegments returns all segments of one WAL incarnation, sorted by offset.
func (s *Store) ListSegments(ctx context.Context, tid, iid string, gen int64) ([]SegMeta, error) {
	prefix := walPrefix(s.prefix, tid, iid, gen)
	var out []SegMeta
	var tok *string
	for {
		resp, err := withRetry(func() (*s3.ListObjectsV2Output, error) {
			return s.dataCli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(s.dataBucket), Prefix: aws.String(prefix), ContinuationToken: tok,
			})
		})
		if err != nil {
			return nil, err
		}
		for _, o := range resp.Contents {
			// .../<prefix>/t/<tid>/wal/<gen>/<epoch>/<offset>.seg
			parts := strings.Split(aws.ToString(o.Key), "/")
			if len(parts) < 3 {
				continue
			}
			epoch, err1 := strconv.ParseInt(parts[len(parts)-2], 10, 64)
			offStr := strings.TrimSuffix(parts[len(parts)-1], ".seg")
			off, err2 := strconv.ParseInt(offStr, 16, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			out = append(out, SegMeta{Key: aws.ToString(o.Key), Epoch: epoch, Offset: off, Size: aws.ToInt64(o.Size)})
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		tok = resp.NextContinuationToken
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Offset != out[j].Offset {
			return out[i].Offset < out[j].Offset
		}
		return out[i].Epoch > out[j].Epoch // duplicate offset: newer epoch wins the sort
	})
	return out, nil
}

func (s *Store) DeleteObjects(ctx context.Context, keys ...string) error {
	for start := 0; start < len(keys); start += 1000 { // S3 limit per call
		end := min(start+1000, len(keys))
		var ids []s3types.ObjectIdentifier
		for _, k := range keys[start:end] {
			ids = append(ids, s3types.ObjectIdentifier{Key: aws.String(k)})
		}
		if _, err := s.dataCli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.dataBucket), Delete: &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)},
		}); err != nil {
			return err
		}
	}
	return nil
}

// ListTenantIDs lists tenant ids under the prefix (test/harness helper).
// ListTenantIDs lists tenant ids under the prefix (test/harness helper).
// Tenants live under 256 shard directories, so this walks them all.
func (s *Store) ListTenantIDs(ctx context.Context) ([]string, error) {
	var ids []string
	for shard := 0; shard < 256; shard++ {
		prefix := fmt.Sprintf("%s/%02x/t/", s.prefix, shard)
		var tok *string
		for {
			resp, err := s.cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
				Delimiter: aws.String("/"), ContinuationToken: tok,
			})
			if err != nil {
				return nil, err
			}
			for _, p := range resp.CommonPrefixes {
				id := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(p.Prefix), prefix), "/")
				ids = append(ids, id)
			}
			if !aws.ToBool(resp.IsTruncated) {
				break
			}
			tok = resp.NextContinuationToken
		}
	}
	return ids, nil
}

// ListPrefix returns all keys under a full key prefix (test/ops + sweeper).
func (s *Store) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	var tok *string
	for {
		resp, err := s.dataCli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.dataBucket), Prefix: aws.String(prefix), ContinuationToken: tok,
		})
		if err != nil {
			return nil, err
		}
		for _, o := range resp.Contents {
			out = append(out, aws.ToString(o.Key))
		}
		if !aws.ToBool(resp.IsTruncated) {
			return out, nil
		}
		tok = resp.NextContinuationToken
	}
}

// DeleteTenant removes every object under the tenant prefix (test/ops).
// In split mode both planes are emptied.
func (s *Store) DeleteTenant(ctx context.Context, tid string) error {
	prefix := s.tenantObjectPrefix(tid) + "/"
	type plane struct {
		cli    *s3.Client
		bucket string
	}
	planes := []plane{{s.cli, s.bucket}}
	if s.dataBucket != s.bucket {
		planes = append(planes, plane{s.dataCli, s.dataBucket})
	}
	for _, pl := range planes {
		var tok *string
		for {
			resp, err := pl.cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(pl.bucket), Prefix: aws.String(prefix), ContinuationToken: tok,
			})
			if err != nil {
				return err
			}
			var ids []s3types.ObjectIdentifier
			for _, o := range resp.Contents {
				ids = append(ids, s3types.ObjectIdentifier{Key: o.Key})
			}
			if len(ids) > 0 {
				if _, err := pl.cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(pl.bucket), Delete: &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)},
				}); err != nil {
					return err
				}
			}
			if !aws.ToBool(resp.IsTruncated) {
				break
			}
			tok = resp.NextContinuationToken
		}
	}
	return nil
}

// Usage returns object count and total bytes under a prefix (harness helper).
func (s *Store) Usage(ctx context.Context, prefix string) (int64, int64, error) {
	p := s.key(prefix)
	var n, bytes int64
	var tok *string
	for {
		resp, err := s.cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket), Prefix: aws.String(p), ContinuationToken: tok,
		})
		if err != nil {
			return 0, 0, err
		}
		n += int64(len(resp.Contents))
		for _, o := range resp.Contents {
			bytes += aws.ToInt64(o.Size)
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		tok = resp.NextContinuationToken
	}
	return n, bytes, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
