package hoglite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	maxReadSessionManifestBytes = 4 << 20
	readSessionLogQueueSize     = 256
)

var (
	errReadSessionExpired             = errors.New("read session credentials expired")
	errReadSessionManifestUnavailable = errors.New("read session manifest unavailable")
)

// ReadSessionS3Config sets S3 transport options for request-local manifest reads.
// Credentials always come from the verified caller JWT.
type ReadSessionS3Config struct {
	BaseEndpoint string
	UsePathStyle bool
	HTTPClient   *http.Client
}

// ValidateReadSessionS3Config checks optional S3 settings at startup.
func ValidateReadSessionS3Config(cfg ReadSessionS3Config) error {
	if cfg.BaseEndpoint == "" {
		return nil
	}
	u, err := url.Parse(cfg.BaseEndpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("read session S3 endpoint must be an absolute https URL without a query")
	}
	return nil
}

func (s *Server) readSessionHTTPClient() *http.Client {
	if s.readSessionS3.HTTPClient != nil {
		return s.readSessionS3.HTTPClient
	}
	s.readS3Once.Do(func() {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = 32
		transport.MaxIdleConnsPerHost = 32
		s.readS3Client = &http.Client{
			Timeout:   scopedNamespaceCeiling,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	})
	return s.readS3Client
}

// readSessionManifest fetches one tenant manifest with the caller's AWS session.
// It does not cache the manifest or use the daemon's storage client.
func (s *Server) readSessionManifest(ctx context.Context, tid string, creds readSessionCredentials) (mf *Manifest, err error) {
	defer func() {
		if err != nil && s.mgr != nil && s.mgr.metrics != nil {
			s.mgr.metrics.ReadSessionManifestErrors.Add(1)
		}
	}()
	if ctx.Err() != nil {
		return nil, errReadSessionManifestUnavailable
	}
	if s.auth == nil || s.auth.readSession == nil || s.mgr.prefix != s.auth.readSession.prefix {
		return nil, errReadSessionManifestUnavailable
	}
	if ValidateReadSessionS3Config(s.readSessionS3) != nil {
		return nil, errReadSessionManifestUnavailable
	}
	// Refuse expired storage credentials before constructing a client or calling S3.
	if s.auth.now().Unix() >= creds.expiresAt {
		return nil, errReadSessionExpired
	}
	rs := s.auth.readSession
	bucket := rs.bucket
	region := rs.region
	key := manifestKey(rs.prefix, tid)
	if creds.bucket != bucket || creds.region != region || creds.key != key ||
		creds.accessKeyID == "" || creds.secretAccessKey == "" || creds.sessionToken == "" {
		return nil, errReadSessionManifestUnavailable
	}
	opts := s3.Options{
		Region: region,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			if s.auth.now().Unix() >= creds.expiresAt {
				return aws.Credentials{}, errReadSessionExpired
			}
			return aws.Credentials{
				AccessKeyID:     creds.accessKeyID,
				SecretAccessKey: creds.secretAccessKey,
				SessionToken:    creds.sessionToken,
				Source:          "caller-read-session",
				CanExpire:       true,
				Expires:         time.Unix(creds.expiresAt, 0),
			}, nil
		}),
		Retryer:      aws.NopRetryer{},
		HTTPClient:   s.readSessionHTTPClient(),
		UsePathStyle: s.readSessionS3.UsePathStyle,
	}
	if s.readSessionS3.BaseEndpoint != "" {
		opts.BaseEndpoint = aws.String(s.readSessionS3.BaseEndpoint)
	}
	cli := s3.New(opts)
	out, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if errors.Is(err, errReadSessionExpired) {
			return nil, errReadSessionExpired
		}
		return nil, errReadSessionManifestUnavailable
	}
	defer out.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(out.Body, maxReadSessionManifestBytes+1))
	if err != nil || len(raw) > maxReadSessionManifestBytes {
		return nil, errReadSessionManifestUnavailable
	}
	var parsed Manifest
	if json.Unmarshal(raw, &parsed) != nil {
		return nil, errReadSessionManifestUnavailable
	}
	if s.mgr != nil && s.mgr.metrics != nil {
		s.mgr.metrics.ReadSessionManifestReads.Add(1)
		s.logReadSessionManifestRead(tid, bucket, key, len(raw))
	}
	return &parsed, nil
}

// logReadSessionManifestRead enqueues one diagnostic log line without blocking
// the request path. Lines are dropped if the bounded queue is full.
func (s *Server) logReadSessionManifestRead(tid, bucket, key string, n int) {
	if s.mgr == nil || s.mgr.metrics == nil || s.mgr.metrics.ReadSessionLogger == nil {
		return
	}
	s.readSessionLogOnce.Do(func() {
		ch := make(chan string, readSessionLogQueueSize)
		s.readSessionLogCh = ch
		stop := s.mgr.leaseStop
		logger := s.mgr.metrics.ReadSessionLogger
		go func() {
			for {
				select {
				case line := <-ch:
					logger.Printf("%s", line)
				case <-stop:
					for {
						select {
						case line := <-ch:
							logger.Printf("%s", line)
						default:
							return
						}
					}
				}
			}
		}()
	})
	line := fmt.Sprintf(
		"read-session manifest read tenant=%s bucket=%s key=%s source=caller-read-session bytes=%d",
		tid, bucket, key, n,
	)
	select {
	case s.readSessionLogCh <- line:
	default:
	}
}
