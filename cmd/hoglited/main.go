package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"hoglite/internal/hoglite"
)

func main() {
	var (
		id          = flag.String("id", envOr("HL_ID", defaultID()), "replica id (default: host-pid-random)")
		addr        = flag.String("addr", envOr("HL_ADDR", ":9101"), "listen address")
		class       = flag.String("class", envOr("HL_CLASS", "standard"), "replica class")
		dir         = flag.String("dir", envOr("HL_DIR", "/tmp/hoglite-"+envOr("HL_ID", "r1")), "local cache dir")
		bucket      = flag.String("bucket", envOr("HL_BUCKET", ""), "S3 bucket")
		prefix      = flag.String("prefix", envOr("HL_PREFIX", "hl"), "S3 key prefix")
		dataBkt     = flag.String("data-bucket", envOr("HL_DATA_BUCKET", ""), "separate data-plane bucket (e.g. S3 Express); default: same as -bucket")
		peersFl     = flag.String("peers", envOr("HL_PEERS", ""), "id=url,id=url")
		maxOpen     = flag.Int("max-open", 5000, "max simultaneously open tenants")
		leaseMs     = flag.Int("lease-ms", 12000, "pod lease length (crash failover bound)")
		hbMs        = flag.Int("heartbeat-ms", 1000, "pod lease heartbeat interval")
		batchMs     = flag.Int("batch-window-ms", 5, "group commit window")
		whaleRate   = flag.Float64("whale-rate", 0, "commits/s sustained that promotes a tenant to whale class (0 = off; enable only when a whale pool exists)")
		sweep       = flag.Bool("sweep", true, "run the orphan-object sweeper (self-elected across replicas)")
		forceFormat = flag.Bool("force-format", false, "adopt a pre-marker prefix as the current format (dangerous: old-layout tenants are invisible)")
		satCommitMs = flag.Float64("sat-commit-ms", 500, "commit-latency budget in ms for the saturation metric (1.0 = at limit)")
		satInflight = flag.Float64("sat-inflight-cap", 128, "in-flight segment cap for the saturation metric")
		compress    = flag.Bool("compress", envOr("HL_COMPRESS", "true") != "false", "publish new generations zstd-compressed (base and every WAL segment; applies from the next checkpoint per tenant; reads accept both). Pods of a build without compression support cannot take over a compressed tenant: set false while such pods may still run, then enable in a second rollout")
		deltaCkpt   = flag.Bool("delta-checkpoints", envOr("HL_DELTA_CHECKPOINTS", "true") != "false", "checkpoints upload page deltas instead of full bases (needs -compress; applies from the next checkpoint per tenant; reads accept both). Pods of a build without delta support cannot take over such a tenant: set false while such pods may still run, then enable in a second rollout")
	)
	flag.Parse()
	if *bucket == "" {
		log.Fatal("-bucket / HL_BUCKET required")
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// standard retry mode: adaptive mode's client-side rate limiter blocks
	// unrelated tenants on one SlowDown, and it never refills its retry
	// quota on success (upstream bug aws-sdk-go-v2#3550). hoglite wraps
	// every S3 call in its own deadline and in-order ack pipeline, so the
	// SDK retry quota only turns long S3 slowness into permanent-looking
	// errors. Disable it; the enclosing deadlines bound every attempt.
	cfg.RetryMaxAttempts = 10
	cfg.Retryer = func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 10
			o.RateLimiter = ratelimit.None
		})
	}
	// two clients, so a burst of WAL-segment PUTs cannot starve manifest
	// CAS and heartbeats. Both share one bucket; isolation comes from the
	// separate retryers and HTTP pools.
	// two clients, so a burst of WAL-segment PUTs cannot starve manifest
	// CAS and heartbeats. Both share one bucket; isolation comes from the
	// separate retryers and HTTP pools. BuildableClient keeps the SDK's
	// timeouts and TLS/proxy defaults; only the pool sizes differ.
	ctrlCli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
			t.MaxIdleConns = 64
			t.MaxIdleConnsPerHost = 64
		})
	})
	dataCli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
			t.MaxIdleConns = 256
			t.MaxIdleConnsPerHost = 256
		})
	})
	// the lease heartbeat gets its own client: a manifest or segment storm
	// must never starve the fence
	leaseCli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
			t.MaxIdleConns = 8
			t.MaxIdleConnsPerHost = 8
		})
	})
	store := hoglite.NewStore(ctrlCli, *bucket, *prefix)
	store = store.WithDataPlane(dataCli, *bucket)
	store = store.WithLeasePlane(leaseCli)
	if *dataBkt != "" && *dataBkt != *bucket {
		store = store.WithDataPlane(dataCli, *dataBkt)
	}

	// storage-format gate: never serve a prefix whose layout belongs to a
	// different build. A rolling deploy across formats fails fast here
	// instead of split-braining (old pods write t/, new pods write <hh>/t/).
	v, found, err := store.ReadFormatMarker(ctx)
	if err != nil {
		log.Fatalf("format marker: %v", err)
	}
	if !found {
		// no marker: either a fresh prefix or one written by a pre-marker
		// build. Refuse to claim a prefix that has old-layout data unless
		// the operator explicitly overrides. Check BOTH layouts: sharded
		// (<hh>/t/) and pre-shard (t/).
		ids, lerr := store.ListTenantIDs(ctx)
		if lerr != nil {
			log.Fatalf("format marker: list tenants: %v", lerr)
		}
		oldKeys, lerr := store.ListPrefix(ctx, strings.Trim(*prefix, "/")+"/t/")
		if lerr != nil {
			log.Fatalf("format marker: list old layout: %v", lerr)
		}
		if (len(ids) > 0 || len(oldKeys) > 0) && !*forceFormat {
			log.Fatalf("prefix %q has data (%d tenants, %d old-layout keys) but no FORMAT marker. Use a fresh prefix, or -force-format to adopt it as format %s", *prefix, len(ids), len(oldKeys), hoglite.FormatVersion)
		}
		if err := store.WriteFormatMarker(ctx, hoglite.FormatVersion); err != nil {
			log.Fatalf("write format marker: %v", err)
		}
	} else if v != hoglite.FormatVersion {
		log.Fatalf("prefix %q is format %s, this build serves %s: deploy with a fresh prefix or migrate", *prefix, v, hoglite.FormatVersion)
	}

	opts := hoglite.DefaultOpts()
	opts.Lease = time.Duration(*leaseMs) * time.Millisecond
	opts.HeartbeatEvery = time.Duration(*hbMs) * time.Millisecond
	if opts.HeartbeatEvery <= 0 {
		log.Fatal("-heartbeat-ms must be > 0")
	}
	if opts.HeartbeatEvery*3 >= opts.Lease {
		log.Fatalf("-heartbeat-ms (%v) must be well under -lease-ms (%v): the gate margin is lease/3", opts.HeartbeatEvery, opts.Lease)
	}
	opts.BatchWindow = time.Duration(*batchMs) * time.Millisecond
	opts.Compress = *compress
	if !*deltaCkpt {
		opts.DeltaMaxChain = 0
	}
	ctrl := hoglite.ControllerOpts{Enabled: *whaleRate > 0, ScanEvery: 15 * time.Second,
		WhaleThreshold: *whaleRate, SustainedScans: 2, Dwell: 5 * time.Minute}
	// fencing arithmetic: the last PUT an owner can start is at
	// owner_until - lease/3; it completes by +PutTimeout. An acquirer lists
	// segments at owner_until + SkewMargin. Require the completion to
	// precede the list, or late writes could land in a dead gen AFTER the
	// new owner already replayed it.
	if opts.PutTimeout >= opts.Lease/3+opts.SkewMargin {
		log.Printf("WARNING: fencing margin violated: PutTimeout %v must be < lease/3 + skew %v", opts.PutTimeout, opts.SkewMargin)
	}
	metrics := &hoglite.Metrics{Logger: log.Default()}
	mgr := hoglite.NewManagerSat(store, *prefix, *id, *class, *dir, opts, *maxOpen, metrics, ctrl, *satCommitMs*1000, *satInflight)
	if *sweep {
		sw := hoglite.NewSweeper(mgr, store, *prefix, opts, metrics)
		go sw.Run(ctx)
	}

	peers := map[string]string{}
	for _, p := range strings.Split(*peersFl, ",") {
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			peers[kv[0]] = strings.TrimRight(kv[1], "/")
		}
	}
	srv := hoglite.NewServer(mgr, peers)

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}
	go func() {
		log.Printf("hoglited %s class=%s listening on %s bucket=%s prefix=%s", *id, *class, *addr, *bucket, *prefix)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	log.Printf("signal %v: checkpoint+release all tenants", s)
	done := make(chan struct{})
	go func() { mgr.ShutdownAll(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		log.Printf("drain timed out; exiting anyway")
	}
	fmt.Println(httpSrv.Shutdown(context.Background()))
}

func defaultID() string {
	h, _ := os.Hostname()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%d-%s", h, os.Getpid(), hex.EncodeToString(b))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
