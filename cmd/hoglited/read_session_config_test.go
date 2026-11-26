package main

import (
	"log"
	"os"
	"testing"
)

func TestBuildReadSessionConfig(t *testing.T) {
	t.Run("empty profile disables read sessions even with residual settings", func(t *testing.T) {
		rs, rsS3, err := buildReadSessionConfig(
			"",
			"remote:events,remote:imports",
			"example-owner",
			"root",
			"us-east-1",
			"http://127.0.0.1:9000",
			true,
		)
		if err != nil {
			t.Fatalf("empty profile returned error: %v", err)
		}
		if rs != nil {
			t.Fatalf("empty profile returned non-nil ReadSessionConfig: %+v", rs)
		}
		if rsS3.BaseEndpoint != "" || rsS3.UsePathStyle {
			t.Fatalf("empty profile returned non-empty S3 config: %+v", rsS3)
		}
	})

	t.Run("non-empty profile builds config and accepts https endpoint", func(t *testing.T) {
		rs, rsS3, err := buildReadSessionConfig(
			"hoglake-read-session-v1",
			"remote:events,remote:imports",
			"example-owner",
			"root",
			"us-east-1",
			"https://s3.example.test",
			true,
		)
		if err != nil {
			t.Fatalf("valid config returned error: %v", err)
		}
		if rs == nil || rs.Profile != "hoglake-read-session-v1" || len(rs.RemoteTargets) != 2 ||
			rs.RemoteTargets[0] != "remote:events" || rs.RemoteTargets[1] != "remote:imports" ||
			rs.Bucket != "example-owner" || rs.Prefix != "root" || rs.Region != "us-east-1" {
			t.Fatalf("unexpected ReadSessionConfig: %+v", rs)
		}
		if rsS3.BaseEndpoint != "https://s3.example.test" || !rsS3.UsePathStyle {
			t.Fatalf("unexpected ReadSessionS3Config: %+v", rsS3)
		}
	})

	t.Run("non-empty profile rejects plain http endpoint", func(t *testing.T) {
		if _, _, err := buildReadSessionConfig(
			"hoglake-read-session-v1",
			"remote:events,remote:imports",
			"example-owner",
			"root",
			"us-east-1",
			"http://127.0.0.1:9000",
			true,
		); err == nil {
			t.Fatal("plain http endpoint passed")
		}
	})
}

func TestNewDaemonMetricsSeparatesReadSessionLoggerWriter(t *testing.T) {
	m := newDaemonMetrics()
	daemonLog, ok := m.Logger.(*log.Logger)
	if !ok {
		t.Fatalf("Logger type = %T, want *log.Logger", m.Logger)
	}
	rsLog, ok := m.ReadSessionLogger.(*log.Logger)
	if !ok {
		t.Fatalf("ReadSessionLogger type = %T, want *log.Logger", m.ReadSessionLogger)
	}
	if daemonLog == rsLog {
		t.Fatal("ReadSessionLogger shares *log.Logger with daemon Logger")
	}
	if daemonLog.Writer() == rsLog.Writer() {
		t.Fatalf("ReadSessionLogger shares underlying io.Writer (%v) with daemon Logger", rsLog.Writer())
	}
	f, ok := rsLog.Writer().(*os.File)
	if !ok {
		t.Fatalf("ReadSessionLogger writer type = %T, want *os.File", rsLog.Writer())
	}
	t.Cleanup(func() { _ = f.Close() })
	if f.Fd() <= 2 {
		t.Fatalf("ReadSessionLogger fd = %d, want > 2 to avoid Go SIGPIPE on fd 1/2", f.Fd())
	}
}

func TestReadSessionLogWriterSurvivesClosedPipe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer w.Close()

	diagW := newReadSessionLogWriter(w)
	f, ok := diagW.(*os.File)
	if !ok {
		t.Fatalf("writer type = %T, want *os.File", diagW)
	}
	defer f.Close()
	if f.Fd() <= 2 {
		t.Fatalf("dup fd = %d, want > 2", f.Fd())
	}

	// Close the reader so writes hit EPIPE; non-standard fd must return an error, not SIGPIPE.
	if err := r.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	if _, err := f.Write([]byte("read-session manifest read test\n")); err == nil {
		t.Fatal("expected EPIPE write error after closing pipe reader, got nil")
	}
}
