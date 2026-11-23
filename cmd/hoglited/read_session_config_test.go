package main

import "testing"

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
