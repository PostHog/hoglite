#!/usr/bin/env python3
"""Run with Python's standard library. No network requests are made."""

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location(
    "release", Path(__file__).with_name("public-release.py")
)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
SHA = "a" * 40
OTHER = "b" * 40
REPO = "PostHog/hoglite"
PAYLOADS = {
    "hoglited-linux-amd64", "hoglited-linux-arm64",
    "hoglited-darwin-amd64", "hoglited-darwin-arm64",
    "LICENSE", "THIRD_PARTY_NOTICES",
}


def event():
    return {
        "repository": {"full_name": REPO},
        "workflow_run": {
            "name": "CI", "event": "push", "conclusion": "success",
            "head_branch": "main", "head_sha": SHA,
            "head_repository": {"full_name": REPO},
        },
    }


class GateTests(unittest.TestCase):
    def test_success_uses_tested_sha(self):
        self.assertEqual(release.decide("workflow_run", event(), REPO),
                         {"sha": SHA, "publish": True})

    def test_rejects_each_untrusted_source(self):
        for key, value in [
            ("event", "pull_request"), ("head_branch", "feature"),
            ("conclusion", "failure"), ("conclusion", None),
            ("head_repository", {"full_name": "other/hoglite"}),
            ("head_sha", "$(touch /tmp/no)"), ("name", "Other CI"),
        ]:
            with self.subTest(key=key, value=value):
                data = event()
                data["workflow_run"][key] = value
                with self.assertRaises(release.ReleaseError):
                    release.decide("workflow_run", data, REPO)

    def test_requires_canonical_repository(self):
        for repo in ["other/hoglite", "posthog/hoglite", ""]:
            with self.assertRaises(release.ReleaseError):
                release.decide("workflow_run", event(), repo)
        data = event()
        data["repository"]["full_name"] = "other/hoglite"
        with self.assertRaises(release.ReleaseError):
            release.decide("workflow_run", data, REPO)

    def test_pr_head_can_be_fork_but_never_publishes(self):
        data = {"repository": {"full_name": REPO}, "pull_request": {
            "head": {"sha": OTHER}, "base": {"ref": "main"},
        }}
        self.assertEqual(release.decide("pull_request", data, REPO),
                         {"sha": OTHER, "publish": False})
        data["pull_request"]["base"]["ref"] = "feature"
        with self.assertRaises(release.ReleaseError):
            release.decide("pull_request", data, REPO)

    def test_unknown_and_malformed_events_fail(self):
        for kind, data in [("workflow_dispatch", event()), ("workflow_run", {}),
                           ("pull_request", {}), ("workflow_run", {"repository": None})]:
            with self.assertRaises(release.ReleaseError):
                release.decide(kind, data, REPO)


class AssetFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(
            dir=os.environ.get("TMPDIR"))
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        for name in PAYLOADS:
            (self.path / name).write_bytes((name + "\n").encode())

    def make_manifest(self):
        return release.manifest(self.path, create=True)


class AssetTests(AssetFixture):
    def test_allowlist_and_canonical_checksums(self):
        result = self.make_manifest()
        self.assertEqual(set(result), PAYLOADS | {"checksums.txt"})
        expected = "".join(
            hashlib.sha256((self.path / name).read_bytes()).hexdigest()
            + "  " + name + "\n" for name in sorted(PAYLOADS))
        self.assertEqual((self.path / "checksums.txt").read_text(), expected)
        self.assertEqual(release.manifest(self.path), result)

    def test_extra_missing_and_empty_assets(self):
        for kind in ["extra", "missing", "empty", "directory"]:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as folder:
                path = Path(folder)
                for name in PAYLOADS:
                    (path / name).write_text(name)
                if kind == "extra":
                    (path / "untrusted.py").write_text("no")
                elif kind == "missing":
                    (path / "LICENSE").unlink()
                elif kind == "empty":
                    (path / "LICENSE").write_text("")
                else:
                    (path / "LICENSE").unlink()
                    (path / "LICENSE").mkdir()
                with self.assertRaises(release.ReleaseError):
                    release.manifest(path, create=True)

    def test_symlinks_are_never_assets(self):
        (self.path / "LICENSE").unlink()
        (self.path / "LICENSE").symlink_to(self.path / "THIRD_PARTY_NOTICES")
        with self.assertRaises(release.ReleaseError):
            self.make_manifest()

    def test_checksum_mismatch_and_missing_manifest_fail(self):
        with self.assertRaises(release.ReleaseError):
            release.manifest(self.path)
        self.make_manifest()
        (self.path / "checksums.txt").write_text("forged\n")
        with self.assertRaises(release.ReleaseError):
            release.manifest(self.path)


class FakeGh:
    """Model gh's subprocess boundary, including writes and partial failures."""

    def __init__(self, manifest):
        self.manifest = manifest
        self.ref = None
        self.release = None
        self.assets = []
        self.calls = []
        self.fail_path = None
        self.fail_method = None
        self.bad_json = False
        self.corrupt_upload = False
        self.incomplete_publish = False
        self.main_shas = [SHA]
        self.malformed_main = None
        self.main_reads = 0
        self.fail_main_at = None

    def seed(self, draft=False, names=None):
        self.ref = SHA
        self.release = {
            "id": 17, "tag_name": "build-" + SHA, "target_commitish": SHA,
            "draft": draft, "prerelease": True,
        }
        self.assets = [dict(name=name, id=index, state="uploaded", **self.manifest[name])
                       for index, name in enumerate(sorted(
                           self.manifest if names is None else names), 1)]

    def __call__(self, args, **kwargs):
        assert args[:2] == ["gh", "api"], args
        assert kwargs.get("shell", False) is False
        assert kwargs["timeout"] == 120
        assert args[args.index("--hostname") + 1] == "github.com"
        method = args[args.index("--method") + 1]
        path = args[2]
        body = json.loads(kwargs["input"]) if kwargs.get("input") else None
        self.calls.append((method, path, body))
        if self.fail_path and self.fail_path in path and (
                self.fail_method is None or self.fail_method == method):
            raise subprocess.CalledProcessError(1, args, stderr="HTTP 503 or authentication failure")
        if self.bad_json:
            return subprocess.CompletedProcess(args, 0, "<html>bad gateway</html>", "")
        root = "repos/" + REPO
        if path == root + "/releases?per_page=100" and method == "GET":
            assert "--paginate" in args and "--slurp" in args
            result = [[self.release] if self.release else []]
        elif path == root + "/git/ref/heads/main" and method == "GET":
            self.main_reads += 1
            if self.main_reads == self.fail_main_at:
                raise subprocess.CalledProcessError(1, args, stderr="HTTP 503")
            sha = self.main_shas[0]
            if len(self.main_shas) > 1:
                self.main_shas.pop(0)
            result = self.malformed_main if self.malformed_main is not None else {
                "ref": "refs/heads/main", "object": {"type": "commit", "sha": sha},
            }
        elif path.startswith(root + "/git/matching-refs/"):
            result = [] if self.ref is None else [{
                "ref": "refs/tags/build-" + SHA,
                "object": {"type": "commit", "sha": self.ref},
            }]
        elif path == root + "/git/refs" and method == "POST":
            assert body == {"ref": "refs/tags/build-" + SHA, "sha": SHA}
            self.ref = body["sha"]
            result = {}
        elif path == root + "/releases" and method == "POST":
            assert body["draft"] is True and body["make_latest"] == "false"
            assert body["target_commitish"] == SHA and body["prerelease"] is True
            self.release = dict(body, id=17)
            result = self.release
        elif path == root + "/releases/17" and method == "GET":
            result = self.release
        elif path == root + "/releases/17/assets?per_page=100":
            assert "--paginate" in args and "--slurp" in args
            result = [self.assets]
        elif path.startswith(root + "/releases/assets/") and method == "DELETE":
            asset_id = int(path.rsplit("/", 1)[1])
            asset = next(asset for asset in self.assets if asset["id"] == asset_id)
            assert self.release["draft"] is True
            assert self.release["target_commitish"] == SHA and self.ref == SHA
            assert asset["state"] == "starter" and asset["name"] in self.manifest
            self.assets.remove(asset)
            return subprocess.CompletedProcess(args, 0, "", "")
        elif path.startswith("https://uploads.github.com/" + root + "/releases/17/assets?name="):
            assert method == "POST" and "--input" in args
            name = path.split("?name=", 1)[1]
            file = Path(args[args.index("--input") + 1])
            digest = "sha256:" + hashlib.sha256(file.read_bytes()).hexdigest()
            assert name not in {asset["name"] for asset in self.assets}
            self.assets.append({"name": name,
                                "id": max((asset["id"] for asset in self.assets), default=0) + 1,
                                "digest": "sha256:bad" if self.corrupt_upload else digest,
                                "state": "uploaded", "size": file.stat().st_size})
            result = self.assets[-1]
        elif path == root + "/releases/17" and method == "PATCH":
            assert body == {"draft": False, "prerelease": True, "make_latest": "false"}
            assert {a["name"] for a in self.assets} == set(self.manifest)
            if not self.incomplete_publish:
                self.release.update(body)
            result = self.release
        else:
            raise AssertionError((method, path, body))
        return subprocess.CompletedProcess(args, 0, json.dumps(result), "")

    def writes(self):
        return [call for call in self.calls if call[0] != "GET"]


class PublishTests(AssetFixture):
    def setUp(self):
        super().setUp()
        self.fake = FakeGh(self.make_manifest())
        self.addCleanup(patch.stopall)
        patch.object(release.subprocess, "run", self.fake).start()

    def publish(self):
        release.publish(self.path, "workflow_run", event(), REPO)

    def test_new_release_is_verified_before_publication(self):
        self.publish()
        self.assertFalse(self.fake.release["draft"])
        self.assertEqual(len(self.fake.assets), 7)
        self.assertEqual([x[0] for x in self.fake.writes()],
                         ["POST"] * 9 + ["PATCH"])
        count = len(self.fake.writes())
        self.publish()
        self.assertEqual(len(self.fake.writes()), count)

    def test_matching_published_release_is_read_only(self):
        self.fake.seed()
        self.fake.main_shas = [OTHER]
        self.publish()
        self.assertEqual(self.fake.writes(), [])
        self.assertFalse(any("/git/ref/heads/main" in call[1] for call in self.fake.calls))

    def test_superseded_sha_skips_before_any_writes(self):
        for existing in ["new", "draft", "starter"]:
            with self.subTest(existing=existing):
                self.fake.ref = None
                self.fake.release = None
                self.fake.assets = []
                if existing != "new":
                    self.fake.seed(draft=True)
                if existing == "starter":
                    self.starter()
                self.fake.calls = []
                self.fake.main_shas = [OTHER]
                self.publish()
                self.assertEqual(self.fake.writes(), [])

    def test_superseded_sha_leaves_complete_draft_unpublished(self):
        self.fake.main_shas = [SHA, OTHER]
        self.publish()
        self.assertTrue(self.fake.release["draft"])
        self.assertEqual(len(self.fake.assets), 7)
        self.assertFalse(any(call[0] == "PATCH" for call in self.fake.writes()))

    def test_main_ref_api_failure_stops_before_writes(self):
        self.fake.fail_path = "/git/ref/heads/main"
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertEqual(self.fake.writes(), [])

    def test_final_main_ref_error_leaves_draft_unpublished(self):
        self.fake.fail_main_at = 2
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertTrue(self.fake.release["draft"])
        self.assertFalse(any(call[0] == "PATCH" for call in self.fake.writes()))

    def test_final_malformed_main_ref_leaves_draft_unpublished(self):
        self.fake.main_shas = [SHA, "short"]
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertTrue(self.fake.release["draft"])
        self.assertFalse(any(call[0] == "PATCH" for call in self.fake.writes()))

    def test_malformed_main_ref_stops_before_writes(self):
        for value in [{}, [], {"ref": "refs/heads/other", "object": {"type": "commit", "sha": SHA}},
                      {"ref": "refs/heads/main", "object": {"type": "tag", "sha": SHA}},
                      {"ref": "refs/heads/main", "object": {"type": "commit", "sha": "short"}}]:
            with self.subTest(value=value):
                self.fake.malformed_main = value
                with self.assertRaises(release.ReleaseError):
                    self.publish()
                self.assertEqual(self.fake.writes(), [])

    def test_complete_draft_only_needs_publication(self):
        self.fake.seed(draft=True)
        self.publish()
        self.assertEqual([call[0] for call in self.fake.writes()], ["PATCH"])

    def starter(self, name="LICENSE"):
        asset = next(asset for asset in self.fake.assets if asset["name"] == name)
        asset.update(state="starter", size=0, digest=None)
        return asset

    def test_matching_draft_replaces_only_incomplete_starter(self):
        self.fake.seed(draft=True)
        asset = self.starter()
        asset_id = asset["id"]
        self.publish()
        self.assertEqual([call[0] for call in self.fake.writes()], ["DELETE", "POST", "PATCH"])
        self.assertEqual(self.fake.writes()[0][1],
                         "repos/" + REPO + "/releases/assets/" + str(asset_id))
        self.assertFalse(self.fake.release["draft"])
        self.assertEqual(len(self.fake.assets), 7)

    def test_delete_accepts_empty_204_response(self):
        self.fake.seed(draft=True)
        asset = self.starter()
        self.assertIsNone(release.GitHub(REPO).api(
            "DELETE", "repos/" + REPO + "/releases/assets/" + str(asset["id"])))

    def test_failed_starter_cleanup_never_uploads_or_publishes(self):
        self.fake.seed(draft=True)
        self.starter()
        self.fake.fail_path = "/releases/assets/"
        self.fake.fail_method = "DELETE"
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertEqual([call[0] for call in self.fake.writes()], ["DELETE"])
        self.assertTrue(self.fake.release["draft"])

    def test_superseded_sha_immediately_before_cleanup_has_no_writes(self):
        self.fake.seed(draft=True)
        self.starter()
        self.fake.main_shas = [SHA, OTHER]
        self.publish()
        self.assertEqual(self.fake.writes(), [])
        self.assertTrue(self.fake.release["draft"])

    def test_cleanup_rechecks_draft_and_asset_state(self):
        original = release.GitHub.api
        for change in ["published", "uploaded_mismatch", "uploaded_matching"]:
            with self.subTest(change=change):
                self.fake.seed(draft=True)
                self.fake.calls = []
                self.fake.main_reads = 0
                asset = self.starter()
                reads = 0

                def api(client, method, path, *args, **kwargs):
                    nonlocal reads
                    if method == "GET" and path.endswith("/releases/17"):
                        reads += 1
                        if reads == 2:
                            if change == "published":
                                self.fake.release["draft"] = False
                            else:
                                asset.update(self.fake.manifest["LICENSE"], state="uploaded")
                                if change == "uploaded_mismatch":
                                    asset["digest"] = "sha256:bad"
                    return original(client, method, path, *args, **kwargs)

                with patch.object(release.GitHub, "api", new=api):
                    if change == "uploaded_matching":
                        self.publish()
                        self.assertEqual([call[0] for call in self.fake.writes()], ["PATCH"])
                    else:
                        with self.assertRaises(release.ReleaseError):
                            self.publish()
                        self.assertEqual(self.fake.writes(), [])

    def test_starter_cleanup_rejects_published_or_mismatched_releases(self):
        for kind in ["published", "tag", "sha", "name", "uploaded_mismatch", "invalid_id",
                     "starter_with_bytes", "starter_with_digest"]:
            with self.subTest(kind=kind):
                self.fake.seed(draft=kind != "published")
                self.fake.calls = []
                asset = self.starter()
                if kind == "tag":
                    self.fake.ref = OTHER
                elif kind == "sha":
                    self.fake.release["target_commitish"] = OTHER
                elif kind == "name":
                    asset["name"] = "unexpected"
                elif kind == "uploaded_mismatch":
                    self.fake.assets[-1]["digest"] = "sha256:bad"
                elif kind == "invalid_id":
                    asset["id"] = "../17"
                elif kind == "starter_with_bytes":
                    asset["size"] = 1
                elif kind == "starter_with_digest":
                    asset["digest"] = self.fake.manifest["LICENSE"]["digest"]
                with self.assertRaises(release.ReleaseError):
                    self.publish()
                self.assertEqual(self.fake.writes(), [])

    def test_matching_tag_without_release_can_create_draft(self):
        self.fake.ref = SHA
        self.publish()
        self.assertFalse(any(call[1].endswith("/git/refs") for call in self.fake.writes()))

    def test_partial_matching_draft_resumes_without_overwrite(self):
        self.fake.seed(draft=True, names=["LICENSE"])
        self.publish()
        self.assertEqual(len(self.fake.writes()), 7)
        self.assertFalse(self.fake.release["draft"])

    def test_target_and_tag_mismatches_fail_without_writes(self):
        for kind in ["target", "ref", "no_ref", "prerelease"]:
            with self.subTest(kind=kind):
                self.fake.seed()
                self.fake.calls = []
                if kind == "target":
                    self.fake.release["target_commitish"] = OTHER
                elif kind == "ref":
                    self.fake.ref = OTHER
                elif kind == "no_ref":
                    self.fake.ref = None
                else:
                    self.fake.release["prerelease"] = False
                with self.assertRaises(release.ReleaseError):
                    self.publish()
                self.assertEqual(self.fake.writes(), [])

    def test_bad_remote_assets_are_never_overwritten(self):
        for draft in [False, True]:
            for kind in ["digest", "size", "state", "extra", "duplicate", "missing"]:
                if draft and kind == "missing":
                    continue
                with self.subTest(draft=draft, kind=kind):
                    self.fake.seed(draft=draft)
                    self.fake.calls = []
                    if kind in ["digest", "size", "state"]:
                        self.fake.assets[0][kind] = "bad"
                    elif kind == "extra":
                        self.fake.assets.append({"name": "evil"})
                    elif kind == "duplicate":
                        self.fake.assets.append(copy.copy(self.fake.assets[0]))
                    else:
                        self.fake.assets.pop()
                    with self.assertRaises(release.ReleaseError):
                        self.publish()
                    self.assertEqual(self.fake.writes(), [])

    def test_api_read_failures_do_not_mean_missing(self):
        for path in ["releases?", "matching-refs", "/assets?", "/releases/17"]:
            with self.subTest(path=path):
                self.fake.seed()
                self.fake.calls = []
                self.fake.fail_path = path
                self.fake.fail_method = "GET"
                with self.assertRaises(release.ReleaseError):
                    self.publish()
                self.assertEqual(self.fake.writes(), [])

    def test_bad_json_fails_closed(self):
        self.fake.bad_json = True
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertEqual(self.fake.writes(), [])

    def test_timeout_fails_closed(self):
        with patch.object(release.subprocess, "run",
                          side_effect=subprocess.TimeoutExpired("gh", 120)):
            with self.assertRaises(release.ReleaseError):
                self.publish()
        self.assertEqual(self.fake.writes(), [])

    def test_tag_and_draft_creation_failures_stop(self):
        for endpoint in ["/git/refs", "/releases"]:
            with self.subTest(endpoint=endpoint):
                self.fake.ref = None
                self.fake.release = None
                self.fake.assets = []
                self.fake.calls = []
                self.fake.fail_path = endpoint
                self.fake.fail_method = "POST"
                with self.assertRaises(release.ReleaseError):
                    self.publish()
                self.assertFalse(any("uploads.github.com" in call[1]
                                     or call[0] == "PATCH" for call in self.fake.calls))

    def test_local_tampering_stops_before_api_calls(self):
        (self.path / "hoglited-linux-amd64").write_bytes(b"tampered")
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertEqual(self.fake.calls, [])

    def test_malformed_paginated_data_fails_closed(self):
        for data in [{}, [None], [[None]]]:
            with self.subTest(data=data), patch.object(
                    release.subprocess, "run",
                    return_value=subprocess.CompletedProcess([], 0, json.dumps(data), "")):
                with self.assertRaises(release.ReleaseError):
                    self.publish()
        self.assertEqual(self.fake.writes(), [])

    def test_duplicate_release_tags_fail_closed(self):
        self.fake.seed()
        with patch.object(release.GitHub, "pages",
                          return_value=[self.fake.release, self.fake.release]):
            with self.assertRaises(release.ReleaseError):
                self.publish()
        self.assertEqual(self.fake.writes(), [])

    def test_upload_failure_leaves_draft(self):
        self.fake.fail_path = "uploads.github.com"
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertTrue(self.fake.release["draft"])
        self.assertFalse(any(call[0] == "PATCH" for call in self.fake.calls))

    def test_rerun_after_partial_upload_does_not_overwrite(self):
        self.fake.fail_path = "?name=hoglited-linux-amd64"
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertGreater(len(self.fake.assets), 0)
        self.assertTrue(self.fake.release["draft"])
        self.fake.fail_path = None
        self.publish()
        self.assertFalse(self.fake.release["draft"])
        self.assertEqual(len(self.fake.assets), 7)

    def test_uploaded_digest_mismatch_blocks_publication(self):
        self.fake.corrupt_upload = True
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.assertTrue(self.fake.release["draft"])

    def test_publish_failure_or_incomplete_response_fails(self):
        self.fake.seed(draft=True)
        self.fake.fail_path = "/releases/17"
        self.fake.fail_method = "PATCH"
        with self.assertRaises(release.ReleaseError):
            self.publish()
        self.fake.fail_path = None
        self.fake.incomplete_publish = True
        with self.assertRaises(release.ReleaseError):
            self.publish()

    def test_publisher_rechecks_gate(self):
        data = event()
        data["workflow_run"]["event"] = "pull_request"
        with self.assertRaises(release.ReleaseError):
            release.publish(self.path, "workflow_run", data, REPO)
        self.assertEqual(self.fake.calls, [])


class CliTests(AssetFixture):
    def run_cli(self, command, data, **extra):
        script = Path(__file__).with_name("public-release.py")
        event_path = self.path / "event.json"
        output = self.path / "output"
        event_path.write_text(json.dumps(data))
        env = dict(os.environ, GITHUB_EVENT_PATH=str(event_path),
                   GITHUB_EVENT_NAME="workflow_run", GITHUB_REPOSITORY=REPO,
                   GITHUB_OUTPUT=str(output), **extra)
        result = subprocess.run([sys.executable, str(script), command],
                                env=env, capture_output=True, text=True, timeout=10)
        return result, output

    def test_gate_cli_writes_validated_outputs(self):
        result, output = self.run_cli("gate", event())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.read_text(), "sha=" + SHA + "\npublish=true\n")

    def test_gate_cli_writes_nothing_for_rejected_event(self):
        data = event()
        data["workflow_run"]["head_sha"] = "a\npublish=true"
        result, output = self.run_cli("gate", data)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

    def test_gh_executable_failure_is_not_a_missing_release(self):
        # Execute a local gh stub. This test cannot contact GitHub.
        stub = self.path / "gh"
        stub.write_text("#!/bin/sh\nprintf 'authentication failure\\n' >&2\nexit 1\n")
        stub.chmod(0o755)
        with patch.dict(os.environ, PATH=str(self.path)):
            with self.assertRaises(release.ReleaseError):
                release.GitHub(REPO).pages("repos/" + REPO + "/releases?per_page=100")


class WorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).parents[1] / "workflows/public-binary-release.yml"
        cls.workflow = path.read_text()

    def test_official_actions_use_full_sha_pins(self):
        uses = re.findall(r"uses: ([^\s]+)", self.workflow)
        self.assertTrue(uses)
        self.assertTrue(all(re.fullmatch(r"actions/[a-z-]+@[0-9a-f]{40}", use)
                            for use in uses))

    def test_only_the_publisher_has_write_permissions(self):
        readonly, publisher = self.workflow.split("\n  publish:\n", 1)
        self.assertNotIn("contents: write", readonly)
        self.assertEqual(publisher.count("contents: write"), 1)
        self.assertNotIn("id-token:", self.workflow)
        self.assertNotIn("secrets.", self.workflow)
        self.assertNotIn("go build", publisher)
        self.assertNotIn("go test", publisher)
        self.assertEqual(re.findall(r"\n\s+run: ([^\n]+)", publisher), [
            'python3 .github/scripts/public-release.py publish --directory "$RUNNER_TEMP/public-assets"',
        ])

    def test_native_matrix_and_checks_remain_explicit(self):
        targets = re.findall(r"- runner: ([\w.-]+)\n\s+goos: (\w+)\n\s+goarch: (\w+)",
                             self.workflow)
        self.assertEqual(targets, [
            ("ubuntu-24.04", "linux", "amd64"),
            ("ubuntu-24.04-arm", "linux", "arm64"),
            ("macos-15-intel", "darwin", "amd64"),
            ("macos-15", "darwin", "arm64"),
        ])
        self.assertIn('test "$(go env GOOS)" = "$EXPECTED_GOOS"', self.workflow)
        self.assertIn('test "$(go env GOARCH)" = "$EXPECTED_GOARCH"', self.workflow)
        self.assertIn('test "$(go env GOHOSTOS)" = "$EXPECTED_GOOS"', self.workflow)
        self.assertIn('test "$(go env GOHOSTARCH)" = "$EXPECTED_GOARCH"', self.workflow)
        self.assertIn("CGO_ENABLED: '0'", self.workflow)
        self.assertIn("GOFLAGS: -mod=readonly", self.workflow)
        self.assertIn("go test -timeout 20m ./...", self.workflow)
        self.assertIn("-run '^Test(SQLite|WAL)'", self.workflow)

    def test_checkouts_never_persist_credentials(self):
        self.assertEqual(self.workflow.count("uses: actions/checkout@"),
                         self.workflow.count("persist-credentials: false"))
        self.assertIn("cancel-in-progress: false", self.workflow)


if __name__ == "__main__":
    unittest.main(verbosity=2)
