#!/usr/bin/env python3
"""Validate public releases. Preserve published assets."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

REPOSITORY = "PostHog/hoglite"
PAYLOADS = frozenset({
    "hoglited-linux-amd64", "hoglited-linux-arm64",
    "hoglited-darwin-amd64", "hoglited-darwin-arm64",
    "LICENSE", "THIRD_PARTY_NOTICES",
})
ASSETS = PAYLOADS | {"checksums.txt"}


class ReleaseError(Exception):
    """An unsafe or incomplete release must stop without overwriting assets."""


def require(condition, message):
    if not condition:
        raise ReleaseError(message)


def decide(kind, event, repository):
    """Use the tested source SHA, never the workflow_run default-branch SHA."""
    try:
        require(repository == REPOSITORY, "Unexpected workflow repository")
        require(event["repository"]["full_name"] == repository,
                "Unexpected event repository")
        if kind == "workflow_run":
            run = event["workflow_run"]
            require(run["name"] == "CI", "Unexpected source workflow")
            require(run["event"] == "push", "CI must test a push")
            require(run["conclusion"] == "success", "CI must succeed")
            require(run["head_branch"] == "main", "CI must test main")
            require(run["head_repository"]["full_name"] == repository,
                    "CI must test this repository")
            sha = run["head_sha"]
        elif kind == "pull_request":
            require(event["pull_request"]["base"]["ref"] == "main",
                    "Pull requests must target main")
            sha = event["pull_request"]["head"]["sha"]
        else:
            raise ReleaseError("Unsupported release event")
        require(isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{40}", sha),
                "Expected a full lowercase commit SHA")
        return {"sha": sha, "publish": kind == "workflow_run"}
    except (KeyError, TypeError, AttributeError) as error:
        raise ReleaseError("Malformed release event") from error


def manifest(directory, create=False):
    """Allow only six payloads and their canonical SHA-256 manifest."""
    directory = Path(directory)
    names = {path.name for path in directory.iterdir()}
    expected = PAYLOADS if create and "checksums.txt" not in names else ASSETS
    require(names == expected, "Release directory does not match the asset allowlist")
    result = {}
    for name in sorted(names):
        path = directory / name
        require(not path.is_symlink() and path.is_file(), "Assets must be regular files")
        size = path.stat().st_size
        require(size > 0, "Assets must not be empty")
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        result[name] = {"size": size, "digest": "sha256:" + digest.hexdigest()}
    checksums = "".join(
        result[name]["digest"].removeprefix("sha256:") + "  " + name + "\n"
        for name in sorted(PAYLOADS)
    ).encode("ascii")
    checksum_path = directory / "checksums.txt"
    if create and "checksums.txt" not in names:
        with checksum_path.open("xb") as output:
            output.write(checksums)
        return manifest(directory)
    require(checksum_path.read_bytes() == checksums, "Checksums do not match release payloads")
    return result


class GitHub:
    """Use gh without a shell. Every API error is fatal, including lookup errors."""

    def __init__(self, repository):
        self.root = "repos/" + repository

    def api(self, method, path, data=None, upload=None, paginate=False):
        args = ["gh", "api", path, "--method", method,
                "--hostname", "github.com",
                "-H", "Accept: application/vnd.github+json",
                "-H", "X-GitHub-Api-Version: 2022-11-28"]
        if paginate:
            args += ["--paginate", "--slurp"]
        if upload is not None:
            args += ["-H", "Content-Type: application/octet-stream", "--input", str(upload)]
        elif data is not None:
            args += ["--input", "-"]
        try:
            process = subprocess.run(
                args, input=json.dumps(data) if data is not None else None,
                capture_output=True, text=True, check=True, timeout=120,
            )
            if method == "DELETE":
                require(not process.stdout.strip(), "Unexpected asset deletion response")
                return None
            return json.loads(process.stdout)
        except (subprocess.SubprocessError, OSError, ValueError) as error:
            raise ReleaseError("GitHub API request failed: " + method + " " + path) from error

    def pages(self, path):
        pages = self.api("GET", path, paginate=True)
        require(isinstance(pages, list) and all(isinstance(page, list) for page in pages),
                "Malformed paginated API response")
        result = [item for page in pages for item in page]
        require(all(isinstance(item, dict) for item in result), "Malformed API entries")
        return result

    def ref(self, tag, sha, required=False):
        refs = self.api("GET", self.root + "/git/matching-refs/tags/" + tag)
        require(isinstance(refs, list) and all(isinstance(ref, dict) for ref in refs),
                "Malformed tag lookup")
        matches = [ref for ref in refs if ref.get("ref") == "refs/tags/" + tag]
        require(len(matches) <= 1, "Duplicate tag references")
        if not matches:
            require(not required, "Published release tag is missing")
            return False
        require(isinstance(matches[0].get("object"), dict)
                and matches[0]["object"].get("type") == "commit"
                and matches[0]["object"].get("sha") == sha,
                "Release tag does not point directly to the tested commit")
        return True

    def current_main(self, sha):
        ref = self.api("GET", self.root + "/git/ref/heads/main")
        require(isinstance(ref, dict) and ref.get("ref") == "refs/heads/main",
                "Malformed main reference")
        target = ref.get("object")
        require(isinstance(target, dict) and target.get("type") == "commit"
                and isinstance(target.get("sha"), str)
                and re.fullmatch(r"[0-9a-f]{40}", target["sha"]),
                "Malformed main commit")
        if target["sha"] == sha:
            return True
        print("Skipping superseded tested commit " + sha)
        return False


def check_release(item, tag, sha):
    require(isinstance(item, dict), "Malformed release response")
    require(type(item.get("id")) is int and item["id"] > 0, "Invalid release ID")
    require(item.get("tag_name") == tag, "Release tag mismatch")
    require(item.get("target_commitish") == sha, "Release target mismatch")
    require(type(item.get("draft")) is bool, "Release draft status is missing")
    require(item.get("prerelease") is True, "Release must remain a prerelease")


def check_assets(remote, local, complete, allow_starters=False):
    found = set()
    seen = set()
    for asset in remote:
        name = asset.get("name")
        require(isinstance(name, str) and name in local and name not in seen,
                "Unexpected or duplicate remote asset")
        seen.add(name)
        if allow_starters and asset.get("state") == "starter":
            require(type(asset.get("id")) is int and asset["id"] > 0, "Invalid starter asset ID")
            require(asset.get("size") == 0 and asset.get("digest") is None,
                    "Starter assets must be empty and have no digest")
            continue
        require(asset.get("state") == "uploaded", "Remote asset is not uploaded")
        require(asset.get("digest") == local[name]["digest"], "Remote asset digest mismatch")
        require(asset.get("size") == local[name]["size"], "Remote asset size mismatch")
        found.add(name)
    require(not complete or found == set(local), "Published release assets are incomplete")
    return found


def clean_starters(api, endpoint, tag, sha, starters, local):
    """Delete only empty starter assets after freshly checking the matching draft."""
    for name in starters:
        item = api.api("GET", endpoint)
        check_release(item, tag, sha)
        require(item["draft"], "Published assets cannot be deleted")
        api.ref(tag, sha, required=True)
        remote = api.pages(endpoint + "/assets?per_page=100")
        check_assets(remote, local, complete=False, allow_starters=True)
        matches = [asset for asset in remote if asset["name"] == name]
        if matches and matches[0]["state"] == "starter":
            if not api.current_main(sha):
                return None
            api.api("DELETE", api.root + "/releases/assets/" + str(matches[0]["id"]))
    # An upload may have completed meanwhile. Never overwrite its matching bytes.
    return check_assets(api.pages(endpoint + "/assets?per_page=100"), local, complete=False)


def publish(directory, kind, event, repository):
    decision = decide(kind, event, repository)
    require(decision["publish"], "Pull requests cannot publish")
    sha = decision["sha"]
    tag = "build-" + sha
    directory = Path(directory)
    local = manifest(directory)
    api = GitHub(repository)
    releases = api.pages(api.root + "/releases?per_page=100")
    matches = [item for item in releases if item.get("tag_name") == tag]
    require(len(matches) <= 1, "Duplicate release tags")
    item = matches[0] if matches else None
    if item is not None:
        check_release(item, tag, sha)
        item = api.api("GET", api.root + "/releases/" + str(item["id"]))
        check_release(item, tag, sha)
        remote = api.pages(api.root + "/releases/" + str(item["id"]) + "/assets?per_page=100")
        found = check_assets(remote, local, complete=not item["draft"],
                             allow_starters=item["draft"])
        starters = [asset["name"] for asset in remote if asset.get("state") == "starter"]
    else:
        found = set()
        starters = []
    has_ref = api.ref(tag, sha, required=item is not None and not item["draft"])
    if item is not None and not item["draft"]:
        print("Verified existing release " + tag)
        return
    if not api.current_main(sha):
        return
    if not has_ref:
        # Create only. Never update or force an existing tag.
        api.api("POST", api.root + "/git/refs", {"ref": "refs/tags/" + tag, "sha": sha})
        api.ref(tag, sha, required=True)
    if item is None:
        item = api.api("POST", api.root + "/releases", {
            "tag_name": tag, "target_commitish": sha, "name": tag,
            "body": "Native hoglited binaries for commit " + sha + ".\n"
                    "Verify downloads against checksums.txt.\n"
                    "These prereleases do not advance latest.",
            "draft": True, "prerelease": True, "make_latest": "false",
        })
        check_release(item, tag, sha)
        require(item["draft"], "New releases must start as drafts")
    endpoint = api.root + "/releases/" + str(item["id"])
    if starters:
        found = clean_starters(api, endpoint, tag, sha, starters, local)
        if found is None:
            return
    for name in sorted(set(local) - found):
        # Names come from the fixed local allowlist, never from an API URL.
        api.api("POST", "https://uploads.github.com/" + endpoint + "/assets?name=" + name,
                upload=directory / name)
    remote = api.pages(endpoint + "/assets?per_page=100")
    check_assets(remote, local, complete=True)
    item = api.api("GET", endpoint)
    check_release(item, tag, sha)
    require(item["draft"], "Release changed while uploading")
    api.ref(tag, sha, required=True)
    if not api.current_main(sha):
        return
    item = api.api("PATCH", endpoint,
                   {"draft": False, "prerelease": True, "make_latest": "false"})
    check_release(item, tag, sha)
    require(not item["draft"], "Release was not published")
    item = api.api("GET", endpoint)
    check_release(item, tag, sha)
    require(not item["draft"], "Published release is still a draft")
    check_assets(api.pages(endpoint + "/assets?per_page=100"), local, complete=True)
    api.ref(tag, sha, required=True)
    print("Published " + tag)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["gate", "manifest", "publish"])
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "manifest":
            require(args.directory is not None, "An asset directory is required")
            manifest(args.directory, create=True)
            return
        with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as source:
            event = json.load(source)
        kind, repository = os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_REPOSITORY"]
        if args.command == "publish":
            require(args.directory is not None, "An asset directory is required")
            publish(args.directory, kind, event, repository)
        else:
            result = decide(kind, event, repository)
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
                output.write("sha=" + result["sha"] + "\n")
                output.write("publish=" + str(result["publish"]).lower() + "\n")
    except (ReleaseError, OSError, ValueError, KeyError) as error:
        print("Release refused: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
