#!/usr/bin/env bash
# This script gives the functions for .github/workflows/ecr-release.yml.
#
# The workflow publishes the hoglite image to ECR for a Kargo
# Warehouse. The Warehouse checks the tag format and the OCI
# annotations on the image index before it makes Freight. These
# functions make and check that contract.
#
# Subcommands:
#   gate <owner/repo> <sha>
#   select <registry> <sha> hoglite
#   verify <registry>/hoglite:<tag> <sha>
#   check-release <registry> <tag> <sha> hoglite
#
# gate and select write key=value lines to stdout for GITHUB_OUTPUT.
# All other text goes to stderr.

set -Eeuo pipefail
# A command substitution does not inherit errexit by default. This
# option makes a failure inside $(...) stop the script too.
shopt -s inherit_errexit

SOURCE_URL=https://github.com/PostHog/hoglite
TAG_RE='^r[0-9]{12}-[0-9a-f]{6}$'
SHA_RE='^[0-9a-f]{40}$'

log() { printf '%s\n' "$*" >&2; }
die() {
  printf '::error::%s\n' "$*" >&2
  exit 1
}

require_sha() {
  [[ $1 =~ $SHA_RE ]] || die "not a 40-hex commit: '$1'"
}

# gate: read the CI run for one main commit. A real dispatch on main
# uses this gate, so a person cannot publish a commit whose tests
# failed. The function publishes only when the newest CI run for the
# commit completed with success. A missing or incomplete run gives
# publish=false. The next completed CI run decides.
gate() {
  local repo=$1 sha=$2
  require_sha "$sha"
  local runs
  runs=$(gh api "repos/${repo}/actions/runs?head_sha=${sha}&event=push&per_page=100")

  local run
  run=$(jq -r '[.workflow_runs[] | select(.path == ".github/workflows/ci.yml")]
    | max_by(.id) | .id // empty' <<<"$runs")
  if [[ -z $run ]]; then
    log "ci.yml: no run for ${sha} yet. The next completed run decides."
    echo "publish=false"
    return 0
  fi

  local status conclusion
  status=$(jq -r --argjson id "$run" '.workflow_runs[] | select(.id == $id) | .status' <<<"$runs")
  conclusion=$(jq -r --argjson id "$run" '.workflow_runs[] | select(.id == $id) | .conclusion' <<<"$runs")
  log "ci.yml run ${run}: ${status}/${conclusion}"
  if [[ $status/$conclusion == completed/success ]]; then
    log "The CI gate passed for ${sha}. Publish the image."
    echo "publish=true"
    return 0
  fi
  if [[ $status == completed ]]; then
    log "CI did not pass for ${sha}. Do not publish."
  else
    log "CI is not complete for ${sha}. The next completed run decides."
  fi
  echo "publish=false"
}

# list_tags <image>: print all tags of one repository.
# A repository that does not exist yet gives an empty list. All other
# errors stop the script.
list_tags() {
  local image=$1 out
  if ! out=$(crane ls "$image" 2>&1); then
    if grep -q NAME_UNKNOWN <<<"$out"; then
      return 0
    fi
    die "cannot list tags of ${image}: ${out}"
  fi
  printf '%s\n' "$out"
}

# list_release_tags <image> <sha6>: print the release tags of one commit.
list_release_tags() {
  local tags
  tags=$(list_tags "$1") || return 1
  grep -E "^r[0-9]{12}-${2}$" <<<"$tags" || true
}

# select: choose the release tag for the hoglite repository.
#
# The ECR tags are immutable. A rerun must not make a second tag for a
# commit that has a good release. Thus the function does this:
#   1. It finds the newest release tag of this commit in the
#      repository.
#   2. If that tag verifies, it uses the tag again and pushes nothing.
#      The commit is released already. The function refuses to make a
#      second release for it.
#   3. If there is no tag, or the tag fails verification, it makes a
#      new tag from the current UTC minute. Then it pushes the image.
select_tag() {
  local registry=$1 sha=$2
  shift 2
  require_sha "$sha"
  (($# == 1)) || die "select needs exactly one repository"
  local name=$1 short=${sha:0:6}

  local tags
  tags=$(list_release_tags "${registry}/${name}" "$short") ||
    die "cannot read the release tags of ${name}"
  local newest
  newest=$(grep -E "$TAG_RE" <<<"$tags" | sort -u | tail -n 1 || true)

  if [[ -n $newest ]]; then
    if verify "${registry}/${name}:${newest}" "$sha"; then
      log "${name}:${newest} is a good release of ${sha}. Push nothing."
      echo "tag=${newest}"
      echo "push=false"
      return 0
    fi
    log "::warning::${name}:${newest} fails verification. Make a new release tag."
  fi

  # The new tag must not exist in the repository. A failed release in
  # the same minute can have that tag. Then the loop waits for the
  # next minute.
  local tag attempt
  for attempt in $(seq 1 10); do
    tag="r$(date -u +%Y%m%d%H%M)-${short}"
    if ! grep -qx "$tag" <<<"$tags"; then
      break
    fi
    log "${tag} exists already (attempt ${attempt}). Wait for the next minute."
    sleep 15
    tag=""
  done
  [[ -n $tag ]] || die "cannot make a new release tag for ${sha}"
  log "Make the new release tag ${tag} for ${sha}."
  echo "tag=${tag}"
  echo "push=true"
}

# verify: check one pushed image against the Warehouse criteria.
# The Warehouse reads the image index, so the index must have the
# annotations. The build also writes them to each per-arch manifest
# and config. This function checks those copies too.
verify() {
  local ref=$1 sha=$2
  require_sha "$sha"
  local tag=${ref##*:}
  if ! [[ $tag =~ $TAG_RE ]]; then
    log "${ref}: the tag does not match ${TAG_RE}"
    return 1
  fi
  if [[ ${tag:14:6} != "${sha:0:6}" ]]; then
    log "${ref}: the tag does not end with the commit prefix ${sha:0:6}"
    return 1
  fi

  local index
  index=$(crane manifest "$ref") || {
    log "${ref}: cannot read the index"
    return 1
  }
  if ! jq -e --arg rev "$sha" --arg src "$SOURCE_URL" '
    .mediaType == "application/vnd.oci.image.index.v1+json"
    and ([.manifests[]
          | select(.annotations["vnd.docker.reference.type"] != "attestation-manifest")
          | .platform | .os + "/" + .architecture] | sort == ["linux/amd64", "linux/arm64"])
    and .annotations["org.opencontainers.image.revision"] == $rev
    and .annotations["org.opencontainers.image.source"] == $src' >/dev/null <<<"$index"; then
    log "${ref}: the index does not have the two platforms and the revision and source annotations"
    return 1
  fi

  local arch
  for arch in amd64 arm64; do
    if ! crane manifest --platform "linux/${arch}" "$ref" | jq -e --arg rev "$sha" --arg src "$SOURCE_URL" '
      .annotations["org.opencontainers.image.revision"] == $rev
      and .annotations["org.opencontainers.image.source"] == $src' >/dev/null; then
      log "${ref}: the linux/${arch} manifest does not have the revision and source annotations"
      return 1
    fi
    if ! crane config --platform "linux/${arch}" "$ref" | jq -e --arg rev "$sha" --arg src "$SOURCE_URL" '
      .config.Labels["org.opencontainers.image.revision"] == $rev
      and .config.Labels["org.opencontainers.image.source"] == $src' >/dev/null; then
      log "${ref}: the linux/${arch} config does not have the revision and source labels"
      return 1
    fi
  done
  log "${ref}: verified"
}

# check-release: this is the last check before Kargo. The repository
# must have the tag, and the tag must verify. Then the Warehouse can
# make Freight from this commit.
check_release() {
  local registry=$1 tag=$2 sha=$3
  shift 3
  require_sha "$sha"
  (($# == 1)) || die "check-release needs exactly one repository"
  local name=$1
  verify "${registry}/${name}:${tag}" "$sha" ||
    die "the release ${tag} is not complete. The Warehouse cannot make Freight from it."
  log "The release ${tag} is complete in ${name}."
}

cmd=${1:-}
shift || true
case "$cmd" in
  gate) gate "$@" ;;
  select) select_tag "$@" ;;
  verify) verify "$@" ;;
  check-release) check_release "$@" ;;
  *) die "usage: $0 gate|select|verify|check-release ..." ;;
esac
