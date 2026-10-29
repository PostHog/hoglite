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
#   tip <owner/repo> <sha>
#   select <registry> <sha> hoglite
#   verify <registry>/hoglite:<tag> <sha>
#   check-release <registry> <tag> <sha> hoglite
#
# gate and tip write key=value lines to stdout for GITHUB_OUTPUT.
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

# main_tip <owner/repo>: print the 40-hex commit at the tip of main.
main_tip() {
  local tip
  tip=$(gh api "repos/${1}/commits/main" --jq .sha) ||
    die "cannot read the tip of main of ${1}"
  require_sha "$tip"
  printf '%s\n' "$tip"
}

# tip: publish only the tip of main. Kargo promotes the lexically
# newest tag, so tag order is publish order, not commit order. A rerun
# of an old commit's CI would mint a newer tag for old code, and Kargo
# would promote it: a silent rollback. The tip check stops that. A
# commit that is not the tip is skipped; the tip's own CI run releases
# the tip, so no commit is stranded.
tip() {
  local repo=$1 sha=$2 current
  require_sha "$sha"
  current=$(main_tip "$repo")
  if [[ $current != "$sha" ]]; then
    log "${sha} is not the tip of main (${current} is). Only the tip publishes; the tip's own CI run releases it."
    echo "publish=false"
    return 0
  fi
  log "${sha} is the tip of main."
  echo "publish=true"
}

# gate: read the CI run for one main commit. A real dispatch on main
# uses this gate, so a person cannot publish a commit whose tests
# failed. The function publishes only when the commit is the tip of
# main AND the newest CI run for the commit completed with success. A
# missing or incomplete run gives publish=false. The next completed CI
# run decides.
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
    log "The CI gate passed for ${sha}."
    tip "$repo" "$sha"
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
#   1. It lists the release tags that end with the commit's sha6.
#      Two commits can share a six-character prefix, so it checks
#      EVERY candidate, newest first. Verification reads the full
#      40-hex revision annotation, so only a tag of this exact commit
#      verifies.
#   2. If a candidate verifies, it uses that tag again and pushes
#      nothing. The commit is released already. The function refuses
#      to make a second release for it.
#   3. If a candidate cannot be read (verify exit 2), the function
#      stops with no output. A transient registry failure must never
#      mint a second tag for a commit that is released already.
#   4. If there is no candidate, or every candidate positively fails
#      verification (verify exit 1), it makes a new tag from the
#      current UTC minute. Then it pushes the image.
select_tag() {
  local registry=$1 sha=$2
  shift 2
  require_sha "$sha"
  (($# == 1)) || die "select needs exactly one repository"
  local name=$1 short=${sha:0:6}

  local tags
  tags=$(list_release_tags "${registry}/${name}" "$short") ||
    die "cannot read the release tags of ${name}"

  local candidate status
  while IFS= read -r candidate; do
    [[ -n $candidate ]] || continue
    status=0
    verify "${registry}/${name}:${candidate}" "$sha" || status=$?
    case $status in
      0)
        log "${name}:${candidate} is a good release of ${sha}. Push nothing."
        echo "tag=${candidate}"
        echo "push=false"
        return 0
        ;;
      1)
        log "::warning::${name}:${candidate} is not a good release of ${sha}."
        ;;
      *)
        die "cannot read ${name}:${candidate}. Refuse to decide: a registry read failure must not make a second release tag for ${sha}."
        ;;
    esac
  done < <(grep -E "$TAG_RE" <<<"$tags" | sort -ru || true)

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
#
# Exit codes:
#   0  the image verifies
#   1  every read succeeded and the image is positively NOT a release
#      of this commit (wrong tag format, wrong revision, missing
#      platforms or annotations)
#   2  inconclusive: a registry read failed or a reply was not valid
#      JSON. The caller must refuse to decide. A transient read
#      failure must never look like a broken release.
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
  if ! index=$(crane manifest "$ref"); then
    log "${ref}: cannot read the index"
    return 2
  fi
  if ! jq -e . >/dev/null <<<"$index"; then
    log "${ref}: the index is not valid JSON"
    return 2
  fi
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

  local arch manifest config
  for arch in amd64 arm64; do
    if ! manifest=$(crane manifest --platform "linux/${arch}" "$ref"); then
      log "${ref}: cannot read the linux/${arch} manifest"
      return 2
    fi
    if ! jq -e . >/dev/null <<<"$manifest"; then
      log "${ref}: the linux/${arch} manifest is not valid JSON"
      return 2
    fi
    if ! jq -e --arg rev "$sha" --arg src "$SOURCE_URL" '
      .annotations["org.opencontainers.image.revision"] == $rev
      and .annotations["org.opencontainers.image.source"] == $src' >/dev/null <<<"$manifest"; then
      log "${ref}: the linux/${arch} manifest does not have the revision and source annotations"
      return 1
    fi
    if ! config=$(crane config --platform "linux/${arch}" "$ref"); then
      log "${ref}: cannot read the linux/${arch} config"
      return 2
    fi
    if ! jq -e . >/dev/null <<<"$config"; then
      log "${ref}: the linux/${arch} config is not valid JSON"
      return 2
    fi
    if ! jq -e --arg rev "$sha" --arg src "$SOURCE_URL" '
      .config.Labels["org.opencontainers.image.revision"] == $rev
      and .config.Labels["org.opencontainers.image.source"] == $src' >/dev/null <<<"$config"; then
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
  tip) tip "$@" ;;
  select) select_tag "$@" ;;
  verify) verify "$@" ;;
  check-release) check_release "$@" ;;
  *) die "usage: $0 gate|tip|select|verify|check-release ..." ;;
esac
