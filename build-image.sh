#!/bin/bash
# Build and push a multi-arch hoglite image.
# Usage: ECR_REPO=<registry>/hoglite ./build-image.sh v1.0.0
# The Containerfile compiles hoglited from source for each platform.
set -euo pipefail
TAG=${1:?usage: build-image.sh <tag>}
REPO=${ECR_REPO:?set ECR_REPO to the image repository, e.g. <account>.dkr.ecr.<region>.amazonaws.com/hoglite}
REGISTRY=${REPO%%/*}

aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY"
docker buildx build --platform linux/amd64,linux/arm64 -f Containerfile -t "$REPO:$TAG" --push .
echo pushed "$REPO:$TAG"
