#!/bin/bash
# Build and push a multi-arch hoglite image.
# Usage: ECR_REPO=<registry>/hoglite ./build-image.sh v1.0.0
set -euo pipefail
TAG=${1:?usage: build-image.sh <tag>}
REPO=${ECR_REPO:?set ECR_REPO to the image repository, e.g. <account>.dkr.ecr.<region>.amazonaws.com/hoglite}
REGISTRY=${REPO%%/*}

for arch in amd64 arm64; do
  mkdir -p bin-$arch
  CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -o bin-$arch/hoglited ./cmd/hoglited
  # refuse to package the wrong arch (this bug shipped once)
  file bin-$arch/hoglited | grep -qi "$( [ $arch = amd64 ] && echo x86-64 || echo aarch64 )" \
    || { echo "bin-$arch/hoglited has the wrong architecture"; exit 1; }
done

aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY"
docker buildx build --platform linux/amd64,linux/arm64 -f Containerfile -t "$REPO:$TAG" --push .
echo pushed "$REPO:$TAG"
