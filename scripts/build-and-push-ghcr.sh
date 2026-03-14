#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/build-and-push-ghcr.sh <tag>

Build and push the production worker-plane image to GHCR.

Example:
  scripts/build-and-push-ghcr.sh v0.1.9-0006
USAGE
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

TAG="$1"
IMAGE="ghcr.io/nauron-ai/labaclaw-worker-plane:${TAG}"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required" >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "error: gh CLI is required" >&2
  exit 1
fi

echo "Logging into GHCR..."
gh auth token | docker login ghcr.io -u "$(gh api user --jq .login)" --password-stdin

if docker buildx version >/dev/null 2>&1; then
  echo "Building and pushing linux/amd64 image ${IMAGE} with buildx..."
  docker buildx build --platform linux/amd64 -t "${IMAGE}" --push .
else
  echo "error: docker buildx is required to publish linux/amd64 images for on-prem" >&2
  exit 1
fi

echo "Published ${IMAGE}"
