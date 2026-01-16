#!/usr/bin/env bash
# Build Docker images for amd64 and/or arm64 using buildx.
#
# Usage:
#   ./build-docker.sh           # Build both architectures
#   ./build-docker.sh --amd64   # Build amd64 only (loads to docker)
#   ./build-docker.sh --arm64   # Build arm64 only (loads to docker)
#   ./build-docker.sh --push    # Build both and push multi-arch manifest
#
# Environment:
#   IMAGE_NAME  Image name (default: fluent-bit-clp-s3-v2)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-fluent-bit-clp-s3-v2}"

PUSH=false
PLATFORMS="linux/amd64,linux/arm64"

for arg in "$@"; do
    case $arg in
        --push)   PUSH=true ;;
        --amd64)  PLATFORMS="linux/amd64" ;;
        --arm64)  PLATFORMS="linux/arm64" ;;
        -h|--help) sed -n '2,11p' "$0" | cut -c3-; exit 0 ;;
    esac
done

# Ensure buildx builder exists
BUILDER="fluent-bit-clp"
docker buildx inspect "$BUILDER" &>/dev/null || \
    docker buildx create --name "$BUILDER" --bootstrap
docker buildx use "$BUILDER"

echo "Building ${IMAGE_NAME} for: ${PLATFORMS}"

ARGS=(--file "${ROOT_DIR}/plugins/out_clp_s3_v2/Dockerfile" --platform "$PLATFORMS" -t "${IMAGE_NAME}:latest")

if $PUSH; then
    ARGS+=(--push)
elif [[ "$PLATFORMS" == *,* ]]; then
    echo "Note: Multi-arch builds stay in cache. Use --push or single arch to load."
    ARGS+=(--output type=image,push=false)
else
    ARGS+=(--load)
fi

docker buildx build "${ARGS[@]}" "$ROOT_DIR"
echo "Done."
