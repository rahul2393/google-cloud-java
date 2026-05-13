#!/usr/bin/env bash
set -euo pipefail

# Run from google-cloud-java repo root.
# Builds release-version images from Maven Central and one local-source image from current checkout.

IMAGE_REPO="${IMAGE_REPO:-us-central1-docker.pkg.dev/span-cloud-testing/irahul-images/stale-query}"
RELEASE_VERSIONS="${RELEASE_VERSIONS:-6.114.0 6.117.0}"
LOCAL_TAG="${LOCAL_TAG:-directpath-fixes-all}"
DOCKERFILE="${DOCKERFILE:-java-spanner/Dockerfile.prober}"
PUSH=false
BUILD_LOCAL=true
BUILD_RELEASE=true

usage() {
  cat <<USAGE
Usage: $0 [--push] [--no-local] [--no-release]

Env:
  IMAGE_REPO         default: ${IMAGE_REPO}
  RELEASE_VERSIONS  default: "${RELEASE_VERSIONS}"
  LOCAL_TAG         default: ${LOCAL_TAG}
  DOCKERFILE        default: ${DOCKERFILE}

Examples:
  java-spanner/scripts/build-prober-images.sh
  PUSH one local + two release images:
    java-spanner/scripts/build-prober-images.sh --push
  Only 6.117.0:
    RELEASE_VERSIONS="6.117.0" java-spanner/scripts/build-prober-images.sh --no-local
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --push) PUSH=true ;;
    --no-local) BUILD_LOCAL=false ;;
    --no-release) BUILD_RELEASE=false ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
  shift
done

if [[ ! -f "${DOCKERFILE}" || ! -d java-spanner || ! -d sdk-platform-java ]]; then
  echo "Run from google-cloud-java repo root. Missing ${DOCKERFILE}, java-spanner, or sdk-platform-java." >&2
  exit 1
fi

build_image() {
  local tag="$1"
  shift
  echo "------------------------------------------------------------------------"
  echo "Building ${tag}"
  echo "------------------------------------------------------------------------"
  docker build -f "${DOCKERFILE}" -t "${tag}" "$@" .
  if [[ "${PUSH}" == "true" ]]; then
    docker push "${tag}"
  fi
}

if [[ "${BUILD_RELEASE}" == "true" ]]; then
  for version in ${RELEASE_VERSIONS}; do
    build_image "${IMAGE_REPO}:spanner-${version}" \
      --target runtime-release \
      --build-arg "SPANNER_VERSION=${version}"
  done
fi

if [[ "${BUILD_LOCAL}" == "true" ]]; then
  build_image "${IMAGE_REPO}:${LOCAL_TAG}" \
    --target runtime-local
fi

echo "Done. Images:"
if [[ "${BUILD_RELEASE}" == "true" ]]; then
  for version in ${RELEASE_VERSIONS}; do
    echo "  ${IMAGE_REPO}:spanner-${version}"
  done
fi
if [[ "${BUILD_LOCAL}" == "true" ]]; then
  echo "  ${IMAGE_REPO}:${LOCAL_TAG}"
fi
