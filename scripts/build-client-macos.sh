#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
CLIENT_DIR="${ROOT_DIR}/clients"

MODE="release"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug)
      MODE="debug"
      shift
      ;;
    --release)
      MODE="release"
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: scripts/build-client-macos.sh [--release|--debug]

Builds the Rust p2p-gui client for macOS.
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

cd "${CLIENT_DIR}"

if [[ "${MODE}" == "release" ]]; then
  cargo build --release -p p2p-gui
  BINARY="${CLIENT_DIR}/target/release/p2p-gui"
  OUTPUT_DIR="${CLIENT_DIR}/target/release"
else
  cargo build -p p2p-gui
  BINARY="${CLIENT_DIR}/target/debug/p2p-gui"
  OUTPUT_DIR="${CLIENT_DIR}/target/debug"
fi

APP_DIR="${OUTPUT_DIR}/P2P Signaling.app"
CONTENTS_DIR="${APP_DIR}/Contents"
rm -rf "${APP_DIR}"
mkdir -p "${CONTENTS_DIR}/MacOS"
cp "${ROOT_DIR}/clients/p2p-gui/macos/Info.plist" "${CONTENTS_DIR}/Info.plist"
cp "${BINARY}" "${CONTENTS_DIR}/MacOS/p2p-gui"

SIGN_IDENTITY="${P2P_MACOS_CODESIGN_IDENTITY:--}"
codesign --force --sign "${SIGN_IDENTITY}" "${APP_DIR}"
codesign --verify --strict "${APP_DIR}"

echo "Built client: ${APP_DIR}"
