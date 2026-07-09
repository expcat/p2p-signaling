#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
CLIENT_DIR="${ROOT_DIR}/clients"

SERVER="${P2P_SIGNALING_SERVER:-p2p-signaling.yizhe.studio}"
ROOM="${P2P_SIGNALING_ROOM:-}"
ROLE="${P2P_SIGNALING_ROLE:-host}"
MODE="debug"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server|-s)
      SERVER="${2:?--server requires a value}"
      shift 2
      ;;
    --room|-r)
      ROOM="${2:?--room requires a value}"
      shift 2
      ;;
    --role)
      ROLE="${2:?--role requires a value}"
      shift 2
      ;;
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
Usage: scripts/start-client-macos.sh [--server SERVER] [--room ROOM] [--role host|guest] [--release|--debug]

--room is only used by guests; the host's room code is assigned by the server.

Environment defaults:
  P2P_SIGNALING_SERVER=p2p-signaling.yizhe.studio
  P2P_SIGNALING_ROLE=host
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
else
  cargo build -p p2p-gui
  BINARY="${CLIENT_DIR}/target/debug/p2p-gui"
fi

ARGS=(--server "${SERVER}" --role "${ROLE}")
if [[ -n "${ROOM}" ]]; then
  ARGS+=(--room "${ROOM}")
fi

exec "${BINARY}" "${ARGS[@]}"
