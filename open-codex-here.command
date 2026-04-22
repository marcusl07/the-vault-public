#!/bin/zsh

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

exec /opt/homebrew/bin/codex -C "$SCRIPT_DIR" "$@"
