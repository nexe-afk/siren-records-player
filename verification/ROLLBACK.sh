#!/usr/bin/env bash
set -euo pipefail

TARGET_ROOT="${1:?usage: ROLLBACK.sh /absolute/path/to/copy}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -d "$TARGET_ROOT" ]]; then
  printf 'target does not exist: %s\n' "$TARGET_ROOT" >&2
  exit 2
fi

copy_baseline() {
  local source="$1" target="$2"
  mkdir -p "$(dirname "$TARGET_ROOT/$target")"
  cp "$SCRIPT_DIR/$source" "$TARGET_ROOT/$target"
}

copy_baseline package.json.baseline package.json
copy_baseline music-app.ts.baseline src/music-app.ts
copy_baseline music-player.ts.baseline src/music-player.ts
copy_baseline music-server.mjs.baseline scripts/music-server.mjs
copy_baseline launch-music.mjs.baseline scripts/launch-music.mjs
copy_baseline check-music-launcher.mjs.baseline scripts/check-music-launcher.mjs
rm -f "$TARGET_ROOT/scripts/siren-service.mjs"
printf 'ROLLBACK_STATUS=restored Rhine baseline contracts in %s\n' "$TARGET_ROOT"
