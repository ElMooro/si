#!/usr/bin/env bash
# Compatibility entry point: Khalid uses the same exact-artifact, read-only
# candidate validation and revision-pinned promotion as the other governed engines.
# Normal publication happens through the validated :live schedule after release.
set -euo pipefail
if [ "$#" -ne 4 ]; then
  echo "usage: $0 FUNCTION REGION TMP_DIR ENGINE_DIR" >&2
  exit 2
fi
exec bash "$(dirname "${BASH_SOURCE[0]}")/deploy_validated_candidate.sh" \
  "$1" "$2" "$3" "$4/config.json" "3.0.0"
