#!/usr/bin/env bash
# download-fixtures.sh
# Verifies PlayheadTests/Fixtures/Corpus/Media/*.wav against
# fixtures-manifest.json and downloads any missing or mismatched fixtures
# from the GitHub Releases host. Until fixtures-v<N> is published, this
# script is a stub that PRINTS the intended download URL and exits with
# a distinct status code so CI can surface the missing-asset posture.
#
# Usage:
#   scripts/download-fixtures.sh                  # verify + (stub) download
#   scripts/download-fixtures.sh --verify-only    # verify only; never download
#
# Exit codes:
#   0  all fixtures present and SHA-256 matches
#   1  unexpected script error (missing tools, unreadable manifest)
#   2  missing or mismatched fixtures AND no release available to download from
#   3  missing or mismatched fixtures; download stubs would run in a real env

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CORPUS_DIR="$REPO_ROOT/PlayheadTests/Fixtures/Corpus"
MEDIA_DIR="$CORPUS_DIR/Media"
MANIFEST="$CORPUS_DIR/fixtures-manifest.json"

VERIFY_ONLY=0
if [[ "${1:-}" == "--verify-only" ]]; then
    VERIFY_ONLY=1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "download-fixtures: python3 is required" >&2
    exit 1
fi
if ! command -v shasum >/dev/null 2>&1 && ! command -v sha256sum >/dev/null 2>&1; then
    echo "download-fixtures: need shasum or sha256sum" >&2
    exit 1
fi

if [[ ! -f "$MANIFEST" ]]; then
    echo "download-fixtures: manifest not found at $MANIFEST" >&2
    exit 1
fi

# Extract (file, sha256) pairs from manifest via python3.
ENTRIES=$(
python3 - "$MANIFEST" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
for f in m.get("fixtures", []):
    print(f"{f['file']}\t{f['sha256']}")
PY
)

sha256_of() {
    local path="$1"
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$path" | awk '{print $1}'
    else
        sha256sum "$path" | awk '{print $1}'
    fi
}

MISSING=0
MISMATCH=0
OK=0

while IFS=$'\t' read -r REL_FILE EXPECTED_SHA; do
    [[ -z "$REL_FILE" ]] && continue
    ABS_FILE="$CORPUS_DIR/$REL_FILE"
    if [[ ! -f "$ABS_FILE" ]]; then
        echo "MISSING  $REL_FILE"
        MISSING=$((MISSING+1))
        continue
    fi
    ACTUAL=$(sha256_of "$ABS_FILE")
    if [[ "$ACTUAL" != "$EXPECTED_SHA" ]]; then
        echo "MISMATCH $REL_FILE"
        echo "  expected: $EXPECTED_SHA"
        echo "  actual:   $ACTUAL"
        MISMATCH=$((MISMATCH+1))
    else
        OK=$((OK+1))
    fi
done <<< "$ENTRIES"

echo "download-fixtures: ok=$OK missing=$MISSING mismatch=$MISMATCH"

if [[ $((MISSING + MISMATCH)) -eq 0 ]]; then
    exit 0
fi

# Something is off. If --verify-only, stop here.
if [[ "$VERIFY_ONLY" == "1" ]]; then
    exit 2
fi

# STUB: would download from GitHub Releases. No release exists yet; this
# block exists so CI wiring can be tested end-to-end and a real release can
# swap the curl commands in without restructuring the script.
FIXTURES_VERSION="${FIXTURES_VERSION:-v1}"
REPO_SLUG="${FIXTURES_REPO:-playhead/playhead}"
BASE_URL="https://github.com/$REPO_SLUG/releases/download/fixtures-$FIXTURES_VERSION"

echo "download-fixtures: STUB — would download from $BASE_URL"
while IFS=$'\t' read -r REL_FILE EXPECTED_SHA; do
    [[ -z "$REL_FILE" ]] && continue
    ABS_FILE="$CORPUS_DIR/$REL_FILE"
    if [[ ! -f "$ABS_FILE" ]] || [[ "$(sha256_of "$ABS_FILE")" != "$EXPECTED_SHA" ]]; then
        echo "  would curl -L $BASE_URL/$(basename "$REL_FILE") -o $ABS_FILE"
    fi
done <<< "$ENTRIES"

exit 3
