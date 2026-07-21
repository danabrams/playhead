#!/bin/bash
# Playhead — stage the v2 ad-detection specialist model into the app bundle.
#
# The model weights (~321 MB) are deliberately kept OUT of git — plain git
# history bloat compounds across retrains — so a fresh checkout does NOT
# have them. This script copies the exported model from its canonical source
# into the in-repo resource path that `project.yml` bundles as a folder
# reference, so the app can load it at:
#
#     Bundle.main.resourceURL!.appending(path: "qwen3_0_6b_4bit_dynamic_ft_v2")
#
# It is idempotent: if the staged copy already matches the source (by SHA-256
# of the `.aimodel/main.mlirb` weights blob) it does nothing but report.
#
# Usage:
#   scripts/fetch-specialist-model.sh            # stage (idempotent)
#   PLAYHEAD_MODEL_SRC=/path/... scripts/fetch-specialist-model.sh
#
# playhead-b6jq PR2 — packaging only, no inference. See
# Playhead/Resources/Models/README.md.
set -euo pipefail

MODEL_NAME="qwen3_0_6b_4bit_dynamic_ft_v2"
AIMODEL_REL="${MODEL_NAME}.aimodel"
WEIGHTS_REL="${AIMODEL_REL}/main.mlirb"

# Repo root = parent of this script's dir (scripts/..).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Canonical export directory (override via env for retrains / other machines).
SRC="${PLAYHEAD_MODEL_SRC:-/Users/dabrams/coreai-spike/exported/${MODEL_NAME}}"
DEST_PARENT="$REPO_ROOT/Playhead/Resources/Models"
DEST="$DEST_PARENT/$MODEL_NAME"

fail() { printf 'error: %s\n' "$*" >&2; exit 1; }

sha256() { shasum -a 256 "$1" | awk '{print $1}'; }

# --- 1. Verify the source exists (fail loud with instructions) -------------
[ -d "$SRC" ] || fail "canonical model source not found: $SRC
  This machine may not have the exported model, or it lives elsewhere.
  Re-run pointing at the export dir, e.g.:
    PLAYHEAD_MODEL_SRC=/path/to/${MODEL_NAME} scripts/fetch-specialist-model.sh
  The export dir must contain: ${AIMODEL_REL}/, tokenizer/, metadata.json"

[ -f "$SRC/$WEIGHTS_REL" ] || fail "source is missing the weights blob: $SRC/$WEIGHTS_REL
  The export at $SRC looks incomplete. Re-export the model or fix the path."
[ -d "$SRC/tokenizer" ]    || fail "source is missing tokenizer/: $SRC/tokenizer"
[ -f "$SRC/metadata.json" ] || fail "source is missing metadata.json: $SRC/metadata.json"

SRC_SUM="$(sha256 "$SRC/$WEIGHTS_REL")"

# --- 2. Idempotent skip: staged copy already matches ------------------------
if [ -f "$DEST/$WEIGHTS_REL" ]; then
  DEST_SUM="$(sha256 "$DEST/$WEIGHTS_REL")"
  if [ "$DEST_SUM" = "$SRC_SUM" ]; then
    printf 'already staged: %s\n' "$DEST"
    printf '  size:     %s\n' "$(du -sh "$DEST" | awk '{print $1}')"
    printf '  weights:  %s\n' "$WEIGHTS_REL"
    printf '  sha256:   %s\n' "$SRC_SUM"
    exit 0
  fi
  printf 'staged copy is stale (sha mismatch); restaging...\n'
fi

# --- 3. Stage: clean copy of the whole model folder ------------------------
printf 'staging %s -> %s\n' "$SRC" "$DEST"
mkdir -p "$DEST_PARENT"
rm -rf "$DEST"
# Copy the whole export dir contents (aimodel/ + tokenizer/ + metadata.json).
cp -R "$SRC" "$DEST"

# --- 4. Verify the copy is faithful ----------------------------------------
[ -f "$DEST/$WEIGHTS_REL" ] || fail "copy failed: $DEST/$WEIGHTS_REL missing after cp"
COPY_SUM="$(sha256 "$DEST/$WEIGHTS_REL")"
[ "$COPY_SUM" = "$SRC_SUM" ] || fail "copy corrupted: sha256 mismatch
  source: $SRC_SUM
  staged: $COPY_SUM"

# --- 5. Report provenance ---------------------------------------------------
printf 'staged OK\n'
printf '  dest:     %s\n' "$DEST"
printf '  size:     %s\n' "$(du -sh "$DEST" | awk '{print $1}')"
printf '  weights:  %s\n' "$WEIGHTS_REL"
printf '  sha256:   %s\n' "$SRC_SUM"
