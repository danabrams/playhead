#!/usr/bin/env bash
# l2f-daily-loop.sh — the autonomous Tier-A corpus-growth daily orchestrator.
#
# Pipeline (each step idempotent; re-running the script the same day is a no-op):
#   1. cd to repo root.
#   2. scripts/l2f-dai-rediff.py — re-download every manifest entry and detect
#      DAI rotations. Writes <id>.dai-rediff.json into TestFixtures/Corpus/Drafts/
#      for rotated episodes.
#   3. For any newly-rotated episode that doesn't yet have a <id>.draft.json
#      (transcript-heuristic draft from l2f-draft-annotation.swift):
#        a. If the transcript JSON is missing, run l2f-local-transcribe.swift to
#           produce one (requires models/ggml-base.en.bin).
#        b. Run l2f-draft-annotation.swift --allow-missing-audio on the transcript
#           to produce <id>.draft.json.
#   4. scripts/l2f-auto-promote.py — triangulate (drafter, pipeline-dump, rediff)
#      and write Annotations/<id>.json files. Conservative high-precision rules
#      documented in the script. NO human review required.
#   5. Append a summary line to TestFixtures/Corpus/Snapshots/daily-loop.log
#      (gitignored via TestFixtures/Corpus/Snapshots/*).
#
# Exit code 0 on success (including "nothing rotated; nothing to do"). Non-zero
# only on unrecoverable errors (e.g. python3 missing, repo root not a git repo,
# rediff crashing). launchd does NOT restart on non-zero by default with our
# plist template, so a hard failure surfaces in `launchctl print` rather than
# pinning CPU.
#
# Usage:
#     scripts/l2f-daily-loop.sh
#
# Install via launchd:
#     launchctl bootstrap gui/$(id -u) scripts/com.playhead.l2f.daily.plist
# (see com.playhead.l2f.daily.plist for the actual schedule.)

set -uo pipefail
# NOTE: we DO NOT `set -e` because we want to keep going after a per-step
# failure and report it in the summary line. Each step's exit code is captured
# explicitly.

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO" || { echo "ERROR: cannot cd to repo root $REPO" >&2; exit 2; }

if [ ! -d ".git" ]; then
  echo "ERROR: $REPO is not a git repository" >&2
  exit 2
fi

LOG="$REPO/TestFixtures/Corpus/Snapshots/daily-loop.log"
mkdir -p "$(dirname "$LOG")"

NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "============================================================" >&2
echo "l2f-daily-loop.sh starting at $NOW" >&2
echo "  repo: $REPO" >&2

# ─── step 1: rediff existing manifest entries ───────────────────────────────
REDIFF_RC=0
REDIFF_DIAG="$REPO/playhead-dogfood-diagnostics-tier-a-rediff.json"
echo "  [1/4] l2f-dai-rediff.py" >&2
python3 scripts/l2f-dai-rediff.py >&2 || REDIFF_RC=$?

ROTATED=0
if [ -f "$REDIFF_DIAG" ]; then
  ROTATED=$(python3 -c "import json,sys; d=json.load(open('$REDIFF_DIAG')); print(d.get('totals',{}).get('rotated',0))" 2>/dev/null || echo 0)
fi
echo "    rediff rc=$REDIFF_RC rotated=$ROTATED" >&2

# ─── step 2: for each rotated episode, ensure a transcript-drafter draft ─────
NEW_DRAFTS=0
DRAFT_FAILURES=0
if [ "$ROTATED" -gt 0 ] && [ -f "$REDIFF_DIAG" ]; then
  # List rotated episode IDs from the rediff diagnostic.
  ROTATED_IDS=$(python3 -c "
import json
d = json.load(open('$REDIFF_DIAG'))
for ep in d.get('episodes', []):
    if ep.get('rotated') and ep.get('ok'):
        print(ep['episodeId'])
" 2>/dev/null)
  for EP_ID in $ROTATED_IDS; do
    DRAFT_PATH="$REPO/TestFixtures/Corpus/Drafts/${EP_ID}.draft.json"
    TRANSCRIPT_PATH="$REPO/TestFixtures/Corpus/Transcripts/${EP_ID}.json"
    if [ -f "$DRAFT_PATH" ]; then
      echo "    [draft-exists] $EP_ID" >&2
      continue
    fi
    if [ ! -f "$TRANSCRIPT_PATH" ]; then
      echo "    [transcribe] $EP_ID" >&2
      if [ ! -f "$REPO/models/ggml-base.en.bin" ]; then
        echo "    SKIP: models/ggml-base.en.bin missing; cannot transcribe $EP_ID" >&2
        DRAFT_FAILURES=$((DRAFT_FAILURES + 1))
        continue
      fi
      AUDIO_PATH="$REPO/TestFixtures/Corpus/Audio/${EP_ID}.mp3"
      if [ ! -f "$AUDIO_PATH" ]; then
        echo "    SKIP: audio missing at TestFixtures/Corpus/Audio/${EP_ID}.mp3" >&2
        DRAFT_FAILURES=$((DRAFT_FAILURES + 1))
        continue
      fi
      swift scripts/l2f-local-transcribe.swift --model models/ggml-base.en.bin \
        "$AUDIO_PATH" >&2 || {
          echo "    FAIL: transcribe failed for $EP_ID" >&2
          DRAFT_FAILURES=$((DRAFT_FAILURES + 1))
          continue
        }
    fi
    echo "    [draft] $EP_ID" >&2
    swift scripts/l2f-draft-annotation.swift --allow-missing-audio \
      "$TRANSCRIPT_PATH" >&2 || {
        echo "    FAIL: draft-annotation failed for $EP_ID" >&2
        DRAFT_FAILURES=$((DRAFT_FAILURES + 1))
        continue
      }
    if [ -f "$DRAFT_PATH" ]; then
      NEW_DRAFTS=$((NEW_DRAFTS + 1))
    fi
  done
else
  echo "  [2/4] no rotations; skipping transcript/drafter step" >&2
fi

# ─── step 3: auto-promote — triangulate and write annotations ───────────────
echo "  [3/4] l2f-auto-promote.py" >&2
PROMOTE_RC=0
python3 scripts/l2f-auto-promote.py >&2 || PROMOTE_RC=$?

PROMOTE_DIAG="$REPO/playhead-dogfood-diagnostics-auto-promote.json"
NEW_ANNOTATIONS=0
if [ -f "$PROMOTE_DIAG" ]; then
  NEW_ANNOTATIONS=$(python3 -c "import json,sys; d=json.load(open('$PROMOTE_DIAG')); print(d.get('totals',{}).get('annotations_written',0))" 2>/dev/null || echo 0)
fi

# ─── step 4: summary log line ────────────────────────────────────────────────
SUMMARY="$NOW rotated=$ROTATED new-drafts=$NEW_DRAFTS new-annotations=$NEW_ANNOTATIONS draft-failures=$DRAFT_FAILURES rediff-rc=$REDIFF_RC promote-rc=$PROMOTE_RC"
echo "  [4/4] $SUMMARY" >&2
echo "$SUMMARY" >> "$LOG"

# Exit code policy:
#  * REDIFF_RC == 1 means at least one per-episode failure — NOT unrecoverable.
#  * REDIFF_RC > 1 means the script itself failed (e.g. manifest missing).
#  * PROMOTE_RC > 0 means the promote script aborted before running.
#  * DRAFT_FAILURES are logged but don't fail the loop (transcript/audio gaps).
if [ "$REDIFF_RC" -gt 1 ] || [ "$PROMOTE_RC" -gt 0 ]; then
  echo "loop FAILED: rediff-rc=$REDIFF_RC promote-rc=$PROMOTE_RC" >&2
  exit 1
fi
echo "loop OK" >&2
exit 0
