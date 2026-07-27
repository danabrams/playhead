#!/usr/bin/env bash
#
# fast-gate.sh — run the PlayheadFastTests gate reliably, one command.
#
# playhead-qt8y / playhead-ekpn. This wrapper exists because the raw
# `xcodebuild test -testPlan PlayheadFastTests` invocation hits several recurring
# snags on this setup, and because the machine has a hard 16 GB ceiling:
#
#   1. FRESH-WORKTREE BOOTSTRAP: a just-created worktree lacks (a) the gitignored
#      on-device model directory (a large blob kept only in the main checkout and
#      symlinked into each worktree) and (b) an xcodegen-generated scheme wired to
#      the test plans. Without the model, `xcodegen generate` fails spec
#      validation; without the scheme, xcodebuild fails with `Scheme "Playhead"
#      does not have an associated test plan`. This script links the model from
#      the main checkout and regenerates automatically.
#
#   2. COLD-BUILD OOM: a fresh worktree builds the whole project from an empty
#      derivedData — the parallel `swiftc` compile is a memory driver independent
#      of test parallelism, heavy enough to get xcodebuild OOM-killed
#      (`Killed: 9`) on this box. This script caps concurrent compile jobs
#      (`-jobs`, default 4) to keep headroom; incremental rebuilds barely notice.
#
#   3. WEDGED SIMULATOR: a `Mach error -308` / "Failed to install or launch the
#      test runner" (or a sim bootstrap crash) leaves the sim wedged. This script
#      shuts down / erases / boots the destination sim once and retries.
#
# CLONE PARALLELISM IS DELIBERATELY NOT USED (playhead-ekpn). Passing
# `-parallel-testing-worker-count >=2` makes Xcode spawn simulator CLONES, and
# the clone helper resolves `simctl` via the GLOBAL `xcode-select` — which on
# this box is /Library/Developer/CommandLineTools (no `simctl`), so cloning dies
# with `xcrun: error: unable to find utility "simctl"` (exit 65, ~18s, zero tests
# run). `DEVELOPER_DIR` fixes `xcodebuild` itself but NOT the clone helper (the
# 2026-07-16 xcode-select gotcha; enabling clones would need a global
# `xcode-select -s` change = a system-wide, sudo, NEEDS-DAN decision). So this
# runs SINGLE-HOST — the same config as the working default gate: XCTest serial
# + Swift Testing's cheap in-process concurrency (the ~8,300-test bulk stays
# fast in one process).
#
# The dominant OOM cause is running TWO gates/builds at once. Do NOT launch a
# second gate/build alongside this — run gates ONE AT A TIME.
#
# Env overrides:
#   PLAYHEAD_DEST        xcodebuild -destination (default: iPhone 17 sim by name)
#   PLAYHEAD_DERIVED     -derivedDataPath (default: .derivedData)
#   PLAYHEAD_PLAN        test plan name (default: PlayheadFastTests)
#   PLAYHEAD_BUILD_JOBS  concurrent compile jobs cap (default: 4)
#   PLAYHEAD_SIM_ID      simulator UDID for -308 recovery (else parsed from DEST id=)
#   DEVELOPER_DIR        toolchain select (e.g. the Xcode 27 beta)
#
# Extra args are forwarded to xcodebuild (e.g. -only-testing:...).

set -uo pipefail
cd "$(dirname "$0")/.."

DEST="${PLAYHEAD_DEST:-platform=iOS Simulator,name=iPhone 17}"
DERIVED="${PLAYHEAD_DERIVED:-.derivedData}"
PLAN="${PLAYHEAD_PLAN:-PlayheadFastTests}"
JOBS="${PLAYHEAD_BUILD_JOBS:-4}"

SIM_ID="${PLAYHEAD_SIM_ID:-}"
if [ -z "$SIM_ID" ]; then
  case "$DEST" in
    *id=*) SIM_ID="$(printf '%s' "$DEST" | sed -n 's/.*id=\([0-9A-Fa-f-]*\).*/\1/p')" ;;
  esac
fi

# Snag 1: bootstrap a fresh worktree — link the gitignored model, then regenerate
# the project + scheme. Only runs when the scheme is missing the plan.
SCHEME=Playhead.xcodeproj/xcshareddata/xcschemes/Playhead.xcscheme
if [ ! -f "$SCHEME" ] || ! grep -q "${PLAN}.xctestplan" "$SCHEME" 2>/dev/null; then
  MODEL_REL="Playhead/Resources/Models/qwen3_0_6b_4bit_dynamic_ft_v2"
  if [ ! -e "$MODEL_REL" ]; then
    MAIN_ROOT="$(dirname "$(git rev-parse --git-common-dir 2>/dev/null)")"
    if [ -n "$MAIN_ROOT" ] && [ -d "$MAIN_ROOT/$MODEL_REL" ]; then
      echo "fast-gate: linking gitignored model from $MAIN_ROOT"
      ln -s "$MAIN_ROOT/$MODEL_REL" "$MODEL_REL" 2>/dev/null || true
    fi
  fi
  # Resolve xcodegen robustly — a detached/non-login shell often lacks
  # /opt/homebrew/bin on PATH.
  XCODEGEN="$(command -v xcodegen 2>/dev/null || true)"
  for cand in /opt/homebrew/bin/xcodegen /usr/local/bin/xcodegen; do
    [ -n "$XCODEGEN" ] && break
    [ -x "$cand" ] && XCODEGEN="$cand"
  done
  if [ -z "$XCODEGEN" ]; then
    echo "fast-gate: scheme missing plan '${PLAN}' and xcodegen not found — run 'xcodegen generate' manually"
    exit 70
  fi
  echo "fast-gate: bootstrapping scheme for '${PLAN}' — $XCODEGEN generate"
  "$XCODEGEN" generate || { echo "fast-gate: xcodegen generate FAILED (is the model linked?)"; exit 70; }
fi

run_gate () {
  xcodebuild test \
    -scheme Playhead \
    -testPlan "$PLAN" \
    -destination "$DEST" \
    -derivedDataPath "$DERIVED" \
    -jobs "$JOBS" \
    "$@"
}

echo "fast-gate: plan=$PLAN dest=$DEST derived=$DERIVED jobs=$JOBS (single-host; no clone parallelism)"
LOG="$(mktemp -t fast-gate.XXXXXX)"
run_gate "$@" 2>&1 | tee "$LOG"
RC="${PIPESTATUS[0]}"

# Snag 3: wedged-simulator recovery — shut down / erase / boot once, then retry.
if grep -qE "Mach error -308|Failed to install or launch the test runner|Early unexpected exit|signal term before establishing connection" "$LOG" && [ -n "$SIM_ID" ]; then
  echo "fast-gate: wedged simulator — recovering sim $SIM_ID and retrying once"
  xcrun simctl shutdown "$SIM_ID" 2>/dev/null || true
  xcrun simctl erase "$SIM_ID" 2>/dev/null || true
  xcrun simctl boot "$SIM_ID" 2>/dev/null || true
  sleep 6
  run_gate "$@"
  RC=$?
fi

rm -f "$LOG"
exit "$RC"
