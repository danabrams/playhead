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
# THE VERDICT IS ABOUT THE DIFF, NOT THE COUNT (playhead-voez). This gate is
# RED on a clean checkout — dozens of load-sensitive suites blow their 60s time
# limits under the full run's own concurrency, and CLAUDE.md records a deliberate
# decision not to PerfGate them out (they test real behaviours; moving them is a
# coverage call for Dan). The consequence was that the terminal exit code could
# not distinguish "your change broke something" from "the usual ones", so every
# bead hand-diffed against a baseline nobody had written down — which is how
# playhead-ynmk (#313) merged as "looks like the usual flakes".
#
# So the known-broken set is now COMMITTED, in `scripts/gate-baseline.<plan>.json`,
# and this script's exit code is a statement about the difference:
#
#     RED (N known / 0 new)     -> exit 0
#     RED (N known / 2 NEW)     -> exit 65, both named
#     a baseline test PASSES    -> exit 65, named (Dan: the baseline is EXACT)
#
# Refresh it with `scripts/fast-gate.sh --accept-baseline`, and justify the diff
# in the commit message: the file is the record of what is known-broken, so a
# shrinking diff is good news and a growing one needs a reason.
#
# The check applies ONLY to a full-plan run. A selective invocation
# (`-only-testing:`/`-skip-testing:`, which is how scripts/mutation-battery.sh
# drives this script) is a different population — the baseline names hundreds of
# tests it never runs — so the check is skipped and the raw exit code passes
# through untouched. See scripts/gate_baseline.py for the whole design.
#
# Env overrides:
#   PLAYHEAD_DEST        xcodebuild -destination (default: iPhone 17 sim by name)
#   PLAYHEAD_DERIVED     -derivedDataPath (default: .derivedData)
#   PLAYHEAD_PLAN        test plan name (default: PlayheadFastTests)
#   PLAYHEAD_BUILD_JOBS  concurrent compile jobs cap (default: 4)
#   PLAYHEAD_SIM_ID      simulator UDID for -308 recovery (else parsed from DEST id=)
#   PLAYHEAD_SKIP_BASELINE=1  bypass the baseline verdict (raw exit code)
#   PLAYHEAD_GATE_BASELINE    baseline file path override
#   DEVELOPER_DIR        toolchain select (e.g. the Xcode 27 beta)
#
# Extra args are forwarded to xcodebuild (e.g. -only-testing:...).
# `--accept-baseline` is consumed here and never forwarded.

set -uo pipefail
cd "$(dirname "$0")/.."

DEST="${PLAYHEAD_DEST:-platform=iOS Simulator,name=iPhone 17}"
DERIVED="${PLAYHEAD_DERIVED:-.derivedData}"
PLAN="${PLAYHEAD_PLAN:-PlayheadFastTests}"
JOBS="${PLAYHEAD_BUILD_JOBS:-4}"

# playhead-voez: split our own flag off the xcodebuild passthrough, and notice
# whether this is a full-plan run or a selective one. A selective run must leave
# the exit code exactly as xcodebuild set it — mutation-battery.sh reads it.
ACCEPT_BASELINE=0
SELECTIVE=0
FORWARD=()
for arg in "$@"; do
  case "$arg" in
    --accept-baseline) ACCEPT_BASELINE=1 ;;
    -only-testing*|-skip-testing*) SELECTIVE=1; FORWARD+=("$arg") ;;
    *) FORWARD+=("$arg") ;;
  esac
done
set -- ${FORWARD[@]+"${FORWARD[@]}"}

BASELINE_FILE="${PLAYHEAD_GATE_BASELINE:-scripts/gate-baseline.${PLAN}.json}"

SIM_ID="${PLAYHEAD_SIM_ID:-}"
if [ -z "$SIM_ID" ]; then
  case "$DEST" in
    *id=*) SIM_ID="$(printf '%s' "$DEST" | sed -n 's/.*id=\([0-9A-Fa-f-]*\).*/\1/p')" ;;
  esac
fi

# Snag 1: bootstrap a fresh worktree — link the gitignored model, then regenerate
# the project + scheme. Only runs when the scheme is missing the plan.
# playhead-ia2s: lint BEFORE building. It costs ~0.2s warm / ~2.4s cold and
# needs no project, so catching a violation here saves the ~3 minutes the test
# run would have spent before telling you the same thing.
#
# Exit 70 from lint.sh means INFRASTRUCTURE (swiftlint or Xcode missing), not
# dirty code — warn and carry on rather than failing a whole test gate over a
# missing dev tool. Any other non-zero is a real violation and stops the gate.
# Escape hatch: PLAYHEAD_SKIP_LINT=1.
if [ "${PLAYHEAD_SKIP_LINT:-0}" != "1" ]; then
  ./scripts/lint.sh
  LINT_RC=$?
  if [ "$LINT_RC" -eq 70 ]; then
    echo "fast-gate: WARNING — lint could not run (exit 70); continuing to tests"
  elif [ "$LINT_RC" -ne 0 ]; then
    echo "fast-gate: lint FAILED (exit $LINT_RC) — stopping before the build."
    echo "fast-gate: fix the violations, or re-run with PLAYHEAD_SKIP_LINT=1 to bypass."
    exit "$LINT_RC"
  fi
fi

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
run_gate ${1+"$@"} 2>&1 | tee "$LOG"
RC="${PIPESTATUS[0]}"

# Snag 3: wedged-simulator recovery — shut down / erase / boot once, then retry.
# The retry APPENDS to the same log so the baseline check below sees the attempt
# that actually counted. gate_baseline.py cuts everything before the banner, so
# attempt 1's casualties — tests that were mid-flight when the sim died — can
# never union with attempt 2 and manufacture failures out of an artefact.
if grep -qE "Mach error -308|Failed to install or launch the test runner|Early unexpected exit|signal term before establishing connection" "$LOG" && [ -n "$SIM_ID" ]; then
  echo "fast-gate: wedged simulator — recovering sim $SIM_ID and retrying once"
  xcrun simctl shutdown "$SIM_ID" 2>/dev/null || true
  xcrun simctl erase "$SIM_ID" 2>/dev/null || true
  xcrun simctl boot "$SIM_ID" 2>/dev/null || true
  sleep 6
  run_gate "$@" 2>&1 | tee -a "$LOG"
  RC="${PIPESTATUS[0]}"
fi

# ---------------------------------------------------------------------------
# playhead-voez: the baseline verdict.
# ---------------------------------------------------------------------------
finish () { rm -f "$LOG"; exit "$1"; }

if [ "$ACCEPT_BASELINE" -eq 1 ]; then
  if [ "$SELECTIVE" -eq 1 ]; then
    echo "fast-gate: --accept-baseline REFUSED — a selective run (-only-testing/" >&2
    echo "fast-gate: -skip-testing) sees a fraction of the plan, and accepting it" >&2
    echo "fast-gate: would delete every baseline entry the filter excluded." >&2
    finish 2
  fi
  python3 scripts/gate_baseline.py accept \
    --log "$LOG" --baseline "$BASELINE_FILE" --plan "$PLAN"
  finish $?
fi

if [ "${PLAYHEAD_SKIP_BASELINE:-0}" = "1" ]; then
  echo "fast-gate: baseline check bypassed (PLAYHEAD_SKIP_BASELINE=1) — raw exit $RC"
  finish "$RC"
fi

if [ "$SELECTIVE" -eq 1 ]; then
  echo "fast-gate: baseline check SKIPPED — selective run, a different population"
  echo "fast-gate: from the recorded plan. Raw xcodebuild exit $RC stands."
  finish "$RC"
fi

python3 scripts/gate_baseline.py check \
  --log "$LOG" --baseline "$BASELINE_FILE" --plan "$PLAN"
CHECK_RC=$?
case "$CHECK_RC" in
  0) finish 0 ;;
  1) finish 65 ;;
  # Could not evaluate (no baseline file, wrong plan, or a log with no terminal
  # verdict — a build failure or an OOM kill). The check makes no claim, so
  # xcodebuild's own verdict stands rather than being laundered into a pass.
  *) [ "$RC" -eq 0 ] && finish 0; finish "$RC" ;;
esac
