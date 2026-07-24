#!/usr/bin/env bash
#
# fast-gate.sh — run the PlayheadFastTests gate with BOUNDED test parallelism.
#
# playhead-qt8y: the default `xcodebuild test -testPlan PlayheadFastTests`
# invocation leaves parallelization unbounded, so Xcode clones the simulator up
# to (core-count) times — each clone is a full runtime + a ~1 GB Playhead test
# host. On this 16 GB box that drives free memory to ~tens of MB and the run is
# killed mid-suite (`** BUILD INTERRUPTED **`, signal 144) with no test failure —
# pure resource exhaustion, guaranteed when any second xcodebuild runs too.
#
# This wrapper caps the number of parallel test-runner clones
# (`-parallel-testing-worker-count`), which bounds peak memory to ~N test hosts
# while KEEPING Swift Testing's cheap in-process concurrency inside each host —
# so the ~8,300-test Swift Testing bulk stays fast and only the clone count (the
# memory driver) is bounded. A capped run that COMPLETES reliably beats an
# unbounded run that OOMs and must be retried.
#
# It also recovers from a wedged simulator (`Mach error -308` / "Failed to
# install or launch the test runner") by shutting down, erasing, re-booting the
# destination sim once and retrying — the documented recovery for this env.
#
# Env overrides:
#   PLAYHEAD_TEST_WORKERS  parallel worker (clone) cap (default: 2)
#   PLAYHEAD_DEST          xcodebuild -destination (default: iPhone 17 sim)
#   PLAYHEAD_DERIVED       -derivedDataPath (default: .derivedData)
#   PLAYHEAD_SIM_ID        simulator UDID for -308 recovery (optional; derived
#                          from PLAYHEAD_DEST's id=... when present)
#   DEVELOPER_DIR          select a toolchain (e.g. the Xcode 27 beta)
#
# Any extra args are forwarded to xcodebuild (e.g. -only-testing:...).

set -uo pipefail
cd "$(dirname "$0")/.."

WORKERS="${PLAYHEAD_TEST_WORKERS:-2}"
DEST="${PLAYHEAD_DEST:-platform=iOS Simulator,name=iPhone 17}"
DERIVED="${PLAYHEAD_DERIVED:-.derivedData}"

# Best-effort extraction of the sim UDID from the destination (for -308 recovery)
# unless PLAYHEAD_SIM_ID overrides it. A name-only destination leaves it empty and
# recovery is skipped (the run simply fails loudly instead).
SIM_ID="${PLAYHEAD_SIM_ID:-}"
if [ -z "$SIM_ID" ]; then
  case "$DEST" in
    *id=*) SIM_ID="$(printf '%s' "$DEST" | sed -n 's/.*id=\([0-9A-Fa-f-]*\).*/\1/p')" ;;
  esac
fi

run_gate () {
  xcodebuild test \
    -scheme Playhead \
    -testPlan PlayheadFastTests \
    -destination "$DEST" \
    -derivedDataPath "$DERIVED" \
    -parallel-testing-worker-count "$WORKERS" \
    "$@"
}

echo "fast-gate: workers=$WORKERS dest=$DEST derived=$DERIVED"
LOG="$(mktemp -t fast-gate.XXXXXX)"
run_gate "$@" 2>&1 | tee "$LOG"
RC="${PIPESTATUS[0]}"

# Wedged-simulator recovery: shut down / erase / boot the sim once, then retry.
# Covers both the classic CoreSimulator server death (-308 / launch failure) and
# the bootstrap-crash variant ("Early unexpected exit … crashed with signal term
# before establishing connection") seen when a prior run's sim state is left wedged.
if grep -qE "Mach error -308|Failed to install or launch the test runner|Early unexpected exit|signal term before establishing connection" "$LOG" && [ -n "$SIM_ID" ]; then
  echo "fast-gate: wedged simulator detected — recovering sim $SIM_ID and retrying once"
  xcrun simctl shutdown "$SIM_ID" 2>/dev/null || true
  xcrun simctl erase "$SIM_ID" 2>/dev/null || true
  xcrun simctl boot "$SIM_ID" 2>/dev/null || true
  sleep 6
  run_gate "$@"
  RC=$?
fi

rm -f "$LOG"
exit "$RC"
