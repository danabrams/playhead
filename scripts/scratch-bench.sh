#!/usr/bin/env bash
#
# scratch-bench.sh — the reproducible before/after disk benchmark for playhead-cgka.
#
# Runs a FIXED, store-heavy subset of PlayheadTests under scratch-sampler.sh and
# prints the report. The subset is deliberately not "whatever ran today": the
# claim being tested is that a test's scratch is released when the TEST ends
# rather than when the PROCESS ends, and that claim is only falsifiable against
# the same population before and after.
#
# Why these suites: every one of them is a heavy `makeTestStore` /
# `makeTempDir` caller. MEASURED 2026-08-02 on a full-plan run, a migrated
# AnalysisStore directory is 728 KiB (696 KiB analysis.sqlite + 32 KiB -shm),
# and 2,591 of the 2,847 leftover directories were exactly that shape — so the
# store factories, not the bare temp dirs, are what the meter is pointed at.
#
#   scripts/scratch-bench.sh --label before
#   …apply the fix…
#   scripts/scratch-bench.sh --label after
#
# Output lands in .logs/scratch-bench-<label>.{jsonl,log} and the report is
# printed at the end. Exit status is xcodebuild's.
#
# Runs ONE xcodebuild. Never start a second gate alongside it — this box OOMs.

set -uo pipefail
cd "$(dirname "$0")/.."

LABEL="run"
INTERVAL=5
GUARD_FREE_MIB="${PLAYHEAD_GUARD_FREE_MIB:-2500}"
while [ $# -gt 0 ]; do
  case "$1" in
    --label) LABEL="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --guard-free-mib) GUARD_FREE_MIB="$2"; shift 2 ;;
    *) echo "scratch-bench: unknown option $1" >&2; exit 2 ;;
  esac
done

# The fixed population. Swift Testing suites must be named on the command line —
# a test plan's selectedTests silently ignores Swift Testing identifiers.
SUITES=(
  AnalysisStoreCrossUserSharingTests
  AnalysisStoreCRUDTests
  AnalysisStoreFTSTests
  AnalysisStoreFetchCoverageSummariesTests
  AnalysisStoreAdScanCoverageTests
  AdCatalogStoreTests
  DuplicateAssetReconcileTests
  SemanticScanPersistenceTests
  SkipOrchestratorRevertTests
  BackfillJobRunnerTests
  CorpusExporterTests
  SchedulerRegressionTests
  ReconcilerRegressionTests
  StoreRegressionTests
  DownloadManagerCacheTests
  DownloadManagerEvictionTests
)
ONLY=()
for s in "${SUITES[@]}"; do ONLY+=("-only-testing:PlayheadTests/$s"); done

mkdir -p .logs
JSONL=".logs/scratch-bench-${LABEL}.jsonl"
LOG=".logs/scratch-bench-${LABEL}.log"
rm -f "$JSONL" "$LOG"

DEV="${DEVELOPER_DIR:-/Applications/Xcode-beta.app/Contents/Developer}"
DEST="${PLAYHEAD_DEST:-platform=iOS Simulator,name=iPhone 17}"

# Start from a clean scratch root so the sampler's series starts at zero and the
# peak is this run's peak. Directories a previous abnormal exit left behind can
# be UNREADABLE (0o300) — `chmod -R u+w` is not enough, `rm -rf` needs +r to
# enumerate them, which is why this uses u+rwx.
for s in "$HOME"/Library/Developer/CoreSimulator/Devices/*/data/Containers/Data/Application/*/tmp/PlayheadTestScratch; do
  [ -d "$s" ] || continue
  chmod -R u+rwx "$s" 2>/dev/null
  rm -rf "$s" && echo "scratch-bench: cleared $s"
done

echo "scratch-bench: label=$LABEL suites=${#SUITES[@]} guard=${GUARD_FREE_MIB}MiB"
DEVELOPER_DIR="$DEV" xcodebuild test \
  -scheme Playhead -testPlan PlayheadFastTests \
  -destination "$DEST" -derivedDataPath .derivedData -jobs 4 \
  "${ONLY[@]}" >"$LOG" 2>&1 &
XPID=$!

# The sampler needs xcodebuild's OWN pid for its disk guard, not this shell's.
# Resolve it by process NAME (pgrep -x), never by a `-f` command-line pattern:
# a `-f xcodebuild` pattern self-matches any script whose argv contains the
# string, and has already killed a run that was then misreported as a disk
# failure.
XCPID=""
for _ in $(seq 1 30); do
  XCPID="$(pgrep -x xcodebuild 2>/dev/null | head -1)"
  [ -n "$XCPID" ] && break
  sleep 1
done
[ -n "$XCPID" ] || XCPID="$XPID"

./scripts/scratch-sampler.sh --out "$JSONL" --interval "$INTERVAL" --quiet \
  --guard-pid "$XCPID" --guard-free-mib "$GUARD_FREE_MIB" &
SAMPLER=$!

wait "$XPID"; RC=$?
kill "$SAMPLER" 2>/dev/null
wait "$SAMPLER" 2>/dev/null

echo
echo "=== scratch-bench report ($LABEL) — xcodebuild exit $RC"
./scripts/scratch-sampler.sh --report "$JSONL"
echo
echo "=== scratch remaining right after the run"
./scripts/scratch-sampler.sh --breakdown | head -12
exit "$RC"
