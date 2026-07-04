#!/usr/bin/env bash
#
# perf-tests.sh — run load-sensitive MEASUREMENT tests in isolation.
#
# These tests (main-actor latency budget, launch-perf budget, cancellation
# timing) assert absolute wall-clock thresholds that are only valid on a
# quiescent CPU. The ~7,900-test PlayheadFastTests suite runs Swift Testing in
# parallel and saturates the machine, so those tests are gated (PerfGate /
# playhead-zx0l) to skip everywhere EXCEPT here.
#
# This pass:
#   * uses the PlayheadPerfTests plan, which sets PLAYHEAD_RUN_PERF=1 so the
#     gated tests are enabled;
#   * narrows to just the measurement tests via -only-testing (works for both
#     XCTest and Swift Testing on the command line, unlike xctestplan filters);
#   * disables parallel testing so nothing else competes for the CPU.
#
# Run it alone — do not launch other builds while it measures.
#
# Env overrides:
#   PLAYHEAD_DEST     xcodebuild -destination (default: iPhone 17 Pro, iOS 27.0)
#   PLAYHEAD_DERIVED  -derivedDataPath (default: .derivedData-perf)
#   DEVELOPER_DIR     select a toolchain (e.g. the Xcode 27 beta)

set -euo pipefail
cd "$(dirname "$0")/.."

DEST="${PLAYHEAD_DEST:-platform=iOS Simulator,name=iPhone 17 Pro,OS=27.0}"
DERIVED="${PLAYHEAD_DERIVED:-.derivedData-perf}"

# The measurement tests. Add new load-sensitive tests here AND gate them with
# PerfGate in the source so they skip in the parallel suite.
MEASUREMENT_TESTS=(
  "PlayheadTests/PlayheadRuntimeMainActorFreedomTests"
  "PlayheadTests/PlayheadRuntimeLaunchPerfTests"
  "PlayheadTests/AnalysisWorkSchedulerOutcomeBookkeepingTests"
)

only_testing_args=()
for t in "${MEASUREMENT_TESTS[@]}"; do
  only_testing_args+=("-only-testing:${t}")
done

exec xcodebuild test \
  -scheme Playhead \
  -testPlan PlayheadPerfTests \
  -destination "$DEST" \
  -derivedDataPath "$DERIVED" \
  -parallel-testing-enabled NO \
  "${only_testing_args[@]}" \
  "$@"
