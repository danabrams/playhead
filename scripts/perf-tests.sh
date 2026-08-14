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
# READ BOTH OUTPUT FORMATS (playhead-o89d review). This pass runs XCTest suites
# as well as Swift Testing ones, and `Test run with N tests in M suites passed`
# counts ONLY the Swift Testing half. A pass can print that line and still end
# in `** TEST FAILED **` — it does today; see playhead-1och.
#
# WHAT WAS RED, AND WHY IT WAS NOT THE TESTS' FAULT (playhead-1och diagnosed,
# playhead-xul6 fixed, 2026-08-13). This pass is GREEN as of playhead-xul6.
# `PlayheadRuntimeLaunchPerfTests.testInitFitsLaunchBudget` and
# `PlayheadRuntimeMainActorFreedomTests.testMainActorIsNotHeldDuringRuntimeInit`
# both failed, and they were the same defect measured from two sides:
# `CapabilitiesService()` — one line of `PlayheadRuntime.init` — held the MAIN
# ACTOR 0.47-2.13 s in a synchronous `SystemLanguageModel` read. The fix took
# that read out of the launch-time snapshot entirely; measured over three
# consecutive passes afterwards, MainActorFreedom max = 6.5 / 3.4 / 2.9 ms and
# LaunchPerf median = 2.0 / 2.3 / 2.1 ms.
#
# NEITHER THRESHOLD MOVED, and neither should. The 250 ms budget was
# deliberately NOT widened while the pass was red: on the constructions that
# missed the probe init read 57-68 ms, so the budget was being met with 4x
# headroom and these two tests were the only things that could see the block.
# Do not make this pass green by moving a threshold — that is what would have
# deleted the only evidence of a 2-second launch stall.
#
# Env overrides:
#   PLAYHEAD_DEST     xcodebuild -destination (default: iPhone 17 — must be a
#                     device that exists; see the note on DEST below)
#   PLAYHEAD_DERIVED  -derivedDataPath (default: .derivedData-perf)
#   DEVELOPER_DIR     select a toolchain (e.g. the Xcode 27 beta)

set -euo pipefail
cd "$(dirname "$0")/.."

# playhead-o89d: was `iPhone 17 Pro,OS=27.0`, which has not existed on this box
# for as long as anyone has looked — xcodebuild exits before running one test
# with "Unable to find a device matching the provided destination specifier".
# That is the whole point of this script failing silently: a PerfGate'd test is
# only MOVED here rather than deleted if this pass can actually start. Match
# fast-gate.sh's default so the two agree on one destination.
DEST="${PLAYHEAD_DEST:-platform=iOS Simulator,name=iPhone 17}"
DERIVED="${PLAYHEAD_DERIVED:-.derivedData-perf}"

# The measurement tests. Add new load-sensitive tests here AND gate them with
# PerfGate in the source so they skip in the parallel suite.
MEASUREMENT_TESTS=(
  # playhead-8d5r: proves the per-call FM inference deadline bounds WALL-CLOCK
  # even when the call ignores cooperative cancellation. Every behavioural
  # consequence of the deadline is asserted deterministically in the parallel
  # suite; only this elapsed-time measurement needs a quiet CPU (it read 54.4s
  # for a 200ms budget under the full fast gate).
  "PlayheadTests/FMInferenceDeadlineTimingTests"
  "PlayheadTests/PlayheadRuntimeMainActorFreedomTests"
  "PlayheadTests/PlayheadRuntimeLaunchPerfTests"
  "PlayheadTests/LibraryViewUnplayedCountPerfTests"
  # Method-level: these suites are large and mostly NOT load-sensitive, so
  # only their single perf test opts in (gated with PerfGate in-source).
  "PlayheadTests/SemanticScanPersistenceTests/fetchReusableSemanticScanResultPerformance()"
  "PlayheadTests/AdmissionControllerTests/testEnqueueScales()"
  # xsdz.26: 60-minute-episode fingerprinting wall-clock budget (needs the
  # staged corpus audio in the main checkout; skips cleanly without it).
  "PlayheadTests/ChromaFingerprinterPerfTests/sixtyMinuteEpisodeUnderBudget()"
  # playhead-m9xk: skip-transition <500ms latency (real 150ms duck-settle
  # sleep + ContinuousClock measurement). Ordering/reentrancy coverage for
  # the same path runs deterministically in the fast suite via the injected
  # transitionSleeper seam; only this latency measurement is gated here.
  "PlayheadTests/SkipCueSmoothingTests/skipTransitionLatencyWithinProductionBudget()"
  # playhead-vsot round 2: force-quit resume-data scan 2 s cold-launch SLA
  # (median-of-3 wall-clock). Functional completion coverage stays in the
  # fast suite (scanCompletesOverTenBlobCache); only the latency SLA is
  # measured here.
  "PlayheadTests/ScanForSuspendedTransfersTests/scanCompletesWithinSLA()"
  # playhead-vsot round 2: at-scale span-decoder wall-clock budget (spec
  # 200 ms device / 500 ms quiescent simulator). Functional completion
  # stays in the fast suite (decodeAtScaleCompletes).
  "PlayheadTests/MinimalContiguousSpanDecoderTests/performanceDecodeAtScale()"
  # playhead-vsot round 3: SpanMetrics anti-quadratic wall-clock guards
  # (5 s ceiling on 10k-pair summary / 1k+1k pairing). Correctness
  # (metric counts) stays unconditional in the fast suite; only the
  # timing guard is measured here.
  "PlayheadTests/PerformanceSmokeTests/tenKPairs()"
  "PlayheadTests/PerformanceSmokeTests/pairingScale()"
  # playhead-26od: the one test that must SHRINK the FM no-progress bound, to
  # make the watchdog actually abandon a wedged coarse pass. A shrunken silence
  # bound is precisely what the parallel gate destroys — under ~10,000
  # concurrent tests a healthy pass out-waits any budget small enough to be
  # fast. Durability itself is asserted deterministically in the fast suite by
  # reading the database from inside the running pass; only the real
  # abandonment is measured here.
  "PlayheadTests/BackfillCoarseCheckpointTests/abandonedPassLeavesItsScreenedWindowsBehind()"
  # playhead-o89d. Both admitted on an ISOLATED-vs-IN-GATE measurement rather
  # than on a suite name — see the bead for why a name list rots (two
  # measurements fifteen days apart shared not one suite, and Chao1 on the
  # committed baseline puts the load-sensitive population at >=377 against 136
  # ever observed). These two are here because the wall-clock quantity IS the
  # assertion, which is the one sub-property that does not rotate.
  #
  #   benchmarkGate            budget 5,000 ms; alone <1,900 ms (3 tests, 1.9 s
  #                            total, PASS); 443-test scoped selection 25,243 ms
  #                            (FAIL); full plan 105,491 / 162,396 ms (FAIL).
  #   expirationWait…Reserve   a 20 s BOUND separating "about 0 s" (correct)
  #                            from "60 s" (spends the reclaimed reserve). Passes
  #                            5/5 scoped; failed 100 % of recorded full-plan runs.
  "PlayheadTests/Phase3ShadowReplayHarnessTests/benchmarkGate()"
  "PlayheadTests/BackfillExpiryDurabilityTests/expirationWaitIsBoundedByTheGraceNotTheReserve()"
  # Note: AnalysisWorkSchedulerOutcomeBookkeepingTests is intentionally NOT
  # here — its cancel-mid-decode tests were rewritten to be deterministic
  # (via processNextDispatchableJobForTesting) and un-gated, so they run in
  # the normal fast suite and are no longer load-sensitive measurements.
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
