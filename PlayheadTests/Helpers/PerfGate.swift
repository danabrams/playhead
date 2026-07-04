// PerfGate.swift
// Gate for load-sensitive *measurement* tests (absolute wall-clock latency
// budgets, cancellation-timing races). These assertions only produce valid
// results on a quiescent CPU.
//
// The problem: `PlayheadFastTests` runs ~7,900 tests with Swift Testing's
// default in-process parallelism, saturating the machine. A latency test that
// happens to run during that storm measures the suite's own load, not the code
// under test — e.g. `MainActorFreedom` blew past its 100 ms budget under the
// full suite yet logged max=34 ms in isolation. That is flakiness, not a
// regression.
//
// The fix (playhead-zx0l): these tests are *opt-in*. They run only when
// `PLAYHEAD_RUN_PERF=1` is present in the test process environment — set by the
// dedicated serial perf pass (`scripts/perf-tests.sh` via the
// `PlayheadPerfTests` plan), which runs them alone with parallelism disabled so
// the CPU is quiescent. Everywhere else (the parallel fast/integration suites,
// ad-hoc runs) they skip. This mirrors the repo's existing `PLAYHEAD_FM_SMOKE`
// env-gating pattern and works uniformly across XCTest and Swift Testing,
// sidestepping the xctestplan limitation that cannot filter Swift Testing IDs.
//
// Usage:
//   XCTest:        override func setUpWithError() throws {
//                      try XCTSkipUnless(PerfGate.runsMeasurementTests, PerfGate.skipReason)
//                  }
//   Swift Testing: @Test(..., .enabled(if: PerfGate.runsMeasurementTests, PerfGate.skipComment))

import Foundation

enum PerfGate {
    /// True only inside the dedicated serial perf pass. Measurement tests run
    /// when true and skip when false.
    static let runsMeasurementTests: Bool =
        ProcessInfo.processInfo.environment["PLAYHEAD_RUN_PERF"] == "1"

    /// Skip reason for XCTest `XCTSkipUnless`.
    static let skipReason =
        "Load-sensitive measurement test — runs only in the serial perf pass "
        + "(scripts/perf-tests.sh); skipped in the parallel suite. See playhead-zx0l."
}
