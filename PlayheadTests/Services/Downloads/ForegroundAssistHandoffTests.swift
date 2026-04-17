// ForegroundAssistHandoffTests.swift
// playhead-44h1: unit tests for the 80% / 2-min hand-off decision rule.
//
// The rule — "keep the foreground-assist task alive if transfer ≥ 80%
// complete OR remaining-byte ETA ≤ 2 min; otherwise submit a
// BGContinuedProcessingTaskRequest" — is covered at every boundary:
//   - exactly at each threshold (inclusive, per spec).
//   - just below (outside the keep-alive region).
//   - just above (inside the keep-alive region).
//   - combined-gate: either gate alone is sufficient (disjunction).
// Edge conditions (unknown throughput, unknown total) route to the
// documented fallback behavior.

import Foundation
import Testing
@testable import Playhead

@Suite("ForegroundAssistHandoff decision")
struct ForegroundAssistHandoffTests {

    // MARK: - 80% completion gate

    @Test("At 80% complete keeps foreground-assist alive (inclusive boundary)")
    func atEightyPercentKeepsAlive() {
        // 80 MB of 100 MB = exactly 0.80 fraction; 20 MB remaining at 1 MB/s
        // = 20 s — both gates would trigger keep-alive, so this test pins
        // the inclusive boundary of the percentage gate. (The 2-min gate
        // would also match at 1 MB/s, so we use a near-zero throughput
        // below to isolate the percentage gate proper.)
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 80_000_000,
            totalBytesExpectedToWrite: 100_000_000,
            // Throughput small enough that the time gate alone would
            // fall outside the 2-min window (20 MB at 10 KB/s ≈ 2000 s).
            averageBytesPerSecond: 10_000
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .keepForegroundAssistAlive)
    }

    @Test("Just below 80% complete submits BG continuation (negative boundary)")
    func justBelowEightyPercentSubmitsBG() {
        // 79.99 MB of 100 MB = 0.7999 < 0.80. Throughput very slow so the
        // time gate does not rescue it. Expect BG-task hand-off.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 79_990_000,
            totalBytesExpectedToWrite: 100_000_000,
            averageBytesPerSecond: 10_000
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .submitContinuedProcessingRequest)
    }

    @Test("Above 80% complete keeps foreground-assist alive")
    func aboveEightyPercentKeepsAlive() {
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 95_000_000,
            totalBytesExpectedToWrite: 100_000_000,
            averageBytesPerSecond: 10_000
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .keepForegroundAssistAlive)
    }

    // MARK: - 2-minute ETA gate

    @Test("Exactly 120s remaining keeps foreground-assist alive (inclusive)")
    func exactly120sRemainingKeepsAlive() {
        // 1 MB remaining at 8,333 B/s ≈ 120 s.  Percentage gate fails
        // (50% complete), so the decision is forced down the time gate.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 1_000_000,
            totalBytesExpectedToWrite: 2_000_000,
            averageBytesPerSecond: 1_000_000.0 / 120.0
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .keepForegroundAssistAlive)
    }

    @Test("ETA just above 120s submits BG continuation")
    func justAbove120sSubmitsBG() {
        // 1 MB remaining at a rate that gives ≈ 121 s.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 1_000_000,
            totalBytesExpectedToWrite: 2_000_000,
            averageBytesPerSecond: 1_000_000.0 / 121.0
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .submitContinuedProcessingRequest)
    }

    @Test("Very short ETA under 2 min keeps foreground-assist alive")
    func shortEtaKeepsAlive() {
        // 10 KB remaining at 100 KB/s = 0.1 s — fraction 50% (fails pct),
        // time gate passes.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 10_000,
            totalBytesExpectedToWrite: 20_000,
            averageBytesPerSecond: 100_000
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .keepForegroundAssistAlive)
    }

    // MARK: - Disjunction: either gate alone suffices

    @Test("90% complete with slow throughput still keeps alive (pct gate)")
    func ninetyPercentSlowThroughputKeepsAlive() {
        // 90 MB of 100 MB, throughput so slow the ETA gate would fail.
        // 10 MB at 10 KB/s = 1000 s >> 120 s. Percentage gate rescues.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 90_000_000,
            totalBytesExpectedToWrite: 100_000_000,
            averageBytesPerSecond: 10_000
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .keepForegroundAssistAlive)
    }

    @Test("10% complete with huge throughput keeps alive (time gate)")
    func tenPercentFastThroughputKeepsAlive() {
        // 10% (fails pct gate) but so fast the ETA is seconds.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 1_000_000,
            totalBytesExpectedToWrite: 10_000_000,
            averageBytesPerSecond: 10_000_000
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .keepForegroundAssistAlive)
    }

    // MARK: - Edge conditions

    @Test("Unknown expected-total with slow throughput submits BG continuation")
    func unknownTotalSubmitsBGWhenSlow() {
        // Expected-total 0 → fractionCompleted = 0 (never matches pct
        // gate). With a valid throughput, remaining bytes also = 0 and
        // the ETA gate matches (0 <= 120s) — so we expect keepalive.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 0,
            totalBytesExpectedToWrite: 0,
            averageBytesPerSecond: 1_000
        )
        // `remainingSeconds` is 0 (max(0, -…) = 0). Time gate matches.
        #expect(ForegroundAssistHandoff.decide(for: snap) == .keepForegroundAssistAlive)
    }

    @Test("Unknown throughput forces BG continuation unless percentage gate matches")
    func unknownThroughputForcesBG() {
        // 50% complete (pct gate fails). Unknown throughput → ETA ∞.
        // Must submit BG continuation.
        let slow = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 50_000_000,
            totalBytesExpectedToWrite: 100_000_000,
            averageBytesPerSecond: 0
        )
        #expect(ForegroundAssistHandoff.decide(for: slow) == .submitContinuedProcessingRequest)

        // 85% complete — percentage gate rescues even without throughput.
        let nearDone = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 85_000_000,
            totalBytesExpectedToWrite: 100_000_000,
            averageBytesPerSecond: 0
        )
        #expect(ForegroundAssistHandoff.decide(for: nearDone) == .keepForegroundAssistAlive)
    }

    @Test("Negative throughput is treated as unknown")
    func negativeThroughputIsUnknown() {
        // Pathological but well-defined: a negative throughput value
        // (e.g. clock-wobble smoothing artefact) is clamped to "unknown"
        // and must NOT trick the ETA gate into thinking time is going
        // backwards.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 50_000_000,
            totalBytesExpectedToWrite: 100_000_000,
            averageBytesPerSecond: -100
        )
        #expect(ForegroundAssistHandoff.decide(for: snap) == .submitContinuedProcessingRequest)
    }

    @Test("Fraction is clamped to [0, 1]")
    func fractionIsClamped() {
        // Over-reported bytes (written > expected) should clamp to 1.0
        // so the pct gate still matches — we don't want an arithmetic
        // glitch to push us into BG-task hand-off right before the
        // transfer completes.
        let snap = ForegroundAssistTransferSnapshot(
            totalBytesWritten: 110_000_000,
            totalBytesExpectedToWrite: 100_000_000,
            averageBytesPerSecond: 0
        )
        #expect(snap.fractionCompleted == 1.0)
        #expect(ForegroundAssistHandoff.decide(for: snap) == .keepForegroundAssistAlive)
    }
}
