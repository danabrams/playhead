// FusionBudgetClampTests.swift
// playhead-z3ch: Unit tests for FusionBudgetClamp.
//
// Verifies:
//   1. weight is hard-clamped to sourceWeightCap (no redistribution).
//   2. weight at-or-below cap is preserved unchanged.
//   3. detail and source are preserved across clamping.
//   4. an audit log line is emitted when weight > cap (observable via test sink).

import Foundation
import OSLog
import Testing
@testable import Playhead

@Suite("FusionBudgetClamp — unit")
struct FusionBudgetClampTests {

    private static let testLogger = Logger(
        subsystem: "com.playhead.tests",
        category: "FusionBudgetClampTests"
    )

    private func makeMetadataEntry(weight: Double) -> EvidenceLedgerEntry {
        EvidenceLedgerEntry(
            source: .metadata,
            weight: weight,
            detail: .metadata(
                cueCount: 1,
                sourceField: .description,
                dominantCueType: .disclosure
            )
        )
    }

    @Test("clamp leaves weight unchanged when at or below cap")
    func clampPreservesUnderCapWeight() {
        let clamp = FusionBudgetClamp(sourceWeightCap: 0.15)
        let entry = makeMetadataEntry(weight: 0.10)

        let clamped = clamp.clamp(entry, logger: Self.testLogger)

        #expect(clamped.weight == 0.10, "weight at or below cap must be preserved")
        #expect(clamped.source == entry.source)
    }

    @Test("clamp leaves weight exactly at cap unchanged")
    func clampPreservesExactlyAtCap() {
        let clamp = FusionBudgetClamp(sourceWeightCap: 0.15)
        let entry = makeMetadataEntry(weight: 0.15)

        let clamped = clamp.clamp(entry, logger: Self.testLogger)

        #expect(clamped.weight == 0.15)
    }

    @Test("clamp hard-limits weight to cap when raw exceeds cap (no redistribution)")
    func clampHardLimitsOverCapWeight() {
        let clamp = FusionBudgetClamp(sourceWeightCap: 0.15)
        let entry = makeMetadataEntry(weight: 0.42)

        let clamped = clamp.clamp(entry, logger: Self.testLogger)

        #expect(clamped.weight == 0.15, "over-cap weight must be hard-clamped to cap")
        // Detail must be preserved across clamping.
        if case .metadata(let cueCount, let sourceField, let dominantCueType) = clamped.detail {
            #expect(cueCount == 1)
            #expect(sourceField == .description)
            #expect(dominantCueType == .disclosure)
        } else {
            Issue.record("clamped detail must remain metadata variant")
        }
    }

    @Test("clamp emits audit observation when weight exceeds cap")
    func clampEmitsAuditOnOverflow() {
        // Install a test observer to capture clamp events.
        let observed = TestActor<(Double, Double)?>(value: nil)
        FusionBudgetClamp.testClampObserver = { rawWeight, cappedWeight in
            observed.set((rawWeight, cappedWeight))
        }
        defer { FusionBudgetClamp.testClampObserver = nil }

        let clamp = FusionBudgetClamp(sourceWeightCap: 0.15)
        _ = clamp.clamp(makeMetadataEntry(weight: 0.42), logger: Self.testLogger)

        let captured = observed.get()
        #expect(captured != nil, "audit observer must be called on overflow")
        #expect(captured?.0 == 0.42)
        #expect(captured?.1 == 0.15)
    }

    @Test("clamp does not emit audit observation when weight is at or below cap")
    func clampSilentBelowCap() {
        let observed = TestActor<(Double, Double)?>(value: nil)
        FusionBudgetClamp.testClampObserver = { rawWeight, cappedWeight in
            observed.set((rawWeight, cappedWeight))
        }
        defer { FusionBudgetClamp.testClampObserver = nil }

        let clamp = FusionBudgetClamp(sourceWeightCap: 0.15)
        _ = clamp.clamp(makeMetadataEntry(weight: 0.10), logger: Self.testLogger)
        _ = clamp.clamp(makeMetadataEntry(weight: 0.15), logger: Self.testLogger)

        #expect(observed.get() == nil, "no audit emission expected for under-cap weights")
    }
}

/// Tiny sync wrapper over a value for capturing test side effects.
/// (Closure captures into Swift Testing scopes need a reference holder.)
private final class TestActor<T>: @unchecked Sendable {
    private var _value: T
    private let lock = NSLock()
    init(value: T) { self._value = value }
    func get() -> T {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
    func set(_ newValue: T) {
        lock.lock(); defer { lock.unlock() }
        _value = newValue
    }
}
