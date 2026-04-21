// TranscriptPeekNotAdModeTests.swift
// Tests for playhead-vyz "Not an ad" selection mode in TranscriptPeekView.
//
// Scope: format-contract tests. `submitNotAdChunks()` is a @MainActor method
// on a SwiftUI view whose state lives in @State bindings; exercising it as a
// unit requires either factoring a pure helper out of the view or driving it
// via ViewInspector. Neither is in place yet (tracked informally — the
// production integration is still observed in dogfood, not automation).
// What the view writes to persistence is covered transitively by
// `UserCorrectionStoreTests.testRecordVetoTimeRangePersistsExactTimeSpan`.
//
// Verifies here:
//   1. Unique veto span IDs for distinct time ranges on the same asset
//   2. Fixed 3-decimal precision in the ID format tames FP representation drift
//   3. `CorrectionSource.manualVeto` maps to `CorrectionKind.falsePositive`

import Foundation
import Testing

@testable import Playhead

@Suite("TranscriptPeek Not-Ad Mode — Format Contracts")
@MainActor
struct TranscriptPeekNotAdModeTests {

    @Test("Unique veto span IDs for different time ranges on same asset")
    func uniqueVetoSpanIds() {
        // submitNotAdChunks generates unique IDs per veto using fixed-precision
        // time range, preventing collisions when the same asset has multiple corrections.
        let assetId = "asset-1"
        let id1 = String(format: "%@-veto-%.3f-%.3f", assetId, 30.0, 60.0)
        let id2 = String(format: "%@-veto-%.3f-%.3f", assetId, 90.0, 120.0)
        let id3 = String(format: "%@-veto-%.3f-%.3f", assetId, 30.0, 60.0)
        #expect(id1 != id2, "Different time ranges must produce different IDs")
        #expect(id1 == id3, "Same time range must produce same ID")
    }

    @Test("Veto span ID encodes asset and time range for uniqueness")
    func vetoSpanIdEncodesTimeRange() {
        // Production builds IDs as String(format: "%@-veto-%.3f-%.3f", assetId, start, end)
        let id1 = String(format: "%@-veto-%.3f-%.3f", "asset-A", 10.0, 20.0)
        let id2 = String(format: "%@-veto-%.3f-%.3f", "asset-A", 30.0, 40.0)
        let id3 = String(format: "%@-veto-%.3f-%.3f", "asset-B", 10.0, 20.0)
        #expect(id1 != id2, "Same asset, different times must differ")
        #expect(id1 != id3, "Different asset must differ")
        // Verify fixed precision tames floating-point representation.
        let id4 = String(format: "%@-veto-%.3f-%.3f", "asset-A", 10.0 + 1e-10, 20.0)
        #expect(id1 == id4, "Sub-millisecond differences should produce same ID")
    }

    @Test("CorrectionSource.manualVeto maps to CorrectionKind.falsePositive")
    func manualVetoIsFalsePositive() {
        #expect(CorrectionSource.manualVeto.kind == .falsePositive)
    }
}
