// RepetitionFingerprintTests.swift
// playhead-gtt9.12: stub tests — the feature is inert until gtt9.13 lands.

import Foundation
import Testing

@testable import Playhead

@Suite("RepetitionFingerprint (stub)")
struct RepetitionFingerprintTests {

    @Test("stub emits zero scores and produces no signal, but counts compute events")
    func stubIsInert() {
        let windows = (0..<10).map { i in
            AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.25
            )
        }
        var funnel = AcousticFeatureFunnel()
        let scores = RepetitionFingerprint.scores(for: windows, funnel: &funnel)
        #expect(scores.count == windows.count)
        #expect(scores.allSatisfy { $0.score == 0 })
        #expect(funnel.count(.computed, .repetitionFingerprint) == windows.count)
        #expect(funnel.count(.producedSignal, .repetitionFingerprint) == 0)
        #expect(funnel.count(.passedGate, .repetitionFingerprint) == 0)
        #expect(funnel.count(.includedInFusion, .repetitionFingerprint) == 0)
    }

    @Test("passing a nil catalog is accepted (awaiting gtt9.13 merge)")
    func nilCatalogAccepted() {
        let windows = [
            AcousticFeatureFixtures.window(startTime: 0, endTime: 2, rms: 0.25)
        ]
        var funnel = AcousticFeatureFunnel()
        let scores = RepetitionFingerprint.scores(for: windows, catalog: nil, funnel: &funnel)
        #expect(scores.count == 1)
    }
}
