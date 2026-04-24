// SilenceBoundaryTests.swift
// playhead-gtt9.12: silence / bumper boundary feature tests.

import Foundation
import Testing

@testable import Playhead

@Suite("SilenceBoundary")
struct SilenceBoundaryTests {

    @Test("episode with no silence emits no credits")
    func noSilence() {
        let windows = (0..<20).map { i in
            AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.30
            )
        }
        var funnel = AcousticFeatureFunnel()
        let scores = SilenceBoundary.scores(for: windows, funnel: &funnel)
        #expect(scores.allSatisfy { $0.score == 0 })
        #expect(funnel.count(.passedGate, .silenceBoundary) == 0)
    }

    @Test("single silent window credits itself and its neighbours")
    func singleSilentBumper() {
        var windows: [FeatureWindow] = []
        for i in 0..<10 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.30
            ))
        }
        // Replace index 5 with a silent window.
        windows[5] = AcousticFeatureFixtures.window(
            startTime: 10, endTime: 12, rms: 0.005
        )
        var funnel = AcousticFeatureFunnel()
        let scores = SilenceBoundary.scores(for: windows, funnel: &funnel)
        #expect(scores[5].score == 1)
        // Radius = 2, so 3..4 and 6..7 get credit.
        #expect(scores[4].score == 1)
        #expect(scores[6].score == 1)
        #expect(funnel.count(.passedGate, .silenceBoundary) >= 3)
    }

    @Test("very long silence run is rejected as not a bumper")
    func overlyLongSilenceRejected() {
        let cfg = SilenceBoundary.Config.default
        var windows: [FeatureWindow] = []
        // 5 normal windows.
        for i in 0..<5 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.25
            ))
        }
        // Then a silence run longer than the config's max.
        for i in 5..<(5 + cfg.maxSilentWindows + 2) {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.001
            ))
        }
        // Then more normal windows.
        let tailStart = windows.count
        for i in tailStart..<(tailStart + 5) {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.25
            ))
        }
        var funnel = AcousticFeatureFunnel()
        let scores = SilenceBoundary.scores(for: windows, funnel: &funnel)
        #expect(scores.allSatisfy { $0.score == 0 })
        #expect(funnel.count(.passedGate, .silenceBoundary) == 0)
    }
}
