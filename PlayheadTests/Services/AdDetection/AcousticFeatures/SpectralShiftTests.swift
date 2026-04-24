// SpectralShiftTests.swift
// playhead-gtt9.12: spectral-profile shift feature tests.

import Foundation
import Testing

@testable import Playhead

@Suite("SpectralShift")
struct SpectralShiftTests {

    @Test("median computation handles even and odd counts")
    func median() {
        let oddWindows = [0.1, 0.2, 0.3].enumerated().map { i, f in
            AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.2, spectralFlux: f
            )
        }
        #expect(SpectralShift.medianFlux(windows: oddWindows) == 0.2)

        let evenWindows = [0.1, 0.2, 0.3, 0.4].enumerated().map { i, f in
            AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.2, spectralFlux: f
            )
        }
        #expect(SpectralShift.medianFlux(windows: evenWindows) == 0.25)
    }

    @Test("ratio below elevation threshold scores 0")
    func belowElevation() {
        let cfg = SpectralShift.Config.default
        #expect(SpectralShift.mapRatioToScore(cfg.elevationRatio - 0.01, config: cfg) == 0)
    }

    @Test("episode with an elevated-flux block passes gate for those windows")
    func elevatedBlockFires() {
        var windows: [FeatureWindow] = []
        for i in 0..<40 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.2, spectralFlux: 0.02
            ))
        }
        // 10 windows with 8× elevated flux — well above the 1.75× threshold.
        for i in 40..<50 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.2, spectralFlux: 0.16
            ))
        }
        var funnel = AcousticFeatureFunnel()
        let scores = SpectralShift.scores(for: windows, funnel: &funnel)
        #expect(scores.count == windows.count)
        #expect(funnel.count(.producedSignal, .spectralShift) >= 10)
        #expect(funnel.count(.passedGate, .spectralShift) >= 10)
        let elevated = scores[40..<50].map(\.score).max() ?? 0
        #expect(elevated > 0.5)
    }

    @Test("flat-flux episode does not fire")
    func flatEpisode() {
        let windows = (0..<30).map { i in
            AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.2, spectralFlux: 0.05
            )
        }
        var funnel = AcousticFeatureFunnel()
        _ = SpectralShift.scores(for: windows, funnel: &funnel)
        #expect(funnel.count(.passedGate, .spectralShift) == 0)
    }
}
