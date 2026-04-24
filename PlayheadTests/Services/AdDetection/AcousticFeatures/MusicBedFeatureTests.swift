// MusicBedFeatureTests.swift
// playhead-gtt9.12: validation adapter tests for the legacy music-bed signal.

import Foundation
import Testing

@testable import Playhead

@Suite("MusicBedFeature")
struct MusicBedFeatureTests {

    @Test("probability below produced floor scores 0")
    func belowFloor() {
        let cfg = MusicBedFeature.Config.default
        #expect(MusicBedFeature.mapProbability(cfg.producedFloor - 0.05, config: cfg) == 0)
    }

    @Test("probability at saturation scores 1")
    func atSaturation() {
        let cfg = MusicBedFeature.Config.default
        #expect(MusicBedFeature.mapProbability(cfg.saturation, config: cfg) == 1)
    }

    @Test("low-music episode produces few gate hits (validates the 2% baseline)")
    func lowMusicBaseline() {
        let windows = AcousticFeatureFixtures.quietHostBaseline()
        var funnel = AcousticFeatureFunnel()
        _ = MusicBedFeature.scores(for: windows, funnel: &funnel)
        #expect(funnel.count(.computed, .musicBed) == windows.count)
        #expect(funnel.count(.passedGate, .musicBed) == 0)
    }

    @Test("episode with a music bed block scores high for those windows")
    func musicBedBlock() {
        var windows = AcousticFeatureFixtures.quietHostBaseline(count: 40)
        for i in 20..<30 {
            windows[i] = AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.30, spectralFlux: 0.20,
                musicProbability: 0.90, musicBedLevel: .foreground
            )
        }
        var funnel = AcousticFeatureFunnel()
        let scores = MusicBedFeature.scores(for: windows, funnel: &funnel)
        let blockMax = scores[20..<30].map(\.score).max() ?? 0
        #expect(blockMax > 0.7)
        #expect(funnel.count(.passedGate, .musicBed) >= 5)
    }
}
