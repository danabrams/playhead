// TempoOnsetTests.swift
// playhead-gtt9.12: tempo / onset-density feature tests.

import Foundation
import Testing

@testable import Playhead

@Suite("TempoOnset")
struct TempoOnsetTests {

    @Test("pure speech (no music, low flux) produces no signal")
    func pureSpeech() {
        let windows = (0..<30).map { i in
            AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.25, spectralFlux: 0.02, musicProbability: 0.01
            )
        }
        var funnel = AcousticFeatureFunnel()
        let scores = TempoOnset.scores(for: windows, funnel: &funnel)
        #expect(scores.allSatisfy { $0.score == 0 })
        #expect(funnel.count(.passedGate, .tempoOnset) == 0)
    }

    @Test("ad bed (music + energy + flux) fires the gate")
    func musicBedFires() {
        var windows: [FeatureWindow] = []
        // 20 speech windows, then 15 "ad bed" windows.
        for i in 0..<20 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.25, spectralFlux: 0.02, musicProbability: 0.01
            ))
        }
        for i in 20..<35 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.50, spectralFlux: 0.40, musicProbability: 0.80
            ))
        }
        var funnel = AcousticFeatureFunnel()
        let scores = TempoOnset.scores(for: windows, funnel: &funnel)
        let adBlockMax = scores[20..<35].map(\.score).max() ?? 0
        #expect(adBlockMax > 0.5)
        #expect(funnel.count(.passedGate, .tempoOnset) >= 5)
    }
}
