// SpeakerShiftTests.swift
// playhead-gtt9.12: speaker-shift feature tests.

import Foundation
import Testing

@testable import Playhead

@Suite("SpeakerShift")
struct SpeakerShiftTests {

    @Test("proxy below floor scores 0")
    func proxyBelowFloor() {
        let cfg = SpeakerShift.Config.default
        #expect(SpeakerShift.mapProxy(cfg.proxyFloor - 0.05, config: cfg) == 0)
    }

    @Test("proxy at saturation scores 1")
    func proxyAtSaturation() {
        let cfg = SpeakerShift.Config.default
        #expect(SpeakerShift.mapProxy(cfg.proxySaturation, config: cfg) == 1)
    }

    @Test("stable host / occasional shift episode passes gate exactly where proxy is high")
    func stableConversationWithShift() {
        var windows: [FeatureWindow] = []
        for i in 0..<30 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.20,
                speakerChangeProxyScore: 0.05,
                speakerClusterId: 0
            ))
        }
        // Inject one window with a high proxy score mid-episode.
        windows[15] = AcousticFeatureFixtures.window(
            startTime: 30, endTime: 32,
            rms: 0.22,
            speakerChangeProxyScore: 0.90,
            speakerClusterId: 0
        )
        var funnel = AcousticFeatureFunnel()
        let scores = SpeakerShift.scores(for: windows, funnel: &funnel)
        #expect(scores[15].score > 0.6)
        #expect(funnel.count(.passedGate, .speakerShift) >= 1)
    }

    @Test("cluster id flip against prior majority triggers a shift credit")
    func clusterShiftFires() {
        var windows: [FeatureWindow] = []
        // 10 windows of cluster 0.
        for i in 0..<10 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.2, speakerChangeProxyScore: 0.05, speakerClusterId: 0
            ))
        }
        // Then 5 windows of cluster 7 (a different speaker).
        for i in 10..<15 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2,
                rms: 0.2, speakerChangeProxyScore: 0.05, speakerClusterId: 7
            ))
        }
        var funnel = AcousticFeatureFunnel()
        let scores = SpeakerShift.scores(for: windows, funnel: &funnel)
        // Cluster flip emits the documented `clusterShiftCertainty`
        // (0.7 — strong-but-not-certain), not 1.0; mixing it with the
        // continuous proxy component on the same `[0, 1]` scale.
        #expect(scores[10].score == SpeakerShift.clusterShiftCertainty)
        #expect(funnel.count(.passedGate, .speakerShift) >= 1)
    }
}
