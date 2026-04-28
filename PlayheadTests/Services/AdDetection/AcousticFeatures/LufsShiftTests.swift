// LufsShiftTests.swift
// playhead-gtt9.12: synthetic-signal unit tests for the LUFS-shift feature.

import Foundation
import Testing

@testable import Playhead

@Suite("LufsShift")
struct LufsShiftTests {

    @Test("dbfs conversion floors non-positive RMS to a safe minimum")
    func dbfsFlooring() {
        let zero = LufsShift.dbfs(rms: 0)
        #expect(zero < 0) // 20*log10(1e-6) = -120 dB
        #expect(zero.isFinite)
    }

    @Test("mapDeltaToScore is 0 below floor and 1 above saturation")
    func mapping() {
        let cfg = LufsShift.Config.default
        #expect(LufsShift.mapDeltaToScore(0, config: cfg) == 0)
        #expect(LufsShift.mapDeltaToScore(cfg.signalFloorDb - 0.1, config: cfg) == 0)
        #expect(LufsShift.mapDeltaToScore(cfg.saturationDb, config: cfg) == 1)
        #expect(LufsShift.mapDeltaToScore(cfg.saturationDb + 5, config: cfg) == 1)
    }

    @Test("flat-loudness episode produces mostly zero scores and no gate hits")
    func flatEpisodeIsQuiet() {
        let windows = AcousticFeatureFixtures.quietHostBaseline()
        var funnel = AcousticFeatureFunnel()
        let scores = LufsShift.scores(for: windows, funnel: &funnel)
        #expect(scores.count == windows.count)
        #expect(funnel.count(.computed, .lufsShift) == windows.count)
        let gated = funnel.count(.passedGate, .lufsShift)
        #expect(gated == 0)
    }

    @Test("loud-then-quiet step produces an elevated score for the loud block")
    func loudStepProducesSignal() {
        // Baseline 40 windows at rms 0.15 and 10 windows at rms 0.60 — a 12 dB jump.
        var windows: [FeatureWindow] = []
        for i in 0..<40 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.15
            ))
        }
        for i in 40..<50 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.60
            ))
        }
        var funnel = AcousticFeatureFunnel()
        let scores = LufsShift.scores(for: windows, funnel: &funnel)
        let loudBlockScores = scores[40..<50].map(\.score)
        let loudBlockMax = loudBlockScores.max() ?? 0
        #expect(loudBlockMax > 0.3)
        #expect(funnel.count(.producedSignal, .lufsShift) >= 5)
        #expect(funnel.count(.passedGate, .lufsShift) >= 1)
    }

    @Test("empty input returns empty output and no funnel events")
    func emptyInput() {
        var funnel = AcousticFeatureFunnel()
        let scores = LufsShift.scores(for: [], funnel: &funnel)
        #expect(scores.isEmpty)
        #expect(funnel.count(.computed, .lufsShift) == 0)
    }

    @Test("baseline is robust to a sustained-loud insertion (median, not mean)")
    func sustainedLoudInsertionStillDetected() {
        // 30 host-baseline windows at rms 0.15 plus 20 sustained-loud
        // windows at rms 0.60 (≈12 dB louder). With the prior
        // arithmetic-mean baseline, the loud insertion biased the
        // baseline toward itself and the per-window delta shrank. The
        // median baseline is unmoved by an insertion that occupies less
        // than half of the episode (20/50 = 0.4 here), so the loud block
        // should still produce a clear elevated score.
        var windows: [FeatureWindow] = []
        for i in 0..<30 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.15
            ))
        }
        for i in 30..<50 {
            windows.append(AcousticFeatureFixtures.window(
                startTime: Double(i) * 2, endTime: Double(i + 1) * 2, rms: 0.60
            ))
        }
        var funnel = AcousticFeatureFunnel()
        let scores = LufsShift.scores(for: windows, funnel: &funnel)
        let loudBlockMaxRaw = scores[30..<50].map(\.rawMetric).max() ?? 0
        let loudBlockDb: Double = 20 * (log10(0.60) - log10(0.15))  // ≈12.04 dB
        // The detector observes a delta within ~0.5 dB of the true
        // 12 dB step — i.e. the baseline is unmoved by the insertion.
        #expect(loudBlockMaxRaw > loudBlockDb - 0.5)
        #expect(funnel.count(.passedGate, .lufsShift) >= 5)
    }
}
