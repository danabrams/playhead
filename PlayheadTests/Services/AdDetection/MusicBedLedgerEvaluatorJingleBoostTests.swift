// MusicBedLedgerEvaluatorJingleBoostTests.swift
// playhead-2hpn (Plan §6 Phase 3 deliverable 4): tests for the
// `JingleBoost`-conditional weight path on `MusicBedLedgerEvaluator`.
// Covers:
//   * Flag-off (jingleBoost == nil) — byte-identical to pre-2hpn:
//     weight = presenceFraction * acousticCap, capped at acousticCap.
//   * Flag-on baseline (snapshot not confirmed OR span doesn't overlap
//     a jingle slice) — weight = 0.10.
//   * Flag-on boosted (confirmed AND span overlaps a jingle slice) —
//     weight = 0.25.
//
// These assertions lock in the bead-spec contract: a non-confirmed show
// MUST emit baseline; a confirmed show with a span outside the
// intro/outro region MUST also emit baseline; only the intersection
// (confirmed AND overlap) gets the 0.25 boost.

import Foundation
import Testing

@testable import Playhead

private func bedWindow(
    start: Double,
    duration: Double = 2.0,
    level: MusicBedLevel = .background
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: "test",
        startTime: start,
        endTime: start + duration,
        rms: 0.1,
        spectralFlux: 0.05,
        musicProbability: 0.8,
        musicBedOnsetScore: 0,
        musicBedOffsetScore: 0,
        musicBedLevel: level,
        pauseProbability: 0,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 4
    )
}

@Suite("MusicBedLedgerEvaluator + JingleBoost")
struct MusicBedLedgerEvaluatorJingleBoostTests {

    // 10-window span with 8 .background → presenceFraction = 0.8.
    // Same geometry as the legacy `elevatedMusicBedFires` test so the
    // weight comparisons are pinned to a known fraction.
    private let span08: [FeatureWindow] = {
        var w: [FeatureWindow] = []
        for i in 0..<8 { w.append(bedWindow(start: Double(i) * 2.0, level: .background)) }
        w.append(bedWindow(start: 16, level: .none))
        w.append(bedWindow(start: 18, level: .none))
        return w
    }()

    private let config = FusionWeightConfig()

    // MARK: - Flag off (jingleBoost == nil)

    @Test("flag-off path preserves legacy presenceFraction * acousticCap")
    func flagOffByteIdentical() {
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: span08,
            fusionConfig: config,
            jingleBoost: nil
        )
        guard let entry = result.entry else {
            Issue.record("Expected ledger entry on 0.8 presence")
            return
        }
        // Legacy formula: 0.8 * 0.2 = 0.16, capped at 0.2.
        let expected = min(0.8 * config.acousticCap, config.acousticCap)
        #expect(abs(entry.weight - expected) < 1e-9,
                "Flag-off must match the legacy presenceFraction * acousticCap formula")
    }

    // MARK: - Flag on, baseline (0.10)

    @Test("flag-on but not confirmed → 0.10 baseline weight")
    func flagOnNotConfirmedBaseline() {
        let boost = MusicBedLedgerEvaluator.JingleBoost(
            isConfirmed: false,
            spanOverlapsJingle: true // even with overlap, no confirmation = baseline
        )
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: span08,
            fusionConfig: config,
            jingleBoost: boost
        )
        guard let entry = result.entry else {
            Issue.record("Expected ledger entry")
            return
        }
        #expect(abs(entry.weight - MusicBedLedgerEvaluator.musicBedBaselineWeight) < 1e-9,
                "Non-confirmed shows must emit the 0.10 baseline weight")
    }

    @Test("flag-on confirmed but no jingle overlap → 0.10 baseline weight")
    func flagOnConfirmedNoOverlapBaseline() {
        let boost = MusicBedLedgerEvaluator.JingleBoost(
            isConfirmed: true,
            spanOverlapsJingle: false
        )
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: span08,
            fusionConfig: config,
            jingleBoost: boost
        )
        guard let entry = result.entry else {
            Issue.record("Expected ledger entry")
            return
        }
        #expect(abs(entry.weight - MusicBedLedgerEvaluator.musicBedBaselineWeight) < 1e-9,
                "Confirmed show + non-overlapping span = baseline weight")
    }

    // MARK: - Flag on, boosted (0.25)

    @Test("flag-on confirmed AND span overlaps jingle → 0.25 boosted weight")
    func flagOnConfirmedOverlappedBoosted() {
        let boost = MusicBedLedgerEvaluator.JingleBoost(
            isConfirmed: true,
            spanOverlapsJingle: true
        )
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: span08,
            fusionConfig: config,
            jingleBoost: boost
        )
        guard let entry = result.entry else {
            Issue.record("Expected ledger entry")
            return
        }
        #expect(abs(entry.weight - MusicBedLedgerEvaluator.musicBedConfirmedJingleWeight) < 1e-9,
                "Confirmed show + overlapping span = 0.25 boosted weight")
    }

    // MARK: - Non-firing path

    @Test("flag-on does not bypass the 30% presence floor")
    func flagOnStillRespectsPresenceFloor() {
        // 10-window span with only 1 .background — below the 30% floor.
        // Even with the boost provided, the evaluator must NOT fire:
        // the boost is a weight modifier, not a firing override.
        var w: [FeatureWindow] = []
        w.append(bedWindow(start: 0, level: .background))
        for i in 1..<10 {
            w.append(bedWindow(start: Double(i) * 2.0, level: .none))
        }
        let boost = MusicBedLedgerEvaluator.JingleBoost(
            isConfirmed: true,
            spanOverlapsJingle: true
        )
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: w,
            fusionConfig: config,
            jingleBoost: boost
        )
        #expect(result.evaluation.fired == false)
        #expect(result.entry == nil,
                "Boost must not override the 30% presence floor")
    }
}
