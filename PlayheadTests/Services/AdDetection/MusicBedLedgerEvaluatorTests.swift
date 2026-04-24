// MusicBedLedgerEvaluatorTests.swift
// 2026-04-23 Finding 4: `MusicBedLedgerEvaluator` threads the existing
// `FeatureWindow.musicBedLevel` classification into the fused evidence
// ledger as a distinct `.musicBed` source kind. These tests exercise
// the pure evaluator on synthetic FeatureWindow sequences to verify:
//
//   1. Elevated music-bed coverage fires a `.musicBed` ledger entry
//      (distinct from `.acoustic` so the quorum gate's
//      `distinctKinds.count` increments).
//   2. Uniformly-low music-bed coverage (all `.none`) does NOT fire.
//   3. Below-threshold sparse music (e.g. 1 of 10 windows) does NOT
//      fire — guards against spectral noise producing phantom evidence.
//   4. Spans with too few windows do NOT fire, so a single foreground
//      window in a 2-window span cannot produce a maxed-out entry.
//
// These are RED-phase assertions: the evaluator stub in
// `MusicBedLedgerEvaluator.swift` currently returns `nil` unconditionally,
// so `elevatedMusicBedFires` and the weight/fraction expectations MUST
// fail. The "does NOT fire" cases pass for the wrong reason at RED —
// they assert the eventual GREEN behaviour.

import Foundation
import Testing

@testable import Playhead

// MARK: - Test helpers

/// Build a synthetic FeatureWindow with a target `MusicBedLevel`.
/// Mirrors the helper in BracketDetectorTests so the two suites are
/// byte-alike for humans comparing coverage.
private func window(
    start: Double,
    duration: Double = 2.0,
    musicBedLevel: MusicBedLevel = .none,
    rms: Double = 0.1
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: "test-asset",
        startTime: start,
        endTime: start + duration,
        rms: rms,
        spectralFlux: 0.05,
        musicProbability: musicBedLevel == .none ? 0 : 0.8,
        musicBedOnsetScore: 0,
        musicBedOffsetScore: 0,
        musicBedLevel: musicBedLevel,
        pauseProbability: 0,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 4
    )
}

/// Standard fusion config for these tests (default acousticCap = 0.2).
private let config = FusionWeightConfig()

// MARK: - Suite

@Suite("MusicBedLedgerEvaluator")
struct MusicBedLedgerEvaluatorTests {

    // MARK: Fires path

    @Test("elevated music-bed coverage fires a .musicBed entry")
    func elevatedMusicBedFires() {
        // 10-window span: 7 `.background`, 1 `.foreground`, 2 `.none`
        //   → presenceFraction = 0.80 (≥ 0.30 threshold), 1 foreground.
        // Spans a realistic "ad-with-bed" production geometry.
        var windows: [FeatureWindow] = []
        for i in 0..<7 {
            windows.append(window(start: Double(i) * 2.0, musicBedLevel: .background))
        }
        windows.append(window(start: 14.0, musicBedLevel: .foreground))
        windows.append(window(start: 16.0, musicBedLevel: .none))
        windows.append(window(start: 18.0, musicBedLevel: .none))

        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: windows,
            fusionConfig: config
        )

        #expect(result.evaluation.fired == true,
                "Expected evaluator to fire when ≥30% of windows carry a music bed")
        #expect(abs(result.evaluation.presenceFraction - 0.8) < 1e-9,
                "presenceFraction should be 8/10 = 0.8")
        #expect(result.evaluation.foregroundCount == 1)
        #expect(result.evaluation.backgroundCount == 7)

        // The ledger entry must be produced with .musicBed source kind.
        // That's the whole point: distinctKinds.count in the quorum gate
        // only increments if source != .acoustic.
        guard let entry = result.entry else {
            Issue.record("Expected a ledger entry; got nil")
            return
        }
        #expect(entry.source == .musicBed)

        // Weight is positive and capped at acousticCap (shared family).
        #expect(entry.weight > 0)
        #expect(entry.weight <= config.acousticCap)

        // Detail payload carries the diagnostic values.
        if case .musicBed(let presenceFraction, let foregroundCount) = entry.detail {
            #expect(abs(presenceFraction - 0.8) < 1e-9)
            #expect(foregroundCount == 1)
        } else {
            Issue.record("Expected .musicBed detail; got \(entry.detail)")
        }
    }

    @Test("heavy foreground music fires with a distinct entry")
    func foregroundHeavyFires() {
        // 6-window span, 4 .foreground + 2 .background.
        // presenceFraction = 1.0, foregroundCount = 4.
        var windows: [FeatureWindow] = []
        for i in 0..<4 {
            windows.append(window(start: Double(i) * 2.0, musicBedLevel: .foreground))
        }
        for i in 4..<6 {
            windows.append(window(start: Double(i) * 2.0, musicBedLevel: .background))
        }

        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: windows,
            fusionConfig: config
        )

        #expect(result.evaluation.fired == true)
        #expect(result.evaluation.foregroundCount == 4)
        #expect(abs(result.evaluation.presenceFraction - 1.0) < 1e-9)
        #expect(result.entry?.source == .musicBed)
    }

    // MARK: Does-not-fire paths

    @Test("uniformly-low music-bed coverage does NOT fire")
    func uniformLowDoesNotFire() {
        // 10-window span, all `.none`.
        let windows = (0..<10).map {
            window(start: Double($0) * 2.0, musicBedLevel: .none)
        }

        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: windows,
            fusionConfig: config
        )

        #expect(result.evaluation.fired == false)
        #expect(result.entry == nil,
                "No entry should be emitted when every window is .none")
        #expect(result.evaluation.presenceFraction == 0)
    }

    @Test("sparse music below 30% threshold does NOT fire")
    func sparseSubthresholdDoesNotFire() {
        // 10-window span, 2 .background, 8 .none → presenceFraction = 0.20.
        // Below the 0.30 floor — treated as spectral noise, not a bed.
        var windows: [FeatureWindow] = []
        windows.append(window(start: 0, musicBedLevel: .background))
        windows.append(window(start: 2, musicBedLevel: .background))
        for i in 2..<10 {
            windows.append(window(start: Double(i) * 2.0, musicBedLevel: .none))
        }

        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: windows,
            fusionConfig: config
        )

        #expect(result.evaluation.fired == false)
        #expect(result.entry == nil)
        #expect(abs(result.evaluation.presenceFraction - 0.2) < 1e-9,
                "Still report the fraction for diagnostics, but don't fire")
    }

    @Test("too-short span (< 3 windows) does NOT fire")
    func tooShortDoesNotFire() {
        // 2-window span with both `.foreground` — fraction 1.0 but
        // too few windows to trust as an in-audio signal.
        let windows = [
            window(start: 0, musicBedLevel: .foreground),
            window(start: 2, musicBedLevel: .foreground),
        ]

        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: windows,
            fusionConfig: config
        )

        #expect(result.evaluation.fired == false,
                "Short spans must not fire — a 2-window window is too thin to trust as a production bed")
        #expect(result.entry == nil)
    }

    @Test("empty span yields no entry and does not crash")
    func emptyWindowsSafe() {
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: [],
            fusionConfig: config
        )
        #expect(result.evaluation.fired == false)
        #expect(result.entry == nil)
    }

    // MARK: Source-type / family wiring

    @Test(".musicBed is a distinct EvidenceSourceType from .acoustic")
    func musicBedIsDistinctSource() {
        // Sanity: the quorum gate counts distinct source kinds.
        // If .musicBed == .acoustic this optimisation is useless.
        #expect(EvidenceSourceType.musicBed != EvidenceSourceType.acoustic)
    }

    @Test(".musicBed lives in the .acoustic evidence family")
    func musicBedIsAcousticFamily() {
        #expect(SourceEvidenceFamily.for(.musicBed) == .acoustic)
        #expect(SourceEvidenceFamily.for(.musicBed) == SourceEvidenceFamily.for(.acoustic))
    }
}
