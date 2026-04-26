// AcousticLedgerMusicBedTests.swift
// playhead-sqhj (2026-04-26 follow-up to gtt9.4): the 2026-04-23 corpus
// eval found `.acoustic` ledger entries firing on only ~2% of decisions
// because `buildAcousticLedgerEntries` early-exits whenever the boundary
// RMS-drop score is zero. `MusicBedLevel` is computed in `FeatureWindow`
// (and consumed by `BracketDetector` for boundary refinement) but was
// not threaded into the acoustic ledger entry.
//
// This suite pins the post-fix behaviour:
//   1. RMS drop alone still fires (back-compat with pre-sqhj behaviour).
//   2. Music-bed coverage alone (≥30% of windows non-`.none`) now fires
//      an `.acoustic` entry even when RMS drop is zero.
//   3. Empty windows / no signal of either kind still returns no entry
//      (no zero-weight clutter in the ledger).
//   4. The `.acoustic` source is preserved (this is `.acoustic`, not
//      `.musicBed`; `MusicBedLedgerEvaluator` continues to emit the
//      distinct `.musicBed` entry for distinctKinds purposes).

import Foundation
import Testing

@testable import Playhead

@Suite("buildAcousticLedgerEntries — MusicBed wiring")
struct AcousticLedgerMusicBedTests {

    // MARK: - Helpers

    /// Synthesise a `FeatureWindow` with controllable RMS and
    /// `MusicBedLevel`. Mirrors the helper used by
    /// `MusicBedLedgerEvaluatorTests` so the two suites are visually
    /// alike for cross-reference.
    private func window(
        start: Double,
        duration: Double = 2.0,
        rms: Double = 0.10,
        musicBedLevel: MusicBedLevel = .none
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

    private func makeSpan(start: Double, end: Double) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "test-asset", firstAtomOrdinal: 0, lastAtomOrdinal: 10),
            assetId: "test-asset",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: start,
            endTime: end,
            anchorProvenance: []
        )
    }

    private func makeService() async throws -> AdDetectionService {
        let store = try await makeTestStore()
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "sqhj-test",
                fmBackfillMode: .off
            )
        )
    }

    // MARK: - Music-bed-only path (the new behaviour)

    /// Sustained music-bed coverage with NO RMS drop must still produce
    /// an `.acoustic` ledger entry. This is the regression the bead
    /// fixes: previously the early-exit on `breakStrength == 0`
    /// silently dropped the signal even though `MusicBedLevel` was
    /// fully populated.
    @Test("music-bed coverage with no RMS drop fires an .acoustic entry")
    func musicBedAloneFiresAcousticEntry() async throws {
        let svc = try await makeService()

        // Flat RMS across 10 windows → computeRmsDropScore returns 0.
        // 7/10 windows carry a music bed → fraction 0.70, well above
        // the 0.30 floor and ≥ minWindowsRequired.
        var windows: [FeatureWindow] = []
        for i in 0..<7 {
            windows.append(window(start: Double(i) * 2.0, rms: 0.10, musicBedLevel: .background))
        }
        for i in 7..<10 {
            windows.append(window(start: Double(i) * 2.0, rms: 0.10, musicBedLevel: .none))
        }

        let span = makeSpan(start: 0, end: 20)
        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: windows,
            fusionConfig: FusionWeightConfig()
        )

        try #require(entries.count == 1,
                     "Expected one .acoustic entry from music-bed coverage; got \(entries.count)")
        let only = entries[0]
        #expect(only.source == .acoustic,
                "Music-bed-augmented entry must stay on .acoustic source; .musicBed is the parallel path")
        #expect(only.weight > 0,
                "Combined-strength entry must carry a positive weight when music bed is present")
        if case .acoustic(let strength) = only.detail {
            #expect(strength > 0, "Acoustic detail strength must be positive when music bed is present")
        } else {
            Issue.record("Expected .acoustic detail variant; got \(only.detail)")
        }
    }

    // MARK: - RMS-only path (back-compat)

    /// Pure boundary RMS drop without music coverage still fires.
    /// Sanity: pre-sqhj behaviour is preserved.
    @Test("RMS drop with no music bed still fires an .acoustic entry")
    func rmsDropAloneStillFires() async throws {
        let svc = try await makeService()

        // 5-window span with a clear energy drop in the middle.
        // The exact magnitude depends on `RegionScoring.computeRmsDropScore`
        // — what we assert is that the entry is non-empty.
        let windows: [FeatureWindow] = [
            window(start: 0,  rms: 0.40, musicBedLevel: .none),
            window(start: 2,  rms: 0.40, musicBedLevel: .none),
            window(start: 4,  rms: 0.05, musicBedLevel: .none),
            window(start: 6,  rms: 0.05, musicBedLevel: .none),
            window(start: 8,  rms: 0.05, musicBedLevel: .none),
        ]
        let span = makeSpan(start: 0, end: 10)
        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: windows,
            fusionConfig: FusionWeightConfig()
        )

        try #require(entries.count == 1)
        #expect(entries[0].source == .acoustic)
        #expect(entries[0].weight > 0)
    }

    // MARK: - No-signal path

    /// Flat RMS AND no music coverage → nothing fires. Avoids
    /// cluttering the ledger with zero-weight entries.
    @Test("flat RMS and no music bed yields no entry")
    func neitherSignalYieldsNoEntry() async throws {
        let svc = try await makeService()

        let windows = (0..<6).map { window(start: Double($0) * 2.0, rms: 0.10, musicBedLevel: .none) }
        let span = makeSpan(start: 0, end: 12)
        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: windows,
            fusionConfig: FusionWeightConfig()
        )

        #expect(entries.isEmpty,
                "No RMS drop and no music bed must not produce a ledger entry")
    }

    /// Sub-threshold music coverage (below the 30% floor) with no RMS
    /// drop must NOT fire — this is the same conservative threshold
    /// `MusicBedLedgerEvaluator` uses; the acoustic path matches it
    /// to avoid trusting spectral noise.
    @Test("sub-threshold music coverage with no RMS drop does not fire")
    func subThresholdMusicDoesNotFire() async throws {
        let svc = try await makeService()

        // 2 of 10 windows = 0.20 fraction → below 0.30 floor.
        var windows: [FeatureWindow] = []
        windows.append(window(start: 0, rms: 0.10, musicBedLevel: .background))
        windows.append(window(start: 2, rms: 0.10, musicBedLevel: .background))
        for i in 2..<10 {
            windows.append(window(start: Double(i) * 2.0, rms: 0.10, musicBedLevel: .none))
        }

        let span = makeSpan(start: 0, end: 20)
        let entries = await svc.buildAcousticLedgerEntries(
            span: span,
            featureWindows: windows,
            fusionConfig: FusionWeightConfig()
        )
        #expect(entries.isEmpty,
                "Sparse music below the 30% floor must not lift the acoustic entry")
    }

    /// No windows in the span → no entry, no crash. (Pre-sqhj guard
    /// must remain.)
    @Test("empty span yields no entry") func emptySpanYieldsNoEntry() async throws {
        let svc = try await makeService()
        let entries = await svc.buildAcousticLedgerEntries(
            span: makeSpan(start: 100, end: 110),
            featureWindows: [],
            fusionConfig: FusionWeightConfig()
        )
        #expect(entries.isEmpty)
    }
}
