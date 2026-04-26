// AcousticLedgerMusicBedTests.swift
// playhead-sqhj (2026-04-26): pins the contract that `.acoustic` is
// the audio-energy break (RMS-drop) signal and is independent of the
// music-bed signal. Music-bed presence reaches production via
// `MusicBedLedgerEvaluator`'s parallel `.musicBed` entry, NOT through
// `.acoustic`; emitting both for a music-bed-only span would
// double-count one physical signal into the quorum gate's
// `distinctKinds.count`.
//
// This suite pins:
//   1. RMS drop alone fires an `.acoustic` entry (back-compat with
//      pre-sqhj behaviour).
//   2. Music-bed coverage alone (no RMS drop) does NOT fire an
//      `.acoustic` entry — the `.musicBed` entry from
//      `MusicBedLedgerEvaluator` is the path that signal takes.
//   3. Empty windows / no signal of either kind still returns no entry
//      (no zero-weight clutter in the ledger).
//   4. Empty span (no overlapping windows) does not crash.

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

    // MARK: - Music-bed-only path (must NOT emit `.acoustic`)

    /// Sustained music-bed coverage with NO RMS drop must NOT produce
    /// an `.acoustic` ledger entry. The music-bed signal reaches
    /// production via `MusicBedLedgerEvaluator`'s parallel `.musicBed`
    /// entry; emitting `.acoustic` on the same span would double-count
    /// the same physical evidence into the quorum gate's
    /// `distinctKinds.count`.
    @Test("music-bed coverage with no RMS drop does NOT fire an .acoustic entry")
    func musicBedAloneDoesNotFireAcousticEntry() async throws {
        let svc = try await makeService()

        // Flat RMS across 10 windows → computeRmsDropScore returns 0.
        // 7/10 windows carry a music bed → fraction 0.70. Pre-fix this
        // would have lifted `.acoustic`; post-fix it must not.
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

        #expect(entries.isEmpty,
                "Music-bed-only span must not emit `.acoustic`; the `.musicBed` entry from MusicBedLedgerEvaluator is that signal's path to production")
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
