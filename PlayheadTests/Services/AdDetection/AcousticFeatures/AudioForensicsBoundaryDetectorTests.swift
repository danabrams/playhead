// AudioForensicsBoundaryDetectorTests.swift
// playhead-xsdz.8: Unit tests for the composite audio-forensics boundary
// evidence channel. These exercise the PURE detector directly (no actor) —
// the flag-gating / byte-identity contract is covered by
// `AudioForensicsFusionTests` at the fusion seam.
//
// Design intent under test:
//   • A span whose START or END edge sits on a strong physical discontinuity
//     (loudness / spectral-flux / noise-floor / production-environment step)
//     gets exactly ONE `.audioForensics` entry whose weight scales with the
//     discontinuity and is capped at `audioForensicsCap`.
//   • A smooth, content-like boundary (no step) gets NO entry.
//   • Edge cases — empty features, zero-variance episode, single-window /
//     episode-edge boundaries — never crash and never fabricate an entry.

import Foundation
import Testing

@testable import Playhead

@Suite("AudioForensicsBoundaryDetector (playhead-xsdz.8)")
struct AudioForensicsBoundaryDetectorTests {

    // MARK: - Helpers

    private func window(
        start: Double,
        duration: Double = 2.0,
        rms: Double,
        spectralFlux: Double = 0.05,
        musicProbability: Double = 0.02
    ) -> FeatureWindow {
        AcousticFeatureFixtures.window(
            startTime: start,
            endTime: start + duration,
            rms: rms,
            spectralFlux: spectralFlux,
            musicProbability: musicProbability
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
            anchorProvenance: [.classifierSeed(regionId: "r1", score: 0.85)]
        )
    }

    /// A flat, content-like episode: uniform quiet host speech with tiny
    /// deterministic jitter so the episode has *some* variance (so
    /// `hasUsableVariance` is true) but no boundary STEP anywhere.
    private func smoothEpisode(count: Int = 40) -> [FeatureWindow] {
        (0..<count).map { i in
            window(
                start: Double(i) * 2.0,
                rms: 0.20 + Double(i % 3) * 0.002,
                spectralFlux: 0.05 + Double(i % 2) * 0.001,
                musicProbability: 0.02
            )
        }
    }

    /// An episode with a strong physical insertion seam: a loud, music-bedded
    /// block from `[insertStart, insertEnd)` sitting inside quiet host speech.
    /// The span is set to that loud block so BOTH its edges land on a step.
    private func episodeWithLoudInsertion(
        insertStartIdx: Int,
        insertEndIdx: Int,
        count: Int = 40
    ) -> [FeatureWindow] {
        (0..<count).map { i in
            let inAd = i >= insertStartIdx && i < insertEndIdx
            return window(
                start: Double(i) * 2.0,
                // Host ≈ 0.20 RMS, ad block ≈ 0.55 RMS (a clear loudness jump,
                // ~+8.8 dBFS) — well out of the episode dBFS distribution.
                rms: inAd ? 0.55 + Double(i % 2) * 0.01 : 0.20 + Double(i % 3) * 0.002,
                // Host flux low/steady; ad block flux elevated (timbral change).
                spectralFlux: inAd ? 0.30 : 0.05 + Double(i % 2) * 0.001,
                // Host dry speech (no music); ad block music-bedded.
                musicProbability: inAd ? 0.80 : 0.02
            )
        }
    }

    // MARK: - Strong discontinuity emits a capped entry

    @Test("Strong boundary discontinuity emits exactly one capped audioForensics entry")
    func strongDiscontinuityEmitsEntry() throws {
        let detector = AudioForensicsBoundaryDetector()
        let windows = episodeWithLoudInsertion(insertStartIdx: 15, insertEndIdx: 25)
        // Span = the loud insertion block. Edge at idx 15 (t=30) and idx 25 (t=50).
        let span = makeSpan(start: 30.0, end: 50.0)
        let cfg = FusionWeightConfig()  // audioForensicsCap = 0.20

        let entries = detector.buildEntries(
            span: span,
            episodeWindows: windows,
            fusionConfig: cfg
        )

        try #require(entries.count == 1, "Expected exactly one merged audioForensics entry (got \(entries.count))")
        let entry = entries[0]
        #expect(entry.source == .audioForensics)
        #expect(entry.weight > 0)
        #expect(entry.weight <= cfg.audioForensicsCap, "Weight must be capped at audioForensicsCap")
        if case .audioForensics(let score, _, let contributing) = entry.detail {
            #expect(score > 0)
            #expect(contributing >= 1, "At least one sub-signal must have fired on a strong seam")
        } else {
            Issue.record("Expected .audioForensics detail")
        }
    }

    @Test("Entry weight scales with discontinuity strength")
    func weightScalesWithDiscontinuity() throws {
        let detector = AudioForensicsBoundaryDetector()
        let cfg = FusionWeightConfig()

        // Mild step: ad block only slightly louder, no music/flux change.
        let mildWindows: [FeatureWindow] = (0..<40).map { i in
            let inAd = i >= 15 && i < 25
            return window(
                start: Double(i) * 2.0,
                rms: inAd ? 0.30 : 0.20 + Double(i % 3) * 0.002,
                spectralFlux: 0.05,
                musicProbability: 0.02
            )
        }
        // Strong step: large loudness + flux + music jump (the full seam).
        let strongWindows = episodeWithLoudInsertion(insertStartIdx: 15, insertEndIdx: 25)
        let span = makeSpan(start: 30.0, end: 50.0)

        let mild = detector.buildEntries(span: span, episodeWindows: mildWindows, fusionConfig: cfg)
        let strong = detector.buildEntries(span: span, episodeWindows: strongWindows, fusionConfig: cfg)

        try #require(strong.count == 1, "Strong seam must emit an entry")
        // The strong seam must out-weigh the mild one (or the mild one must be
        // below the min-boundary floor and emit nothing).
        let mildWeight = mild.first?.weight ?? 0
        #expect(strong[0].weight > mildWeight,
                "A stronger, multi-modal discontinuity must carry more weight than a mild single-modal one")
    }

    // MARK: - Noise-floor / loudness decorrelation (deferred-correlation fix)

    @Test("Pure uniform loudness jump does NOT double-count via the noise-floor sub-signal")
    func uniformLoudnessJumpDoesNotDoubleCountNoiseFloor() throws {
        // A seam that is a PURE, uniform level change: within each side the RMS
        // is constant (so the 5th-percentile floor and the median move by the
        // SAME amount across the edge) and flux / musicProbability are constant
        // episode-wide (so those sub-signals have zero variance and cannot
        // fire). Before the decorrelation fix, the noise-floor sub-signal fired
        // IDENTICALLY to loudness — inflating contributingSignalCount to 2 and
        // the top-2 merge for ONE physical loudness event. After the fix, the
        // floor step is measured relative to the median (loudness) step, so a
        // uniform gain change contributes nothing to noise-floor.
        let detector = AudioForensicsBoundaryDetector()
        let windows: [FeatureWindow] = (0..<40).map { i in
            let inAd = i >= 15 && i < 25
            return window(
                start: Double(i) * 2.0,
                // Constant within each side ⇒ floor == median on each side ⇒
                // floor step == loudness step exactly.
                rms: inAd ? 0.55 : 0.20,
                spectralFlux: 0.05,        // constant ⇒ fluxSigma == 0 ⇒ no spectral signal
                musicProbability: 0.02     // constant ⇒ musicProbSigma == 0 ⇒ no environment signal
            )
        }
        let span = makeSpan(start: 30.0, end: 50.0)

        let entries = detector.buildEntries(
            span: span,
            episodeWindows: windows,
            fusionConfig: FusionWeightConfig()
        )

        try #require(entries.count == 1, "A clear uniform loudness jump must still emit an entry")
        if case .audioForensics(_, let dominant, let contributing) = entries[0].detail {
            #expect(contributing == 1,
                    "Only the loudness sub-signal may fire; the noise-floor sub-signal must NOT double-count a uniform level change")
            #expect(dominant == "loudnessJump",
                    "The single contributing sub-signal must be the loudness jump")
        } else {
            Issue.record("Expected .audioForensics detail")
        }
    }

    @Test("Decorrelation does not suppress a genuine multi-modal floor+loudness+timbre seam")
    func multiModalSeamStillCorroborates() throws {
        // Guard against OVER-correction: subtracting the common-mode level shift
        // must not zero out a real, multi-modal insertion seam. The loud
        // music-bedded insertion fixture changes loudness AND flux AND music
        // probability across the edge, so at least one NON-loudness sub-signal
        // must still corroborate (contributingCount >= 2) — the decorrelation
        // only removes the loudness-redundant part of the noise-floor sub-signal,
        // not the independent spectral / environment evidence.
        let detector = AudioForensicsBoundaryDetector()
        let windows = episodeWithLoudInsertion(insertStartIdx: 15, insertEndIdx: 25)
        let span = makeSpan(start: 30.0, end: 50.0)

        let entries = detector.buildEntries(
            span: span,
            episodeWindows: windows,
            fusionConfig: FusionWeightConfig()
        )

        try #require(entries.count == 1)
        if case .audioForensics(_, _, let contributing) = entries[0].detail {
            #expect(contributing >= 2,
                    "A multi-modal seam (loudness + flux + music) must still corroborate across >= 2 sub-signals after decorrelation")
        } else {
            Issue.record("Expected .audioForensics detail")
        }
    }

    // MARK: - Smooth (content-like) boundary

    @Test("Smooth content-like boundary emits no entry")
    func smoothBoundaryNoEntry() {
        let detector = AudioForensicsBoundaryDetector()
        let windows = smoothEpisode()
        // Span sits in the middle of uniform host content — no step at either edge.
        let span = makeSpan(start: 30.0, end: 50.0)

        let entries = detector.buildEntries(
            span: span,
            episodeWindows: windows,
            fusionConfig: FusionWeightConfig()
        )

        #expect(entries.isEmpty,
                "A smooth, content-like boundary must NOT emit an audioForensics entry")
    }

    // MARK: - Edge cases

    @Test("Empty feature windows → no entry, no crash")
    func emptyWindowsNoEntry() {
        let detector = AudioForensicsBoundaryDetector()
        let entries = detector.buildEntries(
            span: makeSpan(start: 30.0, end: 50.0),
            episodeWindows: [],
            fusionConfig: FusionWeightConfig()
        )
        #expect(entries.isEmpty)
    }

    @Test("Too few windows (< 3) → no entry")
    func tooFewWindowsNoEntry() {
        let detector = AudioForensicsBoundaryDetector()
        let windows = [
            window(start: 0, rms: 0.2),
            window(start: 2, rms: 0.5),
        ]
        let entries = detector.buildEntries(
            span: makeSpan(start: 2.0, end: 4.0),
            episodeWindows: windows,
            fusionConfig: FusionWeightConfig()
        )
        #expect(entries.isEmpty, "Fewer than 3 windows cannot be honestly normalized")
    }

    @Test("Zero-variance episode (perfectly flat) → no entry")
    func zeroVarianceNoEntry() {
        let detector = AudioForensicsBoundaryDetector()
        // Every window identical: zero population stddev for all features.
        let windows: [FeatureWindow] = (0..<30).map { i in
            window(start: Double(i) * 2.0, rms: 0.30, spectralFlux: 0.10, musicProbability: 0.10)
        }
        let entries = detector.buildEntries(
            span: makeSpan(start: 20.0, end: 40.0),
            episodeWindows: windows,
            fusionConfig: FusionWeightConfig()
        )
        #expect(entries.isEmpty,
                "A perfectly flat episode has no distribution to sigma-normalize against → no entry")
    }

    @Test("Boundary at the very first episode window (no outside context) → no entry from that edge")
    func boundaryAtEpisodeStartNoOutsideContext() {
        let detector = AudioForensicsBoundaryDetector()
        // A loud block right at the START of the episode: the start edge has no
        // "outside" windows, so its step is unmeasurable. The end edge (into
        // quiet host) DOES have a step, so an entry from the end edge is fine —
        // the detector must not crash on the missing-outside start edge.
        let windows = episodeWithLoudInsertion(insertStartIdx: 0, insertEndIdx: 8)
        let span = makeSpan(start: 0.0, end: 16.0)

        // Should not crash; whatever it returns is driven by the END edge only.
        let entries = detector.buildEntries(
            span: span,
            episodeWindows: windows,
            fusionConfig: FusionWeightConfig()
        )
        // The end edge (idx 8) is a real loud→quiet seam, so we expect an entry,
        // and it must be a single, capped, valid one.
        if let entry = entries.first {
            #expect(entry.source == .audioForensics)
            #expect(entry.weight <= FusionWeightConfig().audioForensicsCap)
        }
        #expect(entries.count <= 1, "Detector emits at most one merged entry")
    }

    @Test("Single-window span → no crash, at most one entry")
    func singleWindowSpanNoCrash() {
        let detector = AudioForensicsBoundaryDetector()
        let windows = episodeWithLoudInsertion(insertStartIdx: 15, insertEndIdx: 25)
        // A degenerate span covering exactly one window: start==edge, end one window later.
        let span = makeSpan(start: 30.0, end: 32.0)

        let entries = detector.buildEntries(
            span: span,
            episodeWindows: windows,
            fusionConfig: FusionWeightConfig()
        )
        #expect(entries.count <= 1)
    }

    // MARK: - EpisodeFeatureStats unit

    @Test("EpisodeFeatureStats reports zero variance for a flat episode")
    func statsFlatEpisode() {
        let windows: [FeatureWindow] = (0..<10).map { i in
            window(start: Double(i) * 2.0, rms: 0.3, spectralFlux: 0.1, musicProbability: 0.1)
        }
        let stats = EpisodeFeatureStats(windows: windows, rmsFloor: 1e-6)
        #expect(stats.loudnessSigma == 0)
        #expect(stats.fluxSigma == 0)
        #expect(stats.musicProbSigma == 0)
        #expect(stats.hasUsableVariance == false)
    }

    @Test("EpisodeFeatureStats reports positive variance when features vary")
    func statsVaryingEpisode() {
        let windows = episodeWithLoudInsertion(insertStartIdx: 15, insertEndIdx: 25)
        let stats = EpisodeFeatureStats(windows: windows, rmsFloor: 1e-6)
        #expect(stats.loudnessSigma > 0)
        #expect(stats.fluxSigma > 0)
        #expect(stats.musicProbSigma > 0)
        #expect(stats.hasUsableVariance)
    }
}
