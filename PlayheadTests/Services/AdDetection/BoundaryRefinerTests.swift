// BoundaryRefinerTests.swift
// Regression rails for the shared boundary refinement helper that now delegates
// snapping decisions to TimeBoundaryResolver.

import Testing

@testable import Playhead

@Suite("BoundaryRefiner")
struct BoundaryRefinerTests {

    @Test("fewer than three windows leave both adjustments at zero")
    func fewerThanThreeWindowsDoNotAdjust() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 8.5,
            candidateEnd: 8.5
        )

        #expect(actual.startAdjust == 0.0)
        #expect(actual.endAdjust == 0.0)
    }

    @Test("resolver-backed refinement snaps both boundaries within the three-second radius")
    func snapsBothBoundaries() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 19, end: 20, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 20, end: 21, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 21, end: 22, pause: 0.05, spectralFlux: 0.05),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )

        #expect(actual.startAdjust == -2.5)
        #expect(actual.endAdjust == 0.5)
    }

    @Test("weak or out-of-range windows keep both adjustments at zero")
    func weakOrOutOfRangeWindowsDoNotAdjust() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.10, spectralFlux: 0.05),
            makeWindow(start: 8, end: 9, pause: 0.12, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.10, spectralFlux: 0.04),
            makeWindow(start: 24, end: 25, pause: 0.98, spectralFlux: 0.99),
            makeWindow(start: 25, end: 26, pause: 0.97, spectralFlux: 0.98),
            makeWindow(start: 26, end: 27, pause: 0.96, spectralFlux: 0.97),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )

        #expect(actual.startAdjust == 0.0)
        #expect(actual.endAdjust == 0.0)
    }

    @Test("window ordering no longer affects resolver-backed adjustments")
    func orderingDoesNotMatter() {
        let sorted = [
            makeWindow(start: 7, end: 8, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 19, end: 20, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 20, end: 21, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 21, end: 22, pause: 0.05, spectralFlux: 0.05),
        ]
        let unsorted = [
            sorted[4],
            sorted[1],
            sorted[5],
            sorted[0],
            sorted[3],
            sorted[2],
        ]

        let sortedAdjustments = BoundaryRefiner.computeAdjustments(
            windows: sorted,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )
        let unsortedAdjustments = BoundaryRefiner.computeAdjustments(
            windows: unsorted,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )

        #expect(unsortedAdjustments.startAdjust == sortedAdjustments.startAdjust)
        #expect(unsortedAdjustments.endAdjust == sortedAdjustments.endAdjust)
    }

    @Test("resolver-backed refinement prefers local maxima over a closer interior shoulder")
    func prefersLocalMaximaOverCloserInteriorShoulder() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.92, spectralFlux: 0.80),
            makeWindow(start: 8, end: 9, pause: 0.62, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.97, spectralFlux: 0.95),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 8.4,
            candidateEnd: 30.0
        )

        #expect(abs(actual.startAdjust - 0.6) < 0.000_001)
        #expect(actual.endAdjust == 0.0)
    }

    @Test("snap distance keeps adjustments within plus or minus three seconds")
    func snapDistanceCapsAdjustmentMagnitude() {
        let windows = [
            makeWindow(start: 7, end: 8, pause: 0.98, spectralFlux: 0.98),
            makeWindow(start: 10, end: 11, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 12, end: 13, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 27, end: 28, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 29, end: 30, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 32, end: 33, pause: 0.98, spectralFlux: 0.98),
        ]

        let actual = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 10.0,
            candidateEnd: 30.0
        )

        #expect(actual.startAdjust == -3.0)
        #expect(actual.endAdjust == 3.0)
    }

    private func makeWindow(
        start: Double,
        end: Double,
        pause: Double,
        spectralFlux: Double,
        musicProbability: Double = 0.0,
        speakerChangeProxyScore: Double = 0.0,
        musicBedChangeScore: Double = 0.0
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-boundary",
            startTime: start,
            endTime: end,
            rms: 0.05,
            spectralFlux: spectralFlux,
            musicProbability: musicProbability,
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: musicBedChangeScore,
            pauseProbability: pause,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }
}

// MARK: - PostClassifyBoundaryExpansion

// playhead-gtt9.4.1: Post-classify boundary expansion for high-confidence
// classifier windows that fired on a narrow (2-s) LexicalCandidate inside a
// wider ad envelope. Expands the persisted AdWindow extents to the nearest
// AcousticBreak within BoundaryExpander.ExpansionConfig.neutral radii, without
// re-scoring, without changing the classifier score, and without touching the
// ledger.
//
// Regression target: Conan episode 71F0C2AE, GT ad span [7007.34, 7037.34].
// Classifier fired at [7006, 7008] (a 2-s lexical window) at p = 0.815.
// Pre-fix: persisted AdWindow was ~2 s wide → Sec-F1 ≈ 0.04 vs a 30-s GT.
@Suite("PostClassifyBoundaryExpansion")
struct PostClassifyBoundaryExpansionTests {

    private let defaultTypicalAdDuration: ClosedRange<TimeInterval> = 30...90
    private let defaultAutoSkipThreshold: Double = 0.80

    // MARK: - GREEN gate: high-confidence narrow candidate expands to envelope.

    @Test("high-confidence narrow candidate expands to surrounding acoustic-break envelope")
    func highConfidenceNarrowCandidateExpandsToEnvelope() {
        // Synthesize a 30-s ad envelope with strong acoustic breaks at the
        // leading and trailing edges (rms drop + pause cluster + spectral spike
        // on both sides). Inside the envelope, a 2-s classifier hit at
        // [7006, 7008] — analogous to the 71F0C2AE-7260 fixture's [7007..7037]
        // GT ad span with a 2-s lexical window firing mid-envelope.
        let featureWindows = makeEnvelopeWindows(
            envelopeStart: 7007.34,
            envelopeEnd: 7037.34
        )

        let expanded = PostClassifyBoundaryExpansion.expand(
            startTime: 7006.0,
            endTime: 7008.0,
            adProbability: 0.85,
            featureWindows: featureWindows,
            autoSkipConfidenceThreshold: defaultAutoSkipThreshold,
            typicalAdDuration: defaultTypicalAdDuration
        )

        // Expansion should pull the start boundary backward to (or near) the
        // leading acoustic break, and push the end boundary forward to (or
        // near) the trailing one.
        #expect(expanded.startTime <= 7010.0,
                "expected start to expand backward from 7006 toward the leading break near 7007; got \(expanded.startTime)")
        #expect(expanded.startTime >= 7000.0,
                "start should not run backward past a reasonable envelope")
        #expect(expanded.endTime >= 7035.0,
                "expected end to expand forward from 7008 toward the trailing break near 7037; got \(expanded.endTime)")
        #expect(expanded.endTime <= 7045.0,
                "end should not run forward past the trailing break radius")
        // And the width should materially exceed the original 2 s.
        #expect(expanded.endTime - expanded.startTime >= 25.0,
                "expanded span should be at least ~25 s wide, not the original ~2 s")
    }

    // MARK: - Negative: below-threshold hits must not be expanded.

    @Test("low-confidence narrow hit is not expanded")
    func lowConfidenceNarrowHitIsNotExpanded() {
        let featureWindows = makeEnvelopeWindows(
            envelopeStart: 7007.34,
            envelopeEnd: 7037.34
        )

        let expanded = PostClassifyBoundaryExpansion.expand(
            startTime: 7006.0,
            endTime: 7008.0,
            adProbability: 0.50, // below autoSkip (0.80)
            featureWindows: featureWindows,
            autoSkipConfidenceThreshold: defaultAutoSkipThreshold,
            typicalAdDuration: defaultTypicalAdDuration
        )

        #expect(expanded.startTime == 7006.0)
        #expect(expanded.endTime == 7008.0)
    }

    // MARK: - Negative: already-wide candidates are not expanded.

    @Test("already-wide high-confidence candidate is not expanded")
    func wideHighConfidenceCandidateIsNotExpanded() {
        // Candidate that is already ≥ typicalAdDuration.lowerBound / 2 = 15 s wide.
        // Even at high confidence, no expansion should occur.
        let featureWindows = makeEnvelopeWindows(
            envelopeStart: 7007.34,
            envelopeEnd: 7037.34
        )

        let expanded = PostClassifyBoundaryExpansion.expand(
            startTime: 7010.0,
            endTime: 7030.0, // already 20 s wide
            adProbability: 0.90,
            featureWindows: featureWindows,
            autoSkipConfidenceThreshold: defaultAutoSkipThreshold,
            typicalAdDuration: defaultTypicalAdDuration
        )

        #expect(expanded.startTime == 7010.0)
        #expect(expanded.endTime == 7030.0)
    }

    // MARK: - Fallback: no breaks found → typicalAdDuration extent.

    @Test("no acoustic breaks within radius falls back to typicalAdDuration extent")
    func noBreaksFallsBackToTypicalExtent() {
        // Flat feature windows with no energy drops, no spectral spikes, no
        // pause clusters → AcousticBreakDetector returns []. Expansion must
        // still widen the persisted span via the fallback path.
        let featureWindows = makeFlatWindows(
            startTime: 6900.0,
            endTime: 7100.0
        )

        let expanded = PostClassifyBoundaryExpansion.expand(
            startTime: 7006.0,
            endTime: 7008.0,
            adProbability: 0.85,
            featureWindows: featureWindows,
            autoSkipConfidenceThreshold: defaultAutoSkipThreshold,
            typicalAdDuration: defaultTypicalAdDuration
        )

        // Fallback expands to typicalAdDuration.lowerBound (30 s) centered on
        // the original candidate midpoint (7007).
        let width = expanded.endTime - expanded.startTime
        #expect(width >= 20.0,
                "fallback should widen materially beyond the original 2 s; got \(width) s")
        #expect(expanded.startTime < 7006.0 && expanded.endTime > 7008.0,
                "fallback must expand in both directions; got [\(expanded.startTime), \(expanded.endTime)]")
    }

    // MARK: - Helpers

    /// Build acoustic feature windows that contain a 30-s ad envelope between
    /// `envelopeStart` and `envelopeEnd` with strong leading and trailing breaks
    /// (RMS drop + pause cluster + spectral spike), plus bookending content
    /// segments that have consistent mid-level RMS and low pause probability.
    private func makeEnvelopeWindows(
        envelopeStart: Double,
        envelopeEnd: Double
    ) -> [FeatureWindow] {
        var windows: [FeatureWindow] = []
        let windowDuration: Double = 2.0

        // Padding range: 80 s before envelopeStart → 80 s after envelopeEnd,
        // wide enough to cover BoundaryExpander.ExpansionConfig.neutral radii
        // (60 s forward/backward).
        let padding: Double = 80.0
        let windowStart = envelopeStart - padding
        let windowEnd = envelopeEnd + padding

        var t = windowStart
        while t < windowEnd {
            let nextT = t + windowDuration
            let windowCenter = (t + nextT) / 2.0

            // Inside the envelope: quieter, music-bed-y, low pause probability.
            let insideEnvelope = t >= envelopeStart && nextT <= envelopeEnd

            // Leading / trailing break zones — a 2-window-wide "pause cluster"
            // plus a big RMS discontinuity across the boundary.
            let isLeadingBreakZone = (t >= envelopeStart - 2.0 * windowDuration) && (t < envelopeStart)
            let isTrailingBreakZone = (t >= envelopeEnd) && (t < envelopeEnd + 2.0 * windowDuration)

            let rms: Double
            let pauseProbability: Double
            let spectralFlux: Double

            if insideEnvelope {
                // Ad body: lowish but non-silent RMS, calm spectral flux.
                rms = 0.18
                pauseProbability = 0.15
                spectralFlux = 0.05
            } else if isLeadingBreakZone || isTrailingBreakZone {
                // Break zone: near-silence + high pause probability (2 consecutive
                // high-pause windows form a pause cluster) + spectral spike at the
                // transition.
                rms = 0.03
                pauseProbability = 0.95
                spectralFlux = 0.80
            } else {
                // Content: loud, dense speech.
                rms = 0.60
                pauseProbability = 0.10
                spectralFlux = 0.05
            }

            // Mark a synthetic spectral spike window at exact envelope boundaries
            // to make the merge group contain a spectralSpike signal.
            let atLeadingBoundary = abs(windowCenter - envelopeStart) < windowDuration
            let atTrailingBoundary = abs(windowCenter - envelopeEnd) < windowDuration
            let effectiveFlux = (atLeadingBoundary || atTrailingBoundary) ? 0.95 : spectralFlux

            windows.append(FeatureWindow(
                analysisAssetId: "asset-post-classify-test",
                startTime: t,
                endTime: nextT,
                rms: rms,
                spectralFlux: effectiveFlux,
                musicProbability: insideEnvelope ? 0.6 : 0.1,
                speakerChangeProxyScore: 0,
                musicBedChangeScore: 0,
                pauseProbability: pauseProbability,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            ))
            t = nextT
        }

        return windows
    }

    /// Feature windows with no break signals — used to exercise the fallback
    /// path when AcousticBreakDetector returns [].
    private func makeFlatWindows(
        startTime: Double,
        endTime: Double
    ) -> [FeatureWindow] {
        var windows: [FeatureWindow] = []
        let windowDuration: Double = 2.0
        var t = startTime
        while t < endTime {
            let nextT = t + windowDuration
            windows.append(FeatureWindow(
                analysisAssetId: "asset-flat-windows",
                startTime: t,
                endTime: nextT,
                rms: 0.30,
                spectralFlux: 0.10,
                musicProbability: 0.2,
                speakerChangeProxyScore: 0,
                musicBedChangeScore: 0,
                pauseProbability: 0.15,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            ))
            t = nextT
        }
        return windows
    }
}
