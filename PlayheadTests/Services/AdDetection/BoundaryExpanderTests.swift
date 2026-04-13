// BoundaryExpanderTests.swift
// Tests for the BoundaryExpander stateless utility that converts a single
// user-tap seed time into full ad start/end boundaries.

import Testing
import Foundation

@testable import Playhead

@Suite("BoundaryExpander")
struct BoundaryExpanderTests {

    private let expander = BoundaryExpander()

    // MARK: - Test 1: Existing AdWindow overlap adopts those boundaries

    @Test("existing AdWindow overlap adopts window boundaries with highest priority")
    func existingWindowOverlap() {
        let seed = 75.0
        let adWindows = [
            makeAdWindow(start: 60, end: 90, confidence: 0.8),
        ]

        let result = expander.expand(
            seed: seed,
            featureWindows: [],
            transcriptChunks: [],
            adWindows: adWindows
        )

        #expect(result.source == .existingWindow)
        #expect(result.startTime == 60.0)
        #expect(result.endTime == 90.0)
        #expect(result.boundaryConfidence >= 0.8)
    }

    @Test("adjacent AdWindow within 5s is adopted")
    func adjacentWindowAdopted() {
        let seed = 57.0 // 3s before window start — within adjacency threshold
        let adWindows = [
            makeAdWindow(start: 60, end: 90, confidence: 0.75),
        ]

        let result = expander.expand(
            seed: seed,
            featureWindows: [],
            transcriptChunks: [],
            adWindows: adWindows
        )

        #expect(result.source == .existingWindow)
        #expect(result.startTime == 60.0)
        #expect(result.endTime == 90.0)
    }

    @Test("multiple overlapping AdWindows are unioned")
    func multipleOverlappingWindows() {
        let seed = 85.0
        let adWindows = [
            makeAdWindow(start: 60, end: 90, confidence: 0.7),
            makeAdWindow(start: 88, end: 120, confidence: 0.8),
        ]

        let result = expander.expand(
            seed: seed,
            featureWindows: [],
            transcriptChunks: [],
            adWindows: adWindows
        )

        #expect(result.source == .existingWindow)
        #expect(result.startTime == 60.0)
        #expect(result.endTime == 120.0)
        #expect(result.boundaryConfidence >= 0.8)
    }

    @Test("seed in gap between two non-overlapping adjacent windows picks nearest")
    func seedInGapPicksNearest() {
        // Two windows both within 5s adjacency of the seed, but not overlapping
        // each other. Seed is closer to the second window.
        let seed = 97.0 // gap between [60,90] and [100,120]; closer to [100,120]
        let adWindows = [
            makeAdWindow(start: 60, end: 90, confidence: 0.7),
            makeAdWindow(start: 100, end: 120, confidence: 0.85),
        ]

        let result = expander.expand(
            seed: seed,
            featureWindows: [],
            transcriptChunks: [],
            adWindows: adWindows
        )

        #expect(result.source == .existingWindow)
        // Seed is 7s from end of [60,90] and 3s from start of [100,120].
        // Distance-based fallback should pick [100,120].
        #expect(result.startTime == 100.0, "Should pick the nearer window: got \(result.startTime)")
        #expect(result.endTime == 120.0)
        #expect(result.boundaryConfidence >= 0.85)
    }

    // MARK: - Test 2: Acoustic + lexical signals narrow to lexical markers

    @Test("acoustic and lexical signals produce acousticAndLexical source")
    func acousticAndLexical() {
        // Feature windows with clear silence points around seed.
        let featureWindows = [
            makeFeatureWindow(start: 55, end: 56, pauseProb: 0.9, rms: 0.01),
            makeFeatureWindow(start: 105, end: 106, pauseProb: 0.85, rms: 0.02),
        ]

        // Transcript chunks with sponsor intro before seed and promo after seed.
        // Both chunks have strong signals that individually bypass minHitsForCandidate.
        // The merge gap (30s default) will combine them since the gap
        // between chunk1 end (75) and chunk2 start (85) is only 10s.
        let chunks = [
            makeTranscriptChunk(
                start: 62, end: 75,
                text: "this episode is sponsored by Acme Corp use code SAVE20",
                normalizedText: "this episode is sponsored by acme corp use code save20"
            ),
            makeTranscriptChunk(
                start: 85, end: 98,
                text: "go to acme dot com and use code SAVE20 at checkout let s get back to the show",
                normalizedText: "go to acme dot com and use code save20 at checkout let s get back to the show"
            ),
        ]

        let result = expander.expand(
            seed: 80.0,
            featureWindows: featureWindows,
            transcriptChunks: chunks,
            adWindows: []
        )

        #expect(result.source == .acousticAndLexical)
        #expect(result.boundaryConfidence >= 0.7)
        // The lexical candidate spans ~62–98; acoustic boundaries are 55 and 106.
        // Narrowing: start = max(55, ~62) ≈ 62, end = min(106, ~98) ≈ 98.
        #expect(result.startTime < 80.0)
        #expect(result.endTime > 80.0)
    }

    @Test("explicit neutral config preserves the legacy default expansion exactly")
    func neutralConfigMatchesLegacyDefault() {
        let featureWindows = [
            makeFeatureWindow(start: 55, end: 56, pauseProb: 0.9, rms: 0.01),
            makeFeatureWindow(start: 105, end: 106, pauseProb: 0.85, rms: 0.02),
        ]
        let chunks = [
            makeTranscriptChunk(
                start: 62, end: 75,
                text: "this episode is sponsored by Acme Corp use code SAVE20",
                normalizedText: "this episode is sponsored by acme corp use code save20"
            ),
            makeTranscriptChunk(
                start: 85, end: 98,
                text: "go to acme dot com and use code SAVE20 at checkout let s get back to the show",
                normalizedText: "go to acme dot com and use code save20 at checkout let s get back to the show"
            ),
        ]

        let legacy = expander.expand(
            seed: 80.0,
            featureWindows: featureWindows,
            transcriptChunks: chunks,
            adWindows: []
        )
        let explicitNeutral = expander.expand(
            seed: 80.0,
            featureWindows: featureWindows,
            transcriptChunks: chunks,
            adWindows: [],
            config: .neutral
        )

        #expect(legacy == explicitNeutral)
    }

    @Test("end anchored config searches farther backward than the neutral preset")
    func endAnchoredExpandsFartherBackward() {
        let featureWindows = [
            makeFeatureWindow(start: 15, end: 16, pauseProb: 0.95, rms: 0.01),
            makeFeatureWindow(start: 110, end: 111, pauseProb: 0.93, rms: 0.01),
        ]

        let neutral = expander.expand(
            seed: 100.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: [],
            config: .neutral
        )
        let endAnchored = expander.expand(
            seed: 100.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: [],
            config: .endAnchored
        )

        #expect(neutral.source == .acousticOnly)
        #expect(neutral.startTime == 70.0)
        #expect(neutral.endTime == 111.0)

        #expect(endAnchored.source == .acousticOnly)
        #expect(endAnchored.startTime == 15.0)
        #expect(endAnchored.endTime == 111.0)
    }

    @Test("start anchored config searches farther forward than the neutral preset")
    func startAnchoredExpandsFartherForward() {
        let featureWindows = [
            makeFeatureWindow(start: 90, end: 91, pauseProb: 0.94, rms: 0.01),
            makeFeatureWindow(start: 185, end: 186, pauseProb: 0.96, rms: 0.01),
        ]

        let neutral = expander.expand(
            seed: 100.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: [],
            config: .neutral
        )
        let startAnchored = expander.expand(
            seed: 100.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: [],
            config: .startAnchored
        )

        #expect(neutral.source == .acousticOnly)
        #expect(neutral.startTime == 90.0)
        #expect(neutral.endTime == 130.0)

        #expect(startAnchored.source == .acousticOnly)
        #expect(startAnchored.startTime == 90.0)
        #expect(startAnchored.endTime == 186.0)
    }

    @Test("end anchored lexical search reaches candidates that the neutral preset leaves behind")
    func endAnchoredLexicalSearchReachesFartherBackward() {
        let chunks = [
            makeTranscriptChunk(
                start: 0, end: 15,
                text: "visit betterhelp.com/podcast for details",
                normalizedText: "visit betterhelp com podcast for details"
            ),
        ]

        let neutral = expander.expand(
            seed: 110.0,
            featureWindows: [],
            transcriptChunks: chunks,
            adWindows: [],
            config: .neutral
        )
        let endAnchored = expander.expand(
            seed: 110.0,
            featureWindows: [],
            transcriptChunks: chunks,
            adWindows: [],
            config: .endAnchored
        )

        #expect(neutral.source == .fallback)
        #expect(endAnchored.source == .acousticAndLexical)
        #expect(endAnchored.startTime < neutral.startTime)
        #expect(endAnchored.endTime < neutral.endTime)
    }

    // MARK: - Test 3: Acoustic only uses best silence points

    @Test("acoustic only uses silence points when no lexical signals")
    func acousticOnly() {
        let featureWindows = [
            // Strong silence point before seed.
            makeFeatureWindow(start: 50, end: 51, pauseProb: 0.95, rms: 0.005),
            // Noise between.
            makeFeatureWindow(start: 70, end: 71, pauseProb: 0.1, rms: 0.5),
            // Strong silence point after seed.
            makeFeatureWindow(start: 110, end: 111, pauseProb: 0.92, rms: 0.01),
        ]

        let result = expander.expand(
            seed: 80.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: []
        )

        #expect(result.source == .acousticOnly)
        #expect(result.startTime == 50.0) // fw.startTime for backward
        #expect(result.endTime == 111.0) // fw.endTime for forward
        #expect(result.boundaryConfidence == 0.55)
    }

    @Test("acoustic only with backward silence but no forward silence")
    func acousticOnlyPartial() {
        let featureWindows = [
            makeFeatureWindow(start: 50, end: 51, pauseProb: 0.9, rms: 0.01),
            // No high-scoring windows after seed.
        ]

        let result = expander.expand(
            seed: 80.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: []
        )

        #expect(result.source == .acousticOnly)
        #expect(result.startTime == 50.0)
        // Forward falls back to seed + 30.
        #expect(result.endTime == 110.0)
    }

    @Test("distance penalty prefers the closer acoustic boundary when scores are comparable")
    func acousticDistancePenaltyPrefersCloserBoundary() {
        let featureWindows = [
            makeFeatureWindow(start: 52, end: 53, pauseProb: 0.85, rms: 0.01),
            makeFeatureWindow(start: 80, end: 81, pauseProb: 0.8, rms: 0.01),
        ]

        let result = expander.expand(
            seed: 100.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: []
        )

        #expect(result.source == .acousticOnly)
        #expect(result.startTime == 80.0)
        #expect(result.endTime == 130.0)
    }

    @Test("resolver-selected boundaries at the exact seed time are preserved")
    func exactSeedBoundaryPreserved() {
        let featureWindows = [
            makeFeatureWindow(start: 80, end: 80, pauseProb: 1.0, rms: 0.0),
        ]

        let result = expander.expand(
            seed: 80.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: []
        )

        #expect(result.source == .acousticOnly)
        #expect(result.startTime == 80.0)
        #expect(result.endTime == 80.0)
    }

    // MARK: - Test 4: No signals → fallback seed ± 30s

    @Test("no signals produces fallback with seed plus minus 30 seconds")
    func fallbackNoSignals() {
        let result = expander.expand(
            seed: 80.0,
            featureWindows: [],
            transcriptChunks: [],
            adWindows: []
        )

        #expect(result.source == .fallback)
        #expect(result.startTime == 50.0)
        #expect(result.endTime == 110.0)
        #expect(result.boundaryConfidence == 0.3)
    }

    @Test("fallback snaps to nearby silence when feature windows present but below threshold")
    func fallbackSnapsToSilence() {
        // Windows that are too far from seed to be found by acoustic search
        // but close enough to the fallback boundaries to snap.
        let featureWindows = [
            makeFeatureWindow(start: 48, end: 49, pauseProb: 0.8, rms: 0.02),
            makeFeatureWindow(start: 111, end: 112, pauseProb: 0.85, rms: 0.01),
        ]

        let result = expander.expand(
            seed: 200.0, // Far from any feature window — no acoustic boundary found.
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: []
        )

        #expect(result.source == .fallback)
        #expect(result.boundaryConfidence == 0.3)
    }

    // MARK: - Test 5: Edge case — seed at episode start

    @Test("seed near episode start clamps start boundary to zero")
    func seedNearStart() {
        let result = expander.expand(
            seed: 10.0,
            featureWindows: [],
            transcriptChunks: [],
            adWindows: []
        )

        #expect(result.source == .fallback)
        #expect(result.startTime == 0.0) // Clamped: max(0, 10 - 30)
        #expect(result.endTime == 40.0)
    }

    @Test("seed at zero produces valid boundaries")
    func seedAtZero() {
        let result = expander.expand(
            seed: 0.0,
            featureWindows: [],
            transcriptChunks: [],
            adWindows: []
        )

        #expect(result.source == .fallback)
        #expect(result.startTime == 0.0)
        #expect(result.endTime == 30.0)
    }

    // MARK: - Priority ordering

    @Test("existing window takes priority over acoustic and lexical signals")
    func windowPriorityOverAcousticAndLexical() {
        let adWindows = [
            makeAdWindow(start: 60, end: 90, confidence: 0.9),
        ]
        let featureWindows = [
            makeFeatureWindow(start: 50, end: 51, pauseProb: 0.95, rms: 0.005),
            makeFeatureWindow(start: 100, end: 101, pauseProb: 0.92, rms: 0.01),
        ]
        let chunks = [
            makeTranscriptChunk(
                start: 58, end: 65,
                text: "this episode is sponsored by Acme Corp",
                normalizedText: "this episode is sponsored by acme corp"
            ),
        ]

        let result = expander.expand(
            seed: 75.0,
            featureWindows: featureWindows,
            transcriptChunks: chunks,
            adWindows: adWindows
        )

        #expect(result.source == .existingWindow)
        #expect(result.startTime == 60.0)
        #expect(result.endTime == 90.0)
    }

    // MARK: - Resolver-driven acoustic selection

    @Test("resolver-driven acoustic selection still picks the strongest nearby boundary")
    func scoringFormula() {
        // Window with high pause probability should still be selected when
        // the competing candidate has materially weaker boundary cues.
        let highScore = makeFeatureWindow(start: 50, end: 51, pauseProb: 1.0, rms: 0.0)

        let lowScore = makeFeatureWindow(start: 52, end: 53, pauseProb: 0.0, rms: 0.5)

        let featureWindows = [lowScore, highScore]

        let result = expander.expand(
            seed: 60.0,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: []
        )

        // The high-scoring window at 50-51 should be chosen for start boundary.
        #expect(result.source == .acousticOnly)
        #expect(result.startTime == 50.0)
    }

    // MARK: - Helpers

    private func makeFeatureWindow(
        start: Double,
        end: Double,
        pauseProb: Double,
        rms: Double,
        spectralFlux: Double = 0,
        speakerChangeProxyScore: Double = 0,
        musicBedChangeScore: Double = 0
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test-asset",
            startTime: start,
            endTime: end,
            rms: rms,
            spectralFlux: spectralFlux,
            musicProbability: 0,
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: musicBedChangeScore,
            pauseProbability: pauseProb,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    private func makeAdWindow(
        start: Double,
        end: Double,
        confidence: Double
    ) -> AdWindow {
        AdWindow(
            id: UUID().uuidString,
            analysisAssetId: "test-asset",
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    private func makeTranscriptChunk(
        start: Double,
        end: Double,
        text: String,
        normalizedText: String
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: UUID().uuidString,
            analysisAssetId: "test-asset",
            segmentFingerprint: UUID().uuidString,
            chunkIndex: 0,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: normalizedText,
            pass: "final",
            modelVersion: "test",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }
}
