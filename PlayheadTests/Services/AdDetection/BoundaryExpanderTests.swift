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

    // MARK: - Scoring formula

    @Test("acoustic scoring matches SkipOrchestrator formula")
    func scoringFormula() {
        // Window with high pause probability and low RMS should score highest.
        let highScore = makeFeatureWindow(start: 50, end: 51, pauseProb: 1.0, rms: 0.0)
        // Expected: 1.0 * 0.7 + max(0, 1 - 0 * 10) * 0.3 = 0.7 + 0.3 = 1.0

        let lowScore = makeFeatureWindow(start: 52, end: 53, pauseProb: 0.0, rms: 0.5)
        // Expected: 0.0 * 0.7 + max(0, 1 - 5.0) * 0.3 = 0 + 0 = 0.0

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
        rms: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test-asset",
            startTime: start,
            endTime: end,
            rms: rms,
            spectralFlux: 0,
            musicProbability: 0,
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
