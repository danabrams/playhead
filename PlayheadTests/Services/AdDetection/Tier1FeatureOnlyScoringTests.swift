// Tier1FeatureOnlyScoringTests.swift
// playhead-gtt9.9: Full-audio feature-only scoring path.
//
// The contract under test: ad detection must emit a complete stream of
// scored windows over the ENTIRE episode duration regardless of transcript
// state. Transcript-dependent evidence (lexical / fm / catalog) refines
// scores, but it must NEVER gate whether a region is scored at all.
//
// Before gtt9.9, an empty-transcript episode produced zero classifier calls
// because `hotPathCandidates` only derived from `LexicalCandidate`s built
// from transcript chunks. On the 2026-04-23 capture, 13 of 21 user-confirmed
// ad spans had ZERO scored windows because transcript coverage stopped at
// 90 s — see docs/narl/2026-04-23-real-data-findings.md §1.
//
// This suite protects the Tier 1 invariant:
//   1. With zero transcript chunks, Tier 1 still emits decision-log entries
//      covering ≥ 95 % of episode seconds.
//   2. The emitted windows include `hotPathCandidate`-or-above actions where
//      the classifier fires in plausible pre/mid/post-roll positions.
//   3. Tier 1 and Tier 2 coexist: running Tier 2 after Tier 1 never removes
//      Tier 1 entries (they accumulate in the decision log).

import Foundation
import Testing
@testable import Playhead

@Suite("Tier 1 feature-only scoring — transcript-independent coverage")
struct Tier1FeatureOnlyScoringTests {

    // MARK: - Fixtures

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// Insert a uniform 2 s feature window grid across [0, duration).
    /// Use modest RMS / flux values so the deterministic classifier produces
    /// sub-autoSkip scores in the interior.
    private func insertUniformFeatureGrid(
        store: AnalysisStore,
        assetId: String,
        duration: Double,
        step: Double = 2.0
    ) async throws {
        var windows: [FeatureWindow] = []
        var t = 0.0
        while t < duration {
            let end = min(t + step, duration)
            windows.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: t,
                endTime: end,
                rms: 0.3,
                spectralFlux: 0.2,
                musicProbability: 0.05,
                pauseProbability: 0.1,
                speakerClusterId: 1,
                jingleHash: nil,
                featureVersion: 1
            ))
            t = end
        }
        try await store.insertFeatureWindows(windows)
    }

    private func makeService(
        store: AnalysisStore,
        classifier: ClassifierService = RuleBasedClassifier()
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "tier1-test",
            fmBackfillMode: .off,
            autoSkipConfidenceThreshold: 0.80
        )
        return AdDetectionService(
            store: store,
            classifier: classifier,
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    /// Union-length of a [start,end] list of ranges (naïve, O(n log n)).
    private func unionSeconds(_ ranges: [(Double, Double)]) -> Double {
        let sorted = ranges.sorted { $0.0 < $1.0 }
        var total = 0.0
        var cursor: (Double, Double)?
        for r in sorted {
            if let c = cursor {
                if r.0 <= c.1 {
                    cursor = (c.0, max(c.1, r.1))
                } else {
                    total += c.1 - c.0
                    cursor = r
                }
            } else {
                cursor = r
            }
        }
        if let c = cursor { total += c.1 - c.0 }
        return total
    }

    // MARK: - 1. Transcript-disabled run still covers the whole episode

    @Test("Zero-transcript episode: Tier 1 scored-window coverage >= 95 %")
    func transcriptDisabledEpisodeAchievesCoverage() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-tier1-empty-transcript"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 600  // 10 min
        try await insertUniformFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration
        )

        let spy = SpyDecisionLogger()
        let service = makeService(store: store)
        await service.setDecisionLogger(spy)

        // Contract: Tier 1 runs standalone with NO transcript chunks and
        // still emits a decision-log entry stream that covers the full
        // episode.
        try await service.runTier1FeatureOnlyScoring(
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let entries = await spy.entries
        #expect(!entries.isEmpty,
                "Tier 1 must emit decision-log entries even without transcript")

        let ranges = entries.map { ($0.windowBounds.start, $0.windowBounds.end) }
        let covered = unionSeconds(ranges)
        let ratio = covered / duration
        #expect(ratio >= 0.95,
                "scoredCoverageRatio must be >= 0.95 on a transcript-less episode; got \(ratio)")
    }

    // MARK: - 2. Plausible hotPathCandidate signals without transcript

    @Test("Transcript-disabled run produces hotPathCandidate-or-above windows")
    func transcriptDisabledRunProducesCandidateActions() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-tier1-classifier-hot"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 300
        try await insertUniformFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration
        )

        // Inject a deterministic classifier that fires at pre/mid/post-roll
        // positions. With Tier 1 sliding at 30 s windows, these positions
        // correspond to recognizable episode thirds.
        let classifier = DeterministicPositionClassifier(
            preRoll: 0.82,      // autoSkipEligible
            midRoll: 0.52,      // hotPathCandidate
            postRoll: 0.47,     // hotPathCandidate
            defaultProbability: 0.15
        )
        let spy = SpyDecisionLogger()
        let service = makeService(store: store, classifier: classifier)
        await service.setDecisionLogger(spy)

        try await service.runTier1FeatureOnlyScoring(
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let entries = await spy.entries
        let candidateActions = Set([
            "hotPathCandidate",
            "autoSkipEligible"
        ])
        let candidates = entries.filter {
            candidateActions.contains($0.finalDecision.action)
        }
        #expect(!candidates.isEmpty,
                "Expected at least one candidate/autoSkip decision from Tier 1")

        let hasPreRoll = candidates.contains { $0.windowBounds.start < duration * 0.15 }
        let hasPostRoll = candidates.contains { $0.windowBounds.end > duration * 0.85 }
        #expect(hasPreRoll && hasPostRoll,
                "Expected candidates near pre/post-roll; got \(candidates.map(\.windowBounds))")
    }

    // MARK: - 3. Tier 2 does not remove Tier 1 entries

    @Test("Tier 2 backfill does not delete Tier 1 decision-log entries")
    func tier2DoesNotRemoveTier1Entries() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-tier1-plus-tier2"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 120
        try await insertUniformFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration
        )

        let spy = SpyDecisionLogger()
        let service = makeService(store: store)
        await service.setDecisionLogger(spy)

        // Phase A: Tier 1 runs.
        try await service.runTier1FeatureOnlyScoring(
            analysisAssetId: assetId,
            episodeDuration: duration
        )
        let tier1Count = await spy.entries.count
        #expect(tier1Count > 0, "Expected Tier 1 to emit entries")

        // Phase B: Tier 2 backfill runs on transcript chunks with ad
        // signals. Tier 2 must append more entries; Tier 1 entries remain.
        let chunks = [
            TranscriptChunk(
                id: "c-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp",
                chunkIndex: 0,
                startTime: 30,
                endTime: 60,
                text: "This episode is brought to you by Squarespace dot com slash show.",
                normalizedText: "this episode is brought to you by squarespace dot com slash show.",
                pass: "final",
                modelVersion: "test",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ]
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: duration
        )

        let allEntries = await spy.entries
        #expect(allEntries.count >= tier1Count,
                "Tier 2 must not remove Tier 1 entries; tier1=\(tier1Count), total=\(allEntries.count)")
    }
}

// MARK: - Test doubles

/// Deterministic classifier that returns a probability based on window
/// position (pre/mid/post-roll), independent of features. Used to verify
/// that Tier 1's sliding windows traverse the whole episode.
private final class DeterministicPositionClassifier: @unchecked Sendable, ClassifierService {
    private let preRoll: Double
    private let midRoll: Double
    private let postRoll: Double
    private let defaultProbability: Double

    init(preRoll: Double, midRoll: Double, postRoll: Double, defaultProbability: Double) {
        self.preRoll = preRoll
        self.midRoll = midRoll
        self.postRoll = postRoll
        self.defaultProbability = defaultProbability
    }

    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
        inputs.map { classify(input: $0, priors: priors) }
    }

    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
        let mid = (input.candidate.startTime + input.candidate.endTime) / 2
        let normalized = input.episodeDuration > 0 ? mid / input.episodeDuration : 0.5
        let probability: Double
        if normalized < 0.10 { probability = preRoll }
        else if normalized > 0.90 { probability = postRoll }
        else if abs(normalized - 0.50) < 0.05 { probability = midRoll }
        else { probability = defaultProbability }
        return ClassifierResult(
            candidateId: input.candidate.id,
            analysisAssetId: input.candidate.analysisAssetId,
            startTime: input.candidate.startTime,
            endTime: input.candidate.endTime,
            adProbability: probability,
            startAdjustment: 0,
            endAdjustment: 0,
            signalBreakdown: SignalBreakdown(
                lexicalScore: 0,
                rmsDropScore: 0,
                spectralChangeScore: 0,
                musicScore: 0,
                speakerChangeScore: 0,
                priorScore: 0
            )
        )
    }
}
