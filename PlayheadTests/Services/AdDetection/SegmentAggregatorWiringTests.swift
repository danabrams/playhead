// SegmentAggregatorWiringTests.swift
// playhead-0usd: integration of SegmentAggregator into AdDetectionService's
// production hot path.
//
// The contract under test is additive: the aggregator runs alongside the
// existing single-window promotion path. It MUST NOT replace the single-
// window path, MUST NOT promote an isolated sub-`highConfidence` window on
// its own, and MUST fuse multiple sub-threshold windows spread across an
// ad-scale region into exactly one persisted AdWindow.
//
// This suite protects three invariants:
//   1. DF5C1832 pattern: two sub-`highConfidence` spikes separated by tens
//      of seconds of continuation-grade (>= 0.28) evidence must coalesce
//      into a SINGLE persisted AdWindow via the aggregator path.
//   2. Single high-confidence window (>= 0.60) is still a one-window promotion:
//      exactly one AdWindow persisted. The aggregator must not double-emit.
//   3. C22D6EC6 pattern: a single 0.597 window alone must NOT promote — not
//      via single-window (below 0.60 highConfidence) and not via aggregator
//      (N-nearby requires 2 candidate-strength windows).
//   4. Observability: aggregator-promoted segments carry a distinct
//      finalDecision.action in the decision log so replay tooling can
//      distinguish "single-window path" from "aggregator path".

import Foundation
import Testing
@testable import Playhead

@Suite("SegmentAggregator — wired into AdDetectionService hot path")
struct SegmentAggregatorWiringTests {

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

    /// Insert a uniform 2 s feature window grid across [0, duration). Values
    /// are modest so the deterministic classifier is driven by position, not
    /// acoustic features.
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
        classifier: ClassifierService
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "0usd-test",
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

    // MARK: - 1. DF5C1832: aggregator fuses two sub-threshold spikes

    @Test("DF5C1832 pattern: two sub-0.60 spikes within an ad-scale span coalesce into ONE persisted AdWindow via the aggregator path")
    func df5c1832AggregatorPromotesSingleFusedWindow() async throws {
        // Synthesize the DF5C1832-equivalent pattern at Tier 1's 30-second
        // slot granularity (Tier 1 runs on 30-s slots per
        // `AdDetectionService.tier1DefaultWindowSeconds`):
        //
        //   slot 0  [  0, 30)  @ 0.30   (continuation-grade)
        //   slot 1  [ 30, 60)  @ 0.55   (candidate-grade, below highConfidence)
        //   slot 2  [ 60, 90)  @ 0.30
        //   slot 3  [ 90,120)  @ 0.30
        //   slot 4  [120,150)  @ 0.55   (candidate-grade, below highConfidence)
        //   slot 5  [150,180)  @ 0.30
        //
        // The two 0.55 windows are below the 0.60 high-confidence threshold so
        // they do NOT open individually on the aggregator's fast branch, but
        // together they complete the N=2-nearby start (both within 90 s of each
        // other) and the segment absorbs the intervening 0.30 continuation-
        // grade context, producing:
        //   mean = (0.55 + 0.30 + 0.30 + 0.55) / 4 = 0.425 >= 0.40 promotion
        //   duration = 120 s >= 30 s minAdDuration → promoted.
        //
        // The run passes `chunks: []` so the existing single-window hot path
        // (which scores LexicalCandidate-derived regions) never fires. The
        // ONLY way an AdWindow ends up persisted is through the aggregator.
        let store = try await makeTestStore()
        let assetId = "asset-df5c1832-aggregator"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 180
        try await insertUniformFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [
            0.0:   0.30,
            30.0:  0.55,
            60.0:  0.30,
            90.0:  0.30,
            120.0: 0.55,
            150.0: 0.30
        ], defaultScore: 0.30)
        let service = makeService(store: store, classifier: classifier)

        let windows = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )
        #expect(windows.count == 1,
                "two sub-highConfidence spikes within an ad-scale span must fuse into EXACTLY ONE AdWindow; got \(windows.count)")

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "aggregator must persist exactly one AdWindow for the DF5C1832 pattern; got \(persisted.count)")

        guard let win = persisted.first else { return }
        #expect(win.startTime <= 30.0 + 1e-6,
                "persisted AdWindow must span from the first spike forward (start ≤ 30.0); got \(win.startTime)")
        #expect(win.endTime >= 150.0 - 1e-6,
                "persisted AdWindow must extend to the last spike's end (end ≥ 150.0); got \(win.endTime)")
        #expect(win.decisionState == AdDecisionState.candidate.rawValue,
                "aggregator-emitted windows must enter as .candidate")
    }

    // MARK: - 2. Single-window fast path preserved

    @Test("Single 0.85 window still produces one AdWindow via the existing single-window path (aggregator does not double-emit)")
    func singleHighConfidenceWindowFastPathPreserved() async throws {
        // One chunk containing classic sponsor copy. The RuleBasedClassifier
        // would fire on the lexical features, but we use the deterministic
        // classifier to pin the score at 0.85 for the single candidate region
        // so the test doesn't depend on RuleBasedClassifier heuristics.
        //
        // Expected behavior: the hot path's single-window path persists one
        // AdWindow for the chunk. The aggregator would also see a 0.85 window
        // and open a segment via the high-confidence branch — but the
        // resulting aggregator segment overlaps the single-window AdWindow
        // and MUST be deduplicated so we end up with exactly ONE persisted
        // AdWindow, not two.
        let store = try await makeTestStore()
        let assetId = "asset-single-window-fast-path"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 120
        try await insertUniformFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.10, chunkScore: 0.85)
        let service = makeService(store: store, classifier: classifier)

        let chunk = TranscriptChunk(
            id: "c-\(assetId)-ad",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-ad",
            chunkIndex: 0,
            startTime: 40,
            endTime: 70,
            text: "This episode is brought to you by Squarespace dot com slash show.",
            normalizedText: "this episode is brought to you by squarespace dot com slash show.",
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
        let windows = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        #expect(windows.count == 1,
                "exactly one AdWindow expected for a single high-confidence chunk; got \(windows.count)")

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "aggregator must not double-emit when single-window path already fired; persisted=\(persisted.count)")
    }

    // MARK: - 3. C22D6EC6: single borderline window alone does NOT promote

    @Test("C22D6EC6 pattern: isolated 0.597 window alone persists ZERO AdWindows (below highConfidence, no N-nearby corroboration)")
    func c22D6EC6SingleBorderlineWindowDoesNotPromote() async throws {
        // Episode with a single isolated 0.597 window — below both the 0.60
        // highConfidence branch AND the 0.40 candidateThreshold isn't the
        // concern here; the concern is whether ONE lone 0.597 window fires.
        // 0.597 is above candidateThreshold (0.40), so if the pipeline had
        // a naïve "any window ≥ candidateThreshold → persist" rule it would
        // promote this window. The aggregator's N=2-nearby guard is what
        // prevents that, AND we pass chunks:[] so the single-window hot path
        // doesn't have a lexical candidate to classify either.
        //
        // The only way an AdWindow ends up persisted is if the aggregator
        // incorrectly opens on one 0.597 window.
        let store = try await makeTestStore()
        let assetId = "asset-c22d6ec6-borderline"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 180
        try await insertUniformFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [
            90.0: 0.597
        ], defaultScore: 0.10)
        let service = makeService(store: store, classifier: classifier)

        let windows = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )
        #expect(windows.isEmpty,
                "lone 0.597 window must not promote via any path; got \(windows.count) windows")

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.isEmpty,
                "no AdWindow should be persisted for a lone 0.597 window; got \(persisted.count)")
    }

    // MARK: - 4. Observability: decision log distinguishes the two paths

    @Test("Decision log marks aggregator-promoted segments distinctly from single-window promotions")
    func decisionLogDistinguishesAggregatorFromSingleWindowPromotion() async throws {
        // Same DF5C1832 setup — exactly one aggregator-promoted segment.
        // The decision log MUST include at least one entry whose
        // finalDecision.action is the aggregator-promotion marker
        // ("segmentAggregatorPromoted"), so replay tooling can tell that
        // this window came from fusing multiple sub-threshold scores, not
        // from a single-window spike.
        let store = try await makeTestStore()
        let assetId = "asset-observability-aggregator"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 180
        try await insertUniformFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [
            0.0:   0.30,
            30.0:  0.55,
            60.0:  0.30,
            90.0:  0.30,
            120.0: 0.55,
            150.0: 0.30
        ], defaultScore: 0.30)
        let spy = SpyDecisionLogger()
        let service = makeService(store: store, classifier: classifier)
        await service.setDecisionLogger(spy)

        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let entries = await spy.entries
        let aggregatorEntries = entries.filter {
            $0.finalDecision.action == "segmentAggregatorPromoted"
        }
        #expect(!aggregatorEntries.isEmpty,
                "expected at least one decision-log entry with action=segmentAggregatorPromoted")

        // Single-window action markers must still exist for the underlying
        // per-window Tier 1 scores (they were logged earlier in the run).
        // We assert they're NOT tagged with the aggregator action — they
        // remain hotPathCandidate / hotPathBelowThreshold / autoSkipEligible.
        let singleWindowActions = Set(
            entries
                .filter { $0.finalDecision.action != "segmentAggregatorPromoted" }
                .map { $0.finalDecision.action }
        )
        // At least one per-window Tier 1 action should exist alongside.
        let expectedPerWindowActions: Set<String> = [
            "hotPathCandidate",
            "hotPathBelowThreshold",
            "autoSkipEligible"
        ]
        #expect(!singleWindowActions.intersection(expectedPerWindowActions).isEmpty,
                "per-window Tier 1 actions must remain in the log; got \(singleWindowActions)")
    }
}

// MARK: - Test doubles

/// Deterministic classifier that returns a precomputed score based on the
/// candidate's `startTime`. Unrecognized start times get `defaultScore`.
///
/// When a candidate's id matches the hot-path lexical-scanner shape (i.e. it
/// is NOT a "tier1-..." synthetic id), `chunkScore` is used instead — this
/// lets a single test pin the single-window hot-path score without colliding
/// with Tier 1 slot scoring.
private final class SlotScoringClassifier: @unchecked Sendable, ClassifierService {
    private let scoresByStartTime: [Double: Double]
    private let defaultScore: Double
    private let chunkScore: Double?

    init(
        scoresByStartTime: [Double: Double],
        defaultScore: Double,
        chunkScore: Double? = nil
    ) {
        self.scoresByStartTime = scoresByStartTime
        self.defaultScore = defaultScore
        self.chunkScore = chunkScore
    }

    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
        inputs.map { classify(input: $0, priors: priors) }
    }

    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
        let probability: Double
        if let chunkScore, !input.candidate.id.hasPrefix("tier1-") {
            probability = chunkScore
        } else {
            probability = scoresByStartTime[input.candidate.startTime] ?? defaultScore
        }
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
