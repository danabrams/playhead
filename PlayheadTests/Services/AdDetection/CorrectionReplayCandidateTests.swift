// CorrectionReplayCandidateTests.swift
// playhead-hygc.1.8: Tests for the correction-replay recall step in
// AdDetectionService.runHotPathResult.
//
// A `.falseNegative` `.exactTimeSpan` correction event is the user's
// strongest possible label that "this WAS an ad" for a region the
// detector missed. The hot path's correction-replay step turns these
// into mark-only AdWindows so the suggest-tier banner re-surfaces the
// region — recall-positive, precision-safe (no auto-skip expansion).
//
// Coverage:
//   1. A falseNegative correction with no overlapping AdWindow surfaces
//      a markOnly AdWindow on the next runHotPath.
//   2. The new row is `boundaryState=correctionReplay`,
//      `metadataSource=userCorrectionReplay`, `eligibilityGate=markOnly`.
//   3. A falseNegative correction whose span already overlaps an
//      existing AdWindow (e.g. boundary-singleton recovery) does NOT
//      duplicate-emit.
//   4. A falseNegative correction whose span is fully covered by a
//      later falsePositive correction is suppressed (the user's veto
//      protects precision).
//   5. The emit is idempotent: a second runHotPath does not produce a
//      duplicate row, because the AdWindow persisted in run 1 is found
//      via fetchAdWindows in run 2 and short-circuits the replay.
//   6. A falseNegative correction whose span overlaps a `.reverted`
//      AdWindow is NOT re-emitted — once vetoed, stays vetoed.

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService — correction-replay recall (playhead-hygc.1.8)")
struct CorrectionReplayCandidateTests {

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

    /// AdDetectionService configured exactly as `SegmentAggregatorWiringTests`
    /// configures it — same threshold + version stamps so the boundary
    /// singleton path remains comparable in this suite.
    private func makeService(
        store: AnalysisStore,
        classifier: ClassifierService
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "hygc-1.8-test",
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

    /// Persist a `.falseNegative` `.exactTimeSpan` correction event for
    /// the given asset/range, mirroring the shape `recordUserMarkedAd`
    /// writes when the user reports a missed ad.
    private func appendFalseNegativeCorrection(
        store: AnalysisStore,
        assetId: String,
        startTime: Double,
        endTime: Double
    ) async throws {
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: startTime,
            endTime: endTime
        )
        let event = CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        )
        try await store.appendCorrectionEvent(event)
    }

    /// Persist a `.falsePositive` `.exactTimeSpan` correction event,
    /// mirroring what `revertByTimeRange` writes when the user vetoes a span.
    private func appendFalsePositiveCorrection(
        store: AnalysisStore,
        assetId: String,
        startTime: Double,
        endTime: Double,
        createdAt: Double = Date().timeIntervalSince1970
    ) async throws {
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: startTime,
            endTime: endTime
        )
        let event = CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            createdAt: createdAt,
            source: .manualVeto,
            podcastId: nil,
            correctionType: .falsePositive
        )
        try await store.appendCorrectionEvent(event)
    }

    // MARK: - 1. Recall: bare correction → markOnly AdWindow

    @Test("falseNegative correction with no overlapping AdWindow surfaces a markOnly AdWindow")
    func falseNegativeCorrectionSurfacesMarkOnlyAdWindow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-1"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // No-op classifier so the only AdWindow that can land in the
        // store comes from the correction-replay path.
        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.05
        )
        let service = makeService(store: store, classifier: classifier)

        // User reported a missed ad at 600..680 — but no algorithmic
        // detector ever produced an AdWindow for that range.
        try await appendFalseNegativeCorrection(
            store: store,
            assetId: assetId,
            startTime: 600,
            endTime: 680
        )

        let windows = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        // The returned window set must include the correction-replay row.
        let replayed = windows.filter {
            $0.boundaryState == "correctionReplay" &&
            abs($0.startTime - 600) < 0.01 &&
            abs($0.endTime - 680) < 0.01
        }
        #expect(replayed.count == 1,
                "expected one correction-replay AdWindow, got \(replayed.count)")

        // And the persisted shape must match the precision-safe contract.
        let persisted = try await store.fetchAdWindows(assetId: assetId)
        let row = persisted.first { $0.boundaryState == "correctionReplay" }
        #expect(row != nil, "correction-replay row must persist")
        #expect(row?.eligibilityGate == "markOnly",
                "correction-replay must be markOnly to keep precision contract; got \(row?.eligibilityGate ?? "<nil>")")
        #expect(row?.metadataSource == "userCorrectionReplay",
                "metadataSource must distinguish replay from initial userCorrection write")
        #expect(row?.decisionState == "candidate",
                "correction-replay row must enter as candidate")
        #expect(row?.confidence == 1.0,
                "user-reported ad is the strongest possible label")
    }

    // MARK: - 2. Idempotency: existing AdWindow blocks duplicate emit

    @Test("falseNegative correction overlapping existing AdWindow does NOT duplicate-emit")
    func falseNegativeCorrectionDoesNotDuplicateExistingAdWindow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-dedupe"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // Pre-existing algorithmic AdWindow at 600..680 (e.g. boundary-singleton).
        let preExisting = AdWindow(
            id: "pre-existing-1",
            analysisAssetId: assetId,
            startTime: 600,
            endTime: 680,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 600,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(preExisting)

        try await appendFalseNegativeCorrection(
            store: store,
            assetId: assetId,
            startTime: 605,  // overlaps preExisting
            endTime: 670
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)
        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        // No correction-replay row should be persisted — the pre-existing
        // window covers the user's reported range.
        let persisted = try await store.fetchAdWindows(assetId: assetId)
        let replayRows = persisted.filter { $0.boundaryState == "correctionReplay" }
        #expect(replayRows.isEmpty,
                "correction-replay must not duplicate a pre-existing overlapping AdWindow; got \(replayRows.count)")
        #expect(persisted.count == 1, "only the original pre-existing window should remain; got \(persisted.count)")
    }

    // MARK: - 3. Suppression: later falsePositive correction masks the false-negative

    @Test("falsePositive correction fully covering a falseNegative span suppresses the replay")
    func falsePositiveSuppressesFalseNegativeReplay() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fp-suppresses-fn"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // User first reports a missed ad, then realizes it wasn't and vetoes
        // a fully-enclosing range.
        try await appendFalseNegativeCorrection(
            store: store,
            assetId: assetId,
            startTime: 700,
            endTime: 770
        )
        try await appendFalsePositiveCorrection(
            store: store,
            assetId: assetId,
            startTime: 690,  // fully encloses
            endTime: 780
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)
        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        let replayRows = persisted.filter { $0.boundaryState == "correctionReplay" }
        #expect(replayRows.isEmpty,
                "falsePositive that fully covers a falseNegative must suppress replay; got \(replayRows.count)")
    }

    // MARK: - 4. Idempotency across runs

    @Test("running runHotPath twice does not duplicate the correction-replay row")
    func correctionReplayIsIdempotentAcrossRuns() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-idempotent"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        try await appendFalseNegativeCorrection(
            store: store,
            assetId: assetId,
            startTime: 1000,
            endTime: 1080
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)

        _ = try await service.runHotPath(chunks: [], analysisAssetId: assetId, episodeDuration: duration)
        let afterFirst = try await store.fetchAdWindows(assetId: assetId)
        let firstReplayRows = afterFirst.filter { $0.boundaryState == "correctionReplay" }
        #expect(firstReplayRows.count == 1, "first run should emit one row")

        // Second run: the AdWindow from run 1 is now in the store, so
        // the overlap check must short-circuit before emitting again.
        _ = try await service.runHotPath(chunks: [], analysisAssetId: assetId, episodeDuration: duration)
        let afterSecond = try await store.fetchAdWindows(assetId: assetId)
        let secondReplayRows = afterSecond.filter { $0.boundaryState == "correctionReplay" }
        #expect(secondReplayRows.count == 1, "second run must NOT duplicate; got \(secondReplayRows.count)")
        // And the row identity should be unchanged.
        #expect(firstReplayRows.first?.id == secondReplayRows.first?.id,
                "row id must be stable across runs")
    }

    // MARK: - 5. Reverted AdWindow blocks re-emit

    @Test("falseNegative correction overlapping a reverted AdWindow does NOT re-emit")
    func revertedAdWindowBlocksCorrectionReplay() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-vs-reverted"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // A previously vetoed AdWindow — the user explicitly said this
        // wasn't an ad, so the correction-replay path must not undo that.
        let revertedRow = AdWindow(
            id: "reverted-row-1",
            analysisAssetId: assetId,
            startTime: 1200,
            endTime: 1260,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.reverted.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 1200,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(revertedRow)

        // Stale falseNegative correction in the same range (e.g. recorded
        // before the user changed their mind).
        try await appendFalseNegativeCorrection(
            store: store,
            assetId: assetId,
            startTime: 1210,
            endTime: 1250
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)
        _ = try await service.runHotPath(chunks: [], analysisAssetId: assetId, episodeDuration: duration)

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        let replayRows = persisted.filter { $0.boundaryState == "correctionReplay" }
        #expect(replayRows.isEmpty,
                "must not resurface a vetoed span via correction-replay; got \(replayRows.count)")
        // The reverted row stays put.
        #expect(persisted.first(where: { $0.id == "reverted-row-1" })?.decisionState == AdDecisionState.reverted.rawValue,
                "previously vetoed window must remain reverted")
    }

    // MARK: - 6. R3: replay row not retired by the same hot-path run that emitted it

    /// playhead-hygc.1.8 R3: regression for the retirement-bug found in R0.
    /// Live `AnalysisCoordinator` calls `runHotPathResult` with
    /// `retireUnmatchedReplayCandidates: true`. That path computes
    /// `replayCandidateIDs` as every existing
    /// `decisionState=.candidate, detectorVersion=config.detectorVersion`
    /// AdWindow overlapping the chunks envelope — which, after R0,
    /// includes the freshly-emitted correction-replay row that was
    /// upserted moments earlier. With no incoming algorithmic AdWindow to
    /// match it, the replay row would land in `retiredWindowIDs` and get
    /// DELETED at the end of the same run that just inserted it. This
    /// breaks the idempotency claim AND would yank the suggest-tier
    /// banner via `retireAdWindows` in production.
    @Test("retireUnmatchedReplayCandidates does not delete a freshly-emitted correction-replay row")
    func retirementDoesNotEatFreshCorrectionReplayRow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-retire-bug"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // User reported a missed ad at 600..680 — the same range will be
        // covered by the upcoming chunk envelope.
        try await appendFalseNegativeCorrection(
            store: store,
            assetId: assetId,
            startTime: 600,
            endTime: 680
        )

        // Build chunks whose envelope (`chunks.startTime.min ...
        // chunks.endTime.max`) includes 600..680. The text contains no
        // lexical ad triggers so the algorithmic detector emits nothing
        // and the only AdWindow on the asset must be the
        // correction-replay row — which would be retired by the
        // unmatched-replay-candidate logic if R3's exclusion isn't in
        // place.
        let chunks: [TranscriptChunk] = [
            TranscriptChunk(
                id: "chunk-1",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-1",
                chunkIndex: 0,
                startTime: 500,
                endTime: 700,
                text: "the speaker is talking about something benign",
                normalizedText: "the speaker is talking about something benign",
                pass: "final",
                modelVersion: "speech-v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: nil
            )
        ]
        try await store.insertTranscriptChunks(chunks)

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)

        let result = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        let replayRows = persisted.filter { $0.boundaryState == "correctionReplay" }
        #expect(replayRows.count == 1,
                "fresh correction-replay row must survive the same-run retirement pass; got \(replayRows.count)")

        // The row id must NOT be in the retiredWindowIDs set returned
        // to AnalysisCoordinator (which would push retireAdWindows to
        // SkipOrchestrator and yank the suggest-tier banner).
        if let replayRow = replayRows.first {
            #expect(!result.retiredWindowIDs.contains(replayRow.id),
                    "fresh correction-replay row id must not be in retiredWindowIDs: \(result.retiredWindowIDs)")
        }
    }
}

// MARK: - Test doubles

/// Deterministic classifier — same shape as `SegmentAggregatorWiringTests`.
/// Returns a precomputed score based on the candidate's `startTime`;
/// unrecognized start times get `defaultScore`.
private final class SlotScoringClassifier: @unchecked Sendable, ClassifierService {
    private let scoresByStartTime: [Double: Double]
    private let defaultScore: Double

    init(scoresByStartTime: [Double: Double], defaultScore: Double) {
        self.scoresByStartTime = scoresByStartTime
        self.defaultScore = defaultScore
    }

    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
        inputs.map { classify(input: $0, priors: priors) }
    }

    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
        let probability = scoresByStartTime[input.candidate.startTime] ?? defaultScore
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
