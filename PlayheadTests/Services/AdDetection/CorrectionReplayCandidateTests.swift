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

    // MARK: - 7. R4: previously-persisted replay row not retired on a SUBSEQUENT run

    /// playhead-hygc.1.8 R4: regression for the same-bug-shifted-by-one-run
    /// case that R3 left uncovered.
    ///
    /// R3 fixed retirement of FRESHLY-emitted replay rows by subtracting the
    /// in-flight emission set from `replayCandidateIDs`. But `correction
    /// ReplayCandidates` short-circuits emission on subsequent runs (the
    /// existing replay row's span overlaps the new FN range, so no new row is
    /// produced). With no fresh emissions on run N, `correctionReplay
    /// WindowIDs` is empty — and `hotPathCandidateIDs` (filtering by
    /// `decisionState=.candidate, detectorVersion=current`) STILL includes
    /// the previously-persisted replay row's ID. Without an algorithmic
    /// match incoming, the row goes into `retiredWindowIDs` and is DELETED
    /// at the end of run N. Net effect: replay row survives run 1, gets
    /// deleted on run 2 — the same-run bug shifted by one run.
    ///
    /// Fix landed in R4: `hotPathCandidateIDs` filters out
    /// `boundaryState == "correctionReplay"` at the source. The retirement
    /// path is for stale algorithmic candidates only; replay rows are
    /// retired exclusively by the user's veto via
    /// `SkipOrchestrator.revertByTimeRange` (which sets
    /// `decisionState = .reverted`, dropping them out of the candidate
    /// filter naturally).
    ///
    /// The R0 idempotency test (`correctionReplayIsIdempotentAcrossRuns`)
    /// passed for the wrong reason — it ran with empty chunks, which takes
    /// the `chunks.isEmpty` early-return branch and never even reaches the
    /// retirement logic. This test exercises the multi-run path with
    /// chunks present so the bug is reachable.
    @Test("correction-replay row survives a SECOND retirement-enabled hot-path run")
    func correctionReplayRowSurvivesSecondRunRetirement() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-multi-run"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        try await appendFalseNegativeCorrection(
            store: store,
            assetId: assetId,
            startTime: 600,
            endTime: 680
        )

        // Chunks whose envelope (500..700) overlaps the FN range so that
        // `replayCandidateIDs` would include the persisted replay row on
        // run 2 (this is the precondition for the R4 bug to fire).
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

        // Run 1: emits replay row, R3-1 fix protects it from same-run retirement.
        let result1 = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )
        let afterRun1 = try await store.fetchAdWindows(assetId: assetId)
        let replayAfterRun1 = afterRun1.filter { $0.boundaryState == "correctionReplay" }
        #expect(replayAfterRun1.count == 1,
                "run 1 must persist exactly one replay row; got \(replayAfterRun1.count)")
        let replayId = replayAfterRun1.first?.id ?? ""
        #expect(!result1.retiredWindowIDs.contains(replayId),
                "run 1 must not retire its own fresh replay row")

        // Run 2: NO fresh emission (correctionReplayCandidates short-
        // circuits because the row already exists). Without R4's fix,
        // `replayCandidateIDs` would include the persisted row's ID and
        // it would be retired. With R4's fix, the source filter excludes
        // correctionReplay rows from `hotPathCandidateIDs` so the
        // retirement set never contains them.
        let result2 = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )
        let afterRun2 = try await store.fetchAdWindows(assetId: assetId)
        let replayAfterRun2 = afterRun2.filter { $0.boundaryState == "correctionReplay" }
        #expect(replayAfterRun2.count == 1,
                "run 2 must NOT retire the previously-persisted replay row; got \(replayAfterRun2.count)")
        #expect(replayAfterRun2.first?.id == replayId,
                "row identity must be stable across runs; got id \(replayAfterRun2.first?.id ?? "<missing>") vs \(replayId)")
        #expect(!result2.retiredWindowIDs.contains(replayId),
                "run 2's retiredWindowIDs must not contain the replay row id; got \(result2.retiredWindowIDs)")

        // And a third run for good measure — population is bounded.
        _ = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )
        let afterRun3 = try await store.fetchAdWindows(assetId: assetId)
        let replayAfterRun3 = afterRun3.filter { $0.boundaryState == "correctionReplay" }
        #expect(replayAfterRun3.count == 1,
                "run 3 must keep population bounded at one replay row; got \(replayAfterRun3.count)")
    }

    // MARK: - 8. R7: overlapping FN ranges in a single run dedupe to ONE row

    /// playhead-hygc.1.8 R7: the existing exact-match `seen` key
    /// (`%.3f-%.3f`) inside `correctionReplayCandidates` only suppresses
    /// truly-identical FN spans. Real-world dogfood corrections include
    /// near-duplicates the user reported with slightly-different ranges
    /// (e.g. one tap captured 600..680, a second tap 605..690 for the
    /// same ad). Without an overlap-aware in-flight dedupe, BOTH would
    /// land — leaving two suggest banners and two persisted rows where
    /// the user reported one ad. R7 adds an overlap check against the
    /// in-flight `emitted` set so only the first range survives; the
    /// second is silently dropped because it overlaps an already-queued
    /// row.
    @Test("overlapping falseNegative ranges dedupe to a single replay row in one run")
    func overlappingFalseNegativeRangesDedupeToOneRow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-overlap-dedupe"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // Three near-duplicate FN reports for the same physical ad.
        try await appendFalseNegativeCorrection(
            store: store, assetId: assetId, startTime: 600, endTime: 680
        )
        try await appendFalseNegativeCorrection(
            store: store, assetId: assetId, startTime: 605, endTime: 690
        )
        try await appendFalseNegativeCorrection(
            store: store, assetId: assetId, startTime: 610, endTime: 700
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
        #expect(replayRows.count == 1,
                "three overlapping FN ranges must dedupe to one replay row; got \(replayRows.count)")
    }

    // MARK: - 9. R7: dual-emission audit — replay + algorithmic rows can co-exist for the same span

    /// playhead-hygc.1.8 R7 audit: when a replay row exists at span S
    /// and a SUBSEQUENT run produces an algorithmic AdWindow for the
    /// same span, what happens?
    ///
    /// Expected (current contract):
    ///   * `correctionReplayCandidates` short-circuits on subsequent
    ///     runs because the replay row IS in `existing`. So only ONE
    ///     replay row exists.
    ///   * The algorithmic detector can emit a row at the same span on
    ///     a later run if Tier 1 / aggregator / candidates fire — that
    ///     row carries a distinct `boundaryState` (segmentAggregated /
    ///     acousticRefined). `matchingHotPathWindows` filters on
    ///     `existing.boundaryState == incoming.boundaryState` so the
    ///     two rows do NOT reconcile/merge — they remain as two
    ///     persisted candidate rows for overlapping spans.
    ///
    /// This test pins that contract: the replay row stays markOnly, the
    /// algorithmic row stays whatever boundary state it carried. R6
    /// flagged this as a residual risk; this test locks the behavior so
    /// any future change (e.g. unifying boundary states or deduping by
    /// span) is detected.
    @Test("replay row co-existing with algorithmic row at overlapping span: both persist with distinct boundary states")
    func replayRowCoexistsWithOverlappingAlgorithmicRow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-coexist-algorithmic"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // FN at 600..680 with no algorithmic match yet → emits a replay row.
        try await appendFalseNegativeCorrection(
            store: store, assetId: assetId, startTime: 600, endTime: 680
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)
        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let afterReplay = try await store.fetchAdWindows(assetId: assetId)
        #expect(afterReplay.filter { $0.boundaryState == "correctionReplay" }.count == 1,
                "replay row must be present after run 1")

        // Now a separate algorithmic boundary-singleton style row gets
        // inserted directly (simulating a later run where Tier 1 fires
        // on the same span). This bypasses the hot-path overlap check
        // — which is by design: replay rows with distinct boundary
        // states do not block algorithmic rows.
        let algorithmic = AdWindow(
            id: "algorithmic-coexist-1",
            analysisAssetId: assetId,
            startTime: 605,
            endTime: 670,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "hygc-1.8-test",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 605,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(algorithmic)

        // A second hot-path run: replay short-circuits (existing
        // replay row covers the FN span). The algorithmic row remains
        // persisted as-is — neither retired by the replay path nor
        // reconciled with the replay row.
        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let final = try await store.fetchAdWindows(assetId: assetId)
        let replayRows = final.filter { $0.boundaryState == "correctionReplay" }
        let algorithmicRows = final.filter {
            $0.boundaryState == AdBoundaryState.segmentAggregated.rawValue
        }
        #expect(replayRows.count == 1,
                "replay row must remain after run 2; got \(replayRows.count)")
        #expect(algorithmicRows.count == 1,
                "algorithmic row must remain after run 2; got \(algorithmicRows.count)")
        #expect(replayRows.first?.eligibilityGate == "markOnly",
                "replay row must remain markOnly")
        #expect(algorithmicRows.first?.id == "algorithmic-coexist-1",
                "algorithmic row identity must be unchanged")
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
