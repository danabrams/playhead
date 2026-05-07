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

    // MARK: - 9b. R8: replay row survives retirement-enabled run even when an algorithmic row co-exists

    /// playhead-hygc.1.8 R8: the R7 dual-emission test
    /// (`replayRowCoexistsWithOverlappingAlgorithmicRow`) used the
    /// empty-chunks branch — which short-circuits before retirement
    /// even runs — so it didn't actually verify that the replay row's
    /// boundaryState filter (R4) holds in the production retirement-
    /// enabled path when an algorithmic row is also present.
    ///
    /// This test pins the asymmetric contract:
    ///   * The replay row is PROTECTED from retirement by the
    ///     `boundaryState != "correctionReplay"` filter at
    ///     `AdDetectionService.swift:hotPathCandidateIDs` (R4).
    ///   * The algorithmic row, by contrast, follows the normal
    ///     stale-candidate retirement contract: when it carries the
    ///     CURRENT `detectorVersion` (so it appears in
    ///     `currentHotPathCandidateWindows`) AND a chunks envelope
    ///     overlapping it produces no incoming algorithmic match, it
    ///     IS retired.
    ///
    /// The audit-point asymmetry matters because R7's residual-risk
    /// note ("neither retires/reconciles the other") was true only of
    /// the no-retirement path. The retirement-enabled path treats the
    /// two rows differently — the replay row's protection is
    /// load-bearing here.
    @Test("retirement-enabled multi-run with co-existing algorithmic row: replay row protected, algorithmic row follows normal retirement")
    func replayRowSurvivesRetirementWhileAlgorithmicFollowsNormalContract() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-coexist-retire"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // FN at 600..680 → emits a replay row on run 1.
        try await appendFalseNegativeCorrection(
            store: store, assetId: assetId, startTime: 600, endTime: 680
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)

        let chunks: [TranscriptChunk] = [
            TranscriptChunk(
                id: "chunk-coexist-retire-1",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-coexist-retire",
                chunkIndex: 0,
                startTime: 580,
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

        // Run 1: emit replay row in the retirement-enabled path.
        let r1 = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )
        let replayId = (try await store.fetchAdWindows(assetId: assetId))
            .first { $0.boundaryState == "correctionReplay" }?.id ?? ""
        #expect(!replayId.isEmpty, "run 1 must emit a replay row")
        #expect(!r1.retiredWindowIDs.contains(replayId),
                "run 1: replay row id must not be retired; got \(r1.retiredWindowIDs)")

        // Now insert a co-existing algorithmic row stamped with the
        // CURRENT detector version so it lands in
        // `currentHotPathCandidateWindows` and is eligible for
        // retirement on the next run.
        let algorithmic = AdWindow(
            id: "algorithmic-retire-coexist-1",
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

        // Run 2: retirement-enabled, chunks envelope overlaps both rows.
        let r2 = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )

        // CONTRACT (asymmetric, half 1 of 2): replay row id is NEVER in
        // retiredWindowIDs (R4 boundaryState filter is load-bearing here).
        #expect(!r2.retiredWindowIDs.contains(replayId),
                "replay row must be protected from retirement even when an algorithmic row co-exists; got \(r2.retiredWindowIDs)")

        // Replay row must still be persisted.
        let final = try await store.fetchAdWindows(assetId: assetId)
        let replayAfter = final.filter { $0.boundaryState == "correctionReplay" }
        #expect(replayAfter.count == 1,
                "replay row must survive retirement-enabled run 2; got \(replayAfter.count)")
        #expect(replayAfter.first?.id == replayId,
                "replay row identity must be stable across runs; got \(replayAfter.first?.id ?? "<missing>") vs \(replayId)")

        // CONTRACT (asymmetric, half 2 of 2 — playhead-hygc.1.8 R9):
        // the algorithmic row IS subject to the normal stale-candidate
        // retirement contract. It carries the current `detectorVersion`,
        // its span (605..670) is fully inside the chunks envelope
        // (580..700), and the run produces no incoming algorithmic match
        // for it (the no-op classifier scores 0.05, well below the 0.40
        // candidate threshold). Per `hotPathCandidateIDs`, every such
        // candidate row whose boundaryState is NOT `correctionReplay`
        // lands in `replayCandidateIDs` — and with no matching incoming
        // window, the id is propagated to `retiredWindowIDs` and the
        // row is deleted at end-of-run.
        //
        // Without this assertion R8's "algorithmic follows normal
        // contract" promise is unobserved (the test name claimed it; only
        // the "replay protected" half was actually pinned). Pinning both
        // halves means a future regression that EITHER under-retires
        // algorithmic rows (false negative on staleness) OR over-retires
        // replay rows (re-introducing R4) fails this single test.
        let algorithmicId = "algorithmic-retire-coexist-1"
        #expect(r2.retiredWindowIDs.contains(algorithmicId),
                "algorithmic row must be retired by the normal stale-candidate path on run 2; got retiredWindowIDs=\(r2.retiredWindowIDs)")
        #expect(final.first { $0.id == algorithmicId } == nil,
                "algorithmic row must be physically removed from the store after retirement; persisted ids=\(final.map(\.id))")
    }

    // MARK: - 10. R8: boundary semantics for overlap dedupe

    /// playhead-hygc.1.8 R8: pin the boundary semantics of the
    /// overlap-aware dedupe added in R7. The predicate is
    /// `range1.startTime < range2.endTime && range1.endTime > range2.startTime`
    /// — i.e. STRICT inequality on both sides. Adjacent ranges that share
    /// an endpoint (e.g. [600, 680] and [680, 760]) do NOT overlap and
    /// must produce two DISTINCT replay rows. Without this pin a future
    /// "fix" that flips the comparison to `<=` / `>=` would silently merge
    /// two genuinely-distinct user-reported ads into one row.
    @Test("adjacent (boundary-touching) falseNegative ranges do NOT overlap and produce two distinct replay rows")
    func adjacentFalseNegativeRangesAreNotDeduped() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-adjacent"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // Two FN reports that touch at exactly t=680. Strict-inequality
        // overlap means these are DISJOINT — both must emit.
        try await appendFalseNegativeCorrection(
            store: store, assetId: assetId, startTime: 600, endTime: 680
        )
        try await appendFalseNegativeCorrection(
            store: store, assetId: assetId, startTime: 680, endTime: 760
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)
        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        let replayRows = persisted
            .filter { $0.boundaryState == "correctionReplay" }
            .sorted { $0.startTime < $1.startTime }
        #expect(replayRows.count == 2,
                "boundary-touching FN ranges must produce TWO distinct replay rows, not one; got \(replayRows.count)")
        if replayRows.count == 2 {
            #expect(abs(replayRows[0].startTime - 600) < 0.01 && abs(replayRows[0].endTime - 680) < 0.01,
                    "first row must span 600..680; got \(replayRows[0].startTime)..\(replayRows[0].endTime)")
            #expect(abs(replayRows[1].startTime - 680) < 0.01 && abs(replayRows[1].endTime - 760) < 0.01,
                    "second row must span 680..760; got \(replayRows[1].startTime)..\(replayRows[1].endTime)")
        }
    }

    // MARK: - 10b. R9: sub-millisecond overlap is treated as overlap (dedupe), not as adjacency

    /// playhead-hygc.1.8 R9: the strict-inequality overlap predicate
    /// (`s1 < e2 && e1 > s2`) draws a HARD line at `==`. The
    /// adjacent-touching test above pins that `[600, 680]` and
    /// `[680, 760]` are DISJOINT (no dedupe). This test pins the
    /// complementary edge: a sub-millisecond overlap of just 0.001 s
    /// (`[600, 680.001]` and `[680, 760]`) IS overlap and DOES dedupe.
    ///
    /// Why this matters: scope serialization rounds to %.3f, so the
    /// finest distinguishable spacing the system can represent for
    /// overlap-vs-adjacency IS one millisecond. If a future "fix" widens
    /// the overlap predicate to `<=`/`>=` it would silently merge the
    /// adjacent case (caught by the test above). If a future "fix"
    /// loosens it to "overlap by at least N ms" it would silently
    /// un-dedupe the sub-millisecond case (caught by THIS test). The
    /// pair pins both directions of the boundary so a single regression
    /// on either side fails loudly.
    @Test("sub-millisecond overlap (1 ms) is treated as overlap and dedupes to a single replay row")
    func subMillisecondOverlapDedupesToOneRow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-submillisecond-overlap"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // [600, 680.001] and [680, 760]: the 1 ms overlap at the tail of
        // the first range satisfies the strict-inequality predicate
        // (680 < 680.001 && 760 > 600), so the second range overlaps
        // the first in the in-flight `emitted` set and is dropped.
        //
        // Insertion ordering is load-bearing for "first wins" — use
        // explicit, monotonically-increasing `createdAt` values so the
        // test cannot flake on same-millisecond clock collisions.
        let now = Date().timeIntervalSince1970
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 600, endTime: 680.001
            ).serialized,
            createdAt: now,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 680, endTime: 760
            ).serialized,
            createdAt: now + 1.0,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))

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
                "1 ms overlap must dedupe to a single replay row; got \(replayRows.count)")
        // The surviving row is the FIRST inserted (the one already in
        // `emitted` when the second is evaluated). Pin which range wins
        // so a future re-ordering of the FN iteration is loud.
        if let row = replayRows.first {
            #expect(abs(row.startTime - 600) < 0.0005 && abs(row.endTime - 680.001) < 0.0005,
                    "surviving replay row must be the first FN range (600..680.001); got \(row.startTime)..\(row.endTime)")
        }
    }

    // MARK: - 11. R8: explicit boundaryState + full stamp pin in one place

    /// playhead-hygc.1.8 R8: consolidate the five replay-row stamps
    /// (`boundaryState`, `metadataSource`, `eligibilityGate`,
    /// `decisionState`, `confidence`) into one assertive test so a
    /// regression on ANY one stamp fails this single test rather than
    /// silently slipping through coverage spread across multiple files.
    /// `falseNegativeCorrectionSurfacesMarkOnlyAdWindow` already covers 4
    /// of the 5 explicitly + boundaryState via filter; this test pins
    /// boundaryState as a direct `#expect` so the contract is loud.
    @Test("replay row carries all five stamps: correctionReplay/userCorrectionReplay/markOnly/candidate/1.0")
    func correctionReplayRowStampsArePinnedExplicitly() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-stamps"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        try await appendFalseNegativeCorrection(
            store: store, assetId: assetId, startTime: 800, endTime: 870
        )

        let classifier = SlotScoringClassifier(scoresByStartTime: [:], defaultScore: 0.05)
        let service = makeService(store: store, classifier: classifier)
        let returned = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        // Single replay row in both the returned set and the persisted
        // store.
        let returnedReplay = returned.filter { abs($0.startTime - 800) < 0.01 && abs($0.endTime - 870) < 0.01 }
        #expect(returnedReplay.count == 1, "expected one returned replay row; got \(returnedReplay.count)")

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        let candidates = persisted.filter { abs($0.startTime - 800) < 0.01 && abs($0.endTime - 870) < 0.01 }
        guard let row = candidates.first else {
            Issue.record("expected one persisted replay row at 800..870; got \(candidates.count)")
            return
        }
        // All five stamps pinned in one place. Any one regressing fails this test.
        #expect(row.boundaryState == "correctionReplay",
                "boundaryState stamp must be 'correctionReplay'; got \(row.boundaryState)")
        #expect(row.metadataSource == "userCorrectionReplay",
                "metadataSource stamp must be 'userCorrectionReplay'; got \(row.metadataSource ?? "<nil>")")
        #expect(row.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
                "eligibilityGate stamp must be 'markOnly'; got \(row.eligibilityGate ?? "<nil>")")
        #expect(row.decisionState == AdDecisionState.candidate.rawValue,
                "decisionState stamp must be 'candidate'; got \(row.decisionState)")
        #expect(row.confidence == 1.0,
                "confidence stamp must be 1.0; got \(row.confidence)")
    }

    // MARK: - 12. R10: defensive guard for malformed FN spans

    /// playhead-hygc.1.8 R10: pin the defensive guard inside
    /// `correctionReplayCandidates`:
    ///
    ///     guard s.isFinite, e.isFinite, e > s else { continue }
    ///
    /// Scope serialization round-trips through `String(format: "%.3f")`
    /// + `Double(...)`, both of which preserve NaN/Inf and also accept
    /// zero/negative durations. A FN scope with `s == e` (zero duration)
    /// or `s > e` (negative duration) or non-finite endpoints would,
    /// without the guard, persist a degenerate AdWindow whose
    /// `startTime == endTime` (or NaN/Inf) — leaking through to the
    /// suggest banner UI as a zero-pixel or invalid entry. The guard is
    /// cheap and silently filters these. Without a test, a future refactor
    /// could drop it without anyone noticing until a malformed event
    /// reaches the store in production.
    ///
    /// R11: infinity coverage was missing — the R10 test enumerated only
    /// NaN. `String(format: "%.3f", Double.infinity)` renders "inf" and
    /// `Double("inf")` parses it back as `Double.infinity`, which is NOT
    /// finite, so the same guard branch (`s.isFinite, e.isFinite`) catches
    /// it. Pinning Inf and -Inf alongside NaN means the `isFinite` half of
    /// the predicate is exercised end-to-end through the round-trip, not
    /// just the `e > s` half.
    @Test("zero-duration / inverted / non-finite falseNegative spans do not produce replay rows")
    func malformedFalseNegativeSpansAreSilentlyDropped() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-malformed"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // Bypass the convenience helper so we can inject malformed values
        // — `appendFalseNegativeCorrection` rounds via the public API,
        // and we want to exercise the defensive filter directly.
        // Zero-duration FN: start == end.
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 600, endTime: 600
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))
        // Negative-duration FN: start > end.
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 800, endTime: 700
            ).serialized,
            createdAt: Date().timeIntervalSince1970 + 1,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))
        // NaN endpoint FN. `%.3f` formatter renders NaN as "nan", and
        // `Double("nan")` parses it back as NaN — so this CAN reach the
        // emit path without the guard.
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: Double.nan, endTime: 700
            ).serialized,
            createdAt: Date().timeIntervalSince1970 + 2,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))
        // R11: positive infinity endpoint. `%.3f` formatter renders +Inf
        // as "inf", `Double("inf")` parses it back as `Double.infinity` —
        // the deserialized scope reaches the guard with `e.isFinite ==
        // false`, which the predicate rejects.
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 1000, endTime: Double.infinity
            ).serialized,
            createdAt: Date().timeIntervalSince1970 + 3,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))
        // R11: negative infinity endpoint. Symmetric coverage so the
        // `s.isFinite` half of the predicate is exercised end-to-end.
        // `%.3f` renders -Inf as "-inf", `Double("-inf")` parses it
        // back as `-Double.infinity`.
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: -Double.infinity, endTime: 1100
            ).serialized,
            createdAt: Date().timeIntervalSince1970 + 4,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))

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
                "malformed FN spans (zero-duration, negative-duration, NaN, +Inf, -Inf) must be silently dropped; got \(replayRows.count) rows: \(replayRows.map { ($0.startTime, $0.endTime) })")
    }

    // MARK: - 13. R11: containment overlap dedupes (third leg of the overlap-boundary triplet)

    /// playhead-hygc.1.8 R11: pin the third leg of the overlap-boundary
    /// triplet. R8 pinned ADJACENCY (`[600, 680]` and `[680, 760]` →
    /// disjoint, two rows). R9 pinned SUB-MILLISECOND OVERLAP
    /// (`[600, 680.001]` and `[680, 760]` → 1 ms overlap, one row). This
    /// pins CONTAINMENT (`[600, 800]` and `[650, 700]` — the second range
    /// fully inside the first).
    ///
    /// Why: the strict-inequality overlap predicate
    /// (`s1 < e2 && e1 > s2`) is symmetric — containment satisfies it
    /// trivially (`600 < 700 && 800 > 650`). But containment is a
    /// distinct geometric case from "shifted-but-partially-overlapping"
    /// (R7's existing test) and from "boundary-touching" (R8 / R9). A
    /// future "fix" that special-cased containment (e.g. "if one range
    /// contains another, keep the LARGER") would dedupe to one row but
    /// pick the wrong winner, silently swapping which span the user sees.
    /// Pinning containment means that subtle re-ordering also fails
    /// loudly: the FIRST inserted FN is the survivor (matching R9's
    /// "first wins" pin).
    @Test("containment (one falseNegative range fully inside another) dedupes to a single replay row; first inserted wins")
    func containmentOverlapDedupesToOneRow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-fn-replay-containment"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // [600, 800] is the OUTER range; [650, 700] is the INNER. The
        // outer is inserted first so it lands in `emitted` first; the
        // inner is rejected by the `overlapsEmitted` check.
        //
        // Explicit, monotonically-increasing `createdAt` so the test
        // cannot flake on same-millisecond clock collisions.
        let now = Date().timeIntervalSince1970
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 600, endTime: 800
            ).serialized,
            createdAt: now,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 650, endTime: 700
            ).serialized,
            createdAt: now + 1.0,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))

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
                "containment must dedupe to a single replay row; got \(replayRows.count)")
        // The surviving row must be the OUTER range — the first inserted
        // is the one already in `emitted` when the inner is evaluated.
        // Pin which range wins so a future re-ordering (e.g. sorting FN
        // ranges by duration before iteration) is loud.
        if let row = replayRows.first {
            #expect(abs(row.startTime - 600) < 0.01 && abs(row.endTime - 800) < 0.01,
                    "surviving row must be the FIRST FN range (outer 600..800); got \(row.startTime)..\(row.endTime)")
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
