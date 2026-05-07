// Hygc18DogfoodScenarioTests.swift
// playhead-hygc.1.8: integration-style test that mirrors the May 6
// dogfood pattern at a small fixture scale. The fixture exercises both
// halves of the bead's narrow change in one episode-shaped scenario:
//
//   * 1 boundary-singleton AdWindow at 0..30 (must remain recovered)
//   * 1 falseNegative correction at 1500..1560 with no overlapping
//     algorithmic AdWindow (recall lever — must surface as markOnly)
//   * 1 markOnly algorithmic AdWindow at 700..760 with a falsePositive
//     correction also at 700..760 (precision lever — must NOT auto-skip
//     and the markOnly row must be revertible by the user gesture)
//
// Acceptance asserts:
//   A. Boundary singleton remains in the persisted set as markOnly.
//   B. The unrecovered falseNegative span produces a markOnly AdWindow
//      tagged `boundaryState=correctionReplay`.
//   C. The markOnly window vetoed by the falsePositive correction is
//      .reverted in the orchestrator (not auto-skipped) and persisted
//      as such in the store after a `revertByTimeRange` gesture.
//   D. Auto-skip count is zero throughout — none of these windows is
//      eligible for auto-skip.

import CoreMedia
import Foundation
import Testing
@testable import Playhead

@Suite("playhead-hygc.1.8 dogfood scenario — integration of recall + precision levers")
struct Hygc18DogfoodScenarioTests {

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

    private func makeService(
        store: AnalysisStore,
        classifier: ClassifierService
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "hygc-1.8-dogfood",
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

    @Test("dogfood-shaped scenario: boundary singleton preserved + replay recall + FP suppression of markOnly")
    func dogfoodScenarioCoversAllThreeLevers() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hygc-dogfood"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // Tier 1 score 0.52 in the first 30 s slot — feeds the
        // boundary-singleton path. All other slots: 0.10 (well below
        // candidate threshold).
        let classifier = SlotScoringClassifier(
            scoresByStartTime: [0.0: 0.52],
            defaultScore: 0.10
        )
        let service = makeService(store: store, classifier: classifier)

        // ── Set up a markOnly algorithmic window at 700..760 that the
        // user has vetoed (i.e. dogfood asset's "8 markOnly windows the
        // user said weren't ads"). We synthesize this directly because
        // generating it through the live classifier would require
        // additional Tier 1 scoring fixture which is orthogonal to this
        // test. The shape mirrors what the boundary-singleton path
        // produces.
        let prevetoedMarkOnly = AdWindow(
            id: "ad-prevetoed-markonly",
            analysisAssetId: assetId,
            startTime: 700,
            endTime: 760,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 700,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(prevetoedMarkOnly)

        // ── User reports a missed ad at 1500..1560 (the recall lever).
        let fnScope = CorrectionScope.exactTimeSpan(
            assetId: assetId, startTime: 1500, endTime: 1560
        )
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: fnScope.serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))

        // ── Run hot path.
        let hotPathWindows = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persistedAfterHotPath = try await store.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        // ── A. Boundary singleton at 0..30 is preserved, markOnly.
        let boundarySingleton = persistedAfterHotPath.first {
            $0.boundaryState == AdBoundaryState.segmentAggregated.rawValue &&
            $0.startTime <= 1.0 && $0.endTime >= 29.0 && $0.endTime <= 32.0
        }
        #expect(boundarySingleton != nil,
                "boundary-singleton recovery must survive playhead-hygc.1.8 changes")
        #expect(boundarySingleton?.eligibilityGate == "markOnly",
                "boundary singleton must remain markOnly")

        // ── B. Unrecovered falseNegative correction surfaced as a
        // correction-replay markOnly AdWindow.
        let replayed = persistedAfterHotPath.first {
            $0.boundaryState == "correctionReplay" &&
            abs($0.startTime - 1500) < 0.01 &&
            abs($0.endTime - 1560) < 0.01
        }
        #expect(replayed != nil,
                "falseNegative correction at 1500..1560 must surface as a markOnly correction-replay AdWindow")
        #expect(replayed?.eligibilityGate == "markOnly",
                "correction-replay must be markOnly to keep precision contract")
        #expect(replayed?.metadataSource == "userCorrectionReplay")

        // The hot path return set must include the new replay row so a
        // live SkipOrchestrator wired to the result picks it up.
        #expect(
            hotPathWindows.contains { $0.id == replayed?.id },
            "runHotPath return must include the new replay row so SkipOrchestrator surfaces it"
        )

        // ── C. Now drive the precision lever: orchestrator receives
        // every persisted window and the user vetoes the prevetoed
        // markOnly window via revertByTimeRange.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: assetId,
            podcastId: "podcast-1"
        )
        await orchestrator.receiveAdWindows(persistedAfterHotPath)

        // ── D. Auto-skip count is zero — none of boundary-singleton,
        // correction-replay, or prevetoed markOnly is eligible for
        // auto-skip. Verified BEFORE any veto so we know the markOnly
        // contract is honored even if the user takes no action.
        #expect(pushedCues.isEmpty,
                "no auto-skip cues must be pushed for markOnly windows; got \(pushedCues.count)")

        // The prevetoed markOnly window is in the suggest dictionary
        // (not the auto-skip dict).
        let suggestIDsBefore = await orchestrator.activeSuggestWindowIDs()
        #expect(suggestIDsBefore.contains("ad-prevetoed-markonly"),
                "prevetoed markOnly must enter suggestWindows; got \(suggestIDsBefore)")
        #expect(!(await orchestrator.activeWindowIDs().contains("ad-prevetoed-markonly")),
                "markOnly must NOT enter the auto-skip windows dict")

        // ── User vetoes 700..760. revertByTimeRange must clear the
        // markOnly entry from the suggest dict AND persist the AdWindow
        // as `decisionState = .reverted`.
        await orchestrator.revertByTimeRange(start: 700, end: 760, podcastId: "podcast-1")

        let suggestIDsAfter = await orchestrator.activeSuggestWindowIDs()
        #expect(!suggestIDsAfter.contains("ad-prevetoed-markonly"),
                "veto must clear markOnly from suggestWindows; got \(suggestIDsAfter)")

        let persistedAfterVeto = try await store.fetchAdWindows(assetId: assetId)
        let prevetoedRow = persistedAfterVeto.first { $0.id == "ad-prevetoed-markonly" }
        #expect(prevetoedRow?.decisionState == AdDecisionState.reverted.rawValue,
                "vetoed markOnly window must persist as reverted; got \(prevetoedRow?.decisionState ?? "<nil>")")

        // Auto-skip count remains zero after veto.
        #expect(pushedCues.isEmpty,
                "veto must not promote anything to auto-skip; got \(pushedCues.count) cues")

        // The boundary-singleton and correction-replay rows are
        // unaffected by the unrelated 700..760 veto.
        let untouched = persistedAfterVeto.filter {
            $0.boundaryState == "correctionReplay" ||
            ($0.boundaryState == AdBoundaryState.segmentAggregated.rawValue && $0.startTime <= 1.0)
        }
        #expect(untouched.allSatisfy { $0.decisionState != AdDecisionState.reverted.rawValue },
                "unrelated windows must not be reverted by a localized veto")
    }

    // playhead-hygc.1.8 R5: integration coverage for the production
    // retirement-enabled multi-run path. The original
    // `dogfoodScenarioCoversAllThreeLevers` test calls `runHotPath`
    // (defaulting to `retireUnmatchedReplayCandidates: false`) with empty
    // chunks — a code path that takes the `chunks.isEmpty` early return
    // and never even reaches the retirement logic. Production
    // `AnalysisCoordinator.handlePersistedTranscriptChunks` calls
    // `runHotPathResult` with `retireUnmatchedReplayCandidates: true` and
    // non-empty chunks, which is exactly the path R3 / R4 had to harden.
    //
    // This test mirrors the production caller shape (chunks present,
    // retirement enabled) over MULTIPLE runs and asserts:
    //   * Run 1 emits the replay row, the row is NOT in
    //     `retiredWindowIDs` (R3 fix).
    //   * Run 2 keeps the previously-emitted replay row and the row's
    //     id is NOT in `retiredWindowIDs` even though it would match
    //     `(decisionState=.candidate, detectorVersion=current)` in
    //     `hotPathCandidateIDs` (R4 fix).
    //   * Run 3 keeps population bounded at one replay row.
    //   * Through all runs, the row identity is stable (same UUID).
    //
    // Without this, the integration suite passes for the wrong reason:
    // the empty-chunks branch short-circuits before retirement runs.
    @Test("integration scenario survives the production multi-run retirement-enabled path")
    func dogfoodScenarioSurvivesMultiRunRetirement() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hygc-dogfood-multi-run"
        try await store.insertAsset(makeAsset(id: assetId))

        let duration: Double = 1800
        try await insertUniformFeatureGrid(store: store, assetId: assetId, duration: duration)

        // No-op classifier: only the correction-replay path can produce
        // an AdWindow. This isolates the multi-run retirement bug from
        // any algorithmic noise.
        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.05
        )
        let service = makeService(store: store, classifier: classifier)

        // FN at 1500..1560 — same shape as the dogfood scenario.
        try await store.appendCorrectionEvent(CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 1500, endTime: 1560
            ).serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative,
            podcastId: nil,
            correctionType: .falseNegative
        ))

        // Chunks whose envelope (1400..1600) overlaps the FN range so the
        // retirement path's `replayCandidateIDs` query would include the
        // persisted replay row on every subsequent run. Lexically benign
        // text → algorithmic detector emits nothing.
        let chunks: [TranscriptChunk] = [
            TranscriptChunk(
                id: "chunk-1",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-1",
                chunkIndex: 0,
                startTime: 1400,
                endTime: 1600,
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

        // Run 1: emit + retain.
        let r1 = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )
        let after1 = try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.boundaryState == "correctionReplay" }
        #expect(after1.count == 1,
                "run 1: must emit exactly one replay row; got \(after1.count)")
        let stableId = after1.first?.id ?? ""
        #expect(!r1.retiredWindowIDs.contains(stableId),
                "run 1: fresh replay row id must not be retired; got \(r1.retiredWindowIDs)")
        #expect(r1.windows.contains { $0.id == stableId },
                "run 1: HotPathRunResult.windows must include the replay row")

        // Run 2: replay short-circuits, persisted row must survive
        // retirement (R4 critical fix).
        let r2 = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )
        let after2 = try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.boundaryState == "correctionReplay" }
        #expect(after2.count == 1,
                "run 2: replay row must survive retirement; got \(after2.count)")
        #expect(after2.first?.id == stableId,
                "run 2: replay row id must be stable; got \(after2.first?.id ?? "<missing>")")
        #expect(!r2.retiredWindowIDs.contains(stableId),
                "run 2: retiredWindowIDs must not contain replay row; got \(r2.retiredWindowIDs)")

        // Run 3: bounded.
        let r3 = try await service.runHotPathResult(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration,
            retireUnmatchedReplayCandidates: true
        )
        let after3 = try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.boundaryState == "correctionReplay" }
        #expect(after3.count == 1,
                "run 3: population must remain bounded at one replay row; got \(after3.count)")
        #expect(!r3.retiredWindowIDs.contains(stableId),
                "run 3: retiredWindowIDs must not contain replay row; got \(r3.retiredWindowIDs)")
    }
}

// MARK: - Test doubles

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
