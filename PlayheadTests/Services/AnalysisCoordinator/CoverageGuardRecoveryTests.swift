// CoverageGuardRecoveryTests.swift
// Tests for AnalysisCoordinator's coverage-guard recovery sweep. The
// sweep inspects `.failed` sessions whose `failureReason` came from the
// `finalizeBackfill` coverage guard and, when the transcript has since
// caught up past the minimum ratio, requeues them for backfill so the
// next pipeline run finalizes them cleanly.

import Foundation
import Testing
@testable import Playhead

// MARK: - Pure decision helper

@Suite("AnalysisCoordinator – Coverage Guard Recovery Verdict")
struct CoverageGuardRecoveryVerdictTests {

    /// A representative failure reason encoded by the coverage guard for a
    /// 3600s episode whose transcript only reached ~689.8s. We reuse the
    /// same format string the guard itself builds so the parser contract
    /// is pinned to production output.
    private static let shortCoverageReason =
        "transcript coverage 689.8/3600.0s (ratio 0.192 < 0.950)"

    @Test("recovered coverage flips the verdict to .recover")
    func recoveredCoverageIsRecoverable() {
        let verdict = AnalysisCoordinator.coverageGuardRecoveryVerdict(
            failureReason: Self.shortCoverageReason,
            coverageEnd: 3500.0  // 3500/3600 ≈ 0.972
        )
        switch verdict {
        case .recover(let coverageEnd, let duration, let ratio):
            #expect(coverageEnd == 3500.0)
            #expect(duration == 3600.0)
            #expect(ratio > 0.95)
        case .skipBelowThreshold, .skipUnrelated:
            Issue.record("Expected .recover for coverage that now exceeds 95%")
        }
    }

    @Test("coverage still below threshold stays skipped")
    func stillBelowThresholdIsSkipped() {
        let verdict = AnalysisCoordinator.coverageGuardRecoveryVerdict(
            failureReason: Self.shortCoverageReason,
            coverageEnd: 800.0
        )
        switch verdict {
        case .skipBelowThreshold(let coverageEnd, let duration, let ratio):
            #expect(coverageEnd == 800.0)
            #expect(duration == 3600.0)
            #expect(ratio < 0.95)
        case .recover, .skipUnrelated:
            Issue.record("Expected .skipBelowThreshold for 800/3600")
        }
    }

    @Test("failure reason from a different source is skipped as unrelated")
    func unrelatedFailureReasonIsSkipped() {
        let verdict = AnalysisCoordinator.coverageGuardRecoveryVerdict(
            failureReason: "audio decode failed: corrupt file",
            coverageEnd: 3600.0
        )
        #expect(verdict == .skipUnrelated)
    }

    @Test("nil failure reason is skipped as unrelated")
    func nilFailureReasonIsSkipped() {
        let verdict = AnalysisCoordinator.coverageGuardRecoveryVerdict(
            failureReason: nil,
            coverageEnd: 3600.0
        )
        #expect(verdict == .skipUnrelated)
    }

    @Test("exactly at the 95% threshold recovers")
    func atThresholdRecovers() {
        // 3420/3600 == 0.95 exactly
        let verdict = AnalysisCoordinator.coverageGuardRecoveryVerdict(
            failureReason: Self.shortCoverageReason,
            coverageEnd: 3420.0
        )
        switch verdict {
        case .recover: break
        case .skipBelowThreshold, .skipUnrelated:
            Issue.record("Exactly at the 95% threshold must recover")
        }
    }

    @Test("duration is parsed back out of the coverage-guard reason string")
    func durationParsesFromFailureReason() {
        let duration = AnalysisCoordinator.parseCoverageGuardEpisodeDuration(
            from: Self.shortCoverageReason
        )
        #expect(duration == 3600.0)
    }

    @Test("malformed failure reason with the right prefix returns nil duration")
    func malformedReasonReturnsNilDuration() {
        let duration = AnalysisCoordinator.parseCoverageGuardEpisodeDuration(
            from: "transcript coverage bogus"
        )
        #expect(duration == nil)
    }
}

// MARK: - End-to-end sweep

@Suite("AnalysisCoordinator – Coverage Guard Recovery Sweep", .serialized)
struct CoverageGuardRecoverySweepTests {

    // Scope: tests only exercise the sweep's store reads and writes, so
    // we build a minimal AnalysisCoordinator with stub collaborators.
    // The sweep never calls any of them, but the type signatures force
    // them to exist.

    private static let storeDirs = TestTempDirTracker()

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "CoverageGuardRecoverySweepTests")
        Self.storeDirs.track(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        // The sweep only reaches through `store`; the other collaborators
        // are untouched. Constructing them is still required because the
        // coordinator owns them as non-optional stored properties.
        let speechService = SpeechService(
            vocabularyProvider: ASRVocabularyProvider(store: store)
        )
        return AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: speechService,
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
    }

    private func makeAsset(
        id: String,
        analysisState: String = SessionState.failed.rawValue
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: analysisState,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeSession(
        id: String,
        assetId: String,
        state: SessionState,
        failureReason: String? = nil
    ) -> AnalysisSession {
        AnalysisSession(
            id: id,
            analysisAssetId: assetId,
            state: state.rawValue,
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: failureReason
        )
    }

    private func makeChunk(
        assetId: String,
        chunkIndex: Int,
        startTime: Double,
        endTime: Double
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(chunkIndex)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(assetId)-\(chunkIndex)",
            chunkIndex: chunkIndex,
            startTime: startTime,
            endTime: endTime,
            text: "x",
            normalizedText: "x",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    // The coverage-guard format string used by finalizeBackfill. Tests that
    // seed a failed session must use the same shape so the parser in the
    // sweep recognises it.
    private func coverageGuardReason(coverage: Double, duration: Double) -> String {
        let ratio = duration > 0 ? coverage / duration : 0
        return String(
            format: "transcript coverage %.1f/%.1fs (ratio %.3f < %.3f)",
            coverage, duration, ratio, AnalysisCoordinator.finalizeBackfillMinCoverageRatio
        )
    }

    // MARK: - Scenarios

    @Test("stranded asset whose transcript has caught up is requeued to .backfill")
    func recoveredAssetIsRequeued() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        // Seed: asset + failed session that failed the coverage guard
        // with the transcript only reaching 600/3600s, followed by enough
        // chunks to now cover 95%+ of the episode.
        let assetId = "asset-recovered"
        let sessionId = "session-recovered"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertSession(makeSession(
            id: sessionId,
            assetId: assetId,
            state: .failed,
            failureReason: coverageGuardReason(coverage: 600, duration: 3600)
        ))
        // Two chunks: the first is the original coverage, the second
        // represents the transcription that completed after the guard
        // fired, pushing the effective coverage past the 95% gate.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: 600),
            makeChunk(assetId: assetId, chunkIndex: 1, startTime: 600, endTime: 3500)
        ])

        let summary = await coordinator.recoverCoverageGuardFailures()

        #expect(summary.recoveredSessionIds == [sessionId])
        #expect(summary.stillBelowThreshold == 0)

        // Session row now reads `.backfill` with no failure reason, and
        // the asset mirrors the state so the scheduler / pipeline can
        // pick it up.
        let session = try await store.fetchSession(id: sessionId)
        #expect(session?.state == SessionState.backfill.rawValue)
        #expect(session?.failureReason == nil)
        let asset = try await store.fetchAsset(id: assetId)
        #expect(asset?.analysisState == SessionState.backfill.rawValue)
    }

    @Test("stranded asset still below 95% stays failed")
    func belowThresholdAssetStaysFailed() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-still-short"
        let sessionId = "session-still-short"
        try await store.insertAsset(makeAsset(id: assetId))
        let originalReason = coverageGuardReason(coverage: 600, duration: 3600)
        try await store.insertSession(makeSession(
            id: sessionId,
            assetId: assetId,
            state: .failed,
            failureReason: originalReason
        ))
        // Coverage still sits around 800s of 3600s (~22%).
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: 800)
        ])

        let summary = await coordinator.recoverCoverageGuardFailures()

        #expect(summary.recoveredSessionIds.isEmpty)
        #expect(summary.stillBelowThreshold == 1)

        let session = try await store.fetchSession(id: sessionId)
        #expect(session?.state == SessionState.failed.rawValue)
        #expect(session?.failureReason == originalReason)
    }

    @Test("session failed for an unrelated reason is left untouched")
    func unrelatedFailureIsUntouched() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-unrelated"
        let sessionId = "session-unrelated"
        try await store.insertAsset(makeAsset(id: assetId))
        let unrelatedReason = "audio decode failed: corrupt file"
        try await store.insertSession(makeSession(
            id: sessionId,
            assetId: assetId,
            state: .failed,
            failureReason: unrelatedReason
        ))
        // Even with full coverage the sweep should ignore this row
        // because its failure reason is not a coverage-guard failure.
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: 3600)
        ])

        let summary = await coordinator.recoverCoverageGuardFailures()

        #expect(summary.recoveredSessionIds.isEmpty)
        // Rows whose failureReason does not match the prefix are not
        // selected by the store query, so they are never inspected and
        // therefore never counted — we still assert the session did not
        // change state.
        let session = try await store.fetchSession(id: sessionId)
        #expect(session?.state == SessionState.failed.rawValue)
        #expect(session?.failureReason == unrelatedReason)
    }

    @Test("session already in .complete is never downgraded")
    func completeSessionIsUntouched() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-complete"
        let sessionId = "session-complete"
        try await store.insertAsset(
            makeAsset(id: assetId, analysisState: SessionState.complete.rawValue)
        )
        try await store.insertSession(makeSession(
            id: sessionId,
            assetId: assetId,
            state: .complete,
            failureReason: nil
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: 3600)
        ])

        let summary = await coordinator.recoverCoverageGuardFailures()

        #expect(summary.recoveredSessionIds.isEmpty)
        let session = try await store.fetchSession(id: sessionId)
        #expect(session?.state == SessionState.complete.rawValue)
    }

    @Test("user-marked ad windows survive a recovery transition")
    func userMarkedAdWindowsSurvive() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-with-user-mark"
        let sessionId = "session-with-user-mark"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertSession(makeSession(
            id: sessionId,
            assetId: assetId,
            state: .failed,
            failureReason: coverageGuardReason(coverage: 600, duration: 3600)
        ))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: 3500)
        ])

        // A user-marked ad window persisted before the sweep runs.
        let userMarked = AdWindow(
            id: "ad-user-marked-1",
            analysisAssetId: assetId,
            startTime: 30,
            endTime: 120,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 30,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        )
        try await store.insertAdWindow(userMarked)

        let summary = await coordinator.recoverCoverageGuardFailures()
        #expect(summary.recoveredSessionIds == [sessionId])

        // The ad_windows table must be byte-for-byte untouched — the
        // sweep only writes to analysis_sessions / analysis_assets.
        let adWindows = try await store.fetchAdWindows(assetId: assetId)
        #expect(adWindows.count == 1)
        #expect(adWindows.first?.id == userMarked.id)
        #expect(adWindows.first?.boundaryState == "userMarked")
        #expect(adWindows.first?.startTime == 30)
        #expect(adWindows.first?.endTime == 120)
    }
}
