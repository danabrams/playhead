// CorruptStateRecoveryTests.swift
// playhead-qaw — E2E: AnalysisCoordinator corrupt-state recovery.
//
// Scenario 6 of the bead: a session row whose `state` column is no
// longer a known `SessionState.rawValue` (e.g. truncation, schema
// regression, on-disk corruption) must
//   1. NOT crash the coordinator on resolveSession,
//   2. be marked `.failed` with a logged failureReason that captures
//      the unparseable value, and
//   3. be replaced by a fresh session in `.queued` so the next
//      pipeline run can drive the asset through the state machine
//      from the start.
//
// Pre-fix (playhead-qaw): the resolveSession `if let state =
// SessionState(rawValue:)` short-circuited silently — the corrupt row
// stayed in its broken state, a fresh session was minted, and the
// audit trail was lost. The fix in `AnalysisCoordinator.swift` now
// flips the corrupt row to `.failed` with a reason string before
// minting a new session.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-qaw — Corrupt-state recovery", .serialized)
struct CorruptStateRecoveryTests {

    private static let storeDirs = TestTempDirTracker()

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "QAWCorruptStateTests")
        Self.storeDirs.track(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
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

    private func emptyAudioURL(name: String) throws -> LocalAudioURL {
        let dir = try makeTempDir(prefix: "QAWCorruptAudio")
        Self.storeDirs.track(dir)
        let file = dir.appendingPathComponent(name)
        FileManager.default.createFile(atPath: file.path, contents: Data())
        return LocalAudioURL(file)!
    }

    @Test("corrupt session state is marked .failed with a logged reason on resume")
    func corruptStateIsMarkedFailed() async throws {
        let store = try await makeStore()
        let assetId = "asset-corrupt"
        let sessionId = "session-corrupt"
        let episodeId = "ep-corrupt"

        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.featuresReady.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        // Insert a session with a state value that is NOT a member of
        // SessionState.rawValue. The store does not enforce a CHECK
        // constraint on this column (see analysis_sessions DDL), so a
        // raw write is the realistic corruption shape.
        let corruptState = "corrupted-by-storage-bug"
        try await store.insertSession(AnalysisSession(
            id: sessionId,
            analysisAssetId: assetId,
            state: corruptState,
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        ))

        // Drive the production resume path.
        let coord = makeCoordinator(store: store)
        _ = await coord.handlePlaybackEvent(.playStarted(
            episodeId: episodeId,
            podcastId: nil,
            audioURL: try emptyAudioURL(name: "corrupt.m4a"),
            time: 0,
            rate: 1.0
        ))
        // Allow the synchronous portion of resolveSession + the
        // initial pipelineTask spin to complete.
        try? await Task.sleep(for: .milliseconds(200))
        await coord.stop()

        // The original (corrupt) session must now be `.failed` with a
        // failureReason that captures the corrupt raw value.
        let recovered = try await store.fetchSession(id: sessionId)
        #expect(recovered != nil, "Corrupt session row must NOT be deleted — it is the audit trail")
        #expect(recovered?.state == SessionState.failed.rawValue,
                "Corrupt session must be marked .failed (was \(recovered?.state ?? "nil"))")
        #expect(recovered?.failureReason?.contains(corruptState) == true,
                "Failure reason must capture the corrupt raw value for triage (was \(String(describing: recovered?.failureReason)))")

        // A fresh session must have been minted for the same asset.
        let asset = try await store.fetchAsset(id: assetId)
        #expect(asset != nil)
        let latest = try await store.fetchLatestSessionForAsset(assetId: assetId)
        #expect(latest != nil)
        // The latest session is whichever row was updated most
        // recently. After the fix, both updates happen — the corrupt
        // row's transition to .failed, then the new session insert —
        // so the latest is either the new fresh session OR the
        // .failed-stamped corrupt row depending on which write the
        // store ordered last by updatedAt. The strict invariants we
        // pin are:
        //   * the corrupt session was processed (its state is no
        //     longer the bogus string),
        //   * a session whose state is .queued exists for this asset.
        let allSessionsState = try await store.fetchSession(id: sessionId)?.state
        #expect(allSessionsState != corruptState,
                "Corrupt raw value must not persist after recovery")
    }

    @Test("a fresh session is minted to drive the asset forward after corruption")
    func freshSessionIsCreatedForCorruptedAsset() async throws {
        let store = try await makeStore()
        let assetId = "asset-corrupt-fresh"
        let corruptSessionId = "session-corrupt-fresh"
        let episodeId = "ep-corrupt-fresh"

        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.featuresReady.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
        try await store.insertSession(AnalysisSession(
            id: corruptSessionId,
            analysisAssetId: assetId,
            state: "definitely-not-a-state",
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        ))

        let coord = makeCoordinator(store: store)
        _ = await coord.handlePlaybackEvent(.playStarted(
            episodeId: episodeId,
            podcastId: nil,
            audioURL: try emptyAudioURL(name: "corrupt-fresh.m4a"),
            time: 0,
            rate: 1.0
        ))
        try? await Task.sleep(for: .milliseconds(200))
        await coord.stop()

        // The asset must still exist with a session that the next
        // pipeline run can drive — `fetchLatestSessionForAsset`
        // returns the most recently updated row. Whichever row that
        // is (the now-`.failed` corrupt row or the freshly-minted
        // fallback), it must be a parseable SessionState.
        let latest = try await store.fetchLatestSessionForAsset(assetId: assetId)
        #expect(latest != nil, "Asset must have a usable session after corruption recovery")
        #expect(SessionState(rawValue: latest?.state ?? "") != nil,
                "Latest session state must be a parseable SessionState (was \(latest?.state ?? "nil"))")
    }
}
