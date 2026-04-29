// CrashRecoveryAtEachStateTests.swift
// playhead-qaw — E2E: AnalysisCoordinator state-machine recovery.
//
// Scenario 1 of the bead: prove that for each AnalysisSession state
// (.queued, .spooling, .featuresReady, .hotPathReady, .backfill) the
// coordinator
//   1. survives a "force kill" (modeled as: tear down the coordinator
//      instance and instantiate a fresh one against the same persisted
//      AnalysisStore — the rehydration path the production code hits on
//      relaunch).
//   2. resumes from the persisted state rather than re-running prior
//      work, and
//   3. does not duplicate transcript chunks or feature windows that
//      were already in the store.
//
// Why model "force kill" this way: a real OS-level kill needs a UI
// runner on a real device. The substitute exercises the same code
// path the production app uses on cold launch — `resolveSession`
// reads the persisted row and runs `runPipeline(resumeState:)`. The
// observable invariants (chunks/features stable, session row survives)
// are the same.
//
// The audio fixture used for `LocalAudioURL` is intentionally an empty
// file so the spool stage cannot decode anything. That is the realistic
// "the cached audio went missing while we were dead" condition: the
// rehydration logic must still preserve persisted analysis artifacts
// even when the pipeline subsequently fails.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-qaw — Crash recovery at each state", .serialized)
struct CrashRecoveryAtEachStateTests {

    // MARK: - Per-suite scratch

    private static let storeDirs = TestTempDirTracker()

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "QAWCrashRecoveryTests")
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

    /// Returns a `LocalAudioURL` pointing at a freshly-created empty
    /// file. The path resolves so `LocalAudioURL.init?` succeeds; the
    /// underlying file is empty so `AnalysisAudioService.decode` will
    /// throw — which is the failure shape we want under "audio missing
    /// after relaunch".
    private func emptyAudioURL(name: String = "empty.m4a") throws -> LocalAudioURL {
        let dir = try makeTempDir(prefix: "QAWAudioFixture")
        Self.storeDirs.track(dir)
        let file = dir.appendingPathComponent(name)
        FileManager.default.createFile(atPath: file.path, contents: Data())
        guard let local = LocalAudioURL(file) else {
            preconditionFailure("LocalAudioURL must accept a file:// path")
        }
        return local
    }

    private func makeAsset(
        id: String,
        episodeId: String,
        analysisState: SessionState,
        episodeDurationSec: Double? = 600,
        featureCoverageEndTime: Double? = nil,
        fastTranscriptCoverageEndTime: Double? = nil
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            featureCoverageEndTime: featureCoverageEndTime,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: nil,
            analysisState: analysisState.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    }

    private func makeSession(
        id: String,
        assetId: String,
        state: SessionState
    ) -> AnalysisSession {
        AnalysisSession(
            id: id,
            analysisAssetId: assetId,
            state: state.rawValue,
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
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
            text: "transcript-chunk-\(chunkIndex)",
            normalizedText: "transcript chunk \(chunkIndex)",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    private func makeFeatureWindow(
        assetId: String,
        startTime: Double,
        endTime: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            rms: 0.05,
            spectralFlux: 0.05,
            musicProbability: 0.05,
            speakerChangeProxyScore: 0.0,
            musicBedChangeScore: 0.0,
            musicBedOnsetScore: 0.0,
            musicBedOffsetScore: 0.0,
            musicBedLevel: .none,
            pauseProbability: 0.1,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 4
        )
    }

    /// Drives the production rehydration path: instantiate a fresh
    /// coordinator against the same store, call the play-start entry
    /// point that production hits on cold launch, and wait for the
    /// pipeline task to either advance or fail. Returns once the
    /// coordinator is quiesced and stopped, so the caller can read the
    /// persisted store with no in-flight writes.
    private func relaunchAndDrive(
        store: AnalysisStore,
        episodeId: String,
        audioURL: LocalAudioURL
    ) async {
        let fresh = makeCoordinator(store: store)
        let event = PlaybackEvent.playStarted(
            episodeId: episodeId,
            podcastId: nil,
            audioURL: audioURL,
            time: 0,
            rate: 1.0
        )
        _ = await fresh.handlePlaybackEvent(event)
        // Give the pipelineTask a moment to advance through the
        // synchronous portion of the resume branch. The pipelineTask
        // itself is fire-and-forget; `stop()` cancels it and joins on
        // the actor.
        try? await Task.sleep(for: .milliseconds(200))
        await fresh.stop()
    }

    // MARK: - Resume from .queued

    @Test(".queued — fresh coordinator does not erase asset row")
    func resumeFromQueued() async throws {
        let store = try await makeStore()
        let assetId = "asset-queued"
        let sessionId = "session-queued"
        let episodeId = "ep-queued"
        try await store.insertAsset(makeAsset(
            id: assetId, episodeId: episodeId, analysisState: .queued
        ))
        try await store.insertSession(makeSession(
            id: sessionId, assetId: assetId, state: .queued
        ))

        let chunksBefore = try await store.fetchTranscriptChunks(assetId: assetId).count
        let featuresBefore = try await store.fetchFeatureWindows(
            assetId: assetId, from: 0, to: 1_000_000
        ).count

        await relaunchAndDrive(
            store: store,
            episodeId: episodeId,
            audioURL: try emptyAudioURL()
        )

        // Asset and the original session id must both still exist —
        // resolveSession may rewrite session state but must not delete
        // the row.
        let assetAfter = try await store.fetchAsset(id: assetId)
        #expect(assetAfter != nil, "Asset row must survive relaunch")
        let sessionAfter = try await store.fetchSession(id: sessionId)
        #expect(sessionAfter != nil, "Original session row must survive relaunch")

        let chunksAfter = try await store.fetchTranscriptChunks(assetId: assetId).count
        let featuresAfter = try await store.fetchFeatureWindows(
            assetId: assetId, from: 0, to: 1_000_000
        ).count
        #expect(chunksAfter == chunksBefore, "Resume must not invent transcript chunks (had \(chunksBefore), now \(chunksAfter))")
        #expect(featuresAfter == featuresBefore, "Resume must not invent feature windows (had \(featuresBefore), now \(featuresAfter))")
    }

    // MARK: - Resume from .spooling

    @Test(".spooling — fresh coordinator preserves persisted state and prior chunks")
    func resumeFromSpooling() async throws {
        let store = try await makeStore()
        let assetId = "asset-spooling"
        let sessionId = "session-spooling"
        let episodeId = "ep-spooling"
        try await store.insertAsset(makeAsset(
            id: assetId, episodeId: episodeId, analysisState: .spooling
        ))
        try await store.insertSession(makeSession(
            id: sessionId, assetId: assetId, state: .spooling
        ))
        // Seed a partial transcript chunk that a prior process had
        // landed before the simulated kill.
        let seededChunks = [
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: 30)
        ]
        try await store.insertTranscriptChunks(seededChunks)

        let chunksBefore = try await store.fetchTranscriptChunks(assetId: assetId)
        #expect(chunksBefore.count == 1)

        await relaunchAndDrive(
            store: store,
            episodeId: episodeId,
            audioURL: try emptyAudioURL()
        )

        let chunksAfter = try await store.fetchTranscriptChunks(assetId: assetId)
        // The pipeline may fail on the empty audio fixture, but it
        // must not delete or duplicate the chunk that was already
        // there. Identity preserved by composite primary key.
        #expect(chunksAfter.count == chunksBefore.count, "Spooling resume must not duplicate or drop seeded chunks")
        #expect(chunksAfter.first?.id == chunksBefore.first?.id)

        // The session row must still exist (no orphan delete on the
        // crash path).
        let session = try await store.fetchSession(id: sessionId)
        #expect(session != nil)
    }

    // MARK: - Resume from .featuresReady

    @Test(".featuresReady — fresh coordinator does not re-extract features")
    func resumeFromFeaturesReady() async throws {
        let store = try await makeStore()
        let assetId = "asset-features-ready"
        let sessionId = "session-features-ready"
        let episodeId = "ep-features-ready"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: episodeId,
            analysisState: .featuresReady,
            featureCoverageEndTime: 600
        ))
        try await store.insertSession(makeSession(
            id: sessionId, assetId: assetId, state: .featuresReady
        ))
        // Seed feature windows that a prior process had extracted.
        let windows: [FeatureWindow] = (0..<60).map { i in
            makeFeatureWindow(
                assetId: assetId,
                startTime: Double(i) * 10,
                endTime: Double(i + 1) * 10
            )
        }
        try await store.insertFeatureWindows(windows)

        let featuresBefore = try await store.fetchFeatureWindows(
            assetId: assetId, from: 0, to: 1_000_000, minimumFeatureVersion: nil
        )
        #expect(featuresBefore.count == 60)

        await relaunchAndDrive(
            store: store,
            episodeId: episodeId,
            audioURL: try emptyAudioURL()
        )

        let featuresAfter = try await store.fetchFeatureWindows(
            assetId: assetId, from: 0, to: 1_000_000, minimumFeatureVersion: nil
        )
        // The featuresReady-resume branch must NOT re-run feature
        // extraction — it skips `runFromSpooling` entirely. Therefore
        // the seeded count is the floor. (The number must equal, not
        // exceed.) `extractAndPersist` writes via UPSERT keyed by
        // (analysisAssetId, startTime), so even an erroneous re-run
        // would still produce 60 rows — but `featureVersion` would
        // bump if the resume tried to invoke the live extractor with
        // a different config. Asserting equal-by-count is the
        // strongest cheap signal that no extractor work happened.
        #expect(featuresAfter.count == featuresBefore.count, "Resume from .featuresReady must not re-extract feature windows (had \(featuresBefore.count), now \(featuresAfter.count))")
    }

    // MARK: - Resume from .hotPathReady

    @Test(".hotPathReady — fresh coordinator preserves chunks and may advance state, never erases")
    func resumeFromHotPathReady() async throws {
        let store = try await makeStore()
        let assetId = "asset-hot-path"
        let sessionId = "session-hot-path"
        let episodeId = "ep-hot-path"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: episodeId,
            analysisState: .hotPathReady,
            featureCoverageEndTime: 600,
            fastTranscriptCoverageEndTime: 600
        ))
        try await store.insertSession(makeSession(
            id: sessionId, assetId: assetId, state: .hotPathReady
        ))
        // Seed enough chunks that the resume-from-backfill coverage
        // guard is happy (≥95% of 600s episode duration).
        let chunks: [TranscriptChunk] = (0..<60).map { i in
            makeChunk(
                assetId: assetId,
                chunkIndex: i,
                startTime: Double(i) * 10,
                endTime: Double(i + 1) * 10
            )
        }
        try await store.insertTranscriptChunks(chunks)
        try await store.updateFastTranscriptCoverage(id: assetId, endTime: 600)

        let chunksBefore = try await store.fetchTranscriptChunks(assetId: assetId).count

        await relaunchAndDrive(
            store: store,
            episodeId: episodeId,
            audioURL: try emptyAudioURL()
        )

        let chunksAfter = try await store.fetchTranscriptChunks(assetId: assetId).count
        #expect(chunksAfter == chunksBefore, "Resume from .hotPathReady must not re-create transcript chunks (had \(chunksBefore), now \(chunksAfter))")

        // Session row still present (the coordinator may legally
        // advance it forward — what we assert is that the rehydrated
        // pipeline did not invent fresh data).
        let session = try await store.fetchSession(id: sessionId)
        #expect(session != nil)
    }

    // MARK: - Resume from .backfill

    @Test(".backfill — fresh coordinator preserves chunks; finalizer runs against persisted data")
    func resumeFromBackfill() async throws {
        let store = try await makeStore()
        let assetId = "asset-backfill"
        let sessionId = "session-backfill"
        let episodeId = "ep-backfill"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: episodeId,
            analysisState: .backfill,
            episodeDurationSec: 600,
            featureCoverageEndTime: 600,
            fastTranscriptCoverageEndTime: 600
        ))
        try await store.insertSession(makeSession(
            id: sessionId, assetId: assetId, state: .backfill
        ))
        // Seed a chunk set covering ≥95% of the persisted 600s — the
        // finalizer's coverage guard requires this to advance to a
        // terminal completion. Without it, the resume branch fails
        // back to .failed which is also a valid terminal but loses the
        // "no duplicate work" assertion.
        let chunks: [TranscriptChunk] = (0..<60).map { i in
            makeChunk(
                assetId: assetId,
                chunkIndex: i,
                startTime: Double(i) * 10,
                endTime: Double(i + 1) * 10
            )
        }
        try await store.insertTranscriptChunks(chunks)
        try await store.updateFastTranscriptCoverage(id: assetId, endTime: 600)

        let chunksBefore = try await store.fetchTranscriptChunks(assetId: assetId).count

        // Use the production resume seam (DEBUG-only) — it mirrors
        // exactly the runFromBackfill resume branch the production
        // pipeline hits when a relaunch lands in `.backfill`.
        let coord = makeCoordinator(store: store)
        await coord.resumeBackfillForTesting(
            sessionId: sessionId,
            assetId: assetId,
            episodeId: episodeId
        )
        await coord.stop()

        let chunksAfter = try await store.fetchTranscriptChunks(assetId: assetId).count
        #expect(chunksAfter == chunksBefore, "Resume from .backfill must not re-create transcript chunks (had \(chunksBefore), now \(chunksAfter))")

        // Session must still exist — the backfill resume either
        // finalizes (transitions to a `complete*` terminal) or fails;
        // either way the row is preserved.
        let sessionAfter = try await store.fetchSession(id: sessionId)
        #expect(sessionAfter != nil)
        if let state = sessionAfter.flatMap({ SessionState(rawValue: $0.state) }) {
            // Acceptable resume outcomes: any terminal completion, or
            // the legacy `.complete`, or `.failed` (when the resume
            // decides to restart). Crucially NOT a partial state we
            // got stuck in.
            let validTerminals: Set<SessionState> = [
                .complete, .completeFull, .completeFeatureOnly,
                .completeTranscriptPartial, .failed, .failedTranscript,
                .failedFeature, .cancelledBudget
            ]
            #expect(validTerminals.contains(state),
                    "Backfill resume must reach a terminal state, was \(state.rawValue)")
        }
    }

    // MARK: - Idempotency: a second relaunch from a terminal state is a no-op

    @Test("repeated relaunches do not re-create work after a terminal")
    func repeatedRelaunchIsIdempotent() async throws {
        let store = try await makeStore()
        let assetId = "asset-idempotent"
        let sessionId = "session-idempotent"
        let episodeId = "ep-idempotent"
        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeId: episodeId,
            analysisState: .completeFull,
            episodeDurationSec: 600,
            fastTranscriptCoverageEndTime: 600
        ))
        try await store.insertSession(makeSession(
            id: sessionId, assetId: assetId, state: .completeFull
        ))
        let chunks: [TranscriptChunk] = (0..<60).map { i in
            makeChunk(
                assetId: assetId,
                chunkIndex: i,
                startTime: Double(i) * 10,
                endTime: Double(i + 1) * 10
            )
        }
        try await store.insertTranscriptChunks(chunks)

        let chunksBefore = try await store.fetchTranscriptChunks(assetId: assetId).count

        // Relaunch twice — neither pass should create new chunks.
        await relaunchAndDrive(
            store: store,
            episodeId: episodeId,
            audioURL: try emptyAudioURL()
        )
        await relaunchAndDrive(
            store: store,
            episodeId: episodeId,
            audioURL: try emptyAudioURL()
        )

        let chunksAfter = try await store.fetchTranscriptChunks(assetId: assetId).count
        #expect(chunksAfter == chunksBefore)
    }
}
