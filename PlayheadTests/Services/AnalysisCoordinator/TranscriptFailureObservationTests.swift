// TranscriptFailureObservationTests.swift
// playhead-8ysk, review round 2.
//
// WHAT ROUND 1 LEFT UNMEASURED. `AnalysisCoordinator`'s transcript-event
// observer grew a `.failed` arm whose whole job is a NEGATIVE: on a run that
// produced nothing, do NOT finalize the backfill. Round 1 aimed a mutation at
// it, named a suite that does not exist, and — correctly — declined to claim
// the arm had survived. Nothing else covered it either: the observer loop runs
// inside a detached `Task` started from the private `runPipeline`, and the
// coordinator holds a CONCRETE `TranscriptEngineService`, so no test can hand
// it an engine that emits `.failed` on demand.
//
// So the arm was asserted by nothing but the compiler's exhaustiveness check,
// which would have been just as happy with a body that called
// `finalizeBackfill` — i.e. with the pre-bead behaviour the arm exists to end,
// where a total transcription failure finalized a backfill over an empty
// transcript and stamped a terminal verdict on it.
//
// THE POSITIVE CONTROL IS THE POINT. This is the defect class round 1 found in
// the shard cache: every test asserted the DISCARD and none proved the SERVE,
// so a build that never used the cache at all passed. "`.failed` does not
// finalize" has exactly the same shape — it passes trivially against a
// coordinator that never finalizes anything, or against a seam wired to a
// session the finalizer cannot move. `completedFinalizesTheSession` runs the
// identical fixture through `.completed` and requires the session to MOVE, so
// the failure assertion is only meaningful because its sibling proves the
// machinery works.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-8ysk — a .failed transcript event must not finalize a backfill", .serialized)
struct TranscriptFailureObservationTests {

    // MARK: - Fixtures

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "TranscriptFailureObservationTests")
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

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            // Full coverage on both axes, so `.completed` reaches a terminal
            // verdict and the control is unambiguous.
            featureCoverageEndTime: 600,
            fastTranscriptCoverageEndTime: 600,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.backfill.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 600
        )
    }

    private func makeSession(id: String, assetId: String) -> AnalysisSession {
        AnalysisSession(
            id: id,
            analysisAssetId: assetId,
            state: SessionState.backfill.rawValue,
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        )
    }

    private func makeChunk(assetId: String, index: Int, start: Double, end: Double) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(index)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(assetId)-\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "x",
            normalizedText: "x",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    private func makeShards(episodeId: String, totalSeconds: Double) -> [AnalysisShard] {
        let half = totalSeconds / 2.0
        return [
            AnalysisShard(id: 0, episodeID: episodeId, startTime: 0, duration: half, samples: []),
            AnalysisShard(id: 1, episodeID: episodeId, startTime: half, duration: half, samples: [])
        ]
    }

    /// Seed one asset + session + full-coverage transcript and hand back
    /// everything the seam needs. Identical for every test in the suite so a
    /// difference in outcome can only come from the EVENT.
    private func seed(
        idSuffix: String
    ) async throws -> (store: AnalysisStore, coordinator: AnalysisCoordinator,
                       assetId: String, sessionId: String, episodeId: String,
                       shards: [AnalysisShard]) {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        let assetId = "asset-\(idSuffix)"
        let sessionId = "session-\(idSuffix)"
        let episodeId = "ep-\(assetId)"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertSession(makeSession(id: sessionId, assetId: assetId))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, index: 0, start: 0, end: 300),
            makeChunk(assetId: assetId, index: 1, start: 300, end: 600)
        ])
        return (store, coordinator, assetId, sessionId, episodeId,
                makeShards(episodeId: episodeId, totalSeconds: 600))
    }

    // MARK: - The positive control

    /// PROVE THE MACHINERY MOVES. Without this, every other assertion in the
    /// file is satisfied by a seam that does nothing.
    @Test("control: .completed for this asset finalizes the session and stops observing")
    func completedFinalizesTheSession() async throws {
        let fixture = try await seed(idSuffix: "completed-control")

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .completed(analysisAssetId: fixture.assetId),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )

        #expect(step == .stopObserving, "`.completed` is terminal for the observer")

        let session = try await fixture.store.fetchSession(id: fixture.sessionId)
        #expect(
            session?.state != SessionState.backfill.rawValue,
            """
            CONTROL FAILED: `.completed` did not move the session off `.backfill`. \
            Every "does not finalize" assertion in this suite is vacuous until \
            this one passes — fix the fixture, do not relax the siblings. \
            Got: \(session?.state ?? "nil")
            """
        )
    }

    // MARK: - The behaviour under test

    /// The arm round 1 could not measure. A `.failed` carrying a real
    /// `TranscriptFailureReason` must leave the session exactly where it was:
    /// the transcript is empty, and finalizing over it is how a run that
    /// produced nothing used to acquire a terminal verdict.
    @Test(".failed for this asset stops observing and leaves the session unfinalized")
    func failedDoesNotFinalize() async throws {
        let fixture = try await seed(idSuffix: "failed-terminal")
        let before = try await fixture.store.fetchSession(id: fixture.sessionId)

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .failed(
                analysisAssetId: fixture.assetId,
                reason: TranscriptFailureReason(
                    failureClass: .silentShard, code: nil, failedShardCount: 12
                )
            ),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )

        #expect(step == .stopObserving, "a total failure is terminal for the observer")

        let after = try await fixture.store.fetchSession(id: fixture.sessionId)
        #expect(
            after?.state == SessionState.backfill.rawValue,
            """
            `.failed` finalized the backfill. The whole point of the arm is that \
            a run which produced nothing must NOT acquire a terminal verdict. \
            Got: \(after?.state ?? "nil")
            """
        )
        #expect(after?.state == before?.state)
        #expect(after?.updatedAt == before?.updatedAt, "the session row was not touched at all")
    }

    /// NOT FINALIZING IS ONLY HALF THE ARM'S JOB (review r3).
    ///
    /// `transcriptEventTask` is not bookkeeping — it is the variable
    /// `runFromBackfill` branches on. Nil means "nothing is transcribing, so
    /// consult `resumeBackfillDecision` and either finalize or throw for a full
    /// restart"; non-nil means "transcription is live, just wait". On the
    /// `.completed` side `finalizeBackfill`'s `defer` clears it. The `.failed`
    /// arm returned `.stopObserving` while leaving a handle to a task that had
    /// already returned, so a session resuming at `.backfill` in this process —
    /// a `.waitingForBackfill` thermal resume, or another playback event for
    /// the episode — would log "Backfill waiting for transcript completion" and
    /// park forever on a transcription that had already failed. The recovery
    /// path was disabled for exactly the runs that need it.
    @Test(".failed releases the observer handle so backfill can recover")
    func failedReleasesTheObserverHandle() async throws {
        let fixture = try await seed(idSuffix: "failed-releases-handle")
        await fixture.coordinator.installTranscriptObserverForTesting()
        try #require(
            await fixture.coordinator.hasTranscriptObserverForTesting,
            "the handle must be installed, or this test proves nothing"
        )

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .failed(
                analysisAssetId: fixture.assetId,
                reason: TranscriptFailureReason(
                    failureClass: .modelNotLoaded, code: nil, failedShardCount: 4
                )
            ),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )
        #expect(step == .stopObserving)

        #expect(
            await fixture.coordinator.hasTranscriptObserverForTesting == false,
            """
            `.failed` abandoned the observer but kept its handle. \
            `runFromBackfill` reads that handle as "transcription is still \
            running" and parks instead of recovering, so this session can never \
            reach `resumeBackfillDecision` again in this process
            """
        )
    }

    /// The counterweight: releasing the handle must be a property of the
    /// TERMINAL arm, not of every event. A `.chunksPersisted` mid-run that
    /// cleared the handle would send `runFromBackfill` down the resume branch
    /// while transcription was still live — finalizing or restarting on top of
    /// a running engine.
    @Test("control: a non-terminal event leaves the observer handle installed")
    func nonTerminalEventKeepsTheObserverHandle() async throws {
        let fixture = try await seed(idSuffix: "keeps-handle")
        await fixture.coordinator.installTranscriptObserverForTesting()

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .chunksPersisted(analysisAssetId: fixture.assetId, chunks: []),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )
        #expect(step == .keepObserving)
        #expect(
            await fixture.coordinator.hasTranscriptObserverForTesting,
            "a mid-run event must not tear down the live observer"
        )
    }

    /// And a `.failed` for ANOTHER asset must not release THIS session's
    /// handle. The engine's event stream is shared, so a failure belonging to a
    /// superseded or unrelated asset is routine; clearing on it would disable
    /// the live session's recovery gate from the outside.
    @Test(".failed for a different asset leaves this session's handle alone")
    func failedForAnotherAssetKeepsTheHandle() async throws {
        let fixture = try await seed(idSuffix: "other-asset-handle")
        await fixture.coordinator.installTranscriptObserverForTesting()

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .failed(
                analysisAssetId: "some-other-asset",
                reason: TranscriptFailureReason(failureClass: .noShards)
            ),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )
        #expect(step == .keepObserving)
        #expect(
            await fixture.coordinator.hasTranscriptObserverForTesting,
            "another asset's failure must not disarm this session's observer"
        )
    }

    /// The `guard analysisAssetId == assetId else { return .keepObserving }`
    /// in the `.failed` arm. A failure belonging to a DIFFERENT asset — the
    /// engine's event stream is shared, so this is routine — must neither tear
    /// down this session's observer nor touch its row.
    @Test(".failed for a different asset is ignored and observation continues")
    func failedForAnotherAssetIsIgnored() async throws {
        let fixture = try await seed(idSuffix: "failed-other-asset")
        let before = try await fixture.store.fetchSession(id: fixture.sessionId)

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .failed(
                analysisAssetId: "asset-somebody-else",
                reason: TranscriptFailureReason(failureClass: .modelNotLoaded)
            ),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )

        #expect(
            step == .keepObserving,
            """
            A `.failed` for another asset tore down this session's observer. \
            The stream is shared across assets; stopping here strands this \
            session waiting for a `.completed` nobody will deliver.
            """
        )
        let after = try await fixture.store.fetchSession(id: fixture.sessionId)
        #expect(after?.state == before?.state)
        #expect(after?.updatedAt == before?.updatedAt)
    }

    /// The symmetric guard on the `.completed` arm, included because the
    /// extraction that made `.failed` measurable also rewrote this one.
    @Test("control: .completed for a different asset does not finalize this session")
    func completedForAnotherAssetIsIgnored() async throws {
        let fixture = try await seed(idSuffix: "completed-other-asset")
        let before = try await fixture.store.fetchSession(id: fixture.sessionId)

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .completed(analysisAssetId: "asset-somebody-else"),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )

        #expect(step == .keepObserving)
        let after = try await fixture.store.fetchSession(id: fixture.sessionId)
        #expect(after?.state == before?.state)
        #expect(after?.updatedAt == before?.updatedAt)
    }

    // MARK: - playhead-ngev: an interruption is not a terminal failure

    /// THE REGRESSION THE NEW EVENTS WOULD OTHERWISE CAUSE.
    ///
    /// The loop now reports the interruptions it used to return from in
    /// silence, and the dominant one is a scrub: `handleScrub` calls
    /// `startTranscription`, which cancels the running loop AND immediately
    /// spawns its replacement for the same asset. That replacement is the
    /// session this observer is watching, and it will emit its own
    /// `.completed`.
    ///
    /// Treating the predecessor's exit as terminal would stop observing and
    /// release the handle while the successor was still transcribing, so
    /// `finalizeBackfill` would never run for an episode that transcribed
    /// perfectly well — a brand-new way to lose a backfill, introduced by a
    /// diagnostics fix. Before this bead the arm saw NOTHING here, so keeping
    /// observing is also what preserves the shipping behaviour.
    @Test("an interrupted run keeps the session under observation")
    func interruptedFailureKeepsObserving() async throws {
        let fixture = try await seed(idSuffix: "interrupted-keeps")
        await fixture.coordinator.installTranscriptObserverForTesting()
        let before = try await fixture.store.fetchSession(id: fixture.sessionId)

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .failed(
                analysisAssetId: fixture.assetId,
                reason: TranscriptFailureReason(
                    failureClass: .cancelled, termination: .interrupted
                )
            ),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )

        #expect(
            step == .keepObserving,
            """
            a scrub tore down the observer for a session whose successor loop \
            is still transcribing. The successor's `.completed` now has nobody \
            listening, so the backfill is never finalized
            """
        )
        #expect(
            await fixture.coordinator.hasTranscriptObserverForTesting,
            "the handle belongs to the live session and must survive an interruption"
        )
        let after = try await fixture.store.fetchSession(id: fixture.sessionId)
        #expect(after?.state == before?.state)
        #expect(after?.updatedAt == before?.updatedAt, "the session row was not touched at all")
    }

    /// The counterweight: it is the TERMINATION that decides, not the class.
    /// An interrupted run can carry a real per-shard diagnosis — shards
    /// failing `model_not_loaded` and then a scrub — and branching on the
    /// class would tear the live session down for exactly those runs.
    @Test("an interruption carrying a real shard diagnosis still keeps observing")
    func interruptedWithDiagnosisKeepsObserving() async throws {
        let fixture = try await seed(idSuffix: "interrupted-with-class")
        await fixture.coordinator.installTranscriptObserverForTesting()

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .failed(
                analysisAssetId: fixture.assetId,
                reason: TranscriptFailureReason(
                    failureClass: .modelNotLoaded, code: nil, failedShardCount: 3,
                    termination: .interrupted
                )
            ),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )

        #expect(step == .keepObserving)
        #expect(await fixture.coordinator.hasTranscriptObserverForTesting)
    }

    /// And the control that stops the guard from swallowing everything: a run
    /// that reached its own conclusion is still terminal, still does not
    /// finalize, and still releases the handle. `failedDoesNotFinalize` and
    /// `failedReleasesTheObserverHandle` above cover the same ground for the
    /// default termination; this states it explicitly.
    @Test("control: a run that reached its own conclusion is still terminal")
    func ranToConclusionFailureIsStillTerminal() async throws {
        let fixture = try await seed(idSuffix: "conclusion-terminal")
        await fixture.coordinator.installTranscriptObserverForTesting()

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .failed(
                analysisAssetId: fixture.assetId,
                reason: TranscriptFailureReason(
                    failureClass: .vadFailed, code: nil, failedShardCount: 8,
                    termination: .ranToConclusion
                )
            ),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )

        #expect(step == .stopObserving)
        #expect(
            await fixture.coordinator.hasTranscriptObserverForTesting == false,
            "a total failure must still release the handle, or backfill cannot recover"
        )
    }

    /// `.chunksPersisted` keeps the loop running. Pinned because the
    /// extraction turned three `continue`/`return` statements into returned
    /// values, and a step returned from the wrong arm would silently end
    /// observation mid-stream — the failure mode would be a session that
    /// stalls forever rather than a test that fails.
    @Test("control: .chunksPersisted keeps observing")
    func chunksPersistedKeepsObserving() async throws {
        let fixture = try await seed(idSuffix: "chunks-continue")

        let step = await fixture.coordinator.handleTranscriptEventForTesting(
            .chunksPersisted(analysisAssetId: fixture.assetId, chunks: []),
            sessionId: fixture.sessionId,
            assetId: fixture.assetId,
            episodeId: fixture.episodeId,
            activeShards: fixture.shards
        )

        #expect(step == .keepObserving, "chunk delivery is not terminal")
        let after = try await fixture.store.fetchSession(id: fixture.sessionId)
        #expect(after?.state == SessionState.backfill.rawValue)
    }
}
