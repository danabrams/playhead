// TranscriptEngineStopTranscriptionTests.swift
// playhead-5uvz.5 (Gap-6): pin the contract that
// `TranscriptEngineService.stopTranscription(analysisAssetId:)` cleanly
// halts an in-flight transcription session and gates any subsequent
// writes/events for that asset.
//
// Before the fix: if the 5-minute transcription timeout fired in
// `AnalysisJobRunner.run` ahead of the engine's `.completed` event, the
// runner returned `.failed("transcription:zeroCoverage")` while
// `TranscriptEngineService` kept running. Late shards persisted
// `transcript_chunks` rows and advanced
// `analysis_assets.fastTranscriptCoverageEndTime` against an asset
// whose owning scheduler had already moved on — confusing
// coverage-guard recovery and the partial-coverage gate.
//
// These tests pin the post-fix contract:
//   1. After `stopTranscription(analysisAssetId:)`, no further
//      `transcript_chunks` rows are inserted for that asset, even if
//      the recognizer was mid-flight when the stop landed.
//   2. After stop, `analysis_assets.fastTranscriptCoverageEndTime` does
//      not advance — late `updateFastTranscriptCoverage` calls are
//      gated.
//   3. After stop, no `.completed` event is emitted for that asset.
//   4. After stop, late `appendShards(_:analysisAssetId:snapshot:)`
//      calls for the stopped asset are dropped.
//   5. A subsequent `startTranscription(...)` for the same asset
//      rescinds the gate (re-runs are allowed).
//   6. Stops for one asset do not affect concurrent transcription
//      sessions on a different asset — the gate is per-asset.

import Foundation
import os
import Testing
@testable import Playhead

// MARK: - StallingRecognizer

/// SpeechRecognizer test double that suspends inside `transcribe(...)`
/// until `release()` is called. Lets a test drive the
/// "engine stalled mid-shard, runner timed out" race deterministically:
/// the loop is parked inside the await, the test calls
/// `stopTranscription`, and only then does the recognizer return — so
/// the post-await persistence path runs in a "stopped" world.
private actor StallingRecognizerCore {
    private var stallContinuations: [CheckedContinuation<Void, Never>] = []
    private(set) var transcribeStartedShardIds: [Int] = []
    /// When true, `transcribe` returns immediately without parking the
    /// caller. Used to verify post-stop appends never reach the
    /// recognizer at all.
    private var bypass: Bool = false

    func enterTranscribe(shardId: Int) async {
        transcribeStartedShardIds.append(shardId)
        if bypass { return }
        await withCheckedContinuation { continuation in
            stallContinuations.append(continuation)
        }
    }

    /// Release every parked `transcribe` caller. Idempotent.
    func release() {
        let waiters = stallContinuations
        stallContinuations = []
        for continuation in waiters {
            continuation.resume()
        }
    }

    /// Stop blocking future `transcribe` calls.
    func disableStall() {
        bypass = true
        release()
    }

    var stalledCount: Int {
        stallContinuations.count
    }

    var startedCount: Int {
        transcribeStartedShardIds.count
    }
}

private final class StallingRecognizer: SpeechRecognizer, @unchecked Sendable {
    let core = StallingRecognizerCore()
    private let _loaded = OSAllocatedUnfairLock(initialState: false)

    func loadModel() async throws {
        _loaded.withLock { $0 = true }
    }

    func unloadModel() async {
        _loaded.withLock { $0 = false }
    }

    func isModelLoaded() async -> Bool {
        _loaded.withLock { $0 }
    }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard _loaded.withLock({ $0 }) else { throw TranscriptEngineError.modelNotLoaded }
        await core.enterTranscribe(shardId: shard.id)

        // After the test releases us, return a single segment that
        // would (without the stop gate) cause a transcript_chunks
        // insert + coverage advance + .chunksPersisted emission.
        let word = TranscriptWord(
            text: "post-stop-shard\(shard.id)",
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            confidence: 0.9
        )
        return [TranscriptSegment(
            id: shard.id,
            words: [word],
            text: "post-stop-shard\(shard.id)",
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            avgConfidence: 0.9,
            passType: .fast
        )]
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(isSpeech: true, speechProbability: 1.0,
                   startTime: shard.startTime,
                   endTime: shard.startTime + shard.duration)]
    }
}

// MARK: - Helpers

private func makeStopTestAsset(id: String) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: "ep-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///test/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "queued",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

/// Spin briefly until `condition` returns true or the deadline elapses.
/// Used to wait for the engine's transcription Task to make a
/// recognizer call without sleeping a fixed wall-clock interval.
private func waitUntil(
    _ condition: () async -> Bool,
    timeout: Duration = .seconds(15),
    pollInterval: Duration = .milliseconds(10)
) async -> Bool {
    let deadline = ContinuousClock.now.advanced(by: timeout)
    while ContinuousClock.now < deadline {
        if await condition() { return true }
        try? await Task.sleep(for: pollInterval)
    }
    return false
}

/// Gather events from the stream until either `.completed` fires or
/// `duration` elapses. Returns the asset id of the completion event,
/// or nil on timeout.
private func awaitCompletion(
    on events: AsyncStream<TranscriptEngineEvent>,
    within duration: Duration
) async -> String? {
    await withTaskGroup(of: String?.self) { group in
        group.addTask {
            for await event in events {
                if case .completed(let assetId) = event {
                    return assetId
                }
            }
            return nil
        }
        group.addTask {
            try? await Task.sleep(for: duration)
            return nil
        }
        let first = await group.next() ?? nil
        group.cancelAll()
        return first
    }
}

/// Collect every `.chunksPersisted` event that arrives within
/// `duration`. Returns the asset ids that were persisted (one entry per
/// event, repeats allowed).
private func collectChunkEventAssetIds(
    on events: AsyncStream<TranscriptEngineEvent>,
    within duration: Duration
) async -> [String] {
    await withTaskGroup(of: [String].self) { group in
        group.addTask {
            var ids: [String] = []
            for await event in events {
                if case .chunksPersisted(let assetId, _) = event {
                    ids.append(assetId)
                }
            }
            return ids
        }
        group.addTask {
            try? await Task.sleep(for: duration)
            return []
        }
        let first = await group.next() ?? []
        group.cancelAll()
        return first
    }
}

// MARK: - Tests

/// Run serialized: every test in this suite stalls inside the
/// process-wide `SpeechRecognitionRequestGate` (`SpeechService.requestGate`),
/// which is shared across all SpeechService instances in the test
/// process. Two parallel stall-then-stop tests would deadlock the
/// second one until the first releases. Serialization keeps the suite
/// runtime bounded by the sum (not the max) of individual test
/// timeouts.
@Suite("TranscriptEngine – stopTranscription (playhead-5uvz.5)", .serialized)
struct TranscriptEngineStopTranscriptionTests {

    /// The headline contract: a stop landing while `transcribe` is
    /// suspended in the recognizer must prevent the post-await
    /// persistence path from inserting any `transcript_chunks` rows or
    /// advancing coverage for the stopped asset.
    @Test("Stop during stalled transcribe: no chunks persisted, no coverage advance, no .completed",
          .timeLimit(.minutes(1)))
    func stopDuringStalledTranscribeWritesNothing() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeStopTestAsset(id: "asset-stalled"))

        let recognizer = StallingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        // Start transcription; the recognizer parks inside `transcribe`,
        // so the engine's loop is suspended at the await — it cannot
        // make progress until the test releases the recognizer.
        await engine.startTranscription(
            shards: [
                makeShard(id: 0, episodeID: "ep-asset-stalled", startTime: 0, duration: 30),
                makeShard(id: 1, episodeID: "ep-asset-stalled", startTime: 30, duration: 30),
            ],
            analysisAssetId: "asset-stalled",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        // Tell the engine no more shards are coming so the only thing
        // gating `.completed` is the stalled recognizer (and, after we
        // call stopTranscription, the gate).
        await engine.finishAppending(analysisAssetId: "asset-stalled")

        // Wait for the engine to actually park inside `transcribe`.
        // Without this guard the test could race the engine's Task
        // scheduling and call stopTranscription before the loop has
        // even reached its first await.
        let parked = await waitUntil { await recognizer.core.startedCount >= 1 }
        #expect(parked, "Recognizer should have entered transcribe before stop")

        // Mimic the AnalysisJobRunner timeout-branch behavior.
        await engine.stopTranscription(analysisAssetId: "asset-stalled")

        // Now release the recognizer. The engine's Task resumes and
        // would (without the stop gate) run through the persistence
        // path: insertTranscriptChunks → emitEvent(.chunksPersisted) →
        // updateFastTranscriptCoverage → eventually emit `.completed`.
        // Every one of those writes must be dropped.
        await recognizer.core.disableStall()

        // Give the engine generous time to do its (gated) work.
        let completedAssetId = await awaitCompletion(on: events, within: .seconds(2))
        #expect(completedAssetId == nil,
                "Stopped engine must not emit .completed for asset-stalled")

        // No transcript_chunks rows for the stopped asset.
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-stalled")
        #expect(chunks.isEmpty,
                "No chunks should be persisted for the stopped asset (got \(chunks.count))")

        // Coverage watermark must not have advanced.
        let asset = try await store.fetchAsset(id: "asset-stalled")
        #expect(asset?.fastTranscriptCoverageEndTime == nil,
                "fastTranscriptCoverageEndTime must remain nil for the stopped asset")
    }

    /// `.chunksPersisted` events for a stopped asset must be dropped at
    /// the emit gate. This pins the second half of the bead's contract:
    /// even if a write somehow slipped through (it shouldn't, per the
    /// previous test), the event-stream subscriber on the runner side
    /// must not be told that progress was made.
    @Test("Stop drops .chunksPersisted events for the stopped asset",
          .timeLimit(.minutes(1)))
    func stopDropsChunksPersistedEvents() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeStopTestAsset(id: "asset-evgated"))

        let recognizer = StallingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-evgated", startTime: 0, duration: 30)],
            analysisAssetId: "asset-evgated",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-evgated")

        _ = await waitUntil { await recognizer.core.startedCount >= 1 }
        await engine.stopTranscription(analysisAssetId: "asset-evgated")
        await recognizer.core.disableStall()

        let chunkAssetIds = await collectChunkEventAssetIds(on: events, within: .seconds(1))
        #expect(!chunkAssetIds.contains("asset-evgated"),
                "Stopped asset must not emit .chunksPersisted (got \(chunkAssetIds))")
    }

    /// Late `appendShards(_:analysisAssetId:snapshot:)` for a stopped
    /// asset must be dropped at the entry gate — without this, a
    /// streaming producer that races the runner's stop would silently
    /// re-arm the session.
    @Test("appendShards for stopped asset is dropped at the gate",
          .timeLimit(.minutes(1)))
    func appendShardsForStoppedAssetIsDropped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeStopTestAsset(id: "asset-late-append"))

        let recognizer = StallingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-late-append", startTime: 0, duration: 30)],
            analysisAssetId: "asset-late-append",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-late-append")
        _ = await waitUntil { await recognizer.core.startedCount >= 1 }

        // Snapshot the recognizer call count before the stop + append.
        let beforeAppend = await recognizer.core.startedCount

        await engine.stopTranscription(analysisAssetId: "asset-late-append")

        // Late append simulating a streaming decoder that hadn't
        // finished when the runner timed out.
        await engine.appendShards(
            [makeShard(id: 99, episodeID: "ep-asset-late-append", startTime: 300, duration: 30)],
            analysisAssetId: "asset-late-append",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await recognizer.core.disableStall()

        // Give any (unwanted) post-stop work time to surface.
        try? await Task.sleep(for: .milliseconds(500))

        let afterAppend = await recognizer.core.startedCount
        #expect(afterAppend == beforeAppend,
                "Late-appended shard 99 must not reach the recognizer (started=\(afterAppend), expected=\(beforeAppend))")

        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-late-append")
        #expect(chunks.isEmpty,
                "No chunks should be persisted after stop, including for late appends")
    }

    /// A fresh `startTranscription(...)` for the same asset rescinds
    /// the stop gate — re-runs (e.g. operator-driven retry, or a fresh
    /// scheduler dispatch after orphan recovery) must not be silently
    /// suppressed.
    @Test("startTranscription after stop clears the gate and runs to completion",
          .timeLimit(.minutes(1)))
    func startAfterStopClearsGate() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeStopTestAsset(id: "asset-rerun"))

        let recognizer = StallingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)

        // First pass — stalled, then stopped.
        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-rerun", startTime: 0, duration: 30)],
            analysisAssetId: "asset-rerun",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-rerun")
        _ = await waitUntil { await recognizer.core.startedCount >= 1 }
        await engine.stopTranscription(analysisAssetId: "asset-rerun")
        await recognizer.core.disableStall()

        // Second pass — fresh start, no stall this time. Must run to
        // completion, persist a chunk, advance coverage.
        let secondEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-rerun", startTime: 0, duration: 30)],
            analysisAssetId: "asset-rerun",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-rerun")

        let completedId = await awaitCompletion(on: secondEvents, within: .seconds(5))
        #expect(completedId == "asset-rerun",
                "Re-run must emit .completed after the gate is cleared")

        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-rerun")
        #expect(!chunks.isEmpty,
                "Re-run must persist at least one chunk")
        let asset = try await store.fetchAsset(id: "asset-rerun")
        #expect((asset?.fastTranscriptCoverageEndTime ?? 0) > 0,
                "Re-run must advance coverage")
    }

    /// Stops are per-asset: stopping one asset must not affect another
    /// asset's transcription. (The engine is single-active-asset by
    /// design, but stale gates from previous sessions cannot leak.)
    @Test("Stop one asset, then run a different asset to completion",
          .timeLimit(.minutes(1)))
    func stopIsPerAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeStopTestAsset(id: "asset-stopped"))
        try await store.insertAsset(makeStopTestAsset(id: "asset-fresh"))

        let recognizer = StallingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)

        // Stall + stop the first asset.
        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-stopped", startTime: 0, duration: 30)],
            analysisAssetId: "asset-stopped",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-stopped")
        _ = await waitUntil { await recognizer.core.startedCount >= 1 }
        await engine.stopTranscription(analysisAssetId: "asset-stopped")
        await recognizer.core.disableStall()

        // Now run a totally different asset. Must complete normally.
        let freshEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-fresh", startTime: 0, duration: 30)],
            analysisAssetId: "asset-fresh",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        await engine.finishAppending(analysisAssetId: "asset-fresh")

        let completedId = await awaitCompletion(on: freshEvents, within: .seconds(5))
        #expect(completedId == "asset-fresh",
                "Fresh asset must complete despite a prior stopTranscription on a different asset")

        // The stopped asset must still have no chunks; the fresh asset
        // should have at least one.
        let stoppedChunks = try await store.fetchTranscriptChunks(assetId: "asset-stopped")
        let freshChunks = try await store.fetchTranscriptChunks(assetId: "asset-fresh")
        #expect(stoppedChunks.isEmpty,
                "Stopped asset must remain empty even after a fresh asset's session ran")
        #expect(!freshChunks.isEmpty,
                "Fresh asset must have its chunks persisted")
    }

    /// Stale stop call (asset that is not active) must be a no-op for
    /// the active session — but must still gate any future writes for
    /// the named asset, in case an in-flight task is racing against an
    /// asset switch.
    @Test("Stop on an inactive asset gates writes without disturbing the active session",
          .timeLimit(.minutes(1)))
    func stopOnInactiveAssetIsGateOnlyNoOp() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeStopTestAsset(id: "asset-active"))
        try await store.insertAsset(makeStopTestAsset(id: "asset-inactive"))

        let recognizer = StallingRecognizer()
        let speech = SpeechService(recognizer: recognizer)
        try await speech.loadFastModel()

        let engine = TranscriptEngineService(speechService: speech, store: store)

        // Start a session for asset-active, but disable the stall so it
        // can run to completion later.
        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-active", startTime: 0, duration: 30)],
            analysisAssetId: "asset-active",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

        // Stop a *different* asset. The active session must not be
        // affected — but a future appendShards for asset-inactive must
        // be dropped.
        await engine.stopTranscription(analysisAssetId: "asset-inactive")

        await engine.finishAppending(analysisAssetId: "asset-active")
        await recognizer.core.disableStall()

        let events = await engine.events()
        let completedId = await awaitCompletion(on: events, within: .seconds(5))
        #expect(completedId == "asset-active",
                "Active session must complete despite a stale stop on a different asset")

        // Verify the gate on asset-inactive is still in force: a late
        // append for it should be dropped.
        let beforeAppendCount = await recognizer.core.startedCount
        await engine.appendShards(
            [makeShard(id: 99, episodeID: "ep-asset-inactive", startTime: 300, duration: 30)],
            analysisAssetId: "asset-inactive",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        try? await Task.sleep(for: .milliseconds(200))
        let afterAppendCount = await recognizer.core.startedCount
        #expect(afterAppendCount == beforeAppendCount,
                "Late append to gated inactive asset must not reach recognizer (before=\(beforeAppendCount), after=\(afterAppendCount))")

        let inactiveChunks = try await store.fetchTranscriptChunks(assetId: "asset-inactive")
        #expect(inactiveChunks.isEmpty,
                "Stale-stop'd inactive asset must have no chunks")
    }
}
