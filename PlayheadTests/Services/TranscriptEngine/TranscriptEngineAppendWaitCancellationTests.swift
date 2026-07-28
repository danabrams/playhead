// TranscriptEngineAppendWaitCancellationTests.swift
// playhead-8m2w Site B: `TranscriptEngineService.waitForMoreShards()`.
//
// The drain loop checks `Task.isCancelled` one line before it parks on the
// append waiter, so a cancel landing in that window parks anyway — and a
// parked `withCheckedContinuation` cannot be unwound by cancellation
// (playhead-xc6b), so the loop stays suspended for the lifetime of the
// process. Nothing else in the pipeline advances behind it:
// `AnalysisWorkScheduler` awaits `runTask.value`, so a stuck transcription
// loop means `currentRunningTask` never clears and no further analysis job
// starts.
//
// Today every `activeTask?.cancel()` in the service is hand-paired with a
// `resumeAllAppendWaiters()`. These tests deliberately do NOT go through those
// paths: they cancel the loop task directly, which is what a producer that
// dies silently (a `shardConsumerTask` stalled inside
// `featureService.extractAndPersist`, so `finishAppending` is never reached)
// looks like from the engine's side.
//
// A regression here HANGS rather than mis-asserts: `await task.value` is the
// liveness claim, and the `.timeLimit` trait is the backstop.

import Foundation
import Testing
@testable import Playhead

/// Suspend until the drain loop is actually parked on the append waiter.
///
/// Without this the test races the loop and can cancel before it reaches the
/// park, exiting at the pre-park `Task.isCancelled` check — which passes even
/// against the bug. No deadline: the `.timeLimit` trait is the backstop, and a
/// loop that never parks is a genuine failure worth surfacing there.
private func awaitParked(on engine: TranscriptEngineService) async {
    while await engine.appendWaiterCountForTesting == 0 {
        try? await Task.sleep(for: .milliseconds(5))
    }
}

@Suite("TranscriptEngine – append-wait cancellation (playhead-8m2w)", .serialized)
struct TranscriptEngineAppendWaitCancellationTests {

    @Test("A cancel with no paired waiter wake still unparks the drain loop",
          .timeLimit(.minutes(1)))
    func unpairedCancelUnparksTheDrainLoop() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-unpaired-cancel"))
        let speech = SpeechService(
            recognizer: StubSpeechRecognizer(),
            serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)

        // One shard and no `finishAppending`: the loop transcribes the shard,
        // finds the backlog empty, and parks on `waitForMoreShards()` with
        // `inputClosed` still false. (An EMPTY shard list would not do: the
        // loop early-returns on it and never reaches the park at all.)
        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-unpaired-cancel")],
            analysisAssetId: "asset-unpaired-cancel",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )

        let task = try #require(await engine.activeTaskForTesting())
        await awaitParked(on: engine)

        // The producer dies. No `stop()`, no `stopTranscription(...)`, no
        // `resumeAllAppendWaiters()` — just cancellation.
        task.cancel()

        // Before playhead-8m2w this never returned.
        await task.value
        #expect(await engine.isActive == false)
    }

    @Test("Cancelling before the loop parks is still not a park-forever",
          .timeLimit(.minutes(1)))
    func cancelRacingTheParkStillExits() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-cancel-race"))
        let speech = SpeechService(
            recognizer: StubSpeechRecognizer(),
            serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-cancel-race")],
            analysisAssetId: "asset-cancel-race",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        let task = try #require(await engine.activeTaskForTesting())

        // Cancel immediately, without waiting for the loop to reach the park.
        // Whichever side wins — the pre-park `Task.isCancelled` check, or the
        // cancellation handler on the park itself — the loop must exit. This
        // is the check-then-park window the bead names, driven from the side
        // that used to lose it.
        task.cancel()
        await task.value
        #expect(await engine.isActive == false)
    }

    @Test("A cancelled drain loop emits no .completed", .timeLimit(.minutes(1)))
    func cancelledDrainLoopDoesNotCompleteTheAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-cancel-no-complete"))
        let speech = SpeechService(
            recognizer: StubSpeechRecognizer(),
            serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-cancel-no-complete")],
            analysisAssetId: "asset-cancel-no-complete",
            snapshot: PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)
        )
        let task = try #require(await engine.activeTaskForTesting())
        await awaitParked(on: engine)
        task.cancel()
        await task.value

        // Waking the waiter must not be mistaken for end-of-input. The loop
        // re-checks cancellation after the park and returns without emitting.
        // A negative check, so a bounded observation window is correct here:
        // too short can only produce a false PASS, never a flaky failure.
        let completed = await withTaskGroup(of: String?.self) { group in
            group.addTask {
                for await event in events {
                    if case .completed(let assetId) = event { return assetId }
                }
                return nil
            }
            group.addTask {
                try? await Task.sleep(for: .milliseconds(200))
                return nil
            }
            let first = await group.next() ?? nil
            group.cancelAll()
            return first
        }
        #expect(completed == nil)
    }
}
