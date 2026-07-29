// TranscriptEngineAppendWaitCancellationTests.swift
// playhead-8m2w Site B: `TranscriptEngineService.waitForMoreShards()`.
//
// The drain loop checks `Task.isCancelled` one line before it parks on the
// append waiter, so a cancel landing in that window parks anyway — and a
// parked `withCheckedContinuation` cannot be unwound by cancellation
// (playhead-xc6b), so the loop stays suspended until some later
// `startTranscription` / `stop` / `stopTranscription` happens to call
// `resumeAllAppendWaiters()`.
//
// WHAT THAT COSTS — stated at its real size, because an inflated claim is
// worth less than an accurate one. The parked loop never emits `.completed`,
// so `AnalysisJobRunner` waits out its full 300 s ceiling on that event
// (`Task.sleep(for: .seconds(300))` racing the stream, AnalysisJobRunner.swift)
// and returns `.failed("transcription:zeroCoverage")`. So the job is lost and
// `AnalysisWorkScheduler`'s `currentRunningTask` is pinned for five minutes —
// NOT, as an earlier version of this comment claimed, forever: the runner's own
// timeout clears it, and the next `startTranscription` unparks the stale loop.
// A five-minute stall plus a zero-coverage failure per occurrence is the honest
// blast radius, and it is quite enough to fix.
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
//
// What these tests pin is the cancellation HANDLER on the park. They cannot
// distinguish it from the `Task.isCancelled` check inside the continuation
// body: that check guards a window a few instructions wide, between the drain
// loop's pre-park cancellation check and the park itself, and no test seam can
// place a cancel inside it. The handler already covers that ordering on its
// own, so the in-body check is defence in depth rather than a second
// independently-testable guarantee — see the note on `waitForMoreShards()`.
//
// The post-cancel assertion is on `appendWaiterCountForTesting`, deliberately
// NOT on `isActive`. `isActive` is `activeTask != nil && !activeTask.isCancelled`,
// so `cancel()` alone makes it false whether or not the loop ever woke up — a
// tautology in these tests, not an observation. The waiter count is a real one:
// it is non-zero at the park and only `resumeAllAppendWaiters()`, which drains
// the list before it resumes, brings it back to zero. A regression that resumed
// a waiter without removing it — the double-resume shape — would leave it at 1.

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
        #expect(await engine.appendWaiterCountForTesting == 0)
    }

    @Test("A cancel after the loop RE-parks is not a park-forever either",
          .timeLimit(.minutes(1)))
    func cancelAfterAReparkStillExits() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "asset-repark"))
        let speech = SpeechService(
            recognizer: StubSpeechRecognizer(),
            serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let snapshot = PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-repark")],
            analysisAssetId: "asset-repark",
            snapshot: snapshot
        )
        let task = try #require(await engine.activeTaskForTesting())
        await awaitParked(on: engine)

        // Wake the loop with real work so it drains and parks a SECOND time.
        // `appendShards` drains `appendWaiters` synchronously, so the count is
        // back to zero when it returns and the next non-zero reading is
        // unambiguously the re-park.
        //
        // This is the property the first test cannot see: the cancellation
        // handler is installed per park and `withTaskCancellationHandler`
        // removes its record when the park returns, so a fix that only armed
        // once would strand the loop here.
        await engine.appendShards(
            [makeShard(id: 1, episodeID: "ep-asset-repark", startTime: 30)],
            analysisAssetId: "asset-repark",
            snapshot: snapshot
        )
        await awaitParked(on: engine)

        task.cancel()
        await task.value
        #expect(await engine.appendWaiterCountForTesting == 0)
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
        // re-checks cancellation after the park and does not complete the
        // asset.
        //
        // playhead-ngev: it is no longer SILENT — it emits
        // `.failed(.cancelled, interrupted)`, which is what turns a 300 s
        // runner stall into an instantly named row
        // (`cancellationIsReportedInsteadOfReturningInSilence` pins that). The
        // claim under test here is narrower and unchanged: whatever else it
        // says, a cancelled loop must never claim COMPLETION, because that is
        // the event subscribers act on.
        //
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
