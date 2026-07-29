// TranscriptEngineFailureEventTests.swift
// playhead-8ysk part 2 — the loop must stop reporting failure as success.
//
// `runTranscriptionLoop` had three exits that told the runner nothing:
//
//   * every shard fails -> fall through and emit `.completed`. The runner
//     treats that event as "coverage is durable" and queues downstream work;
//     the only trace is a coverage number of zero, by which point no `Error`
//     is in scope anywhere. This is the whole reason `asr_failed` names an
//     ABSENCE rather than a cause.
//   * `shards.isEmpty` -> `return` in silence. The runner then sat inside
//     `withTaskGroup` waiting for a `.completed` that could never arrive,
//     until its 300 s timeout fired.
//   * `speechService.isReady()` false -> `return` in silence, same 300 s.
//     This is the observable end of the swallowed launch-time
//     `loadFastModel()` failure, whose empty catch claims "the transcript
//     engine will surface failures when first used" — and until now it did
//     not.
//
// Every test here drives the REAL `TranscriptEngineService` loop and asserts
// on the events a real subscriber receives.

import Foundation
import os
import Testing

@testable import Playhead

// MARK: - Doubles

/// Loads, then fails every shard. The failure is a distinguishable class so
/// the test can prove the reason travelled, not merely that something failed.
private final class AlwaysFailingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let _loaded = OSAllocatedUnfairLock(initialState: false)
    func loadModel() async throws { _loaded.withLock { $0 = true } }
    func unloadModel() async { _loaded.withLock { $0 = false } }
    func isModelLoaded() async -> Bool { _loaded.withLock { $0 } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        throw TranscriptEngineError.vadFailed("no speech boundaries in shard \(shard.id)")
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(isSpeech: true, speechProbability: 1.0,
                   startTime: shard.startTime,
                   endTime: shard.startTime + shard.duration)]
    }
}

/// Succeeds on shard 0 and fails on everything after it — the PARTIAL case.
private final class FailAfterFirstRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let _loaded = OSAllocatedUnfairLock(initialState: false)
    func loadModel() async throws { _loaded.withLock { $0 = true } }
    func unloadModel() async { _loaded.withLock { $0 = false } }
    func isModelLoaded() async -> Bool { _loaded.withLock { $0 } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard shard.id == 0 else {
            throw TranscriptEngineError.vadFailed("shard \(shard.id)")
        }
        let word = TranscriptWord(
            text: "hello",
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            confidence: 0.9
        )
        return [TranscriptSegment(
            id: shard.id,
            words: [word],
            text: "hello",
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

/// Never loads, so `SpeechService.isReady()` is false.
private final class NeverReadyRecognizer: SpeechRecognizer, @unchecked Sendable {
    func loadModel() async throws {}
    func unloadModel() async {}
    func isModelLoaded() async -> Bool { false }
    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        Issue.record("transcribe must never be reached when the engine is not ready")
        return []
    }
    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] { [] }
}

// MARK: - Suite

@Suite("playhead-8ysk — a transcription that produces nothing reports .failed, not .completed")
struct TranscriptEngineFailureEventTests {

    private static func asset(id: String) -> AnalysisAsset {
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

    private static let snapshot = PlaybackSnapshot(
        playheadTime: 0, playbackRate: 1.0, isPlaying: true
    )

    /// Consume until the terminal event for `assetId`. Returns whether it was
    /// `.completed`, and the reason if it was `.failed`. Unbounded on purpose
    /// — the suite's `.timeLimit` is the backstop, and a bounded wait here
    /// could only turn a real hang into a confusing pass.
    private enum Terminal: Equatable {
        case completed
        case failed(TranscriptFailureReason)
    }

    private static func awaitTerminal(
        on events: AsyncStream<TranscriptEngineEvent>,
        assetId: String
    ) async -> Terminal? {
        for await event in events {
            if case .completed(let id) = event, id == assetId { return .completed }
            if case .failed(let id, let reason) = event, id == assetId { return .failed(reason) }
        }
        return nil
    }

    // MARK: - Total failure

    /// THE LIE. Before this bead every shard could fail and the loop still
    /// emitted `.completed`.
    @Test(.timeLimit(.minutes(1)))
    func totalFailureEmitsFailedWithItsReason() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-total-fail"))
        let speech = SpeechService(
            recognizer: AlwaysFailingRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [
                makeShard(id: 0, episodeID: "ep-asset-total-fail", startTime: 0, duration: 30),
                makeShard(id: 1, episodeID: "ep-asset-total-fail", startTime: 30, duration: 30),
            ],
            analysisAssetId: "asset-total-fail",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-total-fail")

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-total-fail")
        guard case .failed(let reason) = terminal else {
            Issue.record("expected .failed, got \(String(describing: terminal))")
            return
        }
        // The class is the one the recognizer actually threw — the point is
        // that the ERROR survived, not merely that a failure was signalled.
        #expect(reason.failureClass == .vadFailed)
        #expect(reason.failedShardCount == 2, "both shards failed")

        // And nothing was written, so `.completed` would have been a lie
        // about durable coverage as well as about success.
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-total-fail")
        #expect(chunks.isEmpty)
    }

    // MARK: - Partial success is NOT a failure

    /// The counterweight, and what stops the change from being "emit .failed
    /// whenever any shard throws". The catches continue on purpose — partial
    /// coverage is better than none — so a run that persisted chunks must
    /// still complete. Without this test, tightening the condition to "any
    /// failure" would pass everything else in this file while throwing away
    /// usable transcript on every real episode with one bad shard.
    @Test(.timeLimit(.minutes(1)))
    func partialSuccessStillCompletes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-partial"))
        let speech = SpeechService(
            recognizer: FailAfterFirstRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [
                makeShard(id: 0, episodeID: "ep-asset-partial", startTime: 0, duration: 30),
                makeShard(id: 1, episodeID: "ep-asset-partial", startTime: 30, duration: 30),
            ],
            analysisAssetId: "asset-partial",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-partial")

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-partial")
        #expect(terminal == .completed,
                "a run that persisted chunks is not a failure (got \(String(describing: terminal)))")

        // The fixture really is partial: shard 0 landed, shard 1 threw.
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-partial")
        #expect(!chunks.isEmpty, "the fixture must persist something, or this proves nothing")
    }

    // MARK: - The two silent returns

    @Test(.timeLimit(.minutes(1)))
    func noShardsEmitsFailedInsteadOfNothing() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-no-shards"))
        let speech = SpeechService(
            recognizer: AlwaysFailingRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [], analysisAssetId: "asset-no-shards", snapshot: Self.snapshot
        )

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-no-shards")
        #expect(terminal == .failed(TranscriptFailureReason(failureClass: .noShards)),
                "got \(String(describing: terminal))")
    }

    /// The `modelNotLoaded` hypothesis, made observable. A device can be fully
    /// eligible on the Apple Intelligence check while the recognizer actor
    /// holds `loaded == false`, because `loadFastModel()` is invoked exactly
    /// once at launch and its failure is swallowed by an empty catch that
    /// nothing ever retries.
    @Test(.timeLimit(.minutes(1)))
    func notReadyEngineEmitsFailedInsteadOfNothing() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-not-ready"))
        let speech = SpeechService(
            recognizer: NeverReadyRecognizer(), serializesRecognizerRequests: false
        )
        // Deliberately NOT calling loadFastModel: this is the post-swallow
        // state on a real device.
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-not-ready", startTime: 0, duration: 30)],
            analysisAssetId: "asset-not-ready",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-not-ready")

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-not-ready")
        #expect(
            terminal == .failed(TranscriptFailureReason(failureClass: .speechEngineNotReady)),
            "got \(String(describing: terminal))"
        )
    }

    // MARK: - The stop gate still applies

    /// `.failed` must obey the same per-asset gate as `.completed`: after
    /// `stopTranscription`, the runner has moved on and any event it observes
    /// would be attributed to whatever it is doing next.
    @Test(.timeLimit(.minutes(1)))
    func failedIsSuppressedForAStoppedAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-stopped"))
        let speech = SpeechService(
            recognizer: AlwaysFailingRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.stopTranscription(analysisAssetId: "asset-stopped")
        // A stop for THIS asset is rescinded by startTranscription, so drive
        // the emission directly against a gated asset instead.
        await engine.emitFailedForTesting(
            analysisAssetId: "asset-stopped",
            reason: TranscriptFailureReason(failureClass: .silentShard)
        )
        await engine.emitFailedForTesting(
            analysisAssetId: "asset-other",
            reason: TranscriptFailureReason(failureClass: .noShards)
        )

        // The first event to arrive must be the ungated one — proving the
        // gated one was dropped rather than merely late.
        var received: [String] = []
        for await event in events {
            if case .failed(let id, _) = event {
                received.append(id)
                break
            }
        }
        #expect(received == ["asset-other"],
                "a .failed for a stopped asset must be dropped (received \(received))")
    }
}
