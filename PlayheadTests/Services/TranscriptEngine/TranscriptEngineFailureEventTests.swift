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
    /// Attempts, not shards. `runTranscriptionLoop` can hand the same shard
    /// over twice — the main loop and then the shard-0 backfill — and review
    /// r4 needs to prove that second attempt really happened.
    private let _calls = OSAllocatedUnfairLock(initialState: 0)
    var callCount: Int { _calls.withLock { $0 } }

    func loadModel() async throws { _loaded.withLock { $0 = true } }
    func unloadModel() async { _loaded.withLock { $0 = false } }
    func isModelLoaded() async -> Bool { _loaded.withLock { $0 } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        _calls.withLock { $0 += 1 }
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

/// Succeeds on every shard. Used to reach the PERSISTENCE step, which is the
/// only way to fail a run without failing ASR.
private final class AlwaysSucceedingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let _loaded = OSAllocatedUnfairLock(initialState: false)
    func loadModel() async throws { _loaded.withLock { $0 = true } }
    func unloadModel() async { _loaded.withLock { $0 = false } }
    func isModelLoaded() async -> Bool { _loaded.withLock { $0 } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
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

/// Shard 0 yields NO segments — silence or music, which `transcribeShard`
/// handles by advancing the coverage watermark and returning successfully
/// without inserting a row. Everything after it throws. The run therefore
/// inserts nothing while genuinely finishing a shard (playhead-8ysk review r4).
private final class SilentThenFailingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let _loaded = OSAllocatedUnfairLock(initialState: false)
    func loadModel() async throws { _loaded.withLock { $0 = true } }
    func unloadModel() async { _loaded.withLock { $0 = false } }
    func isModelLoaded() async -> Bool { _loaded.withLock { $0 } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard shard.id == 0 else {
            throw TranscriptEngineError.vadFailed("shard \(shard.id)")
        }
        return []
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(isSpeech: false, speechProbability: 0.0,
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

    // MARK: - Retry of an asset that once made progress (review r3)

    /// THE HEADLINE CASE, AND IT FAILED HERE FIRST.
    ///
    /// The bead exists because 147 analysis jobs were acquired and 9 finalized
    /// over six days: the device is overwhelmingly RETRYING assets that already
    /// made partial progress. So the retry is the case a failure report has to
    /// survive, and until review r3 it was the one case it did not.
    ///
    /// The gate read `store.fetchTranscriptChunks(assetId:)` — which is
    /// `WHERE analysisAssetId = ?` with no pass scoping — and the asset row is
    /// REUSED across passes (`AnalysisCoordinator` resolves it via
    /// `fetchAssetByEpisodeId` and keeps `existing.id`). So an EARLIER pass's
    /// chunks made `persisted.isEmpty` false and a total failure emitted
    /// `.completed`. Downstream, `AnalysisJobRunner` then reads
    /// `fastTranscriptCoverageEndTime` — cumulative for the same reason — sees
    /// non-zero, and skips the entire zero-coverage branch: no `work_journal`
    /// row, no `failure_class`, no named `lastErrorCode`.
    ///
    /// That is exactly the defect review r2 fixed one layer up (R7), reappearing
    /// underneath the fix. Both come from reading a cumulative store value as a
    /// per-pass measure.
    ///
    /// The fixture is the incident: a pre-existing chunk from pass 1, then a
    /// pass that transcribes nothing.
    @Test(.timeLimit(.minutes(1)))
    func retryThatProducesNothingStillReportsFailed() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-retry"))

        // Pass 1's durable output. The row survives into pass 2 because the
        // asset id does.
        try await store.insertTranscriptChunks([
            TranscriptChunk(
                id: "chunk-from-pass-1",
                analysisAssetId: "asset-retry",
                segmentFingerprint: "seg-pass-1",
                chunkIndex: 0,
                startTime: 0,
                endTime: 5,
                text: "from an earlier pass",
                normalizedText: "from an earlier pass",
                pass: TranscriptPassType.fast.rawValue,
                modelVersion: "speech-v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: nil
            ),
        ])
        let seeded = try await store.fetchTranscriptChunks(assetId: "asset-retry")
        #expect(seeded.count == 1, "the retry premise requires prior-pass output to exist")

        // Pass 2: every shard fails, so this run produces nothing at all.
        let speech = SpeechService(
            recognizer: AlwaysFailingRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [
                makeShard(id: 0, episodeID: "ep-asset-retry", startTime: 0, duration: 30),
                makeShard(id: 1, episodeID: "ep-asset-retry", startTime: 30, duration: 30),
            ],
            analysisAssetId: "asset-retry",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-retry")

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-retry")
        guard case .failed(let reason) = terminal else {
            Issue.record("""
                a retry that transcribed nothing reported \
                \(String(describing: terminal)) — the prior pass's chunks were \
                counted as this pass's output
                """)
            return
        }
        #expect(reason.failureClass == .vadFailed)
        #expect(reason.failedShardCount == 2)

        // And the earlier pass's work is untouched: reporting the failure must
        // not be confused with discarding usable transcript.
        let after = try await store.fetchTranscriptChunks(assetId: "asset-retry")
        #expect(after.count == 1, "the prior pass's chunk must survive the failure report")
    }

    /// The counterweight to the test above, and the reason the fix is a
    /// per-run counter rather than "ignore what is in the store".
    ///
    /// A retry that DOES persist something is still a partial success and must
    /// still complete. Without this, tightening the gate to "any shard failed"
    /// — or to "the store gained no rows", which a dedup-heavy retry can also
    /// satisfy — would pass the test above while throwing away real work.
    @Test(.timeLimit(.minutes(1)))
    func retryThatPersistsSomethingStillCompletes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-retry-partial"))
        try await store.insertTranscriptChunks([
            TranscriptChunk(
                id: "chunk-retry-prior",
                analysisAssetId: "asset-retry-partial",
                segmentFingerprint: "seg-retry-prior",
                chunkIndex: 0,
                startTime: 0,
                endTime: 5,
                text: "from an earlier pass",
                normalizedText: "from an earlier pass",
                pass: TranscriptPassType.fast.rawValue,
                modelVersion: "speech-v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: nil
            ),
        ])

        let speech = SpeechService(
            recognizer: FailAfterFirstRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [
                makeShard(id: 0, episodeID: "ep-asset-retry-partial", startTime: 0, duration: 30),
                makeShard(id: 1, episodeID: "ep-asset-retry-partial", startTime: 30, duration: 30),
            ],
            analysisAssetId: "asset-retry-partial",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-retry-partial")

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-retry-partial")
        #expect(terminal == .completed,
                "a retry that persisted new chunks is a partial success, not a failure")

        let after = try await store.fetchTranscriptChunks(assetId: "asset-retry-partial")
        #expect(after.count > 1, "this run must really have added rows, or it proves nothing")
    }

    // MARK: - Inserting nothing is not the same as doing nothing (review r4)

    /// A RUN THAT INSERTS NO ROWS CAN STILL HAVE DONE ITS WORK.
    ///
    /// `transcribeShard` returns successfully without inserting anything when
    /// the recognizer yields no segments — silence or music. It still advances
    /// the coverage watermark, so that audio is durably not re-attempted. A
    /// gate that reads only "rows inserted this run" cannot tell that apart
    /// from a shard that failed, so a music-heavy episode with one bad shard
    /// reported a TOTAL FAILURE: wrong `lastErrorCode`, a spurious
    /// `work_journal` row, a requeue with backoff, and no `finalizeBackfill`.
    ///
    /// The bar `partialSuccessStillCompletes` sets is "one bad shard among
    /// good ones is a partial success". This is the same claim for the good
    /// shards that have no row to show for themselves.
    @Test(.timeLimit(.minutes(1)))
    func silentShardWithNoRowsIsStillWork() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-silent"))
        let speech = SpeechService(
            recognizer: SilentThenFailingRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [
                makeShard(id: 0, episodeID: "ep-asset-silent", startTime: 0, duration: 30),
                makeShard(id: 1, episodeID: "ep-asset-silent", startTime: 30, duration: 30),
            ],
            analysisAssetId: "asset-silent",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-silent")

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-silent")
        #expect(
            terminal == .completed,
            """
            shard 0 finished and moved the watermark; only shard 1 failed. \
            Reporting a total failure here is the original lie pointing the \
            other way (got \(String(describing: terminal)))
            """
        )

        // The premise: this run really did insert nothing, so it really was
        // the insert count that had to be wrong about it. Without this the
        // test would also pass against a build where shard 0 wrote a row.
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-silent")
        #expect(chunks.isEmpty, "the fixture must insert nothing, or this proves nothing")

        // And the work is durable: the watermark advanced over shard 0 only.
        let asset = try await store.fetchAsset(id: "asset-silent")
        #expect(asset?.fastTranscriptCoverageEndTime == 30)
    }

    /// THE DEDUP-HEAVY RE-RUN — the production shape of the same defect.
    ///
    /// The loop deliberately does not filter shards by coverage; it re-runs
    /// every shard and lets `transcribeShard`'s `segmentFingerprint` dedup
    /// suppress the duplicates. So a second pass over audio that is already
    /// transcribed inserts ZERO rows while succeeding completely — and
    /// re-running already-covered assets is this bead's own incident (147
    /// acquisitions, 9 finalizations).
    ///
    /// `retryThatPersistsSomethingStillCompletes` names "the store gained no
    /// rows, which a dedup-heavy retry can also satisfy" as a rejected
    /// alternative. A per-run INSERT count has that identical blind spot; this
    /// is the fixture that shows it, built by running the real loop twice over
    /// the same store so the dedup happens through the production fingerprint
    /// path rather than a hand-computed one.
    @Test(.timeLimit(.minutes(1)))
    func dedupOnlyRerunWithOneBadShardStillCompletes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-dedup"))
        let shards = [
            makeShard(id: 0, episodeID: "ep-asset-dedup", startTime: 0, duration: 30),
            makeShard(id: 1, episodeID: "ep-asset-dedup", startTime: 30, duration: 30),
        ]

        // Pass 1: transcribe both shards for real.
        let speech1 = SpeechService(
            recognizer: AlwaysSucceedingRecognizer(), serializesRecognizerRequests: false
        )
        try await speech1.loadFastModel()
        let engine1 = TranscriptEngineService(speechService: speech1, store: store)
        let events1 = await engine1.events()
        await engine1.startTranscription(
            shards: shards, analysisAssetId: "asset-dedup", snapshot: Self.snapshot
        )
        await engine1.finishAppending(analysisAssetId: "asset-dedup")
        #expect(await Self.awaitTerminal(on: events1, assetId: "asset-dedup") == .completed)
        let afterPass1 = try await store.fetchTranscriptChunks(assetId: "asset-dedup").count
        #expect(afterPass1 == 2, "pass 1 must actually persist, or pass 2 has nothing to dedup")

        // Pass 2: a fresh engine over the same store. Shard 0 produces the
        // byte-identical segment pass 1 already stored, so it dedups and
        // inserts nothing; shard 1 throws.
        let speech2 = SpeechService(
            recognizer: FailAfterFirstRecognizer(), serializesRecognizerRequests: false
        )
        try await speech2.loadFastModel()
        let engine2 = TranscriptEngineService(speechService: speech2, store: store)
        let events2 = await engine2.events()
        await engine2.startTranscription(
            shards: shards, analysisAssetId: "asset-dedup", snapshot: Self.snapshot
        )
        await engine2.finishAppending(analysisAssetId: "asset-dedup")

        let terminal = await Self.awaitTerminal(on: events2, assetId: "asset-dedup")
        #expect(
            terminal == .completed,
            """
            pass 2 transcribed shard 0 successfully — the row already existed, \
            so there was nothing to insert. Counting inserts alone calls that \
            a total failure (got \(String(describing: terminal)))
            """
        )

        // The premise: pass 2 inserted nothing. If it had, the insert count
        // would have carried the test and the dedup blind spot would be
        // untested.
        let afterPass2 = try await store.fetchTranscriptChunks(assetId: "asset-dedup").count
        #expect(afterPass2 == afterPass1,
                "pass 2 must add no rows, or this fixture does not exercise the dedup path")
    }

    // MARK: - The third `transcribeShard` call site (review r4)

    /// `failedShardCount` COUNTS SHARDS, NOT ATTEMPTS.
    ///
    /// `runTranscriptionLoop` calls `transcribeShard` from three places, and
    /// the third — the shard-0 backfill, reached when the first 30 s is
    /// missing — deliberately does NOT record its error into `shardFailures`.
    /// Round 4 read that silence as the "diagnosis dies with the shard" defect
    /// this bead removed at the other two sites and added the append; three
    /// tests in this file rejected it, because the backfill can only ever
    /// re-attempt a shard the main loop already attempted and classified.
    ///
    /// Those three tests pin the invariant only for multi-shard runs, where an
    /// off-by-one is easy to misread as an unrelated fixture change. This is
    /// the minimal fixture that isolates it: ONE shard, which fails in the main
    /// loop, leaves no early chunk, and is therefore re-attempted and fails
    /// again. Two failed attempts, one failed shard — and the count must say
    /// one, because it is exported to `work_journal.metadata` as a
    /// shard-count-shaped value whose duration-proxy analysis in
    /// `DiagnosticsBundleFailureClassTests` assumes exactly that.
    @Test(.timeLimit(.minutes(1)))
    func shardZeroBackfillDoesNotInflateTheFailedShardCount() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-backfill"))
        let recognizer = AlwaysFailingRecognizer()
        let speech = SpeechService(
            recognizer: recognizer, serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-backfill", startTime: 0, duration: 30)],
            analysisAssetId: "asset-backfill",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-backfill")

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-backfill")
        guard case .failed(let reason) = terminal else {
            Issue.record("expected .failed, got \(String(describing: terminal))")
            return
        }
        #expect(reason.failureClass == .vadFailed)
        #expect(
            reason.failedShardCount == 1,
            """
            one shard failed. The shard-0 backfill re-attempted it and failed \
            again, but recording that would make this a count of attempts and \
            would export a shard count larger than the episode has \
            (got \(reason.failedShardCount))
            """
        )
        // THE PREMISE, ASSERTED RATHER THAN ASSUMED. One shard was handed to
        // the recognizer TWICE — once by the main loop, once by the shard-0
        // backfill. Without this the test passes vacuously against any build
        // where the backfill never ran, which is most of them.
        #expect(
            recognizer.callCount == 2,
            """
            the shard-0 backfill must actually re-attempt shard 0, or the count \
            was never exposed to inflation and this test guards nothing \
            (attempts: \(recognizer.callCount))
            """
        )
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-backfill")
        #expect(chunks.isEmpty, "no early chunk may exist, or the backfill never runs")
    }

    /// THE ACCIDENTAL DEMO, TURNED INTO A GUARD (round-1 review).
    ///
    /// `appendShardsAfterCompletion` never seeded its `analysis_assets` row.
    /// Every chunk insert therefore hit the foreign key, the run did full ASR
    /// work and persisted nothing, and the test passed — because the loop
    /// emitted `.completed` over a total failure. That is this bead's defect,
    /// running green in the suite for as long as the test existed. Seeding the
    /// row fixed that test; nothing pinned the SHAPE.
    ///
    /// So it is pinned here deliberately, with the row left out on purpose.
    /// ASR succeeds on every shard, so this can only be reported by the
    /// persistence path — and it must be reported as `persistence_failed`, not
    /// as `unknown`. Before the review fix the store's errors reached the
    /// classifier's residual bucket, so the whole vocabulary said no more about
    /// this run than `asr_failed` did.
    @Test(.timeLimit(.minutes(1)))
    func persistenceFailureIsNamedNotSwallowed() async throws {
        let store = try await makeTestStore()
        // Deliberately NO insertAsset — the foreign key is the failure.
        let speech = SpeechService(
            recognizer: AlwaysSucceedingRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [
                makeShard(id: 0, episodeID: "ep-asset-nofk", startTime: 0, duration: 30),
                makeShard(id: 1, episodeID: "ep-asset-nofk", startTime: 30, duration: 30),
            ],
            analysisAssetId: "asset-nofk",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-nofk")

        let terminal = await Self.awaitTerminal(on: events, assetId: "asset-nofk")
        guard case .failed(let reason) = terminal else {
            Issue.record("expected .failed, got \(String(describing: terminal))")
            return
        }
        #expect(reason.failureClass == .persistenceFailed,
                "a store failure must be named, not bucketed as \(reason.failureClass.rawValue)")
        #expect(reason.failedShardCount == 2)

        // The fixture really is the masked shape: ASR ran, nothing landed.
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-nofk")
        #expect(chunks.isEmpty, "the fixture must persist nothing, or this proves nothing")
    }

    /// The loop accumulates failures at TWO sites — the initial pass and the
    /// drain of shards appended while it ran — and only the first was covered
    /// (round-1 review). Removing `shardFailures.append` from the drain catch
    /// left every test green, so a run whose failures all arrived on appended
    /// shards would have reported `.completed` over a total failure: the exact
    /// lie this bead removes, still live on the streaming-decoder path that
    /// produces most real shards.
    ///
    /// Driven with no race: the first run is awaited to completion, so the
    /// second `appendShards` starts a fresh loop with `shards: []` and every
    /// one of its failures can only have been accumulated in the drain.
    @Test(.timeLimit(.minutes(1)))
    func appendedShardFailuresAreAccumulatedToo() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(Self.asset(id: "asset-appended-fail"))
        let speech = SpeechService(
            recognizer: AlwaysFailingRecognizer(), serializesRecognizerRequests: false
        )
        try await speech.loadFastModel()
        let engine = TranscriptEngineService(speechService: speech, store: store)

        let firstEvents = await engine.events()
        await engine.startTranscription(
            shards: [makeShard(
                id: 0, episodeID: "ep-asset-appended-fail", startTime: 0, duration: 30
            )],
            analysisAssetId: "asset-appended-fail",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-appended-fail")
        let firstTerminal = await Self.awaitTerminal(
            on: firstEvents, assetId: "asset-appended-fail"
        )
        #expect(firstTerminal == .failed(TranscriptFailureReason(
            failureClass: .vadFailed, failedShardCount: 1
        )), "precondition: the initial pass fails on its one shard")

        // A fresh loop whose entire input arrives through the append queue.
        let appendEvents = await engine.events()
        await engine.appendShards(
            [
                makeShard(id: 1, episodeID: "ep-asset-appended-fail",
                          startTime: 30, duration: 30),
                makeShard(id: 2, episodeID: "ep-asset-appended-fail",
                          startTime: 60, duration: 30),
            ],
            analysisAssetId: "asset-appended-fail",
            snapshot: Self.snapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-appended-fail")

        let terminal = await Self.awaitTerminal(on: appendEvents, assetId: "asset-appended-fail")
        guard case .failed(let reason) = terminal else {
            Issue.record("appended-shard failures must be reported, got \(String(describing: terminal))")
            return
        }
        #expect(reason.failureClass == .vadFailed)
        #expect(reason.failedShardCount == 2,
                "both appended shards failed and both must be counted")
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
