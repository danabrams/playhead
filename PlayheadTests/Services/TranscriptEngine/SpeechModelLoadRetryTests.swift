// SpeechModelLoadRetryTests.swift
// playhead-se2h — a swallowed launch-time model-load failure permanently
// disabled transcription, with no retry.
//
// `loadFastModel()` was invoked EXACTLY ONCE per install, at launch, and
// its failure landed in an empty `catch` whose comment claimed the
// transcript engine would surface it when first used. It did not: the
// failure became an indistinguishable `modelNotLoaded` on every later
// shard, nothing retried, and one transient hiccup disabled transcription
// for the life of the install.
//
// This file pins the four things that must now be true, in the order they
// matter:
//
//   1. A TRANSIENT failure recovers WITHOUT a relaunch — through the real
//      `TranscriptEngineService` loop, not a direct call to the actor.
//   2. The retry is BOUNDED and cannot spin: a device that never loads
//      makes at most `maxLoadAttemptsPerEpoch` attempts, however many
//      runs are scheduled.
//   3. The failure is OBSERVABLE — the journal records it on the REAL
//      path, driven by the transcription loop.
//   4. No state ever claims a model the recognizer does not hold.

import Foundation
import os
import Testing

@testable import Playhead

// MARK: - Doubles

/// Recognizer whose first `failCount` loads throw and whose later loads
/// succeed. Models the whole point of the bead: a load that fails at
/// launch and would succeed a minute later.
private final class TransientlyFailingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private struct State {
        var loaded = false
        var loadAttempts = 0
        var transcribeCalls = 0
    }

    private let state = OSAllocatedUnfairLock(initialState: State())
    private let failuresBeforeSuccess: Int
    private let error: any Error

    init(
        failuresBeforeSuccess: Int,
        error: any Error = TranscriptEngineError.transcriptionFailed("assets still installing")
    ) {
        self.failuresBeforeSuccess = failuresBeforeSuccess
        self.error = error
    }

    var loadAttempts: Int { state.withLock { $0.loadAttempts } }
    var transcribeCalls: Int { state.withLock { $0.transcribeCalls } }

    func loadModel() async throws {
        let attempt: Int = state.withLock { state in
            state.loadAttempts += 1
            return state.loadAttempts
        }
        guard attempt > failuresBeforeSuccess else { throw error }
        state.withLock { $0.loaded = true }
    }

    func unloadModel() async { state.withLock { $0.loaded = false } }

    func isModelLoaded() async -> Bool { state.withLock { $0.loaded } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard state.withLock({ $0.loaded }) else { throw TranscriptEngineError.modelNotLoaded }
        state.withLock { $0.transcribeCalls += 1 }
        return [TranscriptSegment(
            id: shard.id,
            words: [TranscriptWord(
                text: "recovered",
                startTime: shard.startTime,
                endTime: shard.startTime + 1,
                confidence: 0.9
            )],
            text: "recovered",
            startTime: shard.startTime,
            endTime: shard.startTime + 1,
            avgConfidence: 0.9,
            passType: .fast
        )]
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(
            isSpeech: true,
            speechProbability: 1.0,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration
        )]
    }
}

/// Recognizer whose `loadModel()` throws only because the calling task
/// was cancelled — the shape a scrub or `stopTranscription` produces while
/// a load is in flight.
///
/// It wraps the cancellation in `TranscriptEngineError.transcriptionFailed`
/// ON PURPOSE: that is what `AppleSpeechRecognizer.loadModel()` does to
/// anything it does not recognise, so a double that threw a bare
/// `CancellationError` would test a shape production never produces.
private final class CancellationObservingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let state = OSAllocatedUnfairLock(initialState: 0)

    var loadAttempts: Int { state.withLock { $0 } }

    func loadModel() async throws {
        state.withLock { $0 += 1 }
        guard !Task.isCancelled else {
            throw TranscriptEngineError.transcriptionFailed("cancelled: \(CancellationError())")
        }
    }

    func unloadModel() async {}
    func isModelLoaded() async -> Bool { false }
    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] { [] }
    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] { [] }
}

/// Recognizer whose `loadModel()` parks until released, so a second
/// caller can be observed arriving while the first is still inside the
/// asset download.
private final class SuspendingLoadRecognizer: SpeechRecognizer, @unchecked Sendable {
    private struct State {
        var loaded = false
        var loadAttempts = 0
    }

    private let state = OSAllocatedUnfairLock(initialState: State())
    private let entered: AsyncStream<Void>
    private let enteredContinuation: AsyncStream<Void>.Continuation
    private let release: AsyncStream<Void>
    private let releaseContinuation: AsyncStream<Void>.Continuation

    init() {
        (entered, enteredContinuation) = AsyncStream<Void>.makeStream()
        (release, releaseContinuation) = AsyncStream<Void>.makeStream()
    }

    var loadAttempts: Int { state.withLock { $0.loadAttempts } }

    /// Suspends until `loadModel()` has been entered.
    func waitUntilLoadEntered() async {
        var iterator = entered.makeAsyncIterator()
        _ = await iterator.next()
    }

    func releaseLoad() { releaseContinuation.yield(()) }

    func loadModel() async throws {
        state.withLock { $0.loadAttempts += 1 }
        enteredContinuation.yield(())
        var iterator = release.makeAsyncIterator()
        _ = await iterator.next()
        state.withLock { $0.loaded = true }
    }

    func unloadModel() async { state.withLock { $0.loaded = false } }
    func isModelLoaded() async -> Bool { state.withLock { $0.loaded } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] { [] }
    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] { [] }
}

// MARK: - Helpers

/// Non-silent samples on purpose. The shared `makeShard` helper fills with
/// zeros, which the loop rejects with `silent_shard` before ASR is reached
/// — fine for a test whose run never gets that far, useless for the
/// recovery test, whose whole claim is that audio really was transcribed.
private func retryShard(
    id: Int = 0,
    episodeID: String = "ep-retry",
    startTime: TimeInterval = 0,
    duration: TimeInterval = 30
) -> AnalysisShard {
    AnalysisShard(
        id: id,
        episodeID: episodeID,
        startTime: startTime,
        duration: duration,
        samples: (0..<(16_000 * Int(duration))).map { Float(sin(Double($0) * 0.01)) }
    )
}

private func makeJournal() -> (journal: SpeechModelLoadJournal, directory: URL) {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("se2h-retry-\(UUID().uuidString)", isDirectory: true)
    return (SpeechModelLoadJournal(directory: directory), directory)
}

// MARK: - The bound

@Suite("playhead-se2h — the model-load retry is bounded and cannot spin")
struct SpeechModelLoadBoundTests {

    /// THE NO-SPIN PROOF.
    ///
    /// A device whose assets are genuinely unavailable must not turn every
    /// scheduled transcription run into another download attempt. Twenty
    /// calls, three attempts — and the seventeen that made no attempt say
    /// so by name rather than being indistinguishable from a failure.
    @Test("A permanently failing load is attempted at most maxLoadAttemptsPerEpoch times")
    func permanentFailureIsBounded() async {
        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: .max)
        let service = SpeechService(recognizer: recognizer)

        var outcomes: [SpeechModelLoadOutcome] = []
        for _ in 0..<20 {
            outcomes.append(await service.prepareFastModel())
        }

        #expect(
            recognizer.loadAttempts == SpeechService.maxLoadAttemptsPerEpoch,
            """
            20 calls produced \(recognizer.loadAttempts) load attempts. The budget is \
            \(SpeechService.maxLoadAttemptsPerEpoch); anything higher means a genuinely \
            unavailable asset costs one download per scheduled run, forever.
            """
        )
        #expect(outcomes.prefix(SpeechService.maxLoadAttemptsPerEpoch).allSatisfy { $0 == .failed })
        #expect(
            outcomes.dropFirst(SpeechService.maxLoadAttemptsPerEpoch).allSatisfy { $0 == .budgetExhausted },
            "a call that did not attempt must not be reported as a failure — that is the same conflation this bead is about"
        )
        let ready = await service.isReady()
        #expect(!ready)
    }

    /// The bound must survive REENTRANCY. `prepareFastModel` is an actor
    /// method with awaits in it, so a budget check separated from its
    /// consumption by a suspension point would let concurrent callers both
    /// pass the guard and overspend.
    @Test("Concurrent callers cannot overspend the budget")
    func concurrentCallersCannotOverspendTheBudget() async {
        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: .max)
        let service = SpeechService(recognizer: recognizer)

        await withTaskGroup(of: SpeechModelLoadOutcome.self) { group in
            for _ in 0..<32 {
                group.addTask { await service.prepareFastModel() }
            }
            for await _ in group {}
        }

        #expect(
            recognizer.loadAttempts <= SpeechService.maxLoadAttemptsPerEpoch,
            "32 concurrent callers produced \(recognizer.loadAttempts) attempts — the bound is not atomic"
        )
    }

    /// A caller that arrives mid-load must DECLINE, not queue behind an
    /// asset download it cannot cancel, and must not spend a budget slot
    /// for an attempt it did not make.
    @Test("A caller arriving mid-load declines without consuming the budget")
    func concurrentCallerDeclinesWhileALoadIsInFlight() async throws {
        let recognizer = SuspendingLoadRecognizer()
        let service = SpeechService(recognizer: recognizer)

        async let first = service.prepareFastModel()
        await recognizer.waitUntilLoadEntered()

        let second = await service.prepareFastModel()
        #expect(second == .loadInFlight)
        #expect(recognizer.loadAttempts == 1, "the second caller must not start a second download")

        recognizer.releaseLoad()
        #expect(await first == .loaded)
        // The declined call consumed nothing: one attempt was made, and the
        // success reset the epoch.
        let spent = await service.loadAttemptsInCurrentEpoch
        #expect(spent == 0)
    }

    /// Only a SUCCESS clears the budget. Anything weaker (clearing on
    /// every call, on a failure, on a timer) is an unbounded retry wearing
    /// a bound's clothes.
    @Test("A successful load resets the budget; a failure does not")
    func successResetsTheBudgetAndFailureDoesNot() async {
        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: 1)
        let service = SpeechService(recognizer: recognizer)

        #expect(await service.prepareFastModel() == .failed)
        let afterFailure = await service.loadAttemptsInCurrentEpoch
        #expect(afterFailure == 1, "a failure must leave its slot spent")

        #expect(await service.prepareFastModel() == .loaded)
        let afterSuccess = await service.loadAttemptsInCurrentEpoch
        #expect(afterSuccess == 0, "a success ends the failure run, so the next one starts fresh")
    }

    /// A foreground is the only other thing that refreshes the budget, and
    /// it must do NOTHING else — no load, no probe, nothing that could put
    /// work on a lifecycle path the user hits on every return from a
    /// system sheet.
    @Test("A foreground refreshes the budget without loading anything")
    func foregroundRefreshesTheBudgetWithoutLoading() async {
        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: 3)
        let service = SpeechService(recognizer: recognizer)

        for _ in 0..<SpeechService.maxLoadAttemptsPerEpoch {
            _ = await service.prepareFastModel()
        }
        #expect(await service.prepareFastModel() == .budgetExhausted)
        let attemptsBeforeForeground = recognizer.loadAttempts

        await service.noteAppDidBecomeActive()
        #expect(
            recognizer.loadAttempts == attemptsBeforeForeground,
            "noteAppDidBecomeActive must not itself load — the next scheduled run is the trigger"
        )
        let ready = await service.isReady()
        #expect(!ready, "and it must not claim readiness it has not established")

        // The refreshed budget is what makes recovery possible without the
        // process being killed.
        #expect(await service.prepareFastModel() == .loaded)
    }

    /// The foreground refresh must not be a free pass to unbounded work:
    /// each refresh still buys at most the same bounded budget, and each
    /// attempt within it still needs a scheduled run to spend it.
    @Test("Repeated foregrounds still cap attempts per epoch")
    func repeatedForegroundsStillCapAttemptsPerEpoch() async {
        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: .max)
        let service = SpeechService(recognizer: recognizer)

        for _ in 0..<4 {
            await service.noteAppDidBecomeActive()
            for _ in 0..<10 {
                _ = await service.prepareFastModel()
            }
        }

        #expect(
            recognizer.loadAttempts == 4 * SpeechService.maxLoadAttemptsPerEpoch,
            """
            40 calls across 4 foregrounds produced \(recognizer.loadAttempts) attempts; \
            the cap is \(SpeechService.maxLoadAttemptsPerEpoch) per foreground. A larger \
            number means a foreground uncaps the retry rather than refreshing a bound.
            """
        )
    }
}

// MARK: - State honesty

@Suite("playhead-se2h — no state claims a model the recognizer does not hold")
struct SpeechModelLoadStateHonestyTests {

    /// `prepareFastModel` runs on every transcription run, including runs
    /// that happen while a charge-gated final pass owns the recognizer. If
    /// it reloaded, or merely re-stamped the role, every chunk of that
    /// pass would be mis-tagged `fast`.
    @Test("prepareFastModel does not disturb an already-loaded final model")
    func alreadyLoadedFinalModelIsNotClobbered() async throws {
        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: 0)
        let service = SpeechService(recognizer: recognizer)
        try await service.loadFinalModel()
        let attemptsAfterFinalLoad = recognizer.loadAttempts

        let outcome = await service.prepareFastModel()

        #expect(outcome == .alreadyLoaded)
        #expect(recognizer.loadAttempts == attemptsAfterFinalLoad, "nothing must be reloaded")
        let role = await service.activeModelRole
        #expect(role == .asrFinal, "the role must still describe the model actually loaded")
        let spent = await service.loadAttemptsInCurrentEpoch
        #expect(spent == 0, "a no-op must not consume the retry budget")
    }

    /// A failed load must leave the actor claiming nothing.
    @Test("A failed load leaves activeModelRole nil and isReady false")
    func failedLoadClaimsNothing() async {
        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: 1)
        let service = SpeechService(recognizer: recognizer)

        #expect(await service.prepareFastModel() == .failed)
        let role = await service.activeModelRole
        #expect(role == nil)
        let ready = await service.isReady()
        #expect(!ready)

        // And a later success must establish BOTH, together.
        #expect(await service.prepareFastModel() == .loaded)
        let recoveredRole = await service.activeModelRole
        #expect(recoveredRole == .asrFast)
        let recoveredReady = await service.isReady()
        #expect(recoveredReady)
    }
}

// MARK: - Observability on the real path

@Suite("playhead-se2h — the load failure is recorded where a bundle can see it")
struct SpeechModelLoadJournalRecordingTests {

#if canImport(Speech)
    /// The class must be PRECISE. `AppleSpeechRecognizer.loadModel()` used
    /// to collapse every `AppleSpeechBoundaryError` into
    /// `TranscriptEngineError.transcriptionFailed`, so the two failures
    /// that actually happen in the field — a locale whose assets are not
    /// installed, and an analyzer that negotiates no format — both arrived
    /// wearing the name of a failure meaning "recognition ran and reported
    /// an error". That reads as `asr_failed` downstream, i.e. a row that
    /// contradicts itself.
    @Test("A failed load is journaled with its precise class and attempt number")
    func failureIsJournaledWithClassAndAttempt() async {
        let (journal, directory) = makeJournal()
        defer { try? FileManager.default.removeItem(at: directory) }

        let recognizer = TransientlyFailingRecognizer(
            failuresBeforeSuccess: .max,
            error: AppleSpeechBoundaryError.speechAssetsUnsupported(localeIdentifier: "en-US")
        )
        let service = SpeechService(recognizer: recognizer, loadJournal: journal)

        _ = await service.prepareFastModel()
        _ = await service.prepareFastModel()

        let state = await journal.load()
        #expect(state.consecutiveFailureCount == 2)
        #expect(state.recentFailures.map(\.attemptNumber) == [1, 2])
        #expect(
            state.recentFailures.allSatisfy { $0.failureClass == .speechAssetsUnsupported },
            """
            got \(state.recentFailures.map(\.failureClass.rawValue)). A locale whose assets \
            are not installed must not be filed as `transcription_failed` — that class means \
            recognition RAN and reported an error, which reads as `asr_failed` downstream.
            """
        )
        #expect(state.lastSuccessAt == nil)
        #expect(state.lastSuccessfulRole == nil)
    }
#endif

    /// A SCRUB IS NOT A BROKEN SPEECH STACK.
    ///
    /// Every scrub and every `stopTranscription` cancels the transcription
    /// task, so on a device whose model is not loaded this catch runs often.
    /// Journaling those would walk `consecutive_failure_count` to
    /// `persistently_failing` on a device nobody has established anything
    /// about — a counter that names an absence, which is precisely why
    /// `asr_failed` was unusable.
    @Test("A load cancelled by the caller is not journaled as a failure")
    func cancelledLoadIsNotJournaledAsAFailure() async {
        let (journal, directory) = makeJournal()
        defer { try? FileManager.default.removeItem(at: directory) }

        let recognizer = CancellationObservingRecognizer()
        let service = SpeechService(recognizer: recognizer, loadJournal: journal)

        // A task that is already cancelled when it reaches the load.
        let task = Task {
            await service.prepareFastModel()
        }
        task.cancel()
        let outcome = await task.value

        #expect(
            outcome == .cancelled,
            "got \(outcome.rawValue) — a cancelled attempt learned nothing and must say so"
        )
        let state = await journal.load()
        #expect(
            state.status == .unknown,
            """
            got \(state.status.rawValue). A cancellation must leave the journal saying nothing \
            was established — recording it as a failure blames the speech stack for a scrub.
            """
        )
        #expect(state.consecutiveFailureCount == 0)
        #expect(state.recentFailures.isEmpty)

        // The budget slot IS consumed, deliberately: the bound must be a
        // guarantee, so it errs toward fewer attempts rather than making the
        // attempt count a function of how often the user scrubs.
        let spent = await service.loadAttemptsInCurrentEpoch
        #expect(spent == 1)
    }

    @Test("A successful load is journaled with the role it established")
    func successIsJournaledWithItsRole() async {
        let (journal, directory) = makeJournal()
        defer { try? FileManager.default.removeItem(at: directory) }

        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: 1)
        let service = SpeechService(recognizer: recognizer, loadJournal: journal)

        _ = await service.prepareFastModel()
        _ = await service.prepareFastModel()

        let state = await journal.load()
        #expect(state.status == .loaded)
        #expect(state.consecutiveFailureCount == 0)
        #expect(state.lastSuccessfulRole == .asrFast)
        #expect(
            state.recentFailures.count == 1,
            "the failure history must survive the recovery — it is the evidence the retry worked"
        )
    }

    /// A signal that only fires when a test calls the recording API
    /// directly is worthless: the hazard is a production path that never
    /// reaches it. This drives the REAL `TranscriptEngineService` loop and
    /// asserts the journal saw the failure.
    @Test(.timeLimit(.minutes(1)))
    func journalRecordsAFailureDrivenByTheRealTranscriptionLoop() async throws {
        let (journal, directory) = makeJournal()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = try await makeTestStore()
        try await store.insertAsset(retryAsset(id: "asset-journal-real-path"))

        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: .max)
        let speech = SpeechService(
            recognizer: recognizer,
            serializesRecognizerRequests: false,
            loadJournal: journal
        )
        // No direct load call anywhere in this test: the launch attempt is
        // simulated by the loop being the first thing to ask for a model.
        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        await engine.startTranscription(
            shards: [retryShard(episodeID: "ep-asset-journal-real-path")],
            analysisAssetId: "asset-journal-real-path",
            snapshot: retrySnapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-journal-real-path")

        let terminal = await awaitRetryTerminal(on: events, assetId: "asset-journal-real-path")
        #expect(
            terminal == .failed(TranscriptFailureReason(failureClass: .speechEngineNotReady)),
            "got \(String(describing: terminal))"
        )

        let state = await journal.load()
        #expect(
            state.status == .retrying,
            """
            The transcription loop ran, failed to get a model, and the journal says \
            \(state.status.rawValue). If this is `unknown`, the recording is wired to a \
            path production does not take — which is exactly the hole this assertion exists \
            to close.
            """
        )
        #expect(state.consecutiveFailureCount == 1)
        #expect(recognizer.loadAttempts == 1, "the loop must attempt the load, once")
    }
}

// MARK: - Recovery without a relaunch

@Suite("playhead-se2h — a transient launch failure recovers without a relaunch")
struct SpeechModelLoadRecoveryTests {

    /// THE CORE ACCEPTANCE CRITERION.
    ///
    /// Two transcription runs against ONE `SpeechService` — the same actor
    /// instance, no relaunch, no reconstruction. The first run happens
    /// while the assets are unavailable and fails by name. The second run
    /// finds them available, loads, and transcribes.
    ///
    /// Before this bead the second run was unreachable: `loadFastModel()`
    /// had exactly one invocation per install and nothing retried it, so
    /// the only way to the second outcome was for the user to relaunch at
    /// a luckier moment.
    @Test(.timeLimit(.minutes(1)))
    func transientLaunchFailureRecoversOnTheNextRun() async throws {
        let (journal, directory) = makeJournal()
        defer { try? FileManager.default.removeItem(at: directory) }

        let store = try await makeTestStore()
        try await store.insertAsset(retryAsset(id: "asset-recover-1"))
        try await store.insertAsset(retryAsset(id: "asset-recover-2"))

        // TWO failures: the launch attempt and run 1. Run 2 is the first
        // attempt that finds the assets available.
        //
        // Not one: with a single scheduled failure the LAUNCH attempt
        // consumes it and run 1 already recovers, which is a weaker test —
        // it never exercises a transcription run that both fails by name
        // AND leaves a usable retry budget behind, and that pair is the
        // whole mechanism. Three attempts total is also exactly the epoch
        // budget, so this doubles as proof the bound is wide enough to
        // reach the recovery it exists to allow.
        let recognizer = TransientlyFailingRecognizer(failuresBeforeSuccess: 2)
        let speech = SpeechService(
            recognizer: recognizer,
            serializesRecognizerRequests: false,
            loadJournal: journal
        )

        // The launch attempt, exactly as `PlayheadRuntime` makes it.
        let launchOutcome = await speech.prepareFastModel()
        #expect(launchOutcome == .failed, "the premise: launch could not load a model")

        let engine = TranscriptEngineService(speechService: speech, store: store)
        let events = await engine.events()

        // Run 1: still broken. Named failure, not silence.
        await engine.startTranscription(
            shards: [retryShard(episodeID: "ep-asset-recover-1")],
            analysisAssetId: "asset-recover-1",
            snapshot: retrySnapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-recover-1")
        let firstTerminal = await awaitRetryTerminal(on: events, assetId: "asset-recover-1")
        #expect(
            firstTerminal == .failed(TranscriptFailureReason(failureClass: .speechEngineNotReady)),
            "got \(String(describing: firstTerminal))"
        )

        // Run 2: SAME service instance. No relaunch, no new SpeechService,
        // no manual load call — only the next scheduled unit of work.
        await engine.startTranscription(
            shards: [retryShard(id: 1, episodeID: "ep-asset-recover-2")],
            analysisAssetId: "asset-recover-2",
            snapshot: retrySnapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-recover-2")
        let secondTerminal = await awaitRetryTerminal(on: events, assetId: "asset-recover-2")
        #expect(
            secondTerminal == .completed,
            """
            got \(String(describing: secondTerminal)). The second run must RECOVER: the \
            recognizer's assets became available and nothing about the device changed \
            except that another unit of work came along.
            """
        )
        #expect(recognizer.transcribeCalls > 0, "recovery means audio was actually transcribed")
        #expect(
            recognizer.loadAttempts == 3,
            "expected the launch attempt plus one per run; got \(recognizer.loadAttempts)"
        )

        // And the journal tells the story a support engineer needs: it
        // failed, then it recovered.
        let state = await journal.load()
        #expect(state.status == .loaded)
        #expect(state.lastSuccessfulRole == .asrFast)
        #expect(state.recentFailures.count == 2, "one launch failure plus one run-1 failure")
        #expect(
            state.consecutiveFailureCount == 0,
            "the counter must clear on recovery, or a recovered device reads as still broken"
        )
    }
}

// MARK: - Local store + event helpers

private let retrySnapshot = PlaybackSnapshot(
    playheadTime: 0, playbackRate: 1.0, isPlaying: true
)

private func retryAsset(id: String) -> AnalysisAsset {
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

/// `TranscriptEngineEvent` is not `Equatable`, so terminals are folded into
/// a comparable shape — the same idiom `TranscriptEngineFailureEventTests`
/// uses.
private enum RetryTerminal: Equatable {
    case completed
    case failed(TranscriptFailureReason)
}

/// Drain the event stream until this asset's terminal event arrives.
/// Unbounded on purpose: each caller carries a `.timeLimit`, and a bounded
/// wait here could only turn a real hang into a confusing pass.
private func awaitRetryTerminal(
    on events: AsyncStream<TranscriptEngineEvent>,
    assetId: String
) async -> RetryTerminal? {
    for await event in events {
        if case .completed(let id) = event, id == assetId { return .completed }
        if case .failed(let id, let reason) = event, id == assetId { return .failed(reason) }
    }
    return nil
}
