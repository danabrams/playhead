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

// MARK: - Test gate

/// A one-shot gate that any number of tasks can wait on and that opens for
/// all of them at once.
///
/// NOT an `AsyncStream`. The first version of these doubles parked callers
/// with `for await _ in stream { break }`, which HUNG the suite: `AsyncStream`
/// supports a single consumer, so concurrent iterators compete for elements
/// and a `yield`/`finish` does not reliably wake every waiter. A continuation
/// list is the correct primitive for a broadcast, and it is deterministic —
/// no polling, no sleeping, no timing assumption.
private actor SpeechLoadTestGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        if isOpen { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        guard !isOpen else { return }
        isOpen = true
        let pending = waiters
        waiters = []
        for continuation in pending { continuation.resume() }
    }
}

/// Counts arrivals and lets a test wait until a target is reached.
private actor SpeechLoadArrivalLatch {
    private var count = 0
    private var target: Int?
    private var waiter: CheckedContinuation<Void, Never>?

    func arrive() {
        count += 1
        resumeIfSatisfied()
    }

    func waitUntil(_ wanted: Int) async {
        if count >= wanted { return }
        target = wanted
        await withCheckedContinuation { waiter = $0 }
    }

    private func resumeIfSatisfied() {
        guard let target, count >= target, let pending = waiter else { return }
        waiter = nil
        pending.resume()
    }
}

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

/// Recognizer whose `loadModel()` parks until the test releases it, then
/// throws if the calling task was cancelled meanwhile — the shape a scrub
/// or `stopTranscription` produces while a load is in flight.
///
/// The park is what makes the cancellation DETERMINISTIC. `Task {}` starts
/// eagerly, so a test that merely calls `cancel()` after creating the task
/// is racing it: on a loaded machine the load can complete first and the
/// assertion flips. Here the test releases the load only after `cancel()`
/// has landed, so there is no ordering to lose.
///
/// It wraps the cancellation in `TranscriptEngineError.transcriptionFailed`
/// ON PURPOSE: that is what `AppleSpeechRecognizer.loadModel()` does to
/// anything it does not recognise, so a double that threw a bare
/// `CancellationError` would test a shape production never produces — and
/// would let a `error is CancellationError` implementation pass.
private final class CancellationObservingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let state = OSAllocatedUnfairLock(initialState: 0)
    private let arrivals = SpeechLoadArrivalLatch()
    private let gate = SpeechLoadTestGate()

    var loadAttempts: Int { state.withLock { $0 } }

    func waitUntilLoadEntered() async { await arrivals.waitUntil(1) }

    func releaseLoad() { Task { await gate.open() } }

    func loadModel() async throws {
        state.withLock { $0 += 1 }
        await arrivals.arrive()
        await gate.wait()
        guard !Task.isCancelled else {
            throw TranscriptEngineError.transcriptionFailed("cancelled: \(CancellationError())")
        }
    }

    func unloadModel() async {}
    func isModelLoaded() async -> Bool { false }
    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] { [] }
    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] { [] }
}

/// Recognizer that parks inside `isModelLoaded()` — the suspension point
/// `prepareFastModel` awaits BEFORE its budget block.
///
/// This is the double that makes the atomicity claim testable: it holds
/// every concurrent caller at exactly the reentrancy window, so an
/// implementation whose budget check and consumption were separated by a
/// suspension would demonstrably overspend.
private final class SuspendingReadinessRecognizer: SpeechRecognizer, @unchecked Sendable {
    private struct State {
        var loaded = false
        var loadAttempts = 0
    }

    private let state = OSAllocatedUnfairLock(initialState: State())
    private let arrivals = SpeechLoadArrivalLatch()
    private let gate = SpeechLoadTestGate()

    var loadAttempts: Int { state.withLock { $0.loadAttempts } }

    /// Suspends until `count` callers are parked inside `isModelLoaded()`.
    func waitUntilReadinessCallsReach(_ count: Int) async { await arrivals.waitUntil(count) }

    func releaseAllReadinessCalls() { Task { await gate.open() } }

    func isModelLoaded() async -> Bool {
        await arrivals.arrive()
        // Park EVERY caller here — the suspension point `prepareFastModel`
        // awaits immediately before its budget block — until the test opens
        // the gate for all of them at once.
        await gate.wait()
        return state.withLock { $0.loaded }
    }

    func loadModel() async throws {
        state.withLock { $0.loadAttempts += 1 }
        throw TranscriptEngineError.transcriptionFailed("assets unavailable")
    }

    func unloadModel() async { state.withLock { $0.loaded = false } }
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
    private let arrivals = SpeechLoadArrivalLatch()
    private let gate = SpeechLoadTestGate()

    var loadAttempts: Int { state.withLock { $0.loadAttempts } }

    /// Suspends until `count` callers are parked inside `loadModel()`.
    func waitUntilLoadEntered(count: Int = 1) async { await arrivals.waitUntil(count) }

    /// Releases every parked load. Plural matters: the stale-supersede test
    /// has TWO callers inside `loadModel()` at once, and an AsyncStream
    /// `yield` would wake only one of them and hang the test.
    func releaseLoad() { Task { await gate.open() } }

    func loadModel() async throws {
        state.withLock { $0.loadAttempts += 1 }
        await arrivals.arrive()
        await gate.wait()
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

    /// The bound must survive REENTRANCY. `prepareFastModel` awaits
    /// `recognizer.isModelLoaded()` before its budget block, so a budget
    /// check separated from its consumption by a suspension point would let
    /// concurrent callers both pass the guard and overspend.
    ///
    /// `SuspendingReadinessRecognizer` is what makes that window real: it
    /// parks INSIDE `isModelLoaded()`, so all 32 callers pile up at exactly
    /// the suspension point the atomicity claim is about. A recognizer whose
    /// `isModelLoaded()` returns immediately never opens the window, and the
    /// test would pass against a non-atomic implementation.
    @Test("Concurrent callers cannot overspend the budget", .timeLimit(.minutes(1)))
    func concurrentCallersCannotOverspendTheBudget() async {
        let recognizer = SuspendingReadinessRecognizer()
        let service = SpeechService(recognizer: recognizer)

        await withTaskGroup(of: SpeechModelLoadOutcome.self) { group in
            for _ in 0..<32 {
                group.addTask { await service.prepareFastModel() }
            }
            // Let every caller reach the suspension inside isModelLoaded()
            // before any of them is allowed past it.
            await recognizer.waitUntilReadinessCallsReach(32)
            recognizer.releaseAllReadinessCalls()
            for await _ in group {}
        }

        #expect(
            recognizer.loadAttempts <= SpeechService.maxLoadAttemptsPerEpoch,
            "32 concurrent callers produced \(recognizer.loadAttempts) attempts — the bound is not atomic"
        )
        // Without this, an implementation that never loads at all — or one
        // whose in-flight latch wedges permanently — passes the assertion
        // above trivially.
        #expect(
            recognizer.loadAttempts >= 1,
            "no attempt was made at all; the bound is satisfied vacuously"
        )
    }

    /// THE LATCH MUST NOT OUTLIVE THE LOAD IT GUARDS.
    ///
    /// `loadInFlightSince` makes a second caller decline so a slow asset
    /// download is never joined by a duplicate. But nothing beneath
    /// `loadFastModel` is bounded — `AssetInventory`'s
    /// `downloadAndInstall()` is an unbounded network download — so as a
    /// plain boolean latch it was never cleared when a load simply never
    /// returned. Every later run got `.loadInFlight`, the foreground refresh
    /// cleared the budget but not the latch, and the journal recorded
    /// neither success nor failure: permanent, silent, unretried,
    /// undiagnosable. Exactly the four properties this bead removes,
    /// reintroduced by its own fix.
    @Test("A load that never returns does not wedge every later attempt", .timeLimit(.minutes(1)))
    func aStalledLoadIsSupersededRatherThanWedgingTheActor() async {
        let recognizer = SuspendingLoadRecognizer()
        let clock = OSAllocatedUnfairLock(initialState: ContinuousClock.now)
        let service = SpeechService(
            recognizer: recognizer,
            loadJournal: nil,
            now: { clock.withLock { $0 } }
        )

        // A load that parks forever — the stalled download.
        let stalled = Task { await service.prepareFastModel() }
        await recognizer.waitUntilLoadEntered()

        // While it is plausibly still running, a second caller declines.
        #expect(await service.prepareFastModel() == .loadInFlight)
        #expect(recognizer.loadAttempts == 1)

        // Once it has been in flight longer than any real install could be,
        // the latch must no longer speak for it.
        clock.withLock { $0 = $0.advanced(by: SpeechService.staleInFlightLoadThreshold) }
        let superseding = Task { await service.prepareFastModel() }

        // The superseding caller parks in `loadModel()` too, so its arrival
        // is the proof that it got PAST the latch. Waiting on the arrival —
        // rather than on the call's return value — is what keeps this
        // deterministic: a wedged implementation never arrives and the
        // suite's time limit reports it, instead of the test hanging on a
        // value that will never come.
        await recognizer.waitUntilLoadEntered(count: 2)
        #expect(
            recognizer.loadAttempts == 2,
            """
            a load in flight for over \(SpeechService.staleInFlightLoadThreshold) still blocks \
            every new attempt — the guard has outlived the load, so this device can never \
            transcribe again without being force-quit
            """
        )

        recognizer.releaseLoad()
        #expect(await superseding.value != .loadInFlight)
        _ = await stalled.value
    }

    /// A caller that arrives mid-load must DECLINE, not queue behind an
    /// asset download it cannot cancel, and must not spend a budget slot
    /// for an attempt it did not make.
    @Test("A caller arriving mid-load declines without consuming the budget", .timeLimit(.minutes(1)))
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
        let spent = await service.loadAttemptsInCurrentEpochForTesting
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
        let afterFailure = await service.loadAttemptsInCurrentEpochForTesting
        #expect(afterFailure == 1, "a failure must leave its slot spent")

        #expect(await service.prepareFastModel() == .loaded)
        let afterSuccess = await service.loadAttemptsInCurrentEpochForTesting
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
        let spent = await service.loadAttemptsInCurrentEpochForTesting
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
    @Test("A load cancelled by the caller is not journaled as a failure", .timeLimit(.minutes(1)))
    func cancelledLoadIsNotJournaledAsAFailure() async {
        let (journal, directory) = makeJournal()
        defer { try? FileManager.default.removeItem(at: directory) }

        let recognizer = CancellationObservingRecognizer()
        let service = SpeechService(recognizer: recognizer, loadJournal: journal)

        // Deterministic ordering: park the load, cancel, THEN release. A
        // bare `Task { … }; task.cancel()` races the eagerly-started task
        // and passes only when cancellation happens to win.
        let task = Task { await service.prepareFastModel() }
        await recognizer.waitUntilLoadEntered()
        task.cancel()
        recognizer.releaseLoad()
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
        let spent = await service.loadAttemptsInCurrentEpochForTesting
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
