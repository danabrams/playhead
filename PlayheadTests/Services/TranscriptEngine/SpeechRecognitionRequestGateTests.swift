// SpeechRecognitionRequestGateTests.swift
// playhead-8m2w: proofs for the two bounds added to the process-wide ASR
// permit in `SpeechRecognitionRequestGate`.
//
// The defect these guard against is not "slow", it is "never returns".
// `SpeechService.requestGate` is a single `static let` shared by the whole
// process, so before this bead one Apple Speech call that never returned left
// `isHeld` true forever and parked every subsequent transcription behind it —
// no timeout on the holder, no exit for a cancelled waiter.
//
// Every test here is written so that a REGRESSION HANGS rather than
// mis-asserts: the assertions are about mutual exclusion and about which task
// completed, and the liveness claim is carried by the test returning at all.
//
// That design only works if the hang is bounded, so every async test carries
// `.timeLimit(.minutes(1))`. Swift Testing has no default per-test timeout —
// without the trait a regression would wedge the whole gate run instead of
// failing one test, and the process-wide gate this file models is exactly the
// kind of thing that wedges.
//
// ONE TEST IS DELIBERATELY NOT WRITTEN THAT WAY.
// `SpeechRecognitionRequestPermitScopeTests` runs against the real
// process-wide `SpeechService.requestGate` rather than a dedicated instance,
// because what it pins is the shipping wiring. Hanging is not an option there:
// a parked call on the SHARED permit stalls every other test in the process
// that transcribes through it, so a regression would look like a suite-wide
// outage instead of one red test. It fails on an assertion, released by a
// bounded valve, and the valve is sized so it can only ever fire in the
// regression case — see the note on `LoadDuringTranscribeRecognizer`.

import Foundation
import os
import Testing
@testable import Playhead

// MARK: - Test support

/// One-shot broadcast signal. Used instead of `Task.sleep` wherever the test
/// needs to observe a specific point in another task's execution, so the
/// proofs do not depend on wall-clock scheduling.
private actor TestSignal {
    private var isSet = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func signal() {
        guard !isSet else { return }
        isSet = true
        let pending = waiters
        waiters = []
        for continuation in pending { continuation.resume() }
    }

    func wait() async {
        guard !isSet else { return }
        await withCheckedContinuation { waiters.append($0) }
    }
}

/// Records how many operations were inside the gate at once. A correct gate
/// never lets `maximum` exceed 1; a permit leaked by a cancelled waiter shows
/// up here as 2.
private actor OverlapTracker {
    private var current = 0
    private(set) var maximum = 0
    private(set) var completed = 0

    func enter() {
        current += 1
        maximum = max(maximum, current)
    }

    func exit() {
        current -= 1
        completed += 1
    }
}

/// Give a queued task a chance to reach its suspension point. Only ever used
/// to *set up* a race, never to assert a deadline — a short sleep that loses
/// the race still exercises a legal ordering (cancellation landing before the
/// waiter parks), which must also work.
private func yieldToScheduler(milliseconds: Int = 120) async {
    try? await Task.sleep(for: .milliseconds(milliseconds))
}

/// Single-writer boolean observation, recorded from inside a gated operation.
private actor ObservedFlag {
    private(set) var isSet = false
    func set() { isSet = true }
}

/// Parks inside `transcribe` — i.e. while the process-wide permit is held —
/// and records whether a `loadModel()` issued in that window got through.
///
/// The safety valve matters: in the regression case nothing else can release
/// the permit, and this is the ONE shared `SpeechService.requestGate` the whole
/// test process transcribes through. Parking on it unboundedly would turn one
/// failing assertion into a stall for every other test that touches the gate.
private final class LoadDuringTranscribeRecognizer: SpeechRecognizer, @unchecked Sendable {
    /// Cap on how long `transcribe` will hold the shared permit waiting for a
    /// reload that a regression would never let through.
    ///
    /// It is ONLY reached in the regression case — on the passing path the
    /// reload resumes `transcribe` directly and this timer is cancelled unused
    /// — so it is sized against the flake, not against the failure. The real
    /// gap it has to cover is one actor hop plus a lock, i.e. microseconds; 20 s
    /// is ~10,000x that, which matters because this box has been observed
    /// starving tasks for over a minute under a full parallel gate. A short
    /// valve firing before a merely-late reload would fail a correct build.
    static let valveDelay: Duration = .seconds(20)

    private let state = OSAllocatedUnfairLock(initialState: State())
    private let released = TestSignal()
    private let entered = TestSignal()

    private struct State {
        var loaded = false
        var loadCount = 0
        var reloadLandedWhileHoldingPermit = false
    }

    /// `true` iff the reload completed WHILE `transcribe` was parked holding
    /// the permit. Snapshotted at wake time on purpose: a regression that
    /// merely queues the reload behind the permit still runs it eventually,
    /// once the valve lets `transcribe` go, and a flag set by `loadModel`
    /// itself would record that late arrival as a pass.
    var reloadLandedWhileHoldingPermit: Bool {
        state.withLock { $0.reloadLandedWhileHoldingPermit }
    }

    /// Suspend until `transcribe` owns the permit.
    func awaitTranscribeEntry() async { await entered.wait() }

    func loadModel() async throws {
        let isReload = state.withLock { state -> Bool in
            state.loaded = true
            state.loadCount += 1
            return state.loadCount > 1
        }
        // The first load is setup — `transcribe` refuses to run without it.
        // Only a RELOAD can land in the parked window.
        guard isReload else { return }
        await released.signal()
    }

    func unloadModel() async { state.withLock { $0.loaded = false } }
    func isModelLoaded() async -> Bool { state.withLock { $0.loaded } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard state.withLock({ $0.loaded }) else { throw TranscriptEngineError.modelNotLoaded }
        await entered.signal()
        let valve = Task { [released] in
            try? await Task.sleep(for: Self.valveDelay)
            await released.signal()
        }
        await released.wait()
        valve.cancel()
        state.withLock { $0.reloadLandedWhileHoldingPermit = $0.loadCount > 1 }
        return []
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] { [] }
}

/// A recognizer call that never returns, modelled the way the real one fails:
/// a `withCheckedContinuation` with no cancellation handler and no resume
/// left. `Task.cancel()` cannot unwind this — that is the entire point.
private actor WedgedCall {
    private var parked: [CheckedContinuation<Void, Never>] = []
    private var entryWaiters: [CheckedContinuation<Void, Never>] = []
    private var entered = 0

    func park() async {
        entered += 1
        let waiting = entryWaiters
        entryWaiters = []
        for continuation in waiting { continuation.resume() }
        await withCheckedContinuation { parked.append($0) }
    }

    func awaitEntry() async {
        guard entered == 0 else { return }
        await withCheckedContinuation { entryWaiters.append($0) }
    }

    /// Let the abandoned call finish at end of test. Not part of any claim —
    /// it exists so a `CheckedContinuation` is never destroyed unresumed,
    /// which would log a `SWIFT TASK CONTINUATION MISUSE` canary and leave a
    /// zombie task behind for the rest of the suite.
    func unwedge() {
        let pending = parked
        parked = []
        for continuation in pending { continuation.resume() }
    }
}

// MARK: - Waiter cancellation

@Suite("SpeechRecognitionRequestGate — waiter cancellation")
struct SpeechRecognitionRequestGateWaiterTests {

    @Test("A waiter cancelled while queued unblocks with CancellationError",
          .timeLimit(.minutes(1)))
    func cancelledWaiterUnblocks() async throws {
        let gate = SpeechRecognitionRequestGate()
        let holderEntered = TestSignal()
        let holderMayFinish = TestSignal()

        let holder = Task {
            try await gate.withExclusiveAccess {
                await holderEntered.signal()
                await holderMayFinish.wait()
                return 1
            }
        }
        await holderEntered.wait()

        let waiter = Task {
            try await gate.withExclusiveAccess { 2 }
        }
        await yieldToScheduler()
        waiter.cancel()

        // Before playhead-8m2w this await never returned: the waiter's
        // continuation had no cancellation path and only `release()` could
        // ever resume it.
        await #expect(throws: CancellationError.self) {
            _ = try await waiter.value
        }

        await holderMayFinish.signal()
        #expect(try await holder.value == 1)
    }

    @Test("A cancelled waiter does not release a permit it never held",
          .timeLimit(.minutes(1)))
    func cancelledWaiterDoesNotReleaseThePermit() async throws {
        let gate = SpeechRecognitionRequestGate()
        let tracker = OverlapTracker()
        let holderEntered = TestSignal()
        let holderMayFinish = TestSignal()

        let holder = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await holderEntered.signal()
                await holderMayFinish.wait()
                await tracker.exit()
                return 1
            }
        }
        await holderEntered.wait()

        let doomed = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await tracker.exit()
                return 2
            }
        }
        await yieldToScheduler()
        doomed.cancel()
        await #expect(throws: CancellationError.self) {
            _ = try await doomed.value
        }

        // The holder is still inside its operation. If the cancelled waiter
        // had released the permit it never owned, this second waiter would
        // run concurrently with the holder and `maximum` would reach 2.
        let second = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await tracker.exit()
                return 3
            }
        }
        await yieldToScheduler()
        #expect(await tracker.maximum == 1)
        #expect(await tracker.completed == 0)

        await holderMayFinish.signal()
        #expect(try await holder.value == 1)
        #expect(try await second.value == 3)
        #expect(await tracker.maximum == 1)
    }

    @Test("Cancelling one waiter leaves the rest of the queue intact",
          .timeLimit(.minutes(1)))
    func cancellingOneWaiterDoesNotStrandTheOthers() async throws {
        let gate = SpeechRecognitionRequestGate()
        let tracker = OverlapTracker()
        let holderEntered = TestSignal()
        let holderMayFinish = TestSignal()

        let holder = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await holderEntered.signal()
                await holderMayFinish.wait()
                await tracker.exit()
                return 0
            }
        }
        await holderEntered.wait()

        var survivors: [Task<Int, Error>] = []
        for index in 1...4 {
            let survivor = Task {
                try await gate.withExclusiveAccess {
                    await tracker.enter()
                    await tracker.exit()
                    return index
                }
            }
            survivors.append(survivor)
        }
        let doomed = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await tracker.exit()
                return 99
            }
        }
        await yieldToScheduler()
        doomed.cancel()
        await #expect(throws: CancellationError.self) {
            _ = try await doomed.value
        }

        await holderMayFinish.signal()
        _ = try await holder.value
        var seen: Set<Int> = []
        for survivor in survivors {
            seen.insert(try await survivor.value)
        }
        #expect(seen == [1, 2, 3, 4])
        #expect(await tracker.maximum == 1)
    }

    // NAME SAYS ONLY WHAT IT PROVES. An earlier name — "a waiter cancelled
    // after the permit was handed to it releases it" — claimed one specific
    // ordering, and this test cannot force one: no seam places a `cancel()`
    // between `release()`'s hand-off and the waiter waking. Measured in round
    // 2: with the whole waiter-cancellation handler deleted, the three tests
    // above time out and this one still passes, because the racer then simply
    // waits its turn. What it does prove — in either order, which is the point
    // — is that the gate is still usable afterwards.
    @Test("A cancel racing the permit hand-off leaves the gate usable either way",
          .timeLimit(.minutes(1)))
    func cancelRacingTheHandOffLeavesTheGateUsable() async throws {
        let gate = SpeechRecognitionRequestGate()
        let tracker = OverlapTracker()
        let holderEntered = TestSignal()
        let holderMayFinish = TestSignal()

        let holder = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await holderEntered.signal()
                await holderMayFinish.wait()
                await tracker.exit()
                return 0
            }
        }
        await holderEntered.wait()

        // Cancel this waiter and hand it the permit in the same instant. It
        // may wake either owning the permit (and must give it back) or having
        // abandoned it (and must not release one) — both orders must leave
        // the gate usable, which the follow-up acquire proves.
        let racer = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await tracker.exit()
                return 1
            }
        }
        await yieldToScheduler()
        racer.cancel()
        await holderMayFinish.signal()
        _ = try await holder.value
        _ = try? await racer.value

        let after = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await tracker.exit()
                return 2
            }
        }
        #expect(try await after.value == 2)
        #expect(await tracker.maximum == 1)
    }
}

// MARK: - Holder watchdog

@Suite("SpeechRecognitionRequestGate — holder watchdog")
struct SpeechRecognitionRequestGateWatchdogTests {

    @Test("A holder that never returns is abandoned instead of holding forever",
          .timeLimit(.minutes(1)))
    func wedgedHolderIsAbandoned() async throws {
        let deadline: Duration = .milliseconds(250)
        let gate = SpeechRecognitionRequestGate(holderDeadline: deadline)
        let wedge = WedgedCall()

        // Read BEFORE the holder task exists, so it strictly precedes the
        // instant the watchdog timer is armed. That ordering is what makes the
        // lower bound below sound rather than approximate.
        let start = ContinuousClock.now

        let wedged = Task {
            try await gate.withExclusiveAccess {
                await wedge.park()
                return 1
            }
        }
        await wedge.awaitEntry()

        let thrown = await #expect(throws: TranscriptEngineError.self) {
            _ = try await wedged.value
        }
        // WHICH error, not merely which type. `TranscriptEngineError` has four
        // cases and the type check accepts all of them, so on its own it does
        // not distinguish "the watchdog fired" from "something unrelated went
        // wrong on the way in". Pin the case AND the reason: the case is what
        // callers match on, the reason is the operator-facing artifact that
        // says the permit was freed by abandonment rather than by completion.
        if case .transcriptionFailed(let reason) = try #require(thrown) {
            #expect(reason.contains("watchdog deadline"), "unexpected reason: \(reason)")
        } else {
            Issue.record("expected .transcriptionFailed, got \(String(describing: thrown))")
        }

        // THE DEADLINE IS A SCALE, NOT A FLAG. Nothing else in this file reads
        // the clock, so before this line the watchdog could have been wired to
        // any fraction of `holderDeadline` and every test still passed —
        // measured, not assumed: `timeout: deadline / 4` survived the whole
        // suite. That mutation is not academic. The 120 s value is justified in
        // `SpeechRecognitionRequestGate` by being 4x a shard's own wall-clock
        // duration; silently quartering it puts the watchdog *at* one shard's
        // duration, where a merely-slow call is abandoned routinely and the
        // cost stops being one shard's coverage.
        //
        // A LOWER bound is the only load-safe form: `Task.sleep` guarantees at
        // least its duration and contention can only push the fire later, so
        // this can never flake, while any under-scaled timeout fails it
        // deterministically.
        #expect(ContinuousClock.now - start >= deadline)

        await wedge.unwedge()
    }

    @Test("An abandoned call is cancelled, so it stops competing for the recognizer",
          .timeLimit(.minutes(1)))
    func abandonedCallIsCancelled() async throws {
        let gate = SpeechRecognitionRequestGate(holderDeadline: .milliseconds(250))
        let entered = TestSignal()
        let noticedCancellation = TestSignal()

        // The other half of the pair `WedgedCall` models. A wedged call cannot
        // be unwound at all; a merely-slow one can, and the watchdog's job on
        // the way out is to tell it to stop.
        let holder = Task {
            try await gate.withExclusiveAccess {
                await entered.signal()
                // Long enough that only cancellation can end it inside the
                // test's time limit.
                try? await Task.sleep(for: .seconds(30))
                if Task.isCancelled { await noticedCancellation.signal() }
                return 1
            }
        }
        await entered.wait()

        await #expect(throws: TranscriptEngineError.self) {
            _ = try await holder.value
        }

        // Nothing in this test cancels `holder`, so the only route to this
        // signal is the `work.cancel()` on the abandonment branch. That cancel
        // is exactly what the 120 s calibration leans on when it argues the
        // window in which an abandoned call can still overlap the *next*
        // recognizer request is short — the `SFSpeechErrorDomain Code=16`
        // overlap this gate exists to prevent. Measured: deleting that one line
        // left every other test in this file green.
        //
        // Awaiting the signal is the liveness claim; `.timeLimit` is the
        // backstop, so a regression hangs here rather than mis-asserting.
        await noticedCancellation.wait()
    }

    @Test("A never-finishing holder does not wedge a subsequent acquire",
          .timeLimit(.minutes(1)))
    func wedgedHolderDoesNotWedgeALaterAcquire() async throws {
        let gate = SpeechRecognitionRequestGate(holderDeadline: .milliseconds(250))
        let wedge = WedgedCall()

        let wedged = Task {
            try await gate.withExclusiveAccess {
                await wedge.park()
                return 1
            }
        }
        await wedge.awaitEntry()

        // Queued BEFORE the watchdog fires: this is the waiter that used to be
        // parked for the lifetime of the process.
        let queuedBehind = Task {
            try await gate.withExclusiveAccess { 2 }
        }
        #expect(try await queuedBehind.value == 2)

        // And a caller that arrives AFTER the abandonment must also get in.
        let arrivingLater = Task {
            try await gate.withExclusiveAccess { 3 }
        }
        #expect(try await arrivingLater.value == 3)

        _ = try? await wedged.value
        await wedge.unwedge()
    }

    @Test("The watchdog does not fire for a call that merely takes a while",
          .timeLimit(.minutes(1)))
    func slowButFinishingCallIsNotAbandoned() async throws {
        let gate = SpeechRecognitionRequestGate(holderDeadline: .seconds(30))
        let mayFinish = TestSignal()
        let entered = TestSignal()

        let holder = Task {
            try await gate.withExclusiveAccess {
                await entered.signal()
                await mayFinish.wait()
                return 7
            }
        }
        await entered.wait()
        await yieldToScheduler()
        await mayFinish.signal()
        #expect(try await holder.value == 7)
    }

    @Test("Cancellation reaches the recognizer call and the permit is not freed until it unwinds",
          .timeLimit(.minutes(1)))
    func cancellationIsForwardedAndAwaited() async throws {
        let gate = SpeechRecognitionRequestGate(holderDeadline: .seconds(30))
        let tracker = OverlapTracker()
        let entered = TestSignal()
        let unwinding = TestSignal()
        let mayUnwind = TestSignal()
        let observedCancellation = ObservedFlag()

        let holder = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await entered.signal()
                // Returns as soon as the operation itself is cancelled. The
                // operation runs in an unstructured task, so this only fires
                // if the gate forwarded cancellation into it.
                try? await Task.sleep(for: .seconds(10))
                if Task.isCancelled { await observedCancellation.set() }
                await unwinding.signal()
                await mayUnwind.wait()
                await tracker.exit()
                throw CancellationError()
            }
        }
        await entered.wait()
        holder.cancel()
        await unwinding.wait()
        #expect(await observedCancellation.isSet)

        // The operation is still unwinding. Releasing the permit now would
        // let a second recognizer request overlap it, which is the
        // `SFSpeechErrorDomain Code=16` this gate exists to prevent.
        let next = Task {
            try await gate.withExclusiveAccess {
                await tracker.enter()
                await tracker.exit()
                return 1
            }
        }
        await yieldToScheduler()
        #expect(await tracker.maximum == 1)

        await mayUnwind.signal()
        await #expect(throws: CancellationError.self) {
            _ = try await holder.value
        }
        #expect(try await next.value == 1)
        #expect(await tracker.maximum == 1)
    }

    @Test("The shipping deadline bounds a hung call without bounding a slow one")
    func shippingDeadlineIsCalibrated() {
        // A shard is 30 s of audio and the tightest caller
        // (`AnalysisJobRunner`) gives the whole transcription stage 300 s.
        // The per-call ceiling has to sit strictly between "much longer than a
        // shard" and "well inside the stage budget" or it protects nothing.
        let deadline = SpeechRecognitionRequestGate.defaultHolderDeadline
        #expect(deadline >= .seconds(4 * AnalysisAudioService.defaultShardDuration))
        #expect(deadline < .seconds(300))
    }
}

// MARK: - What the permit covers

/// The 120 s ceiling is argued from the size of ONE `SpeechAnalyzer` session,
/// and that argument has a second premise besides the shard duration: locale
/// asset installation runs in `loadModel()` → `AppleSpeechAssetBootstrapper`,
/// which the permit does not wrap, "so a cold first launch cannot spend the
/// budget". A first launch that had to download an asset would blow straight
/// past 120 s, so if model loading were ever routed through
/// `performRecognizerRequest` the watchdog would abandon legitimate installs
/// and the calibration would be wrong — not merely undocumented.
///
/// Round 4 found that half of the calibration unpinned while the other half
/// (`defaultShardDuration`) was pinned by `shippingDeadlineIsCalibrated`.
///
/// This suite also exercises the SHIPPING wiring rather than a dedicated gate:
/// `SpeechService(recognizer:)` defaults `serializesRecognizerRequests` to
/// true, so the permit here is the one process-wide `SpeechService.requestGate`
/// that production transcribes through.
@Suite("SpeechRecognitionRequestGate — what the permit covers")
struct SpeechRecognitionRequestPermitScopeTests {

    @Test("Model loading is outside the permit, so a cold asset install cannot spend the budget",
          .timeLimit(.minutes(1)))
    func modelLoadingIsNotSerializedBehindThePermit() async throws {
        let recognizer = LoadDuringTranscribeRecognizer()
        let service = SpeechService(recognizer: recognizer)
        try await service.loadFastModel()

        let transcribing = Task {
            try await service.transcribe(
                shard: makeShard(id: 0, startTime: 0, duration: 30)
            )
        }
        // Not a sleep: `transcribe` signals from inside the operation, so this
        // returns only once the permit is genuinely held.
        await recognizer.awaitTranscribeEntry()

        // A second load while the permit is held. Ungated, it lands and
        // releases the parked call. Gated, it queues behind the permit and
        // only the valve gets `transcribe` moving again — after which this
        // call still returns, which is why the assertion below reads a
        // snapshot taken at wake time rather than the fact of the load.
        try await service.loadFastModel()
        _ = try await transcribing.value

        #expect(
            recognizer.reloadLandedWhileHoldingPermit,
            "loadModel() is queued behind the recognizer permit — a cold locale-asset install would now be raced against the 120 s watchdog"
        )
    }
}
