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

import Foundation
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
        // wrong on the way in".
        #expect(
            thrown?.description.contains("watchdog deadline") == true,
            "expected the watchdog abandonment error, got \(String(describing: thrown))"
        )

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
