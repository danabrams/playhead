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

    @Test("A waiter cancelled while queued unblocks with CancellationError")
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

    @Test("A cancelled waiter does not release a permit it never held")
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

    @Test("Cancelling one waiter leaves the rest of the queue intact")
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

    @Test("A waiter cancelled after the permit was handed to it releases it")
    func waiterCancelledAfterGrantStillReleases() async throws {
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
        let gate = SpeechRecognitionRequestGate(holderDeadline: .milliseconds(250))
        let wedge = WedgedCall()

        let wedged = Task {
            try await gate.withExclusiveAccess {
                await wedge.park()
                return 1
            }
        }
        await wedge.awaitEntry()

        await #expect(throws: TranscriptEngineError.self) {
            _ = try await wedged.value
        }

        await wedge.unwedge()
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
