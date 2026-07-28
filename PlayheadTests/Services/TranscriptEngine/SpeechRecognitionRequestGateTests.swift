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
