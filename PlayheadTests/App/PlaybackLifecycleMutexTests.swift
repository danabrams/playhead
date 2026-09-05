//
//  PlaybackLifecycleMutexTests.swift
//  PlayheadTests
//
//  playhead-2gka: the playback lifecycle mutex observes cancellation and
//  releases on every exit. Before this bead a task parked in `acquire` could
//  not be cancelled (no handler), so `shutdown()`'s join on a parked lifecycle
//  task waited on a holder that might never return.
//

import Foundation
import Testing
@testable import Playhead

@Suite("PlaybackLifecycleMutex (playhead-2gka)")
struct PlaybackLifecycleMutexTests {
    @Test("withLock runs the body once, holds the lock during it, and releases after")
    func withLockHoldsAndReleases() async {
        let mutex = PlaybackLifecycleMutex()
        let ran = await mutex.withLock { () async -> Bool in
            await mutex.isLockedForTesting()
        }
        #expect(ran == true)
        #expect(await mutex.isLockedForTesting() == false)
    }

    @Test("waiters are served FIFO, one at a time")
    func fifoHandoff() async {
        let mutex = PlaybackLifecycleMutex()
        let order = OrderLog()
        let holder = Task { await mutex.withLock { await order.append("A"); try? await Task.sleep(for: .milliseconds(60)) } }
        try? await Task.sleep(for: .milliseconds(10))
        let second = Task { await mutex.withLock { await order.append("B") } }
        try? await Task.sleep(for: .milliseconds(10))
        let third = Task { await mutex.withLock { await order.append("C") } }
        _ = await holder.value; _ = await second.value; _ = await third.value
        #expect(await order.entries == ["A", "B", "C"])
        #expect(await mutex.isLockedForTesting() == false)
    }

    @Test("a parked waiter that is cancelled never holds the lock; the next waiter does")
    func cancelledWaiterIsSkipped() async {
        let mutex = PlaybackLifecycleMutex()
        let order = OrderLog()
        let holder = Task { await mutex.withLock { await order.append("A"); try? await Task.sleep(for: .milliseconds(80)) } }
        try? await Task.sleep(for: .milliseconds(10))
        let cancelled = Task { await mutex.withLock { await order.append("B-must-not-run") } }
        try? await Task.sleep(for: .milliseconds(10))
        #expect(await mutex.waiterCountForTesting() == 1)
        let third = Task { await mutex.withLock { await order.append("C") } }
        try? await Task.sleep(for: .milliseconds(10))
        cancelled.cancel()
        let cancelledResult = await cancelled.value
        #expect(cancelledResult == nil, "a cancelled wait returns nil without running the body")
        _ = await holder.value; _ = await third.value
        #expect(await order.entries == ["A", "C"])
        #expect(await mutex.isLockedForTesting() == false)
    }

    @Test("a task cancelled BEFORE it asks for the lock gets nil, not the lock")
    func cancelledBeforeAcquire() async {
        let mutex = PlaybackLifecycleMutex()
        let task = Task { () async -> Bool? in
            try? await Task.sleep(for: .milliseconds(40))
            return await mutex.withLock { true }
        }
        task.cancel()
        #expect(await task.value == nil)
        #expect(await mutex.isLockedForTesting() == false)
    }

    @Test("cancelling a parked waiter unblocks a join on it promptly — the shutdown shape")
    func cancelledParkedWaiterJoinsPromptly() async {
        let mutex = PlaybackLifecycleMutex()
        let gate = OrderLog()
        // A holder that returns only when told to.
        let holder = Task { await mutex.withLock { while await gate.entries.isEmpty { try? await Task.sleep(for: .milliseconds(5)) } } }
        try? await Task.sleep(for: .milliseconds(10))
        let parked = Task { await mutex.withLock { await gate.append("parked-ran") } }
        try? await Task.sleep(for: .milliseconds(10))
        #expect(await mutex.waiterCountForTesting() == 1)
        let joinStarted = ContinuousClock.now
        parked.cancel()
        _ = await parked.value
        let joined = ContinuousClock.now - joinStarted
        #expect(joined < .seconds(1), "the join waited on the holder, not on the cancellation: \(joined)")
        #expect(await mutex.waiterCountForTesting() == 0)
        await gate.append("release-holder")
        _ = await holder.value
        #expect(await gate.entries.contains("parked-ran") == false)
    }
}

private actor OrderLog {
    var entries: [String] = []
    func append(_ s: String) { entries.append(s) }
}
