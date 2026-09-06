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

/// Awaits `op` for at most `seconds`; nil means it did not complete. A mutex
/// that never releases, or a waiter that is never cancelled, must read as a
/// RED assertion — not as a test that waits forever (the first battery run of
/// playhead-2gka hung for six hours on exactly that).
private func within<T: Sendable>(_ seconds: Double, _ op: @escaping @Sendable () async -> T) async -> T? {
    await withTaskGroup(of: T?.self) { group in
        group.addTask { await op() }
        group.addTask { try? await Task.sleep(for: .seconds(seconds)); return nil }
        let first = await group.next() ?? nil
        group.cancelAll()
        return first
    }
}

@Suite("PlaybackLifecycleMutex (playhead-2gka)", .timeLimit(.minutes(1)))
struct PlaybackLifecycleMutexTests {
    @Test("withLock runs the body once, holds the lock during it, and releases after")
    func withLockHoldsAndReleases() async {
        let mutex = PlaybackLifecycleMutex()
        let ran = await within(2) { await mutex.withLock { () async -> Bool in await mutex.isLockedForTesting() } }
        #expect(ran == .some(true))
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
        for (name, task) in [("holder", holder), ("second", second), ("third", third)] {
            #expect(await within(3) { await task.value } != nil, "\(name) never finished: the lock was not handed on")
        }
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
        let cancelledJoin = await within(2) { await cancelled.value }
        #expect(cancelledJoin != nil, "the cancelled waiter never returned: cancellation is not observed")
        #expect(cancelledJoin == .some(nil), "a cancelled wait returns nil without running the body")
        #expect(await within(3) { await holder.value } != nil, "the holder never finished")
        #expect(await within(3) { await third.value } != nil, "the next waiter never got the lock")
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
        let joined = await within(2) { await task.value }
        #expect(joined != nil, "the task never returned")
        #expect(joined == .some(nil))
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
        let join = await within(2) { await parked.value }
        let joined = ContinuousClock.now - joinStarted
        await gate.append("release-holder")   // let the holder finish whatever the join did
        #expect(join != nil, "the join waited on the holder, not on the cancellation")
        #expect(joined < .seconds(1), "the join waited on the holder, not on the cancellation: \(joined)")
        #expect(await mutex.waiterCountForTesting() == 0)
        #expect(await within(3) { await holder.value } != nil, "the holder never finished")
        #expect(await gate.entries.contains("parked-ran") == false)
    }
}

private actor OrderLog {
    var entries: [String] = []
    func append(_ s: String) { entries.append(s) }
}
