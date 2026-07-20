// AsyncTestSignals.swift
// playhead-vsot round 3: shared event-driven test primitives that
// replace deadline-poll / fixed-sleep waits in the contention-flake
// families. Every wait here suspends on the ACTUAL completion signal
// (deallocation, or an async write returning) with NO wall-clock
// deadline — the test's `.timeLimit(.minutes(1))` trait is the hang
// backstop, so a genuine regression fails deterministically instead of
// load-dependently.

import Foundation
import ObjectiveC
@testable import Playhead

// MARK: - DeallocLatch

/// Event-driven deallocation signal. A `DeallocSentinel` is attached to
/// the target object via an associated object; when the target
/// deallocates, the sentinel is released with it and its `deinit` fires
/// the latch, resuming any waiter exactly at the moment of deallocation.
///
/// This is the same mechanism that fixed the RuntimeShutdown deinit
/// flake in round 1 (the old fixed `Task.yield`/`Task.sleep` budget kept
/// the executor busy and lost the release race under load). Thread-safe:
/// deallocation may land on any thread.
final class DeallocLatch: @unchecked Sendable {
    private let lock = NSLock()
    private var fired = false
    private var continuations: [CheckedContinuation<Void, Never>] = []

    func signal() {
        lock.lock()
        if fired {
            lock.unlock()
            return
        }
        fired = true
        let waiters = continuations
        continuations = []
        lock.unlock()
        for continuation in waiters {
            continuation.resume()
        }
    }

    /// Suspend until the attached object deallocates. Returns immediately
    /// if it already has. No deadline.
    func wait() async {
        if hasFired() { return }
        await withCheckedContinuation { continuation in
            register(continuation)
        }
    }

    // NSLock lock()/unlock() are unavailable inside async funcs, so the
    // locking work lives in synchronous helpers.
    private func hasFired() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return fired
    }

    private func register(_ continuation: CheckedContinuation<Void, Never>) {
        lock.lock()
        if fired {
            lock.unlock()
            continuation.resume()
            return
        }
        continuations.append(continuation)
        lock.unlock()
    }
}

private final class DeallocSentinel {
    private let latch: DeallocLatch
    init(latch: DeallocLatch) { self.latch = latch }
    deinit { latch.signal() }
}

private nonisolated(unsafe) var deallocSentinelKey: UInt8 = 0

/// Attach a dealloc latch to `object`. When `object` is released, the
/// returned latch's `wait()` resumes. `object` must be a class instance
/// that permits associated objects (any `NSObject`, or a Swift class
/// bridged to ObjC — `PlaybackService` qualifies as an `NSObject`
/// subclass). Call while you still hold a strong reference.
@discardableResult
func attachDeallocLatch(to object: AnyObject) -> DeallocLatch {
    let latch = DeallocLatch()
    objc_setAssociatedObject(
        object,
        &deallocSentinelKey,
        DeallocSentinel(latch: latch),
        .OBJC_ASSOCIATION_RETAIN
    )
    return latch
}

// MARK: - SignalingCorrectionStore

/// Test decorator over `any UserCorrectionStore` that forwards every
/// call to a wrapped store and fires a `TestEventCounter` after each
/// `recordVeto(startTime:...)` completes.
///
/// playhead-vsot round 3: `SkipOrchestrator.recordListenRevert` persists
/// the veto in a fire-and-forget `Task { await store.recordVeto(...) }`,
/// so `recordListenRevert` returns before the write lands. The old tests
/// polled `activeCorrections` under a 5 s deadline — the same
/// fire-and-forget-write-then-short-poll class that flaked the download
/// harvest under the parallel gate. Wrapping the store lets the test
/// await the write's actual completion, then read results through the
/// underlying concrete store. Pure test code; no production change.
final class SignalingCorrectionStore: UserCorrectionStore, @unchecked Sendable {
    private let wrapped: any UserCorrectionStore
    let vetoRecorded: TestEventCounter

    init(wrapping wrapped: any UserCorrectionStore, vetoRecorded: TestEventCounter) {
        self.wrapped = wrapped
        self.vetoRecorded = vetoRecorded
    }

    func recordVeto(span: DecodedSpan) async {
        await wrapped.recordVeto(span: span)
        vetoRecorded.increment()
    }

    func recordVeto(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource
    ) async {
        await wrapped.recordVeto(
            startTime: startTime,
            endTime: endTime,
            assetId: assetId,
            podcastId: podcastId,
            source: source
        )
        vetoRecorded.increment()
    }

    func record(_ event: CorrectionEvent) async throws {
        try await wrapped.record(event)
    }

    func correctionPassthroughFactor(for analysisAssetId: String) async -> Double {
        await wrapped.correctionPassthroughFactor(for: analysisAssetId)
    }

    func correctionBoostFactor(for analysisAssetId: String) async -> Double {
        await wrapped.correctionBoostFactor(for: analysisAssetId)
    }

    func correctionBoostFactor(
        for analysisAssetId: String,
        overlapping startTime: Double,
        endTime: Double
    ) async -> Double {
        await wrapped.correctionBoostFactor(
            for: analysisAssetId,
            overlapping: startTime,
            endTime: endTime
        )
    }

    func activeFalsePositiveScopes(for analysisAssetId: String) async -> [CorrectionScope] {
        await wrapped.activeFalsePositiveScopes(for: analysisAssetId)
    }
}
