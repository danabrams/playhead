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

/// Test decorator over `any UserCorrectionStore` that fires a deterministic
/// signal after either legacy veto persistence or an atomic transaction's
/// post-commit learning notification completes.
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

    func correctionDidPersistAtomically(
        _ event: CorrectionEvent,
        wasNewlyInserted: Bool
    ) async {
        await wrapped.correctionDidPersistAtomically(
            event,
            wasNewlyInserted: wasNewlyInserted
        )
        vetoRecorded.increment()
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

    /// playhead-ar60: a DECORATOR that does not forward this silently
    /// downgrades the wrapped store to the protocol default — which rebuilds
    /// the snapshot from the two ASSET-WIDE scalars and reinstates the very
    /// blanket ar60 removed. Forwarding keeps the wrapped store's real,
    /// scope-aware answer.
    func correctionFactorSnapshot(
        for analysisAssetId: String
    ) async -> CorrectionFactorSnapshot {
        await wrapped.correctionFactorSnapshot(for: analysisAssetId)
    }
}

// MARK: - SuspendingSeamGate

/// A one-shot gate the TEST opens and a SEAM awaits, for the case where a
/// seam has to SUSPEND rather than answer.
///
/// playhead-sdis. `BackgroundSessionIO.Behavior.suspendsThenRefusesCallsLabelled`
/// is the motivating caller: `DownloadManager.backgroundSessionRidingCrossing`
/// writes its in-flight entry, awaits the crossing and clears the entry, so a
/// second caller can find that entry only while the first is suspended. Every
/// other refusing seam answers synchronously, so no joiner can ever exist and
/// the population playhead-sdis exists to count — N episodes lost to ONE daemon
/// refusal — is unreachable from any test.
///
/// AN ACTOR RATHER THAN A `DispatchSemaphore`, and the difference is the whole
/// reason this is not three lines at a call site. `BackgroundSessionIO.perform`
/// is `nonisolated async`, so its prologue runs on the COOPERATIVE POOL; a
/// semaphore wait there occupies a pool thread the runtime will not replace,
/// for the whole barrier, and a rail that eats a pool thread under an
/// 11,000-test parallel plan is a rail that measures the box.
///
/// AND IT COSTS NO WALL CLOCK, which is what makes a rail built on it
/// deterministic. The alternative — occupy a serial queue and let a DEADLINE
/// expire — makes every run a race between the test's own arrival barrier and
/// that bound, and the 2026-08-13 merge gate lost exactly that race. An event
/// the test signals cannot be lost to load.
///
/// `wait()` after `open()` returns at once and waiters resume in arrival
/// order, so no caller can be stranded by ordering. Same one-shot contract as
/// `DeallocLatch` above, signalled by a rail instead of by a `deinit`.
actor SuspendingSeamGate {
    private var opened = false
    private var waiting: [CheckedContinuation<Void, Never>] = []

    /// Releases every current and future waiter. Idempotent.
    func open() {
        guard !opened else { return }
        opened = true
        let pending = waiting
        waiting = []
        for continuation in pending { continuation.resume() }
    }

    /// Suspends until `open()` is called. Returns immediately if it already
    /// was. No deadline — the test's `.timeLimit` trait is the hang backstop.
    func wait() async {
        if opened { return }
        await withCheckedContinuation { continuation in
            waiting.append(continuation)
        }
    }
}
