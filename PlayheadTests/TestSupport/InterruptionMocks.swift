// InterruptionMocks.swift
// playhead-iwiy: test doubles used by the InterruptionHarness.
//
// These types are distinct from the existing Stubs.swift helpers on
// purpose:
//
//   * `StubTaskScheduler` and `StubCapabilitiesProvider` were designed
//     for one-shot unit tests (single `submit`, single `yield`).
//     The harness needs per-cycle reset + dynamic publishing, so the
//     Mock variants below track history across multiple cycles and
//     re-emit on demand.
//
//   * `MockStorageBudget` wraps a real `StorageBudget` actor but lets
//     tests override the admit decision per cycle. The real admission
//     math is kept so the surrounding production invariants still hold.
//
//   * `NetworkLossProtocol` is a URLProtocol subclass the harness
//     registers to simulate loss of connectivity without touching the
//     real network.

import BackgroundTasks
import Foundation
import os
@testable import Playhead

// MARK: - MockBackgroundTaskScheduler

/// Records every submitted `BGTaskRequest` so cycles can assert the
/// scheduler seam was exercised. Separate from `StubTaskScheduler`
/// because the harness wants history across cycles.
///
/// Uses an NSLock rather than OSAllocatedUnfairLock because
/// `BGTaskRequest` is not Sendable (UIKit/BackgroundTasks types
/// predate Swift concurrency). `@unchecked Sendable` documents the
/// external safety contract and NSLock is the pragmatic internal sync
/// primitive. Parallels the existing `StubTaskScheduler`.
final class MockBackgroundTaskScheduler: BackgroundTaskScheduling, @unchecked Sendable {
    private let lock = NSLock()
    private var _requests: [BGTaskRequest] = []
    private var _shouldThrow = false

    var submittedRequests: [BGTaskRequest] {
        lock.lock(); defer { lock.unlock() }
        return _requests
    }

    var shouldThrowOnSubmit: Bool {
        get { lock.lock(); defer { lock.unlock() }; return _shouldThrow }
        set { lock.lock(); _shouldThrow = newValue; lock.unlock() }
    }

    func submit(_ taskRequest: BGTaskRequest) throws {
        lock.lock()
        if _shouldThrow {
            lock.unlock()
            throw NSError(domain: "MockBackgroundTaskScheduler", code: 1)
        }
        _requests.append(taskRequest)
        lock.unlock()
    }

    func reset() {
        lock.lock()
        _requests.removeAll()
        _shouldThrow = false
        lock.unlock()
    }
}

// MARK: - MockCapabilitiesProvider

/// Publishes `CapabilitySnapshot` values on demand. Unlike
/// `StubCapabilitiesProvider` (which yields one snapshot and closes),
/// this keeps the stream open for the harness's full lifetime so
/// repeated `publish(...)` calls reach the consumer.
final class MockCapabilitiesProvider: CapabilitiesProviding, @unchecked Sendable {
    private struct State {
        var snapshot: CapabilitySnapshot
        var continuations: [UUID: AsyncStream<CapabilitySnapshot>.Continuation] = [:]
    }

    private let state: OSAllocatedUnfairLock<State>

    init(initial: CapabilitySnapshot? = nil) {
        self.state = OSAllocatedUnfairLock(
            initialState: State(snapshot: initial ?? makeCapabilitySnapshot())
        )
    }

    var currentSnapshot: CapabilitySnapshot {
        get async { state.withLock { $0.snapshot } }
    }

    func capabilityUpdates() async -> AsyncStream<CapabilitySnapshot> {
        let id = UUID()
        return AsyncStream { continuation in
            let current = state.withLock { s -> CapabilitySnapshot in
                s.continuations[id] = continuation
                return s.snapshot
            }
            continuation.yield(current)
            continuation.onTermination = { [weak self] _ in
                self?.state.withLock { s in
                    s.continuations.removeValue(forKey: id)
                }
            }
        }
    }

    /// Publish a fresh snapshot to every live subscriber. Stored as the
    /// new `currentSnapshot` so late subscribers also see it.
    func publish(_ snapshot: CapabilitySnapshot) {
        let active = state.withLock { s -> [AsyncStream<CapabilitySnapshot>.Continuation] in
            s.snapshot = snapshot
            return Array(s.continuations.values)
        }
        for cont in active {
            cont.yield(snapshot)
        }
    }

    func reset() {
        state.withLock { s in
            s.snapshot = makeCapabilitySnapshot()
        }
    }
}

// MARK: - MockStorageBudget

/// Wraps a real `StorageBudget` actor and lets the harness override the
/// `admit(...)` decision per cycle. Calls without an override fall
/// through to the real actor.
final class MockStorageBudget: @unchecked Sendable {
    private struct State {
        var forcedDecision: StorageAdmissionDecision?
        var sizeProviderCalls: [ArtifactClass] = []
    }

    private let state = OSAllocatedUnfairLock<State>(initialState: State())
    private let real: StorageBudget

    var forcedDecision: StorageAdmissionDecision? {
        get { state.withLock { $0.forcedDecision } }
        set { state.withLock { $0.forcedDecision = newValue } }
    }

    var sizeProviderCalls: [ArtifactClass] {
        state.withLock { $0.sizeProviderCalls }
    }

    init() {
        let size: @Sendable (ArtifactClass) -> Int64 = { _ in 0 }
        let evict: @Sendable (ArtifactClass, Int64) -> Int64 = { _, target in target }
        self.real = StorageBudget(
            sizeProvider: size,
            evictor: evict
        )
    }

    func admit(class cls: ArtifactClass, sizeBytes: Int64) async -> StorageAdmissionDecision {
        let forced = state.withLock { s -> StorageAdmissionDecision? in
            s.sizeProviderCalls.append(cls)
            return s.forcedDecision
        }
        if let forced = forced { return forced }
        return await real.admit(class: cls, sizeBytes: sizeBytes)
    }

    func reset() {
        state.withLock { s in
            s.forcedDecision = nil
            s.sizeProviderCalls.removeAll()
        }
    }
}

// MARK: - NetworkLossProtocol

/// URLProtocol stub that returns `URLError.notConnectedToInternet` for
/// every request. Install via `register()`; tests never need to
/// unregister because each test spins up its own URLSession (the mock
/// is harmless to other tests).
final class NetworkLossProtocol: URLProtocol {
    private static let registeredState = OSAllocatedUnfairLock<Bool>(initialState: false)

    static func register() {
        registeredState.withLock { isRegistered in
            if !isRegistered {
                URLProtocol.registerClass(NetworkLossProtocol.self)
                isRegistered = true
            }
        }
    }

    static func unregister() {
        registeredState.withLock { isRegistered in
            if isRegistered {
                URLProtocol.unregisterClass(NetworkLossProtocol.self)
                isRegistered = false
            }
        }
    }

    override class func canInit(with request: URLRequest) -> Bool {
        guard let scheme = request.url?.scheme?.lowercased() else { return false }
        return scheme == "http" || scheme == "https"
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        let err = URLError(.notConnectedToInternet)
        client?.urlProtocol(self, didFailWithError: err)
    }

    override func stopLoading() {
        // no-op
    }
}
