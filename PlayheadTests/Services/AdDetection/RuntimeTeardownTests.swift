// RuntimeTeardownTests.swift
// Cycle 4 H3: the cycle-2 C7 fix added `withTestRuntime` and the
// `ShadowRetryObserver.testHasExitedLoop` sentinel to pin the observer
// teardown. The cycle-2 helper defaulted `isPreviewRuntime: true`, so
// tests that went through `withTestRuntime` never constructed the
// observer at all (`shadowRetryObserver == nil` in preview runtimes)
// and the sentinel was never read from the runtime path. These tests
// route a full non-preview runtime through `withTestRuntime` and assert
// the observer was both constructed AND cleanly torn down via
// `shutdown()` — the end-to-end invariant C7 was supposed to enforce.
//
// The tests do not mock the observer's capability stream; they rely on
// the runtime's built-in wiring to construct a real `ShadowRetryObserver`
// backed by the live `CapabilitiesService`. The loop exit is driven by
// `shutdown()`'s `.shutdown` wake reason, which is deterministic and
// does not depend on any capability transition firing.

import Foundation
import Testing

@testable import Playhead

@Suite("Cycle 4 H3: runtime teardown")
struct RuntimeTeardownTests {

    @MainActor
    @Test("Preview runtime leaves the shadow retry observer nil")
    func previewRuntimeSkipsObserver() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let observer = runtime._shadowRetryObserverForTesting()
            #expect(observer == nil, "preview runtime must skip the observer to avoid leaked subscriptions")
        }
    }

    /// Non-preview runtime construction calls
    /// `backgroundProcessingService.registerBackgroundTasks()`, which
    /// `BGTaskScheduler` allows AT MOST ONCE per process. A second
    /// non-preview runtime in the same test run will crash with
    /// "Launch handler ... has already been registered". That rules out
    /// "one non-preview test per assertion" and forces us to fold every
    /// non-preview assertion into a single test that constructs the
    /// runtime exactly once.
    ///
    /// This test pins:
    ///   1. Non-preview runtimes DO construct a real `ShadowRetryObserver`
    ///      (the field is non-nil).
    ///   2. `shutdown()` drives the observer's merged-event loop to a
    ///      clean exit — `testHasExitedLoop()` returns true after
    ///      `withTestRuntime` awaits shutdown. This is the sentinel C7
    ///      was supposed to pin end-to-end; cycle-2's default of
    ///      `isPreviewRuntime: true` meant this path was never exercised
    ///      through the runtime.
    @MainActor
    @Test("Non-preview runtime: observer is live and shutdown drives the loop to clean exit")
    func nonPreviewRuntimeObserverLifecycle() async throws {
        var capturedObserver: ShadowRetryObserver?
        try await withTestRuntime { runtime in
            let observer = runtime._shadowRetryObserverForTesting()
            #expect(observer != nil, "non-preview runtime must construct the shadow retry observer")
            capturedObserver = observer

            // The runtime spawns its shadow-retry observer inside a
            // background startup task that waits on
            // `analysisStore.migrate()` before calling
            // `startShadowRetryObserverIfNeeded`, which in turn spawns
            // ANOTHER task to call `observer.start()`. If we let
            // `withTestRuntime` shut down immediately, the startup chain
            // may still be parked and the observer's loop task may not
            // exist yet — `stop()` would then have nothing to await and
            // `loopDidExit` would never be flipped. Poll the observer
            // until its loop task is actually running before releasing
            // the closure (shutdown fires on scope exit).
            let timeout = Date().addingTimeInterval(2.0)
            var loopRunning = false
            while Date() < timeout {
                if await observer?.testIsLoopRunning() == true {
                    loopRunning = true
                    break
                }
                try await Task.sleep(nanoseconds: 10_000_000)  // 10ms
            }
            #expect(loopRunning, "observer loop must start within 2s of runtime construction")
        }
        guard let observer = capturedObserver else {
            Issue.record("observer was nil after withTestRuntime scope")
            return
        }
        // `shutdown()` inside `withTestRuntime` already awaited the loop
        // task to completion — `testHasExitedLoop` must now be true.
        let exited = await observer.testHasExitedLoop()
        #expect(exited, "observer loop must have exited after runtime shutdown")
    }
}
