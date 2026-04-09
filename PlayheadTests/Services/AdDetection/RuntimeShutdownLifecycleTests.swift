// RuntimeShutdownLifecycleTests.swift
// playhead-7h2: focused tests for `PlayheadRuntime.shutdown()` and the
// deinit fallback that together manage the shadow retry observer's
// lifetime.
//
// Context: `PlayheadRuntime` owns an optional `ShadowRetryObserver` plus
// a `shadowRetryObserverStartupTask` that kicks the observer's loop
// once `analysisStore.migrate()` finishes. `shutdown()` flips an
// `isShutdown` flag, cancels the startup task, and awaits
// `observer.stop()`. The `deinit` is a best-effort safety net that only
// cancels the startup task — the observer loop teardown lives in
// `shutdown()`.
//
// The existing `RuntimeTeardownTests` pin the happy path (startup
// completes, shutdown drives the loop to a clean exit). These tests
// cover the variants called out in the bead:
//   1. Shutdown after a fully-started observer drives a clean exit AND
//      is idempotent — `wake()` and `start()` after `stop()` must be
//      no-ops, and a second `runtime.shutdown()` must not re-drive or
//      hang.
//   2. A startup-vs-teardown race where `shutdown()` runs before the
//      startup chain has actually reached `observer.start()`. The
//      observer must NOT end up armed after the dust settles: the
//      startup task's `isShutdown` guard short-circuits.
//   3. The deinit fallback: a runtime dropped without an explicit
//      `shutdown()` must still release itself cleanly, even while the
//      observer loop is running. The runtime deinit cannot tear down
//      the observer loop — the loop's `Task` holds strong self across
//      `runObserverLoop` until an explicit `.shutdown` wake arrives,
//      which a non-async deinit has no way to deliver. The meaningful
//      invariant is cycle avoidance: nothing held by the observer
//      (capability publisher closures, drainer protocols, the loop
//      task itself) may transitively retain the runtime. Callers that
//      care about observer teardown must call `shutdown()` explicitly
//      — that contract is pinned by test 1.
//
// Non-preview runtimes are safe to construct multiple times because
// `BackgroundProcessingService.registerBackgroundTasks()` is guarded by
// a process-wide `registerOnce()` latch — the first construction
// registers the BGTaskScheduler handlers and subsequent constructions
// no-op.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-7h2: runtime shutdown lifecycle")
struct RuntimeShutdownLifecycleTests {

    // MARK: - 1. Explicit shutdown stops the observer (idempotency +
    //             post-shutdown wake/start are no-ops).

    @MainActor
    @Test("shutdown() after startup drives a clean exit and is idempotent")
    func shutdownStopsObserverAndIsIdempotent() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: false)
        guard let observer = runtime._shadowRetryObserverForTesting() else {
            Issue.record("non-preview runtime must construct the shadow retry observer")
            await runtime.shutdown()
            return
        }

        // Wait for the observer loop to actually be running before
        // tearing it down — the startup chain is multi-hop async (the
        // migrate task → startShadowRetryObserverIfNeeded → observer
        // startup task → observer.start()). Without this poll, we race
        // into shutdown before the loop exists and
        // `testHasExitedLoop()` would be trivially false (there was no
        // loop to exit).
        try await waitForLoopRunning(observer: observer)

        // Drive the teardown explicitly so we can assert its effects
        // without relying on scope exit.
        await runtime.shutdown()

        // The loop must have exited cleanly.
        let exited = await observer.testHasExitedLoop()
        #expect(exited, "loop must have exited after first shutdown()")

        // testIsLoopRunning() reads `loopTask != nil && !loopDidExit`.
        // `stop()` nils out `loopTask` on its way out, and the defer in
        // the loop sets `loopDidExit = true`. Either way the observer
        // must not report itself as running.
        let stillRunning = await observer.testIsLoopRunning()
        #expect(!stillRunning, "observer must not report as running after shutdown")

        // Idempotency: a second shutdown() must not hang and must not
        // revive the loop. If `stop()` weren't guarded it would try to
        // yield on a nil continuation or re-await a discarded task.
        await runtime.shutdown()
        let exitedAfterSecond = await observer.testHasExitedLoop()
        #expect(exitedAfterSecond, "sentinel must remain true after second shutdown()")

        // Post-shutdown `wake()` must be a no-op. The observer's
        // `wake()` is guarded by `didShutdown`; it won't restart the
        // loop, so `testIsLoopRunning()` stays false.
        await observer.wake()
        let runningAfterWake = await observer.testIsLoopRunning()
        #expect(!runningAfterWake, "wake() after shutdown must not revive the loop")

        // Post-shutdown `start()` must also be a no-op per the comment
        // on `ShadowRetryObserver.start()`: "Calling start() after
        // stop() is rejected".
        await observer.start()
        let runningAfterStart = await observer.testIsLoopRunning()
        #expect(!runningAfterStart, "start() after shutdown must not revive the loop")
    }

    // MARK: - 2. Startup vs teardown race.

    @MainActor
    @Test("shutdown() racing startup leaves the observer unarmed")
    func shutdownDuringStartupLeavesObserverUnarmed() async throws {
        // Construct a non-preview runtime and IMMEDIATELY shut it down,
        // without yielding or polling for the observer loop to be
        // running. The startup chain runs inside a detached `Task { … }`
        // created in `PlayheadRuntime.init`, so by the time we hit
        // `await runtime.shutdown()` the startup task may or may not
        // have reached `observer.start()` yet.
        //
        // Either way, after `shutdown()` returns the observer must not
        // be armed: the startup task's guard checks `isShutdown` before
        // calling `observer.start()`, and if it did get through,
        // `shutdown()`'s `observer.stop()` call will have driven the
        // loop to exit. The invariant under test is "no matter which
        // side wins the race, the observer is quiescent after
        // shutdown() returns."
        let runtime = PlayheadRuntime(isPreviewRuntime: false)
        let observer = runtime._shadowRetryObserverForTesting()
        #expect(observer != nil, "non-preview runtime must construct the observer")

        // Do NOT poll for the loop to be running — that's the whole
        // point of this test. Shut down immediately.
        await runtime.shutdown()

        guard let observer else { return }

        // After shutdown() returns, the observer must be quiescent.
        // Two legal outcomes:
        //   A) The startup task's body never got to `observer.start()`
        //      because `isShutdown` was already true. In that case the
        //      loop was never created (testIsLoopRunning == false) and
        //      testHasExitedLoop stays false (the defer never ran).
        //   B) The startup task reached `observer.start()` before
        //      `shutdown()` won the race, and `shutdown()` then drove
        //      `stop()` which awaited the loop task to completion. In
        //      that case testHasExitedLoop is true.
        //
        // Both outcomes share: testIsLoopRunning must be false.
        let running = await observer.testIsLoopRunning()
        #expect(!running, "observer must not be armed after a racing shutdown()")

        // A follow-up wake() after shutdown must remain a no-op
        // regardless of which outcome we got — didShutdown is set and
        // blocks the wake-stream yield. We can't directly assert on
        // didShutdown, but we can prove it indirectly: if wake() could
        // revive the loop the observer would report running after it.
        await observer.wake()
        let runningAfterWake = await observer.testIsLoopRunning()
        #expect(!runningAfterWake, "wake() after a racing shutdown must not revive the loop")

        // Bound the startup-task race: the runtime's migrate task may
        // still be running in the background. Give it a brief window
        // to unwind and re-check that nothing accidentally re-armed
        // the observer (e.g. via a late `startShadowRetryObserverIfNeeded`
        // call whose `isShutdown` guard we need to trust). 250ms is
        // well past the migrate path on a clean in-memory store.
        try await Task.sleep(nanoseconds: 250_000_000)
        let runningLate = await observer.testIsLoopRunning()
        #expect(!runningLate, "observer must stay unarmed after the startup chain fully unwinds")
    }

    // MARK: - 3. Deinit fallback.

    @MainActor
    @Test("deinit releases the runtime even while the observer loop is still running")
    func deinitReleasesRuntimeWithoutCycleWhenShutdownSkipped() async throws {
        // The deinit fallback on `PlayheadRuntime` is documented as a
        // best-effort safety net whose only useful action is
        // `shadowRetryObserverStartupTask?.cancel()`. Once
        // `ShadowRetryObserver.start()` has been called, the observer's
        // `loopTask` awaits the actor-isolated `runObserverLoop`, which
        // retains the observer strongly for the entire call. The
        // observer cannot be released until the loop returns via an
        // explicit `.shutdown` wake, and a non-async deinit has no way
        // to deliver that. In practice, by the time deinit runs the
        // migrate chain that owns the only strong reference to the
        // runtime has already awaited its way through
        // `startShadowRetryObserverIfNeeded`, so the observer loop is
        // guaranteed to be running and the observer is guaranteed to
        // leak across runtime teardown. That is the documented design,
        // not a bug.
        //
        // So the one invariant we can meaningfully pin for the deinit
        // path is: the runtime itself releases cleanly even while the
        // observer loop is still running. If the observer loop held a
        // transitive strong reference back to the runtime (e.g. via a
        // capability publisher closure that captured `self` implicitly,
        // or via a drainer protocol satisfied by the runtime) the
        // runtime would leak too. This test pins the absence of that
        // cycle.
        //
        // Explicit observer shutdown is tested by test 1
        // (`shutdownStopsObserverAndIsIdempotent`) — that is the
        // supported path for callers that care about observer teardown.
        //
        // ────────────────────────────────────────────────────────────
        // DO NOT RELAX THIS ASSERTION IF IT STARTS FAILING.
        // ────────────────────────────────────────────────────────────
        // The natural instinct when this fails is "the test is wrong,
        // the runtime just needs more time to release" — and that is
        // how this test was originally written (polling weakObserver
        // instead of weakRuntime). That framing is wrong and was
        // corrected after the investigation documented in playhead-7h2.
        //
        // If `weakRuntime` is non-nil here, SOMETHING owned by the
        // observer is transitively retaining the runtime. The likely
        // suspects, in rough order:
        //
        //   1. A newly-added closure on `CapabilitiesProviding` or the
        //      capability publisher that implicitly captured `self`
        //      from PlayheadRuntime instead of capturing a specific
        //      dependency by value. Look for `Task { ... }` or sink
        //      closures inside `PlayheadRuntime.init` that reference
        //      `self.something` without an explicit `[something]`
        //      capture list.
        //
        //   2. The `ShadowRetryDraining` drainer passed to the
        //      observer being satisfied by a runtime-owned object that
        //      holds a strong back-reference to the runtime. Today
        //      that protocol is satisfied by `AdDetectionService`,
        //      which does not reach back. If someone later wires the
        //      runtime itself (or a closure over `self`) as the
        //      drainer, the cycle is instant.
        //
        //   3. `ShadowRetryObserver.loopTask` growing an explicit
        //      capture of the runtime (for example, to schedule work
        //      back on a MainActor method), which would route a
        //      strong back-ref through the loop's lifetime.
        //
        // Read `ShadowRetryObserver.swift:160-170` before touching
        // this test — the `Task { [weak self] in await self?.runObserverLoop(...) }`
        // pattern intentionally holds strong self only FOR THE
        // DURATION of `runObserverLoop`, not before or after. The
        // observer leaking its own loop across runtime teardown is
        // the documented best-effort contract (deinit cannot stop
        // the loop without an async wake), so a non-nil `weakObserver`
        // would NOT be a bug — but a non-nil `weakRuntime` always is.

        weak var weakRuntime: PlayheadRuntime?

        await {
            let runtime = PlayheadRuntime(isPreviewRuntime: false)
            weakRuntime = runtime
            #expect(
                runtime._shadowRetryObserverForTesting() != nil,
                "non-preview runtime must construct the observer"
            )
            // Give the migrate chain time to reach
            // `startShadowRetryObserverIfNeeded` so we're testing
            // deinit-while-loop-running, which is the hard case for
            // cycle avoidance. Without this yield the test would be
            // trivially easy (no loop task exists yet).
            for _ in 0..<10 { await Task.yield() }
            try? await Task.sleep(nanoseconds: 50_000_000)  // 50ms
            // Runtime drops here.
        }()

        // Bounded wait for ARC to settle. The runtime must release
        // even though the observer loop is still running in the
        // background.
        let deadline = Date().addingTimeInterval(2.0)
        while weakRuntime != nil && Date() < deadline {
            await Task.yield()
            try? await Task.sleep(nanoseconds: 10_000_000)  // 10ms
        }

        #expect(
            weakRuntime == nil,
            "runtime must release after deinit even while the observer loop is still running — a non-nil weak reference means something owned by the observer (e.g. a capability publisher closure, a drainer, or the loop task itself) transitively retains the runtime, creating a cycle that only an explicit shutdown() could break"
        )
    }

    // MARK: - Helpers

    /// Polls the observer until its loop task exists. Mirrors the
    /// pattern used in `RuntimeTeardownTests` — the runtime's startup
    /// chain is multi-hop async, so tests that want to assert clean
    /// teardown must first wait for the loop to actually be running.
    private func waitForLoopRunning(
        observer: ShadowRetryObserver,
        timeout: TimeInterval = 2.0
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await observer.testIsLoopRunning() {
                return
            }
            try await Task.sleep(nanoseconds: 10_000_000)  // 10ms
        }
        Issue.record("observer loop did not start within \(timeout)s")
    }
}
