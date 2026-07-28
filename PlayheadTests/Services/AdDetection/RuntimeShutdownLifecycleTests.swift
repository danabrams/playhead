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
import ObjectiveC
import Testing
import CoreMedia

@testable import Playhead

// MARK: - Deinit latch (playhead-vsot)

/// Event-driven deallocation signal. `DeinitSentinel` is attached to
/// the runtime via an associated object; when the runtime deallocates,
/// the sentinel is released with it and its `deinit` fires the latch,
/// resuming any waiting test exactly at the moment of deallocation.
/// Thread-safe: deallocation may happen on any thread.
private final class DeinitLatch: @unchecked Sendable {
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

    func wait() async {
        if hasFired() { return }
        await withCheckedContinuation { continuation in
            register(continuation)
        }
    }

    // NSLock lock()/unlock() are unavailable in async contexts, so the
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

private final class DeinitSentinel {
    private let latch: DeinitLatch
    init(latch: DeinitLatch) { self.latch = latch }
    deinit { latch.signal() }
}

private nonisolated(unsafe) var deinitSentinelKey: UInt8 = 0

enum RuntimeSeekRaceAction: CaseIterable, Sendable {
    case scrub
    case skipForward
    case skipBackward
}

enum RuntimeSeekEffectStage: CaseIterable, Equatable, Sendable {
    case skip
    case silence
    case scrub
    case persistence
}

private actor RuntimeSeekPersistenceProbe {
    private var positions: [TimeInterval] = []
    private var finishedInvocations = 0

    func append(_ position: TimeInterval) {
        positions.append(position)
    }

    /// playhead-xc6b: recorded on EVERY exit path of the persistence handler,
    /// including the superseded invocation that bails on `Task.isCancelled`.
    /// Waiting for both invocations to finish lets the caller's
    /// `snapshot() == [90]` bound the recorded positions from ABOVE as well as
    /// below.
    ///
    /// Honest scope, checked rather than assumed: today the barrier is
    /// BELT-AND-BRACES, not load-bearing. `requestPlaybackPositionPersistence`
    /// does `await task.value` (PlayheadRuntime.swift:3396), so the seek does
    /// not return until its handler invocation has finished — which means the
    /// count is already 2 before the caller reads, and the poll is satisfied
    /// on its first probe. It is kept because that guarantee lives in
    /// production code the test does not own: a future restructure to
    /// fire-and-forget persistence would silently reopen the window, and this
    /// makes the test state its own requirement locally instead of inheriting
    /// it.
    func noteInvocationFinished() {
        finishedInvocations += 1
    }

    func invocationsFinished() -> Int {
        finishedInvocations
    }

    func snapshot() -> [TimeInterval] {
        positions
    }
}

private actor RuntimePlaybackObserverGate {
    private var started = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func hold() async {
        started = true
        let waiters = startWaiters
        startWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func waitUntilStarted() async {
        if started { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

private actor FirstAcceptedRuntimeSeekGate {
    private var invocationCount = 0
    private var firstStarted = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func holdFirstInvocation() async {
        let invocation = invocationCount
        invocationCount += 1
        guard invocation == 0 else { return }
        firstStarted = true
        let waiters = startWaiters
        startWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func waitUntilFirstStarted() async {
        if firstStarted { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func releaseFirst() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

/// playhead-xc6b — HANG BACKSTOPS ARE MANDATORY IN THIS SUITE.
///
/// Every test here is behavioural: it asserts STATE (a task cancelled, a
/// row flipped, a stream finished) and merely uses a wall-clock deadline
/// as its synchronisation primitive. Those assertions stay valid under
/// load, so these tests belong in the default gate — they are NOT
/// measurement tests and must never be migrated behind `PerfGate`.
///
/// What they did lack is a finite backstop. Before playhead-xc6b, 8 of the
/// 18 tests in this file carried no `.timeLimit` at all. Two of those eight
/// park on a `FirstAcceptedRuntimeSeekGate` continuation
/// (`newestRuntimeSeekSuppressesOlderDownstreamEffects`,
/// `newestSeekWinsAcrossInEffectSuspension`), which has no deadline of its
/// own; the other six await runtime seek hooks and transport state with no
/// deadline either. Be exact about the rest of the file, since this bead is
/// about not overclaiming: every `RuntimePlaybackObserverGate` and
/// `DeinitLatch` waiter here ALREADY carried a limit, so those were RAISED
/// 1 min -> 3 min rather than added. A regression on the ungated paths
/// produced NO DIAGNOSTIC AT ALL: the test task never returned and the run
/// said nothing about which test wedged.
///
/// What the trait actually buys, stated precisely: a REPORTED failure naming
/// the test and the deadline it blew, plus an unwind of every
/// cancellation-aware wait. It cannot force a body parked on a
/// `withCheckedContinuation` with no cancellation handler — which is what the
/// gate actors above use — to return. So it bounds the DIAGNOSIS reliably and
/// the process only when the wait is cancellable. That is still the
/// difference between "the gate hung" and "this test hung".
///
/// The suite-level trait below arms every test in the suite, including ones
/// added later; each test also carries the trait explicitly so the coverage
/// is greppable rather than inherited-and-assumed. Three minutes is chosen
/// against MEASUREMENT, not taste: in a full `PlayheadFastTests` run on this
/// box (2026-07-28) tests carrying a 60 s limit blew it at 119-134 s elapsed,
/// on `main@89bf541a` as well as on this branch. 3 minutes therefore sits
/// clear of observed contention — a trip means a real hang, not a busy
/// machine — while staying far below the gate's outer timeout.
@Suite("playhead-7h2: runtime shutdown lifecycle", .timeLimit(.minutes(3)))
struct RuntimeShutdownLifecycleTests {

    // MARK: - 1. Explicit shutdown stops the observer (idempotency +
    //             post-shutdown wake/start are no-ops).

    @MainActor
    @Test("shutdown() after startup drives a clean exit and is idempotent",
          .timeLimit(.minutes(3)))
    func shutdownStopsObserverAndIsIdempotent() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: false)
        guard let observer = runtime._shadowRetryObserverForTesting() else {
            Issue.record("non-preview runtime must construct the shadow retry observer")
            await runtime.shutdown()
            return
        }
        await runtime._startShadowRetryObserverForTesting()

        #expect(
            await observer.testIsLoopRunning(),
            "explicit test startup seam must arm the observer loop synchronously"
        )

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
    @Test("shutdown() racing startup leaves the observer unarmed",
          .timeLimit(.minutes(3)))
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
        // still be running in the background. Yield repeatedly so any
        // tail of the startup chain gets a chance to run, then re-check
        // that nothing accidentally re-armed the observer (e.g. via a
        // late `startShadowRetryObserverIfNeeded` call whose `isShutdown`
        // guard we need to trust).
        //
        // playhead-p06: the original code used a 250ms wall-clock sleep
        // to "wait for the migrate path on a clean in-memory store".
        // Under parallel execution on a loaded cooperative pool that
        // budget was unreliable. We now assert the invariant *continuously*
        // across a bounded yield budget: at NO point in the yield window
        // may the observer report running.
        for _ in 0..<200 {
            await Task.yield()
            let runningLate = await observer.testIsLoopRunning()
            #expect(!runningLate, "observer must stay unarmed after the startup chain unwinds")
            if await observer.testHasExitedLoop() { break }
        }
    }

    // MARK: - 3. Deinit fallback.

    @MainActor
    @Test("deinit releases the runtime even while the observer loop is still running",
          .timeLimit(.minutes(3)))
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
        // playhead-vsot: the wait below is now UNBOUNDED and
        // event-driven (deinit latch), so "needs more time" can never
        // again be the explanation for a failure here. If this test
        // fails now, it fails via its `.timeLimit` with the latch never
        // fired — that is a REAL retain cycle. Work the suspect list.
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

        // playhead-vsot ROOT CAUSE of the old flake (failed even in
        // isolation): the runtime's release is not synchronous with the
        // closure scope ending. Init performs ObjC-bridged side effects
        // (NotificationCenter async sequences, UIDevice, BGTaskScheduler)
        // whose bookkeeping keeps the last reference alive until the
        // main queue goes IDLE and its pending queue-turn/autorelease
        // work drains. The previous wait was a fixed 200-iteration
        // `Task.yield()` budget — but a tight yield loop itself keeps
        // the MainActor busy, so the drain it was waiting for could not
        // run, and the budget lost the race deterministically (~2 s of
        // yields, release observed only once the queue idled). A
        // 30 s diagnostic showed the runtime releases ~60 ms after the
        // test task actually suspends — there is NO cycle.
        //
        // Fix: wait EVENT-DRIVEN on the actual deallocation. A sentinel
        // is attached to the runtime as an associated object; the
        // runtime's deallocation releases the sentinel, whose deinit
        // fires the latch. `await latch.wait()` genuinely suspends the
        // test task (letting the main queue idle and drain), and
        // resumes exactly at deallocation. A REAL retain cycle keeps
        // the latch silent and the test fails at its `.timeLimit` —
        // deterministic, not load-dependent.
        weak var weakRuntime: PlayheadRuntime?
        let deinitLatch = DeinitLatch()

        await {
            let runtime = PlayheadRuntime(isPreviewRuntime: false)
            weakRuntime = runtime
            objc_setAssociatedObject(
                runtime,
                &deinitSentinelKey,
                DeinitSentinel(latch: deinitLatch),
                .OBJC_ASSOCIATION_RETAIN
            )
            #expect(
                runtime._shadowRetryObserverForTesting() != nil,
                "non-preview runtime must construct the observer"
            )
            // Start the observer through the explicit DEBUG seam so this
            // exercises deinit-while-loop-running without depending on
            // the production migrate/bootstrap task order.
            if let obs = runtime._shadowRetryObserverForTesting() {
                await runtime._startShadowRetryObserverForTesting()
                #expect(
                    await obs.testIsLoopRunning(),
                    "explicit test startup seam must arm the observer loop synchronously"
                )
            }
            // Runtime drops here.
        }()

        // Event-driven: resumes exactly when the runtime deallocates,
        // however long the queue-turn takes under load. No yield budget,
        // no sleep, no deadline — `.timeLimit` is the cycle backstop.
        await deinitLatch.wait()

        #expect(
            weakRuntime == nil,
            "runtime must release after deinit even while the observer loop is still running — a non-nil weak reference means something owned by the observer (e.g. a capability publisher closure, a drainer, or the loop task itself) transitively retains the runtime, creating a cycle that only an explicit shutdown() could break"
        )
    }

    // MARK: - 4. RepeatedAdCache kill-switch observer cancellation (playhead-43ed M1)

    /// The `Task { ... for await _ in NotificationCenter.notifications(...) }`
    /// observer that watches the `b3_repeated_ad_cache_enabled` UserDefaults
    /// key was constructed without a cancellation handle pre-fix, so it
    /// outlived `runtime.shutdown()` — every test that constructed a
    /// non-preview runtime leaked one observer Task plus its captured
    /// `repeatedAdCache` strong reference for the rest of the process.
    /// In production it leaked across teardown of integration-test
    /// runtimes and would have leaked across any future runtime that
    /// can be torn down (e.g. SceneDelegate destruction, App Clip
    /// teardown). Post-fix: `shutdown()` cancels the observer Task and
    /// the `for await` loop terminates promptly.
    @MainActor
    @Test("shutdown() cancels the RepeatedAdCache kill-switch observer task",
          .timeLimit(.minutes(3)))
    func shutdownCancelsRepeatedAdCacheKillSwitchObserver() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: false)
        guard let task = runtime._repeatedAdCacheKillSwitchObserverTaskForTesting() else {
            Issue.record("non-preview runtime must construct the kill-switch observer task")
            await runtime.shutdown()
            return
        }
        // Sanity-check: pre-shutdown the task is alive (not cancelled).
        #expect(!task.isCancelled, "observer task must be running before shutdown()")

        await runtime.shutdown()

        #expect(task.isCancelled, "observer task must be cancelled after shutdown()")

        // Idempotency: a second shutdown() must not crash and the task
        // remains cancelled.
        await runtime.shutdown()
        #expect(task.isCancelled, "observer task must remain cancelled after second shutdown()")
    }

    @MainActor
    @Test("replacement boundary clears prior transport cues without a resolved asset",
          .timeLimit(.minutes(3)))
    func replacementBoundaryClearsCuesBeforeAssetResolution() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        await runtime.playbackService.setSkipCues([
            CMTimeRange(
                start: CMTime(seconds: 10, preferredTimescale: 600),
                end: CMTime(seconds: 20, preferredTimescale: 600)
            ),
        ])
        await runtime.playbackService._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 5,
                duration: 100,
                rate: 1,
                playbackSpeed: 1
            )
        )
        #expect(!(await runtime.playbackService._testingSkipCues).isEmpty)

        await runtime._clearPriorEpisodePlaybackStateForTesting()

        #expect(
            await runtime.playbackService.snapshot().status == .paused,
            "replacement must quiesce the prior item before retiring its cache ownership"
        )
        #expect(
            (await runtime.playbackService._testingSkipCues).isEmpty,
            "the replacement must disarm prior cues even if no asset ever resolves"
        )
        await runtime.shutdown()
    }

    @MainActor
    @Test("shutdown balances playback-owned download protection",
          .timeLimit(.minutes(3)))
    func shutdownReleasesPlaybackProtection() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let episodeId = "shutdown-protected-episode"
        await runtime._protectPlaybackEpisodeForTesting(episodeId)
        #expect(
            await runtime.downloadManager.protectedEpisodeIdsForTesting() == [episodeId]
        )

        await runtime.shutdown()

        #expect(
            await runtime.downloadManager.protectedEpisodeIdsForTesting().isEmpty,
            "shutdown must balance protection because stopPlayback is rejected afterward"
        )
    }

    @MainActor
    @Test(
        "shutdown tears down playback observers and finishes state streams",
        .timeLimit(.minutes(3))
    )
    func shutdownTearsDownPlaybackTransport() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let stream = await runtime.playbackService.observeStates()
        let streamFinished = Task {
            var iterator = stream.makeAsyncIterator()
            _ = await iterator.next()
            return await iterator.next() == nil
        }

        await runtime.shutdown()

        #expect(
            await streamFinished.value,
            "PlaybackTransport.tearDown must finish observers during runtime shutdown"
        )
    }

    @MainActor
    @Test(
        "shutdown joins a playback-state body already suspended downstream",
        .timeLimit(.minutes(3))
    )
    func shutdownJoinsPlaybackStateConsumer() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let bodyGate = RuntimePlaybackObserverGate()
        runtime._setPlaybackStateObserverHookForTesting {
            await bodyGate.hold()
        }

        await runtime.playbackService._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 12,
                duration: 100,
                rate: 1,
                playbackSpeed: 1
            )
        )
        await bodyGate.waitUntilStarted()

        // playhead-xc6b: capture the observer handle BEFORE shutdown nils it.
        // It is the positive control for the negative assertion below.
        let observerTask = try #require(
            runtime._playbackStateObserverTaskForTesting(),
            "the runtime must have installed a playback-state observer to join"
        )

        let shutdown = Task { @MainActor in
            await runtime.shutdown()
        }

        // playhead-xc6b — FAIL-OPEN FIX. This used to be `for _ in 0..<5 {
        // await Task.yield() }`. A fixed yield budget is the wrong shape for
        // a NEGATIVE assertion: under a saturated gate the shutdown task may
        // not have been scheduled at all when the budget expires, so
        // "the observer has not exited" was true for the trivial reason that
        // NOTHING HAD HAPPENED YET. The suite got weaker under load, not just
        // flakier.
        //
        // The repo idiom for proving a negative honestly (see
        // `drainOrchestratorEffects` / `awaitTrustFalseSkipSignals` in
        // TestHelpers.swift) is: establish a POSITIVE control that must be
        // observed first, then assert the negative behind that barrier. The
        // control here is `observerTask.isCancelled`. Precisely: `shutdown()`
        // cancels the playback-state observer task at PlayheadRuntime.swift
        // :3265 and joins it at :3274, with an `await` on the playback
        // LIFECYCLE task in between — so observing the cancellation proves
        // shutdown entered and reached its CANCEL step, not literally the
        // join. That is sufficient here, and is the point: the old budget
        // could not prove shutdown had started at all. The poll scales with
        // load instead of racing it.
        //
        // The name says CANCEL, not JOIN, deliberately: `isCancelled` flips
        // at :3265 and the join is at :3274, so this control proves shutdown
        // reached the cancel — claiming "reached the join" would be exactly
        // the kind of overclaim this bead exists to remove.
        let shutdownReachedObserverCancel = await pollUntil(
            timeout: starvationPollBudget
        ) {
            observerTask.isCancelled
        }
        #expect(
            shutdownReachedObserverCancel,
            "shutdown() must reach its playback-state observer cancel — without this the negative assertion below is vacuous"
        )

        // Now the negative means something: shutdown has demonstrably run far
        // enough to cancel the observer, and the observer still has not
        // exited because its body is parked in `bodyGate.hold()`. The gate's
        // continuation is resumed only by `release()` below, so this cannot
        // flip on its own.
        #expect(
            !runtime._playbackStateObserverDidExitForTesting(),
            "The held body must still own the observer until its downstream hop returns"
        )

        await bodyGate.release()
        await shutdown.value
        #expect(
            runtime._playbackStateObserverDidExitForTesting(),
            "shutdown must join the state consumer before returning"
        )
    }

    @MainActor
    @Test("user-mark write rejects a playback context replaced after the tap",
          .timeLimit(.minutes(3)))
    func staleUserMarkContextIsRejected() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let generationA = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "asset-a",
            episodeId: "episode-a",
            podcastId: "podcast-a"
        )
        let generationB = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "asset-b",
            episodeId: "episode-b",
            podcastId: "podcast-b"
        )

        #expect(generationB != generationA)
        #expect(
            !(await runtime.injectUserMarkedAd(
                start: 10,
                end: 40,
                ifCurrentAnalysisAssetId: "asset-a",
                ifCurrentEpisodeId: "episode-a",
                ifPlaybackLifecycleGeneration: generationA,
                podcastId: "podcast-a"
            )),
            "episode A's expanded boundary must never persist or inject into episode B"
        )
        #expect(
            !(await runtime.injectUserMarkedAd(
                start: 10,
                end: 40,
                ifCurrentAnalysisAssetId: "asset-b",
                ifCurrentEpisodeId: "episode-b",
                ifPlaybackLifecycleGeneration: generationB,
                podcastId: "podcast-a"
            )),
            "current asset/lifecycle material must not be written under a stale show"
        )
        #expect(
            runtime._userMarkPersistenceAttemptCountForTesting() == 0,
            "a mismatched show must fail before any durable write is attempted"
        )

        let generationC = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "asset-c",
            episodeId: "episode-c",
            podcastId: "podcast-c\u{0}other"
        )
        #expect(
            !(await runtime.injectUserMarkedAd(
                start: 10,
                end: 40,
                ifCurrentAnalysisAssetId: "asset-c",
                ifCurrentEpisodeId: "episode-c",
                ifPlaybackLifecycleGeneration: generationC,
                podcastId: "podcast-c\u{0}other"
            )),
            "matching malformed show identities must not reach persistence"
        )
        #expect(runtime._userMarkPersistenceAttemptCountForTesting() == 0)
        await runtime.shutdown()
    }

    @MainActor
    @Test("episode-bound Listen rejects a different current podcast",
          .timeLimit(.minutes(3)))
    func listenRewindRejectsMismatchedPodcast() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        try await runtime.analysisStore.migrate()
        let runId = UUID().uuidString
        let assetId = "asset-listen-show-scope-\(runId)"
        let windowId = "window-listen-show-scope-\(runId)"
        let episodeId = "episode-listen-show-scope-\(runId)"
        try await runtime.analysisStore.insertAsset(makeTestAsset(id: assetId))
        let window = makeSkipTestAdWindow(
            id: windowId,
            assetId: assetId,
            startTime: 10,
            endTime: 40,
            confidence: 1,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await runtime.analysisStore.insertAdWindow(window)
        let generation = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: "podcast-current"
        )

        #expect(
            !(await runtime.recordListenRewind(
                windowId: windowId,
                analysisAssetId: assetId,
                podcastId: "podcast-other",
                ifCurrentAnalysisAssetId: assetId,
                ifCurrentEpisodeId: episodeId,
                ifPlaybackLifecycleGeneration: generation,
                expectedProducerRevision: window
            ))
        )
        #expect(
            try await runtime.analysisStore.fetchAdWindow(id: windowId)?
                .decisionState == AdDecisionState.applied.rawValue
        )
        await runtime.shutdown()
    }

    @MainActor
    @Test("invalid runtime seeks do not mutate the transport state seam",
          .timeLimit(.minutes(3)))
    func invalidRuntimeSeeksFailClosed() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        _ = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "asset-a",
            episodeId: "episode-a",
            podcastId: "podcast-a"
        )
        await runtime.playbackService._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 23,
                duration: 200,
                rate: 1,
                playbackSpeed: 1
            )
        )

        #expect(!(await runtime.seek(to: .nan)))
        #expect(!(await runtime.seek(to: .infinity)))
        #expect(!(await runtime.seek(to: -1)))
        #expect(await runtime.playbackService.snapshot().currentTime == 23)
        #expect(runtime._committedUserSeekCountForTesting() == 0)
        await runtime.shutdown()
    }

    @MainActor
    @Test(
        "scrub and skip controls reject replacement/replay after context capture",
        .timeLimit(.minutes(3)),
        arguments: RuntimeSeekRaceAction.allCases
    )
    func staleUserSeekContextIsRejected(
        _ action: RuntimeSeekRaceAction
    ) async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        _ = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "asset-a",
            episodeId: "episode-a",
            podcastId: "podcast-a"
        )
        await runtime.playbackService._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 50,
                duration: 200,
                rate: 1,
                playbackSpeed: 1
            )
        )
        runtime._setSeekContextCapturedHookForTesting {
            // Forward models same-episode replay; scrub/backward model an
            // episode replacement. Both must invalidate the captured
            // playback generation before any downstream seek side effect.
            let replacementEpisode =
                action == .skipForward ? "episode-a" : "episode-b"
            _ = runtime._setUserMarkPlaybackContextForTesting(
                analysisAssetId: "asset-replacement",
                episodeId: replacementEpisode,
                podcastId: "podcast-replacement"
            )
        }

        let accepted: Bool
        switch action {
        case .scrub:
            accepted = await runtime.seek(to: 90)
        case .skipForward:
            accepted = await runtime.skipForward()
        case .skipBackward:
            accepted = await runtime.skipBackward()
        }

        #expect(!accepted)
        #expect(await runtime.playbackService.snapshot().currentTime == 50)
        #expect(runtime._committedUserSeekCountForTesting() == 0)
        await runtime.shutdown()
    }

    @MainActor
    @Test("transport rejection suppresses every runtime seek side effect",
          .timeLimit(.minutes(3)))
    func rejectedTransportSeekHasNoDownstreamEffects() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        _ = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "asset-a",
            episodeId: "episode-a",
            podcastId: "podcast-a"
        )
        await runtime.playbackService._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 25,
                duration: 200,
                rate: 1,
                playbackSpeed: 1
            )
        )

        // Preview runtime has no installed AVPlayerItem, so the item-fenced
        // transport operation rejects. Runtime must honor that Bool.
        #expect(!(await runtime.seek(to: 80)))
        #expect(await runtime.playbackService.snapshot().currentTime == 25)
        #expect(runtime._committedUserSeekCountForTesting() == 0)
        await runtime.shutdown()
    }

    @MainActor
    @Test("newest same-lifecycle runtime seek owns every downstream effect",
          .timeLimit(.minutes(3)))
    func newestRuntimeSeekSuppressesOlderDownstreamEffects() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        _ = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "asset-a",
            episodeId: "episode-a",
            podcastId: "podcast-a"
        )
        await runtime.playbackService._testingInstallStubCurrentPlayerItem()
        await runtime.playbackService._testingSetItemSeekOperation {
            _, _ in true
        }
        await runtime.playbackService._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 20,
                duration: 200,
                rate: 1,
                playbackSpeed: 1
            )
        )
        let firstAcceptedGate = FirstAcceptedRuntimeSeekGate()
        runtime._setSeekTransportAcceptedHookForTesting {
            await firstAcceptedGate.holdFirstInvocation()
        }

        let olderSeek = Task { @MainActor in
            await runtime.seek(to: 40)
        }
        await firstAcceptedGate.waitUntilFirstStarted()
        let newerSeek = Task { @MainActor in
            await runtime.seek(to: 90)
        }

        #expect(await newerSeek.value)
        #expect(await runtime.playbackService.snapshot().currentTime == 90)
        let countsAfterNewest = runtime
            ._userSeekDownstreamCountsForTesting()
        #expect(countsAfterNewest.skip == 1)
        #expect(countsAfterNewest.silence == 1)
        #expect(countsAfterNewest.scrub == 1)
        #expect(countsAfterNewest.persistence == 1)
        #expect(countsAfterNewest.committed == 1)

        await firstAcceptedGate.releaseFirst()
        #expect(
            !(await olderSeek.value),
            "The older same-episode operation must reject after the newer seek starts"
        )
        let finalCounts = runtime._userSeekDownstreamCountsForTesting()
        #expect(finalCounts.skip == 1)
        #expect(finalCounts.silence == 1)
        #expect(finalCounts.scrub == 1)
        #expect(finalCounts.persistence == 1)
        #expect(finalCounts.committed == 1)
        #expect(
            await runtime.playbackService.snapshot().currentTime == 90
        )
        await runtime.shutdown()
    }

    @MainActor
    @Test(
        "newer seek wins when the older seek is suspended inside every downstream effect",
        .timeLimit(.minutes(3)),
        arguments: RuntimeSeekEffectStage.allCases
    )
    func newestSeekWinsAcrossInEffectSuspension(
        _ stage: RuntimeSeekEffectStage
    ) async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        _ = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "asset-effect-race",
            episodeId: "episode-effect-race",
            podcastId: "podcast-effect-race"
        )
        await runtime.playbackService._testingInstallStubCurrentPlayerItem()
        await runtime.playbackService._testingSetItemSeekOperation {
            _, _ in true
        }
        await runtime.playbackService._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 20,
                duration: 200,
                rate: 1,
                playbackSpeed: 1
            )
        )

        let gate = FirstAcceptedRuntimeSeekGate()
        let persistence = RuntimeSeekPersistenceProbe()
        switch stage {
        case .skip:
            await runtime._setSkipSeekEffectHookForTesting {
                await gate.holdFirstInvocation()
            }
        case .silence:
            runtime._setSilenceSeekEffectHookForTesting {
                await gate.holdFirstInvocation()
            }
        case .scrub:
            await runtime._setScrubSeekEffectHookForTesting {
                await gate.holdFirstInvocation()
            }
        case .persistence:
            // playhead-xc6b: `[weak runtime]`. This closure is STORED ON the
            // runtime (`playbackPositionPersistenceHandler`); capturing
            // `runtime` strongly made the runtime own a closure that owns the
            // runtime, so neither ever deallocated — once per parameter case.
            //
            // `noteInvocationFinished()` runs on every exit path so the test
            // can wait for BOTH invocations (the superseded one and the
            // winner) to have finished before reading the probe. Without it
            // the `== [90]` assertion below raced the older invocation's
            // cancellation check and could observe `[90, 90]`.
            runtime.setPlaybackPositionPersistenceHandler { [weak runtime] _ in
                await gate.holdFirstInvocation()
                if !Task.isCancelled,
                   let runtime,
                   let captured = await runtime.capturePlaybackPosition() {
                    await persistence.append(captured.position)
                }
                await persistence.noteInvocationFinished()
            }
        }

        let olderSeek = Task { @MainActor in
            await runtime.seek(to: 40)
        }
        await gate.waitUntilFirstStarted()
        let newerSeek = Task { @MainActor in
            await runtime.seek(to: 90)
        }

        #expect(await newerSeek.value)
        #expect(await runtime.playbackService.snapshot().currentTime == 90)
        await gate.releaseFirst()
        #expect(!(await olderSeek.value))

        let effectTargets =
            await runtime._userSeekEffectTargetsForTesting()
        #expect(effectTargets.skip == 90)
        #expect(effectTargets.silence == 90)
        #expect(effectTargets.scrub == 90)
        #expect(await runtime.playbackService.snapshot().currentTime == 90)
        #expect(runtime._committedUserSeekCountForTesting() == 1)
        if stage == .persistence {
            // playhead-xc6b: wait for BOTH handler invocations to finish
            // before reading. Exactly two are invoked — the superseded seek's
            // (held by the gate, released above) and the winner's — and both
            // are already inside the handler by this point, so this cannot
            // hang on a third that never comes. See the probe's own doc for
            // why this is belt-and-braces today and kept anyway.
            let bothFinished = await pollUntil(timeout: starvationPollBudget) {
                await persistence.invocationsFinished() == 2
            }
            #expect(
                bothFinished,
                "both persistence handler invocations must finish before the recorded positions can be bounded from above"
            )
            #expect(await persistence.snapshot() == [90])
        }
        await runtime.shutdown()
    }

    @MainActor
    @Test("Replacement, Stop, and shutdown do not join a cancellation-unresponsive cache task",
          // The canonical fast plan starts thousands of Swift Testing cases
          // together. On a saturated simulator, even this focused ~6-second
          // scenario can remain executor-starved for over a minute before it
          // runs. Keep the limit finite while allowing the full-plan scheduler
          // enough headroom; behavioral hangs still fail well before the gate's
          // outer timeout.
          .timeLimit(.minutes(3)))
    func lifecycleInvalidationDoesNotJoinStalledCacheTask() async {
        let replacementRuntime = PlayheadRuntime(isPreviewRuntime: true)
        let replacementLatch = DeinitLatch()
        let stalledDuringReplacement = Task {
            await replacementLatch.wait()
        }
        replacementRuntime._setAudioCacheTaskForTesting(stalledDuringReplacement)

        // `performPlayEpisode` uses this same non-joining invalidation helper
        // before touching replacement transport state.
        replacementRuntime._cancelAudioCacheTaskForTesting()

        #expect(stalledDuringReplacement.isCancelled)
        replacementLatch.signal()
        await stalledDuringReplacement.value
        await replacementRuntime.shutdown()

        let stopRuntime = PlayheadRuntime(isPreviewRuntime: true)
        let stopLatch = DeinitLatch()
        let stalledDuringStop = Task {
            await stopLatch.wait()
        }
        stopRuntime._setAudioCacheTaskForTesting(stalledDuringStop)

        await stopRuntime.stopPlayback()

        #expect(stalledDuringStop.isCancelled)
        stopLatch.signal()
        await stalledDuringStop.value
        await stopRuntime.shutdown()

        let shutdownRuntime = PlayheadRuntime(isPreviewRuntime: true)
        let shutdownLatch = DeinitLatch()
        let stalledDuringShutdown = Task {
            await shutdownLatch.wait()
        }
        shutdownRuntime._setAudioCacheTaskForTesting(stalledDuringShutdown)

        await shutdownRuntime.shutdown()

        #expect(stalledDuringShutdown.isCancelled)
        shutdownLatch.signal()
        await stalledDuringShutdown.value
    }

    @MainActor
    @Test("progressive cache cancellation follows transport quiescence",
          .timeLimit(.minutes(3)))
    func progressiveCancellationFollowsPause() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let generation = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: nil,
            episodeId: "progressive-episode",
            podcastId: "podcast"
        )
        await runtime.playbackService._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 20,
                duration: 100,
                rate: 1,
                playbackSpeed: 1
            )
        )
        await runtime.playbackService._testingInstallStubCurrentPlayerItem()
        #expect(await runtime.playbackService._testingHasPlayerItem)
        #expect(await runtime.playbackService._testingHasCurrentPlayerItem)
        let heldLatch = DeinitLatch()
        let held = Task {
            await heldLatch.wait()
        }
        runtime._setAudioCacheTaskForTesting(held)

        #expect(
            await runtime._quiescePlaybackThenCancelAudioCacheForTesting(
                generation: generation
            )
        )
        #expect(held.isCancelled)
        #expect(
            await runtime.playbackService.snapshot().status == .paused,
            "The shared replacement/Stop boundary must pause before releasing the progressive artifact"
        )
        #expect(
            !(await runtime.playbackService._testingHasPlayerItem),
            "The old item must no longer satisfy a later Play command"
        )
        #expect(
            !(await runtime.playbackService._testingHasCurrentPlayerItem),
            "The old item must be detached from AVPlayer before its loader is released"
        )
        await runtime.playbackService.play()
        #expect(
            await runtime.playbackService.snapshot().status == .paused,
            "Play must remain a no-op until the replacement item is installed"
        )
        heldLatch.signal()
        await held.value
        await runtime.shutdown()
    }

    @MainActor
    @Test(
        "a cache task cancelled before its first turn cannot enter the streaming lane",
        .timeLimit(.minutes(3))
    )
    func cancelledCacheTaskRejectsEntryBeforeDownloadManager() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let generation = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: nil,
            episodeId: "episode-a",
            podcastId: "podcast-a"
        )
        let startGate = RuntimePlaybackObserverGate()
        let staleEntry = Task { @MainActor in
            await startGate.hold()
            return await runtime._audioCacheEntryAdmittedForTesting(
                generation: generation,
                episodeId: "episode-a"
            )
        }

        await startGate.waitUntilStarted()
        staleEntry.cancel()
        await startGate.release()

        #expect(
            !(await staleEntry.value),
            "Cancellation before entry must reject the operation wrapped around DownloadManager.streamingDownload"
        )
        await runtime.shutdown()
    }
}
