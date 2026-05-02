// FinalPassThermalRecoveryObserverTests.swift
// playhead-l8dz: tests for the in-process recovery observer that kicks the
// final-pass launch sweep when device conditions transition false → true.

import Foundation
import os
import Testing

@testable import Playhead

@Suite("playhead-l8dz: FinalPassThermalRecoveryObserver")
struct FinalPassThermalRecoveryObserverTests {

    // MARK: - Helpers

    /// Yields the cooperative pool until `condition` is true or `iterations`
    /// is exhausted. Mirrors the helper used in ShadowRetryTests; required
    /// because the observer's loopTask runs on the actor's executor and
    /// callers can't await it directly.
    private func yieldUntilStable(
        iterations: Int = 200,
        condition: () async -> Bool
    ) async {
        if await condition() { return }
        for _ in 0..<iterations {
            await Task.yield()
            if await condition() { return }
        }
    }

    private func runnableSnapshot() -> CapabilitySnapshot {
        makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: false, isCharging: true)
    }

    private func thermalThrottledSnapshot() -> CapabilitySnapshot {
        makeCapabilitySnapshot(thermalState: .serious, isLowPowerMode: false, isCharging: true)
    }

    private func unpluggedSnapshot() -> CapabilitySnapshot {
        makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: false, isCharging: false)
    }

    private func lowPowerSnapshot() -> CapabilitySnapshot {
        makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: true, isCharging: true)
    }

    /// Mutable counter for kickSweep invocations. Uses an unfair lock so
    /// the kickSweep closure (Sendable) can mutate it safely from any task.
    private final class SweepRecorder: @unchecked Sendable {
        private let lock = OSAllocatedUnfairLock<Int>(initialState: 0)
        var count: Int { lock.withLock { $0 } }
        func bump() { lock.withLock { $0 += 1 } }
    }

    /// Settable clock so tests can advance virtual time past the cooldown.
    private final class TestClock: @unchecked Sendable {
        private let lock = OSAllocatedUnfairLock<Date>(initialState: Date(timeIntervalSince1970: 1_000_000))
        var now: Date { lock.withLock { $0 } }
        func advance(by seconds: TimeInterval) { lock.withLock { $0 = $0.addingTimeInterval(seconds) } }
    }

    /// Deterministic gate: kick closures `await wait()`; tests `release()` to
    /// let them complete. Avoids `Task.sleep`-based timing races where yield
    /// counts can outpace the wall-clock sleep on a busy executor.
    private final class AsyncGate: @unchecked Sendable {
        private struct State {
            var released: Bool = false
            var waiters: [CheckedContinuation<Void, Never>] = []
        }
        private let state = OSAllocatedUnfairLock<State>(initialState: State())

        func wait() async {
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                let resumeNow = state.withLock { s -> Bool in
                    if s.released { return true }
                    s.waiters.append(cont)
                    return false
                }
                if resumeNow { cont.resume() }
            }
        }

        func release() {
            let toResume = state.withLock { s -> [CheckedContinuation<Void, Never>] in
                guard !s.released else { return [] }
                s.released = true
                let waiters = s.waiters
                s.waiters = []
                return waiters
            }
            for cont in toResume { cont.resume() }
        }
    }

    // MARK: - Predicate parity invariant (cycle 2 M2)

    @Test("M2: QualityProfile.nominal does NOT pause all work, so observer's predicate parity holds")
    func qualityProfileNominalDoesNotPauseAllWork() {
        // The observer's `isRunnableNow()` skips the runner's defensive
        // `qualityProfile.pauseAllWork` re-check. That's safe today only
        // because `.nominal` cannot demote to a profile that pauses all
        // work. If a future edit to `QualityProfile` makes `.nominal`
        // pause work under any condition, observer kicks become wasted
        // no-ops AND priorRunnable=true pins until conditions cycle —
        // suppressing future kicks entirely. Pin the invariant here so
        // that breakage is loud at test time, not silent in production.
        let profile = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 1.0,
            batteryState: .charging,
            isLowPowerMode: false
        )
        #expect(profile.schedulerPolicy.pauseAllWork == false)
    }

    // MARK: - Lifecycle

    @Test("stop() exits the observer loop cleanly")
    func stopExitsLoopCleanly() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { }
        )
        await observer.start()
        await yieldUntilStable { await observer.testIsLoopRunning() }
        await observer.stop()
        let exited = await observer.testHasExitedLoop()
        #expect(exited)
    }

    @Test("start() after stop() is rejected")
    func startAfterStopIsRejected() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { }
        )
        await observer.start()
        await yieldUntilStable { await observer.testIsLoopRunning() }
        await observer.stop()

        await observer.start()
        // Loop should NOT come back. Give it a few yields and then check.
        for _ in 0..<10 { await Task.yield() }
        let running = await observer.testIsLoopRunning()
        #expect(!running)
    }

    // MARK: - Transitions

    @Test("First snapshot establishes prior; no sweep kicked")
    func firstSnapshotDoesNotKick() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        // Even if the device starts already runnable, first emission should
        // NOT fire a sweep (the launch-sweep already runs at startup).
        let runnableObserver = FinalPassThermalRecoveryObserver(
            capabilities: MockCapabilitiesProvider(initial: runnableSnapshot()),
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await runnableObserver.start()
        await yieldUntilStable { await runnableObserver.testPriorRunnable() == true }

        for _ in 0..<10 { await Task.yield() }

        await observer.stop()
        await runnableObserver.stop()

        #expect(recorder.count == 0)
    }

    @Test("False → true transition fires kickSweep exactly once")
    func transitionFiresKickSweep() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        await observer.stop()
        #expect(recorder.count == 1)
    }

    @Test("True → true (steady runnable) does not refire")
    func steadyRunnableDoesNotRefire() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        // Re-publish runnable several times — should be a no-op.
        for _ in 0..<5 {
            provider.publish(runnableSnapshot())
            await Task.yield()
        }
        for _ in 0..<10 { await Task.yield() }

        await observer.stop()
        #expect(recorder.count == 1)
    }

    @Test("Oscillation true → false → true fires twice (when cooldown=0)")
    func oscillationFiresAgain() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        // Wait for the in-flight sweep to clear before re-firing — the
        // outstanding-task guard would otherwise suppress the second fire.
        await yieldUntilStable { await observer.testPriorRunnable() == true }
        for _ in 0..<20 { await Task.yield() }

        provider.publish(thermalThrottledSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == false }
        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 2 }

        await observer.stop()
        #expect(recorder.count == 2)
    }

    // MARK: - Cooldown guard

    @Test("Cooldown suppresses second fire within window")
    func cooldownSuppressesSecondFire() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let clock = TestClock()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 120,
            clock: { [clock] in clock.now },
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        // Wait for sweep to clear; oscillate inside cooldown.
        for _ in 0..<20 { await Task.yield() }
        provider.publish(thermalThrottledSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        // Advance clock by 60s — still inside the 120s cooldown.
        clock.advance(by: 60)
        provider.publish(runnableSnapshot())
        for _ in 0..<20 { await Task.yield() }

        await observer.stop()
        #expect(recorder.count == 1)
    }

    @Test("After cooldown elapses, next transition fires again")
    func afterCooldownNextTransitionFires() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let clock = TestClock()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 120,
            clock: { [clock] in clock.now },
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        // Wait for sweep to clear, oscillate.
        for _ in 0..<20 { await Task.yield() }
        provider.publish(thermalThrottledSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        // Advance past cooldown.
        clock.advance(by: 121)
        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 2 }

        await observer.stop()
        #expect(recorder.count == 2)
    }

    // MARK: - Outstanding-task guard

    // MARK: - H1 fresh-probe verification (cycle 1 review)

    @Test("H1: charge probe disagreeing with snapshot.isCharging gates correctly")
    func chargeProbeDisagreesWithSnapshotGatesCorrectly() async {
        // Reproduces the staleness gap the H1 fix closes. The capability
        // stream emits a snapshot that LOOKS runnable
        // (thermal=.nominal, isCharging=true), but the fresh chargeStateProvider
        // returns false — exactly the scenario the runner would refuse.
        // The observer must NOT fire on this transition.
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        // Lock-protected toggle so the probe can flip across publish() calls.
        let chargeProbeState = OSAllocatedUnfairLock<Bool>(initialState: false)
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            chargeStateProvider: { chargeProbeState.withLock { $0 } },
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        // Snapshot says runnable, but fresh charge probe says NOT charging.
        // Observer should treat this as still-not-runnable.
        provider.publish(runnableSnapshot())
        for _ in 0..<20 { await Task.yield() }
        #expect(recorder.count == 0)
        let priorAfterStaleSnapshot = await observer.testPriorRunnable()
        #expect(priorAfterStaleSnapshot == false)

        // Now flip the fresh probe to true; the next snapshot emission
        // should fire because fresh-probe agrees.
        chargeProbeState.withLock { $0 = true }
        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        await observer.stop()
        #expect(recorder.count == 1)
    }

    @Test("H1: capability snapshot probe disagreeing with stream snapshot gates correctly")
    func capabilitySnapshotProbeDisagreesGatesCorrectly() async {
        // Reverse staleness: the stream snapshot says runnable, but the
        // fresh capability snapshot says low-power-mode is on.
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let probedSnapshot = OSAllocatedUnfairLock<CapabilitySnapshot>(
            initialState: makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: true, isCharging: true)
        )
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            capabilitySnapshotProvider: { probedSnapshot.withLock { $0 } },
            chargeStateProvider: { true },
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        // Stream emits "runnable" but fresh probe says LPM is on. No fire.
        provider.publish(runnableSnapshot())
        for _ in 0..<20 { await Task.yield() }
        #expect(recorder.count == 0)

        // Flip the probed snapshot to actually-runnable; next emission fires.
        probedSnapshot.withLock { $0 = makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: false, isCharging: true) }
        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        await observer.stop()
        #expect(recorder.count == 1)
    }

    // MARK: - H2 shutdown-awaits-sweep verification (cycle 1 review)

    @Test("H2: stop() awaits an in-flight sweep before returning")
    func stopAwaitsInFlightSweep() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let gate = AsyncGate()
        let sweepCompleted = OSAllocatedUnfairLock<Bool>(initialState: false)
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [gate, sweepCompleted] in
                await gate.wait()
                sweepCompleted.withLock { $0 = true }
            }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { await observer.testLastSweepAt() != nil }

        // Begin shutdown in a sibling task; release the gate after a beat
        // so we can assert stop() waited rather than returning immediately.
        let stopTask = Task { [observer] in
            await observer.stop()
        }
        for _ in 0..<10 { await Task.yield() }
        #expect(sweepCompleted.withLock { $0 } == false, "sweep should still be parked at gate")

        gate.release()
        await stopTask.value
        #expect(sweepCompleted.withLock { $0 } == true, "stop() must have awaited sweep completion")
    }

    // MARK: - M2 nil-prior-then-real-transition (cycle 1 review)

    @Test("M2: nil → true → false → true sequence still fires once on second runnable")
    func nilThenTrueThenFalseThenTrueFires() async {
        // Establish prior=true via initial snapshot (no fire), then go to
        // throttled (prior=false), then back to runnable (must fire). This
        // covers the path where a device boots ALREADY runnable.
        let provider = MockCapabilitiesProvider(initial: runnableSnapshot())
        let recorder = SweepRecorder()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == true }
        #expect(recorder.count == 0)

        provider.publish(thermalThrottledSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == false }
        #expect(recorder.count == 0)

        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        await observer.stop()
        #expect(recorder.count == 1)
    }

    // MARK: - Lifecycle edge cases (cycle 1 review)

    @Test("start() called twice is idempotent (no second loop spawned)")
    func startTwiceIsIdempotent() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { }
        )
        await observer.start()
        await yieldUntilStable { await observer.testIsLoopRunning() }

        // Second start() — must not blow up, must not spawn another loop.
        await observer.start()
        for _ in 0..<10 { await Task.yield() }
        #expect(await observer.testIsLoopRunning())

        await observer.stop()
    }

    @Test("stop() before start() is a safe no-op")
    func stopBeforeStartIsSafe() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { }
        )
        // No start() — just stop(). Should return without crashing and
        // leave the observer in shutdown state so a later start() no-ops.
        await observer.stop()

        // Subsequent start() must be rejected (didShutdown latch set).
        await observer.start()
        for _ in 0..<10 { await Task.yield() }
        #expect(!(await observer.testIsLoopRunning()))
    }

    // MARK: - M1 shared in-flight flag (cycle 1 review)

    @Test("M1: FinalPassSweepInFlightFlag tryAcquire is exclusive")
    func sweepFlagTryAcquireIsExclusive() {
        let flag = FinalPassSweepInFlightFlag()
        #expect(flag.tryAcquire(), "first acquire should succeed")
        #expect(!flag.tryAcquire(), "second acquire while held should fail")
        flag.release()
        #expect(flag.tryAcquire(), "acquire after release should succeed")
        flag.release()
    }

    @Test("M1: FinalPassSweepInFlightFlag release is idempotent")
    func sweepFlagReleaseIsIdempotent() {
        let flag = FinalPassSweepInFlightFlag()
        _ = flag.tryAcquire()
        flag.release()
        // Double-release should not corrupt state — still acquirable.
        flag.release()
        #expect(flag.tryAcquire())
        flag.release()
    }

    @Test("In-flight sweep suppresses an overlapping kick")
    func inflightSweepSuppressesOverlappingKick() async {
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        // Gated kick: the first kick parks at gate.wait() until the test
        // explicitly releases. While parked, sweepTask is non-nil, so a
        // second false→true transition must hit the outstanding-task guard.
        // Using a deterministic gate (rather than Task.sleep) avoids the
        // race where Task.yield iteration counts can outpace a wall-clock
        // sleep on a busy executor.
        let gate = AsyncGate()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [recorder, gate] in
                await gate.wait()
                recorder.bump()
            }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        // Wait for the sweep task to be created (not necessarily complete).
        await yieldUntilStable { await observer.testLastSweepAt() != nil }

        // Oscillate into a second false→true transition while sweep #1 is
        // still parked at the gate.
        provider.publish(thermalThrottledSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == false }
        provider.publish(runnableSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == true }

        // Sweep #2 should have been refused by the in-flight guard. Now
        // release the gate so kick #1 finishes and bumps the recorder.
        gate.release()
        await yieldUntilStable { recorder.count == 1 }
        for _ in 0..<10 { await Task.yield() }

        await observer.stop()
        // Only the first sweep ran — the second was suppressed by the
        // outstanding-task guard.
        #expect(recorder.count == 1)
    }

    // MARK: - Cycle 2 follow-ups

    @Test("Cycle 2 H1: thermal-state disagreement between stream and probe gates correctly")
    func thermalStateDisagreementBetweenStreamAndProbeGatesCorrectly() async {
        // Mirrors the H1 fresh-probe pattern but for thermalState rather
        // than charging. Stream snapshot says thermal=.nominal, but the
        // fresh capability snapshot probe says .serious. Observer must
        // refuse the kick, otherwise the runner would just refuse the
        // sweep with `deferReason='thermalThrottled'` and churn.
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let probedSnapshot = OSAllocatedUnfairLock<CapabilitySnapshot>(
            initialState: makeCapabilitySnapshot(thermalState: .serious, isLowPowerMode: false, isCharging: true)
        )
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            capabilitySnapshotProvider: { probedSnapshot.withLock { $0 } },
            chargeStateProvider: { true },
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        // Stream emits "runnable" but fresh probe still says thermal=.serious.
        provider.publish(runnableSnapshot())
        for _ in 0..<20 { await Task.yield() }
        #expect(recorder.count == 0)

        // Flip the probed snapshot to .nominal; next emission must fire.
        probedSnapshot.withLock {
            $0 = makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: false, isCharging: true)
        }
        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        await observer.stop()
        #expect(recorder.count == 1)
    }

    @Test("Cycle 2 H1: shutdown in progress suppresses sweep dispatch from buffered capability")
    func shutdownInProgressSuppressesBufferedCapabilityDispatch() async {
        // Reproduces the H1 race: a capability snapshot is already buffered
        // in the merged stream when stop() flips didShutdown=true. Without
        // the guard at the top of maybeFireSweep, the consumer would still
        // spawn an unstructured Task whose cancellation is NOT inherited
        // from loopTask, leaking past stop()'s return.
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        // Use a slow chargeStateProvider so isRunnableNow() parks at an
        // await between the .capability event being dequeued and
        // maybeFireSweep being called. While parked, we trigger stop().
        let gate = AsyncGate()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            chargeStateProvider: { [gate] in
                await gate.wait()
                return true
            },
            cooldownSeconds: 0,
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        // Publish runnable. The consumer parks at chargeStateProvider's
        // gate.wait() inside isRunnableNow() before flipping priorRunnable.
        provider.publish(runnableSnapshot())
        for _ in 0..<10 { await Task.yield() }

        // Begin shutdown. didShutdown=true is set synchronously on the
        // actor, BEFORE the parked consumer can resume.
        let stopTask = Task { [observer] in await observer.stop() }
        for _ in 0..<10 { await Task.yield() }

        // Release the chargeStateProvider gate. The consumer resumes,
        // sees nowRunnable=true, calls maybeFireSweep — which must short-
        // circuit because didShutdown is already true.
        gate.release()
        await stopTask.value

        // No sweep fired because the guard tripped.
        #expect(recorder.count == 0)
    }

    @Test("Cycle 2 M3: double-stop is idempotent and second caller awaits first drain")
    func doubleStopIsIdempotentAndSecondCallerAwaitsDrain() async {
        // The stop() reentrancy doc paragraph promises: a second stop()
        // landing while the first is parked at an await inside loopTask
        // drain returns ONLY after the first call's drain completes.
        // Without that guarantee, PlayheadRuntime.shutdown's idempotent-
        // stop contract leaks observer-spawned work past shutdown.
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let gate = AsyncGate()
        let sweepCompleted = OSAllocatedUnfairLock<Bool>(initialState: false)
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [gate, sweepCompleted] in
                await gate.wait()
                sweepCompleted.withLock { $0 = true }
            }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { await observer.testLastSweepAt() != nil }

        // First stop() parks awaiting the gated sweep.
        let firstStop = Task { [observer] in await observer.stop() }
        for _ in 0..<10 { await Task.yield() }

        // Second stop() lands while first is parked. Per the reentrancy
        // doc, it must take the early branch and `await loopTask.value`.
        let secondStop = Task { [observer] in await observer.stop() }
        for _ in 0..<10 { await Task.yield() }

        // Neither stop should have returned yet — sweep is still gated.
        #expect(sweepCompleted.withLock { $0 } == false)

        gate.release()
        await firstStop.value
        await secondStop.value

        // Both calls returned without crashing; sweep completed.
        #expect(sweepCompleted.withLock { $0 } == true)
    }

    @Test("Cycle 3 L3: post-skew lastSweepAt becomes the new cooldown origin")
    func postSkewLastSweepAtBecomesNewCooldownOrigin() async {
        // After a backwards skew triggers an immediate fire, lastSweepAt
        // is stamped to the post-skew clock(). The cooldown gate from
        // that new stamp must apply normally on the next oscillation.
        // Without this, a single skew event could leave the observer
        // permanently unguarded against thrash.
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let clock = TestClock()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 120,
            clock: { [clock] in clock.now },
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        // Fire #1 at t=t0.
        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        // Skew backwards, oscillate, fire #2 at t=t0-60 (the new origin).
        for _ in 0..<20 { await Task.yield() }
        provider.publish(thermalThrottledSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == false }
        clock.advance(by: -60)
        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 2 }

        // Now advance forward 60s from the post-skew origin (still inside
        // the 120s cooldown from fire #2). Oscillation should be suppressed.
        for _ in 0..<20 { await Task.yield() }
        provider.publish(thermalThrottledSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == false }
        clock.advance(by: 60)
        provider.publish(runnableSnapshot())
        for _ in 0..<20 { await Task.yield() }

        // count is still 2 — the cooldown from fire #2 (post-skew
        // lastSweepAt) suppressed fire #3.
        await observer.stop()
        #expect(recorder.count == 2)
    }

    @Test("Cycle 3 missing-test: stop() cancels the in-flight sweep Task")
    func stopCancelsInFlightSweepTask() async {
        // Verifies that the unstructured Task spawned in maybeFireSweep
        // honors cancellation propagation when stop() runs. If a future
        // edit drops Task.isCancelled checks from the production
        // kickSweep, this test catches it because the kick body is
        // checking Task.isCancelled itself.
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let gate = AsyncGate()
        let cancelObserved = OSAllocatedUnfairLock<Bool>(initialState: false)
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 0,
            kickSweep: { [gate, cancelObserved] in
                await gate.wait()
                if Task.isCancelled {
                    cancelObserved.withLock { $0 = true }
                }
            }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { await observer.testLastSweepAt() != nil }

        // Begin shutdown; kick is parked at gate. stop() will cancel
        // the sweepTask BEFORE awaiting its value (see source order in
        // FinalPassThermalRecoveryObserver.stop()). Releasing the gate
        // lets the kick body resume — and Task.isCancelled must be true.
        let stopTask = Task { [observer] in await observer.stop() }
        for _ in 0..<10 { await Task.yield() }
        gate.release()
        await stopTask.value

        #expect(cancelObserved.withLock { $0 } == true)
    }

    @Test("Cycle 2 L3: backwards clock skew does not freeze the observer")
    func backwardsClockSkewDoesNotFreezeObserver() async {
        // After a sweep fires at t0, an NTP correction or manual time
        // change rolls clock back to t0-60. With cooldown=120 the naive
        // implementation would suppress until wall-clock catches back up
        // — silently freezing the observer for the skew duration. The
        // L3 fix treats negative elapsed as "past cooldown".
        let provider = MockCapabilitiesProvider(initial: thermalThrottledSnapshot())
        let recorder = SweepRecorder()
        let clock = TestClock()
        let observer = FinalPassThermalRecoveryObserver(
            capabilities: provider,
            cooldownSeconds: 120,
            clock: { [clock] in clock.now },
            kickSweep: { [recorder] in recorder.bump() }
        )
        await observer.start()
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 1 }

        // Oscillate back to throttled, then SKEW the clock backwards.
        for _ in 0..<20 { await Task.yield() }
        provider.publish(thermalThrottledSnapshot())
        await yieldUntilStable { await observer.testPriorRunnable() == false }

        clock.advance(by: -60)  // backwards skew

        provider.publish(runnableSnapshot())
        await yieldUntilStable { recorder.count == 2 }

        await observer.stop()
        // Without the L3 guard, this would still be 1 because elapsed=-60
        // is < cooldown=120 and the naive comparison would suppress.
        #expect(recorder.count == 2)
    }
}
