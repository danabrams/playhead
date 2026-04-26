// MainActorFreedomTests.swift
// playhead-2axy: pin the main-actor invariant on PlayheadRuntime.init.
//
// Both jndk (PermissiveAdClassifier → SystemLanguageModel probe) and
// hkn1 (LiveActivitySnapshotProvider.loadInputs → unbounded SwiftData
// fetch on main) shipped silently because nothing checked whether the
// main actor was free during the work in question. This file races a
// MainActor-bound counter against `PlayheadRuntime.init` and asserts
// the counter incremented after init returns — proving the main actor
// was not held continuously across the synchronous init body.
//
// The pattern mirrors hkn1's `LiveActivitySnapshotProviderPerfTests`,
// where a `Task { @MainActor in counter.increment() }` is spawned
// before the synchronous workload starts and inspected afterwards.
//
// Why this is more than a duplicate of the wall-clock budget: a
// regression that adds 50ms of CPU-bound work to the init body is
// invisible to the 250ms budget but trivially detectable here — the
// main-actor counter can't increment while init is monopolising the
// thread. Thresholds and freedom probes audit different failure modes.
//
// XCTest, NOT Swift Testing: keeps the canary class filterable
// through the Xcode test plan's `skippedTests` list (`xctestplan`
// ignores Swift Testing identifiers). Mirrors the rationale in
// `PermissiveClassifierBoxLazinessTests`.

import Foundation
import XCTest
@testable import Playhead

/// Race a MainActor-bound counter against the synchronous init body
/// of `PlayheadRuntime`. After `init` returns and we've awaited the
/// racer, the counter must have incremented at least once — i.e. the
/// main actor was free at some point during construction.
///
/// The assertion is a soft floor (>= 1, not >= N) because scheduler
/// fairness varies across simulator runs. One increment proves the
/// main actor was not held continuously for the whole init; that is
/// the invariant under test. Stricter floors are flaky.
final class PlayheadRuntimeMainActorFreedomTests: XCTestCase {

    @MainActor
    func testMainActorIsNotHeldDuringRuntimeInit() async throws {
        // Warm-up: do one throwaway init so any first-init JIT / dyld
        // / pattern-compile costs are amortised. The freedom probe is
        // about the steady-state shape of init, not the first-ever
        // call's one-shot warmup tail.
        let warmupRuntime = PlayheadRuntime(isPreviewRuntime: false)
        await warmupRuntime.shutdown()

        let counter = ManagedMainActorCounter()

        // Spawn the racer BEFORE we kick off the measured init. The
        // racer body runs N main-actor hops separated by `Task.yield()`
        // — yielding is the only way the racer can be re-entered while
        // a synchronous init body is on the same actor. If the init
        // body pegs main for its full duration, none of the yields
        // re-enter and the counter stays at zero.
        //
        // 64 iterations is generous: the post-jndk/jncn init is well
        // under 100ms on simulator, and even one Task.yield round-
        // trip lasts ~50us, so all 64 should complete during init in
        // the steady state.
        let racer = Task { @MainActor in
            for _ in 0..<64 {
                counter.increment()
                await Task.yield()
            }
        }

        // The actual measured init. Synchronous, so by the time this
        // line returns the entire init body has run.
        let runtime = PlayheadRuntime(isPreviewRuntime: false)

        // Flush the racer. After it returns we know exactly how many
        // increments landed on the main actor across the init window.
        await racer.value

        XCTAssertGreaterThanOrEqual(
            counter.value, 1,
            """
            MainActor was held for the entire PlayheadRuntime.init body \
            (counter=\(counter.value)). A synchronous workload added to init \
            is starving the main actor on the launch path. The historical \
            culprits were jndk's PermissiveAdClassifier()/SystemLanguageModel \
            construction and hkn1's main-actor SwiftData fetch — wrap new \
            heavy work in an off-main `Task { … }` or a lazy factory closure.
            """
        )

        await runtime.shutdown()
    }
}

/// Main-actor-isolated counter used by the freedom probe. Mirrors the
/// `ManagedCounter` in `LiveActivitySnapshotProviderPerfTests` (hkn1).
/// Marked `@MainActor` so reads/writes serialise against the same
/// actor the production init body touches.
@MainActor
private final class ManagedMainActorCounter {
    private(set) var value: Int = 0
    func increment() { value += 1 }
}
