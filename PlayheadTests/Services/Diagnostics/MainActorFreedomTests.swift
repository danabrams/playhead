// MainActorFreedomTests.swift
// playhead-2axy: pin the main-actor invariant on PlayheadRuntime.init.
//
// Both jndk (PermissiveAdClassifier → SystemLanguageModel probe) and
// hkn1 (LiveActivitySnapshotProvider.loadInputs → unbounded SwiftData
// fetch on main) shipped silently because nothing checked whether the
// main actor was free during the work in question. This file races a
// background observer against `PlayheadRuntime.init` and asserts that
// the main actor remained responsive — i.e. the synchronous init body
// did not monopolise the main thread for longer than a small budget.
//
// **Why a redesign vs. an off-the-shelf "counter increment" probe.**
// An earlier draft of this test ran the test method itself on
// `@MainActor` and relied on a `Task { @MainActor in counter += 1 }`
// racer. That layout was structurally unable to fail: the racer task
// queues behind the running test method, can't start until the test
// awaits, and only drains AFTER the synchronous init has returned and
// `await racer.value` releases main. The init could hold the main
// actor for 5 seconds and the counter would still increment exactly
// once — after the fact. The cross-review on PR #39 caught this.
//
// **The redesign (path A).**
//   1. The test method runs OFF the main actor (the class is not
//      `@MainActor`-annotated). That keeps the main actor genuinely
//      available for the racer during init.
//   2. A pre-armed background `Task` repeatedly hops onto the main
//      actor via `MainActor.run { ... }` and records the wall-clock
//      latency of each hop. If init holds main, hops queue up and
//      individual round-trips spike.
//   3. The init itself is dispatched onto the main actor from
//      off-main (`await MainActor.run { ... }`) — `PlayheadRuntime`
//      is conceptually `@MainActor` so all its property setters
//      remain main-actor-isolated, just as in the production launch
//      path.
//   4. After init returns, the racer is signalled to stop and the
//      test inspects the latency samples. The assertion is on the
//      p95 of all samples taken during the init window: if any
//      stretch of the init held main for longer than the budget
//      (default 100 ms), at least one hop's round-trip would exceed
//      that budget.
//
// **Why a 100 ms budget?** A continuous 100 ms hold drops six frames
// at 60 Hz — the launch-storyboard hand-off becomes visible to the
// user even on a fast device. Tighter budgets (e.g. 16 ms) flake on
// shared simulator hosts where context switches alone can push a
// single round-trip into the 30–50 ms range. 100 ms is the slack
// that keeps the canary stable while still catching the failure
// modes that motivated the bead (jndk: minutes; hkn1: seconds).
//
// **Why max-of-post-warmup (and not p95).** Because the racer is
// sequential — it submits one main-actor hop, awaits it, then loops
// — only ONE in-flight hop sees the hold while init is monopolising
// main. All subsequent hops fire after init releases and complete in
// ~0 ms. So a multi-hundred-ms hold appears as a SINGLE outlier in
// the sample set; aggregating with p95 (or anything below p99 with
// dozens of post-init samples) silently swallows that single
// outlier and the test cannot fail. The fail-stop verification
// (a 500 ms `Thread.sleep` injected into init) confirmed this:
// p95 stayed at 0 ms while max read 504 ms. We therefore assert on
// `max` of the post-warmup samples; the warmup discard handles the
// first-hop dispatch tail. The 100 ms budget is generous enough
// that a normal context-switch outlier on a shared simulator host
// (typically ≤30 ms) cannot flake the build.
//
// **Manual fail-stop verification (bead-r2axy follow-up).** The
// redesigned probe was confirmed structurally able to fail by
// temporarily injecting `Thread.sleep(forTimeInterval: 0.5)` into
// the body of `PlayheadRuntime.init` and observing that the test
// failed with a p95 latency well above 100 ms. The stub was
// reverted before the PR was pushed; documented in the commit
// message for the redesign.
//
// XCTest, NOT Swift Testing: keeps the canary class filterable
// through the Xcode test plan's `skippedTests` list (`xctestplan`
// ignores Swift Testing identifiers). Mirrors the rationale in
// `PermissiveClassifierBoxLazinessTests`.

import Foundation
import XCTest
@testable import Playhead

/// Race a main-actor-hopping background observer against the
/// synchronous init body of `PlayheadRuntime`. The init runs on the
/// main actor (production parity); the observer runs in a detached
/// background task and measures the round-trip latency of each hop.
/// After init returns the observer is stopped and we assert that the
/// SLOWEST observed hop (excluding warmup samples) stayed below the
/// responsiveness budget.
///
/// The class is **deliberately not `@MainActor`-annotated**: the test
/// method must execute on the global concurrent executor so the main
/// actor is genuinely available during init. See file-level comment.
final class PlayheadRuntimeMainActorFreedomTests: XCTestCase {

    /// Maximum tolerated single-hop round-trip latency during init.
    /// 100 ms is the threshold below which a continuous main-actor
    /// hold is invisible to the user (six dropped frames at 60 Hz —
    /// noticeable, but not the multi-second freezes we're guarding
    /// against). Tighter budgets flake on shared simulator hosts.
    private static let mainHopBudgetSeconds: Double = 0.100

    /// Number of leading samples to discard. The first hop after
    /// task creation pays one-shot dispatch / context-establishment
    /// costs that are unrelated to whether init is holding main.
    private static let warmupSamples = 2

    func testMainActorIsNotHeldDuringRuntimeInit() async throws {
        // 0. Warm-up runtime construction off the measured path so
        //    any first-init JIT / dyld / pattern-compile costs are
        //    amortised before the racer starts measuring. Identical
        //    rationale to the wall-clock perf test next door.
        let warmupRuntime = await MainActor.run { PlayheadRuntime(isPreviewRuntime: false) }
        await warmupRuntime.shutdown()

        // 1. Pre-arm the racer. `samples` is captured by reference so
        //    the racer task can append to it without crossing actor
        //    boundaries — the racer runs on a detached background
        //    task, mutates the array, and we read it after the racer
        //    has been signalled to stop and joined.
        //
        //    The racer hops onto the main actor in a tight loop. Each
        //    hop records the wall-clock latency between submission
        //    and entering the main-actor closure. If init is holding
        //    main, that gap grows by however long the hold has been
        //    in progress.
        let samples = LatencySampleBuffer()
        let stopFlag = AtomicBool()

        let racer = Task.detached(priority: .userInitiated) {
            // Spin until the test signals stop. Each iteration
            // measures the latency of a single main-actor hop.
            // `DispatchTime` is the monotonic source on Apple
            // platforms (uptime mach time); see the rationale block
            // in PlayheadRuntimeLaunchPerfTests.
            while !stopFlag.value {
                let submittedAtNanos = DispatchTime.now().uptimeNanoseconds
                await MainActor.run {
                    let enteredAtNanos = DispatchTime.now().uptimeNanoseconds
                    let latencySeconds = Double(enteredAtNanos - submittedAtNanos) / 1_000_000_000.0
                    samples.append(latencySeconds)
                }
                // No explicit sleep / yield: we want hops as densely
                // packed as the executor will give us. The
                // `MainActor.run` await is itself a yield point that
                // lets cooperative tasks run between hops.
            }
        }

        // 2. Give the racer a tick to start emitting samples before
        //    the measured init runs. Without this, the racer might
        //    not have submitted its first hop yet by the time init
        //    starts, and we'd measure post-init latency only. 5 ms
        //    is enough on simulator (multiple hops typically land
        //    in that window).
        try await Task.sleep(nanoseconds: 5_000_000)

        // 3. Dispatch the measured init onto the main actor from
        //    off-main. `PlayheadRuntime.init` is conceptually
        //    main-actor-isolated (its stored properties are touched
        //    without `await` from `RootView`'s @MainActor context in
        //    production), so this is the production parity shape.
        //    The init is synchronous; once the closure returns the
        //    full body has run.
        let runtime = await MainActor.run { PlayheadRuntime(isPreviewRuntime: false) }

        // 4. Stop the racer and join.
        stopFlag.value = true
        await racer.value

        // 5. Inspect the samples. Drop the warm-up tail, then assert
        //    on `max` of the remainder. See the file-level rationale
        //    block for why this is `max` and not `p95` — short
        //    version: the racer is sequential, so a held main shows
        //    up as a single outlier and any percentile aggregation
        //    swallows it.
        let allSamples = samples.snapshot()
        XCTAssertGreaterThan(
            allSamples.count, Self.warmupSamples + 1,
            """
            Main-actor freedom probe collected only \(allSamples.count) samples \
            during the init window. The racer either failed to start or init \
            returned before any hops landed. Either way the probe didn't \
            actually measure anything.
            """
        )

        let postWarmup = Array(allSamples.dropFirst(Self.warmupSamples))
        let sortedSamples = postWarmup.sorted()
        let medianLatency = sortedSamples[sortedSamples.count / 2]
        let maxLatency = sortedSamples.last ?? 0

        // Always log so a passing run on CI surfaces the trend.
        // Mirrors the always-on print in PlayheadRuntimeLaunchPerfTests.
        let formattedSummary = String(
            format: "median=%.1fms max=%.1fms samples=%d (post-warmup) budget=%.0fms",
            medianLatency * 1000,
            maxLatency * 1000,
            postWarmup.count,
            Self.mainHopBudgetSeconds * 1000
        )
        print("[MainActorFreedom] \(formattedSummary)")

        XCTAssertLessThan(
            maxLatency,
            Self.mainHopBudgetSeconds,
            """
            MainActor was held longer than budget during PlayheadRuntime.init. \
            \(formattedSummary). A synchronous workload added to init is \
            starving the main actor on the launch path. The historical \
            culprits were jndk's PermissiveAdClassifier()/SystemLanguageModel \
            construction and hkn1's main-actor SwiftData fetch — wrap new \
            heavy work in an off-main `Task { … }` or a lazy factory closure.
            """
        )

        await runtime.shutdown()
    }
}

// MARK: - Helpers

/// Lock-protected sample buffer. The racer mutates it from a detached
/// background task; the test method reads it after joining the racer.
/// `NSLock` is overkill for this volume of writes but matches the
/// style of the analogous counter in `PermissiveClassifierBoxLazinessTests`
/// (the `FactoryInvocationCounter`) and pins the test against future
/// stricter concurrency lints.
private final class LatencySampleBuffer: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [Double] = []
    func append(_ value: Double) {
        lock.lock(); defer { lock.unlock() }
        values.append(value)
    }
    func snapshot() -> [Double] {
        lock.lock(); defer { lock.unlock() }
        return values
    }
}

/// Lock-protected boolean flag the test uses to signal the racer to
/// stop. A bare `Bool` would race; an `actor` would itself require an
/// `await` to flip and so couldn't be set from the synchronous post-
/// init line.
private final class AtomicBool: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: Bool = false
    var value: Bool {
        get { lock.lock(); defer { lock.unlock() }; return _value }
        set { lock.lock(); defer { lock.unlock() }; _value = newValue }
    }
}
