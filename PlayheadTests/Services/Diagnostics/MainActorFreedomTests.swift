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
// **The sample-count guard is FATAL, and it used to take the host
// down (playhead-1och).** The count check below was a plain
// `XCTAssertGreaterThan`, which RECORDS a failure and CONTINUES — the
// class never set `continueAfterFailure = false`. So on a run that
// collected exactly `warmupSamples` samples, the assertion correctly
// reported "the probe didn't actually measure anything" and then the
// very next line subscripted the empty post-warmup array:
// `Swift/ContiguousArrayBuffer.swift:695: Fatal error: Index out of
// range`. xcodebuild restarted the host and printed `Test Suite …
// passed … Executed 0 tests`, so the crash destroyed the evidence of
// itself AND laundered the failure into a green-looking suite. It is a
// `guard … else { XCTFail(…); return }` now: a violated precondition
// stops this test instead of walking into an unguarded read. Note the
// neighbouring `maxLatency` read was already written defensively
// (`?? 0`) and the median read was not — fixing the median read alone
// would have been the wrong fix, because a probe that measured nothing
// must FAIL LOUDLY rather than report a median over no data.
//
// **Each hop records WHEN it was submitted, not just how long it
// took**, and the init records its own body duration. A low sample
// count has two readings that want opposite fixes — the racer was
// starved (probe broken) or the main actor was held for the whole
// window (product broken) — and a bare count cannot tell them apart.
// The timeline can: a starved probe shows late submissions with SMALL
// latencies, a held main shows an early submission whose latency spans
// the hold. The init-body duration then says whether the hold is
// init's synchronous body or main-actor work spawned around it, which
// is the difference between this test's failure message being right
// and being merely suggestive.
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

    // Load-sensitive latency measurement — runs only in the serial perf pass
    // where the CPU is quiescent. See PerfGate / playhead-zx0l.
    override func setUpWithError() throws {
        try XCTSkipUnless(PerfGate.runsMeasurementTests, PerfGate.skipReason)
    }

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
        let samples = HopSampleBuffer()
        let stopFlag = AtomicBool()
        // t0 for every offset reported below. Taken before the racer is
        // created so "the racer's first hop landed at +Xms" is a
        // statement about the racer's own start-up, not about when we
        // happened to start the clock.
        let probeStartNanos = DispatchTime.now().uptimeNanoseconds

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
                    samples.append(HopSample(
                        submitOffsetSeconds: elapsedSeconds(probeStartNanos, submittedAtNanos),
                        latencySeconds: elapsedSeconds(submittedAtNanos, enteredAtNanos)
                    ))
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
        //
        //    playhead-1och: the init body is timed FROM INSIDE the
        //    main-actor closure. The hop latencies below say the main
        //    actor was held; only this says whether it was held by
        //    init's own synchronous body or by main-actor work init
        //    spawned. Those are different defects with different fixes,
        //    and the assertion message at the bottom names the first.
        let (runtime, initBodyStartNanos, initBodyEndNanos) = await MainActor.run {
            () -> (PlayheadRuntime, UInt64, UInt64) in
            let bodyStartNanos = DispatchTime.now().uptimeNanoseconds
            let created = PlayheadRuntime(isPreviewRuntime: false)
            return (created, bodyStartNanos, DispatchTime.now().uptimeNanoseconds)
        }

        // 4. Stop the racer and join.
        stopFlag.value = true
        await racer.value
        let probeEndNanos = DispatchTime.now().uptimeNanoseconds

        // 5. Inspect the samples. Drop the warm-up tail, then assert
        //    on `max` of the remainder. See the file-level rationale
        //    block for why this is `max` and not `p95` — short
        //    version: the racer is sequential, so a held main shows
        //    up as a single outlier and any percentile aggregation
        //    swallows it.
        let allSamples = samples.snapshot()

        // The window description is computed BEFORE the count guard so a
        // run that measured nothing still reports what the window looked
        // like. A bare "collected only 2 samples" is what this test used
        // to emit, and it is unactionable: it names a symptom shared by
        // two opposite causes.
        let initBodySeconds = elapsedSeconds(initBodyStartNanos, initBodyEndNanos)
        let initBodyStartOffset = elapsedSeconds(probeStartNanos, initBodyStartNanos)
        let initBodyEndOffset = elapsedSeconds(probeStartNanos, initBodyEndNanos)
        let windowSummary = String(
            format: "probe window=%.1fms, init body=%.1fms (+%.1fms…+%.1fms)",
            elapsedSeconds(probeStartNanos, probeEndNanos) * 1000,
            initBodySeconds * 1000,
            initBodyStartOffset * 1000,
            initBodyEndOffset * 1000
        )

        guard allSamples.count > Self.warmupSamples + 1 else {
            XCTFail("""
            Main-actor freedom probe collected only \(allSamples.count) samples \
            during the init window, so it measured nothing. \(windowSummary). \
            Hops (submitted-at/latency): [\(Self.describe(allSamples))].

            READ THE TIMELINE BEFORE RE-RUNNING — the two causes want opposite \
            fixes. Late submissions carrying SMALL latencies mean the racer was \
            never scheduled and the PROBE is broken. An early submission whose \
            latency spans most of the window means the main actor really was \
            held for the whole init and the LAUNCH PATH is broken — the same \
            defect PlayheadRuntimeLaunchPerfTests measures from the other side. \
            Do not "fix" this by widening the read of an empty array.
            """)
            await runtime.shutdown()
            return
        }

        let postWarmup = Array(allSamples.dropFirst(Self.warmupSamples))
        let sortedSamples = postWarmup.map(\.latencySeconds).sorted()
        let medianLatency = sortedSamples[sortedSamples.count / 2]
        let maxLatency = sortedSamples.last ?? 0

        // Locate the worst hop against the init body. `median≈0 with one
        // large max` is the documented signature of a single hold (the
        // racer is sequential), so WHERE that one hop sits is the whole
        // diagnosis: a hold that does not overlap the synchronous body
        // is main-actor work init SPAWNED, not init itself.
        let worstHopDescription: String
        if let worstHop = postWarmup.max(by: { $0.latencySeconds < $1.latencySeconds }) {
            let hopEndOffset = worstHop.submitOffsetSeconds + worstHop.latencySeconds
            let overlapsInitBody =
                hopEndOffset > initBodyStartOffset && worstHop.submitOffsetSeconds < initBodyEndOffset
            worstHopDescription = String(
                format: " worst hop +%.1fms…+%.1fms (%@ the init body)",
                worstHop.submitOffsetSeconds * 1000,
                hopEndOffset * 1000,
                overlapsInitBody ? "OVERLAPS" : "does NOT overlap"
            )
        } else {
            worstHopDescription = ""
        }

        // Always log so a passing run on CI surfaces the trend.
        // Mirrors the always-on print in PlayheadRuntimeLaunchPerfTests.
        let formattedSummary = String(
            format: "median=%.1fms max=%.1fms samples=%d (post-warmup) budget=%.0fms",
            medianLatency * 1000,
            maxLatency * 1000,
            postWarmup.count,
            Self.mainHopBudgetSeconds * 1000
        ) + ", \(windowSummary)\(worstHopDescription)"
        print("[MainActorFreedom] \(formattedSummary)")

        XCTAssertLessThan(
            maxLatency,
            Self.mainHopBudgetSeconds,
            """
            MainActor was held longer than budget during PlayheadRuntime.init. \
            \(formattedSummary). Heavy work on the launch path is starving the \
            main actor. The historical culprits were jndk's \
            PermissiveAdClassifier()/SystemLanguageModel construction and hkn1's \
            main-actor SwiftData fetch — wrap new heavy work in an off-main \
            `Task { … }` or a lazy factory closure. Check `worst hop` above \
            first: if it does NOT overlap the init body, the hold is main-actor \
            work init SPAWNED rather than the synchronous body, and the fix \
            belongs at that call site instead.
            """
        )

        await runtime.shutdown()
    }

    /// Renders a hop timeline compactly enough for a failure message.
    private static func describe(_ hops: [HopSample]) -> String {
        hops
            .map { String(format: "+%.1fms/%.1fms", $0.submitOffsetSeconds * 1000, $0.latencySeconds * 1000) }
            .joined(separator: ", ")
    }
}

// MARK: - Helpers

/// Monotonic elapsed seconds between two `DispatchTime` uptime readings.
/// Free function rather than a static so the detached racer can call it
/// without capturing the test class.
private func elapsedSeconds(_ fromNanos: UInt64, _ toNanos: UInt64) -> Double {
    Double(toNanos &- fromNanos) / 1_000_000_000.0
}

/// One main-actor hop: when the racer submitted it (as an offset from
/// the probe's t0) and how long the round-trip took. The SUBMIT OFFSET
/// is the field that makes a starved probe distinguishable from a held
/// main actor; a latency alone cannot say which.
private struct HopSample: Sendable {
    let submitOffsetSeconds: Double
    let latencySeconds: Double
}

/// Lock-protected sample buffer. The racer mutates it from a detached
/// background task; the test method reads it after joining the racer.
/// `NSLock` is overkill for this volume of writes but matches the
/// style of the analogous counter in `PermissiveClassifierBoxLazinessTests`
/// (the `FactoryInvocationCounter`) and pins the test against future
/// stricter concurrency lints.
private final class HopSampleBuffer: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [HopSample] = []
    func append(_ value: HopSample) {
        lock.lock(); defer { lock.unlock() }
        values.append(value)
    }
    func snapshot() -> [HopSample] {
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
