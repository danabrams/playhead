// FMInferenceDeadlineTests.swift
// playhead-8d5r: the per-call Foundation Models inference deadline.

import Dispatch
import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

/// An operation that ignores cooperative cancellation entirely, the way a
/// framework call implemented outside Swift concurrency does.
///
/// This is the single most important fixture in the file. A
/// `withThrowingTaskGroup` race — the shape `ChapterLabelingService` uses —
/// cancels its children AND AWAITS them when the body throws, so against an
/// operation like this it reports a timeout only AFTER the call it was supposed
/// to bound has finished. That is not a bound. `deadlineReturnsPromptlyEven`
/// `WhenTheOperationIgnoresCancellation` is what keeps
/// `FMInferenceDeadline.run` honest about this.
private func uncancellableSleep(seconds: Double) async {
    await withUnsafeContinuation { continuation in
        DispatchQueue.global().asyncAfter(deadline: .now() + seconds) {
            continuation.resume()
        }
    }
}

private struct DeadlineTestError: Error, Equatable {
    let tag: String
}

private func elapsedSeconds(_ body: () async throws -> Void) async -> Double {
    let clock = ContinuousClock()
    let start = clock.now
    try? await body()
    let elapsed = clock.now - start
    return Double(elapsed.components.seconds)
        + Double(elapsed.components.attoseconds) / 1e18
}

@Suite("playhead-8d5r: FM inference deadline")
struct FMInferenceDeadlineTests {

    // MARK: - The primitive

    @Test("a call that finishes inside the deadline returns its value untouched")
    func fastCallReturnsValue() async throws {
        let value = try await FMInferenceDeadline.run(.seconds(30)) {
            "answered"
        }
        #expect(value == "answered")
    }

    /// Safe in the parallel gate: it asserts only WHICH error is thrown, never
    /// how fast. Starvation delays the timeout; it cannot turn it into success,
    /// because the operation sleeps 600x the budget.
    @Test("a call that outlives the deadline throws FMInferenceTimeoutError")
    func slowCallTimesOut() async {
        await #expect(throws: FMInferenceTimeoutError(deadline: .milliseconds(50))) {
            try await FMInferenceDeadline.run(.milliseconds(50)) {
                try await Task.sleep(for: .seconds(30))
                return "never"
            }
        }
    }

    @Test("a real model error propagates unchanged rather than becoming a timeout")
    func realErrorIsNotRelabelledAsTimeout() async {
        await #expect(throws: DeadlineTestError(tag: "refusal")) {
            try await FMInferenceDeadline.run(.seconds(30)) {
                throw DeadlineTestError(tag: "refusal")
            }
        }
    }

    /// A BG-window expiry must surface as `CancellationError`, never as a
    /// timeout. The two take different recovery paths — `.resumeFromCheckpoint`
    /// versus `.persistFailure` — so conflating them would make an interrupted
    /// pass look like a model failure and lose its checkpoint.
    @Test("caller cancellation surfaces as CancellationError, not a timeout")
    func callerCancellationIsNotATimeout() async throws {
        let started = AsyncSignal()
        let task = Task { () -> Result<String, Error> in
            do {
                let value = try await FMInferenceDeadline.run(.seconds(120)) {
                    await started.signal()
                    try await Task.sleep(for: .seconds(120))
                    return "never"
                }
                return .success(value)
            } catch {
                return .failure(error)
            }
        }

        await started.wait()
        task.cancel()

        let outcome = await task.value
        guard case let .failure(error) = outcome else {
            Issue.record("expected the cancelled call to fail")
            return
        }
        #expect(error is CancellationError)
        #expect(!(error is FMInferenceTimeoutError))
        #expect(SemanticScanStatus.from(error: error) == .cancelled)
    }

    @Test("a non-positive deadline disables the bound instead of timing out immediately")
    func zeroDeadlineDisablesTheBound() async throws {
        // A caller that opts out must get the pre-8d5r behavior, not an
        // instant failure — otherwise `.zero` would be a footgun rather than an
        // off switch.
        let value = try await FMInferenceDeadline.run(.zero) {
            try await Task.sleep(for: .milliseconds(20))
            return "answered"
        }
        #expect(value == "answered")
    }

    // MARK: - The value, checked against the real measured tail

    /// The 22 successful scans on the 2026-07-30 device pull that took longer
    /// than 30 s, in descending order, in seconds. Verbatim from
    /// `semantic_scan_results WHERE status = 'success' AND latencyMs > 30000`.
    ///
    /// These are REAL calls that produced REAL coverage. Every one of them
    /// below the shipped deadline is coverage the deadline must not destroy.
    private static let measuredSuccessTailSeconds: [Double] = [
        1122.4, 88.6, 86.7, 76.7, 67.6, 60.5, 59.0, 58.3, 56.6, 56.6, 56.0,
        54.8, 52.1, 51.0, 49.1, 40.3, 40.2, 39.0, 34.6, 34.1, 33.2, 31.3,
    ]

    /// Total successful scans in the same pull. The tail above is the part of
    /// this population that any plausible deadline could touch.
    private static let measuredSuccessCount = 200

    private static func successesKilled(byDeadlineSeconds deadline: Double) -> Int {
        measuredSuccessTailSeconds.count { $0 > deadline }
    }

    /// THE ACCEPTANCE ASSERTION: no legitimate slow success is killed, checked
    /// against the real distribution rather than a fixture invented to pass.
    ///
    /// This test fails loudly if anyone tightens the constant, and the failure
    /// message states the coverage cost in real calls.
    @Test("the shipped deadline kills only the single pathological success in the measured tail")
    func shippedDeadlineRespectsTheMeasuredSuccessTail() {
        let deadlineSeconds = Double(FMInferenceDeadline.standard.components.seconds)
        let killed = Self.successesKilled(byDeadlineSeconds: deadlineSeconds)

        #expect(
            killed == 1,
            """
            the shipped \(deadlineSeconds)s deadline would have killed \(killed) of \
            \(Self.measuredSuccessCount) real successful scans. Only the single \
            1122.4s outlier is acceptable — anything more is silent coverage loss.
            """
        )

        // The one it kills is that outlier, and nothing else.
        let survivorCeiling = Self.measuredSuccessTailSeconds
            .filter { $0 <= deadlineSeconds }
            .max() ?? 0
        #expect(survivorCeiling == 88.6)

        // Err long: the deadline must clear the slowest LEGITIMATE success by a
        // real margin, not scrape past it. 88.6s is the second-slowest success
        // and the top of the hard gap in the distribution.
        #expect(
            deadlineSeconds >= 88.6 * 3,
            "a hang-breaker must leave headroom for a device slower than the one measured"
        )
    }

    /// The rejected alternative, kept as an executable record of WHY. A minute
    /// looks tidy and costs 3% of real coverage.
    @Test("a 60s deadline would have destroyed 6 real successful scans")
    func sixtySecondDeadlineWouldHaveBeenTooAggressive() {
        #expect(Self.successesKilled(byDeadlineSeconds: 60) == 6)
        // Every candidate from 90s up costs the same single call, which is why
        // the value was chosen for headroom rather than tightness.
        #expect(Self.successesKilled(byDeadlineSeconds: 90) == 1)
        #expect(Self.successesKilled(byDeadlineSeconds: 120) == 1)
        #expect(Self.successesKilled(byDeadlineSeconds: 300) == 1)
        #expect(Self.successesKilled(byDeadlineSeconds: 600) == 1)
    }

    /// Behavioural counterpart to the arithmetic above: a call that lands just
    /// inside the deadline must still SUCCEED, not merely be counted as
    /// surviving. Scaled down so it runs at gate speed while preserving the
    /// shape — the call takes ~40% of its budget, the same ratio an 88.6s
    /// success has against the shipped 300s.
    @Test("a slow call that lands inside the deadline still succeeds")
    func slowButLegitimateCallStillSucceeds() async throws {
        // 150x margin, deliberately. An earlier 500ms budget against a 200ms
        // call was a race the parallel gate lost: under ~8,300 concurrent
        // tests a 200ms sleep does not resume in 500ms. The point of this test
        // is that a call finishing inside its budget SUCCEEDS, which does not
        // require the budget to be tight.
        let value = try await FMInferenceDeadline.run(.seconds(30)) {
            try await Task.sleep(for: .milliseconds(200))
            return "answered slowly"
        }
        #expect(value == "answered slowly")
    }

    // MARK: - Status vocabulary

    @Test("a deadline maps to its own named status, not to an existing failure class")
    func timeoutMapsToNamedStatus() {
        let status = SemanticScanStatus.from(
            error: FMInferenceTimeoutError(deadline: .seconds(300))
        )
        #expect(status == .inferenceTimeout)
        // The two classes it would otherwise have been folded into. Keeping
        // them distinct is what makes the before/after measurable at all.
        #expect(status != .failedTransient)
        #expect(status != .cancelled)
        #expect(SemanticScanStatus.inferenceTimeout.rawValue == "inference_timeout")
    }

    @Test("a timeout does not retry in the same pass, and is not an examined window")
    func timeoutRecoveryContract() {
        // A same-pass retry would spend a SECOND full deadline to learn what
        // the first one already established.
        #expect(SemanticScanStatus.inferenceTimeout.retryPolicy == .persistFailure)
        // Nothing was screened, so it must not shrink the scanned-duration
        // denominator (playhead-qbib's invariant).
        #expect(!SemanticScanStatus.inferenceTimeout.didExamineWindow)
        // Window-scoped so playhead-bkhc's banking of completed windows
        // survives one slow window.
        #expect(SemanticScanStatus.inferenceTimeout.failureScope == .window)
        // Apple's safety layer never spoke, so the permissive-guardrail
        // mitigation must not fire.
        #expect(!SemanticScanStatus.inferenceTimeout.isSafetyBlock)
    }

    @Test("playhead-gqx4 cause vocabulary names the timeout rather than guessing")
    func adScanLimitNamesTheTimeout() {
        #expect(
            AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [.inferenceTimeout])
                == .timedOut
        )
        // Distinct from the two it superficially resembles: nothing outside the
        // model stopped it (`interrupted`), and the model did not return an
        // unrecognised error (`transient`).
        #expect(
            AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [.cancelled]) == .interrupted
        )
        #expect(
            AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [.failedTransient]) == .transient
        )
    }
}

// MARK: - Composition with the coarse pass

/// playhead-8d5r x playhead-bkhc. The deadline lives inside a pass whose whole
/// point (bkhc, merged as #307) is that interrupted work is BANKED rather than
/// discarded. These tests assert the two compose: a timeout must cost the
/// window it hit and nothing else.
///
/// Windowing knobs: `contextSize` 431 with a `lines * 8` token rule yields a
/// coarse budget of 52; a one-segment prompt is 48 and a two-segment prompt is
/// 56, so every segment becomes its own window. Same arithmetic as
/// `BackfillRateLimitDeferTests`.
@Suite("playhead-8d5r: deadline inside the coarse pass")
struct FMInferenceDeadlineCoarsePassTests {

    private static let contextSize = 431
    private static let coarseSchemaTokenCount = 4

    /// DETERMINISTIC BY CONSTRUCTION. These tests inject the error the deadline
    /// throws instead of hanging a stub past a short `inferenceDeadline`.
    ///
    /// The first version did hang, with a 150 ms deadline. It passed in
    /// isolation and FAILED in the gate: Swift Testing runs ~8,300 tests
    /// concurrently in-process, and under that starvation a stub that normally
    /// returns in microseconds blows a 150 ms budget — a 200 ms timer was
    /// measured taking 54 s to be noticed. Every window then "timed out" and
    /// the assertions collapsed. Widening the budget would only move the
    /// flake, because the quantity being raced is exactly the quantity the
    /// load perturbs.
    ///
    /// `FMInferenceDeadline.run` propagates a thrown `FMInferenceTimeoutError`
    /// unchanged, so injecting it exercises byte-identical downstream handling
    /// with zero wall-clock dependence. The deadline's own TIMING behaviour is
    /// a separate question, measured under `PerfGate` in
    /// `FMInferenceDeadlineTimingTests`.
    private static func config() -> FoundationModelClassifier.Config {
        FoundationModelClassifier.Config(
            safetyMarginTokens: 5,
            coarseMaximumResponseTokens: 6,
            refinementMaximumResponseTokens: 12
        )
    }

    private static func tokenRule() -> @Sendable (String) -> Int {
        { prompt in prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 8 }
    }

    private static func segments(count: Int) -> [AdTranscriptSegment] {
        makeFMSegments(
            analysisAssetId: "asset-8d5r",
            transcriptVersion: "tx-8d5r-v1",
            lines: (0..<count).map { index in
                (Double(index) * 10, Double(index + 1) * 10, "Window \(index) of the episode under test.")
            }
        )
    }

    /// `timeoutOn` is the set of 1-based coarse call ordinals whose call is
    /// reported as having exceeded the deadline. `slowSuccessOn` ordinals
    /// answer successfully after `slowSeconds` — used only where a test needs
    /// one window to have measurably MORE cost than another, never as a race.
    private static func runtime(
        timeoutOn: Set<Int>,
        callCount: Int,
        otherFailures: [Int: TestFMRuntimeFailure] = [:],
        slowSuccessOn: Set<Int> = [],
        slowSeconds: Double = 0
    ) -> TestFMRuntime {
        let failures: [TestFMRuntimeFailure?] = (1...max(1, callCount)).map { ordinal in
            if timeoutOn.contains(ordinal) { return .inferenceTimeout }
            return otherFailures[ordinal]
        }
        return TestFMRuntime(
            coarseFailures: failures,
            contextSize: contextSize,
            coarseSchemaTokenCount: coarseSchemaTokenCount,
            tokenCountRule: tokenRule(),
            onCoarseRespond: { ordinal in
                if slowSuccessOn.contains(ordinal) {
                    try? await Task.sleep(for: .seconds(slowSeconds))
                }
            }
        )
    }

    /// THE bkhc COMPOSITION ASSERTION at classifier level: a timed-out window
    /// must not cost the windows around it. If a timeout ever propagates as a
    /// throw instead of a per-window failure, this test fails — and that would
    /// be exactly the discard bug bkhc just removed.
    @available(iOS 26.0, *)
    @Test("a single timed-out window keeps every window banked before and after it")
    func singleTimeoutKeepsSiblingWindows() async throws {
        let runtime = Self.runtime(timeoutOn: [2], callCount: 3)
        let classifier = FoundationModelClassifier(
            runtime: runtime.runtime,
            config: Self.config()
        )

        let output = try await classifier.coarsePassA(segments: Self.segments(count: 3))

        #expect(await runtime.coarseCallCount == 3, "the pass must continue past the timed-out window")
        #expect(output.status == .success, "one slow window does not fail a pass that produced output")
        #expect(output.windows.count == 2, "windows 1 and 3 are banked")
        #expect(output.failedWindows.count == 1)
        #expect(output.failedWindows.first?.status == .inferenceTimeout)
        // Named, not folded into a neighbouring class.
        #expect(output.failedWindows.first?.status != .failedTransient)
        #expect(output.failedWindows.first?.status != .cancelled)
    }

    /// The no-progress guard. Two consecutive timeouts end the pass — but with
    /// PARTIAL results, so the window banked before them is still carried out
    /// for the runner to persist.
    @available(iOS 26.0, *)
    @Test("two consecutive timeouts end the pass and still return the banked windows")
    func twoConsecutiveTimeoutsEndThePassWithPartialResults() async throws {
        let runtime = Self.runtime(timeoutOn: [2, 3], callCount: 4)
        let classifier = FoundationModelClassifier(
            runtime: runtime.runtime,
            config: Self.config()
        )

        let output = try await classifier.coarsePassA(segments: Self.segments(count: 4))

        #expect(
            await runtime.coarseCallCount == 3,
            "window 4 must never be attempted — that is the bound the guard buys"
        )
        #expect(output.status == .inferenceTimeout)
        #expect(
            output.windows.count == 1,
            "the window banked before the guard tripped must survive the abort (playhead-bkhc)"
        )
        #expect(output.failedWindows.count == 2)
        #expect(output.failedWindows.allSatisfy { $0.status == .inferenceTimeout })
        // The plan list must come out too — the runner needs it to compute an
        // honest coverage cursor on an aborted pass.
        #expect(output.plans.count == 4)
    }

    /// The guard must be CONSECUTIVE, not cumulative. An episode with an
    /// occasional slow window still scans to the end; otherwise the guard
    /// itself would become a coverage bug on a merely-degraded device.
    @available(iOS 26.0, *)
    @Test("an answering window resets the no-progress counter")
    func answeringWindowResetsTheGuard() async throws {
        // Timeouts at windows 2 and 4, with a healthy window 3 between them.
        let runtime = Self.runtime(timeoutOn: [2, 4], callCount: 5)
        let classifier = FoundationModelClassifier(
            runtime: runtime.runtime,
            config: Self.config()
        )

        let output = try await classifier.coarsePassA(segments: Self.segments(count: 5))

        #expect(await runtime.coarseCallCount == 5, "every window must still be attempted")
        #expect(output.status == .success)
        #expect(output.windows.count == 3)
        #expect(output.failedWindows.count == 2)
    }

    /// The guard asks "is the model still ANSWERING", not "is it succeeding".
    /// A refusal is an answer: the model ran, reached a verdict and returned
    /// it. Treating a refusal as no-progress would let an episode with ordinary
    /// safety blocks scattered between two slow windows lose its whole tail —
    /// and safety blocks are common enough on this corpus (playhead-qbib) that
    /// this is not a hypothetical.
    ///
    /// Found by mutation: deleting the reset in the FAILURE arm (leaving only
    /// the one in the success arm) survived the whole suite until this test.
    @available(iOS 26.0, *)
    @Test("a non-timeout failure between two timeouts also resets the no-progress counter")
    func nonTimeoutFailureResetsTheGuard() async throws {
        // Call 2 hangs, call 3 is REFUSED (an answer), call 4 hangs. Two
        // timeouts, never consecutive, so the pass must run to the end.
        let runtime = Self.runtime(
            timeoutOn: [2, 4],
            callCount: 5,
            otherFailures: [3: .refusal]
        )
        let classifier = FoundationModelClassifier(
            runtime: runtime.runtime,
            config: Self.config()
        )

        let output = try await classifier.coarsePassA(segments: Self.segments(count: 5))

        #expect(
            await runtime.coarseCallCount == 5,
            "a refusal is the model answering — it must clear the no-progress evidence"
        )
        #expect(output.status == .success)
        #expect(output.windows.count == 2, "windows 1 and 5 answered")
        #expect(output.failedWindows.count == 3)
        #expect(output.failedWindows.filter { $0.status == .inferenceTimeout }.count == 2)
        #expect(output.failedWindows.filter { $0.status == .refusal }.count == 1)
    }

    /// The measurement fix. A failure row used to inherit the WHOLE PASS's
    /// wall-clock, which is why the device pull appeared to show failures
    /// costing 35x a success. A timeout that reported its pass's total would be
    /// indistinguishable from the unbounded call it replaced.
    @available(iOS 26.0, *)
    @Test("a timed-out window reports its own elapsed time, not the whole pass's")
    func timeoutCarriesItsOwnLatency() async throws {
        // Window 1 answers successfully after a deliberate 300ms, so the PASS
        // has a cost the timed-out window plainly does not share. Window 2
        // reports a timeout immediately. The failure row must therefore carry a
        // latency far below the pass total — and would carry the pass total
        // exactly if the runner went back to stamping it.
        let runtime = Self.runtime(
            timeoutOn: [2],
            callCount: 2,
            slowSuccessOn: [1],
            slowSeconds: 0.3
        )
        let classifier = FoundationModelClassifier(
            runtime: runtime.runtime,
            config: Self.config()
        )

        let output = try await classifier.coarsePassA(segments: Self.segments(count: 2))

        let failure = try #require(output.failedWindows.first)
        #expect(failure.status == .inferenceTimeout)
        let failureLatency = try #require(failure.latencyMillis)
        // Structural, not a timing race. The timed-out window reports an
        // essentially instantaneous attempt; the pass additionally contains a
        // deliberate 300ms successful window. Nothing here waits on a deadline,
        // so CPU starvation inflates both quantities together and the ratio
        // bound (0.75, against a real ratio near 0.0) cannot flip.
        #expect(failureLatency < output.latencyMillis)
        #expect(
            failureLatency < output.latencyMillis * 0.75,
            """
            failure latency \(failureLatency)ms vs pass latency \(output.latencyMillis)ms — \
            a value at parity means the row went back to inheriting the pass total.
            """
        )
    }

    /// A pass in which nothing times out must be byte-identical in shape to the
    /// pre-8d5r behavior. The deadline is dormant on the healthy path.
    @available(iOS 26.0, *)
    @Test("a healthy pass is unaffected by the deadline")
    func healthyPassIsUnchanged() async throws {
        let runtime = Self.runtime(timeoutOn: [], callCount: 3)
        let classifier = FoundationModelClassifier(
            runtime: runtime.runtime,
            config: Self.config()
        )

        let output = try await classifier.coarsePassA(segments: Self.segments(count: 3))

        #expect(output.status == .success)
        #expect(output.windows.count == 3)
        #expect(output.failedWindows.isEmpty)
    }
}

// MARK: - The one genuinely load-sensitive assertion

/// playhead-8d5r x playhead-zx0l. This suite measures WALL-CLOCK, so it runs
/// only in the serial perf pass (`scripts/perf-tests.sh`) and skips in the
/// parallel gate.
///
/// It was in the fast gate first and failed there, informatively: the deadline
/// reported taking 54.4 s to bound a 10 s call, not because the implementation
/// was wrong but because ~8,300 concurrently running tests starved the timer
/// task for 54 s. Under that load NO wall-clock budget is meaningful, and
/// widening the bound would only have hidden the measurement rather than fixed
/// it. Every behavioural consequence of the deadline — the named status, the
/// no-progress guards, banking, per-attempt latency — is asserted
/// deterministically in the suites above; this is the only assertion that
/// genuinely needs a quiet CPU.
@Suite("playhead-8d5r: FM inference deadline timing")
struct FMInferenceDeadlineTimingTests {

    /// THE LOAD-BEARING TEST. Proves the deadline bounds WALL-CLOCK, not merely
    /// the reported outcome.
    ///
    /// The operation ignores cooperative cancellation and runs for 10 s. A
    /// `withThrowingTaskGroup` race — the shape used elsewhere in this repo —
    /// cancels AND AWAITS its children, so it would return the right error
    /// after ~10 s, which is no bound at all. The abandonable unstructured
    /// implementation returns in ~0.2 s. The assertion sits at 5 s: 25x the
    /// deadline, and 2x under the operation, so neither jitter nor a
    /// regression to the structured shape can produce a false result.
    ///
    /// This is the test that justifies the whole design of
    /// `FMInferenceDeadline`. If it is ever deleted, the file's central claim
    /// becomes unverified.
    @Test(
        "the deadline returns promptly even when the call ignores cancellation",
        .enabled(if: PerfGate.runsMeasurementTests, "perf pass only — see playhead-zx0l")
    )
    func deadlineReturnsPromptlyEvenWhenTheOperationIgnoresCancellation() async {
        var thrown: Error?
        let seconds = await elapsedSeconds {
            do {
                _ = try await FMInferenceDeadline.run(.milliseconds(200)) {
                    await uncancellableSleep(seconds: 10)
                    return "the call eventually finished"
                }
            } catch {
                thrown = error
            }
        }

        #expect(thrown is FMInferenceTimeoutError)
        #expect(
            seconds < 5.0,
            """
            deadline took \(seconds)s to bound a 10s uncancellable call. \
            A value near 10s means the implementation went back to awaiting the \
            abandoned call — see the file header on FMInferenceDeadline.
            """
        )
    }
}

/// One-shot async signal. Lets a test wait until the operation under test has
/// actually started before cancelling it, instead of sleeping and hoping.
private actor AsyncSignal {
    private var signalled = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func signal() {
        guard !signalled else { return }
        signalled = true
        let pending = waiters
        waiters.removeAll()
        for waiter in pending {
            waiter.resume()
        }
    }

    func wait() async {
        if signalled { return }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}
