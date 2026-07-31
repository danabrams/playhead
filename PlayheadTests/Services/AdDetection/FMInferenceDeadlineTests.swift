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

    @Test("a call that outlives the deadline throws FMInferenceTimeoutError")
    func slowCallTimesOut() async {
        await #expect(throws: FMInferenceTimeoutError(deadline: .milliseconds(50))) {
            try await FMInferenceDeadline.run(.milliseconds(50)) {
                try await Task.sleep(for: .seconds(30))
                return "never"
            }
        }
    }

    /// THE LOAD-BEARING TEST. Proves the deadline bounds WALL-CLOCK, not merely
    /// the reported outcome.
    ///
    /// The operation ignores cancellation and runs for 10 s. A structured
    /// task-group race would return in ~10 s (correct status, no bound). The
    /// abandonable unstructured implementation returns in ~0.2 s. The assertion
    /// sits at 5 s: 25x the deadline, so CPU starvation on the gate cannot
    /// produce a false pass, and 2x under the operation, so a regression to the
    /// structured shape cannot produce a false pass either.
    @Test("the deadline returns promptly even when the call ignores cancellation")
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
        let value = try await FMInferenceDeadline.run(.milliseconds(500)) {
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
