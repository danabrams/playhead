// FMNoProgressWatchdogTests.swift
// playhead-qk44: the bound on SILENCE that ends a coarse pass which has
// stopped producing work.
//
// The sampling loop takes an injected `sleep`, so every test in this file
// drives the watchdog by COUNTING samples rather than by waiting for
// wall-clock. That is deliberate and follows the note in `TestFMRuntime`: the
// gate runs ~8,300 tests concurrently and a 200 ms timer has been observed
// taking 54 s to be noticed, so any assertion about elapsed time is a flake.
// Every assertion here is about WHICH outcome and HOW MANY samples.

import Dispatch
import Foundation
import os
import Testing
@testable import Playhead

// MARK: - Helpers

/// An operation that never returns and never observes cancellation, the way a
/// framework call implemented outside Swift concurrency does. This is what the
/// watchdog exists to escape; a bound that only works against cooperative code
/// would not have caught the 2026-07-31 wedge.
private func neverReturns() async {
    await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
}

/// Counts how many times the watchdog sampled, and optionally records progress
/// on the first `progressiveSamples` of them. Returning immediately keeps the
/// test off the wall clock.
private final class SampleDriver: Sendable {
    private let state = OSAllocatedUnfairLock(initialState: 0)
    private let ticker: FMProgressTicker
    private let progressiveSamples: Int

    init(ticker: FMProgressTicker, progressiveSamples: Int = 0) {
        self.ticker = ticker
        self.progressiveSamples = progressiveSamples
    }

    var samples: Int { state.withLock { $0 } }

    var sleep: @Sendable (Duration) async throws -> Void {
        { [self] _ in
            let ordinal = state.withLock { count -> Int in
                count += 1
                return count
            }
            if ordinal <= progressiveSamples {
                ticker.note()
            }
        }
    }
}

private struct WatchdogTestError: Error, Equatable {
    let tag: String
}

@Suite("playhead-qk44: FM no-progress watchdog")
struct FMNoProgressWatchdogTests {

    // MARK: - The primitive

    @Test("an operation that finishes returns its value untouched")
    func completedOperationReturnsValue() async throws {
        let ticker = FMProgressTicker()
        let value = try await FMNoProgressWatchdog.run(
            interval: .seconds(30),
            consecutiveIntervalLimit: 3,
            ticker: ticker,
            sleep: SampleDriver(ticker: ticker).sleep
        ) {
            "answered"
        }
        #expect(value == "answered")
    }

    @Test("an operation that completes no work is ended with FMNoProgressError")
    func silentOperationIsEnded() async {
        let ticker = FMProgressTicker()
        let driver = SampleDriver(ticker: ticker)
        await #expect(
            throws: FMNoProgressError(interval: .seconds(180), consecutiveIntervals: 3)
        ) {
            try await FMNoProgressWatchdog.run(
                interval: .seconds(180),
                consecutiveIntervalLimit: 3,
                ticker: ticker,
                sleep: driver.sleep
            ) {
                await neverReturns()
                return "never"
            }
        }
    }

    /// The load-bearing test in this file, and the reason a silence bound was
    /// chosen over a duration bound at all.
    ///
    /// A healthy coarse pass on device runs 12–45 minutes and completes windows
    /// throughout. If the count were CUMULATIVE, such a pass would be killed
    /// after the third slow stretch no matter how much audio it had banked —
    /// the same wrong predicate `playhead-bkhc` had to fix for barren
    /// background windows. Six interleaved progress ticks must therefore cost
    /// exactly six extra samples and nothing else.
    @Test("progress resets the count: consecutive silence, never cumulative")
    func progressResetsTheConsecutiveCount() async {
        let ticker = FMProgressTicker()
        let progressiveSamples = 6
        let limit = 3
        let driver = SampleDriver(ticker: ticker, progressiveSamples: progressiveSamples)

        await #expect(
            throws: FMNoProgressError(interval: .seconds(180), consecutiveIntervals: limit)
        ) {
            try await FMNoProgressWatchdog.run(
                interval: .seconds(180),
                consecutiveIntervalLimit: limit,
                ticker: ticker,
                sleep: driver.sleep
            ) {
                await neverReturns()
                return "never"
            }
        }

        // A cumulative counter fires at sample 3 (three silent-or-not samples
        // in); a consecutive counter cannot fire until the run of silence
        // itself is `limit` long, which is sample 9.
        #expect(driver.samples == progressiveSamples + limit)
    }

    @Test("a real error propagates unchanged rather than becoming a stall")
    func realErrorIsNotRelabelledAsAStall() async {
        let ticker = FMProgressTicker()
        await #expect(throws: WatchdogTestError(tag: "refusal")) {
            try await FMNoProgressWatchdog.run(
                interval: .seconds(180),
                consecutiveIntervalLimit: 3,
                ticker: ticker,
                sleep: SampleDriver(ticker: ticker).sleep
            ) {
                throw WatchdogTestError(tag: "refusal")
            }
        }
    }

    /// A background-window expiry must surface as `CancellationError`, never as
    /// a stall. The two take different recovery paths in `BackfillJobRunner` —
    /// checkpoint-and-resume versus a named terminal — so conflating them would
    /// turn an interrupted pass into a permanently failed one and lose its
    /// coverage cursor.
    @Test("caller cancellation surfaces as CancellationError, not a stall")
    func callerCancellationIsNotAStall() async {
        let ticker = FMProgressTicker()
        let started = WatchdogSignal()
        let task = Task { () -> Result<String, Error> in
            do {
                let value = try await FMNoProgressWatchdog.run(
                    interval: .seconds(180),
                    consecutiveIntervalLimit: 3,
                    ticker: ticker,
                    // Never samples, so only the cancellation can decide this.
                    sleep: { _ in await neverReturns() },
                    operation: {
                        await started.signal()
                        try await Task.sleep(for: .seconds(120))
                        return "never"
                    }
                )
                return .success(value)
            } catch {
                return .failure(error)
            }
        }

        await started.wait()
        task.cancel()

        let outcome = await task.value
        guard case let .failure(error) = outcome else {
            Issue.record("expected the cancelled operation to fail")
            return
        }
        #expect(error is CancellationError)
        #expect(!(error is FMNoProgressError))
    }

    @Test("a non-positive interval disables the bound instead of stalling immediately")
    func nonPositiveIntervalDisablesTheBound() async throws {
        let ticker = FMProgressTicker()
        let value = try await FMNoProgressWatchdog.run(
            interval: .zero,
            consecutiveIntervalLimit: 3,
            ticker: ticker,
            sleep: { _ in },
            operation: { "answered" }
        )
        #expect(value == "answered")
    }

    @Test("a non-positive limit disables the bound instead of stalling immediately")
    func nonPositiveLimitDisablesTheBound() async throws {
        let ticker = FMProgressTicker()
        let value = try await FMNoProgressWatchdog.run(
            interval: .seconds(180),
            consecutiveIntervalLimit: 0,
            ticker: ticker,
            sleep: { _ in },
            operation: { "answered" }
        )
        #expect(value == "answered")
    }

    // MARK: - The ticker

    @Test("the ticker is monotonic and starts at zero")
    func tickerIsMonotonic() {
        let ticker = FMProgressTicker()
        #expect(ticker.observed == 0)
        ticker.note()
        ticker.note()
        #expect(ticker.observed == 2)
    }

    // MARK: - The shipped budget

    /// The two constants have to be read together, so they are asserted
    /// together. `FMInferenceDeadline.standard` (300 s) is the longest a single
    /// legitimate window can occupy before it either answers or is abandoned
    /// with a recorded failure — both of which tick. At a 180 s interval that
    /// window scores at most two strikes, so a limit of two would kill healthy
    /// passes and a limit of three cannot.
    @Test("the shipped interval and limit cannot be tripped by one slow window")
    func shippedBudgetSparesTheSlowestLegitimateWindow() {
        let interval = FMNoProgressWatchdog.standardInterval
        let limit = FMNoProgressWatchdog.standardConsecutiveIntervalLimit
        let maximumStrikesOneWindowCanScore = Int(
            FMInferenceDeadline.standard / interval
        )
        #expect(limit > maximumStrikesOneWindowCanScore)
    }

    /// The in-process watchdog must reach a wedged job BEFORE the database
    /// reaper would, so the reaper is only ever left the case it can handle
    /// correctly: a row whose owning process is gone. If this inverts, a
    /// foreground sweep starts racing a live runner again.
    @Test("the stall bound fires before the stranded-row reaper's freshness floor")
    func stallBoundFiresBeforeTheDatabaseReaper() {
        let totalSilence = FMNoProgressWatchdog.standardInterval
            * FMNoProgressWatchdog.standardConsecutiveIntervalLimit
        #expect(totalSilence < .seconds(AnalysisStore.strandedJobFreshnessSeconds))
    }

    /// `FMInferenceDeadline.run` treats a non-positive deadline as an OFF
    /// switch, so a zero here would silently un-bound every `tokenCount` call
    /// `FMUnboundedCallCanaryTests` just proved is wrapped.
    @Test("the metadata budget is a real bound, and tighter than the inference budget")
    func metadataBudgetIsPositiveAndTighterThanInference() {
        #expect(FMInferenceDeadline.metadata > .zero)
        // Tokenisation is not generation. If this ever grows to the inference
        // budget, the argument that justifies bounding it more tightly has been
        // lost and the value should be re-argued rather than drifted.
        #expect(FMInferenceDeadline.metadata < FMInferenceDeadline.standard)
    }
}

/// Local copy of the signal helper — `FMInferenceDeadlineTests` has a
/// `private` one and Swift Testing suites in this target must not share
/// mutable fixtures across files.
private actor WatchdogSignal {
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
