// BoundedContinuationTests.swift
// playhead-c25o: proves the bounded-continuation helper cannot park
// forever and — the part most likely to be subtly wrong — cannot
// double-resume its `CheckedContinuation`.
//
// A `CheckedContinuation` resumed twice TRAPS the process. That makes
// the once-guard tests below unusual: their assertion is largely that
// the test finished at all. A regression in the guard does not read as
// a failed `#expect`, it reads as the whole test bundle dying. The
// race-pressure loop is written to make that outcome reproducible
// rather than one-in-a-thousand.

import Foundation
import Testing
@testable import Playhead

/// Thread-safe recorder for `onFallback` reports.
private final class FallbackRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var reasons: [BoundedContinuationFallback] = []

    var recorded: [BoundedContinuationFallback] { lock.withLock { reasons } }

    /// A `@Sendable` sink suitable for the helper's `onFallback` hook.
    /// Hoisted to a stored closure so call sites keep trailing-closure
    /// syntax for the body without tripping
    /// `multiple_closures_with_trailing_closure`.
    var sink: @Sendable (BoundedContinuationFallback) -> Void {
        { [self] reason in lock.withLock { reasons.append(reason) } }
    }
}

/// Holds the resume closure so a test can fire it long after the
/// helper has given up on it.
private final class ResumeBox: @unchecked Sendable {
    private let lock = NSLock()
    private var resume: (@Sendable (String) -> Void)?

    func store(_ closure: @escaping @Sendable (String) -> Void) {
        lock.withLock { resume = closure }
    }

    func fire(_ value: String) {
        let closure = lock.withLock { resume }
        closure?(value)
    }
}

@Suite("Bounded continuation (playhead-c25o)")
struct BoundedContinuationTests {

    // MARK: - Happy path

    @Test("The callback's value wins when it arrives before the timeout",
          .timeLimit(.minutes(1)))
    func callbackValueWins() async {
        let value = await withBoundedCheckedContinuation(
            timeout: .seconds(60),
            fallback: ["fallback"]
        ) { resume in
            resume(["real"])
        }
        #expect(value == ["real"])
    }

    @Test("An asynchronous reply inside the budget still wins",
          .timeLimit(.minutes(1)))
    func asynchronousReplyWins() async {
        let value = await withBoundedCheckedContinuation(
            timeout: .seconds(60),
            fallback: ["fallback"]
        ) { resume in
            Task { resume(["real"]) }
        }
        #expect(value == ["real"])
    }

    // MARK: - Bounded waiting

    @Test("A reply that never arrives resolves to the fallback instead of hanging",
          .timeLimit(.minutes(1)))
    func neverArrivingReplyTimesOut() async {
        let recorder = FallbackRecorder()
        let sink = recorder.sink
        let value = await withBoundedCheckedContinuation(
            timeout: .milliseconds(20),
            fallback: [String](),
            onFallback: sink
        ) { _ in
            // The daemon never replies. Without the timeout this call
            // parks for the lifetime of the process and the test's
            // `.timeLimit` is the only thing that ends it.
        }
        #expect(value.isEmpty)
        #expect(recorder.recorded == [.timedOut])
    }

    @Test("Cancellation resolves with the fallback without waiting out the timeout",
          .timeLimit(.minutes(1)))
    func cancellationResolvesWithFallback() async {
        let entered = TestEventCounter()
        let recorder = FallbackRecorder()
        let sink = recorder.sink
        let bridge = Task { () -> [String] in
            // A ten-minute timeout: only cancellation can resolve this
            // inside the test's time limit.
            await withBoundedCheckedContinuation(
                timeout: .seconds(600),
                fallback: ["fallback"],
                onFallback: sink
            ) { _ in
                entered.increment()
            }
        }
        await entered.wait(for: 1)
        bridge.cancel()

        #expect(await bridge.value == ["fallback"])
        #expect(recorder.recorded == [.cancelled])
    }

    @Test("A task already cancelled when the bridge starts still resolves",
          .timeLimit(.minutes(1)))
    func alreadyCancelledTaskResolves() async {
        // Exercises the resolve-before-attach path: `onCancel` fires
        // before the continuation exists, so the value has to be parked
        // and replayed the moment it attaches.
        let bridge = Task { () -> [String] in
            while !Task.isCancelled { await Task.yield() }
            return await withBoundedCheckedContinuation(
                timeout: .seconds(600),
                fallback: ["fallback"]
            ) { _ in }
        }
        bridge.cancel()
        #expect(await bridge.value == ["fallback"])
    }

    // MARK: - Once guard

    @Test("A reply that lands after the timeout is dropped, not double-resumed",
          .timeLimit(.minutes(1)))
    func lateReplyAfterTimeoutIsDropped() async {
        let box = ResumeBox()
        let value = await withBoundedCheckedContinuation(
            timeout: .milliseconds(20),
            fallback: "fallback"
        ) { resume in
            box.store(resume)
        }
        #expect(value == "fallback")

        // The daemon finally answers — long after we gave up. A second
        // resume of the same CheckedContinuation would trap the process,
        // so reaching the assertion below IS the assertion.
        box.fire("late")
        box.fire("later")
        #expect(value == "fallback")
    }

    @Test("The callback may be invoked repeatedly and concurrently while the timeout races it",
          .timeLimit(.minutes(2)))
    func concurrentResumesAndTimeoutCannotDoubleResume() async {
        // Maximum race pressure: a zero-length timeout fires at
        // essentially the same instant four detached tasks each call the
        // resume closure. Only one of those five resolutions may reach
        // the continuation. If the once guard is wrong, this loop traps
        // the test bundle rather than failing an expectation.
        for iteration in 0..<200 {
            let value = await withBoundedCheckedContinuation(
                timeout: .zero,
                fallback: -1
            ) { resume in
                for replica in 0..<4 {
                    Task.detached { resume(iteration * 10 + replica) }
                }
            }
            let expected = Set((0..<4).map { iteration * 10 + $0 }).union([-1])
            #expect(expected.contains(value))
        }
    }

    @Test("Repeated synchronous resumes from the body are idempotent",
          .timeLimit(.minutes(1)))
    func repeatedSynchronousResumesAreIdempotent() async {
        let value = await withBoundedCheckedContinuation(
            timeout: .seconds(60),
            fallback: -1
        ) { resume in
            resume(1)
            resume(2)
            resume(3)
        }
        #expect(value == 1, "the first resolution must win")
    }

    @Test("The fallback hook fires at most once even when timeout and cancellation race",
          .timeLimit(.minutes(1)))
    func fallbackHookFiresAtMostOnce() async {
        let recorder = FallbackRecorder()
        let sink = recorder.sink
        let entered = TestEventCounter()
        let bridge = Task { () -> Int in
            await withBoundedCheckedContinuation(
                timeout: .milliseconds(5),
                fallback: -1,
                onFallback: sink
            ) { _ in
                entered.increment()
            }
        }
        await entered.wait(for: 1)
        bridge.cancel()
        _ = await bridge.value

        // Whichever of timeout/cancellation won, exactly one report.
        #expect(recorder.recorded.count == 1)
    }
}
