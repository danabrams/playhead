// ListeningTimeAccumulatorTests.swift
// playhead-jw63.3 — the north-star denominator, driven through every way
// listening actually stops.

import Foundation
import Testing

@testable import Playhead

@Suite("ListeningTimeAccumulator — wall-clock listening seconds")
struct ListeningTimeAccumulatorTests {

    private let origin = ContinuousClock.now

    private func instant(afterSeconds seconds: Double) -> ContinuousClock.Instant {
        origin.advanced(by: .seconds(seconds))
    }

    @Test("Nothing accrues before playback starts")
    func idleAccruesNothing() {
        var accumulator = ListeningTimeAccumulator()
        #expect(accumulator.observe(isPlaying: false, at: instant(afterSeconds: 10)) == 0)
        #expect(!accumulator.isAccruing)
    }

    @Test("Play to pause credits the interval exactly once")
    func playToPauseCreditsInterval() {
        var accumulator = ListeningTimeAccumulator()
        #expect(accumulator.observe(isPlaying: true, at: instant(afterSeconds: 0)) == 0)
        #expect(accumulator.isAccruing)

        let credited = accumulator.observe(isPlaying: false, at: instant(afterSeconds: 90))
        #expect(abs(credited - 90) < 0.001)
        // A second pause observation must not re-credit the same interval.
        #expect(accumulator.observe(isPlaying: false, at: instant(afterSeconds: 200)) == 0)
    }

    @Test("Paused time is not listening time")
    func pausedTimeIsNotCredited() {
        var accumulator = ListeningTimeAccumulator()
        _ = accumulator.observe(isPlaying: true, at: instant(afterSeconds: 0))
        _ = accumulator.observe(isPlaying: false, at: instant(afterSeconds: 10))
        _ = accumulator.observe(isPlaying: true, at: instant(afterSeconds: 610))
        let credited = accumulator.observe(isPlaying: false, at: instant(afterSeconds: 620))
        #expect(abs(credited - 10) < 0.001)
    }

    @Test("Committing mid-playback credits the partial interval and keeps going")
    func commitKeepsAccruing() {
        var accumulator = ListeningTimeAccumulator()
        _ = accumulator.observe(isPlaying: true, at: instant(afterSeconds: 0))

        let first = accumulator.commit(at: instant(afterSeconds: 30))
        #expect(abs(first - 30) < 0.001)
        #expect(accumulator.isAccruing)

        // The second commit must credit only the *new* 20 s, not 50.
        let second = accumulator.commit(at: instant(afterSeconds: 50))
        #expect(abs(second - 20) < 0.001)

        let final = accumulator.observe(isPlaying: false, at: instant(afterSeconds: 60))
        #expect(abs(final - 10) < 0.001)
    }

    @Test("Committing while paused credits nothing")
    func commitWhilePausedCreditsNothing() {
        var accumulator = ListeningTimeAccumulator()
        #expect(accumulator.commit(at: instant(afterSeconds: 10)) == 0)
    }

    @Test("An implausibly long interval is clamped, not credited whole")
    func longIntervalIsClamped() {
        var accumulator = ListeningTimeAccumulator()
        _ = accumulator.observe(isPlaying: true, at: instant(afterSeconds: 0))
        let credited = accumulator.observe(
            isPlaying: false,
            at: instant(afterSeconds: 48 * 60 * 60)
        )
        #expect(credited == ListeningTimeAccumulator.maximumCreditedIntervalSeconds)
    }

    @Test("A backwards instant credits zero rather than a negative")
    func backwardsInstantCreditsZero() {
        var accumulator = ListeningTimeAccumulator()
        _ = accumulator.observe(isPlaying: true, at: instant(afterSeconds: 100))
        #expect(accumulator.observe(isPlaying: false, at: instant(afterSeconds: 40)) == 0)
    }
}

@Suite("ListeningSecondsQuantizer — whole seconds without losing the remainder")
struct ListeningSecondsQuantizerTests {

    @Test("Sub-second intervals accumulate instead of rounding away")
    func subSecondIntervalsCarry() {
        var quantizer = ListeningSecondsQuantizer()
        #expect(quantizer.take(0.4) == 0)
        #expect(quantizer.take(0.4) == 0)
        #expect(quantizer.take(0.4) == 1)
        #expect(quantizer.take(0.4) == 0)
        #expect(quantizer.take(0.4) == 1)
    }

    @Test("Whole seconds pass straight through")
    func wholeSecondsPassThrough() {
        var quantizer = ListeningSecondsQuantizer()
        #expect(quantizer.take(90) == 90)
        #expect(quantizer.take(0.5) == 0)
        #expect(quantizer.take(0.5) == 1)
    }

    @Test("Nonsense input contributes nothing")
    func nonsenseInputIgnored() {
        var quantizer = ListeningSecondsQuantizer()
        #expect(quantizer.take(0) == 0)
        #expect(quantizer.take(-5) == 0)
        #expect(quantizer.take(.nan) == 0)
        #expect(quantizer.take(.infinity) == 0)
        #expect(quantizer.take(1) == 1)
    }
}
