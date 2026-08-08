// FMClockPairTests.swift
// playhead-rkfp: the same-span two-clock reading behind
// `semantic_scan_results.suspendingLatencyMs`.
//
// WHY SYNTHETIC INSTANTS. With live clocks the two readings agree to within
// scheduler noise on an always-awake simulator, so a crossed wire — the
// suspending number computed from the continuous clock, or the subtraction
// reversed — would be invisible to any live-clock test. Handing in explicit
// `now` instants is the only way a test can prove WHICH clock feeds WHICH
// number, which is the entire contract: `latencyMs − suspendingLatencyMs`
// is read as "device-asleep share of the row", and a crossed wire makes that
// difference read 0 forever — the exact silent conflation the column exists
// to end.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-rkfp: FMClockPair — one span, two clocks")
struct FMClockPairTests {

    @Test("each reading derives from its own clock — a 10s continuous / 3s suspending span reports both")
    func eachReadingDerivesFromItsOwnClock() {
        let continuousStart = ContinuousClock().now
        let suspendingStart = SuspendingClock().now
        let pair = FMClockPair(
            continuousStart: continuousStart,
            suspendingStart: suspendingStart
        )

        // A span that crossed 7 s of device sleep: wall-clock advanced 10 s,
        // awake time only 3 s.
        let reading = pair.elapsed(
            continuousNow: continuousStart.advanced(by: .seconds(10)),
            suspendingNow: suspendingStart.advanced(by: .seconds(3))
        )

        #expect(reading.continuousMs == 10_000)
        #expect(reading.suspendingMs == 3_000)
    }

    @Test("sub-second components survive the millisecond conversion")
    func subSecondComponentsSurvive() {
        let continuousStart = ContinuousClock().now
        let suspendingStart = SuspendingClock().now
        let pair = FMClockPair(
            continuousStart: continuousStart,
            suspendingStart: suspendingStart
        )

        let reading = pair.elapsed(
            continuousNow: continuousStart.advanced(by: .milliseconds(1_500)),
            suspendingNow: suspendingStart.advanced(by: .milliseconds(250))
        )

        #expect(reading.continuousMs == 1_500)
        #expect(reading.suspendingMs == 250)
    }

    @Test("a live pair reads near-equal on an awake machine, and neither reading is negative")
    func liveReadingsAreSaneOnAnAwakeMachine() {
        let pair = FMClockPair.now()
        let reading = pair.elapsed()
        #expect(reading.continuousMs >= 0)
        #expect(reading.suspendingMs >= 0)
        // No upper-bound assertion: the parallel gate can starve this test
        // arbitrarily. The claim is only that both clocks moved FORWARD from
        // the same origin pair.
    }
}
