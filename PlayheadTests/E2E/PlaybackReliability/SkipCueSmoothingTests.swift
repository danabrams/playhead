// SkipCueSmoothingTests.swift
// playhead-456 — E2E: Playback Reliability, scenario 6 (skip cue smoothing).
//
// The bead requires:
//   * setSkipCues with a CMTimeRange at a known position
//   * playing through that range triggers a smooth skip
//   * streamed: duck → seek → release with no audible pop or silence
//     gap > 300ms
//   * cached: micro-crossfade smooth
//   * skip transition wall-clock < 500ms
//
// Audible-pop / crossfade smoothness is a CoreAudio-level property of
// AVPlayer.volume ramps and is not testable in-process — the simulator
// does not produce audible audio in unit tests. We assert what is
// testable: that setSkipCues stores the ranges, the duck → settle →
// release ORDERING of the transition, the re-entrancy guard, and that
// currentTime advances to the cue end. Audible quality is deferred to
// manual QA on real hardware.
//
// playhead-m9xk: these tests previously measured wall-clock across the
// transition's real 150 ms `Task.sleep` against a 1 s budget — under
// full-suite contention the host scheduler stretched that sleep to
// ~1.8 s, conflating ORDERING correctness with LATENCY. The routine
// simulator tests now drive the injected `transitionSleeper` seam
// (deterministic, no wall-clock assertions); the bead's <500 ms latency
// requirement lives in `skipTransitionLatencyWithinProductionBudget`,
// which uses the REAL sleeper and runs only in the serial perf pass
// (PerfGate / scripts/perf-tests.sh — see playhead-zx0l).

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

// MARK: - GateSleeper

/// Deterministic stand-in for the transition's duck-settle sleep.
/// Records every requested duration and (until `open()` is called)
/// parks the caller, giving tests a hard synchronization point INSIDE
/// the transition — after duck + seek, before release. All waits on
/// this actor are event-driven; the suite's `.timeLimit` is the only
/// backstop.
private actor GateSleeper {
    private(set) var sleepRequests: [Duration] = []
    private var parked: [CheckedContinuation<Void, Never>] = []
    private var entryWaiters: [(threshold: Int, continuation: CheckedContinuation<Void, Never>)] = []
    private var isOpen = false

    /// The sleeper body handed to `PlaybackService.init`.
    func sleep(_ duration: Duration) async {
        sleepRequests.append(duration)
        let reached = sleepRequests.count
        let ready = entryWaiters.filter { $0.threshold <= reached }
        entryWaiters.removeAll { $0.threshold <= reached }
        for waiter in ready { waiter.continuation.resume() }
        if isOpen { return }
        await withCheckedContinuation { parked.append($0) }
    }

    /// Suspends until at least `count` transitions have entered the
    /// sleep. Event-driven: resumed from `sleep(_:)` itself.
    func awaitSleepEntered(count: Int = 1) async {
        if sleepRequests.count >= count { return }
        await withCheckedContinuation { continuation in
            entryWaiters.append((count, continuation))
        }
    }

    /// Releases every parked transition and lets all future sleeps
    /// return immediately.
    func open() {
        isOpen = true
        let waiters = parked
        parked = []
        for continuation in waiters { continuation.resume() }
    }
}

/// playhead-1mq1.3: a sleeper that lets the first sleeps through and parks
/// only from the Nth onward.
///
/// `GateSleeper` parks the FIRST sleep, which is the settle dwell. That is
/// exactly right for the tests that want the transition frozen before its
/// seek settles, and useless for reaching INSIDE the restore ramp: the ramp's
/// sleeps only happen after the settle returns, so a test waiting for sleep #2
/// against a gate closed on sleep #1 waits forever.
private actor RampGateSleeper {
    private let parksFrom: Int
    private(set) var sleepRequests: [Duration] = []
    private var parked: [CheckedContinuation<Void, Never>] = []
    private var entryWaiters: [(threshold: Int, continuation: CheckedContinuation<Void, Never>)] = []
    private var isOpen = false

    init(parksFrom: Int) {
        self.parksFrom = parksFrom
    }

    func sleep(_ duration: Duration) async {
        sleepRequests.append(duration)
        let reached = sleepRequests.count
        let ready = entryWaiters.filter { $0.threshold <= reached }
        entryWaiters.removeAll { $0.threshold <= reached }
        for waiter in ready { waiter.continuation.resume() }
        guard reached >= parksFrom, !isOpen else { return }
        await withCheckedContinuation { continuation in
            parked.append(continuation)
        }
    }

    func awaitSleepEntered(count: Int) async {
        if sleepRequests.count >= count { return }
        await withCheckedContinuation { continuation in
            entryWaiters.append((count, continuation))
        }
    }

    func open() {
        isOpen = true
        let waiters = parked
        parked = []
        for continuation in waiters { continuation.resume() }
    }
}

@Suite("playhead-456 — Skip cue smoothing", .serialized)
struct SkipCueSmoothingTests {

    /// Bead requirement: skip transition < 500ms wall-clock. Verified
    /// with the REAL sleeper in the serial perf pass only (see
    /// `skipTransitionLatencyWithinProductionBudget`).
    private static let productionBudget: Duration = .milliseconds(500)

    private func makeService(
        sleeper: GateSleeper? = nil
    ) async -> PlaybackService {
        if let sleeper {
            return await PlaybackService(
                audioSession: FakeAudioSessionProvider(),
                nowPlayingInfo: FakeNowPlayingInfoProvider(),
                notificationCenter: NotificationCenter(),
                transitionSleeper: { await sleeper.sleep($0) }
            )
        }
        return await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
    }

    /// playhead-1mq1.3: the same construction against any sleeper body, so a
    /// suite-local sleeper (RampGateSleeper) can be used without widening the
    /// GateSleeper-typed helper above.
    private func makeService(
        transitionSleeper: @escaping @Sendable (Duration) async -> Void
    ) async -> PlaybackService {
        await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            transitionSleeper: transitionSleeper
        )
    }

    private func makePlayingState(currentTime: TimeInterval) -> PlaybackState {
        PlaybackState(
            status: .playing,
            currentTime: currentTime,
            duration: 1800,
            rate: 1.0,
            playbackSpeed: 1.0
        )
    }

    // MARK: - setSkipCues stores the ranges

    @Test("setSkipCues stores the supplied CMTimeRange list")
    func setSkipCuesStoresRanges() async {
        let service = await makeService()
        let cues = [
            CMTimeRange(
                start: CMTime(seconds: 90, preferredTimescale: 600),
                end: CMTime(seconds: 120, preferredTimescale: 600)
            ),
            CMTimeRange(
                start: CMTime(seconds: 600, preferredTimescale: 600),
                end: CMTime(seconds: 660, preferredTimescale: 600)
            ),
        ]
        await service.setSkipCues(cues)

        let stored = await service._testingSkipCues
        #expect(stored.count == 2)
        #expect(CMTimeGetSeconds(stored[0].start) == 90)
        #expect(CMTimeGetSeconds(CMTimeRangeGetEnd(stored[0])) == 120)
        #expect(CMTimeGetSeconds(stored[1].start) == 600)
        #expect(CMTimeGetSeconds(CMTimeRangeGetEnd(stored[1])) == 660)
    }

    @Test("setSkipCues with empty list clears prior cues")
    func setSkipCuesEmptyClears() async {
        let service = await makeService()
        await service.setSkipCues([
            CMTimeRange(
                start: CMTime(seconds: 0, preferredTimescale: 600),
                end: CMTime(seconds: 30, preferredTimescale: 600)
            ),
        ])
        await service.setSkipCues([])

        let stored = await service._testingSkipCues
        #expect(stored.isEmpty)
    }

    // MARK: - Duck → settle → release ordering (deterministic)

    /// The transition must duck BEFORE the settle sleep and only
    /// restore volume + advance state AFTER it. The gate sleeper parks
    /// the transition mid-flight so both phases are observable without
    /// any wall-clock assumption.
    @Test("Transition ducks before the settle sleep and releases after it",
          .timeLimit(.minutes(1)))
    func transitionDucksBeforeSettleAndReleasesAfter() async {
        let sleeper = GateSleeper()
        let service = await makeService(sleeper: sleeper)
        await service._testingInjectState(makePlayingState(currentTime: 90))
        let originalVolume = await service._testingPlayerVolume

        let transition = Task {
            await service._testingPerformSkipTransition(to: 120)
        }
        // Event-driven: resumes exactly when the transition reaches the
        // settle sleep — duck and seek have already run by then.
        await sleeper.awaitSleepEntered()

        let duckedVolume = await service._testingPlayerVolume
        let expectedDuckVolume = await PlaybackService._testingDuckVolume
        #expect(duckedVolume == expectedDuckVolume,
                "Mid-transition (parked in settle sleep) the player must be ducked; volume=\(duckedVolume)")
        let midSnapshot = await service.snapshot()
        #expect(midSnapshot.currentTime == 90,
                "State must not advance to the cue end before the settle sleep completes")

        await sleeper.open()
        await transition.value

        let restoredVolume = await service._testingPlayerVolume
        #expect(restoredVolume == originalVolume,
                "Volume must be restored after release; was \(restoredVolume), expected \(originalVolume)")
        let endSnapshot = await service.snapshot()
        #expect(abs(endSnapshot.currentTime - 120) < 0.001,
                "currentTime must land at the cue end after release; got \(endSnapshot.currentTime)")
        // playhead-1mq1.3: the settle sleep is still first and still 150 ms —
        // that is what this suite parks on above, and the ducked-mid-transition
        // assertion depends on it. What follows it is the RESTORE RAMP: eight
        // 10 ms steps bringing the volume back instead of one step change.
        //
        // Asserted as a whole sequence rather than a count, because the ORDER
        // is the claim. A ramp step landing before the settle would mean the
        // volume was coming back while the seek was still in flight.
        let requests = await sleeper.sleepRequests
        #expect(
            requests == [.milliseconds(150)] + Array(
                repeating: .milliseconds(10), count: 8
            ),
            "one 150 ms settle, then the eight-step restore ramp; got \(requests)"
        )
    }

    // MARK: - Position lands at cue end

    @Test("Skip transition advances currentTime to the cue end",
          .timeLimit(.minutes(1)))
    func skipTransitionLandsAtCueEnd() async {
        let sleeper = GateSleeper()
        await sleeper.open()  // no parking — transition runs straight through
        let service = await makeService(sleeper: sleeper)
        await service._testingInjectState(makePlayingState(currentTime: 90))

        await service._testingPerformSkipTransition(to: 120)

        let snap = await service.snapshot()
        #expect(abs(snap.currentTime - 120) < 1.0,
                "currentTime must land at cue end (±1s); got \(snap.currentTime)")
    }

    /// Sequential transitions must each perform a full duck/settle/
    /// release cycle — the re-entrancy guard must reset between
    /// transitions. Replaces the old wall-clock "each within budget"
    /// assertion with an exact per-transition settle-sleep count.
    @Test("Sequential skip transitions each run one full cycle and land at their targets",
          .timeLimit(.minutes(1)))
    func sequentialSkipsEachRunOneFullCycle() async {
        let sleeper = GateSleeper()
        await sleeper.open()
        let service = await makeService(sleeper: sleeper)
        await service._testingInjectState(makePlayingState(currentTime: 0))

        for (index, target) in [60.0, 240.0, 540.0, 900.0].enumerated() {
            await service._testingPerformSkipTransition(to: target)
            let snap = await service.snapshot()
            #expect(abs(snap.currentTime - target) < 0.001,
                    "Skip #\(index) must land at \(target); got \(snap.currentTime)")
            // playhead-1mq1.3: a completed transition is now a settle dwell
            // PLUS one sleep per restore-ramp step. The claim is unchanged —
            // each skip runs exactly one duck cycle, so the guard resets
            // between transitions — but the arithmetic comes from the
            // production constant rather than a literal needing re-derivation
            // by hand whenever the ramp's shape changes.
            let perTransition = await PlaybackService._testingSleepsPerSkipTransition
            let count = await sleeper.sleepRequests.count
            #expect(count == (index + 1) * perTransition,
                    "Skip #\(index) must run exactly one duck cycle (guard resets between transitions); total sleeps=\(count)")
        }
    }

    // MARK: - The restore ramp (playhead-1mq1.3)

    /// The ramp's last step assigns the CAPTURED volume, never the computed
    /// one. Found by a surviving mutant: at the default level of 1.0 the two
    /// agree exactly in Float32, so dropping the assign changed nothing any
    /// test could see.
    ///
    /// 0.8 is one of the levels where they do NOT agree. Interpolating from
    /// the 0.15 duck lands on 0.79999995, which is inaudible and is also a
    /// listener never getting their own level back — and it accumulates, since
    /// every skip would shave the same sliver off again.
    @Test("The restore ramp lands exactly on the captured volume",
          .timeLimit(.minutes(1)))
    func restoreRampLandsExactlyOnCapturedVolume() async {
        let sleeper = GateSleeper()
        let service = await makeService(sleeper: sleeper)
        await service._testingInjectState(makePlayingState(currentTime: 90))
        await service._testingSetPlayerVolume(0.8)
        let originalVolume = await service._testingPlayerVolume

        let transition = Task {
            await service._testingPerformSkipTransition(to: 120)
        }
        await sleeper.awaitSleepEntered()
        await sleeper.open()
        await transition.value

        let restored = await service._testingPlayerVolume
        #expect(
            restored == originalVolume,
            """
            the ramp must land on the captured level exactly; got \(restored) \
            against \(originalVolume)
            """
        )
    }

    /// The ramp stops writing the moment it no longer owns the transition.
    ///
    /// Also found by a surviving mutant. Every earlier guard in the transition
    /// protects a skip that has not happened yet, and an end-state assertion
    /// cannot see this one: whether or not the ramp keeps going, its final
    /// step assigns the same captured volume. What distinguishes them is
    /// somebody ELSE owning the player in between.
    ///
    /// So this parks mid-ramp, tears the transport down — which bumps the item
    /// generation and hands ownership away — and writes a sentinel level. A
    /// ramp that kept writing would walk that sentinel back up to the old
    /// item's volume, which is the stale-completion bug the token guards exist
    /// to stop.
    @Test("A ramp that lost its claim stops writing the player's volume",
          .timeLimit(.minutes(1)))
    func rampStopsWritingOnceItLosesTheClaim() async {
        // Parks from sleep 2 — the first RAMP step — so the settle dwell runs
        // to completion and this is provably INSIDE the ramp, not before it.
        let sleeper = RampGateSleeper(parksFrom: 2)
        let service = await makeService(
            transitionSleeper: { await sleeper.sleep($0) }
        )
        await service._testingInjectState(makePlayingState(currentTime: 90))

        let transition = Task {
            await service._testingPerformSkipTransition(to: 120)
        }
        await sleeper.awaitSleepEntered(count: 2)

        await service.tearDown()
        let sentinel: Float = 0.3
        await service._testingSetPlayerVolume(sentinel)

        await sleeper.open()
        await transition.value

        let after = await service._testingPlayerVolume
        #expect(
            after == sentinel,
            """
            a ramp that no longer owns the transition must not write; the \
            sentinel became \(after)
            """
        )
    }

    // MARK: - Re-entrancy guard (deterministic)

    /// performSkipTransition guards itself against re-entrant invocation
    /// via the `isHandlingSkip` flag. The first transition is parked
    /// inside the settle sleep — deterministically mid-flight — when the
    /// second fires; the second MUST early-out without ducking again.
    /// Replaces the old "total wall-clock < 2× single cost" heuristic
    /// with an exact sleep-entry count.
    @Test("Re-entrant skip transition does not double-duck",
          .timeLimit(.minutes(1)))
    func reentrantSkipDoesNotDoubleDuck() async {
        let sleeper = GateSleeper()
        let service = await makeService(sleeper: sleeper)
        await service._testingInjectState(makePlayingState(currentTime: 90))

        let first = Task {
            await service._testingPerformSkipTransition(to: 120)
        }
        await sleeper.awaitSleepEntered()

        // First transition is provably mid-flight (isHandlingSkip set,
        // parked in the settle sleep). A second transition must bail on
        // the guard: no second settle sleep, no state change to 130.
        await service._testingPerformSkipTransition(to: 130)
        let sleepsAfterReentrant = await sleeper.sleepRequests.count
        #expect(sleepsAfterReentrant == 1,
                "Re-entrant transition must not run a second duck/settle cycle; sleeps=\(sleepsAfterReentrant)")

        await sleeper.open()
        await first.value

        let snap = await service.snapshot()
        #expect(abs(snap.currentTime - 120) < 0.001,
                "The FIRST transition's target must win; the re-entrant call must not seek to 130 (got \(snap.currentTime))")
        // One duck cycle across the PAIR: the re-entrant call contributed
        // nothing, so the totals are the first transition's alone. The
        // mid-flight count above stays 1 either way, because the restore ramp
        // runs after the settle the first transition is parked in — which is a
        // small sign that assertion was measuring the right moment.
        let perTransition = await PlaybackService._testingSleepsPerSkipTransition
        let totalSleeps = await sleeper.sleepRequests.count
        #expect(totalSleeps == perTransition,
                "Exactly one duck cycle across the re-entrant pair; got \(totalSleeps)")
    }

    // MARK: - Wall-clock latency (serial perf pass ONLY — playhead-zx0l)

    /// Bead requirement: "Measure: skip transition duration < 500ms".
    /// Uses the REAL sleeper (production default) and a real
    /// ContinuousClock measurement, so it is load-sensitive by design
    /// and runs only in the quiescent serial perf pass
    /// (`scripts/perf-tests.sh`, PLAYHEAD_RUN_PERF=1). Listed in that
    /// script's MEASUREMENT_TESTS. Skipped in FastTests/IntegrationTests.
    @Test("Skip transition completes within the 500 ms production budget",
          .enabled(if: PerfGate.runsMeasurementTests, "perf pass only — see playhead-zx0l"),
          .timeLimit(.minutes(1)))
    func skipTransitionLatencyWithinProductionBudget() async {
        let service = await makeService()  // real transitionSleeper
        await service._testingInjectState(makePlayingState(currentTime: 90))

        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await service._testingPerformSkipTransition(to: 120)
        }

        #expect(elapsed < Self.productionBudget,
                "Skip transition must complete inside \(Self.productionBudget); was \(elapsed)")
    }
}

// MARK: - Real-device-only scenarios deferred

// not testable in-process — deferred to manual QA / real-device gate:
//   * audible smoothness of duck → seek → release (CoreAudio mixer)
//   * absence of "pop" on volume restore
//   * cached-asset micro-crossfade audio quality
//   * silence-gap measurement < 300ms (requires real audio playback)
