// PlaybackServiceAdSkipCueTests.swift
// playhead-nqwr: the cue AT THE SEAM.
//
// These are behaviours, not "the method was called". The thing that can go
// wrong here is not whether a chime plays — it is whether a chime plays when
// the episode has just been taken away from the listener (a phone call, a
// headphone pulled out of a socket, a pause), or whether it announces a cut
// that never happened. Both of those are P0 in the listening path, and both are
// invisible on the simulator unless a test drives the event onto the seam
// itself. So every case below runs a REAL skip transition and interposes the
// event inside it, through the transport's own `transitionSleeper` seam.
//
// The route-change cases are exact: `PlaybackService` registers its
// route-change observer SYNCHRONOUSLY in `init` (block-based `addObserver`), so
// a post is delivered without a registration race. The interruption observer is
// an async notification sequence whose registration lags init by some
// milliseconds — documented on `finishObserverToken` — so those cases perform
// an explicit readiness handshake first and then post exactly once. Neither
// case carries a wall-clock deadline: if production stops pausing, the drain
// parks and the `.timeLimit` trait fails deterministically instead of
// load-dependently (the shape `InterruptionHandlingTests` argues for).

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

// MARK: - Fakes

/// Records what the transport asked the cue to do, in order.
final class RecordingAdSkipCuePlayer: AdSkipCuePlaying, @unchecked Sendable {
    enum Event: Sendable, Equatable { case play, stop }

    private let lock = NSLock()
    private var _events: [Event] = []

    var events: [Event] { lock.lock(); defer { lock.unlock() }; return _events }
    var playCount: Int { events.count { $0 == .play } }
    var stopCount: Int { events.count { $0 == .stop } }

    func playAdSkipCue() {
        lock.lock(); defer { lock.unlock() }
        _events.append(.play)
    }

    func stopAdSkipCue() {
        lock.lock(); defer { lock.unlock() }
        _events.append(.stop)
    }
}

/// Runs one caller-supplied action the first time the transport's duck-settle
/// sleep is reached — i.e. INSIDE a skip transition, after the seek has
/// completed and before the volume is restored. That window is where an
/// interruption or a route change actually lands in the field.
private actor SeamInterposer {
    private var action: (@Sendable () async -> Void)?
    private var fired = false

    func install(_ action: @escaping @Sendable () async -> Void) {
        self.action = action
    }

    func run() async {
        guard !fired, let action else { return }
        fired = true
        await action()
    }
}

// MARK: - Suite

@Suite("PlaybackService – the ad-skip cue")
struct PlaybackServiceAdSkipCueTests {

    private static let cueStart: TimeInterval = 100
    private static let cueEnd: TimeInterval = 130

    private static func playingState() -> PlaybackState {
        PlaybackState(
            status: .playing,
            currentTime: cueStart,
            duration: 1_800,
            rate: 1.0,
            playbackSpeed: 1.0
        )
    }

    /// A transport with a stub item installed and no KVO wired, parked in
    /// `.playing`. `_testingInstallStubCurrentPlayerItem` is used rather than
    /// `loadItem` on purpose: it installs both ownership slots without
    /// observers, so nothing asynchronously rewrites `status` underneath the
    /// assertions.
    private static func makeService(
        cue: AdSkipCuePlaying,
        enabled: @escaping @Sendable () -> Bool = { true },
        center: NotificationCenter = NotificationCenter(),
        audioSession: FakeAudioSessionProvider = FakeAudioSessionProvider(),
        seekSucceeds: Bool = true,
        sleeper: @escaping @Sendable (Duration) async -> Void = { _ in }
    ) async -> PlaybackService {
        let service = await PlaybackService(
            audioSession: audioSession,
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center,
            transitionSleeper: sleeper,
            itemSeekOperation: { _, _ in seekSucceeds },
            skipCuePlayer: cue,
            skipCueEnabled: enabled
        )
        await service._testingInstallStubCurrentPlayerItem()
        await service._testingInjectState(playingState())
        return service
    }

    private static func drainUntilPaused(_ stream: AsyncStream<PlaybackState>) async {
        for await state in stream where state.status == .paused { return }
    }

    private static func postInterruptionBegan(to center: NotificationCenter) {
        center.post(
            name: AVAudioSession.interruptionNotification,
            object: nil,
            userInfo: [
                AVAudioSessionInterruptionTypeKey:
                    AVAudioSession.InterruptionType.began.rawValue
            ]
        )
    }

    private static func postOldDeviceUnavailable(to center: NotificationCenter) {
        center.post(
            name: AVAudioSession.routeChangeNotification,
            object: nil,
            userInfo: [
                AVAudioSessionRouteChangeReasonKey:
                    AVAudioSession.RouteChangeReason.oldDeviceUnavailable.rawValue
            ]
        )
    }

    // NOTE ON POSTING EXACTLY ONCE. `PlaybackService` observes interruptions
    // through an async notification SEQUENCE, whose registration lags `init`
    // by some milliseconds (the lag documented on `finishObserverToken`, which
    // is why THAT observer was moved to a block). The first version of these
    // tests reposted `.began` in a loop until a pause was observed, and the
    // duplicates are what failed: an extra notification, still queued in the
    // same sequence, was handled AFTER the transition and silenced a cue the
    // transition had legitimately sounded. A test that manufactures a second
    // interruption is not testing an interruption.
    //
    // So every case below posts ONCE and drains the transport's own state
    // stream with no deadline — the shape `InterruptionHandlingTests` argues
    // for one directory over. Each post happens after a full skip transition
    // has already run (many actor hops), so the observer is long since live;
    // and if production ever stopped pausing, the drain parks and the
    // `.timeLimit` trait fails deterministically rather than load-dependently.

    // MARK: - 1. It fires, once, on a real cut

    @Test("A completed skip sounds the cue exactly once",
          .timeLimit(.minutes(1)))
    func completedSkipSoundsCueOnce() async {
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue)

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(await service.snapshot().currentTime == Self.cueEnd,
                "precondition: the cut actually landed")
        #expect(cue.events == [.play],
                "one cut, one acknowledgement; observed \(cue.events)")
        await service.tearDown()
    }

    // MARK: - 2. It does not fire for a cut that did not happen

    @Test("A failed seek sounds nothing", .timeLimit(.minutes(1)))
    func failedSeekSoundsNothing() async {
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue, seekSucceeds: false)

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(await service.snapshot().currentTime != Self.cueEnd)
        #expect(cue.playCount == 0,
                "a cue that announces a cut nobody heard is worse than silence")
        await service.tearDown()
    }

    @Test("A reservation Listen disarmed before it ran sounds nothing",
          .timeLimit(.minutes(1)))
    func disarmedReservationSoundsNothing() async {
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue)
        await service.setSkipCues([
            CMTimeRange(
                start: CMTime(seconds: Self.cueStart, preferredTimescale: 600),
                end: CMTime(seconds: Self.cueEnd, preferredTimescale: 600)
            )
        ])

        let token = await service._testingReserveSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )
        #expect(token != nil, "precondition: the transition was claimed")
        // The eligibility flip that retires the cue between claim and execute.
        await service.setSkipCues([])
        if let token {
            await service._testingExecuteReservedSkipTransition(transitionToken: token)
        }

        #expect(cue.playCount == 0)
        await service.tearDown()
    }

    @Test("An item replaced mid-seam sounds nothing", .timeLimit(.minutes(1)))
    func itemReplacedMidSeamSoundsNothing() async {
        let cue = RecordingAdSkipCuePlayer()
        let interposer = SeamInterposer()
        let service = await Self.makeService(
            cue: cue,
            sleeper: { _ in await interposer.run() }
        )
        await interposer.install { [service] in
            // A new episode takes both ownership slots while the old
            // transition is parked in its duck-settle.
            await service._testingInstallStubCurrentPlayerItem()
        }

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(cue.playCount == 0,
                "the replacement's listener never heard this cut")
        await service.tearDown()
    }

    // MARK: - 3. The listener's switch

    @Test("The cue is silent when the listener has switched it off",
          .timeLimit(.minutes(1)))
    func switchedOffIsSilent() async {
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue, enabled: { false })

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(await service.snapshot().currentTime == Self.cueEnd,
                "the SKIP still happens; only the sound is suppressed")
        #expect(cue.playCount == 0)
        await service.tearDown()
    }

    // MARK: - 4. A route change landing on the seam

    @Test("A route that vanishes on the seam suppresses the cue",
          .timeLimit(.minutes(1)))
    func routeVanishingOnSeamSuppressesCue() async {
        let center = NotificationCenter()
        let cue = RecordingAdSkipCuePlayer()
        let interposer = SeamInterposer()
        let service = await Self.makeService(
            cue: cue, center: center,
            sleeper: { _ in await interposer.run() }
        )
        let stream = await service.observeStates()
        await interposer.install {
            Self.postOldDeviceUnavailable(to: center)
            await Self.drainUntilPaused(stream)
        }

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(await service.snapshot().status == .paused,
                "precondition: the vanished route paused playback")
        #expect(cue.playCount == 0,
                "headphones out mid-skip must not chime into the room speaker")
        await service.tearDown()
    }

    /// The other direction, and the one that must NOT be suppressed: plugging
    /// something IN (CarPlay, a fresh pair of headphones) does not pause, so
    /// the cut is still audible to the listener and still gets its cue.
    @Test("A route ARRIVING on the seam leaves the cue alone",
          .timeLimit(.minutes(1)))
    func routeArrivingOnSeamKeepsCue() async {
        let center = NotificationCenter()
        let cue = RecordingAdSkipCuePlayer()
        let interposer = SeamInterposer()
        let service = await Self.makeService(
            cue: cue, center: center,
            sleeper: { _ in await interposer.run() }
        )
        await interposer.install {
            center.post(
                name: AVAudioSession.routeChangeNotification,
                object: nil,
                userInfo: [
                    AVAudioSessionRouteChangeReasonKey:
                        AVAudioSession.RouteChangeReason.newDeviceAvailable.rawValue
                ]
            )
        }

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(await service.snapshot().status == .playing)
        #expect(cue.playCount == 1,
                "CarPlay connecting is not a reason to withhold the receipt")
        await service.tearDown()
    }

    @Test("A vanished route silences a cue that is already sounding",
          .timeLimit(.minutes(1)))
    func vanishedRouteSilencesSoundingCue() async {
        let center = NotificationCenter()
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue, center: center)

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )
        #expect(cue.events == [.play])

        let stream = await service.observeStates()
        Self.postOldDeviceUnavailable(to: center)
        await Self.drainUntilPaused(stream)

        #expect(cue.events == [.play, .stop],
                "a cue must not outlive the audio it was acknowledging")
        await service.tearDown()
    }

    // MARK: - 5. An interruption landing on the seam

    @Test("An interruption landing on the seam suppresses the cue",
          .timeLimit(.minutes(1)))
    func interruptionOnSeamSuppressesCue() async {
        let center = NotificationCenter()
        let cue = RecordingAdSkipCuePlayer()
        let interposer = SeamInterposer()
        let service = await Self.makeService(
            cue: cue, center: center,
            sleeper: { _ in await interposer.run() }
        )

        // A first, UNINTERRUPTED cut, before anything is interposed. It is the
        // precondition that makes the second one evidence — without it, "no
        // cue" could equally mean the transport never emits at all — and its
        // many actor hops are also what guarantee the async-sequence
        // interruption observer is live before the only post this test makes.
        // `SeamInterposer.run()` is inert until an action is installed, so this
        // transition passes through the sleeper untouched.
        await service._testingPerformSkipTransition(cueStart: 40, cueEnd: 70)
        #expect(cue.playCount == 1, "precondition: an undisturbed cut sounds")
        await service._testingInjectState(Self.playingState())

        let stream = await service.observeStates()
        await interposer.install {
            Self.postInterruptionBegan(to: center)
            await Self.drainUntilPaused(stream)
        }
        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(await service.snapshot().status == .paused,
                "precondition: the interruption paused playback")
        #expect(cue.playCount == 1,
                """
                The interrupted cut must add no second sound — a chime over a \
                phone call is the alert this feature must never be. Observed \
                \(cue.events).
                """)
        await service.tearDown()
    }

    @Test("An interruption silences a cue that is already sounding",
          .timeLimit(.minutes(1)))
    func interruptionSilencesSoundingCue() async {
        let center = NotificationCenter()
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue, center: center)

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )
        #expect(cue.events == [.play])

        let stream = await service.observeStates()
        Self.postInterruptionBegan(to: center)
        await Self.drainUntilPaused(stream)

        #expect(cue.events == [.play, .stop],
                "a cue must not outlive the audio it was acknowledging")
        await service.tearDown()
    }

    // MARK: - 6. Pause, teardown, and skipping while paused

    @Test("A skip performed while paused is silent", .timeLimit(.minutes(1)))
    func skipWhilePausedIsSilent() async {
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue)
        await service.pause()

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(cue.playCount == 0,
                "nothing the listener could have noticed moved")
        await service.tearDown()
    }

    @Test("Teardown silences a sounding cue", .timeLimit(.minutes(1)))
    func teardownSilencesSoundingCue() async {
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue)

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )
        #expect(cue.events == [.play])

        await service.tearDown()
        #expect(cue.events.contains(.stop))
    }

    @Test("Detaching the episode silences a sounding cue",
          .timeLimit(.minutes(1)))
    func detachSilencesSoundingCue() async {
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue)

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )
        #expect(cue.events == [.play])

        _ = await service.pauseAndDetachCurrentItem()
        #expect(cue.events.contains(.stop))
        await service.tearDown()
    }

    // MARK: - 7. The session is never touched

    /// The failure this bead most needs to not ship: a cue that reconfigures or
    /// re-activates the session ducks, interrupts or reroutes the episode at
    /// the one moment the listener is paying attention.
    ///
    /// Asserted over the transport's WHOLE life, with the REAL cue player, so
    /// there is no before/after snapshot that a racing setup task can spoil:
    /// the session is configured exactly once, by `configureAudioSession`, and a
    /// completed skip adds nothing. `tearDown()` joins the setup task, which is
    /// what makes the counts final.
    @Test("A real cue leaves the audio session exactly as it found it",
          .timeLimit(.minutes(1)))
    func realCueLeavesSessionUntouched() async {
        let audioSession = FakeAudioSessionProvider()
        let service = await Self.makeService(
            cue: AdSkipCuePlayer.system(),
            audioSession: audioSession
        )

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )
        await service.tearDown()

        #expect(audioSession.setActiveCalls == [true],
                "observed \(audioSession.setActiveCalls)")
        #expect(audioSession.categoryCalls.count == 1,
                "observed \(audioSession.categoryCalls)")
        #expect(audioSession.categoryCalls.first?.category
                == AVAudioSession.Category.playback.rawValue)
        #expect(audioSession.categoryCalls.first?.mode
                == AVAudioSession.Mode.spokenAudio.rawValue)
    }

    /// The cue must never be ducked with the episode: it is a separate player,
    /// authored quiet, and the whole point is that it is audible in the gap the
    /// duck creates.
    @Test("The cue does not disturb the transport's own volume",
          .timeLimit(.minutes(1)))
    func cueDoesNotDisturbTransportVolume() async {
        let cue = RecordingAdSkipCuePlayer()
        let service = await Self.makeService(cue: cue)
        let before = await service._testingPlayerVolume

        await service._testingPerformSkipTransition(
            cueStart: Self.cueStart, cueEnd: Self.cueEnd
        )

        #expect(await service._testingPlayerVolume == before,
                "the duck must be fully released with the cue in the path")
        #expect(cue.playCount == 1)
        await service.tearDown()
    }

    // MARK: - 8. Several skips close together

    /// The transport emits per CUT — it has no idea how loud the room is — and
    /// the coalescing lives in `AdSkipCuePlayer`. Composed here so the two
    /// layers are measured together: two genuine cuts a tenth of a second
    /// apart produce two emissions and ONE sound.
    @Test("Two cuts inside the re-trigger window make one sound",
          .timeLimit(.minutes(1)))
    func adjacentCutsMakeOneSound() async {
        let starts = AdSkipCueStartCounter()
        let clock = AdSkipCueTestClock()
        let player = AdSkipCuePlayer(
            now: { clock.value },
            startSound: { starts.increment() },
            stopSound: {}
        )
        let service = await Self.makeService(cue: player)

        await service._testingPerformSkipTransition(cueStart: 100, cueEnd: 130)
        clock.advance(by: .milliseconds(100))
        await service._testingInjectState(Self.playingState())
        await service._testingPerformSkipTransition(cueStart: 130, cueEnd: 160)

        #expect(await service.snapshot().currentTime == 160,
                "precondition: both cuts landed")
        #expect(starts.value == 1,
                "adjacent cuts are one break to the listener; observed \(starts.value)")
        await service.tearDown()
    }

    @Test("Two cuts a minute apart make two sounds", .timeLimit(.minutes(1)))
    func separatedCutsMakeTwoSounds() async {
        let starts = AdSkipCueStartCounter()
        let clock = AdSkipCueTestClock()
        let player = AdSkipCuePlayer(
            now: { clock.value },
            startSound: { starts.increment() },
            stopSound: {}
        )
        let service = await Self.makeService(cue: player)

        await service._testingPerformSkipTransition(cueStart: 100, cueEnd: 130)
        clock.advance(by: .seconds(60))
        await service._testingInjectState(Self.playingState())
        await service._testingPerformSkipTransition(cueStart: 130, cueEnd: 160)

        #expect(starts.value == 2, "observed \(starts.value)")
        await service.tearDown()
    }
}

// MARK: - Small thread-safe test primitives

private final class AdSkipCueStartCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0
    var value: Int { lock.lock(); defer { lock.unlock() }; return count }
    func increment() { lock.lock(); defer { lock.unlock() }; count += 1 }
}

private final class AdSkipCueTestClock: @unchecked Sendable {
    private let lock = NSLock()
    private var instant = ContinuousClock.now
    var value: ContinuousClock.Instant {
        lock.lock(); defer { lock.unlock() }; return instant
    }
    func advance(by duration: Duration) {
        lock.lock(); defer { lock.unlock() }
        instant = instant.advanced(by: duration)
    }
}
