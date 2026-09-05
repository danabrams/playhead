// PlaybackTransport.swift
// AVPlayer-based podcast playback engine with seek, rate, skip, and ad-skip
// smoothing. Wrapped in a global actor to serialize all transport operations.
//
// Separate from AnalysisAudioService — different queue, different purpose.

@preconcurrency import AVFoundation
import Foundation
import MediaPlayer

// MARK: - PlaybackState

/// Published snapshot of the current transport state.
struct PlaybackState: Sendable, Equatable {
    enum Status: Sendable, Equatable {
        case idle
        case loading
        case playing
        case paused
        case failed(String)
    }

    var status: Status = .idle
    var currentTime: TimeInterval = 0
    var duration: TimeInterval = 0
    var rate: Float = 0
    var playbackSpeed: Float = 1.0
}

// MARK: - PlaybackServiceActor

/// Global actor that serializes all playback operations.
@globalActor
actor PlaybackServiceActor {
    static let shared = PlaybackServiceActor()
}

extension Notification.Name {
    /// Posted by `PlaybackService` when the underlying `AVPlayerItem`
    /// reaches the end of its asset (re-broadcast of
    /// `AVPlayerItem.didPlayToEndTimeNotification` on the service's
    /// injected `NotificationCenter`). The playback queue's
    /// `PlaybackQueueAutoAdvancer` subscribes to this to drive
    /// auto-advance to the next queued episode (playhead-05i).
    static let playbackDidFinishEpisode = Notification.Name("PlaybackDidFinishEpisode")
}

// MARK: - PlaybackService

/// Playback transport wrapping AVPlayer. Handles long-form podcast audio,
/// background playback, lock screen controls, interruptions, and route changes.
///
/// All mutations go through `PlaybackServiceActor` to prevent data races.
/// Never blocks on SQLite or analysis work.
@PlaybackServiceActor
final class PlaybackService: NSObject, Sendable {

    // MARK: - Constants

    /// Speed bounds per spec.
    static let minSpeed: Float = 0.5
    static let maxSpeed: Float = 3.0

    /// Default skip intervals.
    nonisolated static let skipForwardSeconds: TimeInterval = 30
    nonisolated static let skipBackwardSeconds: TimeInterval = 15

    /// Duck volume during skip transitions on streamed audio.
    private static let duckVolume: Float = 0.15

    /// How long the transition dwells at the ducked level after the seek
    /// lands, before the volume comes back.
    ///
    /// playhead-1mq1.3 renamed this from `duckDuration`, whose doc comment
    /// called it the "duration of duck ramp". It never was one: nothing ramped
    /// over it, and it is a SETTLE DWELL that runs after the seek completes.
    /// The suite that pins it reads it correctly — "exactly one settle sleep"
    /// — so the constant's own name was the only thing claiming otherwise.
    /// That is this repo's standing defect class living in an identifier: a
    /// value that names one thing read as though it named another.
    private static let duckSettleDuration: TimeInterval = 0.15

    /// playhead-1mq1.3: how long the volume takes to come back after a skip.
    ///
    /// The duck DOWN stays a step. It is immediately followed by a seek that
    /// throws that audio away, and fading out first would delay the cut —
    /// latency is correctness on this path, because the banner fires as the
    /// playhead ENTERS the ad (playhead-d3g0) and the listener is waiting.
    ///
    /// The restore edge is the one that lands on the SHOW, mid-sentence, and
    /// a step from 0.15 to full there is the click that makes an intentional
    /// cut read as a glitch. 80 ms sits in the usual anti-click range: long
    /// enough that no discontinuity is audible, short enough that nobody waits
    /// for their audio.
    private static let duckRestoreRampDuration: TimeInterval = 0.08

    /// Volume writes per restore ramp. Eight over 80 ms is one every 10 ms,
    /// comfortably finer than the ear resolves as separate steps.
    private static let duckRestoreRampSteps = 8

    // MARK: - Player

    private let player: AVPlayer
    private var playerItem: AVPlayerItem?
    private var timeObserverToken: Any?

    // MARK: - Injected Seams

    /// Injectable AVAudioSession seam. Production uses the real singleton;
    /// tests pass a fake so parallel instances don't clobber each other's
    /// category/active state. See playhead-86s.
    private let audioSession: AudioSessionProviding

    /// Injectable MPNowPlayingInfoCenter seam. Production wraps the real
    /// MPNowPlayingInfoCenter.default(); tests pass a fake that stores the
    /// dictionary locally so parallel instances don't clobber each other.
    private let nowPlayingInfo: NowPlayingInfoProviding

    /// Injectable NotificationCenter for interruption + route-change
    /// observation. Production uses .default (where AVAudioSession actually
    /// posts); tests pass a private NotificationCenter and post synthetic
    /// interruption notifications to it without disturbing the process
    /// global or other parallel test instances.
    private let notificationCenter: NotificationCenter

    /// Injectable sleep for the duck-settle pause inside skip transitions
    /// (playhead-m9xk). Production uses the default, which is the exact
    /// `try? await Task.sleep(for:)` call the transition previously made
    /// inline — behavior with the default is identical. Tests inject a
    /// controllable sleeper so duck/seek/release ORDERING and the
    /// re-entrancy guard are verified deterministically instead of
    /// measuring wall-clock across a real 150 ms sleep (which the host
    /// scheduler stretches arbitrarily under full-suite load). The
    /// <500 ms transition-latency requirement is measured with the REAL
    /// sleeper in the serial perf pass (PerfGate / scripts/perf-tests.sh).
    private let transitionSleeper: @Sendable (Duration) async -> Void

    /// Injectable item-seek seam. Production delegates to AVPlayerItem and
    /// preserves its completion result; tests can suspend or cancel the
    /// operation to prove stale/cancelled seeks cannot publish a position
    /// after actor reentrancy.
    private let itemSeekOperation:
        @Sendable (AVPlayerItem, CMTime) async -> Bool
    #if DEBUG
    private var itemSeekOperationOverrideForTesting:
        (@Sendable (AVPlayerItem, CMTime) async -> Bool)?
    #endif

    /// playhead-nqwr: the ad-skip cue. Injected so tests observe the SEAM
    /// (was a cue emitted, and was it emitted once) without an audio device.
    /// Production shares one process-wide player — there is one transport, and
    /// building the `AVAudioPlayer` lazily keeps it off the launch path.
    private let skipCuePlayer: AdSkipCuePlaying

    /// playhead-nqwr: the user's switch, read at the seam rather than cached.
    /// A cached copy would need an observer and could answer with a value the
    /// user has already changed; the read is a single `UserDefaults` lookup, on
    /// a path that has just performed a seek.
    private let skipCueEnabled: @Sendable () -> Bool

    // MARK: - State

    private var _state = PlaybackState()
    /// playhead-s9mx: whether `_state.currentTime` is a reading of a real
    /// playhead, as opposed to a value the transport itself wrote for a
    /// reason that has nothing to do with where the listener is.
    ///
    /// `_state.currentTime` is a plain `TimeInterval` with a `0` default, so
    /// "the listener is at the start" and "no item is installed and this
    /// field means nothing" are the same bytes. Every consumer that only
    /// *displays* the value can live with that. The one consumer that
    /// **commits** it — `PlayheadApp.persistPlaybackPosition`, the sole
    /// writer of `Episode.playbackPosition` — cannot: it wrote `0.0` over a
    /// real position for two of Dan's four episodes on 2026-08-15, and the
    /// row it landed in is indistinguishable from one that was never played.
    ///
    /// Set `true` by the four sites that write a genuine position
    /// (`seek`'s itemless seam, `seek`'s real seek, the skip-transition
    /// completion, and the periodic time observer). Set `false` by the two
    /// events after which the field describes nothing:
    ///
    /// - `pauseAndDetachCurrentItem(preservingPosition: false)`, which
    ///   deliberately zeroes it. Note the asymmetry that makes a Bool the
    ///   right shape rather than an `playerItem != nil` check: the
    ///   `preservingPosition: true` call from `performStopPlayback` also
    ///   leaves `playerItem == nil`, and there the value IS the true final
    ///   playhead — Stop persists it on purpose.
    /// - `loadPlayerItem`, because a freshly installed item has not yet
    ///   produced a clock sample and the resume seek runs on
    ///   `AVPlayerItem` directly, without touching `_state`.
    ///
    /// `tearDown` deliberately does NOT clear it: teardown preserves
    /// `currentTime`, so the last reading is still the last real playhead.
    ///
    /// The `false` default covers a third non-playhead value that is not a
    /// write at all — `_state = PlaybackState()` above, whose `currentTime`
    /// is the struct's `0` default. A transport that has never played must
    /// report no playhead, not a listener sitting at 0:00.
    private var isCurrentTimeObservedPlayhead = false
    private var skipCues: [CMTimeRange] = []
    private var isLocalAsset: Bool = false
    /// Identity and cue range that own the active duck/seek/release
    /// transition. A unique token prevents stale deferred cleanup from
    /// clobbering a newer transition, while the range lets Listen cancel a
    /// merged-pod seek even when its target lies beyond the visible banner.
    private var nextSkipTransitionToken: UInt64 = 0
    private var activeSkipTransitionToken: UInt64?
    private var activeSkipTransitionItemGeneration: UInt64?
    private var activeSkipTransitionOriginalVolume: Float?
    private var activeSkipTransitionCueStart: TimeInterval?
    private var activeSkipTransitionTarget: TimeInterval?
    /// Monotonically identifies the item installed by `loadPlayerItem`.
    /// Episode-bound actions capture this before suspending and require the
    /// same generation at the actual seek boundary.
    private var playerItemGeneration: UInt64 = 0
    /// Latest-wins identity for user transport seeks within one item.
    /// Item identity alone cannot reject two overlapping seeks on the same
    /// episode because actor reentrancy lets the older AVFoundation await
    /// resume after the newer one.
    private var userSeekOperationGeneration: UInt64 = 0
    /// Stop/detach is a terminal boundary for the outgoing lock-screen card.
    /// Keep later item-less control/KVO updates from recreating it, while
    /// still allowing replacement metadata to opt back in before its item
    /// finishes loading.
    private var isNowPlayingPublicationSuppressed = false
    /// Long-lived async notification observer owned by this transport.
    /// Teardown cancels and joins it before returning.
    private var interruptionObservationTask: Task<Void, Never>?
    /// The public initializer is intentionally nonisolated so the synchronous
    /// app runtime can construct the transport. Actor-bound setup therefore
    /// runs in one retained task that teardown can cancel and join.
    private nonisolated(unsafe) var setupTask: Task<Void, Never>?
    private var isTornDown = false

    /// playhead-epii: rate multiplier currently applied on top of the
    /// user's `_state.playbackSpeed`. `1.0` means "no compression
    /// override". Set transiently by `SilenceCompressor` while the
    /// playhead is inside a non-content gap (music bed, dead air).
    /// Cleared on `endCompression()` and on every user-initiated speed
    /// change (`setSpeed`) so a manual speed flip mid-compression takes
    /// effect immediately and is preserved when the override clears.
    ///
    /// Effective `player.rate` while playing is
    /// `_state.playbackSpeed * compressionMultiplier`, clamped to
    /// `[Self.minSpeed, Self.maxSpeed]`.
    private var compressionMultiplier: Float = 1.0

    /// playhead-epii: name of the time-pitch algorithm currently in
    /// force on the active `AVPlayerItem`. Tracked so we don't churn
    /// the property on every periodic time observer tick — only set
    /// when the desired algorithm changes.
    private var currentTimePitchAlgorithm: AVAudioTimePitchAlgorithm = .spectral

    // MARK: - Streams

    /// Active state observers. Each observer receives the current snapshot
    /// immediately on subscription, then all subsequent updates.
    private var stateObservers: [UUID: AsyncStream<PlaybackState>.Continuation] = [:]

    // MARK: - Observation

    private var rateObservation: NSKeyValueObservation?
    private var itemStatusObservation: NSKeyValueObservation?

    /// Token for the block-based `AVPlayerItem.didPlayToEndTimeNotification`
    /// observer that re-broadcasts as `.playbackDidFinishEpisode`. Block-
    /// based registration is synchronous (the observer is live by the
    /// time `addObserver` returns), unlike the prior async-sequence path
    /// whose `for await` registration could lag tens of milliseconds
    /// behind init under parallel test load — the source of the
    /// `PlaybackFinishNotificationTests` flake. Removed in `tearDown`.
    ///
    /// `nonisolated(unsafe)` matches the precedent of `commandHandler`
    /// above: mutation is confined to init (single-threaded construction)
    /// and `tearDown` (actor-isolated by class isolation), and the
    /// observer block itself never reads this property.
    private nonisolated(unsafe) var finishObserverToken: (any NSObjectProtocol)?
    private nonisolated(unsafe) var routeChangeObserverToken: (any NSObjectProtocol)?

    /// Separate NSObject that receives remote command callbacks without actor
    /// isolation, then hops to PlaybackServiceActor via Tasks. Stored strongly
    /// here because MPRemoteCommand only holds an unretained reference to targets.
    private nonisolated(unsafe) var commandHandler: RemoteCommandHandler?

    // MARK: - Init

    nonisolated convenience override init() {
        self.init(
            audioSession: SystemAudioSessionProvider.shared,
            nowPlayingInfo: SystemNowPlayingInfoProvider.shared,
            notificationCenter: .default
        )
    }

    /// Designated initializer with injectable system seams. Production code
    /// uses the no-arg convenience init, which wires in the real singletons.
    /// Tests substitute fakes to keep parallel instances isolated from each
    /// other and from the process globals. See playhead-86s.
    ///
    /// `transitionSleeper` (playhead-m9xk) defaults to the real
    /// `Task.sleep` — production callers never pass it, so the shipped
    /// duck-settle behavior is unchanged.
    nonisolated init(
        audioSession: AudioSessionProviding,
        nowPlayingInfo: NowPlayingInfoProviding,
        notificationCenter: NotificationCenter,
        transitionSleeper: @escaping @Sendable (Duration) async -> Void = { duration in
            try? await Task.sleep(for: duration)
        },
        itemSeekOperation: @escaping @Sendable (
            AVPlayerItem,
            CMTime
        ) async -> Bool = { item, target in
            await item.seek(
                to: target,
                toleranceBefore: .zero,
                toleranceAfter: .zero
            )
        },
        skipCuePlayer: AdSkipCuePlaying = AdSkipCuePlayer.shared,
        skipCueEnabled: @escaping @Sendable () -> Bool = {
            AdSkipCueSettings.isEnabled()
        }
    ) {
        let player = AVPlayer()
        player.automaticallyWaitsToMinimizeStalling = true
        self.player = player
        self.audioSession = audioSession
        self.nowPlayingInfo = nowPlayingInfo
        self.notificationCenter = notificationCenter
        self.transitionSleeper = transitionSleeper
        self.itemSeekOperation = itemSeekOperation
        self.skipCuePlayer = skipCuePlayer
        self.skipCueEnabled = skipCueEnabled

        super.init()

        // Register the player-item-finish re-broadcast SYNCHRONOUSLY
        // (before the actor-isolated init Task spawns). Block-based
        // `addObserver` returns only after the observer is live on the
        // center, eliminating the for-await-startup race that previously
        // dropped notifications posted in the first ~50ms after init.
        // The block captures the center weakly; weak-self gates the
        // re-post so tearDown's `removeObserver` is sufficient cleanup
        // even if the block is briefly retained in flight.
        self.finishObserverToken = notificationCenter.addObserver(
            forName: AVPlayerItem.didPlayToEndTimeNotification,
            object: nil,
            queue: nil
        ) { [weak self, weak notificationCenter] _ in
            guard self != nil else { return }
            notificationCenter?.post(
                name: .playbackDidFinishEpisode,
                object: nil
            )
        }
        self.routeChangeObserverToken = notificationCenter.addObserver(
            forName: AVAudioSession.routeChangeNotification,
            object: nil,
            queue: nil
        ) { [weak self] notification in
            let reasonValue = notification.userInfo?[AVAudioSessionRouteChangeReasonKey] as? UInt
            Task { @PlaybackServiceActor [weak self] in
                self?.handleRouteChange(reasonValue: reasonValue)
            }
        }

        // The designated initializer is nonisolated so PlayheadRuntime can
        // construct the service synchronously. Retain the actor hop: teardown
        // cancels and joins it, preventing delayed setup from reinstalling
        // resources after teardown has removed them.
        self.setupTask = Task { @PlaybackServiceActor [weak self] in
            guard let self,
                  !Task.isCancelled,
                  !self.isTornDown
            else {
                return
            }
            self.configureAudioSession()
            self.configureRemoteCommands()
            self.restartPeriodicTimeObserver()
            self.observePlayerRate()
            // Interruptions use an async notification sequence, whose retained
            // task is cancelled and joined by `tearDown()`.
            self.observeInterruptionsAsync()
        }
    }

    /// Tear down observers and streams. Call before releasing the service.
    func tearDown() async {
        guard !isTornDown else { return }
        isTornDown = true
        let pendingSetupTask = setupTask
        pendingSetupTask?.cancel()
        setupTask = nil
        await pendingSetupTask?.value
        let interruptionTask = interruptionObservationTask
        interruptionTask?.cancel()
        interruptionObservationTask = nil
        // playhead-nqwr: nothing may still be sounding once the transport is
        // terminal.
        skipCuePlayer.stopAdSkipCue()
        cancelActiveSkipTransition()
        playerItemGeneration &+= 1
        player.pause()
        player.replaceCurrentItem(with: nil)
        playerItem = nil
        isNowPlayingPublicationSuppressed = true
        nowPlayingInfo.setNowPlayingInfo(nil)
        skipCues.removeAll()
        // KVO delivery is asynchronous. Once teardown has marked the service
        // terminal, rate/item callbacks are ignored, so publish the matching
        // stopped transport state explicitly instead of depending on a final
        // `player.rate == 0` callback that may already be queued.
        // Do not yield another value here: teardown's stream contract is to
        // finish existing observers immediately after their current snapshot.
        _state.status = .paused
        _state.rate = 0
        if let token = timeObserverToken {
            player.removeTimeObserver(token)
            timeObserverToken = nil
        }
        if let token = finishObserverToken {
            notificationCenter.removeObserver(token)
            finishObserverToken = nil
        }
        if let token = routeChangeObserverToken {
            notificationCenter.removeObserver(token)
            routeChangeObserverToken = nil
        }
        rateObservation?.invalidate()
        itemStatusObservation?.invalidate()
        removeRemoteCommandTargets()
        for continuation in stateObservers.values {
            continuation.finish()
        }
        stateObservers.removeAll()
        await interruptionTask?.value
    }

    // MARK: - Audio Session

    private func configureAudioSession() {
        do {
            try audioSession.setCategory(.playback, mode: .spokenAudio, policy: .longFormAudio)
            try audioSession.setActive(true)
        } catch {
            updateState { $0.status = .failed("Audio session: \(error.localizedDescription)") }
        }
    }

    // MARK: - Loading

    /// Load a podcast episode for playback.
    ///
    /// - Parameters:
    ///   - url: Remote or local audio URL.
    ///   - startPosition: Resume position in seconds (0 for start).
    func load(url: URL, startPosition: TimeInterval = 0) async {
        // Runtime shutdown cancels its prefetch task without joining it. That
        // task can therefore arrive here after `tearDown()` has removed every
        // observer and player item; never let a stale load resurrect them.
        guard !isTornDown else { return }
        // Determine if this is a local file.
        isLocalAsset = url.isFileURL

        let asset = AVURLAsset(url: url)
        let item = AVPlayerItem(asset: asset)
        loadPlayerItem(item)

        if startPosition > 0 {
            let target = CMTime(seconds: startPosition, preferredTimescale: 600)
            await player.currentItem?.seek(to: target, toleranceBefore: .zero, toleranceAfter: .zero)
        }
    }

    /// Load a pre-built AVPlayerItem. Used by the runtime to hand in a
    /// player item backed by a ProgressiveResourceLoader without storing
    /// the loader on this actor (which causes executor conflicts during
    /// audio session interruptions).
    func loadItem(_ item: AVPlayerItem, startPosition: TimeInterval = 0) async {
        guard !isTornDown else { return }
        isLocalAsset = true
        loadPlayerItem(item)

        if startPosition > 0 {
            let target = CMTime(seconds: startPosition, preferredTimescale: 600)
            await player.currentItem?.seek(to: target, toleranceBefore: .zero, toleranceAfter: .zero)
        }
    }

    /// Installs an item only while the transport still owns the admission
    /// generation captured at the runtime's detach boundary. This closes the
    /// cross-actor window where an older progressive task can queue a load,
    /// lose its runtime generation, and otherwise reinstall its item after a
    /// newer request has detached it.
    @discardableResult
    func load(
        url: URL,
        startPosition: TimeInterval = 0,
        ifCurrentItemGeneration expectedGeneration: UInt64
    ) async -> Bool {
        guard !isTornDown,
              playerItemGeneration == expectedGeneration else {
            return false
        }
        isLocalAsset = url.isFileURL
        let item = AVPlayerItem(asset: AVURLAsset(url: url))
        loadPlayerItem(item)
        let installedGeneration = playerItemGeneration

        if startPosition > 0 {
            let target = CMTime(seconds: startPosition, preferredTimescale: 600)
            await player.currentItem?.seek(
                to: target,
                toleranceBefore: .zero,
                toleranceAfter: .zero
            )
        }
        return !isTornDown
            && playerItemGeneration == installedGeneration
            && playerItem === item
            && player.currentItem === item
    }

    @discardableResult
    func loadItem(
        _ item: AVPlayerItem,
        startPosition: TimeInterval = 0,
        ifCurrentItemGeneration expectedGeneration: UInt64
    ) async -> Bool {
        guard !isTornDown,
              playerItemGeneration == expectedGeneration else {
            return false
        }
        isLocalAsset = true
        loadPlayerItem(item)
        let installedGeneration = playerItemGeneration

        if startPosition > 0 {
            let target = CMTime(seconds: startPosition, preferredTimescale: 600)
            await player.currentItem?.seek(
                to: target,
                toleranceBefore: .zero,
                toleranceAfter: .zero
            )
        }
        return !isTornDown
            && playerItemGeneration == installedGeneration
            && playerItem === item
            && player.currentItem === item
    }

    private func loadPlayerItem(_ item: AVPlayerItem) {
        guard !isTornDown else { return }
        // Invalidate any suspended duck/seek owned by the prior item before
        // replacing it. The old seek may already be inside AVFoundation; its
        // token checks will reject the completion after this synchronous clear.
        cancelActiveSkipTransition()
        playerItemGeneration &+= 1
        userSeekOperationGeneration &+= 1
        isNowPlayingPublicationSuppressed = false
        // playhead-s9mx: a newly installed item has produced no clock sample
        // yet, and the resume seek that follows in `load`/`loadItem` runs on
        // `AVPlayerItem` directly without touching `_state`. Until the
        // periodic observer ticks, `_state.currentTime` belongs to whatever
        // was playing before — or is the zero the detach left behind.
        isCurrentTimeObservedPlayhead = false
        playerItem = item

        itemStatusObservation?.invalidate()
        let block = makeItemStatusBlock(itemGeneration: playerItemGeneration)
        itemStatusObservation = item.observe(\.status, options: [.new], changeHandler: block)

        // playhead-epii: a new item carries the platform default
        // (`.lowQualityZeroLatency` per AVFoundation) which produces
        // audible artifacts above ~1.5×. Stamp `.spectral` immediately
        // so even baseline 2.0× listening sounds clean, and drop any
        // residual compression override left over from the previous
        // item (different episode / asset id ⇒ different feature
        // window plan).
        compressionMultiplier = 1.0
        currentTimePitchAlgorithm = .spectral
        item.audioTimePitchAlgorithm = .spectral

        updateState { $0.status = .loading }
        player.replaceCurrentItem(with: item)
        observePlayerRate()
        restartPeriodicTimeObserver()
    }

    /// Non-isolated so the closure avoids actor-executor crashes at call site.
    private nonisolated func makeItemStatusBlock(itemGeneration: UInt64)
        -> @Sendable (AVPlayerItem, NSKeyValueObservedChange<AVPlayerItem.Status>) -> Void
    {
        { [weak self] item, _ in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.handleItemStatusChange(
                    item,
                    expectedGeneration: itemGeneration
                )
            }
        }
    }

    private func handleItemStatusChange(
        _ item: AVPlayerItem,
        expectedGeneration: UInt64
    ) {
        applyItemStatusChange(
            item.status,
            duration: CMTimeGetSeconds(item.duration),
            failureMessage: item.error?.localizedDescription,
            for: item,
            expectedGeneration: expectedGeneration
        )
    }

    private func applyItemStatusChange(
        _ status: AVPlayerItem.Status,
        duration: TimeInterval,
        failureMessage: String?,
        for item: AVPlayerItem,
        expectedGeneration: UInt64
    ) {
        guard !isTornDown,
              playerItemGeneration == expectedGeneration,
              playerItem === item,
              player.currentItem === item else {
            return
        }

        switch status {
        case .readyToPlay:
            applyReadyToPlayState(duration: duration)
        case .failed:
            updateState { $0.status = .failed(failureMessage ?? "unknown") }
        case .unknown:
            break
        @unknown default:
            break
        }
    }

    // MARK: - Transport Controls

    func play() {
        guard !isTornDown, playerItem != nil else { return }
        // playhead-epii: factor in any active compression multiplier so
        // resuming inside a compressed region doesn't snap to base
        // speed for one observer cycle. Clamp identically to the
        // setSpeed/applyEffectiveRateIfPlaying paths.
        let target = min(
            max(_state.playbackSpeed * compressionMultiplier, Self.minSpeed),
            Self.maxSpeed
        )
        player.playImmediately(atRate: target)
        updateState { $0.status = .playing }
        updateNowPlayingInfo()
    }

    func pause() {
        guard !isTornDown else { return }
        // playhead-nqwr: silence a cue that is still ringing. This is the
        // shared exit for a user pause, an interruption (`.began`) and a route
        // that vanished (`.oldDeviceUnavailable`) — so unplugging headphones
        // onto a seam cannot leave the chime playing out of the speaker after
        // the episode has stopped.
        skipCuePlayer.stopAdSkipCue()
        player.pause()
        updateState { $0.status = .paused }
        updateNowPlayingInfo()
    }

    /// Pause and synchronously detach the installed item.
    ///
    /// Replacement and Stop release progressive-loader ownership immediately
    /// after this boundary. Merely pausing leaves the old item resumable by a
    /// remote Play command after its backing transfer has been cancelled.
    /// Invalidating the item generation also rejects any suspended seek or KVO
    /// completion owned by the detached episode.
    func pauseAndDetachCurrentItem(
        preservingPosition: Bool = false,
        force: Bool = true
    ) -> UInt64? {
        guard !isTornDown else { return nil }
        guard force || playerItem != nil || player.currentItem != nil else {
            return playerItemGeneration
        }
        // playhead-nqwr: the episode is being taken away; a cue about it must
        // not outlive it.
        skipCuePlayer.stopAdSkipCue()
        cancelActiveSkipTransition()
        playerItemGeneration &+= 1
        rateObservation?.invalidate()
        rateObservation = nil
        player.pause()
        itemStatusObservation?.invalidate()
        itemStatusObservation = nil
        player.replaceCurrentItem(with: nil)
        playerItem = nil
        isNowPlayingPublicationSuppressed = true
        skipCues.removeAll()
        compressionMultiplier = 1
        currentTimePitchAlgorithm = .spectral
        if let token = timeObserverToken {
            player.removeTimeObserver(token)
            timeObserverToken = nil
        }
        if !preservingPosition {
            // playhead-s9mx: the zeroing below is a transport bookkeeping
            // write, not a playhead. Mark it as such BEFORE `updateState`
            // publishes, so no observer can read the zero and the stale
            // `true` in the same snapshot.
            isCurrentTimeObservedPlayhead = false
        }
        updateState {
            $0.status = .paused
            $0.rate = 0
            if !preservingPosition {
                $0.currentTime = 0
                $0.duration = 0
            }
        }
        nowPlayingInfo.setNowPlayingInfo(nil)
        return playerItemGeneration
    }

    /// playhead-s9mx: the transport's `currentTime` **only when it is a real
    /// playhead**, and `nil` when the field is carrying a value no listener
    /// ever produced.
    ///
    /// This is the reading a commit point must use. `snapshot()` keeps
    /// returning the raw state for every display consumer.
    func observedPlayhead() -> TimeInterval? {
        guard isCurrentTimeObservedPlayhead else { return nil }
        return _state.currentTime
    }

    func togglePlayPause() {
        if case .playing = _state.status {
            pause()
        } else {
            play()
        }
    }

    /// Returns the latest transport snapshot for higher-level coordinators.
    func snapshot() -> PlaybackState {
        _state
    }

    /// Returns transport state together with the identity of the installed
    /// player item. The pair is captured in one actor turn.
    func snapshotWithItemGeneration() -> (
        state: PlaybackState,
        itemGeneration: UInt64
    ) {
        (_state, playerItemGeneration)
    }

    /// Subscribe to playback state with immediate hydration.
    /// This avoids remount bugs where a late subscriber sees defaults until the
    /// next transport event arrives.
    func observeStates() -> AsyncStream<PlaybackState> {
        AsyncStream { continuation in
            // Teardown is terminal. A subscriber that attaches after that
            // boundary still receives the final snapshot promised by the
            // hydration contract, but its stream must finish immediately
            // instead of being retained forever with no possible producer.
            if isTornDown {
                continuation.yield(_state)
                continuation.finish()
                return
            }
            let id = UUID()
            stateObservers[id] = continuation
            continuation.yield(_state)
            continuation.onTermination = { [weak self] _ in
                Task { @PlaybackServiceActor in
                    self?.stateObservers.removeValue(forKey: id)
                }
            }
        }
    }

    /// Seek to an absolute position in seconds.
    private func performItemSeek(
        _ item: AVPlayerItem,
        target: CMTime
    ) async -> Bool {
        #if DEBUG
        if let itemSeekOperationOverrideForTesting {
            return await itemSeekOperationOverrideForTesting(item, target)
        }
        #endif
        return await itemSeekOperation(item, target)
    }

    /// Seek to an absolute position in seconds.
    @discardableResult
    func seek(to seconds: TimeInterval) async -> Bool {
        guard !isTornDown, seconds.isFinite, seconds >= 0 else {
            return false
        }
        // Preserve the transport's deterministic state-seam contract when no
        // AVPlayer item is installed. Playback reliability tests and preview
        // callers intentionally drive the state machine without decoded
        // media; there is no item identity that can become stale in this path.
        guard player.currentItem != nil else {
            userSeekOperationGeneration &+= 1
            isCurrentTimeObservedPlayhead = true
            updateState { $0.currentTime = seconds }
            updateNowPlayingInfo()
            return true
        }
        return await seek(
            to: seconds,
            ifCurrentItemGeneration: playerItemGeneration
        )
    }

    /// Seek only if the item installed when the caller captured its transport
    /// context is still current. The identity check after `await` is
    /// load-bearing: `AVPlayerItem.seek` suspends this actor, allowing a new
    /// episode to replace the item before the old seek resumes.
    @discardableResult
    func seek(
        to seconds: TimeInterval,
        ifCurrentItemGeneration expectedGeneration: UInt64
    ) async -> Bool {
        guard !isTornDown,
              seconds.isFinite,
              seconds >= 0,
              playerItemGeneration == expectedGeneration,
              let item = player.currentItem,
              playerItem === item else {
            return false
        }
        userSeekOperationGeneration &+= 1
        let seekGeneration = userSeekOperationGeneration

        // A user transport seek owns the item once it starts. Retire any
        // in-flight automatic transition synchronously first so a subsequent
        // cue removal cannot call cancelPendingSeeks() against this newer
        // seek. The active transition's completion is already token-guarded.
        cancelActiveSkipTransition()
        // A newer user seek supersedes any older pending user seek on this
        // same item. The generation check below remains the authoritative
        // state-publication fence for injected/non-cancellable operations.
        item.cancelPendingSeeks()
        let target = CMTime(seconds: seconds, preferredTimescale: 600)
        let completed = await performItemSeek(item, target: target)
        guard completed,
              userSeekOperationGeneration == seekGeneration,
              playerItemGeneration == expectedGeneration,
              player.currentItem === item,
              playerItem === item else {
            return false
        }
        isCurrentTimeObservedPlayhead = true
        updateState { $0.currentTime = seconds }
        updateNowPlayingInfo()
        return true
    }

    /// Skip forward by the given number of seconds (default 30).
    ///
    /// playhead-jw63.3: this is the remote-command surface — lock screen,
    /// Control Centre, CarPlay, AirPods, Watch. It is disjoint from
    /// `PlayheadRuntime.skipForward()`, which computes its own target, so
    /// counting here and there counts each listener reach exactly once. Ad
    /// auto-skip does not pass through this function.
    func skipForward(_ seconds: TimeInterval = PlaybackService.skipForwardSeconds) async {
        AnalyticsRecorder.manualSkipForwardReach(durationSeconds: _state.duration)
        let newTime = min(_state.currentTime + seconds, _state.duration)
        await seek(to: newTime)
    }

    /// Skip backward by the given number of seconds (default 15).
    func skipBackward(_ seconds: TimeInterval = PlaybackService.skipBackwardSeconds) async {
        let newTime = max(_state.currentTime - seconds, 0)
        await seek(to: newTime)
    }

    // MARK: - Speed Control

    /// Set playback speed, clamped to 0.5x–3.0x.
    ///
    /// playhead-epii: a manual speed change always wins over an active
    /// silence-compression override. We clear the multiplier and the
    /// `.varispeed` algorithm here so the user's flip from 1.0×→1.25×
    /// (for example) lands at exactly 1.25×, even if the playhead was
    /// inside a 2.5× compressed music bed at the time. The compressor
    /// will re-engage on the next lookahead tick if appropriate.
    func setSpeed(_ speed: Float) {
        guard !isTornDown else { return }
        let clamped = min(max(speed, Self.minSpeed), Self.maxSpeed)
        _state.playbackSpeed = clamped
        compressionMultiplier = 1.0
        applyTimePitchAlgorithm(.spectral)
        if case .playing = _state.status {
            player.rate = clamped
        }
        updateState { $0.playbackSpeed = clamped }
        updateNowPlayingInfo()
    }

    // MARK: - Silence Compression Surface (playhead-epii)

    /// Engage a compression override: multiply the user's base speed by
    /// `multiplier` and switch the time-pitch algorithm to `algorithm`.
    /// Idempotent — calling with identical values is a no-op.
    ///
    /// The multiplier is bounded by `setSpeed`'s overall clamp; the
    /// effective player rate becomes
    /// `clamp(_state.playbackSpeed * multiplier, minSpeed, maxSpeed)`,
    /// so a 1.5× user speed × 2.0× compression saturates at 3.0× rather
    /// than escaping the documented bounds.
    ///
    /// Only mutates `player.rate` when the transport is actively
    /// playing. While paused, the multiplier is recorded but not
    /// realized — `play()` reads `_state.playbackSpeed` directly, so we
    /// re-apply on the next observer tick after playback resumes.
    func beginCompression(
        multiplier: Float,
        algorithm: AVAudioTimePitchAlgorithm
    ) {
        guard !isTornDown else { return }
        let clampedMultiplier = max(1.0, multiplier)
        guard
            clampedMultiplier != compressionMultiplier
                || algorithm != currentTimePitchAlgorithm
        else { return }
        compressionMultiplier = clampedMultiplier
        applyTimePitchAlgorithm(algorithm)
        applyEffectiveRateIfPlaying()
    }

    /// Disengage compression: drop the multiplier to 1.0 and restore
    /// the `.spectral` algorithm. If the user changed their base speed
    /// during compression, this method correctly restores to the new
    /// base speed (not the pre-compression base) — `setSpeed` already
    /// stamped the new value into `_state.playbackSpeed`.
    func endCompression() {
        guard !isTornDown,
            compressionMultiplier != 1.0
                || currentTimePitchAlgorithm != .spectral
        else { return }
        compressionMultiplier = 1.0
        applyTimePitchAlgorithm(.spectral)
        applyEffectiveRateIfPlaying()
    }

    /// Test/coordinator-facing read of the current effective compression
    /// multiplier. `1.0` means no compression. Sendable scalar.
    var currentCompressionMultiplier: Float { compressionMultiplier }

    /// Test/coordinator-facing read of the algorithm currently applied
    /// to the active `AVPlayerItem`'s audio mix.
    var currentTimePitchAlgorithmName: AVAudioTimePitchAlgorithm {
        currentTimePitchAlgorithm
    }

    private func applyEffectiveRateIfPlaying() {
        guard case .playing = _state.status else { return }
        let target = min(
            max(_state.playbackSpeed * compressionMultiplier, Self.minSpeed),
            Self.maxSpeed
        )
        player.rate = target
    }

    private func applyTimePitchAlgorithm(_ algorithm: AVAudioTimePitchAlgorithm) {
        guard let item = playerItem else {
            currentTimePitchAlgorithm = algorithm
            return
        }
        if item.audioTimePitchAlgorithm != algorithm {
            item.audioTimePitchAlgorithm = algorithm
        }
        currentTimePitchAlgorithm = algorithm
    }

    // MARK: - Skip Cues

    /// Accept skip cue ranges from SkipOrchestrator.
    /// When playback enters a cue range, the service performs a smooth skip.
    func setSkipCues(_ cues: [CMTimeRange]) {
        guard !isTornDown else { return }
        skipCues = cues
        guard let activeStart = activeSkipTransitionCueStart,
              let activeEnd = activeSkipTransitionTarget
        else {
            return
        }

        // A transition is claimed synchronously when the periodic observer
        // enters a cue, then executed by a queued Task. An eligibility flip,
        // episode clear, or cue recomputation can remove/replace that range
        // before the Task starts. Keep the reservation only while the exact
        // owning cue remains armed; otherwise the stale seek must be canceled.
        let stillArmed = cues.contains { cue in
            let cueStart = CMTimeGetSeconds(cue.start)
            let cueEnd = CMTimeGetSeconds(CMTimeRangeGetEnd(cue))
            return abs(cueStart - activeStart) <= 0.001
                && abs(cueEnd - activeEnd) <= 0.001
        }
        if !stillArmed {
            cancelActiveSkipTransition()
        }
    }

    /// Synchronously disarms any currently-installed cue overlapping the
    /// user's restored banner span. The full span matters when edge padding
    /// moves a cue's start later than the banner's rewind target.
    func disarmSkipCues(overlappingStart start: TimeInterval, end: TimeInterval) {
        guard !isTornDown else { return }
        skipCues.removeAll { cue in
            let cueStart = CMTimeGetSeconds(cue.start)
            let cueEnd = CMTimeGetSeconds(CMTimeRangeGetEnd(cue))
            return cueStart < end && cueEnd > start
        }
        if let cueStart = activeSkipTransitionCueStart,
           let cueEnd = activeSkipTransitionTarget,
           cueStart < end,
           cueEnd > start {
            cancelActiveSkipTransition()
        }
    }

    /// Item-bound form for deferred banner actions. A runtime episode check
    /// performed before this actor hop is not enough: the player item can be
    /// replaced before the hop executes. Reject the stale disarm rather than
    /// removing cues or canceling a transition owned by the replacement item.
    @discardableResult
    func disarmSkipCues(
        overlappingStart start: TimeInterval,
        end: TimeInterval,
        ifCurrentItemGeneration expectedGeneration: UInt64
    ) -> Bool {
        guard !isTornDown,
              playerItemGeneration == expectedGeneration else {
            return false
        }
        disarmSkipCues(overlappingStart: start, end: end)
        return playerItemGeneration == expectedGeneration
    }

    /// Cancels the actor-owned half of a reserved transition and restores the
    /// shared player immediately. Any already-suspended seek completion sees
    /// the cleared token and cannot publish position or volume afterward.
    private func cancelActiveSkipTransition() {
        guard activeSkipTransitionToken != nil else { return }
        player.currentItem?.cancelPendingSeeks()
        if let originalVolume = activeSkipTransitionOriginalVolume {
            player.volume = originalVolume
        }
        activeSkipTransitionToken = nil
        activeSkipTransitionItemGeneration = nil
        activeSkipTransitionOriginalVolume = nil
        activeSkipTransitionCueStart = nil
        activeSkipTransitionTarget = nil
    }

    /// Check if current time has entered a skip cue and handle it.
    private func checkSkipCues(currentTime: CMTime) {
        guard activeSkipTransitionToken == nil,
              !skipCues.isEmpty
        else {
            return
        }

        let currentSeconds = CMTimeGetSeconds(currentTime)
        for cue in skipCues {
            let start = CMTimeGetSeconds(cue.start)
            let end = CMTimeGetSeconds(CMTimeRangeGetEnd(cue))
            if currentSeconds >= start, currentSeconds < end {
                guard let transitionToken = reserveSkipTransition(
                    to: end,
                    cueStart: start
                ) else {
                    return
                }
                Task { @PlaybackServiceActor in
                    await self.performReservedSkipTransition(
                        transitionToken: transitionToken
                    )
                }
                return
            }
        }
    }

    /// Perform a perceptually clean skip transition: duck volume, seek, release.
    private func performSkipTransition(
        to targetSeconds: TimeInterval,
        cueStart: TimeInterval
    ) async {
        guard let transitionToken = reserveSkipTransition(
            to: targetSeconds,
            cueStart: cueStart
        ) else {
            return
        }
        await performReservedSkipTransition(
            transitionToken: transitionToken
        )
    }

    /// Claims the transition synchronously with cue detection. Listen can then
    /// invalidate the claim before the unstructured async task begins, so
    /// queued work cannot resurrect a cue that was already disarmed.
    private func reserveSkipTransition(
        to targetSeconds: TimeInterval,
        cueStart: TimeInterval
    ) -> UInt64? {
        guard activeSkipTransitionToken == nil else { return nil }
        let transitionGeneration = playerItemGeneration
        let originalVolume = player.volume
        nextSkipTransitionToken &+= 1
        let transitionToken = nextSkipTransitionToken
        activeSkipTransitionToken = transitionToken
        activeSkipTransitionItemGeneration = transitionGeneration
        activeSkipTransitionOriginalVolume = originalVolume
        activeSkipTransitionCueStart = cueStart
        activeSkipTransitionTarget = targetSeconds
        return transitionToken
    }

    /// Executes a previously claimed transition. Every field is re-read from
    /// the active reservation so a replacement item or Listen disarm that ran
    /// after detection makes this queued task a no-op.
    private func performReservedSkipTransition(
        transitionToken: UInt64
    ) async {
        guard activeSkipTransitionToken == transitionToken,
              let transitionGeneration =
                activeSkipTransitionItemGeneration,
              let originalVolume = activeSkipTransitionOriginalVolume,
              let targetSeconds = activeSkipTransitionTarget
        else {
            return
        }
        let transitionItem = player.currentItem
        defer {
            if activeSkipTransitionToken == transitionToken {
                activeSkipTransitionToken = nil
                activeSkipTransitionItemGeneration = nil
                activeSkipTransitionOriginalVolume = nil
                activeSkipTransitionCueStart = nil
                activeSkipTransitionTarget = nil
            }
        }

        await duckSeekRelease(
            to: targetSeconds,
            item: transitionItem,
            expectedGeneration: transitionGeneration,
            transitionToken: transitionToken,
            originalVolume: originalVolume
        )
    }

    /// Duck volume, seek precisely, then restore volume.
    private func duckSeekRelease(
        to seconds: TimeInterval,
        item: AVPlayerItem?,
        expectedGeneration: UInt64,
        transitionToken: UInt64,
        originalVolume: Float
    ) async {
        // Duck
        player.volume = Self.duckVolume

        // Seek
        let target = CMTime(seconds: seconds, preferredTimescale: 600)
        if let item {
            let completed = await performItemSeek(item, target: target)
            guard completed else {
                // A failed seek leaves the same transition alive but has
                // already passed the duck. Release only when this token still
                // owns the current item; replacement/disarm paths restore the
                // captured volume while invalidating the token and must not be
                // overwritten by this stale completion.
                if isCurrentSkipTransition(
                    item: item,
                    expectedGeneration: expectedGeneration,
                    transitionToken: transitionToken
                ) {
                    player.volume = originalVolume
                    activeSkipTransitionOriginalVolume = nil
                }
                return
            }
            guard isCurrentSkipTransition(
                item: item,
                expectedGeneration: expectedGeneration,
                transitionToken: transitionToken
            ) else {
                return
            }
        }

        // Brief pause for the seek to settle, then release. Routed
        // through the injectable sleeper seam (playhead-m9xk); the
        // production default performs the identical
        // `try? await Task.sleep(for:)` this line previously inlined.
        await transitionSleeper(
            .milliseconds(Int(Self.duckSettleDuration * 1000))
        )
        if let item {
            guard isCurrentSkipTransition(
                item: item,
                expectedGeneration: expectedGeneration,
                transitionToken: transitionToken
            ) else {
                return
            }
        } else {
            guard playerItemGeneration == expectedGeneration,
                  player.currentItem == nil,
                  activeSkipTransitionToken == transitionToken,
                  activeSkipTransitionItemGeneration
                    == expectedGeneration
            else {
                return
            }
        }

        // The position is already correct, so publish it BEFORE the ramp.
        // The timeline's 0.45 s glide (TimelineRailView) keys off this state,
        // and holding it back for a cosmetic fade would delay the one piece of
        // feedback the listener actually looks at.
        isCurrentTimeObservedPlayhead = true
        updateState { $0.currentTime = seconds }
        updateNowPlayingInfo()

        // playhead-1mq1.3: bring the volume back over a ramp, not a step.
        await rampVolumeToRestore(
            originalVolume: originalVolume,
            item: item,
            expectedGeneration: expectedGeneration,
            transitionToken: transitionToken
        )

        // playhead-nqwr: the LAST statement of the ONLY path that completes a
        // skip, and the only site that sounds the cue.
        //
        // Every earlier `return` in this method is a skip that did not happen —
        // a seek that was cancelled, an item that was replaced, a reservation
        // Listen disarmed — so a cue placed here cannot announce one. It hangs
        // off the transition itself rather than off a parallel prediction of
        // it, which is the mistake playhead-bwxi had to undo for the banner: a
        // second consumer of the same event drifts out of agreement with it.
        emitAdSkipCue()
    }

    /// playhead-1mq1.3: return the player to its captured volume gradually.
    ///
    /// WHY THIS NEVER RETURNS EARLY. Every other guard in the transition
    /// protects a skip that has not happened yet, so bailing out is correct
    /// there. By the time this runs the seek has completed and the position
    /// has been published — the cut DID land. An early return would carry past
    /// `emitAdSkipCue()`, suppressing the cue for a real skip, and the cue has
    /// its own three gates for deciding whether it should sound. So a lost
    /// claim stops this from WRITING and nothing more.
    ///
    /// A lost claim also means somebody else already owns the volume:
    /// `cancelActiveSkipTransition` and the replacement paths restore the
    /// captured level themselves, and a half-finished ramp writing over them
    /// is precisely the stale-completion bug the token guards exist to stop.
    private func rampVolumeToRestore(
        originalVolume: Float,
        item: AVPlayerItem?,
        expectedGeneration: UInt64,
        transitionToken: UInt64
    ) async {
        let steps = Self.duckRestoreRampSteps
        guard steps > 0 else {
            player.volume = originalVolume
            activeSkipTransitionOriginalVolume = nil
            return
        }
        let start = Self.duckVolume
        let span = originalVolume - start
        // MILLISECONDS. `duckRestoreRampDuration` is in seconds, so the
        // 1000 converts; `.milliseconds` is what consumes it. Naming the unit
        // here is not decoration — the first draft of this line called the
        // value `stepNanoseconds` while computing milliseconds, which is the
        // same defect the `duckSettleDuration` comment above is about.
        let stepMilliseconds = Int(
            Self.duckRestoreRampDuration / Double(steps) * 1000
        )

        for step in 1...steps {
            await transitionSleeper(.milliseconds(stepMilliseconds))
            guard stillOwnsSkipTransition(
                item: item,
                expectedGeneration: expectedGeneration,
                transitionToken: transitionToken
            ) else {
                return
            }
            // The final step assigns the captured value itself rather than a
            // computed one, so no accumulated float error can leave the
            // listener a hair below where they were.
            player.volume = step == steps
                ? originalVolume
                : start + span * (Float(step) / Float(steps))
        }
        activeSkipTransitionOriginalVolume = nil
    }

    /// The claim check both ramp steps and the seek completion share.
    ///
    /// The itemless branch cannot use `isCurrentSkipTransition`, which needs an
    /// item to compare, so it repeats the same three facts against `nil`.
    private func stillOwnsSkipTransition(
        item: AVPlayerItem?,
        expectedGeneration: UInt64,
        transitionToken: UInt64
    ) -> Bool {
        if let item {
            return isCurrentSkipTransition(
                item: item,
                expectedGeneration: expectedGeneration,
                transitionToken: transitionToken
            )
        }
        return playerItemGeneration == expectedGeneration
            && player.currentItem == nil
            && activeSkipTransitionToken == transitionToken
            && activeSkipTransitionItemGeneration == expectedGeneration
    }

    /// playhead-nqwr: sound the ad-skip cue for a cut that has just landed.
    ///
    /// Three gates, and the middle one is what makes an interruption or a
    /// vanished route safe. `pause()` sets `.paused` SYNCHRONOUSLY on this
    /// actor, and it is what both the interruption handler (`.began`) and the
    /// route-change handler (`.oldDeviceUnavailable`) call — so a transport
    /// that is no longer the thing the listener is hearing cannot emit, and the
    /// check cannot race the event because both run here. The `await
    /// transitionSleeper(...)` inside the transition is a real window for one
    /// of those to arrive, which is why this is a live check and not an
    /// assertion.
    ///
    /// Skipping while paused is deliberately silent for the same reason: no
    /// audio moved that the listener could have noticed.
    private func emitAdSkipCue() {
        guard !isTornDown,
              case .playing = _state.status,
              skipCueEnabled()
        else {
            return
        }
        skipCuePlayer.playAdSkipCue()
    }

    private func isCurrentSkipTransition(
        item: AVPlayerItem,
        expectedGeneration: UInt64,
        transitionToken: UInt64
    ) -> Bool {
        playerItemGeneration == expectedGeneration
            && activeSkipTransitionToken == transitionToken
            && activeSkipTransitionItemGeneration
                == expectedGeneration
            && player.currentItem === item
            && playerItem === item
    }

    // MARK: - Time Observer

    /// Cadence of the `AVPlayer` periodic time observer, in seconds.
    ///
    /// Named because it is no longer a private implementation detail:
    /// playhead-d3g0 makes the suggest banner fire when the playhead ENTERS an
    /// ad span, so this interval is the quantisation floor on how quickly the
    /// orchestrator can learn that entry happened. Anything downstream that
    /// states a position-latency budget must be derived from this value rather
    /// than from a hopeful constant — see
    /// `SkipOrchestrator.suggestEntryLatencyBudgetSeconds`.
    nonisolated static let periodicTimeObserverIntervalSeconds: TimeInterval = 0.25

    private func restartPeriodicTimeObserver() {
        if let token = timeObserverToken {
            player.removeTimeObserver(token)
            timeObserverToken = nil
        }
        let interval = CMTime(
            seconds: Self.periodicTimeObserverIntervalSeconds,
            preferredTimescale: 600
        )
        let item = player.currentItem
        let itemGeneration = playerItemGeneration
        let block = makeTimeObserverBlock(
            item: item,
            itemGeneration: itemGeneration
        )
        timeObserverToken = player.addPeriodicTimeObserver(
            forInterval: interval, queue: nil, using: block
        )
    }

    /// Non-isolated so the closure avoids actor-executor crashes at call site.
    private nonisolated func makeTimeObserverBlock(
        item: AVPlayerItem?,
        itemGeneration: UInt64
    ) -> @Sendable (CMTime) -> Void {
        { [weak self] time in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.handlePeriodicTime(
                    time,
                    for: item,
                    expectedGeneration: itemGeneration
                )
            }
        }
    }

    private func handlePeriodicTime(
        _ time: CMTime,
        for item: AVPlayerItem?,
        expectedGeneration: UInt64
    ) {
        guard !isTornDown,
              playerItemGeneration == expectedGeneration else { return }
        if let item {
            guard player.currentItem === item,
                  playerItem === item
            else {
                return
            }
        } else {
            guard player.currentItem == nil else { return }
        }
        let seconds = CMTimeGetSeconds(time)
        guard seconds.isFinite else { return }
        isCurrentTimeObservedPlayhead = true
        updateState { $0.currentTime = seconds }
        checkSkipCues(currentTime: time)
    }

    // MARK: - Rate Observation

    private func observePlayerRate() {
        rateObservation?.invalidate()
        let itemGeneration = playerItemGeneration
        let block = makeRateObserverBlock(itemGeneration: itemGeneration)
        rateObservation = player.observe(\.rate, options: [.new], changeHandler: block)
    }

    /// Non-isolated so the closure avoids actor-executor crashes at call site.
    private nonisolated func makeRateObserverBlock(
        itemGeneration: UInt64
    )
        -> @Sendable (AVPlayer, NSKeyValueObservedChange<Float>) -> Void
    {
        { [weak self] player, change in
            guard let self else { return }
            let rate = change.newValue ?? player.rate
            Task { @PlaybackServiceActor in
                self.applyObservedRate(
                    rate,
                    expectedGeneration: itemGeneration
                )
            }
        }
    }

    // MARK: - Interruptions (Async)

    /// Observe audio session interruptions using an async notification
    /// sequence that runs entirely on PlaybackServiceActor. This avoids
    /// the Swift 6 actor isolation crash that occurs when Combine's .sink
    /// closure accesses actor-isolated self from the main queue.
    private func observeInterruptionsAsync() {
        let center = notificationCenter
        interruptionObservationTask?.cancel()
        interruptionObservationTask = Task { [weak self] in
            let notifications = center.notifications(
                named: AVAudioSession.interruptionNotification
            )
            for await notification in notifications {
                guard !Task.isCancelled, let self else { break }
                guard !self.isTornDown else { break }
                guard let info = notification.userInfo,
                      let typeValue = info[AVAudioSessionInterruptionTypeKey] as? UInt,
                      let type = AVAudioSession.InterruptionType(rawValue: typeValue)
                else { continue }

                switch type {
                case .began:
                    self.pause()
                case .ended:
                    if let optionsValue = info[AVAudioSessionInterruptionOptionKey] as? UInt {
                        let options = AVAudioSession.InterruptionOptions(rawValue: optionsValue)
                        if options.contains(.shouldResume) {
                            self.play()
                        }
                    }
                @unknown default:
                    break
                }
            }
        }
    }

    // MARK: - Route Changes

    private func handleRouteChange(reasonValue: UInt?) {
        guard !isTornDown,
              let reasonValue,
              let reason = AVAudioSession.RouteChangeReason(rawValue: reasonValue)
        else { return }

        switch reason {
        case .oldDeviceUnavailable:
            pause()
        default:
            break
        }
    }

    // MARK: - Now Playing Info Center

    private func updateNowPlayingInfo() {
        guard !isNowPlayingPublicationSuppressed else {
            nowPlayingInfo.setNowPlayingInfo(nil)
            return
        }
        var info = nowPlayingInfo.getNowPlayingInfo() ?? [:]
        info[MPMediaItemPropertyPlaybackDuration] = _state.duration
        info[MPNowPlayingInfoPropertyElapsedPlaybackTime] = _state.currentTime
        info[MPNowPlayingInfoPropertyPlaybackRate] = _state.rate
        info[MPNowPlayingInfoPropertyDefaultPlaybackRate] = _state.playbackSpeed
        nowPlayingInfo.setNowPlayingInfo(info)
    }

    /// Update Now Playing with episode metadata (title, artwork, etc.).
    func setNowPlayingMetadata(
        title: String,
        artist: String? = nil,
        albumTitle: String? = nil,
        artworkImage: UIImage? = nil
    ) {
        guard !isTornDown else { return }
        isNowPlayingPublicationSuppressed = false
        var info = nowPlayingInfo.getNowPlayingInfo() ?? [:]
        info[MPMediaItemPropertyTitle] = title
        if let artist { info[MPMediaItemPropertyArtist] = artist }
        if let albumTitle { info[MPMediaItemPropertyAlbumTitle] = albumTitle }
        if let image = artworkImage {
            info[MPMediaItemPropertyArtwork] = Self.makeArtwork(image: image)
        }
        info[MPMediaItemPropertyPlaybackDuration] = _state.duration
        info[MPNowPlayingInfoPropertyElapsedPlaybackTime] = _state.currentTime
        info[MPNowPlayingInfoPropertyPlaybackRate] = _state.rate
        nowPlayingInfo.setNowPlayingInfo(info)
    }

    /// Non-isolated so the closure avoids actor-executor crashes at call site.
    private nonisolated static func makeArtwork(image: UIImage) -> MPMediaItemArtwork {
        MPMediaItemArtwork(boundsSize: image.size) { _ in image }
    }

    // MARK: - Remote Commands

    /// Register for remote commands via a separate, non-isolated handler object.
    /// Swift 6 taints closures formed inside @globalActor-isolated classes with
    /// the actor's isolation, causing runtime aborts when MediaPlayer invokes
    /// them from the main thread. RemoteCommandHandler is a plain class — its
    /// closures carry no actor isolation and use the closure-based addTarget API
    /// which retains the closure (unlike target/action which is unretained).
    private func configureRemoteCommands() {
        let handler = RemoteCommandHandler(service: self)
        handler.register()
        commandHandler = handler
    }

    /// Remove all registered remote command targets to prevent leaks.
    private func removeRemoteCommandTargets() {
        commandHandler?.unregister()
        commandHandler = nil
    }

    // MARK: - State Update

    private func applyReadyToPlayState(duration: TimeInterval) {
        let currentRate = player.rate
        let wasPlaying = if case .playing = _state.status { true } else { false }
        updateState {
            $0.duration = duration.isFinite ? duration : 0
            $0.rate = currentRate
            $0.status = (currentRate > 0 || wasPlaying) ? .playing : .paused
        }
        updateNowPlayingInfo()
    }

    private func applyObservedRate(
        _ rate: Float,
        expectedGeneration: UInt64? = nil
    ) {
        guard !isTornDown else { return }
        if let expectedGeneration {
            guard playerItemGeneration == expectedGeneration,
                  playerItem != nil,
                  player.currentItem != nil else {
                return
            }
        }
        updateState {
            $0.rate = rate
            switch $0.status {
            case .loading where rate > 0:
                $0.status = .playing
            case .paused where rate > 0:
                $0.status = .playing
            case .playing where rate == 0:
                $0.status = .paused
            default:
                break
            }
        }
        if playerItem != nil, player.currentItem != nil {
            updateNowPlayingInfo()
        }
    }

    private func updateState(_ mutate: (inout PlaybackState) -> Void) {
        mutate(&_state)
        for continuation in stateObservers.values {
            continuation.yield(_state)
        }
    }

#if DEBUG
    /// Test-only hook for setting transport state without loading media.
    ///
    /// playhead-s9mx: injecting a state is a test asserting "the transport
    /// is at this position", so the injected `currentTime` counts as an
    /// observed playhead.
    ///
    /// There is deliberately NO test-only setter for the negative direction.
    /// A test that wants a transport with no playhead must produce one the
    /// way production does — `pauseAndDetachCurrentItem()` or `loadItem` —
    /// because a seam that fakes the flag would let
    /// `PlaybackPositionCommitTests` pass while the real detach stopped
    /// clearing it, which is the entire defect.
    func _testingInjectState(_ state: PlaybackState) {
        _state = state
        isCurrentTimeObservedPlayhead = true
        for continuation in stateObservers.values {
            continuation.yield(_state)
        }
    }

    func _testingApplyReadyToPlayState(duration: TimeInterval) {
        applyReadyToPlayState(duration: duration)
    }

    func _testingApplyObservedRate(_ rate: Float) {
        applyObservedRate(rate)
    }

    func _testingApplyObservedRate(
        _ rate: Float,
        expectedGeneration: UInt64
    ) {
        applyObservedRate(
            rate,
            expectedGeneration: expectedGeneration
        )
    }

    /// Delivers a captured item-status callback payload after a test-controlled
    /// delay. This models a KVO callback that was queued before the item was
    /// replaced but entered the playback actor afterward.
    func _testingDeliverItemStatus(
        _ status: AVPlayerItem.Status,
        duration: TimeInterval,
        for item: AVPlayerItem,
        expectedGeneration: UInt64
    ) {
        applyItemStatusChange(
            status,
            duration: duration,
            failureMessage: nil,
            for: item,
            expectedGeneration: expectedGeneration
        )
    }

    /// Delivers a captured periodic-time callback after replacement.
    func _testingDeliverPeriodicTime(
        _ time: CMTime,
        for item: AVPlayerItem,
        expectedGeneration: UInt64
    ) {
        handlePeriodicTime(
            time,
            for: item,
            expectedGeneration: expectedGeneration
        )
    }

    /// Test-only hook that drives the skip-cue duck/seek/release path
    /// without needing the periodic time observer to hit the cue
    /// naturally. Used by `SkipCueSmoothingTests` (playhead-456) to
    /// measure transition wall-clock latency.
    func _testingPerformSkipTransition(to seconds: TimeInterval) async {
        await performSkipTransition(to: seconds, cueStart: seconds)
    }

    /// Test-only range-aware hook for merged-cue Listen cancellation.
    func _testingPerformSkipTransition(
        cueStart: TimeInterval,
        cueEnd: TimeInterval
    ) async {
        await performSkipTransition(to: cueEnd, cueStart: cueStart)
    }

    /// Test-only split-phase hooks for the detect → Listen → queued-execute
    /// ordering that the production periodic observer can encounter.
    func _testingReserveSkipTransition(
        cueStart: TimeInterval,
        cueEnd: TimeInterval
    ) -> UInt64? {
        reserveSkipTransition(to: cueEnd, cueStart: cueStart)
    }

    func _testingExecuteReservedSkipTransition(
        transitionToken: UInt64
    ) async {
        await performReservedSkipTransition(
            transitionToken: transitionToken
        )
    }

    /// Test-only accessor for the currently-armed skip cue ranges.
    /// Used by `SkipCueSmoothingTests` to assert `setSkipCues` actually
    /// stored the ranges.
    var _testingSkipCues: [CMTimeRange] { skipCues }

    /// Test-only accessor for the player's current volume. Used by
    /// `SkipCueSmoothingTests` (playhead-m9xk) to prove duck/release
    /// ORDERING deterministically: while a transition is parked inside
    /// the injected sleeper, the volume must read `Self.duckVolume`;
    /// after release it must be restored.
    var _testingPlayerVolume: Float { player.volume }

    /// playhead-1mq1.3: set the player's level so a test can choose the volume
    /// the restore ramp has to land back on.
    ///
    /// Needed for two rails the default 1.0 cannot express. The ramp's final
    /// step assigns the CAPTURED value rather than the computed one, and at
    /// 1.0 those agree exactly in Float32 — so a mutant that drops the assign
    /// survives. At 0.8 the computed value is 0.79999995, which is audibly
    /// nothing and arithmetically a listener never getting their level back.
    /// It also lets a test park a sentinel level mid-ramp and prove the ramp
    /// stops writing once it no longer owns the transition.
    func _testingSetPlayerVolume(_ volume: Float) {
        player.volume = volume
    }

    /// Teardown ownership probes. Both references must be cleared before
    /// teardown returns so neither AVPlayer nor the transport can keep the
    /// replaced episode alive.
    var _testingHasPlayerItem: Bool { playerItem != nil }
    var _testingHasCurrentPlayerItem: Bool { player.currentItem != nil }
    var _testingIsTornDown: Bool { isTornDown }

    /// Test-only mirror of the production duck level so ordering tests
    /// compare against the real constant instead of a copied literal.
    /// Actor-isolated like the class; tests read it with `await`.
    static var _testingDuckVolume: Float { duckVolume }

    /// playhead-1mq1.3: how many sleeper calls ONE completed skip transition
    /// makes — the settle dwell, then one per restore-ramp step.
    ///
    /// Exposed because two suites count sleep entries as a proxy for "exactly
    /// one duck cycle ran", and a literal there would have to be re-derived by
    /// hand every time the ramp's shape changes. The count is the arithmetic
    /// the transition actually performs, so those tests keep asserting the
    /// claim they were written for rather than a number that happened to be
    /// true when they were written.
    static var _testingSleepsPerSkipTransition: Int {
        1 + duckRestoreRampSteps
    }

    /// Test-only hook that installs a sentinel `AVPlayerItem` so calls
    /// to `play()` pass the `playerItem != nil` guard. Used by
    /// `playhead-456` E2E tests that need to exercise post-route-change
    /// resume semantics without racing against AVPlayer's asynchronous
    /// asset-load KVO (which can flip the status to `.failed` after a
    /// `_testingInjectState(.playing)` call). The item is a no-op:
    /// no resource loader delegate, no observers attached, and the
    /// itemStatusObservation is not wired up — so no KVO fires and
    /// `_state.status` is not overwritten.
    func _testingInstallStubPlayerItem() {
        isNowPlayingPublicationSuppressed = false
        playerItem = AVPlayerItem(asset: AVURLAsset(
            url: URL(string: "playhead-progressive://stub/sentinel.mp3")!
        ))
    }

    /// Installs the sentinel in both ownership slots without KVO so lifecycle
    /// tests can verify synchronous detachment from AVPlayer itself.
    func _testingInstallStubCurrentPlayerItem() {
        isNowPlayingPublicationSuppressed = false
        let item = AVPlayerItem(asset: AVURLAsset(
            url: URL(string: "playhead-progressive://stub/current.mp3")!
        ))
        playerItem = item
        player.replaceCurrentItem(with: item)
    }

    func _testingSetItemSeekOperation(
        _ operation: (@Sendable (AVPlayerItem, CMTime) async -> Bool)?
    ) {
        itemSeekOperationOverrideForTesting = operation
    }
#endif
}

// MARK: - RemoteCommandHandler

/// Non-isolated handler that registers MPRemoteCommand closures. Because this
/// class has no actor isolation, closures formed in its methods don't inherit
/// @PlaybackServiceActor — avoiding the Swift 6 runtime abort. Uses the
/// closure-based addTarget(handler:) API which retains the closure (unlike
/// target/action which only holds an unretained reference to the target).
final class RemoteCommandHandler {
    private weak var service: PlaybackService?
    private var tokens: [(MPRemoteCommand, Any)] = []

    init(service: PlaybackService) {
        self.service = service
    }

    func register() {
        let center = MPRemoteCommandCenter.shared()

        tokens.append((center.playCommand, center.playCommand.addTarget { [weak self] _ in
            guard let service = self?.service else { return .commandFailed }
            Task { @PlaybackServiceActor in service.play() }
            return .success
        }))

        tokens.append((center.pauseCommand, center.pauseCommand.addTarget { [weak self] _ in
            guard let service = self?.service else { return .commandFailed }
            Task { @PlaybackServiceActor in service.pause() }
            return .success
        }))

        tokens.append((center.togglePlayPauseCommand, center.togglePlayPauseCommand.addTarget { [weak self] _ in
            guard let service = self?.service else { return .commandFailed }
            Task { @PlaybackServiceActor in service.togglePlayPause() }
            return .success
        }))

        center.skipForwardCommand.preferredIntervals = [
            NSNumber(value: PlaybackService.skipForwardSeconds)
        ]
        tokens.append((center.skipForwardCommand, center.skipForwardCommand.addTarget { [weak self] _ in
            guard let service = self?.service else { return .commandFailed }
            Task { @PlaybackServiceActor in await service.skipForward() }
            return .success
        }))

        center.skipBackwardCommand.preferredIntervals = [
            NSNumber(value: PlaybackService.skipBackwardSeconds)
        ]
        tokens.append((center.skipBackwardCommand, center.skipBackwardCommand.addTarget { [weak self] _ in
            guard let service = self?.service else { return .commandFailed }
            Task { @PlaybackServiceActor in await service.skipBackward() }
            return .success
        }))

        tokens.append((center.changePlaybackPositionCommand, center.changePlaybackPositionCommand.addTarget { [weak self] event in
            guard let service = self?.service,
                  let positionEvent = event as? MPChangePlaybackPositionCommandEvent
            else { return .commandFailed }
            let position = positionEvent.positionTime
            Task { @PlaybackServiceActor in await service.seek(to: position) }
            return .success
        }))

        center.changePlaybackRateCommand.supportedPlaybackRates = [
            0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0,
        ]
        tokens.append((center.changePlaybackRateCommand, center.changePlaybackRateCommand.addTarget { [weak self] event in
            guard let service = self?.service,
                  let rateEvent = event as? MPChangePlaybackRateCommandEvent
            else { return .commandFailed }
            let rate = rateEvent.playbackRate
            Task { @PlaybackServiceActor in service.setSpeed(rate) }
            return .success
        }))
    }

    func unregister() {
        for (command, token) in tokens {
            command.removeTarget(token)
        }
        tokens.removeAll()
    }
}

// MARK: - ProgressiveResourceLoader

/// Serves bytes from a local file that is still being written to.
/// AVPlayer calls this delegate because the asset uses a custom URL scheme
/// (`playhead-progressive://`). We declare the full content length upfront
/// so AVPlayer knows the real duration and buffers naturally.
final class ProgressiveResourceLoader: NSObject, AVAssetResourceLoaderDelegate {

    let queue = DispatchQueue(label: "com.playhead.progressive-loader")

    private let fileURL: URL
    private let totalBytes: Int64
    private let contentType: String

    /// Pending requests waiting for more data to arrive on disk.
    private var pendingRequests: [AVAssetResourceLoadingRequest] = []

    /// Timer that checks for new data to fulfill pending requests.
    private var pollTimer: DispatchSourceTimer?

    /// When true, the loader drops all pending requests and ignores new ones.
    /// Set during audio session interruptions (Siri, phone calls) to prevent
    /// ObjC exceptions from calling respond/finishLoading on cancelled requests.
    private var suspended = false

    init(fileURL: URL, totalBytes: Int64, contentType: String) {
        self.fileURL = fileURL
        self.totalBytes = totalBytes
        self.contentType = contentType
        super.init()
        startPolling()
    }

    deinit {
        pollTimer?.cancel()
    }

    // MARK: - Suspend / Resume

    /// Stop serving bytes. Called when audio session is interrupted.
    func suspend() {
        queue.async { [self] in
            suspended = true
            pendingRequests.removeAll()
        }
    }

    /// Resume serving bytes. Called when audio session interruption ends.
    func resume() {
        queue.async { [self] in
            suspended = false
        }
    }

    // MARK: - AVAssetResourceLoaderDelegate

    func resourceLoader(
        _ resourceLoader: AVAssetResourceLoader,
        shouldWaitForLoadingOfRequestedResource loadingRequest: AVAssetResourceLoadingRequest
    ) -> Bool {
        guard !suspended else { return false }

        // Fill content information on first request.
        if let contentInfo = loadingRequest.contentInformationRequest {
            contentInfo.contentType = contentType
            contentInfo.contentLength = totalBytes
            contentInfo.isByteRangeAccessSupported = true
            contentInfo.isEntireLengthAvailableOnDemand = false
        }

        // Try to fulfill the data request immediately.
        if fulfillRequest(loadingRequest) {
            return true
        }

        // Data not yet available — queue it for later.
        pendingRequests.append(loadingRequest)
        return true
    }

    func resourceLoader(
        _ resourceLoader: AVAssetResourceLoader,
        didCancel loadingRequest: AVAssetResourceLoadingRequest
    ) {
        pendingRequests.removeAll { $0 === loadingRequest }
    }

    // MARK: - Request Fulfillment

    @discardableResult
    private func fulfillRequest(_ loadingRequest: AVAssetResourceLoadingRequest) -> Bool {
        guard !loadingRequest.isCancelled, !suspended else { return true }

        guard let dataRequest = loadingRequest.dataRequest else {
            if !loadingRequest.isCancelled { loadingRequest.finishLoading() }
            return true
        }

        let readOffset = dataRequest.currentOffset
        let endOfRequest = dataRequest.requestedOffset + Int64(dataRequest.requestedLength)
        let remaining = endOfRequest - readOffset

        guard remaining > 0 else {
            if !loadingRequest.isCancelled { loadingRequest.finishLoading() }
            return true
        }

        let fileSize: Int64
        do {
            let attrs = try FileManager.default.attributesOfItem(atPath: fileURL.path)
            fileSize = (attrs[.size] as? Int64) ?? 0
        } catch {
            if !loadingRequest.isCancelled { loadingRequest.finishLoading(with: error) }
            return true
        }

        if readOffset >= fileSize {
            return false
        }

        let availableEnd = min(readOffset + remaining, fileSize)
        let bytesToRead = Int(availableEnd - readOffset)

        guard bytesToRead > 0 else { return false }

        do {
            let handle = try FileHandle(forReadingFrom: fileURL)
            defer { try? handle.close() }
            try handle.seek(toOffset: UInt64(readOffset))
            let data = handle.readData(ofLength: bytesToRead)
            guard !loadingRequest.isCancelled, !suspended else { return true }
            dataRequest.respond(with: data)
        } catch {
            if !loadingRequest.isCancelled { loadingRequest.finishLoading(with: error) }
            return true
        }

        if dataRequest.currentOffset >= endOfRequest {
            if !loadingRequest.isCancelled { loadingRequest.finishLoading() }
            return true
        }

        return false
    }

    // MARK: - Polling

    private func startPolling() {
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + 0.1, repeating: 0.1)
        timer.setEventHandler { [weak self] in
            self?.processPendingRequests()
        }
        timer.resume()
        pollTimer = timer
    }

    private func processPendingRequests() {
        guard !suspended else {
            pendingRequests.removeAll()
            return
        }
        pendingRequests.removeAll { request in
            if request.isCancelled { return true }
            return fulfillRequest(request)
        }
    }
}
