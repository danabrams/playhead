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
    /// Duration of duck ramp in seconds.
    private static let duckDuration: TimeInterval = 0.15

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

    // MARK: - State

    private var _state = PlaybackState()
    private var skipCues: [CMTimeRange] = []
    private var isLocalAsset: Bool = false
    private var isHandlingSkip: Bool = false

    // MARK: - Streams

    /// Active state observers. Each observer receives the current snapshot
    /// immediately on subscription, then all subsequent updates.
    private var stateObservers: [UUID: AsyncStream<PlaybackState>.Continuation] = [:]

    // MARK: - Observation

    private var rateObservation: NSKeyValueObservation?
    private var itemStatusObservation: NSKeyValueObservation?

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
    nonisolated init(
        audioSession: AudioSessionProviding,
        nowPlayingInfo: NowPlayingInfoProviding,
        notificationCenter: NotificationCenter
    ) {
        let player = AVPlayer()
        player.automaticallyWaitsToMinimizeStalling = true
        self.player = player
        self.audioSession = audioSession
        self.nowPlayingInfo = nowPlayingInfo
        self.notificationCenter = notificationCenter

        super.init()

        Task { @PlaybackServiceActor [self] in
            self.configureAudioSession()
            self.configureRemoteCommands()
            self.startPeriodicTimeObserver()
            self.observePlayerRate()
            // Interruptions and route changes use async notification
            // sequences so they run entirely on PlaybackServiceActor.
            // Combine's .sink runs on the notification's posting thread
            // (main queue), which triggers Swift 6 actor isolation
            // assertions when accessing actor-isolated self.
            self.observeInterruptionsAsync()
            self.observeRouteChangesAsync()
            self.observePlayerItemFinishAsync()
        }
    }

    /// Tear down observers and streams. Call before releasing the service.
    func tearDown() {
        if let token = timeObserverToken {
            player.removeTimeObserver(token)
            timeObserverToken = nil
        }
        rateObservation?.invalidate()
        itemStatusObservation?.invalidate()
        removeRemoteCommandTargets()
        for continuation in stateObservers.values {
            continuation.finish()
        }
        stateObservers.removeAll()
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
        isLocalAsset = true
        loadPlayerItem(item)

        if startPosition > 0 {
            let target = CMTime(seconds: startPosition, preferredTimescale: 600)
            await player.currentItem?.seek(to: target, toleranceBefore: .zero, toleranceAfter: .zero)
        }
    }

    private func loadPlayerItem(_ item: AVPlayerItem) {
        playerItem = item

        itemStatusObservation?.invalidate()
        let block = makeItemStatusBlock()
        itemStatusObservation = item.observe(\.status, options: [.new], changeHandler: block)

        updateState { $0.status = .loading }
        player.replaceCurrentItem(with: item)
    }

    /// Non-isolated so the closure avoids actor-executor crashes at call site.
    private nonisolated func makeItemStatusBlock()
        -> @Sendable (AVPlayerItem, NSKeyValueObservedChange<AVPlayerItem.Status>) -> Void
    {
        { [weak self] item, _ in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.handleItemStatusChange(item)
            }
        }
    }

    private func handleItemStatusChange(_ item: AVPlayerItem) {
        switch item.status {
        case .readyToPlay:
            let duration = CMTimeGetSeconds(item.duration)
            applyReadyToPlayState(duration: duration)
        case .failed:
            let msg = item.error?.localizedDescription ?? "unknown"
            updateState { $0.status = .failed(msg) }
        case .unknown:
            break
        @unknown default:
            break
        }
    }

    // MARK: - Transport Controls

    func play() {
        guard playerItem != nil else { return }
        player.playImmediately(atRate: _state.playbackSpeed)
        updateState { $0.status = .playing }
        updateNowPlayingInfo()
    }

    func pause() {
        player.pause()
        updateState { $0.status = .paused }
        updateNowPlayingInfo()
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

    /// Subscribe to playback state with immediate hydration.
    /// This avoids remount bugs where a late subscriber sees defaults until the
    /// next transport event arrives.
    func observeStates() -> AsyncStream<PlaybackState> {
        AsyncStream { continuation in
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
    func seek(to seconds: TimeInterval) async {
        let target = CMTime(seconds: seconds, preferredTimescale: 600)
        await player.currentItem?.seek(
            to: target, toleranceBefore: .zero, toleranceAfter: .zero
        )
        updateState { $0.currentTime = seconds }
        updateNowPlayingInfo()
    }

    /// Skip forward by the given number of seconds (default 30).
    func skipForward(_ seconds: TimeInterval = PlaybackService.skipForwardSeconds) async {
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
    func setSpeed(_ speed: Float) {
        let clamped = min(max(speed, Self.minSpeed), Self.maxSpeed)
        _state.playbackSpeed = clamped
        if case .playing = _state.status {
            player.rate = clamped
        }
        updateState { $0.playbackSpeed = clamped }
        updateNowPlayingInfo()
    }

    // MARK: - Skip Cues

    /// Accept skip cue ranges from SkipOrchestrator.
    /// When playback enters a cue range, the service performs a smooth skip.
    func setSkipCues(_ cues: [CMTimeRange]) {
        skipCues = cues
    }

    /// Check if current time has entered a skip cue and handle it.
    private func checkSkipCues(currentTime: CMTime) {
        guard !isHandlingSkip, !skipCues.isEmpty else { return }

        let currentSeconds = CMTimeGetSeconds(currentTime)
        for cue in skipCues {
            let start = CMTimeGetSeconds(cue.start)
            let end = CMTimeGetSeconds(CMTimeRangeGetEnd(cue))
            if currentSeconds >= start, currentSeconds < end {
                Task { @PlaybackServiceActor in
                    await self.performSkipTransition(to: end)
                }
                return
            }
        }
    }

    /// Perform a perceptually clean skip transition: duck volume, seek, release.
    private func performSkipTransition(to targetSeconds: TimeInterval) async {
        guard !isHandlingSkip else { return }
        isHandlingSkip = true
        defer { isHandlingSkip = false }

        await duckSeekRelease(to: targetSeconds)
    }

    /// Duck volume, seek precisely, then restore volume.
    private func duckSeekRelease(to seconds: TimeInterval) async {
        let originalVolume = player.volume

        // Duck
        player.volume = Self.duckVolume

        // Seek
        let target = CMTime(seconds: seconds, preferredTimescale: 600)
        await player.currentItem?.seek(
            to: target, toleranceBefore: .zero, toleranceAfter: .zero
        )

        // Brief pause for the seek to settle, then release.
        try? await Task.sleep(for: .milliseconds(Int(Self.duckDuration * 1000)))

        // Restore volume
        player.volume = originalVolume

        updateState { $0.currentTime = seconds }
        updateNowPlayingInfo()
    }

    // MARK: - Time Observer

    private func startPeriodicTimeObserver() {
        let interval = CMTime(seconds: 0.25, preferredTimescale: 600)
        let block = makeTimeObserverBlock()
        timeObserverToken = player.addPeriodicTimeObserver(
            forInterval: interval, queue: nil, using: block
        )
    }

    /// Non-isolated so the closure avoids actor-executor crashes at call site.
    private nonisolated func makeTimeObserverBlock() -> @Sendable (CMTime) -> Void {
        { [weak self] time in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                let seconds = CMTimeGetSeconds(time)
                guard seconds.isFinite else { return }
                self.updateState { $0.currentTime = seconds }
                self.checkSkipCues(currentTime: time)
            }
        }
    }

    // MARK: - Rate Observation

    private func observePlayerRate() {
        let block = makeRateObserverBlock()
        rateObservation = player.observe(\.rate, options: [.new], changeHandler: block)
    }

    /// Non-isolated so the closure avoids actor-executor crashes at call site.
    private nonisolated func makeRateObserverBlock()
        -> @Sendable (AVPlayer, NSKeyValueObservedChange<Float>) -> Void
    {
        { [weak self] player, _ in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.applyObservedRate(player.rate)
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
        Task { @PlaybackServiceActor [weak self] in
            let notifications = center.notifications(
                named: AVAudioSession.interruptionNotification
            )
            for await notification in notifications {
                guard let self else { break }
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

    // MARK: - Route Changes (Async)

    private func observeRouteChangesAsync() {
        let center = notificationCenter
        Task { @PlaybackServiceActor [weak self] in
            let notifications = center.notifications(
                named: AVAudioSession.routeChangeNotification
            )
            for await notification in notifications {
                guard let self else { break }
                guard let info = notification.userInfo,
                      let reasonValue = info[AVAudioSessionRouteChangeReasonKey] as? UInt,
                      let reason = AVAudioSession.RouteChangeReason(rawValue: reasonValue)
                else { continue }

                switch reason {
                case .oldDeviceUnavailable:
                    self.pause()
                default:
                    break
                }
            }
        }
    }

    // MARK: - Episode Finish (Async)

    /// Re-broadcast `AVPlayerItem.didPlayToEndTimeNotification` as
    /// `Notification.Name.playbackDidFinishEpisode` on the injected
    /// notification center. The playback queue's auto-advancer
    /// subscribes to the re-broadcast (playhead-05i) — re-broadcasting
    /// here keeps the queue layer ignorant of AVFoundation's specific
    /// notification name, and ensures tests that inject a private
    /// `NotificationCenter` see the finish event without having to
    /// fight `.default`.
    ///
    /// Why a re-broadcast rather than letting the queue subscribe
    /// directly to `AVPlayerItem.didPlayToEndTimeNotification`: AVFoundation
    /// posts on `.default`, and the runtime / tests use a per-instance
    /// notification center for isolation. The PlaybackService is the only
    /// type that owns the player item and therefore the right place to
    /// fan out the event onto the configured center.
    private func observePlayerItemFinishAsync() {
        let center = notificationCenter
        Task { @PlaybackServiceActor [weak self] in
            // Subscribe on the injected center. AVFoundation posts on
            // `.default` in production; that is exactly the center
            // PlayheadRuntime injects, so the observer fires for real
            // playback. Tests post a synthetic notification on their
            // private center to drive the same path.
            let notifications = center.notifications(
                named: AVPlayerItem.didPlayToEndTimeNotification
            )
            for await _ in notifications {
                guard self != nil else { break }
                center.post(
                    name: .playbackDidFinishEpisode,
                    object: nil
                )
            }
        }
    }

    // MARK: - Now Playing Info Center

    private func updateNowPlayingInfo() {
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

    private func applyObservedRate(_ rate: Float) {
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
        updateNowPlayingInfo()
    }

    private func updateState(_ mutate: (inout PlaybackState) -> Void) {
        mutate(&_state)
        for continuation in stateObservers.values {
            continuation.yield(_state)
        }
    }

#if DEBUG
    /// Test-only hook for setting transport state without loading media.
    func _testingInjectState(_ state: PlaybackState) {
        _state = state
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

    /// Test-only hook that drives the skip-cue duck/seek/release path
    /// without needing the periodic time observer to hit the cue
    /// naturally. Used by `SkipCueSmoothingTests` (playhead-456) to
    /// measure transition wall-clock latency.
    func _testingPerformSkipTransition(to seconds: TimeInterval) async {
        await performSkipTransition(to: seconds)
    }

    /// Test-only accessor for the currently-armed skip cue ranges.
    /// Used by `SkipCueSmoothingTests` to assert `setSkipCues` actually
    /// stored the ranges.
    var _testingSkipCues: [CMTimeRange] { skipCues }

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
        playerItem = AVPlayerItem(asset: AVURLAsset(
            url: URL(string: "playhead-progressive://stub/sentinel.mp3")!
        ))
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
