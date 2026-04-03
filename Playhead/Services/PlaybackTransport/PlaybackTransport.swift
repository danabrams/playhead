// PlaybackTransport.swift
// AVPlayer-based podcast playback engine with seek, rate, skip, and ad-skip
// smoothing. Wrapped in a global actor to serialize all transport operations.
//
// Separate from AnalysisAudioService — different queue, different purpose.

@preconcurrency import AVFoundation
import Combine
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

    // MARK: - Progressive Loader

    /// Retains the resource loader delegate for progressive playback.
    private var progressiveLoader: ProgressiveResourceLoader?

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

    private var cancellables = Set<AnyCancellable>()
    private var statusObservation: NSKeyValueObservation?
    private var rateObservation: NSKeyValueObservation?
    private var itemStatusObservation: NSKeyValueObservation?

    /// Opaque targets returned by MPRemoteCommandCenter.addTarget so we can remove them.
    private var remoteCommandTargets: [(MPRemoteCommand, Any)] = []

    // MARK: - Init

    nonisolated override init() {
        let player = AVPlayer()
        player.automaticallyWaitsToMinimizeStalling = true
        self.player = player

        super.init()

        Task { @PlaybackServiceActor [self] in
            self.configureAudioSession()
            self.configureRemoteCommands()
            self.startPeriodicTimeObserver()
            self.observePlayerRate()
            self.observeInterruptions()
            self.observeRouteChanges()
        }
    }

    /// Tear down observers and streams. Call before releasing the service.
    func tearDown() {
        if let token = timeObserverToken {
            player.removeTimeObserver(token)
            timeObserverToken = nil
        }
        statusObservation?.invalidate()
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
            let session = AVAudioSession.sharedInstance()
            try session.setCategory(.playback, mode: .spokenAudio, policy: .longFormAudio)
            try session.setActive(true)
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
        // Tear down any progressive loader from a prior session.
        progressiveLoader = nil

        // Determine if this is a local file.
        isLocalAsset = url.isFileURL

        let asset = AVURLAsset(url: url)
        let item = AVPlayerItem(asset: asset)
        playerItem = item

        // Observe item status.
        itemStatusObservation?.invalidate()
        itemStatusObservation = item.observe(\.status, options: [.new]) {
            [weak self] item, _ in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.handleItemStatusChange(item)
            }
        }

        updateState { $0.status = .loading }
        player.replaceCurrentItem(with: item)

        // Wait for the item to become ready, then seek to start position.
        if startPosition > 0 {
            let target = CMTime(seconds: startPosition, preferredTimescale: 600)
            await player.currentItem?.seek(to: target, toleranceBefore: .zero, toleranceAfter: .zero)
        }
    }

    /// Load a podcast episode for progressive playback from a file that is
    /// still being downloaded. Uses AVAssetResourceLoaderDelegate with a
    /// custom URL scheme so AVPlayer sees the full content length upfront
    /// and buffers naturally as bytes arrive on disk.
    ///
    /// - Parameters:
    ///   - fileURL: Local file URL being written to by the download manager.
    ///   - totalBytes: Expected total file size from HTTP Content-Length.
    ///   - contentType: UTI for the audio format (e.g. "public.mp3").
    ///   - startPosition: Resume position in seconds (0 for start).
    func loadProgressive(
        fileURL: URL,
        totalBytes: Int64,
        contentType: String,
        startPosition: TimeInterval = 0
    ) async {
        isLocalAsset = true

        // Create the progressive resource loader that serves bytes from
        // the growing file.
        let loader = ProgressiveResourceLoader(
            fileURL: fileURL,
            totalBytes: totalBytes,
            contentType: contentType
        )
        progressiveLoader = loader

        // Use a custom scheme so AVPlayer invokes our resource loader delegate
        // instead of reading the file directly.
        var components = URLComponents()
        components.scheme = "playhead-progressive"
        components.host = "audio"
        components.path = "/\(fileURL.lastPathComponent)"
        guard let proxyURL = components.url else {
            updateState { $0.status = .failed("Failed to create progressive URL") }
            return
        }

        let asset = AVURLAsset(url: proxyURL)
        asset.resourceLoader.setDelegate(loader, queue: loader.queue)

        let item = AVPlayerItem(asset: asset)
        playerItem = item

        itemStatusObservation?.invalidate()
        itemStatusObservation = item.observe(\.status, options: [.new]) {
            [weak self] item, _ in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.handleItemStatusChange(item)
            }
        }

        updateState { $0.status = .loading }
        player.replaceCurrentItem(with: item)

        if startPosition > 0 {
            let target = CMTime(seconds: startPosition, preferredTimescale: 600)
            await player.currentItem?.seek(to: target, toleranceBefore: .zero, toleranceAfter: .zero)
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

    /// Perform a perceptually clean skip transition.
    ///
    /// - Streamed audio: duck volume, seek, release.
    /// - Local audio: optional micro-crossfade (two-item queue).
    private func performSkipTransition(to targetSeconds: TimeInterval) async {
        guard !isHandlingSkip else { return }
        isHandlingSkip = true
        defer { isHandlingSkip = false }

        if isLocalAsset {
            // Micro-crossfade for local: brief duck, seek, restore.
            // True two-item crossfade is only reliable for local files, but
            // a duck/seek/release is more predictable and still clean.
            await duckSeekRelease(to: targetSeconds)
        } else {
            // Streamed audio: duck → seek → release. Never attempt crossfade.
            await duckSeekRelease(to: targetSeconds)
        }
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
        timeObserverToken = player.addPeriodicTimeObserver(
            forInterval: interval, queue: nil
        ) { [weak self] time in
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
        rateObservation = player.observe(\.rate, options: [.new]) {
            [weak self] player, _ in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.applyObservedRate(player.rate)
            }
        }
    }

    // MARK: - Interruptions

    private func observeInterruptions() {
        NotificationCenter.default.publisher(
            for: AVAudioSession.interruptionNotification
        )
        .sink { [weak self] notification in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.handleInterruption(notification)
            }
        }
        .store(in: &cancellables)
    }

    private func handleInterruption(_ notification: Notification) {
        guard let info = notification.userInfo,
              let typeValue = info[AVAudioSessionInterruptionTypeKey] as? UInt,
              let type = AVAudioSession.InterruptionType(rawValue: typeValue)
        else { return }

        switch type {
        case .began:
            pause()
        case .ended:
            if let optionsValue = info[AVAudioSessionInterruptionOptionKey] as? UInt {
                let options = AVAudioSession.InterruptionOptions(rawValue: optionsValue)
                if options.contains(.shouldResume) {
                    play()
                }
            }
        @unknown default:
            break
        }
    }

    // MARK: - Route Changes

    private func observeRouteChanges() {
        NotificationCenter.default.publisher(
            for: AVAudioSession.routeChangeNotification
        )
        .sink { [weak self] notification in
            guard let self else { return }
            Task { @PlaybackServiceActor in
                self.handleRouteChange(notification)
            }
        }
        .store(in: &cancellables)
    }

    private func handleRouteChange(_ notification: Notification) {
        guard let info = notification.userInfo,
              let reasonValue = info[AVAudioSessionRouteChangeReasonKey] as? UInt,
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
        var info = [String: Any]()
        info[MPMediaItemPropertyPlaybackDuration] = _state.duration
        info[MPNowPlayingInfoPropertyElapsedPlaybackTime] = _state.currentTime
        info[MPNowPlayingInfoPropertyPlaybackRate] = _state.rate
        info[MPNowPlayingInfoPropertyDefaultPlaybackRate] = _state.playbackSpeed
        MPNowPlayingInfoCenter.default().nowPlayingInfo = info
    }

    /// Update Now Playing with episode metadata (title, artwork, etc.).
    func setNowPlayingMetadata(
        title: String,
        artist: String? = nil,
        albumTitle: String? = nil,
        artworkImage: UIImage? = nil
    ) {
        var info = MPNowPlayingInfoCenter.default().nowPlayingInfo ?? [:]
        info[MPMediaItemPropertyTitle] = title
        if let artist { info[MPMediaItemPropertyArtist] = artist }
        if let albumTitle { info[MPMediaItemPropertyAlbumTitle] = albumTitle }
        if let image = artworkImage {
            let artwork = MPMediaItemArtwork(boundsSize: image.size) { _ in image }
            info[MPMediaItemPropertyArtwork] = artwork
        }
        info[MPMediaItemPropertyPlaybackDuration] = _state.duration
        info[MPNowPlayingInfoPropertyElapsedPlaybackTime] = _state.currentTime
        info[MPNowPlayingInfoPropertyPlaybackRate] = _state.rate
        MPNowPlayingInfoCenter.default().nowPlayingInfo = info
    }

    // MARK: - Remote Commands

    private func configureRemoteCommands() {
        let center = MPRemoteCommandCenter.shared()

        let playTarget = center.playCommand.addTarget { [weak self] _ in
            guard let self else { return .commandFailed }
            Task { @PlaybackServiceActor in self.play() }
            return .success
        }
        remoteCommandTargets.append((center.playCommand, playTarget))

        let pauseTarget = center.pauseCommand.addTarget { [weak self] _ in
            guard let self else { return .commandFailed }
            Task { @PlaybackServiceActor in self.pause() }
            return .success
        }
        remoteCommandTargets.append((center.pauseCommand, pauseTarget))

        let toggleTarget = center.togglePlayPauseCommand.addTarget { [weak self] _ in
            guard let self else { return .commandFailed }
            Task { @PlaybackServiceActor in
                if case .playing = self._state.status {
                    self.pause()
                } else {
                    self.play()
                }
            }
            return .success
        }
        remoteCommandTargets.append((center.togglePlayPauseCommand, toggleTarget))

        center.skipForwardCommand.preferredIntervals = [
            NSNumber(value: Self.skipForwardSeconds)
        ]
        let skipFwdTarget = center.skipForwardCommand.addTarget { [weak self] _ in
            guard let self else { return .commandFailed }
            Task { @PlaybackServiceActor in await self.skipForward() }
            return .success
        }
        remoteCommandTargets.append((center.skipForwardCommand, skipFwdTarget))

        center.skipBackwardCommand.preferredIntervals = [
            NSNumber(value: Self.skipBackwardSeconds)
        ]
        let skipBwdTarget = center.skipBackwardCommand.addTarget { [weak self] _ in
            guard let self else { return .commandFailed }
            Task { @PlaybackServiceActor in await self.skipBackward() }
            return .success
        }
        remoteCommandTargets.append((center.skipBackwardCommand, skipBwdTarget))

        let positionTarget = center.changePlaybackPositionCommand.addTarget { [weak self] event in
            guard let self,
                  let positionEvent = event as? MPChangePlaybackPositionCommandEvent
            else { return .commandFailed }
            Task { @PlaybackServiceActor in
                await self.seek(to: positionEvent.positionTime)
            }
            return .success
        }
        remoteCommandTargets.append((center.changePlaybackPositionCommand, positionTarget))

        center.changePlaybackRateCommand.supportedPlaybackRates = [
            0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0,
        ]
        let rateTarget = center.changePlaybackRateCommand.addTarget { [weak self] event in
            guard let self,
                  let rateEvent = event as? MPChangePlaybackRateCommandEvent
            else { return .commandFailed }
            Task { @PlaybackServiceActor in
                self.setSpeed(rateEvent.playbackRate)
            }
            return .success
        }
        remoteCommandTargets.append((center.changePlaybackRateCommand, rateTarget))
    }

    /// Remove all registered remote command targets to prevent leaks.
    private func removeRemoteCommandTargets() {
        for (command, target) in remoteCommandTargets {
            command.removeTarget(target)
        }
        remoteCommandTargets.removeAll()
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
#endif
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

    // MARK: - AVAssetResourceLoaderDelegate

    func resourceLoader(
        _ resourceLoader: AVAssetResourceLoader,
        shouldWaitForLoadingOfRequestedResource loadingRequest: AVAssetResourceLoadingRequest
    ) -> Bool {
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

    /// Attempts to respond to the data request from bytes currently on disk.
    /// Returns true if the request is fully served, false if more data is needed.
    @discardableResult
    private func fulfillRequest(_ loadingRequest: AVAssetResourceLoadingRequest) -> Bool {
        // Guard against cancelled requests — calling respond/finishLoading
        // on a cancelled request raises an Objective-C exception that crashes
        // the app. This happens during Siri or other audio session interruptions
        // when AVPlayer cancels all in-flight resource loading.
        guard !loadingRequest.isCancelled else { return true }

        guard let dataRequest = loadingRequest.dataRequest else {
            loadingRequest.finishLoading()
            return true
        }

        // Use currentOffset — this advances after each respond(with:) call.
        // requestedOffset is the *initial* offset and doesn't move.
        let readOffset = dataRequest.currentOffset
        let endOfRequest = dataRequest.requestedOffset + Int64(dataRequest.requestedLength)
        let remaining = endOfRequest - readOffset

        guard remaining > 0 else {
            loadingRequest.finishLoading()
            return true
        }

        // How many bytes are on disk right now?
        let fileSize: Int64
        do {
            let attrs = try FileManager.default.attributesOfItem(atPath: fileURL.path)
            fileSize = (attrs[.size] as? Int64) ?? 0
        } catch {
            if !loadingRequest.isCancelled {
                loadingRequest.finishLoading(with: error)
            }
            return true
        }

        // If we don't have any bytes at the read position yet, wait.
        if readOffset >= fileSize {
            return false
        }

        // Read available bytes from the current position forward.
        let availableEnd = min(readOffset + remaining, fileSize)
        let bytesToRead = Int(availableEnd - readOffset)

        guard bytesToRead > 0 else { return false }

        do {
            let handle = try FileHandle(forReadingFrom: fileURL)
            defer { try? handle.close() }
            try handle.seek(toOffset: UInt64(readOffset))
            let data = handle.readData(ofLength: bytesToRead)
            guard !loadingRequest.isCancelled else { return true }
            dataRequest.respond(with: data)
        } catch {
            if !loadingRequest.isCancelled {
                loadingRequest.finishLoading(with: error)
            }
            return true
        }

        // Check if we've now served everything.
        if dataRequest.currentOffset >= endOfRequest {
            if !loadingRequest.isCancelled {
                loadingRequest.finishLoading()
            }
            return true
        }

        // Partial serve — still waiting for more bytes.
        return false
    }

    // MARK: - Polling

    /// Periodically checks if pending requests can now be fulfilled.
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
        pendingRequests.removeAll { request in
            if request.isCancelled { return true }
            return fulfillRequest(request)
        }
    }
}
