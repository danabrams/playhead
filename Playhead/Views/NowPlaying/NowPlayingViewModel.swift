// NowPlayingViewModel.swift
// Bridges PlaybackService state to SwiftUI. Runs observation on MainActor
// so view updates are always on the right thread.

import Foundation
import SwiftUI

@MainActor
@Observable
final class NowPlayingViewModel {

    // MARK: - State

    var episodeTitle: String = "No Episode Selected"
    var podcastTitle: String = ""
    var artworkURL: URL?
    var isPlaying: Bool = false
    var currentTime: TimeInterval = 0
    var duration: TimeInterval = 0
    var playbackSpeed: Float = 1.0

    /// Ad segments from SkipOrchestrator, expressed as fractional ranges (0...1)
    /// of the total episode duration. Updated in real-time as detection produces results.
    var adSegmentRanges: [ClosedRange<Double>] = []

    var activeSkipMode: SkipMode = .shadow

    /// playhead-djl0: WHY `activeSkipMode` holds its value. Carried alongside
    /// the mode because `.shadow` alone cannot tell the listener whether their
    /// show is being observed on purpose or was never recognised.
    var skipModeResolution: SkipModeResolution = .noActiveEpisode

    /// Debounce guard for the "Hearing an ad" button — prevents duplicate reports
    /// within a 5-second window from rapid taps.
    private var lastHearingAdReportTime: Date?

    // MARK: - Derived

    var progress: Double {
        guard duration > 0 else { return 0 }
        return currentTime / duration
    }

    var elapsedFormatted: String {
        TimeFormatter.formatTime(currentTime)
    }

    var remainingFormatted: String {
        let remaining = max(duration - currentTime, 0)
        return "-\(TimeFormatter.formatTime(remaining))"
    }

    // MARK: - Dependencies

    private let runtime: PlayheadRuntime
    private var observationTask: Task<Void, Never>?
    private var segmentObservationTask: Task<Void, Never>?
    private var bannerObservationTask: Task<Void, Never>?
    private var skipModeObservationTask: Task<Void, Never>?

    init(runtime: PlayheadRuntime) {
        self.runtime = runtime
        syncMetadata()
    }

    // MARK: - Lifecycle

    func startObserving() {
        guard observationTask == nil else { return }
        syncMetadata()
        let service = runtime.playbackService
        observationTask = Task {
            let stream = await service.observeStates()
            for await state in stream {
                guard !Task.isCancelled else { return }
                await MainActor.run {
                    self.applyState(state)
                }
            }
        }
    }

    func stopObserving() {
        observationTask?.cancel()
        observationTask = nil
        stopObservingAdSegments()
        stopObservingBanners()
        stopObservingSkipMode()
    }

    func stopObservingSkipMode() {
        skipModeObservationTask?.cancel()
        skipModeObservationTask = nil
    }

    func stopObservingAdSegments() {
        segmentObservationTask?.cancel()
        segmentObservationTask = nil
    }

    func stopObservingBanners() {
        bannerObservationTask?.cancel()
        bannerObservationTask = nil
    }

    /// Begin observing ad segment updates from a SkipOrchestrator.
    /// Segments are converted to fractional ranges of the current duration.
    func observeAdSegments(from orchestrator: SkipOrchestrator) {
        segmentObservationTask?.cancel()
        segmentObservationTask = Task {
            let stream = await orchestrator.appliedSegmentsStream()
            for await segments in stream {
                guard !Task.isCancelled else { return }
                let dur = await MainActor.run { self.duration }
                guard dur > 0 else {
                    await MainActor.run {
                        self.adSegmentRanges = []
                    }
                    continue
                }
                let ranges: [ClosedRange<Double>] = segments.compactMap { seg in
                    let lower = seg.start / dur
                    let upper = seg.end / dur
                    guard lower < upper, lower >= 0, upper <= 1.0 else { return nil }
                    return min(max(lower, 0), 1)...min(max(upper, 0), 1)
                }
                await MainActor.run {
                    self.adSegmentRanges = ranges
                }
            }
        }
    }

    /// Begin observing banner items from a SkipOrchestrator.
    /// Each item is enqueued into the provided AdBannerQueue on the MainActor.
    ///
    /// playhead-8cjo: the per-event rule lives in `BannerHostDelivery`, which
    /// took nothing from this view model but the three values below. It is out
    /// here because it is the only place that knows what the QUEUE did with an
    /// item, and neither an orchestrator test nor a queue test can see that
    /// hop — so the rule has to be drivable on its own against a real queue.
    /// Do not re-inline it: a second copy is how the auto tier came to have no
    /// acknowledgement while the suggest tier had one.
    func observeBanners(
        from orchestrator: SkipOrchestrator,
        into queue: AdBannerQueue,
        hostGeneration: UInt64
    ) {
        bannerObservationTask?.cancel()
        bannerObservationTask = Task {
            let stream = await orchestrator.bannerEventStream()
            for await event in stream {
                guard !Task.isCancelled else { return }
                await BannerHostDelivery.forward(
                    event,
                    from: orchestrator,
                    into: queue,
                    hostGeneration: hostGeneration
                )
            }
        }
    }

    func loadSkipMode(from orchestrator: SkipOrchestrator) async {
        activeSkipMode = await orchestrator.currentSkipMode()
        skipModeResolution = await orchestrator.currentSkipModeResolution()
    }

    /// playhead-usn1: track the skip mode and its cause for as long as this
    /// screen is mounted.
    ///
    /// `loadSkipMode` is a single read, and the Now Playing screen is presented
    /// SYNCHRONOUSLY with the tap that starts playback
    /// (`EpisodeListView.playEpisode` sets `navigateToNowPlaying = true` in the
    /// same turn it spawns `runtime.playEpisode`). `SkipOrchestrator
    /// .beginEpisode` — the only thing that resolves the show — runs many
    /// suspensions later, after transport load, `play()`, and analysis-asset
    /// resolution. So the one read reliably observed the value `endEpisode` had
    /// just installed: `.shadow` / `.noActiveEpisode`, i.e. "Show Unknown" with
    /// the per-show menu withheld, for shows whose identity resolves perfectly.
    ///
    /// The podcast TITLE never had this problem because `syncMetadata` re-reads
    /// it on every playback state event. The mode is now on a push cadence,
    /// which is strictly better than matching that one.
    func observeSkipMode(from orchestrator: SkipOrchestrator) {
        skipModeObservationTask?.cancel()
        skipModeObservationTask = Task {
            let stream = await orchestrator.skipModeStream()
            for await snapshot in stream {
                guard !Task.isCancelled else { return }
                await MainActor.run {
                    self.activeSkipMode = snapshot.mode
                    self.skipModeResolution = snapshot.resolution
                }
            }
        }
    }

    func setSkipMode(_ mode: SkipMode, orchestrator: SkipOrchestrator) {
        let previousMode = activeSkipMode
        let previousResolution = skipModeResolution
        noteSkipModeSelection(mode)
        Task {
            let outcome = await runtime.setShowSkipMode(mode, orchestrator: orchestrator)
            // playhead-usn1: the optimistic `.sessionOverride` above is a claim
            // that the choice was taken. When the write is refused for want of a
            // show, that claim is false and must be withdrawn — otherwise the
            // pill reports the listener's choice back to them while nothing
            // anywhere has stored it, which is the defect this bead closes,
            // relocated to the surface.
            if outcome == .refusedNoShowIdentity {
                self.activeSkipMode = previousMode
                self.skipModeResolution = previousResolution
            }
        }
    }

    /// playhead-djl0: the synchronous half of `setSkipMode`, split out so the
    /// state transition is reachable without spawning the runtime write. The
    /// listener's own choice is not the orchestrator's lookup failure, so the
    /// resolution moves to `.sessionOverride` the moment they answer.
    func noteSkipModeSelection(_ mode: SkipMode) {
        activeSkipMode = mode
        skipModeResolution = .sessionOverride
    }

    // MARK: - Actions

    func togglePlayPause() {
        let playing = isPlaying
        Task {
            await runtime.togglePlayPause(isPlaying: playing)
        }
    }

    func skipForward() {
        Task {
            _ = await runtime.skipForward()
        }
    }

    func skipBackward() {
        Task {
            _ = await runtime.skipBackward()
        }
    }

    func seek(to seconds: TimeInterval) {
        Task {
            _ = await runtime.seek(to: seconds)
        }
    }

    /// Handle the "Listen" tap on an ad skip banner.
    ///
    /// 1. Rewind to the snapped start boundary of the skipped ad window.
    /// 2. Set decisionState to .reverted so auto-skip ignores this span.
    /// 3. Feed a false-positive signal to the PodcastProfile trust scoring.
    func handleListenRewind(item: AdSkipBannerItem) {
        Task {
            _ = await handleListenRewindAwaitingAction(item: item)
        }
    }

    @discardableResult
    func handleListenRewindAwaitingAction(
        item: AdSkipBannerItem
    ) async -> Bool {
        let expectedEpisodeId = item.episodeId
        guard let expectedPlaybackGeneration =
            item.playbackLifecycleGeneration,
              let expectedMaterialRevisionToken =
                item.windowMaterialRevisionToken
        else {
            return false
        }
        guard runtime.currentEpisodeId == expectedEpisodeId,
              runtime.playEpisodeGeneration
                == expectedPlaybackGeneration
        else {
            return false
        }
        return await runtime.listenRewind(
            windowId: item.windowId,
            podcastId: item.podcastId,
            analysisAssetId: item.analysisAssetId,
            to: item.adStartTime,
            bannerEndTime: item.adEndTime,
            ifCurrentEpisodeId: expectedEpisodeId,
            ifPlaybackLifecycleGeneration:
                expectedPlaybackGeneration,
            ifWindowMaterialRevisionToken:
                expectedMaterialRevisionToken
        )
    }

    func setSpeed(_ speed: Float) {
        Task {
            await runtime.setSpeed(speed)
        }
    }

    /// Record a false negative correction — the user hears an ad that wasn't detected.
    /// Captures the current playback position as the correction timestamp.
    ///
    /// playhead-98q: now also expands the seed position into a plausible ad segment
    /// boundary using BoundaryExpander, then injects the region into the skip
    /// orchestrator for immediate skip + UI update + persistence.
    func reportHearingAd() {
        guard let assetId = runtime.currentAnalysisAssetId else { return }
        let episodeId = runtime.currentEpisodeId
        let podcastId = runtime.currentPodcastId
        let playbackGeneration = runtime.playEpisodeGeneration
        // Debounce: ignore taps within 5 seconds of the last report.
        // Also allow reports when clock jumps backward (negative interval).
        if let last = lastHearingAdReportTime {
            let interval = Date().timeIntervalSince(last)
            if interval >= 0 && interval < 5.0 { return }
        }
        lastHearingAdReportTime = Date()
        let seedTime = currentTime
        let store = runtime.analysisStore
        let runtimeRef = runtime
        Task {
            // Fetch data for boundary expansion.
            var featureWindows: [FeatureWindow] = []
            var transcriptChunks: [TranscriptChunk] = []
            var adWindows: [AdWindow] = []
            do {
                featureWindows = try await store.fetchAllFeatureWindows(assetId: assetId)
                transcriptChunks = try await store.fetchTranscriptChunks(assetId: assetId)
                adWindows = try await store.fetchAdWindows(assetId: assetId)
            } catch {
                // If we can't fetch data, fall back to a fixed-radius window (defaults above).
            }

            let expander = BoundaryExpander()
            let boundary = expander.expand(
                seed: seedTime,
                featureWindows: featureWindows,
                transcriptChunks: transcriptChunks,
                adWindows: adWindows
            )

            // Inject into skip orchestrator + persist via runtime.
            let persisted = await runtimeRef.injectUserMarkedAd(
                start: boundary.startTime,
                end: boundary.endTime,
                ifCurrentAnalysisAssetId: assetId,
                ifCurrentEpisodeId: episodeId,
                ifPlaybackLifecycleGeneration: playbackGeneration,
                podcastId: podcastId
            )
            guard persisted else { return }

            // Attribute the durable user mark to the show captured at tap time,
            // even if autoplay advances after persistence has begun.
            if let podcastId {
                await runtimeRef.trustService.recordFalseNegativeSignal(podcastId: podcastId)
            }
        }
    }

    // MARK: - Private

    private func applyState(_ state: PlaybackState) {
        isPlaying = state.rate > 0 || {
            if case .playing = state.status { return true }
            return false
        }()
        currentTime = state.currentTime
        duration = state.duration
        playbackSpeed = state.playbackSpeed
        syncMetadata()
    }

    private func syncMetadata() {
        episodeTitle = runtime.currentEpisodeTitle ?? "No Episode Selected"
        podcastTitle = runtime.currentPodcastTitle ?? ""
        artworkURL = runtime.currentArtworkURL
    }
}
