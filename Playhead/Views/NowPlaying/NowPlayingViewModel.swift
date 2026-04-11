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
    func observeBanners(from orchestrator: SkipOrchestrator, into queue: AdBannerQueue) {
        bannerObservationTask?.cancel()
        bannerObservationTask = Task {
            let stream = await orchestrator.bannerItemStream()
            for await item in stream {
                guard !Task.isCancelled else { return }
                await MainActor.run {
                    queue.enqueue(item)
                }
            }
        }
    }

    func loadSkipMode(from orchestrator: SkipOrchestrator) async {
        activeSkipMode = await orchestrator.currentSkipMode()
    }

    func setSkipMode(_ mode: SkipMode, orchestrator: SkipOrchestrator) {
        activeSkipMode = mode
        Task {
            await runtime.setShowSkipMode(mode, orchestrator: orchestrator)
        }
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
            await runtime.skipForward()
        }
    }

    func skipBackward() {
        Task {
            await runtime.skipBackward()
        }
    }

    func seek(to seconds: TimeInterval) {
        Task {
            await runtime.seek(to: seconds)
        }
    }

    /// Handle the "Listen" tap on an ad skip banner.
    ///
    /// 1. Rewind to the snapped start boundary of the skipped ad window.
    /// 2. Set decisionState to .reverted so auto-skip ignores this span.
    /// 3. Feed a false-positive signal to the PodcastProfile trust scoring.
    func handleListenRewind(item: AdSkipBannerItem) {
        // Rewind to the ad start (snapped boundary).
        seek(to: item.adStartTime)

        // Revert the ad window and update trust scoring through the shared runtime.
        Task {
            await runtime.recordListenRewind(
                windowId: item.windowId,
                podcastId: item.podcastId
            )
        }
    }

    func setSpeed(_ speed: Float) {
        Task {
            await runtime.setSpeed(speed)
        }
    }

    /// Record a false negative correction — the user hears an ad that wasn't detected.
    /// Captures the current playback position as the correction timestamp.
    func reportHearingAd() {
        let time = currentTime
        guard let assetId = runtime.currentAnalysisAssetId else { return }
        let correctionStore = runtime.correctionStore
        let podcastId = runtime.currentPodcastId
        Task {
            // Create a false negative correction at the current playback position.
            // Scope uses a window around the current time — the system will
            // expand to nearest acoustic breaks when available (future refinement).
            // For now, use a placeholder ordinal range covering the approximate area.
            let event = CorrectionEvent(
                analysisAssetId: assetId,
                scope: CorrectionScope.exactSpan(
                    assetId: assetId,
                    ordinalRange: 0...Int.max
                ).serialized,
                createdAt: Date().timeIntervalSince1970,
                source: .falseNegative,
                podcastId: podcastId
            )
            do {
                try await correctionStore.record(event)
            } catch {
                // Best-effort — don't surface errors for corrections.
            }

            // Feed false-negative signal to TrustService.
            if let podcastId {
                await runtime.trustService.recordFalseNegativeSignal(podcastId: podcastId)
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
