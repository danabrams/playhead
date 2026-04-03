// NowPlayingViewModel.swift
// Bridges PlaybackService state to SwiftUI. Runs observation on MainActor
// so view updates are always on the right thread.

import Foundation
import SwiftUI

@MainActor
final class NowPlayingViewModel: ObservableObject {

    // MARK: - Published State

    @Published var episodeTitle: String = "No Episode Selected"
    @Published var podcastTitle: String = ""
    @Published var isPlaying: Bool = false
    @Published var currentTime: TimeInterval = 0
    @Published var duration: TimeInterval = 0
    @Published var playbackSpeed: Float = 1.0

    /// Ad segments from SkipOrchestrator, expressed as fractional ranges (0...1)
    /// of the total episode duration. Updated in real-time as detection produces results.
    @Published var adSegmentRanges: [ClosedRange<Double>] = []

    // MARK: - Derived

    var progress: Double {
        guard duration > 0 else { return 0 }
        return currentTime / duration
    }

    var elapsedFormatted: String {
        Self.formatTime(currentTime)
    }

    var remainingFormatted: String {
        let remaining = max(duration - currentTime, 0)
        return "-\(Self.formatTime(remaining))"
    }

    // MARK: - Dependencies

    private let runtime: PlayheadRuntime
    private var observationTask: Task<Void, Never>?
    private var segmentObservationTask: Task<Void, Never>?

    init(runtime: PlayheadRuntime) {
        self.runtime = runtime
        syncMetadata()
    }

    // MARK: - Lifecycle

    func startObserving() {
        guard observationTask == nil else { return }
        syncMetadata()
        let service = runtime.playbackService
        observationTask = Task { @PlaybackServiceActor in
            for await state in service.stateStream {
                await MainActor.run {
                    self.applyState(state)
                }
            }
        }
    }

    func stopObserving() {
        observationTask?.cancel()
        observationTask = nil
        segmentObservationTask?.cancel()
        segmentObservationTask = nil
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

    // MARK: - Private

    private func applyState(_ state: PlaybackState) {
        if case .playing = state.status {
            isPlaying = true
        } else {
            isPlaying = false
        }
        currentTime = state.currentTime
        duration = state.duration
        playbackSpeed = state.playbackSpeed
        syncMetadata()
    }

    private func syncMetadata() {
        episodeTitle = runtime.currentEpisodeTitle ?? "No Episode Selected"
        podcastTitle = runtime.currentPodcastTitle ?? ""
    }

    // MARK: - Formatting

    private static func formatTime(_ seconds: TimeInterval) -> String {
        guard seconds.isFinite, seconds >= 0 else { return "0:00" }
        let totalSeconds = Int(seconds)
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        let secs = totalSeconds % 60
        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, secs)
        }
        return String(format: "%d:%02d", minutes, secs)
    }
}
