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

    // MARK: - Service Reference

    private var playbackService: PlaybackService?
    private var observationTask: Task<Void, Never>?
    private var segmentObservationTask: Task<Void, Never>?

    /// Reference to the SkipOrchestrator for ad segment observation.
    private var skipOrchestrator: SkipOrchestrator?

    // MARK: - Lifecycle

    func startObserving() {
        observationTask = Task { @PlaybackServiceActor in
            let service = PlaybackService()
            await MainActor.run {
                self.playbackService = service
            }
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
        skipOrchestrator = orchestrator
        segmentObservationTask?.cancel()
        segmentObservationTask = Task {
            let stream = await orchestrator.appliedSegmentsStream()
            for await segments in stream {
                guard !Task.isCancelled else { return }
                let dur = self.duration
                guard dur > 0 else {
                    self.adSegmentRanges = []
                    continue
                }
                let ranges: [ClosedRange<Double>] = segments.compactMap { seg in
                    let lower = seg.start / dur
                    let upper = seg.end / dur
                    guard lower < upper, lower >= 0, upper <= 1.0 else { return nil }
                    return min(max(lower, 0), 1)...min(max(upper, 0), 1)
                }
                self.adSegmentRanges = ranges
            }
        }
    }

    // MARK: - Actions

    func togglePlayPause() {
        guard let service = playbackService else { return }
        let playing = isPlaying
        Task { @PlaybackServiceActor in
            if playing {
                service.pause()
            } else {
                service.play()
            }
        }
    }

    func skipForward() {
        guard let service = playbackService else { return }
        Task { @PlaybackServiceActor in
            await service.skipForward()
        }
    }

    func skipBackward() {
        guard let service = playbackService else { return }
        Task { @PlaybackServiceActor in
            await service.skipBackward()
        }
    }

    func seek(to seconds: TimeInterval) {
        guard let service = playbackService else { return }
        Task { @PlaybackServiceActor in
            await service.seek(to: seconds)
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

        // Revert the ad window and update trust scoring in the background.
        Task.detached(priority: .utility) {
            do {
                let store = try AnalysisStore()
                try await store.migrate()
                let detectionService = AdDetectionService(
                    store: store,
                    metadataExtractor: FallbackExtractor()
                )
                try await detectionService.recordListenRewind(
                    windowId: item.windowId,
                    podcastId: item.podcastId
                )
            } catch {
                // Non-blocking: rewind already happened, trust update is best-effort.
            }
        }
    }

    func setSpeed(_ speed: Float) {
        guard let service = playbackService else { return }
        Task { @PlaybackServiceActor in
            service.setSpeed(speed)
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
