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
    }

    // MARK: - Actions

    func togglePlayPause() {
        guard let service = playbackService else { return }
        Task { @PlaybackServiceActor in
            if case .playing = await MainActor.run(body: { self.isPlaying }) ? PlaybackState.Status.playing : PlaybackState.Status.paused {
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
