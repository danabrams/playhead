// SilenceCompressionCoordinatorTests.swift
// playhead-epii — Coordinator-level tests focused on the
// integration concerns: lookahead refresh cadence, asset-id
// transitions, and the apply-to-PlaybackService translation. Pure
// unit tests against recording doubles for both the playback
// surface and the analysis source.

import AVFoundation
import Foundation
import Testing

@testable import Playhead

// MARK: - Recording doubles

/// Records every begin/end call. Sendable via an actor so the
/// recorder is safe to read from any context.
actor RecordingPlaybackController: SilenceCompressionPlaybackControlling {
    private(set) var beginCalls: [(multiplier: Float, algorithm: AVAudioTimePitchAlgorithm)] = []
    private(set) var endCallCount: Int = 0

    func beginCompression(multiplier: Float, algorithm: AVAudioTimePitchAlgorithm) async {
        beginCalls.append((multiplier, algorithm))
    }

    func endCompression() async {
        endCallCount += 1
    }
}

/// Always-canned source: returns the same window list regardless of
/// the requested range. Tests construct the source with the windows
/// that should be visible in the lookahead horizon.
actor RecordingAnalysisSource: SilenceCompressionAnalysisSourcing {
    private let windows: [FeatureWindow]
    private(set) var fetchCount: Int = 0

    init(windows: [FeatureWindow]) {
        self.windows = windows
    }

    func fetchWindows(
        assetId: String, from: Double, to: Double
    ) async throws -> [FeatureWindow] {
        fetchCount += 1
        return windows.filter { window in
            window.endTime > from && window.startTime < to
        }
    }
}

// MARK: - Helpers

private func musicWindow(
    start: Double, end: Double, level: MusicBedLevel = .foreground,
    musicProbability: Double = 0.9
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: "test",
        startTime: start,
        endTime: end,
        rms: 0.3,
        spectralFlux: 0.1,
        musicProbability: musicProbability,
        speakerChangeProxyScore: 0,
        musicBedChangeScore: 0,
        musicBedOnsetScore: 1.0,
        musicBedOffsetScore: 1.0,
        musicBedLevel: level,
        pauseProbability: 0,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 4
    )
}

// MARK: - Suite

@Suite("SilenceCompressionCoordinator (playhead-epii)")
@MainActor
struct SilenceCompressionCoordinatorTests {

    @Test("Episode begin ⇒ ends any prior compression and refreshes plan")
    func episodeBeginIdempotentEnd() async {
        let playback = RecordingPlaybackController()
        let source = RecordingAnalysisSource(windows: (0..<6).map { i in
            musicWindow(start: Double(i) * 2 + 10, end: Double(i + 1) * 2 + 10)
        })
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)
        await coord.beginEpisode(assetId: "asset-1", keepFullMusic: false)
        let endCount = await playback.endCallCount
        #expect(endCount >= 1)
    }

    @Test("notePlayhead inside a music plan ⇒ engages with varispeed/high rate")
    func playheadInsidePlanEngages() async {
        let windows = (0..<6).map { i in
            musicWindow(start: Double(i) * 2 + 10, end: Double(i + 1) * 2 + 10)
        }
        let playback = RecordingPlaybackController()
        let source = RecordingAnalysisSource(windows: windows)
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)
        await coord.beginEpisode(assetId: "asset-1", keepFullMusic: false)
        // First tick fires the lookahead fetch (async). Wait for it
        // to materialize the plan, then tick from inside the plan.
        await coord.notePlayhead(time: 0)
        await Task.yield()
        // Spin briefly to let the fire-and-forget refresh complete.
        for _ in 0..<10 {
            if await source.fetchCount > 0 { break }
            try? await Task.sleep(for: .milliseconds(10))
        }
        // Wait long enough for cadence to allow another tick.
        try? await Task.sleep(for: .milliseconds(50))
        await coord.notePlayhead(time: 11)
        // Coordinator may need an additional tick after the fetch
        // completes for the plan to be visible.
        for _ in 0..<10 {
            if !(await playback.beginCalls.isEmpty) { break }
            await coord.notePlayhead(time: 11)
            try? await Task.sleep(for: .milliseconds(20))
        }
        let calls = await playback.beginCalls
        #expect(!calls.isEmpty, "Coordinator should have engaged compression")
        #expect(calls.last?.algorithm == .varispeed)
    }

    @Test("Per-show override true ⇒ never engages even with valid plan")
    func keepFullMusicBlocksEngagement() async {
        let windows = (0..<6).map { i in
            musicWindow(start: Double(i) * 2 + 10, end: Double(i + 1) * 2 + 10)
        }
        let playback = RecordingPlaybackController()
        let source = RecordingAnalysisSource(windows: windows)
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)
        await coord.beginEpisode(assetId: "asset-1", keepFullMusic: true)
        await coord.notePlayhead(time: 0)
        try? await Task.sleep(for: .milliseconds(80))
        await coord.notePlayhead(time: 11)
        try? await Task.sleep(for: .milliseconds(80))
        let calls = await playback.beginCalls
        #expect(calls.isEmpty)
    }

    @Test("updateKeepFullMusicOverride(true) mid-flight disengages immediately")
    func overrideMidFlightDisengages() async {
        let windows = (0..<6).map { i in
            musicWindow(start: Double(i) * 2 + 10, end: Double(i + 1) * 2 + 10)
        }
        let playback = RecordingPlaybackController()
        let source = RecordingAnalysisSource(windows: windows)
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)
        await coord.beginEpisode(assetId: "asset-1", keepFullMusic: false)
        await coord.notePlayhead(time: 0)
        for _ in 0..<10 {
            if await source.fetchCount > 0 { break }
            try? await Task.sleep(for: .milliseconds(10))
        }
        try? await Task.sleep(for: .milliseconds(80))
        for _ in 0..<10 {
            await coord.notePlayhead(time: 11)
            if !(await playback.beginCalls.isEmpty) { break }
            try? await Task.sleep(for: .milliseconds(20))
        }
        let preEndCount = await playback.endCallCount
        await coord.updateKeepFullMusicOverride(true)
        let postEndCount = await playback.endCallCount
        #expect(postEndCount > preEndCount, "Override flip should disengage")
    }

    @Test("recordUserSpeedChange resets planner so next tick re-engages")
    func speedChangeResetsPlanner() async {
        let windows = (0..<6).map { i in
            musicWindow(start: Double(i) * 2 + 10, end: Double(i + 1) * 2 + 10)
        }
        let playback = RecordingPlaybackController()
        let source = RecordingAnalysisSource(windows: windows)
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)
        await coord.beginEpisode(assetId: "asset-1", keepFullMusic: false)
        // Drive into the plan (engaged state).
        await coord.notePlayhead(time: 0)
        for _ in 0..<10 {
            if await source.fetchCount > 0 { break }
            try? await Task.sleep(for: .milliseconds(10))
        }
        for _ in 0..<10 {
            await coord.notePlayhead(time: 11)
            if !(await playback.beginCalls.isEmpty) { break }
            try? await Task.sleep(for: .milliseconds(20))
        }
        let preBeginCount = await playback.beginCalls.count
        #expect(preBeginCount >= 1)
        // Simulate user changing base speed: planner state must reset
        // and the next tick from inside the same plan should engage
        // again.
        await coord.recordUserSpeedChange()
        // Spin a few ticks to allow the cadence-driven re-engage.
        try? await Task.sleep(for: .milliseconds(80))
        for _ in 0..<10 {
            await coord.notePlayhead(time: 11)
            if await playback.beginCalls.count > preBeginCount { break }
            try? await Task.sleep(for: .milliseconds(20))
        }
        let postBeginCount = await playback.beginCalls.count
        #expect(
            postBeginCount > preBeginCount,
            "Speed change should clear planner state so next tick re-engages"
        )
    }

    @Test("Override OFF mid-episode forces immediate re-fetch")
    func overrideOffForcesRefresh() async {
        let windows = (0..<6).map { i in
            musicWindow(start: Double(i) * 2 + 10, end: Double(i + 1) * 2 + 10)
        }
        let playback = RecordingPlaybackController()
        let source = RecordingAnalysisSource(windows: windows)
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)
        await coord.beginEpisode(assetId: "asset-1", keepFullMusic: true)
        // Tick a few times — should NOT have fetched (override gates
        // the fetch on the keepFullMusic guard).
        await coord.notePlayhead(time: 11)
        try? await Task.sleep(for: .milliseconds(40))
        let preFetchCount = await source.fetchCount
        // Flip override OFF: the next tick must re-fetch even if the
        // cadence hasn't elapsed since the last refresh attempt.
        await coord.updateKeepFullMusicOverride(false)
        await coord.notePlayhead(time: 11)
        // Allow the fire-and-forget refresh to land.
        for _ in 0..<10 {
            if await source.fetchCount > preFetchCount { break }
            try? await Task.sleep(for: .milliseconds(10))
        }
        let postFetchCount = await source.fetchCount
        #expect(
            postFetchCount > preFetchCount,
            "Override flip-OFF should force the next tick to refetch"
        )
    }

    @Test("Asset-id change cancels in-flight refresh and resets state")
    func assetIdChangeResets() async {
        let playback = RecordingPlaybackController()
        let windows = (0..<6).map { i in
            musicWindow(start: Double(i) * 2 + 10, end: Double(i + 1) * 2 + 10)
        }
        let source = RecordingAnalysisSource(windows: windows)
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)
        await coord.beginEpisode(assetId: "asset-1", keepFullMusic: false)
        await coord.notePlayhead(time: 0)
        try? await Task.sleep(for: .milliseconds(50))
        await coord.beginEpisode(assetId: "asset-2", keepFullMusic: false)
        // The second beginEpisode should have ended any in-flight
        // compression on the playback side.
        let endCount = await playback.endCallCount
        #expect(endCount >= 2)
    }
}
