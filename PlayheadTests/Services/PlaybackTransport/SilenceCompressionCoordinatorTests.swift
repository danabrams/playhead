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
        featureVersion: 5
    )
}

// MARK: - Suite

@Suite("SilenceCompressionCoordinator (playhead-epii)", .timeLimit(.minutes(1)))
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
        // playhead-vsot round 3: the first tick fires the lookahead fetch
        // (fire-and-forget). `awaitPendingRefreshForTesting()` waits for
        // that fetch + `compressor.replaceWindows` to complete, so the
        // plan is materialized deterministically instead of polling
        // `source.fetchCount` / sleeping for cadence. A subsequent tick
        // then engages synchronously (notePlayhead awaits `apply`, which
        // awaits `beginCompression`), so no retry-tick loop is needed.
        await coord.notePlayhead(time: 0)
        await coord.awaitPendingRefreshForTesting()
        await coord.notePlayhead(time: 11)
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
        // playhead-vsot round 3: with keepFullMusic=true, notePlayhead
        // early-returns (the guard gates fetch AND tick), so no async
        // work is fired and no sleep is needed — the assertion is
        // deterministic. `awaitPendingRefreshForTesting` is a no-op here
        // (no refresh Task) but kept for symmetry/robustness.
        await coord.notePlayhead(time: 0)
        await coord.awaitPendingRefreshForTesting()
        await coord.notePlayhead(time: 11)
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
        // playhead-vsot round 3: drive to the engaged state
        // deterministically via the refresh seam (see
        // playheadInsidePlanEngages), then flip the override.
        await coord.notePlayhead(time: 0)
        await coord.awaitPendingRefreshForTesting()
        await coord.notePlayhead(time: 11)
        #expect(!(await playback.beginCalls.isEmpty),
                "Setup: compression must be engaged before the override flip")
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
        // playhead-vsot round 3: drive into the engaged state
        // deterministically via the refresh seam.
        await coord.notePlayhead(time: 0)
        await coord.awaitPendingRefreshForTesting()
        await coord.notePlayhead(time: 11)
        let preBeginCount = await playback.beginCalls.count
        #expect(preBeginCount >= 1)
        // Simulate user changing base speed: `recordUserSpeedChange`
        // marks the planner idle and resets the refresh/tick sentinels
        // (to `-inf`), so the very next tick refetches AND re-evaluates
        // from a clean slate. The published windows survive `markIdle`,
        // so that tick re-engages synchronously.
        await coord.recordUserSpeedChange()
        await coord.notePlayhead(time: 11)
        await coord.awaitPendingRefreshForTesting()
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
        // Tick — should NOT have fetched (the keepFullMusic guard gates
        // the fetch). notePlayhead early-returns synchronously, so no
        // sleep is needed to establish the zero baseline.
        await coord.notePlayhead(time: 11)
        let preFetchCount = await source.fetchCount
        // Flip override OFF: the next tick must re-fetch even if the
        // cadence hasn't elapsed since the last refresh attempt.
        await coord.updateKeepFullMusicOverride(false)
        await coord.notePlayhead(time: 11)
        // playhead-vsot round 3: await the fire-and-forget refresh via
        // the seam instead of polling fetchCount under a wall-clock loop.
        await coord.awaitPendingRefreshForTesting()
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
        // playhead-vsot round 3: await the first refresh via the seam
        // instead of a fixed settle sleep before swapping assets.
        await coord.awaitPendingRefreshForTesting()
        await coord.beginEpisode(assetId: "asset-2", keepFullMusic: false)
        // The second beginEpisode should have ended any in-flight
        // compression on the playback side.
        let endCount = await playback.endCallCount
        #expect(endCount >= 2)
    }

    /// cycle-1 M4 regression test: when `beginEpisode` is called with a
    /// new asset while the previous asset's lookahead refresh is still
    /// in flight, the previous Task must be cancelled. Without the
    /// cancellation, the in-flight Task can outlive its asset, complete
    /// after `currentAssetId` has been overwritten, and (in the worst
    /// case) write stale windows into the compressor before the
    /// `assetId == currentAssetId` guard rejects them — wasted work and
    /// a brief inverted-state window.
    ///
    /// We pin the contract by using a `BlockingAnalysisSource` whose
    /// `fetchWindows` suspends on a stream until the test wakes it.
    /// The first `notePlayhead` kicks an in-flight refresh into the
    /// blocking source. We then call `beginEpisode("asset-2")`, which
    /// must cancel the prior Task. We unblock the source and assert
    /// that the cancellation was observed (`Task.isCancelled` checked
    /// inside the source).
    @Test("beginEpisode cancels prior asset's in-flight lookahead refresh")
    func beginEpisodeCancelsInFlightRefresh() async {
        // Source that suspends inside fetchWindows until `release()` is
        // called, so we can deterministically observe cancellation
        // before the Task body completes.
        // playhead-vsot round 3: fully event-driven — `fetchWindows`
        // fires a continuation when it ENTERS (replacing the 5 ms
        // `waitForFetch` poll) and another when it OBSERVES cancellation
        // (replacing the 5 ms observedCancellation poll). No wall-clock
        // deadlines anywhere.
        actor BlockingAnalysisSource: SilenceCompressionAnalysisSourcing {
            private var releaseContinuation: CheckedContinuation<Void, Never>?
            private var fetchEnteredContinuation: CheckedContinuation<Void, Never>?
            private var cancellationContinuation: CheckedContinuation<Void, Never>?
            private(set) var fetchStarted = false
            private(set) var observedCancellation = false

            /// Suspend until `fetchWindows` has been entered.
            func awaitFetchEntered() async {
                if fetchStarted { return }
                await withCheckedContinuation { c in fetchEnteredContinuation = c }
            }

            /// Suspend until the resumed `fetchWindows` observes cancellation.
            func awaitCancellationObserved() async {
                if observedCancellation { return }
                await withCheckedContinuation { c in cancellationContinuation = c }
            }

            func release() {
                releaseContinuation?.resume()
                releaseContinuation = nil
            }

            func fetchWindows(
                assetId: String, from: Double, to: Double
            ) async throws -> [FeatureWindow] {
                fetchStarted = true
                fetchEnteredContinuation?.resume()
                fetchEnteredContinuation = nil
                await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
                    self.releaseContinuation = c
                }
                if Task.isCancelled {
                    observedCancellation = true
                    cancellationContinuation?.resume()
                    cancellationContinuation = nil
                }
                return []
            }
        }

        let playback = RecordingPlaybackController()
        let source = BlockingAnalysisSource()
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)

        await coord.beginEpisode(assetId: "asset-old", keepFullMusic: false)
        // Kick a refresh — `notePlayhead(time: 0)` triggers
        // `inFlightWindowsRefresh = Task { ... fetchWindows ... }` because
        // refreshDelta is treated as past-cadence on first tick.
        await coord.notePlayhead(time: 0)
        // Wait (event-driven) for the Task to land inside fetchWindows so
        // the cancellation has someone to cancel.
        await source.awaitFetchEntered()

        // Swap to a new asset; this should cancel the in-flight Task.
        await coord.beginEpisode(assetId: "asset-new", keepFullMusic: false)

        // Release the suspended fetch so the Task body resumes and
        // observes Task.isCancelled.
        await source.release()

        // Event-driven: resumes exactly when fetchWindows observes the
        // cancellation. No deadline; the `.timeLimit` trait is the hang
        // backstop.
        await source.awaitCancellationObserved()

        let cancelled = await source.observedCancellation
        #expect(
            cancelled,
            "beginEpisode must cancel the prior asset's in-flight refresh; the suspended Task should observe Task.isCancelled when it resumes"
        )
    }

    /// cycle-1 M1 regression test: a throwing `fetchWindows` (transient
    /// SQLite error or a `CancellationError`) must NOT wipe the
    /// previously-published plan. The original shape was
    /// `(try? await source.fetchWindows(...)) ?? []`, which silently
    /// fell back to `[]`, then handed `[]` to `compressor.replaceWindows`,
    /// deriving an empty plan list and disengaging compression on the
    /// next tick. That makes a single transient failure during a music
    /// bed kick the compressor out for the rest of the cadence window.
    /// The fix returns early on throw so the prior plan stays live.
    @Test("Throwing fetchWindows preserves prior plan (no disengage)")
    func throwingFetchPreservesPriorPlan() async {
        actor ToggleableThrowingSource: SilenceCompressionAnalysisSourcing {
            private let validWindows: [FeatureWindow]
            private(set) var fetchCount: Int = 0

            init(validWindows: [FeatureWindow]) {
                self.validWindows = validWindows
            }

            func fetchWindows(
                assetId: String, from: Double, to: Double
            ) async throws -> [FeatureWindow] {
                fetchCount += 1
                if fetchCount == 1 {
                    return validWindows.filter { window in
                        window.endTime > from && window.startTime < to
                    }
                }
                throw CancellationError()
            }
        }

        let windows = (0..<6).map { i in
            musicWindow(start: Double(i) * 2 + 10, end: Double(i + 1) * 2 + 10)
        }
        let playback = RecordingPlaybackController()
        let source = ToggleableThrowingSource(validWindows: windows)
        let coord = SilenceCompressionCoordinator(playback: playback, source: source)
        await coord.beginEpisode(assetId: "asset-1", keepFullMusic: false)

        // playhead-vsot round 3: deterministic via the refresh seam.
        // First tick: lastWindowsRefreshTime is -inf so the first fetch
        // fires unconditionally and returns the valid windows.
        await coord.notePlayhead(time: 0)
        await coord.awaitPendingRefreshForTesting()
        // Tick inside the music plan; engage compression so we have
        // something to disengage if the buggy fallback path runs.
        await coord.notePlayhead(time: 11)
        let preBeginCount = await playback.beginCalls.count
        let preEndCount = await playback.endCallCount
        #expect(
            preBeginCount >= 1,
            "Setup precondition failed: first fetch should have engaged compression"
        )

        // Drive past the 5s cadence to fire the second fetch (which
        // throws). notePlayhead(18) fires refresh #2; awaiting it lets
        // the throw path run (the handler returns early, preserving the
        // prior windows).
        await coord.notePlayhead(time: 18)
        await coord.awaitPendingRefreshForTesting()
        #expect(
            await source.fetchCount >= 2,
            "Second fetch must have fired so the throw path is exercised"
        )

        // After the throwing fetch lands, tick again inside the same
        // music horizon. With the fix, the prior plan list survives and
        // the compressor stays engaged (no disengage). With the broken
        // `(try? ...) ?? []` shape, plans collapse to [] and this tick
        // would fire a `disengage` → endCallCount increments above the
        // pre-throw baseline.
        await coord.notePlayhead(time: 14)
        let postEndCount = await playback.endCallCount
        #expect(
            postEndCount == preEndCount,
            "Throwing fetch must NOT wipe prior plan (would disengage); preEnd=\(preEndCount), postEnd=\(postEndCount)"
        )
    }
}
