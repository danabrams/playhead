// PlaybackPositionCommitTests.swift
// playhead-s9mx: the transport must not offer a number as a playhead when
// it is not one, and the commit point must be able to say which of its
// exits it took.
//
// WHAT WENT WRONG, measured rather than reasoned. Dan listened to four
// episodes on 2026-08-15. Two of them (`Summer S'pouses` Ep2 "Amanda" and
// Ep3 "Tak") sat at `playbackPosition = 0.0` afterwards and could not be
// resumed. The bead's own hypothesis was that one of the two silent exits
// in `PlayheadApp.persistPlaybackPosition` had fired and dropped the write.
//
// It had not. `Episode.playbackAnchor` is nil-defaulted and is written in
// exactly one production place — the line after `playbackPosition`, inside
// the same commit — so a non-nil anchor is proof a commit RAN. In the
// device pull, 4 of 1,594 episode rows carry a non-nil anchor, and they are
// precisely the four episodes of that day:
//
//     Z_PK  position   anchor     title
//      213       0.0      0.0     Summer S'pouses Episode 2: Amanda Lund
//      549   1517.68  1517.68     Summer S'pouses Episode 1
//     1529   6873.14  6873.14     The Quitting Expert …
//     1592       0.0      0.0     Summer S'pouses Episode 3: Tak Boroyan
//
// `anchor = 0.0`, not NULL. The save COMPLETED and committed a zero.
//
// WHERE THE ZERO CAME FROM. `PlaybackService._state.currentTime` is set to
// `0` in exactly one place — `pauseAndDetachCurrentItem` when
// `preservingPosition == false` — and the only production caller that
// passes `false` is `performPlayEpisode`'s replacement detach.
// `detachPriorPlaybackBeforeReplacement` deliberately keeps the OUTGOING
// episode's identity published across that detach. `capturePlaybackPosition`
// read `currentEpisodeId` synchronously and the transport after an `await`,
// on a reentrant MainActor, so it could pair a real episode with a
// transport that had just been zeroed for a different reason entirely.
// This repo's standing defect class: a value that names one thing read as
// though it named another.
//
// The two directions this file pins are equally load-bearing, and a fix
// that only gets one of them is wrong:
//
//   * REPLACEMENT detach (`preservingPosition: false`) must REVOKE the
//     playhead — that is the defect.
//   * STOP detach (`preservingPosition: true`) must PRESERVE it — that is
//     how Ep1 and Diary got their correct end-of-episode positions, and
//     `performStopPlayback` persists immediately after it.
//
// These are behavioural tests: no assertion here reads a clock, so they
// stay valid on a saturated box and must never move behind `PerfGate`.
// `.timeLimit(.minutes(3))` follows the measured rationale in
// `PlaybackServiceActorTests` — a trip means a real hang, not a busy
// machine. Every service is torn down, per the same file's rule about
// process-global remote-command targets.

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

@Suite(
    "playhead-s9mx: a detached transport is not a playhead",
    .timeLimit(.minutes(3))
)
struct PlaybackPositionCommitTests {

    /// The exact position Ep1 committed on 2026-08-15, used throughout so a
    /// failure message points at the real incident rather than a round
    /// number that could have come from anywhere.
    private static let realPlayhead: TimeInterval = 1517.68

    private func makeService() async -> PlaybackService {
        await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
    }

    private func makeItem(_ name: String) -> AVPlayerItem {
        AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/\(name).mp3")!
            )
        )
    }

    // MARK: - Transport

    @Test("A transport that has never played reports no playhead, not zero")
    func freshTransportHasNoObservedPlayhead() async {
        let service = await makeService()
        #expect(await service.snapshot().currentTime == 0)
        #expect(
            await service.observedPlayhead() == nil,
            "`currentTime`'s 0 default is not a listener position, and the commit point must not read it as one"
        )
        await service.tearDown()
    }

    @Test("A seek publishes an observed playhead")
    func seekPublishesAnObservedPlayhead() async {
        let service = await makeService()
        _ = await service.seek(to: Self.realPlayhead)
        #expect(await service.observedPlayhead() == Self.realPlayhead)
        await service.tearDown()
    }

    @Test("A REPLACEMENT detach revokes the playhead it zeroed")
    func replacementDetachRevokesTheObservedPlayhead() async throws {
        let service = await makeService()
        _ = await service.seek(to: Self.realPlayhead)
        #expect(await service.observedPlayhead() == Self.realPlayhead)

        // `preservingPosition` defaults to false — this is byte-for-byte
        // what `performPlayEpisode`'s quiesce does to the outgoing episode.
        _ = try #require(await service.pauseAndDetachCurrentItem())

        #expect(
            await service.snapshot().currentTime == 0,
            "the detach still zeroes the raw state — display consumers are unchanged"
        )
        #expect(
            await service.observedPlayhead() == nil,
            "the zero a replacement detach writes is transport bookkeeping; offering it as a playhead is how a real position got overwritten with 0.0"
        )
        await service.tearDown()
    }

    @Test("A STOP detach preserves the playhead, because Stop persists it")
    func stopDetachPreservesTheObservedPlayhead() async throws {
        let service = await makeService()
        _ = await service.seek(to: Self.realPlayhead)

        _ = try #require(
            await service.pauseAndDetachCurrentItem(preservingPosition: true)
        )

        #expect(await service.snapshot().currentTime == Self.realPlayhead)
        #expect(
            await service.observedPlayhead() == Self.realPlayhead,
            "`performStopPlayback` detaches with preservingPosition:true and persists on the very next line — revoking here would delete the end-of-episode commit that DID work"
        )
        await service.tearDown()
    }

    @Test("Installing a new item revokes the previous item's playhead")
    func installingAnItemRevokesTheObservedPlayhead() async {
        let service = await makeService()
        _ = await service.seek(to: Self.realPlayhead)

        await service.loadItem(makeItem("s9mx-replacement"))

        #expect(
            await service.snapshot().currentTime == Self.realPlayhead,
            "installing an item does not touch `_state.currentTime` — which is exactly the trap"
        )
        #expect(
            await service.observedPlayhead() == nil,
            "a fresh item has produced no clock sample, and `load`'s resume seek runs on AVPlayerItem directly without publishing state"
        )
        await service.tearDown()
    }

    @Test("Teardown preserves the playhead — the last reading was still real")
    func tearDownPreservesTheObservedPlayhead() async {
        let service = await makeService()
        _ = await service.seek(to: Self.realPlayhead)
        await service.tearDown()
        #expect(
            await service.observedPlayhead() == Self.realPlayhead,
            "teardown deliberately preserves currentTime, so a shutdown-time persist still writes the truth"
        )
    }

    // MARK: - Runtime capture

    @MainActor
    @Test("With nothing loaded, capture names the absence")
    func captureNamesTheAbsenceWhenNothingIsLoaded() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            #expect(
                await runtime.capturePlaybackPosition() == .noPublishedEpisode
            )
        }
    }

    /// Acceptance #3 in its cleanest form: an episode that is published but
    /// whose transport has produced no reading must not capture `0.0`. A
    /// committed `0.0` is byte-identical to the `0` every unplayed row
    /// carries, so writing one here would erase the only distinction the
    /// schema has — `playbackAnchor == nil` means "no commit has ever run".
    @MainActor
    @Test("A published episode on a silent transport is not captured at 0.0")
    func captureRefusesZeroOnATransportThatHasNeverPlayed() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            runtime._setUserMarkPlaybackContextForTesting(
                analysisAssetId: nil,
                episodeId: "ep-s9mx-never-played",
                podcastId: nil
            )
            #expect(await runtime.playbackService.snapshot().currentTime == 0)

            let capture = await runtime.capturePlaybackPosition()
            #expect(
                capture
                    == .playheadNotObserved(episodeId: "ep-s9mx-never-played"),
                "capture returned \(capture.outcomeName) — `currentTime`'s 0 default must never be committed as a listener position"
            )
        }
    }

    @MainActor
    @Test("With an episode loaded and a live transport, capture reports the position")
    func captureReportsTheObservedPlayhead() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            runtime._setUserMarkPlaybackContextForTesting(
                analysisAssetId: nil,
                episodeId: "ep-s9mx-amanda",
                podcastId: nil
            )
            _ = await runtime.playbackService.seek(to: Self.realPlayhead)

            #expect(
                await runtime.capturePlaybackPosition()
                    == .captured(
                        episodeId: "ep-s9mx-amanda",
                        position: Self.realPlayhead
                    )
            )
        }
    }

    /// THE REGRESSION. Before playhead-s9mx this returned
    /// `(episodeId: "ep-s9mx-amanda", position: 0.0)` and the commit point
    /// wrote that zero over a real position, in both the row and the
    /// readiness anchor.
    @MainActor
    @Test("The zero a replacement detach leaves is never captured as a position")
    func captureRefusesTheZeroLeftByAReplacementDetach() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let generation = runtime._setUserMarkPlaybackContextForTesting(
                analysisAssetId: nil,
                episodeId: "ep-s9mx-amanda",
                podcastId: nil
            )
            _ = await runtime.playbackService.seek(to: Self.realPlayhead)

            // The replacement detach, driven through the production helper
            // `performPlayEpisode` itself calls. The outgoing episode's
            // identity stays published across it by design.
            #expect(
                await runtime._quiescePlaybackThenCancelAudioCacheForTesting(
                    generation: generation
                )
            )
            #expect(
                runtime.currentEpisodeId == "ep-s9mx-amanda",
                "the detach must leave the outgoing identity published — otherwise this test is proving the wrong thing"
            )
            #expect(await runtime.playbackService.snapshot().currentTime == 0)

            let capture = await runtime.capturePlaybackPosition()
            #expect(
                capture
                    == .playheadNotObserved(episodeId: "ep-s9mx-amanda"),
                "capture returned \(capture.outcomeName); a `.captured(_, 0.0)` here is exactly what put Amanda and Tak at 0.0"
            )
        }
    }

    /// The mirror of the test above, and the reason the fix is a fact about
    /// the VALUE rather than a `playerItem != nil` check: after a Stop
    /// detach the item is nil too, and there the number is real.
    @MainActor
    @Test("The position a stop detach preserves is still captured")
    func captureAcceptsThePositionAStopDetachPreserved() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            runtime._setUserMarkPlaybackContextForTesting(
                analysisAssetId: nil,
                episodeId: "ep-s9mx-ep1",
                podcastId: nil
            )
            _ = await runtime.playbackService.seek(to: Self.realPlayhead)

            _ = await runtime.playbackService.pauseAndDetachCurrentItem(
                preservingPosition: true
            )

            #expect(
                await runtime.capturePlaybackPosition()
                    == .captured(
                        episodeId: "ep-s9mx-ep1",
                        position: Self.realPlayhead
                    )
            )
        }
    }
}
