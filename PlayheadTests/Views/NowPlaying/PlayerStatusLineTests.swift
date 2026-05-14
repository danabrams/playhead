// PlayerStatusLineTests.swift
// Behavioral tests for the player's one-line status row helper.
//
// Scope: playhead-3bv.4 (UI design §C-2 — "One-line player status below
// the scrubber, mirroring the episode-detail status line. Hidden when
// fully analyzed.").
//
// We exercise `playerStatusLineInputs(episode:)` directly rather than
// rendering the SwiftUI hierarchy — the function is the single source
// of truth for what the player surfaces, and the repo does not ship a
// snapshot-testing library (see `EpisodeRowReadinessTests` for the
// canonical articulation of this pattern). Reducer-driven copy is
// already covered by `EpisodeStatusLineCopyTests` and friends; this
// file owns ONLY the player-specific hide/show behavior layered on top.

import XCTest
import SwiftData
@testable import Playhead

@MainActor
final class PlayerStatusLineTests: XCTestCase {

    // MARK: - Helpers

    private func makeContainer() throws -> ModelContainer {
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        return try ModelContainer(
            for: Podcast.self, Episode.self,
            configurations: config
        )
    }

    private func makePodcast(in context: ModelContext) -> Podcast {
        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Player Status Line Podcast",
            author: "Tester"
        )
        context.insert(podcast)
        return podcast
    }

    private func makeEpisode(
        podcast: Podcast,
        coverage: CoverageSummary?,
        anchor: TimeInterval?,
        in context: ModelContext
    ) -> Episode {
        let episode = Episode(
            feedItemGUID: UUID().uuidString,
            feedURL: podcast.feedURL,
            podcast: podcast,
            title: "Episode",
            audioURL: URL(string: "https://example.com/ep.mp3")!,
            coverageSummary: coverage,
            playbackAnchor: anchor
        )
        context.insert(episode)
        return episode
    }

    private func makeCoverage(
        ranges: [ClosedRange<TimeInterval>],
        isComplete: Bool = false
    ) -> CoverageSummary {
        CoverageSummary(
            coverageRanges: ranges,
            isComplete: isComplete,
            modelVersion: "m1",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    // MARK: - Suppression contract (UI design §C-2)

    /// `.complete` readiness must suppress the row entirely. The player
    /// surface is the typographic timeline at that point — adding a
    /// status line would re-introduce the "ready" message the timeline
    /// already carries.
    func testCompleteReadinessSuppressesRow() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...3600.0], isComplete: true),
            anchor: 1200.0,
            in: context
        )
        XCTAssertNil(
            playerStatusLineInputs(episode: episode),
            "Fully-analyzed episode must hide the player status row"
        )
    }

    /// `.proximal` readiness keeps the row visible — the typographic
    /// timeline does not yet have full coverage, and the row reports
    /// "Skip-ready · first N min" style copy via the reducer.
    func testProximalReadinessSurfacesRow() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...1000.0]),
            anchor: 42.5,
            in: context
        )
        let inputs = try XCTUnwrap(
            playerStatusLineInputs(episode: episode),
            "Proximal readiness must surface the player status row"
        )
        XCTAssertNotNil(inputs.coverage, "Coverage flows through to the row")
        XCTAssertEqual(inputs.anchor, 42.5)
    }

    /// `.deferredOnly` readiness keeps the row visible. The reducer
    /// routes this to the queued / waiting copy ("Downloaded · queued
    /// for analysis"-style strings) — the row is the user's only
    /// promise of forward motion at this point.
    func testDeferredOnlyReadinessSurfacesRow() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        // Coverage exists but the anchor sits outside the proximal
        // window → derivePlaybackReadiness returns `.deferredOnly`.
        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...1000.0]),
            anchor: 5000.0,
            in: context
        )
        XCTAssertNotNil(
            playerStatusLineInputs(episode: episode),
            "Deferred-only readiness must surface the player status row"
        )
    }

    /// `.none` readiness — no coverage and no anchor — also surfaces a
    /// row. The reducer routes this to "Queued · waiting"-style copy.
    /// The player is the *active* surface; an empty status row would
    /// silently strip the only signal of "we know about this episode."
    /// This is the deliberate divergence from the Library row helper
    /// (which short-circuits `.none` because the Library has its own
    /// status cues per-row).
    func testNoneReadinessStillSurfacesRowOnPlayerSurface() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let episode = makeEpisode(
            podcast: podcast,
            coverage: nil,
            anchor: nil,
            in: context
        )
        XCTAssertNotNil(
            playerStatusLineInputs(episode: episode),
            "None-readiness episode must still surface a row on the player"
        )
    }

    // MARK: - Reducer plumbing

    /// The inputs the helper builds must be safe to feed back into
    /// `EpisodeStatusLineView` — specifically, the reducer-resolved
    /// status's `playbackReadiness` must reflect the
    /// `(coverage, anchor)` pair the row received, NOT a stale
    /// readiness from some other path. Catches a regression where a
    /// helper might compute readiness twice and let the two values
    /// diverge.
    func testReadinessOnStatusMatchesDerivation() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...1000.0]),
            anchor: 42.5,
            in: context
        )
        let inputs = try XCTUnwrap(
            playerStatusLineInputs(episode: episode)
        )
        XCTAssertEqual(
            inputs.status.playbackReadiness,
            .proximal,
            "Status readiness must match the (coverage, anchor) derivation"
        )
    }

    /// Moving the anchor across the proximal boundary changes the
    /// surfaced row from "hide on complete" / "visible on proximal" —
    /// mirroring the Library checkmark behavior but for the player
    /// row's visibility. Same coverage record across both reads, so
    /// only the anchor flip is responsible for the change.
    func testAnchorChangeFlipsRowReadinessBetweenStates() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...1000.0]),
            anchor: 5000.0,
            in: context
        )
        // Anchor outside coverage → deferredOnly, row visible.
        let outside = try XCTUnwrap(playerStatusLineInputs(episode: episode))
        XCTAssertEqual(outside.status.playbackReadiness, .deferredOnly)

        // Anchor inside coverage → proximal, row still visible but
        // now reports a different readiness band.
        episode.playbackAnchor = 42.5
        let inside = try XCTUnwrap(playerStatusLineInputs(episode: episode))
        XCTAssertEqual(inside.status.playbackReadiness, .proximal)
    }
}
