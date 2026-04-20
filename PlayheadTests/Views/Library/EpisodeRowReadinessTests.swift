// EpisodeRowReadinessTests.swift
// Behavioral test: the Library row's ✓ affordance is a pure function of
// `(episode.coverageSummary, episode.playbackAnchor)`. Moving the anchor
// flips the checkmark visibility without any other state changing.
//
// We exercise `libraryRowShouldShowReadinessCheckmark(episode:)` directly
// rather than rendering the SwiftUI hierarchy — the function is the
// single source of truth for the ✓ decision (see
// `EpisodeListView.EpisodeRow.body`), and the repo does not ship a
// snapshot-testing library. A snapshot test would add a dependency the
// project scope forbids.
//
// Scope: playhead-cthe (Phase 2 deliverable 2).

import XCTest
import SwiftData
@testable import Playhead

@MainActor
final class EpisodeRowReadinessTests: XCTestCase {

    // MARK: - Helpers

    private func makeContainer() throws -> ModelContainer {
        let config = ModelConfiguration(isStoredInMemoryOnly: true)
        return try ModelContainer(
            for: Podcast.self, Episode.self,
            configurations: config
        )
    }

    private func makePodcast(in context: ModelContext) -> Podcast {
        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Readiness Test Podcast",
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

    // MARK: - Badge flip when anchor moves

    /// Primary acceptance test: anchor moving from "outside proximal
    /// window" to "inside proximal window" flips the checkmark from
    /// hidden to visible. Same coverage record — only the anchor
    /// changes.
    func testAnchorMoveFlipsCheckmarkFromHiddenToVisible() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        // Coverage spans [0, 1000]. At anchor 2000 the lookahead window
        // [2000, 2900] falls entirely outside coverage → .deferredOnly,
        // no checkmark.
        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...1000.0]),
            anchor: 2000.0,
            in: context
        )

        XCTAssertFalse(
            libraryRowShouldShowReadinessCheckmark(episode: episode),
            "Anchor 2000 with coverage [0, 1000] is .deferredOnly — no ✓"
        )

        // Move the anchor INTO the covered region. 42.5 + 900 = 942.5
        // fits inside [0, 1000] → .proximal, checkmark visible.
        episode.playbackAnchor = 42.5
        XCTAssertTrue(
            libraryRowShouldShowReadinessCheckmark(episode: episode),
            "Anchor 42.5 with coverage [0, 1000] is .proximal — ✓ visible"
        )

        // Move the anchor back out. The checkmark must hide again —
        // the ✓ is a pure function of the current (coverage, anchor)
        // pair; it must not "stick" after a transient proximal state.
        episode.playbackAnchor = 5000.0
        XCTAssertFalse(
            libraryRowShouldShowReadinessCheckmark(episode: episode),
            "Anchor 5000 with coverage [0, 1000] is .deferredOnly — ✓ hidden"
        )
    }

    // MARK: - Per-readiness cell behavior

    func testNoneReadinessHidesCheckmark() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        // Nil coverage → .none → no checkmark.
        let episode = makeEpisode(
            podcast: podcast,
            coverage: nil,
            anchor: 42.5,
            in: context
        )
        XCTAssertFalse(
            libraryRowShouldShowReadinessCheckmark(episode: episode),
            "Nil coverage is .none — no ✓"
        )
    }

    func testDeferredOnlyReadinessHidesCheckmark() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        // Non-empty coverage + nil anchor → .deferredOnly → no checkmark.
        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...1000.0]),
            anchor: nil,
            in: context
        )
        XCTAssertFalse(
            libraryRowShouldShowReadinessCheckmark(episode: episode),
            "Non-empty coverage with nil anchor is .deferredOnly — no ✓"
        )
    }

    func testProximalReadinessShowsCheckmark() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...1000.0]),
            anchor: 42.5,
            in: context
        )
        XCTAssertTrue(
            libraryRowShouldShowReadinessCheckmark(episode: episode),
            "Proximal readiness must render ✓"
        )
    }

    func testCompleteReadinessShowsCheckmarkAtAnyAnchor() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let episode = makeEpisode(
            podcast: podcast,
            coverage: makeCoverage(ranges: [0.0...3600.0], isComplete: true),
            anchor: nil,
            in: context
        )
        XCTAssertTrue(
            libraryRowShouldShowReadinessCheckmark(episode: episode),
            ".complete renders ✓ even with a nil anchor"
        )

        episode.playbackAnchor = 2400.0
        XCTAssertTrue(
            libraryRowShouldShowReadinessCheckmark(episode: episode),
            ".complete renders ✓ regardless of anchor position"
        )
    }

    // MARK: - Persistence round-trip

    /// The Library cell's derivation must survive a SwiftData save +
    /// fetch cycle. If the Codable encoding of CoverageSummary drops a
    /// field, the checkmark would silently disappear on the next launch.
    func testCoverageAndAnchorRoundTripThroughSwiftData() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let coverage = makeCoverage(ranges: [0.0...1000.0])
        let episode = makeEpisode(
            podcast: podcast,
            coverage: coverage,
            anchor: 42.5,
            in: context
        )
        try context.save()
        let episodeId = episode.canonicalEpisodeKey

        let descriptor = FetchDescriptor<Episode>(
            predicate: #Predicate<Episode> { $0.canonicalEpisodeKey == episodeId }
        )
        let fetched = try XCTUnwrap(context.fetch(descriptor).first)
        XCTAssertEqual(fetched.coverageSummary, coverage)
        XCTAssertEqual(fetched.playbackAnchor, 42.5)
        XCTAssertTrue(
            libraryRowShouldShowReadinessCheckmark(episode: fetched),
            "Checkmark must survive a SwiftData save+fetch round trip"
        )
    }
}
