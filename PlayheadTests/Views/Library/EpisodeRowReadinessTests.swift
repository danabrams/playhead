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
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
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

    /// Regression guard for the Decodable normalization path. The
    /// designated init normalizes `coverageRanges` (sorts + merges) and
    /// derives `firstCoveredOffset`. If the compiler-synthesized decoder
    /// runs it would skip that step, and an externally-written or hand-
    /// mutated JSON row with unsorted/overlapping ranges (or a stale
    /// `firstCoveredOffset`) would decode into a non-canonical instance.
    /// This test feeds exactly such a pathological JSON payload through
    /// the decode path and asserts the decoded value is canonical.
    ///
    /// Pairs with the SwiftData round-trip above — together they cover
    /// both "encode produces canonical JSON" (round-trip) AND "decode
    /// canonicalizes non-canonical JSON" (this test).
    func testDecodableNormalizesOutOfOrderCoverageJSON() throws {
        // Intentionally unsorted + overlapping ranges + a deliberately
        // wrong `firstCoveredOffset` (2000 is larger than every lower
        // bound). ClosedRange<Double> encodes as a two-element JSON array
        // [lowerBound, upperBound] (Foundation's default encoding for
        // ClosedRange where the bound is Codable).
        let badJSON = """
        {
          "coverageRanges": [[500.0, 1000.0], [0.0, 100.0], [50.0, 200.0]],
          "firstCoveredOffset": 2000.0,
          "isComplete": false,
          "modelVersion": "m1",
          "policyVersion": 1,
          "featureSchemaVersion": 1,
          "updatedAt": 1700000000
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        let decoded = try decoder.decode(CoverageSummary.self, from: badJSON)

        // Ranges must be sorted ascending by lowerBound AND non-
        // overlapping — the two pathological inputs [0,100] and [50,200]
        // overlap, so the normalizer merges them into [0,200]. Combined
        // with [500,1000] that leaves exactly two canonical ranges.
        XCTAssertEqual(decoded.coverageRanges, [0.0...200.0, 500.0...1000.0])
        // The designated init derives `firstCoveredOffset` from the
        // normalized ranges — the deliberately-wrong persisted value
        // (2000.0) must be discarded, not trusted.
        XCTAssertEqual(decoded.firstCoveredOffset, decoded.coverageRanges.first?.lowerBound)
        XCTAssertEqual(decoded.firstCoveredOffset, 0.0)

        // And the derivation on the decoded instance matches what the
        // ranges actually support: the canonical normalized ranges are
        // [0, 200] and [500, 1000]; at anchor 42.5 the lookahead window
        // [42.5, 942.5] is not contained in either range — first range
        // ends at 200, second starts at 500 — so the correct derivation
        // is `.deferredOnly`. This is the full path from raw JSON
        // through the decode + normalize pipeline to the derivation.
        XCTAssertEqual(decoded.readiness(anchor: 42.5), .deferredOnly)
    }

    // MARK: - First-✓ tooltip trigger (playhead-f5ao)
    //
    // `FirstCheckmarkTooltipView` says "✓ means we've found ads to
    // skip". It is therefore only ever correct to present it when a ✓
    // is actually on screen, and the ONLY defensible definition of its
    // trigger is the badge predicate itself.
    //
    // That is not how it shipped. `rw49` wired the trigger to
    // `episodes.contains { $0.analysisSummary?.hasAnalysis == true }`
    // at 16:16 on 2026-04-20 — the badge's gate at that moment — and
    // `cthe` re-gated the badge on
    // `derivePlaybackReadiness(coverage:anchor:)` at 16:20. Nothing
    // compared the two, so for four months the tooltip's trigger and
    // the thing it explains were different predicates over different
    // fields. (Neither ever fired, because neither field has a
    // producer, which is what hid it.)
    //
    // These tests compare them. `testTriggerAgreesWithBadgeOnEvery
    // Readiness` is the one that would have gone red at 16:20.

    /// The trigger and the badge must agree on all four readiness
    /// states — not merely "both are false today".
    func testTriggerAgreesWithBadgeOnEveryReadiness() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        // (label, coverage, anchor) spanning .none / .deferredOnly /
        // .proximal / .complete.
        let cases: [(String, CoverageSummary?, TimeInterval?)] = [
            ("none — no coverage at all", nil, 42.5),
            ("none — no coverage, no anchor", nil, nil),
            ("deferredOnly — coverage but no anchor",
             makeCoverage(ranges: [0.0...1000.0]), nil),
            ("deferredOnly — anchor outside coverage",
             makeCoverage(ranges: [0.0...1000.0]), 2000.0),
            ("proximal — anchor inside coverage",
             makeCoverage(ranges: [0.0...1000.0]), 42.5),
            ("complete — isComplete, no anchor",
             makeCoverage(ranges: [0.0...3600.0], isComplete: true), nil),
        ]

        for (label, coverage, anchor) in cases {
            let episode = makeEpisode(
                podcast: podcast,
                coverage: coverage,
                anchor: anchor,
                in: context
            )
            let badge = libraryRowShouldShowReadinessCheckmark(episode: episode)
            let trigger = anyLibraryRowShowsReadinessCheckmark(episodes: [episode])
            XCTAssertEqual(
                trigger, badge,
                "The first-✓ tooltip trigger must equal the ✓ badge predicate "
                + "for a single-episode list (\(label)). A tooltip that "
                + "explains the ✓ cannot fire off a different quantity."
            )
        }
    }

    /// A list whose episodes ALL have analysis somewhere but none near
    /// the anchor renders no ✓, so the tooltip must not fire. This is
    /// the case the pre-f5ao trigger got wrong by construction: it
    /// asked "has any analysis?" where the badge asks "is analysis
    /// usable from here?".
    func testTriggerIsFalseWhenEveryRowIsDeferredOnly() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let episodes = (0..<3).map { index in
            makeEpisode(
                podcast: podcast,
                coverage: makeCoverage(ranges: [0.0...1000.0]),
                anchor: 2000.0 + Double(index),
                in: context
            )
        }

        XCTAssertFalse(
            episodes.contains { libraryRowShouldShowReadinessCheckmark(episode: $0) },
            "Precondition: no row in this fixture renders a ✓"
        )
        XCTAssertFalse(
            anyLibraryRowShowsReadinessCheckmark(episodes: episodes),
            "Analysis existing somewhere is not a ✓ on screen — the tooltip "
            + "must stay hidden while every row is .deferredOnly"
        )
    }

    /// The trigger scans the whole list, not just the head. The Library
    /// sorts newest-first and the analyzed episode is very often not the
    /// newest one.
    func testTriggerFindsAReadyRowThatIsNotFirst() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        var episodes = (0..<4).map { _ in
            makeEpisode(podcast: podcast, coverage: nil, anchor: nil, in: context)
        }
        episodes.append(
            makeEpisode(
                podcast: podcast,
                coverage: makeCoverage(ranges: [0.0...1000.0]),
                anchor: 42.5,
                in: context
            )
        )

        XCTAssertTrue(
            anyLibraryRowShowsReadinessCheckmark(episodes: episodes),
            "A ✓ on the LAST row must still trigger the tooltip"
        )
        XCTAssertFalse(
            anyLibraryRowShowsReadinessCheckmark(episodes: Array(episodes.dropLast())),
            "…and removing that row must take the trigger back down"
        )
    }

    func testTriggerIsFalseForAnEmptyList() {
        XCTAssertFalse(
            anyLibraryRowShowsReadinessCheckmark(episodes: []),
            "An empty list has no ✓ to point at"
        )
    }
}
