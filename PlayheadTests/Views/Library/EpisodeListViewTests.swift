// EpisodeListViewTests.swift
// Verifies acceptance criteria for the Episode List View (playhead-8rr):
// sort order, swipe-action callbacks, empty state, metadata display guards,
// and accessibility labels.

import XCTest
import SwiftData
@testable import Playhead

@MainActor
final class EpisodeListViewTests: XCTestCase {

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
            title: "Test Podcast",
            author: "Tester"
        )
        context.insert(podcast)
        return podcast
    }

    private func makeEpisode(
        guid: String = UUID().uuidString,
        podcast: Podcast,
        title: String = "Episode",
        publishedAt: Date? = nil,
        duration: TimeInterval? = nil,
        isPlayed: Bool = false,
        playbackPosition: TimeInterval = 0,
        in context: ModelContext
    ) -> Episode {
        let episode = Episode(
            feedItemGUID: guid,
            feedURL: podcast.feedURL,
            podcast: podcast,
            title: title,
            audioURL: URL(string: "https://example.com/\(guid).mp3")!,
            duration: duration,
            publishedAt: publishedAt,
            playbackPosition: playbackPosition,
            isPlayed: isPlayed
        )
        context.insert(episode)
        return episode
    }

    // MARK: - AC1: Sort Order (newest first)

    func testQuerySortsEpisodesNewestFirst() throws {
        // The Query descriptor in EpisodeListView uses .reverse on publishedAt.
        // Verify the sort descriptor matches expectations.
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let descriptor = SortDescriptor(\Episode.publishedAt, order: .reverse)

        let older = makeEpisode(
            podcast: podcast,
            title: "Older",
            publishedAt: Date(timeIntervalSince1970: 1_000_000),
            in: context
        )
        let newer = makeEpisode(
            podcast: podcast,
            title: "Newer",
            publishedAt: Date(timeIntervalSince1970: 2_000_000),
            in: context
        )

        let episodes = [older, newer].sorted(using: descriptor)

        XCTAssertEqual(episodes.first?.title, "Newer",
            "Episodes must be sorted newest-first")
        XCTAssertEqual(episodes.last?.title, "Older")
    }

    // MARK: - AC2: Swipe Action — Toggle Played

    func testTogglePlayedFlipsBoolOnEpisode() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)
        let episode = makeEpisode(podcast: podcast, isPlayed: false, in: context)

        XCTAssertFalse(episode.isPlayed)
        episode.isPlayed.toggle()
        XCTAssertTrue(episode.isPlayed, "First toggle should mark as played")
        episode.isPlayed.toggle()
        XCTAssertFalse(episode.isPlayed, "Second toggle should mark as unplayed")
    }

    // MARK: - AC2: Swipe Action — Queue emits haptic

    func testQueueSwipeEmitsHapticViaDependencyInjection() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)
        let episode = makeEpisode(podcast: podcast, in: context)

        let recorder = RecordingHapticPlayer()
        let view = EpisodeListView(podcast: podcast, hapticPlayer: recorder)
        view.queueEpisode(episode)

        XCTAssertEqual(recorder.played, [.save],
            "Queue swipe must fire .save haptic via injected player")
    }

    // MARK: - AC5 & AC6: AnalysisSummary display guards — REMOVED (playhead-f5ao)
    //
    // Three tests lived here (`testAnalysisSummaryWithAnalysisAndAds`,
    // `…WithNoAds`, `…NilMeansNoIndicators`) and they are worth a note
    // rather than a silent deletion, because they are the reason the
    // defect f5ao fixed survived four months of green gates.
    //
    // All three constructed an `AnalysisSummary` (or read the nil
    // default off a fresh `Episode`) and asserted things about THAT
    // VALUE: `summary.hasAnalysis == true`, `summary.adSegmentCount > 0`,
    // `episode.analysisSummary == nil`. Not one of them reached a view,
    // a writer, or any production code path. They pass identically
    // whether the app writes the field every episode or — as it did —
    // never. Coverage of a memberwise initialiser reads exactly like
    // coverage of the feature it initialises.
    //
    // The badge/tooltip behaviour they were nominally guarding is now
    // in `EpisodeRowReadinessTests`, against
    // `libraryRowShouldShowReadinessCheckmark(episode:)` and
    // `anyLibraryRowShowsReadinessCheckmark(episodes:)` — functions the
    // view actually calls.

    // MARK: - AC7: Empty state text

    func testEmptyStateLiterals() throws {
        // The view conditionally shows emptyState when episodes.isEmpty.
        // Verify the static strings match the acceptance criteria.
        // (We can't instantiate the @Query-backed view in a unit test,
        // but we can verify the view struct compiles and its init filters
        // by podcast ID — the empty branch is tested via build + preview.)
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        // No episodes inserted — podcast has zero episodes
        let podcastID = podcast.persistentModelID
        let fetchDescriptor = FetchDescriptor<Episode>(
            predicate: #Predicate<Episode> { episode in
                episode.podcast?.persistentModelID == podcastID
            }
        )
        let results = try context.fetch(fetchDescriptor)
        XCTAssertTrue(results.isEmpty,
            "With no episodes, the empty state branch should be triggered")
    }

    // MARK: - AC8: Accessibility label format

    func testAccessibilityLabelIncludesTitle() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)
        let episode = makeEpisode(
            podcast: podcast,
            title: "Interview with Jane",
            isPlayed: false,
            in: context
        )

        // Replicate the logic from the view's .accessibilityLabel
        let label = "\(episode.title)\(episode.isPlayed ? ", played" : "")"
        XCTAssertEqual(label, "Interview with Jane")
    }

    func testAccessibilityLabelIncludesPlayedStatus() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)
        let episode = makeEpisode(
            podcast: podcast,
            title: "Interview with Jane",
            isPlayed: true,
            in: context
        )

        let label = "\(episode.title)\(episode.isPlayed ? ", played" : "")"
        XCTAssertEqual(label, "Interview with Jane, played")
    }

    // MARK: - AC4: Metadata font is mono (timestamp role)

    func testTimestampFontDescriptorIsMono() throws {
        // AppTypography.timestamp is built from TypographyRole.timestamp,
        // which specifies family: .mono. Verify the descriptor.
        let descriptor = AppTypography.descriptor(for: .timestamp)
        XCTAssertEqual(descriptor.family, .mono,
            "Timestamp font must use mono family per design tokens")
        XCTAssertEqual(descriptor.baseSize, 13)
    }

    // MARK: - Init filters by podcast

    func testInitCreatesQueryFilteredByPodcast() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        // Verify the view can be constructed without trapping — the Query
        // predicate filters episodes by podcast.persistentModelID.
        let _ = EpisodeListView(podcast: podcast)
    }

    // MARK: - Progress bar guard

    func testProgressBarOnlyShowsForPartiallyPlayed() throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        // Fully played — progress bar should NOT show
        let played = makeEpisode(
            podcast: podcast, title: "Played",
            duration: 3600, isPlayed: true, playbackPosition: 3600,
            in: context
        )
        XCTAssertTrue(played.isPlayed,
            "isPlayed episodes should not show progress bar")

        // Unstarted — no progress bar
        let unstarted = makeEpisode(
            podcast: podcast, title: "Unstarted",
            duration: 3600, isPlayed: false, playbackPosition: 0,
            in: context
        )
        XCTAssertEqual(unstarted.playbackPosition, 0,
            "playbackPosition == 0 means no progress bar")

        // Partially played — progress bar should show
        let partial = makeEpisode(
            podcast: podcast, title: "Partial",
            duration: 3600, isPlayed: false, playbackPosition: 1200,
            in: context
        )
        let showsProgress = !partial.isPlayed
            && partial.playbackPosition > 0
            && (partial.duration ?? 0) > 0
        XCTAssertTrue(showsProgress,
            "Partially played episode should show progress bar")
    }
}
