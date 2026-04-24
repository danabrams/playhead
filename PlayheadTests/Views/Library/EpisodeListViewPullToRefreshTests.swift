// EpisodeListViewPullToRefreshTests.swift
// Verifies playhead-riu8: the per-show EpisodeListView exposes
// pull-to-refresh and routes it through an injectable refresher seam
// so one show's feed can be refreshed without navigating back to
// Library. Mirrors the `LibraryView.refreshAllFeeds` silent
// partial-refresh pattern.
//
// The tests pair a behavioural assertion on the extracted helper
// (`performEpisodeRefresh` calls the refresher exactly once with the
// correct podcast and swallows errors) with a source-level canary on
// `EpisodeListView.swift` (the `.refreshable` modifier and the helper
// wiring stay present). The canary is how we lock in the SwiftUI
// modifier without instantiating a live view.

import Foundation
import SwiftData
import Testing
@testable import Playhead

// MARK: - Recording Refresher (test double)

/// Minimal stand-in for `PodcastDiscoveryService` that records which
/// podcasts pull-to-refresh invoked it for. Used across the suite so
/// assertions can check call count and identity.
@MainActor
final class RecordingEpisodeRefresher: EpisodeRefreshing {

    struct Invocation {
        let podcast: Podcast
    }

    var invocations: [Invocation] = []

    /// When non-nil, the next call throws this error instead of
    /// recording an invocation payload. Used to prove the helper
    /// swallows errors (silent partial refresh).
    var nextErrorToThrow: Error?

    func refreshEpisodes(
        for podcast: Podcast,
        in context: ModelContext
    ) async throws -> [Episode] {
        invocations.append(Invocation(podcast: podcast))
        if let err = nextErrorToThrow {
            nextErrorToThrow = nil
            throw err
        }
        return []
    }
}

// MARK: - Suite

@MainActor
@Suite("EpisodeListView pull-to-refresh (playhead-riu8)")
struct EpisodeListViewPullToRefreshTests {

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
            title: "Test Show",
            author: "Tester"
        )
        context.insert(podcast)
        return podcast
    }

    // MARK: - Behavioural: performEpisodeRefresh helper

    @Test("performEpisodeRefresh invokes the refresher exactly once for the supplied podcast")
    func helperCallsRefresherOnce() async throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let refresher = RecordingEpisodeRefresher()

        await performEpisodeRefresh(
            refresher: refresher,
            podcast: podcast,
            modelContext: context
        )

        #expect(refresher.invocations.count == 1,
            "One pull-to-refresh must dispatch exactly one refresh call")
        #expect(refresher.invocations.first?.podcast === podcast,
            "The refresh call must target the podcast whose detail screen we're on")
    }

    @Test("performEpisodeRefresh swallows thrown errors (silent partial refresh)")
    func helperSwallowsErrors() async throws {
        let container = try makeContainer()
        let context = container.mainContext
        let podcast = makePodcast(in: context)

        let refresher = RecordingEpisodeRefresher()
        struct FakeError: Error {}
        refresher.nextErrorToThrow = FakeError()

        // Must not throw — the helper mirrors LibraryView.refreshAllFeeds,
        // which silently tolerates partial failure.
        await performEpisodeRefresh(
            refresher: refresher,
            podcast: podcast,
            modelContext: context
        )

        #expect(refresher.invocations.count == 1,
            "Errors must not prevent the invocation from being counted")
    }

    @Test("PodcastDiscoveryService conforms to EpisodeRefreshing so production wiring works")
    func discoveryServiceConforms() async throws {
        let service = PodcastDiscoveryService()
        let refresher: any EpisodeRefreshing = service
        #expect(refresher is PodcastDiscoveryService,
            "PodcastDiscoveryService must conform to EpisodeRefreshing")
    }

    // MARK: - Source canary: .refreshable wiring stays present

    private static let repoRoot: URL = {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // .../Library/
            .deletingLastPathComponent() // .../Views/
            .deletingLastPathComponent() // .../PlayheadTests/
            .deletingLastPathComponent() // .../<repo root>/
    }()

    private func readSource() throws -> String {
        let url = Self.repoRoot.appendingPathComponent(
            "Playhead/Views/Library/EpisodeListView.swift"
        )
        return try String(contentsOf: url, encoding: .utf8)
    }

    @Test("EpisodeListView source contains a .refreshable modifier")
    func sourceHasRefreshableModifier() throws {
        let source = try readSource()
        #expect(source.contains(".refreshable"),
            "EpisodeListView must expose pull-to-refresh via .refreshable")
    }

    @Test("EpisodeListView .refreshable closure dispatches to performEpisodeRefresh")
    func sourceRoutesRefreshableToHelper() throws {
        let source = try readSource()

        guard let refreshableRange = source.range(of: ".refreshable") else {
            Issue.record("No .refreshable found in EpisodeListView.swift")
            return
        }

        // Grab a window after the modifier to inspect its trailing
        // closure body without being sensitive to whitespace details.
        let windowEnd = source.index(
            refreshableRange.upperBound,
            offsetBy: 400,
            limitedBy: source.endIndex
        ) ?? source.endIndex
        let window = String(source[refreshableRange.upperBound ..< windowEnd])

        #expect(window.contains("performEpisodeRefresh"),
            "refreshable closure must dispatch via the testable performEpisodeRefresh helper")
    }
}
