// LibraryViewUnplayedCountPerfTests.swift
// playhead-fijb: regression test for `PodcastGridCell.unplayedCount` filter
// running on every cell-body redraw. Pre-fix the `unplayedCount` computed
// property iterated `podcast.episodes` (an O(N) SwiftData relationship
// fetch) per cell per frame, causing visible jank on the Library tab.
//
// Post-fix the parent `LibraryView` precomputes a
// `[Podcast.ID: Int]` map once per body evaluation and passes the int into
// each cell. The cell becomes a pure pass-through — no relationship
// traversal during scroll.
//
// This test pins the parent precompute under a 50ms wall-clock budget at
// N=50 podcasts × 50 episodes each. The pre-fix path filtered
// `podcast.episodes` once per cell on every redraw; the post-fix path
// builds the dictionary in a single pass over `podcasts`.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@Suite("LibraryView — unplayed-count precompute (playhead-fijb)")
struct LibraryViewUnplayedCountPerfTests {

    /// Parent-view precompute must stay well under a frame budget at the
    /// realistic upper-bound dataset (50 podcasts × 50 episodes/podcast).
    /// The function under test is the production helper invoked from
    /// `LibraryView.body`; calling it directly gives a deterministic
    /// wall-clock measurement that doesn't depend on SwiftUI rendering.
    @Test("computeUnplayedCounts stays under 50ms at N=50x50")
    @MainActor
    func computeUnplayedCountsDoesNotJank() throws {
        let podcastCount = 50
        let episodesPerPodcast = 50

        // SwiftData container — same schema as production.
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: [config])
        let context = ModelContext(container)

        // Seed N podcasts, each with E episodes (half played, half unplayed).
        var podcasts: [Podcast] = []
        podcasts.reserveCapacity(podcastCount)
        for p in 0..<podcastCount {
            let feedURL = URL(string: "https://example.com/fijb-perf-\(p).rss")!
            let podcast = Podcast(
                feedURL: feedURL,
                title: "Show \(p)",
                author: "Author \(p)",
                artworkURL: nil,
                episodes: [],
                subscribedAt: .now
            )
            context.insert(podcast)
            for e in 0..<episodesPerPodcast {
                let episode = Episode(
                    feedItemGUID: "p\(p)-e\(e)",
                    feedURL: feedURL,
                    podcast: podcast,
                    title: "Episode \(e)",
                    audioURL: URL(string: "https://example.com/p\(p)/e\(e).mp3")!,
                    isPlayed: e.isMultiple(of: 2)
                )
                context.insert(episode)
            }
            podcasts.append(podcast)
        }
        try context.save()

        // Warm-up — first relationship traversal pays a one-time cost as
        // SwiftData faults the inverse. We measure the steady-state body
        // re-evaluation, which is what jank-during-scroll actually feels
        // like.
        _ = LibraryView.computeUnplayedCounts(for: podcasts)

        let start = Date()
        let counts = LibraryView.computeUnplayedCounts(for: podcasts)
        let elapsed = Date().timeIntervalSince(start)

        // Sanity: every podcast yields its expected unplayed total
        // (every odd-index episode is unplayed = E/2 per podcast).
        #expect(counts.count == podcastCount)
        for podcast in podcasts {
            #expect(counts[podcast.id] == episodesPerPodcast / 2)
        }

        // Wall-clock budget. Simulator-under-CI noise is the dominant
        // factor here; 50ms is comfortably above measured local runs
        // (sub-10ms once SwiftData has faulted the relationship) but
        // would be impossible if a regression reintroduced an O(N*M)
        // path that re-filtered every cell every frame.
        #expect(elapsed < 0.05,
                "computeUnplayedCounts took \(elapsed)s; budget is 0.05s at N=\(podcastCount)x\(episodesPerPodcast)")
    }
}
