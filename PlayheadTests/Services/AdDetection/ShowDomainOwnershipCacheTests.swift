import Foundation
import Testing
@testable import Playhead

/// playhead-dyvy — the per-show ownership graph is computed once per feed
/// change, not once per lookup.
@MainActor
@Suite("playhead-dyvy: the ownership graph is walked once per feed change")
struct ShowDomainOwnershipCacheTests {

    private func ownership(_ tag: String) -> EpisodeMetadataSnapshot.ShowDomainOwnership {
        EpisodeMetadataSnapshot.ShowDomainOwnership(showOwned: [tag], ownershipUndetermined: [])
    }

    @Test("the same show at the same size is computed once across many lookups")
    func sameShowSameSizeComputesOnce() {
        let cache = EpisodeMetadataSnapshot.ShowDomainOwnershipCache()
        var walks = 0
        for _ in 0..<50 {
            let got = cache.ownership(podcastId: "show-a", episodeCount: 724) {
                walks += 1
                return ownership("a")
            }
            #expect(got == ownership("a"))
        }
        #expect(walks == 1, "fifty lookups of one show must walk its episodes once; walked \(walks) times")
        #expect(cache.computeCount == 1)
    }

    @Test("a feed that grew is walked again, and the new graph replaces the old")
    func grownFeedIsRecomputed() {
        let cache = EpisodeMetadataSnapshot.ShowDomainOwnershipCache()
        _ = cache.ownership(podcastId: "show-a", episodeCount: 724) { ownership("old") }
        let after = cache.ownership(podcastId: "show-a", episodeCount: 725) { ownership("new") }
        #expect(after == ownership("new"), "a refresh landed an episode; the graph must be rebuilt from it")
        #expect(cache.computeCount == 2)
        let again = cache.ownership(podcastId: "show-a", episodeCount: 725) { ownership("should-not-run") }
        #expect(again == ownership("new"))
        #expect(cache.computeCount == 2, "and the rebuilt graph is then served from the cache")
    }

    @Test("two shows do not share a graph")
    func showsAreIndependent() {
        let cache = EpisodeMetadataSnapshot.ShowDomainOwnershipCache()
        let a = cache.ownership(podcastId: "show-a", episodeCount: 10) { ownership("a") }
        let b = cache.ownership(podcastId: "show-b", episodeCount: 10) { ownership("b") }
        #expect(a != b)
        #expect(cache.computeCount == 2)
    }
}
