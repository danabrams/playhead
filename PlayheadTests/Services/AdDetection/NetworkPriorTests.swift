import Foundation
import Testing

@testable import Playhead

@Suite("NetworkPriors")
struct NetworkPriorTests {

    // MARK: - Decay Formula

    @Test("decay weight is 0.5 at zero episodes observed")
    func decayAtZero() {
        #expect(NetworkPriors.decayedWeight(episodesObserved: 0) == 0.5)
    }

    @Test("decay weight is 0.25 at 5 episodes observed")
    func decayAtFive() {
        #expect(NetworkPriors.decayedWeight(episodesObserved: 5) == 0.25)
    }

    @Test("decay weight is 0 at 10 episodes observed")
    func decayAtTen() {
        #expect(NetworkPriors.decayedWeight(episodesObserved: 10) == 0.0)
    }

    @Test("decay weight is 0 beyond 10 episodes")
    func decayBeyondTen() {
        #expect(NetworkPriors.decayedWeight(episodesObserved: 15) == 0.0)
        #expect(NetworkPriors.decayedWeight(episodesObserved: 100) == 0.0)
    }

    @Test("decay is linear between 0 and 10")
    func decayLinear() {
        let w3 = NetworkPriors.decayedWeight(episodesObserved: 3)
        let w7 = NetworkPriors.decayedWeight(episodesObserved: 7)
        #expect(w3 > w7)
        // w3 = 0.5 * (1 - 0.3) = 0.35
        #expect(abs(w3 - 0.35) < 0.001)
        // w7 = 0.5 * (1 - 0.7) = 0.15
        #expect(abs(w7 - 0.15) < 0.001)
    }

    // MARK: - Aggregation: Basic

    @Test("aggregate returns nil for empty snapshots")
    func aggregateEmpty() {
        let result = NetworkPriorAggregator.aggregate([])
        #expect(result == nil)
    }

    @Test("aggregate single show returns that show's data")
    func aggregateSingleShow() {
        let snap = ShowPriorSnapshot(
            sponsors: ["squarespace": 0.8, "betterhelp": 0.5],
            slotPositions: [0.0, 0.5, 0.95],
            averageAdDuration: 60,
            musicBracketRate: 0.7,
            metadataTrust: 0.9,
            weight: 1.0
        )
        let result = NetworkPriorAggregator.aggregate([snap])!
        #expect(result.showCount == 1)
        // Single show: sponsors kept with minShows=1.
        #expect(result.commonSponsors["squarespace"] != nil)
        #expect(result.commonSponsors["betterhelp"] != nil)
        #expect(result.musicBracketPrevalence == 0.7)
        #expect(result.metadataTrustAverage == 0.9)
    }

    // MARK: - Sponsor Aggregation

    @Test("sponsors appearing in only one show are filtered out when network has multiple shows")
    func sponsorFiltering() {
        let snap1 = ShowPriorSnapshot(
            sponsors: ["squarespace": 0.8, "unique_sponsor_a": 0.5],
            slotPositions: [0.0],
            averageAdDuration: 60,
            musicBracketRate: 0.5,
            metadataTrust: 0.8,
            weight: 1.0
        )
        let snap2 = ShowPriorSnapshot(
            sponsors: ["squarespace": 0.6, "unique_sponsor_b": 0.9],
            slotPositions: [0.0],
            averageAdDuration: 60,
            musicBracketRate: 0.5,
            metadataTrust: 0.8,
            weight: 1.0
        )
        let result = NetworkPriorAggregator.aggregate([snap1, snap2])!
        // Squarespace appears in both shows — kept.
        #expect(result.commonSponsors["squarespace"] != nil)
        // Unique sponsors appear in one show only — filtered.
        #expect(result.commonSponsors["unique_sponsor_a"] == nil)
        #expect(result.commonSponsors["unique_sponsor_b"] == nil)
    }

    @Test("sponsor frequency is weight-averaged")
    func sponsorWeightedFrequency() {
        let snap1 = ShowPriorSnapshot(
            sponsors: ["sponsor": 0.8],
            slotPositions: [],
            averageAdDuration: 60,
            musicBracketRate: 0.5,
            metadataTrust: 0.8,
            weight: 3.0
        )
        let snap2 = ShowPriorSnapshot(
            sponsors: ["sponsor": 0.2],
            slotPositions: [],
            averageAdDuration: 60,
            musicBracketRate: 0.5,
            metadataTrust: 0.8,
            weight: 1.0
        )
        let result = NetworkPriorAggregator.aggregate([snap1, snap2])!
        // Weighted avg: (0.8*3 + 0.2*1) / (3+1) = 2.6/4 = 0.65
        let freq = result.commonSponsors["sponsor"]!
        #expect(abs(freq - 0.65) < 0.01)
    }

    // MARK: - Position Aggregation

    @Test("positions are clustered within radius")
    func positionClustering() {
        // Use clusterPositions directly to avoid outlier trimming interfering.
        let positions = NetworkPriorAggregator.clusterPositions(
            [(0.0, 1.0), (0.02, 1.0), (0.5, 1.0), (0.52, 1.0), (0.95, 1.0)],
            radius: 0.05
        )
        // 0.0 and 0.02 should cluster, 0.5 and 0.52 should cluster, 0.95 separate.
        #expect(positions.count == 3)
        // First cluster near 0.01, second near 0.51, third near 0.95.
        #expect(positions[0] < 0.05)
        #expect(positions[1] > 0.45 && positions[1] < 0.55)
        #expect(positions[2] > 0.9)
    }

    // MARK: - Duration Aggregation

    @Test("duration aggregation produces a valid range")
    func durationRange() {
        let snaps = (0..<10).map { i in
            ShowPriorSnapshot(
                sponsors: [:], slotPositions: [],
                averageAdDuration: 30 + Double(i) * 10,
                musicBracketRate: 0.5, metadataTrust: 0.8, weight: 1.0
            )
        }
        let result = NetworkPriorAggregator.aggregate(snaps)!
        #expect(result.typicalAdDuration.lowerBound > 0)
        #expect(result.typicalAdDuration.upperBound > result.typicalAdDuration.lowerBound)
    }

    @Test("duration range has minimum width of 10 seconds")
    func durationMinimumWidth() {
        let snaps = [
            ShowPriorSnapshot(
                sponsors: [:], slotPositions: [],
                averageAdDuration: 60,
                musicBracketRate: 0.5, metadataTrust: 0.8, weight: 1.0
            ),
            ShowPriorSnapshot(
                sponsors: [:], slotPositions: [],
                averageAdDuration: 61,
                musicBracketRate: 0.5, metadataTrust: 0.8, weight: 1.0
            ),
        ]
        let result = NetworkPriorAggregator.aggregate(snaps)!
        let width = result.typicalAdDuration.upperBound - result.typicalAdDuration.lowerBound
        #expect(width >= 10.0)
    }

    // MARK: - Weighted Average

    @Test("weighted average handles equal weights")
    func weightedAverageEqual() {
        let avg = NetworkPriorAggregator.weightedAverage([
            (value: 0.4, weight: 1.0),
            (value: 0.6, weight: 1.0),
        ])
        #expect(abs(avg - 0.5) < 0.001)
    }

    @Test("weighted average respects weights")
    func weightedAverageUnequal() {
        let avg = NetworkPriorAggregator.weightedAverage([
            (value: 1.0, weight: 3.0),
            (value: 0.0, weight: 1.0),
        ])
        #expect(abs(avg - 0.75) < 0.001)
    }

    @Test("weighted average returns 0 for empty input")
    func weightedAverageEmpty() {
        let avg = NetworkPriorAggregator.weightedAverage([])
        #expect(avg == 0)
    }

    // MARK: - Outlier Trimming

    @Test("trimOutliers removes top and bottom 10%")
    func outlierTrimming() {
        let sorted: [(value: Float, weight: Float)] = (0..<20).map {
            (Float($0), 1.0)
        }
        let trimmed = NetworkPriorAggregator.trimOutliers(sorted, fraction: 0.1)
        // 20 items, trim 2 from each end → 16 items.
        #expect(trimmed.count == 16)
        #expect(trimmed.first!.value == 2.0)
        #expect(trimmed.last!.value == 17.0)
    }

    @Test("trimOutliers does not trim tiny samples")
    func outlierTrimmingSmallSample() {
        let sorted: [(value: Float, weight: Float)] = [(1, 1), (2, 1), (3, 1)]
        let trimmed = NetworkPriorAggregator.trimOutliers(sorted, fraction: 0.1)
        #expect(trimmed.count == 3)
    }

    // MARK: - NetworkPriorStore

    @Test("store get/update/remove lifecycle")
    func storeLifecycle() async {
        let store = NetworkPriorStore()
        let priors = NetworkPriors(
            commonSponsors: ["test": 0.5],
            typicalSlotPositions: [0.0, 0.5],
            typicalAdDuration: 30...90,
            musicBracketPrevalence: 0.6,
            metadataTrustAverage: 0.8,
            showCount: 3
        )

        // Initially empty.
        let initial = await store.priors(forNetwork: "npr")
        #expect(initial == nil)
        #expect(await store.count == 0)

        // Update.
        await store.update(priors: priors, forNetwork: "npr")
        let fetched = await store.priors(forNetwork: "npr")
        #expect(fetched != nil)
        #expect(fetched!.showCount == 3)
        #expect(await store.count == 1)

        // Network IDs.
        let ids = await store.networkIds
        #expect(ids == ["npr"])

        // Remove.
        await store.remove(forNetwork: "npr")
        let afterRemove = await store.priors(forNetwork: "npr")
        #expect(afterRemove == nil)
        #expect(await store.count == 0)
    }

    @Test("store supports multiple networks")
    func storeMultipleNetworks() async {
        let store = NetworkPriorStore()
        let priors1 = NetworkPriors(
            commonSponsors: [:], typicalSlotPositions: [],
            typicalAdDuration: 30...60, musicBracketPrevalence: 0.5,
            metadataTrustAverage: 0.7, showCount: 2
        )
        let priors2 = NetworkPriors(
            commonSponsors: [:], typicalSlotPositions: [],
            typicalAdDuration: 45...90, musicBracketPrevalence: 0.8,
            metadataTrustAverage: 0.9, showCount: 5
        )

        await store.update(priors: priors1, forNetwork: "npr")
        await store.update(priors: priors2, forNetwork: "gimlet")

        #expect(await store.count == 2)
        #expect(await store.priors(forNetwork: "npr")?.showCount == 2)
        #expect(await store.priors(forNetwork: "gimlet")?.showCount == 5)
    }

    // MARK: - Sendable Conformance

    @Test("NetworkPriors is Sendable")
    func networkPriorsSendable() {
        let priors = NetworkPriors(
            commonSponsors: [:], typicalSlotPositions: [],
            typicalAdDuration: 30...60, musicBracketPrevalence: 0.5,
            metadataTrustAverage: 0.7, showCount: 1
        )
        let _: any Sendable = priors
        #expect(priors.showCount == 1)
    }

    @Test("ShowPriorSnapshot is Sendable")
    func showPriorSnapshotSendable() {
        let snap = ShowPriorSnapshot(
            sponsors: [:], slotPositions: [],
            averageAdDuration: 60, musicBracketRate: 0.5,
            metadataTrust: 0.8, weight: 1.0
        )
        let _: any Sendable = snap
        #expect(snap.weight == 1.0)
    }
}
