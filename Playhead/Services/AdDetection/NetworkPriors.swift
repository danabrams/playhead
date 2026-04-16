// NetworkPriors.swift
// ef2.5.2: Network-level priors aggregated across same-network shows.
//
// Aggregates cross-show priors for a podcast network: common sponsors,
// typical ad slot positions, ad duration range, music bracket prevalence,
// and metadata trust. Uses confidence-weighted aggregation with outlier
// trimming (not raw averages).
//
// Not wired into the live pipeline — ef2.5.3 handles integration.

import Foundation

// MARK: - NetworkPriors

/// Aggregated network-level priors derived from multiple shows in the
/// same podcast network. Used as a weak prior for new or low-data shows.
struct NetworkPriors: Sendable, Equatable {
    /// Sponsor name (lowercased) → frequency across network shows (0-1).
    let commonSponsors: [String: Float]
    /// Normalized positions (0-1) where ads typically appear in episodes.
    let typicalSlotPositions: [Float]
    /// Typical ad duration range across the network.
    let typicalAdDuration: ClosedRange<TimeInterval>
    /// How common music brackets are as ad boundary markers (0-1).
    let musicBracketPrevalence: Float
    /// Average metadata reliability across network shows (0-1).
    let metadataTrustAverage: Float
    /// Number of shows contributing to these priors.
    let showCount: Int

    /// Compute the decayed weight for blending network priors with show-level data.
    /// Weight starts at 0.5 and decays linearly to 0 as episodesObserved approaches 10.
    ///
    /// Formula: `0.5 × max(0, 1 - episodesObserved / 10)`
    static func decayedWeight(episodesObserved: Int) -> Float {
        0.5 * max(0, 1.0 - Float(episodesObserved) / 10.0)
    }
}

// MARK: - ShowPriorSnapshot

/// A snapshot of a single show's priors, used as input to the aggregator.
struct ShowPriorSnapshot: Sendable {
    /// Sponsors observed on this show, lowercased → frequency (0-1).
    let sponsors: [String: Float]
    /// Normalized ad slot positions observed (0-1).
    let slotPositions: [Float]
    /// Average ad duration in seconds.
    let averageAdDuration: TimeInterval
    /// Whether music brackets are common on this show (0-1).
    let musicBracketRate: Float
    /// Metadata trust score for this show (0-1).
    let metadataTrust: Float
    /// Weight for this show's contribution (e.g. based on episode count).
    let weight: Float
}

// MARK: - NetworkPriorAggregator

/// Stateless aggregator that builds NetworkPriors from a collection of
/// per-show snapshots. Uses confidence-weighted aggregation with outlier
/// trimming.
enum NetworkPriorAggregator {

    // MARK: - Public API

    /// Aggregate per-show snapshots into network-level priors.
    ///
    /// - Parameter snapshots: One snapshot per show in the network.
    /// - Returns: Aggregated priors, or nil if no snapshots provided.
    static func aggregate(_ snapshots: [ShowPriorSnapshot]) -> NetworkPriors? {
        guard !snapshots.isEmpty else { return nil }

        let sponsors = aggregateSponsors(snapshots)
        let positions = aggregatePositions(snapshots)
        let durationRange = aggregateDuration(snapshots)
        let musicPrevalence = weightedAverage(
            snapshots.map { ($0.musicBracketRate, $0.weight) }
        )
        let metadataTrust = weightedAverage(
            snapshots.map { ($0.metadataTrust, $0.weight) }
        )

        return NetworkPriors(
            commonSponsors: sponsors,
            typicalSlotPositions: positions,
            typicalAdDuration: durationRange,
            musicBracketPrevalence: musicPrevalence,
            metadataTrustAverage: metadataTrust,
            showCount: snapshots.count
        )
    }

    // MARK: - Sponsor Aggregation

    /// Merge sponsor dictionaries, weighted by show weight, and keep only
    /// sponsors appearing in >1 show (network-level signal, not single-show noise).
    static func aggregateSponsors(_ snapshots: [ShowPriorSnapshot]) -> [String: Float] {
        var sponsorWeights: [String: Float] = [:]
        var sponsorShowCounts: [String: Int] = [:]
        var totalWeight: Float = 0

        for snap in snapshots {
            totalWeight += snap.weight
            for (sponsor, freq) in snap.sponsors {
                sponsorWeights[sponsor, default: 0] += freq * snap.weight
                sponsorShowCounts[sponsor, default: 0] += 1
            }
        }

        guard totalWeight > 0 else { return [:] }

        // Only keep sponsors seen on more than one show (for networks with >1 show).
        let minShows = snapshots.count > 1 ? 2 : 1
        var result: [String: Float] = [:]
        for (sponsor, weightedSum) in sponsorWeights {
            if (sponsorShowCounts[sponsor] ?? 0) >= minShows {
                result[sponsor] = weightedSum / totalWeight
            }
        }
        return result
    }

    // MARK: - Position Aggregation

    /// Aggregate slot positions with outlier trimming. Clusters nearby positions
    /// (within 0.05 normalized distance) and returns cluster centroids.
    static func aggregatePositions(_ snapshots: [ShowPriorSnapshot]) -> [Float] {
        var allPositions: [(position: Float, weight: Float)] = []
        for snap in snapshots {
            for pos in snap.slotPositions {
                allPositions.append((pos, snap.weight))
            }
        }

        guard !allPositions.isEmpty else { return [] }

        // Sort by position for clustering.
        allPositions.sort(by: { $0.position < $1.position })

        // Adapt to trimOutliers' (value:weight:) shape.
        let forTrimming = allPositions.map { (value: $0.position, weight: $0.weight) }
        let trimmed = trimOutliers(forTrimming, fraction: 0.1)
        let backToPositions = trimmed.map { (position: $0.value, weight: $0.weight) }

        // Simple greedy clustering: merge positions within 0.05 of each other.
        return clusterPositions(backToPositions, radius: 0.05)
    }

    // MARK: - Duration Aggregation

    /// Aggregate ad durations into a range, trimming outliers.
    static func aggregateDuration(_ snapshots: [ShowPriorSnapshot]) -> ClosedRange<TimeInterval> {
        let durations: [(value: Float, weight: Float)] = snapshots.map {
            (Float($0.averageAdDuration), $0.weight)
        }

        guard !durations.isEmpty else { return 30...90 }

        let trimmed = trimOutliers(durations, fraction: 0.1)

        guard !trimmed.isEmpty else { return 30...90 }

        let values = trimmed.map(\.value)
        let minVal = TimeInterval(values.min()!)
        let maxVal = TimeInterval(values.max()!)

        // Ensure a minimum range width of 10 seconds.
        if maxVal - minVal < 10 {
            let mid = (minVal + maxVal) / 2
            return (mid - 5)...(mid + 5)
        }
        return minVal...maxVal
    }

    // MARK: - Utilities

    /// Compute a weighted average from (value, weight) pairs.
    static func weightedAverage(_ pairs: [(value: Float, weight: Float)]) -> Float {
        let totalWeight = pairs.reduce(Float(0)) { $0 + $1.weight }
        guard totalWeight > 0 else { return 0 }
        let weightedSum = pairs.reduce(Float(0)) { $0 + $1.value * $1.weight }
        return weightedSum / totalWeight
    }

    /// Trim the top and bottom fraction of values (by the value field).
    /// Returns the middle portion. Input must be sorted by value.
    static func trimOutliers<T>(
        _ sorted: [(value: Float, weight: T)],
        fraction: Float
    ) -> [(value: Float, weight: T)] {
        let count = sorted.count
        guard count >= 4 else { return sorted } // Don't trim tiny samples.
        let trimCount = max(1, Int(Float(count) * fraction))
        let start = trimCount
        let end = count - trimCount
        guard start < end else { return sorted }
        return Array(sorted[start..<end])
    }

    /// Cluster positions within `radius` of each other, returning weighted centroids.
    static func clusterPositions(
        _ positions: [(position: Float, weight: Float)],
        radius: Float
    ) -> [Float] {
        guard !positions.isEmpty else { return [] }

        var clusters: [(centroid: Float, totalWeight: Float)] = []

        for (pos, weight) in positions {
            if let idx = clusters.firstIndex(where: { abs($0.centroid - pos) <= radius }) {
                // Merge into existing cluster.
                let old = clusters[idx]
                let newWeight = old.totalWeight + weight
                let newCentroid = (old.centroid * old.totalWeight + pos * weight) / newWeight
                clusters[idx] = (newCentroid, newWeight)
            } else {
                clusters.append((pos, weight))
            }
        }

        return clusters.map(\.centroid).sorted()
    }
}

// MARK: - NetworkPriorStore

/// In-memory cache of network priors, keyed by networkId.
/// Thread-safe via actor isolation.
actor NetworkPriorStore {

    private var cache: [String: NetworkPriors] = [:]

    /// Retrieve cached priors for a network.
    func priors(forNetwork networkId: String) -> NetworkPriors? {
        cache[networkId]
    }

    /// Store or update priors for a network.
    func update(priors: NetworkPriors, forNetwork networkId: String) {
        cache[networkId] = priors
    }

    /// Remove priors for a network.
    func remove(forNetwork networkId: String) {
        cache[networkId] = nil
    }

    /// Number of cached networks.
    var count: Int {
        cache.count
    }

    /// All cached network IDs.
    var networkIds: Set<String> {
        Set(cache.keys)
    }
}
