// FineFeatureBandCache.swift
// ef2.3.7: Cache extracted features for ±3s bands around candidate boundaries.
// Actor-based for thread safety. Evicts when episode changes.
//
// SHADOW MODE only — not wired into live skip pipeline.

import Foundation

// MARK: - FineFeatureBandCache

/// Thread-safe cache for feature windows extracted around candidate boundaries.
/// Keyed by (episodeId, candidateBand) to avoid redundant extraction.
actor FineFeatureBandCache {

    // MARK: - Types

    /// Composite key: episode + quantized boundary band.
    struct BandKey: Hashable, Sendable {
        let episodeId: String
        /// Center of the band (candidate boundary time), quantized to hop resolution.
        let bandCenter: Double
    }

    /// Cached feature band: the windows covering ±radius around a candidate.
    struct CachedBand: Sendable {
        let features: [FeatureWindow]
        let bandCenter: Double
        let radius: Double
    }

    // MARK: - State

    private var currentEpisodeId: String?
    private var cache: [BandKey: CachedBand] = [:]

    // MARK: - Public API

    /// Retrieve cached features for a band, or nil if not cached.
    func get(episodeId: String, bandCenter: Double) -> CachedBand? {
        evictIfEpisodeChanged(episodeId)
        let key = BandKey(episodeId: episodeId, bandCenter: quantize(bandCenter))
        return cache[key]
    }

    /// Store features for a band.
    func put(
        episodeId: String,
        bandCenter: Double,
        radius: Double,
        features: [FeatureWindow]
    ) {
        evictIfEpisodeChanged(episodeId)
        let key = BandKey(episodeId: episodeId, bandCenter: quantize(bandCenter))
        cache[key] = CachedBand(
            features: features,
            bandCenter: bandCenter,
            radius: radius
        )
    }

    /// Number of cached bands (for diagnostics).
    var count: Int { cache.count }

    /// Force-clear all cached data.
    func clear() {
        cache.removeAll()
        currentEpisodeId = nil
    }

    // MARK: - Internals

    /// Evict all cached bands if the episode has changed.
    private func evictIfEpisodeChanged(_ episodeId: String) {
        if currentEpisodeId != episodeId {
            cache.removeAll()
            currentEpisodeId = episodeId
        }
    }

    /// Quantize band center to 50ms resolution to improve cache hit rate
    /// when candidates are close but not identical.
    private func quantize(_ value: Double) -> Double {
        (value * 20.0).rounded() / 20.0
    }
}
