// MusicBracketTrustStore.swift
// ef2.3.6: Per-show Beta posterior trust for music-bed bracket reliability.
//
// playhead-arf8: graduated from SHADOW MODE for *reads*. The bracket-aware
// boundary refiner consults `trust(forShow:)` once per `runBackfill` to
// decide whether to engage the bracket path. Outcome accumulation
// (`recordOutcome(showId:hit:)`) remains intentionally untouched in this
// bead — there is no offline ground-truth signal yet to drive it, so
// every show stays at the `Beta(5,5)` prior mean (0.50). Hit/miss
// recording is scoped post-dogfood once a labelling source exists.
//
// Each show starts with Beta(5,5) = 0.50 mean trust. Bracket hit/miss
// feedback updates the posterior. Trust is persisted to SQLite via
// AnalysisStore so it survives app restarts.
//
// Design: actor wrapping AnalysisStore, mirroring SponsorKnowledgeStore pattern.

import Foundation
import OSLog

// MARK: - MusicBracketTrustStore

/// Actor-backed per-show trust store for music bracket reliability.
/// Delegates SQLite persistence to AnalysisStore.
actor MusicBracketTrustStore {

    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "MusicBracketTrustStore")

    /// In-memory cache of Beta parameters keyed by showId.
    /// Populated lazily on first access per show.
    private var cache: [String: BetaParameters] = [:]

    /// Default prior: Beta(5,5) gives mean 0.50 with moderate confidence.
    /// 10 pseudo-observations means real data takes ~10 episodes to dominate.
    static let defaultAlpha: Double = 5.0
    static let defaultBeta: Double = 5.0

    init(store: AnalysisStore) {
        self.store = store
    }

    // MARK: - Public API

    /// Get the current trust for a show. Returns the Beta posterior mean.
    /// If the show has no record, returns the prior mean (0.50).
    func trust(forShow showId: String) async -> Double {
        let params = await loadOrDefault(showId: showId)
        return params.mean
    }

    /// Record a bracket detection outcome for a show.
    /// - Parameters:
    ///   - showId: The podcast show identifier.
    ///   - hit: `true` if the bracket correctly identified an ad boundary,
    ///          `false` if it was a false positive.
    func recordOutcome(showId: String, hit: Bool) async {
        var params = await loadOrDefault(showId: showId)
        if hit {
            params.alpha += 1.0
        } else {
            params.beta += 1.0
        }
        cache[showId] = params
        await persist(showId: showId, params: params)
    }

    /// Retrieve the raw Beta parameters for a show (for diagnostics).
    func betaParameters(forShow showId: String) async -> BetaParameters {
        await loadOrDefault(showId: showId)
    }

    // MARK: - Internal

    private func loadOrDefault(showId: String) async -> BetaParameters {
        if let cached = cache[showId] {
            return cached
        }

        // Try to load from SQLite.
        if let persisted = await loadFromStore(showId: showId) {
            cache[showId] = persisted
            return persisted
        }

        // Return default prior.
        let defaultParams = BetaParameters(
            alpha: Self.defaultAlpha,
            beta: Self.defaultBeta
        )
        cache[showId] = defaultParams
        return defaultParams
    }

    private func loadFromStore(showId: String) async -> BetaParameters? {
        do {
            return try await store.loadBracketTrust(forShow: showId)
        } catch {
            logger.warning("Failed to load bracket trust for \(showId): \(error)")
            return nil
        }
    }

    private func persist(showId: String, params: BetaParameters) async {
        do {
            try await store.saveBracketTrust(showId: showId, alpha: params.alpha, beta: params.beta)
        } catch {
            logger.error("Failed to persist bracket trust for \(showId): \(error)")
        }
    }
}

// MARK: - BetaParameters

/// Beta distribution parameters for bracket trust.
struct BetaParameters: Sendable, Equatable {
    var alpha: Double
    var beta: Double

    /// Posterior mean = alpha / (alpha + beta).
    var mean: Double {
        guard alpha + beta > 0 else { return 0.5 }
        return alpha / (alpha + beta)
    }

    /// Posterior variance.
    var variance: Double {
        let total = alpha + beta
        guard total > 0 && (total + 1) > 0 else { return 0 }
        return (alpha * beta) / (total * total * (total + 1))
    }

    /// Total observations (pseudo + real).
    var totalObservations: Double {
        alpha + beta
    }
}
