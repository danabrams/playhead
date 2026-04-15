// BoundaryPriorStore.swift
// ef2.3.5: Cue-conditional boundary correction priors.
//
// Stores boundary corrections as distributions (not scalar offsets) so the
// snap pipeline can make smarter decisions based on accumulated user feedback.
//
// Design:
//   - BoundaryPriorKey: (showId, edgeDirection, bracketTemplate?) identifies context.
//   - BoundaryPriorDistribution: rolling median + spread, updated via Welford-style
//     online algorithm. Tight spread → aggressive snap; wide spread → wider search.
//   - BoundaryPriorStore: actor-based, persisted to AnalysisStore's `boundary_priors` table.
//   - 90-day decay (faster than 180-day general correction decay).

import Foundation
import OSLog

// MARK: - EdgeDirection

/// Which edge of an ad boundary a correction applies to.
enum EdgeDirection: String, Sendable, Hashable, Codable {
    case start
    case end
}

// MARK: - BoundaryPriorKey

/// Identifies the context for a boundary prior distribution.
struct BoundaryPriorKey: Sendable, Hashable {
    let showId: String
    let edgeDirection: EdgeDirection
    /// Optional bracket template for future expansion (e.g. "music-bed", "jingle").
    /// `nil` means the default (unqualified) prior for this show + edge.
    let bracketTemplate: String?
}

// MARK: - BoundaryPriorDistribution

/// Statistical summary of boundary corrections for a given context.
///
/// Uses a rolling mean + Welford's online variance to track the distribution
/// of signed offsets. The `median` field stores the running mean — named
/// `median` for SQLite column compatibility; it is NOT a true median and
/// IS sensitive to outliers. Use `spread` (sample standard deviation) to
/// assess distribution quality before trusting the central estimate.
struct BoundaryPriorDistribution: Sendable, Equatable {
    /// Running mean of signed offsets (seconds). Positive = boundary should
    /// move later; negative = boundary should move earlier.
    let median: Double
    /// Sample standard deviation of offsets (seconds). Represents uncertainty.
    let spread: Double
    /// Number of correction samples incorporated.
    let sampleCount: Int
    /// When this distribution was last updated (epoch seconds).
    let lastUpdatedAt: Double

    /// Compute a snap radius guidance value for MinimalContiguousSpanDecoder.
    ///
    /// Tight distribution (small spread) → small radius (aggressive snap).
    /// Wide distribution (large spread) → larger radius (wider search).
    /// Result is always in [baseRadius * 0.25, baseRadius].
    func snapRadiusGuidance(baseRadius: Double) -> Double {
        // Scale factor: spread / 2.0, clamped to [0.25, 1.0].
        // At spread=0.5 → factor=0.25 (tightest); at spread>=2.0 → factor=1.0 (widest).
        let factor = min(1.0, max(0.25, spread / 2.0))
        return baseRadius * factor
    }
}

// MARK: - Decay Weight (90-day)

/// Compute a decay weight for a boundary prior based on age.
///
/// Returns 1.0 at 0 days, decays linearly to 0.1 at 90 days, and is clamped
/// to a minimum of 0.1 for entries older than 90 days. This is faster than
/// the 180-day general correction decay.
func boundaryPriorDecayWeight(ageDays: Double) -> Double {
    min(1.0, max(0.1, 1.0 - (ageDays / 90.0)))
}

// MARK: - BoundaryPriorStore

/// Actor-backed store for boundary correction priors.
///
/// Records signed boundary offsets and maintains a statistical distribution
/// (mean + standard deviation) for each (showId, edgeDirection, bracketTemplate)
/// context. Persisted via AnalysisStore's `boundary_priors` table.
actor BoundaryPriorStore {

    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "BoundaryPriorStore")

    init(store: AnalysisStore) {
        self.store = store
    }

    // MARK: - Record

    /// Update the distribution for the given context with a new signed offset.
    ///
    /// Uses Welford's online algorithm for incremental mean + variance:
    ///   - Load existing distribution (or start fresh).
    ///   - Incorporate the new sample.
    ///   - Persist the updated distribution.
    ///
    /// - Parameters:
    ///   - key: The boundary context (show, edge, optional template).
    ///   - signedOffset: Seconds the boundary should move. Positive = later, negative = earlier.
    func recordBoundaryCorrection(key: BoundaryPriorKey, signedOffset: Double) async {
        let now = Date().timeIntervalSince1970
        do {
            let existing = try await store.loadBoundaryPrior(
                showId: key.showId,
                edgeDirection: key.edgeDirection.rawValue,
                bracketTemplate: key.bracketTemplate
            )

            let updated: BoundaryPriorDistribution
            if let existing {
                updated = incorporateSample(existing: existing, newSample: signedOffset, now: now)
            } else {
                // First sample: use initial spread of 2.0 (generous uncertainty).
                updated = BoundaryPriorDistribution(
                    median: signedOffset,
                    spread: 2.0,
                    sampleCount: 1,
                    lastUpdatedAt: now
                )
            }

            try await store.upsertBoundaryPrior(
                showId: key.showId,
                edgeDirection: key.edgeDirection.rawValue,
                bracketTemplate: key.bracketTemplate,
                median: updated.median,
                spread: updated.spread,
                sampleCount: updated.sampleCount,
                lastUpdatedAt: updated.lastUpdatedAt
            )
        } catch {
            logger.error(
                "recordBoundaryCorrection: failed for show \(key.showId, privacy: .public) edge \(key.edgeDirection.rawValue, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
    }

    // MARK: - Query

    /// Retrieve the current prior distribution for a given context.
    /// Returns `nil` if no corrections have been recorded for this context.
    func prior(for key: BoundaryPriorKey) async -> BoundaryPriorDistribution? {
        do {
            return try await store.loadBoundaryPrior(
                showId: key.showId,
                edgeDirection: key.edgeDirection.rawValue,
                bracketTemplate: key.bracketTemplate
            )
        } catch {
            logger.warning(
                "prior(for:): failed to load for show \(key.showId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// Retrieve all priors for a given show, keyed by BoundaryPriorKey.
    func allPriors(forShow showId: String) async -> [BoundaryPriorKey: BoundaryPriorDistribution] {
        do {
            return try await store.loadAllBoundaryPriors(forShow: showId)
        } catch {
            logger.warning(
                "allPriors(forShow:): failed for show \(showId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return [:]
        }
    }

    // MARK: - Welford Update

    /// Incorporate a new sample into an existing distribution using Welford's
    /// online algorithm for incremental mean and standard deviation.
    private func incorporateSample(
        existing: BoundaryPriorDistribution,
        newSample: Double,
        now: Double
    ) -> BoundaryPriorDistribution {
        let n = existing.sampleCount + 1
        let oldMean = existing.median
        let newMean = oldMean + (newSample - oldMean) / Double(n)

        // Welford's: M2_new = M2_old + (x - oldMean) * (x - newMean)
        // We reconstruct M2_old from spread (sample stddev) and old count:
        //   M2_old = spread^2 * (oldCount - 1)  [sample variance denominator]
        // Special case: when oldCount == 1, the stored spread is the initial
        // prior (2.0), not empirical variance. M2 for a single sample is 0.
        let oldCount = existing.sampleCount
        let oldM2: Double
        if oldCount <= 1 {
            oldM2 = 0.0
        } else {
            oldM2 = existing.spread * existing.spread * Double(oldCount - 1)
        }
        let newM2 = oldM2 + (newSample - oldMean) * (newSample - newMean)
        // Sample stddev (n-1 denominator) — appropriate for small N where
        // population stddev would systematically underestimate uncertainty.
        let newSpread = n > 1 ? sqrt(newM2 / Double(n - 1)) : 2.0

        return BoundaryPriorDistribution(
            median: newMean,
            spread: max(newSpread, 0.1),  // Floor at 0.1s to prevent zero-spread lock-in
            sampleCount: n,
            lastUpdatedAt: now
        )
    }
}
