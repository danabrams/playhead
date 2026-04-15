// CausalSourceDemotionStore.swift
// ef2.3.3: Bounded, show-local demotion of causal sources after false positives.
//
// Design:
//   - Each CausalSource has a demotion delta and floor multiplier.
//   - Demotions are keyed by (showId, causalSource) and persist in SQLite.
//   - foundationModel and acoustic are exempt — always return 1.0.
//   - Fingerprint disputes track per-fingerprint state with a separate table.
//   - All state is show-local: demotions on show A never affect show B.

import Foundation
import OSLog

// MARK: - SourceDemotion

/// A record of a bounded demotion applied to a causal source for a specific show.
struct SourceDemotion: Sendable, Equatable {
    /// Which detection mechanism was demoted.
    let causalSource: CausalSource
    /// The podcast/show this demotion applies to.
    let showId: String
    /// How much the multiplier was reduced in the most recent demotion.
    let demotionDelta: Double
    /// Current multiplier in [floor, 1.0]. Applied to the source's evidence weight.
    let currentMultiplier: Double
    /// The lowest the multiplier can go for this source type.
    let floor: Double
    /// When this demotion was first created (seconds since epoch).
    let createdAt: Double
    /// When this demotion was last updated (seconds since epoch).
    let updatedAt: Double
}

// MARK: - FingerprintDispute

/// Tracks dispute/confirmation state for a specific fingerprint on a specific show.
struct FingerprintDispute: Sendable, Equatable {
    /// The fingerprint hash or identifier being disputed.
    let fingerprintId: String
    /// The podcast/show this dispute applies to.
    let showId: String
    /// How many false positive reports implicated this fingerprint.
    let disputeCount: Int
    /// How many subsequent true positive confirmations this fingerprint received.
    let confirmationCount: Int
    /// Current state: disputed (requires extra corroboration) or cleared.
    let status: FingerprintDisputeStatus
}

/// Whether a disputed fingerprint still requires extra corroboration.
enum FingerprintDisputeStatus: String, Sendable, Codable, Equatable {
    /// Fingerprint was implicated in a false positive; requires extra corroboration.
    case disputed
    /// Fingerprint has been confirmed enough times to clear the dispute.
    case cleared
}

// MARK: - Demotion Rules

/// Per-source demotion parameters. Static configuration — not user-tunable.
private struct DemotionRule {
    /// How much to reduce the multiplier per demotion event.
    let delta: Double
    /// Minimum multiplier value (the floor).
    let floor: Double
}

/// Demotion rules keyed by CausalSource. Sources not in this map are exempt.
private let demotionRules: [CausalSource: DemotionRule] = [
    .lexical:       DemotionRule(delta: 0.20, floor: 0.30),
    .fingerprint:   DemotionRule(delta: 0.10, floor: 0.20),  // Also creates/extends dispute
    .musicBracket:  DemotionRule(delta: 0.10, floor: 0.20),
    .metadata:      DemotionRule(delta: 0.05, floor: 0.05),
    .positionPrior: DemotionRule(delta: 0.10, floor: 0.30),
]

// MARK: - CausalSourceDemotionStore

/// Actor-based store for show-local demotion of causal sources after false positives.
///
/// Queryable from evidence fusion: call `demotionFactor(source:showId:)` to get
/// the current multiplier for a given source on a given show. Returns 1.0 for
/// exempt sources (foundationModel, acoustic) and for sources with no demotions.
actor CausalSourceDemotionStore {

    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "CausalSourceDemotionStore")

    init(store: AnalysisStore) {
        self.store = store
    }

    // MARK: - Public API

    /// Apply a bounded demotion to the given source for the given show.
    ///
    /// If the source is exempt (foundationModel, acoustic), this is a no-op.
    /// The multiplier is reduced by the source's delta, floored at its minimum.
    /// For fingerprint sources, a dispute is also created/extended.
    func applyDemotion(source: CausalSource, showId: String, correctionId: String) async {
        guard let rule = demotionRules[source] else {
            // foundationModel, acoustic: no demotion.
            return
        }

        let now = Date().timeIntervalSince1970

        do {
            // Load current multiplier (default 1.0 if no row exists).
            let current = try await store.loadSourceDemotionMultiplier(
                source: source.rawValue, showId: showId
            ) ?? 1.0

            let newMultiplier = max(rule.floor, current - rule.delta)

            try await store.upsertSourceDemotion(
                source: source.rawValue,
                showId: showId,
                currentMultiplier: newMultiplier,
                floor: rule.floor,
                updatedAt: now
            )

            // Fingerprint: also create/extend dispute.
            if source == .fingerprint {
                try await store.upsertFingerprintDispute(
                    fingerprintId: correctionId,
                    showId: showId,
                    incrementDispute: true,
                    incrementConfirmation: false,
                    now: now
                )
            }

            logger.info(
                "Demoted \(source.rawValue, privacy: .public) on show \(showId, privacy: .public): \(current, privacy: .public) -> \(newMultiplier, privacy: .public) (floor \(rule.floor, privacy: .public))"
            )
        } catch {
            logger.error(
                "applyDemotion failed for \(source.rawValue, privacy: .public) on show \(showId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
    }

    /// Returns the current demotion multiplier for the given source on the given show.
    ///
    /// Returns 1.0 (no demotion) for:
    /// - Exempt sources (foundationModel, acoustic)
    /// - Sources with no recorded demotions on this show
    func demotionFactor(source: CausalSource, showId: String) async -> Double {
        guard demotionRules[source] != nil else {
            // Exempt: always full weight.
            return 1.0
        }

        do {
            return try await store.loadSourceDemotionMultiplier(
                source: source.rawValue, showId: showId
            ) ?? 1.0
        } catch {
            logger.warning(
                "demotionFactor read failed for \(source.rawValue, privacy: .public) on show \(showId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return 1.0
        }
    }

    /// Record a true positive confirmation for a fingerprint on a show.
    ///
    /// Increments the confirmation count. If the confirmation count reaches 2,
    /// the dispute status is cleared (fingerprint is no longer disputed).
    func recordFingerprintConfirmation(fingerprintId: String, showId: String) async {
        let now = Date().timeIntervalSince1970
        do {
            try await store.upsertFingerprintDispute(
                fingerprintId: fingerprintId,
                showId: showId,
                incrementDispute: false,
                incrementConfirmation: true,
                now: now
            )
        } catch {
            logger.error(
                "recordFingerprintConfirmation failed for \(fingerprintId, privacy: .public) on show \(showId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
    }

    /// Returns true if the given fingerprint is currently disputed on the given show.
    ///
    /// A fingerprint is disputed after being attributed to a false positive. It remains
    /// disputed until 2 subsequent true positive confirmations clear it.
    func isFingerprintDisputed(fingerprintId: String, showId: String) async -> Bool {
        do {
            return try await store.loadFingerprintDisputeStatus(
                fingerprintId: fingerprintId, showId: showId
            ) == .some("disputed")
        } catch {
            logger.warning(
                "isFingerprintDisputed read failed for \(fingerprintId, privacy: .public) on show \(showId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return false
        }
    }
}
