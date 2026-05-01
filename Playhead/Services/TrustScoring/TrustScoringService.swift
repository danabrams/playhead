// TrustScoringService.swift
// Per-show trust scoring that controls skip mode.
//
// Each podcast starts in shadow mode (detection runs but no skips fire).
// As the model proves precision on that show, the mode promotes through
// manual (user-tapped skip) to auto (full auto-skip).
//
// Demotion happens when the user signals false positives: tapping "Listen"
// to revert a skip, or rewinding back into a skipped segment.
//
// The user can override a show's mode in Settings at any time.

import Foundation
import OSLog

// MARK: - Skip Mode

/// Controls how the skip orchestrator treats detected ad windows for a show.
enum SkipMode: String, Sendable, CaseIterable {
    /// Detection runs and results are logged, but no skips fire. Default for new shows.
    case shadow
    /// User sees a "Skip Ad" button; no auto-skip.
    case manual
    /// Full auto-skip. Only for shows with proven local precision.
    case auto
}

// MARK: - Trust Scoring Configuration

struct TrustScoringConfig: Sendable {
    /// Minimum observations before promoting shadow -> manual.
    let shadowToManualObservations: Int
    /// Minimum trust score to promote shadow -> manual.
    let shadowToManualTrustScore: Double
    /// Minimum observations before promoting manual -> auto.
    let manualToAutoObservations: Int
    /// Minimum trust score to promote manual -> auto.
    let manualToAutoTrustScore: Double
    /// Maximum recent false-skip signals before demoting auto -> manual.
    let autoToManualFalseSignals: Int
    /// Maximum recent false-skip signals before demoting manual -> shadow.
    let manualToShadowFalseSignals: Int
    /// Trust score penalty per false-skip signal.
    let falseSignalPenalty: Double
    /// Trust score bonus per correct observation.
    let correctObservationBonus: Double
    /// Exceptionally high first-episode confidence to skip shadow.
    let exceptionalFirstEpisodeConfidence: Double

    static let `default` = TrustScoringConfig(
        shadowToManualObservations: 3,
        shadowToManualTrustScore: 0.4,
        manualToAutoObservations: 8,
        manualToAutoTrustScore: 0.75,
        autoToManualFalseSignals: 2,
        manualToShadowFalseSignals: 4,
        falseSignalPenalty: 0.10,
        correctObservationBonus: 0.10,
        exceptionalFirstEpisodeConfidence: 0.92
    )
}

// MARK: - TrustScoringService

/// Evaluates and updates per-show trust, returning the effective skip mode
/// for use by SkipOrchestrator.
///
/// Thread safety: all methods are isolated to the actor. Callers
/// (SkipOrchestrator, AdDetectionService backfill) await into this actor.
actor TrustScoringService {

    private let logger = Logger(subsystem: "com.playhead", category: "TrustScoring")

    private let store: AnalysisStore
    private let config: TrustScoringConfig

    init(store: AnalysisStore, config: TrustScoringConfig = .default) {
        self.store = store
        self.config = config
    }

    // MARK: - Query

    /// Return the effective skip mode for a podcast. Respects user override.
    /// If no profile exists yet, returns `.shadow`.
    func effectiveMode(podcastId: String) async -> SkipMode {
        let profile: PodcastProfile?
        do {
            profile = try await store.fetchProfile(podcastId: podcastId)
        } catch {
            logger.warning("Failed to fetch profile for \(podcastId): \(error.localizedDescription)")
            return .shadow
        }
        guard let profile else { return .shadow }
        return SkipMode(rawValue: profile.mode) ?? .shadow
    }

    // MARK: - Observation Recording

    /// Record a successful observation (episode processed, no false signals).
    /// Call from AdDetectionService backfill after confirming ad windows.
    func recordSuccessfulObservation(
        podcastId: String,
        averageConfidence: Double
    ) async {
        let profile: PodcastProfile?
        do {
            profile = try await store.fetchProfile(podcastId: podcastId)
        } catch {
            logger.warning("Failed to fetch profile for \(podcastId): \(error.localizedDescription)")
            return
        }

        guard let profile else {
            // First observation for a brand-new show.
            let initialMode: SkipMode =
                averageConfidence >= config.exceptionalFirstEpisodeConfidence
                ? .manual : .shadow
            let newProfile = PodcastProfile(
                podcastId: podcastId,
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: averageConfidence >= config.exceptionalFirstEpisodeConfidence
                    ? config.shadowToManualTrustScore + 0.1 : 0.2,
                observationCount: 1,
                mode: initialMode.rawValue,
                recentFalseSkipSignals: 0
            )
            do {
                try await store.upsertProfile(newProfile)
            } catch {
                logger.warning("Failed to upsert new profile for \(podcastId): \(error.localizedDescription)")
            }
            logger.info("New show \(podcastId): mode=\(initialMode.rawValue) confidence=\(averageConfidence, format: .fixed(precision: 2))")
            return
        }

        // Existing show: bump observation count and trust score.
        let newObservations = profile.observationCount + 1
        let newTrust = min(1.0, profile.skipTrustScore + config.correctObservationBonus)
        let currentMode = SkipMode(rawValue: profile.mode) ?? .shadow
        let newMode = evaluatePromotion(
            currentMode: currentMode,
            trustScore: newTrust,
            observations: newObservations,
            recentFalseSignals: profile.recentFalseSkipSignals
        )

        let updated = PodcastProfile(
            podcastId: profile.podcastId,
            sponsorLexicon: profile.sponsorLexicon,
            normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
            repeatedCTAFragments: profile.repeatedCTAFragments,
            jingleFingerprints: profile.jingleFingerprints,
            implicitFalsePositiveCount: profile.implicitFalsePositiveCount,
            skipTrustScore: newTrust,
            observationCount: newObservations,
            mode: newMode.rawValue,
            recentFalseSkipSignals: profile.recentFalseSkipSignals
        )
        do {
            try await store.upsertProfile(updated)
        } catch {
            logger.warning("Failed to upsert profile for \(podcastId) after observation: \(error.localizedDescription)")
        }

        if newMode != currentMode {
            logger.info("Promoted \(podcastId): \(currentMode.rawValue) -> \(newMode.rawValue) trust=\(newTrust, format: .fixed(precision: 2)) obs=\(newObservations)")
        }
    }

    // MARK: - False-Positive Signals

    /// Record a false-skip signal (user tapped "Listen" or rewound after skip).
    /// Decrements trust and may trigger demotion.
    func recordFalseSkipSignal(podcastId: String) async {
        let profile: PodcastProfile?
        do {
            profile = try await store.fetchProfile(podcastId: podcastId)
        } catch {
            logger.warning("Failed to fetch profile for false-skip signal on \(podcastId): \(error.localizedDescription)")
            return
        }
        guard let profile else { return }

        let newFalseSignals = profile.recentFalseSkipSignals + 1
        let newTrust = max(0, profile.skipTrustScore - config.falseSignalPenalty)
        let currentMode = SkipMode(rawValue: profile.mode) ?? .shadow
        let newMode = evaluateDemotion(
            currentMode: currentMode,
            trustScore: newTrust,
            recentFalseSignals: newFalseSignals
        )

        let updated = PodcastProfile(
            podcastId: profile.podcastId,
            sponsorLexicon: profile.sponsorLexicon,
            normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
            repeatedCTAFragments: profile.repeatedCTAFragments,
            jingleFingerprints: profile.jingleFingerprints,
            implicitFalsePositiveCount: profile.implicitFalsePositiveCount + 1,
            skipTrustScore: newTrust,
            observationCount: profile.observationCount,
            mode: newMode.rawValue,
            recentFalseSkipSignals: newFalseSignals
        )
        do {
            try await store.upsertProfile(updated)
        } catch {
            logger.warning("Failed to upsert profile for \(podcastId) after false-skip signal: \(error.localizedDescription)")
        }

        if newMode != currentMode {
            logger.info("Demoted \(podcastId): \(currentMode.rawValue) -> \(newMode.rawValue) trust=\(newTrust, format: .fixed(precision: 2)) falseSignals=\(newFalseSignals)")
        } else {
            logger.info("False signal for \(podcastId): trust=\(newTrust, format: .fixed(precision: 2)) falseSignals=\(newFalseSignals)")
        }
    }

    // MARK: - User Override

    /// Set a user-chosen mode for a podcast, overriding the trust engine.
    /// Stores the mode directly; trust score is not changed.
    func setUserOverride(podcastId: String, mode: SkipMode) async {
        let profile: PodcastProfile?
        do {
            profile = try await store.fetchProfile(podcastId: podcastId)
        } catch {
            logger.warning("Failed to fetch profile for user override on \(podcastId): \(error.localizedDescription)")
            return
        }

        guard let profile else {
            // Create a profile with the override mode.
            let newProfile = PodcastProfile(
                podcastId: podcastId,
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: 0.5,
                observationCount: 0,
                mode: mode.rawValue,
                recentFalseSkipSignals: 0
            )
            do {
                try await store.upsertProfile(newProfile)
            } catch {
                logger.warning("Failed to upsert new profile for user override on \(podcastId): \(error.localizedDescription)")
            }
            logger.info("User override (new profile) \(podcastId): mode=\(mode.rawValue)")
            return
        }

        let updated = PodcastProfile(
            podcastId: profile.podcastId,
            sponsorLexicon: profile.sponsorLexicon,
            normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
            repeatedCTAFragments: profile.repeatedCTAFragments,
            jingleFingerprints: profile.jingleFingerprints,
            implicitFalsePositiveCount: profile.implicitFalsePositiveCount,
            skipTrustScore: profile.skipTrustScore,
            observationCount: profile.observationCount,
            mode: mode.rawValue,
            recentFalseSkipSignals: profile.recentFalseSkipSignals
        )
        do {
            try await store.upsertProfile(updated)
        } catch {
            logger.warning("Failed to upsert profile for user override on \(podcastId): \(error.localizedDescription)")
        }
        logger.info("User override \(podcastId): mode=\(mode.rawValue)")
    }

    // MARK: - False-Negative Signals

    /// Record a false-negative signal (user manually skipped past an ad the
    /// system missed).
    ///
    /// A false negative means the model under-detected: it failed to flag content
    /// the user considered ad-like. That is direct evidence the model is not
    /// performing well on this show, so trust must move *down*. We mirror the
    /// false-positive magnitude (`falseSignalPenalty`) so FN and FP land
    /// symmetrically — neither is catastrophic on its own, but both are real
    /// errors. Mode and demotion counters are unaffected: only `recordFalseSkipSignal`
    /// (a false positive) feeds the demotion path, since auto-skipping
    /// non-ads is the dangerous failure mode.
    func recordFalseNegativeSignal(podcastId: String) async {
        let profile: PodcastProfile?
        do {
            profile = try await store.fetchProfile(podcastId: podcastId)
        } catch {
            logger.warning("Failed to fetch profile for false-negative signal on \(podcastId): \(error.localizedDescription)")
            return
        }
        guard let profile else { return }

        // Mirror the FP magnitude (config.falseSignalPenalty) but in the
        // opposite direction from a successful observation. Clamp at 0 so
        // we never go negative.
        let newTrust = max(0, profile.skipTrustScore - config.falseSignalPenalty)

        let updated = PodcastProfile(
            podcastId: profile.podcastId,
            sponsorLexicon: profile.sponsorLexicon,
            normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
            repeatedCTAFragments: profile.repeatedCTAFragments,
            jingleFingerprints: profile.jingleFingerprints,
            implicitFalsePositiveCount: profile.implicitFalsePositiveCount,
            skipTrustScore: newTrust,
            observationCount: profile.observationCount,
            mode: profile.mode,
            recentFalseSkipSignals: profile.recentFalseSkipSignals
        )
        do {
            try await store.upsertProfile(updated)
        } catch {
            logger.warning("Failed to upsert profile for \(podcastId) after false-negative signal: \(error.localizedDescription)")
        }
        logger.info("False-negative signal for \(podcastId): trust=\(newTrust, format: .fixed(precision: 2))")
    }

    // MARK: - Reset

    /// Reset recent false-skip signals for a podcast (called after a
    /// successful episode with no false signals, to decay old signals).
    func decayFalseSignals(podcastId: String) async {
        let profile: PodcastProfile?
        do {
            profile = try await store.fetchProfile(podcastId: podcastId)
        } catch {
            logger.warning("Failed to fetch profile for false-signal decay on \(podcastId): \(error.localizedDescription)")
            return
        }
        guard let profile else { return }
        guard profile.recentFalseSkipSignals > 0 else { return }

        // Halve the false signal count after each clean episode.
        let decayed = max(0, profile.recentFalseSkipSignals / 2)

        let updated = PodcastProfile(
            podcastId: profile.podcastId,
            sponsorLexicon: profile.sponsorLexicon,
            normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
            repeatedCTAFragments: profile.repeatedCTAFragments,
            jingleFingerprints: profile.jingleFingerprints,
            implicitFalsePositiveCount: profile.implicitFalsePositiveCount,
            skipTrustScore: profile.skipTrustScore,
            observationCount: profile.observationCount,
            mode: profile.mode,
            recentFalseSkipSignals: decayed
        )
        do {
            try await store.upsertProfile(updated)
        } catch {
            logger.warning("Failed to upsert profile for false-signal decay on \(podcastId): \(error.localizedDescription)")
        }
    }

    // MARK: - Promotion / Demotion Logic

    /// Evaluate whether the current mode should be promoted.
    private func evaluatePromotion(
        currentMode: SkipMode,
        trustScore: Double,
        observations: Int,
        recentFalseSignals: Int
    ) -> SkipMode {
        switch currentMode {
        case .shadow:
            if observations >= config.shadowToManualObservations
                && trustScore >= config.shadowToManualTrustScore {
                return .manual
            }
        case .manual:
            if observations >= config.manualToAutoObservations
                && trustScore >= config.manualToAutoTrustScore
                && recentFalseSignals == 0 {
                return .auto
            }
        case .auto:
            break // Already at max.
        }
        return currentMode
    }

    /// Evaluate whether the current mode should be demoted.
    private func evaluateDemotion(
        currentMode: SkipMode,
        trustScore: Double,
        recentFalseSignals: Int
    ) -> SkipMode {
        switch currentMode {
        case .auto:
            if recentFalseSignals >= config.autoToManualFalseSignals {
                return .manual
            }
        case .manual:
            if recentFalseSignals >= config.manualToShadowFalseSignals {
                return .shadow
            }
        case .shadow:
            break // Already at min.
        }
        return currentMode
    }
}
