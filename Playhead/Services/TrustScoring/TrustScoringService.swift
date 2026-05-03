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

// MARK: - Demotion

/// Captured transition emitted by `recordFalseSkipSignal` when the new
/// mode falls below the previous one. Sendable so it can ride the
/// `updateProfileIfExistsCapturing` tuple back across the
/// `AnalysisStore` actor hop without `nonisolated(unsafe)`.
struct Demotion: Sendable {
    let from: SkipMode
    let to: SkipMode
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
        // skeptical-review-cycle-1: atomic merge inside AnalysisStore
        // closes the actor-reentrancy lost-update window between fetch
        // and upsert. The two closures are pure transforms — no awaits.
        let config = self.config
        let result: PodcastProfile
        do {
            result = try await store.mutateProfile(
                podcastId: podcastId,
                create: {
                    let initialMode: SkipMode =
                        averageConfidence >= config.exceptionalFirstEpisodeConfidence
                        ? .manual : .shadow
                    return PodcastProfile(
                        podcastId: podcastId,
                        sponsorLexicon: nil,
                        normalizedAdSlotPriors: nil,
                        repeatedCTAFragments: nil,
                        jingleFingerprints: nil,
                        implicitFalsePositiveCount: 0,
                        skipTrustScore:
                            averageConfidence >= config.exceptionalFirstEpisodeConfidence
                            ? config.shadowToManualTrustScore + 0.1 : 0.2,
                        observationCount: 1,
                        mode: initialMode.rawValue,
                        recentFalseSkipSignals: 0
                    )
                },
                update: { profile in
                    let newObservations = profile.observationCount + 1
                    let newTrust = min(1.0, profile.skipTrustScore + config.correctObservationBonus)
                    let currentMode = SkipMode(rawValue: profile.mode) ?? .shadow
                    let newMode = Self.evaluatePromotion(
                        config: config,
                        currentMode: currentMode,
                        trustScore: newTrust,
                        observations: newObservations,
                        recentFalseSignals: profile.recentFalseSkipSignals
                    )
                    return PodcastProfile(
                        podcastId: profile.podcastId,
                        sponsorLexicon: profile.sponsorLexicon,
                        normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
                        repeatedCTAFragments: profile.repeatedCTAFragments,
                        jingleFingerprints: profile.jingleFingerprints,
                        implicitFalsePositiveCount: profile.implicitFalsePositiveCount,
                        skipTrustScore: newTrust,
                        observationCount: newObservations,
                        mode: newMode.rawValue,
                        recentFalseSkipSignals: profile.recentFalseSkipSignals,
                        traitProfileJSON: profile.traitProfileJSON,
                        title: profile.title,
                        // playhead-084j: explicit carry-forward of the ad-
                        // duration-stats column. Belt-and-suspenders: the
                        // upsert SQL already COALESCEs nil writes against the
                        // persisted column, but matching the established
                        // `traitProfileJSON` pattern keeps this constructor
                        // self-explanatory to future readers and survives
                        // a hypothetical future change to the COALESCE rule.
                        adDurationStatsJSON: profile.adDurationStatsJSON,
                        // playhead-spxs: explicit carry-forward of the
                        // network-identity column. COALESCE-protected in
                        // upsertProfile, but matched here for parity with
                        // the established traitProfileJSON / adDurationStatsJSON
                        // patterns.
                        networkId: profile.networkId
                    )
                }
            )
        } catch {
            logger.warning("Failed to mutate profile for \(podcastId) after observation: \(error.localizedDescription)")
            return
        }

        let resultMode = SkipMode(rawValue: result.mode) ?? .shadow
        if result.observationCount == 1 {
            logger.info("New show \(podcastId): mode=\(result.mode) confidence=\(averageConfidence, format: .fixed(precision: 2))")
        } else {
            logger.info("Observation \(podcastId): mode=\(resultMode.rawValue) trust=\(result.skipTrustScore, format: .fixed(precision: 2)) obs=\(result.observationCount)")
        }
    }

    // MARK: - False-Positive Signals

    /// Record a false-skip signal (user tapped "Listen" or rewound after skip).
    /// Decrements trust and may trigger demotion.
    func recordFalseSkipSignal(podcastId: String) async {
        // skeptical-review-cycle-1: atomic update inside AnalysisStore.
        // No lazy-create — a missing profile means the show has never
        // been observed and stubbing one would corrupt priors.
        // skeptical-review-cycle-5 L-Y4: use the tuple-returning
        // `updateProfileIfExistsCapturing` overload so the demotion
        // transition rides back from the store-actor closure as a
        // value-typed return rather than a `nonisolated(unsafe) var`
        // captured across the actor hop. The result is the same
        // `(merged-profile, demoted?)` pair, but Swift 6 strict
        // concurrency now sees clean data flow.
        let config = self.config
        let outcome: (profile: PodcastProfile, captured: Demotion?)?
        do {
            outcome = try await store.updateProfileIfExistsCapturing(
                podcastId: podcastId,
                update: { profile in
                    let newFalseSignals = profile.recentFalseSkipSignals + 1
                    let newTrust = max(0, profile.skipTrustScore - config.falseSignalPenalty)
                    let currentMode = SkipMode(rawValue: profile.mode) ?? .shadow
                    let newMode = Self.evaluateDemotion(
                        config: config,
                        currentMode: currentMode,
                        trustScore: newTrust,
                        recentFalseSignals: newFalseSignals
                    )
                    let demotion: Demotion? = (newMode != currentMode)
                        ? Demotion(from: currentMode, to: newMode)
                        : nil
                    let merged = PodcastProfile(
                        podcastId: profile.podcastId,
                        sponsorLexicon: profile.sponsorLexicon,
                        normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
                        repeatedCTAFragments: profile.repeatedCTAFragments,
                        jingleFingerprints: profile.jingleFingerprints,
                        implicitFalsePositiveCount: profile.implicitFalsePositiveCount + 1,
                        skipTrustScore: newTrust,
                        observationCount: profile.observationCount,
                        mode: newMode.rawValue,
                        recentFalseSkipSignals: newFalseSignals,
                        traitProfileJSON: profile.traitProfileJSON,
                        title: profile.title,
                        // playhead-084j: see explanatory comment in
                        // `recordSuccessfulObservation` above.
                        adDurationStatsJSON: profile.adDurationStatsJSON,
                        // playhead-spxs: see explanatory comment in
                        // `recordSuccessfulObservation` above.
                        networkId: profile.networkId
                    )
                    return (merged, demotion)
                }
            )
        } catch {
            logger.warning("Failed to mutate profile for \(podcastId) after false-skip signal: \(error.localizedDescription)")
            return
        }

        guard let outcome else { return }
        let result = outcome.profile
        if let demoted = outcome.captured {
            logger.info("Demoted \(podcastId): \(demoted.from.rawValue) -> \(demoted.to.rawValue) trust=\(result.skipTrustScore, format: .fixed(precision: 2)) falseSignals=\(result.recentFalseSkipSignals)")
        } else {
            logger.info("False signal for \(podcastId): trust=\(result.skipTrustScore, format: .fixed(precision: 2)) falseSignals=\(result.recentFalseSkipSignals)")
        }
    }

    // MARK: - User Override

    /// Set a user-chosen mode for a podcast, overriding the trust engine.
    /// Stores the mode directly; trust score is not changed.
    func setUserOverride(podcastId: String, mode: SkipMode) async {
        // skeptical-review-cycle-1: atomic merge — lazy-creates with
        // the override mode if no profile exists yet.
        do {
            _ = try await store.mutateProfile(
                podcastId: podcastId,
                create: {
                    PodcastProfile(
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
                },
                update: { profile in
                    PodcastProfile(
                        podcastId: profile.podcastId,
                        sponsorLexicon: profile.sponsorLexicon,
                        normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
                        repeatedCTAFragments: profile.repeatedCTAFragments,
                        jingleFingerprints: profile.jingleFingerprints,
                        implicitFalsePositiveCount: profile.implicitFalsePositiveCount,
                        skipTrustScore: profile.skipTrustScore,
                        observationCount: profile.observationCount,
                        mode: mode.rawValue,
                        recentFalseSkipSignals: profile.recentFalseSkipSignals,
                        traitProfileJSON: profile.traitProfileJSON,
                        title: profile.title,
                        // playhead-084j: see explanatory comment in
                        // `recordSuccessfulObservation` above.
                        adDurationStatsJSON: profile.adDurationStatsJSON,
                        // playhead-spxs: see explanatory comment in
                        // `recordSuccessfulObservation` above.
                        networkId: profile.networkId
                    )
                }
            )
        } catch {
            logger.warning("Failed to mutate profile for user override on \(podcastId): \(error.localizedDescription)")
            return
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
        // skeptical-review-cycle-1: atomic update; no lazy-create.
        let config = self.config
        let result: PodcastProfile?
        do {
            result = try await store.updateProfileIfExists(
                podcastId: podcastId,
                update: { profile in
                    // Mirror the FP magnitude but in the opposite
                    // direction from a successful observation. Clamp at
                    // 0 so we never go negative.
                    let newTrust = max(0, profile.skipTrustScore - config.falseSignalPenalty)
                    return PodcastProfile(
                        podcastId: profile.podcastId,
                        sponsorLexicon: profile.sponsorLexicon,
                        normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
                        repeatedCTAFragments: profile.repeatedCTAFragments,
                        jingleFingerprints: profile.jingleFingerprints,
                        implicitFalsePositiveCount: profile.implicitFalsePositiveCount,
                        skipTrustScore: newTrust,
                        observationCount: profile.observationCount,
                        mode: profile.mode,
                        recentFalseSkipSignals: profile.recentFalseSkipSignals,
                        traitProfileJSON: profile.traitProfileJSON,
                        title: profile.title,
                        // playhead-084j: see explanatory comment in
                        // `recordSuccessfulObservation` above.
                        adDurationStatsJSON: profile.adDurationStatsJSON,
                        // playhead-spxs: see explanatory comment in
                        // `recordSuccessfulObservation` above.
                        networkId: profile.networkId
                    )
                }
            )
        } catch {
            logger.warning("Failed to mutate profile for \(podcastId) after false-negative signal: \(error.localizedDescription)")
            return
        }
        guard let result else { return }
        logger.info("False-negative signal for \(podcastId): trust=\(result.skipTrustScore, format: .fixed(precision: 2))")
    }

    // MARK: - Reset

    /// Reset recent false-skip signals for a podcast (called after a
    /// successful episode with no false signals, to decay old signals).
    func decayFalseSignals(podcastId: String) async {
        // skeptical-review-cycle-1: atomic update; no lazy-create.
        // skeptical-review-cycle-3 M-C: precheck to short-circuit when
        // the count is already 0. Called once per clean episode across
        // many shows — without the precheck every clean listen pays a
        // value-preserving upsert that walks the row, holds the write
        // lock, and dirties the page. The precheck race is benign: a
        // concurrent increment between fetch and write just defers
        // this decay opportunity to the next clean episode.
        do {
            if let existing = try await store.fetchProfile(podcastId: podcastId),
               existing.recentFalseSkipSignals == 0 {
                return
            }
            _ = try await store.updateProfileIfExists(
                podcastId: podcastId,
                update: { profile in
                    let decayed = max(0, profile.recentFalseSkipSignals / 2)
                    return PodcastProfile(
                        podcastId: profile.podcastId,
                        sponsorLexicon: profile.sponsorLexicon,
                        normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
                        repeatedCTAFragments: profile.repeatedCTAFragments,
                        jingleFingerprints: profile.jingleFingerprints,
                        implicitFalsePositiveCount: profile.implicitFalsePositiveCount,
                        skipTrustScore: profile.skipTrustScore,
                        observationCount: profile.observationCount,
                        mode: profile.mode,
                        recentFalseSkipSignals: decayed,
                        traitProfileJSON: profile.traitProfileJSON,
                        title: profile.title,
                        // playhead-084j: see explanatory comment in
                        // `recordSuccessfulObservation` above.
                        adDurationStatsJSON: profile.adDurationStatsJSON,
                        // playhead-spxs: see explanatory comment in
                        // `recordSuccessfulObservation` above.
                        networkId: profile.networkId
                    )
                }
            )
        } catch {
            logger.warning("Failed to mutate profile for false-signal decay on \(podcastId): \(error.localizedDescription)")
        }
    }

    // MARK: - Promotion / Demotion Logic

    /// Evaluate whether the current mode should be promoted.
    /// Static so the SQL-side `mutateProfile` closures (which run inside
    /// the `AnalysisStore` actor, not this one) can call it without a
    /// cross-actor hop.
    fileprivate static func evaluatePromotion(
        config: TrustScoringConfig,
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

    /// Evaluate whether the current mode should be demoted. Static for
    /// the same reason as `evaluatePromotion`.
    fileprivate static func evaluateDemotion(
        config: TrustScoringConfig,
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
