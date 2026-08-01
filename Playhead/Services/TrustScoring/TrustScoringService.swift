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

// MARK: - Skip Mode Resolution (playhead-djl0)

/// WHY the active `SkipMode` holds the value it does.
///
/// `SkipMode.shadow` names a DELIBERATE posture: detection runs, nothing fires,
/// the show is being watched while it earns trust. Before playhead-djl0 it also
/// named four failures that are not that posture — an episode that carried no
/// canonical show identifier, a trust service that was never wired, a profile
/// read that threw, and a stored `mode` string that did not decode. All five
/// produced a byte-identical `SkipMode`, a byte-identical decision-log line and
/// a byte-identical pill, so a fully working detection pipeline could lose the
/// show's identity with no user-visible trace and no way to tell it apart from
/// "this show is deliberately in shadow".
///
/// This type is the missing distinction. It never changes what the orchestrator
/// DOES — `SkipMode` is still the only input to the skip policy — it records
/// what happened on the way to that mode so the failures can be counted,
/// logged and shown.
///
/// The partition is deliberate: `newShowDefault` is NOT a failure, because
/// `SkipMode.shadow`'s own contract is "Default for new shows". Merging it into
/// the failure set would make the counters read as if every first listen were
/// broken — the same class of mistake this type exists to end.
enum SkipModeResolution: String, Sendable, Hashable, CaseIterable {

    // MARK: Non-failures

    /// No episode is active. The mode is the pre-episode, non-actioning default.
    case noActiveEpisode

    /// The show was identified, its profile was read, and the mode is that
    /// profile's verdict.
    case showTrustProfile

    /// The show was identified and has no profile yet. Shadow here is the
    /// designed default for a show nobody has listened to.
    case newShowDefault

    /// The mode was set explicitly for this session — by the listener through
    /// the Now Playing control, or by a harness.
    case sessionOverride

    // MARK: Failures — each was previously indistinguishable from the above

    /// No canonical show identifier reached `beginEpisode`, and none could be
    /// recovered from the durable job row. The show's trust mode was never
    /// looked up because there was nothing to look it up by.
    case unresolvedShowIdentity

    /// A show identifier was present but no trust service was wired, so there
    /// was nothing to ask. Production always wires one; reaching this in the
    /// field is a wiring regression.
    case trustServiceUnavailable

    /// The show's profile read failed. The show is known and its stored
    /// preference exists — it just could not be reached this time.
    case trustProfileUnreadable

    /// The show's profile was read and its stored `mode` string did not decode
    /// to a `SkipMode`. A forward-compatibility or corruption signal, not a
    /// verdict.
    case unrecognizedTrustProfileMode

    /// Whether this cause is a LOOKUP FAILURE — something went wrong on the way
    /// to the mode — rather than a mode that was genuinely decided.
    ///
    /// One definition, three consumers: the per-cause counters, the durable
    /// diagnostics record, and the Now Playing pill. Keeping it here is what
    /// stops the three from drifting apart.
    var isLookupFailure: Bool {
        switch self {
        case .noActiveEpisode, .showTrustProfile, .newShowDefault, .sessionOverride:
            return false
        case .unresolvedShowIdentity, .trustServiceUnavailable,
             .trustProfileUnreadable, .unrecognizedTrustProfileMode:
            return true
        }
    }

    /// Whether the session knows which show it is playing.
    ///
    /// Strictly narrower than ``isLookupFailure``: a profile that failed to READ
    /// still belongs to a show, so a per-show preference can still be stored
    /// against it. Only an unresolved identity leaves nothing to attach a
    /// preference to — which is why it, and only it, withholds the control.
    var hasResolvedShowIdentity: Bool {
        switch self {
        case .noActiveEpisode, .unresolvedShowIdentity:
            return false
        case .showTrustProfile, .newShowDefault, .sessionOverride,
             .trustServiceUnavailable, .trustProfileUnreadable,
             .unrecognizedTrustProfileMode:
            return true
        }
    }
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
    /// playhead-q45f: weaker false-skip penalty for listen-rewinds. The
    /// inline pre-q45f path in `AdDetectionService.recordListenRewind`
    /// hard-coded a 0.05 decrement (half of `falseSignalPenalty`) but
    /// bypassed the demotion state machine entirely. Routing rewinds
    /// through `recordWeakFalseSkipSignal` keeps the weaker magnitude
    /// while still running `evaluateDemotion` so two rewinds in a row
    /// genuinely demote auto -> manual.
    let weakFalseSignalPenalty: Double

    static let `default` = TrustScoringConfig(
        shadowToManualObservations: 3,
        shadowToManualTrustScore: 0.4,
        manualToAutoObservations: 8,
        manualToAutoTrustScore: 0.75,
        autoToManualFalseSignals: 2,
        manualToShadowFalseSignals: 4,
        falseSignalPenalty: 0.10,
        correctObservationBonus: 0.10,
        exceptionalFirstEpisodeConfidence: 0.92,
        weakFalseSignalPenalty: 0.05
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

enum TrustScoringSignalPrivacy: Sendable, Equatable {
    case standard
    case explicitBannerFeedback
}

/// Typed seam for proving which production TrustScoring log branch executed
/// without scraping unified logging in tests.
enum TrustScoringSignalLogEvent: Sendable, Equatable {
    case standardFailure
    case standardSuccess
    case explicitFeedbackOperationFailed
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
    private let signalLogObserver:
        (@Sendable (TrustScoringSignalLogEvent) -> Void)?

    init(
        store: AnalysisStore,
        config: TrustScoringConfig = .default,
        signalLogObserver:
            (@Sendable (TrustScoringSignalLogEvent) -> Void)? = nil
    ) {
        self.store = store
        self.config = config
        self.signalLogObserver = signalLogObserver
    }

    // MARK: - Query

    /// Return the effective skip mode for a podcast. Respects user override.
    /// If no profile exists yet, returns `.shadow`.
    func effectiveMode(podcastId: String) async -> SkipMode {
        await resolveMode(podcastId: podcastId).mode
    }

    /// playhead-djl0: `effectiveMode` with the CAUSE attached.
    ///
    /// Three of this function's four exits return `.shadow`, and only one of
    /// them means "this show is deliberately being observed". The other two —
    /// a read that threw, and a stored `mode` that did not decode — are
    /// failures wearing the same value, and a caller that only sees `SkipMode`
    /// cannot tell them apart. `effectiveMode` is kept as the mode-only front
    /// door for the callers that genuinely only need the policy input.
    func resolveMode(
        podcastId: String
    ) async -> (mode: SkipMode, resolution: SkipModeResolution) {
        let profile: PodcastProfile?
        do {
            profile = try await store.fetchProfile(podcastId: podcastId)
        } catch {
            logger.warning("Failed to fetch profile for \(podcastId): \(error.localizedDescription)")
            return (.shadow, .trustProfileUnreadable)
        }
        guard let profile else { return (.shadow, .newShowDefault) }
        guard let mode = SkipMode(rawValue: profile.mode) else {
            logger.warning(
                "Profile for \(podcastId) carries unrecognized mode '\(profile.mode)'"
            )
            return (.shadow, .unrecognizedTrustProfileMode)
        }
        return (mode, .showTrustProfile)
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
    func recordFalseSkipSignal(
        podcastId: String,
        privacy: TrustScoringSignalPrivacy = .standard
    ) async {
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
            if privacy == .explicitBannerFeedback {
                signalLogObserver?(.explicitFeedbackOperationFailed)
                logger.warning("Banner feedback trust update failed")
            } else {
                signalLogObserver?(.standardFailure)
                logger.warning("Failed to mutate profile for \(podcastId) after false-skip signal: \(error.localizedDescription)")
            }
            return
        }

        guard let outcome else { return }
        guard privacy != .explicitBannerFeedback else { return }
        signalLogObserver?(.standardSuccess)
        let result = outcome.profile
        if let demoted = outcome.captured {
            logger.info("Demoted \(podcastId): \(demoted.from.rawValue) -> \(demoted.to.rawValue) trust=\(result.skipTrustScore, format: .fixed(precision: 2)) falseSignals=\(result.recentFalseSkipSignals)")
        } else {
            logger.info("False signal for \(podcastId): trust=\(result.skipTrustScore, format: .fixed(precision: 2)) falseSignals=\(result.recentFalseSkipSignals)")
        }
    }

    /// playhead-q45f: weaker false-skip variant for listen-rewinds.
    ///
    /// Mirrors `recordFalseSkipSignal` exactly except for the decrement
    /// magnitude (`weakFalseSignalPenalty` instead of `falseSignalPenalty`).
    /// Both pass through `evaluateDemotion`, so two rewinds in a row will
    /// flip an `auto`-mode show to `manual` — the q45f defect was that
    /// the pre-q45f path inside `AdDetectionService.recordListenRewind`
    /// only mutated `recentFalseSkipSignals` and skipped the state
    /// machine. The `Demotion`-capturing tuple shape is identical so
    /// `nonisolated(unsafe)` is unnecessary here too.
    func recordWeakFalseSkipSignal(podcastId: String) async {
        let config = self.config
        let outcome: (profile: PodcastProfile, captured: Demotion?)?
        do {
            outcome = try await store.updateProfileIfExistsCapturing(
                podcastId: podcastId,
                update: { profile in
                    let newFalseSignals = profile.recentFalseSkipSignals + 1
                    let newTrust = max(0, profile.skipTrustScore - config.weakFalseSignalPenalty)
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
            logger.warning("Failed to mutate profile for \(podcastId) after weak false-skip signal: \(error.localizedDescription)")
            return
        }

        guard let outcome else {
            // playhead-q45f cycle-1 M-2 / cycle-2 M-3 / cycle-3 L-B:
            // preserve the missing-profile telemetry that the pre-q45f
            // inline AdDetectionService.recordListenRewind block emitted.
            // Operationally this captures the "user tapped Listen on a
            // window for a podcast with no profile row" branch.
            // Severity is `warning` (matching the pre-q45f log) so the
            // diagnostics bundle's warning-and-above filter still
            // surfaces it. The phrase "No profile found for podcast"
            // mirrors the pre-q45f message so any external grep
            // (diagnostics dashboards, support-ticket triage) keeps
            // working. The pre-q45f counter
            // (`missingProfileListenRewindCount`) was an
            // AdDetectionService instance variable; the post-q45f
            // delegate has no equivalent state, so the count was
            // dropped (event-log replay through `ad_listen_rewinds`
            // gives a richer source of truth).
            logger.warning("No profile found for podcast \(podcastId) during listen-rewind recording; trust mutation skipped")
            return
        }
        let result = outcome.profile
        if let demoted = outcome.captured {
            logger.info("Weak-demoted \(podcastId): \(demoted.from.rawValue) -> \(demoted.to.rawValue) trust=\(result.skipTrustScore, format: .fixed(precision: 2)) falseSignals=\(result.recentFalseSkipSignals)")
        } else {
            logger.info("Weak false signal for \(podcastId): trust=\(result.skipTrustScore, format: .fixed(precision: 2)) falseSignals=\(result.recentFalseSkipSignals)")
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
    func recordFalseNegativeSignal(
        podcastId: String,
        privacy: TrustScoringSignalPrivacy = .standard
    ) async {
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
            if privacy == .explicitBannerFeedback {
                signalLogObserver?(.explicitFeedbackOperationFailed)
                logger.warning("Banner feedback trust update failed")
            } else {
                signalLogObserver?(.standardFailure)
                logger.warning("Failed to mutate profile for \(podcastId) after false-negative signal: \(error.localizedDescription)")
            }
            return
        }
        guard let result else { return }
        guard privacy != .explicitBannerFeedback else { return }
        signalLogObserver?(.standardSuccess)
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
    /// the same reason as `evaluatePromotion`. Internal (not fileprivate)
    /// is a deliberate test-seam: the replay-side q45f counterfactual gate
    /// (`Q45fReplayGate.replay` in the test target, reachable via
    /// `@testable import Playhead`) calls this directly so production
    /// retunes propagate to NARL eval without duplicating the switch.
    /// `recordFalseSkipSignal` and `recordWeakFalseSkipSignal` are the only
    /// production callers — **no new production sites**. Funnel any new
    /// demotion-triggering paths through one of those two recorders so the
    /// actor's serialized mutation, persistence, and logging stay coupled
    /// to the state-machine evaluation.
    internal static func evaluateDemotion(
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
