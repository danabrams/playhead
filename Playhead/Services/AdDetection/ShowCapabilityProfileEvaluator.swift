// ShowCapabilityProfileEvaluator.swift
// playhead-h6a6: pure-math layer for per-show capability profile
// classification. No SwiftData, no I/O — every input is plumbed in by
// the caller (`ShowCapabilityProfileStore` for persisted counts,
// `AdDetectionService` for the per-episode outcome, the runtime for the
// SLI gate predicate). Keeps the unit tests cheap and the persistence
// tests focused.
//
// Activation floor (NEVER bypassed):
//   * Profile-kind classification returns `.unknown` whenever the
//     show has < `activationFloorEpisodeCount` completed episodes OR
//     the SLI gate predicate returns `false` for the cohort.
//   * The feature-flag check is the CALLER's responsibility — when the
//     flag is OFF the evaluator must not be invoked. The bead spec
//     wants observation gated by the flag, so a flag-off path is a
//     pure no-op (no episode-outcome recording, no kind derivation,
//     no modulation).
//
// Predicate ordering: when multiple predicates fire for the same show
// (rare but possible — e.g. a chapter-rich show might also be
// host-read-only), the evaluator returns the FIRST matching kind
// according to a stable priority order. Priority is documented at
// `classify(...)`'s call site and tested in
// `ShowCapabilityProfileEvaluatorTests`.

import Foundation

// MARK: - ShowCapabilityEpisodeOutcome

/// One episode's contribution to the running per-show profile.
///
/// Each field is the boolean observation the evaluator increments for
/// THIS episode. The evaluator does not own the source-of-truth for
/// these signals — it consumes them from the producer:
///
///   * `chapterMatched` from the chapter-evidence builder (`ChapterEvidence`
///     entries that resolved against detected ads).
///   * `hostVoiced` from the lexical/FM ensemble's host-voice attribution
///     (an ad whose lexical scan matched the host-voice classifier).
///   * `sponsorDeclared` from the RSS/show-notes pre-seed
///     (`FeedDescriptionEvidenceBuilder` matches that resolved against
///     a detected ad).
///   * `dynamicInsertionShift` from the boundary-prior store: this
///     episode's detected boundaries are >`dynamicInsertionShiftSeconds`
///     away from the prior episode's boundaries on the same show.
///
/// Producers may not always have a confident reading — when in doubt
/// they pass `false`, which conservatively keeps the share-of-episodes
/// counter from advancing. This biases the activation toward false
/// negatives (profile stays `.unknown`) rather than false positives
/// (a profile gets observed before it should).
struct ShowCapabilityEpisodeOutcome: Sendable, Equatable {

    /// True when the episode's detected ads were matched to
    /// publisher-supplied chapters.
    let chapterMatched: Bool

    /// True when the episode's detected ads were classified as
    /// host-voiced.
    let hostVoiced: Bool

    /// True when the episode's pre-seed (RSS/show-notes) sponsor
    /// signal produced an ad-positive.
    let sponsorDeclared: Bool

    /// True when this episode's detected ad boundaries shifted from
    /// the prior episode's boundaries by more than the bead-spec
    /// "dynamic-insertion" delta (`dynamicInsertionShiftSeconds`).
    let dynamicInsertionShift: Bool

    /// Cosmetic constant exposed here so producers and tests share
    /// one definition of "boundaries shifted enough to count as
    /// dynamic-insertion". Bead spec: 5 seconds.
    static let dynamicInsertionShiftSeconds: TimeInterval = 5

    /// All-false outcome — used when the producer has nothing
    /// confident to report for this episode. Advances the
    /// `completedEpisodeCount` denominator without moving any of the
    /// per-predicate numerators.
    static let nothingObserved = ShowCapabilityEpisodeOutcome(
        chapterMatched: false,
        hostVoiced: false,
        sponsorDeclared: false,
        dynamicInsertionShift: false
    )
}

// MARK: - SLI gate

/// Caller-supplied predicate that returns `true` iff the show's
/// cohort has Phase-2 SLIs within defended bounds per playhead-d99.
///
/// The evaluator does not know how to read the SLI ledger — that's
/// the runtime's job (it owns the SLI emitter and aggregator). When
/// the predicate is unavailable (e.g. unit tests, headless runs), the
/// caller may pass `{ _ in true }` to skip the SLI half of the floor;
/// the tests cover both branches explicitly.
///
/// `showIdentifier` is forwarded so the predicate can map to the
/// cohort the show falls into (network, duration bucket, etc.).
typealias ShowCapabilitySLIGate = @Sendable (_ showIdentifier: String) -> Bool

// MARK: - Mutated state

/// Result of applying one episode's outcome to the running counters.
/// Returned by the evaluator so the persistence layer can write the
/// updated values back without re-doing the math.
struct ShowCapabilityProfileMutation: Sendable, Equatable {
    let completedEpisodeCount: Int
    let chapterMatchedEpisodeCount: Int
    let hostVoicedEpisodeCount: Int
    let sponsorDeclaredEpisodeCount: Int
    let dynamicInsertionEpisodeCount: Int
    let kind: ShowCapabilityProfileKind
}

// MARK: - Evaluator

enum ShowCapabilityProfileEvaluator {

    /// Apply `outcome` to the prior counters, then derive the new
    /// profile kind. The kind transitions to a non-`.unknown` value
    /// only when:
    ///
    ///   1. The post-increment `completedEpisodeCount` is ≥
    ///      `ShowCapabilityProfile.activationFloorEpisodeCount`.
    ///   2. The caller's SLI gate returns `true` for `showIdentifier`.
    ///   3. At least one predicate's threshold is met.
    ///
    /// `musicBedConfirmed` is the 2hpn signal — `true` iff
    /// `ShowMusicBedProfileResolving.snapshot(showIdentifier:)?
    /// .isConfirmed` is `true` at evaluation time. The bead requires
    /// this profile to consume 2hpn's existing confirmation logic
    /// rather than re-deriving its own jingle predicate.
    static func apply(
        outcome: ShowCapabilityEpisodeOutcome,
        showIdentifier: String,
        priorCompletedEpisodeCount: Int,
        priorChapterMatchedEpisodeCount: Int,
        priorHostVoicedEpisodeCount: Int,
        priorSponsorDeclaredEpisodeCount: Int,
        priorDynamicInsertionEpisodeCount: Int,
        musicBedConfirmed: Bool,
        sliGate: ShowCapabilitySLIGate
    ) -> ShowCapabilityProfileMutation {
        let completedCount = priorCompletedEpisodeCount + 1
        let chapterCount = priorChapterMatchedEpisodeCount + (outcome.chapterMatched ? 1 : 0)
        let hostCount = priorHostVoicedEpisodeCount + (outcome.hostVoiced ? 1 : 0)
        let sponsorCount = priorSponsorDeclaredEpisodeCount + (outcome.sponsorDeclared ? 1 : 0)
        let dynamicCount = priorDynamicInsertionEpisodeCount + (outcome.dynamicInsertionShift ? 1 : 0)

        let kind = classify(
            showIdentifier: showIdentifier,
            completedEpisodeCount: completedCount,
            chapterMatchedEpisodeCount: chapterCount,
            hostVoicedEpisodeCount: hostCount,
            sponsorDeclaredEpisodeCount: sponsorCount,
            dynamicInsertionEpisodeCount: dynamicCount,
            musicBedConfirmed: musicBedConfirmed,
            sliGate: sliGate
        )

        return ShowCapabilityProfileMutation(
            completedEpisodeCount: completedCount,
            chapterMatchedEpisodeCount: chapterCount,
            hostVoicedEpisodeCount: hostCount,
            sponsorDeclaredEpisodeCount: sponsorCount,
            dynamicInsertionEpisodeCount: dynamicCount,
            kind: kind
        )
    }

    /// Classify the show given the current counters.
    ///
    /// Returns `.unknown` when:
    ///   * `completedEpisodeCount` < `activationFloorEpisodeCount`, OR
    ///   * `sliGate(showIdentifier)` returns `false`, OR
    ///   * no predicate fires.
    ///
    /// Predicate priority order (when multiple fire):
    ///   1. `musicBedReliable`        — strongest external signal (2hpn-confirmed).
    ///   2. `chapterRich`             — publisher-supplied structure.
    ///   3. `hostReadOnly`            — content-shape; biases the lexical/FM path.
    ///   4. `sponsorDeclared`         — RSS pre-seed; biases the feed-description path.
    ///   5. `dynamicInsertionHeavy`   — fallback structural signal.
    ///
    /// Rationale: stronger external signals override weaker derived
    /// ones so a show with both publisher chapters AND host-read ads
    /// is observed as `chapter-rich` (the structure the budget
    /// modulator can exploit hardest) rather than `host-read-only`.
    /// The priority is the tested observable; see
    /// `ShowCapabilityProfileEvaluatorTests.priorityOrder`.
    static func classify(
        showIdentifier: String,
        completedEpisodeCount: Int,
        chapterMatchedEpisodeCount: Int,
        hostVoicedEpisodeCount: Int,
        sponsorDeclaredEpisodeCount: Int,
        dynamicInsertionEpisodeCount: Int,
        musicBedConfirmed: Bool,
        sliGate: ShowCapabilitySLIGate
    ) -> ShowCapabilityProfileKind {
        // Floor 1: activation floor on completed-episode count. Strict
        // less-than — exactly `activationFloorEpisodeCount` (5) counts
        // as "floor met".
        guard completedEpisodeCount >= ShowCapabilityProfile.activationFloorEpisodeCount else {
            return .unknown
        }

        // Floor 2: SLI gate (playhead-d99 defended bounds). The caller
        // owns the cohort lookup; we only consult the boolean it
        // returns.
        guard sliGate(showIdentifier) else {
            return .unknown
        }

        // Predicates in priority order. We never short-circuit before
        // floor checks — the floor is the more important invariant.

        if musicBedConfirmed {
            return .musicBedReliable
        }

        let ratio: (Int) -> Double = { numerator in
            // `completedEpisodeCount >= activationFloorEpisodeCount >= 1`
            // by the floor check above, so division is safe.
            Double(numerator) / Double(completedEpisodeCount)
        }

        if ratio(chapterMatchedEpisodeCount) >= ShowCapabilityProfile.chapterRichEpisodeRatio {
            return .chapterRich
        }

        if ratio(hostVoicedEpisodeCount) >= ShowCapabilityProfile.hostReadOnlyEpisodeRatio {
            return .hostReadOnly
        }

        if ratio(sponsorDeclaredEpisodeCount) >= ShowCapabilityProfile.sponsorDeclaredEpisodeRatio {
            return .sponsorDeclared
        }

        if ratio(dynamicInsertionEpisodeCount) >= ShowCapabilityProfile.dynamicInsertionEpisodeRatio {
            return .dynamicInsertionHeavy
        }

        return .unknown
    }
}
