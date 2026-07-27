// AutoSkipPrecisionGate.swift
// playhead-gtt9.11: Split detection recall from auto-skip precision.
//
// Why this exists
// ---------------
// Before this file, the detector had ONE knob (the
// `SegmentAggregator.promotionThreshold = 0.40`) that conflated two very
// different operating goals:
//
//   Detection recall → "don't miss ads" (cheap: annoying when missed).
//   Auto-skip precision → "don't skip real content" (expensive: breaks trust).
//
// The 2026-04-23 expert review (§5) proposed three distinct operating
// layers, with different confidence floors and — crucially — an
// orthogonal **safety-signal conjunction** for auto-skip. This file is the
// downstream gate that turns an aggregator `AdSegmentCandidate` (or a
// single-window classifier result) into one of three classifications:
//
//   1. detectionOnly     — score below `uiCandidateThreshold`. No AdWindow
//                          persisted. Telemetry / replay-tool visibility only.
//                          (The aggregator today already filters below its
//                          own `promotionThreshold = 0.40`; this layer
//                          encodes that decision in a single place so the
//                          single-window fast path honors the same rule.)
//   2. uiCandidate       — score ≥ `uiCandidateThreshold` but either below
//                          `autoSkipThreshold` OR the safety-signal gate did
//                          not fire. Persisted as AdWindow with
//                          `eligibilityGate = "markOnly"`. Visible as
//                          "possible ad" in the UI; NEVER auto-skipped.
//   3. autoSkipEligible  — score ≥ `autoSkipThreshold` AND duration is
//                          plausible AND ≥1 safety signal fires. Persisted
//                          as AdWindow with `eligibilityGate = "autoSkip"`.
//                          Eligible for auto-skip via the orchestrator.
//                          (Note: `"autoSkip"` is the literal raw value
//                          stamped by `AdDetectionService.precisionGateLabel`;
//                          it is NOT a `SkipEligibilityGate` enum case. The
//                          orchestrator's `receiveAdWindows` decode treats
//                          it as a non-`.markOnly` value, falling through
//                          to the standard skip path.)
//
// Scope guardrails
// ----------------
// This file MUST NOT:
//   - invent uncalibrated safety signals. This file consumes signals supplied
//     by existing lexical, acoustic, correction, metadata, and catalog layers.
//   - calibrate thresholds on real data. gtt9.3 owns calibration. The
//     starting values (uiCandidate=0.40, autoSkip=0.55) are documented in
//     `AdDetectionConfig` and are NOT re-tuned here.
//   - modify `SegmentAggregator` internals.
//   - change `DecisionMapper` / `SkipPolicyMatrix` semantics — only the
//     `eligibilityGate` stamped on the output AdWindow, and the
//     `finalDecision.action` string logged for replay.
//
// Signal audit (2026-04-24)
// -------------------------
// Bead spec required ≥2 working safety signals. We ship five:
//
//   • strongLexicalAdPhrase          ← LexicalCandidate.categories
//   • sustainedAcousticAdSignature   ← FeatureWindow.musicBedLevel
//   • metadataSlotPrior              ← segment position / episodeDuration
//   • userConfirmedLocalPattern      ← UserCorrectionStore.correctionBoostFactor
//   • catalogMatch                   ← version-compatible AdCatalogStore match
//
// Catalog evidence is diagnostic-only: it is returned in the fired-signal set
// for observability, but cannot be the decision-bearing signal that admits an
// automatic skip. The other four signals are available as independent
// corroborators in the pipeline today.

import Foundation

// MARK: - SafetySignal

/// One of the safety signals that a high-score segment can exhibit. Any
/// non-catalog signal can admit the segment to the auto-skip path, provided
/// the confidence and duration gates also pass. `.catalogMatch` remains in the
/// set only as diagnostic context and never supplies automatic authority.
///
/// Conservative composition: each signal is independently produced by a
/// different pipeline layer, so one firing is strong corroboration that
/// the classifier's confidence isn't a hallucination.
enum SafetySignal: String, Sendable, Hashable, CaseIterable {
    /// Tier 2 lexical evidence included ≥1 category that is an ad-content
    /// indicator (sponsor, promo code, URL CTA, purchase language). The
    /// weak `.transitionMarker` category alone does NOT count — boundary
    /// markers on their own are too common in normal speech.
    case strongLexicalAdPhrase

    /// Feature windows overlapping the segment have sustained music-bed
    /// presence (≥ `minMusicBedCoverage` of the span classified as
    /// `.background` or `.foreground` music). Host-read ad segments often
    /// use a music bed; news/editorial content usually does not.
    case sustainedAcousticAdSignature

    /// The segment's center lies in a pre-roll or post-roll slot
    /// (first `slotFraction` or last `slotFraction` of the episode). Mid-
    /// roll is deliberately excluded — mid-episode positions are too
    /// common to be a useful prior on their own.
    case metadataSlotPrior

    /// The user has previously corrected this asset (reported a missed ad
    /// or confirmed a skip). `UserCorrectionStore.correctionBoostFactor`
    /// returns > 1.0 when at least one false-negative correction exists
    /// for the asset. This is a weak but honest "this listener has
    /// engaged with ads on this episode before" signal.
    case userConfirmedLocalPattern

    /// A version-compatible, exact-show AdCatalogStore match at or above the
    /// calibrated catalog floor. The service layer still requires a strong
    /// non-catalog corroborator before automatic admission.
    case catalogMatch
}

// MARK: - AutoSkipClassification

/// The three-way classification this gate produces from an input segment.
enum AutoSkipClassification: Sendable, Equatable {
    /// Score below `uiCandidateThreshold`. Caller does NOT persist an
    /// AdWindow; the segment is telemetry-only.
    case detectionOnly

    /// Score ≥ `uiCandidateThreshold` but did not clear the auto-skip
    /// gate (either below `autoSkipThreshold`, duration implausible, or
    /// no safety signal fired). Caller persists an AdWindow with
    /// `eligibilityGate = "markOnly"`.
    case uiCandidate(reason: MarkOnlyReason)

    /// Score ≥ `autoSkipThreshold`, duration plausible, and ≥1
    /// decision-bearing (non-catalog) safety signal fired. Caller persists an
    /// AdWindow with
    /// `eligibilityGate = "autoSkip"` (the literal raw value emitted by
    /// `AdDetectionService.precisionGateLabel`, NOT a
    /// `SkipEligibilityGate` enum case); the orchestrator may auto-skip.
    case autoSkipEligible(firedSignals: Set<SafetySignal>)
}

// MARK: - MarkOnlyReason

/// Why a segment with score ≥ `uiCandidateThreshold` was demoted to
/// `uiCandidate` instead of being admitted to auto-skip. Emitted on the
/// decision log so replay tooling can quantify the precision gate's
/// rejection mix.
enum MarkOnlyReason: String, Sendable, Equatable {
    /// Score ≥ uiCandidateThreshold but < autoSkipThreshold.
    case belowAutoSkipThreshold
    /// Score ≥ autoSkipThreshold but duration outside
    /// `typicalAdDuration`.
    case durationImplausible
    /// Score ≥ autoSkipThreshold, duration plausible, but zero safety
    /// signals fired.
    case noSafetySignals
}

// MARK: - Config

/// Tunables for the precision gate. All defaults match `AdDetectionConfig`
/// as of gtt9.11 landing. `AutoSkipPrecisionGate` is a pure value type; it
/// never reads `AdDetectionConfig` directly — the caller passes in the
/// resolved numbers. This keeps the gate trivially unit-testable.
struct AutoSkipPrecisionGateConfig: Sendable, Equatable {
    /// Minimum score for a UI-candidate persistence. Segments scoring
    /// below this are detection-only. Default 0.40 matches the existing
    /// `SegmentAggregator.promotionThreshold` so the aggregator's
    /// existing promotion filter and the UI gate agree on the floor.
    let uiCandidateThreshold: Double

    /// Minimum score for auto-skip consideration. Stricter than
    /// `uiCandidateThreshold` (0.40 vs 0.55 as shipped) so "possible ad"
    /// markers appear at a lower confidence than actual auto-skips.
    ///
    /// Rationale for the initial 0.55 value: `SegmentAggregator`'s
    /// `highConfidenceThreshold = 0.60` is the single-window seed for
    /// segment creation. A segment that opens because multiple sub-0.60
    /// windows fire coherently naturally averages below 0.60 even when
    /// it IS an ad — requiring 0.60 for the auto-skip gate would
    /// re-introduce the single-knob problem we're solving. 0.55 sits
    /// midway between the 0.40 promotion floor and the 0.60 single-
    /// window seed, making auto-skip meaningfully stricter than
    /// UI-candidate persistence without demanding single-window
    /// equivalent confidence from every aggregated segment. This number
    /// is NOT calibrated on real data; gtt9.3 owns calibration.
    let autoSkipThreshold: Double

    /// Range of plausible ad durations. Segments outside this range are
    /// demoted to `uiCandidate` with reason `.durationImplausible`. The
    /// lower bound excludes micro-segments (likely classifier noise);
    /// the upper bound excludes multi-ad-break coalescence that would
    /// be safer as mark-only.
    let typicalAdDuration: ClosedRange<TimeInterval>

    /// Minimum fraction of the segment's span that must be labeled
    /// `.background` or `.foreground` music in the feature windows for
    /// `sustainedAcousticAdSignature` to fire. Conservative default 0.20
    /// avoids misfiring on short music stingers in non-ad content.
    let minMusicBedCoverage: Double

    /// Fraction of `episodeDuration` at the start/end considered a slot
    /// position for `metadataSlotPrior` to fire. 0.10 means "first 10%
    /// or last 10% of the episode."
    let slotFraction: Double

    /// playhead-gtt9.13 / playhead-o4qr: Minimum compatible
    /// `catalogMatchSimilarity` to fire `SafetySignal.catalogMatch`. It
    /// defaults to the empirically calibrated catalog admission floor and is
    /// independently configurable for controlled gate experiments.
    let catalogMatchSignalFloor: Float

    /// Canonical defaults. The classifier thresholds retain their existing
    /// values; the catalog signal floor is calibrated separately against the
    /// committed device fixture.
    static let `default` = AutoSkipPrecisionGateConfig(
        uiCandidateThreshold: 0.40,
        autoSkipThreshold: 0.55,
        typicalAdDuration: GlobalPriorDefaults.standard.typicalAdDuration,
        minMusicBedCoverage: 0.20,
        slotFraction: 0.10,
        catalogMatchSignalFloor: AdCatalogStore.defaultSimilarityFloor
    )

    init(
        uiCandidateThreshold: Double,
        autoSkipThreshold: Double,
        typicalAdDuration: ClosedRange<TimeInterval>,
        minMusicBedCoverage: Double,
        slotFraction: Double,
        catalogMatchSignalFloor: Float = AdCatalogStore.defaultSimilarityFloor
    ) {
        self.uiCandidateThreshold = uiCandidateThreshold
        self.autoSkipThreshold = autoSkipThreshold
        self.typicalAdDuration = typicalAdDuration
        self.minMusicBedCoverage = minMusicBedCoverage
        self.slotFraction = slotFraction
        self.catalogMatchSignalFloor = catalogMatchSignalFloor
    }
}

// MARK: - Inputs

/// Per-call inputs the gate needs to decide the classification and emit
/// safety signals. Callers on the aggregator path and the single-window
/// path both build one of these; the gate is path-agnostic.
struct AutoSkipPrecisionGateInput: Sendable {
    /// Exact analysis asset that owns both the segment and every acoustic
    /// feature row supplied below.
    let analysisAssetId: String
    let segmentStartTime: Double
    let segmentEndTime: Double
    /// The score that drives the threshold comparison. For aggregator
    /// segments this is `AdSegmentCandidate.segmentScore`; for single-
    /// window classifier hits this is `ClassifierResult.adProbability`.
    let segmentScore: Double
    let episodeDuration: Double
    /// Feature windows overlapping the segment. Used to detect sustained
    /// music-bed coverage. Pass an empty array when no features are
    /// available (disables the acoustic signal for this decision).
    let overlappingFeatureWindows: [FeatureWindow]
    /// Lexical pattern categories present in any evidence that seeded
    /// this segment. For the aggregator path, this is the union across
    /// any lexical candidates that overlap the segment. Empty set when
    /// the segment has no lexical seed (Tier 1-only segments).
    let lexicalCategories: Set<LexicalPatternCategory>
    /// `UserCorrectionStore.correctionBoostFactor(for:)` for this asset.
    /// Pass 1.0 to disable the user-correction signal.
    let userCorrectionBoostFactor: Double
    /// playhead-gtt9.13: Top catalog-match similarity for this segment
    /// from `AdCatalogStore`, in `[0, 1]`. Pass 0 when no catalog match
    /// (or when the catalog is unavailable) — that disables the
    /// `catalogMatch` signal. Any value ≥
    /// `catalogMatchSignalFloor` fires `SafetySignal.catalogMatch`.
    let catalogMatchSimilarity: Float

    init(
        analysisAssetId: String,
        segmentStartTime: Double,
        segmentEndTime: Double,
        segmentScore: Double,
        episodeDuration: Double,
        overlappingFeatureWindows: [FeatureWindow],
        lexicalCategories: Set<LexicalPatternCategory>,
        userCorrectionBoostFactor: Double,
        catalogMatchSimilarity: Float = 0
    ) {
        self.analysisAssetId = analysisAssetId
        self.segmentStartTime = segmentStartTime
        self.segmentEndTime = segmentEndTime
        self.segmentScore = segmentScore
        self.episodeDuration = episodeDuration
        self.overlappingFeatureWindows = overlappingFeatureWindows
        self.lexicalCategories = lexicalCategories
        self.userCorrectionBoostFactor = userCorrectionBoostFactor
        self.catalogMatchSimilarity = catalogMatchSimilarity
    }

    var segmentDuration: TimeInterval {
        max(0, segmentEndTime - segmentStartTime)
    }
}

// MARK: - Gate

/// Pure, stateless classifier. Consumers (`AdDetectionService` in the
/// aggregator and single-window paths; `SkipOrchestrator` when it needs
/// to re-evaluate a persisted marker) call `classify(input:config:)` and
/// branch on the returned `AutoSkipClassification`.
enum AutoSkipPrecisionGate {

    /// Classify one input. Deterministic, allocation-light, no async.
    static func classify(
        input: AutoSkipPrecisionGateInput,
        config: AutoSkipPrecisionGateConfig = .default
    ) -> AutoSkipClassification {
        guard RecurrenceMaterialIdentity.canonicalIdentifier(
                  input.analysisAssetId
              ) != nil,
              input.segmentStartTime.isFinite,
              input.segmentEndTime.isFinite,
              input.segmentScore.isFinite,
              input.episodeDuration.isFinite,
              input.segmentStartTime >= 0,
              input.segmentEndTime > input.segmentStartTime,
              input.episodeDuration >= 0,
              input.episodeDuration == 0
                || input.segmentEndTime <= input.episodeDuration,
              (0...1).contains(input.segmentScore),
              config.uiCandidateThreshold.isFinite,
              config.autoSkipThreshold.isFinite,
              config.typicalAdDuration.lowerBound.isFinite,
              config.typicalAdDuration.upperBound.isFinite,
              config.minMusicBedCoverage.isFinite,
              config.slotFraction.isFinite,
              config.catalogMatchSignalFloor.isFinite,
              (0...1).contains(config.uiCandidateThreshold),
              (0...1).contains(config.autoSkipThreshold),
              config.uiCandidateThreshold <= config.autoSkipThreshold,
              config.typicalAdDuration.lowerBound > 0,
              config.minMusicBedCoverage > 0,
              config.minMusicBedCoverage <= 1,
              config.slotFraction > 0,
              config.slotFraction <= 0.5,
              (0...1).contains(config.catalogMatchSignalFloor) else {
            return .detectionOnly
        }
        // Layer 1: detection-only gate.
        if input.segmentScore < config.uiCandidateThreshold {
            return .detectionOnly
        }

        // Layer 2: UI-candidate with "below autoSkipThreshold" reason.
        if input.segmentScore < config.autoSkipThreshold {
            return .uiCandidate(reason: .belowAutoSkipThreshold)
        }

        // Layer 3: duration plausibility. A score that clears
        // `autoSkipThreshold` on a 3-second "segment" or a 10-minute
        // "segment" is probably not an ad. Clamp to UI-candidate.
        if !config.typicalAdDuration.contains(input.segmentDuration) {
            return .uiCandidate(reason: .durationImplausible)
        }

        // Layer 4: safety-signal conjunction. Learned catalog evidence is
        // diagnostic-only, so it is preserved in the returned signal set but
        // removed when deciding whether any independent corroborator fired.
        let signals = collectSafetySignals(for: input, config: config)
        let decisionBearingSignals = signals.subtracting([.catalogMatch])
        if decisionBearingSignals.isEmpty {
            return .uiCandidate(reason: .noSafetySignals)
        }

        return .autoSkipEligible(firedSignals: signals)
    }

    /// Return the set of safety signals that fire for this input.
    /// Exposed (not private) so unit tests can exercise individual
    /// signals without driving the full classification tree.
    static func collectSafetySignals(
        for input: AutoSkipPrecisionGateInput,
        config: AutoSkipPrecisionGateConfig = .default
    ) -> Set<SafetySignal> {
        var fired: Set<SafetySignal> = []

        if isStrongLexicalAdPhrase(categories: input.lexicalCategories) {
            fired.insert(.strongLexicalAdPhrase)
        }

        if isSustainedAcousticAdSignature(
            analysisAssetId: input.analysisAssetId,
            featureWindows: input.overlappingFeatureWindows,
            segmentStart: input.segmentStartTime,
            segmentEnd: input.segmentEndTime,
            minCoverage: config.minMusicBedCoverage
        ) {
            fired.insert(.sustainedAcousticAdSignature)
        }

        if isMetadataSlotPrior(
            segmentCenter: (input.segmentStartTime + input.segmentEndTime) / 2,
            episodeDuration: input.episodeDuration,
            slotFraction: config.slotFraction
        ) {
            fired.insert(.metadataSlotPrior)
        }

        if input.userCorrectionBoostFactor.isFinite,
           input.userCorrectionBoostFactor > 1.0,
           input.userCorrectionBoostFactor <= 2.0 {
            fired.insert(.userConfirmedLocalPattern)
        }

        // playhead-gtt9.13: catalog-match signal.
        if input.catalogMatchSimilarity.isFinite,
           (0...1).contains(input.catalogMatchSimilarity),
           input.catalogMatchSimilarity >= config.catalogMatchSignalFloor {
            fired.insert(.catalogMatch)
        }

        return fired
    }

    // MARK: - Signal implementations

    /// Fires when the lexical category set contains at least one "ad
    /// content" category. `.transitionMarker` is a weak boundary hint
    /// (e.g., "anyway", "back to the show") that is extremely common in
    /// normal speech; counting it as a safety signal would undermine
    /// the precision gate.
    static func isStrongLexicalAdPhrase(
        categories: Set<LexicalPatternCategory>
    ) -> Bool {
        let strong: Set<LexicalPatternCategory> = [
            .sponsor, .promoCode, .urlCTA, .purchaseLanguage
        ]
        return !categories.isDisjoint(with: strong)
    }

    /// Fires when the fraction of segment wall-time covered by
    /// `.background` or `.foreground` music-bed feature windows is at
    /// least `minCoverage`. Tolerant to partial overlap at the segment
    /// boundaries: counts only the intersected extent, not the full
    /// feature-window span.
    static func isSustainedAcousticAdSignature(
        analysisAssetId: String,
        featureWindows: [FeatureWindow],
        segmentStart: Double,
        segmentEnd: Double,
        minCoverage: Double
    ) -> Bool {
        guard segmentStart.isFinite,
              segmentEnd.isFinite,
              minCoverage.isFinite,
              RecurrenceMaterialIdentity.canonicalIdentifier(
                  analysisAssetId
              ) != nil,
              segmentStart >= 0,
              segmentEnd > segmentStart,
              minCoverage > 0,
              minCoverage <= 1 else {
            return false
        }
        let segmentDuration = segmentEnd - segmentStart
        guard !featureWindows.isEmpty,
              featureWindows.allSatisfy({ window in
                  window.analysisAssetId == analysisAssetId
                      && window.featureVersion
                        == FeatureExtractionConfig.default.featureVersion
                      && window.startTime.isFinite
                      && window.endTime.isFinite
                      && window.startTime >= 0
                      && window.endTime > window.startTime
                      && MusicDetectionConfig.supportedWindowDurations
                          .contains { duration in
                              abs(
                                  (window.endTime - window.startTime)
                                      - duration
                              ) <= 0.001
                          }
                      && [window.rms, window.spectralFlux].allSatisfy {
                          $0.isFinite && $0 >= 0
                      }
                      && [
                          window.musicProbability,
                          window.speakerChangeProxyScore,
                          window.musicBedChangeScore,
                          window.musicBedOnsetScore,
                          window.musicBedOffsetScore,
                          window.pauseProbability,
                      ].allSatisfy {
                          $0.isFinite && (0...1).contains($0)
                      }
              }) else {
            return false
        }
        let featureIntervals = featureWindows
            .map { (start: $0.startTime, end: $0.endTime) }
            .sorted {
                if $0.start != $1.start {
                    return $0.start < $1.start
                }
                return $0.end < $1.end
            }
        guard zip(
            featureIntervals,
            featureIntervals.dropFirst()
        ).allSatisfy({ pair in
            pair.0.end <= pair.1.start
        }) else {
            // Production extraction emits a non-overlapping cohort. Duplicate
            // or overlapping persisted rows are malformed material, not
            // additional acoustic observations, and must not mint independent
            // automatic authority.
            return false
        }
        let musicIntervals = featureWindows.compactMap {
            fw -> (start: Double, end: Double)? in
            switch fw.musicBedLevel {
            case .background, .foreground:
                let lo = max(fw.startTime, segmentStart)
                let hi = min(fw.endTime, segmentEnd)
                return hi > lo ? (lo, hi) : nil
            case .none:
                return nil
            }
        }
        .sorted {
            if $0.start != $1.start {
                return $0.start < $1.start
            }
            return $0.end < $1.end
        }
        guard let first = musicIntervals.first else { return false }

        // Clip at the segment edges and measure wall time. The interval union
        // remains defensive against future changes to the cohort contract.
        var musicSeconds: Double = 0
        var currentStart = first.start
        var currentEnd = first.end
        for interval in musicIntervals.dropFirst() {
            if interval.start <= currentEnd {
                currentEnd = max(currentEnd, interval.end)
            } else {
                musicSeconds += currentEnd - currentStart
                currentStart = interval.start
                currentEnd = interval.end
            }
        }
        musicSeconds += currentEnd - currentStart

        return (musicSeconds / segmentDuration) >= minCoverage
    }

    /// Fires when the segment center lies within `slotFraction` of the
    /// start or end of the episode. Conservative on purpose: mid-roll
    /// positions (the bulk of the episode) are excluded because
    /// "something happens in the middle of an episode" is not useful
    /// prior information on its own.
    static func isMetadataSlotPrior(
        segmentCenter: Double,
        episodeDuration: Double,
        slotFraction: Double
    ) -> Bool {
        guard segmentCenter.isFinite,
              episodeDuration.isFinite,
              slotFraction.isFinite,
              episodeDuration > 0,
              segmentCenter >= 0,
              segmentCenter <= episodeDuration,
              slotFraction > 0,
              slotFraction <= 0.5
        else { return false }

        let preRollEnd = episodeDuration * slotFraction
        let postRollStart = episodeDuration * (1.0 - slotFraction)

        return segmentCenter <= preRollEnd || segmentCenter >= postRollStart
    }
}

// MARK: - Decision-log action strings

/// `finalDecision.action` values stamped by the precision gate. Declared
/// here (rather than inlining string literals in `AdDetectionService`) so
/// replay tooling has one symbol to reference. The
/// `segmentAggregatorPromoted` string continues to be stamped by the
/// aggregator wiring for audit, alongside one of these gate actions.
enum AutoSkipPrecisionGateAction {
    /// A segment crossed `uiCandidateThreshold` but the precision gate
    /// demoted it to mark-only (either below autoSkipThreshold, duration
    /// implausible, or no safety signal fired).
    static let markOnlyCandidate: String = "markOnlyCandidate"
}
