// BackfillEvidenceFusion.swift
// Phase 6 (playhead-4my.6.1): Evidence ledger accumulation and decision mapping.
//
// Design:
//   • BackfillEvidenceFusion: pure value type, accumulates per-source evidence
//     into an EvidenceLedgerEntry ledger controlled by FMBackfillMode.
//   • DecisionMapper: pure value type, converts the ledger into a DecisionResult
//     carrying proposalConfidence, skipConfidence, and eligibilityGate.
//   • FM Positive-Only Rule: noAds/abstain/uncertain FM entries are silently dropped
//     regardless of mode — they affect scheduling/telemetry only, not the ledger.
//   • Gate blocks action at decision level; score is never clamped by the gate.

import Foundation
import OSLog

// MARK: - ClassificationTrustMatrix (ef2.4.5)

/// Pure lookup mapping (CommercialIntent × Ownership) → classificationTrust.
///
/// Trust modulates FM evidence weight in `BackfillEvidenceFusion.buildLedger()`.
/// Whether to *skip* host-reads is a policy decision (SkipPolicyMatrix, ef2.4.1),
/// NOT a classification decision — hence `paid|show` returns 1.0 (not discounted).
///
/// | CommercialIntent | Ownership   | classificationTrust |
/// |-----------------|-------------|---------------------|
/// | paid            | thirdParty  | 1.0                 |
/// | paid            | show        | 1.0                 |
/// | owned           | show        | 0.7                 |
/// | affiliate       | thirdParty  | 0.9                 |
/// | organic         | any         | 0.15                |
/// | unknown         | any         | 0.6                 |
enum ClassificationTrustMatrix {
    /// Fallback trust for unmapped (CommercialIntent × Ownership) combinations.
    static let fallback: Double = 0.6

    /// Look up classificationTrust for a given (CommercialIntent, Ownership) pair.
    static func trust(
        commercialIntent: CommercialIntent,
        ownership: Ownership
    ) -> Double {
        switch commercialIntent {
        case .paid:
            // paid|thirdParty = 1.0, paid|show = 1.0, paid|anything = 1.0
            return 1.0
        case .owned:
            if ownership == .show { return 0.7 }
            return fallback
        case .affiliate:
            if ownership == .thirdParty { return 0.9 }
            return fallback
        case .organic:
            return 0.15
        case .unknown:
            return fallback
        }
    }
}

// MARK: - FusionWeightConfig

/// Per-source weight caps for evidence fusion. v1 defaults reflect initial calibration.
/// All caps are configurable to allow future tuning without changing fusion logic.
struct FusionWeightConfig: Sendable {
    /// Maximum weight contribution from a single FM entry.
    let fmCap: Double
    /// Maximum weight contribution from the classifier score entry.
    let classifierCap: Double
    /// Maximum weight contribution from a single lexical entry.
    let lexicalCap: Double
    /// Maximum weight contribution from a single acoustic entry.
    let acousticCap: Double
    /// Maximum weight contribution from a single catalog entry.
    let catalogCap: Double
    /// Maximum weight contribution from fingerprint evidence.
    let fingerprintCap: Double

    init(
        fmCap: Double = 0.4,
        classifierCap: Double = 0.3,
        lexicalCap: Double = 0.2,
        acousticCap: Double = 0.2,
        catalogCap: Double = 0.2,
        fingerprintCap: Double = 0.25
    ) {
        self.fmCap = fmCap
        self.classifierCap = classifierCap
        self.lexicalCap = lexicalCap
        self.acousticCap = acousticCap
        self.catalogCap = catalogCap
        self.fingerprintCap = fingerprintCap
    }
}

// MARK: - BackfillEvidenceFusion

/// Accumulates evidence from all sources into a ledger of `EvidenceLedgerEntry` items.
///
/// FM mode gating:
///   - `.off` / `.shadow`    → FM entries excluded from decision ledger
///   - `.rescoreOnly`        → FM entries join ledger for existing candidates
///   - `.proposalOnly`       → FM can propose new regions; rescoring follows .off semantics
///   - `.full`               → all FM entries join ledger
///
/// FM Positive-Only Rule: only `containsAd` disposition entries contribute. Entries
/// with `.noAds`, `.uncertain`, or `.abstain` dispositions are silently dropped
/// regardless of mode — they affect scheduling and diagnostics only.
struct BackfillEvidenceFusion: Sendable {
    private static let logger = Logger(subsystem: "com.playhead", category: "BackfillEvidenceFusion")

    let span: DecodedSpan
    /// Raw classifier score (0–1). Converted to a capped weight and added as a `.classifier` entry.
    let classifierScore: Double
    /// Pre-constructed FM ledger entries (caller's responsibility to create from FM scan results).
    let fmEntries: [EvidenceLedgerEntry]
    /// Pre-constructed lexical ledger entries.
    let lexicalEntries: [EvidenceLedgerEntry]
    /// Pre-constructed acoustic ledger entries.
    let acousticEntries: [EvidenceLedgerEntry]
    /// Pre-constructed catalog ledger entries.
    let catalogEntries: [EvidenceLedgerEntry]
    /// Pre-constructed fingerprint ledger entries (Phase 9).
    var fingerprintEntries: [EvidenceLedgerEntry] = []
    let mode: FMBackfillMode
    let config: FusionWeightConfig

    /// Build the decision ledger for this span.
    ///
    /// Always includes: classifier, lexical, acoustic, catalog entries.
    /// Conditionally includes FM entries based on `mode.contributesToExistingCandidateLedger`.
    func buildLedger() -> [EvidenceLedgerEntry] {
        var ledger: [EvidenceLedgerEntry] = []

        // Classifier entry: always included (it's the old legacy path, always present)
        // Clamp to [0, cap]: adProbability is documented as [0,1] but defensive clamping prevents
        // a negative classifier score from producing a negative ledger weight.
        let classifierWeight = min(max(0.0, classifierScore) * config.classifierCap, config.classifierCap)
        ledger.append(EvidenceLedgerEntry(
            source: .classifier,
            weight: classifierWeight,
            detail: .classifier(score: classifierScore)
        ))

        // FM entries: gated by mode, filtered to containsAd only (Positive-Only Rule).
        // Non-positive dispositions (noAds/uncertain/abstain) are intentionally dropped;
        // they inform scheduling and telemetry only, not the ledger.
        if mode.contributesToExistingCandidateLedger {
            let allFMEntries = fmEntries.filter { entry in
                guard case .fm = entry.detail else { return false }
                return true
            }
            let positiveOnlyFMEntries = allFMEntries.filter { entry in
                guard case .fm(let disposition, _, _) = entry.detail else { return false }
                return disposition == .containsAd
            }
            let droppedCount = allFMEntries.count - positiveOnlyFMEntries.count
            if droppedCount > 0 {
                // Log so callers can observe the Positive-Only Rule in action.
                Self.logger.info("FM Positive-Only Rule dropped \(droppedCount)/\(allFMEntries.count) FM entries (non-containsAd dispositions).")
            }
            for entry in positiveOnlyFMEntries {
                // ef2.4.5: modulate FM weight by classificationTrust before capping.
                // Trust is derived from (CommercialIntent × Ownership) at entry creation
                // time and stored on the entry. Default of 1.0 preserves pre-ef2.4.5 behavior.
                let trustModulatedWeight = entry.weight * entry.classificationTrust
                let capped = EvidenceLedgerEntry(
                    source: .fm,
                    weight: min(trustModulatedWeight, config.fmCap),
                    detail: entry.detail,
                    classificationTrust: entry.classificationTrust
                )
                ledger.append(capped)
            }
        }

        // Lexical entries: always included
        for entry in lexicalEntries {
            let capped = EvidenceLedgerEntry(
                source: .lexical,
                weight: min(entry.weight, config.lexicalCap),
                detail: entry.detail
            )
            ledger.append(capped)
        }

        // Acoustic entries: always included
        for entry in acousticEntries {
            let capped = EvidenceLedgerEntry(
                source: .acoustic,
                weight: min(entry.weight, config.acousticCap),
                detail: entry.detail
            )
            ledger.append(capped)
        }

        // Catalog entries: always included
        for entry in catalogEntries {
            let capped = EvidenceLedgerEntry(
                source: .catalog,
                weight: min(entry.weight, config.catalogCap),
                detail: entry.detail
            )
            ledger.append(capped)
        }

        // Fingerprint entries: always included (Phase 9)
        for entry in fingerprintEntries {
            let capped = EvidenceLedgerEntry(
                source: .fingerprint,
                weight: min(entry.weight, config.fingerprintCap),
                detail: entry.detail
            )
            ledger.append(capped)
        }

        return ledger
    }
}

// MARK: - DecisionResult

/// The output of `DecisionMapper`: three orthogonal decision signals.
///
/// `eligibilityGate` can block action without affecting the honesty of `skipConfidence`.
struct DecisionResult: Sendable, Equatable {
    /// Raw confidence estimate from summing all ledger entry weights, capped at 1.0.
    let proposalConfidence: Double
    /// Calibrated score for SkipOrchestrator (0–1 scale). Derived from proposalConfidence
    /// via a linear map. Not clamped by the eligibility gate.
    let skipConfidence: Double
    /// Whether this span is actionable. A blocked gate does not reduce the score.
    let eligibilityGate: SkipEligibilityGate
}

// MARK: - DecisionMapper

/// Converts an accumulated evidence ledger into a `DecisionResult`.
///
/// Quorum check reads `anchorProvenance` from the span — does NOT re-derive
/// multi-window consensus. The quorum rules are:
///
/// **fmConsensus provenance:**
/// Multi-window consensus is already satisfied. Remaining checks:
///   - 2+ distinct evidence kinds in ledger
///   - transcript quality == .good
///   - span duration in [5s, 180s]
///
/// **fmAcousticCorroborated provenance:**
/// Single-window FM + acoustic co-location. Needs external corroboration:
///   - at least one lexical, catalog, or acoustic entry in ledger
///
/// **No FM provenance:**
/// Quorum check is not applicable. Gate defaults to `.eligible` if score > 0.
struct DecisionMapper: Sendable {
    let span: DecodedSpan
    let ledger: [EvidenceLedgerEntry]
    let config: FusionWeightConfig
    let transcriptQuality: TranscriptQuality
    /// Phase 7.2: Combined correction factor pre-computed by the caller (actor context).
    /// < 1.0 = false-positive suppression (user said "not an ad").
    /// > 1.0 = false-negative boost (user said "hearing an ad").
    /// Default of 1.0 means no active corrections.
    let correctionFactor: Double
    /// ef2.4.3: Per-source calibration profile. v0 (identity) is the default.
    let calibrationProfile: ScoreCalibrationProfile

    init(
        span: DecodedSpan,
        ledger: [EvidenceLedgerEntry],
        config: FusionWeightConfig,
        transcriptQuality: TranscriptQuality = .good,
        correctionFactor: Double = 1.0,
        calibrationProfile: ScoreCalibrationProfile = .v0
    ) {
        self.span = span
        self.ledger = ledger
        self.config = config
        self.transcriptQuality = transcriptQuality
        self.correctionFactor = correctionFactor
        self.calibrationProfile = calibrationProfile
    }

    func map() -> DecisionResult {
        // ef2.4.3: Apply per-source calibration before summing contributions.
        // Each entry's weight is transformed through the source-specific calibrator
        // from the calibration profile. v0 (identity) produces identical results
        // to the uncalibrated path; v1 reshapes per-source contributions.
        let calibratedSum = ledger.reduce(0.0) { sum, entry in
            let sourceCalibrator = calibrationProfile.calibrator(for: entry.source)
            return sum + sourceCalibrator.calibrate(entry.weight)
        }
        let rawProposalConfidence = min(1.0, calibratedSum)
        let proposalConfidence = rawProposalConfidence

        // Phase 7.2: apply combined correction factor to skipConfidence.
        // The factor is pre-computed by the caller (AdDetectionService, an actor)
        // from PersistentUserCorrectionStore.
        // < 1.0 = false-positive suppression; if effective confidence drops below 0.40,
        //         gate the span as blockedByUserCorrection.
        // > 1.0 = false-negative boost; caps at 1.0 to avoid over-confident decisions.
        // TODO: ef2.1.7 — Apply calibrationProfile.decisionThresholds here.
        // v0 thresholds are zero (no-op). When a real v1 profile ships with non-zero
        // skipMinimum/proposalMinimum, gate the score before correction factor.
        let rawSkipConfidence = calibrate(proposalConfidence)
        let effectiveConfidence = min(1.0, rawSkipConfidence * max(0.0, correctionFactor))
        let gate: SkipEligibilityGate
        if correctionFactor < 1.0 && effectiveConfidence < 0.40 {
            gate = .blockedByUserCorrection
        } else {
            gate = computeGate()
        }
        let skipConfidence = effectiveConfidence

        return DecisionResult(
            proposalConfidence: proposalConfidence,
            skipConfidence: skipConfidence,
            eligibilityGate: gate
        )
    }

    // MARK: - Private

    /// Calibration from proposalConfidence to skipConfidence via the active profile.
    /// v0 profile: identity mapping (direct pass-through). Future profiles apply
    /// piecewise-linear calibration learned from shadow-mode data.
    ///
    /// Non-finite guard is retained as a safety belt: NaN/Inf inputs always return 0.0
    /// regardless of the profile's calibrator (a computationally overflowed ledger is a
    /// data integrity error — err on the conservative side).
    private func calibrate(_ raw: Double) -> Double {
        guard raw.isFinite else { return 0.0 }
        // The profile's calibrator handles [0,1] clamping internally.
        // We use .fusedScore (not .classifier) because proposalConfidence is a post-fusion
        // aggregate, not a raw per-source classifier score. Using a distinct key prevents
        // future non-identity profiles from conflating the two distributions.
        let clamped = max(0.0, min(1.0, raw))
        return calibrationProfile.calibrator(for: .fusedScore).calibrate(clamped)
    }

    /// Determine the eligibility gate by reading `anchorProvenance` directly.
    /// Score is computed independently — gate NEVER modifies it.
    private func computeGate() -> SkipEligibilityGate {
        let provenance = span.anchorProvenance

        // Classify FM provenance type from anchorProvenance
        let hasFMConsensus = provenance.contains {
            if case .fmConsensus = $0 { return true }
            return false
        }
        let hasFMAcoustic = provenance.contains {
            if case .fmAcousticCorroborated = $0 { return true }
            return false
        }

        if hasFMConsensus {
            return quorumGateForFMConsensus()
        } else if hasFMAcoustic {
            return quorumGateForFMAcoustic()
        } else {
            // No FM provenance — quorum not applicable
            return .eligible
        }
    }

    /// Quorum check for spans anchored by fmConsensus.
    /// Multi-window consensus is already satisfied; checks remaining requirements.
    private func quorumGateForFMConsensus() -> SkipEligibilityGate {
        // Check span duration [5s, 180s]
        let duration = span.duration
        guard duration >= 5.0 && duration <= 180.0 else {
            return .blockedByEvidenceQuorum
        }

        // Check transcript quality
        guard transcriptQuality == .good else {
            return .blockedByEvidenceQuorum
        }

        // Check 2+ distinct evidence kinds in ledger
        let distinctKinds = Set(ledger.map { $0.source })
        guard distinctKinds.count >= 2 else {
            return .blockedByEvidenceQuorum
        }

        return .eligible
    }

    /// Quorum check for spans anchored by fmAcousticCorroborated only.
    /// Needs external corroboration from any non-FM source: classifier, lexical, catalog, or acoustic.
    /// Classifier is included because it is an independent, non-FM signal that provides corroboration.
    private func quorumGateForFMAcoustic() -> SkipEligibilityGate {
        let externalSources: Set<EvidenceSourceType> = [.classifier, .lexical, .catalog, .acoustic, .fingerprint]
        let hasExternalCorroboration = ledger.contains { externalSources.contains($0.source) }
        return hasExternalCorroboration ? .eligible : .blockedByEvidenceQuorum
    }
}
