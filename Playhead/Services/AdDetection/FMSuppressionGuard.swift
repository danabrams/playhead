// FMSuppressionGuard.swift
// Phase ef2.4.6: Targeted FM suppression of weak evidence when FM strongly says noAds.
//
// Design:
//   When FM strongly says noAds (with strict guards), weak evidence is downweighted
//   rather than globally subtracted. Strong positive anchors (URL, promo code,
//   disclosure, fingerprint) are NEVER suppressed — monotonicity is preserved.
//
//   Suppression only activates when ALL five strict guards pass:
//     1. FM disposition is .noAds
//     2. CertaintyBand is at least .moderate
//     3. No strong anchors present (no URL, promo code, disclosure in lexical/catalog)
//     4. No fingerprint match in ledger
//     5. 2+ overlapping noAds FM windows (consensus)

import Foundation
import OSLog

// MARK: - FMSuppressionGuard

/// Evaluates whether targeted FM suppression criteria are met for a span.
///
/// All five strict guards must pass for suppression to activate. This is a pure
/// value type with no side effects — the caller is responsible for applying the
/// suppression result to the ledger.
struct FMSuppressionGuard: Sendable {

    /// FM scan results overlapping the span (all dispositions, not just containsAd).
    let overlappingFMResults: [FMSuppressionWindow]

    /// The built evidence ledger (post-fusion, pre-decision).
    let ledger: [EvidenceLedgerEntry]

    /// Anchor provenance from the span.
    let anchorProvenance: [AnchorRef]

    /// Evaluate all five strict guards.
    func evaluate() -> FMSuppressionGuardResult {
        // Guard 1: at least one FM disposition is .noAds
        let noAdsWindows = overlappingFMResults.filter { $0.disposition == .noAds }
        guard !noAdsWindows.isEmpty else {
            return .notTriggered(reason: "no noAds FM disposition")
        }

        // Guard 2: CertaintyBand is at least .moderate on at least one noAds window
        let moderateOrStrongNoAds = noAdsWindows.filter { $0.band.isAtLeastModerate }
        guard !moderateOrStrongNoAds.isEmpty else {
            return .notTriggered(reason: "noAds FM certainty below moderate")
        }

        // Guard 3: no strong anchors present in lexical/catalog entries
        guard !hasStrongAnchors else {
            return .notTriggered(reason: "strong anchors present (URL/promoCode/disclosure)")
        }

        // Guard 4: no fingerprint match in ledger
        guard !hasFingerprintMatch else {
            return .notTriggered(reason: "fingerprint match present")
        }

        // Guard 5: 2+ overlapping noAds windows (consensus)
        guard moderateOrStrongNoAds.count >= 2 else {
            return .notTriggered(reason: "fewer than 2 noAds FM windows with moderate+ certainty")
        }

        return .triggered
    }

    // MARK: - Private

    /// Strong anchors are urlCTA, promoCode, or sponsor categories in lexical entries,
    /// or any positive catalog entry. These represent high-trust positive evidence
    /// that must never be suppressed.
    private var hasStrongAnchors: Bool {
        let strongLexicalCategories: Set<String> = [
            LexicalPatternCategory.urlCTA.rawValue,
            LexicalPatternCategory.promoCode.rawValue,
            LexicalPatternCategory.sponsor.rawValue,
        ]
        for entry in ledger {
            switch entry.detail {
            case .lexical(let matchedCategories):
                if matchedCategories.contains(where: { strongLexicalCategories.contains($0) }) {
                    return true
                }
            case .catalog(let entryCount):
                // Catalog entries represent evidence-catalog hits (URL, promoCode, etc.)
                // Their presence in the ledger means strong anchors were found.
                if entryCount > 0 {
                    return true
                }
            default:
                break
            }
        }

        // Also check anchor provenance for evidenceCatalog refs
        for ref in anchorProvenance {
            if case .evidenceCatalog = ref {
                return true
            }
        }

        return false
    }

    /// Check for fingerprint evidence in the ledger.
    private var hasFingerprintMatch: Bool {
        ledger.contains { $0.source == .fingerprint && $0.weight > 0 }
    }
}

// MARK: - FMSuppressionWindow

/// Minimal representation of an FM scan window for suppression guard evaluation.
/// Decoupled from SemanticScanResult to keep the guard testable as a pure value type.
struct FMSuppressionWindow: Sendable {
    let disposition: CoarseDisposition
    let band: CertaintyBand
}

// MARK: - CertaintyBand extension

extension CertaintyBand {
    /// Whether this band is at least moderate (.moderate or .strong).
    var isAtLeastModerate: Bool {
        switch self {
        case .moderate, .strong: return true
        case .weak: return false
        }
    }
}

// MARK: - FMSuppressionGuardResult

/// Whether suppression guards passed or not, with a reason string for logging.
enum FMSuppressionGuardResult: Sendable {
    case triggered
    case notTriggered(reason: String)

    var isTriggered: Bool {
        if case .triggered = self { return true }
        return false
    }
}

// MARK: - FMSuppressionResult

/// Details of what was suppressed and why, for replay attribution and logging.
struct FMSuppressionResult: Sendable {
    /// Whether suppression was applied.
    let applied: Bool
    /// Human-readable reason for suppression (or why it was not applied).
    let reason: String
    /// Number of weak evidence entries that were downweighted.
    let downweightedCount: Int
    /// Whether the eligibility was capped at markOnly (no strong proposal survived).
    let cappedToMarkOnly: Bool
    /// The modified ledger after suppression (same as input if not applied).
    let suppressedLedger: [EvidenceLedgerEntry]
}

// MARK: - FMSuppressionApplicator

/// Applies targeted suppression to weak evidence entries in the ledger.
///
/// Weak evidence sources (lexical drift, classifier/slot priors, catalog when not
/// anchored by strong signals) are downweighted by `suppressionFactor`. Strong
/// positive evidence (FM containsAd, fingerprint) is preserved unconditionally.
struct FMSuppressionApplicator: Sendable {
    private static let logger = Logger(subsystem: "com.playhead", category: "FMSuppression")

    /// Weight multiplier for weak evidence under suppression. Default 0.3 = 70% reduction.
    let suppressionFactor: Double

    init(suppressionFactor: Double = 0.3) {
        self.suppressionFactor = suppressionFactor
    }

    /// Apply suppression to the ledger, returning a result with the modified ledger
    /// and attribution details.
    func apply(
        guardResult: FMSuppressionGuardResult,
        ledger: [EvidenceLedgerEntry]
    ) -> FMSuppressionResult {
        guard guardResult.isTriggered else {
            let reason: String
            if case .notTriggered(let r) = guardResult {
                reason = r
            } else {
                reason = "unknown"
            }
            return FMSuppressionResult(
                applied: false,
                reason: "Suppression not triggered: \(reason)",
                downweightedCount: 0,
                cappedToMarkOnly: false,
                suppressedLedger: ledger
            )
        }

        var suppressedLedger: [EvidenceLedgerEntry] = []
        var downweightedCount = 0
        var hasStrongProposal = false

        for entry in ledger {
            if isStrongEvidence(entry) {
                // Strong evidence is NEVER suppressed.
                suppressedLedger.append(entry)
                if entry.weight > 0 {
                    hasStrongProposal = true
                }
            } else {
                // Weak evidence: downweight by suppressionFactor.
                let suppressedWeight = entry.weight * suppressionFactor
                let suppressed = EvidenceLedgerEntry(
                    source: entry.source,
                    weight: suppressedWeight,
                    detail: entry.detail,
                    classificationTrust: entry.classificationTrust
                )
                suppressedLedger.append(suppressed)
                downweightedCount += 1
            }
        }

        let cappedToMarkOnly = !hasStrongProposal

        Self.logger.info(
            "FM suppression applied: downweighted=\(downweightedCount) cappedToMarkOnly=\(cappedToMarkOnly) factor=\(self.suppressionFactor)"
        )

        return FMSuppressionResult(
            applied: true,
            reason: "FM noAds consensus suppression (factor=\(suppressionFactor))",
            downweightedCount: downweightedCount,
            cappedToMarkOnly: cappedToMarkOnly,
            suppressedLedger: suppressedLedger
        )
    }

    // MARK: - Private

    /// Strong evidence is preserved unconditionally during suppression.
    /// FM containsAd entries and fingerprint matches are strong.
    private func isStrongEvidence(_ entry: EvidenceLedgerEntry) -> Bool {
        switch entry.source {
        case .fm:
            // FM containsAd entries are strong positive evidence.
            if case .fm(let disposition, _, _) = entry.detail {
                return disposition == .containsAd
            }
            return false
        case .fingerprint:
            return true
        case .classifier, .lexical, .acoustic, .catalog, .fusedScore, .metadata:
            // playhead-z3ch: metadata is a coarse pre-seed prior, not strong
            // evidence. It must yield to FM noAds suppression like the other
            // soft signals.
            return false
        }
    }
}
