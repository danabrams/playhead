// CorrectionNormalizer.swift
// playhead-gtt9.7: normalize raw FrozenCorrection rows before the NARL
// harness consumes them for span-level precision/recall.
//
// Why this exists: the correction stream mixes whole-asset vetoes with
// span-level FPs/FNs. On 2026-04-23 real data, 9 of 10 "falsePositive"
// corrections were whole-asset manualVetoes, not per-span detection errors.
// Feeding those straight into span-level metrics inflates FP counts and
// makes the numbers that drive priorShift retune (gtt9.3) untrustworthy.
//
// Contract (pure function, harness-side):
//   Input:  [FrozenTrace.FrozenCorrection] — one call per episode/trace.
//   Output: NormalizedCorrections — three streams (wholeAsset, spanFN,
//           spanFP), an unknown count, and a boundary-refinement count.
//
// Classification heuristic (§11 of the 2026-04-23 expert report; see the
// playhead-gtt9.7 bead description for excerpts):
//   1. scope parses as `.wholeAssetVeto` → wholeAsset bucket. kind:
//        • correctionType == "falsePositive" → .veto
//        • correctionType == "falseNegative" → .endorse
//        • null/other → fall back to source: manualVeto/listenRevert → .veto,
//          else .endorse (fallback is defensive; real captures always carry
//          correctionType on v2 rows).
//   2. scope is `.exactTimeSpan` with 0 ≤ start < end:
//        • correctionType == "falseNegative" → spanFN
//        • correctionType == "falsePositive" → spanFP
//        • correctionType ∈ startToo…/endToo… → boundaryRefinementCount++
//        • null or unknown correctionType → source heuristic
//          (manualVeto/listenRevert → spanFP; falseNegative → spanFN;
//          else unknown)
//   3. scope is `.exactSpan` (ordinal range, not whole-asset) → unknown.
//      The normalizer runs before atoms are resolved; the harness's
//      existing NarlGroundTruth path still handles these downstream when
//      atom ordinals are available.
//   4. scope is `.unhandled` (sponsor-on-show, malformed, etc.) → unknown.
//
// Merging: after classification, adjacent spans on the same asset with the
// same kind merge when their gap is ≤ 5 s (design value from the bead).
//
// Deduplication: the merge pass already collapses exact and near-duplicate
// spans because near-duplicates trivially satisfy the 5 s adjacency rule.
// Whole-asset corrections dedupe per (assetId, kind).

import Foundation
@testable import Playhead

// MARK: - Output types

/// A whole-asset-level correction — user toggled the entire episode at once.
/// These never belong in span-level precision/recall; the harness reports
/// them as a separate per-show counter.
struct NormalizedWholeAssetCorrection: Sendable, Equatable {
    enum Kind: Sendable, Equatable {
        /// User said the whole episode is NOT ad-content (vetoed ad-skip).
        /// This is the dominant real-data pattern — see the 2026-04-23 expert
        /// report §3 (referenced in the playhead-gtt9.7 bead description).
        case veto
        /// User said the whole episode IS ad-content. Rare but valid.
        case endorse
    }
    let assetId: String
    let kind: Kind
}

/// A span-level correction — user marked a specific time range as FN or FP.
struct NormalizedSpanCorrection: Sendable, Equatable {
    enum Kind: Sendable, Equatable {
        case falseNegative
        case falsePositive
    }
    let assetId: String
    let kind: Kind
    let range: NarlTimeRange
}

/// Bundle of classification outputs the harness consumes.
struct NormalizedCorrections: Sendable, Equatable {
    let wholeAssetCorrections: [NormalizedWholeAssetCorrection]
    let spanFN: [NormalizedSpanCorrection]
    let spanFP: [NormalizedSpanCorrection]
    /// Rows we could not confidently place in any bucket (ordinal spans
    /// without atom resolution, unrecognized source+type combinations,
    /// malformed scopes, or truly unhandled scope prefixes). Excluded from
    /// span-level precision/recall. Distinct from `layerBCount` — see
    /// `CorrectionNormalizer.classify(_:)` for the split.
    let unknownCount: Int
    /// Boundary-refinement rows (`startTooEarly/Late`, `endTooEarly/Late`).
    /// Deliberately separate from `unknown` because they're recognized but
    /// describe sub-second drift on already-detected spans — they should
    /// not move span-level counts.
    let boundaryRefinementCount: Int
    /// Layer B scopes (`sponsorOnShow`, `phraseOnShow`, `campaignOnShow`,
    /// `domainOwnershipOnShow`, `jingleOnShow`). These are production-valid
    /// show-level corrections the user issued against a whole podcast, not
    /// malformed data — but the harness's per-episode span-level metrics
    /// have no receiver for them. They're counted here so an operator
    /// reading the report can distinguish "5 corrections we don't yet
    /// evaluate against" from "5 corrections we failed to parse".
    let layerBCount: Int

    static let empty = NormalizedCorrections(
        wholeAssetCorrections: [],
        spanFN: [],
        spanFP: [],
        unknownCount: 0,
        boundaryRefinementCount: 0,
        layerBCount: 0
    )

    /// Total corrections actually placed in a span bucket (for logging
    /// before/after normalization deltas).
    var spanCorrectionCount: Int { spanFN.count + spanFP.count }
}

// MARK: - Normalizer

enum CorrectionNormalizer {

    /// Gap tolerance for merging adjacent spans of the same kind on the
    /// same asset. Per-bead value: 5 seconds.
    static let mergeGapSeconds: Double = 5.0

    /// Edge-tolerance for near-duplicate detection on whole-asset rows and
    /// the pre-merge dedup step. The merge pass subsumes per-edge tolerance
    /// ≤ `mergeGapSeconds` anyway, but we keep the ±1 s constant documented
    /// to match the bead spec ("overlapping time ~±1s").
    static let nearDuplicateEdgeToleranceSeconds: Double = 1.0

    /// Normalize a stream of raw corrections.
    static func normalize(
        _ corrections: [FrozenTrace.FrozenCorrection]
    ) -> NormalizedCorrections {
        var wholeAssets: [NormalizedWholeAssetCorrection] = []
        var rawFN: [(assetId: String, range: NarlTimeRange)] = []
        var rawFP: [(assetId: String, range: NarlTimeRange)] = []
        var unknown = 0
        var boundary = 0
        var layerB = 0

        for correction in corrections {
            switch classify(correction) {
            case .wholeAsset(let assetId, let kind):
                wholeAssets.append(NormalizedWholeAssetCorrection(
                    assetId: assetId, kind: kind
                ))
            case .spanFN(let assetId, let range):
                rawFN.append((assetId, range))
            case .spanFP(let assetId, let range):
                rawFP.append((assetId, range))
            case .boundary:
                boundary += 1
            case .layerB:
                layerB += 1
            case .unknown:
                unknown += 1
            }
        }

        // Dedup whole-asset by (assetId, kind).
        let dedupedWholeAssets: [NormalizedWholeAssetCorrection] = {
            var seen = Set<String>()
            var out: [NormalizedWholeAssetCorrection] = []
            for c in wholeAssets {
                let key = "\(c.assetId)|\(c.kind == .veto ? "veto" : "endorse")"
                if seen.insert(key).inserted { out.append(c) }
            }
            return out
        }()

        // Merge (which also collapses exact/near-dup) per asset per kind.
        let mergedFN = mergePerAsset(rawFN).map {
            NormalizedSpanCorrection(assetId: $0.assetId, kind: .falseNegative, range: $0.range)
        }
        let mergedFP = mergePerAsset(rawFP).map {
            NormalizedSpanCorrection(assetId: $0.assetId, kind: .falsePositive, range: $0.range)
        }

        return NormalizedCorrections(
            wholeAssetCorrections: dedupedWholeAssets,
            spanFN: mergedFN,
            spanFP: mergedFP,
            unknownCount: unknown,
            boundaryRefinementCount: boundary,
            layerBCount: layerB
        )
    }

    // MARK: - Classification

    private enum ClassificationResult {
        case wholeAsset(assetId: String, kind: NormalizedWholeAssetCorrection.Kind)
        case spanFN(assetId: String, range: NarlTimeRange)
        case spanFP(assetId: String, range: NarlTimeRange)
        case boundary
        /// Layer B show-level scope (sponsorOnShow, phraseOnShow,
        /// campaignOnShow, domainOwnershipOnShow, jingleOnShow). Recognized
        /// production scopes, but deferred — the harness does not yet
        /// evaluate against show-level corrections.
        case layerB
        case unknown
    }

    /// Prefixes that identify Layer B (show-level) correction scopes. These
    /// are production-valid per `CorrectionScope.swift`; we recognize them
    /// in the normalizer so they aren't lumped in with malformed scopes in
    /// `unknownCount`. Kept as a static set so the classify path stays O(1).
    private static let layerBScopePrefixes: Set<String> = [
        "sponsorOnShow:",
        "phraseOnShow:",
        "campaignOnShow:",
        "domainOwnershipOnShow:",
        "jingleOnShow:",
    ]

    /// Classify a single correction using only the shape of its scope and
    /// its correctionType/source fields.
    private static func classify(
        _ correction: FrozenTrace.FrozenCorrection
    ) -> ClassificationResult {
        let scope = NarlCorrectionScope.parse(correction.scope)

        switch scope {
        case .wholeAssetVeto(let assetId):
            return .wholeAsset(
                assetId: assetId,
                kind: wholeAssetKind(
                    correctionType: correction.correctionType,
                    source: correction.source
                )
            )

        case .exactTimeSpan(let assetId, let start, let end):
            guard start >= 0, end > start else { return .unknown }
            let range = NarlTimeRange(start: start, end: end)
            // Prefer explicit correctionType (v2 rows).
            if let t = correction.correctionType {
                switch t {
                case "falseNegative": return .spanFN(assetId: assetId, range: range)
                case "falsePositive": return .spanFP(assetId: assetId, range: range)
                case "startTooEarly", "startTooLate", "endTooEarly", "endTooLate":
                    return .boundary
                default:
                    // Unknown explicit type — fall through to source heuristic.
                    break
                }
            }
            // Source heuristic for null/unrecognized correctionType.
            switch correction.source {
            case "falseNegative":
                return .spanFN(assetId: assetId, range: range)
            case "manualVeto", "listenRevert":
                return .spanFP(assetId: assetId, range: range)
            default:
                return .unknown
            }

        case .exactSpan:
            // Ordinal-range (not whole-asset). Harness-side we don't have
            // atoms available here; leave it for the downstream
            // NarlGroundTruth pipeline and tally as unknown for normalizer
            // reporting purposes.
            return .unknown

        case .unhandled(let raw):
            // Layer B scopes are production-valid show-level corrections
            // that `NarlCorrectionScope.parse` returns as `.unhandled`
            // (because the harness doesn't evaluate against them). Tease
            // them out of `unknown` so the report distinguishes them from
            // malformed rows.
            if Self.layerBScopePrefixes.contains(where: { raw.hasPrefix($0) }) {
                return .layerB
            }
            return .unknown
        }
    }

    private static func wholeAssetKind(
        correctionType: String?,
        source: String
    ) -> NormalizedWholeAssetCorrection.Kind {
        if let t = correctionType {
            switch t {
            case "falsePositive": return .veto
            case "falseNegative": return .endorse
            default: break
            }
        }
        // Fallback by source: manualVeto/listenRevert are almost always
        // veto in production captures (user tapped "not an ad"). The
        // falseNegative source indicates the user added the whole episode
        // as ad-content.
        switch source {
        case "manualVeto", "listenRevert":
            return .veto
        case "falseNegative":
            return .endorse
        default:
            return .veto  // conservative: treat unrecognized as veto
        }
    }

    // MARK: - Merge + dedup

    /// Merge adjacent spans (gap ≤ `mergeGapSeconds`) per asset. Near and
    /// exact duplicates trivially satisfy the gap rule and therefore
    /// collapse by construction.
    private static func mergePerAsset(
        _ raw: [(assetId: String, range: NarlTimeRange)]
    ) -> [(assetId: String, range: NarlTimeRange)] {
        guard !raw.isEmpty else { return [] }
        let byAsset = Dictionary(grouping: raw, by: { $0.assetId })
        var out: [(assetId: String, range: NarlTimeRange)] = []
        for assetId in byAsset.keys.sorted() {
            let spans = byAsset[assetId]!
                .map(\.range)
                .sorted { $0.start < $1.start }
            let merged = mergeAdjacent(spans, gap: mergeGapSeconds)
            for r in merged { out.append((assetId, r)) }
        }
        return out
    }

    /// Sort + merge: any two ranges whose gap (start_b - end_a) is ≤ `gap`
    /// collapse into one covering range. Overlapping ranges collapse too
    /// (gap would be negative). Input must be pre-sorted by start.
    static func mergeAdjacent(
        _ sorted: [NarlTimeRange],
        gap: Double
    ) -> [NarlTimeRange] {
        guard !sorted.isEmpty else { return [] }
        var out: [NarlTimeRange] = [sorted[0]]
        for r in sorted.dropFirst() {
            let last = out[out.count - 1]
            if r.start - last.end <= gap {
                out[out.count - 1] = NarlTimeRange(
                    start: last.start,
                    end: max(last.end, r.end)
                )
            } else {
                out.append(r)
            }
        }
        return out
    }
}
