// NarlGroundTruth.swift
// playhead-narl.1: Ground-truth construction for the counterfactual eval harness.
//
// Pure helper. Input: a FrozenTrace plus its corrections (already carried inside
// the trace). Output: a set of positive ad-window [start, end] intervals to
// score a pipeline's predictions against, OR a signal that the episode should
// be excluded entirely from the eval run.
//
// Rules (see docs/plans/2026-04-21-narl-eval-harness-design.md §A.4):
//   1. Start with auto-detected spans from baseline decisions in the FrozenTrace.
//   2. Subtract any span overlapped by a `falsePositive` correction (exactTimeSpan scope).
//   3. Add every `falseNegative` correction (exactTimeSpan scope).
//   4. For ordinal-range `exactSpan` corrections that are not whole-asset vetoes:
//      resolve atom ordinals to time via the FrozenTrace atoms, then treat as window-level.
//   5. Exclude the episode entirely if it carries an
//      `exactSpan:<assetId>:0:INT64_MAX` veto — user flagged that episode's data
//      as unreliable.
//
// The helper lives in the test target because its input type (FrozenTrace) is
// defined in the test target. Placing it under PlayheadTests/.../NarlEval keeps
// the narl-specific surface discoverable without polluting the prod module.

import Foundation
@testable import Playhead

// MARK: - Correction model (parsed from FrozenCorrection.scope)

/// A correction scope parsed from the string encoded in
/// `FrozenTrace.FrozenCorrection.scope` via the production `CorrectionScope`
/// serializer. We re-parse the raw string here because FrozenCorrection carries
/// scope as a string for schema stability.
enum NarlCorrectionScope: Sendable, Equatable {
    case exactTimeSpan(assetId: String, startTime: Double, endTime: Double)
    case exactSpan(assetId: String, lowerOrdinal: Int, upperOrdinal: Int)
    case wholeAssetVeto(assetId: String)
    case unhandled(raw: String)

    /// Parse a scope string. Returns `.unhandled(raw:)` for forms we don't
    /// need to evaluate (show-level scopes, jingle vetoes, etc.) — the caller
    /// ignores those when building ground truth.
    static func parse(_ raw: String) -> NarlCorrectionScope {
        // exactSpan:<assetId>:<lo>:<hi>
        // Whole-asset veto pattern: exactSpan:<assetId>:0:<INT64_MAX> OR exactSpan:<assetId>:0:<Int.max>
        if raw.hasPrefix("exactSpan:") {
            let body = String(raw.dropFirst("exactSpan:".count))
            // Asset IDs are SHA-derived fingerprints; shouldn't contain colons,
            // but split from the right so a colon in an assetId wouldn't break us.
            let parts = body.split(separator: ":", omittingEmptySubsequences: false).map(String.init)
            guard parts.count >= 3 else { return .unhandled(raw: raw) }
            let hiStr = parts[parts.count - 1]
            let loStr = parts[parts.count - 2]
            let assetId = parts[0..<(parts.count - 2)].joined(separator: ":")
            guard let lo = Int(loStr), let hi = Int64(hiStr) else {
                return .unhandled(raw: raw)
            }
            // Whole-asset veto: lower bound is 0 AND upper is INT64_MAX or Int.max.
            // Historical code path stored the ClosedRange's upperBound, which
            // was Int.max on 64-bit platforms (9223372036854775807) —
            // identical to INT64_MAX.
            if lo == 0 && hi == Int64.max {
                return .wholeAssetVeto(assetId: assetId)
            }
            // Clamp hi to Int range for the non-veto case. If hi exceeds Int.max
            // (shouldn't happen for a non-whole-asset veto) we treat it as a
            // whole-asset veto conservatively — "touch nothing" is safer than
            // claiming a partial range.
            guard hi <= Int64(Int.max) else {
                return .wholeAssetVeto(assetId: assetId)
            }
            return .exactSpan(assetId: assetId, lowerOrdinal: lo, upperOrdinal: Int(hi))
        }
        // exactTimeSpan:<assetId>:<start>:<end> (start/end formatted %.3f)
        if raw.hasPrefix("exactTimeSpan:") {
            let body = String(raw.dropFirst("exactTimeSpan:".count))
            let parts = body.split(separator: ":", omittingEmptySubsequences: false).map(String.init)
            guard parts.count >= 3 else { return .unhandled(raw: raw) }
            let endStr = parts[parts.count - 1]
            let startStr = parts[parts.count - 2]
            let assetId = parts[0..<(parts.count - 2)].joined(separator: ":")
            guard let start = Double(startStr), let end = Double(endStr), end >= start else {
                return .unhandled(raw: raw)
            }
            return .exactTimeSpan(assetId: assetId, startTime: start, endTime: end)
        }
        return .unhandled(raw: raw)
    }
}

// MARK: - Correction semantic classification

/// Maps `FrozenCorrection` payloads to a narl-semantic category.
///
/// Preferred source of truth (v2): the `correctionType` field on
/// `FrozenCorrection`, which is the raw value of the production
/// `CorrectionType` enum (`falsePositive`, `falseNegative`,
/// `startTooEarly/Late`, `endTooEarly/Late`). We map these directly.
///
/// Fallback (v1 fixtures or rows missing `correctionType`): explicit mapping
/// of `CorrectionSource` raw values (`listenRevert`, `manualVeto`,
/// `falseNegative`). Unknown sources are `.unknown` and ignored — and
/// crucially, `manualVeto` (a documented production source that was
/// previously dropped by the substring heuristic) is mapped explicitly.
///
/// Boundary cases (`startTooEarly/Late`, `endTooEarly/Late`) do not add or
/// remove entire positive windows; they describe sub-second boundary drift
/// in an already-detected ad. Window-level IoU can't score the difference
/// between a clean 0.5 s boundary shift and a perfect match, so we
/// deliberately treat boundary corrections as `.unknown` (i.e. ignored) for
/// ground-truth construction and surface them in the `skippedCorrectionCount`
/// counter. See narl design §A.4 rule 4.
enum NarlCorrectionKind: Sendable, Equatable {
    case falsePositive  // user flagged a detected span as not an ad
    case falseNegative  // user flagged a missed span as an ad
    /// Boundary refinement — intentionally ignored for ground-truth
    /// construction (see note above). Tallied separately for diagnostics.
    case boundaryRefinement
    case unknown
}

/// Resolve a kind from a frozen correction, preferring the explicit
/// `correctionType` payload when available.
///
/// Explicit `CorrectionType` mapping (matches
/// `Playhead/Services/AdDetection/CorrectionAttribution.swift`):
///   - `falsePositive` → `.falsePositive`
///   - `falseNegative` → `.falseNegative`
///   - `startTooEarly/Late`, `endTooEarly/Late` → `.boundaryRefinement`
///
/// Fallback `CorrectionSource` mapping (matches
/// `Playhead/Services/AdDetection/AdDecisionResult.swift`):
///   - `listenRevert`, `manualVeto` → `.falsePositive`
///   - `falseNegative` → `.falseNegative`
func narlCorrectionKind(
    fromType correctionType: String?,
    source: String
) -> NarlCorrectionKind {
    // Prefer explicit correctionType when present (v2 fixtures).
    if let t = correctionType {
        switch t {
        case "falsePositive": return .falsePositive
        case "falseNegative": return .falseNegative
        case "startTooEarly", "startTooLate",
             "endTooEarly", "endTooLate":
            return .boundaryRefinement
        default:
            // Unknown correctionType — fall through to source-based mapping.
            break
        }
    }

    // Explicit CorrectionSource mapping (raw values on the production enum).
    // Case-sensitive to match the enum's raw values; fixtures written by the
    // corpus builder preserve the raw value verbatim.
    switch source {
    case "listenRevert", "manualVeto":
        return .falsePositive
    case "falseNegative":
        return .falseNegative
    default:
        break
    }

    // Tolerant fallback for legacy raw strings that historically mixed
    // gesture names with intent names. This branch exists specifically to
    // keep older captures and test fixtures (e.g. "reportMissedAd",
    // "dismissBanner") decodable; new captures should rely on
    // `correctionType` instead.
    let lowered = source.lowercased()
    if lowered.contains("listen") || lowered.contains("dismiss")
        || lowered.contains("falsepositive") || lowered.contains("notanad")
        || lowered.contains("manualveto") {
        return .falsePositive
    }
    if lowered.contains("flag") || lowered.contains("falsenegative")
        || lowered.contains("missedad") || lowered.contains("reportad") {
        return .falseNegative
    }
    return .unknown
}

/// Back-compat shim for existing tests that pass a raw source string.
func narlCorrectionKind(fromSource raw: String) -> NarlCorrectionKind {
    narlCorrectionKind(fromType: nil, source: raw)
}

// MARK: - Time range

/// A closed-open [start, end) time interval in episode-seconds.
struct NarlTimeRange: Sendable, Equatable, Hashable {
    let start: Double
    let end: Double

    init(start: Double, end: Double) {
        // Normalize to start<=end; callers trust us.
        self.start = min(start, end)
        self.end = max(start, end)
    }

    var duration: Double { end - start }

    func overlaps(_ other: NarlTimeRange) -> Bool {
        start < other.end && other.start < end
    }

    /// Intersection of two ranges, or nil if disjoint.
    func intersection(_ other: NarlTimeRange) -> NarlTimeRange? {
        let s = max(start, other.start)
        let e = min(end, other.end)
        return s < e ? NarlTimeRange(start: s, end: e) : nil
    }
}

// MARK: - Ground truth result

/// Ground-truth output for one episode.
struct NarlEpisodeGroundTruth: Sendable, Equatable {
    /// True when the episode should be dropped from metrics entirely
    /// (whole-asset veto detected).
    let isExcluded: Bool
    /// The reason for exclusion, when applicable.
    let exclusionReason: String?
    /// Positive ad-window ranges in episode-seconds. Empty when excluded, OR
    /// when the episode legitimately has no ads.
    let adWindows: [NarlTimeRange]
    /// Number of falsePositive corrections that trimmed the positive set.
    let falsePositiveCorrectionCount: Int
    /// Number of falseNegative corrections that added to the positive set.
    let falseNegativeCorrectionCount: Int
    /// Number of ordinal-range corrections resolved to time via atoms
    /// (disjoint from fp/fn counts — `ordinalCorrectionCount` is a *subset*
    /// counter that tallies how many of the above fp/fn corrections arrived
    /// via `exactSpan` ordinal scopes rather than time-based scopes).
    let ordinalCorrectionCount: Int
    /// Number of boundary-refinement corrections (startTooEarly/Late,
    /// endTooEarly/Late). These do NOT move the ad-window set — window-
    /// level IoU can't distinguish sub-second boundary drift cleanly — but
    /// we count them so consumers can spot episodes where boundary quality
    /// is the dominant failure mode.
    let boundaryRefinementCount: Int
    /// Number of unhandled / malformed corrections silently skipped.
    let skippedCorrectionCount: Int

    static let excluded = NarlEpisodeGroundTruth(
        isExcluded: true,
        exclusionReason: nil,
        adWindows: [],
        falsePositiveCorrectionCount: 0,
        falseNegativeCorrectionCount: 0,
        ordinalCorrectionCount: 0,
        boundaryRefinementCount: 0,
        skippedCorrectionCount: 0
    )
}

// MARK: - Builder

/// Pure helper that constructs the ground-truth positive set from a FrozenTrace.
///
/// This is the single source of truth for §A.4 of the narl design. All harness
/// runners call into this — unit tests exercise each rule in isolation against
/// hand-built traces.
enum NarlGroundTruth {

    /// Build ground truth for one trace per §A.4.
    static func build(for trace: FrozenTrace) -> NarlEpisodeGroundTruth {
        // Step 0: pre-scan for whole-asset veto. Any such veto drops the episode.
        for correction in trace.corrections {
            switch NarlCorrectionScope.parse(correction.scope) {
            case .wholeAssetVeto(let assetId):
                return NarlEpisodeGroundTruth(
                    isExcluded: true,
                    exclusionReason: "wholeAssetVeto:\(assetId)",
                    adWindows: [],
                    falsePositiveCorrectionCount: 0,
                    falseNegativeCorrectionCount: 0,
                    ordinalCorrectionCount: 0,
                    boundaryRefinementCount: 0,
                    skippedCorrectionCount: 0
                )
            default: break
            }
        }

        // Step 1: start with baseline decision spans where isAd=true.
        // A FrozenTrace baseline may contain non-ad spans (classification=false);
        // those are not ground-truth positives.
        var positives: [NarlTimeRange] = trace.baselineReplaySpanDecisions
            .filter { $0.isAd }
            .map { NarlTimeRange(start: $0.startTime, end: $0.endTime) }

        var fpCount = 0
        var fnCount = 0
        var ordinalCount = 0
        var boundaryCount = 0
        var skippedCount = 0

        // Atom ordinal → time lookup, for step 4. Atoms are ordered and
        // contiguous in narL exports; we use array position as ordinal.
        let atomsOrderedByStart = trace.atoms.sorted { $0.startTime < $1.startTime }

        for correction in trace.corrections {
            let kind = narlCorrectionKind(
                fromType: correction.correctionType,
                source: correction.source
            )
            switch NarlCorrectionScope.parse(correction.scope) {
            case .exactTimeSpan(_, let startTime, let endTime):
                let range = NarlTimeRange(start: startTime, end: endTime)
                switch kind {
                case .falsePositive:
                    positives = subtract(range: range, from: positives)
                    fpCount += 1
                case .falseNegative:
                    positives = unionAdd(range: range, into: positives)
                    fnCount += 1
                case .boundaryRefinement:
                    // Boundary-refinement corrections describe sub-second
                    // drift on already-detected ads; do not alter the set.
                    boundaryCount += 1
                case .unknown:
                    skippedCount += 1
                }
            case .exactSpan(_, let lo, let hi):
                // Resolve ordinals to time via atoms.
                guard lo >= 0, hi >= lo,
                      lo < atomsOrderedByStart.count,
                      !atomsOrderedByStart.isEmpty else {
                    skippedCount += 1
                    continue
                }
                // Clamp upper bound to array size; narL's corrections sometimes
                // use open-ended upper bounds that exceed current atom count.
                let hiClamped = min(hi, atomsOrderedByStart.count - 1)
                let startTime = atomsOrderedByStart[lo].startTime
                let endTime = atomsOrderedByStart[hiClamped].endTime
                guard endTime > startTime else {
                    skippedCount += 1
                    continue
                }
                let range = NarlTimeRange(start: startTime, end: endTime)
                switch kind {
                case .falsePositive:
                    positives = subtract(range: range, from: positives)
                    fpCount += 1
                    ordinalCount += 1
                case .falseNegative:
                    positives = unionAdd(range: range, into: positives)
                    fnCount += 1
                    ordinalCount += 1
                case .boundaryRefinement:
                    boundaryCount += 1
                case .unknown:
                    skippedCount += 1
                }
            case .wholeAssetVeto:
                // Already handled in step 0; unreachable here.
                break
            case .unhandled:
                skippedCount += 1
            }
        }

        // Normalize: merge overlapping, sort by start.
        positives = mergeOverlaps(positives)

        return NarlEpisodeGroundTruth(
            isExcluded: false,
            exclusionReason: nil,
            adWindows: positives,
            falsePositiveCorrectionCount: fpCount,
            falseNegativeCorrectionCount: fnCount,
            ordinalCorrectionCount: ordinalCount,
            boundaryRefinementCount: boundaryCount,
            skippedCorrectionCount: skippedCount
        )
    }

    // MARK: - Range algebra

    /// Remove `range` from the positive set. Ranges partially overlapping
    /// `range` are clipped; fully covered ranges are removed.
    static func subtract(range: NarlTimeRange, from positives: [NarlTimeRange]) -> [NarlTimeRange] {
        var out: [NarlTimeRange] = []
        for p in positives {
            guard p.overlaps(range) else {
                out.append(p)
                continue
            }
            // Left remnant
            if p.start < range.start {
                out.append(NarlTimeRange(start: p.start, end: min(p.end, range.start)))
            }
            // Right remnant
            if p.end > range.end {
                out.append(NarlTimeRange(start: max(p.start, range.end), end: p.end))
            }
            // Fully contained → contributes nothing.
        }
        return out
    }

    /// Add `range` to the positive set with merge-on-overlap.
    static func unionAdd(range: NarlTimeRange, into positives: [NarlTimeRange]) -> [NarlTimeRange] {
        mergeOverlaps(positives + [range])
    }

    /// Merge overlapping or touching ranges.
    ///
    /// Uses `r.start <= last.end` (inclusive), so ranges that touch at a single
    /// point — e.g. `[100, 120)` and `[120, 140)` — are merged into
    /// `[100, 140)`. This is the correct behavior for narl's ad-window set:
    /// adjacent false-negative corrections describing the same contiguous
    /// ad should collapse to one positive window rather than two zero-gap
    /// windows (which would double-count under window-level F1). If you
    /// ever need strict `<` semantics (e.g. "segmentation preserves
    /// touching-but-distinct labels"), add a separate helper — don't change
    /// this one, because the harness' metrics are calibrated on the merging
    /// behavior.
    static func mergeOverlaps(_ ranges: [NarlTimeRange]) -> [NarlTimeRange] {
        guard !ranges.isEmpty else { return [] }
        let sorted = ranges.sorted { $0.start < $1.start }
        var out: [NarlTimeRange] = [sorted[0]]
        for r in sorted.dropFirst() {
            let last = out[out.count - 1]
            if r.start <= last.end {
                out[out.count - 1] = NarlTimeRange(start: last.start, end: max(last.end, r.end))
            } else {
                out.append(r)
            }
        }
        return out
    }
}
