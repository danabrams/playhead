// SpecialistMarkComposer.swift
// playhead-b6jq PR 5 (Phase B2): turn PR4's persisted raw
// `specialist_scan_results` into user-visible MARK-ONLY banner marks.
//
// # What this is
//
// PR 4 runs the on-device specialist over candidate windows during backfill and
// PERSISTS raw verdicts (`SpecialistScanResult`), acting on nothing. PR 5
// consumes those rows and composes them into `AdWindow` marks the user sees as
// suggest-tier banners. This type is the PURE, ALWAYS-COMPILED core of that
// step: no store, no actor, no CoreAI, no FM coupling — just `[SpecialistScanResult]`
// + the asset's existing `[AdWindow]` in, `[AdWindow]` out. That makes the
// τ / merge / dedupe / emit contract unit-testable on the simulator with
// synthetic rows and keeps the composer decoupled from FM mode (row PRODUCTION
// stays FM-coupled in the runner; composition does not — see PR5 blueprint §7).
//
// # The mark contract (blueprint §2)
//
// Every emitted mark is, unconditionally:
//   • `eligibilityGate == .markOnly` — a HARD-CODED literal, never derived from
//     any policy switch. Specialist marks route to the suggest-tier banner via
//     `SkipOrchestrator.receiveAdWindows`' markOnly branch and can NEVER reach
//     auto-skip. Auto-skip stays deterministic-only.
//   • `decisionState == .candidate` — never confirmed/applied.
//   • `detectorVersion == "specialist-ft-v2"` — pins reconcile isolation so the
//     FM reconcile (`detection-v1`) can't clobber specialist marks and vice
//     versa.
//   • `metadataConfidence == nil` — so `AdBannerView.bannerCopy` uses the generic
//     "Sounds like a sponsor break." copy (no advertiser hallucination).
//   • `startEdgeAnchor / endEdgeAnchor == .unanchored` — belt+suspenders: even if
//     a future auto-skip edge policy ran on these, unanchored auto-skips nothing.

import CryptoKit
import Foundation

/// Pure, always-compiled composer: `specialist_scan_results` → mark-only
/// `AdWindow`s. No I/O; the caller supplies the persisted scan rows and the
/// asset's existing windows and persists whatever this returns.
enum SpecialistMarkComposer {

    // MARK: - Provenance constants

    /// Detector version stamped on every specialist mark. The reconcile-isolation
    /// backbone (`AdDetectionService.isReconcilableBackfillWindow`) scopes to this
    /// exact string so specialist marks and FM (`detection-v1`) marks can never
    /// retire one another. Single source of truth; `AdDetectionService`
    /// re-exports it as `specialistDetectorVersion`.
    static let detectorVersion = "specialist-ft-v2"

    /// `metadataSource` stamped on every specialist mark. Drives the subtle
    /// suggest-banner glyph (`AdBannerView.showsSpecialistGlyph`) and, paired with
    /// `metadataConfidence == nil`, guarantees the generic no-hallucination copy.
    static let metadataSource = "specialist-v1"

    /// `boundaryState` stamped on every specialist mark. A NON-user literal that
    /// MUST stay OUT of `AdDetectionService.reconcileProtectedBoundaryStates`
    /// (pinned by an axis test) so the specialist reconcile can retire its own
    /// stale rows.
    static let boundaryState = "specialistScan"

    // MARK: - Tunables (bead playhead-b6jq PR5; corpus A/B levers)

    /// Stage-1 decision threshold: keep a scan row iff `probabilityOfAd >= tau`.
    /// τ is the PR5 decision (NOT the raw `isAd` P≥0.5 flag). Pinned at exactly
    /// 0.70 so every passing mark's `confidence` (= `probabilityOfAd`) auto-clears
    /// `SkipOrchestrator.preloadConfidenceThreshold` (also 0.70) on cross-launch.
    static let tau = 0.70

    /// Stage-2 merge gap: adjacent τ-passing windows within this many seconds of
    /// each other coalesce into one span. PR4 tiles fixed ~25 s windows that are
    /// often exactly adjacent, so a small positive gap stitches a single host-read
    /// back together instead of emitting a string of touching marks.
    static let mergeGap = 2.0

    /// Stage-3 dedupe threshold: drop a merged span whose duration is `>=` this
    /// fraction covered by existing VISIBLE non-specialist marks (no double-count).
    static let dedupeCoverageThreshold = 0.70

    /// Decision states that make an existing mark "visible" for dedupe purposes.
    /// A suppressed/reverted FM row must NOT suppress a specialist mark.
    static let visibleDecisionStates: Set<String> = [
        AdDecisionState.candidate.rawValue,
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue
    ]

    // MARK: - Intermediate span

    /// A merged span carrying the max confidence and the ad class of its
    /// max-confidence contributing row.
    struct MergedSpan: Equatable {
        var start: Double
        var end: Double
        /// Max `probabilityOfAd` across the merged rows (conservative for
        /// markOnly — never averaged down).
        var confidence: Double
        /// `adClass` carried from the max-confidence row in the group.
        var adClass: String?
    }

    // MARK: - Compose

    /// Compose mark-only `AdWindow`s from the asset's persisted specialist scan
    /// rows, deduped against the asset's existing (non-specialist) visible marks.
    ///
    /// Four stages (blueprint §2):
    ///   1. τ filter: keep `probabilityOfAd >= tau`.
    ///   2. Merge: sort by start, sweep-merge within `mergeGap`; confidence = max,
    ///      adClass from the max-confidence row.
    ///   3. Dedupe: drop any merged span `>= dedupeCoverageThreshold` covered by
    ///      existing VISIBLE non-specialist marks.
    ///   4. Emit: one mark-only `AdWindow` per survivor, content-addressed id.
    static func compose(
        scanRows: [SpecialistScanResult],
        existingWindows: [AdWindow],
        analysisAssetId: String
    ) -> [AdWindow] {
        // Stage 1 — τ filter.
        let passing = scanRows.filter { $0.probabilityOfAd >= tau }
        guard !passing.isEmpty else { return [] }

        // Stage 2 — merge adjacent spans.
        let merged = mergeSpans(passing)

        // Stage 3 — dedupe vs existing visible NON-specialist marks. A prior
        // specialist row is deliberately excluded here (it must not self-suppress;
        // idempotency across recomposes rides on content-addressed ids + the
        // version-scoped reconcile, not on this dedupe).
        let existingVisible: [(start: Double, end: Double)] = existingWindows
            .filter {
                $0.detectorVersion != detectorVersion
                    && visibleDecisionStates.contains($0.decisionState)
            }
            .map { (start: $0.startTime, end: $0.endTime) }

        let survivors = merged.filter { span in
            coveredFraction(of: span, by: existingVisible) < dedupeCoverageThreshold
        }

        // Stage 4 — emit one mark-only AdWindow per survivor.
        return survivors.map { makeMark($0, analysisAssetId: analysisAssetId) }
    }

    // MARK: - Stage 2: merge

    /// Sort by start and sweep-merge rows whose start is within `mergeGap` of the
    /// running span's end. Merged confidence = max; adClass carried from the
    /// max-confidence row.
    static func mergeSpans(_ rows: [SpecialistScanResult]) -> [MergedSpan] {
        let sorted = rows.sorted { $0.windowStartTime < $1.windowStartTime }
        var result: [MergedSpan] = []
        for row in sorted {
            if var last = result.last, row.windowStartTime <= last.end + mergeGap {
                last.end = max(last.end, row.windowEndTime)
                // Strictly-greater keeps the earliest max-confidence row's class
                // on ties (deterministic).
                if row.probabilityOfAd > last.confidence {
                    last.confidence = row.probabilityOfAd
                    last.adClass = row.adClass
                }
                result[result.count - 1] = last
            } else {
                result.append(
                    MergedSpan(
                        start: row.windowStartTime,
                        end: row.windowEndTime,
                        confidence: row.probabilityOfAd,
                        adClass: row.adClass
                    )
                )
            }
        }
        return result
    }

    // MARK: - Stage 3: coverage dedupe

    /// Asymmetric coverage-of-`span`: the unioned length of `span ∩ existing`,
    /// divided by `span`'s duration. NOT IoU — a span fully enclosed by existing
    /// marks reads 1.0 regardless of how much wider the existing marks are.
    static func coveredFraction(
        of span: MergedSpan,
        by existing: [(start: Double, end: Double)]
    ) -> Double {
        let duration = span.end - span.start
        guard duration > 0 else { return 0 }
        var intersections: [(start: Double, end: Double)] = []
        for e in existing {
            let s = max(span.start, e.start)
            let en = min(span.end, e.end)
            if en > s { intersections.append((start: s, end: en)) }
        }
        return unionLength(intersections) / duration
    }

    /// Total length covered by a set of intervals, overlaps counted once.
    static func unionLength(_ intervals: [(start: Double, end: Double)]) -> Double {
        guard !intervals.isEmpty else { return 0 }
        let sorted = intervals.sorted { $0.start < $1.start }
        var total = 0.0
        var curStart = sorted[0].start
        var curEnd = sorted[0].end
        for iv in sorted.dropFirst() {
            if iv.start <= curEnd {
                curEnd = max(curEnd, iv.end)
            } else {
                total += curEnd - curStart
                curStart = iv.start
                curEnd = iv.end
            }
        }
        total += curEnd - curStart
        return total
    }

    // MARK: - Stage 4: emit

    /// Build the content-addressed mark-only `AdWindow` for a surviving span.
    static func makeMark(_ span: MergedSpan, analysisAssetId: String) -> AdWindow {
        AdWindow(
            id: markId(analysisAssetId: analysisAssetId, start: span.start, end: span.end),
            analysisAssetId: analysisAssetId,
            startTime: span.start,
            endTime: span.end,
            // τ-passing → confidence >= 0.70 → auto-clears the preload floor.
            confidence: span.confidence,
            boundaryState: boundaryState,
            // NEVER confirmed/applied.
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: metadataSource,
            // nil → generic no-hallucination banner copy.
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            // ALWAYS markOnly — hard-coded literal, never a policy switch.
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            // Belt+suspenders: unanchored auto-skips nothing.
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    /// Content-addressed id: `specialist-<16 hex>` over
    /// `asset=…|version=specialist-ft-v2|start=…|end=…`. An identical recompose
    /// mints the identical id, so the version-scoped reconcile retires nothing and
    /// the store's INSERT-OR-REPLACE is a true no-op (idempotency by construction).
    static func markId(analysisAssetId: String, start: Double, end: Double) -> String {
        let canonical =
            "asset=\(analysisAssetId)|version=\(detectorVersion)|start=\(start)|end=\(end)"
        let digest = SHA256.hash(data: Data(canonical.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return "specialist-\(hex.prefix(16))"
    }
}
