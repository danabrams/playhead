// FragilityDiagnosticObserver.swift
// playhead-xsdz.7 fragility diagnostic:
//
// Observation-only sink for the per-span Evidence-Fragility geometry that the
// xsdz.7 precision gate (`AdDetectionConfig.applyFragilityPenalty`) reads. The
// production `AdDetectionService.runBackfill` decision path fires this observer
// for EVERY decoded span — regardless of whether the fragility penalty is
// enabled — so a single live A/B pass can dump the per-span fragility-score
// distribution and answer "is the geometry even discriminative between the FP
// and TP groups?".
//
// Contract (mirrors `RegionShadowObserver` / `Phase5ProjectorObserver`):
//   • Compiled in all configurations. The diagnostic fire site is a no-op
//     when the observer is `nil`, which is the production wiring: PlayheadRuntime
//     never constructs one (it is not even referenced there), so release builds
//     have zero diagnostic footprint and byte-identical decision behavior.
//   • Behavior-neutral: the observer NEVER feeds back into the decision path.
//     It only RECORDS the inputs the real decision already computed; the gate
//     output is untouched whether the observer is nil or live.
//   • Writes accumulate per asset (each `record` appends one span's row), so a
//     full backfill leaves the complete per-span ledger geometry for the asset.
//   • Actor for safe cross-concurrency-domain access from tests (backfill runs
//     on an arbitrary task executor; tests read from the main actor).
//
// Why the component inputs are derived HERE (not at the fire site): the fire
// site passes the raw decision inputs it already has — the post-suppression
// `ledger`, the decision's `proposalConfidence` / `skipConfidence` /
// `promotionTrack`, the `.standard`-track auto-skip threshold, and the
// fragility SCORE computed by the existing `AdDetectionConfig.fragilityScore`
// helper (NOT reimplemented). The observer then derives the descriptive
// component terms (`maxSingleEntryWeight`, `distinctEvidenceFamilyDepth`,
// `margin`) from that same ledger using the SAME public taxonomy the formula
// uses (`EvidenceSourceType.isObservabilityOnly`, strictly-positive `weight`,
// `SourceEvidenceFamily.for`). Keeping that derivation off the production hot
// path (it only runs when an observer is injected, i.e. in tests) preserves the
// "only change is the nil-default observer" constraint while still surfacing the
// formula's intermediate geometry for the diagnostic.

import Foundation
import OSLog

/// One recorded span's fragility geometry. Carries exactly the fields the
/// xsdz.7 diagnostic needs to characterize the FP vs TP fragility distribution.
/// Pure value type so tests (and the harness JSON dump) can consume it directly.
struct FragilitySpanDiagnostic: Sendable, Equatable {
    /// Stable decoded-span id (matches `DecodedSpan.id` for the span that
    /// produced the persisted `AdWindow`, when one was persisted).
    let spanId: String
    /// Span start in seconds (== the decision's refined span start, which the
    /// persisted fusion `AdWindow.startTime` is built from verbatim).
    let spanStart: Double
    /// Span end in seconds (== the persisted fusion `AdWindow.endTime`).
    let spanEnd: Double
    /// The decision's fused proposal confidence (drives margin + concentration).
    let proposalConfidence: Double
    /// The decision's skip confidence at the fragility-gate evaluation point
    /// (pre-penalty: this is what the baseline arm sees, since the baseline
    /// runs with the gate OFF).
    let skipConfidence: Double
    /// `maxSingleEntryWeight`: the largest strictly-positive, scoring (non
    /// observability) ledger entry weight — the concentration numerator.
    let maxSingleEntryWeight: Double
    /// `distinctEvidenceFamilyDepth`: number of distinct `SourceEvidenceFamily`
    /// buckets with a strictly-positive scoring entry — the depth term.
    let distinctEvidenceFamilyDepth: Int
    /// `margin = proposalConfidence − effectiveAutoSkipThreshold(.standard)`.
    /// Recorded against the `.standard` track per the diagnostic spec so every
    /// row is comparable on one threshold axis.
    let margin: Double
    /// The computed fragility score from `AdDetectionConfig.fragilityScore`
    /// (the SAME helper the real gate uses) over the SAME ledger the decision
    /// used. NOT reimplemented here.
    let fragilityScore: Double
}

actor FragilityDiagnosticObserver {

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "FragilityDiagnosticObserver"
    )

    /// Per-asset accumulated span rows, in the order `record` was called.
    private var rows: [String: [FragilitySpanDiagnostic]] = [:]
    private var recordCounts: [String: Int] = [:]

    init() {}

    /// Record one span's fragility geometry for an asset.
    ///
    /// The fire site supplies the raw inputs the real decision used; this method
    /// derives the descriptive component terms from the SAME `ledger` (using the
    /// same public taxonomy the formula uses) and stores a fully-formed row.
    ///
    /// - Parameters:
    ///   - assetId: analysis asset id the span belongs to.
    ///   - spanId: stable decoded-span id.
    ///   - spanStart: refined span start (seconds).
    ///   - spanEnd: refined span end (seconds).
    ///   - proposalConfidence: the decision's proposal confidence.
    ///   - skipConfidence: the decision's pre-penalty skip confidence.
    ///   - standardAutoSkipThreshold: `config.effectiveAutoSkipThreshold(.standard)`,
    ///     resolved by the fire site so the margin axis is fixed to `.standard`.
    ///   - fragilityScore: the value of `config.fragilityScore(...)` over the
    ///     SAME ledger (computed by the fire site; not recomputed here).
    ///   - ledger: the post-suppression evidence ledger that fed the decision.
    func record(
        assetId: String,
        spanId: String,
        spanStart: Double,
        spanEnd: Double,
        proposalConfidence: Double,
        skipConfidence: Double,
        standardAutoSkipThreshold: Double,
        fragilityScore: Double,
        ledger: [EvidenceLedgerEntry]
    ) {
        // Scoring entries with strictly-positive weight. Observability-only
        // rows never enter fusion and must not count toward concentration or
        // depth — IDENTICAL filter to `AdDetectionConfig.fragilityScore`.
        let scoringEntries = ledger.filter {
            !$0.source.isObservabilityOnly && $0.weight > 0
        }
        let maxSingleEntryWeight = scoringEntries.map(\.weight).max() ?? 0.0
        let distinctFamilies = Set(scoringEntries.map { SourceEvidenceFamily.for($0.source) })
        let margin = proposalConfidence - standardAutoSkipThreshold

        let row = FragilitySpanDiagnostic(
            spanId: spanId,
            spanStart: spanStart,
            spanEnd: spanEnd,
            proposalConfidence: proposalConfidence,
            skipConfidence: skipConfidence,
            maxSingleEntryWeight: maxSingleEntryWeight,
            distinctEvidenceFamilyDepth: distinctFamilies.count,
            margin: margin,
            fragilityScore: fragilityScore
        )
        rows[assetId, default: []].append(row)
        recordCounts[assetId, default: 0] += 1
        logger.debug(
            "Recorded fragility span \(spanId, privacy: .public) for asset \(assetId, privacy: .public): score=\(fragilityScore, privacy: .public)"
        )
    }

    /// All recorded span rows for an asset (in record order), or nil if none.
    func spanRows(for assetId: String) -> [FragilitySpanDiagnostic]? {
        rows[assetId]
    }

    /// Number of times `record` has been called for an asset.
    func recordCount(for assetId: String) -> Int {
        recordCounts[assetId, default: 0]
    }
}
