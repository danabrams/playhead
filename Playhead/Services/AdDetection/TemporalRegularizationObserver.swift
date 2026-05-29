// TemporalRegularizationObserver.swift
// playhead-actempo.10 temporal-regularization FIRE instrumentation:
//
// Observation-only sink that records, per asset, how many candidate detections
// had their `skipConfidence` actually CHANGED by the xsdz.10 temporal-
// regularization pass (the isolation penalty and/or the min-dwell penalty). This
// is the temporal-reg signal's "fire" count — unlike the brand-appearance /
// audio-forensics channels, xsdz.10 is NOT a ledger entry; it is a post-fusion
// multiplicative penalty on `skipConfidence`, so "did it fire?" is answered by
// "how many spans did the pass move?", which is exactly what this observer
// records.
//
// The temporal-reg live A/B (`AudioForensicsTemporalRegLiveABTests`) needs this
// so a NULL result is interpretable: a metric delta ≤±2 FP on the dogfood corpus
// is FM intra-run noise, so "metrics identical to baseline" is ambiguous unless
// we know whether the penalty pass even moved any span. The observer records the
// EXACT count the production decision path already computed (the number of spans
// whose `TemporalRegularizer.Adjustment.changed` is true and whose new value the
// service substitutes), so the recorded number is the precise "penalty-applied
// span" count — never an approximation.
//
// Contract (mirrors `FragilityDiagnosticObserver` / `BrandAppearanceChannelTapObserver`
// / `RegionShadowObserver` / `Phase5ProjectorObserver`):
//   • Compiled in all configurations. The fire site is a no-op when the observer
//     is `nil`, which is the production wiring: `PlayheadRuntime` never constructs
//     one (it is not even referenced there), so release builds have zero footprint
//     and byte-identical decision behavior.
//   • Behavior-neutral: the observer NEVER feeds back into the decision path. It
//     only RECORDS the count the real pass already produced; the adjusted
//     confidences are untouched whether the observer is nil or live.
//   • Counts accumulate per asset (each `record` adds one backfill's
//     penalty-applied span count plus its candidate count), so a full backfill
//     leaves the temporal-reg fire total for the asset.
//   • Actor for safe cross-concurrency-domain access from tests (backfill runs on
//     an arbitrary task executor; tests read from the main actor).

import Foundation
import OSLog

/// Per-asset fire tally for the xsdz.10 temporal-regularization pass. Pure value
/// type so tests (and the harness JSON dump) consume it directly.
struct TemporalRegularizationFireCounts: Sendable, Equatable {
    /// Number of candidate detections whose `skipConfidence` was actually changed
    /// by the temporal-regularization pass (isolation penalty and/or min-dwell
    /// penalty applied). This is the signal's "fire" count.
    var penaltyAppliedSpans: Int = 0
    /// Number of candidate detections the pass evaluated for this asset (the
    /// denominator — the size of `pendingDecisions` the pass ran over). Spans the
    /// pass never ran over (when the flag is off or there is ≤1 candidate) are not
    /// counted.
    var candidateSpans: Int = 0
}

actor TemporalRegularizationObserver {

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "TemporalRegularizationObserver"
    )

    /// Per-asset accumulated fire counts.
    private var counts: [String: TemporalRegularizationFireCounts] = [:]

    init() {}

    /// Record one temporal-regularization pass's outcome for an asset.
    ///
    /// The fire site supplies the EXACT counts the production pass computed: the
    /// number of candidate detections the pass ran over and the number whose
    /// `skipConfidence` it actually changed. This method only accumulates them; it
    /// derives nothing and never feeds back into the decision.
    ///
    /// - Parameters:
    ///   - assetId: analysis asset id the candidates belong to.
    ///   - candidateSpans: number of candidate detections the pass evaluated.
    ///   - penaltyAppliedSpans: number whose `skipConfidence` the pass changed.
    func record(assetId: String, candidateSpans: Int, penaltyAppliedSpans: Int) {
        var tally = counts[assetId, default: TemporalRegularizationFireCounts()]
        tally.candidateSpans += candidateSpans
        tally.penaltyAppliedSpans += penaltyAppliedSpans
        counts[assetId] = tally
        logger.debug(
            "Recorded temporal-reg fire for asset \(assetId, privacy: .public): \(penaltyAppliedSpans, privacy: .public)/\(candidateSpans, privacy: .public) spans penalized"
        )
    }

    /// The accumulated fire counts for an asset (zeroed defaults if none seen).
    func fireCounts(for assetId: String) -> TemporalRegularizationFireCounts {
        counts[assetId, default: TemporalRegularizationFireCounts()]
    }
}
