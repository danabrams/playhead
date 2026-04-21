// FusionBudgetClamp.swift
// playhead-z3ch: Hard-clamp wrapper for evidence-source weight budgets.
//
// The clamp enforces a per-source weight ceiling at ingress to fusion.
// Excess weight is hard-clamped (NOT redistributed); over-cap clamps emit
// an audit-log line at info level so the tightening is observable in field
// diagnostics. Used by `BackfillEvidenceFusion.buildLedger()` to enforce
// the metadata-source ceiling (Plan §7.4: metadataCap = 0.15) when feed-
// description-derived entries are appended to the ledger.
//
// Design notes:
//   • Pure value type with a single `clamp(_:logger:)` method — trivially
//     unit-testable in isolation per playhead-z3ch's "Unit" test bullet.
//   • The wrapper preserves source/detail/classificationTrust so the
//     clamped entry remains a faithful continuation of the original
//     evidence record (only the weight changes).
//   • A DEBUG-style observer hook (`testClampObserver`) is exposed so tests
//     can assert audit-emission semantics without scraping OSLog. Production
//     code must never set this.

import Foundation
import OSLog

struct FusionBudgetClamp: Sendable {
    /// Inclusive upper bound on the post-clamp weight for entries passing
    /// through this clamp. Values strictly greater than `sourceWeightCap`
    /// are reduced to `sourceWeightCap`; values at or below are returned
    /// untouched.
    let sourceWeightCap: Double

    init(sourceWeightCap: Double) {
        self.sourceWeightCap = sourceWeightCap
    }

    /// Clamp the entry's weight to `sourceWeightCap`. Weights at or below
    /// the cap are preserved unchanged. Weights strictly greater than the
    /// cap are hard-clamped (no redistribution to other entries) and an
    /// audit line is logged at info level.
    func clamp(_ entry: EvidenceLedgerEntry, logger: Logger) -> EvidenceLedgerEntry {
        guard entry.weight > sourceWeightCap else {
            return entry
        }
        let rawWeight = entry.weight
        let cappedWeight = sourceWeightCap
        // Audit-log so callers can observe the clamp in production diagnostics.
        // Format mirrors the FM Positive-Only Rule logging precedent in
        // BackfillEvidenceFusion (info level, fixed-precision weights).
        logger.info(
            "FusionBudgetClamp: clamped \(entry.source.rawValue, privacy: .public) entry weight \(rawWeight, format: .fixed(precision: 4)) → \(cappedWeight, format: .fixed(precision: 4)) (cap=\(self.sourceWeightCap, format: .fixed(precision: 4)))"
        )
        // Test-only observer hook for verifying audit emissions without
        // scraping OSLog. Always nil in production; set/reset by Swift Testing.
        Self.testClampObserver?(rawWeight, cappedWeight)
        return EvidenceLedgerEntry(
            source: entry.source,
            weight: cappedWeight,
            detail: entry.detail,
            classificationTrust: entry.classificationTrust
        )
    }

    /// Test-only sink that fires once per clamp event with `(rawWeight, cappedWeight)`.
    /// Production code MUST leave this nil. Tests should set on entry and reset on
    /// exit to avoid cross-test leakage. Marked `nonisolated(unsafe)` to permit
    /// in-test mutation across actor contexts (the underlying tests serialize
    /// the assignment themselves).
    nonisolated(unsafe) static var testClampObserver: (@Sendable (Double, Double) -> Void)?
}
