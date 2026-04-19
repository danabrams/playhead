// SwiftDataDiagnosticsOptInSink.swift
// SwiftData-backed adapter for `DiagnosticsOptInSink`. Flips
// `Episode.diagnosticsOptIn` on the rows that shipped in a diagnostics
// bundle after `DiagnosticsOptInResetPolicy` says the flag should
// reset.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Why this file lives in Support/Diagnostics:
//   The Support layer is explicitly exempt from the
//   "views/app must not import persistence" lint added by playhead-ol05
//   (see Playhead/ConcurrencyInvariants/LintRules.swift). Keeping the
//   persistence adapter here lets the coordinator take
//   `DiagnosticsOptInSink` as an abstraction while the concrete
//   SwiftData work stays localized.

import Foundation
import SwiftData

/// Flips `Episode.diagnosticsOptIn` to the supplied value for every
/// supplied episode ID. Missing IDs (episode was deleted between the
/// build and the reset) are silently skipped — the reset is idempotent
/// against concurrent deletion.
@MainActor
final class SwiftDataDiagnosticsOptInSink: DiagnosticsOptInSink {

    private let context: ModelContext

    init(context: ModelContext) {
        self.context = context
    }

    /// Set `Episode.diagnosticsOptIn = newValue` for every
    /// `canonicalEpisodeKey` in the supplied list that currently exists
    /// in the store. Errors from the fetch/save cycle are logged but not
    /// rethrown — the coordinator has already presented the bundle by
    /// the time this runs, so surfacing an error back up would be
    /// ambiguous UX (the export succeeded).
    func applyResetToEpisodes(
        matchingEpisodeIds: [String],
        newValue: Bool
    ) {
        guard !matchingEpisodeIds.isEmpty else { return }

        // Swift's `FetchDescriptor` cannot take a type-safe IN-clause on
        // the raw `[String]` directly because `#Predicate` macro
        // expansion requires the input array be referenceable in the
        // predicate expression. Fetching all Episodes and filtering
        // client-side is acceptable here: the full-table scan is bounded
        // by the user's subscribed episode count, and the reset runs at
        // most once per user-initiated diagnostics export.
        let predicateSet = Set(matchingEpisodeIds)
        let descriptor = FetchDescriptor<Episode>()
        let rows: [Episode]
        do {
            rows = try context.fetch(descriptor)
        } catch {
            // Persistence error during reset is non-fatal at the
            // coordinator layer; the next successful export will retry.
            return
        }

        for row in rows where predicateSet.contains(row.canonicalEpisodeKey) {
            if row.diagnosticsOptIn != newValue {
                row.diagnosticsOptIn = newValue
            }
        }

        try? context.save()
    }
}
