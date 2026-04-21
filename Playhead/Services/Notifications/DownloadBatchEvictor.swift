// DownloadBatchEvictor.swift
// Hard-deletes `DownloadBatch` rows that have been closed for ≥ 7 days.
// playhead-zp0x (D4 in the bead spec).
//
// Eviction policy:
//   * `closedAt == nil` → keep (still open, the coordinator owns it).
//   * `closedAt != nil` and `now - closedAt >= 7 days` → delete.
//   * `closedAt != nil` and `now - closedAt < 7 days` → keep (recently
//     closed; the cap-flag bookkeeping is still useful for diagnostics).
//
// Wired from `PlayheadApp` / `PlayheadRuntime` first-foreground hook.
// One-shot: each invocation runs the predicate against the current
// `now` and deletes matching rows. Safe to call repeatedly; idempotent
// once all eligible rows are gone.

import Foundation
import OSLog
import SwiftData

/// Deletes closed `DownloadBatch` rows older than the retention window.
/// Stateless — every method is pure with respect to its inputs.
enum DownloadBatchEvictor {

    /// Retention window after `closedAt` before a `DownloadBatch` is
    /// eligible for hard delete. 7 days per bead spec D4.
    static let retentionInterval: TimeInterval = 7 * 24 * 60 * 60

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "DownloadBatchEvictor"
    )

    /// Evict `DownloadBatch` rows whose `closedAt` is at least
    /// `retentionInterval` seconds before `now`. Returns the number of
    /// rows deleted (used by tests to assert behavior; production
    /// callers can ignore the return value).
    @discardableResult
    static func evict(modelContext: ModelContext, now: Date) -> Int {
        // The cutoff is "rows whose `closedAt <= cutoffDate`" where
        // `cutoffDate = now - retentionInterval`. Any row whose
        // `closedAt` is strictly older than the cutoff has lived past
        // the retention window.
        let cutoffDate = now.addingTimeInterval(-retentionInterval)

        // SwiftData predicate macros don't support `??` against
        // `.distantFuture` (member-access-without-explicit-base error
        // from the macro expansion). Fetch all batches that have a non-
        // nil closedAt and filter the cutoff in-memory — the predicate
        // narrows the fetch enough that the in-memory pass is cheap.
        let descriptor = FetchDescriptor<DownloadBatch>(
            predicate: #Predicate<DownloadBatch> { batch in
                batch.closedAt != nil
            }
        )

        let allClosed: [DownloadBatch]
        do {
            allClosed = try modelContext.fetch(descriptor)
        } catch {
            logger.error(
                "fetch eviction candidates failed: \(error.localizedDescription, privacy: .public)"
            )
            return 0
        }

        // In-memory cutoff filter (see comment on the descriptor).
        let candidates = allClosed.filter { batch in
            guard let closedAt = batch.closedAt else { return false }
            return closedAt <= cutoffDate
        }

        guard !candidates.isEmpty else { return 0 }

        for batch in candidates {
            modelContext.delete(batch)
        }

        do {
            try modelContext.save()
            logger.info("evicted \(candidates.count, privacy: .public) closed DownloadBatch row(s)")
            return candidates.count
        } catch {
            logger.warning(
                "evictor save failed: \(error.localizedDescription, privacy: .public)"
            )
            return 0
        }
    }
}
