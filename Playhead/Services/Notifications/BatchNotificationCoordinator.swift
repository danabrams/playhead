// BatchNotificationCoordinator.swift
// Drives `BatchNotificationReducer` + `BatchNotificationService` against
// the persistent set of open `DownloadBatch` rows. playhead-zp0x.
//
// Per scheduler pass (or per coordinator tick):
//   1. Fetch all `DownloadBatch` rows where `closedAt == nil`.
//   2. For each batch, build `[BatchChildSurfaceSummary]` from per-
//      episode `EpisodeSurfaceStatus` projections (caller-supplied
//      summary builder so the coordinator stays decoupled from the
//      surface-status reducer machinery).
//   3. Run `BatchNotificationReducer.reduce(...)` to get a verdict.
//   4. Apply precedence + cap rules (the reducer already handles
//      precedence; the coordinator owns the cap).
//   5. If a notification should fire, call
//      `BatchNotificationService.emit(eligibility:batch:)`.
//   6. Update the batch's persistence-rule counters
//      (`consecutiveBlockedPasses`, `firstBlockedAt`), the cap flags
//      (`tripReadyNotified`, `actionRequiredNotified`), and `closedAt`
//      when every child has reached terminal state.
//
// Concurrency: `actor`-isolated. Production wires the coordinator
// through a Task-based timer (or, ideally, a NotificationCenter signal
// from the scheduler). Tests drive `runOncePass(now:)` directly with a
// pinned `now` so the persistence-rule wall-clock branch is
// deterministic.
//
// Generic policy: a batch with `tripContextRaw == DownloadTripContext.generic.rawValue`
// is skipped entirely — no permission ask, no notification. The
// coordinator's `runOncePass` short-circuits these batches to a no-op.

import Foundation
import OSLog
import SwiftData

/// Coordinator that drives the batch-notification pipeline.
///
/// `runOncePass(now:)` is the single entry point — production calls it
/// from a periodic Task; tests call it directly with deterministic
/// inputs. The coordinator owns the persistence-rule state machine and
/// the cap flags; the reducer is purely declarative.
///
/// `@MainActor`-isolated because SwiftData's `ModelContext.mainContext`
/// (which production injects here) is itself bound to the main actor.
/// Hopping to a separate actor isolation domain would expose the
/// non-Sendable `ModelContext` to a data race; pinning the coordinator
/// to MainActor keeps every fetch / mutate on the same isolation
/// boundary that owns the underlying context.
@MainActor
final class BatchNotificationCoordinator {

    // MARK: - Dependencies

    /// MainActor-bound model context used to fetch and mutate
    /// `DownloadBatch` rows. Coordinator hops via `await` to read
    /// batches and mutate counters / cap flags.
    private let modelContext: ModelContext

    /// Per-batch summary builder. The coordinator does NOT know how
    /// `EpisodeSurfaceStatus` is computed — the runtime injects a
    /// closure that walks each batch's `episodeKeys`, fetches the
    /// matching `Episode` rows, runs the surface-status reducer, and
    /// returns the per-child summaries. Decoupling here keeps the
    /// coordinator free of capability snapshots and SwiftData
    /// `FetchDescriptor` plumbing.
    private let summaryBuilder: @Sendable ([String]) async -> [BatchChildSurfaceSummary]

    /// Notification surface. The contract is `emit(eligibility:batch:)`.
    private let service: BatchNotificationService

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "BatchNotificationCoordinator"
    )

    // MARK: - Init

    init(
        modelContext: ModelContext,
        service: BatchNotificationService,
        summaryBuilder: @escaping @Sendable ([String]) async -> [BatchChildSurfaceSummary]
    ) {
        self.modelContext = modelContext
        self.service = service
        self.summaryBuilder = summaryBuilder
    }

    // MARK: - Public entry point

    /// Run a single coordinator pass. Tests call this directly with a
    /// pinned `now`; production wires it into a periodic Task or a
    /// scheduler-pass NotificationCenter signal.
    ///
    /// `now` is supplied by the caller so the persistence-rule wall-
    /// clock branch is deterministic in tests.
    func runOncePass(now: Date) async {
        let openBatches: [DownloadBatch]
        do {
            openBatches = try fetchOpenBatches()
        } catch {
            logger.error("fetch open batches failed: \(error.localizedDescription, privacy: .public)")
            return
        }

        for batch in openBatches {
            await processBatch(batch, now: now)
        }
    }

    // MARK: - Per-batch reduction

    private func processBatch(_ batch: DownloadBatch, now: Date) async {
        // Generic batches: per spec, no permission ask and no
        // notifications. We still record `closedAt` when applicable so
        // the row can be evicted on schedule, but we never emit.
        let isGeneric = (batch.tripContextRaw == DownloadTripContext.generic.rawValue)

        let summaries = await summaryBuilder(batch.episodeKeys)

        // Run the reducer regardless of generic-ness so we can stamp
        // `lastEligibility` for diagnostic correlation.
        let persistence = BatchNotificationReducer.PersistenceState(
            consecutiveBlockedPasses: batch.consecutiveBlockedPasses,
            firstBlockedAt: batch.firstBlockedAt
        )
        let reduction = BatchNotificationReducer.reduce(
            childSummaries: summaries,
            persistence: persistence,
            now: now
        )
        let verdict = reduction.verdict
        batch.lastEligibility = verdict.rawValue

        // Determine whether all children are in a terminal disposition
        // (ready, failed, cancelled, unavailable). When true, mark the
        // batch closed so future passes skip it and the evictor can
        // hard-delete after 7 days.
        //
        // Empty summaries close too: a batch with no children (or whose
        // children have been deleted from SwiftData) has nothing left
        // to wait for, so `closedAt` must be stamped or the row would
        // leak indefinitely past the evictor's 7-day TTL.
        let allTerminal = summaries.allSatisfy { summary in
            isTerminal(summary)
        }
        if allTerminal && batch.closedAt == nil {
            batch.closedAt = now
        }

        // Generic batches: nothing else to do — bookkeeping only.
        if isGeneric {
            try? modelContext.save()
            return
        }

        // Update persistence-rule counters BEFORE the cap check so the
        // counters reflect the current pass's contribution. The
        // reducer already used the prior-pass values to compute the
        // verdict; advancing here preps the next pass.
        //
        // Advance the streak whenever the reducer reports a
        // `pendingBlocker` candidate, NOT just when the verdict has
        // already been promoted. The persistence rule (≥2 passes AND
        // ≥30 minutes) is a gate on FIRING — not on counting passes.
        // Without this, the first blocked pass returns `.none` (rule
        // not yet satisfied) and the streak would be reset on the next
        // pass before it ever had a chance to accumulate.
        if reduction.pendingBlocker != nil {
            // Same blocked streak continues (or starts).
            batch.consecutiveBlockedPasses += 1
            if batch.firstBlockedAt == nil {
                batch.firstBlockedAt = now
            }
        } else {
            // No blocker present (trip-ready, no-fixable-blocker, or
            // empty batch) → reset the streak.
            batch.consecutiveBlockedPasses = 0
            batch.firstBlockedAt = nil
        }

        // Cap enforcement: trip-ready and action-required each fire
        // at most once per batch lifetime. The cap flag is persisted
        // BEFORE the emit call so a process kill in the window between
        // emit and save cannot cause a duplicate notification on next
        // launch. Failure mode if save-then-emit is interrupted: the
        // user gets no notification (silence is preferable to spam).
        switch verdict {
        case .tripReady:
            if !batch.tripReadyNotified {
                batch.tripReadyNotified = true
                persist(batch)
                await service.emit(eligibility: .tripReady, batch: batch)
                return
            }
        case .blockedAnalysisUnavailable, .blockedStorage, .blockedWifiPolicy:
            if !batch.actionRequiredNotified {
                batch.actionRequiredNotified = true
                persist(batch)
                await service.emit(eligibility: verdict, batch: batch)
                return
            }
        case .none:
            break
        }

        persist(batch)
    }

    private func persist(_ batch: DownloadBatch) {
        do {
            try modelContext.save()
        } catch {
            logger.warning(
                "save failed for batch \(batch.id.uuidString, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
    }

    // MARK: - Helpers

    /// Whether a child summary represents a terminal state. A child
    /// is terminal when it has reached ready, or when its disposition
    /// is `.failed`, `.cancelled`, or `.unavailable` (the last because
    /// an unavailable child cannot make progress without user action).
    private func isTerminal(_ summary: BatchChildSurfaceSummary) -> Bool {
        if summary.isReady { return true }
        switch summary.disposition {
        case .failed, .cancelled, .unavailable:
            return true
        case .queued, .paused:
            return false
        }
    }

    private func fetchOpenBatches() throws -> [DownloadBatch] {
        let descriptor = FetchDescriptor<DownloadBatch>(
            predicate: #Predicate<DownloadBatch> { $0.closedAt == nil }
        )
        return try modelContext.fetch(descriptor)
    }
}
