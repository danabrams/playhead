// AnalysisStoreWorkJournalRecorder.swift
// playhead-work-journal-wiring: production binding for the
// `WorkJournalRecording` protocol that delegates to `AnalysisStore`.
//
// **Why this exists.** Prior to this file, `PlayheadRuntime` never
// installed a real recorder on `AnalysisWorkScheduler`, so the only
// `recordPreempted(...)` site in the scheduler (the cancel-mid-decode
// catch arm) was a no-op against `NoopWorkJournalRecorder`. The
// production `work_journal` table accumulated only `acquired` rows
// (written atomically by `AnalysisStore.acquireLeaseWithJournal`) —
// no `released` / `finalized` / `failed` / `preempted` rows. The
// journal was therefore useless for forensic debugging when background
// analysis halted.
//
// This recorder closes that gap. It implements the existing
// `WorkJournalRecording` shape (the same protocol the
// `DownloadManager` delegate already conforms to) by:
//
//   1. Resolving the live `{generationID, schedulerEpoch}` for the
//      episode via `AnalysisStore.fetchLatestJobForEpisode(_:)`. This
//      mirrors the resolution pattern in
//      `AnalysisCoordinator.recordForegroundAssistOutcome`.
//   2. Constructing a `WorkJournalEntry` with the `EventType` mapped
//      from the recorder method (`recordFinalized` → `.finalized`,
//      `recordFailed` → `.failed`, `recordPreempted` → `.preempted`).
//   3. Persisting the entry via `AnalysisStore.appendWorkJournalEntry`.
//
// All store errors are swallowed and logged at the recorder boundary —
// the recorder is best-effort. The scheduler's outcome arms commit
// their state changes via `commitProcessJobOutcomeArm` BEFORE invoking
// the recorder, so a journal-append failure cannot corrupt job state.
// A logged warning is the cost of an unwritable journal row; the job
// itself remains correct.
//
// **Why we resolve generationID per-call instead of caching.**
// The scheduler's `processJob` calls `acquireLeaseWithJournal` (which
// mints a fresh generationID) at the top of each iteration, but the
// scheduler never surfaces that ID externally. Re-fetching the latest
// job on each recorder call is cheap (single-row indexed SQL lookup)
// and keeps the recorder API surface identical to the download path,
// which has the same protocol shape and uses the same resolution
// pattern. If the recorder caused contention in practice, a future
// optimization could push the generationID down to the recorder via a
// new protocol method without changing the call sites.

import Foundation
import OSLog

/// Production binding of `WorkJournalRecording` for the analysis pipeline.
///
/// Looks up the live `{generationID, schedulerEpoch}` for an episode
/// at write time and persists a `WorkJournalEntry` row via
/// `AnalysisStore.appendWorkJournalEntry`. Best-effort: store errors
/// are logged and swallowed so a journal-append failure cannot disrupt
/// scheduler state.
///
/// `final` + `Sendable`: the only stored state is an immutable
/// `AnalysisStore` reference (itself an actor), so the recorder is
/// safe to share across isolation domains.
final class AnalysisStoreWorkJournalRecorder: WorkJournalRecording, Sendable {

    private let store: AnalysisStore
    private let logger = Logger(
        subsystem: "com.playhead",
        category: "WorkJournalRecorder"
    )

    init(store: AnalysisStore) {
        self.store = store
    }

    func recordFinalized(episodeId: String) async {
        await persist(
            episodeId: episodeId,
            eventType: .finalized,
            cause: nil,
            metadataJSON: "{}"
        )
    }

    func recordFailed(episodeId: String, cause: InternalMissCause) async {
        await persist(
            episodeId: episodeId,
            eventType: .failed,
            cause: cause,
            metadataJSON: "{}"
        )
    }

    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        await persist(
            episodeId: episodeId,
            eventType: .failed,
            cause: cause,
            metadataJSON: metadataJSON
        )
    }

    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        await persist(
            episodeId: episodeId,
            eventType: .preempted,
            cause: cause,
            metadataJSON: metadataJSON
        )
    }

    // MARK: - Internal

    /// Resolves the live job row for `episodeId`, builds a
    /// `WorkJournalEntry` with the requested event type, and appends
    /// it. Errors are logged and swallowed — every callsite is a tail
    /// call after the scheduler's outcome arm has already committed
    /// its state-machine writes, so a journal-append failure is purely
    /// a diagnostics loss, not a correctness loss.
    private func persist(
        episodeId: String,
        eventType: WorkJournalEntry.EventType,
        cause: InternalMissCause?,
        metadataJSON: String
    ) async {
        do {
            guard let job = try await store.fetchLatestJobForEpisode(episodeId) else {
                logger.warning(
                    "WorkJournal append skipped: no analysis_jobs row for episode=\(episodeId, privacy: .public) event=\(eventType.rawValue, privacy: .public)"
                )
                return
            }
            // The scheduler's `acquireLeaseWithJournal` always mints a
            // canonical UUID, so this guard should never fail in
            // production — but legacy rows from before the work_journal
            // schema landed have empty `generationID` strings, and a
            // future rollback could leave one stranded. Mirror the
            // safety check in `recordForegroundAssistOutcome` and skip
            // rather than write an orphan row.
            guard let generationUUID = UUID(uuidString: job.generationID) else {
                logger.warning(
                    "WorkJournal append skipped: non-UUID generationID=\(job.generationID, privacy: .public) for episode=\(episodeId, privacy: .public) event=\(eventType.rawValue, privacy: .public)"
                )
                return
            }
            let entry = WorkJournalEntry(
                id: UUID().uuidString,
                episodeId: episodeId,
                generationID: generationUUID,
                schedulerEpoch: job.schedulerEpoch,
                timestamp: Date().timeIntervalSince1970,
                eventType: eventType,
                cause: cause,
                metadata: metadataJSON,
                artifactClass: .scratch
            )
            try await store.appendWorkJournalEntry(entry)
        } catch {
            logger.error(
                "WorkJournal append failed for episode=\(episodeId, privacy: .public) event=\(eventType.rawValue, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }
    }
}
