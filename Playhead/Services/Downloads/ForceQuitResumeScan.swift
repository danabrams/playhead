// ForceQuitResumeScan.swift
// playhead-hyht: force-quit manual-resume scan + resume semantics.
//
// Background URLSession does NOT auto-relaunch a force-quit app on the
// next OS-delivered event. Any in-flight transfer the OS had when the
// user force-quit becomes "suspended" in a manual-resume state — the
// app must re-instantiate the background session, harvest the
// OS-produced resume-data blob, and hand it back to URLSession via
// `downloadTask(withResumeData:)` once the user taps "Resume in app".
//
// This file adds:
//   * A resume-data blob store (one file per episode) under
//     `<cacheDirectory>/resumeData/`.
//   * `DownloadManager.scanForSuspendedTransfers()` — runs on cold
//     launch from `PlayheadAppDelegate`, enumerates persisted blobs,
//     emits `WorkJournal.preempted` with cause
//     `.appForceQuitRequiresRelaunch` for each resumable transfer, and
//     prunes zero-length / corrupted blobs with a
//     `failed/pipelineError` emission so support triage can see them.
//   * `DownloadManager.resumeSuspendedTransfer(episodeId:)` — reads
//     the stored blob and calls `downloadTask(withResumeData:)` on the
//     interactive session (user-initiated resume path). The blob is
//     deleted on success; idempotent finalization is guaranteed by
//     uzdq's generationID rotation so stale pre-force-quit callbacks
//     land on the old generation and are dropped.
//
// Bead: playhead-hyht (Phase 1 deliverable 11, blocks playhead-5bb3
// and playhead-dfem Phase 1.5 surfaces).

import Foundation
import OSLog

// MARK: - Scan result types

/// Summary of a `scanForSuspendedTransfers()` pass. The two sets are
/// disjoint — each persisted blob ends up in exactly one of them.
struct SuspendedTransferScanOutcome: Sendable, Equatable {
    /// Transfers whose persisted resume-data blob is non-empty and will
    /// be resumed when the user taps the Activity screen CTA. The scan
    /// appended a `preempted` WorkJournal row for each entry here.
    let resumableTransferIds: Set<String>

    /// Transfers whose persisted resume-data blob was empty or
    /// otherwise unusable. The scan appended a `failed/pipelineError`
    /// WorkJournal row and removed the blob so the next scan does not
    /// re-report it.
    let corruptedTransferIds: Set<String>
}

/// Outcome of `resumeSuspendedTransfer(episodeId:)`. Callers choose
/// copy / CTA rendering off of this tag.
enum SuspendedTransferResumeOutcome: Sendable, Equatable {
    /// Resume data blob was found and handed to URLSession.
    case resumed
    /// No blob persisted for this episode — nothing to resume.
    case missing
    /// Blob present but unusable. Has been deleted; user must retry
    /// from scratch (clean-restart path).
    case corrupted
}

// MARK: - Resume-data blob store

extension DownloadManager {

    private static let resumeDataLogger = Logger(
        subsystem: "com.playhead", category: "ForceQuitResume"
    )

    /// File URL where the resume-data blob for `episodeId` is stored.
    /// Hashed via `safeFilename` to keep the on-disk name bounded and
    /// filesystem-safe; the episodeId round-trip lives in an index file
    /// alongside each blob so the scan can recover the original id.
    private func resumeDataFileURL(forHash hash: String) -> URL {
        resumeDataDirectory.appendingPathComponent("\(hash).resume")
    }

    private func resumeDataIndexFileURL(forHash hash: String) -> URL {
        resumeDataDirectory.appendingPathComponent("\(hash).episode")
    }

    /// Writes `data` as the resume-data blob for `episodeId`. Overwrites
    /// any prior blob for that episode.
    ///
    /// Internal so tests can reach it via `@testable import`. Production
    /// writes happen inside `scanForSuspendedTransfers()` — callers do
    /// NOT need to call this directly.
    func persistResumeData(episodeId: String, data: Data) throws {
        let fm = FileManager.default
        if !fm.fileExists(atPath: resumeDataDirectory.path) {
            try fm.createDirectory(
                at: resumeDataDirectory, withIntermediateDirectories: true
            )
        }
        let hash = Self.safeFilename(for: episodeId)
        let blobURL = resumeDataFileURL(forHash: hash)
        let indexURL = resumeDataIndexFileURL(forHash: hash)

        try data.write(to: blobURL, options: .atomic)
        try Data(episodeId.utf8).write(to: indexURL, options: .atomic)
    }

    /// Reads the persisted resume-data blob for `episodeId`, returning
    /// `nil` when no blob is stored.
    func loadResumeData(episodeId: String) throws -> Data? {
        let hash = Self.safeFilename(for: episodeId)
        let blobURL = resumeDataFileURL(forHash: hash)
        guard FileManager.default.fileExists(atPath: blobURL.path) else {
            return nil
        }
        return try Data(contentsOf: blobURL)
    }

    /// Deletes the persisted resume-data blob (and its index file) for
    /// `episodeId`. No-op when no blob exists.
    func deleteResumeData(episodeId: String) throws {
        let hash = Self.safeFilename(for: episodeId)
        let blobURL = resumeDataFileURL(forHash: hash)
        let indexURL = resumeDataIndexFileURL(forHash: hash)
        let fm = FileManager.default
        if fm.fileExists(atPath: blobURL.path) {
            try fm.removeItem(at: blobURL)
        }
        if fm.fileExists(atPath: indexURL.path) {
            try fm.removeItem(at: indexURL)
        }
        reportedSuspendedTransfers.remove(episodeId)
    }

    /// Enumerates every episodeId that currently has a persisted
    /// resume-data blob. Reads the `.episode` index files written
    /// alongside each blob so the scan recovers ids — not filename
    /// hashes — for the WorkJournal payload.
    func persistedResumeDataEpisodeIds() -> Set<String> {
        let fm = FileManager.default
        guard let entries = try? fm.contentsOfDirectory(
            atPath: resumeDataDirectory.path
        ) else {
            return []
        }
        var ids: Set<String> = []
        for entry in entries where entry.hasSuffix(".episode") {
            let indexURL = resumeDataDirectory.appendingPathComponent(entry)
            if let data = try? Data(contentsOf: indexURL),
               let id = String(data: data, encoding: .utf8),
               !id.isEmpty {
                ids.insert(id)
            }
        }
        return ids
    }

}

// MARK: - Scan

extension DownloadManager {

    /// Cold-launch manual-resume scan (playhead-hyht).
    ///
    /// Must be invoked within 2 s of
    /// `application(_:didFinishLaunchingWithOptions:)` — see
    /// `PlayheadAppDelegate`. The scan:
    ///
    ///   1. Iterates every episodeId that has a persisted resume-data
    ///      blob under `<cacheDirectory>/resumeData/`.
    ///   2. For non-empty blobs: emits a WorkJournal `preempted` row
    ///      with cause `.appForceQuitRequiresRelaunch` and metadata
    ///      `{episode_id, bytes}`. Surface attribution flows through
    ///      `CauseAttributionPolicy` to `.paused` + `.resumeInApp` +
    ///      `.openAppToResume` (see `CauseAttributionPolicy.attribute`).
    ///   3. For empty / unreadable blobs: emits a
    ///      `failed/pipelineError` row and deletes the blob so
    ///      subsequent scans do not re-report the same stale
    ///      condition.
    ///
    /// Idempotent: a second pass on the same set of blobs is a no-op
    /// (the in-memory `reportedSuspendedTransfers` guard prevents a
    /// double emission).
    @discardableResult
    func scanForSuspendedTransfers() async throws -> SuspendedTransferScanOutcome {
        let logger = Self.resumeDataLogger
        let ids = persistedResumeDataEpisodeIds()
        guard !ids.isEmpty else {
            logger.info("scanForSuspendedTransfers: no persisted resume-data blobs")
            return SuspendedTransferScanOutcome(
                resumableTransferIds: [],
                corruptedTransferIds: []
            )
        }

        var resumable: Set<String> = []
        var corrupted: Set<String> = []

        let now = Date().timeIntervalSince1970
        let recorder = workJournalRecorder

        // Cross-session dedup: if a persisted blob's episode is also
        // present as a live URLSession task (interactive, maintenance,
        // or legacy session), the OS still owns the transfer and will
        // deliver completion through the existing delegate path. Emit
        // `appForceQuitRequiresRelaunch` here would create a phantom
        // resume-prompt for a transfer that's actually still running.
        let liveEpisodeIds = await liveBackgroundDownloadEpisodeIds()

        for episodeId in ids {
            // Idempotence guard: if the same blob was already reported
            // during this process's lifetime, skip the re-emission.
            guard !reportedSuspendedTransfers.contains(episodeId) else {
                continue
            }

            // Skip blobs whose transfer is still live in some session.
            // The blob is left on disk; if the transfer ultimately
            // completes the delegate's resume-data harvest path will
            // overwrite it, and a subsequent scan will reconcile.
            if liveEpisodeIds.contains(episodeId) {
                logger.info("scanForSuspendedTransfers: skip \(episodeId, privacy: .public) — still live in URLSession")
                continue
            }

            let blob: Data?
            do {
                blob = try loadResumeData(episodeId: episodeId)
            } catch {
                logger.error("scanForSuspendedTransfers: failed to read blob for \(episodeId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                blob = nil
            }

            if let data = blob, !data.isEmpty {
                resumable.insert(episodeId)
                reportedSuspendedTransfers.insert(episodeId)
                // Build the SliceMetadata blob via the instrumentation
                // helper (increments SliceCounters.slicesPaused[.appForceQuitRequiresRelaunch])
                // and fold the hyht-specific fields (episode_id,
                // bytes_written, suspended_at, cause) into `extras` so
                // the flat-sibling JSON shape keeps working for pre-1nl6
                // consumers. `sliceDurationMs` is 0 because the scan
                // runs on cold launch and has no prior start instant.
                let metadata = await SliceCompletionInstrumentation.recordPaused(
                    cause: .appForceQuitRequiresRelaunch,
                    deviceClass: DeviceClass.detect(),
                    sliceDurationMs: 0,
                    bytesProcessed: data.count,
                    shardsCompleted: 0,
                    extras: [
                        "episode_id": episodeId,
                        "bytes_written": String(data.count),
                        "suspended_at": String(now),
                        "cause": InternalMissCause.appForceQuitRequiresRelaunch.rawValue,
                        "stage": "forceQuitResumeScan.resumable",
                    ]
                )
                await recorder.recordPreempted(
                    episodeId: episodeId,
                    cause: .appForceQuitRequiresRelaunch,
                    metadataJSON: metadata.encodeJSON()
                )
                logger.info("scanForSuspendedTransfers: preempted=\(episodeId, privacy: .public) bytes=\(data.count)")
            } else {
                // Corrupted / zero-length blob: emit diagnostic + prune
                // so the user sees a clean-restart once, not forever.
                corrupted.insert(episodeId)
                do {
                    try deleteResumeData(episodeId: episodeId)
                } catch {
                    logger.error("scanForSuspendedTransfers: failed to prune corrupted blob for \(episodeId, privacy: .public): \(error)")
                }
                // Same story as the resumable branch — the scan has no
                // slice start timestamp; `bytesProcessed` is 0 because
                // the corrupted blob produced no usable payload.
                let metadata = await SliceCompletionInstrumentation.recordFailed(
                    cause: .pipelineError,
                    deviceClass: DeviceClass.detect(),
                    sliceDurationMs: 0,
                    bytesProcessed: 0,
                    shardsCompleted: 0,
                    extras: [
                        "episode_id": episodeId,
                        "stage": "forceQuitResumeScan.corrupted",
                    ]
                )
                await recorder.recordFailed(
                    episodeId: episodeId,
                    cause: .pipelineError,
                    metadataJSON: metadata.encodeJSON()
                )
                logger.error("scanForSuspendedTransfers: corrupted blob for \(episodeId, privacy: .public), pruning")
            }
        }

        return SuspendedTransferScanOutcome(
            resumableTransferIds: resumable,
            corruptedTransferIds: corrupted
        )
    }

    /// Union of `taskDescription` values across already-instantiated
    /// background sessions that represent non-completed download tasks.
    /// Used by `scanForSuspendedTransfers` to avoid emitting a
    /// force-quit prompt for an episode whose transfer is still live —
    /// an OS-held task that survived the app's force-quit and
    /// reattached on relaunch should not also be represented as a
    /// stale resume-data blob.
    ///
    /// Only queries sessions the actor has already brought up; the
    /// scan runs inside the AppDelegate's 2 s SLA and instantiating a
    /// background URLSession from cold can take hundreds of ms. The
    /// cost of missing a session not yet instantiated is at most one
    /// dismissable phantom prompt when the OS later wakes us via
    /// `application(_:handleEventsForBackgroundURLSession:)` and the
    /// next scan reconciles. Calls run concurrently via `async let`.
    /// Tasks with empty or nil `taskDescription` are skipped —
    /// production code stamps every download task with `episodeId` (see
    /// `setTaskDescription` callsites in `DownloadManager` and the
    /// resume path in this file).
    private func liveBackgroundDownloadEpisodeIds() async -> Set<String> {
        let sessions = backgroundSessionsAlreadyInstantiated()
        guard !sessions.isEmpty else { return [] }
        let allTaskLists = await withTaskGroup(of: [URLSessionTask].self) { group in
            for session in sessions {
                group.addTask { await session.allTasks }
            }
            var collected: [[URLSessionTask]] = []
            for await tasks in group { collected.append(tasks) }
            return collected
        }
        var result: Set<String> = []
        for tasks in allTaskLists {
            for task in tasks where task.state != .completed {
                if let id = task.taskDescription, !id.isEmpty {
                    result.insert(id)
                }
            }
        }
        return result
    }

    /// Resumes a previously-suspended transfer by handing its
    /// OS-persisted resume-data blob back to the interactive
    /// background session.
    ///
    /// The blob is deleted on success — the OS now owns the in-flight
    /// task and will redeliver progress + completion events through
    /// the existing `EpisodeDownloadDelegate` path. Idempotent
    /// finalization is guaranteed by the uzdq lease contract: a new
    /// generationID is minted when the caller re-acquires the lease,
    /// so any stale callback from the pre-force-quit transfer hits
    /// generationID mismatch in `AnalysisStore.releaseEpisodeLease`
    /// and is dropped.
    ///
    /// - Returns: A tag describing what happened — `.resumed` on
    ///   success, `.missing` if no blob was stored, `.corrupted` if
    ///   the blob was unusable (empty). Corrupted blobs are pruned
    ///   before returning so the user sees a clean-restart error
    ///   exactly once.
    @discardableResult
    func resumeSuspendedTransfer(
        episodeId: String
    ) async throws -> SuspendedTransferResumeOutcome {
        let logger = Self.resumeDataLogger

        guard let blob = try loadResumeData(episodeId: episodeId) else {
            logger.info("resumeSuspendedTransfer: no blob for \(episodeId, privacy: .public)")
            return .missing
        }

        guard !blob.isEmpty else {
            logger.error("resumeSuspendedTransfer: empty blob for \(episodeId, privacy: .public), purging")
            try? deleteResumeData(episodeId: episodeId)
            let metadata = await SliceCompletionInstrumentation.recordFailed(
                cause: .pipelineError,
                deviceClass: DeviceClass.detect(),
                sliceDurationMs: 0,
                bytesProcessed: 0,
                shardsCompleted: 0,
                extras: [
                    "episode_id": episodeId,
                    "stage": "forceQuitResumeScan.resume.corrupted",
                ]
            )
            await workJournalRecorder.recordFailed(
                episodeId: episodeId,
                cause: .pipelineError,
                metadataJSON: metadata.encodeJSON()
            )
            return .corrupted
        }

        // Route through the interactive session — force-quit resumes
        // are always user-initiated.
        let session = backgroundSession(for: .interactive)
        let task = session.downloadTask(withResumeData: blob)
        task.taskDescription = episodeId
        task.resume()

        // The OS owns the transfer from here; drop our blob so a
        // future force-quit-scan does not re-emit preempted. Use
        // try? — a delete failure here is benign (we'll re-emit on
        // next launch) and must not surface as a user error after
        // we've already handed the blob to URLSession.
        do {
            try deleteResumeData(episodeId: episodeId)
        } catch {
            logger.error("resumeSuspendedTransfer: post-handoff delete failed for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)")
        }
        logger.info("resumeSuspendedTransfer: resumed \(episodeId, privacy: .public)")
        return .resumed
    }

}
