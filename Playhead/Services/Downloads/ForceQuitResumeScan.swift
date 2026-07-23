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
    /// playhead-wrj8: a complete pinned artifact already exists for this
    /// episode — resuming would splice a fresh DAI stitch into (or beside)
    /// the played file, so the blob is discarded and nothing is fetched.
    case alreadyComplete
    /// playhead-wrj8: the persisted resume blob could NOT be proven fresh
    /// against the live server (ETag / Content-Length changed, or the
    /// validator could not be established). Rather than
    /// `downloadTask(withResumeData:)` — whose Range/If-Range request would
    /// splice a different-length stitch into the played file — the blob is
    /// discarded and a FRESH full download is started to a new artifact.
    case redownloadedFresh
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

    /// playhead-wrj8: sidecar recording the source URL + HTTP validator of
    /// the suspended transfer, so `resumeSuspendedTransfer` can prove the
    /// server bytes haven't rotated before splicing the blob.
    private func resumeValidatorFileURL(forHash hash: String) -> URL {
        resumeDataDirectory.appendingPathComponent("\(hash).validator")
    }

    /// playhead-wrj8: persisted resume-freshness validator.
    struct ResumeValidatorRecord: Codable, Sendable, Equatable {
        var url: String?
        var etag: String?
        var contentLength: Int64?
    }

    /// Writes `data` as the resume-data blob for `episodeId`. Overwrites
    /// any prior blob for that episode.
    ///
    /// Internal so tests can reach it via `@testable import`. Production
    /// writes happen via the delegate resume-data harvest — callers do
    /// NOT need to call this directly.
    ///
    /// playhead-wrj8: `sourceURL` + `validator` are persisted in a sidecar
    /// so the resume path can validate freshness. Both default to `nil` for
    /// backward compatibility (legacy 2-arg callers / blobs without a
    /// harvested response resume as before).
    func persistResumeData(
        episodeId: String,
        data: Data,
        sourceURL: URL? = nil,
        validator: HTTPAssetMetadata? = nil
    ) throws {
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

        let validatorURL = resumeValidatorFileURL(forHash: hash)
        if sourceURL != nil || validator != nil {
            let record = ResumeValidatorRecord(
                url: sourceURL?.absoluteString,
                etag: validator?.etag,
                contentLength: validator?.contentLength
            )
            if let encoded = try? JSONEncoder().encode(record) {
                try encoded.write(to: validatorURL, options: .atomic)
            }
        } else {
            // No validator harvested — drop any stale sidecar so the
            // resume path takes the legacy (unvalidated) branch rather
            // than comparing against outdated data.
            try? fm.removeItem(at: validatorURL)
        }
    }

    /// Loads the persisted resume-freshness validator for `episodeId`.
    func loadResumeValidator(episodeId: String) -> ResumeValidatorRecord? {
        let hash = Self.safeFilename(for: episodeId)
        let url = resumeValidatorFileURL(forHash: hash)
        guard let data = try? Data(contentsOf: url) else { return nil }
        return try? JSONDecoder().decode(ResumeValidatorRecord.self, from: data)
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
        let validatorURL = resumeValidatorFileURL(forHash: hash)
        let fm = FileManager.default
        if fm.fileExists(atPath: blobURL.path) {
            try fm.removeItem(at: blobURL)
        }
        if fm.fileExists(atPath: indexURL.path) {
            try fm.removeItem(at: indexURL)
        }
        // playhead-wrj8: symmetric cleanup of the freshness sidecar.
        try? fm.removeItem(at: validatorURL)
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

        // playhead-wrj8: if a COMPLETE pinned artifact already exists,
        // resuming would splice a fresh DAI stitch into / beside the bytes
        // the user already played and marked. Discard the blob and keep the
        // played copy untouched.
        if servingURLIfComplete(for: episodeId) != nil {
            logger.info("resumeSuspendedTransfer: \(episodeId, privacy: .public) already complete — discarding resume blob")
            try? deleteResumeData(episodeId: episodeId)
            return .alreadyComplete
        }

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

        // playhead-wrj8: FRESHNESS GATE. A `downloadTask(withResumeData:)`
        // replays as a Range / If-Range request against the enclosure. On a
        // DAI origin that re-cuts a different ad stitch per request, the
        // resumed bytes are a DIFFERENT-length stitch than the partial we
        // already have — splicing them corrupts the played file. So when we
        // have a persisted validator + source URL, prove the server bytes
        // are unchanged (ETag AND Content-Length match) before resuming.
        // On mismatch, or when we cannot establish the current validator,
        // discard the blob and download FRESH to a clean artifact instead.
        //
        // Legacy blobs with no persisted validator/URL take the original
        // (unvalidated) resume path — non-destructive to pre-wrj8 state.
        if let record = loadResumeValidator(episodeId: episodeId),
           let urlString = record.url,
           let sourceURL = URL(string: urlString) {
            let current = await currentServerValidator(for: sourceURL)
            let stillFresh = Self.resumeValidatorsMatch(stored: record, current: current)
            if !stillFresh {
                logger.info("resumeSuspendedTransfer: \(episodeId, privacy: .public) server rotated (or unverifiable) — discarding resume blob, downloading fresh")
                try? deleteResumeData(episodeId: episodeId)
                // playhead-wrj8: clear any INCOMPLETE leftover artifact (+
                // its stale pin) for this episode first. We only reach here
                // when `servingURLIfComplete` was nil (no complete pin), so
                // anything present is a partial; removing it guarantees the
                // fresh `backgroundDownload` isn't skipped by its
                // existence check and lands a clean, fully-pinned artifact.
                let leftover = completeFileURL(for: episodeId)
                if FileManager.default.fileExists(atPath: leftover.path) {
                    try? FileManager.default.removeItem(at: leftover)
                }
                deletePin(for: episodeId)
                // Fresh full download to a new artifact via the normal
                // background path (its own overwrite guard + pinning apply).
                backgroundDownload(episodeId: episodeId, from: sourceURL)
                return .redownloadedFresh
            }
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

    /// playhead-wrj8: a stored resume blob is safe to splice only when the
    /// live server validator is present AND matches on BOTH ETag and
    /// Content-Length. A `nil` current validator (HEAD failed / server
    /// omitted headers) is treated as "cannot prove freshness" → NOT a
    /// match, forcing a fresh download rather than risking a rotated splice.
    static func resumeValidatorsMatch(
        stored: ResumeValidatorRecord,
        current: HTTPAssetMetadata?
    ) -> Bool {
        guard let current else { return false }
        // ETag is the strongest freshness signal. If we captured one at
        // suspend time, demand an EXACT match now — a mismatch OR the
        // server no longer surfacing an ETag both mean we cannot prove the
        // bytes are unchanged, so treat either as "rotated" and force a
        // fresh download. (A Content-Length fallback here would false-
        // negative when a rotated DAI stitch happens to share the old
        // length.)
        if let storedETag = stored.etag {
            return current.etag == storedETag
        }
        // No ETag was ever captured — fall back to Content-Length equality
        // (both sides must be present and equal). A length change alone is
        // proof the enclosure rotated.
        if let storedLen = stored.contentLength, let currentLen = current.contentLength {
            return storedLen == currentLen
        }
        // Nothing provable → do not splice.
        return false
    }

}
