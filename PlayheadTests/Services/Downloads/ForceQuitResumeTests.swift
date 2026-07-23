// ForceQuitResumeTests.swift
// playhead-hyht: force-quit manual-resume scan + resume semantics.
//
// After a force-quit, the OS does NOT auto-relaunch the app to drain
// background URLSession events. On next cold launch the DownloadManager
// runs `scanForSuspendedTransfers()` to locate suspended transfers,
// harvest their OS-persisted resume-data blobs, emit a WorkJournal
// `preempted` entry with cause `.appForceQuitRequiresRelaunch`, and
// wait for the user to tap "Resume in app" — at which point the stored
// blob is re-submitted via `urlSession.downloadTask(withResumeData:)`.

import Foundation
import Testing
import UIKit
@testable import Playhead

// MARK: - Recording WorkJournal (extended for hyht)

/// Captures `preempted` + `failed` emissions from the scan.
private actor HyhtRecorder: WorkJournalRecording {
    struct Preempted: Sendable, Equatable {
        let episodeId: String
        let cause: InternalMissCause
        let metadataJSON: String
    }
    struct Failure: Sendable, Equatable {
        let episodeId: String
        let cause: InternalMissCause
        let metadataJSON: String?
    }

    private(set) var preempted: [Preempted] = []
    private(set) var failures: [Failure] = []
    private(set) var finalized: [String] = []

    /// playhead-vsot round 2: tests waiting for a specific episode's
    /// `preempted` emission. Resumed from `recordPreempted` the moment
    /// the matching entry lands — event-driven, replacing the 2 s
    /// `pollUntil` deadline that expired under full-suite load before
    /// the app-delegate's async scan Task was ever scheduled.
    private var preemptedWaiters:
        [(episodeId: String, continuation: CheckedContinuation<Void, Never>)] = []

    func recordFinalized(episodeId: String) async {
        finalized.append(episodeId)
    }

    func recordFailed(episodeId: String, cause: InternalMissCause) async {
        failures.append(Failure(episodeId: episodeId, cause: cause, metadataJSON: nil))
    }

    // playhead-1nl6: protocol now requires the metadata-carrying
    // overload directly — the silent default-forward that dropped the
    // JSON blob was removed.
    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        failures.append(Failure(episodeId: episodeId, cause: cause, metadataJSON: metadataJSON))
    }

    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        preempted.append(Preempted(episodeId: episodeId, cause: cause, metadataJSON: metadataJSON))
        let ready = preemptedWaiters.filter { $0.episodeId == episodeId }
        preemptedWaiters.removeAll { $0.episodeId == episodeId }
        for waiter in ready {
            waiter.continuation.resume()
        }
    }

    /// Suspend until a `preempted` entry for `episodeId` has been
    /// recorded. Returns immediately if it already was. No deadline —
    /// the test's `.timeLimit` trait is the backstop, so a genuine
    /// "scan never ran" regression fails deterministically instead of
    /// load-dependently.
    func awaitPreempted(episodeId: String) async {
        if preempted.contains(where: { $0.episodeId == episodeId }) { return }
        await withCheckedContinuation { continuation in
            preemptedWaiters.append((episodeId: episodeId, continuation: continuation))
        }
    }
}

// MARK: - Resume-data directory layout

@Suite("DownloadManager – force-quit resume-data storage")
struct ForceQuitResumeDataStorageTests {

    @Test("persistResumeData writes blob to the resume-data subdirectory")
    func persistWritesBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let blob = Data([0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04])
        try await manager.persistResumeData(episodeId: "ep-hyht-1", data: blob)

        let loaded = try await manager.loadResumeData(episodeId: "ep-hyht-1")
        #expect(loaded == blob)
    }

    @Test("persistResumeData overwrites a prior blob for the same episode")
    func persistOverwrites() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        try await manager.persistResumeData(episodeId: "ep-hyht-2", data: Data([0x01]))
        try await manager.persistResumeData(episodeId: "ep-hyht-2", data: Data([0x02, 0x03]))

        let loaded = try await manager.loadResumeData(episodeId: "ep-hyht-2")
        #expect(loaded == Data([0x02, 0x03]))
    }

    @Test("loadResumeData returns nil when no blob is persisted")
    func loadMissingReturnsNil() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let loaded = try await manager.loadResumeData(episodeId: "ep-missing")
        #expect(loaded == nil)
    }

    @Test("persistedResumeDataEpisodeIds enumerates stored blobs")
    func enumeration() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        try await manager.persistResumeData(episodeId: "ep-a", data: Data([0xAA]))
        try await manager.persistResumeData(episodeId: "ep-b", data: Data([0xBB]))

        let ids = await manager.persistedResumeDataEpisodeIds()
        #expect(ids == Set(["ep-a", "ep-b"]))
    }

    @Test("deleteResumeData removes the blob")
    func deleteBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        try await manager.persistResumeData(episodeId: "ep-del", data: Data([0x01]))
        try await manager.deleteResumeData(episodeId: "ep-del")

        let loaded = try await manager.loadResumeData(episodeId: "ep-del")
        #expect(loaded == nil)
    }
}

// MARK: - scanForSuspendedTransfers — unit tests via injected seeds

@Suite("DownloadManager – scanForSuspendedTransfers")
struct ScanForSuspendedTransfersTests {

    @Test("Scan emits preempted entry for every persisted resume-data blob")
    func scanEmitsPreemptedPerBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        // Seed two persisted blobs as if they were left behind by a
        // force-quit prior to this cold launch.
        try await manager.persistResumeData(episodeId: "ep-1", data: Data([0x01, 0x02]))
        try await manager.persistResumeData(episodeId: "ep-2", data: Data([0x03, 0x04, 0x05]))

        let outcome = try await manager.scanForSuspendedTransfers()

        #expect(outcome.resumableTransferIds == Set(["ep-1", "ep-2"]))
        #expect(outcome.corruptedTransferIds.isEmpty)

        let preempted = await recorder.preempted
        #expect(preempted.count == 2)
        let ids = Set(preempted.map(\.episodeId))
        #expect(ids == Set(["ep-1", "ep-2"]))
        for entry in preempted {
            #expect(entry.cause == .appForceQuitRequiresRelaunch)
            // metadataJSON carries episode + byte count at minimum.
            #expect(entry.metadataJSON.contains(entry.episodeId))
        }
    }

    @Test("Scan is idempotent — re-running does not double-emit preempted entries")
    func scanIsIdempotent() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        try await manager.persistResumeData(episodeId: "ep-idem", data: Data([0x01]))

        _ = try await manager.scanForSuspendedTransfers()
        _ = try await manager.scanForSuspendedTransfers()

        let preempted = await recorder.preempted
        #expect(preempted.count == 1)
    }

    @Test("Scan handles an empty cache with no emissions and no errors")
    func scanWithNoBlobs() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        let outcome = try await manager.scanForSuspendedTransfers()

        #expect(outcome.resumableTransferIds.isEmpty)
        #expect(outcome.corruptedTransferIds.isEmpty)

        let preempted = await recorder.preempted
        #expect(preempted.isEmpty)
    }

    /// Fast-suite functional pin at the SLA test's scale: the scan
    /// COMPLETES over a 10-blob cache and reports every seeded blob.
    /// Event-driven by construction — the wait IS the direct `await` on
    /// `scanForSuspendedTransfers()`; a hang fails the `.timeLimit`
    /// deterministically. No clock anywhere (playhead-vsot round 2 /
    /// m9xk pattern: latency belongs to the serial perf lane below).
    @Test("Scan completes over a 10-blob cache and reports every blob",
          .timeLimit(.minutes(1)))
    func scanCompletesOverTenBlobCache() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        for i in 0..<10 {
            try await manager.persistResumeData(
                episodeId: "ep-sla-\(i)",
                data: Data(repeating: UInt8(i), count: 1024)
            )
        }

        let outcome = try await manager.scanForSuspendedTransfers()
        #expect(outcome.resumableTransferIds == Set((0..<10).map { "ep-sla-\($0)" }))
        #expect(outcome.corruptedTransferIds.isEmpty)
    }

    /// Bead requirement: "scan completes within 2 s on cold launch".
    /// This is a wall-clock LATENCY measurement — only valid on a
    /// quiescent CPU, so it is PerfGate-gated and runs exclusively in
    /// the serial perf pass (scripts/perf-tests.sh; listed in its
    /// MEASUREMENT_TESTS). Under the parallel fast plan the previous
    /// version failed the 2026-07-20 gate at ~92 s wall with a sample
    /// past even its 30 s "hang ceiling" — pure scheduler starvation,
    /// not a scan regression (the scan takes ~12 ms in isolation).
    /// The functional completion coverage stays in the fast suite via
    /// `scanCompletesOverTenBlobCache` above; the 30 s ceiling is gone
    /// because the `.timeLimit` trait is the hang backstop.
    @Test("Scan completes within the 2-second SLA specified by the bead",
          .enabled(if: PerfGate.runsMeasurementTests, "perf pass only — see playhead-zx0l"),
          .timeLimit(.minutes(1)))
    func scanCompletesWithinSLA() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        // Seed a handful of blobs so the scan has real work to do.
        for i in 0..<10 {
            try await manager.persistResumeData(
                episodeId: "ep-sla-\(i)",
                data: Data(repeating: UInt8(i), count: 1024)
            )
        }

        // Median of 3 runs on the quiescent perf lane.
        var samples: [Duration] = []
        for _ in 0..<3 {
            let start = ContinuousClock.now
            _ = try await manager.scanForSuspendedTransfers()
            samples.append(ContinuousClock.now - start)
        }
        let median = samples.sorted()[1]
        #expect(median < .seconds(2),
                "Cold-launch scan SLA is 2 s; median was \(median) (samples: \(samples))")
    }

    @Test("Corrupted (zero-length) resume-data emits failed/pipelineError and deletes the blob")
    func corruptedBlobEmitsPipelineError() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        // A zero-length blob is the canonical "corrupted" signal — URLSession
        // cannot reconstruct a task from it.
        try await manager.persistResumeData(episodeId: "ep-corrupt", data: Data())

        let outcome = try await manager.scanForSuspendedTransfers()

        #expect(outcome.resumableTransferIds.isEmpty)
        #expect(outcome.corruptedTransferIds == Set(["ep-corrupt"]))

        let preempted = await recorder.preempted
        #expect(preempted.isEmpty)

        let failures = await recorder.failures
        #expect(failures.count == 1)
        #expect(failures.first?.episodeId == "ep-corrupt")
        #expect(failures.first?.cause == .pipelineError)

        // Corrupted blob is removed so the next scan doesn't re-report it.
        let loaded = try await manager.loadResumeData(episodeId: "ep-corrupt")
        #expect(loaded == nil)
    }

    @Test("Scan tolerates a mix of valid and corrupted blobs")
    func scanMixedBlobs() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        try await manager.persistResumeData(episodeId: "ep-good", data: Data([0x01, 0x02, 0x03]))
        try await manager.persistResumeData(episodeId: "ep-bad", data: Data())

        let outcome = try await manager.scanForSuspendedTransfers()

        #expect(outcome.resumableTransferIds == Set(["ep-good"]))
        #expect(outcome.corruptedTransferIds == Set(["ep-bad"]))

        let preempted = await recorder.preempted
        #expect(preempted.count == 1)
        #expect(preempted.first?.episodeId == "ep-good")

        let failures = await recorder.failures
        #expect(failures.count == 1)
        #expect(failures.first?.episodeId == "ep-bad")
    }
}

// MARK: - resumeSuspendedTransfer

@Suite("DownloadManager – resumeSuspendedTransfer")
struct ResumeSuspendedTransferTests {

    @Test("resumeSuspendedTransfer consumes the persisted blob on success")
    func resumeConsumesBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        // NOTE: we cannot inject a real suspended URLSessionDownloadTask in a
        // unit test, so this exercises the test seam. A non-empty blob takes
        // the "attempt resume" branch and the seam reports success.
        try await manager.persistResumeData(episodeId: "ep-res", data: Data([0xAB, 0xCD]))

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: "ep-res")
        #expect(outcome == .resumed)

        // On success the blob is removed — the OS now owns continuation.
        let loaded = try await manager.loadResumeData(episodeId: "ep-res")
        #expect(loaded == nil)

        // playhead-6e8m: `resumeSuspendedTransfer` constructed a real
        // background URLSession on the process-global
        // `com.playhead.transfer.interactive` identifier and handed it
        // a garbage 2-byte resume-data blob. Without invalidation the
        // session + orphan task stays alive and leaks into any sibling
        // test that subsequently constructs a `DownloadManager` (the
        // identifier collides). Tear down explicitly here.
        await manager.invalidateBackgroundSessionsForTesting()
    }

    @Test("resumeSuspendedTransfer returns .missing when no blob is persisted")
    func resumeMissingBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: "ep-none")
        #expect(outcome == .missing)
    }

    @Test("resumeSuspendedTransfer rejects corrupted blob and deletes it")
    func resumeCorruptedBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        try await manager.persistResumeData(episodeId: "ep-corrupt2", data: Data())

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: "ep-corrupt2")
        #expect(outcome == .corrupted)

        // Blob is purged so the user sees a clean-restart error once, not
        // forever.
        let loaded = try await manager.loadResumeData(episodeId: "ep-corrupt2")
        #expect(loaded == nil)

        // Emits failed/pipelineError for the support-triage Diagnostics path.
        let failures = await recorder.failures
        #expect(failures.count == 1)
        #expect(failures.first?.episodeId == "ep-corrupt2")
        #expect(failures.first?.cause == .pipelineError)
    }

    // MARK: - playhead-wrj8: resume freshness gate

    @Test("wrj8: a resume whose server ETag/length rotated re-downloads fresh instead of splicing")
    func resumeRotatedValidatorRedownloadsFresh() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-rotated"
        let src = URL(string: "https://dai.example.com/ep.mp3")!
        // Suspended transfer captured validator: ETag "A", length 100.
        try await manager.persistResumeData(
            episodeId: episodeId,
            data: Data([0x01, 0x02, 0x03]),
            sourceURL: src,
            validator: HTTPAssetMetadata(etag: "\"A\"", contentLength: 100, lastModified: nil)
        )
        // An INCOMPLETE partial (withheld by an under-length pin) sits at
        // the target path — the fresh re-download must clear it so it isn't
        // skipped by the existence check.
        let leftover = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0x11, count: 40).write(to: leftover)
        await manager.writePin(
            AudioAssetPin(expectedBytes: 100, sha256: nil, sourceURL: nil, etag: nil),
            for: episodeId
        )
        // Server now serves a DIFFERENT stitch: ETag "B", length 80.
        await manager.setResumeValidatorProviderForTesting { _ in
            HTTPAssetMetadata(etag: "\"B\"", contentLength: 80, lastModified: nil)
        }

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: episodeId)
        #expect(outcome == .redownloadedFresh)
        // Blob discarded — the fresh full download owns continuation now.
        #expect(try await manager.loadResumeData(episodeId: episodeId) == nil)
        // The incomplete leftover + its stale pin were cleared so the fresh
        // download is not skipped by the existence check.
        #expect(!FileManager.default.fileExists(atPath: leftover.path))
        #expect(await manager.loadPin(for: episodeId) == nil)

        await manager.invalidateBackgroundSessionsForTesting()
    }

    @Test("wrj8: a resume whose server validator still matches splices the blob")
    func resumeFreshValidatorResumes() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-fresh"
        let src = URL(string: "https://cdn.example.com/ep.mp3")!
        try await manager.persistResumeData(
            episodeId: episodeId,
            data: Data([0xAB, 0xCD]),
            sourceURL: src,
            validator: HTTPAssetMetadata(etag: "\"A\"", contentLength: 100, lastModified: nil)
        )
        // Server unchanged — same ETag + length.
        await manager.setResumeValidatorProviderForTesting { _ in
            HTTPAssetMetadata(etag: "\"A\"", contentLength: 100, lastModified: nil)
        }

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: episodeId)
        #expect(outcome == .resumed)
        #expect(try await manager.loadResumeData(episodeId: episodeId) == nil)

        await manager.invalidateBackgroundSessionsForTesting()
    }

    @Test("wrj8: a resume is discarded when a complete pinned artifact already exists")
    func resumeDiscardedWhenAlreadyComplete() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-already-complete"
        let completeURL = await manager.completeFileURL(for: episodeId)
        let played = Data(repeating: 0xEE, count: 1024)
        try played.write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(expectedBytes: 1024, sha256: nil, sourceURL: nil, etag: nil),
            for: episodeId
        )

        try await manager.persistResumeData(
            episodeId: episodeId,
            data: Data([0x01]),
            sourceURL: URL(string: "https://dai.example.com/ep.mp3")!,
            validator: HTTPAssetMetadata(etag: "\"A\"", contentLength: 100, lastModified: nil)
        )

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: episodeId)
        #expect(outcome == .alreadyComplete)
        #expect(try await manager.loadResumeData(episodeId: episodeId) == nil)
        // The played artifact is untouched.
        #expect(try Data(contentsOf: completeURL) == played)
    }
}

// MARK: - WorkJournalRecording protocol — preempted default

@Suite("WorkJournalRecording.recordPreempted")
struct WorkJournalRecordingPreemptedDefaultTests {

    @Test("NoopWorkJournalRecorder swallows preempted like other events")
    func noopDoesNotThrow() async {
        let recorder = NoopWorkJournalRecorder()
        await recorder.recordPreempted(
            episodeId: "ep-noop",
            cause: .appForceQuitRequiresRelaunch,
            metadataJSON: "{}"
        )
        // No assertion — the contract is that this does not throw or crash.
    }
}

// MARK: - Delegate resume-data harvest (playhead-g2wq)

@Suite("EpisodeDownloadDelegate – resume-data harvest (playhead-g2wq)")
struct EpisodeDownloadDelegateResumeHarvestTests {

    /// Minimal URLSessionTask double exposing `taskDescription`. Cannot
    /// instantiate a real URLSessionTask without a session; subclass is
    /// adequate because the delegate only reads `taskDescription` and
    /// `countOfBytesReceived` in the didCompleteWithError path.
    private final class G2wqStubTask: URLSessionTask, @unchecked Sendable {
        private let _taskDescription: String?
        init(taskDescription: String?) {
            self._taskDescription = taskDescription
            super.init()
        }
        override var taskDescription: String? {
            get { _taskDescription }
            set { /* immutable stub */ }
        }
    }

    @Test("didCompleteWithError harvests NSURLSessionDownloadTaskResumeData and writes it to resumeDataDirectory",
          .timeLimit(.minutes(1)))
    func harvestsResumeDataIntoResumeDirectory() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Real manager so the delegate's onResumeDataHarvested callback
        // is wired to the real `persistResumeData` actor path. This is
        // the whole point of the bead — we must hit the directory write
        // so we catch any future directory-path regression.
        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let delegate = await manager.sessionDelegateForTesting()

        let resumeBlob = Data([0x01, 0x02, 0x03, 0xAB, 0xCD, 0xEF])
        let cancelError = NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorCancelled,
            userInfo: [
                NSURLSessionDownloadTaskResumeData: resumeBlob,
                NSLocalizedDescriptionKey: "cancelled",
            ]
        )

        // playhead-vsot round 3: event-driven instead of a 2 s
        // `pollUntil` deadline (the same short-wall-clock class that
        // flaked the interruption/route-change/scan families under the
        // parallel gate — the harvest's persist Task can be starved past
        // 2 s). Chain the delegate's `onResumeDataHarvested`: the
        // production closure (wired in `DownloadManager.init`) still runs
        // and persists — proving the init wiring — and our wrapper awaits
        // an idempotent persist round-trip so it can signal true
        // completion. `persistResumeData` overwrites with identical
        // bytes, so the double write is a no-op on the observable state.
        // No deadline; the `.timeLimit` trait is the hang backstop.
        let persisted = TestEventCounter()
        let productionHarvest = delegate.onResumeDataHarvested
        // playhead-wrj8: closure widened to carry the source URL + HTTP
        // validator harvested off the task; forward them through.
        delegate.onResumeDataHarvested = { episodeId, data, sourceURL, metadata in
            productionHarvest?(episodeId, data, sourceURL, metadata)
            Task {
                try? await manager.persistResumeData(
                    episodeId: episodeId, data: data, sourceURL: sourceURL, validator: metadata
                )
                persisted.increment()
            }
        }

        let task = G2wqStubTask(taskDescription: "ep-g2wq-harvest")
        delegate.urlSession(URLSession.shared, task: task, didCompleteWithError: cancelError)

        await persisted.wait(for: 1)

        let loaded = try await manager.loadResumeData(episodeId: "ep-g2wq-harvest")
        #expect(loaded == resumeBlob)

        // Belt-and-suspenders: the scan enumerator should now list the
        // harvested episode, proving the index file was written too.
        let ids = await manager.persistedResumeDataEpisodeIds()
        #expect(ids.contains("ep-g2wq-harvest"))
    }

    @Test("didCompleteWithError skips harvest when NSURLSessionDownloadTaskResumeData is absent")
    func skipsHarvestWhenResumeDataAbsent() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let delegate = await manager.sessionDelegateForTesting()

        // Error WITHOUT NSURLSessionDownloadTaskResumeData in userInfo —
        // e.g. DNS failure, server-side 5xx, unrecoverable transport
        // error. The harvest path must NOT write anything to the
        // resume-data directory in this case.
        let nonResumableError = NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorCannotFindHost,
            userInfo: [
                NSLocalizedDescriptionKey: "cannot find host",
            ]
        )

        let task = G2wqStubTask(taskDescription: "ep-g2wq-no-blob")
        delegate.urlSession(URLSession.shared, task: task, didCompleteWithError: nonResumableError)

        // Give any spurious async harvest Task a chance to run (it
        // shouldn't exist), then assert nothing was persisted.
        try await Task.sleep(for: .milliseconds(200))

        let ids = await manager.persistedResumeDataEpisodeIds()
        #expect(ids.isEmpty, "Resume-data directory must remain empty when error carries no NSURLSessionDownloadTaskResumeData blob")

        let loaded = try await manager.loadResumeData(episodeId: "ep-g2wq-no-blob")
        #expect(loaded == nil)
    }
}

// MARK: - App launch wiring

@MainActor
@Suite("PlayheadAppDelegate – scanForSuspendedTransfers wiring")
struct PlayheadAppDelegateScanWiringTests {

    @Test("didFinishLaunching triggers scanForSuspendedTransfers on the registered manager",
          .timeLimit(.minutes(1)))
    func launchTriggersScan() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        try await manager.persistResumeData(episodeId: "ep-launch", data: Data([0x01, 0x02, 0x03]))

        DownloadManager.registerShared(manager)
        defer { DownloadManager.registerShared(nil) }

        let delegate = PlayheadAppDelegate()
        _ = delegate.application(
            UIApplication.shared,
            didFinishLaunchingWithOptions: nil
        )

        // The scan runs on an async Task the delegate spawns. Await the
        // ACTUAL signal — the recorder's preempted emission — instead of
        // polling under a 2 s deadline that expires under full-suite
        // load before that Task is even scheduled (playhead-vsot round
        // 2; failed the 2026-07-20 parallel gate at ~84 s wall).
        await recorder.awaitPreempted(episodeId: "ep-launch")
        let preempted = await recorder.preempted
        #expect(preempted.contains { $0.episodeId == "ep-launch" })
    }
}
