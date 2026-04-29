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

    @Test("Scan completes within the 2-second SLA specified by the bead")
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

        // Cooperative-pool jitter under the parallel test plan
        // (3000+ tests racing) routinely inflates wall-clock latency
        // for an operation that takes ~12 ms in isolation — see
        // commits 11ed665 / 26bca6f for the same pattern. Use median
        // of 3 runs so a single starved sample doesn't fail the test;
        // production SLA (2 s on cold launch) is unchanged.
        var samples: [Duration] = []
        for _ in 0..<3 {
            let start = ContinuousClock.now
            _ = try await manager.scanForSuspendedTransfers()
            samples.append(ContinuousClock.now - start)
        }
        let median = samples.sorted()[1]
        #expect(median < .seconds(2))
        // Hang ceiling: any sample blowing past 30 s indicates a real
        // regression (deadlock, infinite loop) rather than scheduler
        // jitter — surface as a failure so we don't paper over it.
        #expect(samples.allSatisfy { $0 < .seconds(30) })
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

    @Test("didCompleteWithError harvests NSURLSessionDownloadTaskResumeData and writes it to resumeDataDirectory")
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

        let task = G2wqStubTask(taskDescription: "ep-g2wq-harvest")
        delegate.urlSession(URLSession.shared, task: task, didCompleteWithError: cancelError)

        // The harvest routes through an actor hop; poll until the blob
        // lands on disk or we give up.
        let sawBlob = await pollUntil(timeout: .seconds(2)) {
            let loaded = try? await manager.loadResumeData(episodeId: "ep-g2wq-harvest")
            return loaded == resumeBlob
        }
        #expect(sawBlob)

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

    @Test("didFinishLaunching triggers scanForSuspendedTransfers on the registered manager")
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

        // Scan runs async; poll for the preempted emission.
        let sawPreempted = await pollUntil(timeout: .seconds(2)) {
            await recorder.preempted.contains { $0.episodeId == "ep-launch" }
        }
        #expect(sawPreempted)
    }
}
