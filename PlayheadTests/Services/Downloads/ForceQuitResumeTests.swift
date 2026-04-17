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
    }

    private(set) var preempted: [Preempted] = []
    private(set) var failures: [Failure] = []
    private(set) var finalized: [String] = []

    func recordFinalized(episodeId: String) async {
        finalized.append(episodeId)
    }

    func recordFailed(episodeId: String, cause: InternalMissCause) async {
        failures.append(Failure(episodeId: episodeId, cause: cause))
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
        try await manager.persistResumeDataForTesting(episodeId: "ep-hyht-1", data: blob)

        let loaded = try await manager.loadResumeDataForTesting(episodeId: "ep-hyht-1")
        #expect(loaded == blob)
    }

    @Test("persistResumeData overwrites a prior blob for the same episode")
    func persistOverwrites() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        try await manager.persistResumeDataForTesting(episodeId: "ep-hyht-2", data: Data([0x01]))
        try await manager.persistResumeDataForTesting(episodeId: "ep-hyht-2", data: Data([0x02, 0x03]))

        let loaded = try await manager.loadResumeDataForTesting(episodeId: "ep-hyht-2")
        #expect(loaded == Data([0x02, 0x03]))
    }

    @Test("loadResumeData returns nil when no blob is persisted")
    func loadMissingReturnsNil() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let loaded = try await manager.loadResumeDataForTesting(episodeId: "ep-missing")
        #expect(loaded == nil)
    }

    @Test("persistedResumeDataEpisodeIds enumerates stored blobs")
    func enumeration() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        try await manager.persistResumeDataForTesting(episodeId: "ep-a", data: Data([0xAA]))
        try await manager.persistResumeDataForTesting(episodeId: "ep-b", data: Data([0xBB]))

        let ids = await manager.persistedResumeDataEpisodeIdsForTesting()
        #expect(ids == Set(["ep-a", "ep-b"]))
    }

    @Test("deleteResumeData removes the blob")
    func deleteBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        try await manager.persistResumeDataForTesting(episodeId: "ep-del", data: Data([0x01]))
        try await manager.deleteResumeDataForTesting(episodeId: "ep-del")

        let loaded = try await manager.loadResumeDataForTesting(episodeId: "ep-del")
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
        try await manager.persistResumeDataForTesting(episodeId: "ep-1", data: Data([0x01, 0x02]))
        try await manager.persistResumeDataForTesting(episodeId: "ep-2", data: Data([0x03, 0x04, 0x05]))

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

        try await manager.persistResumeDataForTesting(episodeId: "ep-idem", data: Data([0x01]))

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
            try await manager.persistResumeDataForTesting(
                episodeId: "ep-sla-\(i)",
                data: Data(repeating: UInt8(i), count: 1024)
            )
        }

        let start = ContinuousClock.now
        _ = try await manager.scanForSuspendedTransfers()
        let elapsed = ContinuousClock.now - start

        #expect(elapsed < .seconds(2))
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
        try await manager.persistResumeDataForTesting(episodeId: "ep-corrupt", data: Data())

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
        let loaded = try await manager.loadResumeDataForTesting(episodeId: "ep-corrupt")
        #expect(loaded == nil)
    }

    @Test("Scan tolerates a mix of valid and corrupted blobs")
    func scanMixedBlobs() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        try await manager.persistResumeDataForTesting(episodeId: "ep-good", data: Data([0x01, 0x02, 0x03]))
        try await manager.persistResumeDataForTesting(episodeId: "ep-bad", data: Data())

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
        try await manager.persistResumeDataForTesting(episodeId: "ep-res", data: Data([0xAB, 0xCD]))

        let outcome = try await manager.resumeSuspendedTransferForTesting(episodeId: "ep-res")
        #expect(outcome == .resumed)

        // On success the blob is removed — the OS now owns continuation.
        let loaded = try await manager.loadResumeDataForTesting(episodeId: "ep-res")
        #expect(loaded == nil)
    }

    @Test("resumeSuspendedTransfer returns .missing when no blob is persisted")
    func resumeMissingBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let outcome = try await manager.resumeSuspendedTransferForTesting(episodeId: "ep-none")
        #expect(outcome == .missing)
    }

    @Test("resumeSuspendedTransfer rejects corrupted blob and deletes it")
    func resumeCorruptedBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let recorder = HyhtRecorder()
        let manager = DownloadManager(cacheDirectory: dir, workJournalRecorder: recorder)
        try await manager.bootstrap()

        try await manager.persistResumeDataForTesting(episodeId: "ep-corrupt2", data: Data())

        let outcome = try await manager.resumeSuspendedTransferForTesting(episodeId: "ep-corrupt2")
        #expect(outcome == .corrupted)

        // Blob is purged so the user sees a clean-restart error once, not
        // forever.
        let loaded = try await manager.loadResumeDataForTesting(episodeId: "ep-corrupt2")
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

        try await manager.persistResumeDataForTesting(episodeId: "ep-launch", data: Data([0x01, 0x02, 0x03]))

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
