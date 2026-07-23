// DownloadManagerTests.swift
// Unit tests for the audio asset cache and download manager.

import BackgroundTasks
import Foundation
import Dispatch
import CryptoKit
import Testing
import UIKit
@testable import Playhead

// MARK: - Bootstrap & Directory Structure

@Suite("DownloadManager – Setup")
struct DownloadManagerSetupTests {

    // Uses shared makeTempDir() from TestHelpers.swift

    @Test("Bootstrap creates required directories")
    func bootstrap() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let fm = FileManager.default
        let partialsDir = dir.appendingPathComponent("partials")
        let completeDir = dir.appendingPathComponent("complete")
        #expect(fm.fileExists(atPath: partialsDir.path))
        #expect(fm.fileExists(atPath: completeDir.path))
    }
}

// MARK: - Cache Operations

@Suite("DownloadManager – Cache")
struct DownloadManagerCacheTests {

    // Uses shared makeTempDir() from TestHelpers.swift

    @Test("isCached returns false for uncached episode")
    func notCached() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let cached = await manager.isCached(episodeId: "nonexistent")
        #expect(!cached)
    }

    @Test("isCached returns true after file placed in complete dir")
    func isCachedAfterManualPlace() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        // Manually place a file.
        let completeURL = await manager.completeFileURL(for: "test-ep")
        try Data("fake audio".utf8).write(to: completeURL)

        let cached = await manager.isCached(episodeId: "test-ep")
        #expect(cached)
    }

    @Test("cachedFileURL returns URL for cached file")
    func cachedFileURL() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let completeURL = await manager.completeFileURL(for: "test-ep")
        try Data("fake audio".utf8).write(to: completeURL)

        let result = await manager.cachedFileURL(for: "test-ep")
        #expect(result != nil)
        #expect(result == completeURL)
    }

    @Test("cachedFileURL returns nil for uncached episode")
    func cachedFileURLMissing() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let result = await manager.cachedFileURL(for: "nonexistent")
        #expect(result == nil)
    }

    @Test("cachedEpisodeIds matching detects manually placed complete files")
    func cachedEpisodeIdsMatchingDetectsCompleteFiles() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let cachedURL = await manager.completeFileURL(for: "ep-cached")
        try Data("fake audio".utf8).write(to: cachedURL)
        let strayURL = await manager.completeFileURL(for: "ep-stray")
        try Data("not audio".utf8).write(to: strayURL.deletingPathExtension().appendingPathExtension("txt"))

        let cached = await manager.cachedEpisodeIds(matching: [
            "ep-cached",
            "ep-stray",
            "ep-missing"
        ])

        #expect(cached == ["ep-cached"])
    }

    @Test("removeCache deletes both partial and complete files")
    func removeCache() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let completeURL = await manager.completeFileURL(for: "test-ep")
        let partialURL = await manager.partialFileURL(for: "test-ep")
        try Data("complete".utf8).write(to: completeURL)
        try Data("partial".utf8).write(to: partialURL)

        try await manager.removeCache(for: "test-ep")

        let fm = FileManager.default
        #expect(!fm.fileExists(atPath: completeURL.path))
        #expect(!fm.fileExists(atPath: partialURL.path))
    }

    @Test("clearCache removes all cached files")
    func clearCache() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        for i in 0..<3 {
            let url = await manager.completeFileURL(for: "ep-\(i)")
            try Data("audio-\(i)".utf8).write(to: url)
        }

        try await manager.clearCache()

        let size = try await manager.currentCacheSize()
        #expect(size == 0)
    }
}

// MARK: - Eviction

@Suite("DownloadManager – Eviction")
struct DownloadManagerEvictionTests {

    // Uses shared makeTempDir() from TestHelpers.swift

    @Test("Eviction skips analysis-protected episodes")
    func evictionProtectsAnalysis() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // 100 bytes max cache.
        let manager = DownloadManager(cacheDirectory: dir, maxCacheBytes: 100)
        try await manager.bootstrap()

        // Place two files, each 60 bytes.
        let data = Data(repeating: 0x42, count: 60)
        let url1 = await manager.completeFileURL(for: "protected-ep")
        let url2 = await manager.completeFileURL(for: "unprotected-ep")
        try data.write(to: url1)
        try data.write(to: url2)

        // Protect one episode.
        await manager.protectForAnalysis(episodeId: "protected-ep")

        try await manager.evictIfNeeded()

        let fm = FileManager.default
        #expect(fm.fileExists(atPath: url1.path)) // Protected: kept
        #expect(!fm.fileExists(atPath: url2.path)) // Unprotected: evicted
    }
}

// MARK: - Fingerprinting

@Suite("DownloadManager – Fingerprinting")
struct DownloadManagerFingerprintTests {

    @Test("Weak fingerprint combines URL and HTTP metadata")
    func weakFingerprint() {
        let url = URL(string: "https://example.com/ep1.mp3")!
        let metadata = HTTPAssetMetadata(
            etag: "\"abc123\"",
            contentLength: 12345678,
            lastModified: "Mon, 01 Jan 2024 00:00:00 GMT"
        )
        let weak = AudioFingerprint.makeWeak(url: url, metadata: metadata)
        #expect(weak.contains("https://example.com/ep1.mp3"))
        #expect(weak.contains("abc123"))
        #expect(weak.contains("12345678"))
        #expect(weak.contains("Mon, 01 Jan 2024"))
    }

    @Test("Weak fingerprint handles missing metadata gracefully")
    func weakFingerprintMissingMeta() {
        let url = URL(string: "https://example.com/ep.mp3")!
        let metadata = HTTPAssetMetadata(etag: nil, contentLength: nil, lastModified: nil)
        let weak = AudioFingerprint.makeWeak(url: url, metadata: metadata)
        #expect(weak.contains("https://example.com/ep.mp3"))
        // Should not crash, just have empty segments.
        #expect(!weak.isEmpty)
    }

    @Test("AudioFingerprint equality")
    func fingerprintEquality() {
        let a = AudioFingerprint(weak: "abc", strong: "def")
        let b = AudioFingerprint(weak: "abc", strong: "def")
        let c = AudioFingerprint(weak: "abc", strong: "ghi")
        #expect(a == b)
        #expect(a != c)
    }
}

// MARK: - Integrity

@Suite("DownloadManager – Integrity")
struct DownloadManagerIntegrityTests {

    // Uses shared makeTempDir() from TestHelpers.swift

    @Test("verifyIntegrity returns true for matching hash")
    func integrityMatch() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let content = Data("test audio content".utf8)
        let completeURL = await manager.completeFileURL(for: "hash-ep")
        try content.write(to: completeURL)

        // Compute expected hash.
        let fp = try await manager.computeStrongFingerprint(
            episodeId: "hash-ep",
            url: URL(string: "https://example.com/ep.mp3")!
        )
        guard let strongHash = fp?.strong else {
            Issue.record("Expected strong fingerprint")
            return
        }

        let valid = try await manager.verifyIntegrity(
            episodeId: "hash-ep", expectedHash: strongHash
        )
        #expect(valid)
    }

    @Test("verifyIntegrity returns false for wrong hash")
    func integrityMismatch() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let completeURL = await manager.completeFileURL(for: "hash-ep")
        try Data("content".utf8).write(to: completeURL)

        let valid = try await manager.verifyIntegrity(
            episodeId: "hash-ep", expectedHash: "0000000000000000"
        )
        #expect(!valid)
    }

    @Test("verifyIntegrity throws for missing file")
    func integrityMissing() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        await #expect(throws: DownloadManagerError.self) {
            try await manager.verifyIntegrity(
                episodeId: "nonexistent", expectedHash: "abc"
            )
        }
    }
}


// MARK: - Foreground Assist Handoff (playhead-44h1 fix)

/// Captures `BGTaskRequest` submissions so tests can assert the
/// willResignActive path submitted a `BGContinuedProcessingTaskRequest`
/// with the expected wildcard identifier.
final class CapturingTaskScheduler: BackgroundTaskScheduling, @unchecked Sendable {
    var submitted: [BGTaskRequest] = []
    var shouldThrow: Error?
    /// playhead-vsot round 3: fires on every successful submit so tests
    /// can await the fire-and-forget observer→Task→submit chain instead
    /// of polling `submitted` under a wall-clock deadline.
    let submittedSignal = TestEventCounter()

    func submit(_ taskRequest: BGTaskRequest) throws {
        if let shouldThrow { throw shouldThrow }
        submitted.append(taskRequest)
        submittedSignal.increment()
    }

    func pendingTaskRequestIdentifiers() async -> [String] {
        submitted.map(\.identifier)
    }
}

@Suite("DownloadManager – Foreground-assist handoff (playhead-44h1)")
struct DownloadManagerForegroundAssistHandoffTests {

    @Test("noteTransferProgress records latest bytes for snapshot construction")
    func noteTransferProgressUpdatesSlot() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)

        let episodeId = "ep-44h1-progress"
        await manager.noteTransferProgress(DownloadProgress(
            episodeId: episodeId, bytesWritten: 1_000_000, totalBytes: 10_000_000
        ))
        let progress = await manager.foregroundAssistProgressForTesting(episodeId: episodeId)
        #expect(progress?.bytesWritten == 1_000_000)
        #expect(progress?.totalBytes == 10_000_000)
    }

    @Test("Completed transfer clears the foreground-assist progress slot")
    func completedTransferClearsSlot() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)
        let episodeId = "ep-44h1-done"

        await manager.noteTransferProgress(DownloadProgress(
            episodeId: episodeId, bytesWritten: 500_000, totalBytes: 10_000_000
        ))
        await manager.noteTransferProgress(DownloadProgress(
            episodeId: episodeId,
            bytesWritten: 10_000_000,
            totalBytes: 10_000_000
        ))
        let progress = await manager.foregroundAssistProgressForTesting(episodeId: episodeId)
        #expect(progress == nil,
                "A complete progress event must clear the foreground-assist slot")
    }

    // `BGContinuedProcessingTaskRequest` is iOS-26-only and unavailable
    // in Mac Catalyst. Production (`DownloadManager.submitContinuedProcessing`)
    // returns early on Catalyst, so this iOS-only behavior cannot be
    // exercised there. Guarding the whole method keeps the test target
    // compiling for Catalyst (needed by the env-gated ChapterPlan
    // snapshot capture, which runs as a Catalyst process).
    #if !targetEnvironment(macCatalyst)
    @Test("willResignActive submits BGContinuedProcessingTaskRequest when transfer is far from done")
    func willResignActiveSubmitsBGRequest() async throws {
        // Transfer is only 10% complete at 50 KB/s throughput → ETA
        // 180 s → both the 80% gate (0.1 < 0.8) AND the 2-min gate
        // (180 > 120) fail → submit a BGContinuedProcessingTaskRequest.
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)
        let scheduler = CapturingTaskScheduler()
        await manager.setBackgroundTaskSchedulerForTesting(scheduler)

        let episodeId = "ep-44h1-faraway"
        // 10 MB total, 1 MB written over 20 s → 50 KB/s throughput,
        // 9 MB remaining → 180 s ETA.
        let startedAt = Date(timeIntervalSinceNow: -20)
        await manager.seedForegroundAssistProgressForTesting(
            episodeId: episodeId,
            bytesWritten: 1_000_000,
            totalBytes: 10_000_000,
            firstObservedAt: startedAt,
            firstObservedBytes: 0
        )

        let decisions = await manager.handleWillResignActive()
        #expect(decisions.contains(.submitContinuedProcessingRequest))
        #expect(scheduler.submitted.count == 1)
        let identifier = scheduler.submitted.first?.identifier ?? ""
        #expect(identifier.hasPrefix(BackgroundTaskID.continuedProcessing + "."),
                "Submitted identifier must follow the wildcard convention")
        #expect(identifier.hasSuffix("." + episodeId),
                "Submitted identifier must end with the episode id suffix")
        #expect(scheduler.submitted.first is BGContinuedProcessingTaskRequest,
                "Submitted request must be a BGContinuedProcessingTaskRequest")
    }
    #endif

    @Test("willResignActive keeps foreground-assist alive when transfer is near done")
    func willResignActiveKeepsAliveOnHighFraction() async throws {
        // Transfer 90% complete → keep-alive branch → NO BG task
        // request submitted. Logs the decision but does not act.
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)
        let scheduler = CapturingTaskScheduler()
        await manager.setBackgroundTaskSchedulerForTesting(scheduler)

        let episodeId = "ep-44h1-nearly"
        await manager.seedForegroundAssistProgressForTesting(
            episodeId: episodeId,
            bytesWritten: 9_000_000,
            totalBytes: 10_000_000,
            firstObservedAt: Date(timeIntervalSinceNow: -30),
            firstObservedBytes: 0
        )

        let decisions = await manager.handleWillResignActive()
        #expect(decisions.contains(.keepForegroundAssistAlive))
        #expect(scheduler.submitted.isEmpty,
                "Keep-alive branch MUST NOT submit a BG task request")
    }

    @Test("No active transfers → no BG task submission on willResignActive")
    func willResignActiveNoopsWhenNoTransfers() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)
        let scheduler = CapturingTaskScheduler()
        await manager.setBackgroundTaskSchedulerForTesting(scheduler)

        let decisions = await manager.handleWillResignActive()
        #expect(decisions.isEmpty)
        #expect(scheduler.submitted.isEmpty)
    }

    @Test("Posted willResignActive notification drives a real BG task submission end-to-end",
          .timeLimit(.minutes(1)))
    @MainActor
    func postedWillResignActiveDrivesSubmission() async throws {
        // Review-fix Blocker 1: registerForegroundAssistLifecycleObserver
        // must install a notification observer that routes a posted
        // willResignActive into handleWillResignActive without any test
        // hook calling that method directly. Seed progress far enough
        // from done that the decision is submitBG, post the notification,
        // and wait for the scheduler to observe the submission.
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)
        let scheduler = CapturingTaskScheduler()
        await manager.setBackgroundTaskSchedulerForTesting(scheduler)

        let episodeId = "ep-44h1-notif"
        await manager.seedForegroundAssistProgressForTesting(
            episodeId: episodeId,
            bytesWritten: 1_000_000,
            totalBytes: 10_000_000,
            firstObservedAt: Date(timeIntervalSinceNow: -20),
            firstObservedBytes: 0
        )
        await manager.registerForegroundAssistLifecycleObserver()
        defer {
            Task { await manager.deregisterForegroundAssistLifecycleObserver() }
        }

        NotificationCenter.default.post(
            name: UIApplication.willResignActiveNotification,
            object: nil
        )

        // The observer hops into a Task; await the submit signal
        // (playhead-vsot round 3) rather than polling under a 5 s
        // deadline that can starve under the parallel gate.
        await scheduler.submittedSignal.wait(for: 1)

        #expect(!scheduler.submitted.isEmpty,
                "Posted willResignActive must route through the registered observer to the scheduler")
        let identifier = scheduler.submitted.first?.identifier ?? ""
        #expect(identifier.hasSuffix("." + episodeId),
                "Observer-driven submission must use the episode-id-suffixed identifier")
    }
}

// MARK: - Progress Snapshot (playhead-btoa.2)

@Suite("DownloadManager – progressSnapshot")
struct DownloadManagerProgressSnapshotTests {

    // Uses shared makeTempDir() from TestHelpers.swift

    @Test("Empty foreground-assist state → empty snapshot")
    func emptySnapshot() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)

        let snapshot = await manager.progressSnapshot()
        #expect(snapshot.isEmpty,
                "No active downloads must yield an empty snapshot map")
    }

    @Test("Single in-flight transfer → snapshot reports its fraction")
    func singleInFlightFraction() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)

        let episodeId = "ep-btoa-half"
        await manager.seedForegroundAssistProgressForTesting(
            episodeId: episodeId,
            bytesWritten: 50,
            totalBytes: 100,
            firstObservedAt: Date(timeIntervalSinceNow: -1),
            firstObservedBytes: 0
        )

        let snapshot = await manager.progressSnapshot()
        #expect(snapshot.count == 1)
        #expect(snapshot[episodeId] == 0.5)
    }

    @Test("totalBytes == 0 entries are skipped (no divide-by-zero)")
    func zeroTotalBytesSkipped() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)

        let episodeId = "ep-btoa-unknown-total"
        await manager.seedForegroundAssistProgressForTesting(
            episodeId: episodeId,
            bytesWritten: 0,
            totalBytes: 0,
            firstObservedAt: Date(timeIntervalSinceNow: -1),
            firstObservedBytes: 0
        )

        let snapshot = await manager.progressSnapshot()
        #expect(snapshot[episodeId] == nil,
                "Unknown total (totalBytes == 0) must not appear in the snapshot")
        #expect(snapshot.isEmpty,
                "Only entry was the zero-total stub, so snapshot must be empty")
    }

    @Test("Two in-flight transfers each appear with their own fraction")
    func twoInFlightFractions() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)

        let firstId = "ep-btoa-quarter"
        let secondId = "ep-btoa-three-quarters"
        await manager.seedForegroundAssistProgressForTesting(
            episodeId: firstId,
            bytesWritten: 25,
            totalBytes: 100,
            firstObservedAt: Date(timeIntervalSinceNow: -2),
            firstObservedBytes: 0
        )
        await manager.seedForegroundAssistProgressForTesting(
            episodeId: secondId,
            bytesWritten: 750,
            totalBytes: 1000,
            firstObservedAt: Date(timeIntervalSinceNow: -2),
            firstObservedBytes: 0
        )

        let snapshot = await manager.progressSnapshot()
        #expect(snapshot.count == 2)
        #expect(snapshot[firstId] == 0.25)
        #expect(snapshot[secondId] == 0.75)
    }
}

// MARK: - Immutable-artifact invariant (playhead-wrj8)

/// The bytes PLAYED == ANALYZED == MARKED-AGAINST must be one immutable
/// artifact for the life of a downloaded episode. On a DAI show the
/// enclosure re-cuts a different ad stitch per request, so any path that
/// silently overwrites / serves-a-partial rotates the audio the user marked
/// ads against. These tests lock the completeness pin + overwrite refusal.
@Suite("DownloadManager – immutable artifact (playhead-wrj8)")
struct DownloadManagerImmutableArtifactTests {

    @Test("A truncated (under-length) pinned file is NOT served as cached")
    func truncatedFileWithheld() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-trunc"
        let completeURL = await manager.completeFileURL(for: episodeId)
        // Only 1000 of an expected 5000 bytes are on disk.
        try Data(repeating: 0xC3, count: 1000).write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(expectedBytes: 5000, sha256: nil, sourceURL: nil, etag: nil),
            for: episodeId
        )

        #expect(await manager.cachedFileURL(for: episodeId) == nil)
        #expect(await manager.isCached(episodeId: episodeId) == false)

        // Once the file reaches the pinned length it serves.
        try Data(repeating: 0xC3, count: 5000).write(to: completeURL)
        #expect(await manager.cachedFileURL(for: episodeId) == completeURL)
        #expect(await manager.isCached(episodeId: episodeId))
    }

    @Test("A legacy bare file with no pin is served (non-destructive migration)")
    func legacyBareFileServed() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let completeURL = await manager.completeFileURL(for: "ep-legacy")
        try Data(repeating: 0xD4, count: 2048).write(to: completeURL)

        #expect(await manager.cachedFileURL(for: "ep-legacy") == completeURL)
        #expect(await manager.isCached(episodeId: "ep-legacy"))
    }

    @Test("A complete pinned artifact is NOT overwritten by a later background completion (different stitch)")
    func pinnedArtifactSurvivesBackgroundCompletion() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-pin-immutable"
        let completeURL = await manager.completeFileURL(for: episodeId)

        // Stitch A: the bytes the user played + marked ads against.
        let stitchA = Data(repeating: 0xA1, count: 4096)
        try stitchA.write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(stitchA.count),
                sha256: nil,
                sourceURL: "https://dai.example.com/ep.mp3",
                etag: "\"A\""
            ),
            for: episodeId
        )

        // A background transfer completes later with a DIFFERENT stitch
        // (different length + content) — the DAI rotation vector.
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStagingWrj8A", isDirectory: true)
        try FileManager.default.createDirectory(at: stagingDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: stagingDir) }
        let staged = stagingDir.appendingPathComponent(
            "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        )
        let stitchB = Data(repeating: 0xB2, count: 2048)
        try stitchB.write(to: staged)

        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: staged,
            originalURL: URL(string: "https://dai.example.com/ep.mp3"),
            metadata: HTTPAssetMetadata(etag: "\"B\"", contentLength: 2048, lastModified: nil)
        )

        // The played bytes are untouched, and the rotated deposit was discarded.
        let onDisk = try Data(contentsOf: completeURL)
        #expect(onDisk == stitchA, "played artifact must not be overwritten by a rotated re-fetch")
        #expect(!FileManager.default.fileExists(atPath: staged.path), "staged rotated copy must be discarded")
        #expect(await manager.cachedFileURL(for: episodeId) == completeURL)
    }

    @Test("Eviction protection is refcounted — one release does not unprotect a doubly-held episode")
    func protectionRefcounted() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // 100-byte budget with two 60-byte files (120 total) forces one eviction.
        let manager = DownloadManager(cacheDirectory: dir, maxCacheBytes: 100)
        try await manager.bootstrap()

        let data = Data(repeating: 0x42, count: 60)
        let heldURL = await manager.completeFileURL(for: "held")
        let freeURL = await manager.completeFileURL(for: "free")
        try data.write(to: heldURL)
        try data.write(to: freeURL)

        // Two overlapping owners (playback + analysis) protect "held".
        await manager.protectForAnalysis(episodeId: "held")
        await manager.protectForAnalysis(episodeId: "held")
        // Analysis finishes and releases once — playback still holds it.
        await manager.unprotectFromAnalysis(episodeId: "held")

        try await manager.evictIfNeeded()

        let fm = FileManager.default
        #expect(fm.fileExists(atPath: heldURL.path), "still-held episode must survive eviction")
        #expect(!fm.fileExists(atPath: freeURL.path), "unprotected episode is the eviction victim")

        // Release the last holder → now unprotected.
        await manager.unprotectFromAnalysis(episodeId: "held")
        #expect(await manager.protectedEpisodeIdsForTesting().isEmpty)
    }
}
