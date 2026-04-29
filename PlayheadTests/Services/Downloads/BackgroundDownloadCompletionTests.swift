// BackgroundDownloadCompletionTests.swift
// playhead-24cm.1: regression coverage for the
// `EpisodeDownloadDelegate` → `DownloadManager` handoff.
//
// I3: file placement must honor `DownloadManager.cacheDirectory` (the
//     pre-fix delegate hardcoded `defaultCacheDirectory()`, which broke
//     custom cache dirs in tests and any future multi-profile host).
// I4: the delegate must populate a non-empty weak fingerprint —
//     previously it injected `AudioFingerprint(weak: "", strong: ...)`
//     which polluted downstream weak-fingerprint dedup.
//
// We drive `handleBackgroundDownloadComplete` directly because the real
// background-session callback requires a live `URLSessionDownloadTask`
// that we cannot construct without a session. The actor method is the
// load-bearing part of both fixes.

import Foundation
import Testing
@testable import Playhead

@Suite("DownloadManager – background completion (playhead-24cm.1)")
struct BackgroundDownloadCompletionTests {

    /// I3: the actor must place the final file inside the injected
    /// `cacheDirectory`, not the process-wide default cache dir.
    @Test("File placement honors custom cacheDirectory")
    func filePlacementHonorsCacheDirectory() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-24cm-1-i3"
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStagingI3", isDirectory: true)
        try FileManager.default.createDirectory(
            at: stagingDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: stagingDir) }

        let stagedFile = stagingDir.appendingPathComponent(
            "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        )
        try Data("fake mp3 bytes".utf8).write(to: stagedFile)

        let originalURL = URL(string: "https://example.com/episode-i3.mp3")
        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: stagedFile,
            originalURL: originalURL,
            metadata: HTTPAssetMetadata(
                etag: "\"i3-etag\"",
                contentLength: 13,
                lastModified: "Wed, 17 Apr 2026 00:00:00 GMT"
            )
        )

        // Final file lives in the custom cache dir, not the default.
        let expectedURL = await manager.completeFileURL(for: episodeId)
        let fm = FileManager.default
        #expect(fm.fileExists(atPath: expectedURL.path))
        #expect(expectedURL.path.hasPrefix(dir.path),
                "Expected final URL \(expectedURL.path) to live under custom cache dir \(dir.path)")

        let defaultDir = DownloadManager.defaultCacheDirectory()
        #expect(!expectedURL.path.hasPrefix(defaultDir.path),
                "Final URL must NOT land in defaultCacheDirectory() when a custom one is injected")

        // Staged file consumed.
        #expect(!fm.fileExists(atPath: stagedFile.path))

        // Cached lookup succeeds via the actor-side accessor.
        let cached = await manager.cachedFileURL(for: episodeId)
        #expect(cached == expectedURL)
    }

    /// I4: the resulting fingerprint must carry a non-empty weak,
    /// synthesized from URL + HTTP metadata exactly as the progressive
    /// path does.
    @Test("Weak fingerprint matches AudioFingerprint.makeWeak from URL + HTTP metadata")
    func weakFingerprintIsNonEmptyAndMatchesProgressivePath() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-24cm-1-i4"
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStagingI4", isDirectory: true)
        try FileManager.default.createDirectory(
            at: stagingDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: stagingDir) }

        let stagedFile = stagingDir.appendingPathComponent(
            "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        )
        try Data("ep i4 audio".utf8).write(to: stagedFile)

        let originalURL = URL(string: "https://example.com/episode-i4.mp3")!
        let metadata = HTTPAssetMetadata(
            etag: "\"i4-etag\"",
            contentLength: 11,
            lastModified: "Thu, 18 Apr 2026 00:00:00 GMT"
        )

        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: stagedFile,
            originalURL: originalURL,
            metadata: metadata
        )

        let fingerprint = await manager.fingerprint(for: episodeId)
        let unwrapped = try #require(fingerprint)

        // Must be non-empty — the pre-fix delegate stamped "" here.
        #expect(!unwrapped.weak.isEmpty,
                "Weak fingerprint must not be the empty sentinel after a background completion")

        // Must match the synthesis the progressive path uses.
        let expectedWeak = AudioFingerprint.makeWeak(url: originalURL, metadata: metadata)
        #expect(unwrapped.weak == expectedWeak)

        // Strong fingerprint is also computed against the placed file.
        #expect(unwrapped.strong != nil)
        #expect(unwrapped.strong?.isEmpty == false)
    }

    /// I4 corollary: a prior progressive pass that already cached a real
    /// weak fingerprint must NOT be regressed when the background-session
    /// callback later fires without HTTP response metadata. The progressive
    /// pass remains the source of truth in that case.
    @Test("Background completion preserves prior weak fingerprint when delegate has no URL")
    func priorWeakFingerprintPreservedWhenNoURL() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-24cm-1-i4-preserve"

        // Simulate a prior progressive pass: write a file directly into
        // the cache and pre-compute the strong fingerprint via the
        // actor's public seam, which also carries a real weak fp.
        let progressiveURL = await manager.completeFileURL(for: episodeId)
        try Data("pre-existing audio".utf8).write(to: progressiveURL)
        let priorURL = URL(string: "https://example.com/preserve.mp3")!
        _ = try await manager.computeStrongFingerprint(
            episodeId: episodeId, url: priorURL
        )

        let priorFp = try #require(await manager.fingerprint(for: episodeId))
        #expect(!priorFp.weak.isEmpty)
        let priorWeak = priorFp.weak

        // Now fire the background completion path WITHOUT a URL — same
        // file content, but the delegate could not harvest metadata.
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStagingI4Preserve", isDirectory: true)
        try FileManager.default.createDirectory(
            at: stagingDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: stagingDir) }
        let stagedFile = stagingDir.appendingPathComponent(
            "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        )
        try Data("pre-existing audio".utf8).write(to: stagedFile)

        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: stagedFile,
            originalURL: nil,
            metadata: nil
        )

        let after = try #require(await manager.fingerprint(for: episodeId))
        #expect(after.weak == priorWeak,
                "Prior weak fingerprint must survive a metadata-less background completion")
        #expect(after.strong != nil)
    }
}
