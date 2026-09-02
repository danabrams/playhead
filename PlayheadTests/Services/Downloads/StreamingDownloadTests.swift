// StreamingDownloadTests.swift
// Tests for streaming download with playable threshold and completion signal.

import Foundation
import Testing
@testable import Playhead

// MARK: - Already Cached

@Suite("StreamingDownload – Cached Files")
struct StreamingDownloadCachedTests {

    @Test("Returns immediately for already-cached episode")
    func cachedEpisode() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let completeURL = await manager.completeFileURL(for: "cached-ep")
        let data = Data(repeating: 0xAA, count: 1024)
        try data.write(to: completeURL)

        let result = try await manager.streamingDownload(
            episodeId: "cached-ep",
            from: URL(string: "https://example.com/ep.mp3")!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )

        #expect(result.fileURL == completeURL)
        #expect(result.contentType == "public.mp3")
        // downloadComplete should be a no-op for cached files.
        try await result.downloadComplete()
    }

    @Test("routing-suffix cache hit advertises neutral audio instead of normalized mp3")
    func routingSuffixCacheHitUsesPinSourceForContentType() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "cached-routing-suffix"
        let completeURL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0xAB, count: 128)
        try bytes.write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: nil,
                sourceURL: "https://example.com/route.php",
                etag: nil
            ),
            for: episodeId
        )

        let result = try await manager.streamingDownload(
            episodeId: episodeId,
            from: URL(string: "https://example.com/route.php")!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )

        #expect(result.fileURL == completeURL)
        #expect(result.contentType == "public.audio")
    }

    @Test("source-less managed pin advertises neutral audio, not its fallback filename")
    func sourceLessManagedPinUsesNeutralContentType() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "cached-source-less-managed-pin"
        let completeURL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0xBC, count: 128)
        try bytes.write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: nil,
                sourceURL: nil,
                etag: nil
            ),
            for: episodeId
        )

        let result = try await manager.streamingDownload(
            episodeId: episodeId,
            from: URL(string: "https://example.com/route.php")!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )

        #expect(result.fileURL == completeURL)
        #expect(result.contentType == "public.audio")
    }

    @Test("Pinned artifact survives a refreshed source extension")
    func pinnedArtifactResolvesAcrossSourceExtension() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "cross-extension-cache-hit"
        let mp3URL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0xAC, count: 256)
        try bytes.write(to: mp3URL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: nil,
                sourceURL: "https://example.com/episode.mp3",
                etag: nil
            ),
            for: episodeId
        )

        let streaming = try await manager.streamingDownload(
            episodeId: episodeId,
            from: URL(string: "https://example.com/episode.m4a")!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )
        #expect(streaming.fileURL == mp3URL)
        #expect(streaming.contentType == "public.mp3")

        let progressive = try await manager.progressiveDownload(
            episodeId: episodeId,
            from: URL(string: "https://example.com/episode.m4a")!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )
        #expect(progressive == mp3URL)

        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://example.com/episode.m4a")!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )
        #expect(
            await manager.backgroundSessionsAlreadyInstantiated().isEmpty,
            "A cross-extension cache hit must not queue a replacement transfer"
        )
        #expect(try Data(contentsOf: mp3URL) == bytes)
        #expect(await manager.cachedFileURL(for: episodeId) == mp3URL)
    }

    @Test("Strong pin selects the right file when extension siblings exist")
    func strongPinDisambiguatesExtensionSiblings() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "multi-extension-pin"

        let staleMP3 = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xBD, count: 800).write(to: staleMP3)

        await manager._setExtensionCacheForTesting(
            episodeId: episodeId,
            fileExtension: "m4a"
        )
        let ownedM4A = await manager.completeFileURL(for: episodeId)
        let ownedBytes = Data(repeating: 0xCE, count: 700)
        try ownedBytes.write(to: ownedM4A)
        let ownedHash = try FileHasher.sha256(fileURL: ownedM4A)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(ownedBytes.count),
                sha256: ownedHash,
                sourceURL: "https://example.com/episode.m4a",
                etag: nil
            ),
            for: episodeId
        )

        // Poison the mutable hint toward the larger stale sibling. The strong
        // pin must still recover the exact finalized m4a bytes.
        await manager._setExtensionCacheForTesting(
            episodeId: episodeId,
            fileExtension: "mp3"
        )

        #expect(await manager.servingURLIfComplete(for: episodeId) == ownedM4A)
        #expect(try Data(contentsOf: ownedM4A) == ownedBytes)
    }

    @Test("weak completeness pins reject oversized managed artifacts")
    func weakPinRequiresExactByteLength() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "oversized-weak-pin"
        let completeURL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xD1, count: 257).write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: 256,
                sha256: nil,
                sourceURL: "https://example.com/episode.mp3",
                etag: nil
            ),
            for: episodeId
        )

        #expect(await manager.servingURLIfComplete(for: episodeId) == nil)
        #expect(await manager.cachedFileURL(for: episodeId) == nil)
    }

    @Test("routing suffix resolves a weak pin through its canonical cache extension")
    func weakPinRoutingSuffixSelectsCanonicalSibling() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "weak-pin-routing-siblings"
        let mp3URL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0xCD, count: 192)
        try bytes.write(to: mp3URL)
        await manager._setExtensionCacheForTesting(
            episodeId: episodeId,
            fileExtension: "m4a"
        )
        let m4aURL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xDE, count: bytes.count).write(to: m4aURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: nil,
                sourceURL: "https://example.com/audio.php",
                etag: nil
            ),
            for: episodeId
        )

        #expect(
            await manager.servingURLIfComplete(for: episodeId) == mp3URL
        )
    }

    @Test("weak pin with case-variant canonical siblings is withheld")
    func weakPinCaseVariantSiblingsAreAmbiguous() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "weak-pin-case-variant-siblings"
        let upperDirectory = dir.appendingPathComponent(
            "upper", isDirectory: true
        )
        let lowerDirectory = dir.appendingPathComponent(
            "lower", isDirectory: true
        )
        try FileManager.default.createDirectory(
            at: upperDirectory,
            withIntermediateDirectories: true
        )
        try FileManager.default.createDirectory(
            at: lowerDirectory,
            withIntermediateDirectories: true
        )
        let equalLengthCandidates = [
            upperDirectory.appendingPathComponent("episode.MP3"),
            lowerDirectory.appendingPathComponent("episode.mp3"),
        ]
        let bytes = Data(repeating: 0xD4, count: 192)
        try bytes.write(to: equalLengthCandidates[0])
        try bytes.write(to: equalLengthCandidates[1])
        await manager._setAudioArtifactURLsForTesting(
            episodeId: episodeId,
            candidates: equalLengthCandidates
        )
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: nil,
                sourceURL: "https://example.com/audio.php",
                etag: nil
            ),
            for: episodeId
        )

        #expect(await manager.servingURLIfComplete(for: episodeId) == nil)
        #expect(await manager.cachedFileURL(for: episodeId) == nil)
    }

    @Test("strong-pin serving memoizes authentication until file identity changes")
    func strongPinServingMemoizesFullFileHash() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "strong-pin-verification-memo"
        let completeURL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0xEF, count: 4_096)
        try bytes.write(to: completeURL)
        let hash = try FileHasher.sha256(fileURL: completeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: hash,
                sourceURL: "https://example.com/episode.mp3",
                etag: nil
            ),
            for: episodeId
        )

        #expect(await manager.servingURLIfComplete(for: episodeId) == completeURL)
        #expect(await manager.servingURLIfComplete(for: episodeId) == completeURL)
        #expect(
            await manager._strongPinVerificationHashCountForTesting() == 1,
            "Repeated immutable-cache lookups must reuse the verified strong pin"
        )

        try Data(repeating: 0xF0, count: bytes.count).write(to: completeURL)
        try FileManager.default.setAttributes(
            [.modificationDate: Date().addingTimeInterval(60)],
            ofItemAtPath: completeURL.path
        )
        #expect(await manager.servingURLIfComplete(for: episodeId) == nil)
        #expect(
            await manager._strongPinVerificationHashCountForTesting() == 2,
            "A file identity change must invalidate the memo and re-authenticate"
        )
    }

    @Test("a mutable streaming path stays outside the immutable cache surface")
    func activeStreamingTransferWithholdsExactLengthPath() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "active-stream-exact-length"
        let transferId = await manager._beginStreamingTransferForTesting(
            episodeId: episodeId
        )
        let completeURL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0xE2, count: 192)
        try bytes.write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: nil,
                sourceURL: "https://example.com/episode.mp3",
                etag: nil
            ),
            for: episodeId
        )

        #expect(
            await manager.servingURLIfComplete(for: episodeId) == nil,
            "Length equality is not finalization while the byte-pump token still owns the path"
        )
        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: episodeId,
                transferId: transferId,
                bytesWritten: Int64(bytes.count)
            )
        )
        #expect(await manager.servingURLIfComplete(for: episodeId) == completeURL)
    }

    @Test("Legacy bare artifact survives a refreshed source extension")
    func legacyArtifactResolvesAcrossSourceExtension() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "legacy-cross-extension"
        let mp3URL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0xDF, count: 128)
        try bytes.write(to: mp3URL)

        let result = try await manager.streamingDownload(
            episodeId: episodeId,
            from: URL(string: "https://example.com/episode.m4a")!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )

        #expect(result.fileURL == mp3URL)
        #expect(result.contentType == "public.mp3")
        #expect(try Data(contentsOf: mp3URL) == bytes)
    }

    @Test("Ambiguous legacy extension siblings are withheld through background serving")
    func ambiguousLegacySiblingsFailClosedAcrossServingPath() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        defer {
            Task { await manager.invalidateBackgroundSessionsForTesting() }
        }
        let episodeId = "ambiguous-legacy-siblings"
        let mp3URL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xD1, count: 128).write(to: mp3URL)
        await manager._setExtensionCacheForTesting(
            episodeId: episodeId,
            fileExtension: "m4a"
        )
        let m4aURL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xD2, count: 128).write(to: m4aURL)
        await manager._setExtensionCacheForTesting(
            episodeId: episodeId,
            fileExtension: "mp3"
        )

        #expect(await manager.loadPin(for: episodeId) == nil)
        #expect(await manager.servingURLIfComplete(for: episodeId) == nil)
        #expect(await manager.cachedFileURL(for: episodeId) == nil)
        #expect(!(await manager.isCached(episodeId: episodeId)))

        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(
                string: "https://example.invalid/replacement.m4a"
            )!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(
                episodeId: episodeId
            ),
            "The full serving path must reject ambiguous legacy bytes and admit a replacement transfer"
        )
        await manager.invalidateBackgroundSessionsForTesting()
    }

    @Test("Incomplete pinned file does not block background retry admission")
    func incompletePinnedFileAllowsBackgroundRetry() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        defer {
            Task { await manager.invalidateBackgroundSessionsForTesting() }
        }
        let episodeId = "incomplete-background-retry"
        let partialURL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xE0, count: 64).write(to: partialURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: 1_024,
                sha256: nil,
                sourceURL: "https://example.com/episode.mp3",
                etag: nil
            ),
            for: episodeId
        )

        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://example.invalid/episode.mp3")!,
            context: .unattributed(
                reason: .testHarness, isExplicitDownload: false
            )
        )

        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(
                episodeId: episodeId
            )
        )
        await manager.invalidateBackgroundSessionsForTesting()
    }
}

// MARK: - UTI Mapping

@Suite("DownloadManager – UTI Mapping")
struct DownloadManagerUTITests {

    @Test("Maps common podcast audio formats to UTIs")
    func commonFormats() {
        #expect(DownloadManager.utiForExtension("mp3") == "public.mp3")
        #expect(DownloadManager.utiForExtension("m4a") == "public.mpeg-4-audio")
        #expect(DownloadManager.utiForExtension("aac") == "public.aac-audio")
        #expect(DownloadManager.utiForExtension("wav") == "com.microsoft.waveform-audio")
        #expect(DownloadManager.utiForExtension("mp4") == "public.mpeg-4")
        #expect(DownloadManager.utiForExtension("ogg") == "org.xiph.ogg")
        #expect(DownloadManager.utiForExtension("opus") == "org.xiph.opus")
    }

    @Test("Case-insensitive extension matching")
    func caseInsensitive() {
        #expect(DownloadManager.utiForExtension("MP3") == "public.mp3")
        #expect(DownloadManager.utiForExtension("M4A") == "public.mpeg-4-audio")
    }

    @Test("Unknown extension returns generic audio UTI")
    func unknownFormat() {
        #expect(DownloadManager.utiForExtension("xyz") == "public.audio")
        #expect(DownloadManager.utiForExtension("") == "public.audio")
    }
}

// MARK: - StreamingDownloadResult

@Suite("StreamingDownloadResult – Fields")
struct StreamingDownloadResultTests {

    @Test("Result carries totalBytes and contentType from HTTP response")
    func resultFields() {
        let result = DownloadManager.StreamingDownloadResult(
            fileURL: URL(fileURLWithPath: "/tmp/test.mp3"),
            totalBytes: 66_549_234,
            contentType: "public.mp3",
            downloadComplete: {}
        )

        #expect(result.totalBytes == 66_549_234)
        #expect(result.contentType == "public.mp3")
        #expect(result.fileURL.lastPathComponent == "test.mp3")
    }

    @Test("nil totalBytes when server omits Content-Length")
    func nilTotalBytes() {
        let result = DownloadManager.StreamingDownloadResult(
            fileURL: URL(fileURLWithPath: "/tmp/test.mp3"),
            totalBytes: nil,
            contentType: "public.audio",
            downloadComplete: {}
        )

        #expect(result.totalBytes == nil)
    }
}

// MARK: - Playable Threshold

@Suite("StreamingDownload – Threshold")
struct StreamingDownloadThresholdTests {

    @Test("Default threshold is 8 MB")
    func defaultThreshold() {
        #expect(DownloadManager.defaultPlayableThreshold == 8 * 1024 * 1024)
    }
}

// MARK: - Transfer Ownership

@Suite("StreamingDownload – Transfer Ownership")
struct StreamingDownloadOwnershipTests {

    @Test("late same-episode completion cannot finalize replacement partial")
    func sameEpisodeLateCompletionIsRejected() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "same-episode-replay"
        let fileURL = await manager.completeFileURL(for: episodeId)

        let first = await manager._beginStreamingTransferForTesting(
            episodeId: episodeId
        )
        try Data(repeating: 0xA1, count: 64).write(to: fileURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: 512,
                sha256: nil,
                sourceURL: nil,
                etag: nil
            ),
            for: episodeId
        )

        let replacement = await manager._beginStreamingTransferForTesting(
            episodeId: episodeId
        )
        let replacementBytes = Data(repeating: 0xB2, count: 128)
        try replacementBytes.write(to: fileURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: 1_024,
                sha256: nil,
                sourceURL: nil,
                etag: nil
            ),
            for: episodeId
        )

        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: episodeId,
                transferId: first,
                bytesWritten: 64
            ) == false
        )
        #expect(try Data(contentsOf: fileURL) == replacementBytes)
        #expect(await manager.loadPin(for: episodeId)?.expectedBytes == 1_024)
        #expect(await manager.cachedFileURL(for: episodeId) == nil)

        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: episodeId,
                transferId: replacement,
                bytesWritten: Int64(replacementBytes.count)
            )
        )
        #expect(
            await manager.loadPin(for: episodeId)?.expectedBytes
                == Int64(replacementBytes.count)
        )
        #expect(await manager.cachedFileURL(for: episodeId) == fileURL)
    }

    @Test("late prior-episode completion cannot affect active streaming lane")
    func priorEpisodeLateCompletionIsRejected() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let priorEpisodeId = "prior-episode"
        let activeEpisodeId = "active-episode"

        let prior = await manager._beginStreamingTransferForTesting(
            episodeId: priorEpisodeId
        )
        let priorURL = await manager.completeFileURL(for: priorEpisodeId)
        try Data(repeating: 0xC3, count: 64).write(to: priorURL)

        let active = await manager._beginStreamingTransferForTesting(
            episodeId: activeEpisodeId
        )
        let activeURL = await manager.completeFileURL(for: activeEpisodeId)
        let activeBytes = Data(repeating: 0xD4, count: 96)
        try activeBytes.write(to: activeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: 768,
                sha256: nil,
                sourceURL: nil,
                etag: nil
            ),
            for: activeEpisodeId
        )

        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: priorEpisodeId,
                transferId: prior,
                bytesWritten: 64
            ) == false
        )
        #expect(!FileManager.default.fileExists(atPath: priorURL.path))
        #expect(try Data(contentsOf: activeURL) == activeBytes)
        #expect(
            await manager.loadPin(for: activeEpisodeId)?.expectedBytes == 768
        )

        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: activeEpisodeId,
                transferId: active,
                bytesWritten: Int64(activeBytes.count)
            )
        )
    }

    @Test("explicit cancellation invalidates the matching streaming transfer")
    func explicitCancellationInvalidatesStreamingTransfer() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "cancelled-stream"
        let transfer = await manager._beginStreamingTransferForTesting(
            episodeId: episodeId
        )
        let fileURL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xE5, count: 80).write(to: fileURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: 640,
                sha256: nil,
                sourceURL: nil,
                etag: nil
            ),
            for: episodeId
        )

        await manager.cancelDownload(episodeId: episodeId)

        #expect(!FileManager.default.fileExists(atPath: fileURL.path))
        #expect(await manager.loadPin(for: episodeId) == nil)
        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: episodeId,
                transferId: transfer,
                bytesWritten: 80
            ) == false
        )
    }

    @Test("extension-changing replay removes the exact superseded partial")
    func extensionChangingReplayRemovesSupersededPartial() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "extension-changing-replay"

        let first = await manager._beginStreamingTransferForTesting(
            episodeId: episodeId,
            fileExtension: "mp3"
        )
        let firstURL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xF6, count: 80).write(to: firstURL)

        let replacement = await manager._beginStreamingTransferForTesting(
            episodeId: episodeId,
            fileExtension: "m4a"
        )
        let replacementURL = await manager.completeFileURL(for: episodeId)

        #expect(firstURL != replacementURL)
        #expect(!FileManager.default.fileExists(atPath: firstURL.path))
        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: episodeId,
                transferId: first,
                bytesWritten: 80
            ) == false
        )

        let replacementBytes = Data(repeating: 0xA7, count: 96)
        try replacementBytes.write(to: replacementURL)
        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: episodeId,
                transferId: replacement,
                bytesWritten: Int64(replacementBytes.count)
            )
        )
        #expect(await manager.cachedFileURL(for: episodeId) == replacementURL)
    }

    @Test("finalization uses the active transfer path, not mutable extension cache")
    func finalizationUsesOwnedPath() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let episodeId = "finalize-owned-path"
        let transfer = await manager._beginStreamingTransferForTesting(
            episodeId: episodeId,
            fileExtension: "mp3"
        )
        let ownedURL = await manager.completeFileURL(for: episodeId)
        let ownedBytes = Data(repeating: 0xB8, count: 111)
        try ownedBytes.write(to: ownedURL)

        await manager._setExtensionCacheForTesting(
            episodeId: episodeId,
            fileExtension: "m4a"
        )
        let foreignURL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0xC9, count: 37).write(to: foreignURL)

        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: episodeId,
                transferId: transfer,
                bytesWritten: Int64(ownedBytes.count)
            )
        )
        #expect(
            await manager.loadPin(for: episodeId)?.expectedBytes
                == Int64(ownedBytes.count)
        )
        #expect(await manager.cachedFileURL(for: episodeId) == ownedURL)
    }

    @Test("pre-threshold continuation wait remains cancellation-aware")
    func preThresholdWaitHasCancellationHandler() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/Downloads/DownloadManager.swift"
        )
        let body = try #require(
            SwiftSourceInspector.firstBody(
                in: source,
                after: "func streamingDownload("
            )
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let wrapper = try #require(
            stripped.range(
                of: "let result: StreamingDownloadResult = try await withTaskCancellationHandler"
            )
        )
        let handoff = try #require(
            stripped.range(
                of: "didHandOffBytePump = true",
                range: wrapper.upperBound..<stripped.endIndex
            )
        )
        let cancellationSlice = stripped[
            wrapper.lowerBound..<handoff.lowerBound
        ]

        #expect(
            cancellationSlice.contains("withCheckedThrowingContinuation")
        )
        #expect(cancellationSlice.contains("onCancel:"))
        #expect(cancellationSlice.contains("cancelTransfer()"))
    }

    @Test("small streaming files retain the real completion waiter")
    func smallFilesDoNotReturnNoOpCompletion() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/Downloads/DownloadManager.swift"
        )
        let body = try #require(
            SwiftSourceInspector.firstBody(
                in: source,
                after: "func streamingDownload("
            )
        )
        let start = try #require(body.range(of: "let completionStream"))
        let end = try #require(
            body.range(
                of: "didHandOffBytePump = true",
                range: start.upperBound..<body.endIndex
            )
        )
        let transferBody = String(
            body[start.lowerBound..<end.lowerBound]
        )

        #expect(
            transferBody.components(
                separatedBy: "downloadComplete: waitForComplete"
            ).count - 1 == 2,
            "Threshold and small-file handoffs must expose the same completion contract"
        )
        #expect(
            !transferBody.contains("downloadComplete: {}"),
            "A small file cannot report completion before finalization accepts ownership"
        )
    }
}


// MARK: - playhead-zmog: a streamed episode is a completed download too

/// Pressing PLAY on a not-yet-downloaded episode finalizes through
/// `finalizeStreamingTransfer`, which writes the completeness pin, sets the
/// fingerprint and registers an analysis asset — a fully downloaded episode by
/// every measure except one: it was the only completion path that never told
/// the day-0 kickoff observer.
///
/// TWO CONSEQUENCES, and the second is newer and worse than the bead that
/// filed this:
///   1. a device pull could not tell a streamed episode that never attempted
///      day-0 from one that was never downloaded — byte-identical in the
///      ledger, which is the silence playhead-4dqe exists to remove;
///   2. playhead-jra6's resume sweep re-drives owed kickoffs FROM THAT TABLE,
///      so a path writing no row is a path the sweep can never rescue.
///
/// What this bead does NOT change, checked rather than assumed: the streaming
/// branch fires the play-time trigger with a file still being written, and the
/// bead asked whether that partial file reaches the byte diff. It does not.
/// `DayZeroRediffTrigger` passes it as `RediffRefetchCandidate.localAudioURL`,
/// whose only reader is `RediffRefetchService.processCandidate` — the LAGGED
/// sweep. Day-0 goes through `fetchMintAndRecord`, and the minter resolves the
/// A-side from the asset row's `sourceURL`. The doc comment was right.
@Suite("Streaming finalize leaves a day-0 kickoff record (playhead-zmog)")
struct StreamingDayZeroKickoffTests {

    private func makeTempDir() throws -> URL {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("zmog-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    /// The observer seam is SYNCHRONOUS, so the collector is too — a `Task`
    /// hop here would make the assertion race the notification and turn a
    /// missing call into a flake rather than a failure.
    private final class Completions: @unchecked Sendable {
        private let lock = NSLock()
        private var storage: [(episodeId: String, sourceURL: URL?)] = []

        func note(_ episodeId: String, _ sourceURL: URL?) {
            lock.lock()
            defer { lock.unlock() }
            storage.append((episodeId, sourceURL))
        }

        var seen: [(episodeId: String, sourceURL: URL?)] {
            lock.lock()
            defer { lock.unlock() }
            return storage
        }

        var episodeIds: [String] { seen.map(\.episodeId) }
    }

    @Test("THE ACCEPTANCE: finalizing a streamed transfer notifies the day-0 observer")
    func streamingFinalizeNotifiesTheObserver() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let completions = Completions()
        await manager.setBackgroundDownloadCompletionObserver {
            @Sendable episodeId, sourceURL in
            completions.note(episodeId, sourceURL)
        }

        let episodeId = "zmog-streamed"
        let transferId = await manager._beginStreamingTransferForTesting(episodeId: episodeId)
        let completeURL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0xA7, count: 256)
        try bytes.write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: nil,
                sourceURL: "https://example.com/zmog.mp3",
                etag: nil
            ),
            for: episodeId
        )

        #expect(
            await manager._finalizeStreamingTransferForTesting(
                episodeId: episodeId,
                transferId: transferId,
                bytesWritten: Int64(bytes.count)
            )
        )

        #expect(
            completions.episodeIds == [episodeId],
            """
            A streamed episode that leaves no kickoff row is invisible to a \
            device pull AND unrescuable by playhead-jra6's resume sweep.
            """
        )
    }

    @Test("the notification carries the pinned SOURCE URL, which is what a re-drive re-fetches")
    func notificationCarriesTheSourceURL() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let completions = Completions()
        await manager.setBackgroundDownloadCompletionObserver {
            @Sendable episodeId, sourceURL in
            completions.note(episodeId, sourceURL)
        }

        let episodeId = "zmog-url"
        let transferId = await manager._beginStreamingTransferForTesting(episodeId: episodeId)
        let completeURL = await manager.completeFileURL(for: episodeId)
        let bytes = Data(repeating: 0x5C, count: 128)
        try bytes.write(to: completeURL)
        await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(bytes.count),
                sha256: nil,
                sourceURL: "https://example.com/zmog-url.mp3",
                etag: nil
            ),
            for: episodeId
        )
        _ = await manager._finalizeStreamingTransferForTesting(
            episodeId: episodeId,
            transferId: transferId,
            bytesWritten: Int64(bytes.count)
        )

        // A kickoff with no URL is a claim that cannot be re-driven — the exact
        // state playhead-jra6 measured 12 of on the device. The URL is the one
        // the TRANSFER followed, which the testing seam supplies.
        #expect(completions.seen.first?.sourceURL?.absoluteString
                == "https://example.invalid/\(episodeId).mp3")
        #expect(completions.seen.first?.sourceURL != nil)
    }

    @Test("a finalize that does NOT produce a servable artifact notifies nobody")
    func failedFinalizeNotifiesNobody() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()
        let completions = Completions()
        await manager.setBackgroundDownloadCompletionObserver {
            @Sendable episodeId, sourceURL in
            completions.note(episodeId, sourceURL)
        }

        // A transfer id nobody owns: the finalize refuses before the success
        // path. Telling the observer here would start a readiness wait for
        // bytes that will never resolve, and the resulting `no_pinned_file`
        // give-up would be this hook's fault rather than the network's.
        let refused = await manager._finalizeStreamingTransferForTesting(
            episodeId: "zmog-never-began",
            transferId: UUID(),
            bytesWritten: 10
        )

        #expect(!refused)
        #expect(completions.episodeIds.isEmpty)
    }
}
