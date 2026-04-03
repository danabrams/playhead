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
            from: URL(string: "https://example.com/ep.mp3")!
        )

        #expect(result.fileURL == completeURL)
        #expect(result.contentType == "public.mp3")
        // downloadComplete should be a no-op for cached files.
        try await result.downloadComplete()
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

    @Test("Default threshold is 2 MB")
    func defaultThreshold() {
        #expect(DownloadManager.defaultPlayableThreshold == 2 * 1024 * 1024)
    }
}
