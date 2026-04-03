// DownloadManagerTests.swift
// Unit tests for the audio asset cache and download manager.

import Foundation
import Testing
@testable import Playhead

// MARK: - Bootstrap & Directory Structure

@Suite("DownloadManager – Setup")
struct DownloadManagerSetupTests {

    private func makeTempDir() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

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

    private func makeTempDir() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

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

    private func makeTempDir() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

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

    private func makeTempDir() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

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
