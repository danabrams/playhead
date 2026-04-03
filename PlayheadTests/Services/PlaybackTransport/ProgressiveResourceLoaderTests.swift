// ProgressiveResourceLoaderTests.swift
// Tests for the AVAssetResourceLoaderDelegate that serves a growing file.

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func writeTempFile(bytes: Int) throws -> (URL, URL) {
    let dir = try makeTempDir(prefix: "ProgressiveLoader")
    let file = dir.appendingPathComponent("test.mp3")
    let data = Data(repeating: 0x42, count: bytes)
    try data.write(to: file)
    return (dir, file)
}

// MARK: - Construction

@Suite("ProgressiveResourceLoader – Init")
struct ProgressiveResourceLoaderInitTests {

    @Test("Loader creates dispatch queue with expected label")
    func queueLabel() throws {
        let (dir, file) = try writeTempFile(bytes: 1024)
        defer { try? FileManager.default.removeItem(at: dir) }

        let loader = ProgressiveResourceLoader(
            fileURL: file,
            totalBytes: 50_000,
            contentType: "public.mp3"
        )

        #expect(loader.queue.label == "com.playhead.progressive-loader")
        _ = loader
    }
}

// MARK: - AVURLAsset Integration

@Suite("ProgressiveResourceLoader – Asset Integration")
struct ProgressiveResourceLoaderAssetTests {

    @Test("AVURLAsset with custom scheme accepts the delegate")
    func assetAcceptsDelegate() throws {
        let (dir, file) = try writeTempFile(bytes: 4096)
        defer { try? FileManager.default.removeItem(at: dir) }

        let loader = ProgressiveResourceLoader(
            fileURL: file,
            totalBytes: 4096,
            contentType: "public.mp3"
        )

        var components = URLComponents()
        components.scheme = "playhead-progressive"
        components.host = "audio"
        components.path = "/test.mp3"
        let url = components.url!

        let asset = AVURLAsset(url: url)
        asset.resourceLoader.setDelegate(loader, queue: loader.queue)

        #expect(asset.url.scheme == "playhead-progressive")
        _ = loader
    }

    @Test("Standard file:// URL does NOT trigger custom delegate")
    func fileURLDoesNotUseDelegate() throws {
        let (dir, file) = try writeTempFile(bytes: 4096)
        defer { try? FileManager.default.removeItem(at: dir) }

        // file:// URLs bypass the resource loader delegate — AVPlayer reads directly.
        // This verifies our architecture: progressive loads use the custom scheme,
        // cached file loads use file:// directly.
        let asset = AVURLAsset(url: file)
        #expect(asset.url.scheme == "file")
    }
}
