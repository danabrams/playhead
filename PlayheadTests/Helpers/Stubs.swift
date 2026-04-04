// Stubs.swift
// Configurable test doubles for service protocols used in pre-analysis tests.

import Foundation
@testable import Playhead

// MARK: - StubAnalysisAudioProvider

final class StubAnalysisAudioProvider: AnalysisAudioProviding, @unchecked Sendable {
    var shardsToReturn: [AnalysisShard] = []
    var errorToThrow: Error?

    func decode(fileURL: LocalAudioURL, episodeID: String, shardDuration: TimeInterval) async throws -> [AnalysisShard] {
        if let error = errorToThrow { throw error }
        return shardsToReturn
    }
}

// MARK: - StubAdDetectionProvider

final class StubAdDetectionProvider: AdDetectionProviding, @unchecked Sendable {
    var hotPathResult: [AdWindow] = []
    var hotPathError: Error?
    var backfillError: Error?
    var hotPathCallCount = 0
    var backfillCallCount = 0

    func runHotPath(chunks: [TranscriptChunk], analysisAssetId: String, episodeDuration: Double) async throws -> [AdWindow] {
        hotPathCallCount += 1
        if let error = hotPathError { throw error }
        return hotPathResult
    }

    func runBackfill(chunks: [TranscriptChunk], analysisAssetId: String, podcastId: String, episodeDuration: Double) async throws {
        backfillCallCount += 1
        if let error = backfillError { throw error }
    }
}

// MARK: - StubCapabilitiesProvider

final class StubCapabilitiesProvider: CapabilitiesProviding, @unchecked Sendable {
    var currentSnapshot: CapabilitySnapshot

    init(snapshot: CapabilitySnapshot? = nil) {
        self.currentSnapshot = snapshot ?? CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10 * 1024 * 1024 * 1024,
            capturedAt: .now
        )
    }

    func capabilityUpdates() -> AsyncStream<CapabilitySnapshot> {
        AsyncStream { continuation in
            continuation.yield(currentSnapshot)
            continuation.finish()
        }
    }
}

// MARK: - StubDownloadProvider

final class StubDownloadProvider: DownloadProviding, @unchecked Sendable {
    var cachedURLs: [String: URL] = [:]
    var fingerprints: [String: AudioFingerprint] = [:]

    func cachedFileURL(for episodeId: String) -> URL? {
        cachedURLs[episodeId]
    }

    func fingerprint(for episodeId: String) -> AudioFingerprint? {
        fingerprints[episodeId]
    }
}

// MARK: - StubAnalysisStore

/// Wraps a real AnalysisStore backed by a temp directory for test isolation.
/// Use `makeTestStore()` from TestHelpers.swift to create instances.
final class StubAnalysisStore: @unchecked Sendable {
    let store: AnalysisStore
    let directory: URL

    init(store: AnalysisStore, directory: URL) {
        self.store = store
        self.directory = directory
    }
}
