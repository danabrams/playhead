// TestHelpers.swift
// Shared test utilities used across PlayheadTests suites.

import Foundation
@testable import Playhead

/// Creates a uniquely-named temporary directory for test isolation.
/// Caller is responsible for cleanup (e.g., via `defer`, `addTeardownBlock`,
/// or `TestTempDirTracker`).
func makeTempDir(prefix: String = "PlayheadTests") throws -> URL {
    let url = FileManager.default.temporaryDirectory
        .appendingPathComponent("\(prefix)-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}

/// Thread-safe collector for temp directories that cleans up on deinit.
/// Use at file scope alongside `makeTestStore()`-style helpers to ensure
/// temp directories are removed when the test suite finishes.
final class TestTempDirTracker: @unchecked Sendable {
    private var dirs: [URL] = []
    private let lock = NSLock()

    func track(_ dir: URL) {
        lock.lock()
        dirs.append(dir)
        lock.unlock()
    }

    deinit {
        for dir in dirs {
            try? FileManager.default.removeItem(at: dir)
        }
    }
}

// MARK: - AnalysisStore Factory

/// Shared tracker for test store temp directories.
private let _sharedTestStoreDirs = TestTempDirTracker()

/// Creates an AnalysisStore backed by a temporary directory for isolated testing.
/// The directory is automatically cleaned up when the test process ends.
func makeTestStore() async throws -> AnalysisStore {
    let dir = try makeTempDir(prefix: "PlayheadTests")
    _sharedTestStoreDirs.track(dir)
    let store = try AnalysisStore(directory: dir)
    try await store.migrate()
    return store
}

// MARK: - AnalysisShard Factory

/// Creates a test AnalysisShard with sensible defaults. Silence samples are used
/// so the shard is lightweight yet passes any non-empty-sample checks.
func makeShard(
    id: Int = 0,
    episodeID: String = "test-ep",
    startTime: TimeInterval = 0,
    duration: TimeInterval = 30
) -> AnalysisShard {
    AnalysisShard(
        id: id,
        episodeID: episodeID,
        startTime: startTime,
        duration: duration,
        samples: [Float](repeating: 0, count: 16000 * Int(duration))
    )
}
