// TestHelpers.swift
// Shared test utilities used across PlayheadTests suites.

import Foundation

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
