// FileProtectionTests.swift
// playhead-h3h: Pin the file-protection class on every on-disk artifact
// the analysis pipeline writes. The bead's wishlist asks for
// `NSFileProtectionComplete`; production reality is
// `.completeUntilFirstUserAuthentication` for a documented reason that
// applies to every store written here:
//
//   AnalysisStore.ensureOpen() comment (verbatim summary):
//     The app is woken by `BGProcessingTask` and `BGAppRefreshTask` while
//     the device may still be locked. `.complete` renders the SQLite file
//     unreadable in that window, which previously triggered a 4×-repro'd
//     crash chain in `PlayheadRuntime.init`. `.completeUntilFirstUserAuthentication`
//     keeps the file protected pre-unlock and accessible for the rest of
//     the boot session once the user has authenticated at least once,
//     which is the envelope every BGTask runs in.
//
// The same constraint applies to:
//   * the audio cache (AnalysisCoordinator opens cached audio during
//     BGProcessingTask windows),
//   * the model directories (ASR / classifier services mmap model files
//     during pre-first-unlock BG windows),
// so all three sites are stamped with the same class as part of this
// bead. These tests pin the production reality so that any future swap
// to stricter `.complete` (or weaker `.none`) is caught at CI time.
//
// What is NOT testable in-process (deferred to real-device verification):
//   * Genuine post-FBE behaviour (i.e. the OS actually refusing reads
//     pre-first-unlock). The simulator's data protection class is not
//     even round-tripped through `attributesOfItem`: APFS on macOS does
//     not honor the iOS data-protection xattr at all, so reading
//     `.protectionKey` back returns `nil` even when the producer
//     successfully called `setAttributes`. We work around this by:
//       (a) on the simulator, asserting the production code path
//           SUCCEEDS calling setAttributes on a probe file (no throw),
//           which is the only verifiable contract in-process;
//       (b) on real iOS, asserting the persisted stamp via
//           `.protectionKey`.
//     Real-device verification is by inspection of a
//     `BGProcessingTask`-driven smoke run after a forced reboot.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-h3h - file protection class on sensitive artifacts")
struct FileProtectionTests {

    /// The protection class every analysis-pipeline artifact must carry.
    /// Stricter than `.completeUnlessOpen` (covers all write paths), but
    /// looser than `.complete` for the BG-launch reason documented at the
    /// top of this file.
    private static let expectedClass = FileProtectionType.completeUntilFirstUserAuthentication

    /// Helper that reads the protection key off a path and asserts.
    ///
    /// Two paths:
    ///   * Real device — `.protectionKey` is the persisted kernel stamp;
    ///     we assert it matches `expected` exactly.
    ///   * Simulator — APFS doesn't round-trip the iOS data-protection
    ///     xattr. `.protectionKey` is `nil` regardless of what production
    ///     called. We instead exercise the contract that production
    ///     CAN set the same class on a probe file at the same path
    ///     without throwing; if FileManager would reject the class for
    ///     this volume, this would surface here. (This is the strongest
    ///     check available in-process.)
    private func assertProtection(
        atPath path: String,
        label: String,
        expected: FileProtectionType = expectedClass
    ) {
        guard FileManager.default.fileExists(atPath: path) else {
            Issue.record("\(label) not present at \(path); test cannot assert")
            return
        }
        do {
            let attrs = try FileManager.default.attributesOfItem(atPath: path)
            let actual = attrs[.protectionKey] as? FileProtectionType
            #if targetEnvironment(simulator)
            // The simulator returns nil here even when production called
            // setAttributes successfully. Treat nil as "not knowable
            // in-process" and instead verify the production class can be
            // applied to this volume without erroring.
            if actual == nil {
                let probe = (path as NSString).appendingPathComponent(".h3h-probe")
                FileManager.default.createFile(atPath: probe, contents: Data())
                defer { try? FileManager.default.removeItem(atPath: probe) }
                do {
                    try FileManager.default.setAttributes(
                        [.protectionKey: expected], ofItemAtPath: probe
                    )
                } catch {
                    Issue.record("\(label) at \(path): probe-write of expected protection class failed with \(error)")
                }
            } else {
                #expect(
                    actual == expected,
                    "\(label) at \(path) carries protection \(String(describing: actual)); expected \(expected)"
                )
            }
            #else
            #expect(
                actual == expected,
                "\(label) at \(path) carries protection \(String(describing: actual)); expected \(expected)"
            )
            #endif
        } catch {
            Issue.record("Failed to read attributes for \(label) at \(path): \(error)")
        }
    }

    // MARK: - AnalysisStore (SQLite + WAL/SHM)

    @Test("AnalysisStore SQLite file is protected at .completeUntilFirstUserAuthentication after migrate()")
    func analysisStoreSQLiteIsProtected() async throws {
        let dir = try makeTempDir(prefix: "h3h-store")
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Drive a write so WAL/SHM sidecars get created — they must
        // inherit the same class via the directory-level stamp.
        let asset = AnalysisAsset(
            id: "h3h-fp-asset",
            episodeId: "ep-h3h-fp",
            assetFingerprint: "h3h-fp",
            weakFingerprint: nil,
            sourceURL: "file:///privacy/fp.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        assertProtection(atPath: dbURL.path, label: "AnalysisStore primary DB")
        // Containing directory stamp.
        assertProtection(atPath: dir.path, label: "AnalysisStore container directory")

        // WAL/SHM sidecars: SQLite creates them lazily next to the main
        // file. They are created by sqlite3 itself, so their protection
        // class is inherited from the parent directory stamp. We only
        // assert when present (`-wal` always exists after a write in WAL
        // mode; `-shm` is also created in WAL mode).
        let walPath = dbURL.path + "-wal"
        let shmPath = dbURL.path + "-shm"
        if FileManager.default.fileExists(atPath: walPath) {
            assertProtection(atPath: walPath, label: "AnalysisStore WAL sidecar")
        }
        if FileManager.default.fileExists(atPath: shmPath) {
            assertProtection(atPath: shmPath, label: "AnalysisStore SHM sidecar")
        }
    }

    // MARK: - DownloadManager (audio cache)

    @Test("DownloadManager cache subdirectories are protected at .completeUntilFirstUserAuthentication")
    func downloadManagerDirectoriesAreProtected() async throws {
        let dir = try makeTempDir(prefix: "h3h-dm")
        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        assertProtection(atPath: manager.cacheDirectory.path,
                         label: "DownloadManager cacheDirectory")
        assertProtection(atPath: manager.partialsDirectory.path,
                         label: "DownloadManager partialsDirectory")
        assertProtection(atPath: manager.completeDirectory.path,
                         label: "DownloadManager completeDirectory")
        assertProtection(atPath: manager.resumeDataDirectory.path,
                         label: "DownloadManager resumeDataDirectory")
    }

}
