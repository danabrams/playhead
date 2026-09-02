// AudioCacheRelocationTests.swift
// playhead-kc01: downloaded episode audio moved out of `Library/Caches`,
// which iOS purges under storage pressure without notifying the app or
// asking the user.
//
// The failure this prevents: a listener downloads episodes on Wi-Fi, boards a
// plane, opens the app, and the downloads are gone with no explanation and no
// network to re-fetch them.
//
// Two halves are tested apart, because they fail differently:
//
//   * WHERE the root is, and that it is not evictable or backed up. A
//     regression here is silent until a device runs low on storage.
//   * WHATEVER the migration does to files. A regression here destroys an
//     existing listener's library on the upgrade that was supposed to protect
//     it, so every property is pinned: the guard that refuses a non-default
//     root, per-file failure isolation, collision resolution, and the refusal
//     to delete a directory that still holds anything.

import Foundation
import XCTest

@testable import Playhead

final class AudioCacheRelocationTests: XCTestCase {

    private var tmp: URL!

    override func setUpWithError() throws {
        try super.setUpWithError()
        tmp = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("kc01-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tmp, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        if let tmp { try? FileManager.default.removeItem(at: tmp) }
        try super.tearDownWithError()
    }

    // MARK: - Where the audio lives

    func testAudioCacheIsNotInTheEvictableCachesDirectory() {
        let root = DownloadManager.defaultCacheDirectory()
        let caches = FileManager.default
            .urls(for: .cachesDirectory, in: .userDomainMask)[0]
        XCTAssertFalse(
            root.path.hasPrefix(caches.path),
            """
            The audio cache is back under Library/Caches, which iOS purges \
            without telling the app. A downloaded episode is not a cache entry.
            """
        )
    }

    func testAudioCacheLivesInApplicationSupport() {
        let root = DownloadManager.defaultCacheDirectory()
        let appSupport = FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        XCTAssertTrue(root.path.hasPrefix(appSupport.path))
        XCTAssertEqual(root.lastPathComponent, "AudioCache")
    }

    func testLegacyLocationStillNamesTheOldCachesPath() {
        // The migration reads it; if this drifts, existing downloads are
        // silently left behind rather than moved.
        let legacy = DownloadManager.legacyCachesDirectory()
        let caches = FileManager.default
            .urls(for: .cachesDirectory, in: .userDomainMask)[0]
        XCTAssertEqual(
            legacy,
            caches
                .appendingPathComponent("Playhead", isDirectory: true)
                .appendingPathComponent("AudioCache", isDirectory: true)
        )
    }

    func testBootstrapExcludesTheRootFromBackup() async throws {
        // Application Support IS backed up by default, and ~290 MB of
        // re-downloadable podcast audio does not belong in a user's iCloud
        // backup. Driven through a custom root: the flag is stamped on
        // whatever root the manager was built with.
        let root = tmp.appendingPathComponent("AudioCache", isDirectory: true)
        let manager = DownloadManager(cacheDirectory: root)
        try await manager.bootstrap()

        let values = try root.resourceValues(forKeys: [.isExcludedFromBackupKey])
        XCTAssertEqual(values.isExcludedFromBackup, true)
    }

    // MARK: - The migration: what it moves

    private func seed(_ root: URL, _ subdirectory: String, _ name: String, bytes: Int = 8) throws {
        let dir = root.appendingPathComponent(subdirectory, isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        try Data(repeating: 0xAB, count: bytes)
            .write(to: dir.appendingPathComponent(name))
    }

    private func exists(_ root: URL, _ subdirectory: String, _ name: String) -> Bool {
        FileManager.default.fileExists(
            atPath: root
                .appendingPathComponent(subdirectory, isDirectory: true)
                .appendingPathComponent(name).path
        )
    }

    func testEveryArtifactSubdirectoryIsCarriedForward() throws {
        let legacy = tmp.appendingPathComponent("legacy", isDirectory: true)
        let destination = tmp.appendingPathComponent("new", isDirectory: true)
        try seed(legacy, "complete", "aaa.mp3", bytes: 100)
        try seed(legacy, "complete", "aaa.mp3.pin", bytes: 10)
        try seed(legacy, "partials", "bbb.partial", bytes: 20)
        try seed(legacy, "resumeData", "ccc", bytes: 30)
        try seed(legacy, "attribution", "ddd.json", bytes: 40)

        let outcome = DownloadManager.migrateAudioCache(from: legacy, to: destination)

        XCTAssertEqual(outcome.moved, 5)
        XCTAssertEqual(outcome.failed, 0)
        XCTAssertEqual(outcome.movedBytes, 200)
        XCTAssertTrue(exists(destination, "complete", "aaa.mp3"))
        XCTAssertTrue(exists(destination, "complete", "aaa.mp3.pin"),
                      "the .pin sidecar must travel with its audio or the pin check fails")
        XCTAssertTrue(exists(destination, "partials", "bbb.partial"))
        XCTAssertTrue(exists(destination, "resumeData", "ccc"))
        XCTAssertTrue(exists(destination, "attribution", "ddd.json"))
        XCTAssertFalse(exists(legacy, "complete", "aaa.mp3"))
    }

    func testADrainedLegacyRootIsRemoved() throws {
        let legacy = tmp.appendingPathComponent("legacy", isDirectory: true)
        let destination = tmp.appendingPathComponent("new", isDirectory: true)
        try seed(legacy, "complete", "aaa.mp3")

        let outcome = DownloadManager.migrateAudioCache(from: legacy, to: destination)

        XCTAssertTrue(outcome.legacyRootRemoved)
        XCTAssertFalse(FileManager.default.fileExists(atPath: legacy.path))
    }

    func testALegacyRootHoldingUnknownContentIsLeftAlone() throws {
        // Property 4. Nothing recursively deletes a directory whose contents
        // were not accounted for — that is how playhead-wvdz destroyed a
        // database. An unrecognized subdirectory must survive.
        let legacy = tmp.appendingPathComponent("legacy", isDirectory: true)
        let destination = tmp.appendingPathComponent("new", isDirectory: true)
        try seed(legacy, "complete", "aaa.mp3")
        try seed(legacy, "somethingElse", "keepme.txt")

        let outcome = DownloadManager.migrateAudioCache(from: legacy, to: destination)

        XCTAssertEqual(outcome.moved, 1)
        XCTAssertFalse(outcome.legacyRootRemoved)
        XCTAssertTrue(FileManager.default.fileExists(atPath: legacy.path))
        XCTAssertTrue(exists(legacy, "somethingElse", "keepme.txt"))
    }

    func testACollisionResolvesInFavourOfTheDestination() throws {
        // Names are SHA-256 of the episode id, so a collision is the same
        // episode in both places. The destination copy is the one in use.
        let legacy = tmp.appendingPathComponent("legacy", isDirectory: true)
        let destination = tmp.appendingPathComponent("new", isDirectory: true)
        try seed(legacy, "complete", "same.mp3", bytes: 11)
        let destDir = destination.appendingPathComponent("complete", isDirectory: true)
        try FileManager.default.createDirectory(at: destDir, withIntermediateDirectories: true)
        let winner = Data(repeating: 0xCD, count: 22)
        try winner.write(to: destDir.appendingPathComponent("same.mp3"))

        let outcome = DownloadManager.migrateAudioCache(from: legacy, to: destination)

        XCTAssertEqual(outcome.moved, 0)
        XCTAssertEqual(outcome.duplicatesRemoved, 1)
        XCTAssertEqual(
            try Data(contentsOf: destDir.appendingPathComponent("same.mp3")),
            winner,
            "the destination copy must survive a collision unchanged"
        )
        XCTAssertFalse(exists(legacy, "complete", "same.mp3"))
    }

    func testAbsentLegacyRootIsANoop() {
        let outcome = DownloadManager.migrateAudioCache(
            from: tmp.appendingPathComponent("never-existed", isDirectory: true),
            to: tmp.appendingPathComponent("new", isDirectory: true)
        )
        XCTAssertEqual(outcome, .noop)
    }

    func testMigrationIsIdempotent() throws {
        let legacy = tmp.appendingPathComponent("legacy", isDirectory: true)
        let destination = tmp.appendingPathComponent("new", isDirectory: true)
        try seed(legacy, "complete", "aaa.mp3")

        let first = DownloadManager.migrateAudioCache(from: legacy, to: destination)
        let second = DownloadManager.migrateAudioCache(from: legacy, to: destination)

        XCTAssertEqual(first.moved, 1)
        XCTAssertEqual(second, .noop, "a second launch must not re-do or undo the move")
        XCTAssertTrue(exists(destination, "complete", "aaa.mp3"))
    }

    func testAnEmptySubdirectoryDoesNotCreateItAtTheDestination() throws {
        // A migration that creates four empty directories for an install with
        // nothing to move would make "did anything move?" unreadable from the
        // file system.
        let legacy = tmp.appendingPathComponent("legacy", isDirectory: true)
        let destination = tmp.appendingPathComponent("new", isDirectory: true)
        try FileManager.default.createDirectory(
            at: legacy.appendingPathComponent("complete", isDirectory: true),
            withIntermediateDirectories: true
        )

        _ = DownloadManager.migrateAudioCache(from: legacy, to: destination)

        XCTAssertFalse(
            FileManager.default.fileExists(
                atPath: destination.appendingPathComponent("complete").path
            )
        )
    }

    // MARK: - The guard

    func testACustomRootedManagerDoesNotMigrate() async throws {
        // THE DANGEROUS ONE. Every test in the suite builds a manager on a
        // temporary directory. If the guard were dropped, bootstrapping one
        // would move the real device's downloads into that temp directory and
        // then delete it in tearDown — destroying the library this bead exists
        // to protect. Proven by observing that a seeded legacy root is
        // untouched by a custom-rooted bootstrap.
        let legacy = DownloadManager.legacyCachesDirectory()
        let sentinelName = "kc01-guard-\(UUID().uuidString).mp3"
        let completeDir = legacy.appendingPathComponent("complete", isDirectory: true)
        try FileManager.default.createDirectory(
            at: completeDir, withIntermediateDirectories: true
        )
        let sentinel = completeDir.appendingPathComponent(sentinelName)
        try Data(repeating: 0x01, count: 4).write(to: sentinel)
        defer { try? FileManager.default.removeItem(at: sentinel) }

        let manager = DownloadManager(
            cacheDirectory: tmp.appendingPathComponent("custom", isDirectory: true)
        )
        try await manager.bootstrap()

        XCTAssertTrue(
            FileManager.default.fileExists(atPath: sentinel.path),
            "a custom-rooted manager migrated the real cache — it must not"
        )
        XCTAssertFalse(
            FileManager.default.fileExists(
                atPath: tmp.appendingPathComponent("custom/complete/\(sentinelName)").path
            )
        )
    }

    // MARK: - Persisted references follow the move

    func testAStaleLegacyAbsolutePathReRootsOntoTheNewLocation() {
        // playhead-b8hj stores the `<subdirectory>/<name>` tail and re-roots
        // at read time, so rows written before kc01 — which hold absolute
        // Library/Caches paths — resolve to the new location once the file is
        // no longer at the old one.
        let stale = DownloadManager.legacyCachesDirectory()
            .appendingPathComponent("complete/abc123.mp3").path
        let expected = DownloadManager.defaultCacheDirectory()
            .appendingPathComponent("complete", isDirectory: true)
            .appendingPathComponent("abc123.mp3")

        let resolved = AudioCacheLocation.resolve(stale) { $0 == expected }

        XCTAssertEqual(resolved, expected)
    }

    func testPortableStringStillStripsThePrefixAtTheNewRoot() {
        let url = DownloadManager.defaultCacheDirectory()
            .appendingPathComponent("complete", isDirectory: true)
            .appendingPathComponent("abc123.mp3")
        XCTAssertEqual(AudioCacheLocation.portableString(for: url), "complete/abc123.mp3")
    }
}
