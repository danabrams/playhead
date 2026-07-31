// AudioCacheLocationTests.swift
// playhead-b8hj: a persisted reference to a cached episode must survive the
// app Data container being re-created under a new UUID (reinstall, restore,
// some TestFlight upgrades), and must degrade to "not cached" — never to an
// error or a wrong file — when the audio is genuinely gone.
//
// The container-change tests build a real file tree under one synthetic
// container UUID, persist a reference against it, then resolve against a
// SECOND container that holds the real bytes. Nothing is mocked: the anchor
// predicate is the production `AdDetectionService.isAnchoredRegularFile`, so a
// resolution that "succeeds" on a path with no file behind it cannot pass.

import Foundation
import Testing
@testable import Playhead

@Suite("AudioCacheLocation")
struct AudioCacheLocationTests {

    // MARK: - Fixture

    /// A throwaway `<root>/Library/Caches/Playhead/AudioCache` tree keyed by a
    /// synthetic container UUID, mirroring the real on-device layout closely
    /// enough that the container segment is a genuine variable.
    private struct Container {
        let top: URL
        let cacheRoot: URL

        init(uuid: String) throws {
            top = FileManager.default.temporaryDirectory
                .appendingPathComponent("b8hj-\(UUID().uuidString)", isDirectory: true)
            cacheRoot = top
                .appendingPathComponent("Containers/Data/Application", isDirectory: true)
                .appendingPathComponent(uuid, isDirectory: true)
                .appendingPathComponent("Library/Caches/Playhead/AudioCache", isDirectory: true)
            try FileManager.default.createDirectory(
                at: cacheRoot.appendingPathComponent("complete", isDirectory: true),
                withIntermediateDirectories: true
            )
        }

        @discardableResult
        func writeAudio(named name: String, bytes: Int = 4096) throws -> URL {
            let url = cacheRoot
                .appendingPathComponent("complete", isDirectory: true)
                .appendingPathComponent(name)
            try Data(repeating: 0x41, count: bytes).write(to: url)
            return url
        }

        func remove() {
            try? FileManager.default.removeItem(at: top)
        }
    }

    private static let anchored: @Sendable (URL) -> Bool = {
        AdDetectionService.isAnchoredRegularFile($0)
    }

    /// The real basename shape: SHA-256 of the episode id.
    private static func basename(for episodeId: String, ext: String = "mp3") -> String {
        "\(DownloadManager.safeFilename(for: episodeId)).\(ext)"
    }

    // MARK: - The container-change contract

    @Test("a reference persisted against one container resolves against another")
    func survivesContainerChange() throws {
        let episodeId = "https://feed.example/rss::guid-42"
        let name = Self.basename(for: episodeId)

        let old = try Container(uuid: "BCC522DD-B26A-430D-B0F1-CCDE0D4235F9")
        defer { old.remove() }
        let installed = try old.writeAudio(named: name)

        // Persist while the FIRST container is live.
        let stored = AudioCacheLocation.portableString(for: installed, cacheRoot: old.cacheRoot)

        // Reinstall: a brand-new container UUID holds the same artifact, and
        // the old tree is gone entirely.
        let new = try Container(uuid: "EDA59A90-C6C0-4C03-B101-7E85B3652AB3")
        defer { new.remove() }
        let reinstalled = try new.writeAudio(named: name)
        old.remove()

        let resolved = AudioCacheLocation.resolve(
            stored, cacheRoot: new.cacheRoot, isUsable: Self.anchored
        )

        #expect(resolved?.standardizedFileURL == reinstalled.standardizedFileURL)
        // The audio is genuinely readable, not merely a plausible path.
        let bytes = try Data(contentsOf: try #require(resolved))
        #expect(bytes.count == 4096)
    }

    @Test("a LEGACY absolute row from a dead container still resolves")
    func survivesContainerChangeFromLegacyAbsoluteRow() throws {
        let episodeId = "https://feed.example/rss::guid-legacy"
        let name = Self.basename(for: episodeId)

        let old = try Container(uuid: "6A65498E-7D61-4908-8D3C-F5F18720FFF3")
        let installed = try old.writeAudio(named: name)
        // Exactly what the 36 rows on the owner's device hold today.
        let stored = installed.absoluteString
        #expect(stored.hasPrefix("file:///"))
        old.remove()

        let new = try Container(uuid: "636378E8-B591-429F-9B92-7193D7077CE1")
        defer { new.remove() }
        let reinstalled = try new.writeAudio(named: name)

        let resolved = AudioCacheLocation.resolve(
            stored, cacheRoot: new.cacheRoot, isUsable: Self.anchored
        )
        #expect(resolved?.standardizedFileURL == reinstalled.standardizedFileURL)
    }

    @Test("the persisted form carries no container segment at all")
    func portableStringDropsTheContainer() throws {
        let container = try Container(uuid: "F3714031-87AB-4C7A-BA8A-A3BD12243696")
        defer { container.remove() }
        let name = Self.basename(for: "ep-portable")
        let url = try container.writeAudio(named: name)

        let stored = AudioCacheLocation.portableString(for: url, cacheRoot: container.cacheRoot)

        #expect(stored == "complete/\(name)")
        #expect(!stored.contains("F3714031-87AB-4C7A-BA8A-A3BD12243696"))
        #expect(!stored.contains("Containers"))
        #expect(!stored.contains("Caches"))
        // A reader that has NOT been taught the format must resolve nothing
        // rather than open a wrong path.
        #expect(URL(string: stored)?.isFileURL != true)
        // ...but the dedupe guard, which only ever wanted the basename, still
        // reads it correctly. See `AssetMergeRow.artifactBasename`.
        #expect(URL(string: stored)?.lastPathComponent == name)
    }

    // MARK: - Genuinely missing degrades to "not cached"

    @Test("a missing file resolves to nil rather than a path or an error")
    func missingFileIsNotCached() throws {
        let container = try Container(uuid: "A958AA51-2C69-4487-9D10-E5F9FEA26974")
        defer { container.remove() }
        let name = Self.basename(for: "ep-evicted")
        let url = try container.writeAudio(named: name)
        let stored = AudioCacheLocation.portableString(for: url, cacheRoot: container.cacheRoot)

        // iOS evicts Library/Caches at will — same container, file gone.
        try FileManager.default.removeItem(at: url)

        #expect(AudioCacheLocation.resolve(
            stored, cacheRoot: container.cacheRoot, isUsable: Self.anchored
        ) == nil)

        // Re-download restores it, and the SAME stored string finds it again:
        // the row was never stranded.
        let redownloaded = try container.writeAudio(named: name)
        #expect(AudioCacheLocation.resolve(
            stored, cacheRoot: container.cacheRoot, isUsable: Self.anchored
        )?.standardizedFileURL == redownloaded.standardizedFileURL)
    }

    @Test("a zero-byte artifact is not usable audio")
    func emptyFileIsNotCached() throws {
        let container = try Container(uuid: "00657D1A-AC03-4A19-A394-4D1B394804B3")
        defer { container.remove() }
        let name = Self.basename(for: "ep-truncated")
        try container.writeAudio(named: name, bytes: 0)

        #expect(AudioCacheLocation.resolve(
            "complete/\(name)", cacheRoot: container.cacheRoot, isUsable: Self.anchored
        ) == nil)
    }

    @Test(
        "unresolvable inputs yield nil",
        arguments: [
            "",
            "   ",
            "file://",
            "https://cdn.example.com/ep.mp3",
            "ep.mp3",
            "complete/",
            "complete/..",
            "complete/.",
            "/var/mobile/Containers/Data/Application/DEAD/Library/Caches/x.mp3"
        ]
    )
    func unresolvableInputsYieldNil(stored: String) throws {
        let container = try Container(uuid: "DBD1DD58-173F-4D60-A1EC-FE2B4EB06B54")
        defer { container.remove() }
        #expect(AudioCacheLocation.resolve(
            stored, cacheRoot: container.cacheRoot, isUsable: Self.anchored
        ) == nil)
    }

    /// The escape target is planted OUTSIDE the cache root, where a `..` that
    /// was honoured would actually land. A fixture inside the root would make
    /// the naive `suffix(2)` implementation pass by accident.
    @Test("a traversal tail cannot escape the cache root")
    func traversalCannotEscape() throws {
        let container = try Container(uuid: "AEF96D16-F54D-4206-BF4E-564AEB3040A1")
        defer { container.remove() }
        let outside = container.cacheRoot.deletingLastPathComponent()
            .appendingPathComponent("secret.bin")
        try Data(repeating: 0x42, count: 128).write(to: outside)
        #expect(AdDetectionService.isAnchoredRegularFile(outside), "control: the target is reachable")

        // The last component is the artifact name and is never a directory
        // hop, so `..` cannot be used to climb out.
        for stored in ["complete/../../secret.bin", "../secret.bin", "complete/../secret.bin"] {
            #expect(AudioCacheLocation.resolve(
                stored, cacheRoot: container.cacheRoot, isUsable: Self.anchored
            ) == nil, "\(stored) must not resolve")
        }
    }

    // MARK: - Extension drift is NOT rescued, on purpose

    /// Deliberate: guessing a sibling extension is a resolution
    /// `DownloadManager.servingURLIfComplete` would refuse — it checks the
    /// `.pin` byte length and hash and returns nil outright when two audio
    /// siblings share a stem. Handing the byte differ a truncated or stale
    /// A-side mints a wrong mark; a miss merely re-downloads.
    @Test("extension drift degrades to not-cached rather than guessing a sibling")
    func extensionDriftIsNotCached() throws {
        let container = try Container(uuid: "A0A98C22-9A20-4716-B47A-9C81738A3999")
        defer { container.remove() }
        let stem = DownloadManager.safeFilename(for: "https://feed.example/rss::guid-m4a")
        let landed = try container.writeAudio(named: "\(stem).m4a")
        #expect(AdDetectionService.isAnchoredRegularFile(landed), "control: the sibling IS on disk")

        #expect(AudioCacheLocation.resolve(
            "complete/\(stem).mp3", cacheRoot: container.cacheRoot, isUsable: Self.anchored
        ) == nil)
    }

    @Test("a pin sidecar or staging file is never mistaken for the audio")
    func ignoresNonAudioSiblings() throws {
        let container = try Container(uuid: "76435512-3E77-4BD9-A778-9B35626CAF69")
        defer { container.remove() }
        let stem = DownloadManager.safeFilename(for: "ep-pin-only")
        let complete = container.cacheRoot.appendingPathComponent("complete", isDirectory: true)
        // Everything that legitimately shares this stem in `complete/` except
        // the audio itself: the completeness pin and a background staging file.
        try Data(repeating: 0x01, count: 64)
            .write(to: complete.appendingPathComponent("\(stem).pin"))
        try Data(repeating: 0x02, count: 64)
            .write(to: complete.appendingPathComponent("\(stem).abcdef.mp3"))

        #expect(AudioCacheLocation.resolve(
            "complete/\(stem).mp3", cacheRoot: container.cacheRoot, isUsable: Self.anchored
        ) == nil)
        // ...and the stem itself is not a directory listing key: asking for the
        // pin by name must not hand back the staging file either.
        #expect(AudioCacheLocation.resolve(
            "complete/\(stem).pin", cacheRoot: container.cacheRoot, isUsable: Self.anchored
        )?.lastPathComponent == "\(stem).pin", "an exact name still resolves exactly")
    }

    // MARK: - The current container must not regress

    @Test("a live path in the CURRENT container resolves unchanged")
    func currentContainerPathStillResolves() throws {
        let container = try Container(uuid: "64FD4CF1-0EA8-455C-81DD-6FB9B693492C")
        defer { container.remove() }
        let url = try container.writeAudio(named: Self.basename(for: "ep-live"))

        // Absolute (a row written before this change, container still alive).
        #expect(AudioCacheLocation.resolve(
            url.absoluteString, cacheRoot: container.cacheRoot, isUsable: Self.anchored
        )?.standardizedFileURL == url.standardizedFileURL)
        // Relative (a row written after this change).
        #expect(AudioCacheLocation.resolve(
            AudioCacheLocation.portableString(for: url, cacheRoot: container.cacheRoot),
            cacheRoot: container.cacheRoot,
            isUsable: Self.anchored
        )?.standardizedFileURL == url.standardizedFileURL)
    }

    @Test("a file outside the cache is stored verbatim and still opens")
    func outOfCachePathsAreLeftAlone() throws {
        let container = try Container(uuid: "EDA59A90-C6C0-4C03-B101-7E85B3652AB3")
        defer { container.remove() }
        // The corpus dump harness snapshots audio outside the audio cache.
        let corpus = FileManager.default.temporaryDirectory
            .appendingPathComponent("b8hj-corpus-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: corpus, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: corpus) }
        let snapshot = corpus.appendingPathComponent("episode.m4a")
        try Data(repeating: 0x43, count: 2048).write(to: snapshot)

        let stored = AudioCacheLocation.portableString(for: snapshot, cacheRoot: container.cacheRoot)
        #expect(stored == snapshot.absoluteString)
        #expect(AudioCacheLocation.resolve(
            stored, cacheRoot: container.cacheRoot, isUsable: Self.anchored
        )?.standardizedFileURL == snapshot.standardizedFileURL)
    }

    // MARK: - Collision

    /// The load-bearing half is the ABSENT episode: only B's artifact is on
    /// disk, so an implementation that returned "whatever audio is in the
    /// directory" — or that lost the basename while re-rooting — hands A back
    /// B's file. That is the irreversible one: A's transcript fused onto B's
    /// audio.
    @Test("two episodes never resolve to the same file")
    func distinctEpisodesDoNotCollide() throws {
        let container = try Container(uuid: "BCC522DD-B26A-430D-B0F1-CCDE0D4235F9")
        defer { container.remove() }
        let a = "https://feed.example/rss::guid-a"
        let b = "https://feed.example/rss::guid-b"
        let urlB = try container.writeAudio(named: Self.basename(for: b))

        // A is not downloaded in this container. It must read as not-cached,
        // NOT as B.
        #expect(AudioCacheLocation.resolve(
            "complete/\(Self.basename(for: a))", cacheRoot: container.cacheRoot,
            isUsable: Self.anchored
        ) == nil)

        let urlA = try container.writeAudio(named: Self.basename(for: a))
        #expect(urlA != urlB)
        let resolvedA = AudioCacheLocation.resolve(
            "complete/\(Self.basename(for: a))", cacheRoot: container.cacheRoot,
            isUsable: Self.anchored
        )
        let resolvedB = AudioCacheLocation.resolve(
            "complete/\(Self.basename(for: b))", cacheRoot: container.cacheRoot,
            isUsable: Self.anchored
        )
        #expect(resolvedA?.standardizedFileURL == urlA.standardizedFileURL)
        #expect(resolvedB?.standardizedFileURL == urlB.standardizedFileURL)
        #expect(resolvedA != resolvedB)
    }

    @Test("a partials reference never resolves to the complete artifact")
    func partialsAndCompleteStayDistinct() throws {
        let container = try Container(uuid: "EDA59A90-C6C0-4C03-B101-7E85B3652AB3")
        defer { container.remove() }
        let stem = DownloadManager.safeFilename(for: "ep-partial")
        let complete = try container.writeAudio(named: "\(stem).mp3")

        try FileManager.default.createDirectory(
            at: container.cacheRoot.appendingPathComponent("partials", isDirectory: true),
            withIntermediateDirectories: true
        )
        let partial = container.cacheRoot
            .appendingPathComponent("partials", isDirectory: true)
            .appendingPathComponent("\(stem).partial")
        try Data(repeating: 0x44, count: 512).write(to: partial)

        #expect(AudioCacheLocation.resolve(
            "partials/\(stem).partial", cacheRoot: container.cacheRoot, isUsable: Self.anchored
        )?.standardizedFileURL == partial.standardizedFileURL)
        #expect(AudioCacheLocation.resolve(
            "complete/\(stem).mp3", cacheRoot: container.cacheRoot, isUsable: Self.anchored
        )?.standardizedFileURL == complete.standardizedFileURL)

        // The load-bearing case: with the partial GONE, a partials reference
        // must read as not-cached rather than falling through to the complete
        // artifact that shares its stem. A truncated-vs-complete mix-up is a
        // silently wrong A-side.
        try FileManager.default.removeItem(at: partial)
        #expect(AudioCacheLocation.resolve(
            "partials/\(stem).partial", cacheRoot: container.cacheRoot, isUsable: Self.anchored
        ) == nil)
    }

    // MARK: - Round trip

    @Test("portableString → resolve is a round trip for every cache artifact")
    func roundTrip() throws {
        let container = try Container(uuid: "636378E8-B591-429F-9B92-7193D7077CE1")
        defer { container.remove() }
        for ext in ["mp3", "m4a", "aac", "wav"] {
            let name = "\(DownloadManager.safeFilename(for: "ep-\(ext)")).\(ext)"
            let url = try container.writeAudio(named: name)
            let stored = AudioCacheLocation.portableString(for: url, cacheRoot: container.cacheRoot)
            #expect(stored == "complete/\(name)")
            #expect(AudioCacheLocation.resolve(
                stored, cacheRoot: container.cacheRoot, isUsable: Self.anchored
            )?.standardizedFileURL == url.standardizedFileURL)
        }
    }
}
