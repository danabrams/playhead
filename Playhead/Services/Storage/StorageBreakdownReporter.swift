// StorageBreakdownReporter.swift
// Lightweight, testable reporter for the two storage categories surfaced in
// Settings → Storage: cached audio (downloaded media) and the on-device
// transcript / analysis database. Also reports the device's available
// capacity so the UI can show "X available" alongside the breakdown.
//
// Scope (playhead-j2u):
//   * NO model-files category — playhead-c6r removed external model
//     manifests; on-device speech assets are managed by iOS and we don't
//     track their footprint.
//   * Per-category byte totals computed via `URLResourceKey
//     .totalFileAllocatedSizeKey` so the numbers reflect actual on-disk
//     usage (clusters/sparse files), not nominal file sizes.
//   * Pure value type + injectable directory paths, so unit tests can
//     point the reporter at a temp tree of known sizes and assert the
//     totals match within the spec's 1 MB tolerance.
//
// Threading: `measure()` is sync but performs file-system enumeration,
// so call sites should hop to a background context (e.g. `Task.detached`
// or the `SettingsViewModel`'s `@MainActor` task scheduler).

import Foundation

// MARK: - StorageBreakdown

/// Snapshot of disk usage relevant to Settings → Storage.
///
/// All sizes are in bytes. `Int64.zero` when the directory does not exist
/// or could not be enumerated; the reporter never throws so the UI always
/// has something to render.
struct StorageBreakdown: Sendable, Equatable {
    /// Total bytes consumed by downloaded podcast audio.
    let cachedAudioBytes: Int64
    /// Total bytes consumed by the on-device transcript / analysis store
    /// (analysis shard cache + the SQLite database file).
    let transcriptDatabaseBytes: Int64
    /// Bytes available on the volume backing the app's container.
    /// `nil` when the value could not be queried (e.g. simulator with
    /// volume metadata stripped).
    let deviceAvailableBytes: Int64?

    /// Sum of the per-category totals.
    var totalBytes: Int64 {
        cachedAudioBytes + transcriptDatabaseBytes
    }

    static let empty = StorageBreakdown(
        cachedAudioBytes: 0,
        transcriptDatabaseBytes: 0,
        deviceAvailableBytes: nil
    )
}

// MARK: - StorageBreakdownReporter

/// Computes a `StorageBreakdown` from a small, injectable set of source
/// directories. The default initializer wires the production paths
/// (`DownloadManager.defaultCacheDirectory()` + the analysis-shards dir
/// in Application Support); tests use the explicit initializer to point
/// at a temp tree.
struct StorageBreakdownReporter: Sendable {

    /// Roots scanned for the "cached audio" category.
    let audioDirectories: [URL]
    /// Roots scanned for the "transcript database" category.
    let transcriptDirectories: [URL]
    /// Volume probed for the device-available figure. Defaults to the
    /// caches directory's volume.
    let volumeProbeURL: URL

    init(
        audioDirectories: [URL],
        transcriptDirectories: [URL],
        volumeProbeURL: URL
    ) {
        self.audioDirectories = audioDirectories
        self.transcriptDirectories = transcriptDirectories
        self.volumeProbeURL = volumeProbeURL
    }

    /// Production wiring — uses the live download manager's cache dir
    /// and the analysis-shards directory in Application Support.
    static func live() -> StorageBreakdownReporter {
        let appSupport = FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let analysisShards = appSupport.appendingPathComponent(
            "AnalysisShards", isDirectory: true
        )
        // The analysis SQLite DB sits alongside other Application Support
        // contents; the AnalysisStore file is named `analysis.sqlite` in
        // the live runtime. We scan the file directly when present, plus
        // its sibling -wal/-shm files, by enumerating Application Support
        // for any `analysis.sqlite*` matches.
        let analysisDBParent = appSupport
        let caches = FileManager.default
            .urls(for: .cachesDirectory, in: .userDomainMask)[0]
        return StorageBreakdownReporter(
            audioDirectories: [DownloadManager.defaultCacheDirectory()],
            transcriptDirectories: [analysisShards, analysisDBParent],
            volumeProbeURL: caches
        )
    }

    /// Compute the breakdown. Never throws: failures degrade to zero.
    func measure() -> StorageBreakdown {
        let audio = audioDirectories.reduce(into: Int64.zero) { acc, dir in
            acc += Self.directorySize(at: dir)
        }
        // Transcript directories may overlap (Application Support root +
        // a subdirectory of it). Filter the SQLite-DB scan to only count
        // `analysis.sqlite*` files so we don't double-count the
        // AnalysisShards subtree under the broader Application Support
        // walk.
        let transcript = transcriptDirectories.reduce(into: Int64.zero) { acc, dir in
            acc += Self.transcriptCategorySize(at: dir)
        }
        return StorageBreakdown(
            cachedAudioBytes: audio,
            transcriptDatabaseBytes: transcript,
            deviceAvailableBytes: Self.deviceAvailable(at: volumeProbeURL)
        )
    }

    // MARK: - Internal helpers

    /// Recursively sum the allocated size of every regular file under `url`.
    /// Hidden files are skipped.
    static func directorySize(at url: URL) -> Int64 {
        let fm = FileManager.default
        guard fm.fileExists(atPath: url.path) else { return 0 }

        guard let enumerator = fm.enumerator(
            at: url,
            includingPropertiesForKeys: [
                .totalFileAllocatedSizeKey,
                .fileAllocatedSizeKey,
                .isRegularFileKey,
            ],
            options: [.skipsHiddenFiles]
        ) else { return 0 }

        var total: Int64 = 0
        for case let fileURL as URL in enumerator {
            guard let values = try? fileURL.resourceValues(
                forKeys: [
                    .totalFileAllocatedSizeKey,
                    .fileAllocatedSizeKey,
                    .isRegularFileKey,
                ]
            ),
                  values.isRegularFile == true else { continue }
            // Prefer `totalFileAllocatedSize` (exact on-disk) and fall back
            // to `fileAllocatedSize` (also clusters); both are more
            // accurate than nominal `fileSize` for the purpose of "what
            // would 'Clear' actually free up?".
            if let n = values.totalFileAllocatedSize {
                total += Int64(n)
            } else if let n = values.fileAllocatedSize {
                total += Int64(n)
            }
        }
        return total
    }

    /// Specialized walk for the transcript-DB roots. Sums only files
    /// matching the analysis-store + analysis-shards conventions so a
    /// scan of the broader Application Support directory doesn't sweep
    /// in unrelated app state.
    static func transcriptCategorySize(at url: URL) -> Int64 {
        let fm = FileManager.default
        guard fm.fileExists(atPath: url.path) else { return 0 }

        // If the URL itself is a directory ending in "AnalysisShards",
        // include every regular file underneath.
        if url.lastPathComponent == "AnalysisShards" {
            return directorySize(at: url)
        }

        // Otherwise treat `url` as the parent of the SQLite DB; sum any
        // top-level files matching `analysis.sqlite*`.
        var total: Int64 = 0
        if let entries = try? fm.contentsOfDirectory(
            at: url,
            includingPropertiesForKeys: [
                .totalFileAllocatedSizeKey,
                .fileAllocatedSizeKey,
                .isRegularFileKey,
            ],
            options: [.skipsHiddenFiles]
        ) {
            for entry in entries
            where entry.lastPathComponent.hasPrefix("analysis.sqlite") {
                guard let values = try? entry.resourceValues(
                    forKeys: [
                        .totalFileAllocatedSizeKey,
                        .fileAllocatedSizeKey,
                        .isRegularFileKey,
                    ]
                ),
                      values.isRegularFile == true else { continue }
                if let n = values.totalFileAllocatedSize {
                    total += Int64(n)
                } else if let n = values.fileAllocatedSize {
                    total += Int64(n)
                }
            }
        }
        return total
    }

    /// Free space on the volume backing `url`. Returns `nil` when the
    /// volume metadata cannot be queried.
    static func deviceAvailable(at url: URL) -> Int64? {
        let keys: Set<URLResourceKey> = [
            .volumeAvailableCapacityForImportantUsageKey,
            .volumeAvailableCapacityKey,
        ]
        guard let values = try? url.resourceValues(forKeys: keys) else { return nil }
        // `volumeAvailableCapacityForImportantUsage` reflects the system's
        // actual available-for-app figure (purgeable caches + free); when
        // unavailable (older OS, simulator), fall back to the raw capacity.
        if let importantUsage = values.volumeAvailableCapacityForImportantUsage {
            return Int64(importantUsage)
        }
        if let raw = values.volumeAvailableCapacity {
            return Int64(raw)
        }
        return nil
    }
}
