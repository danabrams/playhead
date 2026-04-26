// AssetLifecycleLogger.swift
// playhead-gtt9.8: Per-asset lifecycle logger that records one JSONL
// entry per SessionState transition.
//
// Design mirrors `DecisionLogger`:
//   - Actor-serialized append-only writer at
//     `Documents/asset-lifecycle-log.jsonl`.
//   - Rotates to `asset-lifecycle-log.N.jsonl` at 10 MB; rotation is
//     crash-safe via `FileManager.replaceItemAt` and idempotent across
//     warm starts (the next index is seeded from disk at init).
//   - Stable sorted-keys JSON encoding so the file is diff-friendly.
//   - Non-blocking: the actor serializes writes and the ingest call
//     returns as soon as the continuation is scheduled.
//
// Telemetry purpose: for every asset we capture the SessionState
// trajectory (spooling → featuresReady → hotPathReady → backfill →
// completeFull, and every failure/cancellation terminal) so the NARL
// harness and the counterfactual eval tooling can reconstruct
// per-asset lifecycle timelines without querying the SQLite store.
//
// Schema version: 2 (playhead-gtt9.14 adds optional scheduler-state fields).

import Foundation
import OSLog

// MARK: - AssetLifecycleLoggerProtocol

protocol AssetLifecycleLoggerProtocol: Sendable {
    /// Append a single lifecycle record. Must not block the caller
    /// beyond the actor hop; file I/O is serialized inside the actor.
    func record(_ entry: AssetLifecycleLogEntry) async
}

// MARK: - SchedulerStateSnapshotProviding (playhead-gtt9.14)

/// Read-only snapshot surface the `AnalysisCoordinator` consults so the
/// lifecycle log can record the scheduler's (scenePhase,
/// playbackContext, qualityProfile) triple at every state transition.
///
/// The return type intentionally uses string encodings that match the
/// `AssetLifecycleLogEntry` JSON fields directly — the protocol can
/// live in any actor context (the runtime-level implementation hops
/// onto the scheduler actor, the test implementation is plain
/// `@unchecked Sendable`) without leaking scheduler-internal types.
///
/// A `nil` return means "no snapshot available" and the coordinator
/// records v2 entries with the fields left as nil, which is the
/// backwards-compatible no-op path.
protocol SchedulerStateSnapshotProviding: Sendable {
    /// Snapshot of the scheduler's admission inputs. All fields are
    /// optional so a provider can report partial state; the logger
    /// stores what it has and records nil for the rest.
    func schedulerStateSnapshot() async -> SchedulerStateSnapshot
}

/// Transport type for `SchedulerStateSnapshotProviding`. The three
/// fields mirror the v2 JSON keys on `AssetLifecycleLogEntry`.
struct SchedulerStateSnapshot: Sendable, Equatable {
    let scenePhase: String?
    let playbackContext: String?
    let qualityProfile: String?

    static let empty = SchedulerStateSnapshot(
        scenePhase: nil,
        playbackContext: nil,
        qualityProfile: nil
    )
}

/// Adapter that forwards `schedulerStateSnapshot()` calls into an
/// `AnalysisWorkScheduler`. Lives in the logger file to keep the
/// snapshot transport + its canonical producer in one compilation
/// unit; the scheduler itself remains free of
/// `SchedulerStateSnapshotProviding` conformance so tests that don't
/// need the adapter are unaffected.
struct AnalysisWorkSchedulerStateSnapshotAdapter: SchedulerStateSnapshotProviding {
    let scheduler: AnalysisWorkScheduler

    func schedulerStateSnapshot() async -> SchedulerStateSnapshot {
        await scheduler.currentSchedulerStateSnapshot()
    }
}

/// No-op logger for release builds, tests that don't exercise logging,
/// and code paths that run before the logger is wired (e.g. the early
/// store migration phase).
struct NoOpAssetLifecycleLogger: AssetLifecycleLoggerProtocol {
    func record(_ entry: AssetLifecycleLogEntry) async {
        // intentionally blank
    }
}

// MARK: - AssetLifecycleLogEntry

/// Schema-versioned, Codable record for one SessionState transition on
/// a single analysis asset. All numeric coverage fields are in
/// seconds; `timestamp` is Unix epoch seconds.
struct AssetLifecycleLogEntry: Codable, Equatable, Sendable {
    /// Schema version. Increment on breaking changes.
    /// - v1 (gtt9.8): initial shape.
    /// - v2 (gtt9.14): adds `schedulerScenePhase`, `schedulerPlaybackContext`,
    ///   `schedulerQualityProfile`. All optional — legacy v1 JSON decodes
    ///   cleanly with the new fields defaulting to `nil`.
    let schemaVersion: Int
    /// `AnalysisAsset.id` the transition applies to.
    let analysisAssetID: String
    /// `AnalysisSession.id` that drove the transition.
    let sessionID: String
    /// Wall-clock time at which the transition was observed.
    let timestamp: Double
    /// `SessionState.rawValue` the session came from. Empty string on
    /// the first insert.
    let fromState: String
    /// `SessionState.rawValue` the session moved to.
    let toState: String
    /// Optional human-readable classifier reason when `toState` is a
    /// terminal (see `AnalysisCoordinator.classifyBackfillTerminal`).
    let terminalReason: String?
    /// Episode duration if known (from the resolver). 0 when unknown.
    let episodeDurationSec: Double
    /// Feature coverage end time at transition (from
    /// `AnalysisAsset.featureCoverageEndTime`). Nil when unknown.
    let featureCoverageEndSec: Double?
    /// Transcript coverage end time at transition. Nil when no
    /// chunks have been written yet.
    let transcriptCoverageEndSec: Double?

    /// playhead-gtt9.14: scheduler scene-phase projection at the
    /// moment of transition. `"foreground"` or `"background"`. Nil when
    /// no scheduler state was captured (e.g. tests, pre-v2 readers
    /// re-encoding a v1 row).
    let schedulerScenePhase: String?
    /// playhead-gtt9.14: transport-level playback context at the moment
    /// of transition. `"playing"`, `"paused"`, or `"idle"`. Nil when not
    /// captured.
    let schedulerPlaybackContext: String?
    /// playhead-gtt9.14: `QualityProfile.rawValue` applied by the
    /// admission gate at the moment of transition. Nil when not
    /// captured. Useful for post-ship bucketing of stranded-asset
    /// traces by thermal state.
    let schedulerQualityProfile: String?

    static let currentSchemaVersion: Int = 2

    /// Convenience init: captures all v2 fields. Defaulting the v2 fields
    /// to `nil` preserves call-site compatibility with existing v1 test
    /// factories and the `AnalysisCoordinator` transition path before
    /// scheduler snapshots are plumbed end-to-end.
    init(
        schemaVersion: Int,
        analysisAssetID: String,
        sessionID: String,
        timestamp: Double,
        fromState: String,
        toState: String,
        terminalReason: String?,
        episodeDurationSec: Double,
        featureCoverageEndSec: Double?,
        transcriptCoverageEndSec: Double?,
        schedulerScenePhase: String? = nil,
        schedulerPlaybackContext: String? = nil,
        schedulerQualityProfile: String? = nil
    ) {
        self.schemaVersion = schemaVersion
        self.analysisAssetID = analysisAssetID
        self.sessionID = sessionID
        self.timestamp = timestamp
        self.fromState = fromState
        self.toState = toState
        self.terminalReason = terminalReason
        self.episodeDurationSec = episodeDurationSec
        self.featureCoverageEndSec = featureCoverageEndSec
        self.transcriptCoverageEndSec = transcriptCoverageEndSec
        self.schedulerScenePhase = schedulerScenePhase
        self.schedulerPlaybackContext = schedulerPlaybackContext
        self.schedulerQualityProfile = schedulerQualityProfile
    }
}

// MARK: - AssetLifecycleLogger

/// Actor-backed JSONL writer for per-asset lifecycle transitions.
actor AssetLifecycleLogger: AssetLifecycleLoggerProtocol {

    /// Default rotation threshold (10 MB).
    static let defaultRotationThresholdBytes: Int = 10 * 1024 * 1024

    /// Active log file basename.
    static let activeLogFilename: String = "asset-lifecycle-log.jsonl"

    /// Prefix for rotated files: `asset-lifecycle-log.N.jsonl`.
    static let rotatedPrefix: String = "asset-lifecycle-log"
    static let rotatedSuffix: String = ".jsonl"

    /// playhead-jncn: lazy-resolved directory. The convenience init
    /// (no `directory:` arg) used to call
    /// `FileManager.url(.documentDirectory, create: true)` synchronously
    /// inside `PlayheadRuntime.init`; that lookup now defers to
    /// `ensureBootstrapped()`.
    private let directoryOverride: URL?
    private var resolvedDirectory: URL?

    private let rotationThresholdBytes: Int
    private let encoder: JSONEncoder
    private let logger = Logger(subsystem: "com.playhead", category: "AssetLifecycleLogger")

    /// Next rotation index; seeded from disk on first use. Optional
    /// until then so we can distinguish "haven't bootstrapped" from
    /// "no rotated files on disk" (which produces 1).
    private var nextRotationIndex: Int?
    private var fileHandle: FileHandle?

    // MARK: - Init

    /// Convenience init targeting the app's Documents directory.
    ///
    /// playhead-jncn: the Documents lookup + directory create are
    /// deferred to first use. Production callers
    /// `await logger.migrate()` from `PlayheadRuntime`'s deferred init
    /// Task to warm the path off-main.
    init(
        rotationThresholdBytes: Int = AssetLifecycleLogger.defaultRotationThresholdBytes
    ) throws {
        // playhead-jncn: store rotation threshold + encoder only; defer
        // the Documents lookup, directory create, and rotation-index
        // scan to `ensureBootstrapped()`. The `throws` on this init is
        // preserved so the call-site shape stays compatible, but no
        // failure path actually runs here today.
        self.directoryOverride = nil
        self.rotationThresholdBytes = rotationThresholdBytes
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder
        self.resolvedDirectory = nil
        self.nextRotationIndex = nil
    }

    /// Designated init. Tests pass an arbitrary directory and a small
    /// rotation threshold.
    ///
    /// playhead-jncn: directory create + scanNextRotationIndex are
    /// deferred to `ensureBootstrapped()`. Tests that read back the log
    /// file pay the lazy bootstrap on their first `record(_:)` call,
    /// matching the production path.
    init(
        directory: URL,
        rotationThresholdBytes: Int = AssetLifecycleLogger.defaultRotationThresholdBytes
    ) throws {
        self.directoryOverride = directory
        self.rotationThresholdBytes = rotationThresholdBytes
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder
        self.resolvedDirectory = nil
        self.nextRotationIndex = nil
    }

    /// playhead-jncn: lazy first-use bootstrap. Resolves the directory
    /// (Documents lookup for the convenience init), creates it, and
    /// seeds `nextRotationIndex` from disk. Idempotent.
    /// `PlayheadRuntime` calls this from the deferred init Task so the
    /// expensive setup runs off-main.
    func migrate() throws {
        try ensureBootstrapped()
    }

    /// Lazy bootstrap shared by `migrate()` and the write path.
    /// Idempotent on the (resolvedDirectory, nextRotationIndex) tuple.
    private func ensureBootstrapped() throws {
        if resolvedDirectory == nil {
            let dir: URL
            if let override = directoryOverride {
                dir = override
            } else {
                dir = try FileManager.default.url(
                    for: .documentDirectory,
                    in: .userDomainMask,
                    appropriateFor: nil,
                    create: true
                )
            }
            try FileManager.default.createDirectory(
                at: dir, withIntermediateDirectories: true
            )
            self.resolvedDirectory = dir
        }
        if nextRotationIndex == nil, let dir = resolvedDirectory {
            self.nextRotationIndex = Self.scanNextRotationIndex(in: dir)
        }
    }

    // MARK: - Public API

    func record(_ entry: AssetLifecycleLogEntry) async {
        do {
            try ensureBootstrapped()
            try appendEntry(entry)
            try rotateIfNeeded()
        } catch {
            logger.warning("AssetLifecycleLogger.record failed: \(error.localizedDescription, privacy: .public)")
        }
    }

    // MARK: - Test hooks

    /// Returns the currently-scheduled rotation index. Tests only.
    /// Triggers the lazy bootstrap so tests that probe pre-write
    /// observe the seeded value.
    func currentNextRotationIndex() -> Int {
        try? ensureBootstrapped()
        return nextRotationIndex ?? 1
    }

    /// Force-close the handle; tests call this before reading the file
    /// back through `Data(contentsOf:)` so pending writes flush.
    func flushAndClose() {
        closeHandle()
    }

    /// Absolute path to the currently-active log file. Triggers the
    /// lazy bootstrap on first call so callers can rely on the path.
    var activeLogURL: URL {
        try? ensureBootstrapped()
        let dir = resolvedDirectory ?? directoryOverride ?? URL(fileURLWithPath: NSTemporaryDirectory())
        return dir.appendingPathComponent(Self.activeLogFilename)
    }

    /// All rotated log URLs, ordered by numeric index.
    func rotatedLogURLs() -> [URL] {
        try? ensureBootstrapped()
        guard let dir = resolvedDirectory else { return [] }
        return Self.listRotatedLogs(in: dir)
    }

    // MARK: - Internal

    private func appendEntry(_ entry: AssetLifecycleLogEntry) throws {
        let data = try encoder.encode(entry)
        var line = Data()
        line.reserveCapacity(data.count + 1)
        line.append(data)
        line.append(0x0A) // newline
        try write(line)
    }

    private func write(_ data: Data) throws {
        let url = activeLogURL
        if fileHandle == nil {
            if !FileManager.default.fileExists(atPath: url.path) {
                FileManager.default.createFile(atPath: url.path, contents: nil)
            }
            fileHandle = try FileHandle(forWritingTo: url)
            try fileHandle?.seekToEnd()
        }
        try fileHandle?.write(contentsOf: data)
    }

    private func rotateIfNeeded() throws {
        let url = activeLogURL
        let attrs = try FileManager.default.attributesOfItem(atPath: url.path)
        let size = (attrs[.size] as? NSNumber)?.intValue ?? 0
        guard size >= rotationThresholdBytes else { return }

        // Livelock guard (see DecisionLogger): if a single record is
        // larger than the threshold, rotation would produce a fresh
        // file that's still oversized. Skip rather than loop.
        if try lineCount(at: url) <= 1 {
            logger.warning(
                "AssetLifecycleLogger: active log exceeds threshold but has \u{2264}1 line; skipping rotation"
            )
            return
        }
        try rotateNow()
    }

    private func lineCount(at url: URL) throws -> Int {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var count = 0
        while let chunk = try handle.read(upToCount: 64 * 1024), !chunk.isEmpty {
            for byte in chunk where byte == 0x0A {
                count += 1
                if count >= 2 { return count }
            }
        }
        return count
    }

    private func rotateNow() throws {
        // playhead-jncn: callers come through `record(_:)` which always
        // calls `ensureBootstrapped()` first, but re-run defensively.
        try ensureBootstrapped()
        guard let dir = resolvedDirectory, let idx = nextRotationIndex else {
            return
        }
        let src = activeLogURL
        let dstName = "\(Self.rotatedPrefix).\(idx)\(Self.rotatedSuffix)"
        let dst = dir.appendingPathComponent(dstName)

        closeHandle()

        let fm = FileManager.default
        if fm.fileExists(atPath: dst.path) {
            _ = try fm.replaceItemAt(dst, withItemAt: src)
        } else {
            try fm.moveItem(at: src, to: dst)
        }
        nextRotationIndex = idx + 1
        logger.info("AssetLifecycleLogger: rotated active log to \(dstName, privacy: .public)")
    }

    private func closeHandle() {
        if let handle = fileHandle {
            try? handle.close()
            fileHandle = nil
        }
    }

    // MARK: - Static helpers

    fileprivate static func scanNextRotationIndex(in directory: URL) -> Int {
        listRotatedLogs(in: directory)
            .compactMap { extractRotationIndex(from: $0.lastPathComponent) }
            .max()
            .map { $0 + 1 } ?? 1
    }

    fileprivate static func listRotatedLogs(in directory: URL) -> [URL] {
        guard let items = try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else { return [] }
        let matches = items.filter { url in
            extractRotationIndex(from: url.lastPathComponent) != nil
        }
        return matches.sorted { lhs, rhs in
            let li = extractRotationIndex(from: lhs.lastPathComponent) ?? 0
            let ri = extractRotationIndex(from: rhs.lastPathComponent) ?? 0
            return li < ri
        }
    }

    fileprivate static func extractRotationIndex(from name: String) -> Int? {
        let prefix = rotatedPrefix + "."
        let suffix = rotatedSuffix
        guard name.hasPrefix(prefix), name.hasSuffix(suffix) else { return nil }
        let middle = name.dropFirst(prefix.count).dropLast(suffix.count)
        return Int(middle)
    }
}
