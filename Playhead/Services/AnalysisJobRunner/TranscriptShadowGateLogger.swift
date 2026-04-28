// TranscriptShadowGateLogger.swift
// playhead-gtt9.1: per-shard structured-event sink for the shadow-mode
// acoustic-likelihood transcript gate.
//
// `AnalysisJobRunner` emits one `TranscriptShadowGateEntry` per shard it
// considers for transcription. Entries carry the per-shard likelihood, the
// gate threshold, and the categorical decision (would-skip /
// safety-sample-keep / quality-precondition-keep / above-threshold /
// score-unknown) plus a `transcribed: Bool` recording whether the shard
// actually reached the transcript engine. Replay tooling reads these rows
// to compute would-have-skipped recall against host-read ground truth
// before the team flips `AcousticTranscriptGateConfig.skipEnabled` to true.
//
// Why a separate sink (vs. piggy-backing on `DecisionLogger`)? `DecisionLogger`'s
// schema is rigid — every row is a per-window classifier decision with a fused-
// confidence breakdown. Stuffing shard-level scheduling decisions into that
// schema would either bloat every existing window record with empty optionals
// or break replay tooling that joins on `windowBounds`. A dedicated narrow
// schema lets shadow-eval consumers parse the JSONL stream without the
// noise from the much larger window-decision corpus.

import Foundation
import OSLog

// MARK: - TranscriptShadowGateEntry (JSONL record schema)

/// Schema-versioned, Codable record for one shadow-gate decision.
///
/// One row is emitted per shard the runner considers in
/// `evaluateAcousticTranscriptGate`. Fields mirror the inputs to the
/// gate decision so the eval pipeline can reproduce it offline:
///   * `likelihood` is the `AcousticLikelihoodScorer.maxLikelihoodInSpan`
///     output for the shard's `[startTime, endTime)` span. `nil` when no
///     persisted `feature_windows` row overlapped the shard.
///   * `threshold` is the active `likelihoodThreshold` at decision time.
///   * `decision` records the categorical outcome.
///   * `wouldGate` is `true` iff the shard would be skipped under
///     production-skip mode (i.e. `decision == .wouldSkip`). The
///     `safety-sample-keep` arm reports `wouldGate=true` with
///     `transcribed=true` so eval can distinguish "kept by sampling"
///     from "kept because likelihood passed the threshold".
///   * `transcribed` is `true` iff the shard was actually handed to the
///     transcript engine. In shadow mode this is `true` for every row
///     except the production-skip + would-skip case (which never fires
///     under default config).
struct TranscriptShadowGateEntry: Codable, Equatable, Sendable {

    /// Schema version; increment on breaking changes. Current: 2.
    let schemaVersion: Int

    /// Unix time at which the decision was emitted (seconds since epoch).
    let timestamp: Double

    /// Analysis-asset content fingerprint. Joins to `AnalysisAsset.id`.
    let analysisAssetID: String

    /// Episode identifier carried by the originating
    /// `AnalysisRangeRequest`. Useful for cross-episode rollups during
    /// shadow-eval (the asset id is per-fingerprint, not per-episode).
    let episodeID: String

    /// Shard identifier — `AnalysisShard.id` is unique within an episode.
    let shardID: Int

    /// Shard span start in episode-relative seconds.
    let shardStart: Double

    /// Shard span end in episode-relative seconds (exclusive).
    let shardEnd: Double

    /// Acoustic likelihood produced by `maxLikelihoodInSpan`. `nil` when
    /// no overlapping `feature_windows` row was persisted at the moment
    /// the gate ran — typically a fresh feature-extraction race or a
    /// `feature_version` skew. Eval treats `nil` as "score unknown" and
    /// never counts that shard in the would-skip rate.
    let likelihood: Double?

    /// Active `likelihoodThreshold` at decision time. Captured per-row so
    /// eval can detect threshold drift in the historical corpus.
    let threshold: Double

    /// Categorical outcome — see the `Decision` enum.
    let decision: Decision

    /// True iff the shard would be withheld from the transcript engine
    /// under production-skip mode. The `safety-sample-keep` arm reports
    /// `wouldGate=true, transcribed=true` so consumers can distinguish
    /// "kept by sampling" from "kept because likelihood passed".
    let wouldGate: Bool

    /// True iff the shard was actually handed to the transcript engine.
    /// Shadow-mode rows carry `transcribed=true` for every category;
    /// production-skip rows carry `transcribed=false` only for `.wouldSkip`.
    let transcribed: Bool

    /// Short git SHA stamped at logger init from `BuildInfo.commitSHA`.
    /// Always set on v2 rows (falls back to `"unknown"` outside a git
    /// context per the `BuildInfo` contract). Decodes as `nil` on v1
    /// rows so pre-bump captures round-trip cleanly.
    let buildCommitSHA: String?

    static let currentSchemaVersion: Int = 2

    // playhead-b58j: explicit Codable so v1 rows (no buildCommitSHA key)
    // decode cleanly with `buildCommitSHA = nil`. v2 always emits the
    // key (even when nil → JSON null) so consumers self-identify the
    // capture cohort. Mirrors the DecisionLogEntry.LedgerEntry pattern
    // from playhead-epfk.
    private enum CodingKeys: String, CodingKey {
        case schemaVersion, timestamp, analysisAssetID, episodeID,
             shardID, shardStart, shardEnd, likelihood, threshold,
             decision, wouldGate, transcribed, buildCommitSHA
    }

    init(
        schemaVersion: Int,
        timestamp: Double,
        analysisAssetID: String,
        episodeID: String,
        shardID: Int,
        shardStart: Double,
        shardEnd: Double,
        likelihood: Double?,
        threshold: Double,
        decision: Decision,
        wouldGate: Bool,
        transcribed: Bool,
        buildCommitSHA: String?
    ) {
        self.schemaVersion = schemaVersion
        self.timestamp = timestamp
        self.analysisAssetID = analysisAssetID
        self.episodeID = episodeID
        self.shardID = shardID
        self.shardStart = shardStart
        self.shardEnd = shardEnd
        self.likelihood = likelihood
        self.threshold = threshold
        self.decision = decision
        self.wouldGate = wouldGate
        self.transcribed = transcribed
        self.buildCommitSHA = buildCommitSHA
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.schemaVersion = try c.decode(Int.self, forKey: .schemaVersion)
        self.timestamp = try c.decode(Double.self, forKey: .timestamp)
        self.analysisAssetID = try c.decode(String.self, forKey: .analysisAssetID)
        self.episodeID = try c.decode(String.self, forKey: .episodeID)
        self.shardID = try c.decode(Int.self, forKey: .shardID)
        self.shardStart = try c.decode(Double.self, forKey: .shardStart)
        self.shardEnd = try c.decode(Double.self, forKey: .shardEnd)
        self.likelihood = try c.decodeIfPresent(Double.self, forKey: .likelihood)
        self.threshold = try c.decode(Double.self, forKey: .threshold)
        self.decision = try c.decode(Decision.self, forKey: .decision)
        self.wouldGate = try c.decode(Bool.self, forKey: .wouldGate)
        self.transcribed = try c.decode(Bool.self, forKey: .transcribed)
        // playhead-b58j: pre-bump (v1) rows omit the key → nil.
        self.buildCommitSHA = try c.decodeIfPresent(String.self, forKey: .buildCommitSHA)
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(schemaVersion, forKey: .schemaVersion)
        try c.encode(timestamp, forKey: .timestamp)
        try c.encode(analysisAssetID, forKey: .analysisAssetID)
        try c.encode(episodeID, forKey: .episodeID)
        try c.encode(shardID, forKey: .shardID)
        try c.encode(shardStart, forKey: .shardStart)
        try c.encode(shardEnd, forKey: .shardEnd)
        try c.encode(likelihood, forKey: .likelihood)
        try c.encode(threshold, forKey: .threshold)
        try c.encode(decision, forKey: .decision)
        try c.encode(wouldGate, forKey: .wouldGate)
        try c.encode(transcribed, forKey: .transcribed)
        // Always emit (even when nil → JSON null) so v2 rows are wire-
        // distinguishable from v1.
        try c.encode(buildCommitSHA, forKey: .buildCommitSHA)
    }

    /// Categorical decision for a shadow-gate evaluation.
    enum Decision: String, Codable, Equatable, Sendable {
        /// Likelihood ≥ threshold. The acoustic prior says "transcribe."
        case aboveThreshold

        /// Likelihood < threshold but the safety-sample coin came up
        /// heads. The shard is transcribed anyway so we keep a calibration
        /// stream of low-likelihood ground truth even after `skipEnabled`
        /// flips.
        case safetySampleKeep

        /// Likelihood < threshold and the safety-sample coin came up
        /// tails. In shadow mode the shard is still transcribed; in
        /// production-skip mode it is dropped from the engine input.
        case wouldSkip

        /// Asset's persisted fast-transcript watermark already covers
        /// this shard — we're re-running over good transcript. M1
        /// mitigation: never gate out a shard whose region already has
        /// transcript chunks the rest of the pipeline depends on.
        case qualityPreconditionKeep

        /// No `feature_windows` row overlapped the shard at decision
        /// time. Defensive: never gate out unknowns.
        case scoreUnknown
    }
}

// MARK: - TranscriptShadowGateLogging

/// Protocol seam for the shadow-gate sink. The release build installs
/// `NoOpTranscriptShadowGateLogger`; DEBUG/dogfood builds install the
/// real JSONL writer (added in a follow-up wiring bead — production has
/// no consumer yet). Tests inject a recording stub.
protocol TranscriptShadowGateLogging: Sendable {
    /// Append a single shadow-gate record. Must not block the caller
    /// beyond the actor hop; file I/O is serialized inside the
    /// implementation when one is configured.
    func record(_ entry: TranscriptShadowGateEntry) async
}

/// Default logger used in production until the dogfood-write bead lands.
/// Every `record(_:)` call is dropped silently — the runner still pays
/// the ~hundred-byte allocation per shard, but no I/O happens.
struct NoOpTranscriptShadowGateLogger: TranscriptShadowGateLogging {
    func record(_ entry: TranscriptShadowGateEntry) async {
        // intentionally blank
    }
}

// MARK: - TranscriptShadowGateLogger (actor-backed JSONL writer)

/// Actor-backed JSONL writer for shadow-mode transcript-gate decisions.
/// DEBUG-only by convention — `PlayheadRuntime` gates construction
/// behind `#if DEBUG` so release builds never write to disk.
///
/// Mechanical clone of `DecisionLogger`: lazy `migrate()` bootstrap,
/// 10 MB rotation with crash-safe `replaceItemAt` swap, livelock guard
/// for >threshold single-line records, idempotent rotation-index scan.
/// See `DecisionLogger.swift` for the canonical design notes.
///
/// Every encoded row is stamped with `BuildInfo.commitSHA` so eval
/// tooling can correlate captures with the exact binary that produced
/// them. Callers pass `buildCommitSHA: nil`; the actor overwrites in
/// `appendEntry` before encoding.
///
/// Durability: best-effort, **not crash-durable**. We don't fsync after
/// every record (the eval pipeline tolerates loss of the final
/// in-flight rows on a crash, and a per-shard fsync would dominate
/// shadow-mode CPU on cold caches). The handle is closed on
/// `flushAndClose()` and on rotation, both of which trigger an
/// implicit flush of any user-space buffering. Callers needing
/// stronger guarantees should call `flushAndClose()` and reopen
/// (review playhead-rfu-aac M3).
actor TranscriptShadowGateLogger: TranscriptShadowGateLogging {

    static let defaultRotationThresholdBytes: Int = 10 * 1024 * 1024
    static let activeLogFilename: String = "transcript-shadow-gate.jsonl"
    static let rotatedPrefix: String = "transcript-shadow-gate"
    static let rotatedSuffix: String = ".jsonl"

    private let directoryOverride: URL?
    private var resolvedDirectory: URL?

    private let rotationThresholdBytes: Int
    private let buildCommitSHA: String
    private let encoder: JSONEncoder
    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptShadowGateLogger")

    private var nextRotationIndex: Int?
    private var fileHandle: FileHandle?

    // MARK: - Init

    /// Convenience init that targets `FileManager.default
    /// .urls(for: .documentDirectory, in: .userDomainMask)[0]`.
    /// Documents lookup + directory create are deferred to first use;
    /// production callers `await logger.migrate()` from `PlayheadRuntime`'s
    /// deferred init Task to warm the path off-main.
    init(rotationThresholdBytes: Int = TranscriptShadowGateLogger.defaultRotationThresholdBytes) throws {
        self.directoryOverride = nil
        self.rotationThresholdBytes = rotationThresholdBytes
        // Capture the build SHA once at actor init — callers pass `nil` and
        // we overwrite in `appendEntry`. Reading at init avoids hot-path
        // repeats and keeps the eval pipeline correlation deterministic.
        self.buildCommitSHA = BuildInfo.commitSHA
        // playhead-rfu-aac L2: no field on TranscriptShadowGateEntry is a
        // Date — `timestamp` is a `Double`. The previous `.iso8601`
        // dateEncodingStrategy was load-bearing only by accident (it set a
        // policy that no encoder path ever invoked). Drop it to remove the
        // dead config and avoid confusing future maintainers who might
        // expect schema fields to round-trip as ISO-8601 strings.
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        self.encoder = encoder
        self.resolvedDirectory = nil
        self.nextRotationIndex = nil
    }

    /// Designated init for testing: points the logger at an arbitrary
    /// directory. The directory is created on first use, not at init.
    init(
        directory: URL,
        rotationThresholdBytes: Int = TranscriptShadowGateLogger.defaultRotationThresholdBytes
    ) throws {
        self.directoryOverride = directory
        self.rotationThresholdBytes = rotationThresholdBytes
        self.buildCommitSHA = BuildInfo.commitSHA
        // playhead-rfu-aac L2: no field on TranscriptShadowGateEntry is a
        // Date — `timestamp` is a `Double`. The previous `.iso8601`
        // dateEncodingStrategy was load-bearing only by accident (it set a
        // policy that no encoder path ever invoked). Drop it to remove the
        // dead config and avoid confusing future maintainers who might
        // expect schema fields to round-trip as ISO-8601 strings.
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        self.encoder = encoder
        self.resolvedDirectory = nil
        self.nextRotationIndex = nil
    }

    /// Lazy first-use bootstrap. Resolves the directory (Documents lookup
    /// for the convenience init), creates it, seeds `nextRotationIndex`
    /// from disk. Idempotent.
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

    /// Append one record to the log. Rotates if the active file exceeds the
    /// threshold after the write.
    func record(_ entry: TranscriptShadowGateEntry) async {
        do {
            try ensureBootstrapped()
            try appendEntry(entry)
            try rotateIfNeeded()
        } catch {
            logger.warning("TranscriptShadowGateLogger.record failed: \(error.localizedDescription, privacy: .public)")
        }
    }

    // MARK: - Test hooks

    /// Returns the currently-scheduled rotation index. For tests.
    /// Triggers the lazy bootstrap so tests that probe this value
    /// pre-write observe the seeded value rather than a zero.
    func currentNextRotationIndex() -> Int {
        try? ensureBootstrapped()
        return nextRotationIndex ?? 1
    }

    /// Force-close the handle. Tests use this before reading the file
    /// back through `Data(contentsOf:)` to make sure pending writes flushed.
    func flushAndClose() {
        closeHandle()
    }

    /// Absolute path to the currently-active log file. Triggers the
    /// lazy bootstrap on first call so callers can rely on the path
    /// being valid.
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

    private func appendEntry(_ entry: TranscriptShadowGateEntry) throws {
        // Stamp every encoded row with the build SHA captured at init.
        // Callers pass `nil`; we own the stamp here so the eval pipeline
        // can correlate captures to the binary that produced them.
        let stamped = TranscriptShadowGateEntry(
            schemaVersion: entry.schemaVersion,
            timestamp: entry.timestamp,
            analysisAssetID: entry.analysisAssetID,
            episodeID: entry.episodeID,
            shardID: entry.shardID,
            shardStart: entry.shardStart,
            shardEnd: entry.shardEnd,
            likelihood: entry.likelihood,
            threshold: entry.threshold,
            decision: entry.decision,
            wouldGate: entry.wouldGate,
            transcribed: entry.transcribed,
            buildCommitSHA: buildCommitSHA
        )
        let data = try encoder.encode(stamped)
        var line = Data()
        line.reserveCapacity(data.count + 1)
        line.append(data)
        line.append(0x0A)  // newline
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
        // playhead-rfu-aac M2: `attributesOfItem(atPath:)` reads kernel
        // attributes which can lag the FileHandle's user-space write
        // buffer. Without a flush, a record large enough to push the
        // file past `rotationThresholdBytes` could fall under the
        // pre-flush size attribute and we'd skip rotation for the next
        // record (or worse, several). Synchronize before measuring.
        try? fileHandle?.synchronize()
        let url = activeLogURL
        let attrs = try FileManager.default.attributesOfItem(atPath: url.path)
        let size = (attrs[.size] as? NSNumber)?.intValue ?? 0
        guard size >= rotationThresholdBytes else { return }
        // Livelock guard: a single record larger than the threshold would
        // produce a fresh active file that is itself still oversized,
        // looping the rotation forever. Skip rotation when the active
        // file has only one line. Mirrors DecisionLogger.
        if try lineCount(at: url) <= 1 {
            logger.warning(
                "TranscriptShadowGateLogger: active log exceeds threshold but has \u{2264}1 line; skipping rotation to avoid livelock"
            )
            return
        }
        try rotateNow()
    }

    /// Count newlines in `url` up to a small budget. Stops after seeing a
    /// second newline because the livelock guard only needs to distinguish
    /// "one record" from "more than one record."
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
        try ensureBootstrapped()
        guard let dir = resolvedDirectory, let idx = nextRotationIndex else {
            return
        }
        let src = activeLogURL
        let dstName = "\(Self.rotatedPrefix).\(idx)\(Self.rotatedSuffix)"
        let dst = dir.appendingPathComponent(dstName)

        closeHandle()

        // Crash-safe: atomic `replaceItemAt` when destination exists,
        // else `moveItem`. Mirrors DecisionLogger.
        let fm = FileManager.default
        if fm.fileExists(atPath: dst.path) {
            _ = try fm.replaceItemAt(dst, withItemAt: src)
        } else {
            try fm.moveItem(at: src, to: dst)
        }
        nextRotationIndex = idx + 1
        logger.info("TranscriptShadowGateLogger: rotated active log to \(dstName, privacy: .public)")
    }

    private func closeHandle() {
        if let handle = fileHandle {
            try? handle.close()
            fileHandle = nil
        }
    }

    // MARK: - Static helpers

    /// Scan the directory for existing `transcript-shadow-gate.N.jsonl`
    /// files and return the next index to use. Idempotent across launches.
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

    /// Returns the rotation index embedded in a filename like
    /// `transcript-shadow-gate.7.jsonl`, or nil if the name is the active
    /// file or doesn't match the pattern.
    fileprivate static func extractRotationIndex(from name: String) -> Int? {
        let prefix = rotatedPrefix + "."
        let suffix = rotatedSuffix
        guard name.hasPrefix(prefix), name.hasSuffix(suffix) else { return nil }
        let middle = name.dropFirst(prefix.count).dropLast(suffix.count)
        return Int(middle)  // nil when middle is "jsonl" (active file)
    }
}
