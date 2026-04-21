// DecisionLogger.swift
// playhead-8em9 (narL): Per-window decision logger for personal dogfooding.
//
// Design:
//   • Actor-backed append-only JSONL writer at Documents/decision-log.jsonl.
//   • Rotates to decision-log.N.jsonl on 10 MB thresholds. Rotation is
//     idempotent across warm starts and crash-safe (uses
//     `FileManager.replaceItemAt`).
//   • Records from both the hot path (per-candidate classifier decision,
//     no fused ledger yet) and the backfill path (full fusion ledger).
//     Hot-path entries carry a single `.classifier` ledger entry so replay
//     tooling sees a consistent schema, and the fused-confidence breakdown
//     degenerates to the classifier's own contribution.
//   • Each record carries enough structured inputs to replay `.default`-
//     equivalent decisions offline without re-running the pipeline.
//   • Non-blocking: the actor serializes writes and the ingest call
//     returns as soon as the continuation is scheduled.
//   • DEBUG-only by convention — production wiring in `PlayheadRuntime`
//     gates construction behind `#if DEBUG`. Release builds never
//     instantiate this logger, so no log file is written on a shipping
//     binary.
//
// Schema version: 2.
//   v2: rename `episodeID` → `analysisAssetID` (value is a content fingerprint,
//       not a canonical episode key; prior name was misleading for replay tools).
//
// Example record (pretty-printed; on disk is one line, compact JSON):
//   {
//     "schemaVersion": 2,
//     "analysisAssetID": "asset-abc",
//     "timestamp": 1745284800.1,
//     "windowBounds": { "start": 120.5, "end": 150.0 },
//     "activationConfig": { "counterfactualGateOpen": true, ... },
//     "evidence": [
//       { "source": "lexical", "weight": 0.2,
//         "detail": { "kind": "lexical", "matchedCategories": ["url"] } }
//     ],
//     "fusedConfidence": { "proposalConfidence": 0.72, "skipConfidence": 0.74,
//                          "breakdown": [{"source":"lexical","weight":0.2,
//                                         "capApplied":0.2,"authority":"strong"}] },
//     "finalDecision": { "action": "autoSkipEligible", "gate": "eligible",
//                        "skipConfidence": 0.74, "thresholdCrossed": 0.55 }
//   }

import Foundation
import OSLog

// MARK: - DecisionLoggerProtocol

/// Protocol seam for `DecisionLogger`. The release build installs a no-op
/// implementation; DEBUG builds install `DecisionLogger`.
protocol DecisionLoggerProtocol: Sendable {
    /// Append a single decision record. Must not block the caller beyond
    /// the actor hop; file I/O is serialized inside the implementation.
    func record(_ entry: DecisionLogEntry) async
}

/// No-op logger for release builds and tests that don't exercise logging.
struct NoOpDecisionLogger: DecisionLoggerProtocol {
    func record(_ entry: DecisionLogEntry) async {
        // intentionally blank
    }
}

// MARK: - DecisionLogEntry (JSONL record schema)

/// Schema-versioned, Codable record for one window decision.
///
/// Fields are chosen so offline replay can reconstruct `.default` decisions
/// without re-running the pipeline. In particular:
///   - Evidence ledger is serialized with source + weight + detail.
///   - `activationConfig` captures the gate state at decision time.
///   - `fusedConfidence.breakdown` mirrors `DecisionExplanation` per-source.
struct DecisionLogEntry: Codable, Equatable, Sendable {

    /// Schema version; increment on breaking changes. Current: 1.
    let schemaVersion: Int

    /// Analysis-asset content fingerprint (SHA-256-derived). Not the canonical
    /// episode key; downstream tooling must join on this, not on podcast IDs.
    let analysisAssetID: String

    /// Unix time at which the decision was observed (seconds since epoch).
    let timestamp: Double

    /// Window bounds in episode-relative seconds.
    let windowBounds: Bounds

    /// Snapshot of `MetadataActivationConfig` at decision time.
    let activationConfig: ActivationConfigSnapshot

    /// Per-entry evidence ledger used to compute the fused score.
    let evidence: [LedgerEntry]

    /// Fused confidence breakdown including per-source contributions.
    let fusedConfidence: FusedConfidence

    /// The final decision (action, gate, threshold).
    let finalDecision: FinalDecision

    static let currentSchemaVersion: Int = 2

    struct Bounds: Codable, Equatable, Sendable {
        let start: Double
        let end: Double
    }

    struct ActivationConfigSnapshot: Codable, Equatable, Sendable {
        let lexicalInjectionEnabled: Bool
        let lexicalInjectionMinTrust: Float
        let lexicalInjectionDiscount: Double
        let classifierPriorShiftEnabled: Bool
        let classifierPriorShiftMinTrust: Float
        let classifierShiftedMidpoint: Double
        let classifierBaselineMidpoint: Double
        let fmSchedulingEnabled: Bool
        let fmSchedulingMinTrust: Float
        let counterfactualGateOpen: Bool

        init(_ config: MetadataActivationConfig) {
            self.lexicalInjectionEnabled = config.lexicalInjectionEnabled
            self.lexicalInjectionMinTrust = config.lexicalInjectionMinTrust
            self.lexicalInjectionDiscount = config.lexicalInjectionDiscount
            self.classifierPriorShiftEnabled = config.classifierPriorShiftEnabled
            self.classifierPriorShiftMinTrust = config.classifierPriorShiftMinTrust
            self.classifierShiftedMidpoint = config.classifierShiftedMidpoint
            self.classifierBaselineMidpoint = config.classifierBaselineMidpoint
            self.fmSchedulingEnabled = config.fmSchedulingEnabled
            self.fmSchedulingMinTrust = config.fmSchedulingMinTrust
            self.counterfactualGateOpen = config.counterfactualGateOpen
        }
    }

    struct LedgerEntry: Codable, Equatable, Sendable {
        let source: String
        let weight: Double
        let classificationTrust: Double
        let detail: Detail

        struct Detail: Codable, Equatable, Sendable {
            /// Discriminator matching the `EvidenceLedgerDetail` case name.
            let kind: String
            /// Classifier score, if applicable.
            let score: Double?
            /// FM disposition, band, cohort prompt.
            let disposition: String?
            let band: String?
            let cohortPromptLabel: String?
            /// Lexical matched categories.
            let matchedCategories: [String]?
            /// Acoustic break strength.
            let breakStrength: Double?
            /// Catalog entry count.
            let entryCount: Int?
            /// Fingerprint fields.
            let matchCount: Int?
            let averageSimilarity: Double?
            /// Metadata fields (z3ch).
            let cueCount: Int?
            let sourceField: String?
            let dominantCueType: String?
        }

        init(_ entry: EvidenceLedgerEntry) {
            self.source = entry.source.rawValue
            self.weight = entry.weight
            self.classificationTrust = entry.classificationTrust
            self.detail = Detail(entry.detail)
        }
    }

    struct FusedConfidence: Codable, Equatable, Sendable {
        let proposalConfidence: Double
        let skipConfidence: Double
        /// Per-source contribution breakdown (mirrors DecisionExplanation.evidenceBreakdown).
        let breakdown: [SourceEvidence]
    }

    struct FinalDecision: Codable, Equatable, Sendable {
        /// The policy action applied (skip/mark-only/detect-only/log-only/suppress/auto-skip).
        let action: String
        /// Resolved eligibility gate.
        let gate: String
        /// Final skip confidence used for the promotion check.
        let skipConfidence: Double
        /// Threshold required to cross for auto-skip promotion.
        let thresholdCrossed: Double
    }
}

extension DecisionLogEntry.LedgerEntry.Detail {
    init(_ detail: EvidenceLedgerDetail) {
        switch detail {
        case .classifier(let score):
            self.init(
                kind: "classifier",
                score: score,
                disposition: nil, band: nil, cohortPromptLabel: nil,
                matchedCategories: nil,
                breakStrength: nil,
                entryCount: nil,
                matchCount: nil, averageSimilarity: nil,
                cueCount: nil, sourceField: nil, dominantCueType: nil
            )
        case .fm(let disposition, let band, let cohortPromptLabel):
            self.init(
                kind: "fm",
                score: nil,
                disposition: disposition.rawValue,
                band: band.rawValue,
                cohortPromptLabel: cohortPromptLabel,
                matchedCategories: nil,
                breakStrength: nil,
                entryCount: nil,
                matchCount: nil, averageSimilarity: nil,
                cueCount: nil, sourceField: nil, dominantCueType: nil
            )
        case .lexical(let matchedCategories):
            self.init(
                kind: "lexical",
                score: nil,
                disposition: nil, band: nil, cohortPromptLabel: nil,
                matchedCategories: matchedCategories,
                breakStrength: nil,
                entryCount: nil,
                matchCount: nil, averageSimilarity: nil,
                cueCount: nil, sourceField: nil, dominantCueType: nil
            )
        case .acoustic(let breakStrength):
            self.init(
                kind: "acoustic",
                score: nil,
                disposition: nil, band: nil, cohortPromptLabel: nil,
                matchedCategories: nil,
                breakStrength: breakStrength,
                entryCount: nil,
                matchCount: nil, averageSimilarity: nil,
                cueCount: nil, sourceField: nil, dominantCueType: nil
            )
        case .catalog(let entryCount):
            self.init(
                kind: "catalog",
                score: nil,
                disposition: nil, band: nil, cohortPromptLabel: nil,
                matchedCategories: nil,
                breakStrength: nil,
                entryCount: entryCount,
                matchCount: nil, averageSimilarity: nil,
                cueCount: nil, sourceField: nil, dominantCueType: nil
            )
        case .fingerprint(let matchCount, let averageSimilarity):
            self.init(
                kind: "fingerprint",
                score: nil,
                disposition: nil, band: nil, cohortPromptLabel: nil,
                matchedCategories: nil,
                breakStrength: nil,
                entryCount: nil,
                matchCount: matchCount, averageSimilarity: averageSimilarity,
                cueCount: nil, sourceField: nil, dominantCueType: nil
            )
        case .metadata(let cueCount, let sourceField, let dominantCueType):
            self.init(
                kind: "metadata",
                score: nil,
                disposition: nil, band: nil, cohortPromptLabel: nil,
                matchedCategories: nil,
                breakStrength: nil,
                entryCount: nil,
                matchCount: nil, averageSimilarity: nil,
                cueCount: cueCount,
                sourceField: sourceField.rawValue,
                dominantCueType: dominantCueType.rawValue
            )
        }
    }
}

// MARK: - DecisionLogger

/// Actor-backed JSONL writer for per-window decisions.
///
/// The writer appends one compact JSON object per line. On every write,
/// if the active file size exceeds the rotation threshold, the current
/// file is renamed to `decision-log.N.jsonl` (next available N) and a
/// fresh `decision-log.jsonl` is started. Rotation is idempotent across
/// app launches: on init the logger scans the directory for existing
/// rotated files and seeds its next-index counter from the max observed.
actor DecisionLogger: DecisionLoggerProtocol {

    /// Default rotation threshold (10 MB).
    static let defaultRotationThresholdBytes: Int = 10 * 1024 * 1024

    /// Active log file basename.
    static let activeLogFilename: String = "decision-log.jsonl"

    /// Prefix for rotated files; actual name is `rotatedPrefix.\(N).jsonl`.
    /// E.g. decision-log.1.jsonl, decision-log.2.jsonl.
    static let rotatedPrefix: String = "decision-log"
    static let rotatedSuffix: String = ".jsonl"

    private let directory: URL
    private let rotationThresholdBytes: Int
    private let encoder: JSONEncoder
    private let logger = Logger(subsystem: "com.playhead", category: "DecisionLogger")

    /// Next rotation index; seeded from disk at init.
    private var nextRotationIndex: Int

    /// Lazily-opened handle for the active file. Reset after rotation.
    private var fileHandle: FileHandle?

    // MARK: - Init

    /// Convenience init that targets `FileManager.default
    /// .urls(for: .documentDirectory, in: .userDomainMask)[0]`.
    init(
        rotationThresholdBytes: Int = DecisionLogger.defaultRotationThresholdBytes
    ) throws {
        let docs = try FileManager.default.url(
            for: .documentDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        try self.init(directory: docs, rotationThresholdBytes: rotationThresholdBytes)
    }

    /// Designated init for testing: points the logger at an arbitrary
    /// directory. The directory is created if it doesn't exist.
    init(
        directory: URL,
        rotationThresholdBytes: Int = DecisionLogger.defaultRotationThresholdBytes
    ) throws {
        self.directory = directory
        self.rotationThresholdBytes = rotationThresholdBytes
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]  // stable, line-diff-friendly
        // ISO-8601 so replay tooling can parse deterministically across OS
        // versions. No Date-typed fields today, but setting the strategy
        // now prevents silent drift when one is added later.
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder

        // Ensure directory exists.
        try FileManager.default.createDirectory(
            at: directory, withIntermediateDirectories: true
        )

        // Seed nextRotationIndex from the highest-numbered rotated file
        // already on disk — ensures idempotency across warm starts.
        self.nextRotationIndex = Self.scanNextRotationIndex(in: directory)
    }

    // MARK: - Public API

    /// Append one record to the log. Rotates if the active file exceeds the
    /// threshold after the write.
    func record(_ entry: DecisionLogEntry) async {
        do {
            try appendEntry(entry)
            try rotateIfNeeded()
        } catch {
            logger.warning("DecisionLogger.record failed: \(error.localizedDescription, privacy: .public)")
        }
    }

    // MARK: - Test hooks

    /// Returns the currently-scheduled rotation index. For tests.
    func currentNextRotationIndex() -> Int {
        nextRotationIndex
    }

    /// Force-close the handle. Tests use this before reading the file
    /// back through `Data(contentsOf:)` to make sure pending writes flushed.
    func flushAndClose() {
        closeHandle()
    }

    /// Absolute path to the currently-active log file.
    var activeLogURL: URL {
        directory.appendingPathComponent(Self.activeLogFilename)
    }

    /// All rotated log URLs, ordered by numeric index.
    func rotatedLogURLs() -> [URL] {
        Self.listRotatedLogs(in: directory)
    }

    // MARK: - Internal

    private func appendEntry(_ entry: DecisionLogEntry) throws {
        let data = try encoder.encode(entry)
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
        let url = activeLogURL
        let attrs = try FileManager.default.attributesOfItem(atPath: url.path)
        let size = (attrs[.size] as? NSNumber)?.intValue ?? 0
        guard size >= rotationThresholdBytes else { return }

        // Livelock guard: if a single record is itself larger than the
        // rotation threshold, rotating would produce a fresh file that is
        // still oversized (it still holds that one record). Skip rotation
        // when the active file contains only one line so we don't loop
        // forever rotating the same >threshold record. We warn once so
        // the operator can investigate, but we do not truncate the record
        // — corpus replay tooling prefers a valid oversized record to a
        // silently truncated one.
        if try lineCount(at: url) <= 1 {
            logger.warning(
                "DecisionLogger: active log exceeds threshold but has \u{2264}1 line; skipping rotation to avoid livelock"
            )
            return
        }
        try rotateNow()
    }

    /// Count newlines in `url` up to a small budget. A full byte count is
    /// not required — we only need to distinguish "one record" from
    /// "more than one record" for the livelock guard, so we stop reading
    /// after seeing a second newline.
    private func lineCount(at url: URL) throws -> Int {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var count = 0
        // 64 KiB is larger than any realistic single-record JSON
        // projection but small enough to stay off the wide-path.
        while let chunk = try handle.read(upToCount: 64 * 1024), !chunk.isEmpty {
            for byte in chunk where byte == 0x0A {
                count += 1
                if count >= 2 { return count }
            }
        }
        return count
    }

    private func rotateNow() throws {
        let src = activeLogURL
        let dstName = "\(Self.rotatedPrefix).\(nextRotationIndex)\(Self.rotatedSuffix)"
        let dst = directory.appendingPathComponent(dstName)

        closeHandle()

        // Crash-safe rotation: use `replaceItemAt` when a destination
        // already exists (atomic swap), otherwise fall back to `moveItem`.
        // The previous close → removeItem → moveItem sequence could leave
        // a gap where both `src` and `dst` were missing if the process
        // died between the two filesystem operations.
        let fm = FileManager.default
        if fm.fileExists(atPath: dst.path) {
            // `replaceItemAt` performs an atomic replace where supported.
            // The source must exist — which it does here because we only
            // call rotateNow() after rotateIfNeeded() verified a >0 byte
            // active log.
            _ = try fm.replaceItemAt(dst, withItemAt: src)
        } else {
            try fm.moveItem(at: src, to: dst)
        }
        nextRotationIndex += 1
        logger.info("DecisionLogger: rotated active log to \(dstName, privacy: .public)")
    }

    private func closeHandle() {
        if let handle = fileHandle {
            try? handle.close()
            fileHandle = nil
        }
    }

    // MARK: - Static helpers

    /// Scan the directory for existing `decision-log.N.jsonl` files and
    /// return the next index to use. Idempotent across launches.
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
    /// `decision-log.7.jsonl`, or nil if the name is the active file or
    /// doesn't match the pattern.
    fileprivate static func extractRotationIndex(from name: String) -> Int? {
        let prefix = rotatedPrefix + "."
        let suffix = rotatedSuffix
        guard name.hasPrefix(prefix), name.hasSuffix(suffix) else { return nil }
        let middle = name.dropFirst(prefix.count).dropLast(suffix.count)
        return Int(middle)  // nil when middle is e.g. "jsonl" (that means "decision-log.jsonl")
    }
}
