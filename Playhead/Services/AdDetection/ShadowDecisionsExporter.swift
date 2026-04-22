// ShadowDecisionsExporter.swift
// playhead-narl.2: Writes `shadow-decisions.jsonl` alongside the corpus export.
//
// The harness's corpus builder in `playhead-narl.1` reads this file to
// populate FrozenTrace with `.allEnabled` FM evidence for windows that
// `.default` never scheduled FM on.
//
// Schema shape: see ``shadowSchemaVersion`` / the line-shape doc on
// ``ShadowFMResponse``. One JSON object per line, base64-encoded `fmResponse`.

import Foundation
import OSLog

// MARK: - ShadowDecisionsExportResult

/// Summary returned after writing `shadow-decisions.jsonl`.
struct ShadowDecisionsExportResult: Sendable, Equatable {
    /// Absolute path of the written file.
    let fileURL: URL
    /// Number of rows emitted (one JSON line per row).
    let rowCount: Int
}

// MARK: - ShadowDecisionsExportSource

/// Narrow read seam over the persisted shadow rows. `AnalysisStore`
/// conforms by materializing rows in (assetId, windowStart) order.
///
/// The protocol returns `[ShadowFMResponse]` rather than driving a closure
/// so we can keep the signature `Sendable`-clean across the actor boundary
/// without a visitor-style closure capture dance. At realistic volumes
/// (thousands of rows), the memory cost is a few MB — acceptable for an
/// export path that runs on demand, not in a hot loop.
protocol ShadowDecisionsExportSource: Sendable {
    /// Returns every persisted shadow row, ordered by (assetId, windowStart).
    /// An empty array means "no shadow rows yet" — the exporter still
    /// writes a zero-byte `shadow-decisions.jsonl` so downstream tooling
    /// can distinguish "file missing" from "file present but empty".
    func allShadowFMResponses() async throws -> [ShadowFMResponse]
}

extension AnalysisStore: ShadowDecisionsExportSource {
    // The actor's `allShadowFMResponses()` defined in AnalysisStore.swift
    // already satisfies the protocol; marking the conformance is enough.
}

// MARK: - ShadowDecisionsExporter

enum ShadowDecisionsExporter {

    static let filename: String = "shadow-decisions.jsonl"

    private static let logger = Logger(subsystem: "com.playhead", category: "ShadowDecisionsExporter")

    /// Write (or overwrite) `shadow-decisions.jsonl` in `documentsURL` by
    /// streaming rows from `source`. The write is atomic against full
    /// failure: a throw before the final return unlinks the partially-written
    /// file so consumers never see a truncated jsonl.
    ///
    /// Writes an empty file (zero rows) rather than skipping when the store
    /// has no shadow rows — downstream tooling distinguishes "file missing"
    /// (shadow capture hasn't shipped yet) from "file present but empty"
    /// (shadow capture is on; no rows yet).
    static func export(
        source: some ShadowDecisionsExportSource,
        documentsURL: URL
    ) async throws -> ShadowDecisionsExportResult {
        let fm = FileManager.default
        if !fm.fileExists(atPath: documentsURL.path) {
            try fm.createDirectory(at: documentsURL, withIntermediateDirectories: true)
        }
        let fileURL = documentsURL.appendingPathComponent(filename)
        fm.createFile(atPath: fileURL.path, contents: nil, attributes: nil)
        let handle = try FileHandle(forWritingTo: fileURL)

        var didSucceed = false
        defer {
            try? handle.close()
            if !didSucceed {
                try? fm.removeItem(at: fileURL)
            }
        }

        let rows = try await source.allShadowFMResponses()
        for row in rows {
            let lineData = try serialize(row)
            try handle.write(contentsOf: lineData)
            try handle.write(contentsOf: Data([0x0A])) // '\n'
        }
        let rowCount = rows.count

        didSucceed = true
        return ShadowDecisionsExportResult(fileURL: fileURL, rowCount: rowCount)
    }

    // MARK: - Per-line serialization

    /// Serialize one row as a compact JSON object (no trailing newline).
    /// Deterministic key ordering is not required — the corpus builder is
    /// line-oriented and parses each line independently.
    static func serialize(_ row: ShadowFMResponse) throws -> Data {
        let obj: [String: Any] = [
            "schemaVersion": shadowSchemaVersion,
            "type": "shadow_fm_response",
            "assetId": row.assetId,
            "windowStart": row.windowStart,
            "windowEnd": row.windowEnd,
            "configVariant": row.configVariant.rawValue,
            "fmResponseBase64": row.fmResponse.base64EncodedString(),
            "capturedAt": row.capturedAt,
            "capturedBy": row.capturedBy.rawValue,
            "fmModelVersion": row.fmModelVersion as Any? ?? NSNull(),
        ]
        // .withoutEscapingSlashes keeps asset ids / model-version strings readable.
        return try JSONSerialization.data(
            withJSONObject: obj,
            options: [.withoutEscapingSlashes]
        )
    }

    // MARK: - Parse (round-trip helper)

    /// Parse a single JSONL line back into a ``ShadowFMResponse``. Used by
    /// the round-trip test and by a minimal reader to prove the corpus
    /// builder can reconstruct rows from the exported file.
    ///
    /// Strict about `schemaVersion` — lines from a future schema are
    /// rejected rather than silently dropped. The harness's corpus builder
    /// should mirror this policy.
    static func parse(line: Data) throws -> ShadowFMResponse {
        guard let any = try JSONSerialization.jsonObject(with: line) as? [String: Any] else {
            throw ShadowDecisionsParseError.notAJSONObject
        }
        guard let schemaVersion = any["schemaVersion"] as? Int else {
            throw ShadowDecisionsParseError.missingField("schemaVersion")
        }
        guard schemaVersion == shadowSchemaVersion else {
            throw ShadowDecisionsParseError.unsupportedSchema(schemaVersion)
        }
        guard let assetId = any["assetId"] as? String else {
            throw ShadowDecisionsParseError.missingField("assetId")
        }
        guard let windowStart = any["windowStart"] as? Double else {
            throw ShadowDecisionsParseError.missingField("windowStart")
        }
        guard let windowEnd = any["windowEnd"] as? Double else {
            throw ShadowDecisionsParseError.missingField("windowEnd")
        }
        guard let variantRaw = any["configVariant"] as? String,
              let variant = ShadowConfigVariant(rawValue: variantRaw) else {
            throw ShadowDecisionsParseError.missingField("configVariant")
        }
        guard let b64 = any["fmResponseBase64"] as? String else {
            throw ShadowDecisionsParseError.missingField("fmResponseBase64")
        }
        guard let blob = Data(base64Encoded: b64) else {
            throw ShadowDecisionsParseError.invalidBase64
        }
        guard let capturedAt = any["capturedAt"] as? Double else {
            throw ShadowDecisionsParseError.missingField("capturedAt")
        }
        guard let capturedByRaw = any["capturedBy"] as? String,
              let capturedBy = ShadowCapturedBy(rawValue: capturedByRaw) else {
            throw ShadowDecisionsParseError.missingField("capturedBy")
        }
        let fmModelVersion = (any["fmModelVersion"] as? String)

        return ShadowFMResponse(
            assetId: assetId,
            windowStart: windowStart,
            windowEnd: windowEnd,
            configVariant: variant,
            fmResponse: blob,
            capturedAt: capturedAt,
            capturedBy: capturedBy,
            fmModelVersion: fmModelVersion
        )
    }

    /// Parse a whole file's worth of newline-delimited rows. Skips blank
    /// lines so a trailing newline on the file doesn't yield an empty row.
    static func parseAll(fileURL: URL) throws -> [ShadowFMResponse] {
        let data = try Data(contentsOf: fileURL)
        var rows: [ShadowFMResponse] = []
        // Split on 0x0A; a CR-LF jsonl is non-conforming to our writer's
        // own output, but tolerate a \r trailer on each line for paranoia.
        for chunk in data.split(separator: 0x0A, omittingEmptySubsequences: true) {
            let trimmed: Data = chunk.last == 0x0D
                ? chunk.dropLast()
                : chunk
            guard !trimmed.isEmpty else { continue }
            rows.append(try parse(line: trimmed))
        }
        return rows
    }
}

// MARK: - ShadowDecisionsParseError

enum ShadowDecisionsParseError: Error, Equatable {
    case notAJSONObject
    case missingField(String)
    case unsupportedSchema(Int)
    case invalidBase64
}
