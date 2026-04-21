// CorpusExporter.swift
// narE (playhead-dgzw): Debug-menu corpus export.
//
// Reads CorrectionEvent + DecodedSpan rows from analysis.sqlite and writes a
// JSONL corpus file to Documents/ so it is visible via the Files app /
// Finder sharing (UIFileSharingEnabled + LSSupportsOpeningDocumentsInPlace
// are already set for dev builds by narL's Info.plist entries).
//
// Design:
//   - Debug-only: wrapped in #if DEBUG. The release build strips the type.
//   - Streaming: uses FileHandle.write per record; does not materialize an
//     Array of all rows in memory. Per-asset fetches keep the working set
//     proportional to the largest single asset's span count.
//   - Schema version: every emitted record carries `schemaVersion: 1` so
//     downstream tooling can drift-check per record.
//   - Join keys: records use the analysisAssetId and atom-ordinal keys
//     exactly as they appear on DecodedSpan / CorrectionEvent, matching
//     narL's decision-log.jsonl by construction.
//   - Corrupt-scope tolerance: a correction row whose `scope` does not
//     deserialize via CorrectionScope.deserialize(_:) is logged and
//     skipped; it does NOT abort the export.
//   - Optional metadata (podcastId, coverage times, etc.) is serialized
//     as explicit null, not omitted.

#if DEBUG

import Foundation
import OSLog

// MARK: - CorpusExportResult

/// Result of a corpus export, returned to the caller so the UI can show
/// a share sheet or a confirmation summary.
struct CorpusExportResult: Sendable, Equatable {
    /// Absolute path of the written JSONL file.
    let fileURL: URL
    /// Number of asset rows emitted.
    let assetCount: Int
    /// Number of DecodedSpan rows emitted.
    let spanCount: Int
    /// Number of CorrectionEvent rows emitted.
    let correctionCount: Int
    /// Number of CorrectionEvent rows skipped due to unparseable `scope` strings.
    let skippedCorrectionCount: Int
    /// Absolute path of the sibling `decision-log.jsonl` written by narL,
    /// if it exists in the same Documents directory — exposed so the
    /// caller can build a combined bundle.
    let decisionLogManifestURL: URL?
}

// MARK: - CorpusExporter

enum CorpusExporter {

    /// JSONL schema version stamped on every emitted record. Bump when
    /// the record shape changes in a non-backward-compatible way; downstream
    /// tooling is expected to version-gate at the per-record level.
    static let schemaVersion: Int = 1

    private static let logger = Logger(subsystem: "com.playhead", category: "CorpusExporter")

    // MARK: - Filename

    /// Build the filename used for an export written at `date`.
    ///
    /// ISO-8601 seconds in UTC, with colons replaced by dashes so the file is
    /// Finder / Files-app friendly.
    static func filename(for date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]  // YYYY-MM-DDTHH:MM:SSZ
        formatter.timeZone = TimeZone(identifier: "UTC")
        let iso = formatter.string(from: date)
        let safe = iso.replacingOccurrences(of: ":", with: "-")
        return "corpus-export.\(safe).jsonl"
    }

    // MARK: - Per-record serializers

    /// Serialize an `AnalysisAsset` row as a single JSONL line body (no trailing newline).
    /// Throws if JSONSerialization fails, which in practice requires a non-UTF-8 field
    /// that shouldn't appear in real data.
    static func assetLine(_ asset: AnalysisAsset) throws -> Data {
        let obj: [String: Any] = [
            "type": "asset",
            "schemaVersion": schemaVersion,
            "analysisAssetId": asset.id,
            "episodeId": asset.episodeId,
            "assetFingerprint": asset.assetFingerprint,
            "weakFingerprint": asset.weakFingerprint as Any? ?? NSNull(),
            "sourceURL": asset.sourceURL,
            "analysisState": asset.analysisState,
            "analysisVersion": asset.analysisVersion,
            "artifactClass": asset.artifactClass.rawValue,
            "featureCoverageEndTime": asset.featureCoverageEndTime as Any? ?? NSNull(),
            "fastTranscriptCoverageEndTime": asset.fastTranscriptCoverageEndTime as Any? ?? NSNull(),
            "confirmedAdCoverageEndTime": asset.confirmedAdCoverageEndTime as Any? ?? NSNull(),
        ]
        return try jsonLineData(from: obj)
    }

    /// Serialize a `DecodedSpan` as a single JSONL line body.
    static func spanLine(_ span: DecodedSpan) throws -> Data {
        // Encode anchorProvenance via the type's own Codable adapter so the
        // JSON shape matches what the on-disk `decoded_spans.anchorProvenanceJSON`
        // column stores — downstream tooling can reuse the same decoder.
        let provenanceData = try JSONEncoder().encode(span.anchorProvenance)
        let provenanceAny = try JSONSerialization.jsonObject(with: provenanceData)

        let obj: [String: Any] = [
            "type": "decision",
            "schemaVersion": schemaVersion,
            "spanId": span.id,
            "analysisAssetId": span.assetId,
            "firstAtomOrdinal": span.firstAtomOrdinal,
            "lastAtomOrdinal": span.lastAtomOrdinal,
            "startTime": span.startTime,
            "endTime": span.endTime,
            "anchorProvenance": provenanceAny,
        ]
        return try jsonLineData(from: obj)
    }

    /// Serialize a `CorrectionEvent` as a single JSONL line body. Returns nil if
    /// the row has an unparseable `scope` string, signalling the caller to skip it.
    static func correctionLine(_ event: CorrectionEvent) throws -> Data? {
        // Reject rows whose scope cannot be deserialized — corrupt persisted
        // input that would confuse downstream replay tools. We still let the
        // rest of the export proceed.
        guard CorrectionScope.deserialize(event.scope) != nil else {
            return nil
        }

        var targetRefsAny: Any = NSNull()
        if let refs = event.targetRefs {
            let data = try JSONEncoder().encode(refs)
            targetRefsAny = try JSONSerialization.jsonObject(with: data)
        }

        let obj: [String: Any] = [
            "type": "correction",
            "schemaVersion": schemaVersion,
            "id": event.id,
            "analysisAssetId": event.analysisAssetId,
            "scope": event.scope,
            "createdAt": event.createdAt,
            "source": event.source?.rawValue as Any? ?? NSNull(),
            "podcastId": event.podcastId as Any? ?? NSNull(),
            "correctionType": event.correctionType?.rawValue as Any? ?? NSNull(),
            "causalSource": event.causalSource?.rawValue as Any? ?? NSNull(),
            "targetRefs": targetRefsAny,
        ]
        return try jsonLineData(from: obj)
    }

    // MARK: - Export

    /// Perform the full export against `store`, writing into `documentsURL`
    /// as `corpus-export.<timestamp>.jsonl`. Returns a summary result.
    ///
    /// Streams records to disk via `FileHandle.write` — no Array-in-memory
    /// accumulation of the full corpus. Memory use is bounded by the largest
    /// single asset's span or correction count.
    static func export(
        store: AnalysisStore,
        documentsURL: URL,
        now: Date = Date()
    ) async throws -> CorpusExportResult {
        let fm = FileManager.default
        if !fm.fileExists(atPath: documentsURL.path) {
            try fm.createDirectory(at: documentsURL, withIntermediateDirectories: true)
        }

        let fileURL = documentsURL.appendingPathComponent(filename(for: now))

        // Create (or truncate) the output file, then stream records with
        // FileHandle so we never hold all rows in memory simultaneously.
        fm.createFile(atPath: fileURL.path, contents: nil, attributes: nil)
        let handle = try FileHandle(forWritingTo: fileURL)
        // Ensure the handle is closed on every path so the fsync on close
        // flushes our final writes (otherwise the last record can trail in
        // the buffer when a test reads the file right after this returns).
        defer {
            try? handle.close()
        }

        var assetCount = 0
        var spanCount = 0
        var correctionCount = 0
        var skippedCorrectionCount = 0

        let assets = try await store.fetchAllAssets()
        for asset in assets {
            // asset record
            let assetData = try assetLine(asset)
            try write(line: assetData, to: handle)
            assetCount += 1

            // decision (DecodedSpan) records for this asset
            let spans = (try? await store.fetchDecodedSpans(assetId: asset.id)) ?? []
            for span in spans {
                let spanData = try spanLine(span)
                try write(line: spanData, to: handle)
                spanCount += 1
            }

            // correction records for this asset
            let events = (try? await store.loadCorrectionEvents(analysisAssetId: asset.id)) ?? []
            for event in events {
                guard let data = try correctionLine(event) else {
                    skippedCorrectionCount += 1
                    logger.warning(
                        "export: skipping correction row id=\(event.id, privacy: .public) asset=\(event.analysisAssetId, privacy: .public) — unparseable scope \(event.scope, privacy: .public)"
                    )
                    continue
                }
                try write(line: data, to: handle)
                correctionCount += 1
            }
        }

        // Optional narL pairing: if decision-log.jsonl exists in the same
        // Documents directory, surface it so the caller can drag both files
        // out in a single Finder operation. We do NOT copy or concatenate —
        // two files, one timestamped corpus bundle.
        let siblingDecisionLog = documentsURL.appendingPathComponent("decision-log.jsonl")
        let decisionLogManifestURL = fm.fileExists(atPath: siblingDecisionLog.path)
            ? siblingDecisionLog
            : nil

        return CorpusExportResult(
            fileURL: fileURL,
            assetCount: assetCount,
            spanCount: spanCount,
            correctionCount: correctionCount,
            skippedCorrectionCount: skippedCorrectionCount,
            decisionLogManifestURL: decisionLogManifestURL
        )
    }

    // MARK: - Private helpers

    /// Encode a dictionary as compact JSON (no pretty-printing, no sorted keys
    /// required — downstream tooling parses line-by-line).
    private static func jsonLineData(from obj: [String: Any]) throws -> Data {
        // .fragmentsAllowed is not needed here (always objects), and
        // .withoutEscapingSlashes keeps sourceURL and fingerprint paths readable.
        return try JSONSerialization.data(
            withJSONObject: obj,
            options: [.withoutEscapingSlashes]
        )
    }

    /// Write a single JSONL line (record body + LF) to the handle.
    private static func write(line data: Data, to handle: FileHandle) throws {
        try handle.write(contentsOf: data)
        try handle.write(contentsOf: Data([0x0A]))  // '\n'
    }
}

#endif
