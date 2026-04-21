// CorpusExporterTests.swift
// Tests for narE (playhead-dgzw): debug-only corpus export.
//
// CorpusExporter reads CorrectionEvent and DecodedSpan rows from
// analysis.sqlite and writes a JSONL corpus file to Documents/.
// Tests cover per-record serialization, join logic, empty-store,
// corrupt-scope handling, file naming, and streaming-to-disk.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("CorpusExporter — narE")
struct CorpusExporterTests {

    // MARK: - Filename

    @Test("filename(for:) uses ISO-8601 seconds, filesystem-safe colons replaced")
    func filenameFormat() {
        // 2026-04-21T15:30:45Z → "corpus-export.2026-04-21T15-30-45Z.jsonl"
        // (colons replaced with dashes; Z suffix kept).
        var comps = DateComponents()
        comps.year = 2026
        comps.month = 4
        comps.day = 21
        comps.hour = 15
        comps.minute = 30
        comps.second = 45
        comps.timeZone = TimeZone(identifier: "UTC")
        let date = Calendar(identifier: .iso8601).date(from: comps)!

        let name = CorpusExporter.filename(for: date)
        #expect(name == "corpus-export.2026-04-21T15-30-45Z.jsonl",
                "got \(name)")
        // No colon allowed in filename (Files app / Finder hostility).
        #expect(!name.contains(":"))
    }

    // MARK: - schemaVersion constant

    @Test("schemaVersion is 1 and every emitted record carries it")
    func schemaVersionConstant() throws {
        #expect(CorpusExporter.schemaVersion == 1)
        // Each record serializer stamps schemaVersion: 1.
        let asset = makeTestAsset(id: "asset-X")
        let assetJSON = try decodeJSONObject(from: CorpusExporter.assetLine(asset))
        #expect(assetJSON["schemaVersion"] as? Int == 1)
        #expect(assetJSON["type"] as? String == "asset")

        let span = makeSpan(assetId: "asset-X")
        let spanJSON = try decodeJSONObject(from: CorpusExporter.spanLine(span))
        #expect(spanJSON["schemaVersion"] as? Int == 1)
        #expect(spanJSON["type"] as? String == "decision")

        let scope = CorrectionScope.exactSpan(assetId: "asset-X", ordinalRange: 10...20)
        let event = CorrectionEvent(
            analysisAssetId: "asset-X",
            scope: scope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        guard let correctionData = try CorpusExporter.correctionLine(event) else {
            Issue.record("correctionLine returned nil for a valid event")
            return
        }
        let correctionJSON = try decodeJSONObject(from: correctionData)
        #expect(correctionJSON["schemaVersion"] as? Int == 1)
        #expect(correctionJSON["type"] as? String == "correction")
    }

    // MARK: - Asset record

    @Test("asset record carries analysisAssetId, episodeId, sourceURL; missing optional metadata serialized as null")
    func assetRecordShape() throws {
        let asset = makeTestAsset(id: "asset-A")
        let data = try CorpusExporter.assetLine(asset)
        let json = try decodeJSONObject(from: data)
        #expect(json["type"] as? String == "asset")
        #expect(json["analysisAssetId"] as? String == "asset-A")
        #expect(json["episodeId"] as? String == "ep-asset-A")
        #expect(json["sourceURL"] as? String == "file:///tmp/asset-A.m4a")
        #expect(json["analysisState"] as? String == "new")
        // Missing optional metadata (coverage times not set) must be null, not missing.
        #expect(json.keys.contains("featureCoverageEndTime"))
        #expect(json.keys.contains("fastTranscriptCoverageEndTime"))
        #expect(json.keys.contains("confirmedAdCoverageEndTime"))
        #expect(json["featureCoverageEndTime"] is NSNull)
    }

    // MARK: - Decision record (DecodedSpan)

    @Test("decision record carries assetId, atom ordinal range, start/end, anchorProvenance")
    func decisionRecordShape() throws {
        let span = makeSpan(assetId: "asset-A", startTime: 120.5, endTime: 180.25,
                            firstOrdinal: 42, lastOrdinal: 67)
        let data = try CorpusExporter.spanLine(span)
        let json = try decodeJSONObject(from: data)
        #expect(json["type"] as? String == "decision")
        #expect(json["analysisAssetId"] as? String == "asset-A")
        #expect(json["spanId"] as? String == span.id)
        #expect(json["firstAtomOrdinal"] as? Int == 42)
        #expect(json["lastAtomOrdinal"] as? Int == 67)
        #expect(json["startTime"] as? Double == 120.5)
        #expect(json["endTime"] as? Double == 180.25)
        // anchorProvenance is always an array (possibly empty).
        #expect(json["anchorProvenance"] is [Any])
    }

    // MARK: - Correction record

    @Test("correction record carries scope, source, correctionType, causalSource, targetRefs, createdAt, analysisAssetId; missing podcastId serialized as null")
    func correctionRecordShape() throws {
        let scope = CorrectionScope.exactSpan(assetId: "asset-A", ordinalRange: 5...12)
        let event = CorrectionEvent(
            id: "corr-1",
            analysisAssetId: "asset-A",
            scope: scope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto,
            podcastId: nil,
            correctionType: .falsePositive,
            causalSource: .foundationModel,
            targetRefs: CorrectionTargetRefs(sponsorEntity: "Squarespace")
        )
        guard let data = try CorpusExporter.correctionLine(event) else {
            Issue.record("correctionLine returned nil for a well-formed event")
            return
        }
        let json = try decodeJSONObject(from: data)
        #expect(json["type"] as? String == "correction")
        #expect(json["id"] as? String == "corr-1")
        #expect(json["analysisAssetId"] as? String == "asset-A")
        #expect(json["scope"] as? String == scope.serialized)
        #expect(json["createdAt"] as? Double == 1_700_000_000)
        #expect(json["source"] as? String == "manualVeto")
        #expect(json["correctionType"] as? String == "falsePositive")
        #expect(json["causalSource"] as? String == "foundationModel")
        // podcastId must be present as null, not omitted.
        #expect(json.keys.contains("podcastId"))
        #expect(json["podcastId"] is NSNull)
        // targetRefs survives as a nested object.
        let targetRefs = json["targetRefs"] as? [String: Any]
        #expect(targetRefs?["sponsorEntity"] as? String == "Squarespace")
    }

    @Test("correction record tolerates missing optional fields — only analysisAssetId + scope are required")
    func correctionMinimalRecord() throws {
        let event = CorrectionEvent(
            id: "corr-2",
            analysisAssetId: "asset-B",
            scope: CorrectionScope.exactSpan(assetId: "asset-B", ordinalRange: 1...1).serialized,
            createdAt: 0,
            source: nil,
            podcastId: nil,
            correctionType: nil,
            causalSource: nil,
            targetRefs: nil
        )
        guard let data = try CorpusExporter.correctionLine(event) else {
            Issue.record("correctionLine returned nil for a minimal-but-valid event")
            return
        }
        let json = try decodeJSONObject(from: data)
        // All optionals null, not omitted.
        for key in ["source", "podcastId", "correctionType", "causalSource", "targetRefs"] {
            #expect(json.keys.contains(key), "\(key) must be present")
            #expect(json[key] is NSNull, "\(key) must be null for a minimal record")
        }
    }

    // MARK: - Corrupt scope handling

    @Test("correctionLine returns nil for an unparseable scope string — caller logs and skips")
    func corruptScopeSkipped() throws {
        let event = CorrectionEvent(
            id: "corr-bad",
            analysisAssetId: "asset-A",
            scope: "!!!garbage_not_a_valid_scope",
            createdAt: 0,
            source: .manualVeto
        )
        let data = try CorpusExporter.correctionLine(event)
        #expect(data == nil, "Unparseable scope must make correctionLine return nil so the caller skips the row")
    }

    // MARK: - Join logic + end-to-end export against AnalysisStore

    @Test("export against empty store produces an empty file — valid (zero records), not a crash")
    func exportEmptyStore() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-empty")

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.assetCount == 0)
        #expect(result.spanCount == 0)
        #expect(result.correctionCount == 0)
        #expect(result.skippedCorrectionCount == 0)
        #expect(FileManager.default.fileExists(atPath: result.fileURL.path))

        let contents = try String(contentsOf: result.fileURL, encoding: .utf8)
        // Empty file or only trailing newline is acceptable.
        #expect(contents.isEmpty || contents == "\n")
    }

    @Test("export writes one line per asset, plus one line per DecodedSpan, plus one line per CorrectionEvent, each with the expected type discriminator")
    func exportJoinsAllThreeTypes() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-join")

        // Seed two assets, one span on each, one correction on the first.
        let a1 = makeTestAsset(id: "asset-1")
        let a2 = makeTestAsset(id: "asset-2")
        try await store.insertAsset(a1)
        try await store.insertAsset(a2)

        let span1 = makeSpan(assetId: "asset-1", firstOrdinal: 10, lastOrdinal: 20)
        let span2 = makeSpan(assetId: "asset-2", firstOrdinal: 30, lastOrdinal: 40)
        try await store.upsertDecodedSpans([span1, span2])

        let scope = CorrectionScope.exactSpan(assetId: "asset-1", ordinalRange: 10...20)
        let event = CorrectionEvent(
            analysisAssetId: "asset-1",
            scope: scope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        try await correctionStore.record(event)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.assetCount == 2)
        #expect(result.spanCount == 2)
        #expect(result.correctionCount == 1)
        #expect(result.skippedCorrectionCount == 0)

        let records = try parseJSONL(at: result.fileURL)
        #expect(records.count == 2 + 2 + 1,
                "expected 2 asset + 2 decision + 1 correction = 5 records, got \(records.count)")
        let typeCounts = Dictionary(grouping: records) { $0["type"] as? String ?? "?" }
            .mapValues { $0.count }
        #expect(typeCounts["asset"] == 2)
        #expect(typeCounts["decision"] == 2)
        #expect(typeCounts["correction"] == 1)
    }

    @Test("export skips corrupt-scope correction rows without aborting the overall export")
    func exportSkipsCorruptScopes() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-corrupt")

        let a1 = makeTestAsset(id: "asset-1")
        try await store.insertAsset(a1)

        // Insert a correction event with a deliberately corrupt scope string.
        // We bypass PersistentUserCorrectionStore because it always writes valid
        // scopes; AnalysisStore.appendCorrectionEvent takes the raw string.
        let goodScope = CorrectionScope.exactSpan(assetId: "asset-1", ordinalRange: 5...10)
        let goodEvent = CorrectionEvent(
            id: "good",
            analysisAssetId: "asset-1",
            scope: goodScope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto
        )
        let badEvent = CorrectionEvent(
            id: "bad",
            analysisAssetId: "asset-1",
            scope: "!!!not_a_scope_at_all",
            createdAt: 1_700_000_001,
            source: .manualVeto
        )
        try await store.appendCorrectionEvent(goodEvent)
        try await store.appendCorrectionEvent(badEvent)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.correctionCount == 1,
                "Only the good correction should be exported")
        #expect(result.skippedCorrectionCount == 1,
                "One corrupt-scope row should be logged as skipped")

        let records = try parseJSONL(at: result.fileURL)
        let correctionRecords = records.filter { ($0["type"] as? String) == "correction" }
        #expect(correctionRecords.count == 1)
        let ids = correctionRecords.compactMap { $0["id"] as? String }
        #expect(ids == ["good"])
    }

    @Test("export writes streaming via FileHandle — file opens and closes cleanly even with many rows")
    func exportStreamingWorksAtScale() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-stream")

        // Seed 50 assets with 5 spans each (250 decision rows) to exercise
        // the streaming path without depending on Array-in-memory accumulation.
        var spans: [DecodedSpan] = []
        for i in 0..<50 {
            let assetId = "asset-\(i)"
            try await store.insertAsset(makeTestAsset(id: assetId))
            for j in 0..<5 {
                let first = j * 10
                let last = first + 5
                spans.append(makeSpan(assetId: assetId, firstOrdinal: first, lastOrdinal: last))
            }
        }
        try await store.upsertDecodedSpans(spans)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.assetCount == 50)
        #expect(result.spanCount == 250)

        // File exists, line count matches, and the last line parses cleanly
        // (proves the FileHandle was flushed + closed).
        let records = try parseJSONL(at: result.fileURL)
        #expect(records.count == 50 + 250)
        #expect(records.last?["type"] as? String != nil)
    }

    @Test("filename is produced from current timestamp in the Documents directory — file lives where Files.app can see it")
    func filePathLocation() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-path")
        try await store.insertAsset(makeTestAsset(id: "asset-z"))

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.fileURL.deletingLastPathComponent().path == docs.path,
                "file must be in the provided documents URL")
        #expect(result.fileURL.lastPathComponent.hasPrefix("corpus-export."))
        #expect(result.fileURL.lastPathComponent.hasSuffix(".jsonl"))
    }

    // MARK: - Test helpers

    private func makeSpan(
        assetId: String,
        startTime: Double = 10.0,
        endTime: Double = 40.0,
        firstOrdinal: Int = 100,
        lastOrdinal: Int = 200
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: firstOrdinal, lastAtomOrdinal: lastOrdinal),
            assetId: assetId,
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: []
        )
    }

    private func decodeJSONObject(from data: Data) throws -> [String: Any] {
        guard let obj = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw NSError(domain: "CorpusExporterTests", code: 1,
                          userInfo: [NSLocalizedDescriptionKey: "not a JSON object"])
        }
        return obj
    }

    private func parseJSONL(at url: URL) throws -> [[String: Any]] {
        let text = try String(contentsOf: url, encoding: .utf8)
        var out: [[String: Any]] = []
        for line in text.split(whereSeparator: { $0.isNewline }) {
            let lineStr = String(line)
            guard !lineStr.isEmpty else { continue }
            guard let data = lineStr.data(using: .utf8) else {
                throw NSError(domain: "CorpusExporterTests", code: 2,
                              userInfo: [NSLocalizedDescriptionKey: "bad utf8: \(lineStr)"])
            }
            guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                throw NSError(domain: "CorpusExporterTests", code: 3,
                              userInfo: [NSLocalizedDescriptionKey: "not a JSON object: \(lineStr)"])
            }
            out.append(json)
        }
        return out
    }
}

#endif
