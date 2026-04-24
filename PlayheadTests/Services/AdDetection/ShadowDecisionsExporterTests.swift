// ShadowDecisionsExporterTests.swift
// playhead-narl.2: Export + round-trip tests for `shadow-decisions.jsonl`.
//
// Coverage:
//   1. Empty store writes an empty file (zero-byte; file present, no rows).
//   2. Single-row round-trip: persisted row → exported JSONL → parsed row
//      reconstructs exactly (including the opaque BLOB).
//   3. Multi-row round-trip: stable (assetId, windowStart) ordering.
//   4. Binding AC: export → parse → a minimal corpus-builder-style reader
//      can reconstruct `.allEnabled` FM evidence keyed by (assetId, start, end).
//   5. Schema version guard: a line with an unsupported schemaVersion raises
//      `.unsupportedSchema`.
//   6. Kill-switch export path: with the kill switch on but no shadow rows,
//      the exporter still writes an empty file (the harness distinguishes
//      "no file yet" from "file present but empty").

import Foundation
import Testing

@testable import Playhead

@Suite("Shadow decisions exporter + round-trip (playhead-narl.2)")
struct ShadowDecisionsExporterTests {

    // MARK: - 1. Empty store

    @Test("Empty store writes shadow-decisions.jsonl with zero rows")
    func emptyStoreWritesEmptyFile() async throws {
        let store = try await makeTestStore()
        let docsDir = try makeTempDir(prefix: "Shadow-Export-Empty")
        defer { try? FileManager.default.removeItem(at: docsDir) }

        let result = try await ShadowDecisionsExporter.export(
            source: store, documentsURL: docsDir
        )
        #expect(result.rowCount == 0)
        #expect(FileManager.default.fileExists(atPath: result.fileURL.path))
        let data = try Data(contentsOf: result.fileURL)
        #expect(data.isEmpty)
    }

    // MARK: - 2. Single-row round-trip

    @Test("Single-row round-trip reconstructs the row bit-for-bit")
    func singleRowRoundTrip() async throws {
        let store = try await makeTestStore()
        let blob = Data([0xDE, 0xAD, 0xBE, 0xEF, 0x7E, 0x01])
        let original = ShadowFMResponse(
            assetId: "asset-1",
            windowStart: 10.5,
            windowEnd: 25.25,
            configVariant: .allEnabledShadow,
            fmResponse: blob,
            capturedAt: 1_700_000_123.75,
            capturedBy: .laneA,
            fmModelVersion: "fm-7.2"
        )
        try await store.upsertShadowFMResponse(original)

        let docsDir = try makeTempDir(prefix: "Shadow-Export-Single")
        defer { try? FileManager.default.removeItem(at: docsDir) }

        let result = try await ShadowDecisionsExporter.export(
            source: store, documentsURL: docsDir
        )
        #expect(result.rowCount == 1)

        let parsed = try ShadowDecisionsExporter.parseAll(fileURL: result.fileURL)
        #expect(parsed == [original])
    }

    // MARK: - 3. Multi-row stable ordering

    @Test("Multi-row export: rows are ordered by (assetId, windowStart)")
    func multiRowStableOrdering() async throws {
        let store = try await makeTestStore()
        // Insert out of order.
        let rows = [
            makeRow(asset: "b", start: 30, end: 40),
            makeRow(asset: "a", start: 20, end: 30),
            makeRow(asset: "a", start: 0, end: 10),
            makeRow(asset: "b", start: 10, end: 20),
        ]
        for row in rows { try await store.upsertShadowFMResponse(row) }

        let docsDir = try makeTempDir(prefix: "Shadow-Export-Multi")
        defer { try? FileManager.default.removeItem(at: docsDir) }
        let result = try await ShadowDecisionsExporter.export(
            source: store, documentsURL: docsDir
        )
        #expect(result.rowCount == 4)

        let parsed = try ShadowDecisionsExporter.parseAll(fileURL: result.fileURL)
        #expect(parsed.map { $0.assetId } == ["a", "a", "b", "b"])
        #expect(parsed.map { $0.windowStart } == [0, 20, 10, 30])
    }

    // MARK: - 4. Binding AC: minimal corpus-builder reader

    @Test("Binding AC: a minimal corpus-builder-style reader reconstructs allEnabled FM evidence")
    func bindingACCorpusBuilderReader() async throws {
        // Seed a realistic multi-asset, multi-window store.
        let store = try await makeTestStore()
        let asset1Rows: [ShadowFMResponse] = [
            ShadowFMResponse(
                assetId: "episode-conan-ep42",
                windowStart: 0, windowEnd: 30,
                configVariant: .allEnabledShadow,
                fmResponse: Data([0x01, 0x02, 0x03]),
                capturedAt: 1_700_000_100,
                capturedBy: .laneA,
                fmModelVersion: "fm-7.2"
            ),
            ShadowFMResponse(
                assetId: "episode-conan-ep42",
                windowStart: 30, windowEnd: 60,
                configVariant: .allEnabledShadow,
                fmResponse: Data([0x04, 0x05]),
                capturedAt: 1_700_000_200,
                capturedBy: .laneB,
                fmModelVersion: "fm-7.2"
            ),
        ]
        let asset2Rows: [ShadowFMResponse] = [
            ShadowFMResponse(
                assetId: "episode-doac-ep99",
                windowStart: 100, windowEnd: 160,
                configVariant: .allEnabledShadow,
                fmResponse: Data([0xAA, 0xBB, 0xCC, 0xDD]),
                capturedAt: 1_700_000_300,
                capturedBy: .laneA,
                fmModelVersion: "fm-7.3"
            ),
        ]
        for r in asset1Rows + asset2Rows { try await store.upsertShadowFMResponse(r) }

        // Export bundle.
        let docsDir = try makeTempDir(prefix: "Shadow-Export-CorpusAC")
        defer { try? FileManager.default.removeItem(at: docsDir) }
        let result = try await ShadowDecisionsExporter.export(
            source: store, documentsURL: docsDir
        )
        #expect(result.rowCount == 3)

        // Minimal corpus-builder reader: index by (assetId, windowStart, windowEnd).
        // This mirrors what `playhead-narl.1` will do — it doesn't need this
        // exact type, only the ability to reconstruct keyed FM evidence.
        let parsed = try ShadowDecisionsExporter.parseAll(fileURL: result.fileURL)
        var index: [String: [ShadowWindowKey: Data]] = [:]
        for row in parsed {
            let key = ShadowWindowKey(start: row.windowStart, end: row.windowEnd)
            index[row.assetId, default: [:]][key] = row.fmResponse
        }

        // Assert reconstruction exactness for every originally-stored row.
        for original in asset1Rows + asset2Rows {
            let key = ShadowWindowKey(start: original.windowStart, end: original.windowEnd)
            let reconstructed = index[original.assetId]?[key]
            #expect(reconstructed == original.fmResponse,
                    "asset=\(original.assetId) window=\(key) FM evidence missing or mismatched")
        }
    }

    // MARK: - 5. Schema version guard

    @Test("Parser rejects an unsupported schemaVersion")
    func parserRejectsUnsupportedSchemaVersion() throws {
        let obj: [String: Any] = [
            "schemaVersion": 99,
            "type": "shadow_fm_response",
            "assetId": "asset-x",
            "windowStart": 0.0,
            "windowEnd": 1.0,
            "configVariant": "allEnabledShadow",
            "fmResponseBase64": Data([0x00]).base64EncodedString(),
            "capturedAt": 1_700_000_000.0,
            "capturedBy": "laneA",
            "fmModelVersion": "fm-test",
        ]
        let data = try JSONSerialization.data(withJSONObject: obj)
        #expect(throws: ShadowDecisionsParseError.self) {
            _ = try ShadowDecisionsExporter.parse(line: data)
        }
    }

    // MARK: - 5b. Top-level isAd / shadowConfidence (gtt9.4.4)
    //
    // The shadow row's opaque `fmResponseBase64` blob hides the classifier's
    // decision behind a base64-encoded `ShadowFMPayload`. Without a top-level
    // boolean, the NARL corpus builder reads `obj["isAd"] as? Bool` → nil for
    // every row and folds them all as weight=0 (see gtt9.4.5 and the
    // 2026-04-24 spike doc). These tests pin the write-side fix: the exporter
    // must decode the blob and surface `isAd` (Bool) and `shadowConfidence`
    // (Float in [0, 1]) at the JSON root.

    @Test("serialize emits top-level isAd=true and shadowConfidence>0 for an ad-bearing refinement")
    func serializeEmitsIsAdTrueAndConfidenceForAdSpan() throws {
        let response = RefinementWindowSchema(spans: [
            SpanRefinementSchema(
                commercialIntent: .paid,
                ownership: .thirdParty,
                firstLineRef: 0,
                lastLineRef: 4,
                certainty: .strong,
                boundaryPrecision: .precise,
                evidenceAnchors: []
            )
        ])
        let payload = ShadowFMPayload(
            payloadSchemaVersion: shadowFMPayloadSchemaVersion,
            promptText: "host says: brought to you by hims dot com",
            refinementResponse: response,
            errorTag: nil
        )
        let row = makeRow(asset: "asset-ad", start: 0, end: 30,
                          payload: payload)

        let line = try ShadowDecisionsExporter.serialize(row)
        let obj = try JSONSerialization.jsonObject(with: line) as? [String: Any]
        #expect(obj?["isAd"] as? Bool == true)
        let conf = (obj?["shadowConfidence"] as? NSNumber)?.doubleValue
        #expect(conf != nil)
        #expect((conf ?? 0) > 0.5,
                "strong-certainty paid span should produce a high shadowConfidence; got \(conf ?? -1)")
        #expect((conf ?? 0) <= 1.0)
    }

    @Test("serialize emits top-level isAd=false and shadowConfidence=0 for an organic-only refinement")
    func serializeEmitsIsAdFalseForOrganicSpan() throws {
        let response = RefinementWindowSchema(spans: [
            SpanRefinementSchema(
                commercialIntent: .organic,
                ownership: .show,
                firstLineRef: 0,
                lastLineRef: 4,
                certainty: .moderate,
                boundaryPrecision: .usable,
                evidenceAnchors: []
            )
        ])
        let payload = ShadowFMPayload(
            payloadSchemaVersion: shadowFMPayloadSchemaVersion,
            promptText: "host monologue, no commerce",
            refinementResponse: response,
            errorTag: nil
        )
        let row = makeRow(asset: "asset-organic", start: 0, end: 30, payload: payload)

        let line = try ShadowDecisionsExporter.serialize(row)
        let obj = try JSONSerialization.jsonObject(with: line) as? [String: Any]
        #expect(obj?["isAd"] as? Bool == false)
        let conf = (obj?["shadowConfidence"] as? NSNumber)?.doubleValue ?? -1
        #expect(conf == 0.0)
    }

    @Test("serialize emits top-level isAd=false and shadowConfidence=0 for a refinement with empty spans")
    func serializeEmitsIsAdFalseForEmptySpans() throws {
        let response = RefinementWindowSchema(spans: [])
        let payload = ShadowFMPayload(
            payloadSchemaVersion: shadowFMPayloadSchemaVersion,
            promptText: "no spans returned",
            refinementResponse: response,
            errorTag: nil
        )
        let row = makeRow(asset: "asset-empty", start: 0, end: 30, payload: payload)

        let line = try ShadowDecisionsExporter.serialize(row)
        let obj = try JSONSerialization.jsonObject(with: line) as? [String: Any]
        #expect(obj?["isAd"] as? Bool == false)
        let conf = (obj?["shadowConfidence"] as? NSNumber)?.doubleValue ?? -1
        #expect(conf == 0.0)
    }

    @Test("serialize keeps fmResponseBase64 for back-compat alongside the new top-level fields")
    func serializeRetainsBase64ForBackCompat() async throws {
        let response = RefinementWindowSchema(spans: [
            SpanRefinementSchema(
                commercialIntent: .paid,
                ownership: .thirdParty,
                firstLineRef: 0, lastLineRef: 2,
                certainty: .moderate,
                boundaryPrecision: .usable,
                evidenceAnchors: []
            )
        ])
        let payload = ShadowFMPayload(
            payloadSchemaVersion: shadowFMPayloadSchemaVersion,
            promptText: "x",
            refinementResponse: response,
            errorTag: nil
        )
        let row = makeRow(asset: "asset-bc", start: 0, end: 30, payload: payload)

        let line = try ShadowDecisionsExporter.serialize(row)
        let obj = try JSONSerialization.jsonObject(with: line) as? [String: Any]
        #expect(obj?["fmResponseBase64"] as? String != nil,
                "back-compat: opaque blob must remain in the wire format")
        // Round-trip the row through parseAll so old consumers that only
        // know about fmResponseBase64 keep working.
        let docsDir = try makeTempDir(prefix: "Shadow-Export-BackCompat")
        defer { try? FileManager.default.removeItem(at: docsDir) }
        let store = try await makeTestStore()
        try await store.upsertShadowFMResponse(row)
        let result = try await ShadowDecisionsExporter.export(
            source: store, documentsURL: docsDir
        )
        let parsed = try ShadowDecisionsExporter.parseAll(fileURL: result.fileURL)
        #expect(parsed == [row])
    }

    @Test("serialize emits isAd=false / shadowConfidence=0 when refinementResponse is nil (FM error path)")
    func serializeEmitsZeroForNilRefinementResponse() throws {
        let payload = ShadowFMPayload(
            payloadSchemaVersion: shadowFMPayloadSchemaVersion,
            promptText: "x",
            refinementResponse: nil,
            errorTag: "refusal"
        )
        let row = makeRow(asset: "asset-err", start: 0, end: 30, payload: payload)
        let line = try ShadowDecisionsExporter.serialize(row)
        let obj = try JSONSerialization.jsonObject(with: line) as? [String: Any]
        #expect(obj?["isAd"] as? Bool == false)
        let conf = (obj?["shadowConfidence"] as? NSNumber)?.doubleValue ?? -1
        #expect(conf == 0.0)
    }

    // MARK: - 6. Kill-switch does not suppress export file itself

    @Test("Empty-but-present shadow-decisions.jsonl is distinguishable from a missing file")
    func emptyButPresentFileIsDistinguishable() async throws {
        let store = try await makeTestStore()
        let docsDir = try makeTempDir(prefix: "Shadow-Export-KillSwitch")
        defer { try? FileManager.default.removeItem(at: docsDir) }

        let result = try await ShadowDecisionsExporter.export(
            source: store, documentsURL: docsDir
        )
        let fm = FileManager.default
        #expect(fm.fileExists(atPath: result.fileURL.path))
        // Size 0 is the "present but empty" signal.
        let attrs = try fm.attributesOfItem(atPath: result.fileURL.path)
        #expect((attrs[.size] as? NSNumber)?.intValue == 0)
    }

    // MARK: - Helpers

    private func makeRow(asset: String, start: TimeInterval, end: TimeInterval) -> ShadowFMResponse {
        ShadowFMResponse(
            assetId: asset, windowStart: start, windowEnd: end,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0xAB]),
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-test"
        )
    }

    /// Variant that wires a `ShadowFMPayload` into the row's `fmResponse`
    /// blob. Used by the gtt9.4.4 tests that exercise the exporter's
    /// blob-decoding path.
    private func makeRow(
        asset: String,
        start: TimeInterval,
        end: TimeInterval,
        payload: ShadowFMPayload
    ) -> ShadowFMResponse {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        // Test-only force-try: encoding a Codable value with finite
        // numbers and plain Strings is total here. A failure would be a
        // Foundation regression worth crashing the suite on.
        // swiftlint:disable:next force_try
        let bytes = try! encoder.encode(payload)
        return ShadowFMResponse(
            assetId: asset, windowStart: start, windowEnd: end,
            configVariant: .allEnabledShadow,
            fmResponse: bytes,
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-test"
        )
    }
}
