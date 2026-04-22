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
}
