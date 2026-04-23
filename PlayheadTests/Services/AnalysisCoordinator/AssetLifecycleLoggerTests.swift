// AssetLifecycleLoggerTests.swift
// playhead-gtt9.8: per-asset lifecycle logger that appends one JSONL
// record per SessionState transition to
// `Documents/asset-lifecycle-log.jsonl`. Schema mirrors DecisionLogger
// (10 MB rotation threshold, stable JSON key ordering, crash-safe
// rotation via `FileManager.replaceItemAt`).
//
// These tests are the red gate for Commit 7: they construct an
// `AssetLifecycleLogger` pointed at a temp directory, record a
// synthetic transition, force a flush, decode the resulting JSON line,
// and assert the round-trip matches. The logger must expose the same
// shape of test hooks DecisionLogger does (`flushAndClose`,
// `activeLogURL`, `rotatedLogURLs`).

import Foundation
import Testing

@testable import Playhead

@Suite("AssetLifecycleLogger — gtt9.8")
struct AssetLifecycleLoggerTests {

    @Test("record appends one JSONL line to the active log")
    func recordAppendsLine() async throws {
        let dir = try makeTempDir(prefix: "AssetLifecycleLogger")
        let logger = try AssetLifecycleLogger(directory: dir)

        let entry = AssetLifecycleLogEntry(
            schemaVersion: AssetLifecycleLogEntry.currentSchemaVersion,
            analysisAssetID: "asset-alpha",
            sessionID: "sess-alpha",
            timestamp: 1_745_360_000.0,
            fromState: "backfill",
            toState: "completeFull",
            terminalReason: "full coverage: transcript 0.981, feature 0.992",
            episodeDurationSec: 3600.0,
            featureCoverageEndSec: 3575.0,
            transcriptCoverageEndSec: 3540.0
        )
        await logger.record(entry)
        await logger.flushAndClose()

        let url = await logger.activeLogURL
        let data = try Data(contentsOf: url)
        let text = String(decoding: data, as: UTF8.self)
        let lines = text.split(separator: "\n", omittingEmptySubsequences: true)
        #expect(lines.count == 1)

        let decoded = try JSONDecoder().decode(
            AssetLifecycleLogEntry.self,
            from: Data(lines[0].utf8)
        )
        #expect(decoded == entry)
    }

    @Test("multiple records accumulate as distinct lines")
    func multipleRecordsProduceMultipleLines() async throws {
        let dir = try makeTempDir(prefix: "AssetLifecycleLogger")
        let logger = try AssetLifecycleLogger(directory: dir)

        for i in 0..<5 {
            let entry = AssetLifecycleLogEntry(
                schemaVersion: AssetLifecycleLogEntry.currentSchemaVersion,
                analysisAssetID: "asset-\(i)",
                sessionID: "sess-\(i)",
                timestamp: Double(1_745_360_000 + i),
                fromState: "backfill",
                toState: "completeFull",
                terminalReason: "full",
                episodeDurationSec: 3600,
                featureCoverageEndSec: 3575,
                transcriptCoverageEndSec: 3540
            )
            await logger.record(entry)
        }
        await logger.flushAndClose()

        let url = await logger.activeLogURL
        let text = String(decoding: try Data(contentsOf: url), as: UTF8.self)
        let lines = text.split(separator: "\n", omittingEmptySubsequences: true)
        #expect(lines.count == 5)
    }

    @Test("rotation fires when active file crosses the threshold")
    func rotationFires() async throws {
        let dir = try makeTempDir(prefix: "AssetLifecycleLogger")
        // A tiny threshold (512 bytes) so we don't have to write 10 MB
        // of records to exercise the rotation path.
        let logger = try AssetLifecycleLogger(
            directory: dir,
            rotationThresholdBytes: 512
        )

        let padding = String(repeating: "x", count: 300)
        for i in 0..<6 {
            let entry = AssetLifecycleLogEntry(
                schemaVersion: AssetLifecycleLogEntry.currentSchemaVersion,
                analysisAssetID: "asset-\(i)",
                sessionID: "sess-\(i)",
                timestamp: Double(1_745_360_000 + i),
                fromState: "backfill",
                toState: "completeFull",
                terminalReason: padding,
                episodeDurationSec: 3600,
                featureCoverageEndSec: 3575,
                transcriptCoverageEndSec: 3540
            )
            await logger.record(entry)
        }
        await logger.flushAndClose()

        let rotated = await logger.rotatedLogURLs()
        #expect(rotated.count >= 1, "Expected at least one rotated log after 6 × 300B records")
    }
}
