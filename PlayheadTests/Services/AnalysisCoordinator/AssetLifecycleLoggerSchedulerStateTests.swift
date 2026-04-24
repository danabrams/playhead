// AssetLifecycleLoggerSchedulerStateTests.swift
// playhead-gtt9.14: chunk-start lifecycle events must capture the
// scheduler's (scenePhase, playbackContext, qualityProfile) tuple so the
// NARL harness can bucket post-ship real-data traces by admission state.
//
// Schema v2 adds three optional fields:
//   schedulerScenePhase       (String?)  "foreground" | "background"
//   schedulerPlaybackContext  (String?)  "playing" | "paused" | "idle"
//   schedulerQualityProfile   (String?)  QualityProfile.rawValue
//
// Backwards compat: v1 rows decode cleanly against v2; the new fields
// default to nil when absent. This mirrors the logger's additive-only
// evolution policy.

import Foundation
import Testing
@testable import Playhead

@Suite("AssetLifecycleLogger — scheduler-state fields (playhead-gtt9.14)")
struct AssetLifecycleLoggerSchedulerStateTests {

    @Test("schema version bumped to 2")
    func schemaVersionIsTwo() {
        #expect(AssetLifecycleLogEntry.currentSchemaVersion == 2,
                "gtt9.14 adds scheduler-state fields; schema version must reflect the expansion")
    }

    @Test("entry round-trips with scheduler-state fields populated")
    func roundTripWithSchedulerFields() async throws {
        let dir = try makeTempDir(prefix: "AssetLifecycleLoggerSchedulerState")
        let logger = try AssetLifecycleLogger(directory: dir)

        let entry = AssetLifecycleLogEntry(
            schemaVersion: AssetLifecycleLogEntry.currentSchemaVersion,
            analysisAssetID: "asset-sched-1",
            sessionID: "sess-sched-1",
            timestamp: 1_745_500_000.0,
            fromState: "featuresReady",
            toState: "backfill",
            terminalReason: nil,
            episodeDurationSec: 3600,
            featureCoverageEndSec: 1200,
            transcriptCoverageEndSec: 1200,
            schedulerScenePhase: "foreground",
            schedulerPlaybackContext: "paused",
            schedulerQualityProfile: "nominal"
        )
        await logger.record(entry)
        await logger.flushAndClose()

        let url = await logger.activeLogURL
        let text = String(decoding: try Data(contentsOf: url), as: UTF8.self)
        let lines = text.split(separator: "\n", omittingEmptySubsequences: true)
        #expect(lines.count == 1)

        let decoded = try JSONDecoder().decode(
            AssetLifecycleLogEntry.self,
            from: Data(lines[0].utf8)
        )
        #expect(decoded == entry)
        #expect(decoded.schedulerScenePhase == "foreground")
        #expect(decoded.schedulerPlaybackContext == "paused")
        #expect(decoded.schedulerQualityProfile == "nominal")
    }

    @Test("legacy v1 JSON decodes without scheduler fields (nil defaults)")
    func legacyV1DecodesCleanly() throws {
        // A literal v1 record — pre-gtt9.14. The decoder must accept it
        // and surface the three new fields as nil.
        let legacyJSON = """
        {
          "analysisAssetID": "asset-legacy",
          "episodeDurationSec": 1800,
          "featureCoverageEndSec": 900,
          "fromState": "spooling",
          "schemaVersion": 1,
          "sessionID": "sess-legacy",
          "terminalReason": null,
          "timestamp": 1745400000,
          "toState": "featuresReady",
          "transcriptCoverageEndSec": 900
        }
        """
        let data = Data(legacyJSON.utf8)
        let decoded = try JSONDecoder().decode(AssetLifecycleLogEntry.self, from: data)

        #expect(decoded.schemaVersion == 1)
        #expect(decoded.schedulerScenePhase == nil)
        #expect(decoded.schedulerPlaybackContext == nil)
        #expect(decoded.schedulerQualityProfile == nil)
        #expect(decoded.analysisAssetID == "asset-legacy")
    }

    @Test("entry with nil scheduler fields (v2 without snapshot) round-trips")
    func v2WithNilSchedulerFields() async throws {
        let dir = try makeTempDir(prefix: "AssetLifecycleLoggerSchedulerStateNil")
        let logger = try AssetLifecycleLogger(directory: dir)

        let entry = AssetLifecycleLogEntry(
            schemaVersion: AssetLifecycleLogEntry.currentSchemaVersion,
            analysisAssetID: "asset-nil",
            sessionID: "sess-nil",
            timestamp: 1_745_600_000,
            fromState: "",
            toState: "spooling",
            terminalReason: nil,
            episodeDurationSec: 0,
            featureCoverageEndSec: nil,
            transcriptCoverageEndSec: nil,
            schedulerScenePhase: nil,
            schedulerPlaybackContext: nil,
            schedulerQualityProfile: nil
        )
        await logger.record(entry)
        await logger.flushAndClose()

        let url = await logger.activeLogURL
        let text = String(decoding: try Data(contentsOf: url), as: UTF8.self)
        let lines = text.split(separator: "\n", omittingEmptySubsequences: true)
        let decoded = try JSONDecoder().decode(
            AssetLifecycleLogEntry.self,
            from: Data(lines[0].utf8)
        )
        #expect(decoded == entry)
    }
}
