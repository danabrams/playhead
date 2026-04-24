// FrozenTraceProvenanceTests.swift
// playhead-gtt9.21: capture-provenance fields on FrozenTrace.
//
// Stamps `detectorVersion` (mirroring `AdDetectionConfig.detectorVersion`
// at session-start time) and `buildCommitSHA` (a short git SHA baked at
// build time) into FrozenTrace so eval reports can attribute a fixture
// to the device binary that produced it. Without these fields, a
// regression dig — like the 2026-04-24 Conan diagnosis (commit 6e37335
// vs. capture wall-clock) — has no choice but to crawl `git log` to
// decide whether a fixture is pre-fix or post-fix.
//
// Acceptance contract proven by these tests:
//   1. Fields encode and decode round-trip (no silent drop).
//   2. Old fixtures missing both fields decode with empty-string defaults
//      so the schema stays backward-compatible.
//   3. Fields are preserved by `withHoldoutDesignation(_:)` (the canonical
//      copy path the harness uses to flip training/holdout).

import XCTest
@testable import Playhead

final class FrozenTraceProvenanceTests: XCTestCase {

    // MARK: - Helpers

    private func makeTrace(
        detectorVersion: String,
        buildCommitSHA: String
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: "ep-prov-001",
            podcastId: "pod-prov-001",
            episodeDuration: 1200,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training,
            detectorVersion: detectorVersion,
            buildCommitSHA: buildCommitSHA
        )
    }

    // MARK: - Round-trip preservation

    func testProvenanceFieldsRoundTrip() throws {
        let trace = makeTrace(detectorVersion: "detection-v1", buildCommitSHA: "abc1234")

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(trace)

        let decoder = JSONDecoder()
        let decoded = try decoder.decode(FrozenTrace.self, from: data)

        XCTAssertEqual(decoded.detectorVersion, "detection-v1",
                       "detectorVersion must survive encode → decode round-trip")
        XCTAssertEqual(decoded.buildCommitSHA, "abc1234",
                       "buildCommitSHA must survive encode → decode round-trip")
    }

    func testProvenanceFieldsAppearInEncodedJSON() throws {
        let trace = makeTrace(detectorVersion: "detection-v9", buildCommitSHA: "deadbee")
        let data = try JSONEncoder().encode(trace)
        let str = String(data: data, encoding: .utf8) ?? ""
        XCTAssertTrue(str.contains("\"detectorVersion\""),
                      "detectorVersion key must be present in emitted JSON, got: \(str)")
        XCTAssertTrue(str.contains("\"buildCommitSHA\""),
                      "buildCommitSHA key must be present in emitted JSON, got: \(str)")
        XCTAssertTrue(str.contains("\"detection-v9\""),
                      "detectorVersion value must be present in emitted JSON, got: \(str)")
        XCTAssertTrue(str.contains("\"deadbee\""),
                      "buildCommitSHA value must be present in emitted JSON, got: \(str)")
    }

    // MARK: - Backward compatibility (old fixtures)

    func testOldFixtureWithoutProvenanceFieldsDecodesAsEmptyString() throws {
        // An older fixture missing both new fields. The harness's tolerance
        // contract: missing → empty string. (See ADR comment on FrozenTrace.)
        // Note: `capturedAt` is encoded as a Double (epoch seconds)
        // by JSONEncoder's default strategy — match that here so the
        // legacy fixture decodes through the same code path our real
        // fixtures use.
        let legacyJSON = """
        {
          "episodeId": "legacy-ep",
          "podcastId": "legacy-pod",
          "episodeDuration": 600.0,
          "traceVersion": "frozen-trace-v2",
          "capturedAt": 1714402200.0,
          "featureWindows": [],
          "atoms": [],
          "evidenceCatalog": [],
          "corrections": [],
          "decisionEvents": [],
          "baselineReplaySpanDecisions": [],
          "holdoutDesignation": "training"
        }
        """
        let data = legacyJSON.data(using: .utf8)!
        let decoded = try JSONDecoder().decode(FrozenTrace.self, from: data)

        XCTAssertEqual(decoded.detectorVersion, "",
                       "missing detectorVersion must decode as empty string for back-compat")
        XCTAssertEqual(decoded.buildCommitSHA, "",
                       "missing buildCommitSHA must decode as empty string for back-compat")
        // Sanity: the rest of the trace decoded normally.
        XCTAssertEqual(decoded.episodeId, "legacy-ep")
        XCTAssertEqual(decoded.podcastId, "legacy-pod")
    }

    // MARK: - withHoldoutDesignation copy path

    func testHoldoutDesignationCopyPreservesProvenance() throws {
        let trace = makeTrace(detectorVersion: "detection-v1", buildCommitSHA: "feedface")
        let copy = trace.withHoldoutDesignation(.holdout)
        XCTAssertEqual(copy.detectorVersion, "detection-v1",
                       "withHoldoutDesignation must preserve detectorVersion")
        XCTAssertEqual(copy.buildCommitSHA, "feedface",
                       "withHoldoutDesignation must preserve buildCommitSHA")
        XCTAssertEqual(copy.holdoutDesignation, .holdout)
    }

    // MARK: - BuildInfo runtime accessor

    func testBuildInfoCommitSHAIsAccessible() {
        // BuildInfo.commitSHA is the runtime accessor the corpus exporter
        // stamps onto each asset row. It must always return a non-empty
        // string (either the real SHA from the build phase, or the
        // "unknown" fallback). An empty string is the only value that's
        // never legal — it would silently degrade provenance to absent.
        let sha = BuildInfo.commitSHA
        XCTAssertFalse(sha.isEmpty,
                       "BuildInfo.commitSHA must never be empty — fallback to 'unknown' instead")
    }
}
