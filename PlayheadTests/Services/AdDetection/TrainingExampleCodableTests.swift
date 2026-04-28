// TrainingExampleCodableTests.swift
// playhead-4my.10.1: Codable round-trip + schema versioning + null-field
// serialization for the TrainingExample snapshot envelope.
//
// The snapshot envelope is what gets written into the SQLite cohort blob
// columns (`scanCohortJSON`, `decisionCohortJSON`) and any export feed.
// It carries an explicit `schemaVersion` so future migrations can branch on
// the on-disk format. Nullable fields (`textSnapshot`, `eligibilityGate`,
// etc.) MUST serialize as JSON `null` (not be omitted) — the schema is
// self-identifying and downstream pipelines expect the keys to be present.

import Foundation
import Testing

@testable import Playhead

@Suite("TrainingExample Codable — playhead-4my.10.1")
struct TrainingExampleCodableTests {

    private func makeExample(
        textSnapshot: String? = nil,
        eligibilityGate: String? = nil,
        userAction: String? = nil
    ) -> TrainingExample {
        TrainingExample(
            id: "te-1",
            analysisAssetId: "asset-1",
            startAtomOrdinal: 100,
            endAtomOrdinal: 200,
            transcriptVersion: "tv-1",
            startTime: 60.0,
            endTime: 120.0,
            textSnapshotHash: "hash-abc",
            textSnapshot: textSnapshot,
            bucket: .positive,
            commercialIntent: "paid",
            ownership: "thirdParty",
            evidenceSources: ["fm", "lexical"],
            fmCertainty: 0.92,
            classifierConfidence: 0.81,
            userAction: userAction,
            eligibilityGate: eligibilityGate,
            scanCohortJSON: "{\"prompt\":\"v1\"}",
            decisionCohortJSON: "{\"fusion\":\"v1\"}",
            transcriptQuality: "good",
            createdAt: 1_700_000_000.0
        )
    }

    @Test("schemaVersion is encoded into the JSON envelope")
    func schemaVersionPresentOnEncode() throws {
        let example = makeExample()
        let data = try JSONEncoder().encode(example)
        let raw = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        let version = raw["schemaVersion"] as? Int
        #expect(version == TrainingExample.schemaVersion)
    }

    @Test("round-trips with no losses")
    func roundTripsCleanly() throws {
        let original = makeExample(
            textSnapshot: "hello world",
            eligibilityGate: "eligible",
            userAction: "skipped"
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(TrainingExample.self, from: data)
        #expect(decoded == original)
    }

    @Test("nullable textSnapshot encodes as JSON null, not omitted")
    func nullableTextSnapshotEncodesAsNull() throws {
        let example = makeExample(textSnapshot: nil)
        let data = try JSONEncoder().encode(example)
        let raw = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        // Self-identifying schema: the key must be present even when the
        // value is nil. `JSONSerialization` materializes JSON null as
        // `NSNull`; absence would yield no key at all.
        #expect(raw.keys.contains("textSnapshot"))
        #expect(raw["textSnapshot"] is NSNull)
    }

    @Test("nullable userAction and eligibilityGate encode as JSON null")
    func otherNullableFieldsEncodeAsNull() throws {
        let example = makeExample(
            textSnapshot: "x",
            eligibilityGate: nil,
            userAction: nil
        )
        let data = try JSONEncoder().encode(example)
        let raw = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect(raw["userAction"] is NSNull)
        #expect(raw["eligibilityGate"] is NSNull)
    }

    @Test("decoder tolerates missing nullable fields (decodeIfPresent)")
    func decoderTolerantOfMissingNullableFields() throws {
        // Construct a minimal JSON envelope without optional fields. The
        // decoder must default these to nil rather than throw.
        let json = """
        {
          "schemaVersion": \(TrainingExample.schemaVersion),
          "id": "te-2",
          "analysisAssetId": "asset-2",
          "startAtomOrdinal": 0,
          "endAtomOrdinal": 10,
          "transcriptVersion": "tv-2",
          "startTime": 0.0,
          "endTime": 5.0,
          "textSnapshotHash": "h2",
          "bucket": "uncertain",
          "commercialIntent": "unknown",
          "ownership": "unknown",
          "evidenceSources": [],
          "fmCertainty": 0.0,
          "classifierConfidence": 0.0,
          "scanCohortJSON": "{}",
          "decisionCohortJSON": "{}",
          "transcriptQuality": "unusable",
          "createdAt": 0.0
        }
        """
        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(TrainingExample.self, from: data)
        #expect(decoded.textSnapshot == nil)
        #expect(decoded.userAction == nil)
        #expect(decoded.eligibilityGate == nil)
        #expect(decoded.bucket == .uncertain)
    }
}
