// TranscriptShadowGateLoggerTests.swift
// playhead-b58j: coverage for the schema-v2 round-trip and the
// actor-backed JSONL writer.

import Foundation
import Testing
@testable import Playhead

// MARK: - Schema

@Suite("TranscriptShadowGateEntry — schema v2 round-trip")
struct TranscriptShadowGateEntryCodableTests {

    private func makeEntry(buildCommitSHA: String? = "abc1234") -> TranscriptShadowGateEntry {
        TranscriptShadowGateEntry(
            schemaVersion: TranscriptShadowGateEntry.currentSchemaVersion,
            timestamp: 1_745_000_000.0,
            analysisAssetID: "asset-a",
            episodeID: "ep-a",
            shardID: 3,
            shardStart: 30.0,
            shardEnd: 60.0,
            likelihood: 0.42,
            threshold: 0.55,
            decision: .wouldSkip,
            wouldGate: true,
            transcribed: true,
            buildCommitSHA: buildCommitSHA
        )
    }

    @Test("currentSchemaVersion is 2")
    func currentSchemaVersionIsTwo() {
        #expect(TranscriptShadowGateEntry.currentSchemaVersion == 2)
    }

    @Test("Encoded v2 entry is compact, newline-free, and round-trips")
    func encodesCompactJSON() throws {
        let entry = makeEntry()
        let data = try JSONEncoder().encode(entry)
        let json = String(decoding: data, as: UTF8.self)
        #expect(!json.contains("\n"),
                "Encoded entry must not embed newlines (JSONL requires one record per line)")
        let decoded = try JSONDecoder().decode(TranscriptShadowGateEntry.self, from: data)
        #expect(decoded == entry)
    }

    @Test("v2 always emits buildCommitSHA key (even when nil)")
    func encodeAlwaysEmitsBuildCommitSHAKey() throws {
        let entry = makeEntry(buildCommitSHA: nil)
        let data = try JSONEncoder().encode(entry)
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("\"buildCommitSHA\""),
                "v2 wire shape must always carry the key so consumers can self-identify the cohort")
    }

    @Test("v1 row decodes with nil buildCommitSHA")
    func v1RowDecodesWithNilBuildCommitSHA() throws {
        // Hand-crafted pre-bump row — no buildCommitSHA key, schemaVersion=1.
        let v1Json = """
        {"schemaVersion":1,"timestamp":1745000000.0,"analysisAssetID":"asset-a",\
        "episodeID":"ep-a","shardID":3,"shardStart":30.0,"shardEnd":60.0,\
        "likelihood":0.42,"threshold":0.55,"decision":"wouldSkip",\
        "wouldGate":true,"transcribed":true}
        """
        let decoded = try JSONDecoder().decode(
            TranscriptShadowGateEntry.self, from: Data(v1Json.utf8)
        )
        #expect(decoded.schemaVersion == 1)
        #expect(decoded.buildCommitSHA == nil)
        #expect(decoded.decision == .wouldSkip)
    }
}
