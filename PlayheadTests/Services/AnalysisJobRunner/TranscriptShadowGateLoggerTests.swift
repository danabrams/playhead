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

// MARK: - Logger (file I/O)

@Suite("TranscriptShadowGateLogger — append + rotation", .serialized)
struct TranscriptShadowGateLoggerFileIOTests {

    private func makeTempDir(function: String = #function) throws -> URL {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("transcript-shadow-gate-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        return base
    }

    private func sampleEntry(asset: String = "asset-a",
                             timestamp: Double = 1_745_000_000.0) -> TranscriptShadowGateEntry {
        TranscriptShadowGateEntry(
            schemaVersion: TranscriptShadowGateEntry.currentSchemaVersion,
            timestamp: timestamp,
            analysisAssetID: asset,
            episodeID: "ep-\(asset)",
            shardID: 1,
            shardStart: 0.0,
            shardEnd: 30.0,
            likelihood: 0.42,
            threshold: 0.55,
            decision: .wouldSkip,
            wouldGate: true,
            transcribed: true,
            buildCommitSHA: nil  // logger overwrites with BuildInfo.commitSHA
        )
    }

    @Test("record(_:) appends one JSON line per call to transcript-shadow-gate.jsonl")
    func appendsJSONL() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try TranscriptShadowGateLogger(directory: dir)
        await logger.record(sampleEntry(asset: "a"))
        await logger.record(sampleEntry(asset: "b"))
        await logger.flushAndClose()

        let url = dir.appendingPathComponent(TranscriptShadowGateLogger.activeLogFilename)
        let data = try Data(contentsOf: url)
        let lines = String(decoding: data, as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
        #expect(lines.count == 2)

        let decoder = JSONDecoder()
        let first = try decoder.decode(TranscriptShadowGateEntry.self, from: Data(lines[0].utf8))
        let second = try decoder.decode(TranscriptShadowGateEntry.self, from: Data(lines[1].utf8))
        #expect(first.analysisAssetID == "a")
        #expect(second.analysisAssetID == "b")
    }

    @Test("Every encoded row carries the logger's buildCommitSHA stamp")
    func everyEntryStampedWithBuildCommitSHA() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try TranscriptShadowGateLogger(directory: dir)
        // Caller passes nil; logger must overwrite with BuildInfo.commitSHA.
        await logger.record(sampleEntry(asset: "a"))
        await logger.flushAndClose()

        let url = dir.appendingPathComponent(TranscriptShadowGateLogger.activeLogFilename)
        let data = try Data(contentsOf: url)
        let line = String(decoding: data, as: UTF8.self)
            .split(separator: "\n").first.map(String.init) ?? ""
        let decoded = try JSONDecoder().decode(
            TranscriptShadowGateEntry.self, from: Data(line.utf8)
        )
        #expect(decoded.buildCommitSHA == BuildInfo.commitSHA)
        #expect(decoded.buildCommitSHA?.isEmpty == false,
                "BuildInfo.commitSHA contract: never empty (falls back to 'unknown')")
    }

    @Test("Exceeding threshold rotates active file to transcript-shadow-gate.1.jsonl")
    func rotatesOnThreshold() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Threshold of 1 byte triggers rotation as soon as the active file
        // has >= 2 lines (livelock guard requires >1 line before rotating).
        let logger = try TranscriptShadowGateLogger(directory: dir, rotationThresholdBytes: 1)
        await logger.record(sampleEntry(asset: "a"))
        await logger.record(sampleEntry(asset: "b"))
        await logger.flushAndClose()

        let rotated = dir.appendingPathComponent("transcript-shadow-gate.1.jsonl")
        #expect(FileManager.default.fileExists(atPath: rotated.path))
    }

    @Test("Warm start seeds next rotation index from highest existing rotated file")
    func warmStartSeedsFromDisk() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Pre-seed a synthetic rotated file at index 5.
        let preExisting = dir.appendingPathComponent("transcript-shadow-gate.5.jsonl")
        try "pre-seeded\n".data(using: .utf8)!.write(to: preExisting)

        let logger = try TranscriptShadowGateLogger(directory: dir, rotationThresholdBytes: 1)
        let seed = await logger.currentNextRotationIndex()
        #expect(seed == 6)

        await logger.record(sampleEntry(asset: "warm-a"))
        await logger.record(sampleEntry(asset: "warm-b"))
        await logger.flushAndClose()

        let r6 = dir.appendingPathComponent("transcript-shadow-gate.6.jsonl")
        #expect(FileManager.default.fileExists(atPath: r6.path))
        #expect(FileManager.default.fileExists(atPath: preExisting.path),
                "Pre-existing rotated file must be preserved across warm start")
    }

    @Test("Livelock guard skips rotation when active file has only one line")
    func livelockGuardSkipsRotation() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try TranscriptShadowGateLogger(directory: dir, rotationThresholdBytes: 1)
        await logger.record(sampleEntry(asset: "lone"))
        await logger.flushAndClose()

        let rotated = dir.appendingPathComponent("transcript-shadow-gate.1.jsonl")
        #expect(!FileManager.default.fileExists(atPath: rotated.path),
                "Single-line file must NOT rotate — would loop forever on a >threshold record")
        let active = dir.appendingPathComponent(TranscriptShadowGateLogger.activeLogFilename)
        #expect(FileManager.default.fileExists(atPath: active.path))
    }

    @Test("No-op TranscriptShadowGateLogger writes no files")
    func noOpWritesNothing() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let noop: TranscriptShadowGateLogging = NoOpTranscriptShadowGateLogger()
        await noop.record(sampleEntry())

        let contents = try FileManager.default.contentsOfDirectory(atPath: dir.path)
        #expect(contents.isEmpty, "NoOp logger must not write any files")
    }
}
