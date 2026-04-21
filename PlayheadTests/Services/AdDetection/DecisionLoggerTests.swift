// DecisionLoggerTests.swift
// playhead-8em9 (narL): Coverage for DecisionLogEntry serialization,
// DecisionLogger append behavior, and idempotent rotation.

import Foundation
import Testing
@testable import Playhead

@Suite("DecisionLogEntry — JSONL serialization")
struct DecisionLogEntryCodableTests {

    @Test("Encoding a minimal entry produces a compact, newline-free JSON object")
    func encodesCompactJSON() throws {
        let entry = DecisionLogEntry(
            schemaVersion: DecisionLogEntry.currentSchemaVersion,
            episodeID: "asset-a",
            timestamp: 1_745_000_000.0,
            windowBounds: .init(start: 10.0, end: 40.0),
            activationConfig: .init(.default),
            evidence: [
                DecisionLogEntry.LedgerEntry(
                    EvidenceLedgerEntry(
                        source: .lexical,
                        weight: 0.2,
                        detail: .lexical(matchedCategories: ["url"])
                    )
                )
            ],
            fusedConfidence: .init(
                proposalConfidence: 0.55,
                skipConfidence: 0.60,
                breakdown: [
                    SourceEvidence(source: "lexical", weight: 0.2,
                                   capApplied: 0.2, authority: .strong)
                ]
            ),
            finalDecision: .init(
                action: "detectOnly",
                gate: "eligible",
                skipConfidence: 0.60,
                thresholdCrossed: 0.55
            )
        )

        let data = try JSONEncoder().encode(entry)
        let json = String(decoding: data, as: UTF8.self)
        #expect(!json.contains("\n"),
                "Encoded entry must not embed newlines (JSONL requires one record per line)")
        // Sanity: round-trip.
        let decoded = try JSONDecoder().decode(DecisionLogEntry.self, from: data)
        #expect(decoded == entry)
    }

    @Test("ActivationConfigSnapshot captures all ten fields from the source config")
    func snapshotCapturesAllGates() {
        let snap = DecisionLogEntry.ActivationConfigSnapshot(.allEnabled)
        #expect(snap.lexicalInjectionEnabled == true)
        #expect(snap.classifierPriorShiftEnabled == true)
        #expect(snap.fmSchedulingEnabled == true)
        #expect(snap.counterfactualGateOpen == true)
        #expect(snap.lexicalInjectionDiscount == 0.75)
        #expect(snap.classifierShiftedMidpoint == 0.22)
        #expect(snap.classifierBaselineMidpoint == 0.25)
        #expect(snap.classifierPriorShiftMinTrust == 0.08)

        let defSnap = DecisionLogEntry.ActivationConfigSnapshot(.default)
        #expect(defSnap.counterfactualGateOpen == false)
        #expect(defSnap.lexicalInjectionEnabled == false)
    }

    @Test("LedgerEntry round-trips every EvidenceLedgerDetail variant")
    func ledgerDetailAllCases() throws {
        let cases: [EvidenceLedgerEntry] = [
            .init(source: .classifier, weight: 0.25, detail: .classifier(score: 0.7)),
            .init(source: .lexical, weight: 0.20,
                  detail: .lexical(matchedCategories: ["url", "promo"])),
            .init(source: .acoustic, weight: 0.20, detail: .acoustic(breakStrength: 0.9)),
            .init(source: .catalog, weight: 0.20, detail: .catalog(entryCount: 3)),
            .init(source: .fingerprint, weight: 0.15,
                  detail: .fingerprint(matchCount: 2, averageSimilarity: 0.88)),
        ]
        for entry in cases {
            let projection = DecisionLogEntry.LedgerEntry(entry)
            let data = try JSONEncoder().encode(projection)
            let decoded = try JSONDecoder().decode(
                DecisionLogEntry.LedgerEntry.self, from: data
            )
            #expect(decoded == projection, "Round-trip failed for source=\(entry.source.rawValue)")
        }
    }
}

@Suite("DecisionLogger — append + rotation", .serialized)
struct DecisionLoggerFileIOTests {

    // MARK: - Helpers

    private func makeTempDir(function: String = #function) throws -> URL {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("decision-logger-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        return base
    }

    private func sampleEntry(episode: String = "asset-a",
                             timestamp: Double = 1_745_000_000.0) -> DecisionLogEntry {
        DecisionLogEntry(
            schemaVersion: DecisionLogEntry.currentSchemaVersion,
            episodeID: episode,
            timestamp: timestamp,
            windowBounds: .init(start: 10.0, end: 40.0),
            activationConfig: .init(.default),
            evidence: [
                DecisionLogEntry.LedgerEntry(
                    EvidenceLedgerEntry(
                        source: .lexical,
                        weight: 0.2,
                        detail: .lexical(matchedCategories: ["url"])
                    )
                )
            ],
            fusedConfidence: .init(
                proposalConfidence: 0.55,
                skipConfidence: 0.60,
                breakdown: [
                    SourceEvidence(source: "lexical", weight: 0.2,
                                   capApplied: 0.2, authority: .strong)
                ]
            ),
            finalDecision: .init(
                action: "detectOnly",
                gate: "eligible",
                skipConfidence: 0.60,
                thresholdCrossed: 0.55
            )
        )
    }

    // MARK: - Append

    @Test("record(_:) appends one JSON line per call to decision-log.jsonl")
    func appendsJSONL() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try DecisionLogger(directory: dir,
                                        rotationThresholdBytes: 10 * 1024 * 1024)
        await logger.record(sampleEntry(episode: "a"))
        await logger.record(sampleEntry(episode: "b"))
        await logger.flushAndClose()

        let url = dir.appendingPathComponent(DecisionLogger.activeLogFilename)
        let data = try Data(contentsOf: url)
        let text = String(decoding: data, as: UTF8.self)
        let lines = text.split(separator: "\n", omittingEmptySubsequences: true)
        #expect(lines.count == 2, "Expected 2 JSONL lines, got \(lines.count)")

        let decoder = JSONDecoder()
        let first = try decoder.decode(DecisionLogEntry.self,
                                       from: Data(lines[0].utf8))
        let second = try decoder.decode(DecisionLogEntry.self,
                                        from: Data(lines[1].utf8))
        #expect(first.episodeID == "a")
        #expect(second.episodeID == "b")
    }

    @Test("record(_:) persists the MetadataActivationConfig snapshot verbatim")
    func recordsConfigSnapshot() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try DecisionLogger(directory: dir)
        var entry = sampleEntry()
        // Swap in an allEnabled snapshot.
        entry = DecisionLogEntry(
            schemaVersion: entry.schemaVersion,
            episodeID: entry.episodeID,
            timestamp: entry.timestamp,
            windowBounds: entry.windowBounds,
            activationConfig: .init(.allEnabled),
            evidence: entry.evidence,
            fusedConfidence: entry.fusedConfidence,
            finalDecision: entry.finalDecision
        )
        await logger.record(entry)
        await logger.flushAndClose()

        let url = dir.appendingPathComponent(DecisionLogger.activeLogFilename)
        let data = try Data(contentsOf: url)
        let line = String(decoding: data, as: UTF8.self)
            .split(separator: "\n")
            .first
            .map(String.init) ?? ""
        let decoded = try JSONDecoder().decode(DecisionLogEntry.self,
                                               from: Data(line.utf8))
        #expect(decoded.activationConfig.counterfactualGateOpen == true)
        #expect(decoded.activationConfig.lexicalInjectionEnabled == true)
    }

    // MARK: - Rotation

    @Test("Exceeding threshold rotates active file to decision-log.1.jsonl")
    func rotatesOnThreshold() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Tiny threshold so one write triggers rotation.
        let logger = try DecisionLogger(directory: dir, rotationThresholdBytes: 1)
        await logger.record(sampleEntry(episode: "asset-a"))
        // Each record() call checks the size after the write, so after
        // this one call the file is already > 1 byte and should rotate
        // before the NEXT write. The rotation happens synchronously on
        // the actor, so by the time record() returns the file has been
        // moved to decision-log.1.jsonl.
        await logger.flushAndClose()

        let active = dir.appendingPathComponent(DecisionLogger.activeLogFilename)
        let rotated = dir.appendingPathComponent("decision-log.1.jsonl")
        #expect(!FileManager.default.fileExists(atPath: active.path),
                "Active log should have been rotated away")
        #expect(FileManager.default.fileExists(atPath: rotated.path),
                "Expected decision-log.1.jsonl to exist after rotation")

        // The rotated file should contain the one line we wrote.
        let data = try Data(contentsOf: rotated)
        let text = String(decoding: data, as: UTF8.self)
        #expect(text.contains("\"episodeID\":\"asset-a\""))
    }

    @Test("Two rotations produce decision-log.1.jsonl and decision-log.2.jsonl")
    func producesSequentialRotationNumbers() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try DecisionLogger(directory: dir, rotationThresholdBytes: 1)
        await logger.record(sampleEntry(episode: "a"))
        await logger.record(sampleEntry(episode: "b"))
        await logger.flushAndClose()

        let r1 = dir.appendingPathComponent("decision-log.1.jsonl")
        let r2 = dir.appendingPathComponent("decision-log.2.jsonl")
        #expect(FileManager.default.fileExists(atPath: r1.path))
        #expect(FileManager.default.fileExists(atPath: r2.path))
    }

    @Test("Warm start seeds next rotation index from highest existing rotated file (idempotent)")
    func warmStartSeedsFromDisk() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Pre-populate the directory with a synthetic rotated file at index 5.
        let preExisting = dir.appendingPathComponent("decision-log.5.jsonl")
        try "pre-seeded\n".data(using: .utf8)!.write(to: preExisting)

        let logger = try DecisionLogger(directory: dir, rotationThresholdBytes: 1)
        let seed = await logger.currentNextRotationIndex()
        #expect(seed == 6,
                "Next rotation index must be one past highest existing file, got \(seed)")

        // Trigger a rotation and verify it writes to decision-log.6.jsonl,
        // NOT re-rotating or clobbering decision-log.5.jsonl.
        await logger.record(sampleEntry(episode: "warm"))
        await logger.flushAndClose()

        let r5 = dir.appendingPathComponent("decision-log.5.jsonl")
        let r6 = dir.appendingPathComponent("decision-log.6.jsonl")
        #expect(FileManager.default.fileExists(atPath: r5.path),
                "Pre-existing rotated file must be preserved across warm start")
        #expect(FileManager.default.fileExists(atPath: r6.path),
                "New rotation should be indexed 6, not 1")

        // The pre-existing content must be untouched.
        let r5Data = try String(contentsOf: r5, encoding: .utf8)
        #expect(r5Data == "pre-seeded\n")
    }

    @Test("No rotations seeds next index at 1")
    func noRotationsSeedsAtOne() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try DecisionLogger(directory: dir,
                                        rotationThresholdBytes: 10 * 1024 * 1024)
        let seed = await logger.currentNextRotationIndex()
        #expect(seed == 1)
    }

    @Test("No-op DecisionLogger implementation does not write any file")
    func noOpWritesNothing() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let noop: DecisionLoggerProtocol = NoOpDecisionLogger()
        await noop.record(sampleEntry())

        let contents = try FileManager.default.contentsOfDirectory(atPath: dir.path)
        #expect(contents.isEmpty, "NoOp logger must not write any files")
    }
}

// MARK: - Pipeline integration

/// Test spy that records entries in-memory. Safe across actor boundaries
/// via an internal actor of its own.
actor SpyDecisionLogger: DecisionLoggerProtocol {
    private(set) var entries: [DecisionLogEntry] = []

    func record(_ entry: DecisionLogEntry) async {
        entries.append(entry)
    }

    func snapshotEntries() -> [DecisionLogEntry] { entries }
}

@Suite("DecisionLogger — pipeline integration", .serialized)
struct DecisionLoggerPipelineTests {

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeAdChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome back to the show today.",
            "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.",
            "Back to our conversation about technology and the future of podcasting."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-decision-logger-v1",
            fmBackfillMode: .off
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    @Test("AdDetectionService.runBackfill emits one DecisionLogEntry per fusion window")
    func runBackfillEmitsEntries() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-logger-integration"
        try await analysisStore.insertAsset(makeAsset(id: assetId))

        let spy = SpyDecisionLogger()
        let service = makeService(store: analysisStore)
        await service.setDecisionLogger(spy)

        try await service.runBackfill(
            chunks: makeAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let entries = await spy.snapshotEntries()
        #expect(!entries.isEmpty,
                "Expected at least one DecisionLogEntry to be recorded for a chunk set with ad signals")

        // Each entry must belong to the asset and carry a valid schema version.
        for entry in entries {
            #expect(entry.episodeID == assetId)
            #expect(entry.schemaVersion == DecisionLogEntry.currentSchemaVersion)
            #expect(entry.windowBounds.end >= entry.windowBounds.start)
            #expect(entry.finalDecision.thresholdCrossed >= 0)
        }
    }

    @Test("Entry activationConfig snapshot matches MetadataActivationConfig.resolved() at emit time")
    func recordsResolvedActivationConfig() async throws {
        MetadataActivationOverride.reset()
        defer { MetadataActivationOverride.reset() }

        let analysisStore = try await makeTestStore()
        let assetId = "asset-logger-config-snap"
        try await analysisStore.insertAsset(makeAsset(id: assetId))

        let spy = SpyDecisionLogger()
        let service = makeService(store: analysisStore)
        await service.setDecisionLogger(spy)

        // Default resolution: gate closed.
        try await service.runBackfill(
            chunks: makeAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let entries = await spy.snapshotEntries()
        guard let first = entries.first else {
            Issue.record("Expected at least one entry")
            return
        }
        #expect(first.activationConfig.counterfactualGateOpen == false,
                "Without override, snapshot must report default (gate closed).")
    }
}
