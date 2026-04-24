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
            analysisAssetID: "asset-a",
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
        #expect(snap.classifierShiftedMidpoint == 0.33)
        #expect(snap.classifierBaselineMidpoint == 0.37)
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
            analysisAssetID: episode,
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
        #expect(first.analysisAssetID == "a")
        #expect(second.analysisAssetID == "b")
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
            analysisAssetID: entry.analysisAssetID,
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

        // Tiny threshold so two records trigger rotation. The livelock
        // guard intentionally refuses to rotate a single-line file even
        // when it exceeds the threshold (the rotation would be infinite
        // — the rotated file would still be oversized), so we write two
        // entries: the first establishes >=2 lines after the second, and
        // the second record's post-write check then rotates.
        let logger = try DecisionLogger(directory: dir, rotationThresholdBytes: 1)
        await logger.record(sampleEntry(episode: "asset-a"))
        await logger.record(sampleEntry(episode: "asset-b"))
        await logger.flushAndClose()

        let rotated = dir.appendingPathComponent("decision-log.1.jsonl")
        #expect(FileManager.default.fileExists(atPath: rotated.path),
                "Expected decision-log.1.jsonl to exist after rotation")

        let data = try Data(contentsOf: rotated)
        let text = String(decoding: data, as: UTF8.self)
        #expect(text.contains("\"analysisAssetID\":\"asset-a\""))
        #expect(text.contains("\"analysisAssetID\":\"asset-b\""))
    }

    @Test("Two rotations produce decision-log.1.jsonl and decision-log.2.jsonl")
    func producesSequentialRotationNumbers() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try DecisionLogger(directory: dir, rotationThresholdBytes: 1)
        // Two records per rotation under the livelock guard.
        await logger.record(sampleEntry(episode: "a"))
        await logger.record(sampleEntry(episode: "b"))  // triggers rotation 1
        await logger.record(sampleEntry(episode: "c"))
        await logger.record(sampleEntry(episode: "d"))  // triggers rotation 2
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

        // Trigger a rotation (two records — see livelock guard) and
        // verify it writes to decision-log.6.jsonl, NOT re-rotating or
        // clobbering decision-log.5.jsonl.
        await logger.record(sampleEntry(episode: "warm-a"))
        await logger.record(sampleEntry(episode: "warm-b"))
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

    private func makeService(
        store: AnalysisStore,
        classifier: ClassifierService = RuleBasedClassifier(),
        autoSkipConfidenceThreshold: Double = 0.80
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-decision-logger-v1",
            fmBackfillMode: .off,
            autoSkipConfidenceThreshold: autoSkipConfidenceThreshold
        )
        return AdDetectionService(
            store: store,
            classifier: classifier,
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

        let entries = await spy.entries
        #expect(!entries.isEmpty,
                "Expected at least one DecisionLogEntry to be recorded for a chunk set with ad signals")

        // Each entry must belong to the asset and carry a valid schema version.
        for entry in entries {
            #expect(entry.analysisAssetID == assetId)
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

        let entries = await spy.entries
        guard let first = entries.first else {
            Issue.record("Expected at least one entry")
            return
        }
        #expect(first.activationConfig.counterfactualGateOpen == false,
                "Without override, snapshot must report default (gate closed).")
    }

    @Test("runHotPath emits a DecisionLogEntry for every classifier result")
    func runHotPathEmitsEntries() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-logger-hot-path"
        try await analysisStore.insertAsset(makeAsset(id: assetId))

        let spy = SpyDecisionLogger()
        let service = makeService(store: analysisStore)
        await service.setDecisionLogger(spy)

        _ = try await service.runHotPath(
            chunks: makeAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 90.0
        )

        let entries = await spy.entries
        #expect(!entries.isEmpty,
                "Expected at least one hot-path DecisionLogEntry for a chunk set with lexical ad signals")
        for entry in entries {
            #expect(entry.analysisAssetID == assetId)
            // Hot-path entries carry exactly one `.classifier` ledger
            // entry (pre-fusion), and finalDecision.action distinguishes
            // them from backfill-fusion entries.
            #expect(entry.evidence.count == 1)
            #expect(entry.evidence.first?.detail.kind == "classifier")
            // playhead-0usd: `segmentAggregatorPromoted` is a second hot-path
            // channel (aggregator-derived) that emits alongside the single-
            // window actions.
            #expect(entry.finalDecision.action == "hotPathCandidate"
                    || entry.finalDecision.action == "hotPathBelowThreshold"
                    || entry.finalDecision.action == "autoSkipEligible"
                    || entry.finalDecision.action == "segmentAggregatorPromoted")
        }
    }

    // MARK: - Classifier-Only High-Confidence Promotion
    //
    // Regression for the 2026-04-23 dogfood capture
    // (asset 71F0C2AE-7260-4D1E-B41A-BCFD5103A641 @ [7006..7008]):
    // a classifier-only window with adProbability above the autoSkip
    // threshold was logged as "hotPathCandidate" and was therefore
    // invisible to the NARL corpus builder's isAdUnderDefault heuristic
    // (which only treats "autoskip"/"markonly"/"skip" actions as ads).
    // Real-world effect: GT=3, Pred=0, Sec-F1=0 despite the classifier
    // confidently firing on the ad.
    //
    // Fix contract: when adProbability >= autoSkipConfidenceThreshold,
    // the decision-log action must be "autoSkipEligible" so downstream
    // consumers (including the NARL harness) see a skip-worthy signal.

    @Test("High-confidence classifier-only window is logged as autoSkipEligible, not hotPathCandidate")
    func hotPathPromotesHighConfidenceClassifierOnlyResult() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-hotpath-high-conf"
        try await analysisStore.insertAsset(makeAsset(id: assetId))

        let spy = SpyDecisionLogger()
        // 0.8154 mirrors the production capture at [7006..7008].
        let classifier = StubHighConfidenceClassifier(adProbability: 0.8154)
        let service = makeService(
            store: analysisStore,
            classifier: classifier,
            autoSkipConfidenceThreshold: 0.80
        )
        await service.setDecisionLogger(spy)

        _ = try await service.runHotPath(
            chunks: makeAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 90.0
        )

        let entries = await spy.entries
        #expect(!entries.isEmpty,
                "Expected at least one hot-path DecisionLogEntry")
        // At least one entry must be promoted to autoSkipEligible because
        // the stub classifier returns 0.8154 >= autoSkipConfidenceThreshold (0.80).
        let promoted = entries.filter { $0.finalDecision.action == "autoSkipEligible" }
        let actions = entries.map { $0.finalDecision.action }
        #expect(!promoted.isEmpty,
                "Expected at least one entry with action == autoSkipEligible; got \(actions)")
    }

    @Test("Sub-autoSkip classifier-only result stays as hotPathCandidate")
    func hotPathDoesNotPromoteSubAutoSkipClassifierResult() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-hotpath-sub-threshold"
        try await analysisStore.insertAsset(makeAsset(id: assetId))

        let spy = SpyDecisionLogger()
        // 0.6480 mirrors the second production candidate at [7006..7008]:
        // above candidateThreshold (0.40) but below autoSkip (0.80).
        let classifier = StubHighConfidenceClassifier(adProbability: 0.6480)
        let service = makeService(
            store: analysisStore,
            classifier: classifier,
            autoSkipConfidenceThreshold: 0.80
        )
        await service.setDecisionLogger(spy)

        _ = try await service.runHotPath(
            chunks: makeAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 90.0
        )

        let entries = await spy.entries
        #expect(!entries.isEmpty,
                "Expected at least one hot-path DecisionLogEntry")
        // No entry should be promoted to autoSkipEligible because 0.6480 < 0.80.
        let promoted = entries.filter { $0.finalDecision.action == "autoSkipEligible" }
        let actions = entries.map { $0.finalDecision.action }
        #expect(promoted.isEmpty,
                "Entries below autoSkipConfidenceThreshold must NOT be promoted; got \(actions)")
    }
}

// MARK: - Test Doubles

/// Returns a fixed adProbability for every candidate, with no other evidence
/// (lexical/acoustic/prior scores all zero). Used to isolate the hot-path
/// promotion contract from any heuristic scoring behavior of
/// RuleBasedClassifier.
private final class StubHighConfidenceClassifier: @unchecked Sendable, ClassifierService {
    private let adProbability: Double

    init(adProbability: Double) {
        self.adProbability = adProbability
    }

    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
        inputs.map { classify(input: $0, priors: priors) }
    }

    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
        ClassifierResult(
            candidateId: input.candidate.id,
            analysisAssetId: input.candidate.analysisAssetId,
            startTime: input.candidate.startTime,
            endTime: input.candidate.endTime,
            adProbability: adProbability,
            startAdjustment: 0,
            endAdjustment: 0,
            signalBreakdown: SignalBreakdown(
                lexicalScore: 0,
                rmsDropScore: 0,
                spectralChangeScore: 0,
                musicScore: 0,
                speakerChangeScore: 0,
                priorScore: 0
            )
        )
    }
}

// MARK: - Concurrency / failure / rotation-gap tests

@Suite("DecisionLogger — concurrency, directory failure, rotation-index gaps", .serialized)
struct DecisionLoggerEdgeCaseTests {

    private func makeTempDir(function: String = #function) throws -> URL {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("decision-logger-edge-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        return base
    }

    private func sampleEntry(episode: String) -> DecisionLogEntry {
        DecisionLogEntry(
            schemaVersion: DecisionLogEntry.currentSchemaVersion,
            analysisAssetID: episode,
            timestamp: 1_745_000_000.0,
            windowBounds: .init(start: 0, end: 30),
            activationConfig: .init(.default),
            evidence: [],
            fusedConfidence: .init(
                proposalConfidence: 0.5,
                skipConfidence: 0.5,
                breakdown: []
            ),
            finalDecision: .init(
                action: "detectOnly",
                gate: "eligible",
                skipConfidence: 0.5,
                thresholdCrossed: 0.55
            )
        )
    }

    @Test("Concurrent record(_:) calls serialize without line corruption")
    func concurrentRecordsSerializeCleanly() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try DecisionLogger(directory: dir,
                                        rotationThresholdBytes: 10 * 1024 * 1024)
        // Issue 50 concurrent writes. The actor guarantees serialization;
        // this test asserts the observable contract (one decodable JSON
        // object per line, all 50 present).
        let count = 50
        await withTaskGroup(of: Void.self) { group in
            for i in 0..<count {
                group.addTask {
                    await logger.record(self.sampleEntry(episode: "ep-\(i)"))
                }
            }
        }
        await logger.flushAndClose()

        let url = dir.appendingPathComponent(DecisionLogger.activeLogFilename)
        let data = try Data(contentsOf: url)
        let text = String(decoding: data, as: UTF8.self)
        let lines = text.split(separator: "\n", omittingEmptySubsequences: true)
        #expect(lines.count == count, "Expected \(count) JSONL lines, got \(lines.count)")

        let decoder = JSONDecoder()
        var episodes = Set<String>()
        for line in lines {
            let decoded = try decoder.decode(DecisionLogEntry.self,
                                             from: Data(line.utf8))
            episodes.insert(decoded.analysisAssetID)
        }
        #expect(episodes.count == count,
                "Every concurrent write must be persisted exactly once")
    }

    @Test("Init on a read-only parent swallows write failures without crashing")
    func readOnlyDirectoryDoesNotCrash() async throws {
        // Use a genuinely non-creatable path (no write permission at
        // the first path component). On iOS simulator / macOS / Linux
        // the root filesystem is not writable for the process, so
        // `/this-cannot-be-created` is a reliable choice.
        let bogusDir = URL(fileURLWithPath: "/decision-logger-cannot-create-\(UUID().uuidString)")

        do {
            // Construction itself may throw (createDirectory fails) — that
            // is one acceptable outcome per the bead spec.
            let logger = try DecisionLogger(directory: bogusDir,
                                            rotationThresholdBytes: 10 * 1024 * 1024)
            // If construction somehow succeeded, a subsequent record must
            // not crash; the actor logs the failure and swallows it.
            await logger.record(sampleEntry(episode: "should-fail"))
            await logger.flushAndClose()
            // Reaching here proves the "records+swallows" branch holds.
        } catch {
            // Init-time throw is the other acceptable outcome.
            #expect(error is CocoaError || (error as NSError).domain == NSCocoaErrorDomain
                    || (error as NSError).domain == NSPOSIXErrorDomain,
                    "Expected a filesystem error, got \(type(of: error))")
        }
    }

    @Test("Rotation-index scan honors gaps and picks max+1, not next empty slot")
    func rotationIndexScanSkipsGaps() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Pre-seed files at indices 1 and 3 (skip 2). The next rotation
        // must land on 4 (max + 1), not 2 (first gap), so we never
        // overwrite a future-ordered file that a previous process wrote.
        try "stub-1\n".data(using: .utf8)!.write(
            to: dir.appendingPathComponent("decision-log.1.jsonl")
        )
        try "stub-3\n".data(using: .utf8)!.write(
            to: dir.appendingPathComponent("decision-log.3.jsonl")
        )

        let logger = try DecisionLogger(directory: dir, rotationThresholdBytes: 1)
        #expect(await logger.currentNextRotationIndex() == 4,
                "Next rotation index must be max(existing)+1, not first gap")

        // Actually trigger a rotation — at threshold=1 bytes, two records
        // are enough: the first leaves the file >1 byte with >1 line (the
        // livelock guard requires line count >1, so we record twice
        // before rotating).
        await logger.record(sampleEntry(episode: "warm-a"))
        await logger.record(sampleEntry(episode: "warm-b"))
        await logger.flushAndClose()

        let r4 = dir.appendingPathComponent("decision-log.4.jsonl")
        #expect(FileManager.default.fileExists(atPath: r4.path),
                "Rotation must land on decision-log.4.jsonl (max+1)")
        let r2 = dir.appendingPathComponent("decision-log.2.jsonl")
        #expect(!FileManager.default.fileExists(atPath: r2.path),
                "First-gap slot must stay empty — index math is max+1, not first-empty")
    }

    @Test("Active decision-log.jsonl is excluded from the rotation-index scan")
    func activeFileFilteredFromRotationScan() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Create only the active file. No rotated files anywhere.
        try "stub\n".data(using: .utf8)!.write(
            to: dir.appendingPathComponent(DecisionLogger.activeLogFilename)
        )

        let logger = try DecisionLogger(directory: dir,
                                        rotationThresholdBytes: 10 * 1024 * 1024)
        // extractRotationIndex must return nil for "decision-log.jsonl",
        // so the scan sees zero rotated files and seeds nextRotationIndex
        // at 1. If the filter were wrong, it would try to parse "jsonl"
        // as an Int, still return nil — but to be exhaustive we also
        // assert no URL returned from listRotatedLogs points at the
        // active file.
        let seed = await logger.currentNextRotationIndex()
        #expect(seed == 1, "Active file must not contribute to rotation-index scan")
        let rotated = await logger.rotatedLogURLs()
        #expect(!rotated.contains(where: { $0.lastPathComponent == DecisionLogger.activeLogFilename }),
                "Active file must be excluded from rotatedLogURLs")
    }

    @Test("Single-record oversized write does not livelock on rotation")
    func oversizedSingleRecordSkipsRotation() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Threshold = 1 byte. A single record is much larger than that.
        // Without the livelock guard, rotateNow would rotate, the next
        // call into record() would write a fresh line into a file that
        // is still oversized after one write, and the check would loop.
        // With the guard, the first record stays put (>threshold but
        // only 1 line), and the second record triggers rotation at 2 lines.
        let logger = try DecisionLogger(directory: dir, rotationThresholdBytes: 1)
        await logger.record(sampleEntry(episode: "big-1"))
        // After one record, livelock guard prevents rotation.
        let preRotate = await logger.currentNextRotationIndex()
        #expect(preRotate == 1, "Single-line oversized file must not rotate")

        await logger.record(sampleEntry(episode: "big-2"))
        await logger.flushAndClose()
        // After two records, rotation fires normally.
        let r1 = dir.appendingPathComponent("decision-log.1.jsonl")
        #expect(FileManager.default.fileExists(atPath: r1.path),
                "Once >1 line, rotation should proceed")
    }

    @Test("schemaVersion mismatch: decoder accepts unknown version, surfaces the value verbatim")
    func schemaVersionMismatchSurfacesValue() throws {
        // Document the chosen behavior: DecisionLogEntry treats
        // `schemaVersion` as a raw Int, so an unknown value (99) decodes
        // successfully and is surfaced on the decoded struct. Replay
        // tooling is expected to branch on this field, not the decoder.
        let entry = DecisionLogEntry(
            schemaVersion: 1,
            analysisAssetID: "asset-schema-v",
            timestamp: 1_745_000_000.0,
            windowBounds: .init(start: 0, end: 30),
            activationConfig: .init(.default),
            evidence: [],
            fusedConfidence: .init(
                proposalConfidence: 0.5,
                skipConfidence: 0.5,
                breakdown: []
            ),
            finalDecision: .init(
                action: "detectOnly",
                gate: "eligible",
                skipConfidence: 0.5,
                thresholdCrossed: 0.55
            )
        )
        let data = try JSONEncoder().encode(entry)
        var json = String(decoding: data, as: UTF8.self)
        // Swap the schema version field from 1 to 99.
        #expect(json.contains("\"schemaVersion\":1"))
        json = json.replacingOccurrences(of: "\"schemaVersion\":1",
                                         with: "\"schemaVersion\":99")
        let mutated = Data(json.utf8)
        let decoded = try JSONDecoder().decode(DecisionLogEntry.self, from: mutated)
        #expect(decoded.schemaVersion == 99,
                "Unknown schema versions must decode successfully with the value surfaced so replay tooling can branch")
    }
}
