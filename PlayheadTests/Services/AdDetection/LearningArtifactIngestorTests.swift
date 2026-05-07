// LearningArtifactIngestorTests.swift
// playhead-hygc.1.7: Tests for the durable-learning ingestion path that
// converts deduped `correction_events` rows into training_examples and
// sponsor-knowledge state changes.
//
// Coverage:
//   1. FN exactTimeSpan correction → bucketer sees `userReportedFalseNegative`
//      and the materialized example carries the correct `userAction`.
//   2. FP listenRevert exactTimeSpan correction → bucketer sees `userReverted`
//      and the materialized example carries `"reverted"`.
//   3. sponsorOnShow FN correction → SponsorKnowledgeStore receives a
//      candidate confirmation against the sponsor entity.
//   4. sponsorOnShow FP correction → SponsorKnowledgeStore receives a
//      rollback against the sponsor entity.
//   5. Re-ingesting the same correction is idempotent — counters report it
//      as deduped, no new training_examples row, no double rollback.
//   6. Diagnostics counters track raw / deduped / ingested / sponsor side
//      effects so we can audit the path post-hoc.
//   7. Materialization runs after each ingestion call so downstream
//      consumers (NARL exporter, learning eval) see fresh artifacts
//      without waiting for a subsequent backfill pass.

import Foundation
import Testing

@testable import Playhead

@Suite("LearningArtifactIngestor — playhead-hygc.1.7")
struct LearningArtifactIngestorTests {

    private let assetId = "asset-ing-1"
    private let podcastId = "pod-ing-1"
    private let transcriptVersion = "tv-ing-1"

    // MARK: - Fixtures

    private func makeAsset() -> AnalysisAsset {
        AnalysisAsset(
            id: assetId,
            episodeId: "ep-ing-1",
            assetFingerprint: "fp-ing-1",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/ing-1.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func scanRow(
        id: String,
        firstOrdinal: Int, lastOrdinal: Int,
        startTime: Double, endTime: Double
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: firstOrdinal,
            windowLastAtomOrdinal: lastOrdinal,
            windowStartTime: startTime,
            windowEndTime: endTime,
            scanPass: "coarse",
            transcriptQuality: .good,
            disposition: .uncertain,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 100,
            outputTokenCount: 20,
            latencyMs: 50,
            prewarmHit: false,
            scanCohortJSON: ScanCohort.productionJSON(),
            transcriptVersion: transcriptVersion,
            reuseScope: nil,
            runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        )
    }

    private func fnSpanCorrection(startTime: Double, endTime: Double) -> CorrectionEvent {
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: startTime,
            endTime: endTime
        )
        return CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            createdAt: 1_700_000_500,
            source: .falseNegative,
            podcastId: podcastId,
            correctionType: .falseNegative
        )
    }

    private func fpListenRevertCorrection(startTime: Double, endTime: Double) -> CorrectionEvent {
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: startTime,
            endTime: endTime
        )
        return CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            createdAt: 1_700_000_500,
            source: .listenRevert,
            podcastId: podcastId,
            correctionType: .falsePositive
        )
    }

    private func sponsorCorrection(
        sponsor: String,
        kind: CorrectionKind
    ) -> CorrectionEvent {
        let scope = CorrectionScope.sponsorOnShow(
            podcastId: podcastId,
            sponsor: sponsor
        )
        let source: CorrectionSource = (kind == .falseNegative) ? .falseNegative : .manualVeto
        return CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            createdAt: 1_700_000_500,
            source: source,
            podcastId: podcastId,
            correctionType: kind.correctionType
        )
    }

    // MARK: - Tests

    @Test("FN exactTimeSpan correction is materialized into a userReportedFalseNegative training example")
    func fnSpanProducesUserReportedFalseNegativeExample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        // One spine row that overlaps the correction span [10, 20].
        try await store.insertSemanticScanResult(scanRow(
            id: "scan-1",
            firstOrdinal: 0, lastOrdinal: 50,
            startTime: 0, endTime: 30
        ))

        let knowledge = SponsorKnowledgeStore(store: store)
        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )

        _ = try await ingestor.ingest(
            correction: fnSpanCorrection(startTime: 10, endTime: 20)
        )

        let examples = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(examples.count == 1)
        #expect(examples.first?.userAction == "reportedAd")
    }

    @Test("FP listenRevert exactTimeSpan correction yields userAction=reverted")
    func fpListenRevertProducesRevertedExample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanRow(
            id: "scan-1",
            firstOrdinal: 0, lastOrdinal: 50,
            startTime: 0, endTime: 30
        ))

        let knowledge = SponsorKnowledgeStore(store: store)
        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )

        _ = try await ingestor.ingest(
            correction: fpListenRevertCorrection(startTime: 5, endTime: 15)
        )

        let examples = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(examples.count == 1)
        #expect(examples.first?.userAction == "reverted")
    }

    @Test("sponsorOnShow FN correction confirms the sponsor entity in SponsorKnowledgeStore")
    func sponsorFNConfirmsKnowledgeEntry() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let knowledge = SponsorKnowledgeStore(store: store)
        // Pre-existing candidate so confirmation is observable: starting
        // count 1, expected after ingest: 2 → quarantined.
        try await knowledge.recordCandidate(
            podcastId: podcastId,
            entityType: .sponsor,
            entityValue: "Squarespace",
            analysisAssetId: assetId,
            sourceAtomOrdinals: [10],
            transcriptVersion: transcriptVersion,
            confidence: 0.8
        )

        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )
        _ = try await ingestor.ingest(
            correction: sponsorCorrection(sponsor: "Squarespace", kind: .falseNegative)
        )

        let entry = try await knowledge.entry(
            podcastId: podcastId,
            entityType: .sponsor,
            normalizedValue: "squarespace"
        )
        #expect(entry?.confirmationCount == 2)
        #expect(entry?.state == .active, "two confirmations should reach active")
    }

    @Test("sponsorOnShow FP correction records a rollback in SponsorKnowledgeStore")
    func sponsorFPRecordsRollback() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let knowledge = SponsorKnowledgeStore(store: store)
        // Build up to active state so the rollback has something to demote.
        for i in 1...3 {
            try await knowledge.recordCandidate(
                podcastId: podcastId,
                entityType: .sponsor,
                entityValue: "BetterHelp",
                analysisAssetId: "asset-pre-\(i)",
                sourceAtomOrdinals: [i * 10],
                transcriptVersion: transcriptVersion,
                confidence: 0.8
            )
        }
        let before = try await knowledge.entry(
            podcastId: podcastId,
            entityType: .sponsor,
            normalizedValue: "betterhelp"
        )
        #expect(before?.state == .active)
        #expect(before?.rollbackCount == 0)

        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )
        _ = try await ingestor.ingest(
            correction: sponsorCorrection(sponsor: "BetterHelp", kind: .falsePositive)
        )

        let after = try await knowledge.entry(
            podcastId: podcastId,
            entityType: .sponsor,
            normalizedValue: "betterhelp"
        )
        #expect(after?.rollbackCount == 1)
    }

    @Test("Re-ingesting the same correction is idempotent: deduped count increments, no duplicate side effects")
    func reIngestingSameCorrectionIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let knowledge = SponsorKnowledgeStore(store: store)
        // Active sponsor so we can detect double rollback.
        for i in 1...3 {
            try await knowledge.recordCandidate(
                podcastId: podcastId,
                entityType: .sponsor,
                entityValue: "Athletic Greens",
                analysisAssetId: "asset-pre-\(i)",
                sourceAtomOrdinals: [i * 10],
                transcriptVersion: transcriptVersion,
                confidence: 0.8
            )
        }

        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )
        let first = try await ingestor.ingest(
            correction: sponsorCorrection(sponsor: "Athletic Greens", kind: .falsePositive)
        )
        let second = try await ingestor.ingest(
            correction: sponsorCorrection(sponsor: "Athletic Greens", kind: .falsePositive)
        )

        #expect(first.outcome == .ingested)
        #expect(second.outcome == .deduped)
        let after = try await knowledge.entry(
            podcastId: podcastId,
            entityType: .sponsor,
            normalizedValue: "athletic greens"
        )
        #expect(after?.rollbackCount == 1, "duplicate ingest must not double-decrement")
    }

    @Test("Diagnostics counters track raw, deduped, ingested, sponsor-rollback, sponsor-confirm")
    func diagnosticsCountersAccumulate() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanRow(
            id: "scan-1",
            firstOrdinal: 0, lastOrdinal: 50,
            startTime: 0, endTime: 30
        ))
        let knowledge = SponsorKnowledgeStore(store: store)
        try await knowledge.recordCandidate(
            podcastId: podcastId,
            entityType: .sponsor,
            entityValue: "MintMobile",
            analysisAssetId: assetId,
            sourceAtomOrdinals: [10],
            transcriptVersion: transcriptVersion,
            confidence: 0.8
        )

        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )
        _ = try await ingestor.ingest(
            correction: fnSpanCorrection(startTime: 5, endTime: 15)
        )
        _ = try await ingestor.ingest(
            correction: sponsorCorrection(sponsor: "MintMobile", kind: .falseNegative)
        )
        // Same span correction again — should be deduped.
        _ = try await ingestor.ingest(
            correction: fnSpanCorrection(startTime: 5, endTime: 15)
        )

        let diag = await ingestor.diagnostics()
        #expect(diag.raw == 3)
        #expect(diag.ingested == 2)
        #expect(diag.deduped == 1)
        #expect(diag.sponsorCandidatesConfirmed == 1)
        #expect(diag.sponsorRollbacksApplied == 0)
    }

    @Test("Ingestion materializes training examples without waiting for backfill")
    func ingestionTriggersMaterialization() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanRow(
            id: "scan-1",
            firstOrdinal: 0, lastOrdinal: 50,
            startTime: 0, endTime: 30
        ))

        let knowledge = SponsorKnowledgeStore(store: store)
        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )

        // Pre-state: no training examples yet.
        let before = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(before.isEmpty)

        _ = try await ingestor.ingest(
            correction: fnSpanCorrection(startTime: 10, endTime: 20)
        )

        // Post-state: materialization ran as part of ingest.
        let after = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(!after.isEmpty)
    }

    /// playhead-hygc.1.7 R1: privacy invariant — the durable learning
    /// surface MUST NOT contain raw transcript text. The materializer
    /// produces a `textSnapshotHash` (deterministic SHA-derived id) but
    /// leaves `textSnapshot` nil so the learning corpus carries provenance
    /// without leaking user-private text. This test pins the contract so
    /// a future "let's stash a snippet for debugging" change breaks the
    /// build instead of silently shipping raw transcripts.
    @Test("Materialized training examples carry no raw transcript text (privacy)")
    func materializedExamplesCarryNoRawTranscriptText() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanRow(
            id: "scan-privacy",
            firstOrdinal: 0, lastOrdinal: 50,
            startTime: 0, endTime: 30
        ))

        let knowledge = SponsorKnowledgeStore(store: store)
        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )
        _ = try await ingestor.ingest(
            correction: fnSpanCorrection(startTime: 10, endTime: 20)
        )

        let examples = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(!examples.isEmpty, "materialization must produce at least one row")
        for example in examples {
            #expect(example.textSnapshot == nil,
                "training_examples.textSnapshot must be nil — raw transcript text MUST NOT land in the learning corpus")
            #expect(!example.textSnapshotHash.isEmpty,
                "textSnapshotHash must be present so provenance is preserved without raw text")
        }
    }

    /// playhead-hygc.1.7 R1: malformed scopes are rejected at the front
    /// door. They must NOT touch the materializer, sponsor store, or the
    /// `correction_events` table — they're counted in `skippedMalformed`
    /// for diagnostics only. This guards against a future bad upstream
    /// caller polluting durable artifacts with garbage scope text.
    @Test("Malformed scope is skipped without persisting any side effect")
    func malformedScopeIsSkippedWithoutSideEffects() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanRow(
            id: "scan-malformed",
            firstOrdinal: 0, lastOrdinal: 50,
            startTime: 0, endTime: 30
        ))
        let knowledge = SponsorKnowledgeStore(store: store)
        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )

        let bogus = CorrectionEvent(
            analysisAssetId: assetId,
            scope: "totallyUnknownPrefix:not:a:valid:scope",
            createdAt: 1_700_000_500,
            source: .falseNegative,
            podcastId: podcastId,
            correctionType: .falseNegative
        )
        let result = try await ingestor.ingest(correction: bogus)
        #expect(result.outcome == .skippedMalformed)
        #expect(result.analysisAssetId == nil)

        let diag = await ingestor.diagnostics()
        #expect(diag.skippedMalformed == 1)
        #expect(diag.ingested == 0)
        #expect(diag.deduped == 0)

        // Materializer must not have run for this asset (no training_examples
        // rows produced) and the sponsor store must remain untouched.
        let examples = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(examples.isEmpty,
            "malformed scope must not materialize any training_examples row")
    }

    /// playhead-hygc.1.7 R2: end-to-end NARL round-trip.
    ///
    /// Bead acceptance criterion: "A false positive listen revert and a
    /// false negative user-marked ad both persist to canonical
    /// corrections, materialize into training_examples with the expected
    /// bucket, and can be exported into NARL without duplicates."
    ///
    /// Pin the full flow as one test so a regression in any of the three
    /// hops (correction → ingestor → corpus export) is caught here. We
    /// drive both gestures through the ingestor twice (once each) to
    /// also confirm the dedupe path doesn't silently drop a legitimately
    /// distinct second correction.
    #if DEBUG
    @Test("End-to-end: FN + FP corrections materialize and export into the NARL corpus without duplicates")
    func endToEndFNFPRoundTripIntoNARLCorpus() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        // Two scan-result rows so each correction has its own spine
        // window to materialize against.
        try await store.insertSemanticScanResult(scanRow(
            id: "scan-fn",
            firstOrdinal: 0, lastOrdinal: 50,
            startTime: 0, endTime: 30
        ))
        try await store.insertSemanticScanResult(scanRow(
            id: "scan-fp",
            firstOrdinal: 51, lastOrdinal: 100,
            startTime: 30, endTime: 60
        ))

        let knowledge = SponsorKnowledgeStore(store: store)
        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )

        // 1. FN: user-marked ad on [10, 20].
        let fn = fnSpanCorrection(startTime: 10, endTime: 20)
        let fnResult = try await ingestor.ingest(correction: fn)
        #expect(fnResult.outcome == .ingested)

        // 2. FP: listenRevert on [40, 50] (a different region, distinct identity).
        let fp = fpListenRevertCorrection(startTime: 40, endTime: 50)
        let fpResult = try await ingestor.ingest(correction: fp)
        #expect(fpResult.outcome == .ingested)

        // 3. Verify materialization: TWO training_examples, one per scan
        //    spine, with the expected userActions.
        let examples = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(examples.count == 2,
            "Expected one training_example per scan spine row; got \(examples.count)")
        let actions = Set(examples.compactMap { $0.userAction })
        #expect(actions.contains("reportedAd"), "FN must materialize as userAction='reportedAd'")
        #expect(actions.contains("reverted"), "FP must materialize as userAction='reverted'")

        // 4. Export into the NARL corpus and read back the correction lines.
        //    `.distinctSemantic` (default) must emit one correction line per
        //    distinct identity — so two corrections in, two out.
        let docs = FileManager.default.temporaryDirectory
            .appendingPathComponent("hygc17-narl-roundtrip-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: docs) }
        let exportResult = try await CorpusExporter.export(
            store: store,
            documentsURL: docs,
            dedupMemo: CorpusExportDedupMemo()
        )
        #expect(exportResult.correctionCount == 2,
            "FN+FP corrections must export as exactly two correction lines; got \(exportResult.correctionCount)")

        // 5. Re-ingest both corrections (replay safety): the ingestor
        //    must report them as `.deduped` and the exporter must STILL
        //    emit only two correction lines — no duplicates leaked into
        //    NARL through the dedupe seam.
        let fnReplay = try await ingestor.ingest(correction: fn)
        let fpReplay = try await ingestor.ingest(correction: fp)
        #expect(fnReplay.outcome == .deduped)
        #expect(fpReplay.outcome == .deduped)

        let docs2 = FileManager.default.temporaryDirectory
            .appendingPathComponent("hygc17-narl-roundtrip-replay-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: docs2) }
        let replayExport = try await CorpusExporter.export(
            store: store,
            documentsURL: docs2,
            dedupMemo: CorpusExportDedupMemo()
        )
        #expect(replayExport.correctionCount == 2,
            "Replayed FN+FP must NOT produce duplicate correction lines in NARL; got \(replayExport.correctionCount)")

        // 6. Confirm the on-disk JSONL actually carries correction
        //    records by counting "type":"correction" lines, so we don't
        //    silently rely only on the exporter's counter.
        let bytes = try Data(contentsOf: replayExport.fileURL)
        let lines = bytes.split(separator: 0x0A)
        let correctionLineCount = lines.filter { line in
            String(decoding: line, as: UTF8.self).contains("\"type\":\"correction\"")
        }.count
        #expect(correctionLineCount == 2,
            "On-disk corpus must contain exactly two `type=correction` lines; got \(correctionLineCount)")
    }
    #endif

    /// playhead-hygc.1.7 R2: concurrent re-ingest stress.
    ///
    /// Sequential re-ingest is already pinned by
    /// `reIngestingSameCorrectionIsIdempotent`, but actor reentrancy means
    /// two callers can both pass the `seenIdentities.contains` check before
    /// either reaches the post-`appendCorrectionEvent` `seenIdentities.insert`
    /// suspension boundary. Without explicit reservation of the identity
    /// before the first await, a concurrent storm produces a double sponsor
    /// rollback (rollbackCount > 1) for what the user authored as a single
    /// gesture. This test fans 5 ingest calls out via TaskGroup against the
    /// same correction identity and pins:
    ///   • exactly one .ingested outcome,
    ///   • the rest reported as .deduped,
    ///   • sponsor rollback applied EXACTLY once.
    @Test("Concurrent re-ingest of the same correction yields exactly one ingest + one rollback")
    func concurrentReIngestIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let knowledge = SponsorKnowledgeStore(store: store)
        // Build the sponsor up to active so a rollback has something to demote.
        for i in 1...3 {
            try await knowledge.recordCandidate(
                podcastId: podcastId,
                entityType: .sponsor,
                entityValue: "ConcurrentSponsor",
                analysisAssetId: "asset-pre-\(i)",
                sourceAtomOrdinals: [i * 10],
                transcriptVersion: transcriptVersion,
                confidence: 0.8
            )
        }
        let before = try await knowledge.entry(
            podcastId: podcastId,
            entityType: .sponsor,
            normalizedValue: "concurrentsponsor"
        )
        #expect(before?.state == .active)
        #expect(before?.rollbackCount == 0)

        let ingestor = LearningArtifactIngestor(
            store: store,
            knowledgeStore: knowledge
        )

        // Fan out 5 concurrent ingest calls against the SAME correction
        // identity. With actor reentrancy and no explicit reservation,
        // multiple tasks pass the seenIdentities.contains check before
        // any one inserts — and each runs the rollback side effect.
        let correction = sponsorCorrection(sponsor: "ConcurrentSponsor", kind: .falsePositive)
        let outcomes = await withTaskGroup(of: LearningIngestionOutcome.self, returning: [LearningIngestionOutcome].self) { group in
            for _ in 0..<5 {
                group.addTask {
                    do {
                        let r = try await ingestor.ingest(correction: correction)
                        return r.outcome
                    } catch {
                        return .skippedMalformed
                    }
                }
            }
            var results: [LearningIngestionOutcome] = []
            for await outcome in group { results.append(outcome) }
            return results
        }

        let ingestedCount = outcomes.filter { $0 == .ingested }.count
        let dedupedCount = outcomes.filter { $0 == .deduped }.count
        #expect(ingestedCount == 1,
            "exactly one concurrent caller may report .ingested; got \(ingestedCount)")
        #expect(dedupedCount == 4,
            "the remaining concurrent callers must report .deduped; got \(dedupedCount)")

        let after = try await knowledge.entry(
            podcastId: podcastId,
            entityType: .sponsor,
            normalizedValue: "concurrentsponsor"
        )
        #expect(after?.rollbackCount == 1,
            "concurrent re-ingest of one identity must apply rollback exactly once; got \(after?.rollbackCount ?? -1)")

        let diag = await ingestor.diagnostics()
        #expect(diag.sponsorRollbacksApplied == 1,
            "diagnostics must report exactly one sponsor rollback; got \(diag.sponsorRollbacksApplied)")
    }
}
