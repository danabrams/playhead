// TrainingExampleMaterializerTests.swift
// playhead-4my.10.1: end-to-end materialization test. Drives a real
// AnalysisStore through:
//   1. seed semantic_scan_results (the "spine" — one row per window),
//   2. seed evidence_events for each window,
//   3. seed decision_events (post-fusion outcomes) for some windows,
//   4. seed correction_events (user revert / FN report) for some windows,
//   5. invoke `TrainingExampleMaterializer.materialize(...)`,
//   6. read back `loadTrainingExamples(forAsset:)` and assert:
//      - the count matches the number of scan-result rows,
//      - every bucket the test seeded is reachable,
//      - cohort fields round-trip,
//      - re-running the materializer is idempotent (replace, not append).

import Foundation
import Testing

@testable import Playhead

@Suite("TrainingExampleMaterializer — playhead-4my.10.1")
struct TrainingExampleMaterializerTests {

    private let assetId = "asset-mat-1"
    private let transcriptVersion = "tv-mat-1"
    // AnalysisStore validates scanCohortJSON by decoding it as a real
    // `ScanCohort` value (see `validateScanCohortJSON`). Hand-rolling a
    // partial JSON here would fail that gate, so we round-trip through
    // the canonical production cohort. decisionCohortJSON is opaque to
    // the store and only needs to be a non-empty string.
    private let scanCohortJSON = ScanCohort.productionJSON()
    private let decisionCohortJSON =
        "{\"fusionVersion\":\"f1\"}"

    // MARK: - Fixtures

    private func makeAsset() -> AnalysisAsset {
        AnalysisAsset(
            id: assetId,
            episodeId: "ep-mat-1",
            assetFingerprint: "fp-mat-1",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/mat-1.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func scanResult(
        id: String,
        firstOrdinal: Int,
        lastOrdinal: Int,
        startTime: Double,
        endTime: Double,
        disposition: CoarseDisposition,
        quality: TranscriptQuality = .good
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: firstOrdinal,
            windowLastAtomOrdinal: lastOrdinal,
            windowStartTime: startTime,
            windowEndTime: endTime,
            scanPass: "coarse",
            transcriptQuality: quality,
            disposition: disposition,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 100,
            outputTokenCount: 20,
            latencyMs: 50,
            prewarmHit: false,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: transcriptVersion,
            reuseScope: nil,
            runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        )
    }

    private func evidenceEvent(
        id: String,
        sourceType: EvidenceSourceType,
        firstOrdinal: Int,
        lastOrdinal: Int,
        certainty: Double = 0.0
    ) throws -> EvidenceEvent {
        let ordinals = Array(firstOrdinal...lastOrdinal)
        let json = String(
            data: try JSONSerialization.data(withJSONObject: ordinals),
            encoding: .utf8
        ) ?? "[]"
        // Stuff a `certainty` hint into evidenceJSON so the materializer
        // can read it back; the schema is opaque to AnalysisStore.
        let payload = "{\"certainty\":\(certainty)}"
        return EvidenceEvent(
            id: id,
            analysisAssetId: assetId,
            eventType: "scan",
            sourceType: sourceType,
            atomOrdinals: json,
            evidenceJSON: payload,
            scanCohortJSON: scanCohortJSON,
            createdAt: 1_700_000_000,
            runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        )
    }

    private func decisionEvent(
        id: String,
        windowId: String,
        skipConfidence: Double,
        gate: String,
        policy: String,
        createdAt: Double = 1_700_000_001
    ) -> DecisionEvent {
        DecisionEvent(
            id: id,
            analysisAssetId: assetId,
            eventType: "fusion",
            windowId: windowId,
            proposalConfidence: skipConfidence,
            skipConfidence: skipConfidence,
            eligibilityGate: gate,
            policyAction: policy,
            decisionCohortJSON: decisionCohortJSON,
            createdAt: createdAt,
            explanationJSON: nil
        )
    }

    private func adWindow(
        id: String,
        startTime: Double,
        endTime: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            confidence: 0.5,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: AdDetectionConfig.default.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    private func correctionEvent(
        id: String,
        scope: String,
        source: CorrectionSource
    ) -> CorrectionEvent {
        CorrectionEvent(
            id: id,
            analysisAssetId: assetId,
            scope: scope,
            createdAt: 1_700_000_002,
            source: source,
            podcastId: nil,
            correctionType: source.kind.correctionType,
            causalSource: nil,
            targetRefs: nil
        )
    }

    // MARK: - Tests

    @Test("materializer produces one example per scan-result spine row")
    func oneExamplePerScanResult() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // 3 windows on the spine.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-C", firstOrdinal: 21, lastOrdinal: 30,
            startTime: 20, endTime: 30, disposition: .uncertain,
            quality: .unusable
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId,
            store: store,
            now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(loaded.count == 3)
    }

    @Test("all four buckets are reachable from a realistic fixture")
    func allFourBucketsReachable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Window A: confirmed paid ad — FM containsAd, lexical hit, decision
        // skip-eligible, no user revert -> .positive
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-fm", sourceType: .fm,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-lex", sourceType: .lexical,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.8),
            transcriptVersion: transcriptVersion
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-A", windowId: "win-A",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))

        // Window B: editorial mention — FM noAds, lexical noAds, decision
        // not skip-eligible, no user signal -> .negative
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-B", startTime: 10, endTime: 20)
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-B", windowId: "win-B",
            skipConfidence: 0.05, gate: "ineligible", policy: "noAction"
        ))

        // Window C: unusable transcript -> .uncertain
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-C", firstOrdinal: 21, lastOrdinal: 30,
            startTime: 20, endTime: 30, disposition: .abstain,
            quality: .unusable
        ))

        // Window D: FM-positive but user reverted -> .disagreement
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-D", firstOrdinal: 31, lastOrdinal: 40,
            startTime: 30, endTime: 40, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-D", startTime: 30, endTime: 40)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-D-fm", sourceType: .fm,
                          firstOrdinal: 31, lastOrdinal: 40, certainty: 0.93),
            transcriptVersion: transcriptVersion
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-D", windowId: "win-D",
            skipConfidence: 0.85, gate: "eligible", policy: "autoSkipEligible"
        ))
        try await store.appendCorrectionEvent(correctionEvent(
            // Scope encodes the time range so the materializer can match
            // by interval-overlap. `.listenRevert` corrections originate
            // from time-bound UI gestures and serialize as `.exactTimeSpan`.
            id: "corr-D",
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 30.0, endTime: 40.0
            ).serialized,
            source: .listenRevert
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId,
            store: store,
            now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let buckets = Set(loaded.map { $0.bucket })
        #expect(buckets.contains(.positive))
        #expect(buckets.contains(.negative))
        #expect(buckets.contains(.uncertain))
        #expect(buckets.contains(.disagreement))
    }

    @Test("each bucket-fixture scenario maps to its expected bucket")
    func eachScenarioMapsToExpectedBucket() async throws {
        // Stronger contract than `allFourBucketsReachable`: that test only
        // asserts every bucket is *reachable* from the fixture (a smoke
        // signal). This one pins each scan id directly to its expected
        // bucket, so a bucketer regression that swaps two scenarios would
        // fail with a self-explanatory message — the scan ids name the
        // bucket they're meant to land in, so the assertion message reads
        // "te-scan-positive bucket should be .positive". (playhead-4my.10.2)
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // scan-positive: confirmed paid ad — FM containsAd, lexical hit,
        // decision skip-eligible -> .positive
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-positive", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-positive", startTime: 0, endTime: 10)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-positive-fm", sourceType: .fm,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-positive-lex", sourceType: .lexical,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.8),
            transcriptVersion: transcriptVersion
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-positive", windowId: "win-positive",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))

        // scan-negative: editorial mention — FM noAds, decision not
        // skip-eligible -> .negative
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-negative", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-negative", startTime: 10, endTime: 20)
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-negative", windowId: "win-negative",
            skipConfidence: 0.05, gate: "ineligible", policy: "noAction"
        ))

        // scan-uncertain: unusable transcript -> .uncertain
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-uncertain", firstOrdinal: 21, lastOrdinal: 30,
            startTime: 20, endTime: 30, disposition: .abstain,
            quality: .unusable
        ))

        // scan-disagreement: FM-positive but user reverted -> .disagreement
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-disagreement", firstOrdinal: 31, lastOrdinal: 40,
            startTime: 30, endTime: 40, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-disagreement", startTime: 30, endTime: 40)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-disagreement-fm", sourceType: .fm,
                          firstOrdinal: 31, lastOrdinal: 40, certainty: 0.93),
            transcriptVersion: transcriptVersion
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-disagreement", windowId: "win-disagreement",
            skipConfidence: 0.85, gate: "eligible", policy: "autoSkipEligible"
        ))
        try await store.appendCorrectionEvent(correctionEvent(
            id: "corr-disagreement",
            scope: CorrectionScope.exactTimeSpan(
                assetId: assetId, startTime: 30.0, endTime: 40.0
            ).serialized,
            source: .listenRevert
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let byId = Dictionary(uniqueKeysWithValues: loaded.map { ($0.id, $0) })
        // Per-scenario assertions: each scan id names its expected bucket,
        // so a swap regression fails with a self-explanatory message.
        #expect(try #require(byId["te-scan-positive"]).bucket == .positive)
        #expect(try #require(byId["te-scan-negative"]).bucket == .negative)
        #expect(try #require(byId["te-scan-uncertain"]).bucket == .uncertain)
        #expect(try #require(byId["te-scan-disagreement"]).bucket == .disagreement)
    }

    @Test("materialization is idempotent: re-running replaces, doesn't append")
    func materializationIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_200
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(loaded.count == 1)
    }

    @Test("cohort JSON round-trips through materialization")
    func cohortJSONProvenance() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-A", windowId: "win-A",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let row = try #require(loaded.first)
        #expect(row.scanCohortJSON == scanCohortJSON)
        #expect(row.decisionCohortJSON == decisionCohortJSON)
        #expect(row.transcriptVersion == transcriptVersion)
    }

    // MARK: - playhead-4my.10.2 gaps

    /// A second canonical cohort that differs from production by
    /// promptLabel only — exercises mixed-cohort assets without
    /// changing any persistence-validation semantics. Built via the
    /// shared `makeCohortJSON(promptLabel:)` helper in TestHelpers
    /// (L3 dedup) so we encode the cohort in exactly one place.
    private func altCohortJSON() -> String {
        makeCohortJSON(promptLabel: "phase3-shadow-v2-alt")
    }

    private func scanResult(
        id: String,
        firstOrdinal: Int,
        lastOrdinal: Int,
        startTime: Double,
        endTime: Double,
        disposition: CoarseDisposition,
        scanCohortJSON cohort: String,
        quality: TranscriptQuality = .good
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: firstOrdinal,
            windowLastAtomOrdinal: lastOrdinal,
            windowStartTime: startTime,
            windowEndTime: endTime,
            scanPass: "coarse",
            transcriptQuality: quality,
            disposition: disposition,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: 100,
            outputTokenCount: 20,
            latencyMs: 50,
            prewarmHit: false,
            scanCohortJSON: cohort,
            transcriptVersion: transcriptVersion,
            reuseScope: nil,
            runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        )
    }

    @Test("materializer stamps each example with its scan-row's cohort")
    func materializerPreservesPerScanCohort() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let cohortAlt = altCohortJSON()
        // Two scans on disjoint spans, under two different cohorts. The
        // materializer must carry each scan's cohort through into the
        // emitted training example — the cohort is the spine row's, not
        // a constant captured at materialization time.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-prod", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-alt", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds,
            scanCohortJSON: cohortAlt
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        // Cohort-bound subsets are derived consumer-side via Swift filter.
        // The cohort overload that briefly existed on AnalysisStore was
        // removed in cycle 2 of playhead-4my.10.2 (tests-only scope).
        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let prod = loaded.filter { $0.scanCohortJSON == scanCohortJSON }
        let alt = loaded.filter { $0.scanCohortJSON == cohortAlt }
        #expect(prod.map { $0.id } == ["te-scan-prod"])
        #expect(alt.map { $0.id } == ["te-scan-alt"])
        #expect(prod.first?.scanCohortJSON == scanCohortJSON)
        #expect(alt.first?.scanCohortJSON == cohortAlt)
    }

    @Test("materialization stays cohort-filterable after a cohort flip prune")
    func materializationProvenanceSurvivesCohortFlip() async throws {
        // End-to-end provenance: after a cohort flip, the unfiltered load
        // returns BOTH the surviving prior-cohort row and the new-cohort
        // row, AND consumer-side cohort filtering continues to partition
        // them cleanly. This composes durability (H2) with consumer-side
        // provenance filtering (the layer that replaced the deleted
        // SQL overload). Pre-fix paths that wiped prior-cohort rows on
        // re-materialization would fail the durability half; paths that
        // canonicalize cohort strings would fail the filter half.
        // (playhead-4my.10.2)
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let cohortA = scanCohortJSON  // ScanCohort.productionJSON()
        let cohortB = altCohortJSON()

        // Cohort A: two scans on the spine.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A1", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A2", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        // Cohort flip: prune cohort-A scans, insert a cohort-B scan,
        // re-materialize. H2 guarantees the cohort-A training rows
        // survive even though their upstream scan rows are gone.
        let prunedCount = try await store.pruneOrphanedScansForCurrentCohort(
            currentScanCohortJSON: cohortB
        )
        #expect(prunedCount >= 2)

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B1", firstOrdinal: 100, lastOrdinal: 110,
            startTime: 50, endTime: 55, disposition: .containsAd,
            scanCohortJSON: cohortB
        ))
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_200
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let underA = loaded.filter { $0.scanCohortJSON == cohortA }
        let underB = loaded.filter { $0.scanCohortJSON == cohortB }
        #expect(Set(underA.map(\.id)) == ["te-scan-A1", "te-scan-A2"],
                "prior-cohort training rows survive the cohort flip")
        #expect(underB.map(\.id) == ["te-scan-B1"],
                "new-cohort row appears in the new partition")
        // Sanity: the union accounts for every row in the load.
        #expect(underA.count + underB.count == loaded.count)
    }

    @Test("re-materializing with extra evidence on the same span produces no duplicate rows")
    func noDuplicatesOnExtraEvidenceForSameSpan() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // One scan-row spine — therefore exactly one training example.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-fm", sourceType: .fm,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.9),
            transcriptVersion: transcriptVersion
        )

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )
        let firstPass = try await store.loadTrainingExamples(forAsset: assetId)
        try #require(firstPass.count == 1)

        // Add a second evidence event on the same span — different source
        // type, same ordinal range. Re-materialize.
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-lex", sourceType: .lexical,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.7),
            transcriptVersion: transcriptVersion
        )
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_200
        )

        let secondPass = try await store.loadTrainingExamples(forAsset: assetId)
        // No duplicates — still exactly one row, and ids are unique.
        #expect(secondPass.count == 1)
        #expect(Set(secondPass.map { $0.id }).count == secondPass.count)
        // The single row should have absorbed the new evidence source.
        #expect(secondPass.first?.evidenceSources.sorted() == ["fm", "lexical"])
        // Snapshot-rewritten semantic: the row's `createdAt` must reflect
        // the SECOND materialization's `now`, not the first. Pre-fix this
        // could silently regress to "first-write-wins" and the corpus
        // would show stale timestamps after evidence updates. (playhead-4my.10.2)
        #expect(secondPass.first?.createdAt == 1_700_000_200)
    }

    @Test("re-materializing after adding a new scan span produces N+1 rows with no duplicates")
    func noDuplicatesOnNewScanRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )
        let firstPass = try await store.loadTrainingExamples(forAsset: assetId)
        try #require(firstPass.count == 2)
        let firstIds = Set(firstPass.map { $0.id })

        // Add a third disjoint scan-row and re-materialize.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-C", firstOrdinal: 21, lastOrdinal: 30,
            startTime: 20, endTime: 30, disposition: .uncertain
        ))
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_200
        )

        let secondPass = try await store.loadTrainingExamples(forAsset: assetId)
        let secondIds = Set(secondPass.map { $0.id })
        #expect(secondPass.count == 3)
        // Every id is unique (the deterministic id construction
        // `te-<scanId>` is the dedup key).
        #expect(secondIds.count == secondPass.count)
        // The two ids from the first pass are still present.
        #expect(firstIds.isSubset(of: secondIds))
        // And exactly one new id was added.
        #expect(secondIds.subtracting(firstIds).count == 1)
    }

    @Test("materializer no-ops when the asset has no scan-result spine")
    func emptyAssetDoesNotCrash() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(loaded.isEmpty)
    }

    // MARK: - C1: correction-scope parser uses real CorrectionScope.serialized

    @Test("C1: .exactTimeSpan correction localizes by time, not whole asset")
    func exactTimeSpanCorrectionScopedToOverlap() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Two scans at non-overlapping time ranges. Only scan-A overlaps the
        // correction's time window.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 100, endTime: 110, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        try await store.insertAdWindow(
            adWindow(id: "win-B", startTime: 100, endTime: 110)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-fm", sourceType: .fm,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-B-fm", sourceType: .fm,
                          firstOrdinal: 11, lastOrdinal: 20, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        // Use the canonical CorrectionScope.serialized output. `.exactTimeSpan`
        // is what `.listenRevert` corrections produce in production — pre-fix
        // the parser treated the prefix as `.exactSpan` and matched every
        // scan in the asset, contaminating every region with userReverted.
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId, startTime: 0.0, endTime: 9.0
        ).serialized
        try await store.appendCorrectionEvent(correctionEvent(
            id: "corr-A", scope: scope, source: .listenRevert
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let byId = Dictionary(uniqueKeysWithValues: loaded.map { ($0.id, $0) })
        let a = try #require(byId["te-scan-A"])
        let b = try #require(byId["te-scan-B"])
        // Scan-A overlaps the correction time range -> .disagreement (model-vs-user).
        #expect(a.bucket == .disagreement)
        // Scan-B is far away — must NOT inherit the correction.
        #expect(b.bucket != .disagreement)
    }

    @Test("C1: .exactSpan correction localizes by atom ordinal, not time-as-ordinal")
    func exactSpanCorrectionScopedByOrdinal() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 100.0, endTime: 200.0, disposition: .containsAd
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B", firstOrdinal: 50, lastOrdinal: 60,
            startTime: 300.0, endTime: 400.0, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 100.0, endTime: 200.0)
        )
        try await store.insertAdWindow(
            adWindow(id: "win-B", startTime: 300.0, endTime: 400.0)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-fm", sourceType: .fm,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-B-fm", sourceType: .fm,
                          firstOrdinal: 50, lastOrdinal: 60, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        // .exactSpan is ORDINAL-based. Crucially, ordinals 0...10 do NOT
        // overlap times 300...400 — pre-fix the parser tried to read the
        // ordinals as Doubles and silently fell through to "match every
        // scan in the asset".
        let scope = CorrectionScope.exactSpan(
            assetId: assetId, ordinalRange: 0...10
        ).serialized
        try await store.appendCorrectionEvent(correctionEvent(
            id: "corr-A", scope: scope, source: .manualVeto
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let byId = Dictionary(uniqueKeysWithValues: loaded.map { ($0.id, $0) })
        let a = try #require(byId["te-scan-A"])
        let b = try #require(byId["te-scan-B"])
        // Scan-A's ordinals 0...10 overlap correction's 0...10 -> disagreement.
        #expect(a.bucket == .disagreement)
        // Scan-B's ordinals 50...60 do NOT overlap correction 0...10.
        #expect(b.bucket != .disagreement)
    }

    // MARK: - H1: parseCertainty maps CertaintyBand strings, not raw doubles

    @Test("H1: EvidencePayload certainty=\"strong\" produces a non-zero fmCertainty")
    func certaintyBandStrongMaterializesAboveGate() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )

        // Construct a real persisted-shape evidenceJSON with the string-band
        // certainty that production writes. Pre-fix the materializer parsed
        // this as a Double and got 0.0, which silently disabled the
        // bucketer's `fmCertainty >= 0.7` positive gate.
        let payload = """
        {"commercialIntent":"paid","ownership":"thirdParty","certainty":"strong","boundaryPrecision":"precise","firstLineRef":0,"lastLineRef":1,"jobId":"job-1","memoryWriteEligible":true,"anchors":null,"ownershipInferenceWasSuppressed":false}
        """
        let ordinals = Array(0...10)
        let ordinalsJSON = String(
            data: try JSONSerialization.data(withJSONObject: ordinals),
            encoding: .utf8
        ) ?? "[]"
        _ = try await store.insertEvidenceEvent(
            EvidenceEvent(
                id: "ev-A-fm",
                analysisAssetId: assetId,
                eventType: "scan",
                sourceType: .fm,
                atomOrdinals: ordinalsJSON,
                evidenceJSON: payload,
                scanCohortJSON: scanCohortJSON,
                createdAt: 1_700_000_000,
                runMode: .targeted,
                jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
            ),
            transcriptVersion: transcriptVersion
        )

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let row = try #require(loaded.first)
        #expect(row.fmCertainty == 0.9, "strong band should map to 0.9")
        #expect(row.fmCertainty >= 0.7, "must clear the bucketer's positive gate")
    }

    @Test("H1: parseCertainty unit — band → double mapping")
    func parseCertaintyMapsBands() {
        // Direct unit assertions on the helpers. These pin the mapping the
        // bucketer's positive gate (0.7) depends on.
        #expect(TrainingExampleMaterializer.certaintyBandToDouble("weak") == 0.3)
        #expect(TrainingExampleMaterializer.certaintyBandToDouble("moderate") == 0.6)
        #expect(TrainingExampleMaterializer.certaintyBandToDouble("strong") == 0.9)
        #expect(TrainingExampleMaterializer.certaintyBandToDouble("bogus") == nil)

        // String form (production):
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":"strong"}"#) == 0.9)
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":"moderate"}"#) == 0.6)
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":"weak"}"#) == 0.3)
        // Numeric fallback (fixtures):
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":0.42}"#) == 0.42)
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":1}"#) == 1.0)
        // No certainty -> 0.0:
        #expect(TrainingExampleMaterializer.parseCertainty(#"{}"#) == 0.0)
    }

    // MARK: - H2: cohort durability across re-materialization

    @Test("H2: re-materialization on a smaller spine preserves prior-cohort rows")
    func cohortDurabilityAcrossReMaterialization() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Cohort A: two scans on the spine. We use a hand-rolled cohort JSON
        // with sorted keys so the canonicalizer in
        // `pruneOrphanedScansForCurrentCohort` matches it byte-for-byte.
        let cohortA = ScanCohort.productionJSON()
        try await store.insertSemanticScanResult(SemanticScanResult(
            id: "scan-A1",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0, windowLastAtomOrdinal: 10,
            windowStartTime: 0, windowEndTime: 10,
            scanPass: "coarse",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1, errorContext: nil,
            inputTokenCount: 100, outputTokenCount: 20,
            latencyMs: 50, prewarmHit: false,
            scanCohortJSON: cohortA,
            transcriptVersion: transcriptVersion,
            reuseScope: nil, runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        ))
        try await store.insertSemanticScanResult(SemanticScanResult(
            id: "scan-A2",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 11, windowLastAtomOrdinal: 20,
            windowStartTime: 10, windowEndTime: 20,
            scanPass: "coarse",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1, errorContext: nil,
            inputTokenCount: 100, outputTokenCount: 20,
            latencyMs: 50, prewarmHit: false,
            scanCohortJSON: cohortA,
            transcriptVersion: transcriptVersion,
            reuseScope: nil, runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let cohortAExamples = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(cohortAExamples.count == 2)
        let cohortAIds = Set(cohortAExamples.map(\.id))

        // Simulate cohort flip: cohort B is the new "current" cohort, so the
        // prune deletes all cohort-A scans (and their downstream evidence).
        // Then insert a fresh cohort-B scan and re-materialize.
        let cohortBStruct = ScanCohort(
            promptLabel: "phase3-shadow-v2",
            promptHash: "phase3-prompt-2026-04-30",
            schemaHash: "phase3-schema-2026-04-30",
            scanPlanHash: "phase3-plan-2026-04-30",
            normalizationHash: "phase3-norm-2026-04-30",
            osBuild: "26.0.0",
            locale: "en_US",
            appBuild: "1"
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let cohortB = String(data: try encoder.encode(cohortBStruct), encoding: .utf8)!

        let prunedCount = try await store.pruneOrphanedScansForCurrentCohort(
            currentScanCohortJSON: cohortB
        )
        // Two cohort-A scan rows should have been deleted by the prune.
        #expect(prunedCount >= 2)

        try await store.insertSemanticScanResult(SemanticScanResult(
            id: "scan-B1",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 100, windowLastAtomOrdinal: 110,
            windowStartTime: 50, windowEndTime: 55,
            scanPass: "coarse",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1, errorContext: nil,
            inputTokenCount: 100, outputTokenCount: 20,
            latencyMs: 50, prewarmHit: false,
            scanCohortJSON: cohortB,
            transcriptVersion: transcriptVersion,
            reuseScope: nil, runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        ))

        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_200
        )

        let merged = try await store.loadTrainingExamples(forAsset: assetId)
        let mergedIds = Set(merged.map(\.id))
        // Pre-H2: cohort-A rows would have been DELETEd by the asset-scoped
        // wipe in `replaceTrainingExamples`. Post-fix: id-keyed upsert
        // preserves them across cohort flips.
        #expect(cohortAIds.isSubset(of: mergedIds), "prior cohort rows must survive re-materialization")
        #expect(mergedIds.contains("te-scan-B1"), "new cohort row must be inserted")
    }

    // MARK: - M2: userAction reflects actual skip execution

    @Test("M2: eligible-but-unskipped window is labelled eligibleNotSkipped")
    func eligibleWithoutExecutionIsNotSkipped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        // wasSkipped: false on the AdWindow (default in fixture).
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-A", windowId: "win-A",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let row = try #require(
            (try await store.loadTrainingExamples(forAsset: assetId)).first
        )
        #expect(row.userAction == "eligibleNotSkipped")
    }

    @Test("M2: actually-skipped window is labelled skipped")
    func actuallySkippedWindowIsSkipped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        // wasSkipped: true via overload below.
        let win = AdWindow(
            id: "win-A",
            analysisAssetId: assetId,
            startTime: 0, endTime: 10,
            confidence: 0.9,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: AdDetectionConfig.default.detectorVersion,
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "none", metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false
        )
        try await store.insertAdWindow(win)
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-A", windowId: "win-A",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let row = try #require(
            (try await store.loadTrainingExamples(forAsset: assetId)).first
        )
        #expect(row.userAction == "skipped")
    }

    // MARK: - M5: failed/refusal scans are not materialized

    @Test("M5: non-success scan rows do not produce training examples")
    func nonSuccessScansSkipped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Successful scan -> materialized.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        // Refusal scan -> filtered out.
        try await store.insertSemanticScanResult(SemanticScanResult(
            id: "scan-refusal",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 11,
            windowLastAtomOrdinal: 20,
            windowStartTime: 10,
            windowEndTime: 20,
            scanPass: "coarse",
            transcriptQuality: .good,
            disposition: .abstain,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 1, errorContext: "model refused",
            inputTokenCount: nil, outputTokenCount: nil,
            latencyMs: nil, prewarmHit: false,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: transcriptVersion,
            reuseScope: nil, runMode: .targeted,
            jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(loaded.map(\.id) == ["te-scan-A"])
    }

    // MARK: - L1: inferCommercialIntent has no fmPositive-dependent branch

    @Test("L1: uncertain bucket returns same intent regardless of fmPositive flag")
    func uncertainCommercialIntentIsUnambiguous() async throws {
        // The simplification (collapsed dead ternary) is locked in by the
        // fact that .uncertain is ALWAYS unknown. Build two windows with
        // different FM positivity that both land in .uncertain (unusable
        // transcript) and confirm both report the same intent.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-fm-pos", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd, quality: .unusable
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-fm-neg", firstOrdinal: 11, lastOrdinal: 20,
            startTime: 10, endTime: 20, disposition: .noAds, quality: .unusable
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let byId = Dictionary(uniqueKeysWithValues: loaded.map { ($0.id, $0) })
        let pos = try #require(byId["te-scan-fm-pos"])
        let neg = try #require(byId["te-scan-fm-neg"])
        #expect(pos.bucket == .uncertain)
        #expect(neg.bucket == .uncertain)
        // Both report `.unknown` regardless of fmPositive.
        #expect(pos.commercialIntent == neg.commercialIntent)
        #expect(pos.commercialIntent == CommercialIntent.unknown.rawValue)
    }

    // MARK: - L2: half-open time interval — adjacent ad-windows don't both claim a scan

    @Test("L2: a scan on the boundary between two AdWindows is claimed by exactly one (or neither)")
    func halfOpenBoundaryAvoidsDoubleClaim() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Two adjacent AdWindows touching at t=10.
        try await store.insertAdWindow(
            adWindow(id: "win-left", startTime: 0, endTime: 10)
        )
        try await store.insertAdWindow(
            adWindow(id: "win-right", startTime: 10, endTime: 20)
        )
        // A scan that exactly straddles the boundary [10, 20) — half-open
        // semantics mean only win-right claims it. Pre-test it's possible
        // the docstring's "must not double-claim" guarantee silently regressed.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-boundary", firstOrdinal: 50, lastOrdinal: 60,
            startTime: 10, endTime: 20, disposition: .containsAd
        ))
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-left", windowId: "win-left",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-right", windowId: "win-right",
            skipConfidence: 0.4, gate: "ineligible", policy: "noAction"
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        // Decision picked must come from exactly ONE of the two. Pre-fix /
        // pre-rename, both could match (closed-interval overlap) and the
        // higher-confidence one would win arbitrarily; the documented
        // half-open contract picks win-right (the one whose start matches).
        let row = try #require(
            (try await store.loadTrainingExamples(forAsset: assetId)).first
        )
        // win-right's eligibilityGate is "ineligible".
        #expect(row.eligibilityGate == "ineligible")
    }

    // MARK: - cycle-2 M-C: skip attribution follows the picked AdWindow

    @Test("cycle-2 M-C: scanWasSkipped reads off the picked AdWindow, not the union")
    func skipAttributionMatchesPickedDecisionWindow() async throws {
        // Two overlapping AdWindows on a single scan. Window-A has the higher
        // skipConfidence (so the picker selects it for eligibilityGate /
        // decisionCohortJSON), but window-B is the one that was actually
        // skipped. Pre-fix the materializer aggregated `wasSkipped` across
        // BOTH windows and stamped userAction="skipped" on the example —
        // mismatched provenance with the decision data, which came from
        // window-A. Post-fix the example must report
        // userAction="eligibleNotSkipped" (window-A's eligible-not-skipped
        // status) because that is the window the picker chose.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        // Two AdWindows, both overlapping the scan in time.
        let windowA = AdWindow(
            id: "win-A",
            analysisAssetId: assetId,
            startTime: 0, endTime: 5,
            confidence: 0.9,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: AdDetectionConfig.default.detectorVersion,
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "none", metadataConfidence: nil,
            metadataPromptVersion: nil,
            // Higher-confidence window — the picker selects this one — but
            // it was NOT actually skipped.
            wasSkipped: false,
            userDismissedBanner: false
        )
        let windowB = AdWindow(
            id: "win-B",
            analysisAssetId: assetId,
            startTime: 5, endTime: 10,
            confidence: 0.5,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: AdDetectionConfig.default.detectorVersion,
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "none", metadataConfidence: nil,
            metadataPromptVersion: nil,
            // Lower-confidence window — actually skipped at playback. Pre-fix
            // this contaminated the picked-window's example with a wrong
            // `userAction = "skipped"`.
            wasSkipped: true,
            userDismissedBanner: false
        )
        try await store.insertAdWindow(windowA)
        try await store.insertAdWindow(windowB)
        // Two decisions: one per AdWindow. Picker should select win-A
        // (skipConfidence 0.9 > 0.5).
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-A", windowId: "win-A",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-B", windowId: "win-B",
            skipConfidence: 0.5, gate: "eligible", policy: "autoSkipEligible"
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let row = try #require(
            (try await store.loadTrainingExamples(forAsset: assetId)).first
        )
        // Picker chose win-A → eligibilityGate is win-A's "eligible".
        #expect(row.eligibilityGate == "eligible")
        // Coherent provenance: userAction comes from THE SAME window the
        // picker selected (win-A, wasSkipped=false), not the union.
        #expect(row.userAction == "eligibleNotSkipped",
                "userAction must follow the picked window, not the union")
    }

    /// cycle-3 L5: inverse of the cycle-2 M-C test above. The other test
    /// pinned that the picker correctly carries `wasSkipped=false` from
    /// the picked (higher-confidence, not-skipped) window. This one pins
    /// the symmetric case: the higher-confidence decision belongs to the
    /// SKIPPED window, so the example must report `wasSkipped=true`
    /// (`userAction="skipped"`). Without both directions, a future bug
    /// that hard-coded `wasSkipped=false` would still pass the original
    /// M-C assertion.
    @Test("cycle-3 L5: scanWasSkipped follows the picked window when the picked window is the skipped one")
    func skipAttributionFollowsPickedWindowInverseCase() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        // Two AdWindows, both overlapping the scan in time. Mirror the
        // M-C topology but flip which window was actually skipped: the
        // higher-confidence window IS the skipped one this time.
        let windowA = AdWindow(
            id: "win-A",
            analysisAssetId: assetId,
            startTime: 0, endTime: 5,
            confidence: 0.9,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: AdDetectionConfig.default.detectorVersion,
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "none", metadataConfidence: nil,
            metadataPromptVersion: nil,
            // Higher-confidence window AND the skipped one. Picker must
            // select this one → userAction must be "skipped".
            wasSkipped: true,
            userDismissedBanner: false
        )
        let windowB = AdWindow(
            id: "win-B",
            analysisAssetId: assetId,
            startTime: 5, endTime: 10,
            confidence: 0.5,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: AdDetectionConfig.default.detectorVersion,
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "none", metadataConfidence: nil,
            metadataPromptVersion: nil,
            // Lower-confidence window, NOT skipped. A buggy union-based
            // picker could either over-attribute (still skipped via win-A)
            // or under-attribute (mask the skip with win-B's false). The
            // post-fix picker reads off the SAME window it selected.
            wasSkipped: false,
            userDismissedBanner: false
        )
        try await store.insertAdWindow(windowA)
        try await store.insertAdWindow(windowB)
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-A", windowId: "win-A",
            skipConfidence: 0.9, gate: "eligible", policy: "autoSkipEligible"
        ))
        try await store.appendDecisionEvent(decisionEvent(
            id: "dec-B", windowId: "win-B",
            skipConfidence: 0.5, gate: "eligible", policy: "autoSkipEligible"
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let row = try #require(
            (try await store.loadTrainingExamples(forAsset: assetId)).first
        )
        // Picker chose win-A (skipConfidence 0.9 > 0.5).
        #expect(row.eligibilityGate == "eligible")
        // win-A.wasSkipped == true → userAction must be "skipped".
        #expect(row.userAction == "skipped",
                "userAction must follow the picked window's wasSkipped (true here)")
    }

    /// cycle-3 L2: deterministic tiebreak on equal `skipConfidence`. With
    /// two decisions tied at the same confidence, the picker must
    /// consistently pick the same one across runs. Pre-fix
    /// `loadDecisionEvents` ordered by `createdAt` only — equal
    /// `createdAt` produced whatever rowid order SQLite returned,
    /// which is undefined. Post-fix the SQL is `ORDER BY createdAt
    /// ASC, rowid ASC` and the picker walks candidates with strict
    /// inequality so the first equal-confidence row wins.
    @Test("cycle-3 L2: equal-skipConfidence decisions resolve deterministically across runs")
    func equalSkipConfidenceResolvesDeterministically() async throws {
        // Run the materialization twice over an identical seed and
        // assert both runs converge on the same picked decision. (We
        // also assert the FIRST-inserted candidate wins, which is the
        // contract `(createdAt ASC, rowid ASC)` enforces.)
        var pickedGates: [String] = []
        for runIndex in 0..<2 {
            let store = try await makeTestStore()
            try await store.insertAsset(makeAsset())
            try await store.insertSemanticScanResult(scanResult(
                id: "scan-tie-\(runIndex)", firstOrdinal: 0, lastOrdinal: 10,
                startTime: 0, endTime: 10, disposition: .containsAd
            ))
            // Two AdWindows that both overlap the scan in time, distinct
            // ids, distinct eligibilityGates so we can identify which
            // decision the picker took.
            try await store.insertAdWindow(adWindow(
                id: "win-first", startTime: 0, endTime: 5
            ))
            try await store.insertAdWindow(adWindow(
                id: "win-second", startTime: 5, endTime: 10
            ))
            // Two decisions tied at exactly equal skipConfidence. Use
            // identical createdAt timestamps to force the rowid
            // tiebreaker to engage. Insert "win-first" FIRST so its
            // rowid is lower — the deterministic contract says the
            // first-inserted equal-confidence row wins.
            try await store.appendDecisionEvent(decisionEvent(
                id: "dec-first", windowId: "win-first",
                skipConfidence: 0.75,
                gate: "eligible-first", policy: "autoSkipEligible",
                createdAt: 1_700_000_000
            ))
            try await store.appendDecisionEvent(decisionEvent(
                id: "dec-second", windowId: "win-second",
                skipConfidence: 0.75,
                gate: "eligible-second", policy: "autoSkipEligible",
                createdAt: 1_700_000_000
            ))

            let materializer = TrainingExampleMaterializer()
            try await materializer.materialize(
                forAsset: assetId, store: store, now: 1_700_000_100
            )
            let row = try #require(
                (try await store.loadTrainingExamples(forAsset: assetId)).first
            )
            pickedGates.append(row.eligibilityGate ?? "<nil>")
        }
        // Both runs must have picked the same decision.
        #expect(pickedGates[0] == pickedGates[1],
                "two identical seeds picked different decisions (\(pickedGates[0]) vs \(pickedGates[1])) — picker is non-deterministic on ties")
        // And the first-inserted row should win (lower rowid).
        #expect(pickedGates[0] == "eligible-first",
                "first-inserted equal-confidence candidate must win the tie, got \(pickedGates[0])")
    }

    // MARK: - cycle-2 L-B: FM-emitted commercialIntent / ownership wired through

    @Test("cycle-2 L-B: EvidencePayload commercialIntent / ownership propagate to the example")
    func fmCommercialIntentAndOwnershipPropagate() async throws {
        // Pre-fix the materializer ignored the FM-emitted strings entirely
        // and stamped a bucket-driven placeholder. Post-fix the per-span
        // FM verdict wins; the placeholder is only used when no FM evidence
        // row supplied a value.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        // Real persisted-shape EvidencePayload with explicit FM strings.
        // The bucketer would otherwise emit `.unknown` placeholders.
        let payload = """
        {"commercialIntent":"paid","ownership":"thirdParty","certainty":"strong","boundaryPrecision":"precise","firstLineRef":0,"lastLineRef":1,"jobId":"job-1","memoryWriteEligible":true,"anchors":null,"ownershipInferenceWasSuppressed":false}
        """
        let ordinals = Array(0...10)
        let ordinalsJSON = String(
            data: try JSONSerialization.data(withJSONObject: ordinals),
            encoding: .utf8
        ) ?? "[]"
        _ = try await store.insertEvidenceEvent(
            EvidenceEvent(
                id: "ev-A-fm",
                analysisAssetId: assetId,
                eventType: "scan",
                sourceType: .fm,
                atomOrdinals: ordinalsJSON,
                evidenceJSON: payload,
                scanCohortJSON: scanCohortJSON,
                createdAt: 1_700_000_000,
                runMode: .targeted,
                jobPhase: BackfillJobPhase.fullEpisodeScan.rawValue
            ),
            transcriptVersion: transcriptVersion
        )

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let row = try #require(
            (try await store.loadTrainingExamples(forAsset: assetId)).first
        )
        // Bucket would derive these to ".paid"/".thirdParty" anyway in this
        // case, but the assertion that matters is that even when the bucket
        // would have derived ".unknown" we'd still see the FM verdict. Pin
        // the propagation contract via parsePayloadStringField directly.
        #expect(row.commercialIntent == "paid")
        #expect(row.ownership == "thirdParty")
    }

    @Test("cycle-2 L-B: parsePayloadStringField unit — present, missing, malformed")
    func parsePayloadStringFieldUnit() {
        // Present:
        let present = #"{"commercialIntent":"paid","ownership":"thirdParty"}"#
        #expect(TrainingExampleMaterializer.parsePayloadStringField(present, key: "commercialIntent") == "paid")
        #expect(TrainingExampleMaterializer.parsePayloadStringField(present, key: "ownership") == "thirdParty")
        // Missing key:
        #expect(TrainingExampleMaterializer.parsePayloadStringField(present, key: "absent") == nil)
        // Empty string -> nil (treat as no opinion):
        #expect(TrainingExampleMaterializer.parsePayloadStringField(#"{"x":""}"#, key: "x") == nil)
        // Wrong type:
        #expect(TrainingExampleMaterializer.parsePayloadStringField(#"{"x":42}"#, key: "x") == nil)
        // Malformed JSON:
        #expect(TrainingExampleMaterializer.parsePayloadStringField("not json", key: "x") == nil)
        // Empty:
        #expect(TrainingExampleMaterializer.parsePayloadStringField("", key: "x") == nil)
    }

    // MARK: - cycle-2 L-C: coarse-only positive yields fmCertainty=0 (intentional)

    @Test("cycle-2 L-C: scan disposition=.containsAd with no evidence row pins fmCertainty=0")
    func coarseOnlyPositiveYieldsZeroCertainty() async throws {
        // A coarse-pass-only positive: the scan flagged the region but
        // there's no per-region FM evidence row to read certainty from.
        // The materializer intentionally stamps fmCertainty=0 here so
        // weak coarse hits do NOT clear the bucketer's `>= 0.7` positive
        // gate without corroborating evidence. Pinned so a future change
        // wanting to default coarse hits to a non-zero band must update
        // this test deliberately.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let row = try #require(
            (try await store.loadTrainingExamples(forAsset: assetId)).first
        )
        #expect(row.fmCertainty == 0.0,
                "coarse-only positive must yield fmCertainty=0 — corroborating evidence row required for any band")
    }

    // MARK: - cycle-2 L-D: parseCertainty silent-fallback shapes

    @Test("cycle-2 L-D: parseCertainty silently returns 0 across malformed shapes")
    func parseCertaintyMalformedJsonReturnsZero() {
        // L-D: the silent-zero fallback is intentional. This test pins the
        // current behaviour for every malformed shape we anticipate; any
        // future producer change MUST be caught by a separate audit.
        // Not JSON at all:
        #expect(TrainingExampleMaterializer.parseCertainty("nope") == 0.0)
        // Empty string:
        #expect(TrainingExampleMaterializer.parseCertainty("") == 0.0)
        // JSON but not an object:
        #expect(TrainingExampleMaterializer.parseCertainty("[1,2,3]") == 0.0)
        #expect(TrainingExampleMaterializer.parseCertainty("\"strong\"") == 0.0)
        // Missing key:
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"other":42}"#) == 0.0)
        // Unknown band string:
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":"medium"}"#) == 0.0)
        // Wrong type — null:
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":null}"#) == 0.0)
        // Wrong type — bool:
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":true}"#) == 0.0)
        // Wrong type — array:
        #expect(TrainingExampleMaterializer.parseCertainty(#"{"certainty":[0.5]}"#) == 0.0)
    }

    // MARK: - cycle-2 M-A: cohort prune leaves training_examples physically untouched

    @Test("cycle-2 M-A: cohort prune does not modify materialized training_examples rows")
    func cohortPruneLeavesTrainingExamplesPhysicallyUntouched() async throws {
        // Stronger version of the H2 durability test: assert the persisted
        // `createdAt` timestamps survive the prune byte-equal. The bead's
        // contract is that `pruneOrphanedScansForCurrentCohort` MUST NOT
        // touch `training_examples` — not even rewrite an unchanged row
        // (which would bump `rowid` and could perturb downstream readers).
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let before = try await store.loadTrainingExamples(forAsset: assetId)
        let beforeCreatedAt = before.map(\.createdAt)
        let beforeIds = before.map(\.id)
        #expect(!before.isEmpty, "must have a baseline row to compare against")

        // Flip the cohort so the prune's WHERE clause matches every existing
        // scan-result row. The prune wipes scan_results + evidence_events
        // (cohort-scoped tables) but MUST NOT touch training_examples.
        let cohortB = ScanCohort(
            promptLabel: "phase3-shadow-v2",
            promptHash: "phase3-prompt-cycle2",
            schemaHash: "phase3-schema-cycle2",
            scanPlanHash: "phase3-plan-cycle2",
            normalizationHash: "phase3-norm-cycle2",
            osBuild: "26.0.0",
            locale: "en_US",
            appBuild: "1"
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let cohortBJSON = String(data: try encoder.encode(cohortB), encoding: .utf8)!
        _ = try await store.pruneOrphanedScansForCurrentCohort(
            currentScanCohortJSON: cohortBJSON
        )

        let after = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(after.map(\.id) == beforeIds, "prune must not delete rows")
        // Byte-equality on createdAt: the prune must not have rewritten the
        // row with a fresh timestamp or any other field churn.
        #expect(after.map(\.createdAt) == beforeCreatedAt,
                "prune must not perturb training_examples rows")
    }

    // MARK: - Match-all CorrectionScope cases (sponsorOnShow, etc.)

    @Test("cycle-2: sponsorOnShow correction matches every scan in the asset")
    func sponsorOnShowMatchesEveryScan() async throws {
        try await assertWiderScopeMatchesEveryScan(
            scope: .sponsorOnShow(podcastId: "pc-1", sponsor: "AcmeCo")
        )
    }

    @Test("cycle-2: phraseOnShow correction matches every scan in the asset")
    func phraseOnShowMatchesEveryScan() async throws {
        try await assertWiderScopeMatchesEveryScan(
            scope: .phraseOnShow(podcastId: "pc-1", phrase: "promo code")
        )
    }

    @Test("cycle-2: campaignOnShow correction matches every scan in the asset")
    func campaignOnShowMatchesEveryScan() async throws {
        try await assertWiderScopeMatchesEveryScan(
            scope: .campaignOnShow(podcastId: "pc-1", campaign: "summer-2026")
        )
    }

    @Test("cycle-2: domainOwnershipOnShow correction matches every scan in the asset")
    func domainOwnershipOnShowMatchesEveryScan() async throws {
        try await assertWiderScopeMatchesEveryScan(
            scope: .domainOwnershipOnShow(podcastId: "pc-1", domain: "example.com")
        )
    }

    @Test("cycle-2: jingleOnShow correction matches every scan in the asset")
    func jingleOnShowMatchesEveryScan() async throws {
        try await assertWiderScopeMatchesEveryScan(
            scope: .jingleOnShow(podcastId: "pc-1", jingleId: "jingle-1")
        )
    }

    /// Helper for the five wider-scope cases: assert the correction propagates
    /// to every scan-region of the asset, regardless of time/ordinal range.
    /// Each scope variant is non-span-bound by construction (it applies at
    /// show level, not span level), so the materializer's `correctionOverlaps`
    /// must return true for both scans.
    private func assertWiderScopeMatchesEveryScan(scope: CorrectionScope) async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Two scans at distant times/ordinals so a buggy implementation that
        // tried (incorrectly) to localize a wider scope would fail one of them.
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-A", firstOrdinal: 0, lastOrdinal: 10,
            startTime: 0, endTime: 10, disposition: .containsAd
        ))
        try await store.insertSemanticScanResult(scanResult(
            id: "scan-B", firstOrdinal: 1000, lastOrdinal: 1010,
            startTime: 5_000, endTime: 5_100, disposition: .containsAd
        ))
        try await store.insertAdWindow(
            adWindow(id: "win-A", startTime: 0, endTime: 10)
        )
        try await store.insertAdWindow(
            adWindow(id: "win-B", startTime: 5_000, endTime: 5_100)
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-A-fm", sourceType: .fm,
                          firstOrdinal: 0, lastOrdinal: 10, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        _ = try await store.insertEvidenceEvent(
            evidenceEvent(id: "ev-B-fm", sourceType: .fm,
                          firstOrdinal: 1000, lastOrdinal: 1010, certainty: 0.95),
            transcriptVersion: transcriptVersion
        )
        // Every wider scope persists with the source kind set to a
        // false-positive correction (user said "this is not an ad on this
        // show" / domain-owned / jingle / etc.).
        try await store.appendCorrectionEvent(correctionEvent(
            id: "corr-wider", scope: scope.serialized,
            // .manualVeto is a false-positive correction in any scope; the
            // matching itself doesn't depend on the source.
            source: .manualVeto
        ))

        let materializer = TrainingExampleMaterializer()
        try await materializer.materialize(
            forAsset: assetId, store: store, now: 1_700_000_100
        )

        let loaded = try await store.loadTrainingExamples(forAsset: assetId)
        let byId = Dictionary(uniqueKeysWithValues: loaded.map { ($0.id, $0) })
        let a = try #require(byId["te-scan-A"])
        let b = try #require(byId["te-scan-B"])
        // Both scans must carry the wider correction (.disagreement bucket
        // = FM-positive-but-user-reverted).
        #expect(a.bucket == .disagreement,
                "scan-A must inherit a wider-scope correction (\(scope))")
        #expect(b.bucket == .disagreement,
                "scan-B must inherit a wider-scope correction (\(scope))")
    }

    // MARK: - cycle-2 H-A: AnalysisStoreError surfaces a useful description

    @Test("cycle-2 H-A: String(describing: AnalysisStoreError) carries the case name and payload")
    func analysisStoreErrorDescriptionCarriesCaseName() {
        // The AdDetectionService catch handler used to log
        // `error.localizedDescription`, which for `AnalysisStoreError`
        // (which conforms to Error + CustomStringConvertible but NOT
        // LocalizedError) bridges through NSError and returns the useless
        // "The operation couldn't be completed. (Playhead.AnalysisStoreError
        // error N.)" string. Post-fix the handler logs `String(describing:)`
        // (which routes to `description`) plus the explicit case-name token
        // from `BackfillJobRunner.caseName(of:)`.
        //
        // Pin both: the description must be non-empty and must include
        // enough payload to actually triage from the field.
        let err = AnalysisStoreError.encodingFailure(
            "training_examples.evidenceSourcesJSON: encoder returned non-UTF8 bytes"
        )
        let detail = String(describing: err)
        #expect(!detail.isEmpty)
        #expect(detail.contains("Encoding failure"),
                "description should surface the case payload: \(detail)")
        // The case-name token used by the production logger must be the
        // stable enum-case form, not the bridged ordinal.
        #expect(BackfillJobRunner.caseName(of: err) == "encodingFailure")

        // Cover insertFailed too — different case, same contract: detail
        // must carry the message, caseName must match.
        let err2 = AnalysisStoreError.insertFailed("disk full")
        let detail2 = String(describing: err2)
        #expect(detail2.contains("Insert failed"))
        #expect(detail2.contains("disk full"))
        #expect(BackfillJobRunner.caseName(of: err2) == "insertFailed")
    }
}
