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
        policy: String
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
            createdAt: 1_700_000_001,
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
}
