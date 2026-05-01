// DecisionResultArtifactWiringTests.swift
// Bug 6: regression test that pins the production wiring gap fix —
// `AdDetectionService.runBackfill` must persist exactly one
// `ad_decision_results` row per asset, with `decisionJSON` round-tripping
// the per-window decisions and matching the count of `decision_events`
// rows written for the same asset.
//
// Prior to the fix, `saveDecisionResultArtifact` was only called from
// tests, so the production table sat empty in shipped builds even though
// `decision_events` was populated normally.

import Foundation
import Testing
@testable import Playhead

// MARK: - Local helpers

/// Test asset shaped to drive the full backfill pipeline.
private func makeArtifactTestAsset(id: String) -> AnalysisAsset {
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

/// Transcript chunks that contain a strong lexical ad signal so the
/// pipeline produces at least one fusion window. Mirrors the chunk shape
/// used by `BackfillFusionPipelineTests` so we exercise the same code
/// paths the existing decision_events tests already cover.
private func makeArtifactAdChunks(assetId: String) -> [TranscriptChunk] {
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

private func makeArtifactService(store: AnalysisStore) -> AdDetectionService {
    let config = AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: "test-decision-results-v1",
        fmBackfillMode: .off
    )
    return AdDetectionService(
        store: store,
        classifier: RuleBasedClassifier(),
        metadataExtractor: FallbackExtractor(),
        config: config
    )
}

// MARK: - Suite

@Suite("DecisionResultArtifact wiring (Bug 6)")
struct DecisionResultArtifactWiringTests {

    @Test("runBackfill persists exactly one DecisionResultArtifact whose decisionJSON round-trips and matches decision_events count")
    func runBackfillPersistsArtifactPerAsset() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-artifact-wiring"
        try await store.insertAsset(makeArtifactTestAsset(id: assetId))

        let service = makeArtifactService(store: store)
        let chunks = makeArtifactAdChunks(assetId: assetId)

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-artifact-wiring",
            episodeDuration: 90.0
        )

        // 1) Exactly one row exists for this asset (UNIQUE constraint on
        //    analysisAssetId already enforces this; we still assert via
        //    the loader to catch a regression that produces zero rows).
        let loaded = try await store.loadDecisionResultArtifact(for: assetId)
        let artifact = try #require(loaded, "Bug 6 regression: ad_decision_results should have one row per analysed asset, but loadDecisionResultArtifact returned nil")
        #expect(artifact.analysisAssetId == assetId)
        #expect(!artifact.id.isEmpty, "Artifact must carry a non-empty id")
        #expect(artifact.createdAt > 0, "Artifact must carry a sensible createdAt")

        // 2) decisionJSON must decode back to the same per-window decisions
        //    the pipeline persisted. We round-trip through PersistedDecisionResult
        //    (the on-disk DTO) so this test is anchored to the persistence
        //    contract, not the runtime AdDecisionResult type.
        let decoded = try decodePersistedDecisions(artifact.decisionJSON)

        // 3) The count of decision_events rows must match the count of
        //    windows in the artifact's decisionJSON. Both are produced by
        //    the same per-span loop; if they diverge, the artifact is
        //    out-of-sync with the audit trail.
        let events = try await store.loadDecisionEvents(for: assetId)
        let backfillEvents = events.filter { $0.eventType == "backfill_fusion" }
        #expect(
            backfillEvents.count == decoded.count,
            "decision_events (eventType=backfill_fusion) count (\(backfillEvents.count)) must match decisionJSON window count (\(decoded.count))"
        )

        // 4) Per-window correspondence: every decoded artifact decision
        //    should have a matching decision_events row keyed by windowId.
        //    This is the strongest single check that the artifact was
        //    built from the same fusionDecisionResults that fed Step 16.
        let eventWindowIds = Set(backfillEvents.map(\.windowId))
        for decision in decoded {
            #expect(
                eventWindowIds.contains(decision.id),
                "Artifact decision \(decision.id) has no matching decision_events row"
            )
        }

        // 5) inputArtifactRefs must be a JSON array of the per-asset
        //    fusion-window ids; this is what downstream consumers (replay,
        //    NARL) use to walk back from a decision to its inputs.
        let refs = try decodeStringArray(artifact.inputArtifactRefs)
        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(
            refs.count == windows.count,
            "inputArtifactRefs count (\(refs.count)) must match fusion window count (\(windows.count))"
        )
        for window in windows {
            #expect(refs.contains(window.id), "inputArtifactRefs must include fusion window \(window.id)")
        }

        // 6) decisionCohortJSON must be non-empty and decodable as a
        //    DecisionCohort. The encoded form is the canonical cohort the
        //    decision_events rows for this asset were tagged with.
        let cohortData = try #require(artifact.decisionCohortJSON.data(using: .utf8))
        let cohort = try JSONDecoder().decode(DecisionCohort.self, from: cohortData)
        #expect(!cohort.fusionHash.isEmpty, "Persisted cohort must carry a fusionHash")
    }

    @Test("re-running runBackfill replaces the artifact (idempotent upsert) — still exactly one row per asset")
    func runBackfillUpsertsArtifactOnRerun() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-artifact-upsert"
        try await store.insertAsset(makeArtifactTestAsset(id: assetId))

        let service = makeArtifactService(store: store)
        let chunks = makeArtifactAdChunks(assetId: assetId)

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-artifact-upsert",
            episodeDuration: 90.0
        )
        let first = try await store.loadDecisionResultArtifact(for: assetId)

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-artifact-upsert",
            episodeDuration: 90.0
        )
        let second = try await store.loadDecisionResultArtifact(for: assetId)

        // The UNIQUE(analysisAssetId) + INSERT OR REPLACE contract means
        // there is at most one row, and the second run wins.
        let firstArtifact = try #require(first, "First run must persist an artifact")
        let secondArtifact = try #require(second, "Second run must persist an artifact")
        #expect(secondArtifact.createdAt >= firstArtifact.createdAt,
                "Re-run artifact createdAt must not regress")

        // Same inputs → same per-window decision content (boundaries and
        // confidence). Window UUIDs are regenerated per run by the boundary
        // refiner, so id is excluded from this structural comparison; the
        // artifact id and createdAt are also expected to differ.
        let firstDecisions = try decodePersistedDecisions(firstArtifact.decisionJSON)
        let secondDecisions = try decodePersistedDecisions(secondArtifact.decisionJSON)
        #expect(firstDecisions.count == secondDecisions.count,
                "Re-run must produce same number of decisions")
        for (a, b) in zip(firstDecisions, secondDecisions) {
            #expect(a.analysisAssetId == b.analysisAssetId)
            #expect(a.startTime == b.startTime)
            #expect(a.endTime == b.endTime)
            #expect(a.skipConfidence == b.skipConfidence)
            #expect(a.eligibilityGate == b.eligibilityGate)
        }
    }
}

// MARK: - Decoding helpers

private func decodePersistedDecisions(_ json: String) throws -> [PersistedDecisionResult] {
    let data = try #require(json.data(using: .utf8))
    return try JSONDecoder().decode([PersistedDecisionResult].self, from: data)
}

private func decodeStringArray(_ json: String) throws -> [String] {
    let data = try #require(json.data(using: .utf8))
    return try JSONDecoder().decode([String].self, from: data)
}
