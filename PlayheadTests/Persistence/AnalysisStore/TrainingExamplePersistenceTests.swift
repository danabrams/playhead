// TrainingExamplePersistenceTests.swift
// playhead-4my.10.1: AnalysisStore CRUD for the `training_examples` table.
//
// We do not mock AnalysisStore — these run against a real temp-dir SQLite
// store. The migration adds a v16 schema bump, and the store exposes:
//   - createTrainingExample(_:)              — single insert
//   - createTrainingExamples(_:)             — batch insert
//   - loadTrainingExamples(forAsset:)        — ordered fetch by createdAt
//   - replaceTrainingExamples(forAsset:_:)   — per-row id-keyed upsert (for
//                                               idempotent re-materialization
//                                               on repeat backfills). Per
//                                               the cohort-survival contract,
//                                               this does NOT wipe prior rows
//                                               for the asset — only rows
//                                               whose `id` is in the supplied
//                                               batch are overwritten.

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisStore.training_examples — playhead-4my.10.1")
struct TrainingExamplePersistenceTests {

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

    private func makeExample(
        id: String,
        analysisAssetId: String,
        bucket: TrainingExampleBucket = .positive,
        createdAt: Double = 1_700_000_000.0,
        textSnapshot: String? = nil,
        userAction: String? = nil,
        eligibilityGate: String? = nil
    ) -> TrainingExample {
        TrainingExample(
            id: id,
            analysisAssetId: analysisAssetId,
            startAtomOrdinal: 100,
            endAtomOrdinal: 200,
            transcriptVersion: "tv-1",
            startTime: 60.0,
            endTime: 120.0,
            textSnapshotHash: "h-\(id)",
            textSnapshot: textSnapshot,
            bucket: bucket,
            commercialIntent: "paid",
            ownership: "thirdParty",
            evidenceSources: ["fm", "lexical"],
            fmCertainty: 0.9,
            classifierConfidence: 0.7,
            userAction: userAction,
            eligibilityGate: eligibilityGate,
            scanCohortJSON: "{\"prompt\":\"v1\"}",
            decisionCohortJSON: "{\"fusion\":\"v1\"}",
            transcriptQuality: "good",
            createdAt: createdAt
        )
    }

    @Test("schema version is at least 16 after migration")
    func schemaVersionBumpedToSixteen() async throws {
        let store = try await makeTestStore()
        let version = try await store.schemaVersion() ?? 0
        #expect(version >= 16)
    }

    @Test("training_examples table exists after migration")
    func trainingExamplesTableExists() async throws {
        let dir = try makeTempDir()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        _ = store // silence unused warning
        let exists = try probeTableExists(in: dir, table: "training_examples")
        #expect(exists)
    }

    @Test("insert + load round-trip preserves all fields")
    func insertAndLoadRoundTrip() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-rt-1")
        try await store.insertAsset(asset)

        let example = makeExample(
            id: "te-rt-1",
            analysisAssetId: asset.id,
            textSnapshot: "this is the captured text",
            userAction: "skipped",
            eligibilityGate: "eligible"
        )
        try await store.createTrainingExample(example)

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        try #require(loaded.count == 1)
        #expect(loaded[0] == example)
    }

    @Test("nullable fields round-trip as nil when absent")
    func nullableFieldsRoundTripAsNil() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-rt-2")
        try await store.insertAsset(asset)

        let example = makeExample(
            id: "te-rt-2",
            analysisAssetId: asset.id,
            textSnapshot: nil,
            userAction: nil,
            eligibilityGate: nil
        )
        try await store.createTrainingExample(example)

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        try #require(loaded.count == 1)
        #expect(loaded[0].textSnapshot == nil)
        #expect(loaded[0].userAction == nil)
        #expect(loaded[0].eligibilityGate == nil)
    }

    @Test("loadTrainingExamples returns rows ordered by createdAt ascending")
    func loadOrderedByCreatedAt() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-order")
        try await store.insertAsset(asset)

        let third = makeExample(id: "te-c", analysisAssetId: asset.id, createdAt: 300)
        let first = makeExample(id: "te-a", analysisAssetId: asset.id, createdAt: 100)
        let second = makeExample(id: "te-b", analysisAssetId: asset.id, createdAt: 200)
        // Intentionally write out of order.
        try await store.createTrainingExample(third)
        try await store.createTrainingExample(first)
        try await store.createTrainingExample(second)

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        #expect(loaded.map { $0.id } == ["te-a", "te-b", "te-c"])
    }

    @Test("loadTrainingExamples scopes to a single asset")
    func loadScopesToSingleAsset() async throws {
        let store = try await makeTestStore()
        let assetA = makeAsset(id: "asset-A")
        let assetB = makeAsset(id: "asset-B")
        try await store.insertAsset(assetA)
        try await store.insertAsset(assetB)

        try await store.createTrainingExample(
            makeExample(id: "te-A1", analysisAssetId: assetA.id)
        )
        try await store.createTrainingExample(
            makeExample(id: "te-B1", analysisAssetId: assetB.id)
        )

        let loadedA = try await store.loadTrainingExamples(forAsset: assetA.id)
        let loadedB = try await store.loadTrainingExamples(forAsset: assetB.id)
        #expect(loadedA.map { $0.id } == ["te-A1"])
        #expect(loadedB.map { $0.id } == ["te-B1"])
    }

    @Test("replaceTrainingExamples upserts rows by id without wiping prior rows for the asset")
    func replaceTrainingExamplesIsIdempotentByRowId() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-replace")
        try await store.insertAsset(asset)

        // Pre-existing rows from a prior materialization (e.g. an older
        // cohort). These must SURVIVE a subsequent `replaceTrainingExamples`
        // call that doesn't touch their ids — that's the cohort-durability
        // guarantee the bead is about.
        let first = makeExample(id: "te-old-1", analysisAssetId: asset.id, bucket: .positive)
        let second = makeExample(id: "te-old-2", analysisAssetId: asset.id, bucket: .negative)
        try await store.createTrainingExamples([first, second])

        // A new run upserts a different id. Prior rows stay; the new row is
        // added. If the same id reappears in a later batch, it overwrites
        // (because each TrainingExample.id is deterministic from scan id,
        // re-materializing the same scan produces the same id).
        let replacement = makeExample(id: "te-new-1", analysisAssetId: asset.id, bucket: .disagreement)
        try await store.replaceTrainingExamples(
            forAsset: asset.id,
            with: [replacement]
        )

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        let ids = Set(loaded.map(\.id))
        #expect(ids == ["te-old-1", "te-old-2", "te-new-1"])
    }

    @Test("replaceTrainingExamples preserves prior cohort rows when the new batch is empty")
    func replaceTrainingExamplesEmptyBatchIsNoop() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-empty-batch")
        try await store.insertAsset(asset)

        let prior = makeExample(id: "te-prior-1", analysisAssetId: asset.id, bucket: .positive)
        try await store.createTrainingExamples([prior])

        // Empty batch (e.g. a cohort flip wiped the spine, materializer
        // produced 0 examples). Must NOT wipe the prior cohort's row.
        try await store.replaceTrainingExamples(
            forAsset: asset.id,
            with: []
        )

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        #expect(loaded.map(\.id) == ["te-prior-1"])
    }

    @Test("replaceTrainingExamples overwrites a prior row with the same id")
    func replaceTrainingExamplesOverwritesById() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-overwrite")
        try await store.insertAsset(asset)

        let original = makeExample(
            id: "te-shared-id", analysisAssetId: asset.id,
            bucket: .uncertain, createdAt: 100
        )
        try await store.createTrainingExample(original)

        // Same id, different content (e.g. signals improved on re-materialization).
        let updated = makeExample(
            id: "te-shared-id", analysisAssetId: asset.id,
            bucket: .positive, createdAt: 200
        )
        try await store.replaceTrainingExamples(
            forAsset: asset.id,
            with: [updated]
        )

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        #expect(loaded.count == 1)
        #expect(loaded.first?.bucket == .positive)
        #expect(loaded.first?.createdAt == 200)
    }

    @Test("M4: training_examples FK is ON DELETE RESTRICT — cannot delete an asset with examples")
    func deletingAssetWithTrainingExamplesIsBlocked() async throws {
        // The whole point of materialized training data is to outlast
        // upstream cohort prunes; ON DELETE RESTRICT on the analysisAssetId
        // FK enforces that contract at the storage layer. Deleting an asset
        // that still has training examples should raise — pre-fix the FK was
        // ON DELETE CASCADE and the corpus would silently disappear.
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-fk-restrict")
        try await store.insertAsset(asset)

        let example = makeExample(id: "te-fk-1", analysisAssetId: asset.id)
        try await store.createTrainingExample(example)

        // Attempting to delete the parent must throw (FK constraint).
        var didThrow = false
        do {
            try await store.deleteAsset(id: asset.id)
        } catch {
            didThrow = true
        }
        #expect(didThrow, "delete should fail with FK constraint")

        // The training example must still be there.
        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        #expect(loaded.map(\.id) == ["te-fk-1"])
    }

    @Test("evidenceSources round-trips as ordered array")
    func evidenceSourcesRoundTrips() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-ev")
        try await store.insertAsset(asset)

        let example = TrainingExample(
            id: "te-ev",
            analysisAssetId: asset.id,
            startAtomOrdinal: 0,
            endAtomOrdinal: 10,
            transcriptVersion: "tv",
            startTime: 0,
            endTime: 5,
            textSnapshotHash: "h",
            textSnapshot: nil,
            bucket: .uncertain,
            commercialIntent: "unknown",
            ownership: "unknown",
            evidenceSources: ["fm", "lexical", "metadata"],
            fmCertainty: 0,
            classifierConfidence: 0,
            userAction: nil,
            eligibilityGate: nil,
            scanCohortJSON: "{}",
            decisionCohortJSON: "{}",
            transcriptQuality: "degraded",
            createdAt: 1
        )
        try await store.createTrainingExample(example)

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        #expect(loaded.first?.evidenceSources == ["fm", "lexical", "metadata"])
    }
}
