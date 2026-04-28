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

    @Test("schema version is at least 17 after migration")
    func schemaVersionBumpedToSeventeen() async throws {
        // cycle-2 M-A: v17 rebuilds `training_examples` with the post-fix
        // shape (FK RESTRICT, nullable decisionCohortJSON) so any DB that
        // already opened at v16 picks up the corrected schema.
        let store = try await makeTestStore()
        let version = try await store.schemaVersion() ?? 0
        #expect(version >= 17)
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

    /// cycle-3 L4: focused round-trip for `decisionCohortJSON == nil`.
    /// The cycle-1 fix made the column nullable (it was previously
    /// `NOT NULL`, which prevented the materializer from honestly
    /// representing "no decision overlapped this scan"). The pre-fix
    /// shape gave a NOT NULL constraint failure on the INSERT path.
    /// Pin nullability explicitly so a future schema reverter can't
    /// quietly tighten it back to NOT NULL without breaking a test.
    @Test("L4: decisionCohortJSON round-trips as nil when nil")
    func decisionCohortJSONRoundTripsAsNil() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-decision-null")
        try await store.insertAsset(asset)

        let example = TrainingExample(
            id: "te-decision-null",
            analysisAssetId: asset.id,
            startAtomOrdinal: 0,
            endAtomOrdinal: 10,
            transcriptVersion: "tv-1",
            startTime: 0,
            endTime: 5,
            textSnapshotHash: "h-1",
            textSnapshot: nil,
            bucket: .negative,
            commercialIntent: "organic",
            ownership: "unknown",
            evidenceSources: [],
            fmCertainty: 0,
            classifierConfidence: 0,
            userAction: nil,
            eligibilityGate: nil,
            scanCohortJSON: "{}",
            // The whole point of this test: nil must persist + load as nil.
            decisionCohortJSON: nil,
            transcriptQuality: "good",
            createdAt: 1_700_000_000
        )
        try await store.createTrainingExample(example)

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        try #require(loaded.count == 1)
        #expect(loaded[0].decisionCohortJSON == nil,
                "decisionCohortJSON must round-trip as nil, not collapse to empty string or default")
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

    // MARK: - playhead-4my.10.2 gaps

    /// Two canonically-encoded cohorts (sorted-keys JSON) that differ only
    /// in `promptLabel`. Using the real `ScanCohort` encoder rather than
    /// hand-rolled `{"label":"…"}` strings exercises the actual production
    /// shape — same path the materializer takes when stamping cohort onto
    /// a row. (playhead-4my.10.2)
    private static func cohortJSON(promptLabel: String) -> String {
        let cohort = ScanCohort(
            promptLabel: promptLabel,
            promptHash: "phase3-prompt-2026-04-06",
            schemaHash: "phase3-schema-2026-04-06",
            scanPlanHash: "phase3-plan-2026-04-06",
            normalizationHash: "phase3-norm-2026-04-06",
            osBuild: "26.0.0",
            locale: "en_US",
            appBuild: "1"
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = (try? encoder.encode(cohort)) ?? Data()
        return String(data: data, encoding: .utf8) ?? "{}"
    }

    @Test("loadTrainingExamples(forAsset:scanCohortJSON:) filters by cohort")
    func loadFiltersByScanCohort() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-cohort-filter")
        try await store.insertAsset(asset)

        // Two examples on the same asset under two distinct scan cohorts.
        // Provenance filtering is what lets a downstream consumer say
        // "give me only the rows produced under cohort X" — essential
        // for cross-cohort comparisons and cohort-bound exports.
        let cohortA = Self.cohortJSON(promptLabel: "cohort-A")
        let cohortB = Self.cohortJSON(promptLabel: "cohort-B")
        let exampleA = TrainingExample(
            id: "te-A", analysisAssetId: asset.id,
            startAtomOrdinal: 0, endAtomOrdinal: 10,
            transcriptVersion: "tv", startTime: 0, endTime: 5,
            textSnapshotHash: "hA", textSnapshot: nil,
            bucket: .positive, commercialIntent: "paid",
            ownership: "thirdParty", evidenceSources: ["fm"],
            fmCertainty: 0.9, classifierConfidence: 0.8,
            userAction: nil, eligibilityGate: nil,
            scanCohortJSON: cohortA, decisionCohortJSON: "{}",
            transcriptQuality: "good", createdAt: 1
        )
        let exampleB = TrainingExample(
            id: "te-B", analysisAssetId: asset.id,
            startAtomOrdinal: 11, endAtomOrdinal: 20,
            transcriptVersion: "tv", startTime: 5, endTime: 10,
            textSnapshotHash: "hB", textSnapshot: nil,
            bucket: .negative, commercialIntent: "organic",
            ownership: "unknown", evidenceSources: [],
            fmCertainty: 0.0, classifierConfidence: 0.0,
            userAction: nil, eligibilityGate: nil,
            scanCohortJSON: cohortB, decisionCohortJSON: "{}",
            transcriptQuality: "good", createdAt: 2
        )
        try await store.createTrainingExamples([exampleA, exampleB])

        let filteredA = try await store.loadTrainingExamples(
            forAsset: asset.id, scanCohortJSON: cohortA
        )
        let filteredB = try await store.loadTrainingExamples(
            forAsset: asset.id, scanCohortJSON: cohortB
        )
        #expect(filteredA.map { $0.id } == ["te-A"])
        #expect(filteredB.map { $0.id } == ["te-B"])
    }

    @Test("loadTrainingExamples(forAsset:scanCohortJSON:) scopes to asset and cohort")
    func loadFilterIsAssetScoped() async throws {
        let store = try await makeTestStore()
        let assetA = makeAsset(id: "asset-scope-A")
        let assetB = makeAsset(id: "asset-scope-B")
        try await store.insertAsset(assetA)
        try await store.insertAsset(assetB)

        let cohort = Self.cohortJSON(promptLabel: "shared")
        let onA = TrainingExample(
            id: "te-onA", analysisAssetId: assetA.id,
            startAtomOrdinal: 0, endAtomOrdinal: 10,
            transcriptVersion: "tv", startTime: 0, endTime: 5,
            textSnapshotHash: "h", textSnapshot: nil,
            bucket: .positive, commercialIntent: "paid",
            ownership: "thirdParty", evidenceSources: [],
            fmCertainty: 0, classifierConfidence: 0,
            userAction: nil, eligibilityGate: nil,
            scanCohortJSON: cohort, decisionCohortJSON: "{}",
            transcriptQuality: "good", createdAt: 1
        )
        let onB = TrainingExample(
            id: "te-onB", analysisAssetId: assetB.id,
            startAtomOrdinal: 0, endAtomOrdinal: 10,
            transcriptVersion: "tv", startTime: 0, endTime: 5,
            textSnapshotHash: "h", textSnapshot: nil,
            bucket: .positive, commercialIntent: "paid",
            ownership: "thirdParty", evidenceSources: [],
            fmCertainty: 0, classifierConfidence: 0,
            userAction: nil, eligibilityGate: nil,
            scanCohortJSON: cohort, decisionCohortJSON: "{}",
            transcriptQuality: "good", createdAt: 1
        )
        try await store.createTrainingExamples([onA, onB])

        let loadedA = try await store.loadTrainingExamples(
            forAsset: assetA.id, scanCohortJSON: cohort
        )
        #expect(loadedA.map { $0.id } == ["te-onA"])
    }

    @Test("loadTrainingExamples(forAsset:scanCohortJSON:) returns empty for unknown cohort")
    func loadFilterEmptyForUnknownCohort() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-unknown-cohort")
        try await store.insertAsset(asset)

        try await store.createTrainingExample(
            makeExample(id: "te-only", analysisAssetId: asset.id)
        )

        let loaded = try await store.loadTrainingExamples(
            forAsset: asset.id,
            scanCohortJSON: Self.cohortJSON(promptLabel: "never-existed")
        )
        #expect(loaded.isEmpty)
    }

    @Test("loadTrainingExamples(forAsset:scanCohortJSON:) — multi-asset multi-cohort 2x2 matrix")
    func loadFilterMultiAssetMultiCohortMatrix() async throws {
        // Pin the (asset × cohort) matrix: two assets × two cohorts = 4 rows.
        // The combined filter must isolate exactly ONE row per (asset, cohort)
        // pair — neither the asset nor the cohort filter alone is sufficient.
        // (playhead-4my.10.2)
        let store = try await makeTestStore()
        let assetA = makeAsset(id: "asset-matrix-A")
        let assetB = makeAsset(id: "asset-matrix-B")
        try await store.insertAsset(assetA)
        try await store.insertAsset(assetB)

        let cohortX = Self.cohortJSON(promptLabel: "matrix-X")
        let cohortY = Self.cohortJSON(promptLabel: "matrix-Y")

        func row(id: String, asset: String, cohort: String) -> TrainingExample {
            TrainingExample(
                id: id, analysisAssetId: asset,
                startAtomOrdinal: 0, endAtomOrdinal: 10,
                transcriptVersion: "tv", startTime: 0, endTime: 5,
                textSnapshotHash: "h-\(id)", textSnapshot: nil,
                bucket: .positive, commercialIntent: "paid",
                ownership: "thirdParty", evidenceSources: [],
                fmCertainty: 0, classifierConfidence: 0,
                userAction: nil, eligibilityGate: nil,
                scanCohortJSON: cohort, decisionCohortJSON: "{}",
                transcriptQuality: "good", createdAt: 1
            )
        }
        try await store.createTrainingExamples([
            row(id: "te-AX", asset: assetA.id, cohort: cohortX),
            row(id: "te-AY", asset: assetA.id, cohort: cohortY),
            row(id: "te-BX", asset: assetB.id, cohort: cohortX),
            row(id: "te-BY", asset: assetB.id, cohort: cohortY),
        ])

        // Each (asset, cohort) cell of the matrix must return exactly its row.
        let ax = try await store.loadTrainingExamples(
            forAsset: assetA.id, scanCohortJSON: cohortX
        )
        let ay = try await store.loadTrainingExamples(
            forAsset: assetA.id, scanCohortJSON: cohortY
        )
        let bx = try await store.loadTrainingExamples(
            forAsset: assetB.id, scanCohortJSON: cohortX
        )
        let by = try await store.loadTrainingExamples(
            forAsset: assetB.id, scanCohortJSON: cohortY
        )
        #expect(ax.map(\.id) == ["te-AX"])
        #expect(ay.map(\.id) == ["te-AY"])
        #expect(bx.map(\.id) == ["te-BX"])
        #expect(by.map(\.id) == ["te-BY"])
    }

    @Test("loadTrainingExamples(forAsset:scanCohortJSON:) empty-string only matches literal empty cohort")
    func loadFilterEmptyStringSemantics() async throws {
        // Document the helper's empty-string behavior: it does NOT mean
        // "no filter". An empty string matches rows whose stored cohort
        // is literally the empty string (a degenerate state that can't
        // occur with `ScanCohort.productionJSON()` but might appear in
        // a fixture). Locking this in keeps the byte-exact-equality
        // contract honest. (playhead-4my.10.2)
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-empty-cohort")
        try await store.insertAsset(asset)

        let realCohort = Self.cohortJSON(promptLabel: "real")
        let withRealCohort = TrainingExample(
            id: "te-real", analysisAssetId: asset.id,
            startAtomOrdinal: 0, endAtomOrdinal: 10,
            transcriptVersion: "tv", startTime: 0, endTime: 5,
            textSnapshotHash: "h-real", textSnapshot: nil,
            bucket: .positive, commercialIntent: "paid",
            ownership: "thirdParty", evidenceSources: [],
            fmCertainty: 0, classifierConfidence: 0,
            userAction: nil, eligibilityGate: nil,
            scanCohortJSON: realCohort, decisionCohortJSON: "{}",
            transcriptQuality: "good", createdAt: 1
        )
        let withEmptyCohort = TrainingExample(
            id: "te-empty", analysisAssetId: asset.id,
            startAtomOrdinal: 11, endAtomOrdinal: 20,
            transcriptVersion: "tv", startTime: 5, endTime: 10,
            textSnapshotHash: "h-empty", textSnapshot: nil,
            bucket: .negative, commercialIntent: "organic",
            ownership: "unknown", evidenceSources: [],
            fmCertainty: 0, classifierConfidence: 0,
            userAction: nil, eligibilityGate: nil,
            scanCohortJSON: "", decisionCohortJSON: "{}",
            transcriptQuality: "good", createdAt: 2
        )
        try await store.createTrainingExamples([withRealCohort, withEmptyCohort])

        // Empty string matches the literal-empty row only — not "all rows".
        let emptyMatch = try await store.loadTrainingExamples(
            forAsset: asset.id, scanCohortJSON: ""
        )
        #expect(emptyMatch.map(\.id) == ["te-empty"])
        // And the real cohort still finds only its row.
        let realMatch = try await store.loadTrainingExamples(
            forAsset: asset.id, scanCohortJSON: realCohort
        )
        #expect(realMatch.map(\.id) == ["te-real"])
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
