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

    // MARK: - playhead-4my.10.2: cohort-bound provenance via Swift filter
    //
    // playhead-4my.10.2 originally landed a SQL-backed
    // `loadTrainingExamples(forAsset:scanCohortJSON:)` overload, but cycle-2
    // review flagged that as a production-source change in a tests-only
    // bead. The overload was deleted; the contract it pinned —
    // "downstream consumers can derive a cohort-bound subset from the
    // unfiltered load" — is now exercised by filtering in Swift via
    // `.filter { $0.scanCohortJSON == cohort }`. Each test below is the
    // same characterization at the consumer layer; the contract
    // (byte-exact, asset-scoped, ordering-preserving) is unchanged.

    @Test("filter-by-cohort: rows partition cleanly across two cohorts on the same asset")
    func loadFiltersByScanCohort() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-cohort-filter")
        try await store.insertAsset(asset)

        // Two examples on the same asset under two distinct scan cohorts.
        // Provenance filtering is what lets a downstream consumer say
        // "give me only the rows produced under cohort X" — essential
        // for cross-cohort comparisons and cohort-bound exports.
        let cohortA = makeCohortJSON(promptLabel: "cohort-A")
        let cohortB = makeCohortJSON(promptLabel: "cohort-B")
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

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        let filteredA = loaded.filter { $0.scanCohortJSON == cohortA }
        let filteredB = loaded.filter { $0.scanCohortJSON == cohortB }
        #expect(filteredA.map { $0.id } == ["te-A"])
        #expect(filteredB.map { $0.id } == ["te-B"])
    }

    @Test("filter-by-cohort: returns empty when no row matches a well-formed but unused cohort")
    func loadFilterEmptyForUnknownCohort() async throws {
        // M2 fix: BOTH the seeded row and the query string are well-formed
        // canonical ScanCohort JSON — they just differ on `promptLabel`.
        // The empty result therefore proves the filter discriminates by
        // cohort identity, not by malformed-JSON happenstance. The
        // unfiltered load is asserted to contain the row, so we know
        // "empty result" really means "no match" rather than "no rows".
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-unknown-cohort")
        try await store.insertAsset(asset)

        let storedCohort = makeCohortJSON(promptLabel: "stored")
        let queriedCohort = makeCohortJSON(promptLabel: "queried")
        try await store.createTrainingExample(
            TrainingExample(
                id: "te-only", analysisAssetId: asset.id,
                startAtomOrdinal: 0, endAtomOrdinal: 10,
                transcriptVersion: "tv", startTime: 0, endTime: 5,
                textSnapshotHash: "h", textSnapshot: nil,
                bucket: .positive, commercialIntent: "paid",
                ownership: "thirdParty", evidenceSources: [],
                fmCertainty: 0, classifierConfidence: 0,
                userAction: nil, eligibilityGate: nil,
                scanCohortJSON: storedCohort, decisionCohortJSON: "{}",
                transcriptQuality: "good", createdAt: 1
            )
        )

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        let filteredByQueried = loaded.filter { $0.scanCohortJSON == queriedCohort }
        let filteredByStored = loaded.filter { $0.scanCohortJSON == storedCohort }
        #expect(filteredByQueried.isEmpty,
                "filter must reject a well-formed but distinct cohort")
        #expect(filteredByStored.map(\.id) == ["te-only"],
                "same store filtered by stored cohort must surface the row")
    }

    @Test("filter-by-cohort: multi-asset multi-cohort 2x2 matrix isolates each cell")
    func loadFilterMultiAssetMultiCohortMatrix() async throws {
        // Pin the (asset × cohort) matrix: two assets × two cohorts = 4 rows.
        // Both halves of the contract are exercised:
        //   - asset scoping: filtering on assetA's load never returns assetB's
        //     row (and vice versa) — this subsumes the standalone
        //     `loadFilterIsAssetScoped` test that was deleted.
        //   - cohort scoping: filtering by cohort within an asset returns
        //     exactly the (asset, cohort) cell.
        // (playhead-4my.10.2)
        let store = try await makeTestStore()
        let assetA = makeAsset(id: "asset-matrix-A")
        let assetB = makeAsset(id: "asset-matrix-B")
        try await store.insertAsset(assetA)
        try await store.insertAsset(assetB)

        let cohortX = makeCohortJSON(promptLabel: "matrix-X")
        let cohortY = makeCohortJSON(promptLabel: "matrix-Y")

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

        // Per-asset loads must NOT see the sibling asset's rows — the asset
        // scope is enforced by the underlying load, not by the in-Swift
        // filter. (Symmetric assertion: both A and B confined.)
        let loadedA = try await store.loadTrainingExamples(forAsset: assetA.id)
        let loadedB = try await store.loadTrainingExamples(forAsset: assetB.id)
        #expect(Set(loadedA.map(\.id)) == ["te-AX", "te-AY"])
        #expect(Set(loadedB.map(\.id)) == ["te-BX", "te-BY"])

        // Each (asset, cohort) cell of the matrix returns exactly its row.
        let ax = loadedA.filter { $0.scanCohortJSON == cohortX }
        let ay = loadedA.filter { $0.scanCohortJSON == cohortY }
        let bx = loadedB.filter { $0.scanCohortJSON == cohortX }
        let by = loadedB.filter { $0.scanCohortJSON == cohortY }
        #expect(ax.map(\.id) == ["te-AX"])
        #expect(ay.map(\.id) == ["te-AY"])
        #expect(bx.map(\.id) == ["te-BX"])
        #expect(by.map(\.id) == ["te-BY"])
    }

    @Test("filter-by-cohort: empty-string only matches literal empty-cohort rows")
    func loadFilterEmptyStringSemantics() async throws {
        // Document the empty-string filter behavior: it does NOT mean
        // "no filter". An empty string matches rows whose stored cohort
        // is literally the empty string (a degenerate state that can't
        // occur with `ScanCohort.productionJSON()` but might appear in
        // a fixture). Locking this in keeps the byte-exact-equality
        // contract honest.
        //
        // Two phases:
        //   (1) mixed corpus: empty cohort + real cohort coexist; ""
        //       picks only the empty row.
        //   (2) negative case (L1): with NO empty-cohort rows, "" yields
        //       empty — proves "" doesn't fall through to "all rows".
        // (playhead-4my.10.2)
        let realCohort = makeCohortJSON(promptLabel: "real")

        // Phase 1: mixed corpus.
        do {
            let store = try await makeTestStore()
            let asset = makeAsset(id: "asset-empty-cohort-mixed")
            try await store.insertAsset(asset)

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

            let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
            let emptyMatch = loaded.filter { $0.scanCohortJSON == "" }
            #expect(emptyMatch.map(\.id) == ["te-empty"],
                    "empty-string filter matches only the literal-empty row")
            let realMatch = loaded.filter { $0.scanCohortJSON == realCohort }
            #expect(realMatch.map(\.id) == ["te-real"])
        }

        // Phase 2: corpus with NO empty-cohort rows. Empty filter must
        // return empty — definitively NOT "match everything".
        do {
            let store = try await makeTestStore()
            let asset = makeAsset(id: "asset-empty-cohort-only-real")
            try await store.insertAsset(asset)

            try await store.createTrainingExample(
                TrainingExample(
                    id: "te-only-real", analysisAssetId: asset.id,
                    startAtomOrdinal: 0, endAtomOrdinal: 10,
                    transcriptVersion: "tv", startTime: 0, endTime: 5,
                    textSnapshotHash: "h", textSnapshot: nil,
                    bucket: .positive, commercialIntent: "paid",
                    ownership: "thirdParty", evidenceSources: [],
                    fmCertainty: 0, classifierConfidence: 0,
                    userAction: nil, eligibilityGate: nil,
                    scanCohortJSON: realCohort, decisionCohortJSON: "{}",
                    transcriptQuality: "good", createdAt: 1
                )
            )

            let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
            let emptyMatch = loaded.filter { $0.scanCohortJSON == "" }
            #expect(emptyMatch.isEmpty,
                    "empty-string filter does NOT degrade to 'no filter'")
        }
    }

    @Test("byte-exact cohort matching: same content, unsorted keys is NOT equal")
    func loadFilterIsByteExactNotCanonicalized() async throws {
        // The cohort-equality contract is documented as byte-exact on the
        // stored JSON string — no canonicalization is attempted at the
        // filter layer because the writer always uses
        // `ScanCohort.productionJSON()` (sorted-keys). This test pins
        // that contract: a string with the same KEYS+VALUES but in a
        // different (unsorted) key order must NOT be considered equal.
        // After H1 the filter is `==`, which is intrinsically byte-exact
        // — but locking the test in still pins the consumer-layer contract
        // so a future "be helpful and canonicalize" regression fails loudly.
        // (playhead-4my.10.2)
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-byte-exact")
        try await store.insertAsset(asset)

        let sortedCohort = makeCohortJSON(promptLabel: "byte-exact")
        // Hand-roll an unsorted-keys re-encoding of the same content.
        // Decoding sortedCohort and re-encoding without `.sortedKeys`
        // gives a permutation Swift's encoder happens to emit; on Apple
        // platforms that's typically stored-property declaration order,
        // which differs from sorted-keys. The exact ordering is not
        // important — only that the resulting string is non-empty,
        // semantically equal, and byte-different from the sorted form.
        let decoder = JSONDecoder()
        let cohort = try decoder.decode(ScanCohort.self,
                                        from: Data(sortedCohort.utf8))
        let encoder = JSONEncoder()
        // Intentionally NO .sortedKeys here.
        let unsortedData = try encoder.encode(cohort)
        let unsortedCohort = String(data: unsortedData, encoding: .utf8) ?? ""
        try #require(!unsortedCohort.isEmpty)
        // Pre-condition: the two strings differ byte-wise. If they don't
        // (encoder happens to emit sorted), the test silently degrades —
        // require it to ensure we're really exercising the non-canonical
        // path.
        try #require(unsortedCohort != sortedCohort,
                     "encoder without .sortedKeys must produce a byte-different string")

        try await store.createTrainingExample(
            TrainingExample(
                id: "te-sorted", analysisAssetId: asset.id,
                startAtomOrdinal: 0, endAtomOrdinal: 10,
                transcriptVersion: "tv", startTime: 0, endTime: 5,
                textSnapshotHash: "h", textSnapshot: nil,
                bucket: .positive, commercialIntent: "paid",
                ownership: "thirdParty", evidenceSources: [],
                fmCertainty: 0, classifierConfidence: 0,
                userAction: nil, eligibilityGate: nil,
                scanCohortJSON: sortedCohort, decisionCohortJSON: "{}",
                transcriptQuality: "good", createdAt: 1
            )
        )

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        // Byte-exact: the unsorted-keys query is NOT a match.
        let unsortedMatch = loaded.filter { $0.scanCohortJSON == unsortedCohort }
        #expect(unsortedMatch.isEmpty,
                "filter must NOT canonicalize JSON before comparing")
        // Sanity: the sorted form does match — so the row is queryable.
        let sortedMatch = loaded.filter { $0.scanCohortJSON == sortedCohort }
        #expect(sortedMatch.map(\.id) == ["te-sorted"])
    }

    @Test("filter-by-cohort preserves createdAt ordering within the cohort")
    func loadFilterPreservesCreatedAtOrdering() async throws {
        // Order preservation is a property of the underlying load
        // (`ORDER BY createdAt ASC, rowid ASC`); after H1 the cohort
        // filter is a stable Swift `.filter`, so ordering carries
        // through. Pin both halves: the unfiltered load is ordered
        // by createdAt, AND that order is preserved through the
        // cohort filter. Two rows in the same (asset, cohort) cell
        // with different createdAt values, inserted out of order,
        // and cross-cohort interleaved so the filter actually exercises
        // pruning — not just identity.
        // (playhead-4my.10.2)
        let store = try await makeTestStore()
        let asset = makeAsset(id: "asset-order-cohort")
        try await store.insertAsset(asset)

        let cohort = makeCohortJSON(promptLabel: "ordered")
        let otherCohort = makeCohortJSON(promptLabel: "other")

        func row(id: String, cohort: String, createdAt: Double) -> TrainingExample {
            TrainingExample(
                id: id, analysisAssetId: asset.id,
                startAtomOrdinal: 0, endAtomOrdinal: 10,
                transcriptVersion: "tv", startTime: 0, endTime: 5,
                textSnapshotHash: "h-\(id)", textSnapshot: nil,
                bucket: .positive, commercialIntent: "paid",
                ownership: "thirdParty", evidenceSources: [],
                fmCertainty: 0, classifierConfidence: 0,
                userAction: nil, eligibilityGate: nil,
                scanCohortJSON: cohort, decisionCohortJSON: "{}",
                transcriptQuality: "good", createdAt: createdAt
            )
        }
        // Insert out of write-order to prove load orders by createdAt,
        // not by insertion order. Interleave a row from another cohort
        // between the two target rows so the cohort filter has to
        // actually exclude something.
        try await store.createTrainingExample(row(id: "te-late", cohort: cohort, createdAt: 300))
        try await store.createTrainingExample(row(id: "te-other", cohort: otherCohort, createdAt: 200))
        try await store.createTrainingExample(row(id: "te-early", cohort: cohort, createdAt: 100))

        let loaded = try await store.loadTrainingExamples(forAsset: asset.id)
        // Underlying load is createdAt-ascending across cohorts.
        #expect(loaded.map(\.id) == ["te-early", "te-other", "te-late"])
        // Filter preserves that ordering and excludes the other cohort.
        let filtered = loaded.filter { $0.scanCohortJSON == cohort }
        #expect(filtered.map(\.id) == ["te-early", "te-late"])
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
