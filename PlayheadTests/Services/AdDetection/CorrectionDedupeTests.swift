// CorrectionDedupeTests.swift
// playhead-hygc.1.6: tests for the dedupe contract on `correction_events`.
//
// Acceptance criteria covered:
//   (a) two identical submissions → one row + audit count = 2
//   (b) tiny float jitter within identityKeyTimeToleranceSeconds → one row + audit
//   (c) jitter beyond tolerance → two distinct rows
//   (d) FP and FN for the same span do NOT collapse into each other
//   (e) corpus export distinct-by-default; .rawEvents preserves duplicates
//   (f) replay round-trip: insert, close store, reopen, re-insert same → 1 row
//   (g) fixture-backed: May 6 dogfood `asset_012 falseNegative-x4` collapses
//       to ONE under read-side `distinctSemanticCorrections`.
//
// Test framework: XCTest (matches the surrounding UserCorrectionStoreTests
// suite and per the project memo "xcodegen + xctestplan can only filter
// XCTest" so these tests participate in PlayheadFastTests selection).

import XCTest
import Foundation
import SQLite3
@testable import Playhead

#if DEBUG
final class CorrectionDedupeTests: XCTestCase {

    // MARK: - Fixture path resolution

    /// Walks up from the test file's `#filePath` to PlayheadTests/ and
    /// builds the path to the dogfood fixture loader, whose
    /// `#filePath`-defaulted helpers expect to be invoked from a sibling
    /// of `Fixtures/Dogfood/`.
    ///
    /// We can't just call `DogfoodAnalysisHealthFixtureLoader.load()`
    /// directly because Swift's `#filePath` default-argument resolution
    /// happens at the CALL site — passing this test file's path through
    /// the default would point the loader at a non-existent
    /// `PlayheadTests/Services/AdDetection/2026-05-06/` directory.
    static func dogfoodLoaderFilePath(testFilePath: String) -> String {
        // Walk up the path components until we find PlayheadTests/, then
        // descend into the loader's known location. Failing to find the
        // anchor returns a deliberately-invalid path so the loader's own
        // error path fires with a useful message.
        let url = URL(fileURLWithPath: testFilePath)
        var dir = url.deletingLastPathComponent()
        while dir.pathComponents.count > 1 {
            if dir.lastPathComponent == "PlayheadTests" {
                return dir
                    .appendingPathComponent("Fixtures")
                    .appendingPathComponent("Dogfood")
                    .appendingPathComponent("DogfoodAnalysisHealthFixtureLoader.swift")
                    .path
            }
            dir = dir.deletingLastPathComponent()
        }
        return "/__no_PlayheadTests_in_path__"
    }

    // MARK: - (a) Identical submissions collapse

    func testIdenticalSubmissionsCollapseAndAuditCount() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-a"))

        let scope = CorrectionScope.exactTimeSpan(
            assetId: "asset-a",
            startTime: 100.000,
            endTime: 200.000
        )
        // Distinct row ids on every submission so we exercise the
        // `ON CONFLICT(identity)` branch — not the legacy
        // `INSERT OR IGNORE` (same primary key) shortcut.
        let first = CorrectionEvent(
            id: UUID().uuidString,
            analysisAssetId: "asset-a",
            scope: scope.serialized,
            createdAt: 1_700_000_000.0,
            source: .falseNegative,
            correctionType: .falseNegative
        )
        let second = CorrectionEvent(
            id: UUID().uuidString,
            analysisAssetId: "asset-a",
            scope: scope.serialized,
            createdAt: 1_700_000_010.0,  // 10s later
            source: .falseNegative,
            correctionType: .falseNegative
        )
        try await correctionStore.record(first)
        try await correctionStore.record(second)

        let loaded = try await correctionStore.activeCorrections(for: "asset-a")
        XCTAssertEqual(loaded.count, 1, "Two identical submissions must collapse to one row")
        let row = try XCTUnwrap(loaded.first)
        XCTAssertEqual(row.submissionCount, 2,
            "Audit count must reflect cumulative submissions (got \(String(describing: row.submissionCount)))")
        // firstSeen pinned to the original; lastSeen advances.
        XCTAssertEqual(row.createdAt, 1_700_000_000.0, accuracy: 0.001,
            "createdAt must pin the first observation, not move forward on re-submit")
        XCTAssertEqual(row.lastSeenAt ?? -1, 1_700_000_010.0, accuracy: 0.001,
            "lastSeenAt must track the most recent submission")
    }

    // MARK: - (b) Jitter within tolerance collapses

    func testJitterWithinToleranceCollapses() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-b"))

        // The user "submitted the same correction" but the upstream
        // boundary times jittered by < tolerance. Identity must
        // collapse so the user does not pay 2× for one logical correction.
        let toleranceHalf = CorrectionScope.identityKeyTimeToleranceSeconds / 4.0
        let scope1 = CorrectionScope.exactTimeSpan(
            assetId: "asset-b",
            startTime: 50.000,
            endTime: 80.000
        )
        let scope2 = CorrectionScope.exactTimeSpan(
            assetId: "asset-b",
            startTime: 50.000 + toleranceHalf,  // ~25 ms shift
            endTime: 80.000 - toleranceHalf
        )
        // Sanity check: the two scopes have different `serialized`
        // forms but the same `normalizedIdentityKey`.
        XCTAssertNotEqual(scope1.serialized, scope2.serialized,
            "Pre-condition: serialized forms must differ at the wire layer")
        XCTAssertEqual(scope1.normalizedIdentityKey, scope2.normalizedIdentityKey,
            "Pre-condition: identity-key must match within tolerance")

        try await correctionStore.record(CorrectionEvent(
            id: UUID().uuidString,
            analysisAssetId: "asset-b",
            scope: scope1.serialized,
            createdAt: 100.0,
            source: .manualVeto,
            correctionType: .falsePositive
        ))
        try await correctionStore.record(CorrectionEvent(
            id: UUID().uuidString,
            analysisAssetId: "asset-b",
            scope: scope2.serialized,
            createdAt: 200.0,
            source: .manualVeto,
            correctionType: .falsePositive
        ))

        let loaded = try await correctionStore.activeCorrections(for: "asset-b")
        XCTAssertEqual(loaded.count, 1, "Jitter within tolerance must collapse")
        XCTAssertEqual(loaded.first?.submissionCount, 2)
    }

    // MARK: - (c) Jitter beyond tolerance → distinct rows

    func testJitterBeyondToleranceProducesDistinctRows() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-c"))

        // An edit that meaningfully widens the span past the tolerance
        // bucket is treated as a new correction — distinct identity,
        // distinct row, distinct audit history.
        let tolerance = CorrectionScope.identityKeyTimeToleranceSeconds
        let scope1 = CorrectionScope.exactTimeSpan(
            assetId: "asset-c",
            startTime: 50.000,
            endTime: 80.000
        )
        let scope2 = CorrectionScope.exactTimeSpan(
            assetId: "asset-c",
            startTime: 50.000,
            endTime: 80.000 + tolerance * 5.0  // 500ms widening
        )
        XCTAssertNotEqual(scope1.normalizedIdentityKey, scope2.normalizedIdentityKey,
            "Pre-condition: identity-key must differ beyond tolerance")

        try await correctionStore.record(CorrectionEvent(
            id: UUID().uuidString,
            analysisAssetId: "asset-c",
            scope: scope1.serialized,
            createdAt: 100.0,
            source: .manualVeto,
            correctionType: .falsePositive
        ))
        try await correctionStore.record(CorrectionEvent(
            id: UUID().uuidString,
            analysisAssetId: "asset-c",
            scope: scope2.serialized,
            createdAt: 200.0,
            source: .manualVeto,
            correctionType: .falsePositive
        ))

        let loaded = try await correctionStore.activeCorrections(for: "asset-c")
        XCTAssertEqual(loaded.count, 2,
            "Distinct identities (beyond tolerance) must NOT collapse")
        // Each row records its own first observation; neither has been resubmitted.
        XCTAssertEqual(Set(loaded.map { $0.submissionCount ?? 0 }), Set([1, 1]),
            "Each distinct identity gets its own audit history with count = 1")
    }

    // MARK: - (c.1) Tolerance boundary probe

    /// Pin the bucket-quantization canonicalization at and around the
    /// 0.1s threshold. The current implementation uses
    /// `(time / 0.1).rounded() * 0.1` (Swift default
    /// `.toNearestOrEven`) — i.e. a quantization grid, not a true
    /// "tolerance window." Values in the same 0.1s bucket collapse;
    /// values straddling a bucket boundary do NOT, even if they are
    /// less than the tolerance apart.
    ///
    /// Tests (b) and (c) only proved "small collapses" and
    /// "large does not". They did not pin behavior at the boundary
    /// itself. Add a boundary probe so a future change to the
    /// canonicalization strategy is a deliberate, observed delta
    /// rather than a silent regression.
    func testToleranceBoundaryCanonicalization() {
        let tol = CorrectionScope.identityKeyTimeToleranceSeconds
        XCTAssertEqual(tol, 0.1, accuracy: 1e-9, "Pre-condition: tolerance is 100ms")

        // Numeric canonicalization grid points, spanning a single
        // bucket and its neighbours. Pinned to specific outputs so a
        // future change to the rounding strategy (e.g. `.toNearestOrAwayFromZero`)
        // is caught here.
        let q800 = CorrectionScope.canonicalizeTimeForIdentityKey(80.000)
        let q804 = CorrectionScope.canonicalizeTimeForIdentityKey(80.040)
        let q806 = CorrectionScope.canonicalizeTimeForIdentityKey(80.060)
        let q810 = CorrectionScope.canonicalizeTimeForIdentityKey(80.100)
        let q814 = CorrectionScope.canonicalizeTimeForIdentityKey(80.140)
        XCTAssertEqual(q800, 80.0, accuracy: 1e-9, "80.000 lands in 80.0 bucket")
        XCTAssertEqual(q804, 80.0, accuracy: 1e-9, "80.040 quantizes down to 80.0 bucket")
        XCTAssertEqual(q806, 80.1, accuracy: 1e-9, "80.060 quantizes up to 80.1 bucket")
        XCTAssertEqual(q810, 80.1, accuracy: 1e-9, "80.100 lands exactly on the 80.1 bucket")
        XCTAssertEqual(q814, 80.1, accuracy: 1e-9, "80.140 quantizes down to 80.1 bucket")

        // Identity collapse vs split: build pairs that share a bucket
        // (must collapse) vs pairs that straddle a bucket boundary
        // (must split, even if they are arbitrarily close).
        func keysFor(end1: Double, end2: Double) -> (String, String) {
            let a = CorrectionScope.exactTimeSpan(
                assetId: "asset-bdy", startTime: 50.000, endTime: end1)
            let b = CorrectionScope.exactTimeSpan(
                assetId: "asset-bdy", startTime: 50.000, endTime: end2)
            return (a.normalizedIdentityKey, b.normalizedIdentityKey)
        }

        // Same bucket (80.0): two values both in [79.95, 80.05) rounding
        // window must collapse. Pick endpoints near the ends of the bucket.
        let (kSame1, kSame2) = keysFor(end1: 79.960, end2: 80.040)
        XCTAssertEqual(kSame1, kSame2,
            "Two times in the same 0.1s quantization bucket must produce the same identity key")

        // Cross bucket boundary by ~80ms (well under tolerance) — they
        // straddle the 80.05 midpoint so they fall in different buckets.
        // This pins the documented quantization-not-window semantics.
        let (kAcross1, kAcross2) = keysFor(end1: 80.040, end2: 80.120)
        XCTAssertNotEqual(kAcross1, kAcross2,
            "Two times that straddle a quantization bucket boundary produce distinct identities — quantization, not a true tolerance window")

        // Clearly beyond tolerance (3x): must split.
        let (kBeyond1, kBeyond2) = keysFor(end1: 80.000, end2: 80.300)
        XCTAssertNotEqual(kBeyond1, kBeyond2,
            "Times >3x the bucket apart must produce distinct identities")
    }

    // MARK: - (d) FP and FN do not collapse on identical span

    func testFalsePositiveAndFalseNegativeDoNotCollapseOnSameSpan() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-d"))

        // Same scope, opposite semantic types. The dogfood fixture's
        // `asset_013 480.000:690.000` row appears in BOTH the FN and FP
        // buckets — a real user gestured both ways on the same span.
        // The identity tuple includes correctionType for exactly this
        // reason; the pair must remain two distinct rows.
        let scope = CorrectionScope.exactTimeSpan(
            assetId: "asset-d",
            startTime: 480.000,
            endTime: 690.000
        )
        try await correctionStore.record(CorrectionEvent(
            id: UUID().uuidString,
            analysisAssetId: "asset-d",
            scope: scope.serialized,
            createdAt: 100.0,
            source: .falseNegative,
            correctionType: .falseNegative
        ))
        try await correctionStore.record(CorrectionEvent(
            id: UUID().uuidString,
            analysisAssetId: "asset-d",
            scope: scope.serialized,
            createdAt: 200.0,
            source: .manualVeto,
            correctionType: .falsePositive
        ))

        let loaded = try await correctionStore.activeCorrections(for: "asset-d")
        XCTAssertEqual(loaded.count, 2,
            "FP and FN on the same span must remain distinct rows")
        let types = Set(loaded.compactMap { $0.correctionType })
        XCTAssertEqual(types, Set([.falsePositive, .falseNegative]),
            "Both semantic types must be preserved")
    }

    // MARK: - (e) Corpus export modes

    func testCorpusExportDistinctByDefaultAndRawEventsPreservesDuplicates() async throws {
        // We do NOT round-trip through the SQL upsert here — that path
        // already enforces uniqueness, which would mask the read-side
        // dedupe contract that this test pins. Instead drive the
        // exporter with an in-memory `CorpusExportSource` whose
        // `loadCorrectionEvents` returns synthetic duplicate rows
        // shaped like a pre-v23 backup snapshot.
        let assetId = "asset-e"
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: 10.000,
            endTime: 20.000
        )
        let baseTime: Double = 1_700_000_000.0
        let duplicates: [CorrectionEvent] = (0..<4).map { i in
            CorrectionEvent(
                id: "dup-\(i)",
                analysisAssetId: assetId,
                scope: scope.serialized,
                createdAt: baseTime + Double(i),
                source: .falseNegative,
                correctionType: .falseNegative
            )
        }

        let asset = makeTestAsset(id: assetId)
        let source = InMemoryCorpusExportSource(
            assets: [asset],
            corrections: [assetId: duplicates]
        )
        let docsDistinct = try makeTempDir(prefix: "CorpusDedupeDistinct")
        let docsRaw = try makeTempDir(prefix: "CorpusDedupeRaw")

        // Default mode collapses.
        let resultDistinct = try await CorpusExporter.export(
            store: source,
            documentsURL: docsDistinct,
            // playhead-vnni: bypass the cross-test dedup memo so
            // identical-content exports written by separate tests don't
            // collapse onto the same on-disk file (which would race
            // against the parallel `docsRaw` path here).
            dedupMemo: CorpusExportDedupMemo()
        )
        XCTAssertEqual(resultDistinct.correctionCount, 1,
            "Default `distinctSemantic` mode must emit one correction line for the duplicate cluster")

        // Raw mode preserves every row.
        let resultRaw = try await CorpusExporter.export(
            store: source,
            documentsURL: docsRaw,
            dedupMemo: CorpusExportDedupMemo(),
            correctionMode: .rawEvents
        )
        XCTAssertEqual(resultRaw.correctionCount, 4,
            "`rawEvents` mode must emit every persisted row, including pre-v23 duplicates")
    }

    // MARK: - (f) Replay round-trip across re-open

    func testReplayRoundTripStillCollapsesAcrossReopen() async throws {
        // Open a store, persist a correction, close it, re-open at the
        // same path, persist the same logical correction again. The
        // UNIQUE INDEX must survive the close/reopen and dedupe must
        // hold without us re-running the migration in-process.
        let dir = try makeTempDir(prefix: "CorrectionDedupeRoundTrip")
        let assetId = "asset-f"

        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: 7.000,
            endTime: 13.500
        )

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
            try await store.insertAsset(makeTestAsset(id: assetId))
            let cs = PersistentUserCorrectionStore(store: store)
            try await cs.record(CorrectionEvent(
                id: UUID().uuidString,
                analysisAssetId: assetId,
                scope: scope.serialized,
                createdAt: 1_700_000_000.0,
                source: .falseNegative,
                correctionType: .falseNegative
            ))
            let loaded = try await cs.activeCorrections(for: assetId)
            XCTAssertEqual(loaded.count, 1, "Pre-condition: one row before reopen")
            // `AnalysisStore` is an actor with no explicit close; SQLite's
            // file handle is owned by the actor and dropped when the
            // value deallocates after this scope exits.
        }

        // Force the migration cache to re-run on the reopened path so we
        // exercise the post-migration UNIQUE INDEX (fresh in-process state).
        AnalysisStore.resetMigratedPathsForTesting()

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
            let cs = PersistentUserCorrectionStore(store: store)
            // Re-submit the same logical correction via a NEW row id;
            // the UNIQUE INDEX must catch it.
            try await cs.record(CorrectionEvent(
                id: UUID().uuidString,
                analysisAssetId: assetId,
                scope: scope.serialized,
                createdAt: 1_700_000_999.0,
                source: .falseNegative,
                correctionType: .falseNegative
            ))
            let loaded = try await cs.activeCorrections(for: assetId)
            XCTAssertEqual(loaded.count, 1,
                "Same identity re-submitted after reopen must still produce one row")
            XCTAssertEqual(loaded.first?.submissionCount, 2,
                "Audit count must accumulate across reopen")
        }
    }

    // MARK: - (g) Fixture-backed: May 6 dogfood

    func testDogfoodFixtureKnownDuplicatesCollapseUnderReadSideDedupe() async throws {
        // The loader's `#filePath` defaults resolve to the CALL site, not
        // the loader's source. Re-anchor on the loader's known location
        // (PlayheadTests/Fixtures/Dogfood/) so the fixture loads from
        // outside the AdDetection test directory.
        let loaderPath = Self.dogfoodLoaderFilePath(testFilePath: #filePath)
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load(filePath: loaderPath)

        // Replay the fixture's `(correction_type, scope, count)` rows as
        // synthetic CorrectionEvents — the fixture stores the
        // POST-grouping form so each row's `count` is the multiplicity
        // of pre-dedupe rows on disk.
        var raw: [CorrectionEvent] = []
        var rowIndex = 0
        for r in fixture.correctionRows {
            guard let type = CorrectionType(rawValue: r.correctionType) else {
                XCTFail("Unrecognized correctionType in fixture: \(r.correctionType)")
                continue
            }
            // Asset id is encoded in the scope itself; pull it out so
            // we set `analysisAssetId` consistently with the wire format.
            let assetId: String = {
                guard let scope = CorrectionScope.deserialize(r.scope) else { return "asset-unknown" }
                switch scope {
                case .exactTimeSpan(let aid, _, _),
                     .exactSpan(let aid, _):
                    return aid
                case .sponsorOnShow(let pid, _),
                     .phraseOnShow(let pid, _),
                     .campaignOnShow(let pid, _),
                     .domainOwnershipOnShow(let pid, _),
                     .jingleOnShow(let pid, _):
                    return pid
                }
            }()
            let source: CorrectionSource = (type == .falseNegative) ? .falseNegative : .manualVeto
            for i in 0..<r.count {
                raw.append(CorrectionEvent(
                    id: "fixture-\(rowIndex)-\(i)",
                    analysisAssetId: assetId,
                    scope: r.scope,
                    createdAt: 1_700_000_000.0 + Double(rowIndex) + Double(i) * 0.001,
                    source: source,
                    correctionType: type
                ))
            }
            rowIndex += 1
        }

        // Read-side dedupe collapses every cluster.
        let dedupeResult = distinctSemanticCorrections(raw)
        let collapsed = dedupeResult.distinct
        let submissionCounts = dedupeResult.submissionCounts
        XCTAssertEqual(
            collapsed.count,
            fixture.correctionRows.count,
            "Read-side dedupe must produce one row per fixture cluster"
        )

        // Pin the canonical regression: the asset_012 falseNegative-x4
        // cluster called out in the brief MUST collapse to exactly 1
        // AND the surviving row's audit metadata must reflect the
        // pre-collapse multiplicity. Earlier versions of this test
        // only asserted the count, leaving an implementation that
        // dropped audit metadata silent. Tighten by also asserting
        // (a) submissionCount for the bucket = 4, and (b) the survivor
        // is the chronologically-earliest row in the bucket — so
        // first-observation provenance is preserved across dedupe.
        let asset012FN = collapsed.filter {
            $0.analysisAssetId == "asset_012"
                && $0.correctionType == .falseNegative
                && $0.scope == "exactTimeSpan:asset_012:3386.000:3394.140"
        }
        XCTAssertEqual(asset012FN.count, 1,
            "asset_012 falseNegative scope 3386.000:3394.140 must collapse to exactly 1 row")
        let asset012Survivor = try XCTUnwrap(asset012FN.first)
        XCTAssertEqual(submissionCounts[asset012Survivor.id], 4,
            "asset_012 falseNegative cluster: pre-collapse multiplicity (4) must survive on the dedupe-result audit map")
        // Locate the original raw rows for this cluster to verify the
        // survivor's createdAt is the OLDEST in the bucket — a regression
        // here would mean later submissions could overwrite original
        // observation provenance.
        let asset012RawCluster = raw.filter {
            $0.analysisAssetId == "asset_012"
                && $0.correctionType == .falseNegative
                && $0.scope == "exactTimeSpan:asset_012:3386.000:3394.140"
        }
        XCTAssertEqual(asset012RawCluster.count, 4,
            "Pre-condition: 4 raw rows expected for asset_012 falseNegative cluster")
        let oldestRawCreatedAt = try XCTUnwrap(asset012RawCluster.map(\.createdAt).min())
        XCTAssertEqual(asset012Survivor.createdAt, oldestRawCreatedAt, accuracy: 0.0001,
            "Survivor must pin the earliest createdAt in the cluster (first-observation provenance)")

        // The asset_013 480.000:690.000 row appears in BOTH the FN
        // (count: 3) and FP (count: 2) buckets in the fixture — they
        // must NOT collapse into each other. This is the same invariant
        // as test (d) but anchored on real captured data.
        let asset013Both = collapsed.filter {
            $0.analysisAssetId == "asset_013"
                && $0.scope == "exactTimeSpan:asset_013:480.000:690.000"
        }
        XCTAssertEqual(asset013Both.count, 2,
            "asset_013 480.000:690.000 must remain two rows (one FN, one FP)")
        XCTAssertEqual(
            Set(asset013Both.compactMap { $0.correctionType }),
            Set([.falsePositive, .falseNegative]),
            "Both semantic types must survive read-side dedupe"
        )
    }

    // MARK: - (h) V22 → V23 in-place SQL collapse migration

    /// Pin the SQL collapse code path executed by
    /// `migrateCorrectionEventsDedupeV23IfNeeded` when an existing v22
    /// database carries duplicate correction_events rows.
    ///
    /// Prior tests covered:
    ///   - the read-side `distinctSemanticCorrections` utility
    ///     (testDogfoodFixtureKnownDuplicatesCollapseUnderReadSideDedupe)
    ///   - the live `INSERT ... ON CONFLICT DO UPDATE` upsert path
    ///     (testReplayRoundTripStillCollapsesAcrossReopen)
    ///
    /// This pins the third leg: the one-shot SQL collapse executed
    /// during the v22 → v23 schema upgrade itself. The risk surface
    /// covered here is migration-local — wrong survivor selection,
    /// missing audit-column population, or failure to apply the
    /// canonicalization rules consistently with the runtime model.
    func testV22ToV23MigrationCollapsesPreExistingDuplicatesAndPopulatesAudit() async throws {
        // 1. Bootstrap a fully-migrated v23 DB so all upstream tables
        //    (including correction_events with the v22 column shape)
        //    exist in the right schema.
        let dir = try makeTempDir(prefix: "CorrectionV23MigrateCollapse")
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        let bootstrapVersion = try await bootstrap.schemaVersion()
        XCTAssertEqual(bootstrapVersion, 24,
            "Pre-condition: bootstrap must reach v24")
        try await bootstrap.insertAsset(makeTestAsset(id: "asset-mig"))

        // 2. Rewind: drop the v23 audit columns + UNIQUE INDEX, reset
        //    _meta back to '22'. This puts the DB in the exact shape a
        //    real pre-v23 device would have at upgrade time.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        XCTAssertEqual(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil), SQLITE_OK)
        defer { sqlite3_close_v2(db) }
        // Recreate the table without the v23 audit columns by copying
        // through a v22-shape side table. This is the most faithful
        // simulation of a real pre-v23 device, which never wrote those
        // columns at all.
        let rewind = """
            DROP INDEX IF EXISTS idx_correction_events_identity;
            CREATE TABLE correction_events_v22 (
                id               TEXT PRIMARY KEY,
                analysisAssetId  TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                scope            TEXT NOT NULL,
                createdAt        REAL NOT NULL,
                source           TEXT,
                podcastId        TEXT,
                correctionType   TEXT,
                causalSource     TEXT,
                targetRefsJSON   TEXT
            );
            DROP TABLE correction_events;
            ALTER TABLE correction_events_v22 RENAME TO correction_events;
            CREATE INDEX IF NOT EXISTS idx_correction_events_asset ON correction_events(analysisAssetId);
            CREATE INDEX IF NOT EXISTS idx_correction_events_scope ON correction_events(scope);
            UPDATE _meta SET value = '22' WHERE key = 'schema_version';
            """
        var errMsg: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(db, rewind, nil, nil, &errMsg)
        if rc != SQLITE_OK, let m = errMsg {
            let msg = String(cString: m)
            sqlite3_free(errMsg)
            XCTFail("Rewind SQL failed: \(msg)")
            return
        }

        // 3. Insert duplicate rows directly via SQL — bypassing the
        //    upsert path entirely so we exercise the migration's
        //    in-place collapse, not the live conflict resolver.
        //    Cluster: 4 rows on the same `.exactTimeSpan` identity at
        //    times that differ by < tolerance (<= 0.05s jitter).
        let baseTime: Double = 1_700_000_000.0
        let scopeStr = "exactTimeSpan:asset-mig:100.000:200.000"
        let scopeJitterStr = "exactTimeSpan:asset-mig:100.020:200.020"  // same bucket
        let insertRows: [(String, String, Double)] = [
            ("dup-id-3", scopeStr,       baseTime + 30.0),  // newest
            ("dup-id-1", scopeStr,       baseTime + 10.0),  // OLDEST → survivor
            ("dup-id-4", scopeJitterStr, baseTime + 40.0),  // jitter, still same bucket
            ("dup-id-2", scopeStr,       baseTime + 20.0),
        ]
        for (id, scope, ts) in insertRows {
            let insertSQL = """
                INSERT INTO correction_events
                (id, analysisAssetId, scope, createdAt, source, correctionType)
                VALUES ('\(id)', 'asset-mig', '\(scope)', \(ts), 'falseNegative', 'falseNegative')
                """
            XCTAssertEqual(sqlite3_exec(db, insertSQL, nil, nil, nil), SQLITE_OK,
                "Direct SQL insert of duplicate must succeed (id=\(id))")
        }
        // Sanity: 4 rows landed.
        var countStmt: OpaquePointer?
        XCTAssertEqual(sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM correction_events", -1, &countStmt, nil), SQLITE_OK)
        XCTAssertEqual(sqlite3_step(countStmt), SQLITE_ROW)
        let preCount = Int(sqlite3_column_int64(countStmt, 0))
        sqlite3_finalize(countStmt)
        XCTAssertEqual(preCount, 4, "Pre-condition: 4 raw rows seeded")

        // 4. Re-migrate. This runs the v22 → v23 path, which must
        //    collapse the cluster.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let postMigrateVersion = try await store.schemaVersion()
        XCTAssertEqual(postMigrateVersion, 24,
            "Migration must reach v24")

        // 5. Verify collapse + audit metadata via the public load path.
        let cs = PersistentUserCorrectionStore(store: store)
        let loaded = try await cs.activeCorrections(for: "asset-mig")
        XCTAssertEqual(loaded.count, 1,
            "v22 → v23 collapse must produce exactly one row from the 4-row cluster")
        let row = try XCTUnwrap(loaded.first)
        // Survivor must be `dup-id-1` (smallest createdAt, tie-broken by id).
        XCTAssertEqual(row.id, "dup-id-1",
            "Survivor must be the chronologically-earliest row in the bucket")
        XCTAssertEqual(row.createdAt, baseTime + 10.0, accuracy: 0.001,
            "Survivor's createdAt must pin the first observation")
        XCTAssertEqual(row.submissionCount, 4,
            "submissionCount must reflect cumulative bucket size")
        XCTAssertEqual(row.lastSeenAt ?? -1, baseTime + 40.0, accuracy: 0.001,
            "lastSeenAt must equal the bucket's max createdAt")

        // 6. Idempotence: re-running the migration on an already-v23 DB
        //    must be a no-op. Reset the migrated-paths cache, run again,
        //    and confirm the row count + audit metadata are unchanged.
        AnalysisStore.resetMigratedPathsForTesting()
        let store2 = try AnalysisStore(directory: dir)
        try await store2.migrate()
        let cs2 = PersistentUserCorrectionStore(store: store2)
        let loaded2 = try await cs2.activeCorrections(for: "asset-mig")
        XCTAssertEqual(loaded2.count, 1, "Idempotent re-migrate must not duplicate rows")
        XCTAssertEqual(loaded2.first?.submissionCount, 4,
            "Idempotent re-migrate must preserve audit metadata exactly (no double-counting)")
        XCTAssertEqual(loaded2.first?.id, "dup-id-1",
            "Idempotent re-migrate must preserve the survivor identity")
    }
}

// MARK: - InMemoryCorpusExportSource

/// Minimal `CorpusExportSource` that returns a configured
/// `[assetId: [CorrectionEvent]]` map. Used by the export-mode test to
/// drive the dedupe code path with synthetic duplicate input that does
/// NOT round-trip through the v23 SQL upsert (which would otherwise
/// silently collapse the input before `CorpusExporter.export` ever sees it).
private struct InMemoryCorpusExportSource: CorpusExportSource {
    let assets: [AnalysisAsset]
    let corrections: [String: [CorrectionEvent]]

    func fetchAllAssets() async throws -> [AnalysisAsset] { assets }
    func fetchDecodedSpans(assetId: String) async throws -> [DecodedSpan] { [] }
    func fetchAdWindows(assetId: String) async throws -> [AdWindow] { [] }
    func loadCorrectionEvents(analysisAssetId: String) async throws -> [CorrectionEvent] {
        corrections[analysisAssetId] ?? []
    }
    func fetchPodcastId(forEpisodeId episodeId: String) async throws -> String? { nil }
    func fetchPodcastProfile(podcastId: String) async throws -> PodcastProfile? { nil }
    func allShadowFMResponses() async throws -> [ShadowFMResponse] { [] }
    func fetchListenRewinds(forAssetId assetId: String) async throws -> [AdListenRewindRow] { [] }
}
#endif
