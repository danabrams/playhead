// AdWindowSkipConfidenceSplitV47MigrationTests.swift
// playhead-ar60: pin the V47 migration that stops `ad_windows.confidence`
// from being two quantities — it adds `skipConfidence` and MOVES the fusion
// path's actuation number into it, restoring each row's detection number from
// the `decision_events` row that produced it.
//
// The numbers are not invented for the test. Every value below is a verbatim
// `Double` from the 2026-08-02 device pull (`DE0784D8MidRollPodFixture`),
// where 40 of 104 `ad_windows` rows carried an actuation number in a column
// that means detection for the other six producers, and 40 of 40 of them had
// a `decision_events` row whose `skipConfidence` was bit-identical to the
// row's `confidence`.
//
// Coverage targets:
//   1. Fresh-DB migrate() reaches head with the column present.
//   2. `currentSchemaVersion` includes V47 (pinned to the LITERAL).
//   3. A v46-shaped `ad_windows` upgrades in place and the REPAIR runs: the
//      seam false positive's confidence goes 0.00115 → 0.45634 and its
//      actuation number lands in `skipConfidence`, unchanged.
//   4. ANTI-INVENTION: a row with no matching `decision_events` witness is
//      left completely alone — `skipConfidence` NULL, `confidence` untouched.
//   5. A row whose confidence matches NO event of its own window id is not
//      repaired from some other window's event.
//   6. The migration is idempotent (a second pass must not move a value
//      twice, which would put the detection number in the actuation column).
//   7. CRUD round-trip, and `actuationConfidence`'s fallback.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("ad_windows detection/actuation split V47 migration (playhead-ar60)")
struct AdWindowSkipConfidenceSplitV47MigrationTests {

    private typealias Pull = DE0784D8MidRollPodFixture

    private static let table = "ad_windows"
    private static let column = "skipConfidence"

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "AdWindowSkipConfidenceV47")
    }

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

    /// A window shaped the way the PRE-ar60 fusion writer shaped one: the
    /// actuation number in `confidence`, nothing in `skipConfidence`.
    private func preSplitFusionWindow(
        id: String,
        assetId: String,
        startTime: Double,
        endTime: Double,
        persistedConfidence: Double,
        decisionState: String = AdDecisionState.confirmed.rawValue
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            confidence: persistedConfidence,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: startTime,
            metadataSource: "fallback",
            metadataConfidence: 0.1,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.blockedByUserCorrection.rawValue
        )
    }

    private func decisionEvent(
        windowId: String,
        assetId: String,
        proposalConfidence: Double,
        skipConfidence: Double,
        createdAt: Double
    ) -> DecisionEvent {
        DecisionEvent(
            id: "de-\(windowId)-\(createdAt)",
            analysisAssetId: assetId,
            eventType: "backfill_fusion",
            windowId: windowId,
            proposalConfidence: proposalConfidence,
            skipConfidence: skipConfidence,
            eligibilityGate: SkipEligibilityGate.blockedByUserCorrection.rawValue,
            policyAction: SkipPolicyAction.detectOnly.rawValue,
            decisionCohortJSON: "{}",
            createdAt: createdAt
        )
    }

    /// Drop the V47 column and rewind `_meta.schema_version`.
    ///
    /// Pinned to the LITERAL 46: "pre-ar60" is v46, a fixed historical fact.
    /// Written as `currentSchemaVersion - 1` it would stop meaning that the
    /// moment head moved past 47 — the rewind would land ON 47, V47's
    /// `observed < 47` guard would decline, and the test would assert the
    /// absence of a column it had just prevented from being added.
    private func rewindToV46(_ store: AnalysisStore) async throws {
        try await store.execForTesting(
            "ALTER TABLE ad_windows DROP COLUMN skipConfidence;"
        )
        try await store.setMetaValue(forKey: "schema_version", value: "46")
    }

    private func columnPresent(in dir: URL) throws -> Bool {
        try probeColumnExists(in: dir, table: Self.table, column: Self.column)
    }

    // MARK: - Migration ladder

    @Test("fresh DB migrate() lands the actuation column at head")
    func freshDbHasV47Column() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Drift guard, pinned to the LITERAL head. Written as
        // `== AnalysisStore.currentSchemaVersion` it would pass for every
        // possible value and police nothing.
        //
        // 48 → 49 (playhead-mn5e/2qz6). Worth a second look here because V47
        // was itself a REPAIR migration over `ad_windows.confidence`, and V49
        // is another data-changing rung: it resets
        // `podcast_profiles.observationCount` to 0. Different table, no
        // overlap — V49 names neither `ad_windows` nor `decision_events`, so
        // the repaired detection number this suite pins is not re-touched.
        // 49 → 50 (playhead-e6d3) is a THIRD data-changing rung, and gets the
        // same second look: it UPDATEs `backfill_jobs.retryCount` on rows the
        // flat under-coverage rule retired. Different table again — it names
        // neither `ad_windows` nor `decision_events`.
        // 50 → 51 read for this rung (playhead-wogi): V51 lowers
        // `backfill_jobs.progressCursor` to the prefix each asset's own
        // `semantic_scan_results` passA rows support, and touches no other
        // column and no other table. Nothing this rung asserts is named by it.
        #expect(AnalysisStore.currentSchemaVersion == 51)
        #expect(try columnPresent(in: dir))
    }

    @Test("v46-shaped ad_windows upgrades in place and the REPAIR restores the detection number the row lost")
    func seededV46RowIsRepairedFromItsOwnDecisionEvent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "DE0784D8-BE2F-4BEB-8BA1-3D9EF51AD659"
        let witness = try #require(Pull.frozenFalsePositiveDecision)
        let frozen = try #require(Pull.frozenFalsePositiveWindow)
        // The fixture and the witness must describe the SAME row, or this test
        // is pinning a coincidence.
        #expect(frozen.confidence == witness.persistedConfidence)
        #expect(witness.persistedConfidence == witness.skipConfidence)

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: assetId))
        try await bootstrap.insertAdWindow(
            preSplitFusionWindow(
                id: witness.windowId,
                assetId: assetId,
                startTime: frozen.startTime,
                endTime: frozen.endTime,
                persistedConfidence: witness.persistedConfidence
            )
        )
        // Two events for this window, exactly as the device carried: the run
        // that wrote the row, and a LATER run whose skipConfidence differs.
        // The later one must NOT be used — the join is on the value, not on
        // recency, because recency would pair this row's actuation number with
        // a different run's proposal.
        try await bootstrap.appendDecisionEvent(
            decisionEvent(
                windowId: witness.windowId,
                assetId: assetId,
                proposalConfidence: witness.proposalConfidence,
                skipConfidence: witness.skipConfidence,
                createdAt: 1_785_890_554.95
            )
        )
        try await bootstrap.appendDecisionEvent(
            decisionEvent(
                windowId: witness.windowId,
                assetId: assetId,
                proposalConfidence: 0.9,
                skipConfidence: 3.7238917452052238e-07,
                createdAt: 1_785_943_480.23
            )
        )

        try await rewindToV46(bootstrap)
        #expect(!(try columnPresent(in: dir)), "the fixture must genuinely predate the column")

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try columnPresent(in: dir))

        let repaired = try #require(try await store.fetchAdWindow(id: witness.windowId))
        // The MOVE: the actuation number is unchanged, in the right column.
        #expect(repaired.skipConfidence == witness.skipConfidence)
        #expect(repaired.actuationConfidence == witness.skipConfidence,
                "the number every actuation reader sees must be untouched by the migration")
        // The RESTORE: bit-exact, from the row's own run.
        #expect(repaired.confidence == witness.proposalConfidence)
        #expect(repaired.confidence != 0.9,
                "the LATER event's proposal belongs to a different run and must not be used")
        // Scale of what the column was hiding: ~397x on this row.
        #expect(repaired.confidence / repaired.actuationConfidence > 390)
        #expect(repaired.confidence / repaired.actuationConfidence < 400)
        // Nothing else moved.
        #expect(repaired.startTime == frozen.startTime)
        #expect(repaired.endTime == frozen.endTime)
        #expect(repaired.eligibilityGate == "blockedByUserCorrection")
        #expect(repaired.metadataConfidence == 0.1,
                "the extractor owns metadataConfidence; the migration must not touch it")
    }

    @Test("ANTI-INVENTION: a row with no decision_events witness is left completely alone")
    func rowWithoutWitnessIsUntouched() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "asset-no-witness"
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: assetId))
        // A hot-path row: a real UUID id, and no `decision_events` row anywhere
        // in the corpus names it. On both device pulls, NO non-fusion row
        // matched the join — the join is its own discriminator.
        try await bootstrap.insertAdWindow(
            preSplitFusionWindow(
                id: "3F0A1C22-0000-4000-8000-000000000001",
                assetId: assetId,
                startTime: 100,
                endTime: 130,
                persistedConfidence: 0.4102145
            )
        )

        try await rewindToV46(bootstrap)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let row = try #require(
            try await store.fetchAdWindow(id: "3F0A1C22-0000-4000-8000-000000000001")
        )
        #expect(row.skipConfidence == nil,
                "no witness ⇒ nothing is known about a separate actuation number")
        #expect(row.confidence == 0.4102145,
                "a row whose detection number cannot be recovered keeps what it shipped with")
        #expect(row.actuationConfidence == 0.4102145,
                "the fallback is what makes the split behaviour-preserving for one-number producers")
    }

    @Test("a row is never repaired from ANOTHER window's decision event")
    func repairIsScopedToTheRowsOwnWindowId() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "asset-cross-window"
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: assetId))
        try await bootstrap.insertAdWindow(
            preSplitFusionWindow(
                id: "fusion-aaaaaaaaaaaaaaaa",
                assetId: assetId,
                startTime: 10,
                endTime: 40,
                persistedConfidence: 0.00250
            )
        )
        // Same asset, same skipConfidence VALUE, different window.
        try await bootstrap.appendDecisionEvent(
            decisionEvent(
                windowId: "fusion-bbbbbbbbbbbbbbbb",
                assetId: assetId,
                proposalConfidence: 0.77,
                skipConfidence: 0.00250,
                createdAt: 1_785_890_554.95
            )
        )

        try await rewindToV46(bootstrap)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let row = try #require(
            try await store.fetchAdWindow(id: "fusion-aaaaaaaaaaaaaaaa")
        )
        #expect(row.skipConfidence == nil)
        #expect(row.confidence == 0.00250,
                "a value match on a DIFFERENT window id is a coincidence, not a witness")
    }

    @Test("the migration is idempotent — a second pass cannot move the value twice")
    func migrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "asset-idempotent"
        let witness = try #require(Pull.frozenFalsePositiveDecision)

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: assetId))
        try await bootstrap.insertAdWindow(
            preSplitFusionWindow(
                id: witness.windowId,
                assetId: assetId,
                startTime: 2828.4,
                endTime: 2836.44,
                persistedConfidence: witness.persistedConfidence
            )
        )
        try await bootstrap.appendDecisionEvent(
            decisionEvent(
                windowId: witness.windowId,
                assetId: assetId,
                proposalConfidence: witness.proposalConfidence,
                skipConfidence: witness.skipConfidence,
                createdAt: 1_785_890_554.95
            )
        )
        try await rewindToV46(bootstrap)

        AnalysisStore.resetMigratedPathsForTesting()
        let first = try AnalysisStore(directory: dir)
        try await first.migrate()
        let once = try #require(try await first.fetchAdWindow(id: witness.windowId))
        #expect(once.confidence == witness.proposalConfidence)
        #expect(once.skipConfidence == witness.skipConfidence)

        // Re-run the REPAIR, not just the ladder. Simply calling `migrate()`
        // again proves nothing: `guard observed < 47` returns before the
        // UPDATE, so a version-guard test would stay green with the
        // `WHERE skipConfidence IS NULL` clause DELETED. Rewinding only the
        // recorded version — leaving the column and its now-repaired data in
        // place — is the state that actually exercises the clause. Without it
        // the second pass moves `confidence` (now the PROPOSAL) into
        // `skipConfidence`, leaving the row asserting an actuation permission
        // of 0.456 for a span the user vetoed.
        try await first.setMetaValue(forKey: "schema_version", value: "46")
        AnalysisStore.resetMigratedPathsForTesting()
        let second = try AnalysisStore(directory: dir)
        try await second.migrate()

        let row = try #require(try await second.fetchAdWindow(id: witness.windowId))
        #expect(try await second.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(row.confidence == witness.proposalConfidence,
                "the detection number must not be moved a second time")
        #expect(row.skipConfidence == witness.skipConfidence,
                "the actuation column must still hold the actuation number")
    }

    // MARK: - CRUD round-trip

    @Test("insert → fetch preserves both quantities, and nil means one-number")
    func roundTripCarriesBothQuantities() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "asset-roundtrip"
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: assetId))

        let witness = try #require(Pull.frozenFalsePositiveDecision)
        // Shaped the way `buildFusionAdWindow` shapes one AFTER ar60.
        var split = preSplitFusionWindow(
            id: "fusion-split-roundtrip",
            assetId: assetId,
            startTime: 2828.4,
            endTime: 2836.44,
            persistedConfidence: witness.proposalConfidence
        )
        split = AdWindow(
            id: split.id,
            analysisAssetId: split.analysisAssetId,
            startTime: split.startTime,
            endTime: split.endTime,
            confidence: witness.proposalConfidence,
            skipConfidence: witness.skipConfidence,
            boundaryState: split.boundaryState,
            decisionState: split.decisionState,
            detectorVersion: split.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: split.evidenceStartTime,
            metadataSource: split.metadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: split.eligibilityGate
        )
        try await store.insertAdWindow(split)

        let one = try #require(try await store.fetchAdWindow(id: "fusion-split-roundtrip"))
        #expect(one.confidence == witness.proposalConfidence)
        #expect(one.skipConfidence == witness.skipConfidence)
        #expect(one.actuationConfidence == witness.skipConfidence)
        #expect(one.carriesUsableActuationConfidence)

        // The list reader resolves the ALTER-appended column by name too.
        let many = try await store.fetchAdWindows(assetId: assetId)
        let listed = try #require(many.first { $0.id == "fusion-split-roundtrip" })
        #expect(listed.skipConfidence == witness.skipConfidence)
        #expect(listed.confidence == witness.proposalConfidence)

        // A one-number producer: nil round-trips as nil and the fallback holds.
        try await store.insertAdWindow(
            preSplitFusionWindow(
                id: "hot-path-roundtrip",
                assetId: assetId,
                startTime: 500,
                endTime: 540,
                persistedConfidence: 0.62
            )
        )
        let single = try #require(try await store.fetchAdWindow(id: "hot-path-roundtrip"))
        #expect(single.skipConfidence == nil)
        #expect(single.actuationConfidence == 0.62)
    }
}
