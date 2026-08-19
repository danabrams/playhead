// DetectorTrustObservationCountV58MigrationTests.swift
// playhead-scc6: pin the V58 reset of the PER-CLASS `observationCount` inside
// `podcast_profiles.detectorTrustJSON`.
//
// WHY THERE IS A MIGRATION AT ALL. playhead-2qz6's V49 corrected
// `podcast_profiles.observationCount` from a per-backfill unit to a per-episode
// one with `UPDATE podcast_profiles SET observationCount = 0`, and that reaches
// ONE of the two places the quantity lives. `DetectorTrustLedger` stores a
// per-class copy of it in a different column of the same table — and that copy
// is the one that decides things, because `TrustScoringService
// .resolveDetectorModes` returns the STORED entry in preference to the seed and
// `SkipOrchestrator.skipMode(for:)` reads the per-class verdict rather than
// `profile.mode`.
//
// THE FIELD STATE MOVED WHILE THE BEAD SAT OPEN. Filed against the 2026-08-12
// pull, on which `detectorTrustJSON` was NULL on both shows — a defect with zero
// instances. On all four pulls of 2026-08-18 it is NON-NULL on both shows, four
// entries each, and the per-class counts read `fusion 16` against **8** distinct
// `trust_episode_observations` rows on simplecast, `fusion 10` against **7** on
// flightcast. The fixtures below are those two ledgers verbatim.
//
// The directions this file covers, because closing one leaves the others open:
//
//   1. THE RESET HAPPENS, on a store whose ledger was written in the old unit,
//      driven through the real ladder from a rewound V57.
//   2. NO OTHER FIELD OF ANY ENTRY MOVES — and `mode` above all. A repair that
//      re-ran promotion, or that wrote the ledger back through
//      `SkipDetectorClass.allCases`, would pass a count-only assertion.
//   3. A CLASS KEY THIS BINARY DOES NOT RECOGNISE is repaired and KEPT. It is
//      the same unit; dropping it would silently delete a newer build's history.
//   4. EVERY OTHER COLUMN SURVIVES — mode, trust, the show scalar, the veto
//      counter, the learned priors, the trait blob. V49's contract, this rung.
//   5. WHAT IT COSTS, asserted rather than argued: a `.shadow` class carrying an
//      inflated count was ONE gesture from `manual` and is now three. That is
//      the deactivation, and it is the point.
//   6. IDEMPOTENT ACROSS LAUNCHES, which is the property a device has — the
//      version ladder is the guard. The per-row `!= 0` predicate is a COST
//      guard, not an already-migrated marker, and the difference is asserted.
//   7. THE POPULATION IS THE READABLE ONE. A column the decoder rejects is
//      already read as an EMPTY ledger, so every class falls back to its seed
//      and no inflated count survives to be read — there is nothing to repair.
//   8. A FIXTURE WITHOUT `podcast_profiles` still reaches v58, and V58 does not
//      step over a rolled-back V39.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("the per-class observationCount counts EPISODES (playhead-scc6 V58)")
struct DetectorTrustObservationCountV58MigrationTests {

    // MARK: - Raw-column probe
    //
    // Deliberately NOT routed through `AnalysisStore.fetchProfile` or
    // `PodcastProfile.detectorTrustLedger`, for V55/V56/V57's reason: the claim
    // is about what is ON DISK, and asking the store would ask the same read
    // whose correctness is under test. `detectorTrustLedger` in particular
    // SWALLOWS a decode failure into an empty ledger, so a repair that destroyed
    // a blob would read as an empty ledger rather than as damage.

    private func rawLedgerJSON(in directory: URL, podcastId: String) throws -> String? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "rawLedgerJSON", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            "SELECT detectorTrustJSON FROM podcast_profiles WHERE podcastId = ?",
            -1, &stmt, nil
        ) == SQLITE_OK else {
            throw NSError(domain: "rawLedgerJSON", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, (podcastId as NSString).utf8String, -1, nil)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        guard sqlite3_column_type(stmt, 0) != SQLITE_NULL else { return nil }
        return String(cString: sqlite3_column_text(stmt, 0))
    }

    /// The raw column, decoded WITHOUT the store's forgiving path — a throw here
    /// means the migration damaged a blob, which is a different failure from
    /// "the ledger is empty" and must not be able to look like it.
    private func decodeStrict(_ json: String) throws -> [String: DetectorTrustEntry] {
        try JSONDecoder().decode([String: DetectorTrustEntry].self, from: Data(json.utf8))
    }

    private func storedEntries(in directory: URL, podcastId: String) throws -> [String: DetectorTrustEntry] {
        try decodeStrict(try #require(try rawLedgerJSON(in: directory, podcastId: podcastId)))
    }

    /// Rewind to the V57 version stamp. Unlike V52/V55/V56 there is no SHAPE to
    /// rewind: V58 adds no column and drops none, it rewrites a value. The
    /// fixture supplies the pre-V58 shape by writing an inflated per-class count,
    /// which is the state a field device is in.
    ///
    /// Pinned to the LITERAL 57: "pre-scc6" is a fixed historical fact, and
    /// `currentSchemaVersion - 1` stops meaning it the moment head moves.
    private func rewindToV57(_ store: AnalysisStore) async throws {
        try await store.setMetaValue(forKey: "schema_version", value: "57")
    }

    private func makeProfile(
        podcastId: String,
        detectorTrustJSON: String?,
        observationCount: Int = 8,
        mode: String = "manual",
        trust: Double = 0.1,
        falseSignals: Int = 2
    ) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: "acme,brand",
            normalizedAdSlotPriors: "[0.1,0.5]",
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 4,
            skipTrustScore: trust,
            observationCount: observationCount,
            mode: mode,
            recentFalseSkipSignals: falseSignals,
            traitProfileJSON: #"{"insertionVolatility":0.1,"transcriptReliability":0.7,"singleSpeakerDominance":0.5,"speakerTurnRate":0,"sponsorRecurrence":0,"musicDensity":0.4,"episodesObserved":0,"structureRegularity":0.8}"#,
            title: "A Show",
            adDurationStatsJSON: #"{"meanDuration":41.08485035056562,"sampleCount":488}"#,
            networkId: nil,
            detectorTrustJSON: detectorTrustJSON
        )
    }

    // MARK: - The device pulls, verbatim

    /// simplecast on the 2026-08-18 t3 pull. `fusion` reads 16 against 8
    /// `trust_episode_observations` rows.
    private static let simplecastLedgerJSON = """
        {"rediffByteExact":{"observationCount":0,"trustScore":0.5,"mode":"auto","falseSkipWeight":0},\
        "userAsserted":{"observationCount":0,"trustScore":0.30000000000000004,"mode":"auto","falseSkipWeight":0.5},\
        "fusion":{"observationCount":16,"trustScore":1,"mode":"auto","falseSkipWeight":0.5},\
        "segmentAggregated":{"observationCount":1,"trustScore":0.5,"mode":"auto","falseSkipWeight":0}}
        """

    /// flightcast on the same pull. `fusion` reads 10 against 7.
    private static let flightcastLedgerJSON = """
        {"userAsserted":{"trustScore":0.8499999999999998,"mode":"auto","falseSkipWeight":0,"observationCount":5},\
        "fusion":{"trustScore":0.49999999999999967,"mode":"auto","falseSkipWeight":0,"observationCount":10},\
        "segmentAggregated":{"trustScore":0.9999999999999999,"mode":"auto","falseSkipWeight":0,"observationCount":5},\
        "rediffByteExact":{"trustScore":0.5,"mode":"auto","falseSkipWeight":0,"observationCount":0}}
        """

    // MARK: - 1. The reset happens

    @Test("V58 resets every inflated per-class observationCount to 0")
    func migrationResetsTheInflatedPerClassCounts() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Reset")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(
            makeProfile(podcastId: "simplecast", detectorTrustJSON: Self.simplecastLedgerJSON)
        )
        try await store.upsertProfile(
            makeProfile(podcastId: "flightcast", detectorTrustJSON: Self.flightcastLedgerJSON,
                        mode: "auto", falseSignals: 0)
        )
        try await rewindToV57(store)
        #expect(try await store.schemaVersion() == 57)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)

        for id in ["simplecast", "flightcast"] {
            let entries = try storedEntries(in: dir, podcastId: id)
            #expect(entries.count == 4, "\(id): every key must survive the repair")
            for (cls, entry) in entries {
                #expect(
                    entry.observationCount == 0,
                    """
                    \(id)/\(cls): 16 and 10 are counts of WRITES — one per gesture, one per \
                    named detector per call — and neither is claim-gated. They cannot be \
                    carried into a unit that means episodes. Got \(entry.observationCount)
                    """
                )
            }
        }
    }

    // MARK: - 2. Only the one key moves — and `mode` above all

    @Test("V58 preserves every other field of every entry, bit-exact")
    func migrationPreservesEveryOtherEntryField() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Fields")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(
            makeProfile(podcastId: "simplecast", detectorTrustJSON: Self.simplecastLedgerJSON)
        )
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let before = try decodeStrict(Self.simplecastLedgerJSON)
        let after = try storedEntries(in: dir, podcastId: "simplecast")
        #expect(Set(after.keys) == Set(before.keys))
        for (cls, was) in before {
            let now = try #require(after[cls])
            // Bit-exact, not approximately: `0.30000000000000004` and
            // `0.49999999999999967` are on the device today, and a repair that
            // rounded them would be a defect wearing a plausible face.
            #expect(now.trustScore == was.trustScore, "\(cls): trustScore moved")
            #expect(now.falseSkipWeight == was.falseSkipWeight, "\(cls): falseSkipWeight moved — that counter is a record of the LISTENER's vetoes")
            #expect(now.mode == was.mode, "\(cls): mode moved")
            #expect(now.observationCount == 0)
        }
    }

    /// The mutant this exists for: a repair that "helpfully" re-ran promotion
    /// after zeroing the count, or that rebuilt the ledger from
    /// `DetectorTrustLedger.seed`. Both would demote a `.manual`/`.auto` entry
    /// whose count no longer clears the rung — a demotion nobody chose, which is
    /// exactly what V49's own test forbids one column over.
    @Test("V58 moves no entry's mode in either direction")
    func migrationMovesNoEntryMode() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Modes")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        // One entry per rung, each with a count that a re-run promotion would
        // read differently once it is zero.
        let mixed = """
            {"fusion":{"trustScore":0.9,"mode":"auto","falseSkipWeight":0,"observationCount":12},\
            "segmentAggregated":{"trustScore":0.8,"mode":"manual","falseSkipWeight":0,"observationCount":9},\
            "userAsserted":{"trustScore":0.7,"mode":"shadow","falseSkipWeight":0,"observationCount":6},\
            "rediffByteExact":{"trustScore":0.5,"mode":"auto","falseSkipWeight":0,"observationCount":0}}
            """
        try await store.upsertProfile(makeProfile(podcastId: "mixed", detectorTrustJSON: mixed))
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let after = try storedEntries(in: dir, podcastId: "mixed")
        #expect(after["fusion"]?.mode == "auto")
        #expect(after["segmentAggregated"]?.mode == "manual")
        #expect(after["userAsserted"]?.mode == "shadow")
        #expect(after["rediffByteExact"]?.mode == "auto")
        #expect(after.values.allSatisfy { $0.observationCount == 0 })

        // …and the gate agrees, which is the reading that matters: the mode the
        // skip gate consults is the STORED one, unchanged.
        let modes = await TrustScoringService(store: reopened)
            .resolveDetectorModes(podcastId: "mixed")
        #expect(modes.mode(for: .fusion) == .auto)
        #expect(modes.mode(for: .segmentAggregated) == .manual)
        #expect(modes.mode(for: .userAsserted) == .shadow)
        #expect(modes.mode(for: .rediffByteExact) == .auto)
    }

    // MARK: - 3. An unrecognised class key

    /// `DetectorTrustLedger`'s storage argument is that a key a newer build
    /// added survives a round trip through an older one. The repair honours it
    /// by walking the decoded DICTIONARY rather than `SkipDetectorClass
    /// .allCases` — and it repairs that key too, because it is the same unit.
    @Test("a class key this binary does not recognise is repaired and kept")
    func migrationRepairsAndKeepsUnknownClassKeys() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Unknown")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let withFuture = """
            {"fusion":{"trustScore":0.9,"mode":"auto","falseSkipWeight":0.5,"observationCount":12},\
            "someFutureDetector":{"trustScore":0.6,"mode":"manual","falseSkipWeight":1.5,"observationCount":7}}
            """
        try await store.upsertProfile(makeProfile(podcastId: "future", detectorTrustJSON: withFuture))
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let after = try storedEntries(in: dir, podcastId: "future")
        #expect(
            Set(after.keys) == ["fusion", "someFutureDetector"],
            "a key dropped by the repair is a newer build's history silently deleted; got \(after.keys.sorted())"
        )
        #expect(after["someFutureDetector"]?.observationCount == 0)
        #expect(after["someFutureDetector"]?.mode == "manual")
        #expect(after["someFutureDetector"]?.trustScore == 0.6)
        #expect(after["someFutureDetector"]?.falseSkipWeight == 1.5)
        #expect(after["fusion"]?.observationCount == 0)
    }

    // MARK: - 4. Every other column survives — V49's contract, this rung

    @Test("V58 changes no other column: the show scalar, mode, trust, vetoes and priors survive")
    func migrationTouchesNothingElse() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Untouched")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        // A DEMOTED profile, deliberately: the reading that matters most is that
        // a user's vetoes and the mode they produced survive a repair aimed at a
        // different quantity. V49's and V57's fixture choice.
        try await store.upsertProfile(
            makeProfile(
                podcastId: "show-demoted",
                detectorTrustJSON: Self.simplecastLedgerJSON,
                observationCount: 8,
                mode: "shadow",
                trust: 0.1,
                falseSignals: 2
            )
        )
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let after = try #require(await reopened.fetchProfile(podcastId: "show-demoted"))
        #expect(after.mode == "shadow")
        #expect(after.skipTrustScore == 0.1)
        #expect(after.recentFalseSkipSignals == 2)
        #expect(after.implicitFalsePositiveCount == 4)
        #expect(
            after.observationCount == 8,
            "the SHOW scalar is already in EPISODES — V49 gave it a ledger and V58 must not re-reset it"
        )
        #expect(after.sponsorLexicon == "acme,brand")
        #expect(after.normalizedAdSlotPriors == "[0.1,0.5]")
        #expect(after.adDurationStatsJSON == #"{"meanDuration":41.08485035056562,"sampleCount":488}"#)
        #expect(after.title == "A Show")
        #expect(after.traitProfile.episodesObserved == 0, "V57's column, untouched by this rung")
    }

    // MARK: - 5. What it costs, asserted rather than argued

    /// THE HONEST CONSEQUENCE. A `.shadow` class carrying an inflated count was
    /// ONE gesture from `manual`; after the repair it is three — because three
    /// is what `shadowToManualObservations` has always meant, and the number on
    /// disk was not counting that.
    ///
    /// Asserted end to end through `recordCorrectObservation`, the writer that
    /// advances a per-class count, so this is the behaviour and not the arithmetic.
    @Test("a shadow class that was one gesture from manual now needs three")
    func migrationDeactivatesAShadowClassUntilRealObservationsAccrue() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Cost")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        // trust 0.5 clears `shadowToManualTrustScore` (0.4) on its own, so the
        // COUNT is the only thing deciding this promotion.
        let nearlyPromoted = """
            {"fusion":{"trustScore":0.5,"mode":"shadow","falseSkipWeight":0,"observationCount":9}}
            """
        try await store.upsertProfile(
            makeProfile(podcastId: "show-a", detectorTrustJSON: nearlyPromoted,
                        observationCount: 0, mode: "shadow", trust: 0.5, falseSignals: 0)
        )
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        let service = TrustScoringService(store: reopened)

        // One Yes. Before the repair 9 + 1 cleared the rung; now 0 + 1 does not.
        await service.recordCorrectObservation(
            podcastId: "show-a", analysisAssetId: "asset-1", detector: .fusion
        )
        #expect(
            await service.resolveDetectorModes(podcastId: "show-a").mode(for: .fusion) == .shadow,
            "one gesture must no longer buy `manual` on a count that was never episodes"
        )
        #expect(try storedEntries(in: dir, podcastId: "show-a")["fusion"]?.observationCount == 1)

        // Two more, on distinct episodes, and the class earns it honestly.
        await service.recordCorrectObservation(
            podcastId: "show-a", analysisAssetId: "asset-2", detector: .fusion
        )
        await service.recordCorrectObservation(
            podcastId: "show-a", analysisAssetId: "asset-3", detector: .fusion
        )
        #expect(
            await service.resolveDetectorModes(podcastId: "show-a").mode(for: .fusion) == .manual,
            "three real observations still promote — the rung is unchanged, only the counter is honest"
        )
        #expect(try storedEntries(in: dir, podcastId: "show-a")["fusion"]?.observationCount == 3)
    }

    // MARK: - 6. Idempotent, for the right reason

    @Test("a second launch does not reset a per-class count earned under the NEW unit")
    func migrationIsIdempotentAcrossLaunches() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Idem")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(
            makeProfile(podcastId: "show-a", detectorTrustJSON: Self.simplecastLedgerJSON)
        )
        try await rewindToV57(store)
        AnalysisStore.resetMigratedPathsForTesting()
        let first = try AnalysisStore(directory: dir)
        try await first.migrate()
        #expect(try storedEntries(in: dir, podcastId: "show-a")["fusion"]?.observationCount == 0)

        // Now let a real gesture accrue, as it would in the field.
        await TrustScoringService(store: first).recordCorrectObservation(
            podcastId: "show-a", analysisAssetId: "asset-1", detector: .fusion
        )
        #expect(try storedEntries(in: dir, podcastId: "show-a")["fusion"]?.observationCount == 1)

        // The next launch. No stamp rewind — a device that has reached 58 stays
        // there, so the ladder is what makes this a no-op.
        AnalysisStore.resetMigratedPathsForTesting()
        let second = try AnalysisStore(directory: dir)
        try await second.migrate()

        #expect(
            try storedEntries(in: dir, podcastId: "show-a")["fusion"]?.observationCount == 1,
            "a later launch must not throw away an observation counted under the new unit"
        )
    }

    /// THE LIMIT, stated rather than discovered later. Like V49 and V57, V58 has
    /// NO per-row predicate that can tell a legitimately-counted 1 from an
    /// inflated 1 — the two are the same integer, and the fact that separates
    /// them (which unit wrote it) is exactly what was never recorded. So a
    /// deliberate stamp rewind to 57 after this rung has run DOES reset a real
    /// count, and that is asserted here rather than left as a surprise.
    ///
    /// It is not a field state: nothing rewinds `schema_version`, and the two
    /// production entry points both climb monotonically.
    @Test("a deliberate stamp rewind DOES reset a real count — the stated limit")
    func aStampRewindResetsARealCount() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Rewind")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        let honest = """
            {"fusion":{"trustScore":0.5,"mode":"manual","falseSkipWeight":0,"observationCount":4}}
            """
        try await store.upsertProfile(makeProfile(podcastId: "show-a", detectorTrustJSON: honest))
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(
            try storedEntries(in: dir, podcastId: "show-a")["fusion"]?.observationCount == 0,
            "the rung cannot tell 4 episodes from 4 gestures, and does not pretend to"
        )
    }

    @Test("a ledger already at zero is not rewritten at all")
    func migrationLeavesAnAllZeroLedgerAlone() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Zero")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        // Written with a key order `JSONEncoder` would not produce, so a
        // needless rewrite is visible as a byte difference. The predicate is
        // "some entry is non-zero", so nothing should touch this row.
        let zeroJSON = #"{"fusion":{"observationCount":0,"trustScore":0.9,"mode":"auto","falseSkipWeight":0.5}}"#
        try await store.upsertProfile(makeProfile(podcastId: "show-zero", detectorTrustJSON: zeroJSON))
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try rawLedgerJSON(in: dir, podcastId: "show-zero") == zeroJSON)
    }

    // MARK: - 7. The repaired population is the READABLE one

    @Test("a ledger the decoder rejects is left exactly as it is")
    func migrationLeavesUndecodableLedgersAlone() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Corrupt")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        // Not JSON at all, and JSON whose entries are missing required keys.
        // Both are ALREADY read as an EMPTY `DetectorTrustLedger` by
        // `PodcastProfile.detectorTrustLedger`, under which every class falls
        // back to its seed — so neither carries an inflated per-class count that
        // anything can read, and neither needs a repair. Destroying them would be
        // a repair inventing work for itself.
        try await store.upsertProfile(makeProfile(podcastId: "show-garbage", detectorTrustJSON: "{not json"))
        try await store.upsertProfile(
            makeProfile(podcastId: "show-partial", detectorTrustJSON: #"{"fusion":{"observationCount":99}}"#)
        )
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try rawLedgerJSON(in: dir, podcastId: "show-garbage") == "{not json")
        #expect(try rawLedgerJSON(in: dir, podcastId: "show-partial") == #"{"fusion":{"observationCount":99}}"#)
        // …and the reason that is safe, asserted rather than asserted-about: the
        // 99 reaches no reader, because the ledger it lives in does not decode.
        let partial = try #require(await reopened.fetchProfile(podcastId: "show-partial"))
        #expect(partial.detectorTrustLedger.entries.isEmpty)
        #expect(
            partial.detectorTrustLedger.entry(for: .fusion, seededFrom: partial).observationCount
                == partial.observationCount,
            "an unreadable ledger falls back to the seed, which is the show scalar V49 already fixed"
        )
    }

    @Test("a NULL detectorTrustJSON is not given one")
    func migrationDoesNotFabricateALedger() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58Null")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(makeProfile(podcastId: "show-null", detectorTrustJSON: nil))
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(
            try rawLedgerJSON(in: dir, podcastId: "show-null") == nil,
            "a show that has never forked its ledger stays byte-identical to a pre-gard row"
        )
    }

    // MARK: - 8. The ladder still climbs

    @Test("a fixture with no podcast_profiles still reaches head")
    func migrationSkipsMissingTable() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58NoTable")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.execForTesting("DROP TABLE podcast_profiles")
        try await rewindToV57(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    @Test("V58 does NOT step over a rolled-back V39")
    func migrationDoesNotStepOverARolledBackRung() async throws {
        let dir = try makeTempDir(prefix: "DetectorTrustCountV58NoStepOver")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.upsertProfile(
            makeProfile(podcastId: "show-a", detectorTrustJSON: Self.simplecastLedgerJSON)
        )

        // The state a device is in when V39 rolled back, reproduced exactly as
        // `MergedChildRowDedupeV40MigrationTests` does: no unique asset-identity
        // index, a colliding pair V39 must delete, a trigger that makes the
        // delete ABORT, and `schema_version` at 38. Stamping 38 alone proves
        // nothing — V39 would simply succeed and the whole ladder would climb.
        try await store.execForTesting("DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try await store.execForTesting("DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint")
        try await store.execForTesting("""
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('dupe-old', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0)
            """)
        try await store.execForTesting("""
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
            VALUES ('dupe-new', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 2.0)
            """)
        try await store.execForTesting("""
            CREATE TRIGGER scc6_v39_guard BEFORE DELETE ON analysis_assets
            BEGIN SELECT RAISE(ABORT, 'v58 step-over fixture'); END
            """)
        try await store.setMetaValue(forKey: "schema_version", value: "38")

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 38, "the ladder must stay where V39 left it")
        #expect(
            try storedEntries(in: dir, podcastId: "show-a")["fusion"]?.observationCount == 16,
            "V58 must not repair a store the ladder cannot legally climb"
        )

        // And the retry a later launch performs completes BOTH rungs.
        try await store.execForTesting("DROP TRIGGER scc6_v39_guard")
        try await store.migrateOnlyForTesting()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try storedEntries(in: dir, podcastId: "show-a")["fusion"]?.observationCount == 0)
    }
}
