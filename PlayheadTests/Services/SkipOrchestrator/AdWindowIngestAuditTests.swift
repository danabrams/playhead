// AdWindowIngestAuditTests.swift
// playhead-isp5 — the persisted-ad-window delivery path leaves evidence.
//
// THE FIELD CASE. 2026-08-01 03:20, episode D9B513CD. Day-0 rediff fired 14 s
// into a first listen and WORKED: four divergent slots, four marks, 120.3 MB
// fetched, all persisted at confidence 1.00 with `eligibilityGate = markOnly`.
// The first window is the pre-roll, 0.0–45.1 s, and the playhead was INSIDE it
// when the mint landed. No skip (correct: mark-only never auto-skips) and NO
// BANNER (not correct, and not explained by two investigations).
//
// WHY IT STAYED UNEXPLAINED. `ingestPersistedAdWindows` reported its drop to
// `os_log`, a channel no device pull retrieves, and `receiveAdWindows` has
// nineteen terminal dispositions that were mutually indistinguishable from the
// outside — and indistinguishable from "the ingest never ran". So the two
// candidate stories, "the delivery never happened" and "the delivery happened
// and every row was filtered", left byte-identical evidence.
//
// WHAT THESE TESTS PIN. Each disposition now has a name, a process-lifetime
// count, and a durable `ad_window_ingest_census` row in the
// `SurfaceStatusInvariantLogger` session file the diagnostics bundle already
// ships — the channel playhead-djl0 (#317) established and playhead-v7q6
// (#316) settled as THE audit trail.
//
// THE CAUSE THE INSTRUMENTATION FOUND, pinned by
// `theFieldPreRollIsDroppedByTheInventoryFilterAsTooEarly` below: the pre-roll
// is rejected by playhead-xr3t's `InventorySanityFilter` as `.tooEarly`,
// because rule (b) rejects any span starting inside the first three seconds of
// the episode — which is every pre-roll. That test characterises TODAY'S
// production behaviour; it is not an endorsement of it. Whether an ad span
// abutting the episode edge should be treated as invalid is a policy question
// for Dan (see the follow-up bead), and flipping it changes AUTO-SKIP
// admission for every detector, not only the banner.
//
// NOTE ON THE FILTER'S CONSTRUCTION IN TESTS. `SkipOrchestrator.init` defaults
// `inventoryFilter` to `InventorySanityFilter(isEnabled: false)`; production
// passes `.production()`, which is ON. That divergence is exactly why djl0's
// own reproduction of this field case emitted the banner and passed. Every
// test below that means to reproduce PRODUCTION says `isEnabled: true`
// explicitly.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum IngestFixture {

    static let assetId = "asset-1"
    static let episodeId = "ep-1"
    static let podcastId = "podcast-1"

    /// The four spans day-0 minted for D9B513CD. `preRoll` is the one the
    /// listener was inside.
    static let preRoll = (id: "d0-1", start: 0.0, end: 45.1)
    static let midRollA = (id: "d0-2", start: 1_436.4, end: 1_508.1)
    static let midRollB = (id: "d0-3", start: 3_194.5, end: 3_371.2)
    static let postRoll = (id: "d0-4", start: 3_899.8, end: 3_929.9)

    /// - Parameter inventoryFilterEnabled: `true` reproduces PRODUCTION
    ///   (`InventorySanityFilter.production()` is ON by default). `false` is
    ///   the `SkipOrchestrator.init` default that every other orchestrator
    ///   suite silently inherits.
    static func makeOrchestrator(
        store: AnalysisStore,
        inventoryFilterEnabled: Bool,
        invariantLogger: SurfaceStatusInvariantLogger = SurfaceStatusInvariantLogger()
    ) async throws -> SkipOrchestrator {
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        return SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store),
            invariantLogger: invariantLogger,
            inventoryFilter: InventorySanityFilter(
                isEnabled: inventoryFilterEnabled
            )
        )
    }

    /// A row shaped exactly like what `mintByteExactDayZeroMarks` persists for
    /// a segment-recovered (non-strict) slot: confidence 1.00, byte-exact
    /// provenance, `markOnly`, unanchored on both edges, `candidate`.
    static func dayZeroWindow(
        id: String,
        start: Double,
        end: Double,
        eligibilityGate: String = SkipEligibilityGate.markOnly.rawValue,
        confidence: Double = 1.0,
        decisionState: String = AdDecisionState.candidate.rawValue
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: decisionState,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: eligibilityGate,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    static let fieldWindows: [AdWindow] = [
        dayZeroWindow(id: preRoll.id, start: preRoll.start, end: preRoll.end),
        dayZeroWindow(id: midRollA.id, start: midRollA.start, end: midRollA.end),
        dayZeroWindow(id: midRollB.id, start: midRollB.start, end: midRollB.end),
        dayZeroWindow(id: postRoll.id, start: postRoll.start, end: postRoll.end),
    ]

    static func persist(_ windows: [AdWindow], in store: AnalysisStore) async throws {
        try await store.upsertHotPathAdWindows(
            windows, existingIDs: [], retiredIDs: []
        )
    }

    /// Reproduce the field session up to the instant the mint lands: episode
    /// started, playhead 14 s in, rows on disk, mid-session ingest fired.
    @discardableResult
    static func runFieldDelivery(
        _ orchestrator: SkipOrchestrator,
        store: AnalysisStore,
        windows: [AdWindow] = fieldWindows
    ) async throws -> Int {
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, episodeId: episodeId, podcastId: podcastId
        )
        await orchestrator.updatePlayheadTime(14)
        try await persist(windows, in: store)
        return await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: assetId
        )
    }
}

/// A logger writing into a per-test temporary directory, plus its cleanup.
private func makeScopedInvariantLogger() -> (SurfaceStatusInvariantLogger, URL) {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("isp5-\(UUID().uuidString)", isDirectory: true)
    return (SurfaceStatusInvariantLogger(directory: directory), directory)
}

/// Drain the logger's JSON Lines session file, returning the descriptions of
/// every entry carrying `code`.
///
/// The sentinel is enqueued AFTER the writes under test on the SAME serial
/// queue, so its arrival proves those writes have landed — which turns "no
/// census row arrived" into a positive observation instead of a race.
private func drainDescriptions(
    _ logger: SurfaceStatusInvariantLogger,
    code: InvariantViolation.Code,
    sentinel: String
) throws -> [String] {
    logger.invariantViolated(code: .unknown, description: sentinel)
    var entries: [SurfaceStateTransitionEntry] = []
    for _ in 0..<10 {
        logger.flushForTesting()
        guard let sessionURL = logger.currentSessionFileURL,
              let data = try? Data(contentsOf: sessionURL) else { continue }
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        entries = String(decoding: data, as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
            .compactMap {
                try? decoder.decode(
                    SurfaceStateTransitionEntry.self, from: Data($0.utf8)
                )
            }
        if entries.contains(where: {
            $0.invariantViolation?.description == sentinel
        }) { break }
    }
    return entries
        .compactMap(\.invariantViolation)
        .filter { $0.code == code }
        .map(\.description)
}

// MARK: - 1. The field case, named

@Suite("The 2026-08-01 delivery names where each window went (playhead-isp5)",
       .timeLimit(.minutes(1)))
struct AdWindowIngestFieldCaseTests {

    private typealias Fx = IngestFixture

    /// THE ANSWER. With the filter configured as PRODUCTION configures it, the
    /// pre-roll the listener was inside is rejected by the inventory sanity
    /// filter as `.tooEarly` — it starts at 0.0 s and rule (b) rejects any span
    /// starting inside the first three seconds.
    ///
    /// The mid-roll assertion is the VACUITY CONTROL. Without it a delivery
    /// that dropped everything — or never ran at all — would satisfy the
    /// pre-roll expectation just as well, which is precisely the ambiguity this
    /// bead exists to remove.
    @Test("the field pre-roll is dropped by the inventory filter as tooEarly")
    func theFieldPreRollIsDroppedByTheInventoryFilterAsTooEarly() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true
        )
        let delivered = try await Fx.runFieldDelivery(orchestrator, store: store)
        #expect(delivered == 4, "control: all four rows reached the door")

        let preRoll = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.preRoll.id
        )
        #expect(preRoll?.outcome == .droppedInventorySanity)
        #expect(preRoll?.detail == InventorySanityRejectionReason.tooEarly.rawValue)

        let midRoll = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.midRollA.id
        )
        #expect(midRoll?.outcome == .armedSuggest,
                "vacuity control: the delivery ran and the filter is not rejecting everything")
    }

    /// The same session, read the way a device pull reads it: one durable row,
    /// naming the door, the count forwarded, and the per-cause census with the
    /// rejection REASON attached. Nothing here consults the orchestrator's
    /// in-memory state — this is the half that survives to a support ticket.
    @Test("the delivery leaves ONE durable census row that names the cause")
    func theDeliveryLeavesADurableCensusRow() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true, invariantLogger: logger
        )
        try await Fx.runFieldDelivery(orchestrator, store: store)

        let rows = try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "isp5-field"
        )
        let ingestRows = rows.filter {
            $0.contains("door=\(AdWindowIngestDoor.midSessionIngest.rawValue)")
        }
        #expect(ingestRows.count == 1,
                "exactly one row per delivery — got \(ingestRows)")
        let row = try #require(ingestRows.first)
        #expect(row.contains("forwarded=4"))
        #expect(row.contains(
            "\(AdWindowIngestOutcome.droppedInventorySanity.rawValue):"
                + "\(InventorySanityRejectionReason.tooEarly.rawValue)=1"
        ), "the REASON is in the row, not only the outcome — got \(row)")
    }

    /// The two stories the field evidence could not tell apart. Same asset,
    /// same rows, one difference: whether the minted asset is the one playing.
    /// If these two rows ever render the same string the defect is back.
    @Test("a delivery that never ran and one that ran and dropped are DISTINGUISHABLE")
    func aDeliveryThatNeverRanIsDistinguishableFromOneThatDropped() async throws {
        let (ranLogger, ranDir) = makeScopedInvariantLogger()
        let (missedLogger, missedDir) = makeScopedInvariantLogger()
        defer {
            try? FileManager.default.removeItem(at: ranDir)
            try? FileManager.default.removeItem(at: missedDir)
        }

        // A. the ingest ran, and the filter dropped the pre-roll.
        let ranStore = try await makeTestStore()
        let ran = try await Fx.makeOrchestrator(
            store: ranStore, inventoryFilterEnabled: true,
            invariantLogger: ranLogger
        )
        try await Fx.runFieldDelivery(
            ran, store: ranStore, windows: [Fx.fieldWindows[0]]
        )

        // B. the ingest fired for an episode that is not the one playing.
        let missedStore = try await makeTestStore()
        let missed = try await Fx.makeOrchestrator(
            store: missedStore, inventoryFilterEnabled: true,
            invariantLogger: missedLogger
        )
        await missed.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        let missedCount = await missed.ingestPersistedAdWindows(
            analysisAssetId: "some-other-asset"
        )
        #expect(missedCount == 0, "control: nothing was forwarded")

        let ranRows = try drainDescriptions(
            ranLogger, code: .adWindowIngestCensus, sentinel: "isp5-ran"
        ).filter { $0.contains("door=\(AdWindowIngestDoor.midSessionIngest.rawValue)") }
        let missedRows = try drainDescriptions(
            missedLogger, code: .adWindowIngestCensus, sentinel: "isp5-missed"
        ).filter { $0.contains("door=\(AdWindowIngestDoor.midSessionIngest.rawValue)") }

        #expect(try #require(ranRows.first).contains(
            "\(AdWindowIngestOutcome.droppedInventorySanity.rawValue)=1"
        ))
        #expect(try #require(missedRows.first).contains(
            "\(AdWindowIngestOutcome.doorDroppedNotPlaying.rawValue)=1"
        ))
        #expect(ranRows.first != missedRows.first,
                "if these ever compare equal the defect is back")
    }
}

// MARK: - 2. Each outcome is counted per cause

@Suite("Ingest outcomes are counted per cause (playhead-isp5)",
       .timeLimit(.minutes(1)))
struct AdWindowIngestOutcomeCountTests {

    private typealias Fx = IngestFixture

    /// Arming is a POSITIVE outcome with its own name, not merely the absence
    /// of a drop. With the filter off (the `init` default) every one of the
    /// four field rows arms.
    @Test("accepted-and-armed is its own counted outcome")
    func armedSuggestIsItsOwnCountedOutcome() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: false
        )
        try await Fx.runFieldDelivery(orchestrator, store: store)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.armedSuggest) == 4)
    }

    /// The vacuity control for the whole counter API: a cause that did not
    /// happen must read zero. A counter that reports every cause as non-zero
    /// is as useless as one that reports none.
    @Test("a cause that did not occur counts zero")
    func aCauseThatDidNotOccurCountsZero() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: false
        )
        try await Fx.runFieldDelivery(orchestrator, store: store)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.droppedInventorySanity) == 0)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.doorDroppedNotPlaying) == 0)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.droppedUserReverted) == 0)
    }

    /// Turning the production filter on is the ONLY difference between this
    /// and `armedSuggestIsItsOwnCountedOutcome`, and it moves exactly one row
    /// from armed to dropped. This is the measurement of the field defect's
    /// reach: one window per episode, always the pre-roll.
    @Test("the production filter moves exactly the pre-roll from armed to dropped")
    func theProductionFilterMovesExactlyThePreRoll() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true
        )
        try await Fx.runFieldDelivery(orchestrator, store: store)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.armedSuggest) == 3)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.droppedInventorySanity) == 1)
    }

    /// A gate that BLOCKS and a gate that asks are different names. Both leave
    /// the listener without an auto-skip, which is why the old evidence could
    /// not separate them.
    @Test("a blocked gate and a mark-only gate get different names")
    func aBlockedGateAndAMarkOnlyGateGetDifferentNames() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: false
        )
        try await Fx.runFieldDelivery(orchestrator, store: store, windows: [
            Fx.dayZeroWindow(id: "mark", start: 100, end: 160),
            Fx.dayZeroWindow(
                id: "blocked", start: 200, end: 260,
                eligibilityGate: SkipEligibilityGate.blockedByPolicy.rawValue
            ),
        ])
        let mark = await orchestrator.lastAdWindowIngestOutcome(forWindowId: "mark")
        let blocked = await orchestrator.lastAdWindowIngestOutcome(forWindowId: "blocked")
        #expect(mark?.outcome == .armedSuggest)
        #expect(blocked?.outcome == .droppedBlockedGate)
    }

    /// A row admitted to the AUTO-SKIP tier is a third named outcome, distinct
    /// from both arming and every drop.
    @Test("a window admitted to the managed tier is its own outcome")
    func anAdmittedManagedWindowIsItsOwnOutcome() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: false
        )
        try await Fx.runFieldDelivery(orchestrator, store: store, windows: [
            Fx.dayZeroWindow(
                id: "eligible", start: 100, end: 160,
                eligibilityGate: SkipEligibilityGate.eligible.rawValue
            ),
        ])
        let admitted = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: "eligible"
        )
        #expect(admitted?.outcome == .admittedManaged)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.admittedManaged) == 1)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.armedSuggest) == 0,
                "vacuity control: the eligible row did NOT also arm a suggestion")
    }
}

// MARK: - 3. The door outcomes

@Suite("A delivery that never reached the window loop still says so (playhead-isp5)",
       .timeLimit(.minutes(1)))
struct AdWindowIngestDoorOutcomeTests {

    private typealias Fx = IngestFixture

    /// The remaining candidate the bead named first: did the ingest run at all?
    /// It is now a counted, durable fact rather than an `os_log` line.
    @Test("an ingest for an episode that is not playing is counted, not just logged")
    func anIngestForAnUnplayedEpisodeIsCounted() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true
        )
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        _ = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: "some-other-asset"
        )
        #expect(await orchestrator.adWindowIngestOutcomeCount(.doorDroppedNotPlaying) == 1)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.droppedForeignAsset) == 0,
                "vacuity control: the door refused BEFORE the window loop")
    }

    /// "The store had nothing admissible" and "the ingest never fired" are
    /// different facts. The census carries how many rows were READ, so a floor
    /// that rejected everything is separable from an empty table.
    @Test("no admissible rows records how many rows were read")
    func noAdmissibleRowsRecordsHowManyWereRead() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true, invariantLogger: logger
        )
        // A row below the 0.7 preload confidence floor: present on disk, never
        // admissible.
        try await Fx.runFieldDelivery(orchestrator, store: store, windows: [
            Fx.dayZeroWindow(id: "weak", start: 100, end: 160, confidence: 0.1),
        ])
        #expect(await orchestrator.adWindowIngestOutcomeCount(.doorDroppedNoAdmissibleRows) >= 1)

        let rows = try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "isp5-empty"
        ).filter { $0.contains("door=\(AdWindowIngestDoor.midSessionIngest.rawValue)") }
        #expect(try #require(rows.first).contains(
            "\(AdWindowIngestOutcome.doorDroppedNoAdmissibleRows.rawValue):read=1"
        ), "the row separates an empty table from a filter that rejected everything — got \(rows)")
    }

    /// Both doors run the SAME admission rule, so both must be auditable. The
    /// cross-launch preload's row is what makes an episode-start loss visible
    /// without a mid-session mint ever happening.
    @Test("the cross-launch preload writes its own census row")
    func theCrossLaunchPreloadWritesItsOwnRow() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true, invariantLogger: logger
        )
        try await Fx.persist(Fx.fieldWindows, in: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )

        let rows = try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "isp5-preload"
        ).filter { $0.contains("door=\(AdWindowIngestDoor.crossLaunchPreload.rawValue)") }
        #expect(try #require(rows.first).contains("forwarded=4"))
        #expect(try #require(rows.first).contains(
            "\(AdWindowIngestOutcome.droppedInventorySanity.rawValue)=1"
        ), "the preload loses the pre-roll for the same reason the ingest does — got \(rows)")
    }
}

// MARK: - 4. Lifetime rules

@Suite("Ingest stamps are per-episode, counts are per-process (playhead-isp5)",
       .timeLimit(.minutes(1)))
struct AdWindowIngestLifetimeTests {

    private typealias Fx = IngestFixture

    /// djl0's precedent, applied: the per-window stamp describes the CURRENT
    /// episode and must not outlive it, while the per-cause tally is a
    /// build-level measurement and must.
    @Test("endEpisode clears the per-window stamps and keeps the counts")
    func endEpisodeClearsStampsAndKeepsCounts() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true
        )
        try await Fx.runFieldDelivery(orchestrator, store: store)
        #expect(await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.preRoll.id
        ) != nil, "control: the stamp was there before endEpisode")

        await orchestrator.endEpisode()

        #expect(await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.preRoll.id
        ) == nil)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.droppedInventorySanity) == 1,
                "the per-cause tally is a process-lifetime measurement")
    }
}

// MARK: - 5. The taxonomy itself

@Suite("The ingest taxonomy partitions its cases (playhead-isp5)",
       .timeLimit(.minutes(1)))
struct AdWindowIngestTaxonomyTests {

    /// Exactly three outcomes mean the window reached somewhere a listener can
    /// eventually see it. A new case landing in the delivered bucket by
    /// accident would inflate every future `delivered=` figure in the field
    /// logs — the "what would this read if the thing never happened?" check,
    /// applied to the audit row's own numerator.
    @Test("exactly three outcomes count as delivered")
    func exactlyThreeOutcomesCountAsDelivered() {
        let delivered = Set(
            AdWindowIngestOutcome.allCases.filter(\.isDelivered)
        )
        #expect(delivered == [.admittedManaged, .armedSuggest, .retainedAppliedReceipt])
    }

    /// A door outcome describes a whole CALL and a per-window outcome
    /// describes one row; mixing them would make `forwarded` and the census
    /// counts incommensurable.
    @Test("exactly four outcomes are door outcomes")
    func exactlyFourOutcomesAreDoorOutcomes() {
        let door = Set(AdWindowIngestOutcome.allCases.filter(\.isDoorOutcome))
        #expect(door == [
            .doorDroppedNotPlaying, .doorDroppedStoreReadFailed,
            .doorDroppedEpisodeReplaced, .doorDroppedNoAdmissibleRows,
        ])
    }

    /// No door outcome may also be a delivery. The two classifiers are
    /// independent switches, so nothing but a test stops them disagreeing.
    @Test("no door outcome is also a delivered outcome")
    func noDoorOutcomeIsAlsoDelivered() {
        for outcome in AdWindowIngestOutcome.allCases where outcome.isDoorOutcome {
            #expect(!outcome.isDelivered, "\(outcome.rawValue) is both")
        }
    }

    /// Raw values are the audit key that a field log is grepped by — a
    /// collision would silently merge two causes into one bucket.
    @Test("every outcome raw value is unique")
    func everyOutcomeRawValueIsUnique() {
        let raws = AdWindowIngestOutcome.allCases.map(\.rawValue)
        #expect(Set(raws).count == raws.count)
    }

    /// The row is grepped, so its rendering must be stable and must not lose a
    /// non-zero count. Sorted by raw value so two identical deliveries render
    /// byte-identically across sessions.
    @Test("the census renders every non-zero count in a stable order")
    func censusRendersEveryNonZeroCountInStableOrder() {
        let census = AdWindowIngestCensus(
            door: .midSessionIngest,
            analysisAssetId: "asset-1",
            forwarded: 4,
            counts: [.armedSuggest: 3, .droppedInventorySanity: 1],
            details: ["\(AdWindowIngestOutcome.droppedInventorySanity.rawValue):tooEarly": 1]
        )
        #expect(census.auditDescription ==
            "door=mid_session_ingest asset=asset-1 forwarded=4 delivered=3 "
            + "ingest_armed_suggest=3 ingest_dropped_inventory_sanity=1 "
            + "ingest_dropped_inventory_sanity:tooEarly=1")
    }

    /// `delivered` is the one number a reader will trust at a glance, so it
    /// must read zero when nothing landed. A denominator-shaped mistake here
    /// would make a total loss look like a partial one.
    @Test("a census where nothing landed reads delivered=0")
    func aCensusWhereNothingLandedReadsZero() {
        let census = AdWindowIngestCensus(
            door: .midSessionIngest,
            analysisAssetId: "asset-1",
            forwarded: 4,
            counts: [.droppedInventorySanity: 4],
            details: [:]
        )
        #expect(census.deliveredCount == 0)
        #expect(census.auditDescription.contains("delivered=0"))
    }
}
