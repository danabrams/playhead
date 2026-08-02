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
// THE CAUSE THE INSTRUMENTATION FOUND: the pre-roll was rejected by
// playhead-xr3t's `InventorySanityFilter` as `.tooEarly`, because rule (b)
// rejected any span STARTING inside the first three seconds of the episode —
// which is every pre-roll.
//
// FIXED BY playhead-b6r2, and the tests below now pin the fix rather than the
// defect. Rule (b) reads the span's INNER edge at both ends, so a pre-roll
// passes, a post-roll passes, and a head/tail artifact lying wholly inside the
// margin band is still rejected. `theProductionFilterDropsNoFieldWindow` is
// the measurement: with the filter configured exactly as production configures
// it, zero of the four field windows are lost, where one used to be.
//
// NOTE ON THE FILTER'S CONSTRUCTION IN TESTS. `SkipOrchestrator.init` used to
// default `inventoryFilter` to `InventorySanityFilter(isEnabled: false)` while
// production passed `.production()`, which is ON — the divergence that let
// djl0's own reproduction of this field case emit the banner and pass.
// playhead-b6r2 bound the init default to
// `InventorySanityFilter.productionDefaultConfiguration`, so the two no longer
// drift. The tests below still pass `isEnabled:` explicitly, because a suite
// that means to reproduce a configuration should say which one.

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

    /// A span the inventory filter genuinely rejects: 2.0 s lying wholly
    /// inside the head margin, so rule (a) (which passes exactly 2.0 s)
    /// provably did not make the call and the reason reads `.tooEarly`.
    ///
    /// playhead-b6r2 needs this because the pre-roll no longer serves as the
    /// suite's example of an inventory-sanity drop. Three tests below assert
    /// that the census can NAME that drop; using a row the filter accepts
    /// would have turned each of them green-and-vacuous.
    static let headArtifact = (id: "artifact-head", start: 0.0, end: 2.0)

    static var headArtifactWindow: AdWindow {
        dayZeroWindow(
            id: headArtifact.id, start: headArtifact.start, end: headArtifact.end
        )
    }

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

    /// THE ANSWER, AND THEN THE FIX. This asserted that the pre-roll the
    /// listener was inside is rejected as `.tooEarly` — the cause isp5's
    /// instrumentation found. playhead-b6r2 corrected rule (b) to read the
    /// span's inner edge, so the same delivery now ARMS it, and this test
    /// asserts the corrected outcome. The geometry, the configuration and the
    /// vacuity control are untouched; only the expected disposition moved.
    ///
    /// The mid-roll assertion is the VACUITY CONTROL and is now doing more
    /// work than before: with the pre-roll also expected to arm, an
    /// orchestrator whose filter was accidentally OFF would satisfy the first
    /// expectation. What separates the two is
    /// `theProductionFilterDropsNoFieldWindow`'s companion below, which
    /// proves the filter is still rejecting in the same configuration.
    @Test("the field pre-roll is armed by the inventory filter, not dropped")
    func theFieldPreRollIsArmedNotDropped() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true
        )
        let delivered = try await Fx.runFieldDelivery(orchestrator, store: store)
        #expect(delivered == 4, "control: all four rows reached the door")

        let preRoll = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.preRoll.id
        )
        #expect(preRoll?.outcome == .armedSuggest)

        let midRoll = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.midRollA.id
        )
        #expect(midRoll?.outcome == .armedSuggest,
                "vacuity control: the delivery ran and reached the same tier")
    }

    /// The same session, read the way a device pull reads it: one durable row,
    /// naming the door, the count forwarded, and the per-cause census with the
    /// rejection REASON attached. Nothing here consults the orchestrator's
    /// in-memory state — this is the half that survives to a support ticket.
    /// playhead-b6r2 kept every structural claim here — one row per delivery,
    /// the forwarded count, the REASON rendered alongside the outcome — and
    /// moved which row carries the rejection. The field four no longer supply
    /// one, so the delivery gains an explicit head artifact; without it the
    /// "the reason is in the row" claim would have had nothing to demonstrate
    /// on and would have been deleted rather than preserved.
    @Test("the delivery leaves ONE durable census row that names the cause")
    func theDeliveryLeavesADurableCensusRow() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true, invariantLogger: logger
        )
        try await Fx.runFieldDelivery(
            orchestrator, store: store,
            windows: Fx.fieldWindows + [Fx.headArtifactWindow]
        )

        let rows = try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "isp5-field"
        )
        let ingestRows = rows.filter {
            $0.contains("door=\(AdWindowIngestDoor.midSessionIngest.rawValue)")
        }
        #expect(ingestRows.count == 1,
                "exactly one row per delivery — got \(ingestRows)")
        let row = try #require(ingestRows.first)
        #expect(row.contains("forwarded=5"))
        #expect(row.contains(
            "\(AdWindowIngestOutcome.droppedInventorySanity.rawValue):"
                + "\(InventorySanityRejectionReason.tooEarly.rawValue)=1"
        ), "the REASON is in the row, not only the outcome — got \(row)")
        #expect(row.contains("\(AdWindowIngestOutcome.armedSuggest.rawValue)=4"),
                "playhead-b6r2: the four field slots armed alongside it — got \(row)")
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

        // A. the ingest ran, and the filter dropped the row.
        //
        // playhead-b6r2: was `[Fx.fieldWindows[0]]`, the pre-roll, which the
        // corrected rule (b) admits. The claim under test is that a delivery
        // which RAN and dropped renders differently from one that never ran,
        // so arm A only has to be a genuine drop — the head artifact is one.
        let ranStore = try await makeTestStore()
        let ran = try await Fx.makeOrchestrator(
            store: ranStore, inventoryFilterEnabled: true,
            invariantLogger: ranLogger
        )
        try await Fx.runFieldDelivery(
            ran, store: ranStore, windows: [Fx.headArtifactWindow]
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

    /// THE BEAD'S VERIFICATION NUMBER. Turning the production filter on is the
    /// ONLY difference between this and `armedSuggestIsItsOwnCountedOutcome`.
    /// It used to move exactly one row from armed to dropped — the pre-roll,
    /// every episode — and that was the measurement of the field defect's
    /// reach. playhead-b6r2 says the figure must be zero, and this is where it
    /// is read.
    ///
    /// The `== 4` is not redundant with the zero: a filter that had been
    /// switched off entirely would also report zero drops, and the whole point
    /// of this bead is that a guard turned off in the observation surface is
    /// indistinguishable from a guard that works. The companion below carries
    /// the other half of that separation.
    @Test("the production filter drops none of the four field windows")
    func theProductionFilterDropsNoFieldWindow() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true
        )
        try await Fx.runFieldDelivery(orchestrator, store: store)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.armedSuggest) == 4)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.droppedInventorySanity) == 0)
    }

    /// The other half: in the SAME configuration, the filter still rejects.
    /// Without this, every "zero inventory-sanity drops" assertion in this
    /// file would be satisfied by a filter that had quietly stopped running.
    @Test("the production filter still rejects a head artifact in the same delivery")
    func theProductionFilterStillRejects() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true
        )
        try await Fx.runFieldDelivery(
            orchestrator, store: store,
            windows: Fx.fieldWindows + [Fx.headArtifactWindow]
        )
        let artifact = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.headArtifact.id
        )
        #expect(artifact?.outcome == .droppedInventorySanity)
        #expect(artifact?.detail == InventorySanityRejectionReason.tooEarly.rawValue)
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

    /// The one silence in the audit, and it is deliberate. `beginEpisode` runs
    /// on every episode start; an episode with no persisted windows cannot have
    /// lost one, so a row there would bootstrap a diagnostics session file for
    /// every episode ever opened in order to say nothing. The COUNT is still
    /// kept, so the partition stays complete and the silence is not a hole.
    @Test("a preload that read nothing counts but writes no durable row")
    func aPreloadThatReadNothingWritesNoRow() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true, invariantLogger: logger
        )
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        #expect(await orchestrator.adWindowIngestOutcomeCount(.doorDroppedNoAdmissibleRows) == 1,
                "the outcome is still counted — the silence is only about the FILE")

        let rows = try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "isp5-silent"
        )
        #expect(rows.isEmpty, "an empty store has nothing to account for — got \(rows)")
    }

    /// Both doors run the SAME admission rule, so both must be auditable. The
    /// cross-launch preload's row is what makes an episode-start loss visible
    /// without a mid-session mint ever happening.
    ///
    /// playhead-b6r2: the second expectation used to read
    /// `droppedInventorySanity=1` and was captioned "the preload loses the
    /// pre-roll for the same reason the ingest does". The shared-rule claim is
    /// the point and survives intact — it is now demonstrated in the other
    /// direction, on a delivery carrying both a row the filter admits and one
    /// it rejects, so the row still has to name the drop.
    @Test("the cross-launch preload writes its own census row")
    func theCrossLaunchPreloadWritesItsOwnRow() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true, invariantLogger: logger
        )
        try await Fx.persist(
            Fx.fieldWindows + [Fx.headArtifactWindow], in: store
        )
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )

        let rows = try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "isp5-preload"
        ).filter { $0.contains("door=\(AdWindowIngestDoor.crossLaunchPreload.rawValue)") }
        #expect(try #require(rows.first).contains("forwarded=5"))
        #expect(try #require(rows.first).contains(
            "\(AdWindowIngestOutcome.armedSuggest.rawValue)=4"
        ), "the preload arms the pre-roll for the same reason the ingest does — got \(rows)")
        #expect(try #require(rows.first).contains(
            "\(AdWindowIngestOutcome.droppedInventorySanity.rawValue)=1"
        ), "and enforces the same rule — got \(rows)")
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
    /// playhead-b6r2: the delivery gains the head artifact so the surviving
    /// count is still a `droppedInventorySanity` one. The claim — stamps are
    /// per-episode, tallies are per-process — is unchanged, but reading it off
    /// a cause that is now always zero would have made the second expectation
    /// true for the wrong reason.
    @Test("endEpisode clears the per-window stamps and keeps the counts")
    func endEpisodeClearsStampsAndKeepsCounts() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true
        )
        try await Fx.runFieldDelivery(
            orchestrator, store: store,
            windows: Fx.fieldWindows + [Fx.headArtifactWindow]
        )
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

// MARK: - 5. The retroactive sweep is counter-evidence, not silence

/// playhead-9v09 — THE SILENT RETRACTION PATH.
///
/// isp5 named every way a persisted window can ARRIVE. It did not name the way
/// one leaves after arriving: `reapplyInventoryFilterToManagedWindows` retires
/// already-admitted managed windows AND suggestions when
/// `setDeclaredChapters` / `setEpisodeDuration` hand the inventory filter a
/// context it did not have at admission time. Until this bead that retirement
/// stamped nothing, so a span armed by the cross-launch preload and swept back
/// milliseconds later left the census reading `ingest_armed_suggest = 1` with
/// no counter-evidence anywhere. The audit trail said armed, the listener saw
/// nothing, and both were true.
///
/// The ordering is the ordinary one. The PRELOAD door runs at `beginEpisode`,
/// before either setter, and `AnalysisCoordinator` calls `setEpisodeDuration`
/// immediately before `receiveAdWindows` on the hot path — so the retroactive
/// sweep is a real sequence, not a hypothetical one.
///
/// WHY IT OUTRANKS ITS SIZE. The census is now the instrument the whole
/// mid-roll program is verified through; every bead since asserts on
/// `ad_window_ingest_census` counts rather than on absence. An instrument with
/// a silent retraction path can certify a fix that did not hold — a future bead
/// asserts `ingest_armed_suggest == 2`, passes, and ships, while a duration
/// arriving from the analysis coordinator retires the window moments later.
/// That is the same defect shape isp5 was built to catch, one layer up, in the
/// catcher itself.
@Suite("A retroactively retired window is stamped, counted and readable as a balance (playhead-9v09)",
       .timeLimit(.minutes(1)))
struct AdWindowIngestRetroactiveRetirementTests {

    private typealias Fx = IngestFixture

    /// A duration that puts the field POST-ROLL inside the filter's tail band
    /// (`[duration - 3, duration]`, so `[3899.0, 3902.0]`) and leaves the other
    /// three field spans alone — mid-roll B, the next-latest, starts at 3194.5.
    /// One rejection, chosen so the count is discriminating rather than
    /// "everything went".
    private static let durationRejectingThePostRoll = 3_902.0

    /// A duration past every field span: the sweep RUNS and rejects nothing.
    private static let durationRejectingNothing = 5_000.0

    /// A second post-roll, so one sweep has two windows to retire and the row
    /// can be checked for aggregation rather than one line per window.
    private static let secondPostRoll = (id: "d0-5", start: 3_900.4, end: 3_926.0)

    private static func beginWithFieldWindows(
        _ windows: [AdWindow] = Fx.fieldWindows,
        logger: SurfaceStatusInvariantLogger
    ) async throws -> (SkipOrchestrator, AnalysisStore) {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true, invariantLogger: logger
        )
        try await Fx.persist(windows, in: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        return (orchestrator, store)
    }

    private static func sweepRows(_ rows: [String]) -> [String] {
        rows.filter {
            $0.contains(
                "door=\(AdWindowIngestDoor.retroactiveInventorySweep.rawValue)"
            )
        }
    }

    /// THE BEAD'S FIRST ACCEPTANCE. Arm a span through the preload door, then
    /// deliver a duration that makes the inventory filter reject it. The census
    /// must show BOTH the arm and the retirement, with the reason.
    ///
    /// Read the way a device pull reads it — two rows in the session file,
    /// nothing consulted in memory. The preload row is the arm; the sweep row
    /// is the counter-evidence that did not exist before this bead.
    @Test("the census shows BOTH the arm and the retirement, with the reason")
    func theCensusShowsBothTheArmAndTheRetirement() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let (orchestrator, _) = try await Self.beginWithFieldWindows(logger: logger)

        #expect(await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.postRoll.id
        )?.outcome == .armedSuggest,
        "control: the preload armed the post-roll before any duration arrived")

        await orchestrator.setEpisodeDuration(
            Self.durationRejectingThePostRoll, analysisAssetId: Fx.assetId
        )

        let rows = try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "9v09-both"
        )
        let preloadRow = try #require(rows.first {
            $0.contains("door=\(AdWindowIngestDoor.crossLaunchPreload.rawValue)")
        }, "the arm — got \(rows)")
        #expect(preloadRow.contains(
            "\(AdWindowIngestOutcome.armedSuggest.rawValue)=4"
        ), "all four field spans armed — got \(preloadRow)")

        let sweeps = Self.sweepRows(rows)
        #expect(sweeps.count == 1, "one row per sweep — got \(sweeps)")
        let sweepRow = try #require(sweeps.first)
        #expect(sweepRow.contains("forwarded=1"))
        #expect(sweepRow.contains("delivered=0"))
        #expect(sweepRow.contains("retired=1"),
                "the balance's subtrahend is on the row — got \(sweepRow)")
        #expect(sweepRow.contains(
            "\(AdWindowIngestOutcome.retiredReapplyInventoryFilter.rawValue)=1"
        ), "the retirement is named — got \(sweepRow)")
        #expect(sweepRow.contains(
            "\(AdWindowIngestOutcome.retiredReapplyInventoryFilter.rawValue):"
                + "\(InventorySanityRejectionReason.tooLate.rawValue)=1"
        ), "and carries the filter's REASON — got \(sweepRow)")
    }

    /// THE BEAD'S SECOND ACCEPTANCE, AND THE REAL TEST. A span that is armed
    /// and STAYS armed records no retirement — an outcome that fires on every
    /// delivery is exactly as useless as one that never fires.
    ///
    /// The absence assertions here are load-bearing, so each has a positive
    /// witness (playhead-le02: since playhead-d3g0 made banner emission
    /// deferred, tests asserting only "nothing was emitted" prove nothing).
    /// Witness one: the post-roll is still `armedSuggest` after the benign
    /// duration, so the sweep did not quietly take it without saying so.
    /// Witness two, and the stronger one: the SAME orchestrator, the SAME
    /// window, one more duration — and the row appears. The silence was about
    /// the geometry, not about the wiring.
    @Test("a span that stays armed records NO retirement, and the sweep is still live")
    func aSpanThatStaysArmedRecordsNoRetirement() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let (orchestrator, _) = try await Self.beginWithFieldWindows(logger: logger)

        await orchestrator.setEpisodeDuration(
            Self.durationRejectingNothing, analysisAssetId: Fx.assetId
        )

        #expect(await orchestrator.adWindowIngestOutcomeCount(
            .retiredReapplyInventoryFilter
        ) == 0)
        #expect(await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.postRoll.id
        )?.outcome == .armedSuggest,
        "witness: the span survived the sweep and is still armed")
        let quiet = Self.sweepRows(try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "9v09-quiet"
        ))
        #expect(quiet.isEmpty, "a sweep that retires nothing writes no row — got \(quiet)")

        // The witness that the mechanism was live the whole time.
        await orchestrator.setEpisodeDuration(
            Self.durationRejectingThePostRoll, analysisAssetId: Fx.assetId
        )
        #expect(await orchestrator.adWindowIngestOutcomeCount(
            .retiredReapplyInventoryFilter
        ) == 1, "same orchestrator, same window, only the duration moved")
        let loud = Self.sweepRows(try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "9v09-loud"
        ))
        #expect(loud.count == 1, "exactly the one sweep that retired — got \(loud)")
    }

    /// The sweep retires MANAGED windows as well as suggestions, and the reason
    /// it reports is the filter's own rather than a constant: this one is
    /// `overlapsDeclaredChapter`, arriving through `setDeclaredChapters`, and
    /// the window it takes back was in the AUTO-SKIP tier.
    @Test("a managed window swept by a declared chapter is named with THAT reason")
    func aManagedWindowSweptByAChapterIsNamedWithThatReason() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let (orchestrator, _) = try await Self.beginWithFieldWindows([
            Fx.dayZeroWindow(
                id: "eligible-mid",
                start: Fx.midRollA.start,
                end: Fx.midRollA.end,
                eligibilityGate: SkipEligibilityGate.eligible.rawValue
            ),
        ], logger: logger)

        #expect(await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: "eligible-mid"
        )?.outcome == .admittedManaged,
        "control: the window entered the auto-skip tier, not the suggest tier")

        await orchestrator.setDeclaredChapters(
            [
                ChapterEvidence(
                    startTime: 1_400,
                    endTime: 1_600,
                    title: "Editorial interview",
                    source: .rssInline,
                    disposition: .content,
                    qualityScore: 1
                ),
            ],
            analysisAssetId: Fx.assetId
        )

        let stamp = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: "eligible-mid"
        )
        #expect(stamp?.outcome == .retiredReapplyInventoryFilter,
                "the stamp now answers 'where did it go?' with the retirement")
        #expect(stamp?.detail
            == InventorySanityRejectionReason.overlapsDeclaredChapter.rawValue,
        "the reason is the filter's, not a constant — this one is not tooLate")

        let sweeps = Self.sweepRows(try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "9v09-chapter"
        ))
        #expect(try #require(sweeps.first).contains(
            "\(AdWindowIngestOutcome.retiredReapplyInventoryFilter.rawValue):"
                + "\(InventorySanityRejectionReason.overlapsDeclaredChapter.rawValue)=1"
        ), "got \(sweeps)")
    }

    /// One sweep, one row, aggregated — not one row per window. A reader
    /// counting rows to size a loss must get the same answer as one reading
    /// `retired=`.
    @Test("a sweep that retires two windows writes ONE row that aggregates them")
    func aSweepRetiringTwoWindowsWritesOneAggregatedRow() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let (orchestrator, _) = try await Self.beginWithFieldWindows(
            Fx.fieldWindows + [
                Fx.dayZeroWindow(
                    id: Self.secondPostRoll.id,
                    start: Self.secondPostRoll.start,
                    end: Self.secondPostRoll.end
                ),
            ],
            logger: logger
        )

        await orchestrator.setEpisodeDuration(
            Self.durationRejectingThePostRoll, analysisAssetId: Fx.assetId
        )

        let sweeps = Self.sweepRows(try drainDescriptions(
            logger, code: .adWindowIngestCensus, sentinel: "9v09-two"
        ))
        #expect(sweeps.count == 1, "one sweep, one row — got \(sweeps)")
        let row = try #require(sweeps.first)
        #expect(row.contains("forwarded=2"))
        #expect(row.contains("retired=2"))
        #expect(row.contains(
            "\(AdWindowIngestOutcome.retiredReapplyInventoryFilter.rawValue)=2"
        ), "got \(row)")
        #expect(row.contains(
            "\(AdWindowIngestOutcome.retiredReapplyInventoryFilter.rawValue):"
                + "\(InventorySanityRejectionReason.tooLate.rawValue)=2"
        ), "both reasons aggregate into one tally — got \(row)")
    }

    /// THE BALANCE, read off the process-lifetime counters the way a field
    /// investigation reads them: armed minus retired is what the listener could
    /// actually have seen. The survivor check is what stops that being an
    /// accounting identity — the subtraction has to name real windows.
    @Test("armed minus retired is what the listener could have seen")
    func armedMinusRetiredIsWhatTheListenerCouldHaveSeen() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let (orchestrator, _) = try await Self.beginWithFieldWindows(logger: logger)

        await orchestrator.setEpisodeDuration(
            Self.durationRejectingThePostRoll, analysisAssetId: Fx.assetId
        )

        let armed = await orchestrator.adWindowIngestOutcomeCount(.armedSuggest)
        let retired = await orchestrator.adWindowIngestOutcomeCount(
            .retiredReapplyInventoryFilter
        )
        #expect(armed == 4)
        #expect(retired == 1)
        #expect(armed - retired == 3)

        for id in [Fx.preRoll.id, Fx.midRollA.id, Fx.midRollB.id] {
            #expect(await orchestrator.lastAdWindowIngestOutcome(
                forWindowId: id
            )?.outcome == .armedSuggest,
            "\(id) is one of the three the balance says survived")
        }
        #expect(await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.postRoll.id
        )?.outcome == .retiredReapplyInventoryFilter,
        "and the post-roll is the one it says did not")
    }

    /// The counter is a process-lifetime measurement like every other ingest
    /// tally (djl0's precedent), while the per-window stamp is per-episode.
    /// Without this, a retraction rate could never be read across a session.
    @Test("endEpisode clears the retirement stamp and keeps the count")
    func endEpisodeClearsTheStampAndKeepsTheCount() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let (orchestrator, _) = try await Self.beginWithFieldWindows(logger: logger)
        await orchestrator.setEpisodeDuration(
            Self.durationRejectingThePostRoll, analysisAssetId: Fx.assetId
        )
        #expect(await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.postRoll.id
        ) != nil, "control: the stamp was there before endEpisode")

        await orchestrator.endEpisode()

        #expect(await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.postRoll.id
        ) == nil)
        #expect(await orchestrator.adWindowIngestOutcomeCount(
            .retiredReapplyInventoryFilter
        ) == 1, "the per-cause tally is a process-lifetime measurement")
    }
}

// MARK: - 6. The taxonomy itself

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

    /// playhead-9v09: exactly one outcome TAKES BACK a window that had already
    /// been admitted. This is the subtrahend of the census balance, so a case
    /// drifting into the retraction bucket by accident would deflate every
    /// future `delivered − retired` figure in the field logs — the same
    /// numerator check `exactlyThreeOutcomesCountAsDelivered` applies to the
    /// other side of the subtraction.
    @Test("exactly one outcome is a retraction")
    func exactlyOneOutcomeIsARetraction() {
        let retractions = Set(
            AdWindowIngestOutcome.allCases.filter(\.isRetraction)
        )
        #expect(retractions == [.retiredReapplyInventoryFilter])
    }

    /// A retraction is the negation of a delivery, and it names ONE window
    /// rather than a whole call. Three independent switches; only a test stops
    /// them disagreeing.
    @Test("a retraction is neither a delivery nor a door outcome")
    func aRetractionIsNeitherADeliveryNorADoorOutcome() {
        for outcome in AdWindowIngestOutcome.allCases where outcome.isRetraction {
            #expect(!outcome.isDelivered,
                    "\(outcome.rawValue) both delivers and retracts")
            #expect(!outcome.isDoorOutcome,
                    "\(outcome.rawValue) is both a whole call and one window")
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

    /// playhead-9v09: the retraction row renders `retired=` and a delivery row
    /// does NOT — the one deliberate asymmetry with `delivered=`.
    ///
    /// A delivery row is written the instant the delivery concludes, and a
    /// sweep milliseconds later can still retract what it just armed, so
    /// `retired=0` on a delivery row would be a claim the row is in no position
    /// to make. Absence here means "this row has nothing to say about
    /// retraction", not "nothing was retracted" — which is why the sweep gets
    /// its own row rather than a field on someone else's.
    @Test("a retraction row renders retired= and a delivery row does not")
    func aRetractionRowRendersRetiredAndADeliveryRowDoesNot() {
        let sweep = AdWindowIngestCensus(
            door: .retroactiveInventorySweep,
            analysisAssetId: "asset-1",
            forwarded: 2,
            counts: [.retiredReapplyInventoryFilter: 2],
            details: [
                "\(AdWindowIngestOutcome.retiredReapplyInventoryFilter.rawValue)"
                    + ":\(InventorySanityRejectionReason.tooLate.rawValue)": 2,
            ]
        )
        #expect(sweep.retiredCount == 2)
        #expect(sweep.auditDescription ==
            "door=retroactive_inventory_sweep asset=asset-1 forwarded=2 "
            + "delivered=0 retired=2 "
            + "ingest_retired_reapplied_inventory_filter=2 "
            + "ingest_retired_reapplied_inventory_filter:tooLate=2")

        let delivery = AdWindowIngestCensus(
            door: .crossLaunchPreload,
            analysisAssetId: "asset-1",
            forwarded: 1,
            counts: [.armedSuggest: 1],
            details: [:]
        )
        #expect(delivery.retiredCount == 0)
        #expect(!delivery.auditDescription.contains("retired="),
                "a delivery row cannot honestly claim retired=0 — got \(delivery.auditDescription)")
    }
}
