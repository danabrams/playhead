// SemanticSweepMarkSurfacingTests.swift
// playhead-y3ya — the semantic verdict reaches the listener, MEASURED on the
// census rather than inferred from the absence of a drop.
//
// `SemanticSweepMarkComposerTests` pins what the composer emits. This file pins
// that what it emits actually ARMS: the marks go through the SAME door day-0
// rediff marks use (`ingestPersistedAdWindows` → `forwardPersistedAdWindows` →
// `receiveAdWindows` → the markOnly branch → `registerSuggestedWindow`), and the
// evidence is playhead-isp5's `ad_window_ingest_census`.
//
// WHY THE CENSUS AND NOT A COUNT OF BANNERS. isp5 exists because "the ingest
// never ran" and "the ingest ran and refused every row" left byte-identical
// evidence for two investigations. Asserting `ingest_armed_suggest` incremented
// is a positive observation; asserting "nothing was dropped" is satisfiable by a
// delivery that never happened. Every absence asserted here therefore ships with
// a row that MUST still drop in the same call.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum SweepSurfaceFixture {

    static let assetId = "asset-de0784d8"
    static let episodeId = "ep-de0784d8"
    static let podcastId = "podcast-doac"

    /// The episode duration DE0784D8 must have had, so the inventory filter's
    /// tail rule is live rather than dormant (the playhead-b6r2 lesson: a
    /// fixture without a duration silently disables half the filter).
    static let episodeDuration = 3_578.0

    static let firstVerdict = (start: 508.0, end: 599.0)
    static let secondVerdict = (start: 1_604.0, end: 1_731.0)

    static func scanRow(
        id: String,
        start: Double,
        end: Double,
        disposition: CoarseDisposition = .containsAd
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: disposition,
            // playhead-92im: the support payload the field's own DE0784D8
            // 508–599 s row carries. A `containsAd` row with `spansJSON: "[]"`
            // means the model returned NO support, which now grades the mark at
            // the floor — 0 of the 55 `containsAd` rows in the 2026-08-10 pull
            // are in that state, so using it here modelled a row production
            // does not write and would have made the preload arming below test
            // the unevidenced case by accident.
            spansJSON: disposition == .containsAd
                ? #"{"supportLineRefs":[17,18,20],"certainty":"strong"}"#
                : "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "y3ya"),
            transcriptVersion: "tv-1"
        )
    }

    static var fieldScanRows: [SemanticScanResult] {
        [
            scanRow(id: "scan-1", start: firstVerdict.start, end: firstVerdict.end),
            scanRow(id: "scan-2", start: secondVerdict.start, end: secondVerdict.end),
        ]
    }

    static func composedFieldMarks(existing: [AdWindow] = []) -> [AdWindow] {
        SemanticSweepMarkComposer.compose(
            scanRows: fieldScanRows,
            existingWindows: existing,
            analysisAssetId: assetId
        )
    }

    /// A span the inventory filter genuinely rejects: 2.0 s lying wholly inside
    /// the head margin, so rule (a)'s 2.0 s floor provably did not make the call
    /// and the reason reads `.tooEarly`. This is the VACUITY CONTROL — it rides
    /// in the same delivery as the sweep marks so "zero drops" cannot be
    /// satisfied by a filter, or a door, that quietly stopped running.
    static let headArtifact = (id: "artifact-head", start: 0.0, end: 2.0)

    static var headArtifactWindow: AdWindow {
        AdWindow(
            id: headArtifact.id,
            analysisAssetId: assetId,
            startTime: headArtifact.start,
            endTime: headArtifact.end,
            confidence: 1.0,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: headArtifact.start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    static func makeOrchestrator(
        store: AnalysisStore,
        invariantLogger: SurfaceStatusInvariantLogger = SurfaceStatusInvariantLogger()
    ) async throws -> SkipOrchestrator {
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                episodeDurationSec: episodeDuration
            )
        )
        return SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store),
            invariantLogger: invariantLogger,
            // Explicitly PRODUCTION. b6r2 bound the init default to the same
            // configuration, but a suite reproducing a field session should say
            // which one it means.
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
    }

    /// Reproduce the session up to the instant the sweep marks land: episode
    /// started, playhead before the first verdict, rows on disk, mid-session
    /// ingest fired.
    @discardableResult
    static func deliver(
        _ windows: [AdWindow],
        to orchestrator: SkipOrchestrator,
        store: AnalysisStore,
        playheadTime: TimeInterval = 14
    ) async throws -> Int {
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, episodeId: episodeId, podcastId: podcastId
        )
        await orchestrator.setEpisodeDuration(episodeDuration, analysisAssetId: assetId)
        await orchestrator.updatePlayheadTime(playheadTime)
        try await store.upsertHotPathAdWindows(windows, existingIDs: [], retiredIDs: [])
        return await orchestrator.ingestPersistedAdWindows(analysisAssetId: assetId)
    }
}

/// Drain the logger's JSON Lines session file, returning the descriptions of
/// every entry carrying `code`. The sentinel is enqueued AFTER the writes under
/// test on the SAME serial queue, so its arrival turns "no census row arrived"
/// into a positive observation instead of a race.
private func drainSweepCensusDescriptions(
    _ logger: SurfaceStatusInvariantLogger,
    sentinel: String
) -> [String] {
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
                try? decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8))
            }
        if entries.contains(where: { $0.invariantViolation?.description == sentinel }) {
            break
        }
    }
    return entries
        .compactMap(\.invariantViolation)
        .filter { $0.code == .adWindowIngestCensus }
        .map(\.description)
}

private func makeScopedSweepLogger() -> (SurfaceStatusInvariantLogger, URL) {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("y3ya-\(UUID().uuidString)", isDirectory: true)
    return (SurfaceStatusInvariantLogger(directory: directory), directory)
}

// MARK: - 1. The verdict arms

@Suite("A semantic verdict arms a suggest candidate (playhead-y3ya)",
       .timeLimit(.minutes(1)))
struct SemanticSweepArmsSuggestTests {

    private typealias Fx = SweepSurfaceFixture

    /// THE BEAD'S ACCEPTANCE, at the surface. Both DE0784D8 verdicts arm.
    @Test("both field verdicts arm the suggest tier")
    func bothFieldVerdictsArm() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        let marks = Fx.composedFieldMarks()
        #expect(marks.count == 2, "control: the composer produced the marks to deliver")

        try await Fx.deliver(marks, to: orchestrator, store: store)

        for mark in marks {
            let outcome = await orchestrator.lastAdWindowIngestOutcome(forWindowId: mark.id)
            #expect(outcome?.outcome == .armedSuggest, "window \(mark.id)")
        }
    }

    /// The count isp5 named as "the success case the 2026-08-01 field episode
    /// expected and did not get". Asserted on the counter, not on a log line.
    @Test("the ingest census counts two armed suggestions")
    func theCensusCountsTwoArmedSuggestions() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)

        try await Fx.deliver(Fx.composedFieldMarks(), to: orchestrator, store: store)

        let armed = await orchestrator.adWindowIngestOutcomeCount(.armedSuggest)
        #expect(armed == 2)
    }

    /// VACUITY CONTROL, and the reason it is in the same delivery: an
    /// orchestrator whose door had stopped running, or whose filter had been
    /// disabled, satisfies "two armed" only by accident. The head artifact MUST
    /// still be rejected, and rejected for the stated reason.
    @Test("a head artifact in the same delivery is still dropped as tooEarly")
    func theSameDeliveryStillDrops() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)

        try await Fx.deliver(
            Fx.composedFieldMarks() + [Fx.headArtifactWindow],
            to: orchestrator,
            store: store
        )

        let dropped = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.headArtifact.id
        )
        #expect(dropped?.outcome == .droppedInventorySanity)
        #expect(dropped?.detail == InventorySanityRejectionReason.tooEarly.rawValue)
    }

    /// A sweep mark is mark-only, so it must reach the SUGGEST tier and NOT the
    /// managed auto-skip set. `.armedSuggest` already implies the branch taken,
    /// but the count of managed admissions is the independent read.
    @Test("a sweep mark never enters the managed auto-skip set")
    func aSweepMarkNeverBecomesManaged() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)

        try await Fx.deliver(Fx.composedFieldMarks(), to: orchestrator, store: store)

        let managed = await orchestrator.adWindowIngestOutcomeCount(.admittedManaged)
        #expect(managed == 0)
    }

    /// The audit trail is DURABLE, not just in-memory: the census row lands in
    /// the `SurfaceStatusInvariantLogger` session file the device pull reads, so
    /// the next dogfood session can answer this question without a rebuild.
    @Test("the delivery leaves a durable census row naming the armed suggestions")
    func theDeliveryLeavesADurableCensusRow() async throws {
        let store = try await makeTestStore()
        let (logger, directory) = makeScopedSweepLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, invariantLogger: logger
        )

        try await Fx.deliver(Fx.composedFieldMarks(), to: orchestrator, store: store)

        let rows = drainSweepCensusDescriptions(logger, sentinel: "y3ya-drain")
        #expect(rows.contains {
            $0.contains(AdWindowIngestDoor.midSessionIngest.rawValue)
                && $0.contains("\(AdWindowIngestOutcome.armedSuggest.rawValue)=2")
        }, "census rows: \(rows)")
    }

    /// The cross-launch half of "user-visible". `preloadAdmissibleWindows`
    /// requires `confidence >= 0.70`, and the mark's confidence constant exists
    /// solely to clear it — so the same marks must arm through the OTHER door
    /// too, with no mid-session ingest involved.
    @Test("a sweep mark also arms through the cross-launch preload")
    func aSweepMarkArmsOnRelaunch() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        let marks = Fx.composedFieldMarks()
        try await store.upsertHotPathAdWindows(marks, existingIDs: [], retiredIDs: [])

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )

        let armed = await orchestrator.adWindowIngestOutcomeCount(.armedSuggest)
        #expect(armed == 2)
    }
}

// MARK: - 2. The negative survives the round trip

@Suite("A declined verdict surfaces nothing end to end (playhead-y3ya)",
       .timeLimit(.minutes(1)))
struct SemanticSweepDeclinedSurfacesNothingTests {

    private typealias Fx = SweepSurfaceFixture

    /// The bead's negative, carried all the way to the surface rather than
    /// stopped at the composer's return value: a declined verdict arms nothing.
    /// The vacuity control is inline — the SAME delivery carries one accepted
    /// verdict, so "zero armed" cannot be produced by a door that never fired.
    @Test("declined verdicts arm nothing while an accepted one in the same run arms")
    func declinedVerdictsArmNothing() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        let marks = SemanticSweepMarkComposer.compose(
            scanRows: [
                Fx.scanRow(id: "n1", start: 200, end: 290, disposition: .noAds),
                Fx.scanRow(id: "u1", start: 700, end: 790, disposition: .uncertain),
                Fx.scanRow(id: "a1", start: 900, end: 990, disposition: .abstain),
                Fx.scanRow(
                    id: "y1",
                    start: Fx.firstVerdict.start,
                    end: Fx.firstVerdict.end
                ),
            ],
            existingWindows: [],
            analysisAssetId: Fx.assetId
        )

        try await Fx.deliver(marks, to: orchestrator, store: store)

        let armed = await orchestrator.adWindowIngestOutcomeCount(.armedSuggest)
        #expect(armed == 1, "only the containsAd verdict reached the tier")
        #expect(marks.count == 1)
        #expect(marks.first?.startTime == Fx.firstVerdict.start)
    }
}
