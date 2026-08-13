// SkipOrchestratorPreloadActuationFloorTests.swift
//
// playhead-atr3: the cross-launch preload floor reads ACTUATION, not raw
// detection.
//
// WHAT THE FLOOR IS. `SkipOrchestrator.preloadAdmissibleWindows` is the single
// admission rule behind two doors — `beginEpisode`'s cross-launch preload and
// the playhead-96ot mid-session ingest. A row it excludes never reaches
// `evaluateWindow` at all, so on the next launch that span is neither skipped
// nor bannered: it is as if the detector had never spoken.
//
// WHAT CHANGED, AND WHY IT IS A RESTORATION. Before ar60's V47 column split the
// fusion path wrote its post-correction ACTUATION number into
// `AdWindow.confidence`, so this floor already compared 0.7 against actuation.
// The split moved actuation to `skipConfidence` and left `confidence` holding
// raw detection — silently switching this floor onto the higher of the two
// numbers for exactly the population that has a user correction on it. Dan's
// ruling: *the highest quality signal we have is a user correction*, so the
// floor reads `actuationConfidence`.
//
// WHY IT IS SAFE, AND WHAT THIS SUITE GUARDS. ar60's headline measurement was
// that five spans crossed this same 0.7 floor on inflation from marks that did
// NOT overlap them, because one asset-wide correction scalar was handed to
// every span. The remedy was scope, not a second threshold (Dan considered a
// numeric bound on the boost and declined it): `CorrectionFactorSnapshot`
// evaluates per span. `aReinforcementCannotLiftASpanItDoesNotOverlap` is the
// rail on that — it derives its actuation numbers by CALLING the production
// `factor(overlapping:_:)`, so a regression to asset-wide scoping reddens it
// rather than quietly re-creating the defect ar60 was filed for.
//
// DELETE-THE-CALL-SITE. Reverting the production line to `$0.confidence`
// reddens `aCorrectedUpSpanIsAdmitted` (0.55 detection / 0.88 actuation stops
// being admitted) and `aCorrectedDownSpanIsExcluded` (0.95 detection / 0.0475
// actuation starts being admitted), in opposite directions. The one-number
// control in each test is what keeps the pair from passing vacuously.

import Foundation
import Testing
@testable import Playhead

@Suite("Cross-launch preload floor reads ACTUATION (playhead-atr3)")
struct SkipOrchestratorPreloadActuationFloorTests {

    /// A local copy of `SkipOrchestrator.preloadConfidenceThreshold`, which is
    /// `private`. Pinned by `theFloorUnderTestIsStillTheProductionFloor` below,
    /// so a change to the production constant cannot leave these fixtures
    /// straddling a boundary that has moved.
    private static let floor: Double = 0.7

    // MARK: - Fixtures

    /// A persisted fusion row: two confidences that genuinely differ, and a
    /// DETECTOR boundary state so the playhead-ynmk user-assertion bypass is
    /// out of the picture. Every numeric rail here must be decided by the
    /// floor's arithmetic, not by that bypass.
    private func fusionRow(
        id: String,
        startTime: Double,
        endTime: Double,
        detection: Double,
        actuation: Double?,
        boundaryState: String = "acousticRefined"
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: startTime,
            endTime: endTime,
            confidence: detection,
            skipConfidence: actuation,
            boundaryState: boundaryState,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "fusion-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: startTime,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    /// Persist `rows`, run the cross-launch preload, and report the decision
    /// record each row produced — keyed by window id, absent when the row never
    /// got one.
    ///
    /// WHY THE DECISION LOG IS THE OBSERVABLE. It is written by
    /// `evaluateWindow`, which is downstream of `preloadAdmissibleWindows` and
    /// upstream of every tier decision, so "has a record" answers ADMITTED and
    /// nothing else. `confirmedWindows()` would not: a row admitted with a
    /// crushed actuation number is then judged on that same number and does not
    /// confirm, so the corrected-down rail would read empty under BOTH the
    /// correct and the reverted production line — a rail that cannot fail.
    private func preloadRecords(
        _ rows: [AdWindow]
    ) async throws -> [String: SkipDecisionRecord] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        for row in rows {
            try await store.insertAdWindow(row)
        }
        // playhead-wq34: the show must be in `.auto`, and that is a
        // precondition of the MEASUREMENT rather than a convenience. This suite
        // reads admission off `getDecisionLog()`, and only the managed tier
        // reaches `evaluateWindow` — which is also where the vacuity argument
        // above lives (a corrected-DOWN row is admitted by the floor and then
        // suppressed by the same number). On a show whose classes are not
        // `.auto`, wq34 routes an admitted row to the suggest tier, which logs
        // no decision, so every rail here would read "excluded" and the suite
        // would pass whatever the floor did.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        try await Task.sleep(for: .milliseconds(100))
        var byId: [String: SkipDecisionRecord] = [:]
        for record in await orchestrator.getDecisionLog() {
            byId[record.adWindowId] = record
        }
        return byId
    }

    // MARK: - The floor's constant

    @Test("the fixtures straddle the production floor, not a stale copy of it")
    func theFloorUnderTestIsStillTheProductionFloor() async throws {
        // `preloadConfidenceThreshold` is private, so this suite carries a
        // copy. Prove the copy by behaviour: a row just under the local floor
        // is excluded and a row just over it is admitted. If production moved
        // its constant, one of these two flips and the whole suite's fixture
        // design gets re-examined instead of silently testing nothing.
        let records = try await preloadRecords([
            fusionRow(
                id: "atr3-just-under",
                startTime: 100, endTime: 160,
                detection: Self.floor - 0.01, actuation: nil
            ),
            fusionRow(
                id: "atr3-just-over",
                startTime: 300, endTime: 360,
                detection: Self.floor, actuation: nil
            ),
        ])
        #expect(records["atr3-just-under"] == nil,
                "a row below \(Self.floor) must not be admitted")
        #expect(records["atr3-just-over"] != nil,
                "a row at \(Self.floor) must be admitted")
    }

    // MARK: - The read: actuation, in both directions

    @Test("detection under the floor, overlapping correction over it: ADMITTED")
    func aCorrectedUpSpanIsAdmitted() async throws {
        // The shape ar60's per-span snapshot now produces for a span the user
        // has REINFORCED: detection 0.55, one overlapping `.falseNegative`
        // whose decay-weighted factor lifts actuation to 0.88.
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [],
            boosters: [.init(weight: 0.6, range: 100...160)]
        )
        let detection = 0.55
        let actuation = detection * snapshot.factor(overlapping: 100, 160)
        #expect(detection < Self.floor, "the fixture must straddle the floor")
        #expect(actuation >= Self.floor, "the fixture must straddle the floor")

        let records = try await preloadRecords([
            fusionRow(
                id: "atr3-corrected-up",
                startTime: 100, endTime: 160,
                detection: detection, actuation: actuation
            ),
            // Control: the SAME detection number with no correction recorded.
            // It must stay out, or "admitted" would mean nothing.
            fusionRow(
                id: "atr3-uncorrected-control",
                startTime: 300, endTime: 360,
                detection: detection, actuation: nil
            ),
        ])

        let record = try #require(
            records["atr3-corrected-up"],
            """
            A span the user reinforced must survive to the next launch. \
            Detection \(detection) is under the \(Self.floor) floor; \
            actuation \(actuation) is over it. Reading detection here \
            discards the best evidence the system has.
            """
        )
        #expect(record.confidence == actuation,
                "and it must then be JUDGED on the same number it was admitted on")
        #expect(records["atr3-uncorrected-control"] == nil,
                "the uncorrected twin must still be excluded")
    }

    @Test("detection over the floor, overlapping correction under it: EXCLUDED")
    func aCorrectedDownSpanIsExcluded() async throws {
        // The converse, and the direction that costs the user show: detection
        // 0.95, one overlapping `.falsePositive` that crushes actuation to
        // 0.0475. Pre-V47 this row's single column held 0.0475 and the floor
        // excluded it; reading detection would re-admit it.
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [.init(weight: 0.95, range: 100...160)],
            boosters: []
        )
        let detection = 0.95
        let actuation = detection * snapshot.factor(overlapping: 100, 160)
        #expect(detection >= Self.floor, "the fixture must straddle the floor")
        #expect(actuation < Self.floor, "the fixture must straddle the floor")

        let records = try await preloadRecords([
            fusionRow(
                id: "atr3-corrected-down",
                startTime: 100, endTime: 160,
                detection: detection, actuation: actuation
            ),
            // Control: the same detection number, no correction. A one-number
            // producer is unaffected by any of this and must still be admitted
            // — otherwise "excluded" could just mean the preload ran empty.
            fusionRow(
                id: "atr3-uncorrected-high-control",
                startTime: 300, endTime: 360,
                detection: detection, actuation: nil
            ),
        ])

        #expect(
            records["atr3-corrected-down"] == nil,
            """
            A span the user said is NOT an ad must not re-enter cross-launch \
            continuity on its raw detection number. Detection \(detection), \
            actuation \(actuation), floor \(Self.floor). Admitted records: \
            \(records.keys.sorted()).
            """
        )
        let control = try #require(records["atr3-uncorrected-high-control"])
        #expect(control.confidence == detection,
                "a one-number producer must resolve to `confidence`, unchanged")
    }

    // MARK: - The bound on the boost is the SCOPE, not a second threshold

    @Test("a reinforcement cannot lift a span it does not overlap over the floor")
    func aReinforcementCannotLiftASpanItDoesNotOverlap() async throws {
        // This is the rail that makes reading actuation safe at all.
        //
        // ar60 measured the failure it guards: ONE asset-wide correction
        // scalar, resolved once per backfill, was handed to every span, so two
        // false-negative marks nearly doubled the confidence of seven spans
        // they did not overlap — five of which crossed THIS 0.7 floor on
        // inflation alone (true detection 0.37-0.66). Dan declined a numeric
        // bound on the boost; the bound is that a correction only reaches the
        // spans it actually covers.
        //
        // Both actuation numbers below are computed by the PRODUCTION
        // `factor(overlapping:_:)`. If per-span scoping ever regresses to
        // asset-wide — an `Entry` recorded with `range: nil`, an `applies`
        // that stops consulting the range, or a caller that goes back to the
        // one-scalar API — `distant` inherits `overlapped`'s 1.6x, crosses the
        // floor, and this test fails.
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [],
            boosters: [.init(weight: 0.6, range: 100...160)]
        )
        let detection = 0.55
        let overlappedActuation = detection * snapshot.factor(overlapping: 100, 160)
        let distantActuation = detection * snapshot.factor(overlapping: 1000, 1060)

        #expect(snapshot.factor(overlapping: 1000, 1060) == 1.0,
                "a span no correction covers must see a factor of exactly 1")
        #expect(distantActuation < Self.floor,
                "the un-overlapped span must stay under the floor on its own merits")

        // The counterfactual, stated so the rail names what it is protecting:
        // the SAME reinforcement expressed asset-wide (the compatibility shim,
        // i.e. exactly the pre-ar60 blanket) DOES lift the distant span over
        // the floor. Scope is the whole difference.
        let blanket = CorrectionFactorSnapshot(assetWidePassthrough: 1, boost: 1.6)
        #expect(detection * blanket.factor(overlapping: 1000, 1060) >= Self.floor,
                "under the pre-ar60 blanket the distant span WOULD cross — that is the defect")

        let records = try await preloadRecords([
            fusionRow(
                id: "atr3-overlapped",
                startTime: 100, endTime: 160,
                detection: detection, actuation: overlappedActuation
            ),
            fusionRow(
                id: "atr3-distant",
                startTime: 1000, endTime: 1060,
                detection: detection, actuation: distantActuation
            ),
        ])

        #expect(records["atr3-overlapped"] != nil,
                "the span the user actually reinforced must be admitted")
        #expect(
            records["atr3-distant"] == nil,
            """
            A reinforcement must not lift a span it does not overlap over the \
            \(Self.floor) preload floor. Detection \(detection), actuation \
            \(distantActuation). Admitted records: \(records.keys.sorted()).
            """
        )
    }

    // MARK: - The playhead-ynmk / qs0d bypass survives the switch

    @Test("a user-asserted row is still admitted with BOTH numbers under the floor")
    func aUserAssertedRowStillBypassesTheFloor() async throws {
        // playhead-ynmk/qs0d: the floor is a claim about DETECTOR quality, and
        // a user assertion is not a detector claim. Switching which number the
        // floor reads must not quietly re-gate the asserted population — a row
        // the user answered "Yes" to has to stay in cross-launch continuity
        // even when the detector was unsure (0.40 was the field value).
        //
        // Both numbers are under the floor here, so the row can ONLY be
        // admitted by the bypass.
        let records = try await preloadRecords([
            fusionRow(
                id: "atr3-user-confirmed",
                startTime: 100, endTime: 160,
                detection: 0.40, actuation: 0.10,
                boundaryState: UserSpanAssertion.userConfirmedSuggested.rawValue
            ),
            fusionRow(
                id: "atr3-user-marked",
                startTime: 300, endTime: 360,
                detection: 0.40, actuation: 0.10,
                boundaryState: UserSpanAssertion.userMarked.rawValue
            ),
            // Same two numbers, detector boundary state: excluded. This is
            // what proves the two above were admitted BY the bypass.
            fusionRow(
                id: "atr3-detector-twin",
                startTime: 500, endTime: 560,
                detection: 0.40, actuation: 0.10
            ),
        ])

        #expect(records["atr3-user-confirmed"] != nil,
                "a banner confirmation must survive relaunch (playhead-qs0d)")
        #expect(records["atr3-user-marked"] != nil,
                "a user-drawn mark must survive relaunch")
        #expect(records["atr3-detector-twin"] == nil,
                "the detector twin must be excluded — the bypass is the difference")
    }
}
