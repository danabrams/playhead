// ComposedMarkGateTests.swift
// playhead-mqqd: the additive composers' eligibility stamp is DERIVED, and the
// listener outcome is unchanged.
//
// The suite is in three parts, and the middle one is the load-bearing one.
//
//   THE RULE — exhaustive over the 3 × 3 anchor space, plus the two relations
//   that keep it from being an independent invention: it can never admit
//   something playhead-2350 would demote (`.eligible` ⇒ `isFullyAnchored`), and
//   it agrees with `SkipDetectorClass.classify`'s `.rediffByteExact` predicate,
//   which is where the `deterministic-on-both-edges` bar comes from.
//
//   THE COUPLING — an emitted mark's `eligibilityGate` is the gate DERIVED FROM
//   THE EXTENT THAT MARK IS PERSISTED WITH, and its anchor columns are that same
//   extent's. This is the assertion that reddens if anybody puts the literal
//   back, and it is the reason the change is worth making even though the value
//   is unchanged today: with a literal, the gate and the anchors are two
//   constants that must agree and nothing checks it (playhead-6qvf).
//
//   THE LISTENER — the real `SkipOrchestrator` receiving the real `compose(…)`
//   output of both producers, in `auto` trust mode at trust 0.95. Both surface
//   as a suggest-tier banner and neither auto-skips: identical before and after
//   this bead, which is the outcome the bead argues for rather than a promotion
//   nobody measured.

import Foundation
import Testing
@testable import Playhead

@Suite("ComposedMarkGate (playhead-mqqd: the composers' stamp is derived)")
struct ComposedMarkGateTests {

    private static let assetId = "asset-1"

    private static func support(
        _ start: AutoSkipEdgeAnchor,
        _ end: AutoSkipEdgeAnchor
    ) -> SpanExtentSupport {
        SpanExtentSupport(startAnchor: start, endAnchor: end)
    }

    /// Every anchor pair, so the truth table is enumerated rather than sampled.
    private static var allPairs: [(AutoSkipEdgeAnchor, AutoSkipEdgeAnchor)] {
        AutoSkipEdgeAnchor.allCases.flatMap { start in
            AutoSkipEdgeAnchor.allCases.map { (start, $0) }
        }
    }

    // MARK: - The rule

    @Test("eligible iff BOTH edges are byte-exact — all nine anchor pairs")
    func truthTableIsExhaustive() {
        for (start, end) in Self.allPairs {
            let expected: SkipEligibilityGate =
                (start == .rediffByteExact && end == .rediffByteExact)
                ? .eligible
                : .markOnly
            #expect(
                ComposedMarkGate.eligibility(for: Self.support(start, end)) == expected,
                "start=\(start.rawValue) end=\(end.rawValue)"
            )
        }
    }

    /// A lone byte-exact edge is worth nothing, which is the whole point of
    /// tracking the two edges independently: the invented edge is the one that
    /// clips the show, and it does not matter which end it is on.
    @Test("one proven edge is not enough, in either direction")
    func oneProvenEdgeIsNotEnough() {
        #expect(
            ComposedMarkGate.eligibility(for: Self.support(.rediffByteExact, .unanchored))
                == .markOnly
        )
        #expect(
            ComposedMarkGate.eligibility(for: Self.support(.unanchored, .rediffByteExact))
                == .markOnly
        )
    }

    /// A stinger snap corroborates an edge and is enough for playhead-2350's
    /// `isFullyAnchored`. It is deliberately NOT enough here: a composed mark's
    /// PRESENCE evidence is one coarse FM verdict or a chain walk, and that does
    /// not get to spend an acoustic corroboration on a cut.
    @Test("a stinger-snapped pair clears 2350's bar and still does not clear this one")
    func stingerSnapIsNotEnoughForAComposedMark() {
        let both = Self.support(.stingerSnapped, .stingerSnapped)
        #expect(both.isFullyAnchored, "the 2350 bar admits it")
        #expect(ComposedMarkGate.eligibility(for: both) == .markOnly, "this one does not")
    }

    /// THE ONE-DIRECTIONAL RELATION, stated as an implication over the whole
    /// domain rather than as prose: this gate can never admit a span that
    /// playhead-2350 would demote. It only ever admits less.
    @Test("eligible here implies fully anchored there — all nine anchor pairs")
    func neverAdmitsMoreThan2350() {
        for (start, end) in Self.allPairs {
            let extent = Self.support(start, end)
            if ComposedMarkGate.eligibility(for: extent) == .eligible {
                #expect(
                    extent.isFullyAnchored,
                    "start=\(start.rawValue) end=\(end.rawValue)"
                )
            }
        }
    }

    /// The bar is not invented here — it is `SkipDetectorClass.classify`'s
    /// `.rediffByteExact` predicate, the one class whose mode is a property of
    /// the instrument rather than of a show's history. Pinning the two together
    /// is what stops them becoming two questions that merely happen to agree.
    @Test("the bar agrees with the detector classification that grants .rediffByteExact")
    func agreesWithDetectorClassification() {
        let composedBoundaryStates = [
            SemanticSweepMarkComposer.boundaryState,
            AdPodContinuation.boundaryState
        ]
        for boundaryState in composedBoundaryStates {
            for (start, end) in Self.allPairs {
                let gate = ComposedMarkGate.eligibility(for: Self.support(start, end))
                let detectorClass = SkipDetectorClass.classify(
                    boundaryState: boundaryState,
                    startAnchor: start,
                    endAnchor: end
                )
                #expect(
                    (gate == .eligible) == (detectorClass == .rediffByteExact),
                    "\(boundaryState) start=\(start.rawValue) end=\(end.rawValue) gate=\(gate.rawValue) class=\(detectorClass.rawValue)"
                )
            }
        }
    }

    // MARK: - The coupling

    /// THE ASSERTION THAT REDDENS IF THE LITERAL COMES BACK.
    ///
    /// It compares the emitted row's stamp against the gate DERIVED from the
    /// producer's own declared extent, rather than against a constant this test
    /// also types. Restoring `eligibilityGate: SkipEligibilityGate.markOnly
    /// .rawValue` is invisible here while `extentSupport` is `.unanchored` —
    /// and that is exactly right, because while the extent is unanchored the
    /// two ARE the same answer. What this test buys is the day the extent
    /// moves: then the literal disagrees with the evidence on its own row and
    /// this fails, where before the bead nothing anywhere did.
    @Test("a sweep mark's gate is the gate its own declared extent earns")
    func sweepMarkGateTracksItsDeclaredExtent() {
        let mark = SemanticSweepMarkComposer.makeMark(
            SemanticSweepMarkComposer.Extent(start: 508, end: 599),
            attribution: .unrefined,
            analysisAssetId: Self.assetId
        )
        let declared = SemanticSweepMarkComposer.extentSupport

        #expect(mark.startEdgeAnchor == declared.startAnchor.rawValue)
        #expect(mark.endEdgeAnchor == declared.endAnchor.rawValue)
        #expect(
            mark.eligibilityGate == ComposedMarkGate.eligibility(for: declared).rawValue
        )
    }

    @Test("a continuation mark's gate is the gate its own declared extent earns")
    func continuationMarkGateTracksItsDeclaredExtent() {
        let mark = AdPodContinuation.makeMark(
            start: 669,
            end: 797.2,
            confidence: 0.7,
            analysisAssetId: Self.assetId
        )
        let declared = AdPodContinuation.extentSupport

        #expect(mark.startEdgeAnchor == declared.startAnchor.rawValue)
        #expect(mark.endEdgeAnchor == declared.endAnchor.rawValue)
        #expect(
            mark.eligibilityGate == ComposedMarkGate.eligibility(for: declared).rawValue
        )
    }

    /// Read back off the ROW, through `AdWindow.extentSupport` — the same decode
    /// `SkipOrchestrator.admissionSkipMode` performs on a persisted row. A row
    /// whose stamp is not what its own persisted columns earn is the drift this
    /// bead removes, and it is checked against the REAL `compose(…)` output of
    /// both producers, not against `makeMark` alone.
    @Test("every mark either composer really emits is self-consistent")
    func composedMarksAreSelfConsistent() {
        let emitted = Self.realSweepMarks() + Self.realContinuationMarks()
        #expect(emitted.count >= 2, "both producers must actually have emitted something")

        for mark in emitted {
            #expect(
                mark.eligibilityGate
                    == ComposedMarkGate.eligibility(for: mark.extentSupport).rawValue,
                "\(mark.id) gate=\(mark.eligibilityGate ?? "nil") start=\(mark.startEdgeAnchor) end=\(mark.endEdgeAnchor)"
            )
        }
    }

    /// The measured claim this bead reports: with the extent both producers can
    /// actually prove, the DERIVED answer is the literal it replaced. Stated as
    /// a value assertion so a change to the rule that happened to keep the
    /// coupling intact still has to argue for itself.
    @Test("the derived answer is markOnly today, for every mark both producers emit")
    func derivationReproducesTheOldLiteralToday() {
        for mark in Self.realSweepMarks() + Self.realContinuationMarks() {
            #expect(mark.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
            #expect(mark.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(mark.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        }
    }

    // MARK: - The listener

    /// WHAT A LISTENER RECEIVES for a sweep mark, driven through the real
    /// orchestrator in the most permissive configuration that exists — `auto`
    /// mode, trust 0.95, 50 observations. A banner, and no removed audio.
    @Test("a real sweep mark surfaces as a suggest banner and never skips, even in auto")
    func sweepMarkReachesTheListenerAsABanner() async throws {
        let mark = try #require(Self.realSweepMarks().first)
        let outcome = try await Self.deliver(mark)

        #expect(outcome.bannerTier == .suggest, "a card the listener can act on")
        #expect(!outcome.enteredManagedTier, "and no audio removed")
        #expect(!outcome.wasSkipped)
    }

    @Test("a real continuation mark surfaces as a suggest banner and never skips, even in auto")
    func continuationMarkReachesTheListenerAsABanner() async throws {
        let mark = try #require(Self.realContinuationMarks().first)
        let outcome = try await Self.deliver(mark)

        #expect(outcome.bannerTier == .suggest)
        #expect(!outcome.enteredManagedTier)
        #expect(!outcome.wasSkipped)
    }

    // MARK: - Real producer output

    /// The two field verdicts from `SemanticSweepMarkComposer`'s own header
    /// (episode DE0784D8, 2026-08-01) run through the production entry point.
    private static func realSweepMarks() -> [AdWindow] {
        SemanticSweepMarkComposer.compose(
            scanRows: [
                sweepRow(id: "scan-1", start: 508, end: 599),
                sweepRow(id: "scan-2", start: 1604, end: 1731)
            ],
            existingWindows: [],
            analysisAssetId: assetId
        )
    }

    private static func sweepRow(
        id: String,
        start: Double,
        end: Double
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
            disposition: .containsAd,
            spansJSON: #"{"supportLineRefs":[17,18],"certainty":"strong"}"#,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: #"{"promptLabel":"y3ya"}"#,
            transcriptVersion: "tv-1"
        )
    }

    /// The `conan-2026-07-09` pod from `AdPodContinuation`'s own header, run
    /// through the production entry point off an ELIGIBLE seed at 0.98 — the
    /// strongest thing the pipeline emits.
    private static func realContinuationMarks() -> [AdWindow] {
        AdPodContinuation.compose(
            existingWindows: [continuationSeed()],
            adCopyLinks: [
                AdPodContinuation.AdCopyLink(start: 669.0, end: 714.5),
                AdPodContinuation.AdCopyLink(start: 729.7, end: 746.0),
                AdPodContinuation.AdCopyLink(start: 751.7, end: 774.4),
                AdPodContinuation.AdCopyLink(start: 778.6, end: 797.2)
            ],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470,
            analysisAssetId: assetId
        )
    }

    private static func continuationSeed() -> AdWindow {
        AdWindow(
            id: "seed-1",
            analysisAssetId: assetId,
            startTime: 797.2,
            endTime: 810.7,
            confidence: 0.98,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 797.2,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
    }

    // MARK: - Delivery probe

    private struct DeliveryOutcome {
        let bannerTier: AdBannerTier?
        let enteredManagedTier: Bool
        let wasSkipped: Bool
    }

    /// Drive one composed mark through the REAL `SkipOrchestrator` and report
    /// what the listener got. `auto` / 0.95 / 50 observations is the most
    /// permissive trust state the app can be in, so a banner here is a banner
    /// everywhere.
    private static func deliver(_ mark: AdWindow) async throws -> DeliveryOutcome {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeDurationSec: 1470)
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.95,
            observations: 50
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: assetId,
            podcastId: "podcast-1"
        )
        let stream = await orchestrator.bannerItemStream()

        await orchestrator.receiveAdWindows([mark])
        // playhead-d3g0: delivery ARMS the card; the playhead ENTERING the span
        // is what presents it. Both steps are production's, so the probe does
        // both.
        await orchestrator.updatePlayheadTime(mark.startTime)

        let collectTask = Task<AdSkipBannerItem?, Never> {
            for await item in stream where item.windowId == mark.id {
                return item
            }
            return nil
        }
        try await Task.sleep(for: .milliseconds(150))
        collectTask.cancel()
        let banner = await collectTask.value

        let confirmed = await orchestrator.confirmedWindows()
        let log = await orchestrator.getDecisionLog()
        return DeliveryOutcome(
            bannerTier: banner?.tier,
            enteredManagedTier: confirmed.contains { $0.id == mark.id },
            wasSkipped: log.contains {
                $0.adWindowId == mark.id && ($0.decision == .applied || $0.decision == .confirmed)
            }
        )
    }
}
