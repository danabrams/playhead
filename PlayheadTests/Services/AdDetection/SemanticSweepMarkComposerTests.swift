// SemanticSweepMarkComposerTests.swift
// playhead-y3ya — a semantic `containsAd` verdict has standing on its own.
//
// THE FIELD CASE. 2026-08-01, episode DE0784D8. `semantic_scan_results` records
// that the Foundation Model returned `containsAd` for 508–599 s and for
// 1604–1731 s. There is NO `ad_window` anywhere near either. On the one episode
// FM has ever partially scanned on Dan's phone, it fired twice and produced zero
// user-visible output.
//
// WHY. FM evidence reaches fusion only through
// `AdDetectionService.buildFMLedgerEntries`, which walks the asset's existing
// `DecodedSpan`s and adds weight to the ones a `containsAd` window OVERLAPS. It
// can never CREATE a span. So a sweep-lane verdict with no narrow lexical /
// acoustic / catalog seed under it contributes to nothing and is discarded —
// presence without extent is thrown away. That violates the stated portfolio
// policy (any signal fires → banner); every other lane honours it.
//
// WHAT THIS COMPOSER IS. The same shape as `SpecialistMarkComposer`: a pure,
// always-compiled function from persisted scan rows + the asset's existing
// windows to MARK-ONLY `AdWindow`s. No store, no actor, no model. It routes
// through the identical markOnly/suggest path day-0 rediff marks use — there is
// no second surfacing path.
//
// THE EXTENT POLICY IT PINS, in the order the stages run:
//   1. PRESENCE — only `containsAd` rows that actually examined their window.
//   2. REFINE — a `passB` row is the model's OWN narrowing of a `passA` window,
//      already computed and already persisted in seconds. Where one exists,
//      it is the extent. This is the bead's "refine with pass-B before
//      emitting" bullet, paid for at zero additional FM budget.
//   3. MERGE — adjacent coarse windows covering one pod become one banner.
//   4. CLIP — a PROVEN edge (`.rediffByteExact` / `.stingerSnapped`, the same
//      definition `SpanExtentSupport` uses) may pull an edge inward. It is
//      never REQUIRED: absent anchors change nothing. Hard boundaries CLIP FM
//      edges, they must never GATE eligibility — that inversion is the bug.
//   5. DEDUPE — never emit over an existing window, in ANY decisionState.
//   6. EMIT — `markOnly`, `candidate`, both edge anchors `unanchored`.
//
// EVERY EMITTED MARK IS UNANCHORED AND MARK-ONLY BY CONSTRUCTION, so
// playhead-2350's gate holds trivially and playhead-ynmk makes a confirmation a
// MARK rather than a cut. The downside of a wrong verdict is a wrong banner,
// never lost show.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum SweepFixture {

    static let assetId = "asset-de0784d8"

    /// The two verdicts from the field DB. Both had NO `ad_window` near them.
    static let firstVerdict = (start: 508.0, end: 599.0)
    static let secondVerdict = (start: 1_604.0, end: 1_731.0)

    static func row(
        id: String,
        start: Double,
        end: Double,
        disposition: CoarseDisposition = .containsAd,
        status: SemanticScanStatus = .success,
        scanPass: String = "passA",
        errorContext: String? = nil
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: .good,
            disposition: disposition,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: errorContext,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "y3ya"),
            transcriptVersion: "tv-1"
        )
    }

    /// The two field verdicts as persisted `passA` rows.
    static var fieldRows: [SemanticScanResult] {
        [
            row(id: "scan-1", start: firstVerdict.start, end: firstVerdict.end),
            row(id: "scan-2", start: secondVerdict.start, end: secondVerdict.end),
        ]
    }

    static func window(
        id: String,
        start: Double,
        end: Double,
        decisionState: String = AdDecisionState.candidate.rawValue,
        confidence: Double = 0.4,
        startEdgeAnchor: String = AutoSkipEdgeAnchor.unanchored.rawValue,
        endEdgeAnchor: String = AutoSkipEdgeAnchor.unanchored.rawValue
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: "aggregator",
            decisionState: decisionState,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: "aggregator",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: startEdgeAnchor,
            endEdgeAnchor: endEdgeAnchor
        )
    }

    /// `anchors: nil` exercises production's default — the anchor edges are
    /// harvested from `existing` — rather than silently disabling the clip.
    static func compose(
        rows: [SemanticScanResult],
        existing: [AdWindow] = [],
        anchors: [Double]? = nil
    ) -> [AdWindow] {
        SemanticSweepMarkComposer.compose(
            scanRows: rows,
            existingWindows: existing,
            provenAnchorEdges: anchors,
            analysisAssetId: assetId
        )
    }
}

// MARK: - 1. The field case

@Suite("A containsAd verdict with no seed under it becomes a candidate (playhead-y3ya)",
       .timeLimit(.minutes(1)))
struct SemanticSweepFieldCaseTests {

    private typealias Fx = SweepFixture

    /// THE BEAD. Both DE0784D8 verdicts produce a candidate. Against main this
    /// is zero — there is no producer that turns a sweep-lane verdict into a
    /// window at all.
    @Test("both DE0784D8 verdicts produce a candidate")
    func bothFieldVerdictsProduceACandidate() {
        let marks = Fx.compose(rows: Fx.fieldRows)

        #expect(marks.count == 2)
    }

    /// The extent is the coarse window, unchanged, because nothing narrowed it
    /// and nothing anchored it. Stated as its own claim so a future refinement
    /// that silently moved an edge is visible here rather than in a count.
    @Test("an unrefined, unanchored verdict emits its coarse window verbatim")
    func anUnanchoredVerdictEmitsItsCoarseWindow() {
        let marks = Fx.compose(rows: Fx.fieldRows).sorted { $0.startTime < $1.startTime }

        #expect(marks.map(\.startTime) == [Fx.firstVerdict.start, Fx.secondVerdict.start])
        #expect(marks.map(\.endTime) == [Fx.firstVerdict.end, Fx.secondVerdict.end])
    }

    /// The whole safety argument in one assertion: every emitted mark is
    /// mark-only. `SkipOrchestrator` routes exactly this stamp to the suggest
    /// tier, and nothing else can auto-skip.
    @Test("every emitted mark is markOnly")
    func everyMarkIsMarkOnly() {
        let marks = Fx.compose(rows: Fx.fieldRows)

        #expect(marks.allSatisfy { $0.eligibilityGate == SkipEligibilityGate.markOnly.rawValue })
    }

    /// playhead-2350 holds by construction rather than by evaluation: the
    /// composer never claims an anchor, so `isFullyAnchored` is false for every
    /// mark and the unanchored-edge block has nothing left to do.
    @Test("every emitted mark is unanchored on both edges")
    func everyMarkIsUnanchoredOnBothEdges() {
        let marks = Fx.compose(rows: Fx.fieldRows)

        #expect(marks.allSatisfy {
            $0.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue
                && $0.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue
        })
    }

    /// A mark must clear `SkipOrchestrator.preloadAdmissibleWindows`' 0.70
    /// confidence floor or it is invisible on the next launch — the cross-launch
    /// half of "user-visible".
    @Test("a mark clears the cross-launch preload confidence floor")
    func aMarkClearsThePreloadFloor() {
        let marks = Fx.compose(rows: Fx.fieldRows)

        #expect(marks.allSatisfy { $0.confidence >= 0.70 })
    }

    /// Never confirmed, never applied. A verdict is a proposal.
    @Test("a mark is emitted as a candidate, never confirmed")
    func aMarkIsACandidate() {
        let marks = Fx.compose(rows: Fx.fieldRows)

        #expect(marks.allSatisfy { $0.decisionState == AdDecisionState.candidate.rawValue })
    }

    /// Content-addressed ids: recomposing the same inputs mints the same rows,
    /// so the version-scoped reconcile retires nothing and the store's
    /// INSERT-OR-REPLACE is a true no-op.
    @Test("recomposing the same rows mints identical ids")
    func recomposeIsIdempotent() {
        let first = Fx.compose(rows: Fx.fieldRows).map(\.id).sorted()
        let second = Fx.compose(rows: Fx.fieldRows).map(\.id).sorted()

        #expect(first == second)
    }
}

// MARK: - 2. The negative — a verdict FM declined produces nothing

@Suite("A declined verdict produces nothing (playhead-y3ya)",
       .timeLimit(.minutes(1)))
struct SemanticSweepDeclinedVerdictTests {

    private typealias Fx = SweepFixture

    /// The bead's stated negative. `noAds` is the model saying it looked and
    /// there is nothing there.
    @Test("a noAds verdict produces nothing")
    func noAdsProducesNothing() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "n1", start: 508, end: 599, disposition: .noAds)
        ])

        #expect(marks.isEmpty)
    }

    /// `uncertain` is NOT a presence claim. Admitting it would be the threshold
    /// lowering this bead is forbidden to do, wearing a different hat.
    @Test("an uncertain verdict produces nothing")
    func uncertainProducesNothing() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "u1", start: 508, end: 599, disposition: .uncertain)
        ])

        #expect(marks.isEmpty)
    }

    @Test("an abstain verdict produces nothing")
    func abstainProducesNothing() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "a1", start: 508, end: 599, disposition: .abstain)
        ])

        #expect(marks.isEmpty)
    }

    /// A row whose status says the window was never examined carries no verdict
    /// at all, whatever its `disposition` column happens to hold. The field
    /// sweep ended `2581–2676 | abstain | cancelled`; a cancelled row is not
    /// evidence.
    @Test("a containsAd row whose scan never examined the window produces nothing")
    func unexaminedProducesNothing() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "c1", start: 508, end: 599, status: .cancelled)
        ])

        #expect(marks.isEmpty)
    }

    /// playhead-pz32's no-work sentinel spans the WHOLE attempted range and
    /// carries `.noAds`, which `didExamineWindow` treats as a real verdict. Its
    /// discriminator is the `errorContext` marker. Reading the status alone here
    /// would mint one enormous mark over an episode nothing scanned.
    @Test("a no-work sentinel row produces nothing")
    func noWorkSentinelProducesNothing() {
        let marks = Fx.compose(rows: [
            Fx.row(
                id: "s1",
                start: 0,
                end: 3_578,
                status: .noAds,
                errorContext: SemanticScanResult.noWorkSentinelErrorContextPrefix + "planEmpty"
            )
        ])

        #expect(marks.isEmpty)
    }

    /// VACUITY CONTROL for this whole suite. Every expectation above is an
    /// absence, and a composer that had quietly stopped running would satisfy
    /// all of them. The same call carries a verdict that MUST still produce a
    /// mark.
    @Test("a containsAd verdict delivered alongside declined ones still marks")
    func theDeclinedSuiteIsNotVacuous() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "n1", start: 100, end: 190, disposition: .noAds),
            Fx.row(id: "u1", start: 200, end: 290, disposition: .uncertain),
            Fx.row(id: "a1", start: 300, end: 390, disposition: .abstain),
            Fx.row(id: "c1", start: 400, end: 490, status: .cancelled),
            Fx.row(id: "y1", start: 508, end: 599),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 508)
        #expect(marks.first?.endTime == 599)
    }
}

// MARK: - 3. The extent policy

@Suite("The semantic-sweep extent policy (playhead-y3ya)",
       .timeLimit(.minutes(1)))
struct SemanticSweepExtentPolicyTests {

    private typealias Fx = SweepFixture

    /// REFINE. A `passB` row is the model's own narrowing of a `passA` window,
    /// already computed and persisted in SECONDS
    /// (`BackfillJobRunner.makePassBScanResult` projects the refined spans
    /// through the segment table). Where one exists it is the extent — the
    /// bead's pass-B bullet, at zero additional FM budget.
    @Test("a passB refinement replaces the coarse window it lies inside")
    func passBReplacesTheCoarseWindow() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "a", start: 508, end: 599),
            Fx.row(id: "b", start: 520, end: 551, scanPass: "passB"),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 520)
        #expect(marks.first?.endTime == 551)
    }

    /// The other half of the same rule, and the reason it is safe: a passB pass
    /// that ran and DECLINED does not retract the coarse presence verdict. Pass
    /// A said an ad is here; pass B failed to localize it. Presence survives,
    /// extent stays coarse — playhead-ynmk's presence-not-extent rule applied to
    /// the composer's own input.
    @Test("a declined passB refinement leaves the coarse presence verdict standing")
    func aDeclinedPassBLeavesTheCoarseWindow() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "a", start: 508, end: 599),
            Fx.row(id: "b", start: 520, end: 551, disposition: .noAds, scanPass: "passB"),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 508)
        #expect(marks.first?.endTime == 599)
    }

    /// A passB verdict outside every coarse containsAd window is itself a
    /// verdict and stands alone. Dropping it would rebuild, one layer down, the
    /// exact "presence needs a host to attach to" rule this bead removes.
    @Test("a passB verdict with no coarse parent stands on its own")
    func anOrphanPassBStandsAlone() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "b", start: 2_838, end: 2_954, scanPass: "passB")
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 2_838)
    }

    /// MERGE. The field sweep tiles ~95 s windows front to back; one 3-minute
    /// pod lands across two of them. Two touching banners for one ad break is a
    /// worse surface than one.
    @Test("adjacent coarse verdicts covering one pod merge into one mark")
    func adjacentVerdictsMerge() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "a", start: 508, end: 599),
            Fx.row(id: "b", start: 599, end: 694),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 508)
        #expect(marks.first?.endTime == 694)
    }

    /// The merge has a bound. Two verdicts a minute apart are two ad breaks, not
    /// one; merging them would claim the show between them.
    @Test("verdicts separated by more than the merge gap stay separate")
    func distantVerdictsDoNotMerge() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "a", start: 508, end: 599),
            Fx.row(id: "b", start: 700, end: 795),
        ])

        #expect(marks.count == 2)
    }

    /// CLIP. A proven edge inside the coarse window pulls the coarse edge in.
    /// This is the "clip to seam / stinger / rediff edges WHEN AVAILABLE" bullet.
    @Test("a proven edge inside the window clips the coarse start")
    func aProvenEdgeClipsTheStart() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 599)],
                               anchors: [515])

        #expect(marks.first?.startTime == 515)
        #expect(marks.first?.endTime == 599)
    }

    @Test("a proven edge inside the window clips the coarse end")
    func aProvenEdgeClipsTheEnd() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 599)],
                               anchors: [592])

        #expect(marks.first?.startTime == 508)
        #expect(marks.first?.endTime == 592)
    }

    /// THE INVERSION THAT PRODUCED THIS BUG, asserted directly. Hard boundaries
    /// CLIP FM edges; they must never GATE eligibility. With no anchor anywhere,
    /// the mark is emitted unchanged rather than withheld.
    @Test("no anchor anywhere still emits the mark")
    func absentAnchorsNeverGate() {
        let withAnchor = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 599)],
                                    anchors: [515])
        let withoutAnchor = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 599)])

        #expect(withAnchor.count == 1, "control: the anchored arm marks")
        #expect(withoutAnchor.count == 1, "and so does the unanchored arm")
    }

    /// A clip is a refinement of geometry, never a claim about it. Recording
    /// `.stingerSnapped` here would let a future auto-skip policy act on an edge
    /// a coarse FM window merely happened to sit near.
    @Test("a clipped mark still records both edges as unanchored")
    func aClippedMarkStaysUnanchored() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 599)],
                               anchors: [515, 592])

        #expect(marks.first?.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(marks.first?.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
    }

    /// An anchor far from either edge is a boundary belonging to something else
    /// — clipping to it would invent extent, which is the failure playhead-2350
    /// documented. Bound the reach.
    @Test("an anchor beyond the clip radius does not move an edge")
    func aDistantAnchorDoesNotClip() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 599)],
                               anchors: [560])

        #expect(marks.first?.startTime == 508)
        #expect(marks.first?.endTime == 599)
    }

    /// A clip must never destroy the mark it is refining.
    @Test("a clip that would collapse the mark is refused")
    func aCollapsingClipIsRefused() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 512)],
                               anchors: [509, 510])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 508)
        #expect(marks.first?.endTime == 512)
    }

    /// The anchor harvest reads the SAME definition of "somebody proved this
    /// edge" that `SpanExtentSupport` uses — a non-`unanchored`
    /// `AutoSkipEdgeAnchor` on a persisted row.
    @Test("proven anchor edges are harvested only from non-unanchored columns")
    func provenAnchorEdgesAreHarvestedFromAnchoredColumnsOnly() {
        let edges = SemanticSweepMarkComposer.provenAnchorEdges(in: [
            Fx.window(
                id: "byte", start: 100, end: 140,
                startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
                endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
            ),
            Fx.window(
                id: "half", start: 300, end: 340,
                startEdgeAnchor: AutoSkipEdgeAnchor.stingerSnapped.rawValue
            ),
            Fx.window(id: "none", start: 500, end: 540),
        ])

        #expect(edges == [100, 140, 300])
    }

    /// A degenerate row carries no extent. Emitting it would put a zero-width
    /// banner on the timeline.
    @Test("a zero-width verdict produces nothing")
    func aZeroWidthVerdictProducesNothing() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 508)])

        #expect(marks.isEmpty)
    }

    /// Below the inventory filter's duration floor a mark cannot survive ingest,
    /// so emitting it would only add census noise naming a drop nobody can act
    /// on.
    @Test("a verdict narrower than the mark floor produces nothing")
    func aSubFloorVerdictProducesNothing() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 508, end: 509)])

        #expect(marks.isEmpty)
    }
}

// MARK: - 4. Dedupe — additive only, and never resurface a veto

@Suite("The sweep marks only where the pipeline produced nothing (playhead-y3ya)",
       .timeLimit(.minutes(1)))
struct SemanticSweepAdditiveOnlyTests {

    private typealias Fx = SweepFixture

    /// The population this bead is defined over: "there is no `ad_window`
    /// anywhere near it". Where fusion DID emit, the sweep stays out — one ad,
    /// one banner.
    @Test("a verdict overlapping an existing window produces nothing")
    func anOverlappedVerdictProducesNothing() {
        let marks = Fx.compose(
            rows: [Fx.row(id: "a", start: 508, end: 599)],
            existing: [Fx.window(id: "w", start: 550, end: 560)]
        )

        #expect(marks.isEmpty)
    }

    /// A user veto is terminal. `mintByteExactDayZeroMarks` and
    /// `correctionReplayCandidates` both refuse to emit over a `.reverted` row
    /// for exactly this reason; a third producer that ignored it would undo a
    /// veto through a new door. Dan vetoed the acoustic junk on THIS episode
    /// five times.
    @Test("a verdict overlapping a user-reverted window produces nothing")
    func aVetoIsNeverResurfaced() {
        let marks = Fx.compose(
            rows: [Fx.row(id: "a", start: 508, end: 599)],
            existing: [
                Fx.window(
                    id: "vetoed", start: 550, end: 560,
                    decisionState: AdDecisionState.reverted.rawValue
                )
            ]
        )

        #expect(marks.isEmpty)
    }

    /// THE FORBIDDEN FIX, asserted as an absence. The near-zero-confidence
    /// acoustic windows (1.6e-3 down to 6.7e-6) are inputs to this composer's
    /// dedupe and nothing else: their gate, their confidence and their count all
    /// come out unchanged. Nothing here promotes them.
    @Test("the acoustic junk population is not promoted")
    func theAcousticJunkPopulationIsNotPromoted() {
        let junk = [
            Fx.window(id: "j1", start: 900, end: 930, confidence: 1.6e-3),
            Fx.window(id: "j2", start: 1_000, end: 1_030, confidence: 6.7e-6),
        ]

        let marks = Fx.compose(rows: Fx.fieldRows, existing: junk)

        #expect(marks.count == 2, "control: the two field verdicts still mark")
        #expect(marks.allSatisfy { mark in
            !junk.contains { $0.id == mark.id }
        }, "no junk row is re-emitted under a sweep id")
        #expect(marks.allSatisfy { $0.confidence >= 0.70 },
                "and no sub-threshold confidence rides in on a sweep mark")
    }

    /// VACUITY CONTROL. Two of the three tests above assert an absence. This one
    /// runs the SAME call with a verdict that is genuinely clear of every
    /// existing row, so "nothing was emitted" cannot be satisfied by a composer
    /// that stopped running.
    @Test("a verdict clear of every existing window still marks in the same call")
    func theDedupeSuiteIsNotVacuous() {
        let marks = Fx.compose(
            rows: [
                Fx.row(id: "covered", start: 508, end: 599),
                Fx.row(id: "clear", start: 1_604, end: 1_731),
            ],
            existing: [Fx.window(id: "w", start: 550, end: 560)]
        )

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 1_604)
    }

    /// `hasKnownExportDisposition` is an ALL-or-nothing gate: ONE unrecognized
    /// `candidate` boundaryState aborts the whole asset's cross-user snapshot,
    /// silently, with no log and no error. A sweep mark must therefore be a
    /// RECOGNIZED (local-only) disposition even though it is never exported —
    /// and unlike the two producers already listed there, this one ships ON, so
    /// the omission would bite in the field rather than staying dormant.
    @Test("a sweep mark does not abort the asset's cross-user snapshot")
    func sweepMarksAreAKnownExportDisposition() {
        let mark = SemanticSweepMarkComposer.makeMark(
            SemanticSweepMarkComposer.Extent(start: 508, end: 599),
            analysisAssetId: Fx.assetId
        )

        #expect(CrossUserAnalysisSnapshot.Window.hasKnownExportDisposition(mark))
    }

    /// A stale sweep row must be RETIRABLE, which requires its boundary state to
    /// stay OUT of the protected set — the mirror image of the claim above, and
    /// the two are one token apart in the same table.
    @Test("a sweep mark is reconcilable under its own detector version")
    func sweepMarksAreReconcilable() {
        let mark = SemanticSweepMarkComposer.makeMark(
            SemanticSweepMarkComposer.Extent(start: 508, end: 599),
            analysisAssetId: Fx.assetId
        )

        #expect(AdDetectionService.isReconcilableBackfillWindow(
            mark, detectorVersion: SemanticSweepMarkComposer.detectorVersion
        ))
        #expect(!AdDetectionService.isReconcilableBackfillWindow(
            mark, detectorVersion: "detection-v1"
        ), "and is invisible to the FM reconcile, so neither can clobber the other")
    }

    /// A previously-composed sweep mark must not suppress its own recompose —
    /// idempotency rides on content-addressed ids and the version-scoped
    /// reconcile, not on self-dedupe. Without this carve-out the second run over
    /// unchanged inputs would emit nothing and the reconcile would retire the
    /// mark it emitted on the first.
    @Test("a prior sweep mark does not suppress its own recompose")
    func aPriorSweepMarkDoesNotSelfSuppress() {
        let first = Fx.compose(rows: Fx.fieldRows)
        let second = Fx.compose(rows: Fx.fieldRows, existing: first)

        #expect(second.map(\.id).sorted() == first.map(\.id).sorted())
    }
}
