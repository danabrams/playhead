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

    /// The support payload a `passA` `containsAd` row actually carries, verbatim
    /// from the field DB's DE0784D8 508–599 s row. `spansJSON: "[]"` — what the
    /// fixture used before playhead-92im — is the shape
    /// `BackfillJobRunner.encodeSupport` writes only when the model returned NO
    /// support at all, and it appears on 0 of the 55 `containsAd` rows in the
    /// 2026-08-10 pull. Defaulting to it made every fixture row look
    /// unevidenced.
    static func coarseSupport(
        _ certainty: CertaintyBand,
        lineRefs: [Int] = [17, 18, 20]
    ) -> String {
        let refs = lineRefs.map(String.init).joined(separator: ",")
        return #"{"supportLineRefs":[\#(refs)],"certainty":"\#(certainty.rawValue)"}"#
    }

    /// A `passB` row's payload: an ARRAY of refined spans, each with its own
    /// band. Verbatim shape from the pull's F4CE7F47 590–667 s row.
    static func refinedSupport(
        _ certainties: [CertaintyBand],
        ownershipInferenceWasSuppressed: Bool = false
    ) -> String {
        let suppressed = ownershipInferenceWasSuppressed ? "true" : "false"
        let spans = certainties.map {
            #"{"anchors":[],"certainty":"\#($0.rawValue)","commercialIntent":"paid","#
                + #""firstLineRef":2,"lastLineRef":2,"ownership":"thirdParty","#
                + #""ownershipInferenceWasSuppressed":\#(suppressed)}"#
        }
        return "[\(spans.joined(separator: ","))]"
    }

    static func row(
        id: String,
        start: Double,
        end: Double,
        disposition: CoarseDisposition = .containsAd,
        status: SemanticScanStatus = .success,
        scanPass: String = "passA",
        errorContext: String? = nil,
        transcriptQuality: TranscriptQuality = .good,
        spansJSON: String? = nil
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            // Only a `containsAd` row carries support; every other disposition
            // persists "[]", which is what the field rows show.
            spansJSON: spansJSON
                ?? (disposition == .containsAd ? coarseSupport(.strong) : "[]"),
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
    ///
    /// playhead-92im NARROWED THIS CLAIM and the narrowing is the bead. It used
    /// to hold for every sweep mark ever composed, because the confidence was a
    /// constant sitting exactly on the floor. It now holds only at the CEILING,
    /// which the field rows reach: `strong` certainty, `good` transcript, no
    /// dissenting replicate. A weaker verdict does not clear it — see
    /// `SemanticSweepConfidenceTests`, which pins that as the intended
    /// behaviour rather than as an accident.
    @Test("a mark composed from the field's own strong verdicts clears the preload floor")
    func aMarkClearsThePreloadFloor() {
        let marks = Fx.compose(rows: Fx.fieldRows)

        #expect(marks.count == 2, "control: the composer ran and emitted both verdicts")
        #expect(marks.allSatisfy { $0.confidence >= 0.70 })
        #expect(marks.allSatisfy {
            $0.confidence == SemanticSweepMarkComposer.maximumMarkConfidence
        }, "and they clear it AT the ceiling, with no headroom above it")
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
    /// A said an ad is here; pass B failed to localize it. Presence survives —
    /// playhead-ynmk's presence-not-extent rule applied to the composer's own
    /// input.
    ///
    /// **WHAT IT STANDS AT CHANGED IN playhead-shu5, and this test used to
    /// assert the other answer.** It expected [508, 599] — the whole coarse
    /// window — on the reasoning that a declined pass B contributes nothing.
    /// That reasoning skipped a step: the declined row's own WINDOW is not
    /// nothing. The refinement planner builds it out of `focusLineRefs`, which
    /// is the coarse row's `supportLineRefs` expanded to `minimumZoomSpanLines`,
    /// and the pass-B writer persists the plan's segment bounds precisely so
    /// that a row which found no ads can still "say where we looked". So the
    /// coarse verdict stands over the seconds the model POINTED AT, not over
    /// the ~95 s tile it was handed.
    ///
    /// The claim this test was written to protect is untouched and is still
    /// asserted below: the mark SURVIVES. A failure to localize is not a
    /// retraction, and it never was.
    @Test("a declined passB refinement leaves presence standing, at the window it examined")
    func aDeclinedPassBLeavesPresenceStandingAtItsOwnWindow() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "a", start: 508, end: 599),
            Fx.row(id: "b", start: 520, end: 551, disposition: .noAds, scanPass: "passB"),
        ])

        #expect(marks.count == 1, "presence is NOT retracted by a failure to localize")
        #expect(marks.first?.startTime == 520)
        #expect(marks.first?.endTime == 551)
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
    ///
    /// TWO anchors, one per edge, and that is the whole point of the shape.
    /// The first version of this test used a SINGLE mid-window anchor, and
    /// mutation rail Y12 (radius 20 s -> 1000 s) SURVIVED against it: one
    /// interior anchor is a candidate for BOTH edges, so an unbounded radius
    /// collapsed the extent to a point, the min-duration guard refused the clip,
    /// and the mark came back unchanged. The assertion was green for a reason
    /// that had nothing to do with the radius. With an anchor in each half the
    /// clip is well-formed, so only the radius can prevent it.
    @Test("an anchor beyond the clip radius does not move an edge")
    func aDistantAnchorDoesNotClip() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 100, end: 380)],
                               anchors: [150, 330])

        #expect(marks.first?.startTime == 100)
        #expect(marks.first?.endTime == 380)
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

    /// THE WIDTH CEILING. The coarse lane really does return `containsAd` over
    /// enormous windows — `SpanExtentSupport`'s header records 17.04-1183.62 s
    /// on the THEMOVE replay. A nineteen-minute banner claims show, not an ad,
    /// and carries no usable extent. Dropping it is the honest answer; recovering
    /// it is a TARGETING problem (playhead-lxkq), not a surfacing one.
    @Test("a verdict wider than the mark ceiling produces nothing")
    func anOverWideVerdictProducesNothing() {
        let marks = Fx.compose(rows: [Fx.row(id: "a", start: 100, end: 500)])

        #expect(marks.isEmpty)
    }

    /// The ceiling is enforced on the MERGE too, and the two do different jobs.
    /// Without it a run of adjacent coarse windows fuses into one over-wide
    /// extent that the width filter then drops WHOLE — losing every verdict in
    /// the run. Stopping the merge keeps them, as marks that each stay inside
    /// the bound.
    @Test("the merge stops rather than growing past the mark ceiling")
    func theMergeStopsAtTheCeiling() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "a", start: 0, end: 95),
            Fx.row(id: "b", start: 95, end: 190),
            Fx.row(id: "c", start: 190, end: 285),
            Fx.row(id: "d", start: 285, end: 380),
        ]).sorted { $0.startTime < $1.startTime }

        #expect(marks.count == 2, "every verdict survives, none over the bound")
        #expect(marks.allSatisfy {
            $0.endTime - $0.startTime <= SemanticSweepMarkComposer.maximumMarkDurationSeconds
        })
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
                "and no junk row's confidence rides in on a sweep mark")
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

// MARK: - 6. The confidence is derived, not minted (playhead-92im)

/// A CONSTANT IS NOT A CONFIDENCE.
///
/// Before this suite the composer stamped `0.70` on every mark it ever
/// produced. The device pull of 2026-08-10 shows the result exactly: 22 sweep
/// rows, min 0.70, max 0.70 — one value — against `detection-v1`'s 74 rows
/// spanning 6.7e-06 to 1.0. Apply the standing diagnostic (what would this
/// number read if the evidence it summarises had never existed?) and the answer
/// was 0.70, always. There was no signal in it to threshold on, which is why
/// the two marks Dan vetoed — F4CE7F47 590–679 s and 48E903D7 596–677 s,
/// ~170 s of show between them, zero confirmed hits — were numerically
/// indistinguishable from every mark that was never questioned.
///
/// EVERY TEST BELOW FAILS AGAINST THE CONSTANT, because under it any two marks
/// compare equal. Each carries its own in-call vacuity control: an assertion
/// that the composer actually emitted the marks being compared, so "the two
/// values differ" can never be satisfied by a composer that emitted nothing.
@Suite("A sweep mark's confidence is derived from its evidence (playhead-92im)",
       .timeLimit(.minutes(1)))
struct SemanticSweepConfidenceTests {

    private typealias Fx = SweepFixture

    private static func confidence(of marks: [AdWindow], startingAt start: Double) -> Double? {
        marks.first { $0.startTime == start }?.confidence
    }

    /// THE HEADLINE. Compose a mixed population in ONE call and assert the
    /// output is not a constant. Against the pre-fix composer every value is
    /// 0.70 and `Set` collapses to one element.
    @Test("a mixed population does not collapse to a single value")
    func aMixedPopulationIsNotAConstant() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "strong-good", start: 100, end: 190),
            Fx.row(id: "moderate-good", start: 300, end: 390,
                   spansJSON: Fx.coarseSupport(.moderate)),
            Fx.row(id: "strong-degraded", start: 500, end: 590,
                   transcriptQuality: .degraded),
            Fx.row(id: "unevidenced", start: 700, end: 790, spansJSON: "[]"),
        ])

        #expect(marks.count == 4, "control: all four verdicts marked")
        let values = Set(marks.map(\.confidence))
        #expect(values.count == 4,
                "four distinct evidence profiles, four distinct confidences: \(values.sorted())")
        #expect(marks.allSatisfy { $0.confidence > 0 })
    }

    /// The model's OWN `CertaintyBand`, which the composer never read. Two rows
    /// identical but for `certainty`; only the band may explain the gap.
    @Test("a moderate verdict is worth less than a strong one")
    func aModerateVerdictRanksBelowAStrongOne() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "strong", start: 100, end: 190),
            Fx.row(id: "moderate", start: 300, end: 390,
                   spansJSON: Fx.coarseSupport(.moderate)),
        ])

        #expect(marks.count == 2, "control: both verdicts marked in the same call")
        let strong = Self.confidence(of: marks, startingAt: 100)
        let moderate = Self.confidence(of: marks, startingAt: 300)
        #expect(strong == SemanticSweepMarkComposer.maximumMarkConfidence)
        #expect(moderate != nil && strong != nil)
        #expect((moderate ?? 1) < (strong ?? 0))
    }

    /// THE playhead-3gzp CLAIM, pinned. Both marks Dan vetoed rest on
    /// `degraded` rows; a real confidence has to reflect the transcript the
    /// verdict was formed on. Two rows identical but for `transcriptQuality`.
    @Test("a degraded transcript discounts an otherwise identical verdict")
    func aDegradedTranscriptDiscountsTheVerdict() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "good", start: 100, end: 190),
            Fx.row(id: "degraded", start: 300, end: 390, transcriptQuality: .degraded),
        ])

        #expect(marks.count == 2, "control: both verdicts marked in the same call")
        let good = Self.confidence(of: marks, startingAt: 100)
        let degraded = Self.confidence(of: marks, startingAt: 300)
        #expect(good == SemanticSweepMarkComposer.maximumMarkConfidence)
        #expect(degraded != nil)
        #expect((degraded ?? 1) < (good ?? 0))
    }

    /// THE FIELD RANKING, built from the pull's own payloads. The F4CE7F47
    /// 590–679 s mark Dan vetoed is `strong` on a `degraded` transcript; the
    /// DE0784D8 508–599 s mark nobody has questioned is `strong` on a `good`
    /// one. A useful confidence must separate them, in that direction.
    @Test("the vetoed field mark ranks below the unquestioned one")
    func theVetoedFieldMarkRanksLower() {
        let marks = Fx.compose(rows: [
            // DE0784D8 508.0–599.8, transcriptQuality good, certainty strong.
            Fx.row(id: "de0784d8", start: 508, end: 599.8),
            // F4CE7F47 590.0–679.4, transcriptQuality degraded, certainty
            // strong — re-based to 1500 s so the two do not merge.
            Fx.row(id: "f4ce7f47", start: 1_590, end: 1_679.4, transcriptQuality: .degraded),
        ])

        #expect(marks.count == 2, "control: both field verdicts marked in the same call")
        let clean = Self.confidence(of: marks, startingAt: 508)
        let vetoed = Self.confidence(of: marks, startingAt: 1_590)
        #expect(clean != nil && vetoed != nil)
        #expect((vetoed ?? 1) < (clean ?? 0),
                "vetoed=\(vetoed as Any) clean=\(clean as Any)")
    }

    /// A REPLICATE THAT DISAGREES IS EVIDENCE. The sweep really does re-screen:
    /// 36 of 125 coarse windows on the pull carry more than one `passA` row,
    /// each a separate FM call in a separate run, and on four of them
    /// `containsAd` is the MINORITY verdict. The composer filtered those rows
    /// out and never counted them.
    @Test("a contradicted verdict is worth less than an uncontradicted one")
    func aContradictedVerdictRanksLower() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "clean", start: 100, end: 190),
            Fx.row(id: "disputed", start: 300, end: 390),
            // A second, independent screening of the SAME window that examined
            // it and did not affirm an ad.
            Fx.row(id: "dissent", start: 300, end: 390, disposition: .noAds),
        ])

        #expect(marks.count == 2, "control: the disputed verdict is still MARKED, not dropped")
        let clean = Self.confidence(of: marks, startingAt: 100)
        let disputed = Self.confidence(of: marks, startingAt: 300)
        #expect(clean != nil && disputed != nil)
        #expect((disputed ?? 1) < (clean ?? 0))
    }

    /// THE ANTI-INFLATION HALF of the same factor, and the reason it is
    /// Laplace-smoothed rather than a plain agreement fraction: repeats that
    /// AGREE must add nothing, so an absence of dissent can never read as
    /// corroboration. Four unanimous screenings and one lone screening are the
    /// same claim.
    @Test("agreeing repeats add nothing while one dissent still deducts")
    func unanimityDoesNotInflateButDissentDeflates() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "once", start: 100, end: 190),
            Fx.row(id: "four-a", start: 300, end: 390),
            Fx.row(id: "four-b", start: 300, end: 390),
            Fx.row(id: "four-c", start: 300, end: 390),
            Fx.row(id: "four-d", start: 300, end: 390),
            Fx.row(id: "split-yes", start: 500, end: 590),
            Fx.row(id: "split-no", start: 500, end: 590, disposition: .noAds),
        ])

        #expect(marks.count == 3, "control: three windows, three marks")
        let once = Self.confidence(of: marks, startingAt: 100)
        let four = Self.confidence(of: marks, startingAt: 300)
        let split = Self.confidence(of: marks, startingAt: 500)
        #expect(once == four, "unanimity is not a bonus")
        #expect((split ?? 1) < (once ?? 0), "but a dissenting replicate is a deduction")
    }

    /// THE STANDING DIAGNOSTIC, asserted directly. A `containsAd` row whose
    /// support payload is `"[]"` — what `BackfillJobRunner.encodeSupport` writes
    /// when the model returned no support at all — must NOT read like a mark the
    /// model graded `strong`. Under the constant it read identically.
    @Test("a verdict that graded itself nothing reads the floor, not the ceiling")
    func anUnevidencedVerdictReadsTheFloor() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "graded", start: 100, end: 190),
            Fx.row(id: "ungraded", start: 300, end: 390, spansJSON: "[]"),
        ])

        #expect(marks.count == 2, "control: an ungraded verdict is still MARKED")
        let graded = Self.confidence(of: marks, startingAt: 100)
        let ungraded = Self.confidence(of: marks, startingAt: 300)
        #expect(graded == SemanticSweepMarkComposer.maximumMarkConfidence)
        #expect(ungraded == SemanticSweepMarkComposer.unevidencedMarkConfidence)
        #expect((ungraded ?? 1) < (graded ?? 0))
    }

    /// A `passB` row carries its own bands in an ARRAY. The weakest span
    /// governs, because the mark covers all of them.
    @Test("a refinement is graded by its weakest span")
    func aRefinementIsGradedByItsWeakestSpan() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "coarse-a", start: 100, end: 190),
            Fx.row(id: "refine-a", start: 120, end: 170, scanPass: "passB",
                   spansJSON: Fx.refinedSupport([.strong, .strong])),
            Fx.row(id: "coarse-b", start: 300, end: 390),
            Fx.row(id: "refine-b", start: 320, end: 370, scanPass: "passB",
                   spansJSON: Fx.refinedSupport([.strong, .moderate])),
        ])

        #expect(marks.count == 2, "control: both coarse windows refined and marked")
        let allStrong = Self.confidence(of: marks, startingAt: 120)
        let mixed = Self.confidence(of: marks, startingAt: 320)
        #expect(allStrong != nil && mixed != nil)
        #expect((mixed ?? 1) < (allStrong ?? 0))
    }

    /// THE ORPHAN REFINEMENT, which is F4CE7F47 322–402 s in the field: three
    /// independent `passA` screenings said `uncertain`, one `passB` row said
    /// `containsAd`, and it shipped at the same 0.70 as a mark two clean
    /// screenings agreed on.
    ///
    /// ADMISSION IS DELIBERATELY UNCHANGED — the mark is still emitted, which is
    /// this test's control. Only the number it carries changes.
    @Test("a refinement its own screenings declined is marked, at a far lower grade")
    func anOrphanRefinementIsMarkedButHeavilyDiscounted() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "affirmed", start: 100, end: 190),
            Fx.row(id: "screen-1", start: 300, end: 390, disposition: .uncertain),
            Fx.row(id: "screen-2", start: 300, end: 390, disposition: .uncertain),
            Fx.row(id: "screen-3", start: 300, end: 390, disposition: .uncertain),
            Fx.row(id: "orphan", start: 320, end: 370, scanPass: "passB",
                   spansJSON: Fx.refinedSupport([.strong])),
        ])

        #expect(marks.count == 2, "control: the orphan refinement is STILL emitted")
        let affirmed = Self.confidence(of: marks, startingAt: 100)
        let orphan = Self.confidence(of: marks, startingAt: 320)
        #expect(orphan != nil)
        #expect((orphan ?? 1) < (affirmed ?? 0) / 2,
                "three screenings declined it: orphan=\(orphan as Any)")
    }

    /// A merged mark speaks for its WHOLE extent, so its grade is the
    /// duration-weighted mean of the windows under it — never the best one.
    @Test("a merged mark is graded by its whole extent, not by its best window")
    func aMergedMarkIsGradedByItsWholeExtent() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "w1", start: 100, end: 190),
            Fx.row(id: "w2", start: 190, end: 280, spansJSON: Fx.coarseSupport(.moderate)),
        ])

        #expect(marks.count == 1, "control: the two touching windows merged into one mark")
        let merged = marks.first?.confidence
        #expect(merged != nil)
        #expect((merged ?? 0) < SemanticSweepMarkComposer.maximumMarkConfidence,
                "the moderate half pulls the whole mark down")
        #expect((merged ?? 0) > SemanticSweepMarkComposer.maximumMarkConfidence * 0.75,
                "and the strong half holds it above a purely moderate mark")
    }

    /// Stage 4 refines GEOMETRY. An anchor proves where a boundary is; it says
    /// nothing about whether the model was right that an ad is there, so it must
    /// leave the grade alone.
    @Test("clipping to a proven edge moves the geometry and not the grade")
    func clippingDoesNotChangeTheGrade() {
        let rows = [Fx.row(id: "a", start: 508, end: 599,
                           spansJSON: Fx.coarseSupport(.moderate))]
        let clipped = Fx.compose(rows: rows, anchors: [512])
        let unclipped = Fx.compose(rows: rows, anchors: [])

        #expect(clipped.count == 1 && unclipped.count == 1, "control: both composed a mark")
        #expect(clipped.first?.startTime == 512, "control: the anchor really clipped")
        #expect(unclipped.first?.startTime == 508)
        #expect(clipped.first?.confidence == unclipped.first?.confidence)
        #expect(clipped.first?.confidence != SemanticSweepMarkComposer.maximumMarkConfidence,
                "and the preserved grade is a derived one, not the old constant")
    }

    /// FLOAT DRIFT IS A GATE FAILURE, not a rounding curiosity. A run of
    /// windows that ALL grade at the ceiling must merge to the ceiling
    /// EXACTLY — `>=` against `SkipOrchestrator`'s 0.70 preload floor is an
    /// exact comparison, and a mean computed as `(a*h + b*w)/(h+w)` lands a
    /// few ULP off even when `a == b`.
    ///
    /// THE GEOMETRY IS THE FIELD'S, NOT A CONSTRUCTED ONE. These are the three
    /// coarse windows behind the pull's 8518A3FB 292.0–579.7 s mark, verbatim.
    /// Under the sum-of-products form they merge to 0.6999999999999998 and the
    /// mark silently falls out of cross-launch preload; under the
    /// interpolation form they merge to 0.70. Two of the six ceiling-grade
    /// marks in the 2026-08-10 pull were in that state. A synthetic run of
    /// round-numbered windows does NOT reproduce it — the first draft of this
    /// rail used one and survived the mutant.
    @Test("a run of ceiling-grade windows merges to the ceiling exactly")
    func aRunOfCeilingWindowsMergesToTheCeilingExactly() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "w1", start: 292.02, end: 380.4),
            Fx.row(id: "w2", start: 380.4, end: 468.78),
            Fx.row(id: "w3", start: 468.78, end: 579.66),
        ])

        #expect(marks.count == 1, "control: the three touching windows merged")
        #expect(marks.first?.confidence == SemanticSweepMarkComposer.maximumMarkConfidence,
                "merged=\(String(describing: marks.first?.confidence))")
        #expect((marks.first?.confidence ?? 0) >= 0.70,
                "and it still clears the preload floor's exact comparison")
    }

    /// A FABRICATED BAND IS NOT A BAND, and this is the population it governs:
    /// 9 of the 11 refined `containsAd` spans in the 2026-08-10 pull are
    /// permissive-bypass spans whose `strong` `PermissiveAdClassifier`
    /// HARDCODED — "the FM never inferred these classification dimensions, the
    /// runner is hardcoding them". Reading it as the model's own grade rebuilds
    /// this bead's bug exactly: a value that reads `strong` when nothing
    /// graded anything.
    ///
    /// The two rows are byte-identical but for `ownershipInferenceWasSuppressed`,
    /// so nothing else can explain the gap.
    @Test("a band the runner hardcoded on the permissive path is not the model's")
    func aFabricatedBandDoesNotCountAsCertainty() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "coarse-real", start: 100, end: 190),
            Fx.row(id: "real", start: 120, end: 170, scanPass: "passB",
                   spansJSON: Fx.refinedSupport([.strong])),
            Fx.row(id: "coarse-fab", start: 300, end: 390),
            Fx.row(id: "fabricated", start: 320, end: 370, scanPass: "passB",
                   spansJSON: Fx.refinedSupport(
                       [.strong], ownershipInferenceWasSuppressed: true)),
        ])

        #expect(marks.count == 2, "control: BOTH refinements are still MARKED")
        let real = Self.confidence(of: marks, startingAt: 120)
        let fabricated = Self.confidence(of: marks, startingAt: 320)
        #expect(real == SemanticSweepMarkComposer.maximumMarkConfidence,
                "control: the genuine strong band still reaches the ceiling")
        #expect(fabricated != nil)
        #expect((fabricated ?? 1) < (real ?? 0))
        #expect(fabricated == SemanticSweepMarkComposer.unevidencedMarkConfidence,
                "and it reads exactly what an ungraded verdict reads")
    }

    /// The WEAKEST span governs, and a span nobody graded is the weakest. A
    /// `compactMap` that skipped it would let the one graded span speak for the
    /// ungraded one — the same absence-reads-as-evidence shape one layer down.
    @Test("an ungraded span in a refinement drags the whole row to the floor")
    func anUngradedSpanGovernsTheRefinement() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "coarse-a", start: 100, end: 190),
            Fx.row(id: "all-graded", start: 120, end: 170, scanPass: "passB",
                   spansJSON: Fx.refinedSupport([.strong, .strong])),
            Fx.row(id: "coarse-b", start: 300, end: 390),
            Fx.row(id: "one-ungraded", start: 320, end: 370, scanPass: "passB",
                   spansJSON: #"[{"certainty":"strong"},{"firstLineRef":9}]"#),
        ])

        #expect(marks.count == 2, "control: both refinements marked")
        let graded = Self.confidence(of: marks, startingAt: 120)
        let mixed = Self.confidence(of: marks, startingAt: 320)
        #expect(graded == SemanticSweepMarkComposer.maximumMarkConfidence)
        #expect((mixed ?? 1) < (graded ?? 0))
    }

    /// A SECOND SCREENING OF THE SAME WINDOW adds no seconds, so the
    /// duration-weighted merge cannot see it. It is still a verdict about audio
    /// this mark covers, and the weaker of the two governs — the rule the rest
    /// of the composer already applies to two claims over one extent.
    @Test("a re-screen of the same window at a lower grade is not discarded")
    func aNestedRescreenTakesTheWeakerGrade() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "first", start: 100, end: 190),
            Fx.row(id: "rescreen", start: 100, end: 190,
                   transcriptQuality: .degraded),
        ])

        #expect(marks.count == 1, "control: one window, one mark")
        #expect((marks.first?.confidence ?? 1)
                < SemanticSweepMarkComposer.maximumMarkConfidence,
                "the degraded re-screen is not silently dropped")
    }

    /// NOTHING IS EVER PROMOTED. The ceiling is the constant this bead replaced,
    /// so a detector that is 2-for-2 wrong on the judged episodes cannot come
    /// out of this change stronger than it went in.
    @Test("no evidence combination mints more than the constant it replaced")
    func noMarkIsEverPromoted() {
        let bands: [CertaintyBand?] = [.strong, .moderate, .weak, nil]
        let qualities: [TranscriptQuality] = [.good, .degraded, .unusable]
        var seen: Set<Double> = []
        for band in bands {
            for quality in qualities {
                for affirming in 0...4 {
                    for dissenting in 0...4 {
                        let value = SemanticSweepMarkComposer.markConfidence(
                            band: band,
                            transcriptQuality: quality,
                            affirming: affirming,
                            dissenting: dissenting
                        )
                        #expect(value <= SemanticSweepMarkComposer.maximumMarkConfidence)
                        #expect(value > 0)
                        seen.insert(value)
                    }
                }
            }
        }
        #expect(seen.count > 1, "control: the ladder actually varies over its inputs")
        #expect(seen.contains(SemanticSweepMarkComposer.maximumMarkConfidence),
                "control: the ceiling is reachable, so the bound is tight")
    }
}
