// SemanticSweepCorroborationScopeTests.swift
// playhead-kg6i — a corroboration vote is a REPLICATE OF THE SAME EXPERIMENT.
//
// THE DEFECT. `SemanticSweepMarkComposer.corroboration` counted every
// presence-pass row that OVERLAPPED an extent as an affirming or dissenting
// vote on it, and never looked at `transcriptVersion`. An FM screening is an
// experiment on a transcript; two rows formed against two different
// transcripts of the same audio are two different experiments. Counting one as
// a vote on the other's claim is this repo's standing defect class — a value
// that names one thing (did a re-screen of THIS transcript disagree?) read as
// though it named another (did anything anywhere ever disagree?).
//
// HOW BIG IT IS ON REAL DATA. On the 2026-08-19 t4 device pull, 211 of the 301
// coarse `containsAd` rows carry a `transcriptVersion` the asset's current
// canonical chunk set no longer hashes to, and on NINE of the fifteen assets
// nothing was ever examined at the current version at all — six of those carry
// exactly one row there and it is a playhead-pz32 `noWork:` sentinel spanning
// [0, 0]. So on most assets the un-scoped count was not measuring agreement
// between re-screens; it was measuring how a superseded transcript happened to
// be tiled into windows.
//
// THE WITNESS THAT MADE IT CONCRETE, and `theLoneVerdictIsNotVotedDownBy…`
// below is it verbatim: `561CEF5B` [692.76–791.70] is ONE `containsAd` row at
// version `deace512`. The only other presence-pass rows over that audio are
// [620.34–720.00] and [720.78–820.80], both `noAds`, both at `37772e3f` — a
// different transcript whose window boundaries do not line up with the claim in
// any way. Un-scoped they outvoted it 2-to-1, the factor read
// `(1+1)/(1+1+2) = 0.5`, and the mark graded 0.350 instead of 0.700.
//
// WHAT THESE TESTS ARE FOR, in two halves that must both hold:
//
//   * the CROSS-version half — a vote from another transcript no longer
//     counts, in BOTH directions. `theLoneVerdictIsNotVotedDownBy…` is the
//     dissent direction; `aCrossVersionAffirmerNoLongerPropsUp…` is the
//     affirmation direction, and it is the one that proves this change is not
//     a licence to raise every grade: there the fix makes the mark WEAKER.
//   * the SAME-version half — the deduction is AIMED, not removed.
//     `aSameVersionDissenterStillDeducts` and `aSameVersionAffirmerStillCounts`
//     fail against any "fix" that simply stops counting.
//
// AND WHAT IT IS NOT. Version scoping changes the GRADE and nothing else. It
// does not drop a stale row from the composition — that is option (b) of the
// bead, it would remove 48 of the 79 marks and 4,739.9 s of 8,048.8 s of marked
// audio on the pull, and it is Dan's call.
// `versionScopingChangesTheGradeAndNotTheGeometry` is the rail that keeps the
// two apart: seconds are version-independent, so a stale row's window bounds
// are still real geometry.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum ScopeFixture {

    static let assetId = "asset-561cef5b"

    /// The two versions of the field witness, verbatim prefixes from the
    /// 2026-08-19 t4 pull's `561CEF5B` rows.
    static let claimVersion = "deace512"
    static let otherVersion = "37772e3f"

    /// The `passA` support payload a real `containsAd` row carries.
    static func coarseSupport(_ certainty: CertaintyBand) -> String {
        #"{"supportLineRefs":[17,18,20],"certainty":"\#(certainty.rawValue)"}"#
    }

    /// A `passB` row's payload: an ARRAY of refined spans, each with its own
    /// band and none of them runner-fabricated.
    static func refinedSupport(_ certainty: CertaintyBand) -> String {
        "[" + #"{"anchors":[],"certainty":"\#(certainty.rawValue)","commercialIntent":"paid","#
            + #""firstLineRef":2,"lastLineRef":2,"ownership":"thirdParty","#
            + #""ownershipInferenceWasSuppressed":false}"# + "]"
    }

    static func row(
        id: String,
        start: Double,
        end: Double,
        version: String,
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
            spansJSON: spansJSON
                ?? (disposition == .containsAd ? coarseSupport(.strong) : "[]"),
            status: status,
            attemptCount: 1,
            errorContext: errorContext,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "kg6i"),
            transcriptVersion: version,
            // playhead-iw7q: EXPLICIT, because the default is now `.unknown`.
            // These fixtures stand for a coarse row the MODEL produced — that is
            // what makes their persisted band attributable at all — and the
            // struct's default deliberately withholds the licence from a writer
            // that says nothing. Saying it here is the fixture doing its job.
            verdictProvenance: .model
        )
    }

    static func compose(rows: [SemanticScanResult]) -> [AdWindow] {
        SemanticSweepMarkComposer.compose(
            scanRows: rows,
            existingWindows: [],
            provenAnchorEdges: nil,
            analysisAssetId: assetId
        )
    }

    /// `maximumMarkConfidence * certainty(strong) * quality(good) * factor`.
    static func grade(_ corroborationFactor: Double) -> Double {
        SemanticSweepMarkComposer.maximumMarkConfidence * corroborationFactor
    }
}

private func isClose(_ lhs: Double, _ rhs: Double, tolerance: Double = 1e-9) -> Bool {
    abs(lhs - rhs) <= tolerance
}

// MARK: - The cross-version half

@Suite("a corroboration vote is a replicate of the SAME experiment (playhead-kg6i)",
       .timeLimit(.minutes(1)))
struct SemanticSweepCorroborationScopeTests {

    private typealias Fx = ScopeFixture

    /// THE FIELD WITNESS, verbatim. One verdict at `deace512`; two `noAds` rows
    /// at `37772e3f`, tiled at boundaries the claim does not share. Against the
    /// shipped composer they outvote it and the mark grades 0.350.
    ///
    /// The `marks.count == 1` assertion is the in-call vacuity control: "the
    /// confidence is 0.70" must never be satisfiable by a composer that emitted
    /// nothing, or by one that split the verdict.
    @Test("a lone verdict is not voted down by a transcript it never saw")
    func theLoneVerdictIsNotVotedDownByAnotherTranscript() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "claim", start: 692.76, end: 791.70, version: Fx.claimVersion),
            Fx.row(id: "other-a", start: 620.34, end: 720.00,
                   version: Fx.otherVersion, disposition: .noAds),
            Fx.row(id: "other-b", start: 720.78, end: 820.80,
                   version: Fx.otherVersion, disposition: .noAds),
        ])

        #expect(marks.count == 1, "control: the verdict still marks")
        guard let mark = marks.first else { return }
        #expect(mark.startTime == 692.76)
        #expect(mark.endTime == 791.70)
        #expect(isClose(mark.confidence, Fx.grade(1.0)),
                "its own experiment is unanimous: \(mark.confidence)")
    }

    /// THE OTHER DIRECTION, and the reason this bead is not "raise every
    /// grade". A claim CONTRADICTED inside its own cohort used to be propped up
    /// by an affirming re-screen of a different transcript. Removing that
    /// affirmer makes the mark WEAKER: `(1+2)/(1+2+1) = 0.75` becomes
    /// `(1+1)/(1+1+1) = 2/3`.
    ///
    /// THE DENIAL IS [150, 250] AND NOT [90, 200] FOR A REASON, and the vacuity
    /// control below is what found it. Two verdicts over identical bounds are
    /// two extents that stage 3 folds into one — but `clearedSpans` also makes
    /// the denial a merge BARRIER, and `coversGap(from: last.end, to:
    /// extent.start)` used to be evaluated with an INVERTED range when the two
    /// extents overlap, so a denial that spans BOTH edges barred a merge that
    /// has no gap to bridge and the fixture silently became two marks.
    ///
    /// **playhead-vz3l FIXED THAT, so [90, 200] would compose to one mark here
    /// too — and this fixture deliberately still reads [150, 250].** It is
    /// pinning a CORROBORATION rule, and a denial overlapping one edge is the
    /// clean dissenting replicate that rule is about; re-pointing it at the
    /// wider window would silently make it a second test of the merge barrier.
    /// The [90, 200] shape has its own rail — see
    /// `SemanticSweepMergeBarrierTests.aBarrierContainingTheWholeOverlapDoesNotBar`
    /// — where the observable is the colliding content-addressed id rather than
    /// this test's confidence.
    @Test("a cross-version affirmer no longer props up a contested claim")
    func aCrossVersionAffirmerNoLongerPropsUpAContestedClaim() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "claim", start: 100, end: 190, version: Fx.claimVersion),
            Fx.row(id: "dissent-same", start: 150, end: 250,
                   version: Fx.claimVersion, disposition: .noAds),
            Fx.row(id: "affirm-other", start: 100, end: 190, version: Fx.otherVersion),
        ])

        #expect(marks.count == 1, "control: the verdict still marks")
        guard let mark = marks.first else { return }
        #expect(isClose(mark.confidence, Fx.grade(2.0 / 3.0)),
                "contested 1-against-1 in its own cohort: \(mark.confidence)")
        #expect(mark.confidence < Fx.grade(0.75),
                "the cross-version affirmer is gone, so this is LOWER than shipped")
    }

    // MARK: - The same-version half: the deduction is aimed, not removed

    /// A re-screen of the SAME transcript that disagreed is a real replicate and
    /// must still deduct. Identical to the field witness except that the two
    /// denials share the claim's version: `(1+1)/(1+1+2) = 0.5`.
    @Test("a dissenter at the same transcript version still deducts")
    func aSameVersionDissenterStillDeducts() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "claim", start: 692.76, end: 791.70, version: Fx.claimVersion),
            Fx.row(id: "same-a", start: 620.34, end: 720.00,
                   version: Fx.claimVersion, disposition: .noAds),
            Fx.row(id: "same-b", start: 720.78, end: 820.80,
                   version: Fx.claimVersion, disposition: .noAds),
        ])

        #expect(marks.count == 1, "control: the verdict still marks")
        guard let mark = marks.first else { return }
        #expect(isClose(mark.confidence, Fx.grade(0.5)),
                "two same-version denials still halve it: \(mark.confidence)")
    }

    /// And an affirming replicate of the same experiment must still be counted.
    /// Two `containsAd` rows over identical bounds plus one denial, all at one
    /// version: `(1+2)/(1+2+1) = 0.75`, against `(1+1)/(1+1+1) = 2/3` with the
    /// second affirmer removed. Both compositions are asserted so the test
    /// cannot pass by the two happening to agree.
    ///
    /// The denial overlaps ONE edge only — see
    /// `aCrossVersionAffirmerNoLongerPropsUpAContestedClaim` for why a denial
    /// spanning both edges would turn this fixture into two marks.
    @Test("an affirming replicate at the same version still counts")
    func aSameVersionAffirmerStillCounts() {
        let dissent = Fx.row(id: "dissent", start: 150, end: 250,
                             version: Fx.claimVersion, disposition: .noAds)
        let first = Fx.row(id: "claim-1", start: 100, end: 190, version: Fx.claimVersion)
        let second = Fx.row(id: "claim-2", start: 100, end: 190, version: Fx.claimVersion)

        let withBoth = Fx.compose(rows: [first, second, dissent])
        let withOne = Fx.compose(rows: [first, dissent])

        #expect(withBoth.count == 1, "control: the two identical verdicts are one mark")
        #expect(withOne.count == 1, "control: the single verdict still marks")
        guard let both = withBoth.first, let one = withOne.first else { return }
        #expect(isClose(both.confidence, Fx.grade(0.75)),
                "two affirmers against one denial: \(both.confidence)")
        #expect(isClose(one.confidence, Fx.grade(2.0 / 3.0)),
                "one affirmer against one denial: \(one.confidence)")
        #expect(both.confidence > one.confidence,
                "the second affirming replicate is worth something")
    }

    // MARK: - A backing PAIR is graded in two cohorts, not one

    /// An extent narrowed by pass B rests on TWO rows, and the refinement is
    /// re-run when the transcript moves, so the two can be at different
    /// versions. Each row's term must be counted in ITS OWN cohort.
    ///
    /// Here the REFINEMENT's cohort is the weak one — two denials at its
    /// version, none at the coarse row's — so the `min` must land on
    /// `(1+0)/(1+0+2) = 1/3`. Three readings this kills at once: the shipped
    /// un-scoped count (which pools them into `(1+1)/(1+1+2) = 0.5`), and any
    /// "fix" that computes one count for the whole pair from the FIRST backing
    /// row's version (which would read 1.0 and grade at the ceiling).
    @Test("a pair is graded in each row's own cohort — the refinement's governs here")
    func aBackingPairIsGradedInTheRefinementsOwnCohort() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "coarse", start: 100, end: 190, version: Fx.claimVersion),
            Fx.row(id: "refine", start: 120, end: 160, version: Fx.otherVersion,
                   scanPass: "passB", spansJSON: Fx.refinedSupport(.strong)),
            Fx.row(id: "other-dissent-a", start: 90, end: 200,
                   version: Fx.otherVersion, disposition: .noAds),
            Fx.row(id: "other-dissent-b", start: 95, end: 205,
                   version: Fx.otherVersion, disposition: .noAds),
        ])

        #expect(marks.count == 1, "control: the refined verdict still marks")
        guard let mark = marks.first else { return }
        #expect(mark.startTime == 120, "control: pass B still narrowed the extent")
        #expect(mark.endTime == 160)
        #expect(isClose(mark.confidence, Fx.grade(1.0 / 3.0)),
                "the weaker of the two cohorts governs: \(mark.confidence)")
    }

    /// The mirror, and it exists because one fixture cannot kill both hoists.
    /// Now the COARSE row's cohort is the weak one, so the `min` must land on
    /// `(1+1)/(1+1+2) = 0.5`. A "fix" that computes one count for the pair from
    /// the LAST backing row's version reads 1.0 here and grades at the ceiling.
    /// (This fixture does NOT distinguish the shipped un-scoped count, which
    /// also reads 0.5 — that is what the tests above are for. It is a rail
    /// against a specific wrong repair, and it is written down as such.)
    @Test("a pair is graded in each row's own cohort — the coarse window's governs here")
    func aBackingPairIsGradedInTheCoarseWindowsOwnCohort() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "coarse", start: 100, end: 190, version: Fx.claimVersion),
            Fx.row(id: "refine", start: 120, end: 160, version: Fx.otherVersion,
                   scanPass: "passB", spansJSON: Fx.refinedSupport(.strong)),
            Fx.row(id: "claim-dissent-a", start: 90, end: 200,
                   version: Fx.claimVersion, disposition: .noAds),
            Fx.row(id: "claim-dissent-b", start: 95, end: 205,
                   version: Fx.claimVersion, disposition: .noAds),
        ])

        #expect(marks.count == 1, "control: the refined verdict still marks")
        guard let mark = marks.first else { return }
        #expect(mark.startTime == 120, "control: pass B still narrowed the extent")
        #expect(isClose(mark.confidence, Fx.grade(0.5)),
                "the weaker of the two cohorts governs: \(mark.confidence)")
    }

    // MARK: - What scoping must NOT become

    /// SCOPING IS NOT DROPPING. A verdict whose version NOTHING else in the row
    /// set shares is still a verdict, and it still marks its own window —
    /// seconds are version-independent. This is the rail that separates option
    /// (a) from option (b): a composer that filtered `scanRows` down to one
    /// version would emit nothing here.
    ///
    /// The versions are chosen so the claim's sorts BELOW the others', because
    /// the tempting wrong repair is "keep the newest version", and a fixture
    /// whose claim happens to be newest could not see it.
    @Test("version scoping changes the GRADE and not the geometry")
    func versionScopingChangesTheGradeAndNotTheGeometry() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "claim", start: 692.76, end: 791.70, version: "tv-aaa"),
            Fx.row(id: "elsewhere-a", start: 620.34, end: 720.00,
                   version: "tv-zzz", disposition: .noAds),
            Fx.row(id: "elsewhere-b", start: 720.78, end: 820.80,
                   version: "tv-zzz", disposition: .noAds),
        ])

        #expect(marks.count == 1, "a row at a version nobody shares is still a verdict")
        guard let mark = marks.first else { return }
        #expect(mark.startTime == 692.76, "its window is unchanged")
        #expect(mark.endTime == 791.70)
        #expect(mark.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
        #expect(mark.decisionState == AdDecisionState.candidate.rawValue)
        #expect(mark.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(mark.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(mark.id == SemanticSweepMarkComposer.markId(
            analysisAssetId: ScopeFixture.assetId, start: 692.76, end: 791.70
        ), "the id still addresses geometry only")
    }

    // MARK: - The counter itself

    /// The count, read directly, so a composer-level rail cannot be the only
    /// thing that pins it. The two cohorts are deliberately ASYMMETRIC — v1 has
    /// a denial and v2 does not — so inverting the version comparison changes
    /// the answer rather than merely relabelling it.
    @Test("corroboration counts only the rows at the version it was asked about")
    func corroborationCountsOnlyItsOwnVersion() {
        let rows = [
            Fx.row(id: "v1-affirm", start: 100, end: 200, version: "tv-1"),
            Fx.row(id: "v1-deny", start: 150, end: 250, version: "tv-1", disposition: .noAds),
            Fx.row(id: "v2-affirm", start: 100, end: 200, version: "tv-2"),
            // Excluded for reasons that predate this bead, and each still must be:
            Fx.row(id: "v1-refine", start: 120, end: 180, version: "tv-1",
                   scanPass: "passB", spansJSON: Fx.refinedSupport(.strong)),
            Fx.row(id: "v1-cancelled", start: 100, end: 200, version: "tv-1",
                   status: .cancelled),
            Fx.row(id: "v1-sentinel", start: 100, end: 200, version: "tv-1",
                   disposition: .noAds, status: .noAds,
                   errorContext: SemanticScanResult.noWorkSentinelErrorContextPrefix
                       + "emptySegments"),
        ]
        let extent = SemanticSweepMarkComposer.Extent(start: 140, end: 160)

        let atOne = SemanticSweepMarkComposer.corroboration(
            for: extent, in: rows, atTranscriptVersion: "tv-1"
        )
        let atTwo = SemanticSweepMarkComposer.corroboration(
            for: extent, in: rows, atTranscriptVersion: "tv-2"
        )
        let atNobody = SemanticSweepMarkComposer.corroboration(
            for: extent, in: rows, atTranscriptVersion: "tv-nobody"
        )

        #expect(atOne.affirming == 1, "the pass-B, cancelled and sentinel rows do not vote")
        #expect(atOne.dissenting == 1)
        #expect(atTwo.affirming == 1)
        #expect(atTwo.dissenting == 0, "tv-1's denial is not tv-2's replicate")
        #expect(atNobody.affirming == 0, "a version nobody carries has no replicates")
        #expect(atNobody.dissenting == 0)
    }
}
