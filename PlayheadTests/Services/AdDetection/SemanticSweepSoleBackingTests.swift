// SemanticSweepSoleBackingTests.swift
// playhead-my33 — an unlocalised verdict may not hold a banner up ON ITS OWN.
//
// # The row shape
//
// A coarse (`passA`) `semantic_scan_results` row can carry
// `disposition = containsAd` and `spansJSON = "[]"` at the same time: `"[]"` is
// what `BackfillJobRunner.encodeSupport` writes for a NIL support object. The
// row says *"there is an ad in this ~95 s window"* and points at nothing inside
// it. On the 2026-08-19 t4 pull 19 of the 301 coarse `containsAd` rows are in
// that state, and they are only TEN DISTINCT WINDOWS — the sweep re-scans an
// episode each time its transcript moves.
//
// # Dan's decision, 2026-08-21
//
// **DROP ONLY WHERE IT IS THE SOLE BACKING.** Not keep-all (which keeps four
// wrong marks to keep six right ones), not drop-all (which removes six correct
// detections to remove those four). An unlocalised row may still contribute its
// window WHEN ANOTHER ROW CORROBORATES THE SAME WINDOW; when it is the only
// thing holding a mark up, it contributes nothing and the mark does not exist.
//
// # What "corroborates" means, and the one clause that decides the bead
//
// A DIFFERENT, ADMISSIBLE, PRESENCE-PASS row over THE SAME WINDOW — bounds
// equal to `SupportLineIndex.boundaryEpsilon`, `transcriptVersion` IGNORED.
//
// The version clause is the substance. playhead-kg6i had just landed the rule
// that a vote from a superseded transcript is not a replicate of the same
// experiment, and the reasoning does not carry here: kg6i governs an AGREEMENT
// STATISTIC whose cohort is selected by OVERLAP, so a cross-version row need
// not be about the same audio at all (its witness is `561CEF5B`
// [692.76–791.70] against two rows tiled at [620.34–720.00] and
// [720.78–820.80]). This predicate pins the audio DIRECTLY by requiring
// identical bounds, and then asks a MEMBERSHIP question with no dissent term.
// Once the seconds are identical, a re-transcription makes the second screening
// MORE independent, not less.
//
// It is also decisive rather than decorative: on the pull the 19 rows sit at 19
// distinct `(window, transcriptVersion)` pairs, so a version-scoped predicate
// corroborates NOTHING and Dan's rule would collapse into the drop-all option
// he declined. `aCrossVersionReplicateCorroborates` is that rail, and it is the
// one to read first if anyone proposes scoping this.
//
// # What the suites pin
//
//   1. the rule in BOTH directions — sole drops, corroborated keeps;
//   2. the predicate's four clauses, each in both directions;
//   3. that only `.absent` moved: `.unreadable` still keeps its window alone,
//      which is the control that says this is not "stage 6 stopped working";
//   4. that a surviving mark's GRADE and TIER are untouched, so nothing here
//      can reach auto-skip.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum SoleBackingFixture {

    static let assetId = "asset-my33"

    /// The two versions of the field's re-scan shape, verbatim prefixes from
    /// the 2026-08-19 t4 pull's `A9F6DF05` rows over [4038.48–4105.92].
    static let versionA = "3c8cdb16"
    static let versionB = "dacc49d8"

    /// What a coarse row carries when the model DID name lines.
    static func coarseSupport(_ refs: [Int] = [17, 18, 20],
                              certainty: CertaintyBand = .strong) -> String {
        let joined = refs.map(String.init).joined(separator: ",")
        return #"{"supportLineRefs":[\#(joined)],"certainty":"\#(certainty.rawValue)"}"#
    }

    /// What `BackfillJobRunner.encodeSupport` writes for a NIL support object.
    /// A `containsAd` row carrying THIS is `Localisation.absent`.
    static let namedNothing = "[]"

    static func row(
        id: String,
        start: Double,
        end: Double,
        version: String = versionA,
        disposition: CoarseDisposition = .containsAd,
        status: SemanticScanStatus = .success,
        scanPass: String = "passA",
        errorContext: String? = nil,
        transcriptQuality: TranscriptQuality = .good,
        spansJSON: String = namedNothing
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
            spansJSON: spansJSON,
            status: status,
            attemptCount: 1,
            errorContext: errorContext,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "my33"),
            transcriptVersion: version
        )
    }

    static func compose(rows: [SemanticScanResult],
                        existing: [AdWindow] = []) -> [AdWindow] {
        SemanticSweepMarkComposer.compose(
            scanRows: rows,
            existingWindows: existing,
            supportLines: nil,
            analysisAssetId: assetId
        )
    }

    static func extents(of marks: [AdWindow]) -> [ClosedRange<Double>] {
        marks.map { $0.startTime...$0.endTime }.sorted { $0.lowerBound < $1.lowerBound }
    }
}

// MARK: - 1. The rule

@Suite("an unlocalised verdict holds a mark up only when something replicates it (playhead-my33)",
       .timeLimit(.minutes(1)))
struct SemanticSweepSoleBackingTests {

    private typealias Fx = SoleBackingFixture

    /// **THE RULE, SOLE HALF.** One `containsAd` row, no support object, no
    /// replicate. Before Dan's call this composed a 90 s mark over audio the
    /// model pointed at no part of.
    @Test("a lone verdict that named nothing produces NO mark")
    func aLoneUnlocalisedVerdictProducesNothing() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "lone", start: 700, end: 790),
        ])

        #expect(marks.isEmpty, "composed \(Fx.extents(of: marks))")
    }

    /// **THE RULE, CORROBORATED HALF, AND THE CONTROL FOR THE ONE ABOVE.** The
    /// same row plus a second screening of the same window. Nothing else
    /// differs, so the mark's existence is attributable to the replicate alone.
    @Test("a replicated verdict that named nothing keeps its whole window")
    func aReplicatedUnlocalisedVerdictKeepsItsWindow() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "first", start: 700, end: 790),
            Fx.row(id: "second", start: 700, end: 790, version: Fx.versionB),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 700)
        #expect(marks.first?.endTime == 790,
                "a corroborated absent row contributes its WINDOW, not a narrowing")
    }

    /// **DAN'S OWN FIELD CASE, END TO END**, at the geometry the device holds.
    /// `CD2976E6` [1131.6–1210.86] named lines (unreadable here, so it keeps
    /// its window) and [1211.16–1287.18] named nothing and stands alone. The
    /// two merge across a 0.3 s gap; the mark must fall to the first window.
    @Test("Dan's CD2976E6 mark falls from [1131.6-1287.2] to [1131.6-1210.9]")
    func dansMarkLosesItsUnbackedTail() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "localised", start: 1_131.60, end: 1_210.86,
                   spansJSON: Fx.coarseSupport()),
            Fx.row(id: "absent-tail", start: 1_211.16, end: 1_287.18),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 1_131.60)
        #expect(marks.first?.endTime == 1_210.86,
                "76.3 s of Honnold on sponsorship income is given back")
    }

    /// The same two rows with the tail REPLICATED. This is the control that
    /// keeps the test above from passing for the wrong reason — the tail is not
    /// dropped because it is a tail, it is dropped because nothing backs it.
    @Test("the same tail, replicated, is kept")
    func aReplicatedTailIsKept() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "localised", start: 1_131.60, end: 1_210.86,
                   spansJSON: Fx.coarseSupport()),
            Fx.row(id: "absent-tail", start: 1_211.16, end: 1_287.18),
            Fx.row(id: "absent-tail-2", start: 1_211.16, end: 1_287.18,
                   version: Fx.versionB),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.endTime == 1_287.18)
    }

    /// The field's most common shape: four re-scans of one window, ALL of them
    /// unlocalised. They corroborate each other — `A9F6DF05` [4038.48–4105.92]
    /// really does appear four times on the pull, at four versions, and not one
    /// of the four named a line.
    @Test("four unlocalised re-scans of one window corroborate one another")
    func mutualCorroborationAmongAbsentRows() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "s1", start: 4_038.48, end: 4_105.92, version: "3c8cdb16"),
            Fx.row(id: "s2", start: 4_038.48, end: 4_105.92, version: "dacc49d8"),
            Fx.row(id: "s3", start: 4_038.48, end: 4_105.92, version: "804d6f18"),
            Fx.row(id: "s4", start: 4_038.48, end: 4_105.92, version: "cb4e51b9"),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 4_038.48)
        #expect(marks.first?.endTime == 4_105.92)
    }

    /// A LOCALISED row over the same window corroborates too, and the absent
    /// row still contributes its whole window — so the union is the tile, not
    /// the narrowing. This is `A9F6DF05` ~6814 s and ~2656 s on the pull, where
    /// one of the re-scans did cite a line.
    @Test("a localised sibling corroborates, and the tile is kept")
    func aLocalisedSiblingCorroborates() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "absent", start: 6_814.02, end: 6_874.14),
            Fx.row(id: "named", start: 6_814.02, end: 6_874.14,
                   version: Fx.versionB, spansJSON: Fx.coarseSupport([364])),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 6_814.02)
        #expect(marks.first?.endTime == 6_874.14)
    }

    /// **THE CONTROL THAT SAYS ONLY `.absent` MOVED.** A row that NAMED lines
    /// we cannot read keeps its whole window with no replicate at all. Our
    /// records failed, not the model, and `Localisation` holds the two apart
    /// precisely so this bead could move one of them.
    @Test("a lone UNREADABLE verdict still keeps its window")
    func aLoneUnreadableVerdictIsUntouched() {
        let marks = Fx.compose(rows: [
            Fx.row(id: "unreadable", start: 700, end: 790,
                   spansJSON: Fx.coarseSupport()),
        ])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 700)
        #expect(marks.first?.endTime == 790)
    }

    /// Nothing here touches the grade or the tier. A dropped mark is dropped;
    /// a kept one is the mark it always was, `markOnly` on `.unanchored` edges,
    /// so no confidence this lane can compute reaches auto-skip.
    @Test("a corroborated mark keeps its grade and stays markOnly on unanchored edges")
    func aCorroboratedMarkKeepsItsGradeAndTier() {
        let rows = [
            Fx.row(id: "first", start: 700, end: 790),
            Fx.row(id: "second", start: 700, end: 790, version: Fx.versionB),
        ]
        let marks = Fx.compose(rows: rows)

        #expect(marks.count == 1)
        // The grade is the FLOOR the unevidenced payload earns, and this bead
        // buys no promotion. Two things keep it there: `corroborationFactor` is
        // 1.0 whenever nothing dissents, however many replicates agree
        // (playhead-92im — a repeat that agrees is not a bonus), and
        // playhead-kg6i grades each backing row inside its OWN version's
        // cohort, so a cross-version replicate is not even counted here. The
        // rule decides WHETHER the mark exists; it touches no factor.
        #expect(marks.first?.confidence == SemanticSweepMarkComposer.unevidencedMarkConfidence)
        #expect(marks.first?.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
        #expect(marks.first?.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(marks.first?.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
    }
}

// MARK: - 2. The predicate, clause by clause

@Suite("what CORROBORATES an unlocalised verdict, and what does not (playhead-my33)",
       .timeLimit(.minutes(1)))
struct SemanticSweepCorroborationPredicateTests {

    private typealias Fx = SoleBackingFixture

    private static func claim() -> SemanticScanResult {
        Fx.row(id: "claim", start: 700, end: 790)
    }

    /// **THE kg6i RAIL.** A replicate formed against a DIFFERENT transcript
    /// corroborates. Scoping this predicate to one version — the change kg6i's
    /// reasoning invites — makes this fail, and on the t4 pull it would turn
    /// Dan's sole-backing rule into the drop-all he declined.
    @Test("a CROSS-VERSION replicate corroborates")
    func aCrossVersionReplicateCorroborates() {
        let claim = Self.claim()
        let other = Fx.row(id: "other", start: 700, end: 790, version: Fx.versionB)

        #expect(claim.transcriptVersion != other.transcriptVersion, "control: versions differ")
        #expect(SemanticSweepMarkComposer.corroborates(other, claim))
        #expect(SemanticSweepMarkComposer.isCorroborated(claim, in: [claim, other]))
    }

    /// And a SAME-version replicate corroborates too, so the rule is not
    /// accidentally requiring the versions to DIFFER — the mirror of the rail
    /// above, and the one an inverted comparison would leave standing.
    @Test("a SAME-version replicate corroborates")
    func aSameVersionReplicateCorroborates() {
        let claim = Self.claim()
        let other = Fx.row(id: "other", start: 700, end: 790, version: Fx.versionA)

        #expect(claim.transcriptVersion == other.transcriptVersion, "control: versions match")
        #expect(SemanticSweepMarkComposer.corroborates(other, claim))
    }

    /// A row cannot corroborate itself, however many times the caller passes
    /// it. `id` is the table's primary key, so this is exact rather than a
    /// heuristic.
    @Test("a row does not corroborate itself")
    func aRowDoesNotCorroborateItself() {
        let claim = Self.claim()

        #expect(!SemanticSweepMarkComposer.corroborates(claim, claim))
        #expect(!SemanticSweepMarkComposer.isCorroborated(claim, in: [claim]))
        #expect(!SemanticSweepMarkComposer.isCorroborated(claim, in: [claim, claim, claim]),
                "three copies of one row are still one screening")
    }

    /// **BOUND EQUALITY, NOT OVERLAP.** A row that overlaps most of the window
    /// is a claim about DIFFERENT audio, and an unlocalised claim says nothing
    /// about which part of its own tile is the ad — so an overlapping
    /// affirmation cannot speak for it.
    @Test("an OVERLAPPING row does not corroborate")
    func anOverlappingRowDoesNotCorroborate() {
        let claim = Self.claim()
        let overlapping = Fx.row(id: "overlap", start: 740, end: 830, version: Fx.versionB)

        #expect(overlapping.windowStartTime < claim.windowEndTime, "control: they DO overlap")
        #expect(!SemanticSweepMarkComposer.corroborates(overlapping, claim))
    }

    /// The field geometry of Dan's veto: the neighbour is 0.3 s away and shares
    /// no audio at all. Under an overlap predicate the two would still be
    /// separate, so this rail exists for the ADJACENT case specifically — it is
    /// what an "or touching" relaxation would break.
    @Test("an ADJACENT row does not corroborate")
    func anAdjacentRowDoesNotCorroborate() {
        let claim = Fx.row(id: "tail", start: 1_211.16, end: 1_287.18)
        let neighbour = Fx.row(id: "head", start: 1_131.60, end: 1_210.86,
                               version: Fx.versionB, spansJSON: Fx.coarseSupport())

        #expect(!SemanticSweepMarkComposer.corroborates(neighbour, claim))
    }

    /// The tolerance is `SupportLineIndex.boundaryEpsilon` — enough to absorb a
    /// last-bit `REAL` round trip, far too small to reach another window.
    @Test("bounds within boundaryEpsilon corroborate; bounds outside it do not")
    func theEpsilonIsATolerance() {
        let claim = Self.claim()
        let inside = Fx.row(id: "inside",
                            start: 700 + SupportLineIndex.boundaryEpsilon / 2,
                            end: 790 - SupportLineIndex.boundaryEpsilon / 2,
                            version: Fx.versionB)
        let outside = Fx.row(id: "outside", start: 700, end: 790 + 0.01,
                             version: Fx.versionB)

        #expect(SemanticSweepMarkComposer.corroborates(inside, claim))
        #expect(!SemanticSweepMarkComposer.corroborates(outside, claim),
                "10 ms is not a rounding error")
    }

    /// A DENIAL over the same window is not a corroboration. It is the opposite
    /// claim, and `clearedSpans` already gives it the job it does have.
    @Test("a noAds row over the same window does not corroborate")
    func aDenialDoesNotCorroborate() {
        let claim = Self.claim()
        let denial = Fx.row(id: "denial", start: 700, end: 790,
                            version: Fx.versionB, disposition: .noAds)

        #expect(!SemanticSweepMarkComposer.corroborates(denial, claim))
        #expect(Fx.compose(rows: [claim, denial]).isEmpty,
                "and it does not rescue the mark end to end either")
    }

    /// A row that never EXAMINED its window did not look, so its `disposition`
    /// column is not a verdict about the audio. The pull's own instance is the
    /// `abstain | exceededContextWindow` row over `A9F6DF05` [68.9–187.3];
    /// this uses a `containsAd` disposition so the STATUS is the only thing
    /// that can be doing the work.
    @Test("an unexamined row does not corroborate")
    func anUnexaminedRowDoesNotCorroborate() {
        let claim = Self.claim()
        let unexamined = Fx.row(id: "unexamined", start: 700, end: 790,
                                version: Fx.versionB, status: .exceededContextWindow)

        #expect(unexamined.disposition == .containsAd, "control: only the status differs")
        #expect(!SemanticSweepMarkComposer.corroborates(unexamined, claim))
        #expect(Fx.compose(rows: [claim, unexamined]).isEmpty)
    }

    /// A playhead-pz32 no-work sentinel spans the whole attempted range while
    /// meaning "no work was performed". It reaches `didExamineWindow` through
    /// `errorContext`, not through `status`, so it is a second way to fail the
    /// same clause and a separate rail.
    @Test("a no-work sentinel does not corroborate")
    func aNoWorkSentinelDoesNotCorroborate() {
        let claim = Self.claim()
        let sentinel = Fx.row(id: "sentinel", start: 700, end: 790,
                              version: Fx.versionB,
                              errorContext: "noWork:emptySegments")

        #expect(sentinel.status == .success, "control: the STATUS says examined")
        #expect(!SemanticSweepMarkComposer.corroborates(sentinel, claim))
        #expect(Fx.compose(rows: [claim, sentinel]).isEmpty)
    }

    /// A REFINEMENT is a second look at a claim already made, not a second
    /// screening of the audio, so it is not independent of the row it would be
    /// propping up — the same exclusion `corroboration(for:in:atTranscriptVersion:)`
    /// and `clearedSpans(in:)` both make.
    ///
    /// THIS CLAUSE IS NOT OBSERVABLE THROUGH `compose`, AND SAYING SO IS THE
    /// POINT. An affirming `passB` row overlapping the extent is itself a
    /// CONTRIBUTOR — `localise` admits every `isPresenceVerdict` row regardless
    /// of pass, and its localisation is `.named([its own window])` — so the mark
    /// survives whether or not the coarse row was corroborated. Deleting the
    /// `scanPass` guard is therefore an EQUIVALENT mutation at the compose
    /// seam, and this direct assertion is the only thing that can kill it. It
    /// is kept because it STATES the rule: if `localise`'s contributor set ever
    /// narrows to the presence pass, the guard becomes load-bearing the same
    /// day, and a reader should not have to rediscover why it is there.
    @Test("an affirming passB row does not corroborate")
    func aRefinementDoesNotCorroborate() {
        let claim = Self.claim()
        let refinement = Fx.row(id: "refine", start: 700, end: 790,
                                version: Fx.versionB, scanPass: "passB",
                                spansJSON: Fx.namedNothing)

        #expect(SemanticSweepMarkComposer.isPresenceVerdict(refinement),
                "control: it IS an admissible presence verdict, only the pass differs")
        #expect(!SemanticSweepMarkComposer.corroborates(refinement, claim))
    }

    /// `isCorroborated` is a search, and a claim in a crowd of irrelevant rows
    /// finds nothing. The vacuity guard for every rail above: without this, a
    /// predicate that simply returned `false` would pass most of them.
    @Test("a claim among rows that do not match finds no corroborator")
    func aCrowdOfNonMatchesCorroboratesNothing() {
        let claim = Self.claim()
        let crowd = [
            claim,
            Fx.row(id: "elsewhere", start: 100, end: 190, version: Fx.versionB),
            Fx.row(id: "overlap", start: 740, end: 830, version: Fx.versionB),
            Fx.row(id: "denial", start: 700, end: 790, version: Fx.versionB,
                   disposition: .noAds),
        ]

        #expect(!SemanticSweepMarkComposer.isCorroborated(claim, in: crowd))
        // …and one real replicate flips it, so the search is reachable at all.
        let withReplicate = crowd + [Fx.row(id: "real", start: 700, end: 790,
                                            version: "804d6f18")]
        #expect(SemanticSweepMarkComposer.isCorroborated(claim, in: withReplicate))
    }
}
