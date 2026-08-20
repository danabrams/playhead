// SupportLineLocalisationTests.swift
// playhead-shu5 — a semanticSweepMark's extent is the seconds the MODEL named.
//
// THE FIELD CASE, and it is Dan's own correction of 2026-08-19. He listened to
// 'Most Replayed Moment: Fear Is A Skill You Can Train' (asset CD2976E6),
// confirmed the pre-roll and post-roll, and vetoed two mid-episode marks:
// *"pre and post roll were spot on but there were two false positives in the
// transcript"*. Both true positives came from `dayZeroRediffByteExact` at
// confidence 1.0; both false positives were `semanticSweepMark`.
//
// The second of them, [1510.4–1611.4], is 101.0 s. The window DOES contain
// promotional content — the last 9.5 s, verbatim from `transcript_chunks`:
// *"What you just listened to was a most replayed moment from a previous
// episode. If you want to listen to that full episode, I've linked it down
// below. Check the description."* The model was RIGHT and the boundary was
// wrong: 92 s of the guest talking about climbing and about saying goodbye to
// his wife was inside a banner.
//
// The model had said so. The row's `spansJSON` reads
// `{"certainty":"strong","supportLineRefs":[62]}`, and the identifier
// `supportLineRefs` appeared NOWHERE in the composer. A verdict ABOUT a window
// was stored as a claim about its WHOLE EXTENT.
//
// WHAT THE INDICES ARE, and how it was proven — see `SupportLineIndex`'s
// header for the full record. Short version: they are
// `AdTranscriptSegment.segmentIndex` values in the segmentation of the row's
// OWN `transcriptVersion`, and on the 2026-08-19 t4 pull an offline
// reconstruction reproduces the device's version hash exactly on six assets,
// reproduces 321 of 321 persisted `passA` windows exactly, and finds 276 of
// 276 support refs inside their own window's index range with zero outside.
//
// THE THREE THINGS THESE SUITES PIN:
//   1. localisation happens, from both sources — a declined pass-B row's own
//      window, and refs resolved through a version-matched index;
//   2. it REFUSES rather than guesses whenever the coordinate system might not
//      be the row's, because guessing lands the mark on the show;
//   3. it can only SHRINK — never admit a mark the pipeline suppressed, never
//      delete one whose evidence it could not read.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum LocalisationFixture {

    static let assetId = "asset-cd2976e6"
    static let transcriptVersion = "807613cf4b0f2898cc1437afe79b480f"

    /// One transcript line, as `TranscriptSegmenter` would produce it.
    static func line(
        _ index: Int,
        _ start: Double,
        _ end: Double,
        atoms: ClosedRange<Int>
    ) -> (Int, SupportLineIndex.Line) {
        (index, SupportLineIndex.Line(
            startTime: start,
            endTime: end,
            firstAtomOrdinal: atoms.lowerBound,
            lastAtomOrdinal: atoms.upperBound
        ))
    }

    /// The four lines of CD2976E6's [1510.4–1611.4] window, as the segmentation
    /// at `807613cf` had them. Line 62 is the one the model cited.
    static var fieldLines: [Int: SupportLineIndex.Line] {
        Dictionary(uniqueKeysWithValues: [
            line(59, 1_483.5, 1_510.02, atoms: 1_764...1_795),
            line(60, 1_510.38, 1_559.58, atoms: 1_796...1_845),
            line(61, 1_560.3, 1_570.62, atoms: 1_846...1_860),
            line(62, 1_590.0, 1_611.42, atoms: 1_861...1_868),
            line(63, 1_615.68, 1_654.2, atoms: 1_869...1_900),
        ])
    }

    static var fieldIndex: SupportLineIndex {
        SupportLineIndex(transcriptVersion: transcriptVersion, lines: fieldLines)
    }

    static func support(_ refs: [Int], certainty: CertaintyBand = .strong) -> String {
        let joined = refs.map(String.init).joined(separator: ",")
        return #"{"certainty":"\#(certainty.rawValue)","supportLineRefs":[\#(joined)]}"#
    }

    static func row(
        id: String,
        start: Double,
        end: Double,
        atoms: ClosedRange<Int> = 0...1,
        disposition: CoarseDisposition = .containsAd,
        status: SemanticScanStatus = .success,
        scanPass: String = "passA",
        transcriptQuality: TranscriptQuality = .good,
        transcriptVersion version: String = transcriptVersion,
        spansJSON: String? = nil
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: atoms.lowerBound,
            windowLastAtomOrdinal: atoms.upperBound,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: spansJSON ?? (disposition == .containsAd ? support([62]) : "[]"),
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "shu5"),
            transcriptVersion: version
        )
    }

    /// The CD2976E6 [1510.4–1611.4] coarse row, verbatim from the pull.
    static var fieldCoarseRow: SemanticScanResult {
        row(id: "scan-coarse", start: 1_510.38, end: 1_611.42,
            atoms: 1_796...1_868, spansJSON: support([62]))
    }

    /// Its pass-B refinement, which EXAMINED [1590.0–1611.42] and found no
    /// edges. That window IS `supportLineRefs: [62]` expanded to
    /// `minimumZoomSpanLines` and projected into seconds by the runner.
    static var fieldRefinementRow: SemanticScanResult {
        row(id: "scan-refine", start: 1_590.0, end: 1_611.42,
            atoms: 1_846...1_868, disposition: .noAds, scanPass: "passB",
            spansJSON: "[]")
    }

    static func compose(
        rows: [SemanticScanResult],
        existing: [AdWindow] = [],
        supportLines: SupportLineIndex? = nil
    ) -> [AdWindow] {
        SemanticSweepMarkComposer.compose(
            scanRows: rows,
            existingWindows: existing,
            supportLines: supportLines,
            analysisAssetId: assetId
        )
    }

    static func window(id: String, start: Double, end: Double) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 0.4,
            boundaryState: "aggregator",
            decisionState: AdDecisionState.candidate.rawValue,
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
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }
}

// MARK: - 1. What the refs index into

@Suite("supportLineRefs resolve against the row's OWN segmentation, or not at all (playhead-shu5)",
       .timeLimit(.minutes(1)))
struct SupportLineIndexTests {

    private typealias Fx = LocalisationFixture

    private static func rowWindow(
        atoms: ClosedRange<Int> = 1_796...1_868,
        start: Double = 1_510.38,
        end: Double = 1_611.42,
        version: String = Fx.transcriptVersion
    ) -> SupportLineIndex.RowWindow {
        SupportLineIndex.RowWindow(
            transcriptVersion: version,
            firstAtomOrdinal: atoms.lowerBound,
            lastAtomOrdinal: atoms.upperBound,
            startTime: start,
            endTime: end
        )
    }

    /// THE FIELD ROW. `supportLineRefs: [62]` resolves to [1590.0, 1611.42] —
    /// which is where the "most replayed moment … check the description" call
    /// to action lives (1601.94–1611.42), and 79.6 s narrower than the window.
    @Test("the field row's single ref resolves to the line that holds the CTA")
    func theFieldRefResolvesToTheCTA() {
        let spans = Fx.fieldIndex.resolve(supportLineRefs: [62], in: Self.rowWindow())

        #expect(spans?.count == 1)
        #expect(spans?.first?.start == 1_590.0)
        #expect(spans?.first?.end == 1_611.42)
        // The CTA's own transcript bounds sit inside it.
        #expect((spans?.first?.start ?? .infinity) <= 1_601.94)
        #expect((spans?.first?.end ?? 0) >= 1_611.18)
    }

    /// A CONSECUTIVE run is one span. Two lines of ad copy in a row are one
    /// region of speech, not two claims.
    @Test("consecutive refs become one span")
    func consecutiveRefsBecomeOneSpan() {
        let spans = Fx.fieldIndex.resolve(supportLineRefs: [60, 61], in: Self.rowWindow())

        #expect(spans?.count == 1)
        #expect(spans?.first?.start == 1_510.38)
        #expect(spans?.first?.end == 1_570.62)
    }

    /// A GAP is two spans. Joining them would swallow the unsupported line
    /// between — this bead's own defect, one granularity down.
    @Test("a gap between refs becomes two spans, never one straddling the unsupported line")
    func aGapBecomesTwoSpans() {
        let spans = Fx.fieldIndex.resolve(supportLineRefs: [60, 62], in: Self.rowWindow())

        #expect(spans?.count == 2)
        #expect(spans?.first?.end == 1_559.58, "the first span stops at line 60's end")
        #expect(spans?.last?.start == 1_590.0, "the second starts at line 62, not at 61")
    }

    /// Refs are deduplicated and order-independent — the model's emission order
    /// must not change the geometry.
    @Test("refs are order-independent and deduplicated")
    func refsAreOrderIndependent() {
        let ascending = Fx.fieldIndex.resolve(supportLineRefs: [60, 61], in: Self.rowWindow())
        let jumbled = Fx.fieldIndex.resolve(supportLineRefs: [61, 60, 61], in: Self.rowWindow())

        #expect(ascending == jumbled)
    }

    /// THE REFUSAL THAT MATTERS MOST. On the real device, CD2976E6's row was
    /// scanned at `807613cf` and the transcript has since moved to `cd175ee9`,
    /// where a later final pass added one segment boundary — so today's line 62
    /// is [1570.98, 1593.24], twenty-two seconds of the guest describing saying
    /// goodbye to his wife, and the line the model MEANT is today's 63. An
    /// index that answered anyway would mark the show AND miss the ad.
    @Test("an index for a different transcript version resolves NOTHING")
    func aStaleVersionResolvesNothing() {
        let today = SupportLineIndex(
            transcriptVersion: "cd175ee9c1bc71473bc7789e97c9f9bd",
            lines: Fx.fieldLines
        )

        #expect(today.resolve(supportLineRefs: [62], in: Self.rowWindow()) == nil)
        #expect(Fx.fieldIndex.resolve(supportLineRefs: [62], in: Self.rowWindow()) != nil,
                "control: the same refs DO resolve against the matching version")
    }

    /// The version hash covers `normalizedText` and not TIMES, so a matching
    /// version is not by itself proof that the geometry is the same. The row's
    /// own window must reproduce here.
    @Test("a window whose bounds do not reproduce in this index resolves nothing")
    func aWindowThatDoesNotReproduceResolvesNothing() {
        let drifted = Self.rowWindow(end: 1_611.42 + 0.5)

        #expect(Fx.fieldIndex.resolve(supportLineRefs: [62], in: drifted) == nil)
    }

    /// Same check on the other coordinate: the atom range must be the run's.
    /// This is what caught the real CD2976E6 drift — the times still matched at
    /// the edges and the atom count inside did not.
    @Test("a window whose atom ordinals name no line resolves nothing")
    func aWindowWithUnknownAtomOrdinalsResolvesNothing() {
        let drifted = Self.rowWindow(atoms: 1_796...1_919)

        #expect(Fx.fieldIndex.resolve(supportLineRefs: [62], in: drifted) == nil)
    }

    /// A ref outside the lines the model was SHOWN for this window is not this
    /// window's ref. `focusLineRefs` filters exactly this way upstream.
    @Test("a ref outside the window's own line range resolves nothing")
    func aRefOutsideTheWindowResolvesNothing() {
        #expect(Fx.fieldIndex.resolve(supportLineRefs: [63], in: Self.rowWindow()) == nil)
        #expect(Fx.fieldIndex.resolve(supportLineRefs: [62, 63], in: Self.rowWindow()) == nil)
    }

    /// A run with a HOLE in it is a different segmentation wearing the same
    /// endpoints — refuse rather than span the hole.
    @Test("a non-contiguous run of lines resolves nothing")
    func aRunWithAHoleResolvesNothing() {
        var holed = Fx.fieldLines
        holed[61] = nil
        let index = SupportLineIndex(transcriptVersion: Fx.transcriptVersion, lines: holed)

        #expect(index.resolve(supportLineRefs: [62], in: Self.rowWindow()) == nil)
    }

    /// No refs is not a localisation — it is the caller's job to tell that
    /// apart from an unreadable one, and this returns the same `nil` either
    /// way, so the caller may not learn the difference from here.
    @Test("an empty ref list resolves nothing")
    func anEmptyRefListResolvesNothing() {
        #expect(Fx.fieldIndex.resolve(supportLineRefs: [], in: Self.rowWindow()) == nil)
    }

    /// Built from real segments, the index is keyed by `segmentIndex` — not by
    /// array position, which is what a `for (i, s) in enumerated()` would give
    /// and which differs the moment a caller hands over a narrowed slice.
    @Test("building from segments keys by segmentIndex, not array position")
    func buildingFromSegmentsKeysBySegmentIndex() {
        let atoms = (0...3).map {
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: Fx.assetId,
                    transcriptVersion: Fx.transcriptVersion,
                    atomOrdinal: $0
                ),
                contentHash: "h\($0)",
                startTime: Double($0) * 10,
                endTime: Double($0) * 10 + 5,
                text: "line \($0)",
                chunkIndex: $0
            )
        }
        let segments = [
            AdTranscriptSegment(atoms: Array(atoms[0...1]), segmentIndex: 40),
            AdTranscriptSegment(atoms: Array(atoms[2...3]), segmentIndex: 41),
        ]
        let index = SupportLineIndex(segments: segments, transcriptVersion: Fx.transcriptVersion)

        #expect(index.lineCount == 2)
        #expect(index.line(40)?.startTime == 0)
        #expect(index.line(41)?.endTime == 35)
        #expect(index.line(0) == nil, "array position 0 is NOT line 0")
    }
}

// MARK: - 2. The composer stage

@Suite("stage 6 narrows a mark to the seconds the model named (playhead-shu5)",
       .timeLimit(.minutes(1)))
struct SemanticSweepLocalisationTests {

    private typealias Fx = LocalisationFixture

    /// **THE BEAD, END TO END.** Dan's second false positive, composed from the
    /// two rows the device actually holds. Before this change the mark is the
    /// whole 101.0 s scan window; after it, it is the 21.4 s the model pointed
    /// at, and the CTA is inside it.
    @Test("Dan's CD2976E6 false positive lands on the CTA, not on the whole window")
    func theFieldFalsePositiveLandsOnTheCTA() {
        let marks = Fx.compose(rows: [Fx.fieldCoarseRow, Fx.fieldRefinementRow])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 1_590.0)
        #expect(marks.first?.endTime == 1_611.42)
        let duration = (marks.first?.endTime ?? 0) - (marks.first?.startTime ?? 0)
        #expect(duration < 25, "101.0 s of scan window became \(duration) s of ad")
        #expect((marks.first?.startTime ?? 0) > 1_510.38,
                "79.6 s of the guest talking about climbing is given back")
    }

    /// The same narrowing from the OTHER source: no pass-B row at all, refs
    /// resolved through the index. This is the path that covers the 72 rows on
    /// the pull whose refinement never ran.
    @Test("a coarse row with no refinement is narrowed by its supportLineRefs")
    func refsAloneNarrowTheMark() {
        let marks = Fx.compose(rows: [Fx.fieldCoarseRow], supportLines: Fx.fieldIndex)

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 1_590.0)
        #expect(marks.first?.endTime == 1_611.42)
    }

    /// CONTROL, and the reason the suite is not vacuous: with neither source
    /// the mark is still the whole window. Everything above is a change from
    /// THIS.
    @Test("with no refinement and no index the extent is still the scan window")
    func withoutLocalisationTheExtentIsTheWindow() {
        let marks = Fx.compose(rows: [Fx.fieldCoarseRow])

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 1_510.38)
        #expect(marks.first?.endTime == 1_611.42)
    }

    /// A row scanned against a transcript the app has moved past keeps its
    /// window. Our records failed, not the model — and resolving anyway would
    /// put this mark 22 s off (see `SupportLineIndexTests`).
    @Test("a row from a stale transcript version keeps its window, it is not narrowed")
    func aStaleRowKeepsItsWindow() {
        let today = SupportLineIndex(
            transcriptVersion: "cd175ee9c1bc71473bc7789e97c9f9bd",
            lines: Fx.fieldLines
        )
        let marks = Fx.compose(rows: [Fx.fieldCoarseRow], supportLines: today)

        #expect(marks.count == 1, "the mark is NOT dropped")
        #expect(marks.first?.startTime == 1_510.38)
        #expect(marks.first?.endTime == 1_611.42)
    }

    /// A `containsAd` row whose payload is `"[]"` — the model named nothing —
    /// also keeps its window today. That is playhead-my33's decision to
    /// revisit, and pinning it here is what makes the change visible when he
    /// makes it.
    @Test("a verdict that named no lines keeps its window (playhead-my33 owns changing this)")
    func aVerdictThatNamedNothingKeepsItsWindow() {
        let row = Fx.row(id: "unlocalised", start: 700, end: 790, spansJSON: "[]")
        let marks = Fx.compose(rows: [row], supportLines: Fx.fieldIndex)

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 700)
        #expect(marks.first?.endTime == 790)
    }

    /// `.absent` and `.unreadable` are held apart in the type even though they
    /// get the same answer today, because they are different claims and
    /// playhead-my33 moves only one of them.
    @Test("named-nothing and named-unreadably are DIFFERENT localisations")
    func absentAndUnreadableAreDistinct() {
        let named = Fx.fieldCoarseRow
        let namedNothing = Fx.row(id: "nothing", start: 1_510.38, end: 1_611.42,
                                  atoms: 1_796...1_868, spansJSON: "[]")
        let stale = SupportLineIndex(transcriptVersion: "other", lines: Fx.fieldLines)

        #expect(SemanticSweepMarkComposer.localisation(
            of: namedNothing, in: [namedNothing], supportLines: Fx.fieldIndex) == .absent)
        #expect(SemanticSweepMarkComposer.localisation(
            of: named, in: [named], supportLines: stale) == .unreadable)
        #expect(SemanticSweepMarkComposer.localisation(
            of: named, in: [named], supportLines: nil) == .unreadable,
                "no index at all is UNREADABLE, not absent — the model did name lines")
    }

    /// A pass-B row that AFFIRMED is stage 2's business and its span already
    /// governs; it must not also be read here as a zoom target.
    @Test("an affirming pass-B row does not localise through the declined path")
    func anAffirmingRefinementIsNotADeclinedZoom() {
        let affirming = Fx.row(id: "affirm", start: 1_590.0, end: 1_611.42,
                               atoms: 1_846...1_868, disposition: .containsAd,
                               scanPass: "passB", spansJSON: "[]")

        #expect(SemanticSweepMarkComposer.declinedRefinementSpans(
            over: Fx.fieldCoarseRow, in: [affirming]).isEmpty)
    }

    /// A refinement row belonging to a DIFFERENT transcript version is a
    /// different segmentation's plan wearing the same seconds.
    @Test("a declined refinement from another transcript version does not localise")
    func aDeclinedRefinementFromAnotherVersionDoesNotLocalise() {
        let foreign = Fx.row(id: "foreign", start: 1_590.0, end: 1_611.42,
                             atoms: 1_846...1_868, disposition: .noAds,
                             scanPass: "passB", transcriptVersion: "other", spansJSON: "[]")

        #expect(SemanticSweepMarkComposer.declinedRefinementSpans(
            over: Fx.fieldCoarseRow, in: [foreign]).isEmpty)
        let marks = Fx.compose(rows: [Fx.fieldCoarseRow, foreign])
        #expect(marks.first?.startTime == 1_510.38, "so the coarse extent stands")
    }

    /// A refinement that covers the WHOLE coarse window localised nothing.
    @Test("a declined refinement as wide as the window does not localise")
    func aFullWidthDeclinedRefinementDoesNotLocalise() {
        let fullWidth = Fx.row(id: "full", start: 1_510.38, end: 1_611.42,
                               atoms: 1_796...1_868, disposition: .noAds,
                               scanPass: "passB", spansJSON: "[]")

        #expect(SemanticSweepMarkComposer.declinedRefinementSpans(
            over: Fx.fieldCoarseRow, in: [fullWidth]).isEmpty)
    }

    /// A refinement row that did not EXAMINE its window looked at nothing, so
    /// its bounds are not a zoom target. The field sweep really does end
    /// `abstain | cancelled`.
    @Test("a cancelled refinement does not localise")
    func aCancelledRefinementDoesNotLocalise() {
        let cancelled = Fx.row(id: "cancelled", start: 1_590.0, end: 1_611.42,
                               atoms: 1_846...1_868, disposition: .abstain,
                               status: .cancelled, scanPass: "passB", spansJSON: "[]")

        #expect(SemanticSweepMarkComposer.declinedRefinementSpans(
            over: Fx.fieldCoarseRow, in: [cancelled]).isEmpty)
    }

    /// **SHRINK ONLY, THE ADMISSION HALF.** A verdict the dedupe suppressed
    /// stays suppressed even though its localised span clears the blocking
    /// window entirely. Measured on the pull, localising before the dedupe
    /// surfaced three new marks on 9126552E that are, read as transcript, two
    /// hosts discussing a sculpture.
    @Test("localisation never admits a mark the coarse geometry suppressed")
    func localisationNeverAdmitsASuppressedMark() {
        let blocking = Fx.window(id: "existing", start: 1_520.0, end: 1_560.0)

        #expect(Fx.compose(rows: [Fx.fieldCoarseRow, Fx.fieldRefinementRow],
                           existing: [blocking]).isEmpty,
                "the localised span [1590–1611.4] misses the blocker, and is STILL suppressed")
        #expect(Fx.compose(rows: [Fx.fieldCoarseRow, Fx.fieldRefinementRow]).count == 1,
                "control: without the blocker it composes")
    }

    /// **SHRINK ONLY, THE OTHER HALF.** A localisation too short to survive the
    /// duration floor returns the extent UNCHANGED — `clip`'s rule, that
    /// refining geometry must never destroy the mark it is refining.
    ///
    /// THE FIXTURE HAS TO REPRODUCE THE ROW'S WINDOW, and the first version of
    /// this test did not — it shortened line 62 in place, which broke
    /// `runEnd == window.endTime`, so the index REFUSED and the mark kept its
    /// window for the wrong reason. It passed under mutant SU15 (which deletes
    /// this very fallback) because it never reached it. Two lines here: a short
    /// CITED one and a long uncited one, together spanning the row's window
    /// exactly, so resolution succeeds and the floor is what does the work.
    @Test("a localisation under the duration floor leaves the mark unchanged")
    func anUnderLengthLocalisationLeavesTheMarkAlone() {
        let lines: [Int: SupportLineIndex.Line] = [
            62: SupportLineIndex.Line(startTime: 1_600.0, endTime: 1_601.0,
                                      firstAtomOrdinal: 1_861, lastAtomOrdinal: 1_868),
            63: SupportLineIndex.Line(startTime: 1_601.5, endTime: 1_611.42,
                                      firstAtomOrdinal: 1_869, lastAtomOrdinal: 1_900),
        ]
        let index = SupportLineIndex(transcriptVersion: Fx.transcriptVersion, lines: lines)
        let row = Fx.row(id: "short-support", start: 1_600.0, end: 1_611.42,
                         atoms: 1_861...1_900, spansJSON: Fx.support([62]))

        #expect(index.resolve(
            supportLineRefs: [62],
            in: SupportLineIndex.RowWindow(
                transcriptVersion: Fx.transcriptVersion,
                firstAtomOrdinal: 1_861, lastAtomOrdinal: 1_900,
                startTime: 1_600.0, endTime: 1_611.42
            )
        )?.first?.duration == 1.0, "control: the localisation IS 1.0 s, under the 2.0 s floor")

        let marks = Fx.compose(rows: [row], supportLines: index)
        #expect(marks.count == 1, "refining geometry must never destroy the mark it refines")
        #expect(marks.first?.startTime == 1_600.0)
        #expect(marks.first?.endTime == 1_611.42)
    }

    /// Two non-adjacent supported regions inside one extent become two marks,
    /// each on its own ad, rather than one banner over the show between them.
    @Test("two separated supported regions become two marks, not one straddling them")
    func separatedRegionsBecomeSeparateMarks() {
        let row = Fx.row(id: "two", start: 1_510.38, end: 1_611.42,
                         atoms: 1_796...1_868, spansJSON: Fx.support([60, 62]))
        let marks = Fx.compose(rows: [row], supportLines: Fx.fieldIndex).sorted { $0.startTime < $1.startTime }

        #expect(marks.count == 2)
        #expect(marks.first?.endTime == 1_559.58)
        #expect(marks.last?.startTime == 1_590.0)
    }

    /// The grade is the EVIDENCE, not the geometry. `clip` already carries
    /// confidence through untouched and this stage must too — measured on the
    /// pull, zero of the 78 marks changes confidence.
    @Test("narrowing a mark does not change its confidence")
    func narrowingDoesNotChangeConfidence() {
        let wide = Fx.compose(rows: [Fx.fieldCoarseRow])
        let narrow = Fx.compose(rows: [Fx.fieldCoarseRow], supportLines: Fx.fieldIndex)

        // ASSERT THE VALUE, NOT ONLY THE AGREEMENT. `wide == narrow` compares
        // two outputs of the same function, so a constant substituted into
        // BOTH satisfies it — mutant SU16 replaced the carried grade with
        // `unevidencedMarkConfidence` and this test passed. The field row is
        // `strong` on a `good` transcript with one uncontradicted screening,
        // so its grade is the ceiling exactly.
        #expect(wide.first?.confidence == SemanticSweepMarkComposer.maximumMarkConfidence)
        #expect(narrow.first?.confidence == SemanticSweepMarkComposer.maximumMarkConfidence)
        #expect(narrow.first?.confidence != SemanticSweepMarkComposer.unevidencedMarkConfidence)
        #expect(wide.first?.confidence == narrow.first?.confidence)
        // The narrowing moved the START here — line 62 ends where the window
        // does — so the control has to compare the edge that actually moved.
        #expect(narrow.first?.startTime != wide.first?.startTime, "control: it really did narrow")
    }

    /// A narrowed mark is still MARK-ONLY and still unanchored. Narrowing an
    /// extent is not evidence that anybody proved its edges.
    @Test("a narrowed mark is still markOnly and still unanchored on both edges")
    func aNarrowedMarkIsStillMarkOnly() {
        let mark = Fx.compose(rows: [Fx.fieldCoarseRow], supportLines: Fx.fieldIndex).first

        #expect(mark?.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
        #expect(mark?.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(mark?.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(mark?.decisionState == AdDecisionState.candidate.rawValue)
    }

    /// The pad is a PRODUCT THRESHOLD sitting at its conservative bound, and
    /// zero is a claim: a localised mark's edges are the segment's edges, and
    /// every one of them is an inner edge that would eat show if widened.
    @Test("the localisation pad is zero, and a mark's edges are the line's edges exactly")
    func thePadIsZero() {
        #expect(SemanticSweepMarkComposer.supportLocalisationPadSeconds == 0.0)

        let mark = Fx.compose(rows: [Fx.fieldCoarseRow], supportLines: Fx.fieldIndex).first
        #expect(mark?.startTime == Fx.fieldLines[62]?.startTime)
        #expect(mark?.endTime == Fx.fieldLines[62]?.endTime)
    }

    /// However a pad is set, it may never carry a mark outside the audio the
    /// model examined.
    ///
    /// ASSERTED AT A PAD THAT MAKES IT LOAD-BEARING, not at the shipped one.
    /// At `supportLocalisationPadSeconds == 0.0` the clamp cannot bite through
    /// any production path — a resolved span is built from lines inside a run
    /// that reproduces the row's window, and a declined pass-B window must lie
    /// inside it — so mutant SU17 (delete the clamp) is EQUIVALENT at today's
    /// constant, and the first version of this test passed for that reason
    /// rather than for its own. `padded` takes the pad so the contract can be
    /// stated at the value where it does work; the constant is checked
    /// separately above.
    @Test("a pad is clamped to the window the model examined")
    func aPadIsClampedToTheExaminedWindow() {
        let window = AdSpanBounds(start: 1_590.0, end: 1_611.42)
        let span = AdSpanBounds(start: 1_592.0, end: 1_610.0)

        let unpadded = SemanticSweepMarkComposer.padded([span], within: window, pad: 0)
        #expect(unpadded == [span], "control: a zero pad changes nothing")

        let padded = SemanticSweepMarkComposer.padded([span], within: window, pad: 5)
        #expect(padded.count == 1)
        #expect(padded.first?.start == 1_590.0, "1592 - 5 = 1587 would leave the window")
        #expect(padded.first?.end == 1_611.42, "1610 + 5 = 1615 would leave the window")

        let wide = SemanticSweepMarkComposer.padded([span], within: window, pad: 2)
        #expect(wide.first?.start == 1_590.0, "1592 - 2 = 1590 is exactly the edge")
        #expect(wide.first?.end == 1_611.42, "1610 + 2 = 1612 is past it")
    }
}

// MARK: - 3. The merge barrier

@Suite("a merge may never bridge a window a presence pass CLEARED (playhead-shu5)",
       .timeLimit(.minutes(1)))
struct SemanticSweepMergeBarrierTests {

    private typealias Fx = LocalisationFixture

    /// THE FIELD SHAPE, from 561CEF5B on the 2026-08-19 pull: two coarse
    /// `containsAd` windows 0.42 s apart, and a third `passA` row that examined
    /// [497.3–607.1] and returned `noAds` — a window spanning the join. Under
    /// `mergeGapSeconds` alone the two fuse into one 198.7 s banner over audio
    /// somebody looked at and cleared.
    @Test("two verdicts either side of a cleared window stay two marks")
    func aClearedWindowBarsTheMerge() {
        let rows = [
            Fx.row(id: "a", start: 420.9, end: 529.8, spansJSON: "[]"),
            Fx.row(id: "b", start: 530.22, end: 619.62, spansJSON: "[]"),
            Fx.row(id: "cleared", start: 497.34, end: 607.08, disposition: .noAds),
        ]
        let marks = Fx.compose(rows: rows).sorted { $0.startTime < $1.startTime }

        #expect(marks.count == 2)
        #expect(marks.first?.endTime == 529.8)
        #expect(marks.last?.startTime == 530.22)
    }

    /// CONTROL. The same two verdicts with no cleared window between them are
    /// one mark — so the suite above is testing the barrier and not the gap.
    @Test("without a cleared window the same two verdicts merge")
    func withoutABarrierTheyMerge() {
        let rows = [
            Fx.row(id: "a", start: 420.9, end: 529.8, spansJSON: "[]"),
            Fx.row(id: "b", start: 530.22, end: 619.62, spansJSON: "[]"),
        ]
        let marks = Fx.compose(rows: rows)

        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 420.9)
        #expect(marks.first?.endTime == 619.62)
    }

    /// A row that did not EXAMINE its window cleared nothing, so it may not bar
    /// anything. Otherwise a cancelled scan would silently split real breaks.
    @Test("an unexamined row is not a barrier")
    func anUnexaminedRowIsNotABarrier() {
        let rows = [
            Fx.row(id: "a", start: 420.9, end: 529.8, spansJSON: "[]"),
            Fx.row(id: "b", start: 530.22, end: 619.62, spansJSON: "[]"),
            Fx.row(id: "cancelled", start: 497.34, end: 607.08,
                   disposition: .abstain, status: .cancelled),
        ]

        #expect(Fx.compose(rows: rows).count == 1)
    }

    /// A pass-B `noAds` means "found no edges", never "there is no ad" — the
    /// same rule `corroboration(for:in:)` already follows. It must not bar.
    @Test("a declined refinement is not a barrier")
    func aDeclinedRefinementIsNotABarrier() {
        let rows = [
            Fx.row(id: "a", start: 420.9, end: 529.8, spansJSON: "[]"),
            Fx.row(id: "b", start: 530.22, end: 619.62, spansJSON: "[]"),
            Fx.row(id: "refine", start: 497.34, end: 607.08,
                   disposition: .noAds, scanPass: "passB"),
        ]

        #expect(Fx.compose(rows: rows).count == 1)
    }

    /// THE PREDICATE IS THE DISPOSITION, NOT `spansJSON == "[]"`. A row can be
    /// `containsAd` AND carry `"[]"` — `encodeSupport` writes that string for a
    /// nil support object, and 19 of the 301 coarse verdicts on the pull are
    /// exactly that. Reading it as a denial would invert their meaning.
    @Test("a containsAd row whose support payload is empty is NOT a cleared window")
    func anEmptyPayloadIsNotAClearedWindow() {
        let unlocalised = Fx.row(id: "unlocalised", start: 497.34, end: 607.08,
                                 disposition: .containsAd, spansJSON: "[]")

        #expect(SemanticSweepMarkComposer.clearedSpans(in: [unlocalised]).isEmpty)
        #expect(SemanticSweepMarkComposer.clearedSpans(
            in: [Fx.row(id: "denied", start: 497.34, end: 607.08, disposition: .noAds)]
        ).count == 1, "control: a real denial IS one")
    }

    /// A barrier that does not span the gap does not bar it. Otherwise any
    /// `noAds` row anywhere near a mark would split it.
    @Test("a cleared window that does not span the gap does not bar the merge")
    func aBarrierOffTheGapDoesNotBar() {
        let rows = [
            Fx.row(id: "a", start: 420.9, end: 529.8, spansJSON: "[]"),
            Fx.row(id: "b", start: 530.22, end: 619.62, spansJSON: "[]"),
            Fx.row(id: "elsewhere", start: 200.0, end: 300.0, disposition: .noAds),
        ]

        #expect(Fx.compose(rows: rows).count == 1)
    }

    /// The gap test is OPEN, not closed: a cleared window that merely touches
    /// the join swallows no audio, and barring on it would cost a merge for
    /// nothing.
    @Test("a cleared window that only touches the gap edge does not bar")
    func aTouchingBarrierDoesNotBar() {
        let rows = [
            Fx.row(id: "a", start: 420.9, end: 529.8, spansJSON: "[]"),
            Fx.row(id: "b", start: 530.22, end: 619.62, spansJSON: "[]"),
            Fx.row(id: "touching", start: 530.22, end: 607.08, disposition: .noAds),
        ]

        #expect(Fx.compose(rows: rows).count == 1)
    }
}

// MARK: - 4. The wires

/// A correct composer that is never handed an index is the defect a pure
/// composer battery structurally cannot see: every test above passes its own
/// `SupportLineIndex`, so all of them stay green while production passes `nil`
/// and every mark on the device keeps its whole scan window. There are exactly
/// two `compose` call sites and both must supply one.
///
/// This is a SOURCE canary for the same reason `PlayheadRuntimeWiringSource-
/// CanaryTests` is: driving either site for real needs a store, a transcript
/// and an FM cohort gate, and none of that is reachable from a unit test.
@Suite("both compose call sites are handed a SupportLineIndex (playhead-shu5)",
       .timeLimit(.minutes(1)))
struct SemanticSweepSupportLineWiringSourceCanaryTests {

    /// `PlayheadTests/Services/AdDetection/<this file>` → repo root.
    private static func source(_ relative: String, filePath: String = #filePath) throws -> String {
        let root = URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()   // AdDetection
            .deletingLastPathComponent()   // Services
            .deletingLastPathComponent()   // PlayheadTests
            .deletingLastPathComponent()   // repo root
        return try String(contentsOf: root.appendingPathComponent(relative), encoding: .utf8)
    }

    /// The composer's ONE optional parameter is the one that decides whether
    /// this bead does anything at all, so a call site that omits it compiles,
    /// passes every test above, and ships the old geometry.
    @Test("every SemanticSweepMarkComposer.compose call site passes supportLines")
    func everyCallSitePassesSupportLines() throws {
        let sites = [
            "Playhead/Services/AdDetection/AdDetectionService.swift",
            "Playhead/Services/AdDetection/BackfillJobRunner.swift",
        ]
        for path in sites {
            let text = try Self.source(path)
            let calls = text.components(separatedBy: "SemanticSweepMarkComposer.compose(").dropFirst()
            #expect(!calls.isEmpty, "\(path) no longer calls the composer at all")
            for call in calls {
                let arguments = call.prefix(while: { $0 != ")" })
                #expect(arguments.contains("supportLines:"),
                        "a compose call in \(path) omits supportLines: \(arguments)")
            }
        }
    }

    /// And the index it passes must be built from SEGMENTS, at a
    /// `transcriptVersion`. An index built with the wrong version string
    /// refuses everything silently — the failure would look exactly like
    /// success.
    @Test("the service builds its index from the backfill's own segments and version")
    func theServiceBuildsFromItsOwnSegments() throws {
        let text = try Self.source("Playhead/Services/AdDetection/AdDetectionService.swift")

        #expect(text.contains("SupportLineIndex("))
        #expect(text.contains("segments: TranscriptSegmenter.segment(atoms: atoms)"))
        #expect(text.contains("transcriptVersion: transcriptVersion.transcriptVersion"))
    }

    /// The runner's half. It has the segmentation already — `inputs.segments`
    /// is what `planPassA` windowed against — so re-segmenting there would be a
    /// second source of truth for the coordinate system this bead exists to
    /// pin down.
    @Test("the runner builds its index from inputs.segments, not a fresh segmentation")
    func theRunnerBuildsFromInputsSegments() throws {
        let text = try Self.source("Playhead/Services/AdDetection/BackfillJobRunner.swift")

        #expect(text.contains("SupportLineIndex("))
        #expect(text.contains("segments: inputs.segments"))
        #expect(!text.contains("SupportLineIndex(\n                        segments: TranscriptSegmenter"),
                "the runner must not re-segment; inputs.segments IS the plan's segmentation")
    }
}
