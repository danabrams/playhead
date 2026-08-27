// SupportLineSecondsTests.swift
// playhead-qjcf (schema V66) — a support line stops being a coordinate in a
// system that moves.
//
// THE DEFECT, and it is Dan's own veto one turn further along than
// playhead-shu5 and playhead-my33 could reach.
//
// A coarse row's `supportLineRefs` are `AdTranscriptSegment.segmentIndex`
// values — POSITIONS IN A COORDINATE SYSTEM that is derived from the transcript
// and is not itself persisted. Re-transcribe the episode and that system is
// replaced. `SupportLineIndex.resolve` then correctly REFUSES (resolving anyway
// is measured to land 22 s into the show on this very episode — see that type's
// header), so `SemanticSweepMarkComposer` keeps the row's whole ~95 s scan tile.
// The VERDICT survives; its LOCALISATION does not.
//
// Measured on the 2026-08-19 t4 pull, over the 301 rows with
// `scanPass = 'passA' AND disposition = 'containsAd'`:
//
//     19 carry `spansJSON = "[]"`             -> Localisation.absent
//    282 carry a non-empty supportLineRefs    -> the population V66 writes for
//         of which 108 resolve today
//         and  174 do NOT                     -> Localisation.unreadable
//
// (19 + 282 = 301 exactly: zero rows carry `"supportLineRefs":[]`. Three
// different quantities live in this neighbourhood and they are not
// interchangeable — 211 rows at a superseded transcriptVersion, 280 whose
// version no surviving chunk carries, 174 this stage cannot resolve. Only the
// last is what V66 is about.)
//
// WHAT THIS BEAD DOES NOT DO, pinned as hard as what it does. There is no
// backfill and there cannot be one: the seconds were never written, and the
// segmentation that would produce them is gone. All 174 stay `.unreadable` for
// ever, INCLUDING the coarse row behind the mark Dan vetoed by hand — CD2976E6
// [1131.60-1210.86], `{"supportLineRefs":[46],"certainty":"strong"}` at
// `transcriptVersion 807613cf`, against a current version of `cd175ee9`. Moving
// that one needs a re-scan or a decision to treat `.unreadable` like `.absent`,
// and both are Dan's. `dansVetoedRowIsNOTFixedByThisBead` says so in a test,
// because a bead whose headline claim is "this does not fix the thing you asked
// about" must state it somewhere a reader cannot skim past.
//
// The directions this file covers, because closing one leaves the rest open:
//
//   1. THE PROJECTION. Refs -> seconds, against the segmentation that holds
//      them, and REFUSING rather than partially projecting when it does not.
//   2. THE CODEC, and that an absence has exactly one spelling.
//   3. ONE GROUPING. `resolve` and `persistedSupportSpans` must agree on the
//      row both of them can read — the only place the writer's projection can
//      be checked against an independent reconstruction.
//   4. THE READER. A stale row WITH seconds is `.named`; the same row WITHOUT
//      them is still `.unreadable`; and every way the payload can be wrong is a
//      REFUSAL rather than a repair.
//   5. ADDITIVE ONLY. Nothing that localises today may move.
//   6. THE CARRIER. The projection survives `attributed` and the store.

import Foundation
import SQLite3
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum SecondsFixture {

    static let assetId = "asset-cd2976e6"
    /// The version CD2976E6's marks were SCANNED at.
    static let scannedVersion = "807613cf4b0f2898cc1437afe79b480f"
    /// The version its chunk set hashes to TODAY (playhead-kg6i's witness).
    static let currentVersion = "cd175ee9c1bc71473bc7789e97c9f9bd"

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

    /// The lines of CD2976E6's [1510.4-1611.4] window as the segmentation at
    /// `807613cf` had them. Line 62 is the one the model cited.
    static var lines: [Int: SupportLineIndex.Line] {
        Dictionary(uniqueKeysWithValues: [
            line(59, 1_483.5, 1_510.02, atoms: 1_764...1_795),
            line(60, 1_510.38, 1_559.58, atoms: 1_796...1_845),
            line(61, 1_560.3, 1_570.62, atoms: 1_846...1_860),
            line(62, 1_590.0, 1_611.42, atoms: 1_861...1_868),
            line(63, 1_615.68, 1_654.2, atoms: 1_869...1_900),
        ])
    }

    static var index: SupportLineIndex {
        SupportLineIndex(transcriptVersion: scannedVersion, lines: lines)
    }

    /// The same index stamped with a DIFFERENT version — what the composer is
    /// handed after the episode has been re-transcribed. `resolve` refuses it,
    /// which is the state 174 of the 301 rows on the pull are in.
    static var indexAtAnotherVersion: SupportLineIndex {
        SupportLineIndex(transcriptVersion: currentVersion, lines: lines)
    }

    static func support(_ refs: [Int]) -> String {
        let joined = refs.map(String.init).joined(separator: ",")
        return #"{"certainty":"strong","supportLineRefs":[\#(joined)]}"#
    }

    static func row(
        id: String = "scan-coarse",
        start: Double = 1_510.38,
        end: Double = 1_611.42,
        atoms: ClosedRange<Int> = 1_796...1_868,
        disposition: CoarseDisposition = .containsAd,
        scanPass: String = "passA",
        version: String = scannedVersion,
        spansJSON: String? = nil,
        seconds: String? = nil
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: atoms.lowerBound,
            windowLastAtomOrdinal: atoms.upperBound,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: .good,
            disposition: disposition,
            spansJSON: spansJSON ?? (disposition == .containsAd ? support([62]) : "[]"),
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            scanCohortJSON: makeCohortJSON(promptLabel: "qjcf"),
            transcriptVersion: version,
            verdictProvenance: .model,
            supportLineSpansJSON: seconds
        )
    }

    /// What THIS row's writer would have persisted for it, had V66 existed when
    /// it ran. Built through the production encoder rather than hand-spelled, so
    /// a change to the payload FORMAT cannot leave these rails asserting against
    /// a shape nothing writes.
    static func projectedSeconds(_ refs: [Int] = [62]) -> String {
        SupportLineIndex.encodeSupportLineSpans(
            index.project(supportLineRefs: refs) ?? []
        ) ?? ""
    }

    /// One `AdTranscriptSegment`, as `TranscriptSegmenter` would emit it — what
    /// the WRITER holds, as opposed to the `SupportLineIndex` a reader builds.
    static func segment(_ index: Int, _ start: Double, _ end: Double, atoms: ClosedRange<Int>) -> AdTranscriptSegment {
        let all = atoms.map { ordinal in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: assetId,
                    transcriptVersion: scannedVersion,
                    atomOrdinal: ordinal
                ),
                contentHash: "h-\(ordinal)",
                // Spread the atoms across the segment so `startTime`/`endTime`
                // are a real min/max rather than one atom's own bounds.
                startTime: start + (end - start) * Double(ordinal - atoms.lowerBound)
                    / Double(max(1, atoms.count)),
                endTime: ordinal == atoms.upperBound
                    ? end
                    : start + (end - start) * Double(ordinal - atoms.lowerBound + 1)
                        / Double(max(1, atoms.count)),
                text: "w\(ordinal)",
                chunkIndex: ordinal
            )
        }
        return AdTranscriptSegment(atoms: all, segmentIndex: index)
    }

    static var segments: [AdTranscriptSegment] {
        [
            segment(60, 1_510.38, 1_559.58, atoms: 1_796...1_845),
            segment(61, 1_560.3, 1_570.62, atoms: 1_846...1_860),
            segment(62, 1_590.0, 1_611.42, atoms: 1_861...1_868),
        ]
    }
}

// MARK: - 1. The projection (the WRITER's half)

@Suite("a coarse row's support lines are projected into SECONDS at write time (playhead-qjcf)",
       .timeLimit(.minutes(1)))
struct SupportLineProjectionTests {

    private typealias Fx = SecondsFixture

    @Test("each named line is projected into the seconds it covered")
    func projectsEachNamedLine() throws {
        let projected = try #require(Fx.index.project(supportLineRefs: [62]))
        #expect(projected == [SupportLineSpan(lineRef: 62, start: 1_590.0, end: 1_611.42)])
    }

    @Test("a ref this segmentation does not hold refuses the WHOLE projection")
    func refusesAnUnknownRef() {
        #expect(Fx.index.project(supportLineRefs: [999]) == nil)
    }

    @Test("one unknown ref among known ones refuses too — never a PARTIAL projection")
    func refusesPartial() {
        // The direction that matters. Projecting [62] and dropping 999 would
        // record a claim about FEWER lines than the model made, and every reader
        // downstream would then narrow a mark on it with full confidence. Under-
        // claiming means writing nothing and letting the row keep its window.
        #expect(Fx.index.project(supportLineRefs: [62, 999]) == nil)
        #expect(Fx.index.project(supportLineRefs: [999, 62]) == nil)
    }

    @Test("the projection is deduplicated and sorted by lineRef")
    func deduplicatesAndSorts() throws {
        let projected = try #require(Fx.index.project(supportLineRefs: [63, 62, 62, 63]))
        #expect(projected.map(\.lineRef) == [62, 63])
    }

    @Test("an empty ref list projects nothing")
    func emptyRefsProjectNothing() {
        #expect(Fx.index.project(supportLineRefs: []) == nil)
    }

    @Test("the writer's overload projects from the SEGMENTS it just ran")
    func projectsFromRawSegments() throws {
        // `SupportLineIndex.project(supportLineRefs:segments:)` is what
        // `BackfillJobRunner` reaches: it holds `inputs.segments`, not an index.
        // Both spellings must agree, or the recorded seconds and the resolved
        // ones would be two different quantities from the start.
        let viaSegments = try #require(
            SupportLineIndex.project(supportLineRefs: [62], segments: Fx.segments)
        )
        let viaIndex = try #require(Fx.index.project(supportLineRefs: [62]))
        #expect(viaSegments == viaIndex)
    }

    @Test("the runner writes the projection for a coarse screening that names lines")
    func runnerWritesTheProjection() throws {
        let encoded = try #require(
            BackfillJobRunner.encodeSupportLineSecondsForTesting(
                CoarseSupportSchema(supportLineRefs: [62], certainty: .strong),
                segments: Fx.segments
            )
        )
        let decoded = try #require(SupportLineIndex.decodeSupportLineSpans(encoded))
        #expect(decoded == [SupportLineSpan(lineRef: 62, start: 1_590.0, end: 1_611.42)])
    }

    @Test("the runner writes NOTHING when there is nothing honest to write")
    func runnerRefuses() {
        // No support object — `{"supportLineRefs":[]}`'s three cousins. Each
        // leaves the row exactly as a pre-V66 row, i.e. on the resolve path,
        // which is the behaviour that was already shipping.
        #expect(BackfillJobRunner.encodeSupportLineSecondsForTesting(
            nil, segments: Fx.segments) == nil)
        #expect(BackfillJobRunner.encodeSupportLineSecondsForTesting(
            CoarseSupportSchema(supportLineRefs: [], certainty: .strong),
            segments: Fx.segments) == nil)
        #expect(BackfillJobRunner.encodeSupportLineSecondsForTesting(
            CoarseSupportSchema(supportLineRefs: [999], certainty: .strong),
            segments: Fx.segments) == nil)
        #expect(BackfillJobRunner.encodeSupportLineSecondsForTesting(
            CoarseSupportSchema(supportLineRefs: [62], certainty: .strong),
            segments: []) == nil)
    }
}

// MARK: - 2. The codec, and the ONE spelling of an absence

@Suite("supportLineSpansJSON encodes stably and reads every absence as one thing (playhead-qjcf)",
       .timeLimit(.minutes(1)))
struct SupportLineSpansCodecTests {

    private typealias Fx = SecondsFixture

    @Test("a projection round-trips through the column format")
    func roundTrips() throws {
        let projected = try #require(Fx.index.project(supportLineRefs: [61, 62]))
        let encoded = try #require(SupportLineIndex.encodeSupportLineSpans(projected))
        #expect(SupportLineIndex.decodeSupportLineSpans(encoded) == projected)
    }

    @Test("the encoding is stable BYTES, so a rewrite of the same verdict reads as unchanged")
    func encodingIsStable() throws {
        let a = try #require(Fx.index.project(supportLineRefs: [62, 61]))
        let b = try #require(Fx.index.project(supportLineRefs: [61, 62]))
        #expect(SupportLineIndex.encodeSupportLineSpans(a)
                == SupportLineIndex.encodeSupportLineSpans(b))
        // `.sortedKeys`, not incidental ordering: a diff over two device pulls
        // must not report churn that did not happen.
        let encoded = try #require(SupportLineIndex.encodeSupportLineSpans(a))
        #expect(encoded.range(of: "\"end\"")!.lowerBound
                < encoded.range(of: "\"lineRef\"")!.lowerBound)
        #expect(encoded.range(of: "\"lineRef\"")!.lowerBound
                < encoded.range(of: "\"start\"")!.lowerBound)
    }

    @Test("an empty projection encodes to NOTHING, never to \"[]\"")
    func emptyEncodesToNil() {
        // `"[]"` would be a FOURTH spelling of an absence in a neighbourhood
        // that already has three, and `supportLineRefs(of:)` spends a paragraph
        // disentangling those. A NULL column is the one spelling.
        #expect(SupportLineIndex.encodeSupportLineSpans([]) == nil)
    }

    @Test("every unreadable payload decodes to nothing — NULL, garbage, empty array")
    func decodeIsTotal() {
        #expect(SupportLineIndex.decodeSupportLineSpans(nil) == nil)
        #expect(SupportLineIndex.decodeSupportLineSpans("") == nil)
        #expect(SupportLineIndex.decodeSupportLineSpans("[]") == nil)
        #expect(SupportLineIndex.decodeSupportLineSpans("not json") == nil)
        #expect(SupportLineIndex.decodeSupportLineSpans(#"{"lineRef":1}"#) == nil)
        // A shape that is ALMOST right: seconds with no ref. It must not decode,
        // because the ref is what lets a reader check the payload against the
        // verdict — see `persistedSupportSpans`.
        #expect(SupportLineIndex.decodeSupportLineSpans(#"[{"start":1.0,"end":2.0}]"#) == nil)
    }
}

// MARK: - 3. ONE grouping, and the agreement between the two coordinate systems

@Suite("the recorded seconds and a live resolve group identically (playhead-qjcf)",
       .timeLimit(.minutes(1)))
struct SupportLineGroupingTests {

    private typealias Fx = SecondsFixture

    @Test("consecutive refs are ONE span")
    func consecutiveRefsAreOneSpan() throws {
        let projected = try #require(Fx.index.project(supportLineRefs: [61, 62]))
        let bounds = try #require(SupportLineIndex.contiguousBounds(of: projected))
        #expect(bounds == [AdSpanBounds(start: 1_560.3, end: 1_611.42)])
    }

    @Test("a gap between refs is TWO spans")
    func gappedRefsAreTwoSpans() throws {
        let projected = try #require(Fx.index.project(supportLineRefs: [60, 62]))
        let bounds = try #require(SupportLineIndex.contiguousBounds(of: projected))
        #expect(bounds == [
            AdSpanBounds(start: 1_510.38, end: 1_559.58),
            AdSpanBounds(start: 1_590.0, end: 1_611.42),
        ])
    }

    @Test("adjacency is decided on the REF, never on the seconds")
    func adjacencyIsOnTheRef() throws {
        // Two lines that ABUT in time (…59.58 / 59.58…) and are NOT consecutive
        // lines. Grouping on time would merge them and widen a mark across the
        // line between, which the model did not name.
        let abutting = [
            SupportLineSpan(lineRef: 60, start: 1_510.38, end: 1_559.58),
            SupportLineSpan(lineRef: 62, start: 1_559.58, end: 1_611.42),
        ]
        let bounds = try #require(SupportLineIndex.contiguousBounds(of: abutting))
        #expect(bounds.count == 2)
    }

    @Test("the grouping is order-independent")
    func groupingIsOrderIndependent() throws {
        let forward = try #require(SupportLineIndex.contiguousBounds(of: [
            SupportLineSpan(lineRef: 61, start: 1_560.3, end: 1_570.62),
            SupportLineSpan(lineRef: 62, start: 1_590.0, end: 1_611.42),
        ]))
        let reversed = try #require(SupportLineIndex.contiguousBounds(of: [
            SupportLineSpan(lineRef: 62, start: 1_590.0, end: 1_611.42),
            SupportLineSpan(lineRef: 61, start: 1_560.3, end: 1_570.62),
        ]))
        #expect(forward == reversed)
    }

    @Test("resolve and the recorded projection agree on a row BOTH can read")
    func resolveAndTheProjectionAgree() throws {
        // THE CROSS-CHECK. `resolve` reconstructs from today's segmentation and
        // `persistedSupportSpans` reads what the writer recorded. They overlap on
        // exactly one population — a row whose version still matches — and that
        // overlap is the only place the writer's projection can be checked
        // against an independent derivation. Ordering them in `localisation` is
        // a blast-radius choice precisely BECAUSE this holds.
        for refs in [[62], [61, 62], [60, 62], [60, 61, 62]] {
            let row = Fx.row(
                spansJSON: Fx.support(refs),
                seconds: Fx.projectedSeconds(refs)
            )
            let resolved = try #require(Fx.index.resolve(
                supportLineRefs: refs,
                in: SupportLineIndex.RowWindow(
                    transcriptVersion: row.transcriptVersion,
                    firstAtomOrdinal: row.windowFirstAtomOrdinal,
                    lastAtomOrdinal: row.windowLastAtomOrdinal,
                    startTime: row.windowStartTime,
                    endTime: row.windowEndTime
                )
            ))
            let recorded = try #require(SemanticSweepMarkComposer.persistedSupportSpans(of: row))
            #expect(resolved == recorded, "refs \(refs)")
        }
    }
}

// MARK: - 4. The reader: what the projection rescues, and what it refuses

@Suite("a stale row that recorded its own seconds is localised; one that did not is not (playhead-qjcf)",
       .timeLimit(.minutes(1)))
struct PersistedSupportSecondsReaderTests {

    private typealias Fx = SecondsFixture

    private func localisation(
        _ row: SemanticScanResult,
        index: SupportLineIndex? = SecondsFixture.indexAtAnotherVersion
    ) -> SemanticSweepMarkComposer.Localisation {
        SemanticSweepMarkComposer.localisation(of: row, in: [row], supportLines: index)
    }

    @Test("THE HEADLINE: a row at a superseded version keeps its localisation")
    func staleRowWithSecondsIsNamed() {
        // Same row, same stale version, same refusing index. The only difference
        // is that this row's writer recorded what its refs meant.
        let row = Fx.row(seconds: Fx.projectedSeconds())
        #expect(localisation(row) == .named([AdSpanBounds(start: 1_590.0, end: 1_611.42)]))
    }

    @Test("THE OTHER HALF: the same row without recorded seconds is still UNREADABLE")
    func staleRowWithoutSecondsIsUnreadable() {
        // There is no backfill and there cannot be one, so this is the state
        // every row on every device pull taken before V66 is in — permanently.
        #expect(localisation(Fx.row()) == .unreadable)
    }

    @Test("Dan's vetoed CD2976E6 row is NOT fixed by this bead")
    func dansVetoedRowIsNOTFixedByThisBead() {
        // [1131.60-1210.86], `{"supportLineRefs":[46],"certainty":"strong"}`,
        // scanned at 807613cf against a current version of cd175ee9. Alex
        // Honnold on how he earns a living (playhead-6ruv). It is one of the 174,
        // its `supportLineSpansJSON` is NULL and nothing can fill it, so it keeps
        // its whole 79.3 s window. Moving it needs a RE-SCAN or a decision to
        // treat `.unreadable` like `.absent` — both Dan's, and neither is here.
        let vetoed = Fx.row(
            id: "scan-dan-head",
            start: 1_131.60,
            end: 1_210.86,
            atoms: 1_400...1_500,
            spansJSON: Fx.support([46])
        )
        #expect(vetoed.supportLineSpansJSON == nil)
        #expect(localisation(vetoed) == .unreadable)
        #expect(SemanticSweepMarkComposer.contribution(of: vetoed, in: [vetoed], supportLines: nil)
                == [AdSpanBounds(start: 1_131.60, end: 1_210.86)],
                "it still contributes the WHOLE window — 79.3 s of show")
    }

    @Test("a payload whose refs disagree with the VERDICT is refused")
    func refsMustMatchTheVerdict() {
        // The check the `lineRef` half of SupportLineSpan exists for, and the
        // whole answer to "would seconds alone have done?". The two columns are
        // written on one statement from one screening, so a disagreement means
        // one has been rewritten without the other — a projection of some OTHER
        // verdict. Believing it narrows a mark onto seconds this row never
        // claimed.
        let row = Fx.row(spansJSON: Fx.support([62]), seconds: Fx.projectedSeconds([61]))
        #expect(SemanticSweepMarkComposer.persistedSupportSpans(of: row) == nil)
        #expect(localisation(row) == .unreadable)
    }

    @Test("a payload that is a SUPERSET of the verdict's refs is refused too")
    func supersetRefsAreRefused() {
        // Set equality, not containment — either direction is a drifted record.
        let row = Fx.row(spansJSON: Fx.support([62]), seconds: Fx.projectedSeconds([61, 62]))
        #expect(SemanticSweepMarkComposer.persistedSupportSpans(of: row) == nil)
    }

    @Test("a span outside the row's own window is refused")
    func spanOutsideTheWindowIsRefused() {
        // Every persisted `supportLineRefs` value is a subset of the window's own
        // lineRefs — `FoundationModelClassifier.sanitize` filters against
        // `Set(plan.lineRefs)` and `PermissiveAdClassifier.parse` intersects the
        // same set — so the lines named are inside the window BY CONSTRUCTION. A
        // span outside it is a corrupt record, not a wide one.
        let payload = SupportLineIndex.encodeSupportLineSpans(
            [SupportLineSpan(lineRef: 62, start: 900.0, end: 950.0)]
        )
        let row = Fx.row(spansJSON: Fx.support([62]), seconds: payload)
        #expect(SemanticSweepMarkComposer.persistedSupportSpans(of: row) == nil)
        #expect(localisation(row) == .unreadable)
    }

    @Test("a span that ends before it starts, or is not finite, is refused")
    func degenerateSpansAreRefused() {
        for span in [
            SupportLineSpan(lineRef: 62, start: 1_611.42, end: 1_590.0),
            SupportLineSpan(lineRef: 62, start: 1_590.0, end: 1_590.0),
            SupportLineSpan(lineRef: 62, start: .nan, end: 1_611.42),
            SupportLineSpan(lineRef: 62, start: 1_590.0, end: .infinity),
        ] {
            let row = Fx.row(
                spansJSON: Fx.support([62]),
                seconds: SupportLineIndex.encodeSupportLineSpans([span])
            )
            #expect(SemanticSweepMarkComposer.persistedSupportSpans(of: row) == nil,
                    "\(span)")
        }
    }

    @Test("a row that named NOTHING cannot acquire seconds from a payload")
    func absentRowStaysAbsent() {
        // `.absent` is a property of the VERDICT — the model asserted presence
        // and pointed at nothing — and no amount of recorded geometry changes
        // what the model said. This is also what keeps playhead-my33's
        // sole-backing rule reachable: a payload must never smuggle an
        // unlocalised row onto the `.named` path.
        let row = Fx.row(spansJSON: "[]", seconds: Fx.projectedSeconds())
        #expect(SemanticSweepMarkComposer.persistedSupportSpans(of: row) == nil)
        #expect(localisation(row) == .absent)
    }

    @Test("a REFINEMENT row is untouched by a payload")
    func refinementRowsAreUntouched() {
        // A passB row's own window IS the model's narrowing.
        // `supportLineRefs(of:)` excludes refinement rows, so this can never
        // reach the projection even if some future writer put one there.
        let row = Fx.row(scanPass: "passB", spansJSON: Fx.support([62]),
                         seconds: Fx.projectedSeconds())
        #expect(SemanticSweepMarkComposer.persistedSupportSpans(of: row) == nil)
        #expect(localisation(row) == .named([AdSpanBounds(start: 1_510.38, end: 1_611.42)]))
    }

    @Test("with NO index at all, the recorded seconds still localise the row")
    func noIndexStillLocalises() {
        // The composer is handed `nil` when the caller could not segment. Before
        // V66 that was unconditionally `.unreadable` for every ref-carrying row;
        // now a row that recorded its own seconds needs no index at all, which is
        // the whole point of moving off a derived coordinate system.
        let row = Fx.row(seconds: Fx.projectedSeconds())
        #expect(localisation(row, index: nil)
                == .named([AdSpanBounds(start: 1_590.0, end: 1_611.42)]))
        #expect(localisation(Fx.row(), index: nil) == .unreadable)
    }

    @Test("contribution narrows a stale projected row to the seconds, not the tile")
    func contributionUsesTheSeconds() {
        let row = Fx.row(seconds: Fx.projectedSeconds())
        #expect(SemanticSweepMarkComposer.contribution(
            of: row, in: [row], supportLines: Fx.indexAtAnotherVersion
        ) == [AdSpanBounds(start: 1_590.0, end: 1_611.42)])
        // 21.42 s rather than the 101.04 s window — the shu5 economics, now
        // surviving a re-segmentation.
        #expect(SemanticSweepMarkComposer.contribution(
            of: Fx.row(), in: [Fx.row()], supportLines: Fx.indexAtAnotherVersion
        ) == [AdSpanBounds(start: 1_510.38, end: 1_611.42)])
    }

    @Test("compose narrows the MARK on a stale row that carried its seconds")
    func composeNarrowsTheMark() throws {
        let row = Fx.row(seconds: Fx.projectedSeconds())
        let marks = SemanticSweepMarkComposer.compose(
            scanRows: [row],
            existingWindows: [],
            supportLines: Fx.indexAtAnotherVersion,
            analysisAssetId: Fx.assetId
        )
        let mark = try #require(marks.first)
        #expect(abs(mark.startTime - 1_590.0) < 1e-6)
        #expect(abs(mark.endTime - 1_611.42) < 1e-6)

        // …and without the seconds, the same row still produces the whole tile.
        let wide = SemanticSweepMarkComposer.compose(
            scanRows: [Fx.row()],
            existingWindows: [],
            supportLines: Fx.indexAtAnotherVersion,
            analysisAssetId: Fx.assetId
        )
        let wideMark = try #require(wide.first)
        #expect(abs(wideMark.startTime - 1_510.38) < 1e-6)
        #expect(abs(wideMark.endTime - 1_611.42) < 1e-6)
    }
}

// MARK: - 5. ADDITIVE ONLY

@Suite("the recorded seconds can only ADD localisation, never move one (playhead-qjcf)",
       .timeLimit(.minutes(1)))
struct PersistedSupportSecondsAreAdditiveTests {

    private typealias Fx = SecondsFixture

    @Test("a row that resolves TODAY is byte-identical with and without a payload")
    func resolvableRowsAreUnchanged() {
        // The invariant that makes ordering the record after `resolve`
        // defensible: no row that localises today can move. The new path is
        // reachable only where the old one refused.
        let without = Fx.row()
        let with = Fx.row(seconds: Fx.projectedSeconds())
        let a = SemanticSweepMarkComposer.localisation(
            of: without, in: [without], supportLines: Fx.index)
        let b = SemanticSweepMarkComposer.localisation(
            of: with, in: [with], supportLines: Fx.index)
        #expect(a == b)
        #expect(a == .named([AdSpanBounds(start: 1_590.0, end: 1_611.42)]))
    }

    @Test("even a DISAGREEING payload cannot move a row that resolves today")
    func aWrongPayloadCannotMoveAResolvableRow() {
        // `resolve` runs first, so a projection that somehow disagreed with the
        // live segmentation is INERT on a row the index can read. That is the
        // blast radius the ordering buys, stated as a test rather than as a
        // sentence in a doc comment.
        let row = Fx.row(
            seconds: SupportLineIndex.encodeSupportLineSpans(
                [SupportLineSpan(lineRef: 62, start: 1_511.0, end: 1_512.0)]
            )
        )
        #expect(SemanticSweepMarkComposer.localisation(
            of: row, in: [row], supportLines: Fx.index
        ) == .named([AdSpanBounds(start: 1_590.0, end: 1_611.42)]))
    }

    @Test("the DECLINED pass-B shortcut still wins over a payload")
    func declinedRefinementStillWins() throws {
        // Stage 2 runs before the refs are consulted at all, so a coarse row with
        // a declined narrower pass B underneath it is localised by that window
        // whether or not it recorded seconds. Nothing in the ordering moved.
        let coarse = Fx.row(seconds: Fx.projectedSeconds())
        let declined = Fx.row(
            id: "scan-refine",
            start: 1_560.3,
            end: 1_570.62,
            atoms: 1_846...1_860,
            disposition: .noAds,
            scanPass: "passB",
            spansJSON: "[]"
        )
        let localised = SemanticSweepMarkComposer.localisation(
            of: coarse, in: [coarse, declined], supportLines: Fx.indexAtAnotherVersion)
        #expect(localised == .named([AdSpanBounds(start: 1_560.3, end: 1_570.62)]))
    }
}

// MARK: - 6. The carrier

@Suite("the projection survives the copy and the store (playhead-qjcf)",
       .timeLimit(.minutes(1)))
struct SupportLineSecondsCarrierTests {

    private typealias Fx = SecondsFixture

    @Test("attributed() carries the projection through")
    func attributedCarriesTheProjectionThrough() {
        // `BackfillJobRunner.attributed(_:jobId:)` is on the ONLY path from
        // `makeScanResult` to `insertSemanticScanResult`. Omitting this field
        // there would drop every projected second between the writer and the
        // disk while every rail on either side stayed green — a value silently
        // lost at a copy, which is this bead's own defect class one layer over.
        let row = Fx.row(seconds: Fx.projectedSeconds())
        let stamped = row.attributed(
            createdAt: 1_755_147_470.0, scenePhase: .background, backfillJobId: "fm-1")
        #expect(stamped.supportLineSpansJSON == row.supportLineSpansJSON)
        #expect(stamped.supportLineSpansJSON != nil)
        // …and a row that recorded nothing still records nothing.
        #expect(Fx.row().attributed(
            createdAt: 1.0, scenePhase: nil, backfillJobId: nil
        ).supportLineSpansJSON == nil)
    }

    @Test("the struct's default is nil — a writer that says nothing records nothing")
    func defaultIsNil() {
        #expect(Fx.row().supportLineSpansJSON == nil)
    }

    @Test("the column round-trips through the store, and a NULL stays a NULL")
    func storeRoundTrip() async throws {
        let dir = try makeTempDir(prefix: "SupportLineSecondsV66")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(Self.asset)

        let payload = Fx.projectedSeconds()
        try await store.insertSemanticScanResult(
            Fx.row(id: "scan-with", atoms: 100...110, seconds: payload)
        )
        try await store.insertSemanticScanResult(
            Fx.row(id: "scan-without", atoms: 200...210)
        )

        let withSeconds = try #require(try await store.fetchSemanticScanResult(id: "scan-with"))
        #expect(withSeconds.supportLineSpansJSON == payload)
        // AND ITS NEIGHBOURS SURVIVE. Adding a column to a 35-slot positional
        // bind is exactly the edit that silently drops the slot beside it, and
        // it is what happened here: the first cut of this change deleted
        // `bind(stmt, 34, …)` — `usedPermissiveFallback`, playhead-iw7q's whole
        // column — while every rail in THIS file stayed green, because none of
        // them looked at a field this bead did not add. The V61 suite caught it.
        // A rail about one field is a rail about one field.
        #expect(withSeconds.verdictProvenance == .model)
        #expect(withSeconds.latencySampleCount == nil)
        #expect(withSeconds.spansJSON == Fx.support([62]))
        let withoutSeconds = try #require(try await store.fetchSemanticScanResult(id: "scan-without"))
        #expect(withoutSeconds.supportLineSpansJSON == nil)

        // And on DISK, not merely through the reader whose correctness is also
        // under test — the sibling V52/V55/V56/V61 rule.
        #expect(try Self.rawSeconds(in: dir, rowId: "scan-with") == .some(payload))
        #expect(try Self.rawSeconds(in: dir, rowId: "scan-without") == .some(String?.none))
    }

    @Test("a REPLACE takes the incoming projection, because it takes the incoming verdict")
    func replaceTakesTheIncomingProjection() async throws {
        let dir = try makeTempDir(prefix: "SupportLineSecondsV66Replace")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(Self.asset)

        // Same atom range ⇒ same reuseKeyHash ⇒ the second write replaces the
        // first. `spansJSON` is overwritten on the same statement, and
        // `persistedSupportSpans` requires the two to agree — so carrying the
        // FIRST attempt's projection forward would not merely mis-attribute it,
        // it would make the row REFUSE its own localisation.
        let first = Fx.projectedSeconds([61])
        let second = Fx.projectedSeconds([62])
        try await store.insertSemanticScanResult(
            Fx.row(id: "scan-rep", atoms: 300...310,
                   spansJSON: Fx.support([61]), seconds: first)
        )
        try await store.insertSemanticScanResult(
            Fx.row(id: "scan-rep", atoms: 300...310,
                   spansJSON: Fx.support([62]), seconds: second)
        )
        let row = try #require(try await store.fetchSemanticScanResult(id: "scan-rep"))
        #expect(row.supportLineSpansJSON == second)
        #expect(row.spansJSON == Fx.support([62]))
        // The two agree, so the row can still speak for itself.
        #expect(SemanticSweepMarkComposer.persistedSupportSpans(of: row) != nil)
    }

    @Test("a pre-V66 row climbs to V66 and its projection stays NULL — no backfill")
    func migrationDoesNotBackfill() async throws {
        let dir = try makeTempDir(prefix: "SupportLineSecondsV66Rewind")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(Self.asset)
        try await store.insertSemanticScanResult(
            Fx.row(id: "scan-pre", atoms: 400...410, seconds: Fx.projectedSeconds())
        )

        // Rewind to the V65 SHAPE, not merely the stamp — the sibling suites'
        // rule, and for its reason: a rewind that only touches `_meta` would run
        // the rung against a column that already exists, and the work under test
        // would be indistinguishable from a no-op.
        try await store.execForTesting(
            "ALTER TABLE semantic_scan_results DROP COLUMN supportLineSpansJSON"
        )
        try await store.setMetaValue(forKey: "schema_version", value: "65")
        #expect(try Self.columnExists(in: dir) == false)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try Self.columnExists(in: dir))
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // THE BACKFILL DECISION, and unlike V61's there was never a second
        // candidate: the seconds were never written and 280 of the 301 coarse
        // rows on the t4 pull carry a transcriptVersion no surviving chunk
        // carries, so there is nothing to reconstruct them from.
        #expect(try Self.rawSeconds(in: dir, rowId: "scan-pre") == .some(String?.none))
        let row = try #require(try await reopened.fetchSemanticScanResult(id: "scan-pre"))
        #expect(row.supportLineSpansJSON == nil)
        // …and being at the CURRENT version cannot rescue it either: this is the
        // permanent state of every row written before the rung.
        #expect(SemanticSweepMarkComposer.persistedSupportSpans(of: row) == nil)
    }

    @Test("the migration is idempotent and never clears a projection already recorded")
    func migrationIsIdempotent() async throws {
        let dir = try makeTempDir(prefix: "SupportLineSecondsV66Idem")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(Self.asset)
        let payload = Fx.projectedSeconds()
        try await store.insertSemanticScanResult(
            Fx.row(id: "scan-idem", atoms: 500...510, seconds: payload)
        )

        // Stamp back WITHOUT dropping the column — the shape a store has if the
        // rung is re-entered.
        try await store.setMetaValue(forKey: "schema_version", value: "65")
        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try Self.rawSeconds(in: dir, rowId: "scan-idem") == .some(payload))
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    @Test("a fixture with no semantic_scan_results still reaches head")
    func migrationSkipsMissingTable() async throws {
        let dir = try makeTempDir(prefix: "SupportLineSecondsV66NoTable")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.execForTesting("DROP TABLE semantic_scan_results")
        try await store.setMetaValue(forKey: "schema_version", value: "65")

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    @Test("an oversized projection is REFUSED at insert, on spansJSON's own terms")
    func oversizedProjectionIsRefused() async throws {
        let dir = try makeTempDir(prefix: "SupportLineSecondsV66Cap")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(Self.asset)

        // The projection is ~10x its refs on the wire, so a `spansJSON` that
        // squeaks under the 1 MB cap can produce one that does not.
        let huge = "[" + String(repeating: "x", count: 1_000_001) + "]"
        await #expect(throws: AnalysisStoreError.self) {
            try await store.insertSemanticScanResult(
                Fx.row(id: "scan-huge", atoms: 600...610, seconds: huge)
            )
        }
        #expect(try await store.fetchSemanticScanResult(id: "scan-huge") == nil)
    }

    // MARK: Raw-column probes
    //
    // Deliberately NOT routed through `AnalysisStore` for the disk claims: the
    // claim is about what is ON DISK, and asking the store would ask the same
    // read whose correctness is under test. A matched pair of bugs in the bind
    // and the read would agree perfectly, which is exactly how a column can look
    // present and be inert.

    private static let asset = AnalysisAsset(
        id: SecondsFixture.assetId,
        episodeId: "ep-qjcf",
        assetFingerprint: "fp-qjcf",
        weakFingerprint: nil,
        sourceURL: "file:///tmp/qjcf.mp3",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )

    /// `.some(nil)` = the row exists and the column is NULL.
    /// `nil`        = there is no such row.
    private static func rawSeconds(in directory: URL, rowId: String) throws -> String?? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "rawSeconds", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        let sql = "SELECT supportLineSpansJSON FROM semantic_scan_results WHERE id = ?"
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "rawSeconds", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, (rowId as NSString).utf8String, -1, nil)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        if sqlite3_column_type(stmt, 0) == SQLITE_NULL { return .some(nil) }
        guard let text = sqlite3_column_text(stmt, 0) else { return .some(nil) }
        return .some(String(cString: text))
    }

    private static func columnExists(in directory: URL) throws -> Bool {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "columnExists", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(
            db, "PRAGMA table_info(semantic_scan_results)", -1, &stmt, nil
        ) == SQLITE_OK else {
            throw NSError(domain: "columnExists", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        while sqlite3_step(stmt) == SQLITE_ROW {
            if let name = sqlite3_column_text(stmt, 1),
               String(cString: name) == "supportLineSpansJSON" {
                return true
            }
        }
        return false
    }
}

// MARK: - 7. The wiring, in SOURCE

@Suite("the writer really projects, and the reader really falls back (playhead-qjcf)",
       .timeLimit(.minutes(1)))
struct SupportLineSecondsWiringSourceCanaryTests {

    /// A behavioural rail proves the FUNCTIONS agree. It cannot prove that the
    /// production write path CALLS the projector — `makeScanResult` is private
    /// and its output goes straight to the store. So the one seam a unit test
    /// cannot reach is checked in the source text, the way this repo checks
    /// every other unreachable seam.
    ///
    /// Read a green result as "the call site is spelled here", not as "the
    /// projection is correct" — that is what the six suites above are for.
    private static func source(_ relative: String) throws -> String {
        let here = URL(fileURLWithPath: #filePath)
        let root = here
            .deletingLastPathComponent()   // AdDetection
            .deletingLastPathComponent()   // Services
            .deletingLastPathComponent()   // PlayheadTests
            .deletingLastPathComponent()   // repo root
        return try String(
            contentsOf: root.appendingPathComponent(relative), encoding: .utf8
        )
    }

    @Test("makeScanResult passes a projection into supportLineSpansJSON")
    func writerIsWired() throws {
        let text = try Self.source("Playhead/Services/AdDetection/BackfillJobRunner.swift")
        #expect(text.contains("supportLineSpansJSON: Self.encodeSupportLineSecondsForTesting("),
                "the coarse writer must project; without this the column is NULL for ever")
        #expect(text.contains("segments: inputs.segments"),
                "…and it must project from the segmentation the scan RAN, not any other")
    }

    @Test("localisation consults the recorded seconds before giving up")
    func readerIsWired() throws {
        let text = try Self.source(
            "Playhead/Services/AdDetection/SemanticSweepMarkComposer.swift")
        #expect(text.contains("if let projected = persistedSupportSpans(of: row) {"),
                "the fallback must be on the path `resolve` refuses, or V66 is inert")
    }

    @Test("the store both binds and reads the column")
    func storeIsWired() throws {
        let text = try Self.source("Playhead/Persistence/AnalysisStore/AnalysisStore.swift")
        #expect(text.contains("bind(stmt, 35, result.supportLineSpansJSON)"))
        #expect(text.contains("supportLineSpansJSON: optionalText(stmt, 34)"))
        // Both ladders. A rung added to one and not the other is invisible to
        // any test written for that rung, and it cost V60 a commit.
        let rung = "try migrateSemanticScanSupportLineSecondsV66IfNeeded()"
        #expect(text.components(separatedBy: rung).count - 1 == 2,
                "the rung must appear in migrate() AND migrateOnlyForTesting()")
    }

    @Test("the semantic-scan INSERT binds every placeholder exactly once, in order")
    func insertBindsAreContiguous() throws {
        // THE RAIL THIS BEAD OWES ITS OWN MISTAKE. The first cut of the change
        // added `bind(stmt, 35, …)` by replacing the text of `bind(stmt, 34, …)`
        // and DELETED it — `usedPermissiveFallback`, an unrelated column, went
        // silently NULL on every write. The insert still prepared and still
        // succeeded, because SQLite leaves an unbound parameter as NULL; only
        // the V61 suite noticed, and only because it happens to assert on that
        // one field.
        //
        // A positional bind list is a place where an off-by-one is invisible to
        // every test about the field you are adding, so the shape gets its own
        // check: the indices must be exactly `1...n`, and `n` must equal the
        // placeholder count. This is about the STRUCTURE, so it belongs here
        // rather than in a behavioural suite that would need one assertion per
        // column to say the same thing.
        let text = try Self.source("Playhead/Persistence/AnalysisStore/AnalysisStore.swift")
        let marker = "INSERT OR REPLACE INTO semantic_scan_results"
        let start = try #require(text.range(of: marker)).lowerBound
        let end = try #require(
            text.range(of: "try step(stmt, expecting: SQLITE_DONE)", range: start..<text.endIndex)
        ).upperBound
        let block = String(text[start..<end])

        let values = try #require(block.range(of: "VALUES ("))
        let valuesEnd = try #require(block.range(of: ")", range: values.upperBound..<block.endIndex))
        let placeholders = block[values.upperBound..<valuesEnd.lowerBound]
            .filter { $0 == "?" }.count

        var indices: [Int] = []
        var cursor = block.startIndex
        while let hit = block.range(of: "bind(stmt, ", range: cursor..<block.endIndex) {
            let tail = block[hit.upperBound...]
            let digits = tail.prefix(while: \.isNumber)
            if let value = Int(digits) { indices.append(value) }
            cursor = hit.upperBound
        }

        let detail = "binds \(indices) against \(placeholders) placeholders — an index "
            + "that is missing, repeated or out of order silently writes NULL"
        #expect(indices == Array(1...placeholders), "\(detail)")
    }
}
