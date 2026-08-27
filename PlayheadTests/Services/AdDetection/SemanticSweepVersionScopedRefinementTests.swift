// SemanticSweepVersionScopedRefinementTests.swift
// playhead-9s1z — a pass-B refinement narrows only a window at its OWN
// transcript version, and a refused pairing leaves the mark set entirely.
//
// THE DEFECT. `SemanticSweepMarkComposer.presenceExtents` narrowed a coarse
// `containsAd` window to its intersection with ANY overlapping `passB`
// refinement and never compared the two rows' `transcriptVersion`. The
// refinement's `firstLineRef`/`lastLineRef` were resolved against a DIFFERENT
// segmentation, so the seconds it names are the runner's projection of a claim
// about audio nobody checked was this window's audio. playhead-kg6i fixed the
// COUNTING half of that blindness (votes are version-scoped) and playhead-shu5
// the DECLINING half (`declinedRefinementSpans` requires the same version);
// this is the last one, the affirming geometry.
//
// WHY IT IS A DROP AND NOT A WIDEN, which is the whole of Dan's call. Refusing
// the pairing leaves the coarse window un-narrowed, and there are two things
// one can then do with a ~95 s tile: emit it, or emit nothing. Measured over
// all 15 assets of the 2026-08-19 t4 device pull with the real composer
// compiled on the host (`tools/9s1z`):
//
//     leave it                 78 marks / 7224.0 s
//     same version, WIDEN      77 marks / 7273.5 s   +96.0 s inner / 0 outer / -46.5 s
//     same version, DROP       78 marks / 7224.0 s    0.0 / 0.0 / 0.0
//
// Every second the widening option added was at an INNER edge — show, with the
// nearest episode boundary 337 s away — and it is show in the transcript, not
// by inference: 50.9 s of Conan interviewing Leslie Jones, 20.7 s of the
// blood-pressure segment on `561CEF5B`. And it does not merely widen. See
// `theMillerLiteMark…` below: widening DELETES a real ad.
//
// WHAT THESE TESTS ARE FOR, in three groups that must all hold:
//
//   * the RULE — `aCrossVersionRefinement…` and
//     `aSameVersionNarrowingSurvives…` are the two suppressions, and both fail
//     against the old behaviour.
//   * the NON-RULE — `aCoarseWindowWithNoRefinementAtAll…` and
//     `aRefinementInsideNoCoarseWindow…` fail against any over-broad "fix"
//     that suppresses a window merely for being un-narrowed, or a refinement
//     merely for being unclaimed. Neither is a cross-version case.
//   * the FIELD — the three worked examples off the pull, at their real bounds,
//     which must be BYTE-IDENTICAL before and after this change. They are the
//     rail that says this rule cost no reach on the data it was chosen from.
//
// THE CONTINGENCY, stated here because it is the thing a future reader will
// otherwise rediscover by measurement. The zero cost above is not a property of
// the rule; it is a property of this pull. The rule suppresses 16 coarse
// windows and drops 65 of 371 presence extents, and the marks are unchanged
// only because 11 of those 16 have a coarse row with IDENTICAL BOUNDS at the
// refinement's own version — a same-version sibling that produces the same
// narrowing — and the other 5 sit under audio stage 5 blocks anyway. Where no
// such sibling exists, this rule REMOVES a mark the old behaviour kept. That is
// `aCrossVersionRefinementYieldsNoMarkAtAll`, and it is deliberately a test
// rather than a footnote.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum VersionScopeFixture {

    static let assetId = "asset-9s1z"

    /// Version prefixes verbatim from the t4 pull's `7DD870DC` rows: three
    /// screenings of one episode, two of which also carry a `passB` pass.
    static let refinedVersionA = "a2b45f5d"
    static let refinedVersionB = "74b20e0c"
    static let unrefinedVersion = "f02beebe"

    /// `561CEF5B`'s two.
    static let bpRefinedVersion = "37772e3f"
    static let bpUnrefinedVersion = "deace512"

    /// A coarse row's payload. Non-empty `supportLineRefs` matter: an EMPTY one
    /// makes the row `.absent`, which routes into playhead-my33's sole-backing
    /// rule and would confound what these tests are measuring.
    static func coarseSupport(_ refs: [Int]) -> String {
        let list = refs.map(String.init).joined(separator: ",")
        return #"{"supportLineRefs":[\#(list)],"certainty":"strong"}"#
    }

    /// A `passB` row's payload — an ARRAY of refined spans. Shape copied from
    /// the pull's own `7DD870DC` [3168.96-3215.46] row.
    static func refinedSupport(firstLineRef: Int, lastLineRef: Int) -> String {
        "[" + #"{"anchors":[],"certainty":"strong","commercialIntent":"paid","#
            + #""firstLineRef":\#(firstLineRef),"lastLineRef":\#(lastLineRef),"#
            + #""ownership":"thirdParty","ownershipInferenceWasSuppressed":false}"# + "]"
    }

    static func coarse(
        _ start: Double,
        _ end: Double,
        version: String,
        refs: [Int] = [46],
        disposition: CoarseDisposition = .containsAd
    ) -> SemanticScanResult {
        row(start, end, version: version, scanPass: "passA", disposition: disposition,
            spansJSON: disposition == .containsAd ? coarseSupport(refs) : "[]")
    }

    static func refinement(
        _ start: Double,
        _ end: Double,
        version: String,
        lineRef: Int = 46
    ) -> SemanticScanResult {
        row(start, end, version: version, scanPass: "passB", disposition: .containsAd,
            spansJSON: refinedSupport(firstLineRef: lineRef, lastLineRef: lineRef))
    }

    private static func row(
        _ start: Double,
        _ end: Double,
        version: String,
        scanPass: String,
        disposition: CoarseDisposition,
        spansJSON: String
    ) -> SemanticScanResult {
        SemanticScanResult(
            // Content-addressed enough to stay unique across a fixture: `id` is
            // the table's primary key and playhead-my33's `corroborates` rejects
            // a row that would corroborate itself by comparing it.
            id: "\(scanPass)-\(version)-\(start)-\(end)-\(disposition.rawValue)",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: .good,
            disposition: disposition,
            spansJSON: spansJSON,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "9s1z"),
            transcriptVersion: version,
            // playhead-iw7q: explicit, because the default is `.unknown` and
            // these stand for rows the MODEL graded.
            verdictProvenance: .model
        )
    }

    static func compose(
        _ rows: [SemanticScanResult],
        existingWindows: [AdWindow] = []
    ) -> [(start: Double, end: Double)] {
        SemanticSweepMarkComposer.compose(
            scanRows: rows,
            existingWindows: existingWindows,
            provenAnchorEdges: nil,
            analysisAssetId: assetId
        )
        .map { (start: $0.startTime, end: $0.endTime) }
        .sorted { $0.start < $1.start }
    }

    /// A window from ANOTHER detector, which is what stage 5 blocks on.
    static func blocker(_ start: Double, _ end: Double) -> AdWindow {
        AdWindow(
            id: "blocker-\(start)-\(end)",
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 0.9,
            skipConfidence: nil,
            boundaryState: "lexical",
            decisionState: "confirmed",
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: nil,
            catalogStoreMatchSimilarity: nil,
            catalogFingerprintVersion: nil,
            catalogMatchedEntryId: nil,
            catalogMatchedShowId: nil,
            catalogMatchedLearningSource: nil,
            catalogMatchedLearningLifecycle: nil,
            startEdgeAnchor: "unanchored",
            endEdgeAnchor: "unanchored"
        )
    }
}

private func isClose(_ lhs: Double, _ rhs: Double, tolerance: Double = 1e-9) -> Bool {
    abs(lhs - rhs) <= tolerance
}

private func matches(
    _ marks: [(start: Double, end: Double)],
    _ expected: [(Double, Double)]
) -> Bool {
    guard marks.count == expected.count else { return false }
    return zip(marks, expected).allSatisfy {
        isClose($0.start, $1.0) && isClose($0.end, $1.1)
    }
}

// MARK: - The rule

@Suite("playhead-9s1z: a refinement narrows only its own version's window")
struct SemanticSweepVersionScopedRefinementTests {

    /// BOTH suppressions, and the contingency the doc comment names.
    ///
    /// One coarse tile at one version, one refinement at another, nothing else.
    /// The old behaviour narrowed to [120, 140]. The WIDENING alternative would
    /// have emitted the whole [100, 200] tile. This emits NOTHING — and that is
    /// the case in which this rule costs reach, which is exactly why it is
    /// pinned rather than left to a measurement.
    ///
    /// It also proves the second suppression is load-bearing: without it the
    /// un-claimed refinement falls through to the orphan rule and stands alone
    /// at [120, 140], and the whole change would be a no-op.
    @Test("a cross-version refinement yields no mark at all — neither narrowed nor widened")
    func aCrossVersionRefinementYieldsNoMarkAtAll() {
        let marks = VersionScopeFixture.compose([
            VersionScopeFixture.coarse(100, 200, version: VersionScopeFixture.unrefinedVersion),
            VersionScopeFixture.refinement(120, 140, version: VersionScopeFixture.refinedVersionA)
        ])
        #expect(marks.isEmpty, "got \(marks)")
    }

    /// The deduction is AIMED, not removed. This fails against any "fix" that
    /// simply stops narrowing.
    @Test("a same-version refinement still narrows the window it refines")
    func aSameVersionRefinementStillNarrows() {
        let marks = VersionScopeFixture.compose([
            VersionScopeFixture.coarse(100, 200, version: VersionScopeFixture.refinedVersionA),
            VersionScopeFixture.refinement(120, 140, version: VersionScopeFixture.refinedVersionA)
        ])
        #expect(matches(marks, [(120, 140)]), "got \(marks)")
    }

    /// A cross-version refinement beside a same-version one changes nothing: the
    /// window is narrowed by its own, and the foreign one neither widens the
    /// mark nor reappears as a second one. Under the old behaviour this composed
    /// TWO marks — [120, 140] and [150, 190].
    @Test("a same-version narrowing survives a cross-version refinement beside it")
    func aSameVersionNarrowingSurvivesACrossVersionRefinementBesideIt() {
        let marks = VersionScopeFixture.compose([
            VersionScopeFixture.coarse(100, 200, version: VersionScopeFixture.refinedVersionA),
            VersionScopeFixture.refinement(120, 140, version: VersionScopeFixture.refinedVersionA),
            VersionScopeFixture.refinement(150, 190, version: VersionScopeFixture.refinedVersionB)
        ])
        #expect(matches(marks, [(120, 140)]), "got \(marks)")
    }
}

// MARK: - The non-rule: what must NOT be suppressed

@Suite("playhead-9s1z: the suppression is not over-broad")
struct SemanticSweepVersionScopedRefinementNonRuleTests {

    /// A coarse window nobody refined is un-narrowed for a reason that has
    /// nothing to do with transcript versions, and it keeps its standing. This
    /// is the test that fails if the drop is written as "suppress any window
    /// with no narrowing".
    @Test("a coarse window with no refinement at all still stands whole")
    func aCoarseWindowWithNoRefinementAtAllStillStandsWhole() {
        let marks = VersionScopeFixture.compose([
            VersionScopeFixture.coarse(100, 200, version: VersionScopeFixture.unrefinedVersion)
        ])
        #expect(matches(marks, [(100, 200)]), "got \(marks)")
    }

    /// A refinement inside no coarse `containsAd` window at all is a verdict
    /// nobody screened, and it stands alone — the rule the composer's own orphan
    /// loop is about. This fails if the drop is written as "suppress any
    /// unclaimed refinement".
    @Test("a refinement inside no coarse window at all still stands alone")
    func aRefinementInsideNoCoarseWindowStillStandsAlone() {
        let marks = VersionScopeFixture.compose([
            VersionScopeFixture.refinement(120, 140, version: VersionScopeFixture.refinedVersionA)
        ])
        #expect(matches(marks, [(120, 140)]), "got \(marks)")
    }

    /// Same shape, with a coarse window present but NOT overlapping. Proves the
    /// suppression is keyed on overlap and not merely on "some coarse row exists
    /// on this asset".
    @Test("a non-overlapping coarse window does not suppress an orphan refinement")
    func aNonOverlappingCoarseWindowDoesNotSuppressAnOrphanRefinement() {
        let marks = VersionScopeFixture.compose([
            VersionScopeFixture.coarse(400, 500, version: VersionScopeFixture.unrefinedVersion),
            VersionScopeFixture.refinement(120, 140, version: VersionScopeFixture.refinedVersionA)
        ])
        #expect(matches(marks, [(120, 140), (400, 500)]), "got \(marks)")
    }
}

// MARK: - The field: the three worked examples, at their real bounds

@Suite("playhead-9s1z: the t4 pull's three worked examples keep their bounds")
struct SemanticSweepVersionScopedRefinementFieldTests {

    /// `7DD870DC` 702-789 s. Three coarse screenings of one tile, two of which
    /// carry the same [726.48-738.30] refinement. Under the widening
    /// alternative this became [702.06-789.18], swallowing 24.4 s of the guest
    /// introduction before it and 50.9 s of the interview after it — *"Leslie,
    /// last time, you were on the pod…"*.
    @Test("the guest-plug mark keeps its refined bounds, not its whole tile")
    func theGuestPlugMarkKeepsItsRefinedBounds() {
        let marks = VersionScopeFixture.compose([
            VersionScopeFixture.coarse(702.06, 789.18, version: VersionScopeFixture.unrefinedVersion),
            VersionScopeFixture.coarse(702.06, 789.18, version: VersionScopeFixture.refinedVersionA),
            VersionScopeFixture.coarse(702.06, 789.18, version: VersionScopeFixture.refinedVersionB),
            VersionScopeFixture.refinement(726.48, 738.30, version: VersionScopeFixture.refinedVersionA),
            VersionScopeFixture.refinement(726.48, 738.30, version: VersionScopeFixture.refinedVersionB)
        ])
        #expect(matches(marks, [(726.48, 738.30)]), "got \(marks)")
    }

    /// `561CEF5B` 1517-1615 s, two screenings and one refinement. The widening
    /// alternative took it to [1517.64-1615.02], eating 20.7 s of the show's own
    /// medical content — *"…manage stress, manage cortisol… all of the things
    /// that Mother Nature gave us."*
    @Test("the blood-pressure mark keeps its refined bounds, not its whole tile")
    func theBloodPressureMarkKeepsItsRefinedBounds() {
        let marks = VersionScopeFixture.compose([
            VersionScopeFixture.coarse(1517.64, 1615.02, version: VersionScopeFixture.bpUnrefinedVersion,
                                       refs: [71, 72, 73, 74]),
            VersionScopeFixture.coarse(1517.64, 1615.02, version: VersionScopeFixture.bpRefinedVersion,
                                       refs: [71, 72, 73, 74]),
            VersionScopeFixture.refinement(1517.64, 1594.32, version: VersionScopeFixture.bpRefinedVersion,
                                           lineRef: 71)
        ])
        #expect(matches(marks, [(1517.64, 1594.32)]), "got \(marks)")
    }

    /// THE ONE THAT DECIDED THE OPTION. `7DD870DC` [3168.96-3215.46] is a
    /// fully-scripted Miller Lite host-read — sponsor, URL, call to action and
    /// the legal boilerplate: *"…go to merolite.com slash Conor to find delivery
    /// options near you. Do it now… Miller Brewing Company, Milwaukee,
    /// Wisconsin. 96 calories."*
    ///
    /// It must survive at its refined bounds. Its companion below says what
    /// happens if it ever stops.
    @Test("the Miller Lite mark survives at its refined bounds")
    func theMillerLiteMarkSurvivesAtItsRefinedBounds() {
        let marks = VersionScopeFixture.compose(millerLiteRows(withRefinements: true),
                                                existingWindows: [VersionScopeFixture.blocker(3076.10, 3085.90)])
        #expect(matches(marks, [(3168.96, 3215.46)]), "got \(marks)")
    }

    /// THE RAIL THE COORDINATOR ASKED FOR, and the reason widening was rejected
    /// rather than merely disliked. Take the refinements away — which is exactly
    /// the geometry the widening alternative produces, a coarse tile standing at
    /// its full width — and the mark does not get WIDER. It DISAPPEARS: the
    /// [3158.52-3229.68] tile merges with [3081.84-3157.74] across a 0.78 s gap,
    /// and stage 5 then blocks the whole 147.8 s extent on the neighbouring
    /// `detection-v1` window.
    ///
    /// So a future change that widens this mark back toward its tile does not
    /// cost 46.5 s of precision. It costs the ad. If this test ever starts
    /// returning a mark, the stage-5 blocker has moved and the whole argument
    /// above needs re-measuring — that is what it is here to say.
    @Test("widening the Miller Lite mark back to its tile does not widen it — it deletes it")
    func wideningTheMillerLiteMarkBackToItsTileDeletesIt() {
        let marks = VersionScopeFixture.compose(millerLiteRows(withRefinements: false),
                                                existingWindows: [VersionScopeFixture.blocker(3076.10, 3085.90)])
        #expect(marks.isEmpty, "got \(marks) — expected the stage-5 blocker to eat the widened extent")
    }

    /// The pull's rows over `7DD870DC` 3081-3230 s, distilled to the ones that
    /// carry the behaviour: two adjacent coarse tiles screened at three
    /// versions, and the refinement two of them carry.
    private func millerLiteRows(withRefinements: Bool) -> [SemanticScanResult] {
        var rows: [SemanticScanResult] = []
        for version in [VersionScopeFixture.unrefinedVersion,
                        VersionScopeFixture.refinedVersionA,
                        VersionScopeFixture.refinedVersionB] {
            rows.append(VersionScopeFixture.coarse(3081.84, 3157.74, version: version,
                                                   refs: [188, 189, 190]))
            rows.append(VersionScopeFixture.coarse(3158.52, 3229.68, version: version, refs: [194]))
        }
        guard withRefinements else { return rows }
        rows.append(VersionScopeFixture.refinement(3168.96, 3215.46,
                                                   version: VersionScopeFixture.refinedVersionA,
                                                   lineRef: 194))
        rows.append(VersionScopeFixture.refinement(3168.96, 3215.46,
                                                   version: VersionScopeFixture.refinedVersionB,
                                                   lineRef: 194))
        return rows
    }
}
