// PreRollStartClampTests.swift
// playhead-xsdz.66: coverage for the pre-roll start-at-zero clamp — the
// deterministic DAI WIDTH win for the episode's first ad slot.
//
// Pins the contract from every angle the task requires:
//   • The clamp FIRES AT THE PRODUCTION DEFAULT `N` (not a scaled config): a
//     first slot at 4 s → 0.0 using `AdDetectionConfig.default.preRollStartClampSeconds`.
//   • A first slot far past `N` (a mid-roll) is NOT clamped; mid/post slots are
//     never clamped.
//   • Widened material is capped to mark-only while stricter gates,
//     `decisionState`, `confidence`, and `id` are preserved. Exact
//     acoustic-catalog provenance is cleared because widening invalidates it.
//   • Idempotent, monotonic (never shrink, never invert), order-preserving.
//   • Empty-slots and start-already-0 are no-ops.

import Foundation
import Testing
@testable import Playhead

@Suite("PreRollStartClamp (playhead-xsdz.66 pre-roll width win)")
struct PreRollStartClampTests {

    // MARK: - Helper

    /// Build an `AdWindow` exposing the fields the clamp must preserve, so a
    /// test can assert that only `startTime` moved.
    private func window(
        id: String = UUID().uuidString,
        start: Double,
        end: Double,
        confidence: Double = 0.85,
        // playhead-hcpa: NON-NIL and DIFFERENT from `confidence`, deliberately.
        //
        // The clamp's only production caller is `AdDetectionService`'s fusion
        // emission, and a fusion row is exactly the population that carries two
        // numbers. With `nil` here, `expectUnchanged`'s two actuation
        // assertions were vacuous in both directions at once: the optional
        // compare was `nil == nil`, and `actuationConfidence` fell back to
        // `confidence` on BOTH sides, so it restated the detection assertion
        // three lines above it. Deleting `skipConfidence:` from
        // `withStartTimeClampedToZero()` left this suite green.
        //
        // 0.31 is the SUPPRESSED shape — below `confidence`, and below the 0.65
        // enter threshold that 0.85 clears — so the fallback a dropped forward
        // would take is a fail-open, and the rail now sees it.
        skipConfidence: Double? = 0.31,
        decisionState: AdDecisionState = .confirmed,
        eligibilityGate: SkipEligibilityGate? = .eligible,
        evidenceStart: Double? = nil,
        startEdgeAnchor: AutoSkipEdgeAnchor = .unanchored,
        catalogMatch: Bool = false,
        // A raw String rather than `AdBoundaryState` because that enum has no
        // `userMarked` case — production writes and reads the value as a raw
        // string, and the clamp matches that convention. Tests use the same
        // literal so they exercise the real value.
        boundaryState: String = AdBoundaryState.acousticRefined.rawValue
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: confidence,
            skipConfidence: skipConfidence,
            boundaryState: boundaryState,
            decisionState: decisionState.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: evidenceStart ?? start,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: eligibilityGate?.rawValue,
            catalogStoreMatchSimilarity: catalogMatch ? 0.99 : nil,
            catalogFingerprintVersion: catalogMatch
                ? CatalogFingerprintVersion.currentCatalog.rawValue
                : nil,
            catalogMatchedEntryId: catalogMatch
                ? "11111111-1111-1111-1111-111111111111"
                : nil,
            catalogMatchedShowId: catalogMatch ? "show-pre-roll" : nil,
            catalogMatchedLearningSource: catalogMatch
                ? CatalogLearningSource.userMarkedAd.rawValue
                : nil,
            catalogMatchedLearningLifecycle: catalogMatch
                ? CatalogLearningLifecycle.explicitConfirmation.rawValue
                : nil,
            startEdgeAnchor: startEdgeAnchor.rawValue
        )
    }

    private func expectUnchanged(
        _ actual: AdWindow,
        from expected: AdWindow
    ) {
        #expect(actual.id == expected.id)
        #expect(actual.analysisAssetId == expected.analysisAssetId)
        #expect(actual.startTime.bitPattern == expected.startTime.bitPattern)
        #expect(actual.endTime.bitPattern == expected.endTime.bitPattern)
        #expect(actual.confidence.bitPattern == expected.confidence.bitPattern)
        // playhead-ar60: the actuation number is checked like every other
        // field here. playhead-hcpa: note what this helper does NOT prove —
        // both call sites pass inputs the clamp REFUSES, so `actual` is the
        // untouched input and `withStartTimeClampedToZero()` never ran. The
        // rail on the copy helper itself lives in
        // `widenedEligibleMaterialKeepsItsGate`.
        #expect(actual.skipConfidence?.bitPattern == expected.skipConfidence?.bitPattern)
        #expect(actual.actuationConfidence.bitPattern == expected.actuationConfidence.bitPattern)
        #expect(actual.boundaryState == expected.boundaryState)
        #expect(actual.decisionState == expected.decisionState)
        #expect(actual.detectorVersion == expected.detectorVersion)
        #expect(actual.advertiser == expected.advertiser)
        #expect(actual.product == expected.product)
        #expect(actual.adDescription == expected.adDescription)
        #expect(actual.evidenceText == expected.evidenceText)
        #expect(actual.evidenceStartTime == expected.evidenceStartTime)
        #expect(actual.metadataSource == expected.metadataSource)
        #expect(actual.metadataConfidence == expected.metadataConfidence)
        #expect(actual.metadataPromptVersion == expected.metadataPromptVersion)
        #expect(actual.wasSkipped == expected.wasSkipped)
        #expect(actual.userDismissedBanner == expected.userDismissedBanner)
        #expect(actual.evidenceSources == expected.evidenceSources)
        #expect(actual.eligibilityGate == expected.eligibilityGate)
        #expect(
            actual.catalogStoreMatchSimilarity
                == expected.catalogStoreMatchSimilarity
        )
        #expect(
            actual.catalogFingerprintVersion
                == expected.catalogFingerprintVersion
        )
        #expect(
            actual.catalogMatchedEntryId == expected.catalogMatchedEntryId
        )
        #expect(
            actual.catalogMatchedShowId == expected.catalogMatchedShowId
        )
        #expect(
            actual.catalogMatchedLearningSource
                == expected.catalogMatchedLearningSource
        )
        #expect(
            actual.catalogMatchedLearningLifecycle
                == expected.catalogMatchedLearningLifecycle
        )
        #expect(actual.startEdgeAnchor == expected.startEdgeAnchor)
        #expect(actual.endEdgeAnchor == expected.endEdgeAnchor)
    }

    // MARK: - Fires at the production default

    /// The load-bearing acceptance: a first slot at 4 s is extended to 0.0 using
    /// the SHIPPED production default `N`. If a future change sets the default to
    /// `<= 0` (shipping the clamp inert), this test fails.
    @Test("clamp FIRES at the production default N (first slot 4s → 0.0)")
    func firesAtProductionDefault() {
        let n = AdDetectionConfig.default.preRollStartClampSeconds
        #expect(n > 0, "production default must actually engage the clamp")
        #expect(n == PreRollStartClamp.Configuration.default.maxPreRollStartSeconds,
                "service default and engine default must agree")

        let windows = [window(start: 4.0, end: 34.0)]
        let clamped = PreRollStartClamp.clamp(
            windows: windows,
            config: .init(maxPreRollStartSeconds: n)
        )

        #expect(clamped.count == 1)
        #expect(clamped[0].startTime == 0.0)
        #expect(clamped[0].endTime == 34.0)  // end edge untouched
    }

    /// Same scenario through the ENGINE default (`.default`) — a second guard
    /// that the shipped default value fires.
    @Test("clamp fires under the engine .default config")
    func firesUnderEngineDefault() {
        let clamped = PreRollStartClamp.clamp(windows: [window(start: 4.0, end: 34.0)])
        #expect(clamped[0].startTime == 0.0)
    }

    /// Boundary: a first slot starting EXACTLY at N is inside the inclusive
    /// pre-roll zone `(0, N]` and is clamped.
    @Test("first slot starting exactly at N is clamped (inclusive bound)")
    func inclusiveUpperBound() {
        let n = AdDetectionConfig.default.preRollStartClampSeconds
        let clamped = PreRollStartClamp.clamp(
            windows: [window(start: n, end: n + 30.0)],
            config: .init(maxPreRollStartSeconds: n)
        )
        #expect(clamped[0].startTime == 0.0)
    }

    // MARK: - First slot far past N is a mid-roll → NOT clamped

    @Test("first slot at 300s (mid-roll) is NOT clamped")
    func firstSlotFarPastNotClamped() {
        let windows = [window(start: 300.0, end: 360.0)]
        let clamped = PreRollStartClamp.clamp(windows: windows)
        #expect(clamped[0].startTime == 300.0)  // unchanged
        #expect(clamped[0].endTime == 360.0)
    }

    @Test("first slot just past N is NOT clamped")
    func firstSlotJustPastNotClamped() {
        let n = AdDetectionConfig.default.preRollStartClampSeconds
        let clamped = PreRollStartClamp.clamp(
            windows: [window(start: n + 0.5, end: n + 40.0)],
            config: .init(maxPreRollStartSeconds: n)
        )
        #expect(clamped[0].startTime == n + 0.5)  // unchanged
    }

    // MARK: - Only the first slot; mid/post never clamped

    @Test("only the first slot is clamped; mid-roll and post-roll untouched")
    func onlyFirstSlotClamped() {
        let pre = window(id: "pre", start: 4.0, end: 30.0)
        let mid = window(id: "mid", start: 300.0, end: 360.0)
        let post = window(id: "post", start: 1200.0, end: 1260.0)

        let clamped = PreRollStartClamp.clamp(windows: [pre, mid, post])

        #expect(clamped.count == 3)
        #expect(clamped[0].startTime == 0.0)      // pre-roll widened
        #expect(clamped[0].endTime == 30.0)
        #expect(clamped[1].startTime == 300.0)    // mid-roll untouched
        #expect(clamped[1].endTime == 360.0)
        #expect(clamped[2].startTime == 1200.0)   // post-roll untouched
        #expect(clamped[2].endTime == 1260.0)
    }

    /// "First slot" = earliest START, NOT array position — robust to an unsorted
    /// window list. Also proves the other slots keep their positions.
    @Test("first slot is the earliest-start window, not array[0]")
    func firstSlotDefinedByEarliestStartNotArrayOrder() {
        let mid = window(id: "mid", start: 300.0, end: 360.0)
        let pre = window(id: "pre", start: 4.0, end: 30.0)

        // Deliberately out of order: mid-roll appears first in the array.
        let clamped = PreRollStartClamp.clamp(windows: [mid, pre])

        // Array order preserved; only the earliest-start ("pre") window moved.
        #expect(clamped[0].id == "mid")
        #expect(clamped[0].startTime == 300.0)  // array[0] (mid-roll) untouched
        #expect(clamped[1].id == "pre")
        #expect(clamped[1].startTime == 0.0)    // earliest-start pre-roll widened
    }

    /// TWO visible unanchored slots BOTH inside the pre-roll zone `(0, N]`: only
    /// the EARLIEST-start slot is the pre-roll and is clamped to 0.0; the second
    /// in-zone slot keeps its detected start — its start is NOT free at 0:00
    /// (the first ad precedes it), and clamping it too would collide two windows
    /// at 0. This is the discriminator the single-in-zone-window cases above lack:
    /// a "clamp EVERY in-zone visible window" mutant passes all of them but fails
    /// here, so it pins the first-slot-ONLY contract.
    @Test("only the earliest of TWO in-zone visible slots is clamped")
    func onlyEarliestOfTwoInZoneSlotsClamped() {
        let n = AdDetectionConfig.default.preRollStartClampSeconds
        let firstInZone = window(id: "a", start: 3.0, end: 10.0)
        let secondInZone = window(id: "b", start: 12.0, end: 18.0)
        #expect(firstInZone.startTime <= n && secondInZone.startTime <= n,
                "both fixtures must sit in the pre-roll zone for this to discriminate")

        let clamped = PreRollStartClamp.clamp(windows: [firstInZone, secondInZone])

        #expect(clamped[0].id == "a")
        #expect(clamped[0].startTime == 0.0)     // earliest-start pre-roll widened
        #expect(clamped[1].id == "b")
        #expect(clamped[1].startTime == 12.0)    // second in-zone slot untouched
    }

    // MARK: - Trustworthy anchored edges are exempt (playhead-xsdz.66 M1)

    @Test("byte-exact rediff start edge is NOT clamped (precise boundary preserved)")
    func rediffByteExactStartNotClamped() {
        let clamped = PreRollStartClamp.clamp(
            windows: [window(start: 4.0, end: 64.0, startEdgeAnchor: .rediffByteExact)]
        )
        #expect(clamped[0].startTime == 4.0)  // precise DAI edge untouched
    }

    @Test("stinger-snapped start edge is NOT clamped (precise boundary preserved)")
    func stingerSnappedStartNotClamped() {
        let clamped = PreRollStartClamp.clamp(
            windows: [window(start: 6.0, end: 40.0, startEdgeAnchor: .stingerSnapped)]
        )
        #expect(clamped[0].startTime == 6.0)  // stinger-located edge untouched
    }

    /// An anchored FIRST slot blocks the clamp entirely — it does NOT fall
    /// through to clamp a later unanchored slot (the pre-roll is the anchored
    /// one; there is nothing earlier to widen).
    @Test("an anchored first slot does not cause a later slot to be clamped")
    func anchoredFirstSlotBlocksClamp() {
        let anchoredPre = window(id: "pre", start: 4.0, end: 34.0, startEdgeAnchor: .rediffByteExact)
        let laterUnanchored = window(id: "mid", start: 300.0, end: 360.0, startEdgeAnchor: .unanchored)
        let clamped = PreRollStartClamp.clamp(windows: [anchoredPre, laterUnanchored])
        #expect(clamped[0].startTime == 4.0)     // anchored pre-roll untouched
        #expect(clamped[1].startTime == 300.0)   // later slot untouched (not a pre-roll anyway)
    }

    // MARK: - Geometry changes carry the gate through, never raise it

    /// SPECIFICATION CHANGE, playhead-ye0n (2026-07-29). This test previously
    /// asserted the OPPOSITE — that widening demoted an eligible window to
    /// `markOnly`. It is inverted deliberately, not to accommodate an
    /// implementation, and the old behaviour was a defect for three reasons:
    ///
    ///   • THE RISK IS PER-EDGE, NOT PER-WINDOW. The clamp only moves the first
    ///     slot's START leftward to 0.0 — an OUTER edge bounded by the episode
    ///     boundary. The edge that can eat the show is the INNER one, which the
    ///     clamp never touches. Demoting the whole window for moving the free edge
    ///     surrendered auto-skip on the part that was already trustworthy, and a
    ///     mark-only banner is worth far less than a silent skip — it can even
    ///     cost show content when the listener acts on it.
    ///   • REDUNDANT where it was right: the clamp fires only on `.unanchored`
    ///     edges, and playhead-2350 is already the authority there.
    ///   • IT OVERRODE THE LISTENER: playhead-527u stamps a user-marked window
    ///     `.eligible` at the listener's own boundaries despite it carrying no
    ///     detector anchor, and this demotion undid that.
    @Test("widened eligible material STAYS eligible and other fields are preserved")
    func widenedEligibleMaterialKeepsItsGate() {
        let original = window(
            id: "pre-roll-id",
            start: 4.0,
            end: 34.0,
            confidence: 0.91,
            decisionState: .confirmed,
            eligibilityGate: .eligible,
            evidenceStart: 4.0,
            startEdgeAnchor: .unanchored
        )
        let clamped = PreRollStartClamp.clamp(windows: [original])[0]

        #expect(clamped.startTime == 0.0)
        #expect(clamped.endTime == original.endTime)
        #expect(clamped.id == original.id)
        #expect(
            clamped.eligibilityGate
                == SkipEligibilityGate.eligible.rawValue,
            "widening the FREE outer edge must not cost auto-skip eligibility"
        )
        #expect(clamped.decisionState == original.decisionState)
        #expect(clamped.confidence == original.confidence)
        // playhead-hcpa: the ACTUATION half of the same copy. Its sibling
        // `confidence` assertion one line up is the DETECTION half, and until
        // this bead that was the only one — deleting `skipConfidence:` from
        // `withStartTimeClampedToZero()` left the suite green.
        //
        // `expectUnchanged` does carry a pair of actuation assertions, but it
        // is only ever called on inputs the clamp REFUSES, so it never reaches
        // the copy helper at all. This test is the one that widens.
        #expect(
            clamped.skipConfidence != nil,
            """
            Anti-vacuity: the clamped row must carry a SEPARATE actuation \
            number. `nil` makes `actuationConfidence` collapse onto \
            `confidence`, and the expectation below becomes a restatement of \
            the detection assertion above it.
            """
        )
        #expect(
            original.skipConfidence != original.confidence,
            "fixture integrity: equal numbers make the direction test vacuous"
        )
        #expect(
            clamped.actuationConfidence == original.actuationConfidence,
            """
            Widening the free OUTER edge must not raise how much permission \
            the row has to act. A dropped forward reads as the (higher) \
            detection score, so a fusion span the correction factor \
            discounted would come out of the clamp more skip-eager than it \
            went in. Got \(clamped.actuationConfidence), expected \
            \(original.actuationConfidence).
            """
        )
        #expect(clamped.evidenceStartTime == original.evidenceStartTime)  // evidence not widened
        #expect(clamped.startEdgeAnchor == original.startEdgeAnchor)
        #expect(clamped.endEdgeAnchor == original.endEdgeAnchor)
    }

    /// Carrying the gate through must never become RAISING it. `nil` is the case
    /// the old severity arithmetic silently swallowed — it mapped nil to
    /// `markOnly`, so an ungated window acquired a gate the pipeline never
    /// assigned.
    @Test("a nil gate stays nil — widening never invents a gate")
    func nilGateStaysNil() {
        let ungated = window(start: 4.0, end: 34.0, eligibilityGate: nil)
        let clamped = PreRollStartClamp.clamp(windows: [ungated])[0]
        #expect(clamped.startTime == 0.0, "geometry still widens")
        #expect(clamped.eligibilityGate == nil,
                "the clamp is a WIDTH change; it has no authority to assign a gate")
    }

    /// The inner edge is the one that eats the show, so prove it byte-identical
    /// rather than inferring it from a plain `==` on the end time.
    @Test("the INNER edge is byte-identical across a clamp that fires")
    func innerEdgeIsNeverMoved() {
        let original = window(start: 4.25, end: 34.125, eligibilityGate: .eligible)
        let clamped = PreRollStartClamp.clamp(windows: [original])[0]
        #expect(clamped.startTime == 0.0, "outer edge widened")
        #expect(
            clamped.endTime.bitPattern == original.endTime.bitPattern,
            "a pre-roll's END is its inner edge — never moved, not even by a rounding step"
        )
    }

    // MARK: - A listener's mark is off limits (playhead-lc4c)

    /// `.unanchored` means "no DETECTOR anchored this edge", NOT "no human chose
    /// it" — a listener's window carries no edge anchor, so it reaches the clamp
    /// looking exactly like an FM guess. Fails without the `boundaryState` guard.
    @Test("a USER-MARKED first slot is never moved, even inside the pre-roll zone")
    func userMarkedWindowIsNotClamped() {
        let userMark = window(
            start: 12.0,
            end: 30.0,
            eligibilityGate: .eligible,
            boundaryState: "userMarked"
        )
        let clamped = PreRollStartClamp.clamp(windows: [userMark])[0]
        #expect(clamped.startTime == 12.0,
                "the listener chose 12.0; a positional heuristic may not overrule them")
        #expect(clamped.endTime == 30.0)
        #expect(clamped.eligibilityGate == SkipEligibilityGate.eligible.rawValue)
    }

    /// The case the in-clamp guard CANNOT catch: a DETECTOR window widened over a
    /// separately-persisted mark. The mark is not in `windows` at all, so the
    /// clamp has to be told. Observed shape: a fusion window past a [35, 55) mark
    /// was widened to [0, 60) and engulfed it, leaving two windows over the region.
    @Test("a first slot is NOT widened across a protected user-marked region")
    func protectedRegionRefusesTheClamp() {
        let detector = window(start: 18.0, end: 60.0, eligibilityGate: .markOnly)
        let clamped = PreRollStartClamp.clamp(
            windows: [detector],
            protectedRegions: [(start: 5.0, end: 15.0)]
        )[0]
        #expect(clamped.startTime == 18.0,
                "widening to 0.0 would swallow the listener's [5, 15) mark; refuse instead")
        #expect(clamped.endTime == 60.0)
    }

    /// The refusal must be SCOPED, or one mark anywhere silently disables the
    /// clamp for that episode forever.
    @Test("a protected region OUTSIDE the widening path does not block the clamp")
    func protectedRegionElsewhereStillClamps() {
        let detector = window(start: 4.0, end: 34.0, eligibilityGate: .eligible)
        let clamped = PreRollStartClamp.clamp(
            windows: [detector],
            protectedRegions: [(start: 900.0, end: 960.0)]   // a mid-roll mark
        )[0]
        #expect(clamped.startTime == 0.0,
                "a mark far from [0, 4) must not veto an unrelated pre-roll clamp")
        #expect(clamped.eligibilityGate == SkipEligibilityGate.eligible.rawValue)
    }

    /// A degenerate region must not become an accidental global veto.
    @Test("a zero-width or non-finite protected region protects nothing")
    func degenerateProtectedRegionIsIgnored() {
        let detector = window(start: 4.0, end: 34.0)
        let zeroWidth = PreRollStartClamp.clamp(
            windows: [detector],
            protectedRegions: [(start: 2.0, end: 2.0)]
        )[0]
        #expect(zeroWidth.startTime == 0.0, "a zero-width region cannot contain a mark")
        let nonFinite = PreRollStartClamp.clamp(
            windows: [detector],
            protectedRegions: [(start: 0.0, end: .nan), (start: .infinity, end: .infinity)]
        )[0]
        #expect(nonFinite.startTime == 0.0, "non-finite geometry must not veto the clamp")
    }

    @Test("widening never weakens an existing correction block")
    func stricterGateIsPreserved() {
        let blocked = window(
            start: 4,
            end: 34,
            eligibilityGate: .blockedByUserCorrection
        )
        let clamped = PreRollStartClamp.clamp(windows: [blocked])[0]
        #expect(clamped.startTime == 0)
        #expect(
            clamped.eligibilityGate
                == SkipEligibilityGate.blockedByUserCorrection.rawValue
        )
    }

    /// A NON-deterministic (host-read, mark-only) pre-roll must stay mark-only —
    /// the clamp widens it but never promotes it to auto-skip.
    @Test("host-read mark-only pre-roll stays mark-only after clamp")
    func hostReadStaysMarkOnly() {
        let hostRead = window(
            start: 3.0,
            end: 40.0,
            confidence: 0.55,
            decisionState: .candidate,
            eligibilityGate: .markOnly
        )
        let clamped = PreRollStartClamp.clamp(windows: [hostRead])[0]
        #expect(clamped.startTime == 0.0)                        // widened
        #expect(clamped.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)  // still mark-only
        #expect(clamped.decisionState == AdDecisionState.candidate.rawValue)
    }

    @Test("widening a pre-roll clears exact catalog-match provenance")
    func clampedGeometryClearsCatalogMatchProvenance() {
        let original = window(
            start: 4.0,
            end: 34.0,
            catalogMatch: true
        )
        #expect(
            original.hasCompatibleCatalogMatchProvenance(
                expectedShowId: "show-pre-roll"
            )
        )

        let clamped = PreRollStartClamp.clamp(windows: [original])[0]

        #expect(clamped.startTime == 0)
        #expect(clamped.catalogStoreMatchSimilarity == nil)
        #expect(clamped.catalogFingerprintVersion == nil)
        #expect(clamped.catalogMatchedEntryId == nil)
        #expect(clamped.catalogMatchedShowId == nil)
        #expect(clamped.catalogMatchedLearningSource == nil)
        #expect(clamped.catalogMatchedLearningLifecycle == nil)
        #expect(!clamped.claimsCatalogMatch)
    }

    @Test("a no-op pre-roll clamp preserves still-bound catalog provenance")
    func noOpPreservesCatalogMatchProvenance() {
        let original = window(
            start: 30.0,
            end: 60.0,
            catalogMatch: true
        )

        let unchanged = PreRollStartClamp.clamp(windows: [original])[0]

        #expect(unchanged.startTime == original.startTime)
        #expect(
            unchanged.catalogStoreMatchSimilarity
                == original.catalogStoreMatchSimilarity
        )
        #expect(
            unchanged.catalogMatchedEntryId
                == original.catalogMatchedEntryId
        )
        #expect(
            unchanged.hasCompatibleCatalogMatchProvenance(
                expectedShowId: "show-pre-roll"
            )
        )
    }

    @Test("non-finite configuration and geometry fail closed")
    func malformedInputsDoNotClamp() {
        let valid = window(start: 10, end: 40)
        for threshold in [Double.nan, .infinity] {
            let output = PreRollStartClamp.clamp(
                windows: [valid],
                config: .init(maxPreRollStartSeconds: threshold)
            )
            #expect(output.count == 1)
            guard let first = output.first else { continue }
            expectUnchanged(first, from: valid)
        }

        let malformed = [
            window(start: .infinity, end: .infinity),
            window(start: 10, end: .infinity),
            window(start: 10, end: 5),
        ]
        for input in malformed {
            let output = PreRollStartClamp.clamp(windows: [input])
            #expect(output.count == 1)
            guard let first = output.first else { continue }
            expectUnchanged(first, from: input)
        }
    }

    // MARK: - Suppressed windows are not the "first slot"

    @Test("a suppressed earliest window is skipped; the first VISIBLE slot is clamped")
    func suppressedNotTreatedAsFirstSlot() {
        let suppressed = window(id: "sup", start: 1.0, end: 2.0, decisionState: .suppressed)
        let visible = window(id: "vis", start: 5.0, end: 35.0, decisionState: .confirmed)

        let clamped = PreRollStartClamp.clamp(windows: [suppressed, visible])

        #expect(clamped[0].startTime == 1.0)   // suppressed window untouched
        #expect(clamped[1].startTime == 0.0)   // first VISIBLE slot widened
    }

    // MARK: - No-ops

    @Test("empty slot list is a no-op")
    func emptyIsNoOp() {
        #expect(PreRollStartClamp.clamp(windows: []).isEmpty)
    }

    @Test("a first slot already starting at 0.0 is a no-op")
    func startAlreadyZeroNoOp() {
        let clamped = PreRollStartClamp.clamp(windows: [window(start: 0.0, end: 30.0)])
        #expect(clamped[0].startTime == 0.0)
        #expect(clamped[0].endTime == 30.0)
    }

    @Test("non-positive threshold disables the clamp")
    func disabledWhenThresholdNonPositive() {
        let windows = [window(start: 4.0, end: 34.0)]
        #expect(PreRollStartClamp.clamp(windows: windows, config: .init(maxPreRollStartSeconds: 0)).first?.startTime == 4.0)
        #expect(PreRollStartClamp.clamp(windows: windows, config: .init(maxPreRollStartSeconds: -5)).first?.startTime == 4.0)
    }

    // MARK: - Idempotent + monotonic

    @Test("idempotent: clamp(clamp(x)) == clamp(x)")
    func idempotent() {
        let windows = [window(start: 4.0, end: 34.0), window(start: 300.0, end: 360.0)]
        let once = PreRollStartClamp.clamp(windows: windows)
        let twice = PreRollStartClamp.clamp(windows: once)
        #expect(once.map(\.startTime) == twice.map(\.startTime))
        #expect(once.map(\.endTime) == twice.map(\.endTime))
        #expect(twice[0].startTime == 0.0)
    }

    @Test("monotonic: coverage never shrinks and start never exceeds end")
    func monotonicNeverShrinkNeverInvert() {
        let original = window(start: 8.0, end: 20.0)
        let clamped = PreRollStartClamp.clamp(windows: [original])[0]

        let originalWidth = original.endTime - original.startTime
        let clampedWidth = clamped.endTime - clamped.startTime
        #expect(clampedWidth >= originalWidth)   // never shrinks
        #expect(clamped.startTime <= clamped.endTime)  // never inverts
    }

    // MARK: - Regression (the task's acceptance scenario)

    /// The exact task scenario: a synthetic episode whose first ad slot starts at
    /// 4 s. Without the clamp the mark stays at the detected 4 s (the input);
    /// with the clamp its start becomes 0.0 while the mid-roll is untouched.
    @Test("regression: synthetic first slot at 4s → 0.0")
    func regressionFirstSlotAtFourSeconds() {
        let episode = [
            window(id: "pre", start: 4.0, end: 64.0),
            window(id: "mid", start: 640.0, end: 700.0)
        ]
        let clamped = PreRollStartClamp.clamp(windows: episode)
        #expect(clamped[0].startTime == 0.0)     // clamped (input was 4.0)
        #expect(clamped[0].endTime == 64.0)
        #expect(clamped[1].startTime == 640.0)   // mid-roll untouched
    }
}
