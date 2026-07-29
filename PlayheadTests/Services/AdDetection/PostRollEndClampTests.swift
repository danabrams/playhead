// PostRollEndClampTests.swift
// playhead-aqo9: coverage for the post-roll end-at-EOF clamp — the mirror of
// `PreRollStartClamp` and the larger half of the free-edge width win.
//
// Pins the contract from every angle the bead requires:
//   • The clamp FIRES AT THE PRODUCTION DEFAULT on the geometry MEASURED on
//     Dan's device (a last window ending 38–40 s short of EOF).
//   • THE PROXIMITY GUARD holds: a last window ending 159 s short — the exact
//     disjoint case that made the unguarded form claim 2,813 s of show in one
//     episode — is NOT clamped.
//   • Only the LAST visible slot; earlier slots and the INNER (start) edge are
//     never touched.
//   • Trustworthy `.rediffByteExact` / `.stingerSnapped` end edges are exempt.
//   • Widened material is capped to mark-only while stricter gates,
//     `decisionState`, `confidence`, `startTime` and `id` are preserved; exact
//     acoustic-catalog provenance is cleared because widening invalidates it.
//   • Idempotent, monotonic (never shrink, never invert), order-preserving.
//   • A window already reaching 0.0 is refused — no mark may span a whole file.

import Foundation
import Testing
@testable import Playhead

@Suite("PostRollEndClamp (playhead-aqo9 post-roll width win)")
struct PostRollEndClampTests {

    // MARK: - Helper

    /// Build an `AdWindow` exposing the fields the clamp must preserve, so a
    /// test can assert that only `endTime` moved.
    private func window(
        id: String = UUID().uuidString,
        start: Double,
        end: Double,
        confidence: Double = 0.85,
        decisionState: AdDecisionState = .confirmed,
        eligibilityGate: SkipEligibilityGate? = .eligible,
        evidenceStart: Double? = nil,
        endEdgeAnchor: AutoSkipEdgeAnchor = .unanchored,
        catalogMatch: Bool = false,
        // A raw String rather than `AdBoundaryState` because the enum has no
        // `userMarked` case — production writes and reads that value as a raw
        // string, and the clamps match that convention.
        boundaryState: String = AdBoundaryState.acousticRefined.rawValue
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: confidence,
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
                ? "22222222-2222-2222-2222-222222222222"
                : nil,
            catalogMatchedShowId: catalogMatch ? "show-post-roll" : nil,
            catalogMatchedLearningSource: catalogMatch
                ? CatalogLearningSource.userMarkedAd.rawValue
                : nil,
            catalogMatchedLearningLifecycle: catalogMatch
                ? CatalogLearningLifecycle.explicitConfirmation.rawValue
                : nil,
            endEdgeAnchor: endEdgeAnchor.rawValue
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
        #expect(actual.boundaryState == expected.boundaryState)
        #expect(actual.decisionState == expected.decisionState)
        #expect(actual.detectorVersion == expected.detectorVersion)
        #expect(actual.evidenceStartTime == expected.evidenceStartTime)
        #expect(actual.metadataSource == expected.metadataSource)
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
        #expect(actual.catalogMatchedEntryId == expected.catalogMatchedEntryId)
        #expect(actual.catalogMatchedShowId == expected.catalogMatchedShowId)
        #expect(actual.startEdgeAnchor == expected.startEdgeAnchor)
        #expect(actual.endEdgeAnchor == expected.endEdgeAnchor)
    }

    // MARK: - Fires at the production default, on the measured geometry

    /// The load-bearing acceptance, taken straight off Dan's device: episode
    /// 8FECFDDE runs 3578.11 s, its user-marked post-roll runs 3536.4–3575.9 s,
    /// and the detector's last window stopped at 3540.0 — leaving 35.9 s of ad
    /// audible. The production default must close that.
    ///
    /// If a future change sets the default to `<= 0` (shipping the clamp inert),
    /// this test fails.
    @Test("clamp FIRES at the production default on the measured 8FECFDDE geometry")
    func firesAtProductionDefaultOnMeasuredEpisode() {
        let n = AdDetectionConfig.default.postRollEndClampProximitySeconds
        #expect(n > 0, "production default must actually engage the clamp")
        #expect(n == PostRollEndClamp.Configuration.default.maxEndDistanceSeconds,
                "service default and engine default must agree")

        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: 3420.0, end: 3540.0)],
            episodeDuration: 3578.11,
            config: .init(maxEndDistanceSeconds: n)
        )

        #expect(clamped.count == 1)
        #expect(clamped[0].endTime == 3578.11)
        #expect(clamped[0].startTime == 3420.0, "the INNER edge must not move")
    }

    /// Same scenario through the ENGINE default (`.default`) — a second guard
    /// that the shipped default value fires.
    @Test("clamp fires under the engine .default config")
    func firesUnderEngineDefault() {
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: 1571.0, end: 1615.62)],
            episodeDuration: 1655.82
        )
        #expect(clamped[0].endTime == 1655.82)
    }

    /// Boundary: a last slot ending EXACTLY `N` seconds short of EOF is inside
    /// the inclusive proximity band and is clamped.
    @Test("last slot ending exactly N seconds short of EOF is clamped (inclusive bound)")
    func inclusiveProximityBound() {
        let n = AdDetectionConfig.default.postRollEndClampProximitySeconds
        let duration = 1800.0
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: duration - n - 30.0, end: duration - n)],
            episodeDuration: duration,
            config: .init(maxEndDistanceSeconds: n)
        )
        #expect(clamped[0].endTime == duration)
    }

    // MARK: - THE PROXIMITY GUARD

    /// The reason this clamp needs a guard the pre-roll clamp does not. Episode
    /// B10C7BC8 runs 3468.42 s; its user-marked post-roll is 3397.7–3466.4 s but
    /// the detector's LAST window is 3297.9–3309.1 — 159.4 s short and DISJOINT
    /// from the ad. Snapping that end to EOF is what made the unguarded form
    /// claim 92.6 s of show in this episode alone.
    @Test("proximity guard: a last window 159s short of EOF is NOT clamped")
    func proximityGuardRefusesDisjointLastWindow() {
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: 3297.9, end: 3309.06)],
            episodeDuration: 3468.42
        )
        #expect(clamped[0].endTime == 3309.06, "disjoint last window must not reach EOF")
    }

    /// The worst measured case: D75D7584 runs 2932.94 s and its last visible
    /// window ends at 119.82 — 2,813 s short. The unguarded snap would have
    /// marked three quarters of the episode as an ad.
    @Test("proximity guard: a last window 2813s short of EOF is NOT clamped")
    func proximityGuardRefusesFarLastWindow() {
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: 90.0, end: 119.82)],
            episodeDuration: 2932.94
        )
        #expect(clamped[0].endTime == 119.82)
    }

    /// Boundary: one tick past the band is refused.
    @Test("last slot just past N seconds from EOF is NOT clamped")
    func justPastProximityBoundNotClamped() {
        let n = AdDetectionConfig.default.postRollEndClampProximitySeconds
        let duration = 1800.0
        let end = duration - n - 0.5
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: end - 30.0, end: end)],
            episodeDuration: duration,
            config: .init(maxEndDistanceSeconds: n)
        )
        #expect(clamped[0].endTime == end)
    }

    // MARK: - Only the last slot; earlier slots and inner edges never touched

    @Test("only the last slot is clamped; pre-roll and mid-roll untouched")
    func onlyLastSlotClamped() {
        let pre = window(id: "pre", start: 0.0, end: 30.0)
        let mid = window(id: "mid", start: 900.0, end: 960.0)
        let post = window(id: "post", start: 1750.0, end: 1780.0)

        let clamped = PostRollEndClamp.clamp(
            windows: [pre, mid, post],
            episodeDuration: 1800.0
        )

        #expect(clamped.count == 3)
        #expect(clamped[0].endTime == 30.0)      // pre-roll untouched
        #expect(clamped[1].endTime == 960.0)     // mid-roll untouched
        #expect(clamped[2].endTime == 1800.0)    // post-roll widened
        #expect(clamped[2].startTime == 1750.0)  // its inner edge untouched
    }

    /// "Last slot" = latest END, NOT array position — robust to an unsorted
    /// window list. Also proves the other slots keep their positions.
    @Test("last slot is the latest-end window, not array.last")
    func lastSlotDefinedByLatestEndNotArrayOrder() {
        let post = window(id: "post", start: 1750.0, end: 1780.0)
        let mid = window(id: "mid", start: 900.0, end: 960.0)

        // Deliberately out of order: the post-roll appears first in the array.
        let clamped = PostRollEndClamp.clamp(
            windows: [post, mid],
            episodeDuration: 1800.0
        )

        #expect(clamped[0].id == "post")
        #expect(clamped[0].endTime == 1800.0)  // latest-end post-roll widened
        #expect(clamped[1].id == "mid")
        #expect(clamped[1].endTime == 960.0)   // array.last (mid-roll) untouched
    }

    /// TWO visible unanchored slots BOTH inside the proximity band: only the
    /// LATEST-end slot is the post-roll and is clamped; the earlier in-band slot
    /// keeps its detected end — its end is NOT free at EOF (another ad follows
    /// it), and clamping it too would collide two windows at the file end. This
    /// is the discriminator the single-window cases lack: a "clamp EVERY in-band
    /// visible window" mutant passes all of them but fails here.
    @Test("only the latest of TWO in-band visible slots is clamped")
    func onlyLatestOfTwoInBandSlotsClamped() {
        let duration = 1800.0
        let earlier = window(id: "a", start: 1745.0, end: 1755.0)
        let later = window(id: "b", start: 1760.0, end: 1775.0)
        let n = AdDetectionConfig.default.postRollEndClampProximitySeconds
        #expect(duration - earlier.endTime <= n && duration - later.endTime <= n,
                "both fixtures must sit inside the proximity band for this to discriminate")

        let clamped = PostRollEndClamp.clamp(
            windows: [earlier, later],
            episodeDuration: duration
        )

        #expect(clamped[0].id == "a")
        #expect(clamped[0].endTime == 1755.0)   // earlier in-band slot untouched
        #expect(clamped[1].id == "b")
        #expect(clamped[1].endTime == duration) // latest-end post-roll widened
    }

    // MARK: - Trustworthy anchored edges are exempt

    @Test("byte-exact rediff end edge is NOT clamped (precise boundary preserved)")
    func rediffByteExactEndNotClamped() {
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: 1740.0, end: 1780.0, endEdgeAnchor: .rediffByteExact)],
            episodeDuration: 1800.0
        )
        #expect(clamped[0].endTime == 1780.0)  // precise DAI edge untouched
    }

    @Test("stinger-snapped end edge is NOT clamped (precise boundary preserved)")
    func stingerSnappedEndNotClamped() {
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: 1740.0, end: 1780.0, endEdgeAnchor: .stingerSnapped)],
            episodeDuration: 1800.0
        )
        #expect(clamped[0].endTime == 1780.0)
    }

    /// An anchored LAST slot blocks the clamp entirely — it does NOT fall
    /// through to clamp an earlier unanchored slot (the post-roll is the
    /// anchored one; there is nothing later to widen).
    @Test("an anchored last slot does not cause an earlier slot to be clamped")
    func anchoredLastSlotBlocksClamp() {
        let earlier = window(id: "mid", start: 900.0, end: 960.0, endEdgeAnchor: .unanchored)
        let anchoredPost = window(id: "post", start: 1740.0, end: 1780.0,
                                  endEdgeAnchor: .rediffByteExact)
        let clamped = PostRollEndClamp.clamp(
            windows: [earlier, anchoredPost],
            episodeDuration: 1800.0
        )
        #expect(clamped[0].endTime == 960.0)   // earlier slot untouched
        #expect(clamped[1].endTime == 1780.0)  // anchored post-roll untouched
    }

    // MARK: - Geometry changes carry the gate through, never raise it

    /// SPECIFICATION CHANGE, playhead-aqo9 (2026-07-29). Inverted deliberately —
    /// this previously asserted that widening DEMOTED an eligible window.
    ///
    /// This clamp is the one that was actually costing auto-skips: with the
    /// pre-roll ceiling reverted to its old value, five tests asserting
    /// `eligibilityGate == .eligible` still failed, which is what identified it.
    /// The clamp moves only the last slot's END rightward to the episode
    /// duration — an OUTER edge, bounded by the end of the audio, with no show
    /// content beyond it. The INNER edge (a post-roll's start) is what can eat
    /// the show, and the clamp never touches it. See the companion note in
    /// PreRollStartClampTests for the full reasoning.
    @Test("widened eligible material STAYS eligible and other fields are preserved")
    func widenedEligibleMaterialKeepsItsGate() {
        let original = window(
            id: "post-roll-id",
            start: 1740.0,
            end: 1780.0,
            confidence: 0.91,
            decisionState: .confirmed,
            eligibilityGate: .eligible,
            evidenceStart: 1740.0
        )
        let clamped = PostRollEndClamp.clamp(
            windows: [original],
            episodeDuration: 1800.0
        )[0]

        #expect(clamped.endTime == 1800.0)
        #expect(clamped.startTime == original.startTime)
        #expect(clamped.id == original.id)
        #expect(clamped.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "widening the FREE outer edge must not cost auto-skip eligibility")
        #expect(clamped.decisionState == original.decisionState)
        #expect(clamped.confidence == original.confidence)
        #expect(clamped.evidenceStartTime == original.evidenceStartTime)
        #expect(clamped.startEdgeAnchor == original.startEdgeAnchor)
        #expect(clamped.endEdgeAnchor == original.endEdgeAnchor)
    }

    /// The inner edge is the one that eats the show, so prove it byte-identical
    /// rather than inferring it from `startTime == original.startTime` above.
    @Test("the INNER edge is byte-identical across a clamp that fires")
    func innerEdgeIsNeverMoved() {
        let original = window(start: 1740.25, end: 1780.0, eligibilityGate: .eligible)
        let clamped = PostRollEndClamp.clamp(
            windows: [original],
            episodeDuration: 1800.0
        )[0]
        #expect(clamped.endTime == 1800.0, "outer edge widened")
        #expect(
            clamped.startTime.bitPattern == original.startTime.bitPattern,
            "a post-roll's START is its inner edge — the clamp must never move it, not even by a rounding step"
        )
    }

    /// Carrying the gate through must never become RAISING it. `nil` is the case
    /// the old severity arithmetic swallowed: it mapped nil to `markOnly`, so an
    /// ungated window silently acquired a gate the pipeline never assigned.
    @Test("a nil gate stays nil — widening never invents a gate")
    func nilGateStaysNil() {
        let ungated = window(start: 1740.0, end: 1780.0, eligibilityGate: nil)
        let clamped = PostRollEndClamp.clamp(
            windows: [ungated],
            episodeDuration: 1800.0
        )[0]
        #expect(clamped.endTime == 1800.0)
        #expect(clamped.eligibilityGate == nil,
                "the clamp is a WIDTH change; it has no authority to assign a gate")
    }

    /// The mirror of the pre-roll protected-region refusal: a DETECTOR window may
    /// not be widened over a range the listener marked by hand. The mark is a
    /// separately persisted row and is not in `windows`, so the clamp must be told.
    @Test("a last slot is NOT widened across a protected user-marked region")
    func protectedRegionRefusesTheClamp() {
        let detector = window(start: 1700.0, end: 1750.0, eligibilityGate: .markOnly)
        let clamped = PostRollEndClamp.clamp(
            windows: [detector],
            episodeDuration: 1800.0,
            protectedRegions: [(start: 1770.0, end: 1790.0)]
        )[0]
        #expect(clamped.endTime == 1750.0,
                "widening to EOF would swallow the listener's [1770, 1790) mark; refuse instead")
        #expect(clamped.startTime == 1700.0)
    }

    /// The refusal must be SCOPED, or it silently disables the clamp for any
    /// episode the listener has ever touched.
    @Test("a protected region OUTSIDE the widening path does not block the clamp")
    func protectedRegionElsewhereStillClamps() {
        let detector = window(start: 1740.0, end: 1780.0, eligibilityGate: .eligible)
        let clamped = PostRollEndClamp.clamp(
            windows: [detector],
            episodeDuration: 1800.0,
            protectedRegions: [(start: 60.0, end: 90.0)]   // a pre-roll mark
        )[0]
        #expect(clamped.endTime == 1800.0,
                "a mark far from (1780, 1800] must not veto an unrelated post-roll clamp")
        #expect(clamped.eligibilityGate == SkipEligibilityGate.eligible.rawValue)
    }

    /// A degenerate region must not become an accidental global veto.
    @Test("a zero-width or non-finite protected region protects nothing")
    func degenerateProtectedRegionIsIgnored() {
        let detector = window(start: 1740.0, end: 1780.0)
        let zeroWidth = PostRollEndClamp.clamp(
            windows: [detector],
            episodeDuration: 1800.0,
            protectedRegions: [(start: 1790.0, end: 1790.0)]
        )[0]
        #expect(zeroWidth.endTime == 1800.0, "a zero-width region cannot contain a mark")
        let nonFinite = PostRollEndClamp.clamp(
            windows: [detector],
            episodeDuration: 1800.0,
            protectedRegions: [(start: 1790.0, end: .nan)]
        )[0]
        #expect(nonFinite.endTime == 1800.0, "non-finite geometry must not veto the clamp")
    }

    /// playhead-527u + the fidelity rule: `.unanchored` means "no DETECTOR
    /// anchored this edge", NOT "no human chose it". Fails without the
    /// `boundaryState != "userMarked"` guard.
    @Test("a USER-MARKED post-roll is never moved, even inside the proximity zone")
    func userMarkedWindowIsNotClamped() {
        let userMark = window(
            start: 1740.0,
            end: 1780.0,
            eligibilityGate: .eligible,
            boundaryState: "userMarked"
        )
        let clamped = PostRollEndClamp.clamp(
            windows: [userMark],
            episodeDuration: 1800.0
        )[0]
        #expect(clamped.endTime == 1780.0,
                "the listener chose 1780.0; a positional heuristic may not overrule them")
        #expect(clamped.startTime == 1740.0)
        #expect(clamped.eligibilityGate == SkipEligibilityGate.eligible.rawValue)
    }

    @Test("widening never weakens an existing correction block")
    func stricterGateIsPreserved() {
        let blocked = window(
            start: 1740.0,
            end: 1780.0,
            eligibilityGate: .blockedByUserCorrection
        )
        let clamped = PostRollEndClamp.clamp(
            windows: [blocked],
            episodeDuration: 1800.0
        )[0]
        #expect(clamped.endTime == 1800.0)
        #expect(
            clamped.eligibilityGate
                == SkipEligibilityGate.blockedByUserCorrection.rawValue
        )
    }

    @Test("host-read mark-only post-roll stays mark-only after clamp")
    func hostReadStaysMarkOnly() {
        let hostRead = window(
            start: 1740.0,
            end: 1780.0,
            confidence: 0.55,
            decisionState: .candidate,
            eligibilityGate: .markOnly
        )
        let clamped = PostRollEndClamp.clamp(
            windows: [hostRead],
            episodeDuration: 1800.0
        )[0]
        #expect(clamped.endTime == 1800.0)
        #expect(clamped.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
        #expect(clamped.decisionState == AdDecisionState.candidate.rawValue)
    }

    @Test("widening a post-roll clears exact catalog-match provenance")
    func clampedGeometryClearsCatalogMatchProvenance() {
        let original = window(
            start: 1740.0,
            end: 1780.0,
            catalogMatch: true
        )
        #expect(
            original.hasCompatibleCatalogMatchProvenance(
                expectedShowId: "show-post-roll"
            )
        )

        let clamped = PostRollEndClamp.clamp(
            windows: [original],
            episodeDuration: 1800.0
        )[0]

        #expect(clamped.endTime == 1800.0)
        #expect(clamped.catalogStoreMatchSimilarity == nil)
        #expect(clamped.catalogFingerprintVersion == nil)
        #expect(clamped.catalogMatchedEntryId == nil)
        #expect(clamped.catalogMatchedShowId == nil)
        #expect(clamped.catalogMatchedLearningSource == nil)
        #expect(clamped.catalogMatchedLearningLifecycle == nil)
        #expect(!clamped.claimsCatalogMatch)
    }

    @Test("a no-op post-roll clamp preserves still-bound catalog provenance")
    func noOpPreservesCatalogMatchProvenance() {
        let original = window(
            start: 400.0,
            end: 460.0,
            catalogMatch: true
        )

        let unchanged = PostRollEndClamp.clamp(
            windows: [original],
            episodeDuration: 1800.0
        )[0]

        #expect(unchanged.endTime == original.endTime)
        #expect(
            unchanged.catalogStoreMatchSimilarity
                == original.catalogStoreMatchSimilarity
        )
        #expect(unchanged.catalogMatchedEntryId == original.catalogMatchedEntryId)
        #expect(
            unchanged.hasCompatibleCatalogMatchProvenance(
                expectedShowId: "show-post-roll"
            )
        )
    }

    // MARK: - Whole-episode refusal (the composition guard)

    /// `PreRollStartClamp` runs FIRST in `runBackfill`. In an episode with a
    /// single visible ad window it would set that window's start to 0.0 — and if
    /// this clamp then pushed its end to EOF, the mark would cover the ENTIRE
    /// FILE. Refuse, whether the 0.0 came from the pre-roll clamp or from the
    /// detector itself.
    @Test("a window already reaching 0.0 is refused (no mark spans a whole file)")
    func windowReachingZeroIsRefused() {
        let wholeFile = window(start: 0.0, end: 1780.0)
        let clamped = PostRollEndClamp.clamp(
            windows: [wholeFile],
            episodeDuration: 1800.0
        )
        #expect(clamped[0].endTime == 1780.0, "must not widen a mark to the whole episode")
        #expect(clamped[0].startTime == 0.0)
    }

    /// Composed with the real pre-roll clamp: the single-window episode survives
    /// the two-clamp pipeline with its end intact.
    @Test("composed pre+post clamps never produce a whole-episode mark")
    func composedClampsNeverSpanWholeEpisode() {
        let duration = 200.0
        let single = window(id: "only", start: 10.0, end: 160.0)
        let afterPre = PreRollStartClamp.clamp(windows: [single])
        #expect(afterPre[0].startTime == 0.0, "precondition: the pre-roll clamp must fire here")

        let afterPost = PostRollEndClamp.clamp(
            windows: afterPre,
            episodeDuration: duration
        )
        #expect(afterPost[0].endTime == 160.0, "the composed pipeline must not claim the whole file")
    }

    // MARK: - No-ops and fail-closed

    @Test("empty slot list is a no-op")
    func emptyIsNoOp() {
        #expect(PostRollEndClamp.clamp(windows: [], episodeDuration: 1800.0).isEmpty)
    }

    @Test("a last slot already ending at the episode end is a no-op")
    func endAlreadyAtDurationNoOp() {
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: 1770.0, end: 1800.0)],
            episodeDuration: 1800.0
        )
        #expect(clamped[0].endTime == 1800.0)
        #expect(clamped[0].startTime == 1770.0)
    }

    @Test("a last slot ending PAST the episode end is not pulled back")
    func endPastDurationIsNotShrunk() {
        let clamped = PostRollEndClamp.clamp(
            windows: [window(start: 1770.0, end: 1805.0)],
            episodeDuration: 1800.0
        )
        #expect(clamped[0].endTime == 1805.0, "monotonic: the end never moves leftward")
    }

    @Test("non-positive threshold disables the clamp")
    func disabledWhenThresholdNonPositive() {
        let windows = [window(start: 1740.0, end: 1780.0)]
        #expect(
            PostRollEndClamp.clamp(windows: windows, episodeDuration: 1800.0,
                                   config: .init(maxEndDistanceSeconds: 0)).first?.endTime == 1780.0
        )
        #expect(
            PostRollEndClamp.clamp(windows: windows, episodeDuration: 1800.0,
                                   config: .init(maxEndDistanceSeconds: -5)).first?.endTime == 1780.0
        )
    }

    /// NOTE ON WHAT ENFORCES THIS: the composite is fail-closed, but the
    /// explicit `episodeDuration.isFinite, > 0` guard is not what enforces it —
    /// deleting that guard leaves this test green, because `endTime <
    /// episodeDuration` rejects 0 / -1 / NaN and the proximity comparison
    /// rejects `.infinity`. The guard is kept as defence in depth (see the
    /// comment at its site); this test pins the OBSERVABLE contract, which is
    /// the thing that must not regress however the guards are arranged.
    @Test("a missing or non-finite episode duration fails closed")
    func unknownDurationFailsClosed() {
        let valid = window(start: 1740.0, end: 1780.0)
        for duration in [0.0, -1.0, Double.nan, .infinity] {
            let output = PostRollEndClamp.clamp(windows: [valid], episodeDuration: duration)
            #expect(output.count == 1)
            guard let first = output.first else { continue }
            expectUnchanged(first, from: valid)
        }
    }

    @Test("non-finite configuration and geometry fail closed")
    func malformedInputsDoNotClamp() {
        let valid = window(start: 1740.0, end: 1780.0)
        for threshold in [Double.nan, .infinity] {
            let output = PostRollEndClamp.clamp(
                windows: [valid],
                episodeDuration: 1800.0,
                config: .init(maxEndDistanceSeconds: threshold)
            )
            #expect(output.count == 1)
            guard let first = output.first else { continue }
            expectUnchanged(first, from: valid)
        }

        let malformed = [
            window(start: 1740.0, end: .infinity),
            window(start: .infinity, end: 1780.0),
            window(start: 1780.0, end: 1740.0),
        ]
        for input in malformed {
            let output = PostRollEndClamp.clamp(windows: [input], episodeDuration: 1800.0)
            #expect(output.count == 1)
            guard let first = output.first else { continue }
            expectUnchanged(first, from: input)
        }
    }

    // MARK: - Suppressed windows are not the "last slot"

    @Test("a suppressed latest window is skipped; the last VISIBLE slot is clamped")
    func suppressedNotTreatedAsLastSlot() {
        let suppressed = window(id: "sup", start: 1790.0, end: 1795.0, decisionState: .suppressed)
        let visible = window(id: "vis", start: 1740.0, end: 1770.0, decisionState: .confirmed)

        let clamped = PostRollEndClamp.clamp(
            windows: [suppressed, visible],
            episodeDuration: 1800.0
        )

        #expect(clamped[0].endTime == 1795.0)  // suppressed window untouched
        #expect(clamped[1].endTime == 1800.0)  // last VISIBLE slot widened
    }

    // MARK: - Idempotent + monotonic

    @Test("idempotent: clamp(clamp(x)) == clamp(x)")
    func idempotent() {
        let windows = [window(start: 900.0, end: 960.0), window(start: 1740.0, end: 1780.0)]
        let once = PostRollEndClamp.clamp(windows: windows, episodeDuration: 1800.0)
        let twice = PostRollEndClamp.clamp(windows: once, episodeDuration: 1800.0)
        #expect(once.map(\.startTime) == twice.map(\.startTime))
        #expect(once.map(\.endTime) == twice.map(\.endTime))
        #expect(twice[1].endTime == 1800.0)
    }

    @Test("monotonic: coverage never shrinks and end never precedes start")
    func monotonicNeverShrinkNeverInvert() {
        let original = window(start: 1770.0, end: 1780.0)
        let clamped = PostRollEndClamp.clamp(
            windows: [original],
            episodeDuration: 1800.0
        )[0]

        #expect(clamped.endTime - clamped.startTime >= original.endTime - original.startTime)
        #expect(clamped.endTime >= clamped.startTime)
    }

    // MARK: - Regression (the bead's acceptance scenario)

    /// The bead's own accounting, replayed: across the three episodes whose last
    /// visible window fell inside the proximity band, the clamp recovers the ad
    /// seconds that were left audible and moves no inner edge.
    @Test("regression: the three measured in-band episodes all reach EOF")
    func regressionMeasuredInBandEpisodes() {
        let cases: [(start: Double, end: Double, duration: Double)] = [
            (3420.0, 3540.0, 3578.11),   // 8FECFDDE — 38.1s short
            (1571.0, 1615.62, 1655.82),  // CD1AD629 — 40.2s short
            (3152.0, 3168.84, 3170.42),  // 144C8A80 — 1.6s short
        ]
        for episode in cases {
            let clamped = PostRollEndClamp.clamp(
                windows: [window(start: episode.start, end: episode.end)],
                episodeDuration: episode.duration
            )
            #expect(clamped[0].endTime == episode.duration)
            #expect(clamped[0].startTime == episode.start)
        }
    }
}
