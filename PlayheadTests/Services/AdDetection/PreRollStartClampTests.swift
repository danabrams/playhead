// PreRollStartClampTests.swift
// playhead-xsdz.66: coverage for the pre-roll start-at-zero clamp — the
// deterministic DAI WIDTH win for the episode's first ad slot.
//
// Pins the contract from every angle the task requires:
//   • The clamp FIRES AT THE PRODUCTION DEFAULT `N` (not a scaled config): a
//     first slot at 4 s → 0.0 using `AdDetectionConfig.default.preRollStartClampSeconds`.
//   • A first slot far past `N` (a mid-roll) is NOT clamped; mid/post slots are
//     never clamped.
//   • Auto-skip eligibility is UNCHANGED — the clamp copies `eligibilityGate` /
//     `decisionState` / `confidence` / `id` verbatim and moves ONLY the start.
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
        decisionState: AdDecisionState = .confirmed,
        eligibilityGate: SkipEligibilityGate? = .eligible,
        evidenceStart: Double? = nil,
        startEdgeAnchor: AutoSkipEdgeAnchor = .unanchored
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
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
            startEdgeAnchor: startEdgeAnchor.rawValue
        )
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

    // MARK: - Auto-skip eligibility / all non-boundary fields preserved

    @Test("eligibility, decisionState, confidence, id, anchors, end all preserved")
    func eligibilityPathUntouched() {
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

        #expect(clamped.startTime == 0.0)                       // only this moved
        #expect(clamped.endTime == original.endTime)
        #expect(clamped.id == original.id)                      // ordinal id stable
        #expect(clamped.eligibilityGate == original.eligibilityGate)
        #expect(clamped.decisionState == original.decisionState)
        #expect(clamped.confidence == original.confidence)
        #expect(clamped.evidenceStartTime == original.evidenceStartTime)  // evidence not widened
        #expect(clamped.startEdgeAnchor == original.startEdgeAnchor)
        #expect(clamped.endEdgeAnchor == original.endEdgeAnchor)
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
