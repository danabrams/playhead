// HotPathExtentGateMonotonicityTests.swift
// playhead-bllt — THE NEGATIVE. The proof that this change can only ever make
// FEWER things auto-skippable.
//
// WHY THIS SUITE IS EXHAUSTIVE AND NOT ILLUSTRATIVE
// ------------------------------------------------
// The governing constraint on any change to skip admission — the one that
// governed playhead-2350 and governs this bead — is that no input may become
// MORE eligible than it was. A test that demonstrates the narrowing ("here is a
// row that used to auto-skip and now banners") is necessary and is not
// sufficient: it says the function narrows SOMEWHERE, not that it never widens
// ANYWHERE. Those are different claims and only the second one is the safety
// argument.
//
// `HotPathExtentGate.gatedLabel` was written so the second claim is checkable
// rather than argued: it is pure, total, and its domain is finite —
//
//     label ∈ {nil, "autoSkip", "markOnly", + unrecognised strings}
//     start ∈ AutoSkipEdgeAnchor.allCases  (3)
//     end   ∈ AutoSkipEdgeAnchor.allCases  (3)
//     flag  ∈ {true, false}                (2)
//
// so this suite enumerates ALL of it and asserts the ordinal never rises. Not
// sampled, not property-based-with-a-seed: every cell, every run.
//
// Three checks, because they fail for different mutations:
//   1. MONOTONE — no cell's admission rises. The constraint, stated directly.
//   2. EXACT — the ONLY cell that changes at all is
//      (autoSkip, not-fully-anchored, blocking) → markOnly. Monotonicity alone
//      would tolerate a mutant that also demoted anchored rows, or that dropped
//      rows entirely; both are "fewer skips" and both are wrong.
//   3. COUNT — exactly how many cells change. A mutant that widens the
//      condition (say, `||` for `&&`) still passes 1, still passes 2 for the
//      cells it happens to leave alone, and fails here.

import Foundation
import Testing
@testable import Playhead

@Suite("HotPathExtentGate — monotonicity (playhead-bllt)")
struct HotPathExtentGateMonotonicityTests {

    // MARK: - The domain

    /// Every label the gate can be handed. The three the producer really emits,
    /// plus unrecognised strings — a persisted column has no type, so "what
    /// does this do with a value nobody planned for" is a real question and its
    /// answer must also be monotone.
    private static let labels: [String?] = [
        nil,
        HotPathExtentGate.autoSkipLabel,
        HotPathExtentGate.markOnlyLabel,
        // A `SkipEligibilityGate` raw value that is NOT the producer literal:
        // the exact confusion playhead-y87g had to re-diagnose.
        SkipEligibilityGate.eligible.rawValue,
        SkipEligibilityGate.markOnly.rawValue,
        "",
        "AUTOSKIP",
        "autoskip",
        "autoSkip ",
        "totally-unknown",
    ]

    private static let extents: [SpanExtentSupport] = AutoSkipEdgeAnchor.allCases
        .flatMap { start in
            AutoSkipEdgeAnchor.allCases.map { end in
                SpanExtentSupport(startAnchor: start, endAnchor: end)
            }
        }

    /// Every (label, extent, flag) triple. 10 × 9 × 2 = 180 cells.
    private static let allCells: [(label: String?, extent: SpanExtentSupport, blocking: Bool)] =
        labels.flatMap { label in
            extents.flatMap { extent in
                [true, false].map { (label: label, extent: extent, blocking: $0) }
            }
        }

    // MARK: - 1. MONOTONE

    @Test("no input becomes more admissible — the whole domain, every cell")
    func admissionNeverRises() {
        var checked = 0
        for cell in Self.allCells {
            let after = HotPathExtentGate.gatedLabel(
                cell.label,
                extent: cell.extent,
                blockingUnanchoredAutoSkip: cell.blocking
            )
            let before = HotPathExtentGate.admission(of: cell.label)
            let now = HotPathExtentGate.admission(of: after)
            #expect(
                now <= before,
                """
                MONOTONICITY VIOLATED at (label: \(String(describing: cell.label)), \
                start: \(cell.extent.startAnchor.rawValue), \
                end: \(cell.extent.endAnchor.rawValue), \
                blocking: \(cell.blocking)): admission rose \(before) → \(now). \
                This gate may only ever make FEWER things auto-skippable.
                """
            )
            checked += 1
        }
        // Guard against the domain silently emptying — a `flatMap` typo that
        // enumerated nothing would make every assertion above vacuous, which is
        // the failure mode an exhaustive proof is most exposed to.
        #expect(checked == 180, "expected 180 cells enumerated; got \(checked)")
    }

    @Test("nothing that was not already auto-skippable becomes auto-skippable")
    func autoSkipAdmissionIsNeverCreated() {
        for cell in Self.allCells {
            let after = HotPathExtentGate.gatedLabel(
                cell.label,
                extent: cell.extent,
                blockingUnanchoredAutoSkip: cell.blocking
            )
            guard HotPathExtentGate.admission(of: after) == .autoSkip else { continue }
            #expect(
                HotPathExtentGate.admission(of: cell.label) == .autoSkip,
                """
                (label: \(String(describing: cell.label)), \
                start: \(cell.extent.startAnchor.rawValue), \
                end: \(cell.extent.endAnchor.rawValue), \
                blocking: \(cell.blocking)) reached the auto-skip tier without \
                having been there before the gate ran.
                """
            )
        }
    }

    // MARK: - 2. EXACT

    @Test("the ONLY transition is autoSkip → markOnly on an unanchored extent with the block on")
    func theOnlyTransitionIsTheIntendedOne() {
        for cell in Self.allCells {
            let after = HotPathExtentGate.gatedLabel(
                cell.label,
                extent: cell.extent,
                blockingUnanchoredAutoSkip: cell.blocking
            )
            let shouldDemote = cell.blocking
                && !cell.extent.isFullyAnchored
                && cell.label == HotPathExtentGate.autoSkipLabel
            let expected = shouldDemote ? HotPathExtentGate.markOnlyLabel : cell.label
            #expect(
                after == expected,
                """
                (label: \(String(describing: cell.label)), \
                start: \(cell.extent.startAnchor.rawValue), \
                end: \(cell.extent.endAnchor.rawValue), \
                blocking: \(cell.blocking)): expected \
                \(String(describing: expected)), got \(String(describing: after)).
                """
            )
        }
    }

    @Test("a nil label stays nil — the gate never CREATES a persisted row")
    func nilIsPreserved() {
        for extent in Self.extents {
            for blocking in [true, false] {
                #expect(
                    HotPathExtentGate.gatedLabel(
                        nil,
                        extent: extent,
                        blockingUnanchoredAutoSkip: blocking
                    ) == nil,
                    "nil is `detectionOnly` — do-not-persist — and must survive as nil"
                )
            }
        }
    }

    @Test("a non-nil label never becomes nil — the gate never DROPS a row")
    func demotionIsNotADrop() {
        for cell in Self.allCells where cell.label != nil {
            #expect(
                HotPathExtentGate.gatedLabel(
                    cell.label,
                    extent: cell.extent,
                    blockingUnanchoredAutoSkip: cell.blocking
                ) != nil,
                """
                (label: \(String(describing: cell.label))) was dropped rather \
                than demoted. Trading a wrong skip for a missed ad is a \
                different decision than the one this bead was given: a demoted \
                span keeps its banner.
                """
            )
        }
    }

    @Test("applying the gate twice changes nothing the first pass did not")
    func gateIsIdempotent() {
        for cell in Self.allCells {
            let once = HotPathExtentGate.gatedLabel(
                cell.label,
                extent: cell.extent,
                blockingUnanchoredAutoSkip: cell.blocking
            )
            let twice = HotPathExtentGate.gatedLabel(
                once,
                extent: cell.extent,
                blockingUnanchoredAutoSkip: cell.blocking
            )
            #expect(twice == once, "gate is not idempotent at \(String(describing: cell.label))")
        }
    }

    @Test("the block OFF is a no-op — the kill switch actually kills")
    func blockOffIsIdentity() {
        for label in Self.labels {
            for extent in Self.extents {
                #expect(
                    HotPathExtentGate.gatedLabel(
                        label,
                        extent: extent,
                        blockingUnanchoredAutoSkip: false
                    ) == label,
                    "with the block off the gate must return its input unchanged"
                )
            }
        }
    }

    // MARK: - 3. COUNT

    @Test("exactly 5 of the 180 cells change, and every one of them is a demotion")
    func exactlyTheExpectedCellsChange() {
        // The arithmetic, spelled out so a reader can check it against the
        // DOMAIN rather than against the implementation:
        //   anchor pairs                     3 × 3 = 9
        //   fully anchored ({byte, stinger}²)  2 × 2 = 4
        //   not fully anchored                 9 − 4 = 5
        //   changing cells   1 label ("autoSkip") × 5 extents × 1 flag = 5
        let anchoredPairs = Self.extents.filter(\.isFullyAnchored).count
        #expect(anchoredPairs == 4, "expected 4 fully-anchored anchor pairs; got \(anchoredPairs)")
        let expectedChanges = Self.extents.count - anchoredPairs   // 5

        var changed = 0
        for cell in Self.allCells {
            let after = HotPathExtentGate.gatedLabel(
                cell.label,
                extent: cell.extent,
                blockingUnanchoredAutoSkip: cell.blocking
            )
            guard after != cell.label else { continue }
            changed += 1
            #expect(
                cell.label == HotPathExtentGate.autoSkipLabel
                    && after == HotPathExtentGate.markOnlyLabel,
                """
                a cell changed that is not the intended transition: \
                \(String(describing: cell.label)) → \(String(describing: after))
                """
            )
        }
        #expect(
            changed == expectedChanges,
            """
            expected exactly \(expectedChanges) changing cells (autoSkip × the \
            \(expectedChanges) not-fully-anchored extents × the blocking flag); \
            got \(changed). A different count means the condition is wider or \
            narrower than the rule.
            """
        )
    }

    // MARK: - The admission ordinal itself

    @Test("admission maps each label to the tier the orchestrator actually gives it")
    func admissionMapping() {
        #expect(HotPathExtentGate.admission(of: nil) == .none)
        #expect(HotPathExtentGate.admission(of: HotPathExtentGate.autoSkipLabel) == .autoSkip)
        #expect(HotPathExtentGate.admission(of: HotPathExtentGate.markOnlyLabel) == .banner)
        // Unrecognised is `.none` because `receiveAdWindows` drops it as
        // `droppedMalformedEligibilityGate` — not because it is "unknown".
        #expect(HotPathExtentGate.admission(of: "wat") == .none)
        // And the ordinal is ordered the way the argument needs it to be.
        #expect(HotPathAdmission.none < .banner)
        #expect(HotPathAdmission.banner < .autoSkip)
    }

    @Test("the producer literals are the ones the consumers whitelist")
    func literalsMatchTheConsumerContract() {
        // `"autoSkip"` is NOT a `SkipEligibilityGate` raw value — it rides the
        // legacy nil-gate contract. playhead-y87g disproved the framing that
        // this is a bug; pinning it here keeps a future "tidy-up" from silently
        // mismatching every row already on a device.
        #expect(SkipEligibilityGate(rawValue: HotPathExtentGate.autoSkipLabel) == nil)
        // `"markOnly"` DOES round-trip, which is what routes it to the suggest
        // tier rather than the malformed drop.
        #expect(
            SkipEligibilityGate(rawValue: HotPathExtentGate.markOnlyLabel)
                == .markOnly
        )
    }

    // MARK: - The census detail

    @Test("the census detail fires only when it discriminates, and names the edges")
    func censusDetailNamesTheUnanchoredEdges() {
        #expect(
            HotPathExtentGate.censusDetail(for: .unanchored)
                == "unanchored_extent_start+end"
        )
        #expect(
            HotPathExtentGate.censusDetail(
                for: SpanExtentSupport(
                    startAnchor: .rediffByteExact,
                    endAnchor: .unanchored
                )
            ) == "unanchored_extent_end"
        )
        #expect(
            HotPathExtentGate.censusDetail(
                for: SpanExtentSupport(
                    startAnchor: .unanchored,
                    endAnchor: .stingerSnapped
                )
            ) == "unanchored_extent_start"
        )
        // Silent on the rows it has nothing to say about. A detail that fires
        // on every delivery is exactly as useless as one that never fires.
        for extent in Self.extents where extent.isFullyAnchored {
            #expect(
                HotPathExtentGate.censusDetail(for: extent) == nil,
                "a fully-anchored row must carry NO extent detail (\(extent))"
            )
        }
    }

    // MARK: - The flag this all hangs on

    @Test("the shipping config blocks unanchored auto-skip — one switch, both producers")
    func shippingConfigBlocksUnanchoredAutoSkip() {
        #expect(
            AdDetectionConfig.default.unanchoredExtentBlocksAutoSkip,
            """
            playhead-bllt reuses playhead-2350's flag deliberately, so the kill \
            switch cannot half-fire. If this ships false, the hot path is \
            ungated again and this whole suite is testing a code path nobody \
            runs.
            """
        )
    }
}
