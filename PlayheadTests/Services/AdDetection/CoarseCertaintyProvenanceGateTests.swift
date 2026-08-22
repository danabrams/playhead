// CoarseCertaintyProvenanceGateTests.swift
// playhead-iw7q: the COARSE-side certainty gate, and the asymmetry that makes
// it a different rule from the refined-side one playhead-92im already shipped.
//
// THE TWO SHAPES ARE NOT SYMMETRICAL, and that is the whole design.
//
//   * A `passB` payload is an ARRAY of refined spans and each span carries
//     `ownershipInferenceWasSuppressed` — the discriminator travels WITH the
//     value it qualifies, so it reads correctly on a row of any vintage.
//   * A `passA` payload is ONE `CoarseSupportSchema` object with exactly two
//     fields, neither of which is a provenance. There is nowhere in the payload
//     to put one, which is why closing this half needed a COLUMN
//     (`semantic_scan_results.usedPermissiveFallback`, schema V61) and not a
//     read.
//
// So the coarse gate asks the ROW and the refined gate asks the SPAN, and the
// tests below pin BOTH directions of that: a `.unknown` row must NOT veto a
// refined payload (its spans speak for themselves, and vetoing would silently
// re-grade every refinement written before V61), while a `.unknown` row MUST
// veto a coarse one (nothing else can speak for it).
//
// UNKNOWN IS NOT ZERO. `.permissive` and `.unknown` return the same band —
// `nil` — for two different reasons: one is "the runner graded this" and the
// other is "nobody recorded who graded this". They agree on what may be SPENT
// and disagree on what is KNOWN, which is why both states exist on the row and
// are collapsed only here.

import Foundation
import Testing

@testable import Playhead

@Suite("a coarse band is the MODEL'S only when the row says so (playhead-iw7q)")
struct CoarseCertaintyProvenanceGateTests {

    // MARK: - Fixtures

    /// The `passA` shape `BackfillJobRunner.encodeSupport` writes. Verbatim from
    /// the 2026-08-19 t4 pull's `CD2976E6` [1131.6–1210.9] row.
    private static func coarsePayload(_ certainty: CertaintyBand) -> String {
        #"{"supportLineRefs":[46],"certainty":"\#(certainty.rawValue)"}"#
    }

    /// The `passB` shape `encodeRefinedSpans` writes.
    private static func refinedPayload(
        _ certainty: CertaintyBand,
        ownershipInferenceWasSuppressed: Bool
    ) -> String {
        let flag = ownershipInferenceWasSuppressed ? "true" : "false"
        return "[" + #"{"anchors":[],"certainty":"\#(certainty.rawValue)","#
            + #""commercialIntent":"paid","firstLineRef":2,"lastLineRef":2,"#
            + #""ownership":"thirdParty","ownershipInferenceWasSuppressed":\#(flag)}"# + "]"
    }

    private static func row(
        spansJSON: String,
        provenance: ScanVerdictProvenance,
        scanPass: String = "passA",
        transcriptQuality: TranscriptQuality = .good
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "scan-gate",
            analysisAssetId: "asset-gate",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: 100,
            windowEndTime: 160,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: .containsAd,
            spansJSON: spansJSON,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            scanCohortJSON: makeCohortJSON(promptLabel: "iw7q"),
            transcriptVersion: "tv-1",
            verdictProvenance: provenance
        )
    }

    // MARK: - 1. The coarse gate

    @Test("a MODEL coarse row keeps its band, at every rung of the ladder")
    func modelRowKeepsItsBand() {
        for band in [CertaintyBand.strong, .moderate, .weak] {
            let row = Self.row(spansJSON: Self.coarsePayload(band), provenance: .model)
            #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == band)
            #expect(SemanticSweepMarkComposer.certaintyFactor(of: row)
                    == SemanticSweepMarkComposer.certaintyFactor(band))
        }
    }

    @Test("a PERMISSIVE coarse row is ungraded — the runner wrote that .strong")
    func permissiveRowIsUngraded() {
        let row = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .permissive)
        #expect(row.spansJSON.contains("strong"), "the payload still SAYS strong")
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: row) == 0.5)
    }

    @Test("an UNKNOWN coarse row is ungraded too — silence is not a licence")
    func unknownRowIsUngraded() {
        let row = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .unknown)
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: row) == 0.5)
    }

    /// The direction that would have been shipped by a `DEFAULT 0` backfill:
    /// `.unknown` reading as `.model`. Pinned as an INEQUALITY against the
    /// `.model` row so a mutant that collapses the two is killed by a rail
    /// whose failure message names the confusion.
    @Test("unknown does NOT read as model, on rows that are otherwise identical")
    func unknownIsNotModel() {
        let known = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .model)
        let unknown = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .unknown)
        #expect(known.spansJSON == unknown.spansJSON, "byte-identical payloads")
        #expect(SemanticSweepMarkComposer.certaintyBand(of: known)
                != SemanticSweepMarkComposer.certaintyBand(of: unknown))
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: known) == 1.0)
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: unknown) == 0.5)
    }

    @Test("a coarse row with NO support payload is ungraded whatever its provenance")
    func emptyPayloadIsUngradedForEveryProvenance() {
        for provenance in ScanVerdictProvenance.allCases {
            let row = Self.row(spansJSON: "[]", provenance: provenance)
            #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
        }
    }

    // MARK: - 2. The refined side keeps ITS rule, and the row does not override it

    @Test("a refined payload on an UNKNOWN row keeps its band — the SPAN carries the flag")
    func unknownRowDoesNotVetoARefinedPayload() {
        // This is the asymmetry. Every `passB` row on disk today reads
        // `.unknown`, and vetoing here would silently re-grade all of them
        // while discarding a discriminator the payload actually holds.
        let row = Self.row(
            spansJSON: Self.refinedPayload(.strong, ownershipInferenceWasSuppressed: false),
            provenance: .unknown,
            scanPass: "passB"
        )
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == .strong)
    }

    @Test("playhead-92im's span gate is unchanged: a suppressed span is ungraded on a MODEL row")
    func suppressedSpanStaysUngraded() {
        let row = Self.row(
            spansJSON: Self.refinedPayload(.strong, ownershipInferenceWasSuppressed: true),
            provenance: .model,
            scanPass: "passB"
        )
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
    }

    @Test("a refined payload on a PERMISSIVE row is vetoed — that is a CLAIM, not an absence")
    func permissiveRowVetoesARefinedPayload() {
        let row = Self.row(
            spansJSON: Self.refinedPayload(.strong, ownershipInferenceWasSuppressed: false),
            provenance: .permissive,
            scanPass: "passB"
        )
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
    }

    // MARK: - 3. The consequence a mark actually carries

    @Test("the mark a pre-V61 coarse row backs grades at the FLOOR, not the ceiling")
    func markConfidenceFallsToTheFloor() {
        let unknown = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .unknown)
        let model = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .model)

        let unknownConfidence = SemanticSweepMarkComposer.markConfidence(
            certaintyFactor: SemanticSweepMarkComposer.certaintyFactor(of: unknown),
            transcriptQuality: .good,
            affirming: 1,
            dissenting: 0
        )
        let modelConfidence = SemanticSweepMarkComposer.markConfidence(
            certaintyFactor: SemanticSweepMarkComposer.certaintyFactor(of: model),
            transcriptQuality: .good,
            affirming: 1,
            dissenting: 0
        )
        // 0.700 -> 0.350 is the median move measured over the 2026-08-21 t6
        // pull: 113 of 125 recomposed extents, every one of them DOWNWARD.
        #expect(abs(modelConfidence - SemanticSweepMarkComposer.maximumMarkConfidence) < 1e-12)
        #expect(abs(unknownConfidence - SemanticSweepMarkComposer.unevidencedMarkConfidence) < 1e-12)
        #expect(unknownConfidence < modelConfidence, "the direction is DOWN, always")
    }

    /// The whole change is MONOTONE NON-INCREASING, which is what makes it safe
    /// to ship against a population nobody can attribute: no row can come out of
    /// this gate stronger than it went in.
    @Test("the gate can only ever DEDUCT — no provenance raises a band")
    func gateIsMonotoneNonIncreasing() {
        for band in [CertaintyBand.strong, .moderate, .weak] {
            let ungatedFactor = SemanticSweepMarkComposer.certaintyFactor(band)
            for provenance in ScanVerdictProvenance.allCases {
                let row = Self.row(spansJSON: Self.coarsePayload(band), provenance: provenance)
                #expect(SemanticSweepMarkComposer.certaintyFactor(of: row) <= ungatedFactor)
            }
        }
    }
}
