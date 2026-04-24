// CorrectionNormalizerTests.swift
// playhead-gtt9.7: Unit tests for the correction-normalizer that runs before
// the NARL harness consumes corrections. The normalizer sorts each raw row
// into one of four buckets (wholeAsset / spanFN / spanFP / unknown), merges
// adjacent FN/FP spans on the same asset (gap ≤ 5 s), and deduplicates.
//
// Classification heuristic (documented in CorrectionNormalizer.swift):
//   1. If scope parses as `.wholeAssetVeto` (exactSpan:<aid>:0:INT64_MAX) →
//      wholeAsset. Sub-kind (veto vs endorse) derived from correctionType.
//   2. Else if scope is `.exactTimeSpan` with valid start < end:
//        • correctionType == "falseNegative" → spanFN
//        • correctionType == "falsePositive" → spanFP
//        • correctionType == "startTooEarly/Late"/"endTooEarly/Late" → boundary (ignored)
//        • correctionType nil/unrecognized → source heuristic:
//          "manualVeto"/"listenRevert" → spanFP, "falseNegative" → spanFN,
//          everything else → unknown.
//   3. Else if scope is `.exactSpan` (ordinal-range) — harness-side the
//      normalizer can't resolve atoms, so these are routed to unknown and
//      the harness's existing NarlGroundTruth pipeline handles them downstream
//      when atoms are available.
//   4. Else (unhandled scope) → unknown.
//
// Fixture style matches NarlGroundTruthTests (Swift Testing, hand-built
// FrozenCorrection arrays — no I/O).

import Foundation
import Testing
@testable import Playhead

@Suite("CorrectionNormalizer – classification")
struct CorrectionNormalizerClassificationTests {

    // MARK: - helpers

    private func fc(
        source: String,
        scope: String,
        correctionType: String? = nil,
        createdAt: Double = 1_000
    ) -> FrozenTrace.FrozenCorrection {
        FrozenTrace.FrozenCorrection(
            source: source,
            scope: scope,
            createdAt: createdAt,
            correctionType: correctionType
        )
    }

    // MARK: - wholeAsset (both directions)

    @Test("whole-asset manualVeto (correctionType=falsePositive) is classified as wholeAsset veto")
    func wholeAssetVetoClassified() {
        let corrections = [
            fc(source: "manualVeto",
               scope: "exactSpan:asset-1:0:9223372036854775807",
               correctionType: "falsePositive")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.wholeAssetCorrections.count == 1)
        #expect(norm.wholeAssetCorrections.first?.assetId == "asset-1")
        #expect(norm.wholeAssetCorrections.first?.kind == .veto)
        #expect(norm.spanFN.isEmpty)
        #expect(norm.spanFP.isEmpty)
        #expect(norm.unknownCount == 0)
    }

    @Test("whole-asset endorsement (correctionType=falseNegative) is classified as wholeAsset endorse")
    func wholeAssetEndorseClassified() {
        let corrections = [
            fc(source: "falseNegative",
               scope: "exactSpan:asset-1:0:9223372036854775807",
               correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.wholeAssetCorrections.count == 1)
        #expect(norm.wholeAssetCorrections.first?.kind == .endorse)
        #expect(norm.spanFN.isEmpty)
        #expect(norm.spanFP.isEmpty)
    }

    @Test("whole-asset with null correctionType but manualVeto source is veto")
    func wholeAssetNullTypeVetoSource() {
        let corrections = [
            fc(source: "manualVeto",
               scope: "exactSpan:asset-1:0:9223372036854775807",
               correctionType: nil)
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.wholeAssetCorrections.count == 1)
        #expect(norm.wholeAssetCorrections.first?.kind == .veto)
    }

    @Test("N5: whole-asset with null correctionType AND unknown source routes to unknown (not fabricated veto)")
    func wholeAssetUnknownKindRoutesToUnknown() {
        // Pre-N5 behavior: silently fabricated a `.veto` bucket entry.
        // Post-N5: unknown-kind rows are counted as unknown, so an
        // operator sees them surface in report diagnostics instead of
        // disappearing into the veto count.
        let corrections = [
            fc(source: "someFutureSource",
               scope: "exactSpan:asset-1:0:9223372036854775807",
               correctionType: nil)
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.wholeAssetCorrections.isEmpty)
        #expect(norm.unknownCount == 1)
    }

    // MARK: - spanFN classification

    @Test("exactTimeSpan correction with correctionType=falseNegative is spanFN")
    func spanFNClassified() {
        let corrections = [
            fc(source: "falseNegative",
               scope: "exactTimeSpan:asset-1:100.000:160.000",
               correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.spanFN.count == 1)
        #expect(norm.spanFN.first?.assetId == "asset-1")
        #expect(norm.spanFN.first?.range == NarlTimeRange(start: 100, end: 160))
        #expect(norm.spanFP.isEmpty)
        #expect(norm.wholeAssetCorrections.isEmpty)
    }

    // MARK: - spanFP classification

    @Test("exactTimeSpan correction with correctionType=falsePositive is spanFP")
    func spanFPClassified() {
        let corrections = [
            fc(source: "manualVeto",
               scope: "exactTimeSpan:asset-1:77.100:85.020",
               correctionType: "falsePositive")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.spanFP.count == 1)
        #expect(norm.spanFP.first?.assetId == "asset-1")
        #expect(norm.spanFP.first?.range == NarlTimeRange(start: 77.1, end: 85.02))
        #expect(norm.spanFN.isEmpty)
        #expect(norm.wholeAssetCorrections.isEmpty)
    }

    // MARK: - unknown

    @Test("Ambiguous row with unknown source and nil correctionType is unknown")
    func ambiguousRowIsUnknown() {
        let corrections = [
            fc(source: "someFutureCorrection",
               scope: "exactTimeSpan:asset-1:100.000:160.000",
               correctionType: nil)
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.unknownCount == 1)
        #expect(norm.spanFN.isEmpty)
        #expect(norm.spanFP.isEmpty)
        #expect(norm.wholeAssetCorrections.isEmpty)
    }

    @Test("Ordinal exactSpan (non-whole-asset) is routed to unknown")
    func ordinalSpanIsUnknown() {
        // Harness-side normalizer can't resolve atoms; existing
        // NarlGroundTruth handles ordinal spans downstream with atoms.
        let corrections = [
            fc(source: "falseNegative",
               scope: "exactSpan:asset-1:3:7",
               correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.unknownCount == 1)
        #expect(norm.spanFN.isEmpty)
    }

    @Test("Boundary-refinement correctionType on a span scope does not become spanFN/FP")
    func boundaryRefinementExcludedFromSpans() {
        let corrections = [
            fc(source: "listenRevert",
               scope: "exactTimeSpan:asset-1:120.000:180.000",
               correctionType: "startTooEarly")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.spanFN.isEmpty)
        #expect(norm.spanFP.isEmpty)
        #expect(norm.boundaryRefinementCount == 1)
        #expect(norm.unknownCount == 0)
    }

    @Test("Truly malformed scope (garbage prefix) counts as unknown")
    func garbagePrefixScopeIsUnknown() {
        // Scope doesn't match any recognized prefix — this is the malformed
        // case (unlike Layer B scopes below, which are production-valid).
        let corrections = [
            fc(source: "falseNegative",
               scope: "notARealScope:blah:blah",
               correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.unknownCount == 1)
        #expect(norm.layerBCount == 0)
    }

    // MARK: - Layer B scopes (S1: these are production-valid, not malformed)

    @Test("sponsorOnShow routes to layerBCount, not unknownCount")
    func sponsorOnShowIsLayerB() {
        let corrections = [
            fc(source: "falseNegative",
               scope: "sponsorOnShow:p1:acme",
               correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.layerBCount == 1)
        #expect(norm.unknownCount == 0)
        #expect(norm.spanFN.isEmpty)
        #expect(norm.spanFP.isEmpty)
        #expect(norm.wholeAssetCorrections.isEmpty)
    }

    @Test("phraseOnShow routes to layerBCount")
    func phraseOnShowIsLayerB() {
        let corrections = [
            fc(source: "manualVeto",
               scope: "phraseOnShow:p1:my-phrase",
               correctionType: "falsePositive")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.layerBCount == 1)
        #expect(norm.unknownCount == 0)
    }

    @Test("campaignOnShow routes to layerBCount")
    func campaignOnShowIsLayerB() {
        let corrections = [
            fc(source: "manualVeto",
               scope: "campaignOnShow:p1:camp-2025",
               correctionType: "falsePositive")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.layerBCount == 1)
        #expect(norm.unknownCount == 0)
    }

    @Test("domainOwnershipOnShow routes to layerBCount")
    func domainOwnershipOnShowIsLayerB() {
        let corrections = [
            fc(source: "manualVeto",
               scope: "domainOwnershipOnShow:p1:nytimes.com",
               correctionType: "falsePositive")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.layerBCount == 1)
        #expect(norm.unknownCount == 0)
    }

    @Test("jingleOnShow routes to layerBCount")
    func jingleOnShowIsLayerB() {
        let corrections = [
            fc(source: "manualVeto",
               scope: "jingleOnShow:p1:jingle-abc",
               correctionType: "falsePositive")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.layerBCount == 1)
        #expect(norm.unknownCount == 0)
    }
}

@Suite("CorrectionNormalizer – merging adjacent spans")
struct CorrectionNormalizerMergeTests {

    private func fc(
        scope: String,
        correctionType: String
    ) -> FrozenTrace.FrozenCorrection {
        FrozenTrace.FrozenCorrection(
            source: correctionType == "falseNegative" ? "falseNegative" : "manualVeto",
            scope: scope,
            createdAt: 1_000,
            correctionType: correctionType
        )
    }

    @Test("Two adjacent FN spans (gap ≤ 5 s) on same asset merge into one")
    func adjacentFNSpansMerge() {
        // [100, 150] and [153, 200] — gap is 3s.
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative"),
            fc(scope: "exactTimeSpan:asset-1:153.000:200.000", correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.spanFN.count == 1)
        #expect(norm.spanFN.first?.range == NarlTimeRange(start: 100, end: 200))
    }

    @Test("Two FN spans with gap > 5 s do NOT merge")
    func nonAdjacentFNSpansStaySeparate() {
        // [100, 150] and [160, 200] — gap is 10s.
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative"),
            fc(scope: "exactTimeSpan:asset-1:160.000:200.000", correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)

        #expect(norm.spanFN.count == 2)
    }

    @Test("FN and FP with overlapping time do not merge across kinds")
    func mixedKindsDoNotCrossMerge() {
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative"),
            fc(scope: "exactTimeSpan:asset-1:148.000:200.000", correctionType: "falsePositive")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.spanFN.count == 1)
        #expect(norm.spanFP.count == 1)
    }

    @Test("Adjacent FP spans merge under the same 5 s rule")
    func adjacentFPSpansMerge() {
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:10.000:20.000", correctionType: "falsePositive"),
            fc(scope: "exactTimeSpan:asset-1:22.000:30.000", correctionType: "falsePositive")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.spanFP.count == 1)
        #expect(norm.spanFP.first?.range == NarlTimeRange(start: 10, end: 30))
    }

    @Test("Merging is scoped per asset (same-kind spans on different assets stay separate)")
    func mergeIsPerAsset() {
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative"),
            // exact same time range on a different asset — must stay separate
            fc(scope: "exactTimeSpan:asset-2:100.000:150.000", correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.spanFN.count == 2)
    }

    // MARK: - Boundary + shape pins (S2, S4, S5 from 2026-04-23 review)

    @Test("S2: Gap exactly 5.0 s between same-kind spans merges (≤ boundary pin)")
    func gapExactlyFiveSecondsMerges() {
        // [100, 150] and [155, 200] — gap is exactly 5.0s.
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative"),
            fc(scope: "exactTimeSpan:asset-1:155.000:200.000", correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.spanFN.count == 1)
        #expect(norm.spanFN.first?.range == NarlTimeRange(start: 100, end: 200))
    }

    @Test("S4: Out-of-order input (later-start span first) still merges correctly")
    func outOfOrderInputMerges() {
        // Note reversed order on input — [153, 200] comes first, [100, 150] second.
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:153.000:200.000", correctionType: "falseNegative"),
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.spanFN.count == 1)
        #expect(norm.spanFN.first?.range == NarlTimeRange(start: 100, end: 200))
    }

    @Test("S5: Containment — one span fully inside another — merges to the outer range")
    func containmentMergesToOuter() {
        // [100, 200] contains [130, 170].
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:100.000:200.000", correctionType: "falseNegative"),
            fc(scope: "exactTimeSpan:asset-1:130.000:170.000", correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.spanFN.count == 1)
        #expect(norm.spanFN.first?.range == NarlTimeRange(start: 100, end: 200))
    }
}

@Suite("CorrectionNormalizer – deduplication")
struct CorrectionNormalizerDedupTests {

    private func fc(
        scope: String,
        correctionType: String,
        createdAt: Double = 1_000
    ) -> FrozenTrace.FrozenCorrection {
        FrozenTrace.FrozenCorrection(
            source: correctionType == "falseNegative" ? "falseNegative" : "manualVeto",
            scope: scope,
            createdAt: createdAt,
            correctionType: correctionType
        )
    }

    @Test("Exact-duplicate span corrections deduplicate")
    func exactDuplicateSpanDedup() {
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative"),
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative",
               createdAt: 2_000)  // different timestamp, same span
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.spanFN.count == 1)
    }

    @Test("Near-duplicate span corrections within ±1 s edges deduplicate")
    func nearDuplicateSpanDedup() {
        let corrections = [
            fc(scope: "exactTimeSpan:asset-1:100.000:150.000", correctionType: "falseNegative"),
            // Same intended span, both edges within 1s
            fc(scope: "exactTimeSpan:asset-1:100.500:150.700", correctionType: "falseNegative")
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.spanFN.count == 1)
    }

    @Test("Duplicate whole-asset vetoes collapse to one")
    func duplicateWholeAssetDedup() {
        let corrections = [
            FrozenTrace.FrozenCorrection(
                source: "manualVeto",
                scope: "exactSpan:asset-1:0:9223372036854775807",
                createdAt: 1_000,
                correctionType: "falsePositive"
            ),
            FrozenTrace.FrozenCorrection(
                source: "manualVeto",
                scope: "exactSpan:asset-1:0:9223372036854775807",
                createdAt: 2_000,
                correctionType: "falsePositive"
            )
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.wholeAssetCorrections.count == 1)
    }

    @Test("S3: Whole-asset veto and endorse on the same assetId do NOT dedupe each other")
    func wholeAssetVetoAndEndorseStaySeparate() {
        let corrections = [
            FrozenTrace.FrozenCorrection(
                source: "manualVeto",
                scope: "exactSpan:asset-1:0:9223372036854775807",
                createdAt: 1_000,
                correctionType: "falsePositive"  // → veto
            ),
            FrozenTrace.FrozenCorrection(
                source: "falseNegative",
                scope: "exactSpan:asset-1:0:9223372036854775807",
                createdAt: 2_000,
                correctionType: "falseNegative"  // → endorse
            )
        ]
        let norm = CorrectionNormalizer.normalize(corrections)
        #expect(norm.wholeAssetCorrections.count == 2)
        #expect(norm.wholeAssetCorrections.contains { $0.kind == .veto })
        #expect(norm.wholeAssetCorrections.contains { $0.kind == .endorse })
    }
}

@Suite("CorrectionNormalizer – routing into ground-truth build")
struct CorrectionNormalizerRoutingTests {

    @Test("wholeAsset rows do NOT appear in span-level counts from normalized stream")
    func wholeAssetRoutedSeparately() {
        // The real-data pathology: mostly whole-asset vetoes mixed in with a
        // single span-level FP. After normalization, span-level counts
        // should reflect only the one span, and the whole-asset corrections
        // should be in the wholeAsset bucket.
        let raw: [FrozenTrace.FrozenCorrection] = [
            // Nine whole-asset manualVetoes on asset-1
        ] + (0..<9).map { i in
            FrozenTrace.FrozenCorrection(
                source: "manualVeto",
                scope: "exactSpan:asset-1:0:9223372036854775807",
                createdAt: 1_000 + Double(i),
                correctionType: "falsePositive"
            )
        } + [
            // One genuine span-level FP on asset-2
            FrozenTrace.FrozenCorrection(
                source: "manualVeto",
                scope: "exactTimeSpan:asset-2:77.100:85.020",
                createdAt: 2_000,
                correctionType: "falsePositive"
            )
        ]

        let norm = CorrectionNormalizer.normalize(raw)

        // After dedup, the nine whole-asset rows on one asset collapse.
        #expect(norm.wholeAssetCorrections.count == 1)
        #expect(norm.wholeAssetCorrections.first?.assetId == "asset-1")

        // Exactly one span-level FP, and it's on the other asset.
        #expect(norm.spanFP.count == 1)
        #expect(norm.spanFP.first?.assetId == "asset-2")

        // No span-level FN.
        #expect(norm.spanFN.isEmpty)
    }

    @Test("Span-level counts used by harness exclude wholeAsset rows (integration)")
    func harnessConsumesNormalizedSpanCounts() {
        // This test asserts the invariant the harness depends on: given a
        // mix of whole-asset and span-level corrections on one asset, the
        // span-level FP count the harness sees after normalization equals
        // the number of genuine span-level FP corrections (1), NOT the
        // total falsePositive rows (4).
        let raw: [FrozenTrace.FrozenCorrection] =
            (0..<3).map { i in
                FrozenTrace.FrozenCorrection(
                    source: "manualVeto",
                    scope: "exactSpan:asset-1:0:9223372036854775807",
                    createdAt: 1_000 + Double(i),
                    correctionType: "falsePositive"
                )
            } + [
                FrozenTrace.FrozenCorrection(
                    source: "manualVeto",
                    scope: "exactTimeSpan:asset-1:120.000:150.000",
                    createdAt: 2_000,
                    correctionType: "falsePositive"
                )
            ]

        let norm = CorrectionNormalizer.normalize(raw)
        #expect(norm.spanFP.count == 1)
        // Whole-asset corrections are counted independently.
        #expect(norm.wholeAssetCorrections.count == 1)
    }
}
