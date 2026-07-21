// SpecialistMarkComposerTests.swift
// playhead-b6jq PR 5: pure-unit coverage for the specialist mark composer —
// τ=0.7 filter, merge-within-gap, 70%-overlap dedupe, the mark-only emit
// contract, and content-addressed id stability. No store, no actor, no model:
// synthetic `SpecialistScanResult` rows in, `AdWindow` marks out.

import Foundation
import Testing

@testable import Playhead

@Suite("SpecialistMarkComposer (playhead-b6jq PR5)")
struct SpecialistMarkComposerTests {

    // MARK: - Fixtures

    private func scanRow(
        start: Double,
        end: Double,
        p: Double,
        isAd: Bool? = nil,
        adClass: String? = "hostRead",
        asset: String = "asset-1"
    ) -> SpecialistScanResult {
        SpecialistScanResult(
            id: "spec-\(start)-\(end)",
            analysisAssetId: asset,
            windowStartTime: start,
            windowEndTime: end,
            probabilityOfAd: p,
            isAd: isAd ?? (p >= 0.5),
            adClass: adClass,
            modelVersion: "specialist-v2",
            detectorVersion: "detection-v1",
            transcriptVersion: "tx-1",
            scanCohortJSON: "{}",
            reuseKeyHash: "hash-\(start)-\(end)",
            jobPhase: "specialistHostReadScan",
            createdAt: 1000
        )
    }

    /// An existing NON-specialist visible mark (e.g. FM fusion) for dedupe tests.
    private func existingMark(
        id: String,
        start: Double,
        end: Double,
        detectorVersion: String = "detection-v1",
        decisionState: String = "candidate",
        asset: String = "asset-1"
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: asset,
            startTime: start,
            endTime: end,
            confidence: 0.9,
            boundaryState: "acousticRefined",
            decisionState: decisionState,
            detectorVersion: detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: "eligible"
        )
    }

    // MARK: - (1) τ boundary

    @Test("τ filter: 0.69 dropped, 0.70 kept, 0.71 kept")
    func tauBoundary() {
        let rows = [
            scanRow(start: 0, end: 20, p: 0.69),
            scanRow(start: 100, end: 120, p: 0.70),
            scanRow(start: 200, end: 220, p: 0.71)
        ]
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: "asset-1"
        )
        // 0.70 and 0.71 survive as distinct (non-adjacent) marks; 0.69 dropped.
        #expect(marks.count == 2)
        let starts = Set(marks.map(\.startTime))
        #expect(starts == [100, 200])
        #expect(SpecialistMarkComposer.tau == 0.70)
    }

    @Test("τ filter: all sub-τ rows → no marks")
    func allBelowTau() {
        let rows = [scanRow(start: 0, end: 20, p: 0.5), scanRow(start: 30, end: 50, p: 0.699)]
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: "asset-1"
        )
        #expect(marks.isEmpty)
    }

    // MARK: - (2) merge

    @Test("merge: two adjacent ~25s windows within gap → one span, union bounds, confidence = max")
    func mergeAdjacent() {
        // 0..25 and 25..50 are exactly adjacent (gap 0 <= mergeGap).
        let rows = [
            scanRow(start: 0, end: 25, p: 0.75, adClass: "hostRead"),
            scanRow(start: 25, end: 50, p: 0.92, adClass: "dai")
        ]
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: "asset-1"
        )
        #expect(marks.count == 1)
        let m = marks[0]
        #expect(m.startTime == 0)
        #expect(m.endTime == 50)
        // confidence = max (never averaged down).
        #expect(m.confidence == 0.92)
    }

    @Test("merge: gap larger than mergeGap → two separate spans")
    func mergeRespectsGap() {
        // 0..20 then 30..50: gap of 10 > mergeGap(2.0) → not merged.
        let rows = [
            scanRow(start: 0, end: 20, p: 0.8),
            scanRow(start: 30, end: 50, p: 0.85)
        ]
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: "asset-1"
        ).sorted { $0.startTime < $1.startTime }
        #expect(marks.count == 2)
        #expect(marks[0].startTime == 0 && marks[0].endTime == 20)
        #expect(marks[1].startTime == 30 && marks[1].endTime == 50)
    }

    @Test("merge: within-gap (1.5s < 2.0s) stitches into one span")
    func mergeWithinGap() {
        let rows = [
            scanRow(start: 0, end: 20, p: 0.8),
            scanRow(start: 21.5, end: 40, p: 0.8)  // gap 1.5 <= 2.0
        ]
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: "asset-1"
        )
        #expect(marks.count == 1)
        #expect(marks[0].startTime == 0 && marks[0].endTime == 40)
    }

    // MARK: - (3) dedupe

    @Test("dedupe: 69%-covered span survives")
    func dedupeSixtyNinePercentSurvives() {
        // span 0..100 (duration 100); existing covers 0..69 → 69% < 70% → survive.
        let rows = [scanRow(start: 0, end: 100, p: 0.9)]
        let existing = [existingMark(id: "fm-1", start: 0, end: 69)]
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: existing, analysisAssetId: "asset-1"
        )
        #expect(marks.count == 1)
    }

    @Test("dedupe: 70%-covered span dropped (boundary), 90% dropped")
    func dedupeAtThresholdAndAbove() {
        let rowsSeventy = [scanRow(start: 0, end: 100, p: 0.9)]
        let existingSeventy = [existingMark(id: "fm-1", start: 0, end: 70)]
        #expect(
            SpecialistMarkComposer.compose(
                scanRows: rowsSeventy, existingWindows: existingSeventy, analysisAssetId: "asset-1"
            ).isEmpty,
            "exactly 70% covered must be dropped (>= threshold)"
        )

        let rowsNinety = [scanRow(start: 0, end: 100, p: 0.9)]
        let existingNinety = [existingMark(id: "fm-2", start: 5, end: 95)]  // 90 of 100
        #expect(
            SpecialistMarkComposer.compose(
                scanRows: rowsNinety, existingWindows: existingNinety, analysisAssetId: "asset-1"
            ).isEmpty
        )
    }

    @Test("dedupe: union of two partial existing marks summing ≥70% drops the span")
    func dedupeUnionOfTwo() {
        // span 0..100; existing 0..40 and 45..85 → union 40+40 = 80 ≥ 70 → drop.
        let rows = [scanRow(start: 0, end: 100, p: 0.9)]
        let existing = [
            existingMark(id: "fm-a", start: 0, end: 40),
            existingMark(id: "fm-b", start: 45, end: 85)
        ]
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: existing, analysisAssetId: "asset-1"
        )
        #expect(marks.isEmpty)
    }

    @Test("dedupe: only vs NON-specialist visible marks — a prior specialist row does not self-suppress")
    func dedupeIgnoresPriorSpecialistRow() {
        let rows = [scanRow(start: 0, end: 100, p: 0.9)]
        // A prior specialist mark fully covering the span must NOT suppress it
        // (idempotency rides on content-addressed ids + version-scoped reconcile).
        let priorSpecialist = existingMark(
            id: "specialist-abc", start: 0, end: 100,
            detectorVersion: SpecialistMarkComposer.detectorVersion
        )
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [priorSpecialist], analysisAssetId: "asset-1"
        )
        #expect(marks.count == 1, "prior specialist row must not suppress a re-composed span")
    }

    @Test("dedupe: a SUPPRESSED non-specialist mark does not suppress the span")
    func dedupeIgnoresSuppressedExisting() {
        let rows = [scanRow(start: 0, end: 100, p: 0.9)]
        let suppressed = existingMark(id: "fm-supp", start: 0, end: 100, decisionState: "suppressed")
        let marks = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [suppressed], analysisAssetId: "asset-1"
        )
        #expect(marks.count == 1, "a non-visible (suppressed) mark must not count toward coverage")
    }

    // MARK: - (4) mark-only emit contract (matrix)

    @Test("emit contract: every mark is markOnly + candidate + specialist provenance, across the input matrix")
    func emitContractHolds() {
        // (probabilityOfAd, isAd, adClass) — no combination may yield a gate/state
        // other than markOnly/candidate.
        let matrix: [(p: Double, isAd: Bool, adClass: String?)] = [
            (0.70, true, "hostRead"),
            (0.95, true, "dai"),
            (0.72, false, nil),
            (1.0, true, "hostRead")
        ]
        for row in matrix {
            let rows = [scanRow(start: 10, end: 40, p: row.p, isAd: row.isAd, adClass: row.adClass)]
            let marks = SpecialistMarkComposer.compose(
                scanRows: rows, existingWindows: [], analysisAssetId: "asset-1"
            )
            #expect(marks.count == 1)
            let m = marks[0]
            #expect(m.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
                    "eligibilityGate MUST be markOnly regardless of P/isAd/adClass; got \(String(describing: m.eligibilityGate))")
            #expect(m.decisionState == AdDecisionState.candidate.rawValue,
                    "decisionState MUST be candidate, never confirmed/applied")
            #expect(m.detectorVersion == SpecialistMarkComposer.detectorVersion)
            #expect(m.metadataSource == SpecialistMarkComposer.metadataSource)
            #expect(m.boundaryState == SpecialistMarkComposer.boundaryState)
            #expect(m.metadataConfidence == nil, "nil → generic no-hallucination copy")
            #expect(m.advertiser == nil && m.product == nil && m.adDescription == nil)
            #expect(m.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(m.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(m.confidence == row.p, "confidence carries the τ-passing probabilityOfAd")
            #expect(m.confidence >= SpecialistMarkComposer.tau, "τ-passing → auto-clears preload floor")
        }
    }

    @Test("emit contract: boundaryState is NOT a reconcile-protected state")
    func boundaryStateNotProtected() {
        // Axis test (blueprint §9): "specialistScan" must stay OUT of the
        // protected set so the specialist reconcile can retire its own stale rows.
        #expect(
            !AdDetectionService.reconcileProtectedBoundaryStates.contains(
                SpecialistMarkComposer.boundaryState
            )
        )
    }

    // MARK: - (6) content-addressed id

    @Test("id: content-addressed, `specialist-` prefix, stable across recompose")
    func idStableAndPrefixed() {
        let rows = [scanRow(start: 12.5, end: 37.5, p: 0.8)]
        let first = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: "asset-9"
        )
        let second = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: "asset-9"
        )
        #expect(first.count == 1 && second.count == 1)
        #expect(first[0].id == second[0].id, "identical inputs → identical id (idempotency)")
        #expect(first[0].id.hasPrefix("specialist-"))
        // Distinct assets → distinct ids.
        let other = SpecialistMarkComposer.compose(
            scanRows: rows, existingWindows: [], analysisAssetId: "asset-OTHER"
        )
        #expect(other[0].id != first[0].id)
    }

    // MARK: - unionLength / coveredFraction helpers

    @Test("unionLength counts overlaps once")
    func unionLengthOverlap() {
        #expect(SpecialistMarkComposer.unionLength([(0, 10), (5, 15)]) == 15)
        #expect(SpecialistMarkComposer.unionLength([(0, 10), (20, 30)]) == 20)
        #expect(SpecialistMarkComposer.unionLength([]) == 0)
    }
}
