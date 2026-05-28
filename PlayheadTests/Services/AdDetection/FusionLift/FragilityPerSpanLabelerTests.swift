// FragilityPerSpanLabelerTests.swift
// playhead-xsdz.7 Part A — hermetic, SYNTHETIC unit tests for the per-span
// fragility DIAGNOSTIC join: `FragilityPerSpanLabeler` and the
// `FragilityPerSpanDiagnosticReport` group summary. No audio, no Foundation
// Models, no live pipeline — every input is a hand-built value, so these run on
// the simulator in the default `PlayheadFastTests` plan.
//
// The LOAD-BEARING correctness property: the per-span TP/FP/correctly-rejected
// label MUST be assigned by the SAME greedy-IoU pairing the metrics scorer uses
// (`MetricsBatch.pair` over the SAME bridged GT + skip-eligible detections),
// joined back to each diagnostic span by its `(start, end)` boundaries — which
// the persisted fusion `AdWindow` inherits VERBATIM from the decoded span. If
// the labeling disagreed with the scorer, the diagnostic's FP-vs-TP fragility
// contrast would be measuring a different partition than the sweep's metrics,
// making the whole diagnostic untrustworthy. These tests pin that agreement.

import Foundation
import Testing
@testable import Playhead

@Suite("Per-span fragility diagnostic labeler (xsdz.7 Part A)")
struct FragilityPerSpanLabelerTests {

    // MARK: - Fixtures

    private static func annotationWindow(start: Double, end: Double) -> CorpusAnnotation.AdWindow {
        CorpusAnnotation.AdWindow(
            startSeconds: start,
            endSeconds: end,
            advertiser: "Acme",
            product: "Widget",
            adType: .hostRead,
            transitionType: .explicit,
            confidenceNotes: nil
        )
    }

    /// A persisted fusion `AdWindow` whose `(startTime, endTime)` equal a decoded
    /// span's boundaries (exactly how `buildFusionAdWindow` builds it). A fresh
    /// UUID id is used on purpose — the labeler must NOT key on the id.
    private static func storeAdWindow(
        start: Double,
        end: Double,
        confidence: Double = 0.9,
        decisionState: String = AdDecisionState.confirmed.rawValue
    ) -> AdWindow {
        AdWindow(
            id: UUID().uuidString,
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState,
            detectorVersion: "test-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    private static func diag(
        spanId: String,
        start: Double,
        end: Double,
        fragility: Double = 1.0,
        margin: Double = 0.05,
        maxWeight: Double = 0.5,
        depth: Int = 1,
        proposal: Double = 0.85,
        skip: Double = 0.85
    ) -> FragilitySpanDiagnostic {
        FragilitySpanDiagnostic(
            spanId: spanId,
            spanStart: start,
            spanEnd: end,
            proposalConfidence: proposal,
            skipConfidence: skip,
            maxSingleEntryWeight: maxWeight,
            distinctEvidenceFamilyDepth: depth,
            margin: margin,
            fragilityScore: fragility
        )
    }

    // MARK: - Labeling agrees with the scorer

    @Test("a span that paired with a GT ad is labeled truePositive")
    func label_truePositive() {
        let rows = [Self.diag(spanId: "s1", start: 100, end: 160)]
        let gt = [Self.annotationWindow(start: 100, end: 160)]
        let det = [Self.storeAdWindow(start: 100, end: 160)]

        let labeled = FragilityPerSpanLabeler.label(
            rows: rows, annotationWindows: gt, adWindows: det,
            podcastId: "pod", episodeId: "ep"
        )
        #expect(labeled.count == 1)
        #expect(labeled[0].label == FragilitySpanLabel.truePositive.rawValue)
        #expect(labeled[0].spanId == "s1")
        #expect(labeled[0].episodeId == "ep")
        #expect(labeled[0].podcastId == "pod")
    }

    @Test("a detection with NO overlapping GT is labeled falsePositive")
    func label_falsePositive() {
        let rows = [Self.diag(spanId: "s1", start: 500, end: 560)]
        let gt = [Self.annotationWindow(start: 100, end: 160)] // far away — no overlap
        let det = [Self.storeAdWindow(start: 500, end: 560)]

        let labeled = FragilityPerSpanLabeler.label(
            rows: rows, annotationWindows: gt, adWindows: det,
            podcastId: "pod", episodeId: "ep"
        )
        #expect(labeled.count == 1)
        #expect(labeled[0].label == FragilitySpanLabel.falsePositive.rawValue)
    }

    @Test("a span that did NOT become a skip-eligible detection is correctlyRejected")
    func label_correctlyRejected() {
        // The span was decoded (so the diagnostic fired) but its persisted
        // window is SUPPRESSED — not skip-eligible — so the scorer never sees a
        // detection for it. The labeler must call it correctlyRejected.
        let rows = [Self.diag(spanId: "s1", start: 300, end: 360)]
        let gt: [CorpusAnnotation.AdWindow] = []
        let det = [Self.storeAdWindow(
            start: 300, end: 360,
            decisionState: AdDecisionState.suppressed.rawValue
        )]

        let labeled = FragilityPerSpanLabeler.label(
            rows: rows, annotationWindows: gt, adWindows: det,
            podcastId: "pod", episodeId: "ep"
        )
        #expect(labeled.count == 1)
        #expect(labeled[0].label == FragilitySpanLabel.correctlyRejected.rawValue)
    }

    @Test("a decoded span with no persisted window at all is correctlyRejected")
    func label_noWindowCorrectlyRejected() {
        let rows = [Self.diag(spanId: "s1", start: 300, end: 360)]
        let labeled = FragilityPerSpanLabeler.label(
            rows: rows, annotationWindows: [], adWindows: [],
            podcastId: "pod", episodeId: "ep"
        )
        #expect(labeled[0].label == FragilitySpanLabel.correctlyRejected.rawValue)
    }

    @Test("labeling matches the scorer's TP/FP/miss tally exactly on a mixed episode")
    func label_matchesScorerTally() {
        // Two GT ads; three skip-eligible detections: one TP on GT-A, one FP,
        // one TP on GT-B. GT none-paired count (miss) must be 0. Plus one
        // suppressed span (correctly rejected, not scored).
        let gt = [
            Self.annotationWindow(start: 100, end: 160), // GT-A
            Self.annotationWindow(start: 400, end: 460), // GT-B
        ]
        let det = [
            Self.storeAdWindow(start: 100, end: 158),  // ~TP on GT-A
            Self.storeAdWindow(start: 700, end: 760),  // FP
            Self.storeAdWindow(start: 402, end: 460),  // ~TP on GT-B
            Self.storeAdWindow(start: 900, end: 960, decisionState: AdDecisionState.suppressed.rawValue), // CR
        ]
        let rows = [
            Self.diag(spanId: "tpA", start: 100, end: 158),
            Self.diag(spanId: "fp",  start: 700, end: 760),
            Self.diag(spanId: "tpB", start: 402, end: 460),
            Self.diag(spanId: "cr",  start: 900, end: 960),
        ]

        // Ground-truth from the scorer itself.
        let bridgedGT = gt.enumerated().map { i, w in
            MetricGroundTruthAd(annotationWindow: w, id: "ep-gt-\(i)", podcastId: "pod", episodeId: "ep")
        }
        let bridgedDet = MetricsBatch.skipEligibleDetections(from: det, podcastId: "pod", episodeId: "ep")
        let batch = MetricsBatch.pair(groundTruth: bridgedGT, detections: bridgedDet)
        let f1 = SpanF1(batch: batch)

        let labeled = FragilityPerSpanLabeler.label(
            rows: rows, annotationWindows: gt, adWindows: det,
            podcastId: "pod", episodeId: "ep"
        )
        let tpCount = labeled.filter { $0.label == FragilitySpanLabel.truePositive.rawValue }.count
        let fpCount = labeled.filter { $0.label == FragilitySpanLabel.falsePositive.rawValue }.count
        let crCount = labeled.filter { $0.label == FragilitySpanLabel.correctlyRejected.rawValue }.count

        // The labeler's TP/FP counts must equal the scorer's, span-for-span.
        #expect(tpCount == f1.truePositives, "TP must match scorer: labeler=\(tpCount) scorer=\(f1.truePositives)")
        #expect(fpCount == f1.falsePositives, "FP must match scorer: labeler=\(fpCount) scorer=\(f1.falsePositives)")
        #expect(f1.misses == 0, "both GT ads paired, so misses must be 0")
        #expect(crCount == 1, "the suppressed span is correctly rejected (not a detection)")

        // And the per-span identity must land on the right spans.
        let byId = Dictionary(uniqueKeysWithValues: labeled.map { ($0.spanId, $0.label) })
        #expect(byId["tpA"] == FragilitySpanLabel.truePositive.rawValue)
        #expect(byId["tpB"] == FragilitySpanLabel.truePositive.rawValue)
        #expect(byId["fp"] == FragilitySpanLabel.falsePositive.rawValue)
        #expect(byId["cr"] == FragilitySpanLabel.correctlyRejected.rawValue)
    }

    @Test("the join keys on (start,end), NOT on AdWindow.id (which is a fresh UUID)")
    func label_keyOnBoundsNotId() {
        // The diagnostic span id and the persisted AdWindow id are unrelated.
        // The label must still resolve via the boundary join.
        let rows = [Self.diag(spanId: "decoded-span-id", start: 10, end: 40)]
        let det = [Self.storeAdWindow(start: 10, end: 40)] // random UUID id
        let labeled = FragilityPerSpanLabeler.label(
            rows: rows, annotationWindows: [Self.annotationWindow(start: 10, end: 40)],
            adWindows: det, podcastId: "pod", episodeId: "ep"
        )
        #expect(labeled[0].label == FragilitySpanLabel.truePositive.rawValue)
        #expect(labeled[0].spanId == "decoded-span-id")
    }

    // MARK: - Group summary

    @Test("report verdict is YES when FP fragilities are systematically higher than TP")
    func report_verdictHigher() {
        let rows: [LabeledFragilitySpanRow] = [
            Self.labeled(label: .falsePositive, fragility: 5.0, depth: 1),
            Self.labeled(label: .falsePositive, fragility: 7.0, depth: 1),
            Self.labeled(label: .truePositive, fragility: 1.0, depth: 3),
            Self.labeled(label: .truePositive, fragility: 2.0, depth: 3),
        ]
        let report = FragilityPerSpanDiagnosticReport(episodeCount: 1, rows: rows)
        #expect(report.fpFragilitySystematicallyHigherThanTP == true)
        #expect(report.fpFragilityMedianHigherThanTP == true)
        #expect(report.falsePositiveStats.count == 2)
        #expect(report.truePositiveStats.count == 2)
        #expect(report.falsePositiveStats.meanFragility == 6.0)
        #expect(report.truePositiveStats.meanFragility == 1.5)
        // Depth mean is honest per group.
        #expect(report.falsePositiveStats.meanDepth == 1.0)
        #expect(report.truePositiveStats.meanDepth == 3.0)
    }

    @Test("report verdict is NO when FP fragilities are NOT higher than TP")
    func report_verdictNotHigher() {
        let rows: [LabeledFragilitySpanRow] = [
            Self.labeled(label: .falsePositive, fragility: 1.0),
            Self.labeled(label: .truePositive, fragility: 9.0),
        ]
        let report = FragilityPerSpanDiagnosticReport(episodeCount: 1, rows: rows)
        #expect(report.fpFragilitySystematicallyHigherThanTP == false)
    }

    @Test("report verdict is nil (undefined) when a group is empty")
    func report_verdictNilOnEmptyGroup() {
        let rows: [LabeledFragilitySpanRow] = [
            Self.labeled(label: .truePositive, fragility: 1.0),
            Self.labeled(label: .correctlyRejected, fragility: 0.5),
        ]
        let report = FragilityPerSpanDiagnosticReport(episodeCount: 1, rows: rows)
        #expect(report.falsePositiveStats.count == 0)
        #expect(report.falsePositiveStats.meanFragility == nil)
        #expect(report.fpFragilitySystematicallyHigherThanTP == nil)
        #expect(report.fpFragilityMedianHigherThanTP == nil)
        // Correctly-rejected group is still summarized.
        #expect(report.correctlyRejectedStats.count == 1)
    }

    @Test("report median is the middle value (odd) / mean of two middles (even)")
    func report_medianMath() {
        let odd: [LabeledFragilitySpanRow] = [
            Self.labeled(label: .falsePositive, fragility: 1.0),
            Self.labeled(label: .falsePositive, fragility: 10.0),
            Self.labeled(label: .falsePositive, fragility: 3.0),
        ]
        #expect(FragilityPerSpanDiagnosticReport(episodeCount: 1, rows: odd).falsePositiveStats.medianFragility == 3.0)

        let even: [LabeledFragilitySpanRow] = [
            Self.labeled(label: .falsePositive, fragility: 2.0),
            Self.labeled(label: .falsePositive, fragility: 4.0),
        ]
        #expect(FragilityPerSpanDiagnosticReport(episodeCount: 1, rows: even).falsePositiveStats.medianFragility == 3.0)
    }

    @Test("report JSON round-trips")
    func report_jsonRoundTrips() throws {
        let rows: [LabeledFragilitySpanRow] = [
            Self.labeled(label: .falsePositive, fragility: 5.0),
            Self.labeled(label: .truePositive, fragility: 1.0),
        ]
        let report = FragilityPerSpanDiagnosticReport(episodeCount: 1, rows: rows)
        let data = try report.jsonData()
        let decoded = try JSONDecoder().decode(FragilityPerSpanDiagnosticReport.self, from: data)
        #expect(decoded == report)
    }

    private static func labeled(
        label: FragilitySpanLabel,
        fragility: Double,
        margin: Double = 0.05,
        depth: Int = 1
    ) -> LabeledFragilitySpanRow {
        LabeledFragilitySpanRow(
            episodeId: "ep",
            podcastId: "pod",
            spanId: UUID().uuidString,
            spanStart: 0,
            spanEnd: 30,
            proposalConfidence: 0.85,
            skipConfidence: 0.85,
            maxSingleEntryWeight: 0.5,
            distinctEvidenceFamilyDepth: depth,
            margin: margin,
            fragilityScore: fragility,
            label: label.rawValue
        )
    }
}

// MARK: - Observer geometry derivation

@Suite("Fragility diagnostic observer (xsdz.7 Part A)")
struct FragilityDiagnosticObserverTests {

    private func entry(source: EvidenceSourceType, weight: Double) -> EvidenceLedgerEntry {
        // The observer reads only `source` + `weight`; the detail payload is
        // irrelevant to its component derivation, so any valid detail works.
        EvidenceLedgerEntry(source: source, weight: weight, detail: .classifier(score: weight))
    }

    @Test("observer derives concentration/depth inputs from the SAME taxonomy the formula uses")
    func observer_derivesComponentsFromLedger() async {
        let observer = FragilityDiagnosticObserver()
        // Two textual entries (one family), one acoustic (second family), one
        // observability-only audit row (excluded), one zero-weight (excluded).
        let ledger = [
            entry(source: .lexical, weight: 0.20),
            entry(source: .classifier, weight: 0.50), // max scoring weight
            entry(source: .acoustic, weight: 0.10),   // second family
            entry(source: .audit, weight: 0.99),      // observability-only → excluded
            entry(source: .fm, weight: 0.0),          // zero weight → excluded
        ]
        await observer.record(
            assetId: "asset-1",
            spanId: "span-1",
            spanStart: 12.0,
            spanEnd: 48.0,
            proposalConfidence: 0.85,
            skipConfidence: 0.83,
            standardAutoSkipThreshold: 0.80,
            fragilityScore: 2.34,
            ledger: ledger
        )

        let rows = await observer.spanRows(for: "asset-1")
        #expect(rows?.count == 1)
        let row = try! #require(rows?.first)
        #expect(row.spanId == "span-1")
        #expect(row.spanStart == 12.0)
        #expect(row.spanEnd == 48.0)
        #expect(row.proposalConfidence == 0.85)
        #expect(row.skipConfidence == 0.83)
        // maxSingleEntryWeight = max scoring weight = 0.50 (audit's 0.99 excluded).
        #expect(row.maxSingleEntryWeight == 0.50)
        // depth = distinct families among scoring entries: textual + acoustic = 2.
        #expect(row.distinctEvidenceFamilyDepth == 2)
        // margin = proposalConfidence − standardThreshold = 0.85 − 0.80 = 0.05.
        #expect(abs(row.margin - 0.05) < 1e-9)
        // score is whatever the fire site passed (the production helper's value).
        #expect(row.fragilityScore == 2.34)
        #expect(await observer.recordCount(for: "asset-1") == 1)
    }

    @Test("observer accumulates one row per span in record order")
    func observer_accumulatesPerSpan() async {
        let observer = FragilityDiagnosticObserver()
        for i in 0..<3 {
            await observer.record(
                assetId: "a", spanId: "s\(i)",
                spanStart: Double(i), spanEnd: Double(i) + 1,
                proposalConfidence: 0.8, skipConfidence: 0.8,
                standardAutoSkipThreshold: 0.8, fragilityScore: Double(i),
                ledger: [entry(source: .fm, weight: 0.3)]
            )
        }
        let rows = await observer.spanRows(for: "a")
        #expect(rows?.map(\.spanId) == ["s0", "s1", "s2"])
        #expect(await observer.recordCount(for: "a") == 3)
        #expect(await observer.spanRows(for: "missing") == nil)
    }

    @Test("empty/all-excluded ledger yields zero maxWeight and depth")
    func observer_emptyScoringLedger() async {
        let observer = FragilityDiagnosticObserver()
        await observer.record(
            assetId: "a", spanId: "s",
            spanStart: 0, spanEnd: 1,
            proposalConfidence: 0.5, skipConfidence: 0.5,
            standardAutoSkipThreshold: 0.8, fragilityScore: 0.0,
            ledger: [entry(source: .audit, weight: 0.9)] // observability-only
        )
        let row = await observer.spanRows(for: "a")?.first
        #expect(row?.maxSingleEntryWeight == 0.0)
        #expect(row?.distinctEvidenceFamilyDepth == 0)
        // margin can be negative (proposal below threshold).
        #expect(abs((row?.margin ?? 0) - (-0.3)) < 1e-9)
    }
}
