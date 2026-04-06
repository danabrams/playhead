import Foundation
import XCTest
@testable import Playhead

final class FoundationModelShadowBenchmarkTests: XCTestCase {

    func testSummaryUsesAtomLevelEvidenceSpans() throws {
        let segments = makeMultiAtomBenchmarkSegments()
        let transcriptVersion = "transcript-v1"
        let scanResults = [
            makeBenchmarkScanResult(
                id: "scan-passA",
                analysisAssetId: "asset-benchmark",
                transcriptVersion: transcriptVersion,
                scanPass: "passA",
                windowFirstAtomOrdinal: 0,
                windowLastAtomOrdinal: 3,
                windowStartTime: 0,
                windowEndTime: 26,
                disposition: .containsAd,
                status: .success,
                latencyMs: 120
            ),
            makeBenchmarkScanResult(
                id: "scan-passB",
                analysisAssetId: "asset-benchmark",
                transcriptVersion: transcriptVersion,
                scanPass: "passB",
                windowFirstAtomOrdinal: 0,
                windowLastAtomOrdinal: 3,
                windowStartTime: 6,
                windowEndTime: 18,
                disposition: .containsAd,
                status: .success,
                latencyMs: 180
            ),
        ]
        let evidenceEvents = [
            try makeBenchmarkEvidenceEvent(
                id: "event-passB",
                analysisAssetId: "asset-benchmark",
                atomOrdinals: [1, 2],
                commercialIntent: "paid",
                boundaryPrecision: "precise"
            )
        ]
        let groundTruth = [
            GroundTruthAd(
                id: "sponsor-core",
                startTime: 6,
                endTime: 18,
                type: .sponsor,
                skipConfidence: 1.0,
                advertiser: "Example Sponsor",
                description: "Core sponsor copy only",
                expectedSignals: [],
                missedSignals: []
            ),
            GroundTruthAd(
                id: "cross-promo-tail",
                startTime: 18,
                endTime: 26,
                type: .crossPromo,
                skipConfidence: 0.8,
                advertiser: "Example Cross Promo",
                description: "Tail segment should stay coarse-only",
                expectedSignals: [],
                missedSignals: []
            ),
        ]
        let falsePositives = [
            NonAdSignal(
                startTime: 40,
                endTime: 45,
                description: "non-ad",
                expectedPattern: "example.com",
                reason: "far away"
            )
        ]

        let summary = FoundationModelShadowBenchmarkSummary.build(
            scanResults: scanResults,
            evidenceEvents: evidenceEvents,
            segments: segments,
            groundTruth: groundTruth,
            falsePositives: falsePositives
        )

        XCTAssertEqual(summary.coarseStatus, .success)
        XCTAssertEqual(summary.refinementStatus, .success)
        XCTAssertEqual(summary.coarseHitCount, 2)
        XCTAssertEqual(summary.refinedHitCount, 1)
        XCTAssertEqual(summary.refinedSpanCount, 1)
        XCTAssertEqual(summary.adReports.map(\.id), ["sponsor-core", "cross-promo-tail"])
        XCTAssertTrue(summary.adReports[0].refinedHit)
        XCTAssertEqual(summary.adReports[0].refinedOverlapSeconds, 12.0)
        XCTAssertFalse(summary.adReports[1].refinedHit)
        XCTAssertEqual(summary.adReports[1].refinedOverlapSeconds, 0.0)
        XCTAssertTrue(summary.falsePositiveReports.allSatisfy { !$0.coarseHit && !$0.refinedHit })
        XCTAssertEqual(summary.commercialIntentCounts.count, 1)
        XCTAssertEqual(summary.commercialIntentCounts.first?.0, "paid")
        XCTAssertEqual(summary.commercialIntentCounts.first?.1, 1)
        XCTAssertEqual(summary.boundaryPrecisionCounts.count, 1)
        XCTAssertEqual(summary.boundaryPrecisionCounts.first?.0, "precise")
        XCTAssertEqual(summary.boundaryPrecisionCounts.first?.1, 1)
        XCTAssertEqual(summary.malformedEvidenceEventCount, 0)
    }

    func testBlockedSummaryReportsPolicyBlockWithoutRecallAssertions() throws {
        let segments = makeMultiAtomBenchmarkSegments()
        let transcriptVersion = "transcript-v1"
        let scanResults = [
            makeBenchmarkScanResult(
                id: "scan-passA-blocked",
                analysisAssetId: "asset-benchmark",
                transcriptVersion: transcriptVersion,
                scanPass: "passA",
                windowFirstAtomOrdinal: 0,
                windowLastAtomOrdinal: 3,
                windowStartTime: 0,
                windowEndTime: 26,
                disposition: .abstain,
                status: .guardrailViolation,
                latencyMs: 95
            )
        ]
        let groundTruth = [
            GroundTruthAd(
                id: "sponsor-core",
                startTime: 6,
                endTime: 18,
                type: .sponsor,
                skipConfidence: 1.0,
                advertiser: "Example Sponsor",
                description: "Core sponsor copy only",
                expectedSignals: [],
                missedSignals: []
            )
        ]

        let summary = FoundationModelShadowBenchmarkSummary.build(
            scanResults: scanResults,
            evidenceEvents: [],
            segments: segments,
            groundTruth: groundTruth,
            falsePositives: []
        )
        let rendered = summary.render()

        XCTAssertEqual(summary.blockingStatus, .guardrailViolation)
        XCTAssertEqual(summary.coarseStatus, .guardrailViolation)
        XCTAssertEqual(summary.refinementStatus, .guardrailViolation)
        XCTAssertEqual(summary.coarseWindowCount, 1)
        XCTAssertEqual(summary.refinedHitCount, 0)
        XCTAssertTrue(rendered.contains("Benchmark blocked by FM policy: guardrailViolation"))
        XCTAssertTrue(rendered.contains("Ground-truth recall is not meaningful for blocked FM runs."))
        XCTAssertFalse(rendered.contains("Ground-truth recall: coarse"))
    }

    func testTargetedClipSummarySeparatesBlockedAdsFromHits() {
        let summary = LiveTargetedClipBenchmarkSummary(
            adReports: [
                LiveClipBenchmarkReport(
                    id: "caught-ad",
                    label: "Caught Ad",
                    coarseStatus: .success,
                    refinementStatus: .success,
                    coarseHit: true,
                    refinedHit: true,
                    coarseLatencyMs: 120,
                    refinementLatencyMs: 180
                ),
                LiveClipBenchmarkReport(
                    id: "missed-ad",
                    label: "Missed Ad",
                    coarseStatus: .success,
                    refinementStatus: .success,
                    coarseHit: false,
                    refinedHit: false,
                    coarseLatencyMs: 90,
                    refinementLatencyMs: 0
                ),
                LiveClipBenchmarkReport(
                    id: "blocked-ad",
                    label: "Blocked Ad",
                    coarseStatus: .guardrailViolation,
                    refinementStatus: .guardrailViolation,
                    coarseHit: false,
                    refinedHit: false,
                    coarseLatencyMs: 6000,
                    refinementLatencyMs: 0
                )
            ],
            controlReports: [
                LiveClipBenchmarkReport(
                    id: "control-hit",
                    label: "Control",
                    coarseStatus: .success,
                    refinementStatus: .success,
                    coarseHit: true,
                    refinedHit: false,
                    coarseLatencyMs: 80,
                    refinementLatencyMs: 0
                )
            ]
        )

        XCTAssertEqual(summary.coarseHitCount, 1)
        XCTAssertEqual(summary.refinedHitCount, 1)
        XCTAssertEqual(summary.policyBlockedAdCount, 1)
        XCTAssertEqual(summary.evaluableAdCount, 2)
        XCTAssertEqual(summary.coarseFalsePositiveCount, 1)
        XCTAssertEqual(summary.refinedFalsePositiveCount, 0)

        let rendered = summary.render()
        XCTAssertTrue(rendered.contains("Ads: coarse hits 1/3, refined hits 1/3, policy-blocked 1/3"))
        XCTAssertTrue(rendered.contains("Evaluable ads: 2/3"))
        XCTAssertTrue(rendered.contains("False-positive watch: coarse 1/1, refined 0/1"))
    }

    @available(iOS 26.0, *)
    func testLiveRuntimeShadowBenchmark() async throws {
        #if targetEnvironment(simulator)
        throw XCTSkip("Requires a real Apple Intelligence-capable device.")
        #else
        let fixture = buildFixtureShadowBenchmark()
        let store = try await makeTestStore()
        try await store.insertAsset(makeBenchmarkAsset(
            id: fixture.inputs.analysisAssetId,
            episodeId: ConanFanhausenRevisitedFixture.episodeId
        ))

        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(promptLabel: "fm-shadow-benchmark")
        )

        let result = try await runner.runPendingBackfill(for: fixture.inputs)
        let scanResults = try await store.fetchSemanticScanResults(
            analysisAssetId: fixture.inputs.analysisAssetId
        )
        let evidenceEvents = try await store.fetchEvidenceEvents(
            analysisAssetId: fixture.inputs.analysisAssetId
        )

        XCTAssertFalse(result.admittedJobIds.isEmpty, "Shadow benchmark was never admitted.")
        XCTAssertTrue(result.deferredJobIds.isEmpty, "Shadow benchmark deferred instead of running.")
        XCTAssertFalse(scanResults.isEmpty, "Shadow benchmark produced no semantic scan rows.")

        let summary = FoundationModelShadowBenchmarkSummary.build(
            scanResults: scanResults,
            evidenceEvents: evidenceEvents,
            segments: fixture.inputs.segments,
            groundTruth: fixture.groundTruth,
            falsePositives: fixture.falsePositives
        )

        print(summary.render(runResult: result))

        if let blockingStatus = summary.blockingStatus {
            XCTAssertTrue(
                blockingStatus == .guardrailViolation || blockingStatus == .refusal,
                "Only FM safety/refusal outcomes should short-circuit the live benchmark."
            )
            return
        }

        XCTAssertGreaterThan(summary.coarseWindowCount, 0, "Expected persisted passA windows.")
        XCTAssertEqual(summary.coarseStatus, .success, "Coarse shadow pass did not complete successfully.")
        XCTAssertEqual(summary.refinementStatus, .success, "Refinement shadow pass did not complete successfully.")
        #endif
    }

    @available(iOS 26.0, *)
    func testLiveRuntimeTargetedClipBenchmark() async throws {
        #if targetEnvironment(simulator)
        throw XCTSkip("Requires a real Apple Intelligence-capable device.")
        #else
        let fixture = buildFixtureShadowBenchmark()
        let classifier = FoundationModelClassifier()

        var adReports: [LiveClipBenchmarkReport] = []
        for ad in fixture.groundTruth {
            let clipSegments = makeClipSegments(
                from: fixture.inputs.segments,
                startTime: ad.startTime,
                endTime: ad.endTime,
                padding: 4
            )
            XCTAssertFalse(clipSegments.isEmpty, "Expected non-empty clip for \(ad.id)")
            let report = try await runLiveClipBenchmark(
                id: ad.id,
                label: ad.advertiser,
                targetRange: BenchmarkRange(startTime: ad.startTime, endTime: ad.endTime),
                segments: clipSegments,
                evidenceCatalog: fixture.inputs.evidenceCatalog,
                classifier: classifier
            )
            adReports.append(report)
        }

        var controlReports: [LiveClipBenchmarkReport] = []
        for signal in fixture.falsePositives {
            let clipSegments = makeClipSegments(
                from: fixture.inputs.segments,
                startTime: signal.startTime,
                endTime: signal.endTime,
                padding: 4
            )
            XCTAssertFalse(clipSegments.isEmpty, "Expected non-empty control clip for \(signal.description)")
            let report = try await runLiveClipBenchmark(
                id: signal.description,
                label: signal.description,
                targetRange: BenchmarkRange(startTime: signal.startTime, endTime: signal.endTime),
                segments: clipSegments,
                evidenceCatalog: fixture.inputs.evidenceCatalog,
                classifier: classifier
            )
            controlReports.append(report)
        }

        let summary = LiveTargetedClipBenchmarkSummary(
            adReports: adReports,
            controlReports: controlReports
        )
        print(summary.render())
        #endif
    }
}

private struct FixtureShadowBenchmark {
    let inputs: BackfillJobRunner.AssetInputs
    let groundTruth: [GroundTruthAd]
    let falsePositives: [NonAdSignal]
}

private struct LiveClipBenchmarkReport {
    let id: String
    let label: String
    let coarseStatus: SemanticScanStatus
    let refinementStatus: SemanticScanStatus
    let coarseHit: Bool
    let refinedHit: Bool
    let coarseLatencyMs: Double
    let refinementLatencyMs: Double

    var policyBlocked: Bool {
        coarseStatus == .guardrailViolation || coarseStatus == .refusal ||
        refinementStatus == .guardrailViolation || refinementStatus == .refusal
    }
}

private struct LiveTargetedClipBenchmarkSummary {
    let adReports: [LiveClipBenchmarkReport]
    let controlReports: [LiveClipBenchmarkReport]

    var coarseHitCount: Int { adReports.filter(\.coarseHit).count }
    var refinedHitCount: Int { adReports.filter(\.refinedHit).count }
    var policyBlockedAdCount: Int { adReports.filter(\.policyBlocked).count }
    var evaluableAdCount: Int { adReports.count - policyBlockedAdCount }
    var coarseFalsePositiveCount: Int { controlReports.filter(\.coarseHit).count }
    var refinedFalsePositiveCount: Int { controlReports.filter(\.refinedHit).count }

    func render() -> String {
        var lines: [String] = []
        lines.append("\n=== Foundation Model Targeted Clip Benchmark ===")
        lines.append(
            "Ads: coarse hits \(coarseHitCount)/\(adReports.count), refined hits \(refinedHitCount)/\(adReports.count), policy-blocked \(policyBlockedAdCount)/\(adReports.count)"
        )
        lines.append("Evaluable ads: \(evaluableAdCount)/\(adReports.count)")
        if !controlReports.isEmpty {
            lines.append(
                "False-positive watch: coarse \(coarseFalsePositiveCount)/\(controlReports.count), refined \(refinedFalsePositiveCount)/\(controlReports.count)"
            )
        }

        lines.append("\nPer-ad clips:")
        for report in adReports {
            lines.append(
                "  [\(report.coarseHit ? "coarse" : "-") / \(report.refinedHit ? "refined" : "-")] \(report.id) \(report.label) coarse=\(report.coarseStatus.rawValue) refined=\(report.refinementStatus.rawValue) latency=\(fmt(report.coarseLatencyMs))/\(fmt(report.refinementLatencyMs))ms"
            )
        }

        if !controlReports.isEmpty {
            lines.append("\nControl clips:")
            for report in controlReports {
                lines.append(
                    "  [\(report.coarseHit ? "coarse-hit" : "clean") / \(report.refinedHit ? "refined-hit" : "clean")] \(report.label) coarse=\(report.coarseStatus.rawValue) refined=\(report.refinementStatus.rawValue)"
                )
            }
        }

        return lines.joined(separator: "\n")
    }

    private func fmt(_ value: Double) -> String {
        String(format: "%.1f", value)
    }
}

private struct FoundationModelShadowBenchmarkSummary {
    struct AdReport: Sendable {
        let id: String
        let advertiser: String
        let coarseHit: Bool
        let refinedHit: Bool
        let coarseOverlapSeconds: Double
        let refinedOverlapSeconds: Double
    }

    struct FalsePositiveReport: Sendable {
        let description: String
        let coarseHit: Bool
        let refinedHit: Bool
    }

    let coarseStatus: SemanticScanStatus
    let refinementStatus: SemanticScanStatus
    let coarseWindowCount: Int
    let coarsePositiveWindowCount: Int
    let refinementWindowCount: Int
    let refinedSpanCount: Int
    let coarseLatencyMs: Double
    let refinementLatencyMs: Double
    let coarseHitCount: Int
    let refinedHitCount: Int
    let malformedEvidenceEventCount: Int
    let adReports: [AdReport]
    let falsePositiveReports: [FalsePositiveReport]
    let commercialIntentCounts: [(String, Int)]
    let boundaryPrecisionCounts: [(String, Int)]

    var blockingStatus: SemanticScanStatus? {
        [coarseStatus, refinementStatus].first(where: {
            $0 == .guardrailViolation || $0 == .refusal
        })
    }

    static func build(
        scanResults: [SemanticScanResult],
        evidenceEvents: [EvidenceEvent],
        segments: [AdTranscriptSegment],
        groundTruth: [GroundTruthAd],
        falsePositives: [NonAdSignal]
    ) -> FoundationModelShadowBenchmarkSummary {
        let passA = scanResults
            .filter { $0.scanPass == "passA" }
            .sorted { lhs, rhs in
                if lhs.windowFirstAtomOrdinal == rhs.windowFirstAtomOrdinal {
                    return lhs.id < rhs.id
                }
                return lhs.windowFirstAtomOrdinal < rhs.windowFirstAtomOrdinal
            }
        let passB = scanResults
            .filter { $0.scanPass == "passB" }
            .sorted { lhs, rhs in
                if lhs.windowFirstAtomOrdinal == rhs.windowFirstAtomOrdinal {
                    return lhs.id < rhs.id
                }
                return lhs.windowFirstAtomOrdinal < rhs.windowFirstAtomOrdinal
            }

        let atomLookup = Dictionary(uniqueKeysWithValues: segments.flatMap(\.atoms).map {
            ($0.atomKey.atomOrdinal, $0)
        })

        let coarseRanges = passA
            .filter { $0.status == .success }
            .filter { $0.disposition == .containsAd || $0.disposition == .uncertain }
            .map { BenchmarkRange(startTime: $0.windowStartTime, endTime: $0.windowEndTime) }

        var refinedRanges: [BenchmarkRange] = []
        var intentCounts: [String: Int] = [:]
        var precisionCounts: [String: Int] = [:]
        var malformedEvidenceEventCount = 0

        for event in evidenceEvents where event.sourceType == .fm && event.eventType == "fm.spanRefinement" {
            guard let ordinals = decodeAtomOrdinals(from: event.atomOrdinals),
                  let range = BenchmarkRange(atomOrdinals: ordinals, atomLookup: atomLookup),
                  let payload = decodeEvidencePayload(from: event.evidenceJSON) else {
                malformedEvidenceEventCount += 1
                continue
            }
            refinedRanges.append(range)
            intentCounts[payload.commercialIntent, default: 0] += 1
            precisionCounts[payload.boundaryPrecision, default: 0] += 1
        }

        let adReports = groundTruth.map { ad in
            let target = BenchmarkRange(startTime: ad.startTime, endTime: ad.endTime)
            let coarseOverlap = coarseRanges.map { $0.overlap(with: target) }.max() ?? 0
            let refinedOverlap = refinedRanges.map { $0.overlap(with: target) }.max() ?? 0
            return AdReport(
                id: ad.id,
                advertiser: ad.advertiser,
                coarseHit: coarseOverlap > 0,
                refinedHit: refinedOverlap > 0,
                coarseOverlapSeconds: coarseOverlap,
                refinedOverlapSeconds: refinedOverlap
            )
        }

        let falsePositiveReports = falsePositives.map { signal in
            let target = BenchmarkRange(startTime: signal.startTime, endTime: signal.endTime)
            return FalsePositiveReport(
                description: signal.description,
                coarseHit: coarseRanges.contains { $0.overlap(with: target) > 0 },
                refinedHit: refinedRanges.contains { $0.overlap(with: target) > 0 }
            )
        }

        let coarseStatus = aggregateStatus(
            rows: passA,
            defaultStatus: scanResults.isEmpty ? .failedTransient : .success
        )
        let refinementStatus = aggregateStatus(
            rows: passB,
            defaultStatus: defaultRefinementStatus(passAStatus: coarseStatus, passBRows: passB)
        )

        return FoundationModelShadowBenchmarkSummary(
            coarseStatus: coarseStatus,
            refinementStatus: refinementStatus,
            coarseWindowCount: passA.count,
            coarsePositiveWindowCount: coarseRanges.count,
            refinementWindowCount: passB.count,
            refinedSpanCount: refinedRanges.count,
            coarseLatencyMs: passA.compactMap(\.latencyMs).reduce(0, +),
            refinementLatencyMs: passB.compactMap(\.latencyMs).reduce(0, +),
            coarseHitCount: adReports.filter(\.coarseHit).count,
            refinedHitCount: adReports.filter(\.refinedHit).count,
            malformedEvidenceEventCount: malformedEvidenceEventCount,
            adReports: adReports,
            falsePositiveReports: falsePositiveReports,
            commercialIntentCounts: intentCounts.keys.sorted().map { ($0, intentCounts[$0] ?? 0) },
            boundaryPrecisionCounts: precisionCounts.keys.sorted().map { ($0, precisionCounts[$0] ?? 0) }
        )
    }

    func render(runResult: BackfillJobRunner.RunResult? = nil) -> String {
        var lines: [String] = []
        lines.append("\n=== Foundation Model Shadow Benchmark ===")
        if let runResult {
            lines.append(
                "Jobs: admitted=\(runResult.admittedJobIds.count) deferred=\(runResult.deferredJobIds.count) scans=\(runResult.scanResultIds.count) evidence=\(runResult.evidenceEventIds.count)"
            )
        }
        if let blockingStatus {
            lines.append("Benchmark blocked by FM policy: \(blockingStatus.rawValue)")
        }
        lines.append(
            "Coarse status: \(coarseStatus.rawValue) latency=\(fmt(coarseLatencyMs))ms windows=\(coarseWindowCount) positives=\(coarsePositiveWindowCount)"
        )
        lines.append(
            "Refinement status: \(refinementStatus.rawValue) latency=\(fmt(refinementLatencyMs))ms windows=\(refinementWindowCount) spans=\(refinedSpanCount)"
        )
        if blockingStatus == nil {
            lines.append(
                "Ground-truth recall: coarse \(coarseHitCount)/\(adReports.count), refined \(refinedHitCount)/\(adReports.count)"
            )
        } else {
            lines.append("Ground-truth recall is not meaningful for blocked FM runs.")
        }

        lines.append("\nPer-ad results:")
        for report in adReports {
            lines.append(
                "  [\(report.coarseHit ? "coarse" : "-") / \(report.refinedHit ? "refined" : "-")] \(report.id) \(report.advertiser) coarseOverlap=\(fmt(report.coarseOverlapSeconds))s refinedOverlap=\(fmt(report.refinedOverlapSeconds))s"
            )
        }

        if !falsePositiveReports.isEmpty {
            lines.append("\nFalse-positive watch list:")
            for report in falsePositiveReports {
                lines.append(
                    "  [\(report.coarseHit ? "coarse-hit" : "clean") / \(report.refinedHit ? "refined-hit" : "clean")] \(report.description)"
                )
            }
        }

        if !commercialIntentCounts.isEmpty {
            let rendered = commercialIntentCounts
                .map { "\($0.0)=\($0.1)" }
                .joined(separator: ", ")
            lines.append("\nRefined span intents: \(rendered)")
        }

        if !boundaryPrecisionCounts.isEmpty {
            let rendered = boundaryPrecisionCounts
                .map { "\($0.0)=\($0.1)" }
                .joined(separator: ", ")
            lines.append("Boundary precision: \(rendered)")
        }

        if malformedEvidenceEventCount > 0 {
            lines.append("Malformed evidence events ignored: \(malformedEvidenceEventCount)")
        }

        return lines.joined(separator: "\n")
    }

    private func fmt(_ value: Double) -> String {
        String(format: "%.1f", value)
    }

    private static func aggregateStatus(
        rows: [SemanticScanResult],
        defaultStatus: SemanticScanStatus
    ) -> SemanticScanStatus {
        rows.first(where: { $0.status != .success })?.status ??
        rows.first?.status ??
        defaultStatus
    }

    private static func defaultRefinementStatus(
        passAStatus: SemanticScanStatus,
        passBRows: [SemanticScanResult]
    ) -> SemanticScanStatus {
        guard passBRows.isEmpty else {
            return .success
        }
        return passAStatus == .success ? .success : passAStatus
    }
}

private struct BenchmarkRange: Sendable {
    let startTime: Double
    let endTime: Double

    init(startTime: Double, endTime: Double) {
        self.startTime = startTime
        self.endTime = endTime
    }

    init?(
        atomOrdinals: [Int],
        atomLookup: [Int: TranscriptAtom]
    ) {
        let orderedAtoms = atomOrdinals
            .sorted()
            .compactMap { atomLookup[$0] }
        guard let first = orderedAtoms.first,
              let last = orderedAtoms.last else {
            return nil
        }
        self.init(startTime: first.startTime, endTime: last.endTime)
    }

    func overlap(with other: BenchmarkRange) -> Double {
        max(0, min(endTime, other.endTime) - max(startTime, other.startTime))
    }
}

private func runLiveClipBenchmark(
    id: String,
    label: String,
    targetRange: BenchmarkRange,
    segments: [AdTranscriptSegment],
    evidenceCatalog: EvidenceCatalog,
    classifier: FoundationModelClassifier
) async throws -> LiveClipBenchmarkReport {
    let coarse = try await classifier.coarsePassA(segments: segments)
    let coarseHit = coarse.windows
        .filter { $0.screening.disposition == .containsAd || $0.screening.disposition == .uncertain }
        .contains { window in
            BenchmarkRange(startTime: window.startTime, endTime: window.endTime).overlap(with: targetRange) > 0
        }

    if coarse.status != .success {
        return LiveClipBenchmarkReport(
            id: id,
            label: label,
            coarseStatus: coarse.status,
            refinementStatus: coarse.status,
            coarseHit: coarseHit,
            refinedHit: false,
            coarseLatencyMs: coarse.latencyMillis,
            refinementLatencyMs: 0
        )
    }

    let zoomPlans = try await classifier.planAdaptiveZoom(
        coarse: coarse,
        segments: segments,
        evidenceCatalog: evidenceCatalog
    )
    let refinement = try await classifier.refinePassB(
        zoomPlans: zoomPlans,
        segments: segments,
        evidenceCatalog: evidenceCatalog
    )

    let lineRefLookup = Dictionary(uniqueKeysWithValues: segments.map { ($0.segmentIndex, $0) })
    let refinedHit = refinement.windows
        .flatMap(\.spans)
        .contains { span in
            guard let first = lineRefLookup[span.firstLineRef],
                  let last = lineRefLookup[span.lastLineRef] else {
                return false
            }
            return BenchmarkRange(startTime: first.startTime, endTime: last.endTime)
                .overlap(with: targetRange) > 0
        }

    return LiveClipBenchmarkReport(
        id: id,
        label: label,
        coarseStatus: coarse.status,
        refinementStatus: refinement.status,
        coarseHit: coarseHit,
        refinedHit: refinedHit,
        coarseLatencyMs: coarse.latencyMillis,
        refinementLatencyMs: refinement.latencyMillis
    )
}

private func makeClipSegments(
    from segments: [AdTranscriptSegment],
    startTime: Double,
    endTime: Double,
    padding: Double
) -> [AdTranscriptSegment] {
    let clipStart = max(0, startTime - padding)
    let clipEnd = endTime + padding
    return segments.filter { segment in
        segment.endTime > clipStart && segment.startTime < clipEnd
    }
}

private struct PersistedFMEvidencePayload: Codable {
    let commercialIntent: String
    let boundaryPrecision: String
}

private func buildFixtureShadowBenchmark() -> FixtureShadowBenchmark {
    let chunks = ConanFanhausenRevisitedFixture.parseChunks()
    let (atoms, version) = TranscriptAtomizer.atomize(
        chunks: chunks,
        analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
        normalizationHash: "fm-shadow-benchmark",
        sourceHash: "fixture"
    )
    let segments = TranscriptSegmenter.segment(atoms: atoms)
    let evidenceCatalog = EvidenceCatalogBuilder.build(
        atoms: atoms,
        analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
        transcriptVersion: version.transcriptVersion
    )
    let plannerContext = CoveragePlannerContext(
        observedEpisodeCount: 0,
        stablePrecision: false,
        isFirstEpisodeAfterCohortInvalidation: false,
        recallDegrading: false,
        sponsorDriftDetected: false,
        auditMissDetected: false,
        episodesSinceLastFullRescan: 0,
        periodicFullRescanIntervalEpisodes: 10
    )

    return FixtureShadowBenchmark(
        inputs: BackfillJobRunner.AssetInputs(
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
            podcastId: ConanFanhausenRevisitedFixture.podcastTitle,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: version.transcriptVersion,
            plannerContext: plannerContext
        ),
        groundTruth: ConanFanhausenRevisitedFixture.groundTruthAds,
        falsePositives: ConanFanhausenRevisitedFixture.knownFalsePositives
    )
}

private func makeBenchmarkAsset(id: String, episodeId: String) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId,
        assetFingerprint: "shadow-benchmark-fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///benchmark/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

private func makeBenchmarkScanResult(
    id: String,
    analysisAssetId: String,
    transcriptVersion: String,
    scanPass: String,
    windowFirstAtomOrdinal: Int,
    windowLastAtomOrdinal: Int,
    windowStartTime: Double,
    windowEndTime: Double,
    disposition: CoarseDisposition,
    status: SemanticScanStatus,
    latencyMs: Double
) -> SemanticScanResult {
    SemanticScanResult(
        id: id,
        analysisAssetId: analysisAssetId,
        windowFirstAtomOrdinal: windowFirstAtomOrdinal,
        windowLastAtomOrdinal: windowLastAtomOrdinal,
        windowStartTime: windowStartTime,
        windowEndTime: windowEndTime,
        scanPass: scanPass,
        transcriptQuality: .good,
        disposition: disposition,
        spansJSON: "[]",
        status: status,
        attemptCount: 1,
        errorContext: nil,
        inputTokenCount: nil,
        outputTokenCount: nil,
        latencyMs: latencyMs,
        prewarmHit: true,
        scanCohortJSON: makeTestScanCohortJSON(promptLabel: "fm-shadow-benchmark-unit"),
        transcriptVersion: transcriptVersion
    )
}

private func makeBenchmarkEvidenceEvent(
    id: String,
    analysisAssetId: String,
    atomOrdinals: [Int],
    commercialIntent: String,
    boundaryPrecision: String
) throws -> EvidenceEvent {
    let payload = PersistedFMEvidencePayload(
        commercialIntent: commercialIntent,
        boundaryPrecision: boundaryPrecision
    )
    let payloadJSON = try String(
        decoding: JSONEncoder().encode(payload),
        as: UTF8.self
    )
    let ordinalsJSON = "[\(atomOrdinals.map(String.init).joined(separator: ","))]"
    return EvidenceEvent(
        id: id,
        analysisAssetId: analysisAssetId,
        eventType: "fm.spanRefinement",
        sourceType: .fm,
        atomOrdinals: ordinalsJSON,
        evidenceJSON: payloadJSON,
        scanCohortJSON: makeTestScanCohortJSON(promptLabel: "fm-shadow-benchmark-unit"),
        createdAt: 1
    )
}

private func makeMultiAtomBenchmarkSegments() -> [AdTranscriptSegment] {
    let transcriptVersion = "transcript-v1"
    let atoms = [
        makeBenchmarkAtom(
            analysisAssetId: "asset-benchmark",
            transcriptVersion: transcriptVersion,
            atomOrdinal: 0,
            startTime: 0,
            endTime: 6,
            text: "Cold open lead-in."
        ),
        makeBenchmarkAtom(
            analysisAssetId: "asset-benchmark",
            transcriptVersion: transcriptVersion,
            atomOrdinal: 1,
            startTime: 6,
            endTime: 12,
            text: "Sponsor setup."
        ),
        makeBenchmarkAtom(
            analysisAssetId: "asset-benchmark",
            transcriptVersion: transcriptVersion,
            atomOrdinal: 2,
            startTime: 12,
            endTime: 18,
            text: "Offer details."
        ),
        makeBenchmarkAtom(
            analysisAssetId: "asset-benchmark",
            transcriptVersion: transcriptVersion,
            atomOrdinal: 3,
            startTime: 18,
            endTime: 26,
            text: "Cross promo tag."
        ),
    ]

    return [
        AdTranscriptSegment(
            atoms: Array(atoms[0...2]),
            segmentIndex: 0
        ),
        AdTranscriptSegment(
            atoms: [atoms[3]],
            segmentIndex: 1
        ),
    ]
}

private func makeBenchmarkAtom(
    analysisAssetId: String,
    transcriptVersion: String,
    atomOrdinal: Int,
    startTime: Double,
    endTime: Double,
    text: String
) -> TranscriptAtom {
    TranscriptAtom(
        atomKey: TranscriptAtomKey(
            analysisAssetId: analysisAssetId,
            transcriptVersion: transcriptVersion,
            atomOrdinal: atomOrdinal
        ),
        contentHash: "hash-\(atomOrdinal)",
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: atomOrdinal
    )
}

private func decodeAtomOrdinals(from json: String) -> [Int]? {
    guard let data = json.data(using: .utf8) else {
        return nil
    }
    return try? JSONDecoder().decode([Int].self, from: data)
}

private func decodeEvidencePayload(from json: String) -> PersistedFMEvidencePayload? {
    guard let data = json.data(using: .utf8) else {
        return nil
    }
    return try? JSONDecoder().decode(PersistedFMEvidencePayload.self, from: data)
}
