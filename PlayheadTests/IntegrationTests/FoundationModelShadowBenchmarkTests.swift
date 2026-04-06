import Foundation
import Testing
@testable import Playhead

@Suite("Foundation Model Classifier — Shadow Benchmark")
struct FoundationModelShadowBenchmarkTests {

    @Test("shadow benchmark summary reports coarse and refined hits separately")
    func summaryReportsCoarseAndRefinedHits() {
        let coarse = FMCoarseScanOutput(
            status: .success,
            windows: [
                FMCoarseWindowOutput(
                    windowIndex: 0,
                    lineRefs: [0, 1],
                    startTime: 0,
                    endTime: 26,
                    transcriptQuality: .good,
                    screening: CoarseScreeningSchema(
                        disposition: .containsAd,
                        support: CoarseSupportSchema(
                            supportLineRefs: [1],
                            certainty: .strong
                        )
                    ),
                    latencyMillis: 120
                ),
                FMCoarseWindowOutput(
                    windowIndex: 1,
                    lineRefs: [2, 3],
                    startTime: 30,
                    endTime: 56,
                    transcriptQuality: .good,
                    screening: CoarseScreeningSchema(
                        disposition: .containsAd,
                        support: CoarseSupportSchema(
                            supportLineRefs: [2],
                            certainty: .moderate
                        )
                    ),
                    latencyMillis: 140
                ),
            ],
            latencyMillis: 260,
            prewarmHit: true
        )
        let refinement = FMRefinementScanOutput(
            status: .success,
            windows: [
                FMRefinementWindowOutput(
                    windowIndex: 0,
                    sourceWindowIndex: 0,
                    lineRefs: [0, 1],
                    spans: [
                        makeBenchmarkSpan(
                            firstLineRef: 0,
                            lastLineRef: 1,
                            firstAtomOrdinal: 0,
                            lastAtomOrdinal: 1,
                            commercialIntent: .paid,
                            ownership: .thirdParty,
                            certainty: .strong,
                            boundaryPrecision: .precise
                        )
                    ],
                    latencyMillis: 180
                )
            ],
            latencyMillis: 180,
            prewarmHit: true
        )
        let groundTruth = [
            GroundTruthAd(
                id: "a",
                startTime: 0,
                endTime: 26,
                type: .sponsor,
                skipConfidence: 1.0,
                advertiser: "A",
                description: "A",
                expectedSignals: [],
                missedSignals: []
            ),
            GroundTruthAd(
                id: "b",
                startTime: 30,
                endTime: 56,
                type: .crossPromo,
                skipConfidence: 0.8,
                advertiser: "B",
                description: "B",
                expectedSignals: [],
                missedSignals: []
            ),
        ]
        let falsePositives = [
            NonAdSignal(
                startTime: 90,
                endTime: 95,
                description: "non-ad",
                expectedPattern: "example.com",
                reason: "first-party"
            )
        ]

        let summary = FoundationModelShadowBenchmarkSummary.build(
            coarse: coarse,
            refinement: refinement,
            groundTruth: groundTruth,
            falsePositives: falsePositives
        )

        #expect(summary.coarseHitCount == 2)
        #expect(summary.refinedHitCount == 1)
        #expect(summary.adReports.map(\.id) == ["a", "b"])
        #expect(summary.adReports[0].coarseHit)
        #expect(summary.adReports[0].refinedHit)
        #expect(summary.adReports[1].coarseHit)
        #expect(!summary.adReports[1].refinedHit)
        #expect(summary.falsePositiveReports.allSatisfy { !$0.coarseHit && !$0.refinedHit })
    }

    @available(iOS 26.0, *)
    @Test("live runtime benchmark prints shadow quality for the real episode fixture")
    func liveRuntimeRealEpisodeBenchmark() async throws {
        let segments = buildShadowBenchmarkSegments()
        let evidenceCatalog = buildShadowBenchmarkEvidenceCatalog()
        let classifier = FoundationModelClassifier()

        let coarse = try await classifier.coarsePassA(segments: segments)
        if coarse.status != .success {
            print("\n=== Foundation Model Shadow Benchmark ===")
            print("Live runtime unavailable: \(coarse.status.rawValue)")
            return
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
        let summary = FoundationModelShadowBenchmarkSummary.build(
            coarse: coarse,
            refinement: refinement,
            groundTruth: ConanFanhausenRevisitedFixture.groundTruthAds,
            falsePositives: ConanFanhausenRevisitedFixture.knownFalsePositives
        )

        summary.printReport()
    }
}
