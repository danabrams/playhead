import Testing

@testable import Playhead

@Suite("TargetedWindowNarrower")
struct TargetedWindowNarrowerTests {

    @Test("predicted targeted coverage is the union of all targeted phase narrowings")
    func predictedCoverageMatchesUnionOfPhaseNarrowings() {
        let inputs = makeInputs()

        let union = Set(
            BackfillJobPhase
                .allCases
                .filter { $0 != .fullEpisodeScan }
                .flatMap { phase in
                    TargetedWindowNarrower.narrow(
                        phase: phase,
                        inputs: inputs
                    ).map(\.segmentIndex)
                }
        )
        let predicted = TargetedWindowNarrower.predictedTargetedLineRefs(inputs: inputs)

        #expect(predicted == union)
    }

    @Test("audit narrowing is deterministic for identical inputs")
    func auditNarrowingIsDeterministic() {
        let inputs = makeInputs()
        let first = TargetedWindowNarrower.narrow(
            phase: .scanRandomAuditWindows,
            inputs: inputs
        ).map(\.segmentIndex)
        let second = TargetedWindowNarrower.narrow(
            phase: .scanRandomAuditWindows,
            inputs: inputs
        ).map(\.segmentIndex)

        #expect(first == second)
    }

    @Test("harvester narrowing envelopes far-apart anchors into one contiguous range")
    func harvesterNarrowingIsContiguousAcrossFarApartAnchors() {
        let inputs = makeFarApartInputs()
        let narrowed = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs
        )
        let lineRefs = narrowed.map(\.segmentIndex)

        #expect(!lineRefs.isEmpty)
        let sorted = lineRefs.sorted()
        let contiguous = Array((sorted.first ?? 0)...(sorted.last ?? -1))
        #expect(sorted == contiguous)
    }

    @Test("likely-ad narrowing envelopes far-apart lexical hits into one contiguous range")
    func likelyAdNarrowingIsContiguousAcrossFarApartHits() {
        let inputs = makeFarApartInputs()
        let narrowed = TargetedWindowNarrower.narrow(
            phase: .scanLikelyAdSlots,
            inputs: inputs
        )
        let lineRefs = narrowed.map(\.segmentIndex)

        #expect(!lineRefs.isEmpty)
        #expect(lineRefs.contains(1))
        #expect(lineRefs.contains(7) || lineRefs.contains(8))
        let sorted = lineRefs.sorted()
        let contiguous = Array((sorted.first ?? 0)...(sorted.last ?? -1))
        #expect(sorted == contiguous)
    }

    private func makeInputs() -> TargetedWindowNarrower.Inputs {
        let segments = makeFMSegments(
            analysisAssetId: "asset-narrower",
            transcriptVersion: "tx-narrower-v1",
            lines: [
                (0, 10, "Welcome back to our technical podcast."),
                (10, 20, "This episode is brought to you by ExampleCo."),
                (20, 30, "Visit example.com slash deal and use code PLAYHEAD."),
                (30, 40, "Now back to the main interview."),
                (40, 50, "We discuss system reliability and testing."),
                (50, 60, "Thanks for listening and sharing the show.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: "asset-narrower",
            transcriptVersion: "tx-narrower-v1"
        )
        return TargetedWindowNarrower.Inputs(
            analysisAssetId: "asset-narrower",
            podcastId: "podcast-narrower",
            transcriptVersion: "tx-narrower-v1",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            auditWindowSampleRate: CoveragePlanner.defaultAuditWindowSampleRate
        )
    }

    private func makeFarApartInputs() -> TargetedWindowNarrower.Inputs {
        let segments = makeFMSegments(
            analysisAssetId: "asset-narrower-far",
            transcriptVersion: "tx-narrower-far-v1",
            lines: [
                (0, 10, "Welcome back to our technical podcast."),
                (10, 20, "This episode is brought to you by ExampleCo."),
                (20, 30, "General conversation about architecture."),
                (30, 40, "Discussion of reliability and testing."),
                (40, 50, "Audience Q and A with no ad content."),
                (50, 60, "Normal editorial content continues."),
                (60, 70, "Roadmap and release planning segment."),
                (70, 80, "Visit offerhub.com slash deal for details."),
                (80, 90, "Use promo code PLAYHEAD at checkout today."),
                (90, 100, "Thanks for listening and see you next week.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: "asset-narrower-far",
            transcriptVersion: "tx-narrower-far-v1"
        )
        return TargetedWindowNarrower.Inputs(
            analysisAssetId: "asset-narrower-far",
            podcastId: "podcast-narrower-far",
            transcriptVersion: "tx-narrower-far-v1",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            auditWindowSampleRate: CoveragePlanner.defaultAuditWindowSampleRate
        )
    }
}
