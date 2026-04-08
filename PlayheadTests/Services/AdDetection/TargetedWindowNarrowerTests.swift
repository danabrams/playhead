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
}
