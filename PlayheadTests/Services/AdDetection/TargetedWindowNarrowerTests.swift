import Testing

@testable import Playhead

@Suite("TargetedWindowNarrower")
struct TargetedWindowNarrowerTests {

    @Test("predicted targeted coverage is the union of all targeted phase narrowings")
    func predictedCoverageMatchesUnionOfPhaseNarrowings() {
        let inputs = makeInputs()

        var union = Set<Int>()
        for phase in BackfillJobPhase.allCases where phase != .fullEpisodeScan {
            let result = TargetedWindowNarrower.narrow(phase: phase, inputs: inputs)
            if let segments = result.narrowedSegments {
                union.formUnion(segments.map(\.segmentIndex))
            }
        }
        let predicted = TargetedWindowNarrower.predictedTargetedLineRefs(inputs: inputs)

        #expect(predicted == union)
    }

    @Test("audit narrowing is deterministic for identical inputs")
    func auditNarrowingIsDeterministic() {
        let inputs = makeInputs()
        let first = TargetedWindowNarrower.narrow(
            phase: .scanRandomAuditWindows,
            inputs: inputs
        ).narrowedSegments?.map(\.segmentIndex) ?? []
        let second = TargetedWindowNarrower.narrow(
            phase: .scanRandomAuditWindows,
            inputs: inputs
        ).narrowedSegments?.map(\.segmentIndex) ?? []

        #expect(first == second)
        #expect(!first.isEmpty)
    }

    // MARK: - Cycle 2 C5: per-anchor windows + cap

    @Test("Cycle 2 C5: harvester narrowing produces DISJOINT windows for far-apart anchors")
    func harvesterFarApartProducesDisjointWindows() {
        // Replaces the legacy `harvesterNarrowingIsContiguousAcrossFarApartAnchors`.
        // Under the per-anchor model, anchors that are >= 2*padding+1
        // segments apart MUST produce two disjoint windows rather than a
        // contiguous hull spanning the gap.
        let inputs = makeSyntheticInputs(segmentCount: 100, anchorIndices: [10, 80])
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs
        )
        let segments = try! #require(result.narrowedSegments)
        let lineRefs = segments.map(\.segmentIndex).sorted()

        let hullWidth = (lineRefs.last ?? 0) - (lineRefs.first ?? 0) + 1
        #expect(lineRefs.count < hullWidth, "narrowing must NOT return a contiguous hull")
        // And the gap must contain segments that are NOT in the result.
        #expect(!lineRefs.contains(45), "the gap between anchors must remain unscanned")
    }

    @Test("Cycle 2 C5: single anchor at segment 0 clips to [0, padding]")
    func singleAnchorAtZeroClipsLow() {
        let inputs = makeSyntheticInputs(segmentCount: 30, anchorIndices: [0])
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: NarrowingConfig(perAnchorPaddingSegments: 5, maxNarrowedSegmentsPerPhase: 60)
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex)
        #expect(lineRefs == [0, 1, 2, 3, 4, 5])
    }

    @Test("Cycle 2 C5: anchor at last segment clips to upper bound")
    func anchorAtLastClipsHigh() {
        let inputs = makeSyntheticInputs(segmentCount: 30, anchorIndices: [29])
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: NarrowingConfig(perAnchorPaddingSegments: 5, maxNarrowedSegmentsPerPhase: 60)
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex)
        #expect(lineRefs == [24, 25, 26, 27, 28, 29])
    }

    @Test("Cycle 2 C5: two anchors 100 segments apart produce two disjoint windows")
    func twoFarApartAnchorsAreDisjoint() {
        let inputs = makeSyntheticInputs(segmentCount: 200, anchorIndices: [10, 110])
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: NarrowingConfig(perAnchorPaddingSegments: 5, maxNarrowedSegmentsPerPhase: 60)
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex).sorted()
        // Two windows of width 11 each (anchor ± 5).
        #expect(lineRefs.count == 22)
        #expect(lineRefs.contains(10))
        #expect(lineRefs.contains(110))
        // Confirm there is a gap (the windows are not merged).
        #expect(!lineRefs.contains(50))
    }

    @Test("Cycle 2 C5: two anchors 3 segments apart merge into one window")
    func twoCloseAnchorsMerge() {
        let inputs = makeSyntheticInputs(segmentCount: 50, anchorIndices: [20, 23])
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: NarrowingConfig(perAnchorPaddingSegments: 5, maxNarrowedSegmentsPerPhase: 60)
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex).sorted()
        // Anchor 20 → [15..25], anchor 23 → [18..28], merged → [15..28] = 14 segs
        #expect(lineRefs == Array(15...28))
    }

    @Test("Cycle 2 C5: too many anchors abort narrowing and return aborted=true")
    func tooManyAnchorsAbortsNarrowing() {
        // 7 widely-spaced anchors at padding 5 each give 7*11 = 77 segments
        // (assuming no merging), well over the cap of 60.
        let anchors = [10, 30, 50, 70, 90, 110, 130]
        let inputs = makeSyntheticInputs(segmentCount: 200, anchorIndices: anchors)
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: NarrowingConfig(perAnchorPaddingSegments: 5, maxNarrowedSegmentsPerPhase: 60)
        )
        #expect(result.aborted)
        #expect(result.narrowedSegments == nil)
    }

    // MARK: - Cycle 2 H13: empty / wasEmpty

    @Test("Cycle 2 H13: harvester with no evidence anchors returns wasEmpty")
    func harvesterEmptyAnchorsReturnsWasEmpty() {
        let inputs = makeSyntheticInputs(segmentCount: 30, anchorIndices: [])
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs
        )
        #expect(result.wasEmpty)
        #expect(result.narrowedSegments == nil)
    }

    // MARK: - Cycle 2 C4: recallSample formula direction

    @Test("Cycle 2 C4: recallSample = covered / actual (full overlap)")
    func recallSampleFullOverlap() {
        let predicted: Set<Int> = [1, 2, 3, 4, 5]
        let actual: Set<Int> = [1, 2, 3]
        let recall = TargetedWindowNarrower.recallSample(
            predictedTargetedLineRefs: predicted,
            actualAdLineRefs: actual
        )
        #expect(recall == 1.0)
    }

    @Test("Cycle 2 C4: recallSample = 0.0 when prediction misses everything")
    func recallSampleZeroOverlap() {
        let predicted: Set<Int> = [10, 11, 12]
        let actual: Set<Int> = [1, 2, 3]
        let recall = TargetedWindowNarrower.recallSample(
            predictedTargetedLineRefs: predicted,
            actualAdLineRefs: actual
        )
        #expect(recall == 0.0)
    }

    @Test("Cycle 2 C4: recallSample partial overlap = covered / |actual|")
    func recallSamplePartialOverlap() {
        let predicted: Set<Int> = [1, 5, 6]
        let actual: Set<Int> = [1, 2, 3, 4]
        let recall = TargetedWindowNarrower.recallSample(
            predictedTargetedLineRefs: predicted,
            actualAdLineRefs: actual
        )
        #expect(recall == 0.25)
    }

    @Test("Cycle 2 C4: recallSample is nil when actual is empty (ad-free episode)")
    func recallSampleNilOnAdFreeEpisode() {
        let recall = TargetedWindowNarrower.recallSample(
            predictedTargetedLineRefs: [1, 2, 3],
            actualAdLineRefs: []
        )
        #expect(recall == nil)
    }

    // MARK: - Cycle 2 Rev3-L1: duplicate atom ordinal precondition

    // Note: precondition fires only in debug builds; we cannot exercise it
    // safely from a normal #expect. The precondition's role is to fail
    // loudly during dev/test runs if the atomizer ever emits duplicates
    // — the bare existence of the precondition is the rail.

    // MARK: - Cycle 2 Rev3-M3: audit seed rotates with episodesSinceLastFullRescan

    @Test("Cycle 2 Rev3-M3: audit seed rotates with episodesSinceLastFullRescan")
    func auditSeedRotatesAcrossObservations() {
        let baseInputs = makeInputs()
        let zero = TargetedWindowNarrower.Inputs(
            analysisAssetId: baseInputs.analysisAssetId,
            podcastId: baseInputs.podcastId,
            transcriptVersion: baseInputs.transcriptVersion,
            segments: baseInputs.segments,
            evidenceCatalog: baseInputs.evidenceCatalog,
            auditWindowSampleRate: baseInputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: 0
        )
        let one = TargetedWindowNarrower.Inputs(
            analysisAssetId: baseInputs.analysisAssetId,
            podcastId: baseInputs.podcastId,
            transcriptVersion: baseInputs.transcriptVersion,
            segments: baseInputs.segments,
            evidenceCatalog: baseInputs.evidenceCatalog,
            auditWindowSampleRate: baseInputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: 1
        )
        let segs0 = TargetedWindowNarrower.narrow(phase: .scanRandomAuditWindows, inputs: zero)
            .narrowedSegments?.map(\.segmentIndex) ?? []
        let segs1 = TargetedWindowNarrower.narrow(phase: .scanRandomAuditWindows, inputs: one)
            .narrowedSegments?.map(\.segmentIndex) ?? []
        // Different seed material → different audit window picks at least
        // once across a small batch of consecutive observations.
        var sawDifferent = (segs0 != segs1)
        for tick in 2...8 {
            let candidate = TargetedWindowNarrower.Inputs(
                analysisAssetId: baseInputs.analysisAssetId,
                podcastId: baseInputs.podcastId,
                transcriptVersion: baseInputs.transcriptVersion,
                segments: baseInputs.segments,
                evidenceCatalog: baseInputs.evidenceCatalog,
                auditWindowSampleRate: baseInputs.auditWindowSampleRate,
                episodesSinceLastFullRescan: tick
            )
            let nextSegs = TargetedWindowNarrower.narrow(phase: .scanRandomAuditWindows, inputs: candidate)
                .narrowedSegments?.map(\.segmentIndex) ?? []
            if nextSegs != segs0 {
                sawDifferent = true
            }
        }
        #expect(sawDifferent, "audit seed must rotate across consecutive observations")
    }

    // MARK: - Cycle 2 Rev3-M2: BackfillJobPhase enumeration is pinned

    @Test("Cycle 2 Rev3-M2: BackfillJobPhase.allCases set is exactly the four known cases")
    func backfillJobPhaseAllCasesPinned() {
        let observed = Set(BackfillJobPhase.allCases)
        let expected: Set<BackfillJobPhase> = [
            .fullEpisodeScan,
            .scanHarvesterProposals,
            .scanLikelyAdSlots,
            .scanRandomAuditWindows
        ]
        #expect(observed == expected, "Adding a new BackfillJobPhase requires updating predictedTargetedLineRefs and the narrowing exclusion set")
    }

    // MARK: - Helpers

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

    /// Synthetic builder: produces `segmentCount` placeholder segments
    /// and an evidence catalog whose entries point at the supplied
    /// `anchorIndices`. Each segment is one second long. The evidence
    /// catalog uses the segment's first atom ordinal as the entry's
    /// `atomOrdinal`, so `evidenceLineRefs(...)` resolves the catalog
    /// entry back to the corresponding `segmentIndex`.
    private func makeSyntheticInputs(
        segmentCount: Int,
        anchorIndices: [Int]
    ) -> TargetedWindowNarrower.Inputs {
        let assetId = "asset-syn-\(segmentCount)-\(anchorIndices.count)"
        let transcriptVersion = "tx-syn-v1"
        let lines: [(start: Double, end: Double, text: String)] =
            (0..<segmentCount).map { idx in
                (Double(idx), Double(idx + 1), "synthetic line \(idx) with neutral text")
            }
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        // Build a tiny evidence catalog: one entry per requested anchor.
        // The catalog entries reference the first atom ordinal of the
        // corresponding segment so the narrower's
        // `evidenceLineRefs` lookup resolves them back to the right
        // segment index. We bypass `EvidenceCatalogBuilder` here because
        // it derives entries from atoms via the lexical/sponsor
        // pipeline; for these synthetic tests we want exactly the
        // anchors we asked for.
        let entries: [EvidenceEntry] = anchorIndices.compactMap { anchorIndex in
            guard anchorIndex >= 0, anchorIndex < segments.count else { return nil }
            let segment = segments[anchorIndex]
            let firstAtom = segment.atoms.first
            return EvidenceEntry(
                evidenceRef: anchorIndex,
                category: .brandSpan,
                matchedText: "synthetic-\(anchorIndex)",
                normalizedText: "synthetic-\(anchorIndex)",
                atomOrdinal: firstAtom?.atomKey.atomOrdinal ?? 0,
                startTime: segment.startTime,
                endTime: segment.endTime
            )
        }
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            entries: entries
        )
        return TargetedWindowNarrower.Inputs(
            analysisAssetId: assetId,
            podcastId: "podcast-syn",
            transcriptVersion: transcriptVersion,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            auditWindowSampleRate: CoveragePlanner.defaultAuditWindowSampleRate
        )
    }
}
