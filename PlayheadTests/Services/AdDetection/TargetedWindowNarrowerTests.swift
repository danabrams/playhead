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

    // MARK: - playhead-7q3 (Phase 4): acoustic break snap (Option D)

    @Test("playhead-7q3: nearby acoustic break snaps left edge to the break's segment")
    func acousticBreakSnapsLeftEdge() {
        // Synthetic episode: 30 segments, each one second long.
        // Anchor at segment 10 → default padded window [5, 15].
        // Place an acoustic break at time 2.5s (inside segment index 2).
        // Default snap distance is 2.0s; |segment(5).startTime - 2.5| = 2.5,
        // which is outside the snap window — so with default snap distance
        // the edge does NOT move. Bump snap distance to 3.0s for this test
        // so the break becomes reachable and the snap actually fires.
        let inputs = makeSyntheticInputs(segmentCount: 30, anchorIndices: [10])
        let withBreaks = TargetedWindowNarrower.Inputs(
            analysisAssetId: inputs.analysisAssetId,
            podcastId: inputs.podcastId,
            transcriptVersion: inputs.transcriptVersion,
            segments: inputs.segments,
            evidenceCatalog: inputs.evidenceCatalog,
            auditWindowSampleRate: inputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: inputs.episodesSinceLastFullRescan,
            acousticBreaks: [
                AcousticBreak(
                    time: 2.5,
                    breakStrength: 0.8,
                    signals: [.energyDrop, .pauseCluster]
                )
            ]
        )
        let config = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            acousticBreakSnapMaxDistanceSeconds: 3.0
        )
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withBreaks,
            config: config
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex).sorted()
        // The left edge was segment 5; the break at t=2.5 sits in segment 2.
        // Snap drift is bounded to `perAnchorPaddingSegments = 5`, so the
        // snap can pull the edge to max(5 - 5, 2) = 2. We assert the edge
        // moved to exactly segment 2.
        #expect(lineRefs.first == 2, "left edge should snap to the break's segment (got \(lineRefs.first ?? -1))")
        // The right edge should still be the default-padded 15 (no break
        // nearby) — this is the "no-break behavior unchanged" part for the
        // right side.
        #expect(lineRefs.last == 15)
    }

    @Test("playhead-7q3: no break in range leaves window at default padded edges")
    func noBreakLeavesWindowUnchanged() {
        let inputs = makeSyntheticInputs(segmentCount: 30, anchorIndices: [10])
        // One break, but 20s away from both edges — outside any reasonable
        // snap distance.
        let withBreaks = TargetedWindowNarrower.Inputs(
            analysisAssetId: inputs.analysisAssetId,
            podcastId: inputs.podcastId,
            transcriptVersion: inputs.transcriptVersion,
            segments: inputs.segments,
            evidenceCatalog: inputs.evidenceCatalog,
            auditWindowSampleRate: inputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: inputs.episodesSinceLastFullRescan,
            acousticBreaks: [
                AcousticBreak(time: 25.0, breakStrength: 0.9, signals: [.spectralSpike])
            ]
        )
        let withoutBreaks = inputs
        let withBreaksRefs = (TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withBreaks
        ).narrowedSegments ?? []).map(\.segmentIndex)
        let withoutBreaksRefs = (TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withoutBreaks
        ).narrowedSegments ?? []).map(\.segmentIndex)
        #expect(withBreaksRefs == withoutBreaksRefs)
        #expect(withoutBreaksRefs == [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
    }

    @Test("playhead-7q3: narrowing with breaks is deterministic across repeated calls")
    func narrowingWithBreaksIsDeterministic() {
        let base = makeSyntheticInputs(segmentCount: 30, anchorIndices: [10])
        let breaks = [
            AcousticBreak(time: 2.5, breakStrength: 0.8, signals: [.energyDrop]),
            AcousticBreak(time: 15.5, breakStrength: 0.6, signals: [.spectralSpike]),
            AcousticBreak(time: 16.2, breakStrength: 0.5, signals: [.pauseCluster])
        ]
        let withBreaks = TargetedWindowNarrower.Inputs(
            analysisAssetId: base.analysisAssetId,
            podcastId: base.podcastId,
            transcriptVersion: base.transcriptVersion,
            segments: base.segments,
            evidenceCatalog: base.evidenceCatalog,
            auditWindowSampleRate: base.auditWindowSampleRate,
            episodesSinceLastFullRescan: base.episodesSinceLastFullRescan,
            acousticBreaks: breaks
        )
        let first = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withBreaks
        ).narrowedSegments?.map(\.segmentIndex) ?? []
        let second = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withBreaks
        ).narrowedSegments?.map(\.segmentIndex) ?? []
        #expect(first == second)
        #expect(!first.isEmpty)
    }

    @Test("playhead-7q3: snap-induced widening that blows the cap still aborts")
    func snapRespectsCapAbort() {
        // Exercises the genuinely interesting case: a merged input whose
        // pre-snap size is comfortably under the per-phase cap, but whose
        // post-snap widening (driven by acoustic breaks placed OUTSIDE the
        // existing merged edges) pushes the total over the cap.
        //
        // Setup (padding=5, cap=60, snap distance override=6.0):
        //   - 5 anchors at [20, 50, 80, 110, 140] in a 200-segment episode.
        //   - Pre-snap: 5 disjoint windows of width 11 ([15,25], [45,55],
        //     [75,85], [105,115], [135,145]). Total = 55 < 60 cap, so
        //     the same inputs WITHOUT breaks pass the cap check.
        //   - Breaks are placed one per edge, 4.5s beyond each edge:
        //       * left break at (lo-5).midpoint so snap pulls lo -> lo-5
        //       * right break at (hi+5).midpoint so snap pushes hi -> hi+5
        //     Each window widens from 11 to 21 segs. 5 * 21 = 105 > 60
        //     → the cap check MUST fire AFTER snap and abort.
        //
        // Anchors are spaced far enough (30 segs) that neither the
        // breaks nor the widened intervals bleed across anchors.
        let anchors = [20, 50, 80, 110, 140]
        let baseInputs = makeSyntheticInputs(segmentCount: 200, anchorIndices: anchors)

        // Sanity: without breaks the same config does NOT abort (55 < 60).
        // This pins the "cap abort is snap-induced" claim.
        let baselineResult = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: baseInputs,
            config: NarrowingConfig(
                perAnchorPaddingSegments: 5,
                maxNarrowedSegmentsPerPhase: 60,
                acousticBreakSnapMaxDistanceSeconds: 6.0
            )
        )
        #expect(!baselineResult.aborted, "baseline without breaks must fit under the cap (55 < 60)")
        #expect((baselineResult.narrowedSegments ?? []).count == 55)

        // Each anchor contributes two breaks placed at the midpoint of
        // the segments that sit exactly `padding` (5) segs beyond the
        // pre-snap edges. The synthetic segment `idx` spans [idx, idx+1],
        // so the midpoint is `Double(idx) + 0.5`.
        let breaks: [AcousticBreak] = anchors.flatMap { anchor -> [AcousticBreak] in
            let lo = anchor - 5          // pre-snap left edge
            let hi = anchor + 5          // pre-snap right edge
            let leftTarget = Double(lo - 5) + 0.5   // midpoint of (lo-5)
            let rightTarget = Double(hi + 5) + 0.5  // midpoint of (hi+5)
            return [
                AcousticBreak(
                    time: leftTarget,
                    breakStrength: 0.8,
                    signals: [.energyDrop]
                ),
                AcousticBreak(
                    time: rightTarget,
                    breakStrength: 0.8,
                    signals: [.energyRise]
                )
            ]
        }
        let withBreaks = TargetedWindowNarrower.Inputs(
            analysisAssetId: baseInputs.analysisAssetId,
            podcastId: baseInputs.podcastId,
            transcriptVersion: baseInputs.transcriptVersion,
            segments: baseInputs.segments,
            evidenceCatalog: baseInputs.evidenceCatalog,
            auditWindowSampleRate: baseInputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: baseInputs.episodesSinceLastFullRescan,
            acousticBreaks: breaks
        )
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withBreaks,
            config: NarrowingConfig(
                perAnchorPaddingSegments: 5,
                maxNarrowedSegmentsPerPhase: 60,
                acousticBreakSnapMaxDistanceSeconds: 6.0
            )
        )
        #expect(result.aborted, "snap widening should push merged total past cap (expected 105 > 60)")
        #expect(result.narrowedSegments == nil)
    }

    @Test("playhead-7q3: default NarrowingConfig snaps within 2.0s of an edge")
    func defaultConfigSnapTriggersAtTwoSeconds() {
        // Pins the PRODUCTION-DEFAULT snap behavior. A future change to
        // the `acousticBreakSnapMaxDistanceSeconds` default constant
        // (which is 2.0s, matching the FeatureWindow duration) will trip
        // this test by either failing to snap or snapping too aggressively.
        //
        // Synthetic layout: 30 one-second segments; anchor at segment 10,
        // default padding=5 → pre-snap window [5, 15]. Segment 5 spans
        // [5.0, 6.0], so its startTime is 5.0. Place a break at 3.5s:
        //   distance = |5.0 - 3.5| = 1.5s < 2.0s default ✓
        //   break time 3.5 falls inside segment 3 ([3.0, 4.0]) ✓
        //   snap drift = |5 - 3| = 2 ≤ padding(5) ✓
        // Expected: left edge moves from 5 → 3. Right edge unchanged at 15
        // (no nearby break).
        let inputs = makeSyntheticInputs(segmentCount: 30, anchorIndices: [10])
        let withBreaks = TargetedWindowNarrower.Inputs(
            analysisAssetId: inputs.analysisAssetId,
            podcastId: inputs.podcastId,
            transcriptVersion: inputs.transcriptVersion,
            segments: inputs.segments,
            evidenceCatalog: inputs.evidenceCatalog,
            auditWindowSampleRate: inputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: inputs.episodesSinceLastFullRescan,
            acousticBreaks: [
                AcousticBreak(
                    time: 3.5,
                    breakStrength: 0.7,
                    signals: [.energyDrop, .pauseCluster]
                )
            ]
        )
        // NarrowingConfig.default — no override; this is the point.
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withBreaks
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex).sorted()
        #expect(lineRefs.first == 3, "default 2.0s snap should pull left edge from 5 to 3 (got \(lineRefs.first ?? -1))")
        #expect(lineRefs.last == 15, "right edge should be unchanged (no nearby break)")
        #expect(lineRefs == Array(3...15))
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
