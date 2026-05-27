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
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex).sorted()
        // Anchor at 29: 20-atom lookback adds refs {9..28}, window [4..29] = 26 segs.
        // Upper bound clips correctly at segment 29.
        #expect(lineRefs == Array(4...29))
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
        // Anchor 10: 20-atom lookback capped at 0 → refs {0..10}, window [0..15] = 16 segs.
        // Anchor 110: lookback {90..110}, window [85..115] = 31 segs.
        // Total 47. Gap between 15 and 85 confirms the windows are disjoint.
        #expect(lineRefs.count == 47)
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
        // Anchor 20: lookback {0..19} → refs {0..20}, window [0..25].
        // Anchor 23: lookback {3..22} → refs {3..23}, window [0..28].
        // Merged: [0..28] = 29 segs.
        #expect(lineRefs == Array(0...28))
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
        // Anchor at 10: 20-atom lookback adds refs {0..9}, window [0..15] = 16 segs.
        // Break at 25s is too far from both edges to trigger any snap.
        #expect(withoutBreaksRefs == Array(0...15))
    }

    @Test("playhead-9ua.2: realistic nearby energy-rise break snaps the right edge through the resolver")
    func acousticBreakSnapsRightEdge() {
        let inputs = makeSyntheticInputs(segmentCount: 50, anchorIndices: [20])
        let withBreaks = TargetedWindowNarrower.Inputs(
            analysisAssetId: inputs.analysisAssetId,
            podcastId: inputs.podcastId,
            transcriptVersion: inputs.transcriptVersion,
            segments: inputs.segments,
            evidenceCatalog: inputs.evidenceCatalog,
            auditWindowSampleRate: inputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: inputs.episodesSinceLastFullRescan,
            acousticBreaks: [
                AcousticBreak(time: 26.5, breakStrength: 0.4, signals: [.energyRise])
            ]
        )
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withBreaks
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex).sorted()

        #expect(lineRefs.first == 0)
        #expect(lineRefs.last == 26, "right edge should snap from 25 to the break's segment (got \(lineRefs.last ?? -1))")
    }

    @Test("playhead-9ua.2: weak nearby breaks do not qualify for snapping")
    func weakBreakDoesNotSnap() {
        let inputs = makeSyntheticInputs(segmentCount: 50, anchorIndices: [30])
        let withWeakBreak = TargetedWindowNarrower.Inputs(
            analysisAssetId: inputs.analysisAssetId,
            podcastId: inputs.podcastId,
            transcriptVersion: inputs.transcriptVersion,
            segments: inputs.segments,
            evidenceCatalog: inputs.evidenceCatalog,
            auditWindowSampleRate: inputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: inputs.episodesSinceLastFullRescan,
            acousticBreaks: [
                AcousticBreak(time: 3.5, breakStrength: 0.1, signals: [.energyDrop])
            ]
        )

        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withWeakBreak
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex).sorted()

        #expect(lineRefs == Array(5...35))
    }

    @Test("playhead-9ua.2: weak energy-only breaks near the edge do not inherit spectral score")
    func weakEnergyOnlyBreakNearEdgeDoesNotSnap() {
        let inputs = makeSyntheticInputs(segmentCount: 50, anchorIndices: [30])
        let withWeakBreak = TargetedWindowNarrower.Inputs(
            analysisAssetId: inputs.analysisAssetId,
            podcastId: inputs.podcastId,
            transcriptVersion: inputs.transcriptVersion,
            segments: inputs.segments,
            evidenceCatalog: inputs.evidenceCatalog,
            auditWindowSampleRate: inputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: inputs.episodesSinceLastFullRescan,
            acousticBreaks: [
                AcousticBreak(time: 4.9, breakStrength: 0.1, signals: [.energyDrop])
            ]
        )

        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withWeakBreak
        )
        let lineRefs = (result.narrowedSegments ?? []).map(\.segmentIndex).sorted()

        #expect(lineRefs == Array(5...35))
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
        // post-snap widening (driven by an acoustic break placed OUTSIDE the
        // existing merged edge) pushes the total over the cap.
        //
        // Setup (padding=5, cap=27, snap distance=3.0):
        //   - Single anchor at segment 20 in a 50-segment episode.
        //   - 20-atom lookback → refs {0..20}, window [0..25] = 26 segs < 27 cap.
        //   - A break at 27.5s sits in segment 27 (just beyond the right edge at 25).
        //     distance = |25.0 - 27.5| = 2.5s < 3.0s snap distance ✓
        //     snap drift = |25 - 27| = 2 ≤ padding(5) ✓
        //   - Right edge snaps from 25 → 27 → window [0..27] = 28 > 27 cap → ABORTS.
        let baseInputs = makeSyntheticInputs(segmentCount: 50, anchorIndices: [20])
        let snapConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 27,
            acousticBreakSnapMaxDistanceSeconds: 3.0
        )

        // Sanity: without a break the baseline fits under the cap (26 < 27).
        let baselineResult = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: baseInputs,
            config: snapConfig
        )
        #expect(!baselineResult.aborted, "baseline without breaks must fit under the cap (26 < 27)")
        #expect((baselineResult.narrowedSegments ?? []).count == 26)

        // Break at 27.5s snaps the right edge from 25 → 27, giving 28 > 27 cap.
        let withBreaks = TargetedWindowNarrower.Inputs(
            analysisAssetId: baseInputs.analysisAssetId,
            podcastId: baseInputs.podcastId,
            transcriptVersion: baseInputs.transcriptVersion,
            segments: baseInputs.segments,
            evidenceCatalog: baseInputs.evidenceCatalog,
            auditWindowSampleRate: baseInputs.auditWindowSampleRate,
            episodesSinceLastFullRescan: baseInputs.episodesSinceLastFullRescan,
            acousticBreaks: [
                AcousticBreak(time: 27.5, breakStrength: 0.8, signals: [.energyRise])
            ]
        )
        let result = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: withBreaks,
            config: snapConfig
        )
        #expect(result.aborted, "snap widening should push merged total past cap (28 > 27)")
        #expect(result.narrowedSegments == nil)
    }

    @Test("playhead-7q3: default NarrowingConfig snaps within 2.0s of an edge")
    func defaultConfigSnapTriggersAtTwoSeconds() {
        // Pins the PRODUCTION-DEFAULT snap behavior. A future change to
        // the `acousticBreakSnapMaxDistanceSeconds` default constant
        // (which is 2.0s, matching the FeatureWindow duration) will trip
        // this test by either failing to snap or snapping too aggressively.
        //
        // Anchor at segment 30 (50-segment episode). 20-atom lookback gives
        // refs {10..30}, window [5..35]. Segment 5 spans [5.0, 6.0], so its
        // startTime is 5.0. Place a break at 3.5s:
        //   distance = |5.0 - 3.5| = 1.5s < 2.0s default ✓
        //   break time 3.5 falls inside segment 3 ([3.0, 4.0]) ✓
        //   snap drift = |5 - 3| = 2 ≤ padding(5) ✓
        // Expected: left edge moves from 5 → 3. Right edge unchanged at 35.
        // (Anchor at 10 would give window [0..15] — edge at 0 — too far from
        // break at 3.5 to demonstrate the 2.0s default threshold clearly.)
        let inputs = makeSyntheticInputs(segmentCount: 50, anchorIndices: [30])
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
        #expect(lineRefs.last == 35, "right edge should be unchanged (no nearby break)")
        #expect(lineRefs == Array(3...35))
    }

    // MARK: - playhead-xsdz.2: lexical-cue-cluster tightening

    /// Build a synthetic episode where specific segment indices carry real
    /// ad-cue text (so `LexicalScanner.collectHits` fires inside them) and the
    /// rest is neutral. One anchor at `anchorIndex` seeds the harvester window.
    /// Each segment is one second long, so segment index == start time.
    private func makeLexicalClusterInputs(
        segmentCount: Int,
        anchorIndex: Int,
        adCueSegments: [Int: String]
    ) -> TargetedWindowNarrower.Inputs {
        let assetId = "asset-lexcluster-\(segmentCount)-\(anchorIndex)"
        let transcriptVersion = "tx-lexcluster-v1"
        let lines: [(start: Double, end: Double, text: String)] =
            (0..<segmentCount).map { idx in
                let text = adCueSegments[idx] ?? "neutral conversation line \(idx) about the topic"
                return (Double(idx), Double(idx + 1), text)
            }
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        // Seed exactly one evidence anchor at `anchorIndex` so the harvester
        // phase builds a padded window around it, independent of where the
        // ad-cue text lands. (We test the SNAP, not the anchor seeding.)
        let segment = segments[anchorIndex]
        let entries: [EvidenceEntry] = [
            EvidenceEntry(
                evidenceRef: anchorIndex,
                category: .brandSpan,
                matchedText: "synthetic-\(anchorIndex)",
                normalizedText: "synthetic-\(anchorIndex)",
                atomOrdinal: segment.atoms.first?.atomKey.atomOrdinal ?? 0,
                startTime: segment.startTime,
                endTime: segment.endTime
            )
        ]
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            entries: entries
        )
        return TargetedWindowNarrower.Inputs(
            analysisAssetId: assetId,
            podcastId: "podcast-lexcluster",
            transcriptVersion: transcriptVersion,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            auditWindowSampleRate: CoveragePlanner.defaultAuditWindowSampleRate
        )
    }

    @Test("xsdz.2: a dense ad-cue cluster tightens both edges inward toward the cluster")
    func lexicalClusterTightensBothEdges() {
        // 30-segment episode, anchor at 10 (default lookback would give window
        // [0..15]). Place a tight cluster of ad cues in segments 8..12.
        // With a 1-segment ad-body margin the tightened window is the cluster
        // [8..12] widened by 1 each side → [7..13], intersected with [0..15].
        let inputs = makeLexicalClusterInputs(
            segmentCount: 30,
            anchorIndex: 10,
            adCueSegments: [
                8: "this episode is brought to you by acme tools",
                9: "use code SAVE at checkout for a free trial",
                10: "visit acmetools.com for this special offer",
                11: "promo code SAVE gets you a money back guarantee",
                12: "sign up now at acmetools.com slash deal"
            ]
        )
        // Use a tight 1-segment margin so the snap is crisp on 1s segments.
        let snapConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            lexicalClusterMarginSegments: 1
        )
        let withSnap = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: snapConfig
        ).narrowedSegments?.map(\.segmentIndex).sorted() ?? []

        // Baseline: acoustic-only shaping (lexical cluster snap disabled).
        let baselineConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            lexicalClusterSnapEnabled: false
        )
        let baseline = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: baselineConfig
        ).narrowedSegments?.map(\.segmentIndex).sorted() ?? []

        // Baseline window is the default-padded [0..15].
        #expect(baseline == Array(0...15))
        // Tightened window is strictly narrower and keeps the cue core + margin.
        #expect(withSnap.count < baseline.count, "lexical snap must tighten the window")
        #expect(withSnap.first == 7, "left edge should tighten up to cluster start (8) minus 1-seg margin → 7")
        #expect(withSnap.last == 13, "right edge should tighten down to cluster end (12) plus 1-seg margin → 13")
        #expect(withSnap.contains(10), "the ad core must remain covered")
        #expect(withSnap.contains(11), "the ad core must remain covered")
    }

    @Test("xsdz.2: ad-body margin keeps spoken ad seconds around the literal cues")
    func lexicalClusterMarginKeepsAdBody() {
        // Same cluster (8..12) but with the DEFAULT margin (corpus-tuned to 3).
        // The window must NOT collapse to the bare cue cluster; it keeps a
        // 3-segment ad-body margin so the pre-CTA pitch / post-disclosure
        // wind-down survive. [8-3, 12+3] = [5,15] ∩ [0..15] = [5..15].
        let inputs = makeLexicalClusterInputs(
            segmentCount: 30,
            anchorIndex: 10,
            adCueSegments: [
                8: "this episode is brought to you by acme tools",
                9: "use code SAVE at checkout for a free trial",
                10: "visit acmetools.com for this special offer",
                11: "promo code SAVE gets you a money back guarantee",
                12: "sign up now at acmetools.com slash deal"
            ]
        )
        let withSnap = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs // default config: margin == 3
        ).narrowedSegments?.map(\.segmentIndex).sorted() ?? []
        #expect(withSnap == Array(5...15), "default margin (3) keeps ad-body seconds; only the far-left editorial padding (0..4) is trimmed")
        #expect(withSnap.count < 16, "must tighten vs the [0..15] baseline")
    }

    @Test("xsdz.2: no ad-cue cluster leaves the window at its acoustic-shaped edges")
    func noClusterLeavesWindowUnchanged() {
        // Neutral text everywhere → no ad-cue hits → no cluster → no snap.
        let inputs = makeLexicalClusterInputs(
            segmentCount: 30,
            anchorIndex: 10,
            adCueSegments: [:]
        )
        let withSnap = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs
        ).narrowedSegments?.map(\.segmentIndex).sorted() ?? []
        let baselineConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            lexicalClusterSnapEnabled: false
        )
        let baseline = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: baselineConfig
        ).narrowedSegments?.map(\.segmentIndex).sorted() ?? []
        #expect(withSnap == baseline, "no cluster ⇒ tightening must be a no-op")
        #expect(withSnap == Array(0...15))
    }

    @Test("xsdz.2: with multiple clusters the densest one inside the window wins")
    func multipleClustersDensestWins() {
        // Two clusters inside a window: a sparse 1-hit cluster near the left
        // edge and a dense multi-hit cluster near the right. The densest
        // (right) cluster should drive the tightening, NOT the sparse one.
        // Anchor at 20 → default lookback window [0..25] in a 40-seg episode.
        // Sparse cluster: a single sponsor hit at segment 3 (isolated).
        // Dense cluster: segments 18..23 (5 hits, > gap apart from seg 3).
        let inputs = makeLexicalClusterInputs(
            segmentCount: 40,
            anchorIndex: 20,
            adCueSegments: [
                3: "this episode is brought to you by globex",
                18: "use code DEAL at checkout today",
                19: "visit globex.com for a free trial",
                20: "promo code DEAL gives a money back guarantee",
                21: "sign up now for this special offer",
                22: "head to globex.com slash save",
                23: "use code DEAL for first month free"
            ]
        )
        let snapConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            lexicalClusterMarginSegments: 1
        )
        let withSnap = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: snapConfig
        ).narrowedSegments?.map(\.segmentIndex).sorted() ?? []
        // The dense right cluster (18..23) wins: the window re-centers on it
        // (with a 1-seg margin → [17..24]), not on the lone seg-3 hit.
        #expect(withSnap.first == 17, "densest (right) cluster (start 18) minus 1-seg margin → 17; the lone seg-3 hit must NOT drive tightening")
        #expect(withSnap.last == 24, "dense cluster end (23) plus 1-seg margin → 24")
        #expect(withSnap.contains(20))
        #expect(withSnap.contains(22))
    }

    @Test("xsdz.2: a cluster at the window's edge does not push the edge outward")
    func clusterAtEdgeDoesNotWiden() {
        // Cluster sits flush against the LEFT edge of the padded window.
        // Anchor at 5 → 20-atom lookback caps at 0 → window [0..10].
        // Ad cues in segments 0..3 (a cluster pinned to the left edge).
        // Inward-only tightening means the LEFT edge cannot move past 0, and
        // the RIGHT edge should pull DOWN to the cluster end (~3), never out.
        let inputs = makeLexicalClusterInputs(
            segmentCount: 30,
            anchorIndex: 5,
            adCueSegments: [
                0: "this episode is brought to you by initech",
                1: "use code BOSS at checkout for a free trial",
                2: "visit initech.com slash offer today",
                3: "promo code BOSS for a money back guarantee"
            ]
        )
        let snapConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            lexicalClusterMarginSegments: 1
        )
        let withSnap = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: snapConfig
        ).narrowedSegments?.map(\.segmentIndex).sorted() ?? []
        let baselineConfig = NarrowingConfig(
            perAnchorPaddingSegments: 5,
            maxNarrowedSegmentsPerPhase: 60,
            lexicalClusterSnapEnabled: false
        )
        let baseline = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs,
            config: baselineConfig
        ).narrowedSegments?.map(\.segmentIndex).sorted() ?? []

        #expect(baseline == Array(0...10))
        // Cluster segs 0..3, margin 1 → [-1,4] ∩ [0..10] = [0..4]. Left edge
        // can't move outward below 0; right tightens down to 4.
        #expect(withSnap.first == 0, "inward-only snap must not push the left edge below 0")
        #expect(withSnap.last == 4, "right edge tightens to cluster end (3) plus 1-seg margin → 4")
        #expect(withSnap.count <= baseline.count, "tightening must never widen the window")
    }

    @Test("xsdz.2: lexical-cluster tightening is deterministic across repeated calls")
    func lexicalClusterTighteningIsDeterministic() {
        let inputs = makeLexicalClusterInputs(
            segmentCount: 30,
            anchorIndex: 10,
            adCueSegments: [
                8: "this episode is brought to you by acme",
                9: "use code SAVE at checkout",
                10: "visit acme.com for a free trial",
                11: "promo code SAVE for a special offer"
            ]
        )
        let first = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs
        ).narrowedSegments?.map(\.segmentIndex) ?? []
        let second = TargetedWindowNarrower.narrow(
            phase: .scanHarvesterProposals,
            inputs: inputs
        ).narrowedSegments?.map(\.segmentIndex) ?? []
        #expect(first == second)
        #expect(!first.isEmpty)
    }

    @Test("xsdz.2: tightening never widens — covered set is a subset of the acoustic baseline")
    func lexicalClusterIsAlwaysSubsetOfBaseline() {
        // Property check across several anchor placements: the lexical-cluster
        // snap is inward-only, so for any input its covered segment set must be
        // a subset of (or equal to) the acoustic-only baseline's covered set.
        for anchor in [4, 10, 18, 25] {
            let inputs = makeLexicalClusterInputs(
                segmentCount: 40,
                anchorIndex: anchor,
                adCueSegments: [
                    max(0, anchor - 2): "this episode is brought to you by widgetco",
                    anchor: "use code WIDGET at checkout for a free trial",
                    min(39, anchor + 2): "visit widgetco.com slash deal special offer"
                ]
            )
            let baselineConfig = NarrowingConfig(
                perAnchorPaddingSegments: 5,
                maxNarrowedSegmentsPerPhase: 60,
                lexicalClusterSnapEnabled: false
            )
            let baseline = Set(TargetedWindowNarrower.narrow(
                phase: .scanHarvesterProposals,
                inputs: inputs,
                config: baselineConfig
            ).narrowedSegments?.map(\.segmentIndex) ?? [])
            let withSnap = Set(TargetedWindowNarrower.narrow(
                phase: .scanHarvesterProposals,
                inputs: inputs
            ).narrowedSegments?.map(\.segmentIndex) ?? [])
            #expect(withSnap.isSubset(of: baseline), "anchor \(anchor): lexical snap must never add segments")
        }
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

    @Test("Cycle 2 Rev3-M2: BackfillJobPhase.allCases set is exactly the five known cases")
    func backfillJobPhaseAllCasesPinned() {
        let observed = Set(BackfillJobPhase.allCases)
        let expected: Set<BackfillJobPhase> = [
            .fullEpisodeScan,
            .scanHarvesterProposals,
            .scanLikelyAdSlots,
            .scanRandomAuditWindows,
            .metadataSeededRegion
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
