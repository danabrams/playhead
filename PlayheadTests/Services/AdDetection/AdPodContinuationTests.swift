// AdPodContinuationTests.swift
// playhead-xsdz.65: contract coverage for ad-pod continuation — recovering the
// pod NEIGHBOURS of an ad we already found.
//
// The suite is organised around the two things that must be true at once:
//
//   RECALL — a seed sitting at the tail of a multi-ad pod recovers the earlier
//   creatives, and the recovery is non-vacuous (the same fixture with no
//   ad-copy links yields nothing, so a passing assertion cannot be an artifact
//   of the fixture).
//
//   SAFETY — and this is the half that matters. Every second claimed lands on
//   positive ad evidence; the walk stops at a POSITIVE content-resumed barrier;
//   an absence (`.uncertain` / `.abstain` FM, poor transcript quality) is never
//   a barrier AND never licenses a claim; a listener's mark is never crossed;
//   and nothing emitted can auto-skip. `stopsAtContentBarrier` is the load-
//   bearing one: its two arms differ ONLY in whether the barrier is present, so
//   deleting the stopping condition makes it fail.

import Foundation
import Testing
@testable import Playhead

@Suite("AdPodContinuation (playhead-xsdz.65 pod completeness)")
struct AdPodContinuationTests {

    private static let assetId = "asset-pod-1"

    // MARK: - Helpers

    private func window(
        id: String = UUID().uuidString,
        start: Double,
        end: Double,
        confidence: Double = 0.92,
        decisionState: AdDecisionState = .confirmed,
        boundaryState: String = AdBoundaryState.acousticRefined.rawValue,
        eligibilityGate: SkipEligibilityGate? = .eligible,
        detectorVersion: String = "detection-v1"
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: Self.assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: boundaryState,
            decisionState: decisionState.rawValue,
            detectorVersion: detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: eligibilityGate?.rawValue
        )
    }

    private func link(_ start: Double, _ end: Double) -> AdPodContinuation.AdCopyLink {
        AdPodContinuation.AdCopyLink(start: start, end: end)
    }

    private func barrier(_ start: Double, _ end: Double) -> AdPodContinuation.ContentBarrier {
        AdPodContinuation.ContentBarrier(start: start, end: end)
    }

    /// The `conan-2026-07-09` pod, reduced to its geometry. The rediff-confirmed
    /// slot ends at 810.3; the transcript shows four creatives (a SiriusXM
    /// cross-promo, Carvana, Carter's, DSW) running ~657.6 → 810.7, and the
    /// pipeline emitted only `[797.2, 810.7)` — the tail of the fourth. The three
    /// earlier creatives are the ads the listener actually hears play.
    private enum ConanPod {
        static let seed = (start: 797.2, end: 810.7)
        static let links: [(Double, Double)] = [
            (669.0, 714.5),   // SiriusXM / What Now cross-promo CTA
            (729.7, 746.0),   // Carvana
            (751.7, 774.4),   // Carter's
            (778.6, 797.2)    // DSW (runs into the seed)
        ]
        static let episodeDuration = 1470.0
    }

    private func conanCompose(
        barriers: [AdPodContinuation.ContentBarrier] = [],
        protectedRegions: [(start: Double, end: Double)] = [],
        links: [(Double, Double)] = ConanPod.links,
        config: AdPodContinuation.Configuration = .default
    ) -> [AdWindow] {
        AdPodContinuation.compose(
            existingWindows: [window(start: ConanPod.seed.start, end: ConanPod.seed.end)],
            adCopyLinks: links.map { link($0.0, $0.1) },
            contentBarriers: barriers,
            protectedRegions: protectedRegions,
            episodeDuration: ConanPod.episodeDuration,
            analysisAssetId: Self.assetId,
            config: config
        )
    }

    // MARK: - Recall: the pod's earlier creatives come back

    @Test("a seed at the pod tail recovers the earlier creatives as one mark")
    func recoversEarlierPodCreatives() throws {
        let marks = conanCompose()
        #expect(marks.count == 1)
        let mark = try #require(marks.first)
        #expect(mark.startTime == 669.0, "walk must reach the FIRST link's start")
        #expect(mark.endTime == ConanPod.seed.start, "mark abuts the seed, never overlaps it")
    }

    /// Non-vacuity. Identical seed, identical everything — only the ad-copy
    /// evidence is removed. If this produced a mark, the recall test above would
    /// be measuring the fixture rather than the rule.
    @Test("no ad-copy evidence in the hole means no mark (non-vacuous)")
    func noLinksNoMarks() {
        #expect(conanCompose(links: []).isEmpty)
    }

    @Test("the walk terminates AT the last link, never past it")
    func neverClaimsPastTheLastLink() throws {
        // One link, then a long evidence-free run before the seed. Only the link
        // may be annexed; the run between 500 and the link start stays unclaimed.
        let marks = AdPodContinuation.compose(
            existingWindows: [window(start: 800.0, end: 815.0)],
            adCopyLinks: [link(780.0, 795.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470.0,
            analysisAssetId: Self.assetId
        )
        let mark = try #require(marks.first)
        #expect(marks.count == 1)
        #expect(mark.startTime.bitPattern == (780.0 as Double).bitPattern)
        #expect(mark.endTime.bitPattern == (800.0 as Double).bitPattern)
    }

    @Test("a chain extends in BOTH directions from one seed")
    func extendsBothDirections() {
        let marks = AdPodContinuation.compose(
            existingWindows: [window(start: 1000.0, end: 1020.0)],
            adCopyLinks: [link(960.0, 990.0), link(1030.0, 1060.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        #expect(marks.count == 2)
        #expect(marks.map(\.startTime) == [960.0, 1020.0], "output is start-ordered")
        #expect(marks.map(\.endTime) == [1000.0, 1060.0])
    }

    // MARK: - Safety: the positive content-resumed barrier

    /// THE LOAD-BEARING TEST. A pod followed immediately by show content that
    /// itself carries ad-copy-shaped evidence (a host mentioning a brand and a
    /// URL editorially — the exact case where "no ad cue found" reasoning would
    /// happily keep walking). The two arms differ ONLY in whether the positive
    /// content-resumed barrier is supplied.
    ///
    /// Arm 1 (barrier present): the walk stops at the pod.
    /// Arm 2 (barrier absent):  the post-pod link IS annexed.
    ///
    /// So if the stopping condition is removed from the walk, arm 1 produces
    /// arm 2's answer and this test fails.
    @Test("a pod followed immediately by show stops at the pod")
    func stopsAtContentBarrier() {
        let seed = window(start: 700.0, end: 760.0)
        let links = [link(770.0, 800.0)]   // post-pod, in show territory
        let showBarrier = [barrier(762.0, 900.0)]

        let stopped = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: links,
            contentBarriers: showBarrier,
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        #expect(stopped.isEmpty, "the walk must not cross a positive content-resumed verdict")

        let unstopped = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: links,
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        #expect(unstopped.count == 1, "control arm: without the barrier the same link IS annexed")
        #expect(unstopped.first?.endTime == 800.0)
    }

    @Test("a barrier mid-chain truncates the walk at the barrier instead of hopping it")
    func barrierTruncatesRatherThanSkips() throws {
        // Links at 900 and 1000; a barrier sits between them. The first link is
        // annexed, the second is on the far side of a wall and must not be.
        let marks = AdPodContinuation.compose(
            existingWindows: [window(start: 860.0, end: 880.0)],
            adCopyLinks: [link(885.0, 900.0), link(905.0, 930.0)],
            contentBarriers: [barrier(901.0, 904.0)],
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        let mark = try #require(marks.first)
        #expect(marks.count == 1)
        #expect(mark.endTime == 900.0, "stops at the first link; the wall is not stepped over")
    }

    /// An absence must never become a barrier — and must never become evidence
    /// either. Only an FM `noAds` verdict over good-quality transcript is an
    /// affirmative statement that the audio is show content.
    @Test("only an affirmative FM noAds over good transcript becomes a barrier")
    func barrierDerivationRejectsAbsences() {
        let rows = [
            scanRow(0, 100, .noAds, .success, .good),          // barrier
            scanRow(200, 300, .uncertain, .success, .good),    // absence — not a barrier
            scanRow(400, 500, .abstain, .refusal, .good),      // absence — not a barrier
            scanRow(600, 700, .containsAd, .success, .good),   // positive AD — not a barrier
            scanRow(800, 900, .noAds, .success, .degraded),        // too weak to act on
            scanRow(1000, 1100, .noAds, .failedTransient, .good)  // never answered
        ]
        let barriers = AdPodContinuation.contentBarriers(
            semanticScanResults: rows,
            lexicalHits: []
        )
        #expect(barriers.count == 1)
        #expect(barriers.first == barrier(0, 100))
    }

    @Test("an explicit spoken return marker is a barrier, with a small radius")
    func returnMarkerIsABarrier() {
        let hit = LexicalHit(
            category: .transitionMarker,
            matchedText: "and now back to the show",
            startTime: 500.0,
            endTime: 503.0,
            weight: 0.3
        )
        let barriers = AdPodContinuation.contentBarriers(
            semanticScanResults: [],
            lexicalHits: [hit]
        )
        #expect(barriers.count == 1)
        let radius = AdPodContinuation.returnMarkerBarrierRadius
        #expect(barriers.first == barrier(500.0 - radius, 503.0 + radius))
    }

    /// The defect an audit caught on real data: a host sign-off sitting between a
    /// confirmed sponsor CTA and a post-roll pod, bridged by the chain, claiming
    /// ~9 s of show as ad. Two arms differing ONLY in whether the sign-off prose
    /// is visible to the barrier derivation.
    @Test("a host sign-off between two ads is a barrier and stops the bridge")
    func hostSignOffIsABarrier() {
        let signOff = TranscriptChunk(
            id: "signoff",
            analysisAssetId: Self.assetId,
            segmentFingerprint: "fp",
            chunkIndex: 0,
            startTime: 2966.8,
            endTime: 2973.1,
            text: "Wow, we were all over the place today. Thanks for tuning in everybody. See you tomorrow.",
            normalizedText: "wow, we were all over the place today. thanks for tuning in everybody. see you tomorrow.",
            pass: "final",
            modelVersion: "test",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
        let barriers = AdPodContinuation.contentBarriers(
            semanticScanResults: [],
            lexicalHits: [],
            chunks: [signOff]
        )
        #expect(barriers == [barrier(2966.8, 2973.1)])

        // The bridge: a confirmed sponsor CTA, the sign-off, then the post-roll pod.
        let seed = window(start: 2952.3, end: 2964.1)
        let postRollLink = [link(2973.1, 3025.6)]
        let bridged = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: postRollLink,
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 3052.7,
            analysisAssetId: Self.assetId
        )
        #expect(
            bridged.first?.startTime == 2964.1,
            "control arm: with no barrier the chain bridges the sign-off"
        )
        let stopped = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: postRollLink,
            contentBarriers: barriers,
            protectedRegions: [],
            episodeDuration: 3052.7,
            analysisAssetId: Self.assetId
        )
        #expect(stopped.isEmpty, "the sign-off must stop the chain, not be claimed as ad")
    }

    @Test("ad copy is never mistaken for a show-boundary phrase")
    func adCopyIsNotAShowBoundary() {
        let adRead = TranscriptChunk(
            id: "ad",
            analysisAssetId: Self.assetId,
            segmentFingerprint: "fp",
            chunkIndex: 0,
            startTime: 100.0,
            endTime: 115.0,
            text: "Visit example.com today and use code SHOW for twenty percent off.",
            normalizedText: "visit example.com today and use code show for twenty percent off.",
            pass: "final",
            modelVersion: "test",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
        #expect(
            AdPodContinuation.contentBarriers(
                semanticScanResults: [],
                lexicalHits: [],
                chunks: [adRead]
            ).isEmpty
        )
    }

    @Test("a metadata-origin transition phrase is not an utterance and is not a barrier")
    func metadataTransitionMarkerIsNotABarrier() {
        let hit = LexicalHit(
            category: .transitionMarker,
            matchedText: "back to the show",
            startTime: 500.0,
            endTime: 503.0,
            weight: 0.3,
            isMetadataOrigin: true
        )
        #expect(
            AdPodContinuation.contentBarriers(
                semanticScanResults: [],
                lexicalHits: [hit]
            ).isEmpty
        )
    }

    // MARK: - Safety: the listener's own marks

    /// Two arms differing ONLY in whether the listener's mark is supplied, so the
    /// refusal is proven rather than coincidental: without the mark the chain
    /// walks all the way back to 700; with it, it stops on this side of the mark.
    @Test("the walk refuses to cross a hand-marked span")
    func neverCrossesAUserMark() throws {
        let links: [(Double, Double)] = [(700.0, 731.0), (735.0, 780.0)]
        let mark = (start: 732.0, end: 734.0)

        let refused = conanCompose(protectedRegions: [mark], links: links)
        let refusedMark = try #require(refused.first)
        #expect(refused.count == 1)
        #expect(
            refusedMark.startTime == 735.0,
            "the walk stops on this side of the listener's mark"
        )

        let unrefused = conanCompose(links: links)
        #expect(
            unrefused.first?.startTime == 700.0,
            "control arm: without the mark the same chain reaches 700"
        )
    }

    @Test("a mark elsewhere in the episode does not veto an unrelated continuation")
    func protectedRegionVetoIsScoped() {
        let marks = conanCompose(protectedRegions: [(start: 100.0, end: 160.0)])
        #expect(marks.count == 1, "a far-away mark must not disable the pass")
        #expect(marks.first?.startTime == 669.0)
    }

    @Test("a degenerate protected region protects nothing and vetoes nothing")
    func degenerateProtectedRegionsAreInert() {
        let degenerate: [(start: Double, end: Double)] = [
            (start: 700.0, end: 700.0),
            (start: .nan, end: .nan),
            (start: -.infinity, end: .infinity),
            (start: 800.0, end: 700.0)
        ]
        let marks = conanCompose(protectedRegions: degenerate)
        #expect(marks.count == 1)
        #expect(marks.first?.startTime == 669.0)
    }

    @Test("a user-marked window is never a seed")
    func userMarkedWindowsNeverSeed() {
        let marks = AdPodContinuation.compose(
            existingWindows: [
                window(
                    start: ConanPod.seed.start,
                    end: ConanPod.seed.end,
                    boundaryState: "userMarked"
                )
            ],
            adCopyLinks: ConanPod.links.map { link($0.0, $0.1) },
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: ConanPod.episodeDuration,
            analysisAssetId: Self.assetId
        )
        #expect(marks.isEmpty)
    }

    // MARK: - Safety: nothing emitted can auto-skip

    /// The seed here is `eligible` at 0.98 — the strongest thing the pipeline
    /// emits. The recovered material must STILL be a banner: mark-only gate,
    /// candidate state, both edges unanchored (which under playhead-2350 can
    /// never auto-skip), and no advertiser to hallucinate into the copy.
    @Test("every emitted mark is mark-only, candidate, unanchored, even off an eligible seed")
    func emittedMarksAreAlwaysBannerTier() throws {
        let marks = AdPodContinuation.compose(
            existingWindows: [
                window(
                    start: ConanPod.seed.start,
                    end: ConanPod.seed.end,
                    confidence: 0.98,
                    eligibilityGate: .eligible
                )
            ],
            adCopyLinks: ConanPod.links.map { link($0.0, $0.1) },
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: ConanPod.episodeDuration,
            analysisAssetId: Self.assetId
        )
        let mark = try #require(marks.first)
        #expect(mark.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
        #expect(mark.decisionState == AdDecisionState.candidate.rawValue)
        #expect(mark.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(mark.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(mark.detectorVersion == AdPodContinuation.detectorVersion)
        #expect(mark.boundaryState == AdPodContinuation.boundaryState)
        #expect(mark.advertiser == nil)
        #expect(mark.metadataConfidence == nil)
        #expect(mark.wasSkipped == false)
        #expect(
            mark.confidence == AdPodContinuation.Configuration.default.markConfidenceCeiling,
            "a 0.98 seed's continuation is capped at the mark-tier ceiling"
        )
    }

    @Test("a weak seed's continuation inherits the weak confidence, never raises it")
    func markConfidenceIsNeverRaised() {
        let marks = AdPodContinuation.compose(
            existingWindows: [
                window(start: 800.0, end: 815.0, confidence: 0.41)
            ],
            adCopyLinks: [link(780.0, 795.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470.0,
            analysisAssetId: Self.assetId
        )
        #expect(marks.first?.confidence == 0.41)
    }

    @Test("the pass is additive: no emitted id collides with an input window")
    func neverRewritesAnInputWindow() {
        let seed = window(start: ConanPod.seed.start, end: ConanPod.seed.end)
        let marks = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: ConanPod.links.map { link($0.0, $0.1) },
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: ConanPod.episodeDuration,
            analysisAssetId: Self.assetId
        )
        #expect(!marks.isEmpty)
        #expect(!marks.contains { $0.id == seed.id })
        #expect(marks.allSatisfy { $0.analysisAssetId == seed.analysisAssetId })
    }

    // MARK: - Seed discipline

    @Test("a coarse aggregator candidate tile is never a seed")
    func candidateTilesNeverSeed() {
        let marks = AdPodContinuation.compose(
            existingWindows: [
                window(
                    start: 780.0,
                    end: 810.0,
                    decisionState: .candidate,
                    boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                    eligibilityGate: .markOnly
                )
            ],
            adCopyLinks: [link(700.0, 760.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470.0,
            analysisAssetId: Self.assetId
        )
        #expect(marks.isEmpty)
    }

    @Test("a continuation mark cannot seed another continuation")
    func continuationMarksNeverSeed() {
        let existing = AdPodContinuation.makeMark(
            start: 700.0,
            end: 760.0,
            confidence: 0.7,
            analysisAssetId: Self.assetId
        )
        #expect(!AdPodContinuation.isSeed(existing))
    }

    @Test("a suppressed window is never a seed")
    func suppressedWindowsNeverSeed() {
        #expect(
            !AdPodContinuation.isSeed(
                window(start: 700, end: 760, decisionState: .suppressed)
            )
        )
    }

    // MARK: - Bounds and gaps

    @Test("a link beyond maxLinkGapSeconds does not start a chain")
    func gapBoundIsRespected() {
        let gap = AdPodContinuation.Configuration.default.maxLinkGapSeconds
        let seed = window(start: 1000.0, end: 1020.0)
        let justInside = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: [link(1020.0 + gap - 0.5, 1080.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        #expect(justInside.count == 1)

        let justOutside = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: [link(1020.0 + gap + 0.5, 1080.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        #expect(justOutside.isEmpty)
    }

    @Test("the per-side extension bound truncates a runaway chain")
    func extensionBoundIsRespected() throws {
        // Twenty links, 20 s apart, all reachable — the bound must stop the walk.
        let links = (0..<20).map { i -> AdPodContinuation.AdCopyLink in
            let start = 1000.0 + Double(i) * 20.0
            return link(start, start + 10.0)
        }
        let config = AdPodContinuation.Configuration(maxExtensionSecondsPerSide: 60.0)
        let marks = AdPodContinuation.compose(
            existingWindows: [window(start: 980.0, end: 995.0)],
            adCopyLinks: links,
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 4000.0,
            analysisAssetId: Self.assetId,
            config: config
        )
        let mark = try #require(marks.first)
        #expect(mark.endTime - 995.0 <= 60.0)
    }

    @Test("maxLinkGapSeconds <= 0 disables the pass entirely")
    func nonPositiveGapDisables() {
        #expect(
            conanCompose(config: AdPodContinuation.Configuration(maxLinkGapSeconds: 0)).isEmpty
        )
    }

    @Test("marks are clamped to the episode duration")
    func clampedToEpisodeDuration() {
        let marks = AdPodContinuation.compose(
            existingWindows: [window(start: 900.0, end: 950.0)],
            adCopyLinks: [link(960.0, 1100.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1000.0,
            analysisAssetId: Self.assetId
        )
        #expect(marks.first?.endTime == 1000.0)
    }

    @Test("a hole already covered by an existing visible window yields no mark")
    func fullyCoveredHoleYieldsNoMark() {
        let seed = window(start: 800.0, end: 815.0)
        let neighbour = window(start: 770.0, end: 800.0)
        let marks = AdPodContinuation.compose(
            existingWindows: [seed, neighbour],
            adCopyLinks: [link(780.0, 795.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470.0,
            analysisAssetId: Self.assetId
        )
        #expect(marks.isEmpty, "the neighbour already covers that material")
    }

    /// The residue rule, which is the whole point: a corridor with an
    /// already-detected creative in the middle yields marks for the HOLES either
    /// side of it, and nothing over the creative itself. Emitting the residue
    /// rather than dropping the span is what makes this pass move the metric the
    /// bead measures (uncovered runs), and it is why the pass can never
    /// double-count material the listener already sees.
    @Test("a partially covered corridor yields marks only for the uncovered holes")
    func partiallyCoveredCorridorYieldsResidue() {
        let seed = window(start: 900.0, end: 920.0)
        let alreadyDetected = window(start: 850.0, end: 870.0)
        let marks = AdPodContinuation.compose(
            existingWindows: [seed, alreadyDetected],
            adCopyLinks: [link(820.0, 845.0), link(848.0, 872.0), link(875.0, 898.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        #expect(marks.map(\.startTime) == [820.0, 870.0])
        #expect(marks.map(\.endTime) == [850.0, 900.0])
    }

    @Test("a sub-minimum sliver left by the subtraction is not emitted")
    func sliversAreNotEmitted() {
        let minimum = AdPodContinuation.Configuration.default.minMarkDurationSeconds
        let seed = window(start: 900.0, end: 920.0)
        // Existing coverage leaves only a (minimum - 1) s sliver before the seed.
        let alreadyDetected = window(start: 820.0, end: 900.0 - (minimum - 1.0))
        let marks = AdPodContinuation.compose(
            existingWindows: [seed, alreadyDetected],
            adCopyLinks: [link(825.0, 898.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        #expect(marks.isEmpty)
    }

    /// A previously-persisted continuation row must NOT suppress its own re-
    /// emission: if it did, a re-run would find no residue, emit nothing, and the
    /// version-scoped reconcile would retire the row it wrote last time — a mark
    /// that flickers on every backfill.
    @Test("a prior continuation row does not suppress its own re-emission")
    func priorContinuationRowsDoNotSelfSuppress() {
        let seed = window(start: 800.0, end: 815.0)
        let firstPass = AdPodContinuation.compose(
            existingWindows: [seed],
            adCopyLinks: [link(770.0, 795.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470.0,
            analysisAssetId: Self.assetId
        )
        #expect(firstPass.count == 1)
        let secondPass = AdPodContinuation.compose(
            existingWindows: [seed] + firstPass,
            adCopyLinks: [link(770.0, 795.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 1470.0,
            analysisAssetId: Self.assetId
        )
        #expect(secondPass.map(\.id) == firstPass.map(\.id))
    }

    // MARK: - Determinism

    @Test("recomposing identical inputs mints identical ids in identical order")
    func recomposeIsIdempotent() {
        let first = conanCompose()
        let second = conanCompose()
        #expect(first.map(\.id) == second.map(\.id))
        #expect(first.map(\.startTime) == second.map(\.startTime))
    }

    @Test("two seeds meeting inside one pod do not emit duplicate geometry")
    func overlappingProposalsCollapse() {
        // Two seeds either side of one link; both would propose the same span.
        let marks = AdPodContinuation.compose(
            existingWindows: [
                window(start: 700.0, end: 720.0),
                window(start: 760.0, end: 780.0)
            ],
            adCopyLinks: [link(720.0, 760.0)],
            contentBarriers: [],
            protectedRegions: [],
            episodeDuration: 2000.0,
            analysisAssetId: Self.assetId
        )
        #expect(marks.count == Set(marks.map(\.id)).count, "no duplicate ids")
        #expect(marks.count == 1, "one span of geometry, proposed twice, emitted once")
    }

    // MARK: - Link derivation delegates to the vetted rule

    /// Helper: build the link set for a hit stream plus optional prose chunks.
    private func links(
        hits: [LexicalHit],
        chunkTexts: [(start: Double, end: Double, text: String)] = []
    ) -> [AdPodContinuation.AdCopyLink] {
        let chunks = chunkTexts.enumerated().map { index, chunk in
            TranscriptChunk(
                id: "chunk-\(index)",
                analysisAssetId: Self.assetId,
                segmentFingerprint: "fp-\(index)",
                chunkIndex: index,
                startTime: chunk.start,
                endTime: chunk.end,
                text: chunk.text,
                normalizedText: chunk.text.lowercased(),
                pass: "final",
                modelVersion: "test",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
        return AdPodContinuation.adCopyLinks(chunks: chunks, hits: hits)
    }

    private func hit(
        _ category: LexicalPatternCategory,
        _ text: String,
        _ start: Double,
        _ end: Double,
        metadata: Bool = false,
        negative: Bool = false
    ) -> LexicalHit {
        LexicalHit(
            category: category,
            matchedText: text,
            startTime: start,
            endTime: end,
            weight: 1.0,
            isMetadataOrigin: metadata,
            isNegativePattern: negative
        )
    }

    /// A domain mentioned editorially is `urlCTA`-only: one kind, and a weak one.
    /// This is the dominant false-positive shape and it must not fire.
    @Test("a lone URL is not an ad-copy link")
    func loneURLIsNotALink() {
        #expect(links(hits: [hit(.urlCTA, "example.com", 100, 101)]).isEmpty)
    }

    /// Two URLs are two hits of ONE weak kind. Counting hits rather than KINDS is
    /// the mistake this asserts against — a tech show naming three domains in a
    /// row would otherwise read as an ad pod.
    @Test("several URLs and no strong kind are still not a link")
    func manyWeakHitsAreNotALink() {
        #expect(
            links(hits: [
                hit(.urlCTA, "example.com", 100, 101),
                hit(.urlCTA, "other.com", 108, 109),
                hit(.purchaseLanguage, "free trial", 112, 113)
            ]).isEmpty
        )
    }

    @Test("a sponsor disclosure plus a nearby CTA is an ad-copy link")
    func sponsorPlusCTAIsALink() {
        #expect(
            links(hits: [
                hit(.sponsor, "brought to you by", 100, 102),
                hit(.urlCTA, "example.com", 110, 111)
            ]) == [AdPodContinuation.AdCopyLink(start: 100.0, end: 111.0)]
        )
    }

    /// The case the first build of this pass MISSED entirely, and the reason it
    /// recovered 0.0 s on the corpus: a programmatically stitched DAI creative
    /// carries no sponsor disclosure — "brought to you by" is a HOST-READ
    /// convention. What it carries is offer boilerplate plus a URL.
    @Test("offer-terms boilerplate plus a URL is a link, with no sponsor disclosure")
    func offerTermsPlusURLIsALink() {
        let result = links(
            hits: [hit(.urlCTA, "windows.com", 70, 71)],
            chunkTexts: [
                (start: 60.0, end: 75.0,
                 text: "While supplies last, ends June 30th, see website for more details.")
            ]
        )
        #expect(result.count == 1)
        #expect(result.first?.start == 60.0)
        #expect(result.first?.end == 75.0)
    }

    /// SPECIFICATION CHANGE, recorded so the reversal is auditable. An earlier
    /// draft required TWO distinct kinds, so offer boilerplate alone was not a
    /// link. It is one now, because a strong kind is near-exclusive to
    /// advertising and this rule only ever extends a chain a CONFIRMED ad window
    /// anchored — see the bar's rationale on `adCopyLinks`. Measured on
    /// rediff-confirmed pod boundaries, the change recovered 209 s more ad audio
    /// and added ZERO seconds outside a byte-confirmed DAI slot.
    @Test("offer-terms boilerplate alone IS a link (one strong kind is the bar)")
    func offerTermsAloneIsALink() {
        #expect(
            links(
                hits: [],
                chunkTexts: [
                    (start: 60.0, end: 75.0, text: "Terms and conditions apply.")
                ]
            ) == [AdPodContinuation.AdCopyLink(start: 60.0, end: 75.0)]
        )
    }

    /// The stricter bar is still reachable, and the corpus eval keeps it as the
    /// conservative comparison arm.
    @Test("with the two-kind bar, offer-terms boilerplate alone is not a link")
    func offerTermsAloneIsNotALinkUnderTwoKindBar() {
        let chunk = TranscriptChunk(
            id: "c0",
            analysisAssetId: Self.assetId,
            segmentFingerprint: "fp0",
            chunkIndex: 0,
            startTime: 60.0,
            endTime: 75.0,
            text: "Terms and conditions apply.",
            normalizedText: "terms and conditions apply.",
            pass: "final",
            modelVersion: "test",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
        #expect(
            AdPodContinuation.adCopyLinks(
                chunks: [chunk],
                hits: [],
                allowSingleStrongKindLinks: false
            ).isEmpty
        )
    }

    @Test("a promo code plus offer language is a link")
    func promoCodePlusOfferIsALink() {
        #expect(
            !links(hits: [
                hit(.promoCode, "use code SHOW", 100, 101),
                hit(.purchaseLanguage, "free trial", 106, 107)
            ]).isEmpty
        )
    }

    /// A distant weak signal does not WIDEN a strong link's interval: the strong
    /// signal still carries its own narrow link, but the far-away URL is not
    /// folded into it.
    @Test("a weak signal beyond the co-occurrence window does not widen the link")
    func distantSignalsDoNotPair() {
        let window = LexicalAutoAdEvidenceBuilder.Config.default.cooccurrenceWindow
        let result = links(hits: [
            hit(.sponsor, "brought to you by", 100, 101),
            hit(.urlCTA, "example.com", 100 + window + 5, 100 + window + 6)
        ])
        #expect(result == [AdPodContinuation.AdCopyLink(start: 100.0, end: 101.0)])
    }

    /// The show plugging its OWN domain is exactly what must not be treated as
    /// commercial copy. The pair here is well-formed, so the suppression can only
    /// come from the guardrail.
    @Test("a show-owned-domain negative pattern suppresses the link")
    func negativePatternSuppressesLink() {
        let sponsor = hit(.sponsor, "brought to you by", 100, 102)
        let code = hit(.promoCode, "use code SHOW", 110, 111)
        let showOwnedDomain = hit(.urlCTA, "theshow.com", 105, 106, negative: true)
        #expect(
            !links(hits: [sponsor, code]).isEmpty,
            "control arm: the same pair without the show-owned domain DOES fire"
        )
        #expect(links(hits: [sponsor, code, showOwnedDomain]).isEmpty)
    }

    /// The editorial guardrail — a brand being DISCUSSED, not sold. This calls
    /// `LexicalAutoAdEvidenceBuilder.hasNegativeContext`, the same function the
    /// vetted rule uses, rather than a copy of it.
    @Test("an editorial-context cue near the pair suppresses the link")
    func editorialContextSuppressesLink() {
        let sponsor = hit(.sponsor, "brought to you by", 100, 102)
        let cta = hit(.urlCTA, "example.com", 110, 111)
        let editorial = hit(.sponsor, "according to the lawsuit", 112, 114)
        #expect(links(hits: [sponsor, cta, editorial]).isEmpty)
    }

    @Test("metadata-origin legs cannot carry a link")
    func metadataOnlyLegsDoNotFire() {
        #expect(
            links(hits: [
                hit(.sponsor, "sponsored by", 100, 102, metadata: true),
                hit(.promoCode, "use code SHOW", 110, 111, metadata: true)
            ]).isEmpty
        )
    }

    /// A return marker is a CONTENT signal consumed as a barrier; it must never
    /// double as ad-copy evidence, or the stopping signal would also be a reason
    /// to keep walking.
    @Test("a transition marker is never ad-copy evidence")
    func transitionMarkerIsNotAdCopyEvidence() {
        #expect(
            AdPodContinuation.adCopySignals(
                chunks: [],
                hits: [hit(.transitionMarker, "back to the show", 100, 101)]
            ).isEmpty
        )
    }

    @Test("overlapping links merge into one interval")
    func overlappingLinksMerge() {
        let merged = AdPodContinuation.mergeLinks([link(100, 150), link(140, 200), link(300, 320)])
        #expect(merged == [link(100, 200), link(300, 320)])
    }

    // MARK: - Reconcile isolation

    /// Without this, a stale continuation row could NEVER be retired: the
    /// version-scoped reconcile treats a protected `boundaryState` as
    /// non-reconcilable, so a mark stamped with one would outlive the evidence
    /// that produced it forever. Mirrors the specialist axis test.
    @Test("the continuation boundaryState is reconcilable, not user-protected")
    func continuationRowsAreReconcilable() {
        #expect(
            !AdDetectionService.reconcileProtectedBoundaryStates
                .contains(AdPodContinuation.boundaryState)
        )
        let mark = AdPodContinuation.makeMark(
            start: 100,
            end: 130,
            confidence: 0.7,
            analysisAssetId: Self.assetId
        )
        #expect(
            AdDetectionService.isReconcilableBackfillWindow(
                mark,
                detectorVersion: AdPodContinuation.detectorVersion
            )
        )
    }

    /// Two additive mark producers now share one reconcile function. Prove they
    /// cannot retire each other, and that neither can touch an FM row — the
    /// property that made the generalization safe.
    @Test("version-scoped reconcile keeps additive producers isolated")
    func reconcileKeepsProducersIsolated() {
        let continuationMark = AdPodContinuation.makeMark(
            start: 100,
            end: 130,
            confidence: 0.7,
            analysisAssetId: Self.assetId
        )
        let specialistMark = SpecialistMarkComposer.makeMark(
            SpecialistMarkComposer.MergedSpan(
                start: 400,
                end: 430,
                confidence: 0.8,
                adClass: nil
            ),
            analysisAssetId: Self.assetId
        )
        let fmWindow = window(start: 700, end: 760)
        let existing = [continuationMark, specialistMark, fmWindow]

        // A continuation recompose that produces NOTHING retires only its own row.
        let continuationSweep = AdDetectionService.reconcileVersionScopedMarkSets(
            newMarks: [],
            existingWindows: existing,
            detectorVersion: AdPodContinuation.detectorVersion
        )
        #expect(continuationSweep.retiredIDs == [continuationMark.id])

        // And the specialist sweep retires only ITS own row.
        let specialistSweep = AdDetectionService.reconcileSpecialistMarkSets(
            newMarks: [],
            existingWindows: existing
        )
        #expect(specialistSweep.retiredIDs == [specialistMark.id])

        // Re-producing the identical continuation mark retires nothing.
        let idempotent = AdDetectionService.reconcileVersionScopedMarkSets(
            newMarks: [continuationMark],
            existingWindows: existing,
            detectorVersion: AdPodContinuation.detectorVersion
        )
        #expect(idempotent.retiredIDs.isEmpty)
        #expect(idempotent.windows.map(\.id) == [continuationMark.id])
    }

    // MARK: - Scan-row helper

    private func scanRow(
        _ start: Double,
        _ end: Double,
        _ disposition: CoarseDisposition,
        _ status: SemanticScanStatus,
        _ quality: TranscriptQuality
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "scan-\(start)-\(end)",
            analysisAssetId: Self.assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: "passA",
            transcriptQuality: quality,
            disposition: disposition,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: "{}",
            transcriptVersion: "t1"
        )
    }
}
