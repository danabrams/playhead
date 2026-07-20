// AutoSkipEdgePaddingTests.swift
// playhead-98co: asymmetric start-edge padding for auto-skip.
//
// Two suites:
//   1. Policy unit tests — the pure `AutoSkipEdgePadding` type: per-tier
//      margins (pinned to the derivation doc), shrink-only invariant,
//      per-show demotion, degenerate-span suppression, boundary exactness.
//   2. Orchestrator wiring tests — OFF (default) is a byte-identical no-op;
//      ON demotes unanchored auto-skips to markOnly behavior; anchored
//      edges shrink the pushed cue; user-initiated skips and
//      markOnly/suggest spans are untouched.
//
// Margin numbers mirror docs/autoskip-edge-padding-derivation-2026-07-20.md.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

// MARK: - Policy unit tests

@Suite("AutoSkipEdgePadding policy - derived margins and invariants")
struct AutoSkipEdgePaddingPolicyTests {

    private static let nikkiSlug = "the-nikki-glaser-podcast"
    private static let nikkiFeedURL =
        "https://www.omnycontent.com/d/playlist/e73c998e-6e60-432f-8610-ae210140c5b1/0d8967bb-212c-4f2e-85bb-ae2700380ca7/2558cddf-28c7-463d-b70b-ae2700380cc3/podcast.rss"

    @Test("Flag defaults OFF (Gate 2 held — the policy ships dormant)")
    func flagDefaultsOff() {
        #expect(AutoSkipEdgePadding.isEnabledByDefault == false)
    }

    @Test("Margin constants match the 2026-07-20 derivation")
    func marginConstantsMatchDerivation() {
        #expect(AutoSkipEdgePadding.startMarginRediffByteExactSeconds == 0.50)
        #expect(AutoSkipEdgePadding.startMarginStingerSnappedSeconds == 0.75)
        #expect(AutoSkipEdgePadding.endMarginRediffByteExactSeconds == 0.75)
        #expect(AutoSkipEdgePadding.endMarginStingerSnappedSeconds == 0.75)
        #expect(AutoSkipEdgePadding.endMarginUnanchoredSeconds == 10.25)
        #expect(AutoSkipEdgePadding.minimumSkippableRemainderSeconds == 1.0)
    }

    @Test("Per-edge margins by tier: start requires an anchor, end always defined")
    func perEdgeMargins() {
        #expect(AutoSkipEdgePadding.startMargin(for: .rediffByteExact) == 0.50)
        #expect(AutoSkipEdgePadding.startMargin(for: .stingerSnapped) == 0.75)
        #expect(AutoSkipEdgePadding.startMargin(for: .unanchored) == nil)
        #expect(AutoSkipEdgePadding.endMargin(for: .rediffByteExact) == 0.75)
        #expect(AutoSkipEdgePadding.endMargin(for: .stingerSnapped) == 0.75)
        #expect(AutoSkipEdgePadding.endMargin(for: .unanchored) == 10.25)
    }

    @Test("Skip windows per tier combination — boundary-exact arithmetic")
    func skipWindowsPerTier() {
        // byte / byte: [100, 190] → [100.50, 189.25]
        let byteByte = AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 190,
            startAnchor: .rediffByteExact, endAnchor: .rediffByteExact
        )
        #expect(byteByte?.start == 100.50)
        #expect(byteByte?.end == 189.25)

        // stinger / stinger: [100, 190] → [100.75, 189.25]
        let stingerStinger = AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 190,
            startAnchor: .stingerSnapped, endAnchor: .stingerSnapped
        )
        #expect(stingerStinger?.start == 100.75)
        #expect(stingerStinger?.end == 189.25)

        // stinger start / unanchored end: [100, 190] → [100.75, 179.75]
        let stingerOpen = AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 190,
            startAnchor: .stingerSnapped, endAnchor: .unanchored
        )
        #expect(stingerOpen?.start == 100.75)
        #expect(stingerOpen?.end == 179.75)

        // byte start / stinger end: [100, 190] → [100.50, 189.25]
        let byteStinger = AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 190,
            startAnchor: .rediffByteExact, endAnchor: .stingerSnapped
        )
        #expect(byteStinger?.start == 100.50)
        #expect(byteStinger?.end == 189.25)
    }

    @Test("Unanchored start is unskippable regardless of the end anchor")
    func unanchoredStartUnskippable() {
        for endAnchor in AutoSkipEdgeAnchor.allCases {
            let window = AutoSkipEdgePadding.skipWindow(
                spanStart: 100, spanEnd: 190,
                startAnchor: .unanchored, endAnchor: endAnchor
            )
            #expect(window == nil, "unanchored start must stay markOnly (end=\(endAnchor))")
        }
    }

    @Test("Per-show demotion: nikki stinger-snapped starts stay markOnly (both key aliases); byte-exact start is NOT demoted")
    func nikkiStartDemotion() {
        for key in [Self.nikkiSlug, Self.nikkiFeedURL] {
            #expect(AutoSkipEdgePadding.startMargin(for: .stingerSnapped, showKey: key) == nil)
            let window = AutoSkipEdgePadding.skipWindow(
                spanStart: 100, spanEnd: 190,
                startAnchor: .stingerSnapped, endAnchor: .stingerSnapped,
                showKey: key
            )
            #expect(window == nil, "nikki stinger start must be demoted for key \(key)")
        }
        // The demotion is scoped to the stinger misfire mode: a rediff
        // byte-exact start on the same show is fine.
        let byteWindow = AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 190,
            startAnchor: .rediffByteExact, endAnchor: .stingerSnapped,
            showKey: Self.nikkiSlug
        )
        #expect(byteWindow?.start == 100.50)
        #expect(byteWindow?.end == 189.25)
        // Non-demoted shows pad normally under the stinger tier.
        let morbid = AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 190,
            startAnchor: .stingerSnapped, endAnchor: .stingerSnapped,
            showKey: "morbid"
        )
        #expect(morbid?.start == 100.75)
        // Unknown show (nil key) is not demoted either.
        #expect(AutoSkipEdgePadding.startMargin(for: .stingerSnapped, showKey: nil) == 0.75)
    }

    @Test("Shrink-only invariant across every anchor combination")
    func shrinkOnlyInvariant() {
        let spans: [(Double, Double)] = [(0, 30), (100, 190), (1000, 1090.5), (5181.0, 5243.6)]
        for (spanStart, spanEnd) in spans {
            for startAnchor in AutoSkipEdgeAnchor.allCases {
                for endAnchor in AutoSkipEdgeAnchor.allCases {
                    guard let window = AutoSkipEdgePadding.skipWindow(
                        spanStart: spanStart, spanEnd: spanEnd,
                        startAnchor: startAnchor, endAnchor: endAnchor
                    ) else { continue }
                    #expect(window.start >= spanStart, "skip start must never precede the marked span")
                    #expect(window.end <= spanEnd, "skip end must never exceed the marked span")
                    #expect(
                        window.end - window.start >= AutoSkipEdgePadding.minimumSkippableRemainderSeconds
                    )
                }
            }
        }
    }

    @Test("Degenerate spans: margins consume the span → suppressed; exact remainder boundary survives")
    func degenerateSpans() {
        // stinger/stinger margins total 1.5s; remainder floor 1.0s.
        // Width 2.2 → remainder 0.7 < 1.0 → suppressed.
        #expect(AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 102.2,
            startAnchor: .stingerSnapped, endAnchor: .stingerSnapped
        ) == nil)
        // Width 2.5 → remainder exactly 1.0 → survives (boundary exact).
        let boundary = AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 102.5,
            startAnchor: .stingerSnapped, endAnchor: .stingerSnapped
        )
        #expect(boundary?.start == 100.75)
        #expect(boundary?.end == 101.75)
        // Unanchored end's 10.25s margin consumes a short anchored span.
        #expect(AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 108,
            startAnchor: .rediffByteExact, endAnchor: .unanchored
        ) == nil)
    }

    @Test("Invalid spans: zero, negative, and non-finite widths are suppressed")
    func invalidSpans() {
        #expect(AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 100,
            startAnchor: .rediffByteExact, endAnchor: .rediffByteExact
        ) == nil)
        #expect(AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: 90,
            startAnchor: .rediffByteExact, endAnchor: .rediffByteExact
        ) == nil)
        #expect(AutoSkipEdgePadding.skipWindow(
            spanStart: .nan, spanEnd: 190,
            startAnchor: .rediffByteExact, endAnchor: .rediffByteExact
        ) == nil)
        #expect(AutoSkipEdgePadding.skipWindow(
            spanStart: 100, spanEnd: .infinity,
            startAnchor: .rediffByteExact, endAnchor: .rediffByteExact
        ) == nil)
    }
}

// MARK: - Orchestrator wiring tests

@Suite("AutoSkipEdgePadding wiring - SkipOrchestrator skip-window computation")
struct AutoSkipEdgePaddingWiringTests {

    private static func cueStart(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start)
    }

    private static func cueEnd(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start + cue.duration)
    }

    /// Auto-mode orchestrator over the standard test store/trust harness.
    private static func makeAutoOrchestrator() async throws -> SkipOrchestrator {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        return SkipOrchestrator(store: store, trustService: trustService)
    }

    @Test("OFF (default): behavior is byte-identical — cue uses snapped bounds minus only the existing trailing cushion")
    func offIsNoOp() async throws {
        let orchestrator = try await Self.makeAutoOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        // No setEdgePaddingEnabled call — the default must be OFF.
        let ad = makeSkipTestAdWindow(
            id: "ad-off", startTime: 60, endTime: 120,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        #expect(pushedCues.count == 1)
        if let cue = pushedCues.first {
            #expect(Self.cueStart(cue) == 60)
            #expect(Self.cueEnd(cue) == 119) // 120 - 1.0 trailing cushion only
        }
    }

    @Test("ON, no anchor provenance: auto-skip demoted to markOnly behavior — no cue, confirmed with edge-padding reason")
    func onUnanchoredDemotesToMarkOnly() async throws {
        let orchestrator = try await Self.makeAutoOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )
        await orchestrator.setEdgePaddingEnabled(true)

        let ad = makeSkipTestAdWindow(
            id: "ad-unanchored", startTime: 60, endTime: 120,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        #expect(pushedCues.isEmpty, "No skip cue may fire for an unanchored span with padding ON")

        let log = await orchestrator.getDecisionLog()
        let vetoed = log.filter {
            $0.adWindowId == "ad-unanchored"
                && $0.decision == .confirmed
                && $0.reason.hasPrefix("Edge padding:")
        }
        #expect(!vetoed.isEmpty, "The demotion must be visible in the decision log")
        let applied = log.filter { $0.adWindowId == "ad-unanchored" && $0.decision == .applied }
        #expect(applied.isEmpty, "The window must never promote to .applied")
    }

    @Test("ON + stinger anchors: cue shrinks by the derived margins (padding is cue-only; decision record keeps snapped bounds)")
    func onAnchoredShrinksCue() async throws {
        let orchestrator = try await Self.makeAutoOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )
        await orchestrator.setEdgePaddingEnabled(true)
        await orchestrator.setEdgeAnchors(
            start: .stingerSnapped, end: .stingerSnapped, forWindowId: "ad-anchored"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-anchored", startTime: 60, endTime: 120,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        #expect(pushedCues.count == 1)
        if let cue = pushedCues.first {
            #expect(Self.cueStart(cue) == 60.75) // 60 + 0.75 stinger start margin
            #expect(Self.cueEnd(cue) == 118.25) // 120 - 0.75 margin - 1.0 trailing cushion
        }

        // Padding must not leak into the decision record's span bounds.
        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.adWindowId == "ad-anchored" && $0.decision == .applied }
        #expect(applied.last?.snappedStart == 60)
        #expect(applied.last?.snappedEnd == 120)
    }

    @Test("ON + stinger start / unanchored end: the 10.25s end margin applies through the wiring")
    func onUnanchoredEndMargin() async throws {
        let orchestrator = try await Self.makeAutoOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )
        await orchestrator.setEdgePaddingEnabled(true)
        await orchestrator.setEdgeAnchors(
            start: .stingerSnapped, end: .unanchored, forWindowId: "ad-openend"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-openend", startTime: 60, endTime: 120,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        #expect(pushedCues.count == 1)
        if let cue = pushedCues.first {
            #expect(Self.cueStart(cue) == 60.75) // 60 + 0.75
            #expect(Self.cueEnd(cue) == 108.75) // 120 - 10.25 margin - 1.0 cushion
        }
    }

    @Test("ON + demoted show (nikki): stinger-anchored start still demoted — no cue")
    func onDemotedShowNoCue() async throws {
        let nikkiKey = "the-nikki-glaser-podcast"
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustStore = try await makeTestStore()
        try await trustStore.upsertProfile(PodcastProfile(
            podcastId: nikkiKey,
            sponsorLexicon: nil, normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil, jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.9, observationCount: 10,
            mode: "auto", recentFalseSkipSignals: 0
        ))
        let orchestrator = SkipOrchestrator(
            store: store, trustService: TrustScoringService(store: trustStore)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: nikkiKey
        )
        await orchestrator.setEdgePaddingEnabled(true)
        await orchestrator.setEdgeAnchors(
            start: .stingerSnapped, end: .stingerSnapped, forWindowId: "ad-nikki"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-nikki", startTime: 60, endTime: 120,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        #expect(pushedCues.isEmpty, "nikki stinger-snapped starts are demoted per derivation §5")
    }

    @Test("ON: user-marked span is exempt — skips exactly as marked")
    func onUserMarkedExempt() async throws {
        let orchestrator = try await Self.makeAutoOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )
        await orchestrator.setEdgePaddingEnabled(true)

        await orchestrator.injectUserMarkedAd(start: 200, end: 260, analysisAssetId: "asset-1")

        #expect(pushedCues.count == 1)
        if let cue = pushedCues.first {
            #expect(Self.cueStart(cue) == 200)
            #expect(Self.cueEnd(cue) == 259) // trailing cushion only — no padding
        }
    }

    @Test("ON, manual mode: user tap skips the exact span (manual skips exempt from padding)")
    func onManualSkipExempt() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "manual", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )
        await orchestrator.setEdgePaddingEnabled(true)

        let ad = makeSkipTestAdWindow(
            id: "ad-manual", startTime: 60, endTime: 120,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])
        #expect(pushedCues.isEmpty, "Manual mode fires no cue before the user taps")

        await orchestrator.applyManualSkip(windowId: "ad-manual")

        #expect(pushedCues.count == 1)
        if let cue = pushedCues.first {
            #expect(Self.cueStart(cue) == 60)
            #expect(Self.cueEnd(cue) == 119) // exact span minus trailing cushion; no padding
        }
    }

    @Test("ON: markOnly spans are untouched — suggest tier surfacing, no cue, no state change")
    func onMarkOnlyUntouched() async throws {
        let orchestrator = try await Self.makeAutoOrchestrator()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )
        await orchestrator.setEdgePaddingEnabled(true)

        let markOnly = AdWindow(
            id: "ad-markonly",
            analysisAssetId: "asset-1",
            startTime: 300, endTime: 360,
            confidence: 0.9,
            boundaryState: "lexical",
            decisionState: "confirmed",
            detectorVersion: "detection-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: "brought to you by", evidenceStartTime: 300,
            metadataSource: "none", metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false,
            eligibilityGate: "markOnly"
        )
        await orchestrator.receiveAdWindows([markOnly])

        #expect(pushedCues.isEmpty, "markOnly spans never produce skip cues")
        let suggestIds = await orchestrator.activeSuggestWindowIDs()
        #expect(suggestIds.contains("ad-markonly"), "markOnly span must still surface via the suggest tier")
        let activeIds = await orchestrator.activeWindowIDs()
        #expect(!activeIds.contains("ad-markonly"), "markOnly span must not enter the managed skip set")
    }
}
