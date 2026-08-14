// DayZeroByteExactShadowShowSkipTests.swift
// playhead-exy0: REPRODUCTION of the device state from db-pull8/db-pull9
// (2026-08-13, build 0a43c6ea) — four day-0 byte-exact rediff windows sitting at
// `decisionState = candidate`, `wasSkipped = 0`, on a show whose
// `podcast_profiles.mode` is `shadow` and whose `detectorTrustJSON` is NULL.
//
// Every field below is read off the pulled row, not paraphrased:
//
//   boundaryState    dayZeroRediffByteExact
//   decisionState    candidate           <- the mint's own value, for life
//   eligibilityGate  eligible            <- strict slot + qs0d promotion
//   startEdgeAnchor  rediffByteExact
//   endEdgeAnchor    rediffByteExact
//   confidence       1.0     skipConfidence NULL
//   0.0 - 30.055291823736166   (pre-roll, asset 3C2FFE10, duration 7998.955)
//   7939.030454444587 - 7999.007346926723 (post-roll, same asset)
//
// and the show:
//
//   podcastId  https://rss2.flightcast.com/...   mode shadow
//   skipTrustScore 0.5  observationCount 0  detectorTrustJSON NULL
//
// The claim under test is the product promise: a window the byte differ proved
// on BOTH edges auto-skips on a first listen, before the show has earned any
// trust, because `SkipDetectorClass.rediffByteExact` does not consult show
// trust (`showIndependentSeedMode == .auto`).
//
// WHY THIS SUITE EXISTS AT ALL, given that playhead-qs0d and playhead-gard both
// have orchestrator wiring tests. Every one of them builds its row with
// `decisionState: confirmed` — `rediffWindow()` in `PerDetectorTrustTests` and
// `pushedCue(...)` in `RediffDayZeroAutoSkipPromotionTests` both do — and the
// mint writes `.candidate` and leaves it there for life
// (`AdPodContinuationFlipTests` says so in as many words). Every one of them
// also delivers through `receiveAdWindows` directly, never through the
// cross-launch preload or the playhead-96ot mid-session door. So the population
// the mint ACTUALLY produces, arriving through the doors it ACTUALLY uses, had
// no coverage, and playhead-exy0 was filed on the belief that it therefore did
// not work. It does; this is the rail that says so.
//
// WHAT THE MUTATION BATTERY FOUND, and do not lose it: the promotion of a
// `.candidate` is carried by TWO independent paths, not one.
//
//   M1  `evaluateWindow`'s candidate arm neutered
//       (`if false && windowSkipMode == .auto && confidence >= enterThreshold`)
//       — SURVIVED, all six green. At `confidence == 1.0` the sibling
//       `else if confidence < config.shortSpanOverrideConfidence` (0.85) is
//       FALSE, so the row falls through to the mode switch still `.candidate`
//       and the `.auto` arm returns `.applied` regardless. The comment above
//       that branch — "Candidates need confirmation before skipping" — is not
//       what governs a byte-exact day-0 row, and a reader who removes the
//       `shortSpanOverrideConfidence` fall-through believing the promotion
//       covers it will silently un-ship this bead's whole subject.
//   M2  `SkipDetectorClass.showIndependentSeedMode = .shadow` — KILLED (see the
//       commit message for the count). That constant, not the candidate
//       promotion, is the load-bearing line: without it the class resolves
//       `.shadow`, `managedTierWouldBeSilent` diverts the row to the suggest
//       tier, and the listener gets a card instead of silence.

import CoreMedia
import Foundation
import os
import Testing

@testable import Playhead

@Suite("Day-0 byte-exact auto-skip on a shadow show (playhead-exy0)")
struct DayZeroByteExactShadowShowSkipTests {

    private static let assetId = "asset-1"
    private static let showId = "podcast-1"
    private static let episodeDuration = 7998.955102040816

    /// The pre-roll row, byte-for-byte as `mintByteExactDayZeroMarks` persists a
    /// STRICT slot and as db-pull9 read it back.
    private static func deviceRow(
        id: String,
        start: Double,
        end: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
        )
    }

    /// Persist the two device rows for `assetId`. Separated from the store
    /// build so the mid-session arm can mint AFTER `beginEpisode`, which is the
    /// order production takes: the mint runs on a downloaded asset and hands its
    /// rows to whatever session is live.
    private static func mintDeviceRows(into store: AnalysisStore) async throws {
        try await store.insertAdWindow(
            deviceRow(id: "exy0-preroll", start: 0.0, end: 30.055291823736166)
        )
        try await store.insertAdWindow(
            deviceRow(
                id: "exy0-postroll",
                start: 7939.030454444587,
                end: 7999.007346926723
            )
        )
    }

    /// An orchestrator over the device's own show profile, with an empty
    /// `ad_windows`. `mint` pre-populates it for the cross-launch-preload arms.
    private static func makeOrchestrator(
        mint: Bool = true
    ) async throws -> (SkipOrchestrator, AnalysisStore) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: assetId,
                episodeId: assetId,
                episodeDurationSec: episodeDuration
            )
        )
        if mint { try await mintDeviceRows(into: store) }
        // The device profile: shadow, no ledger. `makeSkipTestTrustService`
        // writes no `detectorTrustJSON`, which is the NULL column the pull read.
        let trustService = try await makeSkipTestTrustService(
            mode: "shadow", trustScore: 0.5, observations: 0
        )
        return (SkipOrchestrator(store: store, trustService: trustService), store)
    }

    // MARK: - The preconditions, verified rather than assumed

    /// The row classifies as the show-trust-exempt class. If this fails, every
    /// assertion below is about a different population.
    @Test("the device row classifies as .rediffByteExact")
    func rowClassifiesAsByteExact() {
        let row = Self.deviceRow(id: "exy0-preroll", start: 0.0, end: 30.055291823736166)
        #expect(row.detectorClass == .rediffByteExact)
        #expect(row.extentSupport.tier == .deterministic)
    }

    /// The seed the ledger hands a class nobody has recorded on this show.
    @Test("an unforked ledger seeds .rediffByteExact at .auto on a shadow show")
    func shadowShowSeedsByteExactAuto() async throws {
        let trustStore = try await makeTestStore()
        try await seedSkipTestTrustProfile(
            in: trustStore,
            podcastId: Self.showId,
            mode: "shadow",
            trustScore: 0.5,
            observations: 0
        )
        let modes = await TrustScoringService(store: trustStore)
            .resolveDetectorModes(podcastId: Self.showId)
        #expect(modes.showMode == .shadow, "the pill still reads the show's own mode")
        #expect(modes.mode(for: .rediffByteExact) == .auto)
        #expect(modes.mode(for: .fusion) == .shadow)
    }

    // MARK: - The deliverable

    /// THE BEAD. A day-0 byte-exact pre-roll, delivered by the cross-launch
    /// preload on a show still in shadow, must be SKIPPED — not carded, not left
    /// at `.candidate`.
    @Test("a candidate byte-exact pre-roll auto-skips on a shadow show")
    func candidateByteExactPreRollAutoSkips() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        let pushedCues = OSAllocatedUnfairLock<[CMTimeRange]>(initialState: [])
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues.withLock { $0 = ranges }
        }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.showId
        )

        let cues = pushedCues.withLock { $0 }
        #expect(
            !cues.isEmpty,
            """
            playhead-exy0: nothing promoted the byte-exact window to a skip. \
            The four device rows are eligible, both edges byte-exact, \
            confidence 1.00, and the class seeds .auto — a listener holding \
            this episode hears the ad.
            """
        )
        let preRoll = cues.first { CMTimeGetSeconds($0.start) < 60 }
        let preRollCue = try #require(preRoll, "the 0.0–30.06 pre-roll must be cued")
        #expect(CMTimeGetSeconds(preRollCue.start) == 0.50,
                "0.0 + the 0.50 s rediff byte-exact start margin")
    }

    /// The window's own state, so a green cue assertion cannot be produced by
    /// some other row. `.applied` is the receipt the device rows do not carry.
    @Test("the byte-exact window reaches .applied, not .candidate")
    func byteExactWindowReachesApplied() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.showId
        )
        let log = await orchestrator.getDecisionLog()
        let byWindow = Dictionary(
            grouping: log, by: \.adWindowId
        ).mapValues { $0.map(\.decision) }
        let preRollDecisions = byWindow["exy0-preroll"]?
            .map(\.rawValue)
            .joined(separator: ",") ?? "none — it never entered the managed tier"
        let preRollIngest = await orchestrator
            .lastAdWindowIngestOutcome(forWindowId: "exy0-preroll")?
            .outcome.rawValue ?? "nil"
        #expect(
            byWindow["exy0-preroll"]?.contains(.applied) == true,
            """
            playhead-exy0: the pre-roll never reached .applied. \
            Decisions logged: [\(preRollDecisions)]; \
            ingest outcome \(preRollIngest)
            """
        )
        #expect(byWindow["exy0-postroll"]?.contains(.applied) == true)
    }

    /// THE MINT'S OWN DOOR. `PlayheadRuntime` hands a completed day-0 mint to
    /// `SkipOrchestrator.ingestPersistedAdWindows` (playhead-96ot), so the same
    /// rows must skip when they arrive mid-listen, not only on a relaunch.
    /// Covered separately from the preload because the two doors share
    /// `forwardPersistedAdWindows` but not their guards.
    @Test("the mint's own mid-session door skips the same rows")
    func midSessionIngestSkipsTheSameRows() async throws {
        let (orchestrator, store) = try await Self.makeOrchestrator(mint: false)
        let pushedCues = OSAllocatedUnfairLock<[CMTimeRange]>(initialState: [])
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues.withLock { $0 = ranges }
        }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.showId
        )
        #expect(pushedCues.withLock { $0 }.isEmpty, "control: nothing minted yet")

        try await Self.mintDeviceRows(into: store)
        let forwarded = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Self.assetId
        )
        #expect(forwarded == 2)
        #expect(!pushedCues.withLock { $0 }.isEmpty,
                "a mint delivered mid-listen must skip, not card")
    }

    /// THE DEVICE'S OWN SEQUENCE, db-pull9. The mint landed while nothing was
    /// playing — the census reads
    /// `door=mid_session_ingest … ingest_door_dropped_not_playing=1` for both
    /// assets — and the rows have sat at `.candidate` ever since. This is what
    /// happens when the listener finally opens the episode, and it is why those
    /// four rows are evidence of a delivery that never ran rather than of a
    /// promotion that refused.
    @Test("a mint dropped for want of a session still skips on the next play")
    func mintDroppedWhileNotPlayingSkipsOnTheNextPlay() async throws {
        let (orchestrator, store) = try await Self.makeOrchestrator(mint: false)
        let pushedCues = OSAllocatedUnfairLock<[CMTimeRange]>(initialState: [])
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues.withLock { $0 = ranges }
        }
        try await Self.mintDeviceRows(into: store)

        // No episode is playing: the device's `dropped_not_playing`.
        let forwarded = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Self.assetId
        )
        #expect(forwarded == 0, "the mint had no live session to deliver to")
        #expect(
            await orchestrator.adWindowIngestOutcomeCount(
                .doorDroppedNotPlaying
            ) == 1,
            "and the census must say so, which is what the device pull shows"
        )

        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.showId
        )
        #expect(!pushedCues.withLock { $0 }.isEmpty,
                "the durable rows are picked up by the cross-launch preload")
    }
}
