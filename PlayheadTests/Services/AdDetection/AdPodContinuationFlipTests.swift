// AdPodContinuationFlipTests.swift
// playhead-eks2: ad-pod continuation is ON in production, and the three
// properties that make that safe are asserted with the flag at its SHIPPING
// value rather than at a value chosen by the test.
//
// WHY THE FLIP
// ------------
// Dan's device, 2026-08-01, episode D9B513CD (Conan, 65.5 min). Ground truth is
// his own manual marks:
//
//     day-0 rediff FOUND    0.0 -  45.1 s   (ad 1)   confidence 1.00
//     day-0 rediff MISSED  47.6 -  94.3 s   (ad 2)   he marked it by hand
//
// Two creatives in one pre-roll pod, 2.5 s apart. Rediff detects what CHANGED
// between two fetches, and ad 2 was byte-identical in both copies, so no amount
// of rediff improvement finds it. Continuation exists for exactly this: recover
// the pod NEIGHBOURS of an ad we already found.
//
// WHY THE DOWNSIDE IS BOUNDED — and why that is a test, not an argument
// ---------------------------------------------------------------------
// Every continuation row is `markOnly` / `candidate` / both edges `unanchored`.
// Two independent gates sit above it and BOTH are asserted here against rows
// that `runBackfill` actually persisted with the production flag:
//   • playhead-2350 — an unanchored edge can never auto-skip.
//   • playhead-ynmk — a banner confirmation asserts PRESENCE, never EXTENT, so
//     confirming a both-edges-unanchored span MARKS and skips nothing.
// So a wrong continuation costs a wrong BANNER, never lost show.
//
// AND THE DELIVERY PATH (playhead-d3g0 + playhead-96ot)
// -----------------------------------------------------
// Continuation windows arrive MID-LISTEN — the pass runs at the tail of a
// backfill, after the pod's first ad is already found — and reach the live
// session through `ingestPersistedAdWindows`. d3g0's emit is CONTAINMENT
// (`time >= start && time < end`), not a transition, so a window armed while the
// playhead is already inside must fire on the NEXT tick, exactly once.
//
// THE MEASURED BLOCKER THIS SUITE ALSO PINS
// -----------------------------------------
// See `dayZeroRediffMarkCannotSeedAContinuationChain`. Flipping the flag does
// NOT recover Dan's ad 2 from the state his device was actually in, because the
// only row that found ad 1 is a day-0 rediff mark and those are persisted
// `decisionState == .candidate`, while `AdPodContinuation.seedDecisionStates`
// admits only `.confirmed` / `.applied`. The exclusion is deliberate for the
// aggregator's coarse 30 s candidate tiles and accidental for a byte-derived
// slot — `userOwnedBoundaryStates`' own note calls a day-0 rediff row "the most
// obviously correct thing to chain a pod walk off". The two arms of that test
// isolate the predicate: same row, same links, same barriers, only the
// `decisionState` differs.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

// MARK: - Banner reader

/// Single-consumer reader over the banner stream. Owns the iterator, so a pull
/// returns already-buffered items without depending on any other task getting
/// scheduled — the observation method `SuggestBannerEntryGateTests` documents.
private struct FlipBannerReader {
    private var iterator: AsyncStream<AdSkipBannerItem>.AsyncIterator

    init(_ stream: AsyncStream<AdSkipBannerItem>) {
        iterator = stream.makeAsyncIterator()
    }

    /// Every item up to `sentinel`, consuming the sentinel. Empty means the
    /// operation under test emitted nothing — a positive observation, not a
    /// timeout.
    mutating func drain(until sentinel: String) async -> [AdSkipBannerItem] {
        var collected: [AdSkipBannerItem] = []
        while let item = await iterator.next() {
            if item.windowId == sentinel {
                return collected
            }
            collected.append(item)
        }
        return collected
    }
}

@Suite("Ad-pod continuation flip (playhead-eks2)", .timeLimit(.minutes(2)))
struct AdPodContinuationFlipTests {

    // MARK: - The field fixture

    private static let assetId = "asset-eks2-d9b513cd"
    /// MUST be the show `makeSkipTestTrustService` seeds — a show with no
    /// profile resolves to `.shadow`, and every cue assertion below would become
    /// vacuous. `assertAutoMode` is the guard that proves it did not.
    private static let podcastId = "podcast-1"
    private static let episodeDuration = 240.0

    /// Ad 1, verbatim from the field: the byte-exact day-0 rediff slot.
    private static let adOne = (start: 0.0, end: 45.1)
    /// Ad 2, verbatim from the field: byte-identical in both fetches, so rediff
    /// is structurally blind to it. Dan marked it by hand.
    private static let adTwo = (start: 47.6, end: 94.3)
    /// A recovered second at or after this point is SHOW, not pod.
    private static let showResumes = 94.3

    /// The pre-roll pod of the field episode as a transcript: two finished DAI
    /// creatives back to back, then the show. Ad 2's chunks each carry
    /// offer-terms boilerplate — the show-agnostic DAI marker
    /// `AdPodContinuation.adTermsPatterns` exists for — because a stitched
    /// creative never says "this episode is brought to you by".
    private static func fieldChunks() -> [TranscriptChunk] {
        let spans: [(start: Double, end: Double, text: String)] = [
            // ── ad 1: 0.0 - 45.1, the slot rediff caught byte-exactly ──
            (0.0, 15.0, "Squarespace is the all in one platform to build your brand and grow your business online with award winning templates."),
            (15.0, 30.0, "Head to squarespace com slash conan and use code CONAN for ten percent off your first purchase."),
            (30.0, 45.1, "That is squarespace com slash conan. Offer valid for new subscriptions only."),
            // ── the 2.5 s stitch gap ──
            // ── ad 2: 47.6 - 94.3, byte-identical in both fetches ──
            (47.6, 58.0, "This break is brought to you by Rocket Money. Use code CONAN and head to rocketmoney com slash conan. Terms and conditions apply."),
            (58.0, 70.0, "Cancel your unwanted subscriptions and lower your bills without lifting a finger. Offer ends soon and exclusions may apply."),
            (70.0, 82.0, "Carters has your family covered for every summer first, from the first splash to the last popsicle, while supplies last."),
            (82.0, 94.3, "Shop the latest styles at participating retailers, and see our website for more details before the season turns."),
            // ── show resumes ──
            (94.3, 120.0, "So I was telling you about the drive up the coast and the diner where the waitress recognised my hair before she recognised me."),
            (120.0, 160.0, "She had a photograph behind the register of a man who looked exactly like my father, which I found genuinely unsettling for about an hour."),
            (160.0, 200.0, "And the whole time Sona is in the back seat pretending she has never met either of us, which is her strongest performance to date."),
            (200.0, 240.0, "We drove another two hours after that and nobody said a single word about the photograph, which I think says something about this family.")
        ]
        return spans.enumerated().map { index, span in
            TranscriptChunk(
                id: "eks2-c\(index)",
                analysisAssetId: assetId,
                segmentFingerprint: "eks2-fp-\(index)",
                chunkIndex: index,
                startTime: span.start,
                endTime: span.end,
                text: span.text,
                normalizedText: span.text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    /// The day-0 rediff mark EXACTLY as `mintByteExactDayZeroMarks` persists a
    /// STRICT byte-exact slot: confidence 1.0, `.candidate`, mark-only, both
    /// edges `.rediffByteExact`. Not a paraphrase — every field here is read off
    /// the production mint site, because this row's `decisionState` is the whole
    /// point of `dayZeroRediffMarkCannotSeedAContinuationChain`.
    private static func dayZeroRediffMark(
        decisionState: String = AdDecisionState.candidate.rawValue
    ) -> AdWindow {
        AdWindow(
            id: "eks2-day0-ad1",
            analysisAssetId: assetId,
            startTime: adOne.start,
            endTime: adOne.end,
            confidence: 1.0,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: decisionState,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: adOne.start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
        )
    }

    /// The link + barrier derivation `runBackfill` Step 18b performs, byte for
    /// byte. Kept in one place so a compose-level test can never measure a
    /// configuration production does not run.
    private static func productionLinks(
        _ chunks: [TranscriptChunk]
    ) -> [AdPodContinuation.AdCopyLink] {
        let hits = LexicalScanner().collectHits(chunks: chunks)
        return AdPodContinuation.mergeLinks(
            AdPodContinuation.adCopyLinks(chunks: chunks, hits: hits)
                + AdPodContinuation.rhetoricalLinks(chunks: chunks)
        )
    }

    private static func productionBarriers(
        _ chunks: [TranscriptChunk]
    ) -> [AdPodContinuation.ContentBarrier] {
        AdPodContinuation.contentBarriers(
            semanticScanResults: [],
            lexicalHits: LexicalScanner().collectHits(chunks: chunks),
            chunks: chunks
        )
    }

    /// Seconds of `windows` that land inside `span`.
    private static func overlap(
        _ windows: [AdWindow],
        with span: (start: Double, end: Double)
    ) -> Double {
        windows.reduce(0.0) { total, window in
            total + max(0, min(window.endTime, span.end) - max(window.startTime, span.start))
        }
    }

    // MARK: - 1. The flip itself

    /// The one-line change this bead exists for. `PlayheadRuntime` builds the
    /// live service from `AdDetectionConfig.default` (`PlayheadRuntime:952`), so
    /// this constant IS the production setting — not a proxy for it.
    @Test("the production config ships pod continuation ON")
    func productionConfigEnablesPodContinuation() {
        #expect(
            AdDetectionConfig.default.podContinuationEnabled,
            "playhead-eks2 flipped this ON (Dan, 2026-08-01); PlayheadRuntime builds the live service from .default"
        )
    }

    /// The init default must track the shipped value, the way every other
    /// flipped flag in this config does (`rediffSlotOwnershipEnabled`,
    /// `selfPromoSuppressionEnabled`, `unanchoredExtentBlocksAutoSkip`, …).
    /// A config built without the argument is what 71 test files construct; if
    /// it disagreed with `.default`, every one of them would be measuring a
    /// configuration production does not run.
    @Test("a config built without the argument matches the shipped value")
    func initDefaultTracksTheShippedValue() {
        let implicit = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "eks2-implicit"
        )
        #expect(implicit.podContinuationEnabled)
        #expect(
            implicit.podContinuationEnabled
                == AdDetectionConfig.default.podContinuationEnabled,
            "the init default and the production default must not drift"
        )
    }

    // MARK: - 2. Dan's field pod, measured

    /// THE MEASURED BLOCKER. Two arms over the field pod, differing ONLY in the
    /// seed row's `decisionState`. Same window geometry, same confidence, same
    /// provenance, same links, same barriers.
    ///
    /// A day-0 byte-exact rediff mark is persisted `.candidate`
    /// (`mintByteExactDayZeroMarks`), and `AdPodContinuation.seedDecisionStates`
    /// is `{confirmed, applied}` — so on Dan's device, where the ONLY row that
    /// found ad 1 was that mark, flipping the flag recovers nothing. The same
    /// row as `.confirmed` recovers essentially all of ad 2.
    ///
    /// This is a characterization test, not an endorsement: it names the
    /// predicate that decides, so the follow-up carve-out is a deliberate edit
    /// here rather than a silent behaviour change.
    @Test("Field case: a day-0 rediff mark cannot seed, the same row confirmed can")
    func dayZeroRediffMarkCannotSeedAContinuationChain() {
        let chunks = Self.fieldChunks()
        let links = Self.productionLinks(chunks)
        let barriers = Self.productionBarriers(chunks)
        #expect(
            links.contains { $0.start < Self.adTwo.end && $0.end > Self.adTwo.start },
            "fixture must carry ad-copy evidence inside ad 2, else both arms are vacuous"
        )

        let asMinted = Self.dayZeroRediffMark()
        #expect(
            !AdPodContinuation.isSeed(asMinted),
            "a day-0 rediff mark is .candidate, and seedDecisionStates is {confirmed, applied}"
        )
        let mintedMarks = AdPodContinuation.compose(
            existingWindows: [asMinted],
            adCopyLinks: links,
            contentBarriers: barriers,
            protectedRegions: [],
            episodeDuration: Self.episodeDuration,
            analysisAssetId: Self.assetId
        )
        #expect(
            mintedMarks.isEmpty,
            """
            Dan's device state recovered \(Self.overlap(mintedMarks, with: Self.adTwo)) s of ad 2. \
            If this now passes material, the seed predicate changed — update the bead, \
            do not weaken the assertion.
            """
        )

        let asConfirmed = Self.dayZeroRediffMark(
            decisionState: AdDecisionState.confirmed.rawValue
        )
        #expect(AdPodContinuation.isSeed(asConfirmed))
        let confirmedMarks = AdPodContinuation.compose(
            existingWindows: [asConfirmed],
            adCopyLinks: links,
            contentBarriers: barriers,
            protectedRegions: [],
            episodeDuration: Self.episodeDuration,
            analysisAssetId: Self.assetId
        )
        let recovered = Self.overlap(confirmedMarks, with: Self.adTwo)
        let adTwoWidth = Self.adTwo.end - Self.adTwo.start
        #expect(
            recovered >= adTwoWidth * 0.9,
            "a confirmed seed must recover ad 2; got \(recovered) s of \(adTwoWidth) s"
        )
        // ZERO SHOW SECONDS. The walk terminates at the last link's end, and the
        // show resumes at 94.3 — a mark reaching past it is the failure this
        // whole mechanism is built to avoid.
        for mark in confirmedMarks {
            #expect(
                mark.endTime <= Self.showResumes,
                "mark \(mark.startTime)-\(mark.endTime) reached into the show"
            )
        }
    }

    // MARK: - 3. End-to-end through the production flag

    /// A CONFIRMED detector window on ad 1 only — the seed a fused pipeline
    /// produces. `detectorVersion` differs from the service's, so the backfill
    /// reconcile leaves it alone and this test does not become a test of fusion
    /// confidence tuning.
    private static func fusionSeedOverAdOne() -> AdWindow {
        AdWindow(
            id: "eks2-fusion-ad1",
            analysisAssetId: assetId,
            startTime: adOne.start,
            endTime: adOne.end,
            confidence: 0.91,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "eks2-seed-detector-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: adOne.start,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
    }

    /// Run the real backfill at the SHIPPED flag value and return the store plus
    /// every persisted continuation row.
    ///
    /// `podContinuationEnabled` is read from `AdDetectionConfig.default` rather
    /// than written as a literal: these rows, and therefore every safety
    /// assertion built on them, describe what production does. Flip the flag back
    /// and these tests report it.
    private static func runShippedBackfill() async throws
        -> (store: AnalysisStore, continuation: [AdWindow]) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: assetId)
        )
        try await store.insertAdWindows([fusionSeedOverAdOne()])
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "eks2-detection-v1",
                fmBackfillMode: .off,
                podContinuationEnabled:
                    AdDetectionConfig.default.podContinuationEnabled
            )
        )
        try await service.runBackfill(
            chunks: fieldChunks(),
            analysisAssetId: assetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration
        )
        let rows = try await store.fetchAdWindows(assetId: assetId)
        return (
            store,
            rows
                .filter { $0.detectorVersion == AdPodContinuation.detectorVersion }
                .sorted { $0.startTime < $1.startTime }
        )
    }

    @Test("the shipped flag persists a continuation mark over the missed creative")
    func shippedFlagRecoversTheMissedCreative() async throws {
        let (_, continuation) = try await Self.runShippedBackfill()
        #expect(
            !continuation.isEmpty,
            "Step 18b must fire at the production flag value on a two-creative pod"
        )
        let recovered = Self.overlap(continuation, with: Self.adTwo)
        #expect(
            recovered > 20.0,
            "recovery must cover the missed creative; got \(recovered) s of \(Self.adTwo.end - Self.adTwo.start) s"
        )
        for row in continuation {
            #expect(
                row.endTime <= Self.showResumes,
                "row \(row.startTime)-\(row.endTime) reached into the show"
            )
        }
    }

    // MARK: - 4/5/6. The orchestrator harness

    /// A LIVE-SESSION store carrying the asset and exactly the rows given.
    ///
    /// Deliberately not the backfill's own store: that one also holds the
    /// `.eligible` fusion seed, which the `beginEpisode` preload would forward
    /// and auto-skip, so "no cue fired" would be about the wrong window. Here the
    /// only detector material is the continuation output, and the positive
    /// control below proves this harness can fire a cue at all.
    private static func makeSessionStore(
        with rows: [AdWindow]
    ) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: assetId)
        )
        if !rows.isEmpty {
            try await store.insertAdWindows(rows)
        }
        return store
    }

    private static func makeOrchestrator(
        store: AnalysisStore
    ) async throws -> SkipOrchestrator {
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        return SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
    }

    /// Fail loudly if the orchestrator is not in auto mode. Every "no cue fired"
    /// assertion below is about the AUTOMATIC path; in `.shadow` or `.manual` no
    /// cue ever fires and the negatives pass for the wrong reason.
    private static func assertAutoMode(_ orchestrator: SkipOrchestrator) async {
        #expect(
            await orchestrator.currentSkipMode() == .auto,
            "fixture is not in auto mode — cue assertions would be vacuous"
        )
    }

    /// The VACUITY CONTROL. An ordinary confirmed window with no precision gate
    /// cues immediately on ingest (`AutoSkipEdgePaddingWiringTests.offIsNoOp`
    /// pins the same recipe). If this stops firing, every "no cue" assertion in
    /// this suite has become an assertion about a broken harness.
    private static func autoSkippableControl() -> AdWindow {
        makeSkipTestAdWindow(
            id: "eks2-control-eligible",
            assetId: assetId,
            startTime: 150,
            endTime: 170,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
    }

    // MARK: - 4. playhead-2350 with the flag ON

    /// A continuation row that `runBackfill` actually persisted, walked past by
    /// the playhead in AUTO mode, must never produce a skip cue — while still
    /// producing a banner, so the negative is not vacuous.
    @Test("2350: a persisted continuation span never auto-skips, and still banners")
    func continuationSpanNeverAutoSkips() async throws {
        let (_, continuation) = try await Self.runShippedBackfill()
        let row = try #require(continuation.first, "no continuation row to test")
        #expect(row.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
        #expect(row.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
        #expect(row.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)

        let store = try await Self.makeSessionStore(with: continuation)
        let orchestrator = try await Self.makeOrchestrator(store: store)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)
        var reader = FlipBannerReader(await orchestrator.bannerItemStream())

        // VACUITY CONTROL FIRST, at [150, 170) — past the pod and past the walk
        // below, so it can never be consumed by it. Every "no cue" assertion
        // that follows is only meaningful because this one fired.
        await orchestrator.receiveAdWindows([Self.autoSkippableControl()])
        #expect(
            !pushedCues.isEmpty,
            "the control window must cue — otherwise every 'no cue' below is about a broken harness"
        )
        let baselineCueCount = pushedCues.count

        await orchestrator.receiveAdWindows(continuation)
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(row.id),
            "a continuation row must reach the SUGGEST tier — otherwise nothing below is exercised"
        )
        #expect(
            pushedCues.count == baselineCueCount,
            "ingesting continuation rows added a cue"
        )

        // Walk the playhead across the whole recovered span in transport ticks.
        let spanStart = continuation.map(\.startTime).min() ?? 0
        let spanEnd = continuation.map(\.endTime).max() ?? 0
        var time = max(0, spanStart - 1)
        while time <= spanEnd + 1 {
            await orchestrator.updatePlayheadTime(time)
            time += PlaybackService.periodicTimeObserverIntervalSeconds
        }
        #expect(
            pushedCues.count == baselineCueCount,
            "walking the recovered span added a cue"
        )
        let trespassing = pushedCues.filter { cue in
            let cueStart = CMTimeGetSeconds(cue.start)
            let cueEnd = CMTimeGetSeconds(cue.start + cue.duration)
            return continuation.contains {
                cueStart < $0.endTime && cueEnd > $0.startTime
            }
        }
        #expect(
            trespassing.isEmpty,
            """
            An unanchored, mark-only continuation span defined a skip boundary. \
            playhead-2350 is the gate that must prevent it. Got \
            \(trespassing.map { (CMTimeGetSeconds($0.start), CMTimeGetSeconds($0.start + $0.duration)) })
            """
        )

        // The banner DID fire — so "no cue" above is a policy result, not silence.
        await orchestrator.receiveAdWindows([
            Self.sentinelSuggestion(id: "eks2-2350-sentinel", at: 120)
        ])
        await orchestrator.updatePlayheadTime(120)
        let banners = await reader.drain(until: "eks2-2350-sentinel")
        #expect(
            banners.contains { $0.windowId == row.id && $0.tier == .suggest },
            "the recovered pod material must still reach the listener as a banner; got \(banners.map(\.windowId))"
        )
    }

    /// A mark-only sentinel used to bound a stream drain. Deliberately NOT a
    /// continuation row: it exists only to mark the frame boundary.
    private static func sentinelSuggestion(id: String, at time: Double) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: time,
            endTime: time + 4,
            confidence: 0.71,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "eks2-sentinel-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: time,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    // MARK: - 5. playhead-ynmk with the flag ON

    /// Tapping Yes on a continuation banner records a MARK. It cannot cut,
    /// because a confirmation asserts presence and the row has no anchored edge
    /// to define extent from.
    @Test("ynmk: confirming a continuation banner marks, it does not skip")
    func confirmingAContinuationBannerMarksRatherThanSkips() async throws {
        let (_, continuation) = try await Self.runShippedBackfill()
        let row = try #require(continuation.first, "no continuation row to test")

        let store = try await Self.makeSessionStore(with: continuation)
        let orchestrator = try await Self.makeOrchestrator(store: store)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        await orchestrator.receiveAdWindows([row])
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(row.id),
            "the tap under test never happens unless the row reaches the suggest tier"
        )
        #expect(
            await orchestrator.acceptSuggestedSkip(windowId: row.id),
            "the gesture is accepted as FEEDBACK even when no extent is skippable"
        )
        #expect(
            pushedCues.isEmpty,
            """
            One tap turned recovered pod material into a skip. That is the ynmk \
            field defect (210 s of show, 2026-07-31) reached through a new door. Got \
            \(pushedCues.map { (CMTimeGetSeconds($0.start), CMTimeGetSeconds($0.start + $0.duration)) })
            """
        )

        let rows = try await store.fetchAdWindows(assetId: Self.assetId)
        let promoted = try #require(
            rows.first { $0.boundaryState == "userConfirmedSuggested" },
            "the confirmation must still create its durable promoted row"
        )
        #expect(
            promoted.wasSkipped == false,
            "no cue fired, so `wasSkipped` must not say one did"
        )
        #expect(
            promoted.decisionState == AdDecisionState.confirmed.rawValue,
            "`applied` means the listener's audio was skipped; nothing was"
        )
        #expect(
            promoted.confidence == row.confidence,
            "a tap must never write confidence; got \(promoted.confidence) from \(row.confidence)"
        )

        // VACUITY CONTROL: this harness CAN fire a cue.
        await orchestrator.receiveAdWindows([Self.autoSkippableControl()])
        #expect(
            !pushedCues.isEmpty,
            "the control window must cue — otherwise 'the tap fired no cue' is about a broken harness"
        )
    }

    // MARK: - 6. playhead-d3g0 + playhead-96ot delivery

    /// The real mid-listen shape: the playhead is ALREADY inside the recovered
    /// span when the backfill persists it, and the rows reach the live session
    /// through `ingestPersistedAdWindows`.
    ///
    /// Three things at once, because separating them would let each pass for the
    /// other's reason: ingest ARMS but does not emit; the next tick INSIDE the
    /// span emits (containment, not a transition); and however many further ticks
    /// land inside, it emits exactly ONCE.
    @Test("d3g0: a continuation window ingested mid-listen arms, fires on entry, once")
    func continuationWindowArmsOnIngestAndFiresOnceOnEntry() async throws {
        let (_, continuation) = try await Self.runShippedBackfill()
        let row = try #require(continuation.first, "no continuation row to test")
        let inside = (row.startTime + row.endTime) / 2.0

        // The real mid-listen ordering: the session begins with NO continuation
        // rows on disk (the pass has not run yet), the listener plays into the
        // pod, and only then does the backfill persist and 96ot deliver. Seeding
        // the store first would let `beginEpisode`'s preload arm them, and the
        // ingest under test would be a no-op replay.
        let store = try await Self.makeSessionStore(with: [])
        let orchestrator = try await Self.makeOrchestrator(store: store)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.assetId,
            podcastId: Self.podcastId
        )
        var reader = FlipBannerReader(await orchestrator.bannerItemStream())

        // The listener is already inside the pod when the pass composes.
        await orchestrator.updatePlayheadTime(inside)
        try await store.insertAdWindows(continuation)

        let forwarded = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Self.assetId
        )
        #expect(forwarded > 0, "96ot must forward the freshly-persisted rows")
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(row.id),
            "ingest must ARM the continuation window"
        )

        // Ingest itself emits nothing — d3g0's whole point. A sentinel driven
        // OUTSIDE every continuation span proves the buffer is empty rather than
        // proving a race we won.
        await orchestrator.receiveAdWindows([
            Self.sentinelSuggestion(id: "eks2-d3g0-arm", at: Self.episodeDuration - 5)
        ])
        await orchestrator.updatePlayheadTime(Self.episodeDuration - 5)
        let atIngest = await reader.drain(until: "eks2-d3g0-arm")
        #expect(
            !atIngest.contains { $0.windowId == row.id },
            "ingest must not emit; d3g0 gives emission to the position path"
        )

        // Back inside the span: the next tick fires it. Containment, not a
        // transition — the playhead never crossed the start edge.
        await orchestrator.updatePlayheadTime(inside)
        await orchestrator.receiveAdWindows([
            Self.sentinelSuggestion(id: "eks2-d3g0-entry", at: Self.episodeDuration - 10)
        ])
        await orchestrator.updatePlayheadTime(Self.episodeDuration - 10)
        let onEntry = await reader.drain(until: "eks2-d3g0-entry")
        #expect(
            onEntry.contains { $0.windowId == row.id && $0.tier == .suggest },
            "a window armed while the playhead is inside must fire on the next tick; got \(onEntry.map(\.windowId))"
        )

        // Twelve more ticks inside the same span must ask nothing further.
        var time = inside
        for _ in 0..<12 {
            time += PlaybackService.periodicTimeObserverIntervalSeconds
            if time >= row.endTime { break }
            await orchestrator.updatePlayheadTime(time)
        }
        await orchestrator.receiveAdWindows([
            Self.sentinelSuggestion(id: "eks2-d3g0-once", at: Self.episodeDuration - 15)
        ])
        await orchestrator.updatePlayheadTime(Self.episodeDuration - 15)
        let repeats = await reader.drain(until: "eks2-d3g0-once")
        #expect(
            !repeats.contains { $0.windowId == row.id },
            "fires at most once per window per episode; got \(repeats.map(\.windowId))"
        )
    }
}
