// ManualVetoReachesPersistedAnalysisTests.swift
// playhead-u45d — "marking a detected span as not an ad is a silent no-op".
//
// The reported gesture is: select transcript rows → "Not ad" → Done → confirm
// "Dismiss ad". Nothing happened, and nothing said so.
//
// Two independent defects produced that one symptom, and a fix to either one
// alone leaves the user looking at the same highlighted span:
//
//   A. `SkipOrchestrator.revertByTimeRange` drew its targets ONLY from the
//      live session's `windows` / `suggestWindows` dictionaries, then returned
//      `false` when that set came up empty. The transcript highlight is drawn
//      from PERSISTED `decoded_spans`, so any span the live session was not
//      tracking — a row below `beginEpisode`'s 0.7 preload confidence floor, a
//      decoded span that never became an `ad_window` at all, an episode that
//      is not the one playing — was structurally un-vetoable.
//
//   B. Even when the veto DID find a target, its only durable mutation was
//      `ad_windows.decisionState = 'reverted'`. The highlight reads
//      `decoded_spans` (which has no decision column) and
//      `ad_windows.boundaryState == "userMarked"` (a different field). The
//      correction succeeded and the display could not see it.
//
// The design these tests pin: the veto's source of truth is the PERSISTED
// store, and the durable "this is not an ad" record is the `CorrectionEvent`
// with an `.exactTimeSpan` scope — the same row `BackfillEvidenceFusion`
// already reads to compute its correction factor. The highlight disappears
// BECAUSE that correction exists, not because the UI hides it: delete the
// correction and the span comes back.

import Foundation
import Testing

@testable import Playhead

@Suite("A transcript veto reaches persisted analysis (playhead-u45d)")
struct ManualVetoReachesPersistedAnalysisTests {

    private func makeVetoSpan(
        assetId: String,
        start: Double,
        end: Double
    ) -> DecodedSpan {
        // Byte-for-byte the shape `TranscriptPeekView.submitNotAdChunks`
        // synthesizes for the callback.
        DecodedSpan(
            id: String(format: "%@-veto-%.3f-%.3f", assetId, start, end),
            assetId: assetId,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: Int.max,
            startTime: start,
            endTime: end,
            anchorProvenance: []
        )
    }

    private func makeDetectedSpan(
        id: String,
        assetId: String,
        start: Double,
        end: Double
    ) -> DecodedSpan {
        DecodedSpan(
            id: id,
            assetId: assetId,
            firstAtomOrdinal: 10,
            lastAtomOrdinal: 20,
            startTime: start,
            endTime: end,
            anchorProvenance: []
        )
    }

    private func makeChunk(
        id: String,
        assetId: String,
        index: Int,
        start: Double,
        end: Double
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: id,
            analysisAssetId: assetId,
            segmentFingerprint: "fingerprint-\(id)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "row \(index)",
            normalizedText: "row \(index)",
            pass: "fast",
            modelVersion: "fast-test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    // MARK: - Defect A

    /// The live trigger on the reporter's device. `beginEpisode` only hydrates
    /// persisted rows at `confidence >= 0.7`, so a lower-confidence (mark-only
    /// tier) window is displayed by the transcript and invisible to the
    /// orchestrator's dictionaries. Before this bead the veto returned `false`
    /// and wrote nothing.
    @Test(
        "A window the live session never hydrated is still vetoable",
        .timeLimit(.minutes(1))
    )
    func vetoReachesAPersistedWindowTheSessionNeverHydrated() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        // Below `SkipOrchestrator.preloadConfidenceThreshold` — `beginEpisode`
        // will read this row and decline to forward it.
        let ad = makeSkipTestAdWindow(
            id: "ad-not-hydrated",
            startTime: 100,
            endTime: 160,
            confidence: 0.55,
            decisionState: AdDecisionState.candidate.rawValue
        )
        try await store.insertAdWindow(ad)

        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )
        #expect(
            await orchestrator.activeWindowIDs().isEmpty,
            """
            Precondition: the orchestrator must NOT be tracking this window, \
            otherwise the test proves nothing about persisted targets.
            """
        )

        let accepted = await orchestrator.revertByTimeRange(
            start: 110,
            end: 150,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: makeVetoSpan(
                assetId: "asset-1",
                start: 110,
                end: 150
            )
        )
        #expect(
            accepted,
            """
            The gesture must report success — the view clears the user's \
            selection only on `true`, so a `false` here IS the reported \
            silent no-op.
            """
        )

        let row = try #require(
            try await store.fetchAdWindow(id: "ad-not-hydrated")
        )
        #expect(
            row.decisionState == AdDecisionState.reverted.rawValue,
            "and the persisted row the user was looking at is reverted"
        )

        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: "asset-1",
            expected: 1
        )
        #expect(receipts.count == 1)
        #expect(receipts.first?.source == .manualVeto)
    }

    /// The other half of Defect A: a `decoded_spans` row that never became an
    /// `ad_window` at all. Spans and windows are different populations — the
    /// Phase 5 projector writes spans from evidence atoms, fusion writes
    /// windows — so the transcript routinely highlights material with no
    /// window behind it. There is no row to revert, but there is still a
    /// correction to record, and refusing the gesture teaches nothing.
    @Test(
        "A highlighted span with no ad_window still records a durable correction",
        .timeLimit(.minutes(1))
    )
    func vetoOfASpanWithNoAdWindowStillRecordsTheCorrection() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.upsertDecodedSpans([
            makeDetectedSpan(
                id: "span-orphan",
                assetId: "asset-1",
                start: 100,
                end: 160
            ),
        ])
        #expect(
            try await store.fetchAdWindows(assetId: "asset-1").isEmpty,
            "Precondition: no ad_window exists for this asset"
        )

        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 4
        )

        let accepted = await orchestrator.revertByTimeRange(
            start: 100,
            end: 160,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 4,
            correctionSpan: makeVetoSpan(
                assetId: "asset-1",
                start: 100,
                end: 160
            )
        )
        #expect(accepted, "a span with no window is still the user's to veto")

        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: "asset-1",
            expected: 1
        )
        let receipt = try #require(receipts.first)
        #expect(receipt.source == .manualVeto)
        #expect(receipt.correctionType == .falsePositive)
        guard case let .exactTimeSpan(scopedAssetId, start, end) =
                try #require(CorrectionScope.deserialize(receipt.scope))
        else {
            Issue.record("veto scope must be an exact time span")
            return
        }
        #expect(scopedAssetId == "asset-1")
        #expect(start == 100)
        #expect(end == 160)
    }

    /// The window-less path must not become a blanket "yes". A tap over audio
    /// the app never flagged — no `ad_window`, no `decoded_spans` row — has no
    /// claim of ours to retract, so it must still refuse rather than record a
    /// correction against nothing and report success.
    ///
    /// This is the boundary that keeps `SkipOrchestratorRevertTests`'
    /// no-overlap trust pin meaningful: the honest failure here is what stops
    /// a stray tap from writing a receipt and a trust penalty.
    @Test(
        "A veto over material the app never flagged is still refused",
        .timeLimit(.minutes(1))
    )
    func vetoOverUnflaggedMaterialIsRefused() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        // A span and a window exist, but BOTH sit far from the gesture.
        try await store.upsertDecodedSpans([
            makeDetectedSpan(
                id: "span-elsewhere",
                assetId: "asset-1",
                start: 100,
                end: 160
            ),
        ])
        try await store.insertAdWindow(
            makeSkipTestAdWindow(
                id: "ad-elsewhere",
                startTime: 100,
                endTime: 160,
                confidence: 0.55,
                decisionState: AdDecisionState.candidate.rawValue
            )
        )

        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 5
        )

        let accepted = await orchestrator.revertByTimeRange(
            start: 800,
            end: 900,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 5,
            correctionSpan: makeVetoSpan(
                assetId: "asset-1",
                start: 800,
                end: 900
            )
        )
        #expect(
            accepted == false,
            "nothing was flagged there, so there is nothing to retract"
        )

        await drainOrchestratorEffects(orchestrator)
        #expect(
            try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
                .isEmpty,
            "and no receipt may be written against material we never flagged"
        )
        let untouched = try #require(
            try await store.fetchAdWindow(id: "ad-elsewhere")
        )
        #expect(untouched.decisionState == AdDecisionState.candidate.rawValue)
    }

    /// ACCEPT THE RECEIPT, REFUSE THE LEARNING — the half of that split the
    /// suite did not pin (review round 1).
    ///
    /// The window-less branch deliberately withholds the trust penalty:
    /// `revertByTimeRange`'s trust block is guarded by `!exactTargets.isEmpty`,
    /// because the trust score measures how often the SKIP SURFACE was wrong
    /// and material that never produced an `ad_window` never risked a skip.
    /// That is a real product decision and it was previously asserted by
    /// nothing — dropping the guard silently penalises a show for a correction
    /// over material it never offered to skip, and `recordWeakFalseSkipSignal`
    /// increments the very same `recentFalseSkipSignals` counter a genuine
    /// false skip does, so the damage is indistinguishable downstream.
    ///
    /// The negative assertion is read behind `drainTrustWrites`, whose own
    /// barrier fails loudly if the trust write path is dead — otherwise "0
    /// signals" would pass just as happily against a service that can no
    /// longer record anything at all.
    @Test(
        "A window-less veto records the correction but NOT a trust penalty",
        .timeLimit(.minutes(1))
    )
    func windowlessVetoWithholdsTheTrustPenalty() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        // A highlighted span with NO `ad_window` behind it.
        try await store.upsertDecodedSpans([
            makeDetectedSpan(
                id: "span-orphan",
                assetId: "asset-1",
                start: 100,
                end: 160
            ),
        ])

        let trustStore = try await makeTestStore()
        try await seedSkipTestTrustProfile(
            in: trustStore,
            podcastId: "podcast-1",
            mode: SkipMode.auto.rawValue,
            trustScore: 0.9,
            observations: 50
        )
        // `recordFalseSkipSignal` never lazy-creates, so the drain barrier
        // needs its own seeded row to be a real barrier.
        try await seedSkipTestTrustProfile(
            in: trustStore,
            podcastId: trustWriteBarrierShow,
            mode: SkipMode.auto.rawValue,
            trustScore: 0.9,
            observations: 50
        )
        let trustService = TrustScoringService(store: trustStore)

        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 7
        )

        let accepted = await orchestrator.revertByTimeRange(
            start: 100,
            end: 160,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 7,
            correctionSpan: makeVetoSpan(
                assetId: "asset-1",
                start: 100,
                end: 160
            )
        )
        #expect(accepted, "the gesture still commits")

        // POSITIVE CONTROL FIRST: the receipt really was written, so a later
        // zero-penalty reading cannot be explained by the gesture having done
        // nothing at all.
        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: "asset-1",
            expected: 1
        )
        #expect(receipts.count == 1, "the correction IS recorded")

        try await drainTrustWrites(trustService, trustStore, orchestrator)
        let profile = try #require(
            try await trustStore.fetchProfile(podcastId: "podcast-1")
        )
        #expect(
            profile.recentFalseSkipSignals == 0,
            """
            A correction over material that never produced an ad_window must \
            NOT penalise the show's trust score — it never risked a skip. \
            The receipt is accepted; only the learning is refused.
            """
        )
        #expect(
            profile.skipTrustScore == 0.9,
            "and the trust score itself is untouched"
        )
    }

    // MARK: - Defect B

    /// The durable correction is what removes the span from the shared read
    /// path. This is deliberately asserted at the STORE, not at the view: the
    /// same `fetchDecodedSpans` every reader calls must answer differently
    /// once the correction exists.
    @Test("A persisted veto removes the span from the shared decoded-span read")
    func persistedVetoRemovesTheSpanFromTheSharedRead() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.upsertDecodedSpans([
            makeDetectedSpan(
                id: "span-vetoed",
                assetId: "asset-1",
                start: 100,
                end: 160
            ),
            makeDetectedSpan(
                id: "span-untouched",
                assetId: "asset-1",
                start: 400,
                end: 460
            ),
        ])

        #expect(
            try await store.fetchDecodedSpans(assetId: "asset-1").count == 2,
            "both spans are visible before any correction"
        )

        _ = try await store.appendCorrectionEvent(
            CorrectionEvent(
                analysisAssetId: "asset-1",
                scope: CorrectionScope.exactTimeSpan(
                    assetId: "asset-1",
                    startTime: 110,
                    endTime: 150
                ).serialized,
                source: .manualVeto,
                podcastId: "podcast-1",
                correctionType: .falsePositive
            )
        )

        let visible = try await store.fetchDecodedSpans(assetId: "asset-1")
        #expect(
            visible.map(\.id) == ["span-untouched"],
            """
            The corrected span is gone from the read every consumer shares, \
            and the span the user did NOT touch is untouched — proving the \
            suppression is keyed on the correction's range, not blanket.
            """
        )

        let raw = try await store.fetchDecodedSpansIncludingUserVetoed(
            assetId: "asset-1"
        )
        #expect(
            raw.map(\.id).sorted() == ["span-untouched", "span-vetoed"],
            """
            The row itself is retained. Structural readers (the width-ownership \
            clobber guard, synthetic-ordinal collision probing, the egress \
            privacy baseline) still see every persisted row.
            """
        )
    }

    /// The suppression's NARROWNESS, which nothing pinned before review round
    /// 1. `userVetoedTimeRanges` reads only `CorrectionSource.manualVeto` —
    /// the one gesture whose stated meaning is "this material is not an ad".
    ///
    /// This boundary is load-bearing in the dangerous direction. `listenRevert`
    /// is recorded whenever the listener rewinds and plays through a skip, and
    /// it is a far weaker claim (they may simply have wanted to hear it). If
    /// the source filter were widened or dropped, ordinary rewind behaviour
    /// would start silently erasing ad highlights across the app — the same
    /// "UI disagrees with the pipeline" failure this bead exists to prevent,
    /// arriving through the front door.
    ///
    /// Note the correction here overlaps the span EXACTLY, so the only thing
    /// keeping the span visible is the source discrimination itself.
    @Test("Only a manual veto suppresses a span — a listenRevert does not")
    func onlyAManualVetoSuppressesTheSpan() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.upsertDecodedSpans([
            makeDetectedSpan(
                id: "span-detected",
                assetId: "asset-1",
                start: 100,
                end: 160
            ),
        ])

        _ = try await store.appendCorrectionEvent(
            CorrectionEvent(
                analysisAssetId: "asset-1",
                scope: CorrectionScope.exactTimeSpan(
                    assetId: "asset-1",
                    startTime: 100,
                    endTime: 160
                ).serialized,
                source: .listenRevert,
                podcastId: "podcast-1",
                correctionType: .falsePositive
            )
        )

        #expect(
            try await store.fetchDecodedSpans(assetId: "asset-1")
                .map(\.id) == ["span-detected"],
            """
            A `listenRevert` over the very same range must NOT suppress the \
            span. Rewinding through a skip is not the assertion "this is not \
            an ad", and treating it as one would erase highlights the \
            detection pipeline still believes in.
            """
        )

        // POSITIVE CONTROL: a manual veto covering the same span DOES
        // suppress it — so the assertion above is discriminating on SOURCE,
        // not silently passing because the range never matched.
        //
        // The control's range is deliberately NOT byte-identical to the
        // `listenRevert` above. `correction_events` dedupes on
        // (assetId, effectiveCorrectionType, normalizedScopeKey,
        // correctionIdentityKey) — `source` is NOT in that key, and
        // `.exactTimeSpan` bounds quantize to a 100 ms bucket — so an
        // identical range would collide with the row already written and
        // `ON CONFLICT … source = COALESCE(correction_events.source,
        // excluded.source)` would KEEP the weaker `listenRevert`. Using the
        // same range here would therefore measure the dedupe collision rather
        // than the source discrimination this test is about.
        //
        // That collision is a real, separately-reported defect (review round
        // 1, playhead-u45d finding 1): a manual veto whose range collides
        // with a pre-existing non-veto false-positive correction is silently
        // swallowed and the span stays highlighted. It is NOT fixed here
        // because both candidate remedies — changing the dedupe identity key,
        // or widening which sources suppress — are persistence/product
        // decisions above this bead's authority.
        _ = try await store.appendCorrectionEvent(
            CorrectionEvent(
                analysisAssetId: "asset-1",
                scope: CorrectionScope.exactTimeSpan(
                    assetId: "asset-1",
                    startTime: 105,
                    endTime: 155
                ).serialized,
                source: .manualVeto,
                podcastId: "podcast-1",
                correctionType: .falsePositive
            )
        )
        #expect(
            try await store.fetchDecodedSpans(assetId: "asset-1").isEmpty,
            "a manual veto overlapping the same span does suppress it"
        )
    }

    // MARK: - The reporter's exact repro

    /// End to end, through the production seam the sheet calls: highlighted
    /// before, not highlighted after, and the correction durable in the store.
    ///
    /// THREE MINUTES, NOT ONE, and only on the two `@MainActor` tests here.
    /// The limit is a hang guard, not an assertion — every expectation below
    /// is unchanged. A `@MainActor` test round-trips to the main actor on
    /// every `await` while ~9,200 tests saturate the cooperative pool, so it
    /// is materially more load-sensitive than its siblings: measured at 0.10s
    /// alone, 5.5s in a four-suite run, and past 60s inside the full gate on a
    /// loaded box — where the non-`MainActor` tests in this same suite came in
    /// at 105s, i.e. the same starvation, just under the old guard. Three
    /// minutes still catches a real hang.
    @Test(
        "Mark not-an-ad → the transcript stops highlighting the span",
        .timeLimit(.minutes(3))
    )
    @MainActor
    func transcriptStopsHighlightingAfterTheVeto() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertTranscriptChunks([
            makeChunk(
                id: "chunk-ad",
                assetId: "asset-1",
                index: 0,
                start: 100,
                end: 130
            ),
            makeChunk(
                id: "chunk-editorial",
                assetId: "asset-1",
                index: 1,
                start: 400,
                end: 430
            ),
        ])
        try await store.upsertDecodedSpans([
            makeDetectedSpan(
                id: "span-detected",
                assetId: "asset-1",
                start: 100,
                end: 160
            ),
        ])
        try await store.insertAdWindow(
            makeSkipTestAdWindow(
                id: "ad-detected",
                startTime: 100,
                endTime: 160,
                confidence: 0.55,
                decisionState: AdDecisionState.candidate.rawValue
            )
        )

        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-1",
            dataSource: LiveTranscriptPeekDataSource(store: store)
        )
        await peek.refresh()
        #expect(peek.chunks.count == 2)
        #expect(
            peek.isAdHighlighted(chunkIndex: 0),
            "precondition: the reporter sees this row highlighted as an ad"
        )
        #expect(peek.isAdHighlighted(chunkIndex: 1) == false)

        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 9
        )

        // Exactly what `TranscriptPeekView.submitNotAdChunks` submits for a
        // selection of the first row.
        let selectedRange = try #require(
            peek.selectedChunkTimeRange(
                selections: [TranscriptChunkSelection(chunk: peek.chunks[0])]
            )
        )
        let accepted = await orchestrator.revertByTimeRange(
            start: selectedRange.start,
            end: selectedRange.end,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 9,
            correctionSpan: makeVetoSpan(
                assetId: "asset-1",
                start: selectedRange.start,
                end: selectedRange.end
            )
        )
        #expect(accepted, "the confirm action must report a committed change")

        await peek.refresh()
        #expect(
            peek.isAdHighlighted(chunkIndex: 0) == false,
            """
            THE BEAD. After "Dismiss ad" the span must no longer be \
            highlighted — and it stops being highlighted because the durable \
            correction now exists, not because the view hid it.
            """
        )
        #expect(
            peek.isAdHighlighted(chunkIndex: 1) == false,
            "and nothing else changed"
        )
        #expect(
            try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
                .contains { $0.source == .manualVeto },
            "the correction is durable where the detection pipeline reads it"
        )
    }

    /// The same defect one field over: the highlight's second source filters
    /// `ad_windows` on `boundaryState == "userMarked"` with no decision
    /// filter, so vetoing an ad the user marked themselves left it lit. The
    /// reverted decision state IS the durable correction — reading it is the
    /// fix, not hiding the row.
    ///
    /// Three minutes for the same reason as the test above: `@MainActor` under
    /// full-gate load, hang guard only, assertions unchanged.
    @Test(
        "A reverted user-marked window stops highlighting its rows",
        .timeLimit(.minutes(3))
    )
    @MainActor
    func revertedUserMarkedWindowStopsHighlighting() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertTranscriptChunks([
            makeChunk(
                id: "chunk-marked",
                assetId: "asset-1",
                index: 0,
                start: 100,
                end: 130
            ),
        ])
        try await store.insertAdWindow(
            AdWindow(
                id: "ad-user-marked",
                analysisAssetId: "asset-1",
                startTime: 100,
                endTime: 160,
                confidence: 1.0,
                boundaryState: "userMarked",
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "userCorrection",
                advertiser: nil,
                product: nil,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: 100,
                metadataSource: "userCorrection",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                eligibilityGate: SkipEligibilityGate.eligible.rawValue
            )
        )

        let peek = TranscriptPeekViewModel(
            analysisAssetId: "asset-1",
            dataSource: LiveTranscriptPeekDataSource(store: store)
        )
        await peek.refresh()
        #expect(
            peek.isAdHighlighted(chunkIndex: 0),
            "precondition: a user-marked window lights its rows"
        )

        try await store.updateAdWindowDecision(
            id: "ad-user-marked",
            decisionState: AdDecisionState.reverted.rawValue
        )
        await peek.refresh()
        #expect(
            peek.isAdHighlighted(chunkIndex: 0) == false,
            "a reverted mark must stop lighting the transcript"
        )
    }
}
