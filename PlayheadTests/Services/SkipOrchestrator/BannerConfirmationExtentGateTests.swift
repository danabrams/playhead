// BannerConfirmationExtentGateTests.swift
// playhead-ynmk: a banner confirmation asserts PRESENCE, never EXTENT.
//
// The field case (Dan's clean install, 2026-07-31, episode DE0784D8, 92 min).
// Three `segmentAggregated` windows were offered as suggest-tier banners and
// confirmed with one tap each. At CONFIRMATION TIME every one carried:
//
//     confidence         0.40 - 0.42
//     startEdgeAnchor    unanchored
//     endEdgeAnchor      unanchored
//     eligibilityGate    markOnly
//     wasSkipped         false
//
// One tap each turned them into `confidence 1.00 / applied / wasSkipped TRUE`
// with the gate cleared. All three were FALSE POSITIVES — aggregator precision
// on this episode's mid-rolls was 0 of 3 — and 30 + 150 + 30 = 210 seconds of
// SHOW was skipped.
//
// Two defects, pinned separately below.
//
//   1. EXTENT. playhead-2350 blocks auto-skip on unanchored edges: presence of
//      an ad is not extent of an ad. That gate held — it marked all three
//      `markOnly`. A single tap cleared it, because `acceptSuggestedSkip`
//      promoted under a fresh UUID (no gate, no anchors stamped) and
//      `isUserInitiatedSkip` exempted `userConfirmedSuggested` from the
//      `AutoSkipEdgePadding` extent policy outright. The exemption's premise —
//      "the user chose those edges deliberately" — is false for a confirmation:
//      the user answered "is this an ad?", and the DETECTOR drew the edges.
//      Contrast `userMarked` (playhead-527u), where the user really did draw
//      them; that exemption is correct and is pinned here so the fix cannot
//      overshoot into it.
//
//   2. MEASUREMENT. `confidence = 1.0` was synthesised by the tap. Apply the
//      standing check: what would that 1.00 read if the span were pure show
//      content? Still 1.00 — it records that a tap happened, not that an ad
//      exists. Same defect class as pz32's checkmark. The measured value must
//      survive the tap; the assertion belongs in its own field
//      (`boundaryState == "userConfirmedSuggested"`, plus the
//      `.bannerSuggestionConfirmed` CorrectionEvent's detection projection).
//
// Do NOT weaken playhead-qs0d: a day-0 byte-exact rediff span is anchored on
// both edges and SHOULD skip on confirmation — that is the 2-of-2 population.
// Pinned by `anchoredConfirmationStillSkips`.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

@Suite("Banner confirmation extent gate (playhead-ynmk)")
struct BannerConfirmationExtentGateTests {

    // MARK: - Fixture

    private static let assetId = "asset-1"
    private static let episodeId = "ep-ynmk"
    /// MUST be the show `makeSkipTestTrustService` seeds. A show with no trust
    /// profile resolves to `.shadow`, and every auto-mode assertion in this
    /// suite silently becomes vacuous — that happened once while writing it,
    /// and `assertAutoMode` below is the guard that caught it.
    private static let podcastId = "podcast-1"

    /// The 150 s mid-roll from the field case: `4800.0 - 4950.0`, confidence
    /// 0.40, both edges unanchored, `eligibilityGate = markOnly`.
    private static let fieldStart = 4800.0
    private static let fieldEnd = 4950.0
    private static let fieldConfidence = 0.40

    /// playhead-hcpa: the two-number (post-V47) shape — a span the DETECTOR
    /// likes and the user has SUPPRESSED.
    ///
    /// The pair is chosen so the two numbers land on opposite sides of every
    /// actuation gate in the orchestrator: 0.90 clears both the 0.65
    /// `enterThreshold` and the 0.7 `preloadConfidenceThreshold`, 0.12 clears
    /// neither. That is what makes the fallback a FAIL-OPEN rather than a
    /// cosmetic difference — drop the forward and `actuationConfidence`
    /// resolves to 0.90, i.e. to "skip it".
    private static let suppressedDetectionConfidence = 0.90
    private static let suppressedActuationConfidence = 0.12

    private static func cueStart(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start)
    }

    private static func cueEnd(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start + cue.duration)
    }

    /// `pushMergedCues` builds its `CMTime`s at `preferredTimescale: 600`, so a
    /// bound that is not a multiple of 1/600 s is quantised. The field spans in
    /// this suite (5462.6 / 5522.7) are not, so an exact `==` — or a tolerance
    /// tighter than one tick — fails on arithmetic, not on policy. One tick is
    /// the honest tolerance: every error this suite is looking for (a missing
    /// 0.50/0.75/10.25 margin, a missing 1.0 s cushion, a raw unpadded span) is
    /// three orders of magnitude larger.
    private static let cueTickTolerance = 1.0 / 600.0 + 1e-9

    /// A suggest-tier (`markOnly`) window with explicit per-edge anchors and an
    /// explicit measured confidence — the two inputs this bead is about.
    ///
    /// playhead-hcpa: `skipConfidence` defaults to `nil` — the one-number shape
    /// every other test here relies on (`actuationConfidence == confidence`).
    /// Only `confirmationPreservesSuppressedActuation` passes it, because it is
    /// the only test that needs a TWO-number row.
    private static func makeSuggestion(
        id: String,
        start: Double = fieldStart,
        end: Double = fieldEnd,
        confidence: Double = fieldConfidence,
        skipConfidence: Double? = nil,
        startAnchor: AutoSkipEdgeAnchor = .unanchored,
        endAnchor: AutoSkipEdgeAnchor = .unanchored
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            skipConfidence: skipConfidence,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: start,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: startAnchor.rawValue,
            endEdgeAnchor: endAnchor.rawValue
        )
    }

    /// Auto-mode orchestrator over the standard store/trust harness, with the
    /// Gate-2 edge-padding master switch left at its OFF production default.
    private static func makeHarness() async throws
        -> (orchestrator: SkipOrchestrator, store: AnalysisStore) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        return (orchestrator, store)
    }

    /// Fail loudly if the orchestrator is not actually in auto mode. Every
    /// "a cue fired" / "no cue fired" assertion here is about the automatic
    /// path; in `.shadow` or `.manual` no cue ever fires and the negative
    /// assertions pass for the wrong reason.
    private static func assertAutoMode(
        _ orchestrator: SkipOrchestrator
    ) async {
        #expect(
            await orchestrator.currentSkipMode() == .auto,
            "fixture is not in auto mode — cue assertions would be vacuous"
        )
    }

    /// The persisted row the confirmation created (the fresh-UUID promotion),
    /// identified by NOT being the original suggestion's id.
    private static func promotedRow(
        in store: AnalysisStore,
        originalId: String
    ) async throws -> AdWindow? {
        let rows = try await store.fetchAdWindows(assetId: assetId)
        return rows.first { $0.id != originalId }
    }

    // MARK: - 1. The field regression

    /// The reproduction. A 0.40-confidence window with BOTH edges unanchored,
    /// confirmed by one tap, must not skip 150 s of show.
    ///
    /// Note what is deliberately NOT set: `setEdgePaddingEnabled`. The Gate-2
    /// master switch is OFF in production and stays off here — a confirmation's
    /// extent is governed by the derived per-edge policy regardless, exactly as
    /// playhead-qs0d made a both-edges-byte-exact span governed regardless.
    @Test("Field case: a tap on a both-edges-unanchored 150 s span skips nothing")
    func unanchoredConfirmationSkipsNoShowContent() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        let suggested = Self.makeSuggestion(id: "ynmk-field-midroll")
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        #expect(
            await orchestrator.activeSuggestWindowIDs()
                .contains("ynmk-field-midroll"),
            "markOnly must reach the suggest tier — otherwise the tap under test never happens"
        )
        #expect(pushedCues.isEmpty, "markOnly ingest must not fire a cue")

        await orchestrator.acceptSuggestedSkip(windowId: "ynmk-field-midroll")

        #expect(
            pushedCues.isEmpty,
            """
            A confirmation on a span with NO anchored edge has no evidence for \
            where the ad ends. It must not define a skip boundary. Got cues: \
            \(pushedCues.map { (Self.cueStart($0), Self.cueEnd($0)) })
            """
        )
    }

    /// The other half of the same tap: the durable row must not claim a skip
    /// that never fired, and must not claim a certainty nothing measured.
    @Test("Field case: the promoted row records the tap, not a skip and not a certainty")
    func unanchoredConfirmationRowIsHonest() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        let suggested = Self.makeSuggestion(id: "ynmk-row-honesty")
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        await orchestrator.acceptSuggestedSkip(windowId: "ynmk-row-honesty")

        let promoted = try #require(
            await Self.promotedRow(in: store, originalId: "ynmk-row-honesty"),
            "the confirmation must still create its durable promoted row"
        )
        #expect(
            promoted.confidence == Self.fieldConfidence,
            """
            A tap must never write `confidence`. It is the DETECTOR's measured \
            quantity; synthesising 1.0 makes a pure-show span read identically \
            to a real ad. Got \(promoted.confidence).
            """
        )
        #expect(
            promoted.boundaryState == "userConfirmedSuggested",
            "the assertion belongs in its own field, and this is that field"
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
            promoted.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue
                && promoted.endEdgeAnchor
                    == AutoSkipEdgeAnchor.unanchored.rawValue,
            "the promoted row must carry the suggestion's real edge provenance"
        )
        // playhead-v7q6: the refusal must be RECORDED, not silent — but the
        // record is this durable row, not the diagnostic decision log.
        //
        // ynmk originally asserted a `logDecision` entry here. That collided
        // with `testExplicitBannerFeedbackRoutesDoNotWriteDetailedLogs`, which
        // forbids every explicit-feedback route from reaching the decision
        // logger, because `logDecision(managed:)` writes the window's exact
        // span and exact feedback receipts belong only in the durable
        // correction store. Two deliberate contracts, directly opposed; the
        // privacy rail wins and the observability intent survives as an
        // id-only os_log line.
        //
        // Nothing is lost. The pair asserted immediately above IS the audit
        // trail and is strictly stronger than a log string: `.confirmed` with
        // `wasSkipped == false` can only mean a confirmation that arrived and
        // was deliberately not applied. A confirmation that never arrived
        // writes no row at all — so the two cases the comment worried about
        // conflating are already distinguishable, durably, and survive a
        // relaunch in a way an in-memory decision log never did.
        #expect(
            promoted.decisionState == AdDecisionState.confirmed.rawValue
                && promoted.wasSkipped == false,
            "a refused confirmation must be durably distinguishable from one that never arrived"
        )
    }

    /// playhead-hcpa: the ACTUATION half of the same promotion.
    ///
    /// `unanchoredConfirmationRowIsHonest` above pins `confidence` — the
    /// DETECTION number. ar60's V47 split gave the row a second number,
    /// `skipConfidence`, and the promotion forwards it; nothing asserted that
    /// forward, so deleting the argument left the whole suite green.
    ///
    /// Why it is the dangerous half. `actuationConfidence` is
    /// `skipConfidence ?? confidence`, so a dropped forward does not read as
    /// "missing" — it reads as the DETECTION score, which for a span the user
    /// suppressed is the HIGHER of the two. The row would come back through
    /// `preloadAdmissibleWindows` and `evaluateWindow` carrying permission it
    /// was never granted: Playhead skipping audio the listener said not to
    /// skip. A fail-open, not a fail-closed.
    ///
    /// The first two expectations are the anti-vacuity control and they are
    /// load-bearing, not decoration. Because of the `?? confidence` fallback,
    /// an assertion "moved" onto the actuation number can silently be the
    /// detection assertion wearing a new name — and would pass on a fixture
    /// whose `skipConfidence` is nil, or whose two numbers are equal. So the
    /// test first proves the row really has TWO numbers and that they DIFFER;
    /// only then is the third expectation capable of failing.
    @Test("hcpa: a confirmation preserves the SUPPRESSED actuation number, not just the detection one")
    func confirmationPreservesSuppressedActuation() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        let suggested = Self.makeSuggestion(
            id: "hcpa-suppressed-actuation",
            confidence: Self.suppressedDetectionConfidence,
            skipConfidence: Self.suppressedActuationConfidence
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        await orchestrator.acceptSuggestedSkip(
            windowId: "hcpa-suppressed-actuation"
        )

        let promoted = try #require(
            await Self.promotedRow(
                in: store,
                originalId: "hcpa-suppressed-actuation"
            ),
            "the confirmation must still create its durable promoted row"
        )

        // Control 1: the fixture is a TWO-number row. Without this, every
        // assertion below reads `confidence` through the fallback and the
        // test is the detection rail renamed.
        #expect(
            promoted.skipConfidence != nil,
            """
            Anti-vacuity: the promoted row must carry a SEPARATE actuation \
            number. `nil` means `actuationConfidence` collapsed onto \
            `confidence` and every actuation assertion here became a \
            restatement of the detection one.
            """
        )
        // Control 2: the two numbers DIFFER, so an assertion on one cannot be
        // satisfied by the other.
        #expect(
            Self.suppressedActuationConfidence
                != Self.suppressedDetectionConfidence,
            "fixture integrity: equal numbers make the direction test vacuous"
        )

        #expect(
            promoted.actuationConfidence
                == Self.suppressedActuationConfidence,
            """
            The promotion must carry the SUPPRESSED actuation number through \
            unchanged. Got \(promoted.actuationConfidence); the detection \
            score is \(Self.suppressedDetectionConfidence), and reading that \
            here would re-grant a span the user suppressed enough permission \
            to clear the 0.65 enter threshold and the 0.7 preload floor.
            """
        )
        #expect(
            promoted.confidence == Self.suppressedDetectionConfidence,
            """
            Direction: the DETECTION half must be untouched by this bead. If \
            this reddens alongside the actuation expectation, the two \
            quantities have been collapsed again rather than both preserved.
            """
        )
    }

    /// The tap is still worth something: it is a MISS calibration signal and a
    /// durable `.falseNegative` correction, both of which must survive the
    /// extent gate. Asserted in conjunction with "no cue fired" so the test
    /// cannot pass by simply skipping as before.
    @Test("Field case: feedback still lands even though no skip fires")
    func unanchoredConfirmationStillRecordsFeedback() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        let controllerStore = try makeTestControllerStore(prefix: "ynmk-controller")
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        let suggested = Self.makeSuggestion(id: "ynmk-feedback")
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        #expect(
            await orchestrator.acceptSuggestedSkip(windowId: "ynmk-feedback"),
            "the gesture is accepted as FEEDBACK even when no extent is skippable"
        )

        #expect(pushedCues.isEmpty, "still no skip on an unanchored extent")

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: Self.podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1)
        #expect(
            state.integral == -1,
            "a confirmed suggestion is still a MISS signal (integral −1)"
        )
        await controllerStore.close()
    }

    // MARK: - 2. playhead-qs0d preservation — anchored confirmations DO skip

    /// The contrast case from the same episode: day-0 rediff produced two
    /// byte-exact windows, both OUTER edges, 2 of 2 correct. A confirmation on
    /// one of those has evidence for both edges and must skip — clamped to the
    /// derived late-safe bounds, which requires the promoted window to inherit
    /// the suggestion's anchors (before this bead the fresh promotion UUID
    /// never got any anchors stamped at all).
    @Test("qs0d: a both-edges byte-exact confirmation still skips, at late-safe bounds")
    func anchoredConfirmationStillSkips() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        let suggested = Self.makeSuggestion(
            id: "ynmk-byte-exact",
            start: 5462.6,
            end: 5522.7,
            confidence: 0.42,
            startAnchor: .rediffByteExact,
            endAnchor: .rediffByteExact
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        await orchestrator.acceptSuggestedSkip(windowId: "ynmk-byte-exact")

        #expect(
            pushedCues.count == 1,
            "an anchored confirmation must still skip — this is the 2-of-2 population"
        )
        let cue = try #require(pushedCues.first)
        // 5462.6 + 0.50 start margin; 5522.7 − 0.75 end margin − 1.0 pod cushion.
        #expect(abs(Self.cueStart(cue) - 5463.1) < Self.cueTickTolerance)
        #expect(abs(Self.cueEnd(cue) - 5520.95) < Self.cueTickTolerance)

        let promoted = try #require(
            await Self.promotedRow(in: store, originalId: "ynmk-byte-exact")
        )
        #expect(
            promoted.wasSkipped,
            "a real skip fired, so the row may say so"
        )
        #expect(
            promoted.decisionState == AdDecisionState.applied.rawValue
        )
        #expect(
            promoted.confidence == 0.42,
            "even when the skip fires, the tap must not overwrite the measurement"
        )
    }

    /// Per-EDGE, not per-window (feedback_outer_edges_free_inner_precious):
    /// an anchored START with an unanchored END still skips, but only up to the
    /// late-safe inset — the unanchored inner edge never defines the boundary.
    @Test("Per-edge: anchored start + unanchored end clamps to the late-safe inset")
    func mixedAnchorConfirmationClampsTheUnanchoredEdge() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        let suggested = Self.makeSuggestion(
            id: "ynmk-mixed-anchor",
            start: 100,
            end: 190,
            confidence: 0.41,
            startAnchor: .stingerSnapped,
            endAnchor: .unanchored
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        await orchestrator.acceptSuggestedSkip(windowId: "ynmk-mixed-anchor")

        #expect(pushedCues.count == 1)
        let cue = try #require(pushedCues.first)
        // 100 + 0.75 stinger start margin; 190 − 10.25 unanchored end margin
        // − 1.0 pod cushion.
        #expect(Self.cueStart(cue) == 100.75)
        #expect(abs(Self.cueEnd(cue) - 178.75) < Self.cueTickTolerance)
    }

    /// The extent resolution is SHOW-SCOPED, like the auto path's. On a show in
    /// `stingerStartDemotedShowKeys` a stinger-snapped start is not trusted, so
    /// a confirmation over one has no late-safe window either.
    ///
    /// Asserts on the persisted row as well as the cue, deliberately: dropping
    /// the show key from the resolution would flip `wasSkipped` to true while
    /// `paddedCueSpan` (which keeps its own show key) still suppressed the cue —
    /// a row claiming a skip that never happened, invisible to a cue-only test.
    ///
    /// No `assertAutoMode` here: this show has no seeded trust profile, so the
    /// mode is `.shadow`. That is fine and is the point — a banner Yes skips
    /// even in shadow mode (`applyManualSkip` does not consult the mode), so the
    /// cue assertion is not vacuous.
    @Test("Show-scoped start demotion reaches the confirmation path too")
    func demotedShowStartAnchorBlocksConfirmationSkip() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: "the-nikki-glaser-podcast"
        )

        let suggested = Self.makeSuggestion(
            id: "ynmk-demoted-show",
            start: 100,
            end: 190,
            confidence: 0.9,
            startAnchor: .stingerSnapped,
            endAnchor: .stingerSnapped
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        await orchestrator.acceptSuggestedSkip(windowId: "ynmk-demoted-show")

        #expect(
            pushedCues.isEmpty,
            "a demoted stinger start is not an anchor — nothing late-safe to skip"
        )
        let promoted = try #require(
            await Self.promotedRow(in: store, originalId: "ynmk-demoted-show")
        )
        #expect(promoted.wasSkipped == false)
        #expect(promoted.decisionState == AdDecisionState.confirmed.rawValue)
    }

    // MARK: - 3. Overshoot guards — the fidelity ladder

    /// The second door. `acceptSuggestedSkip` declines to apply an unanchored
    /// confirmation, but the promoted row is still a `.confirmed` managed window
    /// — so a manual "Skip Ad" tap aimed at it must be refused too, or the row
    /// flips to `applied`/`wasSkipped` for a cue `paddedCueSpan` will drop.
    @Test("Second door: a manual tap cannot apply an unanchored confirmation")
    func manualTapCannotApplyAnUnanchoredConfirmation() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        // Confidence above `stayThreshold` so the promoted window settles at
        // `.confirmed` rather than being suppressed on confidence — otherwise
        // `applyManualSkip`'s own `.confirmed` guard would refuse it for an
        // unrelated reason and this test would prove nothing.
        let suggested = Self.makeSuggestion(
            id: "ynmk-second-door",
            confidence: 0.9
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])
        await orchestrator.acceptSuggestedSkip(windowId: "ynmk-second-door")

        let promotedId = try #require(
            (await orchestrator.activeWindowIDs()).first,
            "the confirmation must still register a managed window to mark it"
        )
        #expect(
            await orchestrator.confirmedWindows()
                .contains { $0.id == promotedId },
            "precondition: the promoted window is manually skippable-looking"
        )

        await orchestrator.applyManualSkip(windowId: promotedId)

        #expect(pushedCues.isEmpty, "still nothing late-safe to skip")
        #expect(
            await orchestrator.confirmedWindows()
                .contains { $0.id == promotedId },
            "the manual tap must not promote it to applied"
        )
    }

    /// playhead-527u / feedback_manual_marks_override: a span the user DREW in
    /// the transcript asserts presence AND extent, and keeps its exact bounds.
    /// The exemption removed for confirmations must not be removed for marks.
    @Test("Fidelity ladder: a user-MARKED span keeps its exact user-drawn extent")
    func userMarkedSpansKeepTheirExactExtent() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)
        // Master switch ON so the policy would bite if the mark were governed.
        await orchestrator.setEdgePaddingEnabled(true)

        let marked = AdWindow(
            id: "ynmk-user-marked",
            analysisAssetId: Self.assetId,
            startTime: 300,
            endTime: 360,
            confidence: 0.9,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 300,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
        await orchestrator.receiveAdWindows([marked])

        #expect(pushedCues.count == 1, "a user mark is still auto-skippable")
        let cue = try #require(pushedCues.first)
        #expect(Self.cueStart(cue) == 300, "no start inset on a user-drawn edge")
        #expect(Self.cueEnd(cue) == 359, "exact end minus only the 1.0 s pod cushion")
    }

    // MARK: - 4. Cross-launch

    /// Dan's install already holds three rows written by the old build:
    /// `applied / wasSkipped 1 / confidence 1.00`, both edges unanchored. On the
    /// next launch the preload forwards `.applied` rows straight back into the
    /// cue path, so without a re-check those exact three spans skip 210 s of
    /// show AGAIN, on every replay, forever.
    @Test("Cross-launch: a legacy unanchored userConfirmedSuggested row never re-cues")
    func legacyUnanchoredConfirmationDoesNotRecueOnRelaunch() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        let legacy = AdWindow(
            id: "ynmk-legacy-row",
            analysisAssetId: Self.assetId,
            startTime: Self.fieldStart,
            endTime: Self.fieldEnd,
            confidence: 1.0,
            boundaryState: "userConfirmedSuggested",
            decisionState: AdDecisionState.applied.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: Self.fieldStart,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false
        )
        try await store.insertAdWindow(legacy)

        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        #expect(
            pushedCues.isEmpty,
            """
            A row already on disk claiming a user-confirmed skip over an \
            unanchored extent must be re-checked, not replayed. Got cues: \
            \(pushedCues.map { (Self.cueStart($0), Self.cueEnd($0)) })
            """
        )
    }

    /// The mirror image, and the reason the preload's 0.7 confidence floor has
    /// to learn about user assertion: once the tap stops synthesising 1.0, an
    /// ANCHORED confirmed row sits at its measured 0.42 and would silently drop
    /// out of the preload — cross-launch continuity for the one population that
    /// should have it.
    @Test("Cross-launch: an anchored user-confirmed row re-cues below the preload floor")
    func anchoredConfirmationRecuesAcrossLaunchAtMeasuredConfidence() async throws {
        let (orchestrator, store) = try await Self.makeHarness()
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        let confirmed = AdWindow(
            id: "ynmk-anchored-relaunch",
            analysisAssetId: Self.assetId,
            startTime: 5462.6,
            endTime: 5522.7,
            confidence: 0.42,
            boundaryState: "userConfirmedSuggested",
            decisionState: AdDecisionState.applied.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 5462.6,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false,
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
        )
        try await store.insertAdWindow(confirmed)

        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await Self.assertAutoMode(orchestrator)

        #expect(
            pushedCues.count == 1,
            "the anchored confirmation must survive relaunch at its MEASURED confidence"
        )
        let cue = try #require(pushedCues.first)
        #expect(abs(Self.cueStart(cue) - 5463.1) < Self.cueTickTolerance)
        #expect(abs(Self.cueEnd(cue) - 5520.95) < Self.cueTickTolerance)
    }
}
