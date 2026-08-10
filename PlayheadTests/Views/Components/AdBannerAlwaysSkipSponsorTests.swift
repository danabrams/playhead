// AdBannerAlwaysSkipSponsorTests.swift
// Behavioral tests for the "Always skip this sponsor" affordance added
// to the auto-skipped banner in playhead-3bv.4.
//
// We exercise the wiring contracts rather than the SwiftUI rendering —
// the repo idiom is to test handler call shapes and visibility
// predicates (see `AdBannerQueueTests` and `AdBannerCopyTests`). The
// inline confirmation state machine is the View's internal `@State`
// and is observable through the host-supplied handler firing exactly
// once with the expected payload.

import XCTest
@testable import Playhead

@MainActor
final class AdBannerAlwaysSkipSponsorTests: XCTestCase {

    // MARK: - Helpers

    private func makeItem(
        id: String = UUID().uuidString,
        windowId: String = "w-1",
        advertiser: String? = "Squarespace",
        product: String? = nil,
        adStartTime: Double = 100.0,
        adEndTime: Double = 130.0,
        metadataConfidence: Double? = 0.85,
        metadataSource: String = "foundationModels",
        podcastId: String = "podcast-test",
        tier: AdBannerTier = .autoSkipped
    ) -> AdSkipBannerItem {
        AdSkipBannerItem(
            id: id,
            windowId: windowId,
            advertiser: advertiser,
            product: product,
            adStartTime: adStartTime,
            adEndTime: adEndTime,
            metadataConfidence: metadataConfidence,
            metadataSource: metadataSource,
            podcastId: podcastId,
            evidenceCatalogEntries: [],
            tier: tier
        )
    }

    // MARK: - Handler nil → button hidden

    /// When the host does not wire `onAlwaysSkipSponsor`, the view's
    /// stored callback is nil — the view's render path is responsible
    /// for hiding the button in that case (the conditional is unit-
    /// tested below by exercising the call shape; this case just
    /// pins the absence).
    func testHandlerNilWhenNotProvided() {
        let queue = AdBannerQueue()
        let view = AdBannerView(queue: queue)
        XCTAssertNil(
            view.onAlwaysSkipSponsor,
            "onAlwaysSkipSponsor should be nil when host does not wire it"
        )
    }

    /// When wired, the handler is callable with the banner item
    /// payload. The same closure shape `NowPlayingView` passes in.
    func testHandlerInvokesWithItemPayload() {
        let queue = AdBannerQueue()
        var captured: AdSkipBannerItem?
        let view = AdBannerView(
            queue: queue,
            onAlwaysSkipSponsor: { item in captured = item }
        )
        let item = makeItem(id: "veto-1", advertiser: "BetterHelp")
        view.onAlwaysSkipSponsor?(item)
        XCTAssertEqual(captured?.id, "veto-1")
        XCTAssertEqual(captured?.advertiser, "BetterHelp")
    }

    // MARK: - Sponsor scope normalization contract

    /// The downstream `SponsorKnowledgeStore.activeEntriesWithNegativeMemory`
    /// lookup compares `CorrectionScope.sponsorOnShow` strings by the
    /// `sponsor` field — and the knowledge store's `normalizedValue` is
    /// `lowercased() + trimmingCharacters(in: .whitespaces)`. The
    /// `NowPlayingView` wiring must apply the same normalization so the
    /// scope a user records here actually negates the same sponsor
    /// entry the next time the knowledge store is consulted. Note the
    /// character set is `.whitespaces`, NOT `.whitespacesAndNewlines` —
    /// a drift would cause sponsor names carrying an embedded newline
    /// to round-trip to different normalized values on either side.
    ///
    /// We pin the canonical scope serialization shape directly: if the
    /// downstream contract drifts (or our wiring stops normalizing)
    /// this test fails.
    func testSponsorScopeSerializesLowercasedTrimmedSponsor() {
        let advertiser = "  Squarespace  "
        let normalized = advertiser
            .lowercased()
            .trimmingCharacters(in: .whitespaces)
        XCTAssertEqual(normalized, "squarespace")

        let scope = CorrectionScope.sponsorOnShow(
            podcastId: "podcast-42",
            sponsor: normalized
        )
        XCTAssertEqual(
            scope.serialized,
            "sponsorOnShow:podcast-42:squarespace",
            "Sponsor scope must be lowercased + trimmed so SponsorKnowledgeStore's negative-memory pass finds the match"
        )
    }

    /// Round-trip check: the banner-side normalization must produce the
    /// same string that `SponsorKnowledgeStore` stamps onto its
    /// `normalizedValue` field for the same input. Constructing a
    /// `SponsorKnowledgeEntry` with `normalizedValue: nil` exercises the
    /// store's own default derivation (`entityValue.lowercased()
    /// .trimmingCharacters(in: .whitespaces)`); comparing both sides
    /// catches any drift if either changes its character set.
    func testSponsorScopeNormalizationMatchesKnowledgeStoreContract() {
        let raw = "  Squarespace  "
        let bannerSide = raw
            .lowercased()
            .trimmingCharacters(in: .whitespaces)
        let entry = SponsorKnowledgeEntry(
            podcastId: "podcast-x",
            entityType: .sponsor,
            entityValue: raw,
            normalizedValue: nil,  // forces the store's default derivation
            state: .active,
            confirmationCount: 1,
            rollbackCount: 0,
            firstSeenAt: 0,
            lastConfirmedAt: 0,
            lastRollbackAt: nil
        )
        XCTAssertEqual(
            bannerSide,
            entry.normalizedValue,
            "Banner-side normalization must equal SponsorKnowledgeStore's normalizedValue derivation"
        )
    }

    /// `CorrectionScope.sponsorOnShow` round-trips through serialize /
    /// deserialize, so the downstream reader sees the same value the
    /// banner produced. Regression guard against future scope-encoding
    /// changes that might silently break the always-skip path.
    func testSponsorScopeRoundTripsThroughSerialization() throws {
        let scope = CorrectionScope.sponsorOnShow(
            podcastId: "podcast-99",
            sponsor: "hellofresh"
        )
        let serialized = scope.serialized
        let decoded = try XCTUnwrap(CorrectionScope.deserialize(serialized))
        guard case .sponsorOnShow(let podcastId, let sponsor) = decoded else {
            XCTFail("Expected sponsorOnShow case")
            return
        }
        XCTAssertEqual(podcastId, "podcast-99")
        XCTAssertEqual(sponsor, "hellofresh")
    }

    // MARK: - CorrectionEvent shape

    /// The event the NowPlayingView wiring builds carries:
    ///   * `analysisAssetId` matching the currently-playing episode
    ///   * the `sponsorOnShow` scope as serialized
    ///   * `source = .falseNegative` and
    ///     `correctionType = .falseNegative` — the REINFORCEMENT
    ///     direction. playhead-q6y3, Dan's ruling 2026-08-09: the tap
    ///     means "yes, this IS an ad, always skip it". This site
    ///     shipped `.manualVeto` / `.falsePositive`, which in this
    ///     codebase is the SUPPRESS direction, so one tap taught the
    ///     pipeline that the advertiser is not an ad across the whole
    ///     show — and suppressing the detection removes the skip.
    ///   * `targetRefs.sponsorEntity` set to the normalized sponsor
    ///      so the LearningArtifactIngestor's sponsor side-effects
    ///      (knowledge-store CONFIRMATION, in this direction) fire
    ///      correctly
    ///
    /// The `source` is the load-bearing half: every live read-side
    /// consumer keys on `event.source?.kind` and reads
    /// `correctionType` only as the legacy fallback for `source == nil`
    /// rows. `CorrectionKind` is asserted here rather than inferred so
    /// that a future rename of the source cannot quietly re-invert the
    /// direction while the field names still read correctly.
    ///
    /// This test reconstructs the event the same way the wiring does.
    /// `testWiringPersistsTheReinforcementDirection` below is what
    /// pins the WIRING to this shape — reconstruction alone would be
    /// vacuous.
    func testCorrectionEventCarriesSponsorTargetRef() {
        let normalized = "squarespace"
        let event = CorrectionEvent(
            analysisAssetId: "asset-abc",
            scope: CorrectionScope.sponsorOnShow(
                podcastId: "podcast-xyz",
                sponsor: normalized
            ).serialized,
            source: .falseNegative,
            podcastId: "podcast-xyz",
            correctionType: .falseNegative,
            targetRefs: CorrectionTargetRefs(sponsorEntity: normalized)
        )
        XCTAssertEqual(event.analysisAssetId, "asset-abc")
        XCTAssertEqual(event.podcastId, "podcast-xyz")
        XCTAssertEqual(event.source, .falseNegative)
        XCTAssertEqual(event.correctionType, .falseNegative)
        XCTAssertEqual(
            event.source?.kind,
            .falseNegative,
            "the direction the read side actually resolves must be BOOST"
        )
        XCTAssertEqual(
            event.effectiveCorrectionType,
            .falseNegative,
            "and the persisted-identity view of it must agree"
        )
        XCTAssertEqual(event.targetRefs?.sponsorEntity, normalized)
        XCTAssertEqual(
            event.scope,
            "sponsorOnShow:podcast-xyz:squarespace"
        )
    }

    /// playhead-q6y3: the WIRING, not a reconstruction of it.
    ///
    /// `NowPlayingView.onAlwaysSkipSponsorAsync` builds its own
    /// `CorrectionEvent` rather than routing through the orchestrator,
    /// so nothing behavioural short of driving SwiftUI observes which
    /// direction it writes. The neighbouring
    /// `CorrectionAttributionCaptureTests` pins the same closure's
    /// capture discipline the same way and explains why a source canary
    /// is the right instrument for a claim about code shape.
    ///
    /// The anti-vacuity control is the `XCTAssertTrue` pair: a canary
    /// built only from "does not contain" passes just as happily when
    /// the inspector returns an empty body because the anchor drifted.
    /// Asserting that the tokens which SHOULD be there ARE there is
    /// what proves the body was really read.
    func testWiringPersistsTheReinforcementDirection() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Views/NowPlaying/NowPlayingView.swift"
        )
        let body = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: source,
                after: "onAlwaysSkipSponsorAsync:"
            ),
            "Could not locate the onAlwaysSkipSponsorAsync closure"
        )
        let stripped = SwiftSourceInspector.strippingComments(body)

        XCTAssertTrue(
            stripped.contains("CorrectionScope.sponsorOnShow"),
            "anti-vacuity: the closure body must really have been read"
        )
        XCTAssertTrue(
            stripped.contains("source: .falseNegative"),
            """
            The tap means "yes, this IS an ad". `source` is the field every \
            live read-side consumer resolves the direction from, so it is the \
            one that has to say BOOST.
            """
        )
        XCTAssertTrue(
            stripped.contains("correctionType: .falseNegative"),
            "and the persisted type must agree, or a nil-source reader disagrees"
        )
        XCTAssertFalse(
            stripped.contains(".falsePositive"),
            """
            playhead-q6y3: a false positive means "the detector was wrong, this \
            was not an ad" — it SUPPRESSES this sponsor across every episode of \
            the show, which removes the very skip the button promises.
            """
        )
        XCTAssertFalse(
            stripped.contains(".manualVeto"),
            "and `.manualVeto` resolves to the same suppress direction via CorrectionSource.kind"
        )
    }

    // MARK: - CTA copy (playhead-q6y3)

    /// Dan asked for a clearer CTA. "Always skip this sponsor" named
    /// neither the sponsor nor the population: "this sponsor" reads as
    /// a pointer at the card in front of you — the same span the banner
    /// is already about — when the tap is a standing instruction about
    /// an advertiser across every episode of the show.
    func testCtaNamesTheSponsorAndThePopulation() {
        let label = AdBannerView.alwaysSkipSponsorLabel(for: "Squarespace")
        XCTAssertEqual(label, "Always skip Squarespace on this show")

        // ANTI-VACUITY: the advertiser is interpolated, not decoration on
        // a constant string. Two advertisers must produce two labels.
        XCTAssertNotEqual(
            label,
            AdBannerView.alwaysSkipSponsorLabel(for: "BetterHelp")
        )
        XCTAssertTrue(
            AdBannerView.alwaysSkipSponsorLabel(for: "BetterHelp")
                .contains("BetterHelp")
        )
    }

    /// The hint carries the full promise the four-word button cannot:
    /// durable, and over every episode. Present tense, no counters —
    /// this is what the app will do, not a tally of what it has done.
    func testHintSpellsOutDurabilityAndPopulation() {
        let hint = AdBannerView.alwaysSkipSponsorHint(for: "Squarespace")
        XCTAssertEqual(
            hint,
            "Skips Squarespace in every episode of this show from now on"
        )
    }

    /// The receipt is the CTA's own words in the present continuous, so
    /// the listener can match receipt to tap without re-reading, and it
    /// states the standing behaviour rather than promising a future
    /// nobody can verify.
    func testReceiptMirrorsTheCta() {
        XCTAssertEqual(
            AdBannerView.alwaysSkipSponsorReceipt(for: "Squarespace"),
            "Always skipping Squarespace on this show"
        )
    }

    /// External-copy rules: no "ad detection", no "AI", no metrics. And
    /// nothing that reads as acting on the CURRENT span only — the
    /// banner is a skip affordance that fires on playhead ENTRY, so
    /// copy implying a retroactive edit of this one segment would be a
    /// lie about what the tap does.
    func testCopyObeysTheExternalCopyRules() {
        let strings = [
            AdBannerView.alwaysSkipSponsorLabel(for: "Squarespace"),
            AdBannerView.alwaysSkipSponsorHint(for: "Squarespace"),
            AdBannerView.alwaysSkipSponsorReceipt(for: "Squarespace"),
        ]
        let banned = [
            "ad detection", "detection", "detect", "AI", "algorithm",
            "confidence", "accuracy", "this ad", "this segment",
            "this one", "%",
        ]
        for string in strings {
            let lowered = string.lowercased()
            for word in banned {
                XCTAssertFalse(
                    lowered.contains(word.lowercased()),
                    "\"\(string)\" must not contain \"\(word)\""
                )
            }
            // ANTI-VACUITY for the loop above: an empty or whitespace
            // string would satisfy every "does not contain" assertion.
            XCTAssertTrue(
                lowered.contains("this show"),
                "\"\(string)\" must name the population it acts over"
            )
            XCTAssertTrue(lowered.contains("squarespace"))
        }
    }

    /// A whitespace/newline-only advertiser never reaches the copy in
    /// production — `normalizedAlwaysSkipSponsor` hides the button — but
    /// the helpers are `static` and reachable, so they must not render a
    /// sentence with a hole in it.
    func testDisplayNameFallsBackRatherThanRenderingAHole() {
        XCTAssertEqual(
            AdBannerView.alwaysSkipSponsorLabel(for: "  \n "),
            "Always skip this sponsor on this show"
        )
        XCTAssertEqual(
            AdBannerView.alwaysSkipSponsorDisplayName("  Squarespace \n"),
            "Squarespace",
            "outer whitespace is trimmed but the listener's casing is kept"
        )
        XCTAssertNotEqual(
            AdBannerView.alwaysSkipSponsorDisplayName("Squarespace"),
            AdBannerView.normalizedAlwaysSkipSponsor("Squarespace"),
            "the display name and the storage key are deliberately different things"
        )
    }

    // MARK: - Confirmation dwell

    /// The View uses `alwaysSkipConfirmationSeconds` to drive the
    /// auto-dismiss after the inline "Will always skip this sponsor"
    /// receipt. Pin the value so future tweaks are deliberate (the UI
    /// design wants the receipt to read as a calm receipt, not a
    /// modal).
    func testConfirmationDwellIsShortEnoughToReadAsReceipt() {
        XCTAssertEqual(
            AdBannerView.alwaysSkipConfirmationSeconds,
            2.0,
            "Confirmation dwell should be ~2s — long enough to read, short enough to never feel modal"
        )
    }

    // MARK: - Queue-advance race regression

    /// The auto-dismiss task spawned by tapping "Always skip this
    /// sponsor" sleeps for ~2s before calling `queue.dismiss()`. If a
    /// second banner is enqueued (and the queue advances) during the
    /// sleep, dismissing then would drop the NEW banner — silently
    /// stealing a skip notification from the user.
    ///
    /// The fix lives in two places:
    ///   1. `.onChange(of: queue.currentBanner?.id)` resets
    ///      `confirmedAlwaysSkipBannerId` to nil on queue advance, so
    ///      the View's @State guard `confirmedAlwaysSkipBannerId == item.id`
    ///      fails for the stale task.
    ///   2. The delayed task itself ALSO checks
    ///      `queue.currentBanner?.id == item.id` as defense-in-depth.
    ///
    /// Both layers depend on the queue's `currentBanner` reflecting
    /// reality after `dismiss()` advances to the next entry. This test
    /// pins THAT contract: after dismissing banner A, the queue's
    /// current banner is B, so the stale guard (which captured A's id)
    /// can never match `queue.currentBanner?.id` on the live queue.
    func testQueueAdvanceMakesStaleBannerIdNotMatchCurrent() async {
        let queue = AdBannerQueue()
        let bannerA = makeItem(id: "banner-A", advertiser: "Squarespace")
        let bannerB = makeItem(
            id: "banner-B",
            windowId: "w-2",
            advertiser: "BetterHelp"
        )

        queue.enqueue(bannerA)
        XCTAssertEqual(queue.currentBanner?.id, "banner-A")

        // Simulate: a second banner arrives during the confirmation dwell.
        queue.enqueue(bannerB)

        // Simulate the queue advancing (banner A finishes / is dismissed
        // by some other path — e.g. its natural lifetime expiring or
        // the user undoing). `dismiss()` clears `currentBanner` to nil
        // immediately, then schedules `showNext()` via a 350ms async
        // sleep.
        //
        // playhead-dd7d: previously this waited a FIXED 500ms and then
        // asserted — a wall-clock gamble that lost under the saturated
        // parallel plan, where the internal 350ms `Task.sleep` dilates past
        // the test's budget so `showNext()` hadn't run yet and
        // `currentBanner` was still nil. Await the advance's ACTUAL
        // completion signal instead: `advanceTaskForTesting()?.value`
        // returns exactly when `showNext()` has advanced the queue,
        // however long the (possibly starved) sleep actually takes.
        queue.dismiss()
        await queue.advanceTaskForTesting()?.value
        XCTAssertEqual(
            queue.currentBanner?.id,
            "banner-B",
            "Queue should advance to banner B after A is dismissed"
        )

        // Now if the stale delayed task fires with its captured
        // `item.id == "banner-A"`, the defense-in-depth check
        // `queue.currentBanner?.id == item.id` is false → it must NOT
        // call `queue.dismiss()`. We pin that asymmetry directly.
        let staleCapturedId = "banner-A"
        XCTAssertNotEqual(
            queue.currentBanner?.id,
            staleCapturedId,
            "Stale delayed-task captured id must not equal the current banner's id after queue advance — the guard relies on this to avoid dismissing the wrong banner"
        )
    }
}
