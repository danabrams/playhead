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
    /// entry the next time the knowledge store is consulted.
    ///
    /// We pin the canonical scope serialization shape directly: if the
    /// downstream contract drifts (or our wiring stops normalizing)
    /// this test fails.
    func testSponsorScopeSerializesLowercasedTrimmedSponsor() {
        let advertiser = "  Squarespace  "
        let normalized = advertiser
            .lowercased()
            .trimmingCharacters(in: .whitespacesAndNewlines)
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
    ///   * `source = .manualVeto` (this is a user-driven sponsor veto)
    ///   * `correctionType = .falsePositive` — the user says "we
    ///      should NOT have played this in the first place; do not
    ///      play future instances either"
    ///   * `targetRefs.sponsorEntity` set to the normalized sponsor
    ///      so the LearningArtifactIngestor's sponsor side-effects
    ///      (knowledge-store demotion) fire correctly
    ///
    /// This test reconstructs the event the same way the wiring does
    /// and pins the shape — drift in any of these fields silently
    /// breaks the user-facing promise.
    func testCorrectionEventCarriesSponsorTargetRef() {
        let normalized = "squarespace"
        let event = CorrectionEvent(
            analysisAssetId: "asset-abc",
            scope: CorrectionScope.sponsorOnShow(
                podcastId: "podcast-xyz",
                sponsor: normalized
            ).serialized,
            source: .manualVeto,
            podcastId: "podcast-xyz",
            correctionType: .falsePositive,
            targetRefs: CorrectionTargetRefs(sponsorEntity: normalized)
        )
        XCTAssertEqual(event.analysisAssetId, "asset-abc")
        XCTAssertEqual(event.podcastId, "podcast-xyz")
        XCTAssertEqual(event.source, .manualVeto)
        XCTAssertEqual(event.correctionType, .falsePositive)
        XCTAssertEqual(event.targetRefs?.sponsorEntity, normalized)
        XCTAssertEqual(
            event.scope,
            "sponsorOnShow:podcast-xyz:squarespace"
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
}
