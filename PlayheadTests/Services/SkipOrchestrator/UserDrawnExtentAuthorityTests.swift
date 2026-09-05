import CoreMedia
import Foundation
import Testing
@testable import Playhead

/// playhead-k683 — a span the LISTENER drew keeps its extent authority when
/// it is offered as, and confirmed from, a suggest card.
///
/// `UserSpanAssertion` separates two claims (playhead-ynmk): `.userMarked`
/// asserts presence AND extent — the listener drew the edges — while
/// `.userConfirmedSuggested` asserts presence only, because the card asked
/// "is this an ad?" and the DETECTOR drew the edges. Two places never asked
/// which one they had: the card's own skip claim, and the promoted row's
/// stamp. So after playhead-d2it routes a live mark to the suggest tier on a
/// non-auto show, the listener was shown a card about their OWN mark that
/// said it could not skip, and a Yes recorded a mark rather than moving
/// playback. They drew the edges; the fidelity ladder says those edges win.
@Suite("playhead-k683: a user-drawn span keeps its extent through the suggest tier")
struct UserDrawnExtentAuthorityTests {

    private static let assetId = "asset-k683"
    private static let episodeId = "ep-k683"
    private static let podcastId = "podcast-k683"
    private static let sentinelStart: Double = 1000

    /// Bounds a drain of the banner stream: a markOnly detector suggestion far
    /// from everything under test, entered last, so its `.present` is the
    /// frame boundary.
    private static func sentinel(id: String) -> AdWindow {
        AdWindow(
            id: id, analysisAssetId: assetId,
            startTime: sentinelStart, endTime: sentinelStart + 4,
            confidence: 0.41,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: "brought to you by", evidenceStartTime: sentinelStart,
            metadataSource: "none", metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false, evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    /// A detector-drawn, both-edges-unanchored suggestion: the playhead-ynmk
    /// field case, and this suite's control.
    private static func detectorSuggestion(id: String, start: Double, end: Double) -> AdWindow {
        AdWindow(
            id: id, analysisAssetId: assetId,
            startTime: start, endTime: end,
            confidence: 0.40,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: "brought to you by", evidenceStartTime: start,
            metadataSource: "none", metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false, evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    private struct FrameReader {
        private var iterator: AsyncStream<AdBannerStreamEvent>.AsyncIterator
        init(_ stream: AsyncStream<AdBannerStreamEvent>) { iterator = stream.makeAsyncIterator() }
        mutating func presented(until sentinel: String) async -> [AdSkipBannerItem] {
            var items: [AdSkipBannerItem] = []
            for _ in 0..<64 {
                guard let event = await iterator.next() else { return items }
                if case let .present(item) = event {
                    if item.windowId == sentinel { return items }
                    items.append(item)
                }
            }
            Issue.record("the sentinel '\(sentinel)' never arrived within 64 events")
            return items
        }
    }

    private static func makeManualShow() async throws -> (SkipOrchestrator, AnalysisStore) {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId))
        let trust = try await makeSkipTestTrustService(mode: "manual", trustScore: 0.5, observations: 0)
        let orchestrator = SkipOrchestrator(
            store: store, trustService: trust,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: assetId, episodeId: episodeId, podcastId: podcastId)
        return (orchestrator, store)
    }

    private static func promotedRow(in store: AnalysisStore, originalId: String) async throws -> AdWindow? {
        try await store.fetchAdWindows(assetId: assetId).first { $0.id != originalId && $0.id != "sentinel" }
    }

    @Test("the card for a user-DRAWN span offers a Skip it can perform", .timeLimit(.minutes(1)))
    func drawnSpansCardClaimsTheSkip() async throws {
        let (orchestrator, _) = try await Self.makeManualShow()
        var reader = FrameReader(await orchestrator.bannerEventStream())

        // playhead-d2it: on a manual show the live mark lands in the suggest tier.
        await orchestrator.injectUserMarkedAd(start: 100, end: 160, analysisAssetId: Self.assetId, windowId: "drawn")
        try #require(await orchestrator.activeSuggestWindowIDs().contains("drawn"), "setup: the mark must reach the suggest tier")

        await orchestrator.updatePlayheadTime(101)
        await orchestrator.receiveAdWindows([Self.sentinel(id: "sentinel")])
        await orchestrator.updatePlayheadTime(Self.sentinelStart + 0.5)
        let card = try #require(
            (await reader.presented(until: "sentinel")).first { $0.windowId == "drawn" },
            "entering the drawn span must present its card"
        )

        #expect(
            card.confirmationSkipsPlayback,
            """
            the listener drew these edges; a card that says it cannot skip \
            them tells the listener their own edges are not good enough
            """
        )
    }

    @Test("confirming a user-DRAWN span promotes it as applied and still user-marked", .timeLimit(.minutes(1)))
    func confirmingDrawnSpanKeepsItsAssertion() async throws {
        let (orchestrator, store) = try await Self.makeManualShow()
        await orchestrator.injectUserMarkedAd(start: 100, end: 160, analysisAssetId: Self.assetId, windowId: "drawn")
        try #require(await orchestrator.activeSuggestWindowIDs().contains("drawn"))
        // The suggest tier's durable row, so the promotion has something to retire.
        try await store.insertAdWindow(
            AdWindow(
                id: "drawn", analysisAssetId: Self.assetId, startTime: 100, endTime: 160,
                confidence: 1.0, boundaryState: UserSpanAssertion.userMarked.rawValue,
                decisionState: AdDecisionState.confirmed.rawValue, detectorVersion: "userCorrection",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: 100,
                metadataSource: "userCorrection", metadataConfidence: nil, metadataPromptVersion: nil,
                wasSkipped: false, userDismissedBanner: false,
                eligibilityGate: SkipEligibilityGate.eligible.rawValue
            )
        )

        #expect(await orchestrator.acceptSuggestedSkip(windowId: "drawn"))

        let promoted = try #require(await Self.promotedRow(in: store, originalId: "drawn"))
        #expect(
            promoted.boundaryState == UserSpanAssertion.userMarked.rawValue,
            "the promoted row must not DOWNGRADE what the listener asserted; got \(promoted.boundaryState)"
        )
        #expect(
            promoted.decisionState == AdDecisionState.applied.rawValue,
            "the listener's own edges are skippable, so a Yes moves playback"
        )
    }

    @Test("the control: a detector-drawn suggestion still asserts presence only", .timeLimit(.minutes(1)))
    func detectorDrawnSuggestionIsUnchanged() async throws {
        let (orchestrator, store) = try await Self.makeManualShow()
        var reader = FrameReader(await orchestrator.bannerEventStream())
        let suggestion = Self.detectorSuggestion(id: "detector", start: 300, end: 450)
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])
        try #require(await orchestrator.activeSuggestWindowIDs().contains("detector"))

        await orchestrator.updatePlayheadTime(301)
        await orchestrator.receiveAdWindows([Self.sentinel(id: "sentinel")])
        await orchestrator.updatePlayheadTime(Self.sentinelStart + 0.5)
        let card = try #require((await reader.presented(until: "sentinel")).first { $0.windowId == "detector" })
        #expect(!card.confirmationSkipsPlayback, "playhead-ynmk: unanchored detector edges are not a skip")

        #expect(await orchestrator.acceptSuggestedSkip(windowId: "detector"))
        let promoted = try #require(await Self.promotedRow(in: store, originalId: "detector"))
        #expect(promoted.boundaryState == UserSpanAssertion.userConfirmedSuggested.rawValue)
        #expect(promoted.decisionState == AdDecisionState.confirmed.rawValue)
    }
}
