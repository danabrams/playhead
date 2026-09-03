import Foundation
import Testing
@testable import Playhead

/// playhead-1mq1.2 — a repeat "this is an ad" correction, which absorbs
/// playhead-59t3 (a repeat minted a duplicate `ad_windows` row) and
/// playhead-q0tj (a repeat that reached past the marked edge did nothing the
/// listener could perceive).
///
/// The device pull of 2026-09-02 carries the 59t3 defect as data: two
/// (asset, span) pairs hold two rows each, because `recordUserMarkedAd`
/// returned a bare `Bool` and every caller injected the id it had just minted
/// rather than the id the durable write resolved to.
@Suite("Repeat user correction")
struct RepeatUserCorrectionTests {

    private func makeService(
        _ store: AnalysisStore
    ) -> AdDetectionService {
        AdDetectionService(store: store, metadataExtractor: FallbackExtractor())
    }

    private func seededStore() async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        return store
    }

    // MARK: - playhead-59t3: one ad, one row

    @Test(
        "a repeat correction inside an existing mark mints no second row",
        .timeLimit(.minutes(1))
    )
    func repeatCorrectionInsideExistingMarkMintsNoSecondRow() async throws {
        let store = try await seededStore()
        let service = makeService(store)

        let first = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            podcastId: "podcast-1",
            windowId: "first-mark"
        )
        #expect(first == .recorded(
            UserMarkIdentity(windowId: "first-mark", startTime: 60, endTime: 120)
        ))

        // The listener taps again, well inside the ad they already marked.
        let repeatCorrection = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1",
            startTime: 80,
            endTime: 100,
            podcastId: "podcast-1",
            windowId: "second-mark"
        )

        #expect(
            repeatCorrection == .alreadyMarked(
                UserMarkIdentity(
                    windowId: "first-mark", startTime: 60, endTime: 120
                )
            ),
            "a contained repeat resolves to the row that already covers it"
        )
        #expect(
            repeatCorrection.isPersisted,
            "the listener's ad IS marked, so the gesture succeeded"
        )
        #expect(
            !repeatCorrection.isNewEvidence,
            "repeating yourself is not a second miss by the detector"
        )

        let rows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(rows.count == 1, "one ad must leave one row, not two")
        #expect(try await store.fetchAdWindow(id: "second-mark") == nil)
        #expect(rows[0].startTime == 60 && rows[0].endTime == 120,
                "a contained repeat must never NARROW the mark")
    }

    @Test(
        "a correction on a disjoint ad still mints its own row",
        .timeLimit(.minutes(1))
    )
    func disjointCorrectionMintsItsOwnRow() async throws {
        let store = try await seededStore()
        let service = makeService(store)

        _ = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 60, endTime: 120,
            podcastId: "podcast-1", windowId: "first-mark"
        )
        let second = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 130, endTime: 180,
            podcastId: "podcast-1", windowId: "second-mark"
        )

        #expect(second.identity?.windowId == "second-mark")
        #expect(second.isNewEvidence)
        let rows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(rows.count == 2, "two ads are two rows")
    }

    @Test(
        "marks that merely touch are not merged",
        .timeLimit(.minutes(1))
    )
    func touchingMarksAreNotMerged() async throws {
        let store = try await seededStore()
        let service = makeService(store)

        _ = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 60, endTime: 120,
            podcastId: "podcast-1", windowId: "first-mark"
        )
        // Two ads in one pod, the second starting exactly where the first ends.
        let second = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 120, endTime: 180,
            podcastId: "podcast-1", windowId: "second-mark"
        )

        #expect(second.identity?.windowId == "second-mark")
        #expect(
            (try await store.fetchAdWindows(assetId: "asset-1")).count == 2,
            "the overlap comparison is strict on both sides"
        )
    }

    // MARK: - playhead-q0tj: a repeat past the edge must DO something

    @Test(
        "a repeat correction past the marked edge widens that same row",
        .timeLimit(.minutes(1))
    )
    func repeatCorrectionPastTheEdgeWidensTheSameRow() async throws {
        let store = try await seededStore()
        let service = makeService(store)

        _ = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 60, endTime: 120,
            podcastId: "podcast-1", windowId: "first-mark"
        )

        // The ad kept playing past 120, so the listener taps again. Their new
        // span reaches mostly PAST the old one — the case a "these spans are
        // nearly equal" rule would miss.
        let widened = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 110, endTime: 160,
            podcastId: "podcast-1", windowId: "second-mark"
        )

        #expect(
            widened == .extended(
                UserMarkIdentity(
                    windowId: "first-mark", startTime: 60, endTime: 160
                )
            ),
            "the listener told us the mark was too short; that is what is stored"
        )
        #expect(widened.isNewEvidence, "a widen carried new information")

        let rows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(rows.count == 1, "widening is not minting")
        #expect(rows[0].id == "first-mark")
        #expect(rows[0].startTime == 60 && rows[0].endTime == 160)
        #expect(
            rows[0].evidenceStartTime == 60,
            "evidenceStartTime must follow startTime, as it does at mint"
        )
    }

    @Test(
        "a repeat correction reaching BACKWARD widens the leading edge",
        .timeLimit(.minutes(1))
    )
    func repeatCorrectionReachingBackwardWidensLeadingEdge() async throws {
        let store = try await seededStore()
        let service = makeService(store)

        _ = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 60, endTime: 120,
            podcastId: "podcast-1", windowId: "first-mark"
        )
        let widened = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 40, endTime: 80,
            podcastId: "podcast-1", windowId: "second-mark"
        )

        #expect(widened.identity?.startTime == 40)
        #expect(widened.identity?.endTime == 120)
        let rows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(rows.count == 1)
        #expect(rows[0].startTime == 40 && rows[0].endTime == 120)
    }

    // MARK: - The correction receipt

    @Test(
        "a contained repeat appends no correction event; a widen appends one",
        .timeLimit(.minutes(1))
    )
    func correctionEventsFollowTheOutcome() async throws {
        let store = try await seededStore()
        let service = makeService(store)

        _ = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 60, endTime: 120,
            podcastId: "podcast-1", windowId: "first-mark"
        )
        let afterFirst = try await store.loadCorrectionEvents(
            analysisAssetId: "asset-1"
        ).count

        _ = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 80, endTime: 100,
            podcastId: "podcast-1", windowId: "contained"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: "asset-1"
            ).count == afterFirst,
            "a gesture that changed nothing must not inflate the receipt log"
        )

        _ = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 110, endTime: 160,
            podcastId: "podcast-1", windowId: "widening"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: "asset-1"
            ).count > afterFirst,
            "a widen is a real correction and is receipted"
        )
    }

    // MARK: - The store guard

    @Test(
        "extendUserMarkedAd refuses a row the listener did not author",
        .timeLimit(.minutes(1))
    )
    func extendRefusesADetectorRow() async throws {
        let store = try await seededStore()
        let detectorRow = AdWindow(
            id: "detector-row",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.8,
            boundaryState: "acousticRefined",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "test",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "test",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        )
        try await store.insertAdWindow(detectorRow)

        let correction = CorrectionEvent(
            analysisAssetId: "asset-1",
            scope: CorrectionScope.exactTimeSpan(
                assetId: "asset-1", startTime: 60, endTime: 200
            ).serialized,
            source: .falseNegative,
            podcastId: "podcast-1",
            correctionType: .falseNegative
        )
        let didWiden = try await store.extendUserMarkedAd(
            id: "detector-row",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 200,
            correction: correction
        )

        #expect(!didWiden, "only a row the LISTENER authored may be widened here")
        let row = try await store.fetchAdWindow(id: "detector-row")
        #expect(row?.endTime == 120, "the refused widen must not have written")
        #expect(
            (try await store.loadCorrectionEvents(
                analysisAssetId: "asset-1"
            )).isEmpty,
            "a refused widen must roll its correction append back"
        )
    }

    @Test(
        "a detector row overlapping the span does not block a new user mark",
        .timeLimit(.minutes(1))
    )
    func detectorRowDoesNotAbsorbAUserMark() async throws {
        let store = try await seededStore()
        let service = makeService(store)
        try await store.insertAdWindow(
            AdWindow(
                id: "detector-row",
                analysisAssetId: "asset-1",
                startTime: 60,
                endTime: 120,
                confidence: 0.8,
                boundaryState: "acousticRefined",
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "test",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: nil,
                metadataSource: "test",
                metadataConfidence: nil, metadataPromptVersion: nil,
                wasSkipped: false, userDismissedBanner: false
            )
        )

        let outcome = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 70, endTime: 110,
            podcastId: "podcast-1", windowId: "user-mark"
        )

        #expect(
            outcome.identity?.windowId == "user-mark",
            "the dedupe looks only at listener-authored rows"
        )
    }

    // MARK: - Cross-asset isolation

    @Test(
        "an overlapping mark on a DIFFERENT asset is not folded in",
        .timeLimit(.minutes(1))
    )
    func marksAreScopedToTheirAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2")
        )
        let service = makeService(store)

        _ = await service.recordUserMarkedAd(
            analysisAssetId: "asset-1", startTime: 60, endTime: 120,
            podcastId: "podcast-1", windowId: "asset-1-mark"
        )
        let other = await service.recordUserMarkedAd(
            analysisAssetId: "asset-2", startTime: 60, endTime: 120,
            podcastId: "podcast-1", windowId: "asset-2-mark"
        )

        #expect(other.identity?.windowId == "asset-2-mark")
        #expect((try await store.fetchAdWindows(assetId: "asset-1")).count == 1)
        #expect((try await store.fetchAdWindows(assetId: "asset-2")).count == 1)
    }
}
