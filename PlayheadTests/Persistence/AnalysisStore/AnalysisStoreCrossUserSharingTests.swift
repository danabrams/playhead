// AnalysisStoreCrossUserSharingTests.swift
// Tests for Phase A full-file-SHA cross-user ad-window sharing.

import Foundation
import Testing
@testable import Playhead

private func seedSharingAsset(
    store: AnalysisStore,
    id: String,
    episodeId: String,
    fileSHA: String,
    episodeDurationSec: Double? = 120
) async throws {
    try await store.insertAsset(
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: fileSHA,
            weakFingerprint: nil,
            sourceURL: "",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    )
}

private func makeSharingWindow(
    id: String,
    assetId: String,
    start: Double = 10,
    end: Double = 40,
    confidence: Double = 0.92,
    evidenceText: String? = "raw transcript evidence should not be shared"
) -> AdWindow {
    AdWindow(
        id: id,
        analysisAssetId: assetId,
        startTime: start,
        endTime: end,
        confidence: confidence,
        boundaryState: AdBoundaryState.acousticRefined.rawValue,
        decisionState: AdDecisionState.confirmed.rawValue,
        detectorVersion: "fm-test-v1",
        advertiser: "Acme",
        product: "Widget",
        adDescription: "Mid-roll promo",
        evidenceText: evidenceText,
        evidenceStartTime: 11,
        metadataSource: "foundation-model",
        metadataConfidence: 0.81,
        metadataPromptVersion: "prompt-v1",
        wasSkipped: true,
        userDismissedBanner: true,
        evidenceSources: "semantic,fusion",
        eligibilityGate: "ready",
        catalogStoreMatchSimilarity: 0.63
    )
}

private func makeSnapshot(
    key: CrossUserAnalysisShareKey,
    windows: [CrossUserAnalysisSnapshot.Window],
    analysisCoverageEndSec: Double = 60,
    schemaVersion: Int = CrossUserAnalysisSnapshot.currentSchemaVersion,
    sourceAnalysisVersion: Int = 1,
    pipelineVersions: PipelineVersions = PipelineVersions.current(),
    measurements: CrossUserAnalysisMeasurements = CrossUserAnalysisMeasurements(
        fmMinutesSaved: nil,
        queueToReadyLatencySec: 2.5,
        batteryDeltaPercent: nil
    )
) -> CrossUserAnalysisSnapshot {
    CrossUserAnalysisSnapshot(
        schemaVersion: schemaVersion,
        key: key,
        provenance: CrossUserAnalysisProvenance(
            exportedAt: 1_800_000_000,
            sourceAnalysisVersion: sourceAnalysisVersion,
            sourceAppBuild: "test-build",
            pipelineVersions: pipelineVersions
        ),
        analysisCoverageEndSec: analysisCoverageEndSec,
        measurements: measurements,
        windows: windows
    )
}

@Suite("AnalysisStoreCrossUserSharing")
struct AnalysisStoreCrossUserSharingTests {

    @Test("export snapshot is keyed by podcast episode and asset full-file SHA without transcript evidence")
    func exportSnapshotUsesFullFileSHATupleAndOmitsTranscriptEvidence() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "full-file-sha-a",
            episodeDurationSec: 180
        )
        try await store.insertAdWindow(makeSharingWindow(id: "source-window", assetId: "asset-a"))

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1",
            measurements: CrossUserAnalysisMeasurements(
                fmMinutesSaved: nil,
                queueToReadyLatencySec: 3.25,
                batteryDeltaPercent: nil
            )
        )

        #expect(snapshot?.key == CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "full-file-sha-a"
        ))
        #expect(snapshot?.measurements.fmMinutesSaved == nil)
        #expect(snapshot?.analysisCoverageEndSec == 40)
        #expect(snapshot?.measurements.queueToReadyLatencySec == 3.25)
        #expect(snapshot?.measurements.batteryDeltaPercent == nil)
        #expect(snapshot?.windows.count == 1)
        #expect(snapshot?.windows.first?.sourceWindowId == "source-window")
        #expect(snapshot?.windows.first?.startTime == 10)
        #expect(snapshot?.windows.first?.endTime == 40)
        #expect(snapshot?.windows.first?.isAd == true)

        let encoded = try #require(snapshot).encodedJSONString()
        #expect(!encoded.contains("raw transcript evidence"))
        #expect(!encoded.contains("evidenceText"))
    }

    @Test("export suppresses snapshots instead of dropping invalid windows with stale coverage")
    func exportSuppressesSnapshotWhenExportableWindowIsInvalid() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "full-file-sha-a"
        )
        try await store.updateConfirmedAdCoverage(id: "asset-a", endTime: 90)
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-invalid-window",
                assetId: "asset-a",
                start: 10,
                end: 90,
                confidence: 1.4
            )
        )

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        #expect(snapshot == nil)
    }

    @Test("import mismatch is an explicit no-op and leaves local windows untouched")
    func importMismatchIsNoOp() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "local-full-file-sha"
        )

        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "podcast-1",
                episodeId: "episode-1",
                fileSHA: "other-full-file-sha"
            ),
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .mismatchedKey(let expected, let actual) = result {
            #expect(expected.fileSHA == "local-full-file-sha")
            #expect(actual.fileSHA == "other-full-file-sha")
        } else {
            Issue.record("Expected mismatchedKey result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects stale snapshots before inserting windows")
    func importRejectsStaleSnapshots() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))],
            sourceAnalysisVersion: 0
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "analysisVersion")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects stale schema snapshots before inserting windows")
    func importRejectsStaleSchemaSnapshots() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))],
            schemaVersion: CrossUserAnalysisSnapshot.currentSchemaVersion - 1
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "schemaVersion")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects stale pipeline-version snapshots before inserting windows")
    func importRejectsStalePipelineVersionSnapshots() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )
        let staleVersions = PipelineVersions(
            modelVersion: "old-detector",
            policyVersion: "old-policy",
            featureSchemaVersion: -1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))],
            pipelineVersions: staleVersions
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "pipelineVersions")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects invalid windows without partially inserting valid windows")
    func importRejectsInvalidWindowsWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )
        let validWindow = CrossUserAnalysisSnapshot.Window(
            adWindow: makeSharingWindow(
                id: "source-valid-window",
                assetId: "asset-a",
                start: 12,
                end: 60
            )
        )
        let invalidWindow = CrossUserAnalysisSnapshot.Window(
            sourceWindowId: "source-invalid-window",
            startTime: 70,
            endTime: 90,
            confidence: 1.4,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "fm-test-v1",
            advertiser: "Acme",
            product: "Widget",
            adDescription: "Invalid confidence promo",
            metadataSource: "foundation-model",
            metadataConfidence: 0.81,
            metadataPromptVersion: "prompt-v1",
            evidenceSources: "semantic,fusion",
            eligibilityGate: "ready",
            catalogStoreMatchSimilarity: 0.63
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [validWindow, invalidWindow]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "window[1]")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects semantically invalid windows without partially inserting valid windows")
    func importRejectsInvalidDecisionStateWindowsWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )
        let validWindow = CrossUserAnalysisSnapshot.Window(
            adWindow: makeSharingWindow(
                id: "source-valid-window",
                assetId: "asset-a",
                start: 12,
                end: 60
            )
        )
        let invalidWindow = CrossUserAnalysisSnapshot.Window(
            sourceWindowId: "source-invalid-decision-window",
            startTime: 70,
            endTime: 90,
            confidence: 0.94,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "externally-unknown-state",
            isAd: true,
            detectorVersion: "fm-test-v1",
            advertiser: "Acme",
            product: "Widget",
            adDescription: "Invalid decision promo",
            metadataSource: "foundation-model",
            metadataConfidence: 0.81,
            metadataPromptVersion: "prompt-v1",
            evidenceSources: "semantic,fusion",
            eligibilityGate: "ready",
            catalogStoreMatchSimilarity: 0.63
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [validWindow, invalidWindow]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "window[1]")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("export strips local playback and correction state from shared windows")
    func exportStripsLocalPlaybackAndCorrectionState() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "full-file-sha-a"
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-applied-window",
                assetId: "asset-a",
                start: 10,
                end: 40,
                confidence: 0.92
            ).withDecisionState(AdDecisionState.applied.rawValue)
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-reverted-window",
                assetId: "asset-a",
                start: 50,
                end: 80,
                confidence: 0.95
            ).withDecisionState(AdDecisionState.reverted.rawValue)
        )

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        let windows = try #require(snapshot?.windows)
        #expect(windows.count == 1)
        #expect(windows.first?.sourceWindowId == "source-applied-window")
        #expect(windows.first?.decisionState == AdDecisionState.confirmed.rawValue)
        #expect(windows.first?.isAd == true)
    }

    @Test("import rejects reverted local lifecycle state from externally supplied snapshots")
    func importRejectsRevertedLifecycleStateWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "source-applied-window", assetId: "asset-a")
                        .withDecisionState(AdDecisionState.applied.rawValue)
                ),
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "source-reverted-window", assetId: "asset-a", start: 50, end: 80)
                        .withDecisionState(AdDecisionState.reverted.rawValue)
                ),
            ]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "window[1]")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }

        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("imported non-ad windows do not count as cue coverage")
    func importNonAdWindowsDoNotCountAsCueCoverage() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    sourceWindowId: "source-non-ad-window",
                    startTime: 12,
                    endTime: 60,
                    confidence: 0.99,
                    boundaryState: AdBoundaryState.acousticRefined.rawValue,
                    decisionState: AdDecisionState.suppressed.rawValue,
                    isAd: false,
                    detectorVersion: "fm-test-v1",
                    advertiser: nil,
                    product: nil,
                    adDescription: nil,
                    metadataSource: "foundation-model",
                    metadataConfidence: 0.81,
                    metadataPromptVersion: "prompt-v1",
                    evidenceSources: "semantic,fusion",
                    eligibilityGate: "not-ad",
                    catalogStoreMatchSimilarity: nil
                ),
            ]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        guard case .imported(let receipt) = result else {
            Issue.record("Expected import to persist the non-ad verdict, got \(result)")
            return
        }
        #expect(receipt.insertedWindowCount == 1)
        #expect(receipt.insertedCueCount == 0)
        #expect(receipt.cueCoverageSec == 0)

        let asset = try await store.fetchAsset(id: "asset-b")
        #expect(asset?.confirmedAdCoverageEndTime == nil)
    }

    @Test("file-backed provider publishes and fetches snapshots by tuple key")
    func fileBackedProviderPublishesAndFetchesByTupleKey() async throws {
        let directory = try makeTempDir(prefix: "CrossUserAnalysisSharingProvider")
        let provider = FileBackedCrossUserAnalysisSharingProvider(directory: directory)
        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha"
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))]
        )

        try await provider.publish(snapshot)

        let fetched = await provider.matchingSnapshot(for: key)
        #expect(fetched == snapshot)
        let miss = await provider.matchingSnapshot(for: CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            episodeId: "episode-1",
            fileSHA: "other-full-file-sha"
        ))
        #expect(miss == nil)
    }

    @Test("matching import remaps windows to the local asset and is idempotent")
    func matchingImportRemapsAndIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha",
            episodeDurationSec: 120
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "local-window",
                assetId: "asset-b",
                start: 100,
                end: 115,
                confidence: 0.4,
                evidenceText: "local evidence stays local"
            )
        )

        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "podcast-1",
                episodeId: "episode-1",
                fileSHA: "shared-full-file-sha"
            ),
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(
                        id: "source-window",
                        assetId: "asset-a",
                        start: 12,
                        end: 60
                    )
                ),
            ],
            measurements: CrossUserAnalysisMeasurements(
                fmMinutesSaved: 2,
                queueToReadyLatencySec: 2.5,
                batteryDeltaPercent: -1.5
            )
        )

        let first = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )
        let second = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        guard case .imported(let firstReceipt) = first else {
            Issue.record("Expected first import to apply, got \(first)")
            return
        }
        guard case .imported(let secondReceipt) = second else {
            Issue.record("Expected second import to be idempotent apply, got \(second)")
            return
        }
        #expect(firstReceipt.insertedWindowCount == 1)
        #expect(firstReceipt.insertedWindowIds.count == 1)
        #expect(firstReceipt.bannerEligibleWindowIds == firstReceipt.insertedWindowIds)
        #expect(firstReceipt.insertedCueCount == 1)
        #expect(firstReceipt.analysisCoverageEndSec == 60)
        #expect(firstReceipt.fmMinutesSaved == 2)
        #expect(firstReceipt.queueToReadyLatencySec == 2.5)
        #expect(firstReceipt.batteryDeltaPercent == -1.5)
        #expect(secondReceipt.insertedWindowCount == 0)
        #expect(secondReceipt.insertedWindowIds.isEmpty)
        #expect(secondReceipt.bannerEligibleWindowIds == firstReceipt.insertedWindowIds)
        #expect(secondReceipt.insertedCueCount == 0)

        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.count == 2)
        let local = try #require(windows.first { $0.id == "local-window" })
        #expect(local.evidenceText == "local evidence stays local")
        #expect(local.wasSkipped == true)
        #expect(local.userDismissedBanner == true)

        let imported = try #require(windows.first { $0.id != "local-window" })
        #expect(imported.analysisAssetId == "asset-b")
        #expect(imported.startTime == 12)
        #expect(imported.endTime == 60)
        #expect(imported.confidence == 0.92)
        #expect(imported.boundaryState == AdBoundaryState.acousticRefined.rawValue)
        #expect(imported.decisionState == AdDecisionState.confirmed.rawValue)
        #expect(imported.advertiser == "Acme")
        #expect(imported.product == "Widget")
        #expect(imported.adDescription == "Mid-roll promo")
        #expect(imported.metadataSource == "foundation-model")
        #expect(imported.metadataConfidence == 0.81)
        #expect(imported.metadataPromptVersion == "prompt-v1")
        #expect(imported.evidenceSources == "semantic,fusion")
        #expect(imported.eligibilityGate == "ready")
        #expect(imported.catalogStoreMatchSimilarity == 0.63)
        #expect(imported.evidenceText == nil)
        #expect(imported.evidenceStartTime == nil)
        #expect(imported.wasSkipped == false)
        #expect(imported.userDismissedBanner == false)
    }

    @Test("import does not duplicate an equivalent local span")
    func importDeduplicatesEquivalentLocalSpan() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "shared-full-file-sha",
            episodeDurationSec: 120
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "local-existing-span",
                assetId: "asset-b",
                start: 12.1,
                end: 60.1,
                confidence: 0.91,
                evidenceText: "local evidence stays local"
            )
        )

        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "podcast-1",
                episodeId: "episode-1",
                fileSHA: "shared-full-file-sha"
            ),
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(
                        id: "source-window",
                        assetId: "asset-a",
                        start: 12,
                        end: 60
                    )
                ),
            ]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        guard case .imported(let receipt) = result else {
            Issue.record("Expected import to no-op on equivalent local span, got \(result)")
            return
        }
        #expect(receipt.insertedWindowCount == 0)
        #expect(receipt.insertedWindowIds.isEmpty)
        #expect(receipt.totalWindowCount == 1)

        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.count == 1)
        #expect(windows.first?.id == "local-existing-span")
        #expect(windows.first?.evidenceText == "local evidence stays local")
    }
}

private extension Encodable {
    func encodedJSONString() throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(self)
        return String(data: data, encoding: .utf8) ?? ""
    }
}

private extension AdWindow {
    func withDecisionState(_ decisionState: String) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            boundaryState: boundaryState,
            decisionState: decisionState,
            detectorVersion: detectorVersion,
            advertiser: advertiser,
            product: product,
            adDescription: adDescription,
            evidenceText: evidenceText,
            evidenceStartTime: evidenceStartTime,
            metadataSource: metadataSource,
            metadataConfidence: metadataConfidence,
            metadataPromptVersion: metadataPromptVersion,
            wasSkipped: wasSkipped,
            userDismissedBanner: userDismissedBanner,
            evidenceSources: evidenceSources,
            eligibilityGate: eligibilityGate,
            catalogStoreMatchSimilarity: catalogStoreMatchSimilarity
        )
    }
}
