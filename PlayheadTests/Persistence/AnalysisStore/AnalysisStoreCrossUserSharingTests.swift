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

    @Test("export snapshot is keyed by podcast full-file SHA and analysis version without transcript evidence")
    func exportSnapshotUsesFullFileSHAAnalysisVersionKeyAndOmitsTranscriptEvidence() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            analysisVersion: 1
        ))
        #expect(snapshot?.schemaVersion == 3)
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
        #expect(!encoded.contains("episodeId"))
        #expect(!encoded.contains("raw transcript evidence"))
        #expect(!encoded.contains("evidenceText"))
    }

    @Test("export suppresses snapshots when the local fingerprint is not a full-file SHA")
    func exportSuppressesSnapshotWhenFingerprintIsWeak() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "https://example.com/audio.mp3|etag|12345|Tue, 01 Jan 2030 00:00:00 GMT"
        )
        try await store.insertAdWindow(makeSharingWindow(id: "source-window", assetId: "asset-a"))

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        #expect(snapshot == nil)
    }

    @Test("export suppresses snapshots when the local fingerprint is not canonical")
    func exportSuppressesSnapshotWhenFingerprintIsNotCanonical() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD"
        )
        try await store.insertAdWindow(makeSharingWindow(id: "source-window", assetId: "asset-a"))

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        #expect(snapshot == nil)
    }

    @Test("export suppresses snapshots when the podcast id is missing")
    func exportSuppressesSnapshotWhenPodcastIdIsMissing() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        try await store.insertAdWindow(makeSharingWindow(id: "source-window", assetId: "asset-a"))

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: ""
        )

        #expect(snapshot == nil)
    }

    @Test("export suppresses snapshots when the podcast id is not canonical")
    func exportSuppressesSnapshotWhenPodcastIdHasOuterWhitespace() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        try await store.insertAdWindow(makeSharingWindow(id: "source-window", assetId: "asset-a"))

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: " podcast-1 "
        )

        #expect(snapshot == nil)
    }

    @Test("import rejects local assets whose fingerprint is not a full-file SHA")
    func importRejectsLocalWeakFingerprintWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "https://example.com/audio.mp3|etag|12345|Tue, 01 Jan 2030 00:00:00 GMT"
        )
        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "podcast-1",
                fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                analysisVersion: 1
            ),
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "fileSHA")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects missing podcast id without partially inserting windows")
    func importRejectsMissingPodcastIdWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )
        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "",
                fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                analysisVersion: 1
            ),
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: ""
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "podcastId")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("export suppresses snapshots instead of dropping invalid windows with stale coverage")
    func exportSuppressesSnapshotWhenExportableWindowIsInvalid() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
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

    @Test("export suppresses snapshots instead of dropping unknown decision states with stale coverage")
    func exportSuppressesSnapshotWhenLocalDecisionStateIsUnknown() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        try await store.updateConfirmedAdCoverage(id: "asset-a", endTime: 90)
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-valid-window",
                assetId: "asset-a",
                start: 10,
                end: 40
            )
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-unknown-state-window",
                assetId: "asset-a",
                start: 50,
                end: 90
            ).withDecisionState("locally-unknown-state")
        )

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        #expect(snapshot == nil)
    }

    @Test("export suppresses snapshots whose shared windows exceed known local duration")
    func exportSuppressesSnapshotsBeyondKnownLocalDuration() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            episodeDurationSec: 30
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-window-beyond-duration",
                assetId: "asset-a",
                start: 20,
                end: 40
            )
        )

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        #expect(snapshot == nil)
    }

    @Test("export suppresses snapshots when local duration is unknown")
    func exportSuppressesSnapshotsWithoutKnownLocalDuration() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            episodeDurationSec: nil
        )
        try await store.insertAdWindow(makeSharingWindow(id: "source-window", assetId: "asset-a"))

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
            fileSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        )

        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "podcast-1",
                fileSHA: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                analysisVersion: 1
            ),
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .mismatchedKey(let expected, let actual) = result {
            #expect(expected.fileSHA == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            #expect(actual.fileSHA == "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
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
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
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
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
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
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
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

    @Test("import rejects invalid provenance timestamps before inserting windows")
    func importRejectsInvalidProvenanceTimestampWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = CrossUserAnalysisSnapshot(
            key: key,
            provenance: CrossUserAnalysisProvenance(
                exportedAt: .infinity,
                sourceAnalysisVersion: 1,
                sourceAppBuild: "test-build"
            ),
            analysisCoverageEndSec: 60,
            measurements: CrossUserAnalysisMeasurements(),
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a")
                ),
            ]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "provenance.exportedAt")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects invalid measurements before inserting windows")
    func importRejectsInvalidMeasurementsWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )
        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a")
                ),
            ],
            measurements: CrossUserAnalysisMeasurements(
                fmMinutesSaved: .infinity,
                queueToReadyLatencySec: 2.5,
                batteryDeltaPercent: nil
            )
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "measurements")
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
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
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

    @Test("import rejects windows with non-canonical required strings without partial insertion")
    func importRejectsWindowsWithNonCanonicalRequiredStringsWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    sourceWindowId: " source-window ",
                    startTime: 12,
                    endTime: 60,
                    confidence: 0.99,
                    boundaryState: AdBoundaryState.acousticRefined.rawValue,
                    decisionState: AdDecisionState.confirmed.rawValue,
                    detectorVersion: "fm-test-v1",
                    advertiser: "Acme",
                    product: "Widget",
                    adDescription: "Whitespace id promo",
                    metadataSource: "foundation-model",
                    metadataConfidence: 0.81,
                    metadataPromptVersion: "prompt-v1",
                    evidenceSources: "semantic,fusion",
                    eligibilityGate: "ready",
                    catalogStoreMatchSimilarity: nil
                ),
            ]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "window[0]")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects inflated coverage that exceeds exported windows")
    func importRejectsInflatedCoverageBeyondExportedWindows() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(
                        id: "source-window",
                        assetId: "asset-a",
                        start: 10,
                        end: 40
                    )
                ),
            ],
            analysisCoverageEndSec: 90
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "analysisCoverageEndSec")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects windows that exceed the snapshot coverage without partial insertion")
    func importRejectsWindowsBeyondClaimedCoverageWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(
                        id: "source-window-beyond-coverage",
                        assetId: "asset-a",
                        start: 12,
                        end: 60
                    )
                ),
            ],
            analysisCoverageEndSec: 30
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "analysisCoverageEndSec")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects duplicate source window ids without partial insertion")
    func importRejectsDuplicateSourceWindowIdsWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let firstWindow = CrossUserAnalysisSnapshot.Window(
            adWindow: makeSharingWindow(
                id: "source-duplicate-window",
                assetId: "asset-a",
                start: 12,
                end: 40
            )
        )
        let secondWindow = CrossUserAnalysisSnapshot.Window(
            adWindow: makeSharingWindow(
                id: "source-duplicate-window",
                assetId: "asset-a",
                start: 50,
                end: 60
            )
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [firstWindow, secondWindow],
            analysisCoverageEndSec: 60
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "window[1].sourceWindowId")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects snapshots beyond known local duration without partially inserting windows")
    func importRejectsSnapshotBeyondKnownLocalDurationWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let validWindow = CrossUserAnalysisSnapshot.Window(
            adWindow: makeSharingWindow(
                id: "source-valid-window",
                assetId: "asset-a",
                start: 12,
                end: 60
            )
        )
        let unsafeWindow = CrossUserAnalysisSnapshot.Window(
            adWindow: makeSharingWindow(
                id: "source-beyond-local-duration-window",
                assetId: "asset-a",
                start: 130,
                end: 150
            )
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [validWindow, unsafeWindow],
            analysisCoverageEndSec: 150
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "episodeDurationSec")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects snapshots when local duration is unknown without partially inserting windows")
    func importRejectsSnapshotWithoutKnownLocalDurationWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: nil
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
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
            analysisCoverageEndSec: 60
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "episodeDurationSec")
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
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
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
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
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

    @Test("export coverage does not include locally reverted windows")
    func exportCoverageDoesNotIncludeLocallyRevertedWindows() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        try await store.updateConfirmedAdCoverage(id: "asset-a", endTime: 90)
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-confirmed-window",
                assetId: "asset-a",
                start: 10,
                end: 40
            )
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-reverted-window",
                assetId: "asset-a",
                start: 50,
                end: 90
            ).withDecisionState(AdDecisionState.reverted.rawValue)
        )

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        let exported = try #require(snapshot)
        #expect(exported.windows.map(\.sourceWindowId) == ["source-confirmed-window"])
        #expect(exported.analysisCoverageEndSec == 40)
    }

    @Test("export drops local correction boundary states without inflating coverage")
    func exportDropsLocalCorrectionBoundaryStatesWithoutInflatingCoverage() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-derived-window",
                assetId: "asset-a",
                start: 10,
                end: 40
            )
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-user-marked-window",
                assetId: "asset-a",
                start: 50,
                end: 90
            ).withBoundaryState("userMarked")
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-correction-replay-window",
                assetId: "asset-a",
                start: 95,
                end: 120
            ).withBoundaryState("correctionReplay")
        )

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        let exported = try #require(snapshot)
        #expect(exported.windows.map(\.sourceWindowId) == ["source-derived-window"])
        #expect(exported.windows.first?.boundaryState == AdBoundaryState.acousticRefined.rawValue)
        #expect(exported.analysisCoverageEndSec == 40)

        let encoded = try exported.encodedJSONString()
        #expect(!encoded.contains("userMarked"))
        #expect(!encoded.contains("correctionReplay"))
    }

    @Test("export suppresses snapshots when a boundary state is unknown")
    func exportSuppressesSnapshotWhenBoundaryStateIsUnknown() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-a",
            episodeId: "episode-1",
            fileSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-valid-window",
                assetId: "asset-a",
                start: 10,
                end: 40
            )
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "source-unknown-boundary-window",
                assetId: "asset-a",
                start: 50,
                end: 90
            ).withBoundaryState("future-boundary-state")
        )

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: "asset-a",
            podcastId: "podcast-1"
        )

        #expect(snapshot == nil)
    }

    @Test("import rejects local correction boundary state without partially inserting valid windows")
    func importRejectsLocalCorrectionBoundaryStateWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let validWindow = CrossUserAnalysisSnapshot.Window(
            adWindow: makeSharingWindow(
                id: "source-valid-window",
                assetId: "asset-a",
                start: 12,
                end: 60
            )
        )
        let correctionWindow = CrossUserAnalysisSnapshot.Window(
            adWindow: makeSharingWindow(
                id: "source-user-marked-window",
                assetId: "asset-a",
                start: 70,
                end: 90
            ).withBoundaryState("userMarked")
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [validWindow, correctionWindow]
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

    @Test("import rejects reverted local lifecycle state from externally supplied snapshots")
    func importRejectsRevertedLifecycleStateWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "source-valid-window", assetId: "asset-a")
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

    @Test("import rejects applied playback lifecycle state from externally supplied snapshots")
    func importRejectsAppliedLifecycleStateWithoutPartialInsert() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "source-applied-window", assetId: "asset-a")
                        .withDecisionState(AdDecisionState.applied.rawValue)
                ),
            ]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "window[0]")
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
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
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

    @Test("import rejects non-ad windows that carry ad metadata")
    func importRejectsNonAdWindowsWithAdMetadata() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    sourceWindowId: "source-incoherent-non-ad",
                    startTime: 12,
                    endTime: 60,
                    confidence: 0.99,
                    boundaryState: AdBoundaryState.acousticRefined.rawValue,
                    decisionState: AdDecisionState.suppressed.rawValue,
                    isAd: false,
                    detectorVersion: "fm-test-v1",
                    advertiser: "Acme",
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

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "window[0]")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("import rejects shared windows with missing provenance identifiers")
    func importRejectsWindowsWithMissingProvenanceIdentifiers() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    sourceWindowId: "",
                    startTime: 12,
                    endTime: 60,
                    confidence: 0.99,
                    boundaryState: AdBoundaryState.acousticRefined.rawValue,
                    decisionState: AdDecisionState.confirmed.rawValue,
                    detectorVersion: "fm-test-v1",
                    advertiser: "Acme",
                    product: "Widget",
                    adDescription: "Missing id promo",
                    metadataSource: "foundation-model",
                    metadataConfidence: 0.81,
                    metadataPromptVersion: "prompt-v1",
                    evidenceSources: "semantic,fusion",
                    eligibilityGate: "ready",
                    catalogStoreMatchSimilarity: nil
                ),
            ]
        )

        let result = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        if case .incompatibleSnapshot(let reason) = result {
            #expect(reason == "window[0]")
        } else {
            Issue.record("Expected incompatibleSnapshot result, got \(result)")
        }
        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.isEmpty)
    }

    @Test("file-backed provider publishes and fetches snapshots by share key")
    func fileBackedProviderPublishesAndFetchesByShareKey() async throws {
        let directory = try makeTempDir(prefix: "CrossUserAnalysisSharingProvider")
        let provider = FileBackedCrossUserAnalysisSharingProvider(directory: directory)
        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
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
            fileSHA: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            analysisVersion: 1
        ))
        #expect(miss == nil)
        let versionMiss = await provider.matchingSnapshot(for: CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 2
        ))
        #expect(versionMiss == nil)
    }

    @Test("file-backed provider ignores snapshots not keyed by a canonical full-file SHA")
    func fileBackedProviderIgnoresNonSHAKeys() async throws {
        let directory = try makeTempDir(prefix: "CrossUserAnalysisSharingProvider")
        let provider = FileBackedCrossUserAnalysisSharingProvider(directory: directory)
        let invalidKey = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "https://example.com/audio.mp3|etag|12345|Tue, 01 Jan 2030 00:00:00 GMT",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: invalidKey,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))]
        )

        try await provider.publish(snapshot)

        let fetched = await provider.matchingSnapshot(for: invalidKey)
        #expect(fetched == nil)
        let invalidVersionKey = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 0
        )
        let invalidVersionSnapshot = makeSnapshot(
            key: invalidVersionKey,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window-v0", assetId: "asset-a"))]
        )
        try await provider.publish(invalidVersionSnapshot)
        let invalidVersionFetched = await provider.matchingSnapshot(for: invalidVersionKey)
        #expect(invalidVersionFetched == nil)
        let contents = try FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        )
        #expect(contents.isEmpty)

        let uppercaseKey = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
            analysisVersion: 1
        )
        let uppercaseSnapshot = makeSnapshot(
            key: uppercaseKey,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))]
        )

        try await provider.publish(uppercaseSnapshot)

        let uppercaseFetched = await provider.matchingSnapshot(for: uppercaseKey)
        #expect(uppercaseFetched == nil)
        let contentsAfterUppercase = try FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        )
        #expect(contentsAfterUppercase.isEmpty)
    }

    @Test("file-backed provider ignores snapshots with missing key components")
    func fileBackedProviderIgnoresMissingKeyComponents() async throws {
        let directory = try makeTempDir(prefix: "CrossUserAnalysisSharingProvider")
        let provider = FileBackedCrossUserAnalysisSharingProvider(directory: directory)
        let invalidKey = CrossUserAnalysisShareKey(
            podcastId: "",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let snapshot = makeSnapshot(
            key: invalidKey,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a"))]
        )

        try await provider.publish(snapshot)

        let fetched = await provider.matchingSnapshot(for: invalidKey)
        #expect(fetched == nil)
        let contents = try FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        )
        #expect(contents.isEmpty)

        let whitespaceKey = CrossUserAnalysisShareKey(
            podcastId: " podcast-1 ",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let whitespaceSnapshot = makeSnapshot(
            key: whitespaceKey,
            windows: [CrossUserAnalysisSnapshot.Window(adWindow: makeSharingWindow(id: "source-window-whitespace", assetId: "asset-a"))]
        )

        try await provider.publish(whitespaceSnapshot)

        let whitespaceFetched = await provider.matchingSnapshot(for: whitespaceKey)
        #expect(whitespaceFetched == nil)
        let contentsAfterWhitespace = try FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        )
        #expect(contentsAfterWhitespace.isEmpty)
    }

    @Test("file-backed provider keeps separator-containing key components distinct")
    func fileBackedProviderKeepsSeparatorContainingKeyComponentsDistinct() async throws {
        let directory = try makeTempDir(prefix: "CrossUserAnalysisSharingProvider")
        let provider = FileBackedCrossUserAnalysisSharingProvider(directory: directory)
        let fileSHA = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        let firstKey = CrossUserAnalysisShareKey(
            podcastId: "podcast",
            fileSHA: fileSHA,
            analysisVersion: 1
        )
        let secondKey = CrossUserAnalysisShareKey(
            podcastId: "podcast|a",
            fileSHA: fileSHA,
            analysisVersion: 1
        )
        let firstSnapshot = makeSnapshot(
            key: firstKey,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "first-window", assetId: "asset-a")
                ),
            ]
        )
        let secondSnapshot = makeSnapshot(
            key: secondKey,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "second-window", assetId: "asset-a")
                ),
            ]
        )

        try await provider.publish(firstSnapshot)
        try await provider.publish(secondSnapshot)

        #expect(await provider.matchingSnapshot(for: firstKey) == firstSnapshot)
        #expect(await provider.matchingSnapshot(for: secondKey) == secondSnapshot)
    }

    @Test("file-backed provider rejects files whose embedded snapshot key does not match")
    func fileBackedProviderRejectsMismatchedEmbeddedSnapshotKey() async throws {
        let directory = try makeTempDir(prefix: "CrossUserAnalysisSharingProvider")
        let provider = FileBackedCrossUserAnalysisSharingProvider(directory: directory)
        let requestedKey = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let embeddedKey = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 2
        )
        let embeddedSnapshot = makeSnapshot(
            key: embeddedKey,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(id: "source-window", assetId: "asset-a")
                ),
            ]
        )
        let data = try JSONEncoder().encode(embeddedSnapshot)
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        try data.write(
            to: FileBackedCrossUserAnalysisSharingProvider.fileURL(
                for: requestedKey,
                directory: directory
            ),
            options: [.atomic]
        )

        let fetched = await provider.matchingSnapshot(for: requestedKey)

        #expect(fetched == nil)
    }

    @Test("matching import remaps windows to the local asset and is idempotent")
    func matchingImportRemapsAndIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
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
                fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                analysisVersion: 1
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
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
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
                fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                analysisVersion: 1
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

    @Test("import does not override an equivalent local suppressed span")
    func importDoesNotOverrideEquivalentLocalSuppressedSpan() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: 120
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "local-suppressed-span",
                assetId: "asset-b",
                start: 12.1,
                end: 60.1,
                confidence: 0.96,
                evidenceText: "local evidence stays local"
            ).withDecisionState(AdDecisionState.suppressed.rawValue)
        )

        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "podcast-1",
                fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                analysisVersion: 1
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
            Issue.record("Expected import to respect local suppressed span, got \(result)")
            return
        }
        #expect(receipt.insertedWindowCount == 0)
        #expect(receipt.insertedWindowIds.isEmpty)
        #expect(receipt.bannerEligibleWindowIds.isEmpty)
        #expect(receipt.insertedCueCount == 0)
        #expect(receipt.totalWindowCount == 1)
        #expect(receipt.cueCoverageSec == 0)

        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.count == 1)
        let local = try #require(windows.first { $0.id == "local-suppressed-span" })
        #expect(local.decisionState == AdDecisionState.suppressed.rawValue)
        #expect(local.evidenceText == "local evidence stays local")
    }

    @Test("import does not override an equivalent local reverted span")
    func importDoesNotOverrideEquivalentLocalRevertedSpan() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: 120
        )
        try await store.insertAdWindow(
            makeSharingWindow(
                id: "local-reverted-span",
                assetId: "asset-b",
                start: 12.1,
                end: 60.1,
                confidence: 0.96,
                evidenceText: "local evidence stays local"
            ).withDecisionState(AdDecisionState.reverted.rawValue)
        )

        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "podcast-1",
                fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                analysisVersion: 1
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
            Issue.record("Expected import to respect local reverted span, got \(result)")
            return
        }
        #expect(receipt.insertedWindowCount == 0)
        #expect(receipt.insertedWindowIds.isEmpty)
        #expect(receipt.bannerEligibleWindowIds.isEmpty)
        #expect(receipt.insertedCueCount == 0)
        #expect(receipt.totalWindowCount == 1)
        #expect(receipt.cueCoverageSec == 0)

        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.count == 1)
        #expect(windows.first?.id == "local-reverted-span")
        #expect(windows.first?.decisionState == AdDecisionState.reverted.rawValue)
        #expect(windows.first?.evidenceText == "local evidence stays local")
    }

    @Test("import does not supersede an imported ad after local suppression")
    func importDoesNotSupersedeImportedAdAfterLocalSuppression() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: 120
        )

        let snapshot = makeSnapshot(
            key: CrossUserAnalysisShareKey(
                podcastId: "podcast-1",
                fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                analysisVersion: 1
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

        let first = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )
        guard case .imported(let firstReceipt) = first,
              let importedId = firstReceipt.insertedWindowIds.first else {
            Issue.record("Expected first import to apply, got \(first)")
            return
        }

        try await store.updateAdWindowDecision(
            id: importedId,
            decisionState: AdDecisionState.suppressed.rawValue
        )

        let second = try await store.importCrossUserAnalysisSnapshot(
            snapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        guard case .imported(let secondReceipt) = second else {
            Issue.record("Expected second import to respect local suppression, got \(second)")
            return
        }
        #expect(secondReceipt.insertedWindowCount == 0)
        #expect(secondReceipt.insertedWindowIds.isEmpty)
        #expect(secondReceipt.bannerEligibleWindowIds.isEmpty)
        #expect(secondReceipt.insertedCueCount == 0)
        #expect(secondReceipt.cueCoverageSec == 0)

        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.count == 1)
        #expect(windows.first?.id == importedId)
        #expect(windows.first?.decisionState == AdDecisionState.suppressed.rawValue)
        #expect(windows.first?.adDescription == "Mid-roll promo")
    }

    @Test("later shared cue ad supersedes prior imported non-ad row with same source id")
    func laterSharedCueAdSupersedesPriorImportedNonAdWithSameSourceId() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let nonAdSnapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    sourceWindowId: "source-changing-window",
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
        let adSnapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(
                        id: "source-changing-window",
                        assetId: "asset-a",
                        start: 12,
                        end: 60
                    )
                ),
            ]
        )

        let first = try await store.importCrossUserAnalysisSnapshot(
            nonAdSnapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )
        let second = try await store.importCrossUserAnalysisSnapshot(
            adSnapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )
        let third = try await store.importCrossUserAnalysisSnapshot(
            adSnapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        guard case .imported(let firstReceipt) = first else {
            Issue.record("Expected first import to apply, got \(first)")
            return
        }
        guard case .imported(let secondReceipt) = second else {
            Issue.record("Expected second import to apply, got \(second)")
            return
        }
        guard case .imported(let thirdReceipt) = third else {
            Issue.record("Expected third import to apply idempotently, got \(third)")
            return
        }
        #expect(firstReceipt.insertedWindowCount == 1)
        #expect(firstReceipt.insertedCueCount == 0)
        #expect(firstReceipt.bannerEligibleWindowIds.isEmpty)
        #expect(secondReceipt.insertedWindowCount == 1)
        #expect(secondReceipt.insertedCueCount == 1)
        #expect(secondReceipt.bannerEligibleWindowIds == secondReceipt.insertedWindowIds)
        #expect(thirdReceipt.insertedWindowCount == 0)
        #expect(thirdReceipt.insertedCueCount == 0)
        #expect(thirdReceipt.bannerEligibleWindowIds == secondReceipt.bannerEligibleWindowIds)

        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.count == 2)
        #expect(windows.contains { $0.decisionState == AdDecisionState.suppressed.rawValue })
        let importedAd = try #require(windows.first {
            $0.decisionState == AdDecisionState.confirmed.rawValue
        })
        #expect(importedAd.adDescription == "Mid-roll promo")
        #expect(importedAd.evidenceText == nil)
        #expect(importedAd.endTime == 60)
    }

    @Test("later shared cue ad is not blocked by unrelated imported non-ad span")
    func laterSharedCueAdIsNotBlockedByUnrelatedImportedNonAdSpan() async throws {
        let store = try await makeTestStore()
        try await seedSharingAsset(
            store: store,
            id: "asset-b",
            episodeId: "episode-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            episodeDurationSec: 120
        )

        let key = CrossUserAnalysisShareKey(
            podcastId: "podcast-1",
            fileSHA: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            analysisVersion: 1
        )
        let nonAdSnapshot = makeSnapshot(
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
        let adSnapshot = makeSnapshot(
            key: key,
            windows: [
                CrossUserAnalysisSnapshot.Window(
                    adWindow: makeSharingWindow(
                        id: "source-ad-window",
                        assetId: "asset-a",
                        start: 12,
                        end: 60
                    )
                ),
            ]
        )

        let first = try await store.importCrossUserAnalysisSnapshot(
            nonAdSnapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )
        let second = try await store.importCrossUserAnalysisSnapshot(
            adSnapshot,
            targetAssetId: "asset-b",
            podcastId: "podcast-1"
        )

        guard case .imported(let firstReceipt) = first else {
            Issue.record("Expected first import to apply, got \(first)")
            return
        }
        guard case .imported(let secondReceipt) = second else {
            Issue.record("Expected second import to apply, got \(second)")
            return
        }
        #expect(firstReceipt.insertedWindowCount == 1)
        #expect(firstReceipt.insertedCueCount == 0)
        #expect(secondReceipt.insertedWindowCount == 1)
        #expect(secondReceipt.insertedCueCount == 1)
        #expect(secondReceipt.bannerEligibleWindowIds == secondReceipt.insertedWindowIds)

        let windows = try await store.fetchAdWindows(assetId: "asset-b")
        #expect(windows.count == 2)
        #expect(windows.contains { $0.decisionState == AdDecisionState.suppressed.rawValue })
        #expect(windows.contains { $0.decisionState == AdDecisionState.confirmed.rawValue })
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

    func withBoundaryState(_ boundaryState: String) -> AdWindow {
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
