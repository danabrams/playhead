// FinalPassLaunchSweepPaginationTests.swift
// playhead-b0uf regression: the launch-time final-pass backfill sweep
// (`PlayheadRuntime.runFinalPassBackfillForAllAssetsAtLaunch`) must page
// through EVERY asset in `analysis_assets`, not just the first page.
//
// The original implementation loaded the whole table via the DEBUG-only
// `AnalysisStore.fetchAllAssets()`, which (a) failed to compile in Release
// (fetchAllAssets is `#if DEBUG`-gated) and (b) was memory-unsafe on a real
// library. The fix pages through the table with the production-safe keyset
// iterator `fetchAssetsKeysetByRowId(afterRowId:limit:)` in bounded batches.
//
// This test seeds MORE assets than a single page holds and asserts the sweep
// still visits every one — proving the pagination covers the whole table with
// no dropped page and no off-by-one at the page boundary. A unit test cannot
// catch the Release-compile break itself (tests build Debug); the Release
// build is the real guard for that. This test guards the behavior.
//
// Observability: the per-page batch podcast-id resolver is invoked once per
// page with that page's episodeIds, BEFORE the per-asset download-cache gate.
// We accumulate every episodeId the resolver is asked about; the union across
// all pages must equal the full seeded set. Using an empty download cache
// (cachedFileURL == nil for every episode) means the heavyweight per-asset
// runner is never invoked, so the test stays fast while still exercising the
// full page-drain loop and the per-page batch resolution.

import Foundation
import os
import Testing

@testable import Playhead

@Suite("FinalPassLaunchSweepPagination")
struct FinalPassLaunchSweepPaginationTests {

    /// Thread-safe accumulator for the episodeIds the batch resolver is asked
    /// about, plus the number of times it was called (one call per page).
    private final class ResolverProbe: @unchecked Sendable {
        private let lock = OSAllocatedUnfairLock(initialState: State())
        private struct State {
            var seen: Set<String> = []
            var callCount = 0
            var maxBatchSize = 0
        }
        func record(_ ids: [String]) {
            lock.withLock {
                $0.seen.formUnion(ids)
                $0.callCount += 1
                $0.maxBatchSize = max($0.maxBatchSize, ids.count)
            }
        }
        var seen: Set<String> { lock.withLock { $0.seen } }
        var callCount: Int { lock.withLock { $0.callCount } }
        var maxBatchSize: Int { lock.withLock { $0.maxBatchSize } }
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            finalPassCoverageEndTime: nil
        )
    }

    /// A runner instance that is required by the sweep signature but is never
    /// invoked in this test (the empty download cache short-circuits every
    /// asset before the runner is reached).
    private func makeUnusedRunner(store: AnalysisStore) -> FinalPassRetranscriptionRunner {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10 * 1024 * 1024 * 1024,
            capturedAt: .now
        )
        return FinalPassRetranscriptionRunner(
            store: store,
            speechService: SpeechService(recognizer: StubSpeechRecognizer()),
            audioProvider: StubAnalysisAudioProvider(),
            capabilitySnapshotProvider: { snapshot },
            batteryLevelProvider: { 0.9 },
            chargeStateProvider: { true },
            confidenceFloor: 0.5,
            modelVersion: "test-final-v1"
        )
    }

    @Test("launch sweep pages through every asset when the store exceeds one page")
    func testSweepVisitsEveryAssetAcrossPages() async throws {
        // Seed strictly more than one page. The sweep's page size is 200
        // (playhead-b0uf); 205 crosses exactly one page boundary and leaves a
        // partial final page (200 + 5), which is the classic off-by-one /
        // dropped-page trap. If this number ever needs to change, keep it
        // comfortably above the sweep's `pageSize`.
        let assetCount = 205
        let store = try await makeTestStore()
        var expectedEpisodeIds: Set<String> = []
        expectedEpisodeIds.reserveCapacity(assetCount)
        for i in 0..<assetCount {
            let id = String(format: "asset-%04d", i)
            try await store.insertAsset(makeAsset(id: id))
            expectedEpisodeIds.insert("ep-\(id)")
        }

        // Empty cache directory → cachedFileURL(for:) is nil for every asset,
        // so the runner is never invoked; the sweep still fetches and batch-
        // resolves every page.
        let cacheDir = try makeTempDir(prefix: "FinalPassSweepCache")
        let downloadManager = DownloadManager(cacheDirectory: cacheDir)
        let runner = makeUnusedRunner(store: store)

        let probe = ResolverProbe()
        let batchResolver: @Sendable ([String]) async -> [String: String] = { ids in
            probe.record(ids)
            return [:]
        }

        _ = await PlayheadRuntime.runFinalPassBackfillForAllAssetsAtLaunch(
            runner: runner,
            analysisStore: store,
            downloadManager: downloadManager,
            podcastIdResolver: nil,
            podcastIdBatchResolver: batchResolver
        )

        // Coverage: every seeded asset was visited across the pages — no
        // dropped page, no off-by-one at the boundary.
        #expect(probe.seen == expectedEpisodeIds)
        #expect(probe.seen.count == assetCount)
        // Pagination actually happened: the resolver was called more than once
        // (a regression to a single whole-table fetch would call it exactly
        // once with all rows), and no single page exceeded the whole set.
        #expect(probe.callCount >= 2)
        #expect(probe.maxBatchSize < assetCount)
    }
}
