// SkipOrchestratorEvidenceCatalogTests.swift
// playhead-vjxc: verifies the evidence catalog plumbing — when a catalog
// is pushed to the orchestrator, banner emissions slice the entries that
// overlap each window's snapped span. Empty / mismatched catalogs degrade
// to an empty `evidenceCatalogEntries` array on the banner.

import Foundation
import Testing
@testable import Playhead

@Suite("SkipOrchestrator - Evidence Catalog Plumbing")
struct SkipOrchestratorEvidenceCatalogTests {

    private func entry(
        ref: Int,
        category: EvidenceCategory,
        text: String,
        start: Double,
        end: Double
    ) -> EvidenceEntry {
        EvidenceEntry(
            evidenceRef: ref,
            category: category,
            matchedText: text,
            normalizedText: text.lowercased(),
            atomOrdinal: ref,
            startTime: start,
            endTime: end,
            count: 1,
            firstTime: start,
            lastTime: end
        )
    }

    private func makeCatalog(
        assetId: String,
        entries: [EvidenceEntry]
    ) -> EvidenceCatalog {
        EvidenceCatalog(
            analysisAssetId: assetId,
            transcriptVersion: "v-test",
            entries: entries
        )
    }

    @Test("Banner carries catalog entries that overlap the skipped window")
    func bannerCarriesOverlappingEntries() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Catalog: one entry inside [60, 120], one entry outside.
        let inside = entry(
            ref: 0, category: .disclosurePhrase,
            text: "sponsored by", start: 75, end: 76
        )
        let outside = entry(
            ref: 1, category: .url,
            text: "unrelated.com", start: 500, end: 501
        )
        let catalog = makeCatalog(
            assetId: "asset-1",
            entries: [inside, outside]
        )
        await orchestrator.setEvidenceCatalog(catalog)

        // Subscribe before the inject so we don't miss the emission.
        let stream = await orchestrator.bannerItemStream()
        let task = Task<AdSkipBannerItem?, Never> {
            for await item in stream { return item }
            return nil
        }

        await orchestrator.injectUserMarkedAd(
            start: 60, end: 120, analysisAssetId: "asset-1"
        )

        try await Task.sleep(for: .milliseconds(120))
        task.cancel()
        let banner = await task.value

        #expect(banner != nil)
        let entries = banner?.evidenceCatalogEntries ?? []
        #expect(entries.count == 1, "Only the overlapping entry should be carried; got \(entries.count)")
        #expect(entries.first?.evidenceRef == 0)
    }

    @Test("Empty catalog produces empty evidence list (graceful default)")
    func emptyCatalogProducesEmptyList() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // No catalog pushed at all — the banner must still emit cleanly.
        let stream = await orchestrator.bannerItemStream()
        let task = Task<AdSkipBannerItem?, Never> {
            for await item in stream { return item }
            return nil
        }

        await orchestrator.injectUserMarkedAd(
            start: 60, end: 120, analysisAssetId: "asset-1"
        )

        try await Task.sleep(for: .milliseconds(120))
        task.cancel()
        let banner = await task.value

        #expect(banner != nil)
        #expect(banner?.evidenceCatalogEntries.isEmpty == true)
    }

    @Test("Mismatched-asset catalog is dropped silently")
    func mismatchedAssetCatalogDropped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Push a catalog for the wrong asset.
        let wrongCatalog = makeCatalog(
            assetId: "different-asset",
            entries: [
                entry(ref: 0, category: .url, text: "x.com", start: 75, end: 76)
            ]
        )
        await orchestrator.setEvidenceCatalog(wrongCatalog)

        let stream = await orchestrator.bannerItemStream()
        let task = Task<AdSkipBannerItem?, Never> {
            for await item in stream { return item }
            return nil
        }
        await orchestrator.injectUserMarkedAd(
            start: 60, end: 120, analysisAssetId: "asset-1"
        )

        try await Task.sleep(for: .milliseconds(120))
        task.cancel()
        let banner = await task.value

        #expect(banner != nil)
        #expect(banner?.evidenceCatalogEntries.isEmpty == true,
                "Mismatched-asset catalog must be dropped, leaving banner with no evidence")
    }

    @Test("Catalog cleared on endEpisode")
    func catalogClearedOnEndEpisode() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }

        // Episode 1: push catalog, then end without emitting.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        let cat1 = makeCatalog(
            assetId: "asset-1",
            entries: [
                entry(ref: 0, category: .url, text: "leaked.com", start: 75, end: 76)
            ]
        )
        await orchestrator.setEvidenceCatalog(cat1)
        await orchestrator.endEpisode()

        // Episode 2: same asset id (worst-case), do NOT push catalog. The
        // previous one must NOT leak into the new banner.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let stream = await orchestrator.bannerItemStream()
        let task = Task<AdSkipBannerItem?, Never> {
            for await item in stream { return item }
            return nil
        }
        await orchestrator.injectUserMarkedAd(
            start: 60, end: 120, analysisAssetId: "asset-1"
        )

        try await Task.sleep(for: .milliseconds(120))
        task.cancel()
        let banner = await task.value

        #expect(banner != nil)
        #expect(banner?.evidenceCatalogEntries.isEmpty == true,
                "Catalog from prior episode must not leak into the new one")
    }

    @Test("Boundary-touching entries are included")
    func boundaryTouchingEntriesIncluded() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Entry exactly at the start boundary.
        let touchingStart = entry(
            ref: 0, category: .disclosurePhrase,
            text: "brought to you by", start: 60, end: 60.5
        )
        // Entry exactly at the end boundary.
        let touchingEnd = entry(
            ref: 1, category: .url,
            text: "x.com", start: 119.5, end: 120
        )
        // Entry strictly outside.
        let outside = entry(
            ref: 2, category: .url,
            text: "y.com", start: 130, end: 131
        )
        await orchestrator.setEvidenceCatalog(
            makeCatalog(assetId: "asset-1", entries: [touchingStart, touchingEnd, outside])
        )

        let stream = await orchestrator.bannerItemStream()
        let task = Task<AdSkipBannerItem?, Never> {
            for await item in stream { return item }
            return nil
        }
        await orchestrator.injectUserMarkedAd(
            start: 60, end: 120, analysisAssetId: "asset-1"
        )

        try await Task.sleep(for: .milliseconds(120))
        task.cancel()
        let banner = await task.value

        #expect(banner != nil)
        let refs = (banner?.evidenceCatalogEntries ?? []).map(\.evidenceRef).sorted()
        #expect(refs == [0, 1], "Boundary-touching entries must be included; got \(refs)")
    }
}
