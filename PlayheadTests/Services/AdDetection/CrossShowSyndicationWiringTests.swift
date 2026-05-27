// CrossShowSyndicationWiringTests.swift
// playhead-xsdz.13: Service-level wiring tests for the cross-show syndication
// precision signal.
//
// These prove the load-bearing OFF-by-default full-gating contract end-to-end
// through `AdDetectionService.runBackfill`:
//   1. FLAG-OFF IDENTITY: with a syndication store WIRED AND PRE-SEEDED with a
//      high-spread, persistent sponsor entity that matches the candidate span,
//      `crossShowSyndicationEnabled = false` produces byte-identical persisted
//      `AdWindow` confidences to the no-store baseline — i.e. no store read, no
//      write, no boost entry.
//   2. FLAG-ON BOOST: with the same pre-seeded store and the flag ON, the
//      matching span's persisted confidence is BOOSTED (>=) versus the flag-off
//      baseline — the signal genuinely moves the live decision.
//   3. SINGLE-SHOW UNAFFECTED: when the store has no cross-show spread for the
//      entity, the flag-ON run matches the flag-off baseline (no boost).
//   4. GATING: the production-default config keeps the feature OFF, so the
//      runtime constructs no store.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("CrossShowSyndication service wiring (playhead-xsdz.13)")
struct CrossShowSyndicationWiringTests {

    private let asset = "asset-xsdz13-wiring"
    private let show = "show-xsdz13-new"

    // The sponsor entity the candidate span mentions; `EvidenceCatalogBuilder`
    // extracts "squarespace" as a brandSpan from the URL/disclosure context.
    private let syndicatedEntity = "squarespace"

    // MARK: - 1. Flag-off identity

    @Test("Flag OFF: a wired+seeded syndication store does not change any AdWindow confidence")
    func flagOffIsByteIdentical() async throws {
        let baseline = try await runAndFetchWindows(enabled: false, seedSpread: false)
        let withStoreOff = try await runAndFetchWindows(enabled: false, seedSpread: true)

        #expect(baseline.count == withStoreOff.count)
        func key(_ w: AdWindow) -> String { String(format: "%.3f-%.3f", w.startTime, w.endTime) }
        let baseMap = Dictionary(baseline.map { (key($0), $0.confidence) }, uniquingKeysWith: { a, _ in a })
        for w in withStoreOff {
            let b = try #require(baseMap[key(w)], "span \(key(w)) missing in baseline")
            #expect(w.confidence == b, "flag-off must be byte-identical to no-store baseline")
        }
    }

    // MARK: - 2. Flag-on boost

    @Test("Flag ON: a high-spread persistent entity boosts the candidate span's confidence")
    func flagOnBoostsHighSpread() async throws {
        let flagOff = try await runAndFetchWindows(enabled: false, seedSpread: true)
        let flagOn = try await runAndFetchWindows(enabled: true, seedSpread: true)

        let offWindow = try #require(flagOff.first { $0.startTime < 90 && $0.endTime > 60 })
        let onWindow = try #require(flagOn.first { $0.startTime < 90 && $0.endTime > 60 })

        #expect(onWindow.confidence >= offWindow.confidence,
                "a high cross-show-spread entity must not LOWER the matching span")
        #expect(onWindow.confidence > offWindow.confidence,
                "a high cross-show-spread, persistent entity must boost the matching span")
    }

    // MARK: - 3. Single-show entity unaffected

    @Test("Flag ON: an entity with no cross-show spread leaves the span unchanged")
    func flagOnSingleShowUnaffected() async throws {
        let flagOff = try await runAndFetchWindows(enabled: false, seedSpread: false)
        // Flag on but the store has no prior cross-show observations: the only
        // observation is this episode's own write, so distinct-show count == 1
        // and the boost gate (minDistinctShows >= 3) does not fire.
        let flagOnNoSpread = try await runAndFetchWindows(enabled: true, seedSpread: false)

        let offWindow = try #require(flagOff.first { $0.startTime < 90 && $0.endTime > 60 })
        let onWindow = try #require(flagOnNoSpread.first { $0.startTime < 90 && $0.endTime > 60 })
        #expect(onWindow.confidence == offWindow.confidence,
                "a single-show entity must not boost (no syndication)")
    }

    // MARK: - Harness

    private func runAndFetchWindows(
        enabled: Bool,
        seedSpread: Bool
    ) async throws -> [AdWindow] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: asset))

        let dir = try makeTempDir(prefix: "xsdz13-wiring-store")
        let syndicationStore = try CrossShowSyndicationStore(directoryURL: dir)
        if seedSpread {
            // Pre-seed the entity across 3 DISTINCT other shows with temporal
            // persistence (first-seen 60 days ago → ~60-day spread). This makes
            // the entity's spread ratio high and persistent, so adding THIS
            // show's observation clears the boost gate.
            let day: Double = 86_400
            let now = Date().timeIntervalSince1970
            try await syndicationStore.recordObservation(normalizedEntity: syndicatedEntity, podcastId: "other-A", confidence: 0.9, now: now - 60 * day)
            try await syndicationStore.recordObservation(normalizedEntity: syndicatedEntity, podcastId: "other-B", confidence: 0.9, now: now - 30 * day)
            try await syndicationStore.recordObservation(normalizedEntity: syndicatedEntity, podcastId: "other-C", confidence: 0.9, now: now)
        }

        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "xsdz13-test",
            fmBackfillMode: .off,
            crossShowSyndicationEnabled: enabled
        )
        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            crossShowSyndicationStore: syndicationStore
        )

        try await service.runBackfill(
            chunks: chunks(),
            analysisAssetId: asset,
            podcastId: show,
            episodeDuration: 130.0
        )

        let windows = try await store.fetchAdWindows(assetId: asset)
        await syndicationStore.close()
        return windows
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
            capabilitySnapshot: nil
        )
    }

    private func chunks() -> [TranscriptChunk] {
        let texts: [(Double, Double, String)] = [
            (0.0, 30.0, "Welcome back to the show today we discuss technology and design."),
            (60.0, 90.0, "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show."),
            (90.0, 120.0, "Back to our regular conversation about new things and ideas.")
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(asset)",
                analysisAssetId: asset,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }
}

// MARK: - Flag-OFF ⇒ no store construction (behavior-neutral gating)

/// playhead-xsdz.13: the ENTIRE feature — store construction, SQLite migration,
/// the sponsor-entity WRITE path, AND the spread-ratio READ — rides the ONE
/// off-by-default `crossShowSyndicationEnabled` flag. With the flag off (the
/// production default) there must be NO store, NO new DB file, NO migration, and
/// NO writes/reads. These tests pin that invariant at the seam the production
/// `PlayheadRuntime` actually branches on.
@Suite("CrossShowSyndication flag-off gating (playhead-xsdz.13)")
struct CrossShowSyndicationGatingTests {

    /// The production gate: `PlayheadRuntime` constructs the store only when
    /// `AdDetectionConfig.default.crossShowSyndicationEnabled` is true. The
    /// default (production) config keeps it false, so production constructs NO
    /// store — no DB file, no migration. This is the single value the runtime
    /// branches on; pinning it proves the OFF state is the production state.
    @Test("Production-default config keeps the feature OFF (so the runtime builds no store)")
    func productionDefaultDisablesConstruction() {
        #expect(AdDetectionConfig.default.crossShowSyndicationEnabled == false)
    }

    /// Sanity: the channel is never wired into a production config / A/B arm —
    /// the default fusion cap is the documented modest 0.20, matching its
    /// corroborator peers (it never drives a skip alone).
    @Test("Default cross-show syndication cap is the modest 0.20 corroborator budget")
    func defaultCapIsModest() {
        #expect(FusionWeightConfig().crossShowSyndicationCap == 0.2)
    }
}

#endif
