// AdDetectionServiceRevalidationTests.swift
// playhead-zx6i — Tests for the B4 revalidation entry point on
// `AdDetectionService`. Two surfaces under test:
//
//   1. `revalidateFromFeatures(...)` fetches persisted chunks from the
//      store and produces the same AdWindow set as a fresh
//      `runBackfill(chunks:...)` over the same chunks. This is the
//      "false_ready_rate parity" acceptance criterion in spec form —
//      the persisted-rows path must produce decisions identical to the
//      live-pass path.
//
//   2. The success-stamp at the end of `runBackfill` is gated on the
//      `PreAnalysisConfig.b4RevalidationFromFeaturesEnabled` flag.
//      Flag OFF must leave the `RevalidationStateStore` untouched.

import Foundation
import Testing
@testable import Playhead

// Serialized because the success-stamp tests mutate the SHARED
// `.standard` UserDefaults `PreAnalysisConfig` blob, and the
// producer-side stamp-write inside `AdDetectionService.runBackfill`
// re-reads that blob LIVE via `PreAnalysisConfig.load()` on every
// successful call (NOT a snapshot captured at `init`; see the R1
// doc-audit comment block in `runBackfill` and §3.5.1 of the design
// doc — only the 2hpn `scopedMusicBedGeneralization` flag still
// snapshots at init). Running the flag-ON and flag-OFF stamp tests
// in parallel would let one test's `config.save(true)` straddle the
// other test's `runBackfill` stamp-write, flipping which branch the
// live read sees and contaminating both assertions. `.serialized`
// forces the two flag-gating tests to run in source order; the
// parity test stays correct either way because it constructs its
// services with `fmBackfillMode: .off` and only inspects ad-window
// output, not the flag.
@Suite("AdDetectionService B4 revalidation", .serialized)
struct AdDetectionServiceRevalidationTests {

    // MARK: - Episode fixture

    private struct Episode: Sendable {
        let assetId: String
        let podcastId: String
        let chunks: [TranscriptChunk]
        let duration: Double
    }

    private func makeEpisode(assetId: String) -> Episode {
        let texts = [
            "Welcome back to the show, listeners. Today's deep dive is on something we've been promising for a while.",
            "But first, this episode is brought to you by Squarespace. Use the promo code SHOW for 20 percent off your first website at squarespace dot com slash show.",
            "And we are also supported by BetterHelp. Visit betterhelp dot com slash podcast to get matched with a therapist.",
            "Now back to our regularly scheduled programming. Our guest today has spent fifteen years studying the topic.",
            "Thanks for listening. We'll see you next week."
        ]
        let chunks = texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)-\(assetId)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
        return Episode(
            assetId: assetId,
            podcastId: "podcast-zx6i",
            chunks: chunks,
            duration: 150
        )
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

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .off
            )
        )
    }

    // MARK: - Tests

    @Test("revalidateFromFeatures reads persisted chunks and produces the same AdWindow set as a fresh runBackfill")
    func revalidateProducesSameWindowsAsFreshRunBackfill() async throws {
        // Baseline: fresh `runBackfill` with chunks passed directly.
        let baselineStore = try await makeTestStore()
        let episode = makeEpisode(assetId: "asset-parity")
        try await baselineStore.insertAsset(makeAsset(id: episode.assetId))
        let baselineService = makeService(store: baselineStore)
        try await baselineService.runBackfill(
            chunks: episode.chunks,
            analysisAssetId: episode.assetId,
            podcastId: episode.podcastId,
            episodeDuration: episode.duration
        )
        let baselineWindows = try await baselineStore.fetchAdWindows(assetId: episode.assetId)

        // Revalidation: seed the chunks, then call
        // `revalidateFromFeatures` (no chunks parameter). The method
        // must fetch them from the store internally and produce the
        // identical decision set.
        let revalidationStore = try await makeTestStore()
        try await revalidationStore.insertAsset(makeAsset(id: episode.assetId))
        try await revalidationStore.insertTranscriptChunks(episode.chunks)
        let revalidationService = makeService(store: revalidationStore)
        try await revalidationService.revalidateFromFeatures(
            analysisAssetId: episode.assetId,
            podcastId: episode.podcastId,
            episodeDuration: episode.duration
        )
        let revalidationWindows = try await revalidationStore.fetchAdWindows(assetId: episode.assetId)

        // Window-count parity: the false_ready_rate cannot have
        // regressed if the decoded span counts match. Stronger
        // structural assertions (start/end/decisionState equality)
        // would couple this test to per-window IDs (which are UUIDs,
        // not reproducible across runs), so we assert on the
        // observable shape: count + decision-state distribution.
        #expect(baselineWindows.count == revalidationWindows.count)

        let baselineDecisionStates = baselineWindows.map(\.decisionState).sorted()
        let revalidationDecisionStates = revalidationWindows.map(\.decisionState).sorted()
        #expect(baselineDecisionStates == revalidationDecisionStates)

        // Extent parity. Round to the nearest second so a sub-second
        // jitter from floating-point ordering doesn't flake the test
        // while still catching any real boundary movement.
        let baselineStarts = baselineWindows.map { $0.startTime.rounded() }.sorted()
        let revalidationStarts = revalidationWindows.map { $0.startTime.rounded() }.sorted()
        #expect(baselineStarts == revalidationStarts, "revalidation must produce window start times identical to a fresh runBackfill")

        let baselineEnds = baselineWindows.map { $0.endTime.rounded() }.sorted()
        let revalidationEnds = revalidationWindows.map { $0.endTime.rounded() }.sorted()
        #expect(baselineEnds == revalidationEnds, "revalidation must produce window end times identical to a fresh runBackfill")
    }

    @Test("revalidateFromFeatures with no persisted chunks is a no-op")
    func revalidateWithNoChunksIsNoop() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-empty"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store)
        // No chunks seeded. The revalidation path must not throw —
        // it should log and return.
        try await service.revalidateFromFeatures(
            analysisAssetId: assetId,
            podcastId: "podcast-zx6i",
            episodeDuration: 150
        )
        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows.isEmpty)
    }

    // MARK: - Success stamp (flag gating)
    //
    // These tests construct an `AdDetectionService`, run a backfill,
    // and inspect the `RevalidationStateStore` to assert that the
    // stamp is written iff the flag is ON. The zx6i flag is NOT
    // captured at service init — `runBackfill` calls
    // `PreAnalysisConfig.load()` LIVE at the stamp-write site (R1
    // doc-audit fix; see the R1 commentary inside `runBackfill` and
    // §3.5.1 of the design doc). The tests therefore flip the
    // persisted config BEFORE calling `runBackfill` so the live read
    // observes the desired value, then restore the prior value on
    // exit. The store reads/writes go through `.standard`
    // UserDefaults in production, so we use a unique asset id per
    // test and `clear` on entry/exit to guarantee isolation without
    // holding a lock on the global suite.

    @Test("flag ON: a successful runBackfill stamps PipelineVersions.current()")
    func flagOnStampsCurrentVersions() async throws {
        let assetId = "asset-stamp-flag-on-\(UUID().uuidString)"
        defer { RevalidationStateStore.clear(forAsset: assetId) }
        RevalidationStateStore.clear(forAsset: assetId)

        // Flip the persisted flag ON for the duration of this test.
        var config = PreAnalysisConfig.load()
        let restoreValue = config.b4RevalidationFromFeaturesEnabled
        config.b4RevalidationFromFeaturesEnabled = true
        config.save()
        defer {
            var restore = PreAnalysisConfig.load()
            restore.b4RevalidationFromFeaturesEnabled = restoreValue
            restore.save()
        }

        let store = try await makeTestStore()
        let episode = makeEpisode(assetId: assetId)
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store)

        try await service.runBackfill(
            chunks: episode.chunks,
            analysisAssetId: assetId,
            podcastId: episode.podcastId,
            episodeDuration: episode.duration
        )

        let stamped = RevalidationStateStore.loadCompletedVersions(forAsset: assetId)
        #expect(stamped == PipelineVersions.current(), "flag ON must stamp the current pipeline versions at end of successful runBackfill")
    }

    @Test("flag OFF: a successful runBackfill leaves the RevalidationStateStore untouched")
    func flagOffDoesNotStamp() async throws {
        let assetId = "asset-stamp-flag-off-\(UUID().uuidString)"
        defer { RevalidationStateStore.clear(forAsset: assetId) }
        RevalidationStateStore.clear(forAsset: assetId)

        // Flip the persisted flag OFF for the duration of this test.
        var config = PreAnalysisConfig.load()
        let restoreValue = config.b4RevalidationFromFeaturesEnabled
        config.b4RevalidationFromFeaturesEnabled = false
        config.save()
        defer {
            var restore = PreAnalysisConfig.load()
            restore.b4RevalidationFromFeaturesEnabled = restoreValue
            restore.save()
        }

        let store = try await makeTestStore()
        let episode = makeEpisode(assetId: assetId)
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store)

        try await service.runBackfill(
            chunks: episode.chunks,
            analysisAssetId: assetId,
            podcastId: episode.podcastId,
            episodeDuration: episode.duration
        )

        let stamped = RevalidationStateStore.loadCompletedVersions(forAsset: assetId)
        #expect(stamped == nil, "flag OFF must NOT stamp — pre-zx6i behaviour byte-identical")
    }

    /// R6 audit pin: the design doc §3.5.1 truth-table row 3 (flag ON
    /// at `run(_:)` top, flag OFF at stamp-write) claims that a PRIOR
    /// stamp written during a flag-ON period remains in place when the
    /// flag flips OFF before stamp-write — `recordCompleted` only
    /// writes, never clears, and there is no flag-OFF eviction. The
    /// claim is structurally guaranteed by the code (no `clear` site
    /// in production; the stamp-write gate is the only producer), but
    /// the prior review rounds had no test pinning this behaviour. A
    /// future change that added a flag-OFF eviction (e.g. "clear stamp
    /// when flag flips" as a tidy-up gesture) would silently invalidate
    /// the row-3 invariant and break ON→OFF→ON resumption: a session
    /// that flipped OFF then back ON would have to redo a full
    /// analysis rather than picking up the cached stamp. This test
    /// pins the no-eviction behaviour so any such regression
    /// surfaces immediately.
    @Test("prior stamp survives a flag-OFF runBackfill (truth-table row 3 no-eviction)")
    func priorStampSurvivesFlagOffRunBackfill() async throws {
        let assetId = "asset-stamp-survives-off-\(UUID().uuidString)"
        defer { RevalidationStateStore.clear(forAsset: assetId) }
        RevalidationStateStore.clear(forAsset: assetId)

        // Pre-seed a stamp as if a prior flag-ON `runBackfill` had
        // completed. We bypass the service here and write directly to
        // the store — the production producer is the only call site,
        // so we synthesise its output to set up the row-3 scenario.
        let priorStamp = PipelineVersions(
            modelVersion: "prior-detector-v1",
            policyVersion: "prior-policy-v1",
            featureSchemaVersion: 1
        )
        RevalidationStateStore.recordCompleted(versions: priorStamp, forAsset: assetId)
        #expect(RevalidationStateStore.loadCompletedVersions(forAsset: assetId) == priorStamp)

        // Flip the persisted flag OFF, then run a successful backfill.
        // The stamp-write gate inside `runBackfill` re-reads the flag
        // live and sees OFF → no NEW stamp. The doc claim is that the
        // PRIOR stamp also remains in place (no eviction).
        var config = PreAnalysisConfig.load()
        let restoreValue = config.b4RevalidationFromFeaturesEnabled
        config.b4RevalidationFromFeaturesEnabled = false
        config.save()
        defer {
            var restore = PreAnalysisConfig.load()
            restore.b4RevalidationFromFeaturesEnabled = restoreValue
            restore.save()
        }

        let store = try await makeTestStore()
        let episode = makeEpisode(assetId: assetId)
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store)

        try await service.runBackfill(
            chunks: episode.chunks,
            analysisAssetId: assetId,
            podcastId: episode.podcastId,
            episodeDuration: episode.duration
        )

        // No NEW stamp (gate read OFF) AND prior stamp still present
        // (no eviction). The combination pins row 3's "PRIOR stamp …
        // remains in place unchanged" wording.
        let stamped = RevalidationStateStore.loadCompletedVersions(forAsset: assetId)
        #expect(stamped == priorStamp, "flag-OFF runBackfill must NOT clear an existing stamp — the row-3 truth-table claim depends on no-eviction semantics")
    }
}
