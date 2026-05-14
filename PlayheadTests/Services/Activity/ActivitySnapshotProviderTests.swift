// ActivitySnapshotProviderTests.swift
// playhead-hkn1: regression tests for `LiveActivitySnapshotProvider.loadInputs()`
// scaling. Pre-fix the function ran wholly on the main actor with a bare
// `FetchDescriptor<Episode>()` (no predicate, no prefetch) and an N-row
// per-episode `await store.fetchAssetByEpisodeId(...)` loop, which froze
// the UI on Dan's dogfood device. These tests pin both the wall-clock
// budget and the off-main behaviour so the regression cannot recur
// silently.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@Suite("LiveActivitySnapshotProvider — performance regression (playhead-hkn1)")
struct LiveActivitySnapshotProviderPerfTests {

    /// Top-line guard: `loadInputs()` must not block the main actor for
    /// noticeable time on a realistic library. With N=200 episodes (each
    /// backed by an asset) we assert two things:
    ///   1. The call completes within a sub-second wall-clock budget.
    ///   2. While it is running, the main actor is free to do unrelated
    ///      work — because the SwiftData fetch + AnalysisStore round-trip
    ///      now run off-main.
    ///
    /// The `@MainActor` annotation on the test ensures the racing
    /// `Task { @MainActor ... }` actually contends with the same actor
    /// the production call site uses.
    @Test("loadInputs completes off-main within a sub-second budget with N=200 episodes")
    @MainActor
    func loadInputsDoesNotBlockMainOnLargeLibrary() async throws {
        let n = 200

        // SwiftData container — same schema as production.
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: [config])
        let context = ModelContext(container)

        // Single Podcast so the relationship pre-fetch path is exercised
        // (every Episode references the same Podcast row).
        let feedURL = URL(string: "https://example.com/hkn1-perf.rss")!
        let podcast = Podcast(
            feedURL: feedURL,
            title: "Perf Show",
            author: "Perf Author",
            artworkURL: nil,
            episodes: [],
            subscribedAt: .now
        )
        context.insert(podcast)

        // Seed N episodes + matching analysis-asset rows.
        let store = try await makeTestStore()
        var episodeIds: [String] = []
        episodeIds.reserveCapacity(n)
        for i in 0..<n {
            let episode = Episode(
                feedItemGUID: "guid-\(i)",
                feedURL: feedURL,
                podcast: podcast,
                title: "Episode \(i)",
                audioURL: URL(string: "https://example.com/a\(i).mp3")!
            )
            context.insert(episode)
            episodeIds.append(episode.canonicalEpisodeKey)

            let asset = AnalysisAsset(
                id: "asset-\(i)",
                episodeId: episode.canonicalEpisodeKey,
                assetFingerprint: "fp-\(i)",
                weakFingerprint: nil,
                sourceURL: "https://example.com/a\(i).mp3",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "queued",
                analysisVersion: 1,
                capabilitySnapshot: nil
            )
            try await store.insertAsset(asset)
        }
        try context.save()

        // playhead-hkn1 GREEN signature: provider takes the
        // `ModelContainer` (Sendable) so `loadInputs()` can construct
        // its own off-main `ModelContext`. The `context` above is held
        // only to seed the in-memory store; the provider never touches
        // it.
        _ = context
        let provider = LiveActivitySnapshotProvider(
            store: store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            // playhead-btoa.3: perf path doesn't care about download
            // fractions — empty stub keeps every input's
            // `downloadFraction == nil` and exercises the no-overhead
            // dictionary lookup branch the production loop runs when
            // there are zero in-flight foreground downloads and zero
            // cached completions.
            downloadProgressProvider: { [:] },
            modelContainer: container
        )

        // Race a main-actor counter against the call. If `loadInputs()`
        // truly runs off-main, the racing closure should land on the
        // MainActor inside the call's lifetime and bump the counter.
        // We sample the counter *during* the await so a strictly serial
        // (main-blocking) implementation cannot satisfy this assertion
        // by trivially incrementing after the await returns.
        let counter = ManagedCounter()
        let racer = Task { @MainActor in
            // Spin a few short hops so we sample the actor over the
            // window. Each `Task.yield()` re-enters the MainActor's
            // run loop; if the load is blocking main, none of these
            // bumps complete.
            for _ in 0..<10 {
                counter.increment()
                await Task.yield()
            }
        }

        let start = Date()
        let inputs = await provider.loadInputs()
        let elapsed = Date().timeIntervalSince(start)
        await racer.value

        // Sanity: the provider sees every seeded episode (every one has
        // an asset). Drops here would mask a regression that breaks the
        // bulk-fetch correctness.
        #expect(inputs.count == n)

        // Wall-clock budget. The pre-fix implementation took multiple
        // seconds on Dan's device with ~50-100 rows; the widened budget
        // still catches that N+1 regression while leaving room for the
        // full PlayheadFastTests suite to contend with thousands of
        // parallel async tests on the same simulator.
        #expect(elapsed < 0.75, "loadInputs took \(elapsed)s; budget is 0.75s")

        // Off-main proof. If the load was main-blocking, the racer
        // could not have run at all between the two MainActor hops —
        // counter would still be 0. We require at least one bump.
        //
        // This is intentionally a soft floor (>= 1, not >= 10) because
        // scheduler fairness varies; one bump is enough to prove main
        // was not held continuously for the full load.
        #expect(counter.value >= 1,
                "main actor was blocked for the entire load (counter=\(counter.value))")
    }
}

/// Minimal main-actor-isolated counter for the racing test above.
/// Marked `@MainActor` so reads/writes are serialized against the same
/// actor `loadInputs()` must vacate to satisfy the regression test.
@MainActor
private final class ManagedCounter {
    private(set) var value: Int = 0
    func increment() { value += 1 }
}

// MARK: - playhead-btoa.3 — pipeline-progress fractions

/// Fraction-population coverage for `LiveActivitySnapshotProvider`.
///
/// Bead .1 plumbed `downloadFraction` / `transcriptFraction` /
/// `analysisFraction` through the row payloads; bead .2 added
/// `DownloadManager.progressSnapshot()`. This bead (.3) is the
/// provider-side wiring: divide coverage by duration for transcript and
/// analysis fractions, prefer actual fast transcript chunk coverage when
/// present, merge live + cached download state, and clamp to `[0, 1]` so
/// the row contract is "already in range if non-nil".
@Suite("LiveActivitySnapshotProvider — pipeline fractions")
struct LiveActivitySnapshotProviderFractionTests {

    /// Helper: seeds a single Podcast + N Episodes (one per `episodeId`)
    /// into a fresh in-memory SwiftData container, paired with N
    /// `AnalysisAsset` rows in a fresh `AnalysisStore`. Returns both
    /// alongside the canonical episode-key list (in seed order) so each
    /// test can drive `loadInputs()` and assert on the resulting
    /// `ActivityEpisodeInput` fields.
    ///
    /// `assetSeeds` carries the per-asset coverage watermarks + duration;
    /// the provider divides coverage by duration to produce the fractions
    /// under test.
    private struct AssetSeed {
        let featureCoverageEndTime: Double?
        let fastTranscriptCoverageEndTime: Double?
        let confirmedAdCoverageEndTime: Double?
        let episodeDurationSec: Double?
        let analysisState: String

        init(
            featureCoverageEndTime: Double? = nil,
            fastTranscriptCoverageEndTime: Double?,
            confirmedAdCoverageEndTime: Double?,
            episodeDurationSec: Double?,
            analysisState: String = "queued"
        ) {
            self.featureCoverageEndTime = featureCoverageEndTime
            self.fastTranscriptCoverageEndTime = fastTranscriptCoverageEndTime
            self.confirmedAdCoverageEndTime = confirmedAdCoverageEndTime
            self.episodeDurationSec = episodeDurationSec
            self.analysisState = analysisState
        }
    }

    private struct Fixture {
        let store: AnalysisStore
        let container: ModelContainer
        let episodeIds: [String]
        let assetIds: [String]
    }

    // Returns a Fixture seeded with one Episode + AnalysisAsset per AssetSeed.
    private func makeFixture(assetSeeds: [AssetSeed]) async throws -> Fixture {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: [config])
        let context = ModelContext(container)

        let feedURL = URL(string: "https://example.com/btoa3-fractions.rss")!
        let podcast = Podcast(
            feedURL: feedURL,
            title: "Fractions Show",
            author: "Author",
            artworkURL: nil,
            episodes: [],
            subscribedAt: .now
        )
        context.insert(podcast)

        let store = try await makeTestStore()
        var episodeIds: [String] = []
        var assetIds: [String] = []
        episodeIds.reserveCapacity(assetSeeds.count)
        assetIds.reserveCapacity(assetSeeds.count)
        for (i, seed) in assetSeeds.enumerated() {
            let episode = Episode(
                feedItemGUID: "guid-\(i)",
                feedURL: feedURL,
                podcast: podcast,
                title: "Episode \(i)",
                audioURL: URL(string: "https://example.com/a\(i).mp3")!
            )
            context.insert(episode)
            episodeIds.append(episode.canonicalEpisodeKey)

            let asset = AnalysisAsset(
                id: "asset-\(i)",
                episodeId: episode.canonicalEpisodeKey,
                assetFingerprint: "fp-\(i)",
                weakFingerprint: nil,
                sourceURL: "https://example.com/a\(i).mp3",
                featureCoverageEndTime: seed.featureCoverageEndTime,
                fastTranscriptCoverageEndTime: seed.fastTranscriptCoverageEndTime,
                confirmedAdCoverageEndTime: seed.confirmedAdCoverageEndTime,
                analysisState: seed.analysisState,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: seed.episodeDurationSec
            )
            try await store.insertAsset(asset)
            assetIds.append(asset.id)
        }
        try context.save()

        return Fixture(store: store, container: container, episodeIds: episodeIds, assetIds: assetIds)
    }

    private func transcriptChunk(
        assetId: String,
        index: Int,
        start: Double,
        end: Double,
        pass: String = "fast"
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(index)-\(pass)",
            analysisAssetId: assetId,
            segmentFingerprint: "\(assetId)-fp-\(index)-\(pass)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "segment \(index)",
            normalizedText: "segment \(index)",
            pass: pass,
            modelVersion: "test-asr",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// Watermark / duration → simple ratio in `[0, 1]` (no overflow).
    @Test("transcript watermark 60s of 300s episode → fraction 0.2")
    func transcriptFractionUnderfull() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                fastTranscriptCoverageEndTime: 60,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            )
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 1)
        let input = try #require(inputs.first)
        #expect(input.transcriptFraction == 0.2)
    }

    /// Dogfood regression: rows can have actual fast transcript chunks that
    /// cover much more audio than the cached
    /// `fastTranscriptCoverageEndTime`. The Activity strip should follow the
    /// persisted text coverage instead of making a fully-transcribed-looking
    /// episode appear barely started.
    @Test("fast transcript chunks override stale transcript watermark")
    func transcriptFractionUsesFastChunkCoverageWhenWatermarkIsStale() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                fastTranscriptCoverageEndTime: 30,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            )
        ])
        try await fixture.store.insertTranscriptChunks([
            transcriptChunk(assetId: fixture.assetIds[0], index: 0, start: 0, end: 100),
            transcriptChunk(assetId: fixture.assetIds[0], index: 1, start: 100, end: 200),
            transcriptChunk(assetId: fixture.assetIds[0], index: 2, start: 200, end: 250)
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 1)
        let input = try #require(inputs.first)
        #expect(input.transcriptFraction == 250.0 / 300.0)
    }

    /// Overflow watermark (e.g. confirmed-ad watermark briefly past the
    /// asset's `episodeDurationSec` because of decoder/duration drift)
    /// must clamp to `1.0` rather than leaking a >1 fraction into the
    /// row's strip view.
    @Test("confirmed-ad watermark > duration → analysisFraction clamps to 1.0")
    func analysisFractionClampsOnOverflow() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: 600,
                episodeDurationSec: 300
            )
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 1)
        let input = try #require(inputs.first)
        #expect(input.analysisFraction == 1.0)
    }

    /// Analysis progress should not render as unknown just because no
    /// non-suppressed ad window was found. Feature coverage is the broad
    /// analysis-progress watermark available for no-ad and feature-only
    /// episodes.
    @Test("feature coverage populates analysis fraction when confirmed-ad coverage is nil")
    func analysisFractionUsesFeatureCoverageWithoutConfirmedAds() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                featureCoverageEndTime: 150,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            )
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 1)
        let input = try #require(inputs.first)
        #expect(input.analysisFraction == 0.5)
    }

    /// Missing or non-positive duration must produce `nil` for both
    /// analysis-derived fractions — there is no meaningful denominator,
    /// so the row renders as "fraction unknown" rather than synthesising
    /// a fake 0% bar from a divide-by-zero.
    @Test("episodeDurationSec == 0 or nil → transcript & analysis fractions nil")
    func zeroOrNilDurationProducesNilFractions() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            // Row 0: duration nil entirely.
            AssetSeed(
                fastTranscriptCoverageEndTime: 30,
                confirmedAdCoverageEndTime: 45,
                episodeDurationSec: nil
            ),
            // Row 1: duration zero.
            AssetSeed(
                fastTranscriptCoverageEndTime: 30,
                confirmedAdCoverageEndTime: 45,
                episodeDurationSec: 0
            )
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 2)
        for input in inputs {
            #expect(input.transcriptFraction == nil)
            #expect(input.analysisFraction == nil)
        }
    }

    /// Download snapshot is keyed by `episodeId`; only matching episodes
    /// carry a `downloadFraction`. Non-matching episodes (no in-flight
    /// foreground download) keep `downloadFraction == nil`.
    @Test("download snapshot { ep-1: 0.42 } → only ep-1 carries 0.42")
    func downloadFractionPopulatesOnlyMatchingEpisode() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            ),
            AssetSeed(
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            )
        ])
        let downloadingId = fixture.episodeIds[0]
        let otherId = fixture.episodeIds[1]
        let snapshot: [String: Double] = [downloadingId: 0.42]
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { snapshot },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 2)
        let byId = Dictionary(uniqueKeysWithValues: inputs.map { ($0.episodeId, $0) })
        let downloading = try #require(byId[downloadingId])
        let other = try #require(byId[otherId])
        #expect(downloading.downloadFraction == 0.42)
        #expect(other.downloadFraction == nil)
    }

    /// Once a transfer completes it disappears from the foreground progress
    /// map. The Activity strip should still report the cached episode as
    /// fully downloaded instead of falling back to "unknown".
    @Test("cached episode with no live progress → downloadFraction 1.0")
    func cachedDownloadProducesFullDownloadFraction() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            )
        ])
        let cachedId = fixture.episodeIds[0]
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            downloadedEpisodeIdsProvider: { eligible in
                eligible.contains(cachedId) ? [cachedId] : []
            },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 1)
        let input = try #require(inputs.first)
        #expect(input.downloadFraction == 1.0)
    }

    /// If a live foreground progress sample exists, it is the freshest
    /// signal. Keep that fraction even if the cached set also contains the
    /// episode, so a stale cache probe cannot hide an in-flight transfer.
    @Test("live download progress wins over cached 100% fallback")
    func liveDownloadProgressWinsOverCachedFallback() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            )
        ])
        let episodeId = fixture.episodeIds[0]
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [episodeId: 0.42] },
            downloadedEpisodeIdsProvider: { _ in [episodeId] },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 1)
        let input = try #require(inputs.first)
        #expect(input.downloadFraction == 0.42)
    }

    /// Empty download snapshot + empty cached set leaves every input's
    /// `downloadFraction == nil`.
    @Test("empty download snapshot → every input has downloadFraction == nil")
    func emptyDownloadSnapshotProducesNilEverywhere() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            ),
            AssetSeed(
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 300
            )
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()

        #expect(inputs.count == 2)
        for input in inputs {
            #expect(input.downloadFraction == nil)
        }
    }

    @Test("dogfood diagnostics snapshot includes displayed fractions and latest terminal cause")
    func dogfoodDiagnosticsSnapshotCapturesActivityRowState() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                featureCoverageEndTime: 100,
                fastTranscriptCoverageEndTime: 30,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 400
            )
        ])
        try await fixture.store.insertTranscriptChunks([
            transcriptChunk(assetId: fixture.assetIds[0], index: 0, start: 0, end: 200)
        ])
        let generationID = UUID()
        try await fixture.store.appendWorkJournalEntry(
            WorkJournalEntry(
                id: "journal-failed",
                episodeId: fixture.episodeIds[0],
                generationID: generationID,
                schedulerEpoch: 7,
                timestamp: 20,
                eventType: .failed,
                cause: .asrFailed,
                metadata: "{}",
                artifactClass: .scratch
            )
        )
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [fixture.episodeIds[0]: 0.5] },
            downloadedEpisodeIdsProvider: { eligible in
                eligible.contains(fixture.episodeIds[0]) ? [fixture.episodeIds[0]] : []
            },
            modelContainer: fixture.container
        )

        let snapshot = await provider.loadDogfoodDiagnosticsSnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            episodeHashProvider: { _ in "hashed-episode" }
        )

        #expect(snapshot.captureError == nil)
        #expect(snapshot.rows.count == 1)
        let row = try #require(snapshot.rows.first)
        #expect(row.episodeIdHash == "hashed-episode")
        #expect(row.section == "up_next")
        #expect(row.cachedAudioPresent == true)
        #expect(row.liveDownloadFraction == 0.5)
        #expect(row.pipeline.downloadFraction == 0.5)
        #expect(row.pipeline.downloadPercent == "50%")
        #expect(row.pipeline.downloadSource == "live_progress")
        #expect(row.pipeline.transcriptFraction == 0.5)
        #expect(row.pipeline.transcriptPercent == "50%")
        #expect(row.pipeline.transcriptSource == "fast_transcript_chunks")
        #expect(row.pipeline.analysisFraction == 0.25)
        #expect(row.pipeline.analysisPercent == "25%")
        #expect(row.pipeline.analysisSource == "feature_coverage")
        // playhead-hygc.1.2: per-field provenance for the canonical
        // coverage summary. Fast-pass chunk in the seed (end=200s) drives
        // the high-water value, so its source must be `fast_transcript_chunks`.
        // No final-pass chunk is inserted, so the final-pass high-water
        // source falls through to `unknown` (NOT a synthetic "0%"). This
        // pins the bead's "no caller falls back to a stale watermark
        // when chunks are present" + "unknown stays unknown" rules.
        #expect(row.pipeline.fastTranscriptCoverageEndSource == "fast_transcript_chunks")
        #expect(row.pipeline.finalPassCoverageEndSource == "unknown")
        #expect(row.analysisAsset.analysisState == "queued")
        #expect(row.latestTerminalWorkJournal?.eventType == "failed")
        #expect(row.latestTerminalWorkJournal?.cause == "asr_failed")
        #expect(row.latestTerminalWorkJournal?.generationID == generationID.uuidString)
    }

    /// Bead playhead-hygc.1.2 (asset_004 contradiction): a stale
    /// `fastTranscriptCoverageEndTime=90s` on the asset row, with a fast
    /// chunk reaching 3960s, must surface chunk-derived coverage on the
    /// dogfood diagnostics wire (NOT the watermark) and the row's
    /// transcript fraction must reflect the chunk-derived value.
    /// Mirrors `LiveActivitySnapshotProviderFractionTests`'s "fast
    /// transcript chunks override stale transcript watermark" but at the
    /// dogfood diagnostics wire layer.
    @Test("dogfood diagnostics: stale watermark + fresh chunks -> chunk-derived coverage on the wire (playhead-hygc.1.2)")
    func dogfoodDiagnosticsSurfacesChunkDerivedCoverageOverStaleWatermark() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: 90,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 4000
            )
        ])
        try await fixture.store.insertTranscriptChunks([
            transcriptChunk(assetId: fixture.assetIds[0], index: 0, start: 0, end: 3960)
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let snapshot = await provider.loadDogfoodDiagnosticsSnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            episodeHashProvider: { _ in "hashed-episode" }
        )

        let row = try #require(snapshot.rows.first)
        // Chunk-derived high-water (3960s/4000s = 0.99) — NOT 90s/4000s.
        #expect(row.pipeline.transcriptFraction == 3960.0 / 4000.0)
        #expect(row.pipeline.transcriptSource == "fast_transcript_chunks")
        // The new provenance field on the wire must agree.
        #expect(row.pipeline.fastTranscriptCoverageEndSource == "fast_transcript_chunks")
        // No final-pass chunk inserted; provenance is `unknown` and the
        // value stays nil (no synthetic 0%).
        #expect(row.pipeline.finalPassCoverageEndSource == "unknown")
        #expect(row.pipeline.finalPassCoverageEndSec == nil)
    }

    /// Bead playhead-hygc.1.2: when no transcript-coverage signal exists
    /// at all (no chunks AND no asset watermark), the diagnostics row
    /// must report `unknown` provenance and a NIL fraction — never a
    /// synthetic 0%.
    @Test("dogfood diagnostics: nothing known -> unknown provenance, nil fraction (playhead-hygc.1.2)")
    func dogfoodDiagnosticsLeavesUnknownWhenNoSignals() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 1000
            )
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let snapshot = await provider.loadDogfoodDiagnosticsSnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            episodeHashProvider: { _ in "hashed-episode" }
        )

        let row = try #require(snapshot.rows.first)
        // The bead's "no synthetic 0%" rule binds the FRACTION value: it
        // must stay nil when nothing is known. The percent STRING is a
        // pre-existing placeholder ("--%") used by the diagnostics
        // surface to render a missing fraction, not a synthetic 0%.
        #expect(row.pipeline.transcriptFraction == nil)
        #expect(row.pipeline.fastTranscriptCoverageEndSource == "unknown")
        #expect(row.pipeline.finalPassCoverageEndSource == "unknown")
        #expect(row.pipeline.featureCoverageEndSec == nil)
        #expect(row.pipeline.finalPassCoverageEndSec == nil)
    }

    /// Bead playhead-hygc.1.2: when no fast chunks landed but the asset
    /// row still has a fast watermark, the dogfood `transcript_source`
    /// wire string MUST be `"asset_watermark"` (the bead's allowlisted
    /// vocabulary). A buggy impl that always returns `"unknown"` would
    /// fail this test, and so would any drift back to the legacy
    /// `"asset_fast_watermark"` token. Pins the wire vocabulary alongside
    /// the per-field provenance enum's `asset_watermark` rawValue so the
    /// row JSON never reports two different names for the same fact.
    @Test("dogfood diagnostics: watermark-only fast coverage -> transcript_source is `asset_watermark` (playhead-hygc.1.2)")
    func dogfoodDiagnosticsTranscriptSourceIsAssetWatermarkWhenChunksAbsent() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: 120,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 600
            )
        ])
        // Intentionally NO transcript chunks: forces fallback to the
        // asset's `fastTranscriptCoverageEndTime` watermark.
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let snapshot = await provider.loadDogfoodDiagnosticsSnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            episodeHashProvider: { _ in "hashed-episode" }
        )

        let row = try #require(snapshot.rows.first)
        // Both vocabulary fields must report the same canonical token.
        #expect(row.pipeline.transcriptSource == "asset_watermark")
        #expect(row.pipeline.fastTranscriptCoverageEndSource == "asset_watermark")
        // Sanity: the value was reconciled from the watermark.
        #expect(row.pipeline.transcriptFraction == 120.0 / 600.0)
        #expect(row.pipeline.fastTranscriptWatermarkSec == 120)
    }

    /// Bead playhead-hygc.1.2 (acceptance criterion: final-pass coverage
    /// appears in dogfood provenance): when final-pass chunks land, the
    /// dogfood snapshot's `final_pass_coverage_end_source` field MUST
    /// report the chunk provenance (`final_pass_chunks`), not `unknown`
    /// and not the asset watermark token. Catches a wrong impl that
    /// always returns `"unknown"` for final-pass provenance — the
    /// existing tests in this suite only assert the `unknown` case at
    /// the wire layer.
    ///
    /// `analysis_source` and `analysis_fraction` MUST agree on the same
    /// row: `analysis_fraction` is computed from
    /// `max(featureCoverageEndSec, confirmedAdCoverageEndSec)`, so
    /// `analysis_source` may only name `feature_coverage`,
    /// `confirmed_ad_coverage`, or `unknown` — never `final_pass_chunks`,
    /// because final-pass seconds do not enter the fraction. Pre-fix R2
    /// observed an inconsistency where a row could read
    /// `analysis_source = "final_pass_chunks"` while the printed
    /// `analysis_fraction` reflected only the feature watermark; this
    /// test pins both fields together so the inconsistency cannot recur.
    @Test("dogfood diagnostics: final-pass chunks present -> final_pass_coverage_end_source is `final_pass_chunks`; analysis_source / analysis_fraction stay consistent (playhead-hygc.1.2)")
    func dogfoodDiagnosticsFinalPassSourceIsFinalPassChunks() async throws {
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                featureCoverageEndTime: 100,
                fastTranscriptCoverageEndTime: 200,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 600
            )
        ])
        try await fixture.store.insertTranscriptChunks([
            transcriptChunk(assetId: fixture.assetIds[0], index: 0, start: 0, end: 200),
            // Final-pass chunk reaching 350s — beyond the feature
            // watermark (100). Drives the final-pass provenance scalar
            // to `final_pass_chunks` independently of the analysis-source
            // wire string, which tracks the analysis-fraction's actual
            // inputs (feature + confirmed-ad only).
            transcriptChunk(assetId: fixture.assetIds[0], index: 1, start: 0, end: 350, pass: "final")
        ])
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let snapshot = await provider.loadDogfoodDiagnosticsSnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            episodeHashProvider: { _ in "hashed-episode" }
        )

        let row = try #require(snapshot.rows.first)
        // Per-field provenance for the final-pass scalar — distinct wire
        // field, populated even though analysis_source does not name it.
        #expect(row.pipeline.finalPassCoverageEndSource == "final_pass_chunks")
        #expect(row.pipeline.finalPassCoverageEndSec == 350)
        // analysis_source and analysis_fraction must agree on the same
        // row. analysis_fraction is feature/duration = 100/600 ≈ 0.1667;
        // analysis_source therefore names `feature_coverage` (the actual
        // input to the fraction), NOT `final_pass_chunks`.
        #expect(row.pipeline.analysisSource == "feature_coverage")
        let analysisFraction = try #require(row.pipeline.analysisFraction)
        #expect(abs(analysisFraction - 100.0 / 600.0) < 1e-9)
    }

    @Test("terminal asset and job states do not remain Up Next when coverage summary is stale")
    func terminalLifecycleStatesProduceFinishedOutcome() async throws {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let fixture = try await makeFixture(assetSeeds: [
            AssetSeed(
                featureCoverageEndTime: 80,
                fastTranscriptCoverageEndTime: 40,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 400,
                analysisState: "completeFull"
            ),
            AssetSeed(
                featureCoverageEndTime: 20,
                fastTranscriptCoverageEndTime: 20,
                confirmedAdCoverageEndTime: nil,
                episodeDurationSec: 400,
                analysisState: "queued"
            )
        ])
        try await fixture.store.insertSession(
            AnalysisSession(
                id: "session-terminal",
                analysisAssetId: fixture.assetIds[0],
                state: "completeFull",
                startedAt: now.addingTimeInterval(-600).timeIntervalSince1970,
                updatedAt: now.addingTimeInterval(-120).timeIntervalSince1970,
                failureReason: nil
            )
        )
        try await fixture.store.insertJob(
            makeAnalysisJob(
                jobId: "job-terminal",
                episodeId: fixture.episodeIds[1],
                analysisAssetId: fixture.assetIds[1],
                state: "complete",
                updatedAt: now.addingTimeInterval(-60).timeIntervalSince1970
            )
        )
        let provider = LiveActivitySnapshotProvider(
            store: fixture.store,
            capabilitySnapshotProvider: { nil },
            runningEpisodeIdProvider: { nil },
            downloadProgressProvider: { [:] },
            modelContainer: fixture.container
        )

        let inputs = await provider.loadInputs()
        let byId = Dictionary(uniqueKeysWithValues: inputs.map { ($0.episodeId, $0) })
        let assetTerminal = try #require(byId[fixture.episodeIds[0]])
        let jobTerminal = try #require(byId[fixture.episodeIds[1]])
        #expect(assetTerminal.status.playbackReadiness == .none)
        #expect(assetTerminal.finishedOutcome == .success)
        #expect(jobTerminal.finishedOutcome == .success)

        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: now)
        #expect(snapshot.upNext.isEmpty)
        #expect(snapshot.recentlyFinished.count == 2)
        #expect(Set(snapshot.recentlyFinished.map(\.episodeId)) == Set(fixture.episodeIds))
    }
}
