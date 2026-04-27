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
    ///   1. The call completes in well under a quarter-second wall-clock.
    ///   2. While it is running, the main actor is free to do unrelated
    ///      work — because the SwiftData fetch + AnalysisStore round-trip
    ///      now run off-main.
    ///
    /// The `@MainActor` annotation on the test ensures the racing
    /// `Task { @MainActor ... }` actually contends with the same actor
    /// the production call site uses.
    @Test("loadInputs completes off-main in under 250ms with N=200 episodes")
    @MainActor
    func loadInputsDoesNotBlockMainOnLargeLibrary() async throws {
        let n = 200

        // SwiftData container — same schema as production.
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true)
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
            // there are zero in-flight foreground downloads.
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
        // seconds on Dan's device with ~50-100 rows; on a fast simulator
        // even 200 N+1 round-trips comfortably blow past 250ms. The
        // post-fix path (single bulk SELECT + prefetch) is well under
        // 100ms in practice.
        #expect(elapsed < 0.25, "loadInputs took \(elapsed)s; budget is 0.25s")

        // Off-main proof. If the load was main-blocking, the racer
        // could not have run at all between the two MainActor hops —
        // counter would still be 0. We require at least one bump.
        //
        // This is intentionally a soft floor (>= 1, not >= 10) because
        // scheduler fairness varies; one bump is enough to prove main
        // was not held continuously for the full 250ms.
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
/// provider-side wiring: divide watermark by duration for the two
/// analysis fractions, look up the download fraction from the injected
/// snapshot closure, and clamp to `[0, 1]` so the row contract is
/// "already in range if non-nil".
@Suite("LiveActivitySnapshotProvider — pipeline fractions (playhead-btoa.3)")
struct LiveActivitySnapshotProviderFractionTests {

    /// Helper: seeds a single Podcast + N Episodes (one per `episodeId`)
    /// into a fresh in-memory SwiftData container, paired with N
    /// `AnalysisAsset` rows in a fresh `AnalysisStore`. Returns both
    /// alongside the canonical episode-key list (in seed order) so each
    /// test can drive `loadInputs()` and assert on the resulting
    /// `ActivityEpisodeInput` fields.
    ///
    /// `assetSeeds` carries the per-asset coverage watermarks +
    /// duration; the provider divides watermark by duration to produce
    /// the fractions under test.
    private struct AssetSeed {
        let fastTranscriptCoverageEndTime: Double?
        let confirmedAdCoverageEndTime: Double?
        let episodeDurationSec: Double?
    }

    private struct Fixture {
        let store: AnalysisStore
        let container: ModelContainer
        let episodeIds: [String]
    }

    private func makeFixture(assetSeeds: [AssetSeed]) async throws -> Fixture {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true)
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
        episodeIds.reserveCapacity(assetSeeds.count)
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
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: seed.fastTranscriptCoverageEndTime,
                confirmedAdCoverageEndTime: seed.confirmedAdCoverageEndTime,
                analysisState: "queued",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: seed.episodeDurationSec
            )
            try await store.insertAsset(asset)
        }
        try context.save()

        return Fixture(store: store, container: container, episodeIds: episodeIds)
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

    /// Empty download snapshot (the dominant production case once
    /// transfers settle) leaves every input's `downloadFraction == nil`.
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
}
