// BatchSummaryBuilderTests.swift
// Integration coverage for `BatchSummaryBuilder` driving the production
// `BatchNotificationCoordinator` end-to-end. playhead-0a0s.
//
// Each test seeds an in-memory SwiftData store with one or more
// `Episode` rows in the shape that should drive a particular
// `BatchNotificationEligibility`, then runs the coordinator and
// asserts on what the recording scheduler observes.
//
// The four required cases per acceptance criteria:
//   * tripReady (downloaded + analyzed)
//   * blockedStorage (cause = .mediaCap → user-fixable)
//   * blockedWifiPolicy (cause = .wifiRequired → user-fixable)
//   * blockedAnalysisUnavailable (eligibility off + appleIntelligenceDisabled)
//
// Plus one negative test for the hardware/region case (NOT user-fixable
// → coordinator stays at `.none`).

import Foundation
import SwiftData
import Testing
import UserNotifications

@testable import Playhead

@MainActor
private final class RecordingScheduler: BatchNotificationService.Scheduler {
    private(set) var requests: [UNNotificationRequest] = []
    func add(_ request: UNNotificationRequest) async throws {
        requests.append(request)
    }
    func snapshot() -> [UNNotificationRequest] { requests }
}

@MainActor
private func makeInMemoryContainer() throws -> ModelContainer {
    let schema = Schema([Podcast.self, Episode.self, DownloadBatch.self])
    let config = ModelConfiguration(
        "BatchSummaryBuilderTests",
        schema: schema,
        isStoredInMemoryOnly: true,
        cloudKitDatabase: .none
    )
    return try ModelContainer(for: schema, configurations: [config])
}

/// Insert an Episode row at `key` with the given download/analysis state,
/// returning the canonical key.
///
/// playhead-f5ao: `analyzed` used to be seeded by writing an
/// `AnalysisSummary(hasAnalysis: analyzed, …)` onto the row. That field
/// was deleted — nothing in the app ever wrote it — and
/// `EpisodeProjection.analyzed` now derives from `coverageSummary`, so
/// the seed writes a real coverage record. `analyzed: true` therefore
/// means "this episode has confirmed coverage", which is what the
/// reducer's `hasAnyConfirmedAnalysis` has always meant on every other
/// surface (`PlayerStatusLine`, `EpisodeListView`).
@MainActor
private func insertEpisode(
    context: ModelContext,
    feedItemGUID: String,
    downloadState: DownloadState,
    analyzed: Bool
) throws -> String {
    let url = URL(string: "https://example.com/feed.xml")!
    let podcast = Podcast(
        feedURL: url,
        title: "Test Podcast",
        author: "Test Author"
    )
    context.insert(podcast)
    let episode = Episode(
        feedItemGUID: feedItemGUID,
        feedURL: url,
        podcast: podcast,
        title: "Test Episode \(feedItemGUID)",
        audioURL: URL(string: "https://example.com/\(feedItemGUID).mp3")!,
        downloadState: downloadState,
        coverageSummary: analyzed ? confirmedCoverage() : nil
    )
    context.insert(episode)
    try context.save()
    return episode.canonicalEpisodeKey
}

/// A minimal non-nil `CoverageSummary` — the shape that makes
/// `EpisodeProjection.analyzed` true.
private func confirmedCoverage() -> CoverageSummary {
    CoverageSummary(
        coverageRanges: [0...600],
        isComplete: true,
        modelVersion: "m1",
        policyVersion: 1,
        featureSchemaVersion: 1,
        updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )
}

/// Build an `AnalysisEligibility` with all gates passing (the default
/// for tests that don't exercise Rule 1).
private func fullyEligible() -> AnalysisEligibility {
    AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )
}

/// Build a `BatchSummaryBuilder` whose `episodeLookup` hops to MainActor
/// and uses the container's main context. The `container` capture is
/// safe because `ModelContainer` is `Sendable`; the per-call hop into
/// `MainActor.run` is what keeps the non-Sendable `ModelContext` access
/// inside its owning isolation domain.
@MainActor
private func makeBuilder(
    container: ModelContainer,
    cause: InternalMissCause?,
    eligibility: AnalysisEligibility
) -> BatchSummaryBuilder {
    BatchSummaryBuilder(
        episodeLookup: { @Sendable lookupKey in
            await MainActor.run {
                let context = container.mainContext
                let descriptor = FetchDescriptor<Episode>(
                    predicate: #Predicate { $0.canonicalEpisodeKey == lookupKey }
                )
                return (try? context.fetch(descriptor).first).map(EpisodeProjection.init)
            }
        },
        causeLookup: { _ in cause },
        eligibilityProvider: { eligibility }
    )
}

@Suite("BatchSummaryBuilder — production-builder integration (playhead-0a0s)")
@MainActor
struct BatchSummaryBuilderTests {

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    // MARK: - tripReady (downloaded + analyzed)

    @Test("tripReady fires when every child is downloaded AND analyzed")
    func tripReadyFromBuilder() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let key = try insertEpisode(
            context: context,
            feedItemGUID: "ep-ready",
            downloadState: .downloaded,
            analyzed: true
        )

        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: [key]
        )
        context.insert(batch)
        try context.save()

        let builder = makeBuilder(
            container: container,
            cause: nil,
            eligibility: fullyEligible()
        )

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: { keys in await builder.summaries(for: keys) }
        )

        await coordinator.runOncePass(now: Self.t0)

        let requests = await scheduler.snapshot()
        let tripReady = requests.filter {
            ($0.content.userInfo["trigger"] as? String) == "tripReady"
        }
        #expect(tripReady.count == 1)
        #expect(batch.tripReadyNotified == true)
    }

    // MARK: - blockedStorage (cause = .mediaCap)

    @Test("blockedStorage fires when cause = .mediaCap → reason = .storageFull / userFixable")
    func blockedStorageFromBuilder() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let key = try insertEpisode(
            context: context,
            feedItemGUID: "ep-storage",
            downloadState: .notDownloaded,
            analyzed: false
        )

        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        // Pre-seed persistence so the FIRST pass already clears the
        // ≥2-passes AND ≥30-minute AND-gate. The natural two-pass
        // progression is exercised by `BatchNotificationCoordinatorTests`
        // already; this test's job is to confirm the
        // builder→reducer→coordinator chain promotes a `.mediaCap` cause
        // into a fired `.blockedStorage` notification given mature
        // persistence state. Seeding is also necessary because the
        // surface-status reducer maps `.mediaCap` to `disposition: .failed`
        // (terminal), which the coordinator would otherwise close after a
        // single pass — preventing the AND-gate from ever clearing on
        // single-episode batches.
        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: [key],
            consecutiveBlockedPasses: 1,
            firstBlockedAt: Self.t0.addingTimeInterval(-35 * 60)
        )
        context.insert(batch)
        try context.save()

        let builder = makeBuilder(
            container: container,
            cause: .mediaCap,
            eligibility: fullyEligible()
        )

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: { keys in await builder.summaries(for: keys) }
        )

        await coordinator.runOncePass(now: Self.t0)

        let requests = await scheduler.snapshot()
        let storage = requests.filter {
            ($0.content.userInfo["trigger"] as? String) == "blockedStorage"
        }
        #expect(storage.count == 1)
        #expect(batch.actionRequiredNotified == true)
    }

    // MARK: - blockedWifiPolicy (cause = .wifiRequired)

    @Test("blockedWifiPolicy fires when cause = .wifiRequired → reason = .waitingForNetwork / userFixable")
    func blockedWifiPolicyFromBuilder() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let key = try insertEpisode(
            context: context,
            feedItemGUID: "ep-wifi",
            downloadState: .notDownloaded,
            analyzed: false
        )

        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: [key]
        )
        context.insert(batch)
        try context.save()

        let builder = makeBuilder(
            container: container,
            cause: .wifiRequired,
            eligibility: fullyEligible()
        )

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: { keys in await builder.summaries(for: keys) }
        )

        await coordinator.runOncePass(now: Self.t0)
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(35 * 60))

        let requests = await scheduler.snapshot()
        let wifi = requests.filter {
            ($0.content.userInfo["trigger"] as? String) == "blockedWifiPolicy"
        }
        #expect(wifi.count == 1)
        #expect(batch.actionRequiredNotified == true)
    }

    // MARK: - blockedAnalysisUnavailable (appleIntelligenceDisabled)

    @Test("blockedAnalysisUnavailable fires for appleIntelligenceDisabled (user-fixable)")
    func blockedAnalysisUnavailableAppleIntelligenceDisabled() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let key = try insertEpisode(
            context: context,
            feedItemGUID: "ep-ai-off",
            downloadState: .notDownloaded,
            analyzed: false
        )

        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        // Pre-seed persistence (see `blockedStorageFromBuilder` for
        // rationale). Eligibility is a per-device signal: when AI is off,
        // every child in the batch maps to `.unavailable / .analysisUnavailable`,
        // which is terminal — so the natural two-pass progression cannot
        // run without the seed.
        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: [key],
            consecutiveBlockedPasses: 1,
            firstBlockedAt: Self.t0.addingTimeInterval(-35 * 60)
        )
        context.insert(batch)
        try context.save()

        let aiOffEligibility = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: false,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Self.t0
        )

        let builder = makeBuilder(
            container: container,
            cause: nil,
            eligibility: aiOffEligibility
        )

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: { keys in await builder.summaries(for: keys) }
        )

        await coordinator.runOncePass(now: Self.t0)

        let requests = await scheduler.snapshot()
        let ai = requests.filter {
            ($0.content.userInfo["trigger"] as? String) == "blockedAnalysisUnavailable"
        }
        #expect(ai.count == 1)
        #expect(batch.actionRequiredNotified == true)
    }

    @Test("blockedAnalysisUnavailable fires for languageUnsupported (user-fixable)")
    func blockedAnalysisUnavailableLanguageUnsupported() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let key = try insertEpisode(
            context: context,
            feedItemGUID: "ep-lang",
            downloadState: .notDownloaded,
            analyzed: false
        )

        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        // Pre-seed persistence; see `blockedStorageFromBuilder` for
        // rationale.
        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: [key],
            consecutiveBlockedPasses: 1,
            firstBlockedAt: Self.t0.addingTimeInterval(-35 * 60)
        )
        context.insert(batch)
        try context.save()

        let langEligibility = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: false,
            modelAvailableNow: true,
            capturedAt: Self.t0
        )

        let builder = makeBuilder(
            container: container,
            cause: nil,
            eligibility: langEligibility
        )

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: { keys in await builder.summaries(for: keys) }
        )

        await coordinator.runOncePass(now: Self.t0)

        let requests = await scheduler.snapshot()
        let lang = requests.filter {
            ($0.content.userInfo["trigger"] as? String) == "blockedAnalysisUnavailable"
        }
        #expect(lang.count == 1)
    }

    // MARK: - hardwareUnsupported / regionUnsupported NEVER fire

    @Test("hardwareUnsupported stays at .none (NOT user-fixable)")
    func hardwareUnsupportedDoesNotFire() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let key = try insertEpisode(
            context: context,
            feedItemGUID: "ep-hw",
            downloadState: .notDownloaded,
            analyzed: false
        )

        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: [key]
        )
        context.insert(batch)
        try context.save()

        let hwEligibility = AnalysisEligibility(
            hardwareSupported: false,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Self.t0
        )

        let builder = makeBuilder(
            container: container,
            cause: nil,
            eligibility: hwEligibility
        )

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: { keys in await builder.summaries(for: keys) }
        )

        // Run THREE passes spread over hours — even with the persistence
        // rule fully cleared, the coordinator must not promote a hardware
        // -unsupported child to action-required.
        await coordinator.runOncePass(now: Self.t0)
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(35 * 60))
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(120 * 60))

        let requests = await scheduler.snapshot()
        #expect(requests.isEmpty)
        #expect(batch.actionRequiredNotified == false)
    }

    @Test("regionUnsupported stays at .none (NOT user-fixable)")
    func regionUnsupportedDoesNotFire() async throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext
        let key = try insertEpisode(
            context: context,
            feedItemGUID: "ep-region",
            downloadState: .notDownloaded,
            analyzed: false
        )

        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = DownloadBatch(
            tripContextRaw: DownloadTripContext.flight.rawValue,
            episodeKeys: [key]
        )
        context.insert(batch)
        try context.save()

        let regionEligibility = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: false,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Self.t0
        )

        let builder = makeBuilder(
            container: container,
            cause: nil,
            eligibility: regionEligibility
        )

        let coordinator = BatchNotificationCoordinator(
            modelContext: context,
            service: service,
            summaryBuilder: { keys in await builder.summaries(for: keys) }
        )

        await coordinator.runOncePass(now: Self.t0)
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(35 * 60))
        await coordinator.runOncePass(now: Self.t0.addingTimeInterval(120 * 60))

        let requests = await scheduler.snapshot()
        #expect(requests.isEmpty)
        #expect(batch.actionRequiredNotified == false)
    }

    // MARK: - EpisodeProjection.analyzed (playhead-f5ao)

    /// `analyzed` used to be lifted from `Episode.analysisSummary?
    /// .hasAnalysis`, a field with no producer anywhere in the app, so
    /// `isReady` was `downloaded && false` on every real device. It is
    /// now derived from `coverageSummary`, which is how
    /// `PlayerStatusLine` and `EpisodeListView` have always answered
    /// the same question when they build an `AnalysisState`
    /// (`hasAnyConfirmedAnalysis: episode.coverageSummary != nil`).
    ///
    /// This pins the derivation in BOTH directions and off a real
    /// SwiftData row, so the lift and the definition are covered
    /// together — a projection built by hand cannot show that
    /// `init(_ episode:)` reads the right column.
    @Test("EpisodeProjection.analyzed is exactly coverageSummary != nil")
    func analyzedDerivesFromCoverage() throws {
        let container = try makeInMemoryContainer()
        let context = container.mainContext

        let withCoverage = try insertEpisode(
            context: context,
            feedItemGUID: "ep-covered",
            downloadState: .downloaded,
            analyzed: true
        )
        let withoutCoverage = try insertEpisode(
            context: context,
            feedItemGUID: "ep-uncovered",
            downloadState: .downloaded,
            analyzed: false
        )

        func projection(_ key: String) throws -> EpisodeProjection {
            let descriptor = FetchDescriptor<Episode>(
                predicate: #Predicate<Episode> { $0.canonicalEpisodeKey == key }
            )
            let row = try #require(context.fetch(descriptor).first)
            return EpisodeProjection(row)
        }

        let covered = try projection(withCoverage)
        #expect(covered.coverageSummary != nil)
        #expect(covered.analyzed == true)
        #expect(BatchSummaryBuilder.analysisState(from: covered).persistedStatus == .done)
        #expect(BatchSummaryBuilder.analysisState(from: covered).hasAnyConfirmedAnalysis == true)

        let uncovered = try projection(withoutCoverage)
        #expect(uncovered.coverageSummary == nil)
        #expect(uncovered.analyzed == false)
        #expect(BatchSummaryBuilder.analysisState(from: uncovered).persistedStatus == .queued)
        #expect(BatchSummaryBuilder.analysisState(from: uncovered).hasAnyConfirmedAnalysis == false)
    }

    // MARK: - Pure projection: makeSummary boundary tests

    @Test("makeSummary: nil episode → cancelled / not-ready / not-fixable")
    func makeSummaryMissingEpisode() {
        let summary = BatchSummaryBuilder.makeSummary(
            canonicalEpisodeKey: "ep-missing",
            episode: nil,
            cause: nil,
            eligibility: fullyEligible()
        )
        #expect(summary.disposition == .cancelled)
        #expect(summary.reason == .cancelled)
        #expect(summary.isReady == false)
        #expect(summary.userFixable == false)
    }

    @Test("makeSummary: downloaded + analyzed → ready (regardless of cause)")
    func makeSummaryReady() {
        let projection = EpisodeProjection(
            downloaded: true,
            coverageSummary: confirmedCoverage(),
            playbackAnchor: nil
        )
        #expect(projection.analyzed == true,
                "confirmed coverage is what `analyzed` means after playhead-f5ao")
        let summary = BatchSummaryBuilder.makeSummary(
            canonicalEpisodeKey: "ep-ready",
            episode: projection,
            cause: nil,
            eligibility: fullyEligible()
        )
        #expect(summary.isReady == true)
    }

    @Test("makeSummary: hardwareUnsupported → analysisUnavailable / NOT user-fixable")
    func makeSummaryHardwareNotFixable() {
        let projection = EpisodeProjection(
            downloaded: false,
            coverageSummary: nil,
            playbackAnchor: nil
        )
        let hwEligibility = AnalysisEligibility(
            hardwareSupported: false,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let summary = BatchSummaryBuilder.makeSummary(
            canonicalEpisodeKey: "ep-hw",
            episode: projection,
            cause: nil,
            eligibility: hwEligibility
        )
        #expect(summary.reason == .analysisUnavailable)
        #expect(summary.analysisUnavailableReason == .hardwareUnsupported)
        #expect(summary.userFixable == false)
    }

    @Test("makeSummary: appleIntelligenceDisabled → analysisUnavailable / user-fixable")
    func makeSummaryAppleIntelligenceFixable() {
        let projection = EpisodeProjection(
            downloaded: false,
            coverageSummary: nil,
            playbackAnchor: nil
        )
        let aiOff = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: false,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let summary = BatchSummaryBuilder.makeSummary(
            canonicalEpisodeKey: "ep-ai",
            episode: projection,
            cause: nil,
            eligibility: aiOff
        )
        #expect(summary.reason == .analysisUnavailable)
        #expect(summary.analysisUnavailableReason == .appleIntelligenceDisabled)
        #expect(summary.userFixable == true)
    }

    // MARK: - AnalysisStore work-journal cause lookup

    @Test("AnalysisStore.fetchLastWorkJournalCause returns most-recent cause across generations")
    func fetchLastWorkJournalCauseRoundtrip() async throws {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()

        let episodeId = "ep-storage-blocked"
        let gen1 = UUID()
        let gen2 = UUID()

        // Older entry for an unrelated cause (different generation).
        try await store.appendWorkJournalEntry(
            WorkJournalEntry(
                id: UUID().uuidString,
                episodeId: episodeId,
                generationID: gen1,
                schedulerEpoch: 0,
                timestamp: 1_700_000_000,
                eventType: .failed,
                cause: .thermal,
                metadata: "{}",
                artifactClass: .scratch
            )
        )
        // Newer entry: the one we expect to surface.
        try await store.appendWorkJournalEntry(
            WorkJournalEntry(
                id: UUID().uuidString,
                episodeId: episodeId,
                generationID: gen2,
                schedulerEpoch: 0,
                timestamp: 1_700_000_100,
                eventType: .failed,
                cause: .mediaCap,
                metadata: "{}",
                artifactClass: .scratch
            )
        )

        let cause = try await store.fetchLastWorkJournalCause(episodeId: episodeId)
        #expect(cause == .mediaCap)
    }

    @Test("AnalysisStore.fetchLastWorkJournalCause returns nil for unknown episode")
    func fetchLastWorkJournalCauseEmpty() async throws {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()

        let cause = try await store.fetchLastWorkJournalCause(episodeId: "ep-never-seen")
        #expect(cause == nil)
    }
}
