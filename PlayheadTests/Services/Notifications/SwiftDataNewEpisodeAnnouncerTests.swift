// SwiftDataNewEpisodeAnnouncerTests.swift
// playhead-snp — production announcer that resolves SwiftData rows
// into NewEpisodeCandidate values + forwards to the
// NewEpisodeNotificationScheduler.

import Foundation
import SwiftData
import Testing
import UserNotifications

@testable import Playhead

@MainActor
private final class RecordingScheduling: NewEpisodeNotificationScheduler.Scheduling {
    private(set) var requests: [UNNotificationRequest] = []
    func add(_ request: UNNotificationRequest) async throws { requests.append(request) }
    func removePending(withIdentifiers identifiers: [String]) async {
        requests.removeAll { identifiers.contains($0.identifier) }
    }
}

@MainActor
private final class AlwaysAuthorized: NewEpisodeNotificationScheduler.AuthorizationProviding {
    func authorizationStatus() async -> UNAuthorizationStatus { .authorized }
    func requestAuthorization() async -> Bool { true }
}

@MainActor
private final class InMemoryLedger: NewEpisodeNotificationScheduler.DedupLedger {
    var seen: Set<String> = []
    func contains(_ key: String) -> Bool { seen.contains(key) }
    func record(_ key: String) { seen.insert(key) }
}

@Suite("SwiftDataNewEpisodeAnnouncer — resolves rows + forwards (playhead-snp)")
@MainActor
struct SwiftDataNewEpisodeAnnouncerTests {

    private static func makeContainer() throws -> ModelContainer {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(
            "SwiftDataNewEpisodeAnnouncerTests",
            schema: schema,
            isStoredInMemoryOnly: true,
            allowsSave: true
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    @Test("Announcer resolves Podcast/Episode and routes one notification per fresh new episode")
    func resolvesAndRoutes() async throws {
        let container = try Self.makeContainer()
        let context = container.mainContext

        let feedURL = URL(string: "https://example.com/show.xml")!
        let podcast = Podcast(feedURL: feedURL, title: "The Show", author: "Host")
        context.insert(podcast)

        let episode = Episode(
            feedItemGUID: "guid-1",
            feedURL: feedURL,
            podcast: podcast,
            title: "Latest Episode",
            audioURL: URL(string: "https://example.com/1.mp3")!,
            publishedAt: .now
        )
        context.insert(episode)
        try context.save()

        let scheduler = RecordingScheduling()
        let authorizer = AlwaysAuthorized()
        let ledger = InMemoryLedger()
        let inner = NewEpisodeNotificationScheduler(
            scheduler: scheduler,
            authorizer: authorizer,
            ledger: ledger
        )

        let announcer = SwiftDataNewEpisodeAnnouncer(
            modelContainer: container,
            scheduler: inner,
            appWideEnabledProvider: { true }
        )

        await announcer.announce(newEpisodes: [
            FeedRefreshNewEpisode(
                canonicalEpisodeKey: episode.canonicalEpisodeKey,
                audioURL: episode.audioURL,
                publishedAt: episode.publishedAt
            )
        ])

        #expect(scheduler.requests.count == 1)
        #expect(scheduler.requests[0].content.title == "The Show")
        #expect(scheduler.requests[0].content.body == "Latest Episode")
    }

    @Test("Announcer skips episodes whose Podcast.notificationsEnabled is false")
    func respectsPerShowToggle() async throws {
        let container = try Self.makeContainer()
        let context = container.mainContext

        let feedURL = URL(string: "https://example.com/show.xml")!
        let podcast = Podcast(feedURL: feedURL, title: "Off Show", author: "Host")
        podcast.notificationsEnabled = false
        context.insert(podcast)
        let episode = Episode(
            feedItemGUID: "guid-2",
            feedURL: feedURL,
            podcast: podcast,
            title: "Won't Notify",
            audioURL: URL(string: "https://example.com/2.mp3")!,
            publishedAt: .now
        )
        context.insert(episode)
        try context.save()

        let scheduler = RecordingScheduling()
        let inner = NewEpisodeNotificationScheduler(
            scheduler: scheduler,
            authorizer: AlwaysAuthorized(),
            ledger: InMemoryLedger()
        )
        let announcer = SwiftDataNewEpisodeAnnouncer(
            modelContainer: container,
            scheduler: inner,
            appWideEnabledProvider: { true }
        )

        await announcer.announce(newEpisodes: [
            FeedRefreshNewEpisode(
                canonicalEpisodeKey: episode.canonicalEpisodeKey,
                audioURL: episode.audioURL,
                publishedAt: episode.publishedAt
            )
        ])

        #expect(scheduler.requests.isEmpty)
    }

    @Test("Announcer drops keys that no longer resolve to an Episode row")
    func skipsUnresolvedKeys() async throws {
        let container = try Self.makeContainer()
        let scheduler = RecordingScheduling()
        let inner = NewEpisodeNotificationScheduler(
            scheduler: scheduler,
            authorizer: AlwaysAuthorized(),
            ledger: InMemoryLedger()
        )
        let announcer = SwiftDataNewEpisodeAnnouncer(
            modelContainer: container,
            scheduler: inner,
            appWideEnabledProvider: { true }
        )

        // Reference a canonical key that does NOT exist in the store.
        await announcer.announce(newEpisodes: [
            FeedRefreshNewEpisode(
                canonicalEpisodeKey: "https://example.com/none::ghost",
                audioURL: URL(string: "https://example.com/g.mp3")!,
                publishedAt: .now
            )
        ])

        #expect(scheduler.requests.isEmpty)
    }

    @Test("Announcer short-circuits when app-wide toggle is off")
    func appWideOffShortCircuits() async throws {
        let container = try Self.makeContainer()
        let context = container.mainContext

        let feedURL = URL(string: "https://example.com/show.xml")!
        let podcast = Podcast(feedURL: feedURL, title: "Show", author: "Host")
        context.insert(podcast)
        let episode = Episode(
            feedItemGUID: "guid-3",
            feedURL: feedURL,
            podcast: podcast,
            title: "Hidden",
            audioURL: URL(string: "https://example.com/3.mp3")!,
            publishedAt: .now
        )
        context.insert(episode)
        try context.save()

        let scheduler = RecordingScheduling()
        let inner = NewEpisodeNotificationScheduler(
            scheduler: scheduler,
            authorizer: AlwaysAuthorized(),
            ledger: InMemoryLedger()
        )
        let announcer = SwiftDataNewEpisodeAnnouncer(
            modelContainer: container,
            scheduler: inner,
            appWideEnabledProvider: { false }
        )

        await announcer.announce(newEpisodes: [
            FeedRefreshNewEpisode(
                canonicalEpisodeKey: episode.canonicalEpisodeKey,
                audioURL: episode.audioURL,
                publishedAt: episode.publishedAt
            )
        ])

        #expect(scheduler.requests.isEmpty)
    }
}
