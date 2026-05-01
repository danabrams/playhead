// NewEpisodeNotificationSchemaTests.swift
// playhead-snp — Pin the additive schema fields that drive the new-
// episode notification toggles. Both default to ON (opt-out).

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("New-episode notification schema fields (playhead-snp)")
@MainActor
struct NewEpisodeNotificationSchemaTests {

    @Test("Podcast.notificationsEnabled defaults to true")
    func podcastDefaultIsTrue() {
        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "T",
            author: "A"
        )
        #expect(podcast.notificationsEnabled == true)
    }

    @Test("UserPreferences.newEpisodeNotificationsEnabled defaults to true")
    func userPreferencesDefaultIsTrue() {
        let prefs = UserPreferences()
        #expect(prefs.newEpisodeNotificationsEnabled == true)
    }

    @Test("Podcast.notificationsEnabled survives a SwiftData round-trip")
    func podcastFieldRoundTrips() throws {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(
            "NewEpisodeSchemaTests.podcast",
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(for: schema, configurations: [config])
        let context = container.mainContext

        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "T",
            author: "A"
        )
        podcast.notificationsEnabled = false
        context.insert(podcast)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<Podcast>()).first
        #expect(fetched?.notificationsEnabled == false)
    }

    @Test("UserPreferences.newEpisodeNotificationsEnabled survives round-trip")
    func userPreferencesFieldRoundTrips() throws {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(
            "NewEpisodeSchemaTests.prefs",
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(for: schema, configurations: [config])
        let context = container.mainContext

        let prefs = UserPreferences()
        prefs.newEpisodeNotificationsEnabled = false
        context.insert(prefs)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<UserPreferences>()).first
        #expect(fetched?.newEpisodeNotificationsEnabled == false)
    }
}
