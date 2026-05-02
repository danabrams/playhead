// PodcastKeepFullMusicSchemaTests.swift
// playhead-epii — Pin the additive schema field on `Podcast` that
// drives the per-show silence-compression override. Default OFF
// (compression-on by default per spec).

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("Podcast.keepFullMusic schema (playhead-epii)")
@MainActor
struct PodcastKeepFullMusicSchemaTests {

    @Test("Podcast.keepFullMusic defaults to false")
    func defaultsFalse() {
        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "T",
            author: "A"
        )
        #expect(podcast.keepFullMusic == false)
    }

    @Test("Podcast.keepFullMusic survives a SwiftData round-trip")
    func roundTrips() throws {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(
            "PodcastKeepFullMusicTests.podcast",
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
        podcast.keepFullMusic = true
        context.insert(podcast)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<Podcast>()).first
        #expect(fetched?.keepFullMusic == true)
    }
}
