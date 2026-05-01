// NewEpisodeNotificationTapHandlerTests.swift
// playhead-snp — pure-orchestration tap handler. Given a notification
// userInfo dict, resolves the episodeKey, looks up the Episode via the
// supplied lookup, and forwards to the play handler. Silent on missing
// fields, missing rows, or wrong-trigger payloads.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("NewEpisodeNotificationTapHandler — routing (playhead-snp)")
@MainActor
struct NewEpisodeNotificationTapHandlerTests {

    private static func makeContainer() throws -> ModelContainer {
        let schema = Schema([Podcast.self, Episode.self])
        let config = ModelConfiguration(
            "TapHandlerTests",
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        return try ModelContainer(for: schema, configurations: [config])
    }

    @Test("Tap with valid episodeKey calls play handler with the resolved episode")
    func tapResolvesAndPlays() async throws {
        let container = try Self.makeContainer()
        let context = container.mainContext

        let feedURL = URL(string: "https://example.com/feed.xml")!
        let podcast = Podcast(feedURL: feedURL, title: "Show", author: "Host")
        context.insert(podcast)
        let episode = Episode(
            feedItemGUID: "g1",
            feedURL: feedURL,
            podcast: podcast,
            title: "T",
            audioURL: URL(string: "https://example.com/1.mp3")!
        )
        context.insert(episode)
        try context.save()

        var played: [String] = []
        let handler = NewEpisodeNotificationTapHandler(
            modelContainer: container,
            playEpisode: { ep in played.append(ep.canonicalEpisodeKey) }
        )

        await handler.handle(userInfo: [
            "trigger": "newEpisode",
            "episodeKey": episode.canonicalEpisodeKey,
            "feedURL": feedURL.absoluteString,
        ])

        #expect(played == [episode.canonicalEpisodeKey])
    }

    @Test("Tap with no episodeKey field is a silent no-op")
    func missingKeyIsNoOp() async throws {
        let container = try Self.makeContainer()
        var played: [String] = []
        let handler = NewEpisodeNotificationTapHandler(
            modelContainer: container,
            playEpisode: { ep in played.append(ep.canonicalEpisodeKey) }
        )

        await handler.handle(userInfo: ["trigger": "newEpisode"])
        #expect(played.isEmpty)
    }

    @Test("Tap whose episodeKey no longer resolves is a silent no-op")
    func unresolvedKeyIsNoOp() async throws {
        let container = try Self.makeContainer()
        var played: [String] = []
        let handler = NewEpisodeNotificationTapHandler(
            modelContainer: container,
            playEpisode: { ep in played.append(ep.canonicalEpisodeKey) }
        )

        await handler.handle(userInfo: [
            "trigger": "newEpisode",
            "episodeKey": "https://example.com/none::ghost",
        ])
        #expect(played.isEmpty)
    }

    @Test("Tap from a non-newEpisode trigger is ignored")
    func wrongTriggerIsIgnored() async throws {
        let container = try Self.makeContainer()
        let context = container.mainContext

        let feedURL = URL(string: "https://example.com/feed.xml")!
        let podcast = Podcast(feedURL: feedURL, title: "Show", author: "Host")
        context.insert(podcast)
        let episode = Episode(
            feedItemGUID: "g1",
            feedURL: feedURL,
            podcast: podcast,
            title: "T",
            audioURL: URL(string: "https://example.com/1.mp3")!
        )
        context.insert(episode)
        try context.save()

        var played: [String] = []
        let handler = NewEpisodeNotificationTapHandler(
            modelContainer: container,
            playEpisode: { ep in played.append(ep.canonicalEpisodeKey) }
        )

        await handler.handle(userInfo: [
            "trigger": "tripReady",
            "episodeKey": episode.canonicalEpisodeKey,
        ])

        #expect(played.isEmpty)
    }

    @Test("Summary trigger is acknowledged but does not invoke play handler")
    func summaryTriggerNoPlay() async throws {
        let container = try Self.makeContainer()
        var played: [String] = []
        let handler = NewEpisodeNotificationTapHandler(
            modelContainer: container,
            playEpisode: { ep in played.append(ep.canonicalEpisodeKey) }
        )

        await handler.handle(userInfo: [
            "trigger": "newEpisodeSummary",
            "overflow": 5,
        ])

        #expect(played.isEmpty)
    }
}
