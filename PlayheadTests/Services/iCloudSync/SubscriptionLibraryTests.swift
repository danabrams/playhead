// SubscriptionLibraryTests.swift
// playhead-5c1t — TDD Cycle 5: SubscriptionLibrary bridge between
// SwiftData `Podcast` rows and the CloudKit-friendly
// `SubscriptionRecord` value type.
//
// The bridge owns:
//   * Translating a SwiftData `Podcast` into a `SubscriptionRecord` for
//     push.
//   * Applying an inbound `SubscriptionRecord` to the local SwiftData
//     library (insert / update / tombstone).
//   * Avoiding double-creation when the initial-fetch path lands a
//     subscription that is already present locally.
//
// Tests run against a real in-memory SwiftData container so the
// behavior under @Model uniqueness + relationship rules is the same one
// production hits.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@MainActor
@Suite("SubscriptionLibrary — SwiftData ↔ SubscriptionRecord bridge")
struct SubscriptionLibraryTests {

    private static func makeContext() throws -> ModelContext {
        let schema = Schema([Podcast.self, Episode.self])
        let config = ModelConfiguration(
            "subscription-library-tests",
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(for: schema, configurations: [config])
        return ModelContext(container)
    }

    @Test("snapshot() returns one record per persisted Podcast (isRemoved=false)")
    func snapshotMapsPodcasts() throws {
        let ctx = try Self.makeContext()
        let library = SubscriptionLibrary(modelContext: ctx)

        let p = Podcast(
            feedURL: URL(string: "https://a.com/f")!,
            title: "Show A",
            author: "Author",
            artworkURL: URL(string: "https://a.com/art.jpg")
        )
        ctx.insert(p)
        try ctx.save()

        let snapshots = library.snapshot()
        #expect(snapshots.count == 1)
        let only = try #require(snapshots.first)
        #expect(only.feedURL == p.feedURL)
        #expect(only.title == "Show A")
        #expect(only.isRemoved == false)
    }

    @Test("apply(remoteRecords:) inserts a brand-new podcast row")
    func applyInsertsNew() throws {
        let ctx = try Self.makeContext()
        let library = SubscriptionLibrary(modelContext: ctx)

        let record = SubscriptionRecord(
            feedURL: URL(string: "https://a.com/f")!,
            title: "Show A",
            author: "Auth",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_000)
        )
        try library.apply(remoteRecords: [record])
        let podcasts = try ctx.fetch(FetchDescriptor<Podcast>())
        #expect(podcasts.count == 1)
        #expect(podcasts.first?.title == "Show A")
    }

    @Test("apply(remoteRecords:) does NOT double-insert when a Podcast with the same feedURL exists")
    func applyIdempotentForExisting() throws {
        let ctx = try Self.makeContext()
        let library = SubscriptionLibrary(modelContext: ctx)

        let url = URL(string: "https://a.com/f")!
        ctx.insert(Podcast(feedURL: url, title: "Local", author: "Local"))
        try ctx.save()

        let remote = SubscriptionRecord(
            feedURL: url,
            title: "Remote",
            author: "Remote",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_500),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_500)
        )
        try library.apply(remoteRecords: [remote])

        let podcasts = try ctx.fetch(FetchDescriptor<Podcast>())
        #expect(podcasts.count == 1, "Same feedURL must collapse to a single row.")
        // Title was updated to the remote value.
        #expect(podcasts.first?.title == "Remote")
    }

    @Test("apply(remoteRecords:) deletes the Podcast when a tombstone (isRemoved=true) lands")
    func applyTombstoneDeletes() throws {
        let ctx = try Self.makeContext()
        let library = SubscriptionLibrary(modelContext: ctx)

        let url = URL(string: "https://a.com/f")!
        ctx.insert(Podcast(feedURL: url, title: "Local", author: "Local"))
        try ctx.save()

        let removal = SubscriptionRecord(
            feedURL: url,
            title: "Local",
            author: "Local",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: true,
            lastModified: Date(timeIntervalSince1970: 1_700_000_500)
        )
        try library.apply(remoteRecords: [removal])

        let podcasts = try ctx.fetch(FetchDescriptor<Podcast>())
        #expect(podcasts.isEmpty,
                "Tombstone applied locally must remove the Podcast row.")
    }

    @Test("apply(remoteRecords:) is a no-op when the record is a tombstone for a podcast we never had")
    func applyTombstoneOfMissingIsNoOp() throws {
        let ctx = try Self.makeContext()
        let library = SubscriptionLibrary(modelContext: ctx)
        let removal = SubscriptionRecord(
            feedURL: URL(string: "https://nope.com/f")!,
            title: "x",
            author: "x",
            artworkURL: nil,
            subscribedAt: .now,
            isRemoved: true,
            lastModified: .now
        )
        try library.apply(remoteRecords: [removal])
        let podcasts = try ctx.fetch(FetchDescriptor<Podcast>())
        #expect(podcasts.isEmpty)
    }
}
