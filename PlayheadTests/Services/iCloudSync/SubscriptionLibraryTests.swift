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

    // MARK: - Cold-start hop

    @Test("bootstrapFromRemote: pre-seeded coordinator records land in the local library")
    func bootstrapFromRemotePopulatesLibrary() async throws {
        let ctx = try Self.makeContext()
        let library = SubscriptionLibrary(modelContext: ctx)

        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let seed = SubscriptionRecord(
            feedURL: URL(string: "https://a.com/f")!,
            title: "Server Show",
            author: "Server Author",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_000)
        )
        await provider.seed(seed.toCKRecord())
        let coordinator = ICloudSyncCoordinator(provider: provider)

        try await library.bootstrapFromRemote(coordinator: coordinator)

        let podcasts = try ctx.fetch(FetchDescriptor<Podcast>())
        #expect(podcasts.count == 1)
        #expect(podcasts.first?.title == "Server Show")
        #expect(podcasts.first?.feedURL == seed.feedURL)
    }

    @Test("bootstrapFromRemote: signed-out is a no-op (no apply, no throw)")
    func bootstrapFromRemoteSignedOutIsNoOp() async throws {
        let ctx = try Self.makeContext()
        let library = SubscriptionLibrary(modelContext: ctx)

        let provider = FakeCloudKitProvider(initialAccountStatus: .noAccount)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        try await library.bootstrapFromRemote(coordinator: coordinator)

        let podcasts = try ctx.fetch(FetchDescriptor<Podcast>())
        #expect(podcasts.isEmpty)
    }

    @Test("bootstrapFromRemote: uploads local-only subscriptions the server doesn't know about")
    func bootstrapFromRemoteUploadsLocalOnly() async throws {
        // Cycle 2 H4 fix: a podcast that exists locally but is NOT in
        // the remote set must be pushed up, not silently destroyed.
        let ctx = try Self.makeContext()
        let library = SubscriptionLibrary(modelContext: ctx)

        // Local-only podcast — user added before iCloud was reachable.
        let localURL = URL(string: "https://local.com/feed")!
        ctx.insert(Podcast(
            feedURL: localURL,
            title: "Local Show",
            author: "Local Author"
        ))
        try ctx.save()

        // Server has a different podcast.
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let serverURL = URL(string: "https://server.com/feed")!
        let serverRecord = SubscriptionRecord(
            feedURL: serverURL,
            title: "Server Show",
            author: "Server Author",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_000)
        )
        await provider.seed(serverRecord.toCKRecord())
        let coordinator = ICloudSyncCoordinator(provider: provider)

        try await library.bootstrapFromRemote(coordinator: coordinator)

        // Local: now has both.
        let podcasts = try ctx.fetch(FetchDescriptor<Podcast>())
        let urls = Set(podcasts.map(\.feedURL))
        #expect(urls == [localURL, serverURL],
                "Local-only podcast must NOT be deleted; remote podcast must be inserted.")

        // Remote: now has both — local-only got pushed up.
        let saved = await provider.records
        let savedURLs = Set(saved.compactMap { try? SubscriptionRecord(ckRecord: $0).feedURL })
        #expect(savedURLs == [localURL, serverURL],
                "Local-only subscription must be uploaded so other devices see it.")
    }

    // MARK: - Writer-tap helpers

    @Test("subscribedRecord(for:) carries Podcast fields and isRemoved=false")
    func subscribedRecordCarriesFields() throws {
        let url = URL(string: "https://a.com/f")!
        let podcast = Podcast(
            feedURL: url,
            title: "Show",
            author: "Author",
            artworkURL: URL(string: "https://a.com/art.jpg")
        )
        let now = Date(timeIntervalSince1970: 1_700_000_500)
        let record = SubscriptionLibrary.subscribedRecord(for: podcast, now: now)

        #expect(record.feedURL == url)
        #expect(record.title == "Show")
        #expect(record.author == "Author")
        #expect(record.isRemoved == false)
        #expect(record.lastModified == now,
                "lastModified must reflect the fresh subscribe so concurrent-device merges resolve in favor of the new edit.")
    }

    @Test("tombstoneRecord(for:) preserves Podcast metadata + sets isRemoved=true")
    func tombstoneRecordSetsRemoved() throws {
        let url = URL(string: "https://a.com/f")!
        let podcast = Podcast(
            feedURL: url,
            title: "Show",
            author: "Author",
            artworkURL: nil
        )
        let now = Date(timeIntervalSince1970: 1_700_000_500)
        let record = SubscriptionLibrary.tombstoneRecord(for: podcast, now: now)

        #expect(record.feedURL == url)
        #expect(record.title == "Show")
        #expect(record.isRemoved == true,
                "Tombstone propagates the unsubscribe to other devices.")
        #expect(record.lastModified == now)
    }

    @Test("Writer-tap: subscribedRecord pushed via coordinator lands in CloudKit")
    func subscribedRecordRoundTripsThroughCoordinator() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)

        let url = URL(string: "https://a.com/f")!
        let podcast = Podcast(feedURL: url, title: "Show", author: "Author")
        let record = SubscriptionLibrary.subscribedRecord(for: podcast)
        try await coordinator.upsertSubscriptionMerging(record)

        let saved = await provider.records
        #expect(saved.count == 1)
        let decoded = try #require(saved.first.flatMap { try? SubscriptionRecord(ckRecord: $0) })
        #expect(decoded.feedURL == url)
        #expect(decoded.isRemoved == false)
    }

    @Test("Writer-tap: tombstoneRecord pushed via coordinator marks server record removed")
    func tombstoneRecordRoundTripsThroughCoordinator() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        // Server already has the add.
        let url = URL(string: "https://a.com/f")!
        let priorAdd = SubscriptionRecord(
            feedURL: url,
            title: "Show",
            author: "Author",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_000)
        )
        await provider.seed(priorAdd.toCKRecord())

        let coordinator = ICloudSyncCoordinator(provider: provider)
        let podcast = Podcast(feedURL: url, title: "Show", author: "Author")
        let tombstone = SubscriptionLibrary.tombstoneRecord(
            for: podcast,
            now: Date(timeIntervalSince1970: 1_700_000_500)
        )
        try await coordinator.upsertSubscriptionMerging(tombstone)

        let serverAfter = try await provider.fetch(
            recordID: SubscriptionRecord.recordID(forFeedURL: url)
        )
        let decoded = try #require(serverAfter.flatMap { try? SubscriptionRecord(ckRecord: $0) })
        #expect(decoded.isRemoved == true,
                "Newer tombstone must overwrite the older add on the server.")
    }
}
