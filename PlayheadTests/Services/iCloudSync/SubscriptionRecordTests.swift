// SubscriptionRecordTests.swift
// playhead-5c1t — TDD Cycle 1: SubscriptionRecord ↔ CKRecord marshaling.
//
// `SubscriptionRecord` is the wire-format representation of a podcast
// subscription that crosses the local SwiftData boundary into CloudKit.
// We deliberately keep this struct independent of `Podcast` (the SwiftData
// `@Model`) so that:
//   * Tests can construct records without spinning up a SwiftData stack.
//   * The CloudKit schema can evolve independently of local persistence.
//
// Conflict-resolution rule: subscriptions are an append-with-tombstone
// set. The record carries `isRemoved: Bool` plus `lastModified: Date`;
// merge picks the most recent `lastModified` per `feedURL`.

import CloudKit
import Foundation
import Testing

@testable import Playhead

@Suite("SubscriptionRecord — CKRecord round-trip + merge rules")
struct SubscriptionRecordTests {

    // MARK: - Marshaling

    @Test("Round-trips through CKRecord losslessly")
    func ckRecordRoundTrip() throws {
        let original = SubscriptionRecord(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Example Show",
            author: "Jane Doe",
            artworkURL: URL(string: "https://example.com/art.jpg")!,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_500)
        )

        let record = original.toCKRecord()
        let decoded = try SubscriptionRecord(ckRecord: record)

        #expect(decoded == original)
    }

    @Test("CKRecord uses feedURL hash as recordName for idempotent upsert")
    func recordNameIsDeterministic() {
        let url = URL(string: "https://example.com/feed.xml")!
        let r1 = SubscriptionRecord(
            feedURL: url,
            title: "A",
            author: "A",
            artworkURL: nil,
            subscribedAt: .now,
            isRemoved: false,
            lastModified: .now
        )
        let r2 = SubscriptionRecord(
            feedURL: url,
            title: "B (renamed on device 2)",
            author: "B",
            artworkURL: nil,
            subscribedAt: .now.addingTimeInterval(60),
            isRemoved: false,
            lastModified: .now.addingTimeInterval(60)
        )

        #expect(r1.toCKRecord().recordID == r2.toCKRecord().recordID,
                "Two records with the same feedURL must share a CKRecord ID so saves overwrite rather than duplicate.")
    }

    @Test("Decoding fails when required fields are missing")
    func decodeRejectsMalformed() {
        let bare = CKRecord(recordType: SubscriptionRecord.recordType)
        #expect(throws: SubscriptionRecord.DecodeError.self) {
            _ = try SubscriptionRecord(ckRecord: bare)
        }
    }

    // MARK: - Merge

    @Test("Merge: newer lastModified wins")
    func mergeNewerWins() {
        let url = URL(string: "https://example.com/feed.xml")!
        let older = SubscriptionRecord(
            feedURL: url,
            title: "Old title",
            author: "x",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_100)
        )
        let newer = SubscriptionRecord(
            feedURL: url,
            title: "New title",
            author: "y",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_500)
        )
        #expect(SubscriptionRecord.merge(local: older, remote: newer) == newer)
        #expect(SubscriptionRecord.merge(local: newer, remote: older) == newer)
    }

    @Test("Merge: tombstone (isRemoved=true) loses to a later add")
    func mergeTombstoneLater() {
        let url = URL(string: "https://example.com/feed.xml")!
        let removed = SubscriptionRecord(
            feedURL: url,
            title: "x",
            author: "x",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: true,
            lastModified: Date(timeIntervalSince1970: 1_700_000_100)
        )
        let readded = SubscriptionRecord(
            feedURL: url,
            title: "x",
            author: "x",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_500),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_500)
        )
        #expect(SubscriptionRecord.merge(local: removed, remote: readded) == readded)
    }

    @Test("Merge: tombstone WINS when its lastModified is newer")
    func mergeTombstoneWinsLater() {
        let url = URL(string: "https://example.com/feed.xml")!
        let added = SubscriptionRecord(
            feedURL: url,
            title: "x",
            author: "x",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: false,
            lastModified: Date(timeIntervalSince1970: 1_700_000_100)
        )
        let removed = SubscriptionRecord(
            feedURL: url,
            title: "x",
            author: "x",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: 1_700_000_000),
            isRemoved: true,
            lastModified: Date(timeIntervalSince1970: 1_700_000_500)
        )
        #expect(SubscriptionRecord.merge(local: added, remote: removed) == removed,
                "A later remove must beat an earlier add — otherwise unsubscribe never propagates across devices.")
    }
}
