// SubscriptionRecord.swift
// playhead-5c1t — wire-format representation of a podcast subscription
// for iCloud sync via CloudKit.
//
// Lives independent of the SwiftData `Podcast` `@Model` so:
//   * Tests can construct records without a SwiftData stack.
//   * The CloudKit schema can evolve independently of local persistence.
//
// Conflict-resolution rule for subscriptions: append-with-tombstone set,
// merged by `lastModified` (newer wins). A later remove (tombstone) DOES
// beat an earlier add — otherwise unsubscribe never propagates across
// devices. A later re-add likewise revives a tombstoned subscription.

import CloudKit
import CryptoKit
import Foundation

/// CKRecord-friendly representation of a podcast subscription.
///
/// - The CKRecord ID is derived from the feed URL so two devices that
///   subscribe to the same show converge on a single record (idempotent
///   upsert) instead of creating duplicates.
/// - `lastModified` is the merge timestamp. The struct itself does not
///   set this — call sites must update it whenever a field changes.
struct SubscriptionRecord: Equatable, Sendable {
    /// CKRecord type name. Stable wire identifier — DO NOT rename without
    /// a CloudKit schema migration.
    static let recordType: String = "PodcastSubscription"

    /// Field names stored on the CKRecord. Hoisted so the encode/decode
    /// paths stay in sync without typo'd string literals.
    enum Field {
        static let feedURL = "feedURL"
        static let title = "title"
        static let author = "author"
        static let artworkURL = "artworkURL"
        static let subscribedAt = "subscribedAt"
        static let isRemoved = "isRemoved"
        static let lastModified = "lastModified"
    }

    var feedURL: URL
    var title: String
    var author: String
    var artworkURL: URL?
    var subscribedAt: Date
    /// Tombstone flag. `true` means the user unsubscribed; the record is
    /// retained so that "remove" can propagate to other devices.
    var isRemoved: Bool
    /// Merge timestamp. The most-recent `lastModified` wins.
    var lastModified: Date

    // MARK: - Errors

    enum DecodeError: Error, Equatable {
        case missingField(String)
        case typeMismatch(String)
    }

    // MARK: - CKRecord

    /// Stable record name derived from the feed URL via SHA-256. Two
    /// devices subscribing to the same feed URL produce the same record
    /// ID, which is what makes saves idempotent upserts.
    static func recordID(forFeedURL url: URL) -> CKRecord.ID {
        let digest = SHA256.hash(data: Data(url.absoluteString.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        // CKRecord names cap at 255 chars; SHA-256 hex is 64 chars.
        return CKRecord.ID(recordName: "sub_\(hex)")
    }

    /// Encode to a fresh CKRecord. Production saves merge with the live
    /// server-side record by ID; this method does not preserve change
    /// tags from a prior fetch (callers must reuse a fetched CKRecord
    /// when the server-change-token path requires it).
    func toCKRecord() -> CKRecord {
        let record = CKRecord(
            recordType: Self.recordType,
            recordID: Self.recordID(forFeedURL: feedURL)
        )
        record[Field.feedURL] = feedURL.absoluteString as NSString
        record[Field.title] = title as NSString
        record[Field.author] = author as NSString
        if let artworkURL {
            record[Field.artworkURL] = artworkURL.absoluteString as NSString
        }
        record[Field.subscribedAt] = subscribedAt as NSDate
        record[Field.isRemoved] = (isRemoved ? 1 : 0) as NSNumber
        record[Field.lastModified] = lastModified as NSDate
        return record
    }

    /// Decode from a CKRecord. Throws `DecodeError` when required fields
    /// are absent — production callers should treat such records as a
    /// schema-skew signal and skip them rather than crashing.
    init(ckRecord: CKRecord) throws {
        guard let feedURLString = ckRecord[Field.feedURL] as? String else {
            throw DecodeError.missingField(Field.feedURL)
        }
        guard let feedURL = URL(string: feedURLString) else {
            throw DecodeError.typeMismatch(Field.feedURL)
        }
        guard let title = ckRecord[Field.title] as? String else {
            throw DecodeError.missingField(Field.title)
        }
        guard let author = ckRecord[Field.author] as? String else {
            throw DecodeError.missingField(Field.author)
        }
        guard let subscribedAt = ckRecord[Field.subscribedAt] as? Date else {
            throw DecodeError.missingField(Field.subscribedAt)
        }
        guard let lastModified = ckRecord[Field.lastModified] as? Date else {
            throw DecodeError.missingField(Field.lastModified)
        }
        let isRemovedNum = ckRecord[Field.isRemoved] as? NSNumber
        let isRemoved = (isRemovedNum?.boolValue ?? false)

        let artworkURL: URL? = {
            if let s = ckRecord[Field.artworkURL] as? String {
                return URL(string: s)
            }
            return nil
        }()

        self.init(
            feedURL: feedURL,
            title: title,
            author: author,
            artworkURL: artworkURL,
            subscribedAt: subscribedAt,
            isRemoved: isRemoved,
            lastModified: lastModified
        )
    }

    init(
        feedURL: URL,
        title: String,
        author: String,
        artworkURL: URL?,
        subscribedAt: Date,
        isRemoved: Bool,
        lastModified: Date
    ) {
        self.feedURL = feedURL
        self.title = title
        self.author = author
        self.artworkURL = artworkURL
        self.subscribedAt = subscribedAt
        self.isRemoved = isRemoved
        self.lastModified = lastModified
    }

    // MARK: - Merge

    /// Deterministic last-modified-wins merge. Tombstones (isRemoved)
    /// participate by their own `lastModified`, so a remove that lands
    /// after an add wins, and a re-add that lands after a remove revives
    /// the subscription. Ties break on `local` (the device-local copy)
    /// so that a network round-trip with no real change is a no-op.
    static func merge(local: SubscriptionRecord, remote: SubscriptionRecord) -> SubscriptionRecord {
        if remote.lastModified > local.lastModified {
            return remote
        }
        return local
    }
}
