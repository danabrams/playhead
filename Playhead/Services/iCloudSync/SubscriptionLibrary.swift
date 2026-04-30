// SubscriptionLibrary.swift
// playhead-5c1t — bridge between SwiftData `Podcast` rows and the
// CloudKit-friendly `SubscriptionRecord` value type.
//
// Two directions:
//   * snapshot() reads the local library and returns a record per
//     persisted Podcast (isRemoved=false). Used to push outgoing edits.
//   * apply(remoteRecords:) merges incoming records into SwiftData,
//     inserting fresh rows, updating existing ones, and removing rows
//     whose corresponding record is a tombstone.
//
// MainActor-isolated because SwiftData's `ModelContext` is not Sendable
// across actors. Call sites that already live on the main actor can use
// this directly; the sync coordinator hops to the main actor when it
// needs to apply remote changes.

import Foundation
import OSLog
import SwiftData

@MainActor
final class SubscriptionLibrary {
    private let logger = Logger(subsystem: "com.playhead", category: "iCloudSync.Library")
    private let modelContext: ModelContext

    init(modelContext: ModelContext) {
        self.modelContext = modelContext
    }

    /// Snapshot of every persisted `Podcast` as a `SubscriptionRecord`.
    /// Tombstones are NOT generated here — the caller emits a tombstone
    /// when it deletes a row.
    func snapshot() -> [SubscriptionRecord] {
        let podcasts = (try? modelContext.fetch(FetchDescriptor<Podcast>())) ?? []
        return podcasts.map { podcast in
            SubscriptionRecord(
                feedURL: podcast.feedURL,
                title: podcast.title,
                author: podcast.author,
                artworkURL: podcast.artworkURL,
                subscribedAt: podcast.subscribedAt,
                isRemoved: false,
                lastModified: podcast.subscribedAt
            )
        }
    }

    /// Apply the given remote records to the local library. New URLs
    /// insert fresh rows; existing URLs update in place; tombstones
    /// remove rows. The merge is idempotent — running with the same
    /// records twice produces the same library state.
    func apply(remoteRecords: [SubscriptionRecord]) throws {
        let existing = (try? modelContext.fetch(FetchDescriptor<Podcast>())) ?? []
        let byFeedURL = Dictionary(uniqueKeysWithValues: existing.map { ($0.feedURL, $0) })

        for record in remoteRecords {
            if record.isRemoved {
                if let podcast = byFeedURL[record.feedURL] {
                    modelContext.delete(podcast)
                }
                continue
            }
            if let podcast = byFeedURL[record.feedURL] {
                // Update mutable fields.
                podcast.title = record.title
                podcast.author = record.author
                podcast.artworkURL = record.artworkURL
                // `subscribedAt` is treated as the local "first added"
                // moment and is not overwritten by inbound records.
            } else {
                let fresh = Podcast(
                    feedURL: record.feedURL,
                    title: record.title,
                    author: record.author,
                    artworkURL: record.artworkURL,
                    subscribedAt: record.subscribedAt
                )
                modelContext.insert(fresh)
            }
        }
        try modelContext.save()
    }
}
