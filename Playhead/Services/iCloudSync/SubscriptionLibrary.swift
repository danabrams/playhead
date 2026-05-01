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

    /// Build the wire-format record describing a freshly-subscribed
    /// podcast. The record's `lastModified` is `now` so a concurrent
    /// device's later edit (rename / artwork update) will win the
    /// merge naturally. Pure — doesn't touch CloudKit; the call site
    /// pushes via the coordinator.
    static func subscribedRecord(for podcast: Podcast, now: Date = .now) -> SubscriptionRecord {
        SubscriptionRecord(
            feedURL: podcast.feedURL,
            title: podcast.title,
            author: podcast.author,
            artworkURL: podcast.artworkURL,
            subscribedAt: podcast.subscribedAt,
            isRemoved: false,
            lastModified: now
        )
    }

    /// Build the tombstone record describing an unsubscribed podcast.
    /// `lastModified` is `now` so the remove beats any earlier add on
    /// other devices. The record retains the prior metadata so a
    /// re-subscription on another device sees coherent fields rather
    /// than `""` placeholders.
    static func tombstoneRecord(for podcast: Podcast, now: Date = .now) -> SubscriptionRecord {
        SubscriptionRecord(
            feedURL: podcast.feedURL,
            title: podcast.title,
            author: podcast.author,
            artworkURL: podcast.artworkURL,
            subscribedAt: podcast.subscribedAt,
            isRemoved: true,
            lastModified: now
        )
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

    /// Cold-start helper: ask the coordinator for the server-side
    /// subscription set, apply it into the local library, and upload
    /// any local-only subscriptions that the server didn't already
    /// know about. Hides the fetch + apply + reconcile choreography
    /// from the launch path so `PlayheadApp.task` only sees a single
    /// call. Best-effort — when the coordinator is signed-out the
    /// fetch returns empty and apply is a no-op. Errors propagate;
    /// the launch path catches and logs.
    ///
    /// Reconciliation policy: **local additions win on first sync.**
    /// Any podcast that exists locally but is NOT in the remote set
    /// is treated as a user's deliberate offline addition and is
    /// uploaded, rather than deleted. The opposite choice — "remote
    /// is authoritative; delete local-only rows" — would silently
    /// destroy podcasts the user subscribed to before iCloud was
    /// reachable.
    ///
    /// Edge case: if Device 1 restores from a stale backup AFTER
    /// Device 2 deleted some podcast Z, this policy resurrects Z on
    /// every device on the next pull. The alternative (silently
    /// dropping Z on Device 1) is strictly worse — the user has no
    /// way to recover. Persisted tombstones flowing through the
    /// normal subscription-toggle path remain the canonical mechanism
    /// for cross-device deletes.
    func bootstrapFromRemote(coordinator: ICloudSyncCoordinator) async throws {
        let records = try await coordinator.initialSubscriptionFetch()
        if !records.isEmpty {
            try apply(remoteRecords: records)
        }

        let remoteFeedURLs = Set(records.lazy.filter { !$0.isRemoved }.map(\.feedURL))
        let localOnly = (try? modelContext.fetch(FetchDescriptor<Podcast>())) ?? []
        let toUpload: [SubscriptionRecord] = localOnly
            .filter { !remoteFeedURLs.contains($0.feedURL) }
            .map { Self.subscribedRecord(for: $0, now: $0.subscribedAt) }
        guard !toUpload.isEmpty else { return }

        // Bounded-concurrency upload. Sequential `await` per podcast on a
        // 50-show library is two network round-trips × 50 = 100 RTTs on
        // the launch path. A small concurrency cap (4) cuts the wallclock
        // by ~4× while staying inside CloudKit's per-request fairness.
        // The per-record upsertSubscriptionMerging is internally guarded
        // by the coordinator's actor, so concurrent calls are serialized
        // there — the parallelism only overlaps the network waits, not
        // the merge logic.
        let concurrencyCap = 4
        await withTaskGroup(of: Void.self) { group in
            var slot = 0
            for record in toUpload {
                if slot >= concurrencyCap {
                    _ = await group.next()
                    slot -= 1
                }
                let captured = record
                group.addTask { [coordinator, logger] in
                    do {
                        _ = try await coordinator.upsertSubscriptionMerging(captured)
                    } catch {
                        logger.warning("Bootstrap upload of local-only subscription failed (\(captured.feedURL.absoluteString)): \(error.localizedDescription)")
                    }
                }
                slot += 1
            }
            await group.waitForAll()
        }
    }

    /// Apply the given remote records to the local library. New URLs
    /// insert fresh rows; existing URLs update in place; tombstones
    /// remove rows. The merge is idempotent — running with the same
    /// records twice produces the same library state.
    ///
    /// Per-feedURL dedup with last-write-wins: if the inbound array
    /// contains a tombstone and a re-add for the same `feedURL` (a
    /// race across devices), only the record with the highest
    /// `lastModified` is applied. Without this, ordering of the array
    /// could resurrect or destroy state non-deterministically and
    /// would lose any local-only fields on the re-insert path.
    ///
    /// Save semantics: SwiftData's `ModelContext.save()` flushes
    /// inserts, updates, and deletes atomically — a save-time
    /// validation failure rolls back the in-memory mutations as a
    /// unit. The throw therefore preserves the prior consistent
    /// state.
    func apply(remoteRecords: [SubscriptionRecord]) throws {
        let collapsed = Self.collapseByFeedURL(remoteRecords)
        let existing = (try? modelContext.fetch(FetchDescriptor<Podcast>())) ?? []
        // `#Unique<Podcast>([\.feedURL])` should keep these unique, but
        // defense in depth: a corrupted or partially-migrated store
        // could surface duplicates, and trapping in `Dictionary(
        // uniqueKeysWithValues:)` would crash the bootstrap path with
        // no recovery. Last-write-wins matches the per-record merge
        // contract used elsewhere in this file.
        let byFeedURL = Dictionary(existing.map { ($0.feedURL, $0) }, uniquingKeysWith: { _, last in last })

        for record in collapsed {
            if record.isRemoved {
                if let podcast = byFeedURL[record.feedURL] {
                    modelContext.delete(podcast)
                }
                continue
            }
            if let podcast = byFeedURL[record.feedURL] {
                // Update mutable fields. Local-only fields like
                // notification toggles live on Podcast and are
                // intentionally not touched here.
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

    /// Collapse multiple records sharing a `feedURL` down to the most
    /// recently-modified record. Stable order: records sort by
    /// `lastModified` ASC and the final value wins, matching the
    /// per-record merge contract.
    private static func collapseByFeedURL(_ records: [SubscriptionRecord]) -> [SubscriptionRecord] {
        var winner: [URL: SubscriptionRecord] = [:]
        for record in records {
            if let existing = winner[record.feedURL] {
                if record.lastModified >= existing.lastModified {
                    winner[record.feedURL] = record
                }
            } else {
                winner[record.feedURL] = record
            }
        }
        return Array(winner.values)
    }
}
