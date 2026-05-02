// SwiftDataNewEpisodeAnnouncer.swift
// playhead-snp — Production conformer of `NewEpisodeAnnouncing`. Hops
// onto the MainActor (where `ModelContext` lives), resolves each
// `canonicalEpisodeKey` to a SwiftData `Episode` row + its parent
// `Podcast`, and forwards `[NewEpisodeCandidate]` to the shared
// `NewEpisodeNotificationScheduler`.
//
// Keys that no longer resolve (subscription deleted, episode pruned)
// are silently dropped — the user notification is best-effort and a
// "subscribe-and-forget" race is not user-visible.
//
// The announcer is constructed once per runtime and held by the
// background-feed-refresh wiring; it has no per-fire state of its own
// beyond the captured collaborators.

import Foundation
import OSLog
import SwiftData

/// Production announcer that bridges the `NewEpisodeAnnouncing`
/// (Sendable, off-main) protocol contract used by
/// `BackgroundFeedRefreshService` to the MainActor-pinned
/// `NewEpisodeNotificationScheduler`.
///
/// The struct itself is `Sendable` by virtue of its only stored fields
/// (`ModelContainer`, `NewEpisodeNotificationScheduler`, value-type
/// closure) being Sendable / MainActor-pinned. The single async hop
/// inside `announce` jumps to the MainActor before touching the
/// `ModelContext`.
struct SwiftDataNewEpisodeAnnouncer: NewEpisodeAnnouncing {

    let modelContainer: ModelContainer
    let scheduler: NewEpisodeNotificationScheduler
    let appWideEnabledProvider: @Sendable @MainActor () -> Bool

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "NewEpisodeAnnouncer"
    )

    init(
        modelContainer: ModelContainer,
        scheduler: NewEpisodeNotificationScheduler,
        appWideEnabledProvider: @escaping @Sendable @MainActor () -> Bool
    ) {
        self.modelContainer = modelContainer
        self.scheduler = scheduler
        self.appWideEnabledProvider = appWideEnabledProvider
    }

    func announce(newEpisodes: [FeedRefreshNewEpisode]) async {
        // Snapshot the keys onto the MainActor side so the closure is
        // Sendable across the hop.
        let keys = newEpisodes.map(\.canonicalEpisodeKey)

        // Cheap short-circuit: if the app-wide toggle is off, do not
        // hit SwiftData at all. The inner scheduler would also reject
        // but skipping the fetch saves work on every refresh fire when
        // the user has disabled notifications.
        let isEnabled = await MainActor.run { appWideEnabledProvider() }
        guard isEnabled else { return }

        let candidates = await MainActor.run { [modelContainer] in
            Self.resolveCandidates(
                keys: keys,
                context: modelContainer.mainContext
            )
        }

        await scheduler.announce(candidates)
    }

    @MainActor
    private static func resolveCandidates(
        keys: [String],
        context: ModelContext
    ) -> [NewEpisodeCandidate] {
        guard !keys.isEmpty else { return [] }
        // skeptical-review-cycle-8 M3: `keys` MUST stay typed as
        // `[String]` (Array). SwiftData's `#Predicate` translator
        // handles `Array.contains` reliably across iOS / macOS Catalyst
        // builds, but `Set.contains` can fall back to a linear scan or
        // fail to translate at all on some toolchain combinations
        // (witnessed during cycle-7 H1 work in
        // `ActivitySnapshotProvider.swift:163`, fixed at
        // `PlayheadApp.swift:159-169`). Do not "tighten" this signature
        // to `Set<String>` for nicer semantics — the new-episode
        // notification path silently breaks if the predicate stops
        // matching.
        let descriptor = FetchDescriptor<Episode>(
            predicate: #Predicate { keys.contains($0.canonicalEpisodeKey) }
        )
        let rows: [Episode]
        do {
            rows = try context.fetch(descriptor)
        } catch {
            return []
        }
        return rows.compactMap { episode -> NewEpisodeCandidate? in
            guard let podcast = episode.podcast else { return nil }
            return NewEpisodeCandidate(
                feedURL: podcast.feedURL,
                feedTitle: podcast.title,
                canonicalEpisodeKey: episode.canonicalEpisodeKey,
                episodeTitle: episode.title,
                publishedAt: episode.publishedAt,
                isPlayed: episode.isPlayed,
                feedNotificationsEnabled: podcast.notificationsEnabled
            )
        }
    }
}
