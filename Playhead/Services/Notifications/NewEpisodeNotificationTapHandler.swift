// NewEpisodeNotificationTapHandler.swift
// playhead-snp — Pure-orchestration tap-routing for new-episode local
// notifications. Decoupled from the live `UNUserNotificationCenterDelegate`
// so tests can drive the routing logic with a plain `[AnyHashable: Any]`.
//
// Behavior contract:
//   - Only routes payloads whose `trigger` is `"newEpisode"` (the
//     individual case). The `"newEpisodeSummary"` trigger is acknowledged
//     but does not invoke the play handler — there's no single episode
//     to route to. A future scope can navigate the user to the Library;
//     this handler stays minimal.
//   - Missing `episodeKey`, missing row, or wrong trigger are silent
//     no-ops — the user has tapped a notification, but the underlying
//     state has shifted (subscription deleted, episode pruned). Crashing
//     or surfacing an alert would be worse than nothing.

import Foundation
import OSLog
import SwiftData

/// Resolves a `userInfo` payload into a Library `Episode` row and hands
/// it to an injected play handler. Held by the runtime; called from the
/// `UNUserNotificationCenterDelegate.didReceive` adapter.
@MainActor
struct NewEpisodeNotificationTapHandler {

    let modelContainer: ModelContainer
    let playEpisode: @MainActor (Episode) async -> Void

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "NewEpisodeTapHandler"
    )

    init(
        modelContainer: ModelContainer,
        playEpisode: @escaping @MainActor (Episode) async -> Void
    ) {
        self.modelContainer = modelContainer
        self.playEpisode = playEpisode
    }

    /// Top-level dispatch from the notification-center delegate.
    /// `userInfo` arrives as the loosely-typed `[AnyHashable: Any]` the
    /// system gives us; we extract the strongly-typed fields and reject
    /// anything that doesn't match our contract.
    func handle(userInfo: [AnyHashable: Any]) async {
        // Trigger gate.
        guard let trigger = userInfo["trigger"] as? String else { return }
        guard trigger == "newEpisode" else {
            // newEpisodeSummary acknowledged but no per-episode play.
            return
        }

        guard let episodeKey = userInfo["episodeKey"] as? String else {
            Self.logger.info("Ignoring tap: missing episodeKey")
            return
        }

        let descriptor = FetchDescriptor<Episode>(
            predicate: #Predicate { $0.canonicalEpisodeKey == episodeKey }
        )
        let context = modelContainer.mainContext
        guard let episode = try? context.fetch(descriptor).first else {
            Self.logger.info(
                "Ignoring tap: no Episode for key \(episodeKey, privacy: .public)"
            )
            return
        }

        await playEpisode(episode)
    }
}
