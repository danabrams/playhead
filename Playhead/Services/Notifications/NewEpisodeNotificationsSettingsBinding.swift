// NewEpisodeNotificationsSettingsBinding.swift
// playhead-snp — Settings-glue helper. Wraps the read/write of the
// `UserPreferences.newEpisodeNotificationsEnabled` flag with the
// cancel-pending side-effect required when the user opts out: any
// previously-scheduled new-episode notifications that haven't yet been
// delivered are removed so they don't fire after the switch.
//
// The Settings view passes this binding to a SwiftUI `Toggle`'s
// `Binding<Bool>` so the side-effect is purely a function of the
// transition (OFF) — no auxiliary observers needed.

import Foundation
import UserNotifications

/// Side-effect surface invoked when the master toggle flips OFF.
/// Production wires this to the live `UNUserNotificationCenter`
/// (removing pending requests whose categoryIdentifier is the new-
/// episode category). Tests pass a recording double.
protocol PendingNewEpisodeNotificationCancelling: Sendable {
    func cancelAllPendingNewEpisodeNotifications() async
}

/// Production cancelling backed by `UNUserNotificationCenter`.
/// Filters pending requests by `categoryIdentifier` so we only remove
/// new-episode notifications, not (e.g.) batch notifications scheduled
/// by the trip-ready coordinator.
struct SystemPendingNewEpisodeCancellation: PendingNewEpisodeNotificationCancelling {

    func cancelAllPendingNewEpisodeNotifications() async {
        let center = UNUserNotificationCenter.current()
        let pending = await center.pendingNotificationRequests()
        let toRemove = pending.filter { request in
            let cat = request.content.categoryIdentifier
            return cat == NewEpisodeNotificationScheduler.categoryIdentifier
                || cat == NewEpisodeNotificationScheduler.summaryCategoryIdentifier
        }
        guard !toRemove.isEmpty else { return }
        center.removePendingNotificationRequests(
            withIdentifiers: toRemove.map(\.identifier)
        )
    }
}

/// Settings-side binding wrapper. Holds a read closure (so SwiftUI re-
/// renders pick up the latest stored value) and a write closure (so
/// Swift's value-type Binding can write through to the live SwiftData
/// model). The cancellation hook fires only on the OFF transition.
@MainActor
struct NewEpisodeNotificationsSettingsBinding {

    let current: @MainActor () -> Bool
    let update: @MainActor (Bool) -> Void
    let cancellation: any PendingNewEpisodeNotificationCancelling

    init(
        current: @escaping @MainActor () -> Bool,
        update: @escaping @MainActor (Bool) -> Void,
        cancellation: any PendingNewEpisodeNotificationCancelling = SystemPendingNewEpisodeCancellation()
    ) {
        self.current = current
        self.update = update
        self.cancellation = cancellation
    }

    /// Apply a new value. No-op when unchanged. Triggers the cancel-
    /// pending hook only on the true→false transition.
    func set(_ newValue: Bool) async {
        let previous = current()
        guard previous != newValue else { return }
        update(newValue)
        if previous == true && newValue == false {
            await cancellation.cancelAllPendingNewEpisodeNotifications()
        }
    }
}
