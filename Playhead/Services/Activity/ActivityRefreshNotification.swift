// ActivityRefreshNotification.swift
// Single notification name used by the scheduler to nudge the Activity
// screen view-model to re-aggregate its snapshot.
//
// Scope: playhead-quh7 (Phase 2 deliverable 4 — Activity screen).
//
// Why a free-standing namespace under `Services/Activity/`:
//   - The producer is the scheduler (`AnalysisWorkScheduler`) and the
//     consumer is the Activity SwiftUI view. Putting the symbol on
//     either side would create an awkward layering dependency
//     (Services → Views, or Views → Services). A neutral namespace
//     under `Services/Activity/` (which already houses `LiveActivityCopy`
//     and `LiveActivitySnapshotProvider`) is the right home.
//
// Why a notification (not an `AsyncSequence` or `@Observable`):
//   - The scheduler is an `actor` and the Activity view is `@MainActor`
//     SwiftUI. `NotificationCenter` cleanly bridges those two worlds
//     without forcing the scheduler to expose an `AsyncSequence` of
//     state changes (a larger surface than v1 needs) or hold an
//     `@Observable` shared store (which would conflate scheduler-state
//     truth with the snapshot the view aggregates).
//
// Refresh discipline (per bead spec): the Activity view subscribes to
// this notification as its sole refresh trigger. There is NO Timer-
// based polling.

import Foundation

/// Notification posted by the scheduler at job-lifecycle edges
/// (`didStart` / `didFinish`) so the Activity view re-aggregates its
/// snapshot.
enum ActivityRefreshNotification {
    /// The single registered notification name. Stable string token so
    /// the producer (`AnalysisWorkScheduler.postActivityRefreshNotification`)
    /// and consumer (`ActivityView.onReceive`) cannot drift apart.
    static let name = Notification.Name("com.playhead.activity.snapshot.shouldRefresh")
}
