// DownloadBatch.swift
// SwiftData @Model record for a "Download-Next-N" batch invocation.
// Owned by the BatchNotificationCoordinator pipeline (playhead-zp0x).
//
// Why this is persisted (not in-memory):
//   * Trip-context downloads are inherently long-running and span
//     foreground/background. A batch that reaches `tripReady` overnight
//     while the app is suspended must still fire the notification, which
//     means the per-batch state (cap flags, persistence-rule counters,
//     creation context) has to survive process restarts.
//   * The two-notification cap per batch lifetime is a hard contract; if
//     state is lost between launches we would re-fire `tripReady` or
//     `actionRequired` and violate the spec.
//
// Schema placement (D2 of the bead spec):
//   * Added to the live `SwiftDataStore.schema`.
//   * Added to `PlayheadSchemaV1.models` — schema is still V1 (no
//     migration stages defined), so adding to V1 is acceptable.
//
// Eviction (D4): a `DownloadBatch` becomes eligible for hard delete once
// `closedAt` is set AND ≥ 7 days have elapsed since closure. See
// `DownloadBatchEvictor`.

import Foundation
import SwiftData

/// Persisted record of a "Download-Next-N" batch invocation. Drives the
/// batch-notification reducer (`BatchNotificationReducer`) and enforces
/// the two-notification cap per batch lifetime.
///
/// Each row owns its own identity (`id`), the trip context the user
/// selected at submit time (stored as `tripContextRaw` because SwiftData
/// stores enums most reliably as their raw value), and the canonical
/// episode keys of the children admitted to the batch.
///
/// The notification-state flags (`tripReadyNotified`,
/// `actionRequiredNotified`) and the persistence-rule bookkeeping
/// (`consecutiveBlockedPasses`, `firstBlockedAt`) are mutated by the
/// `BatchNotificationCoordinator`; nothing else writes to them.
@Model
final class DownloadBatch {

    /// Stable batch identity. Generated at insert time; never reused.
    var id: UUID

    /// Wall-clock submit time. Used for diagnostic logging and for the
    /// "≥ 7 days since closed" eviction calculation when `closedAt` is
    /// set.
    var createdAt: Date

    /// Trip context picked by the user in `DownloadNextView` at submit
    /// time. Stored as `String` (the rawValue of `DownloadTripContext`)
    /// because SwiftData's enum support is strongest for primitive
    /// raw-value forms; decode via `DownloadTripContext(rawValue:)`.
    ///
    /// `"generic"` rows are still recorded so call-site auditability is
    /// uniform, but the coordinator skips notification emission for any
    /// generic batch (no permission ask, no notifications).
    var tripContextRaw: String

    /// Canonical episode keys of the batch's children. Uses
    /// `Episode.canonicalEpisodeKey` as the link key — a relationship
    /// would be heavier and isn't needed for this read-mostly workload.
    /// SwiftData supports `[String]` natively as a primitive value type.
    var episodeKeys: [String]

    /// Cap flag: `tripReady` notification has fired for this batch.
    /// Once `true`, the coordinator never re-fires.
    var tripReadyNotified: Bool

    /// Cap flag: an action-required notification has fired for this
    /// batch. Once `true`, the coordinator never fires another
    /// (max 1 action-required notification per batch lifetime).
    var actionRequiredNotified: Bool

    /// Persistence-rule counter: the number of consecutive scheduler
    /// passes that produced an `actionRequired`-eligible reduction.
    /// Reset to 0 on any progress (a non-blocked reduction).
    var consecutiveBlockedPasses: Int

    /// Wall-clock anchor for the "≥ 30 minutes since first block" half
    /// of the persistence rule. Set on the first blocked pass; cleared
    /// to nil on any progress (so the next blocked pass re-anchors).
    var firstBlockedAt: Date?

    /// Diagnostic-only: rawValue of the `BatchNotificationEligibility`
    /// the most-recent reduction produced. Used for log correlation
    /// when diagnosing why a notification did or did not fire. NEVER
    /// consulted by the coordinator's emit decision (the reducer is
    /// the source of truth for the current pass).
    var lastEligibility: String?

    /// Set by the coordinator when every child has reached a terminal
    /// state. After this is set, the coordinator skips the batch in
    /// subsequent passes; the evictor hard-deletes the row once it is
    /// at least 7 days old.
    var closedAt: Date?

    init(
        id: UUID = UUID(),
        createdAt: Date = .now,
        tripContextRaw: String,
        episodeKeys: [String],
        tripReadyNotified: Bool = false,
        actionRequiredNotified: Bool = false,
        consecutiveBlockedPasses: Int = 0,
        firstBlockedAt: Date? = nil,
        lastEligibility: String? = nil,
        closedAt: Date? = nil
    ) {
        self.id = id
        self.createdAt = createdAt
        self.tripContextRaw = tripContextRaw
        self.episodeKeys = episodeKeys
        self.tripReadyNotified = tripReadyNotified
        self.actionRequiredNotified = actionRequiredNotified
        self.consecutiveBlockedPasses = consecutiveBlockedPasses
        self.firstBlockedAt = firstBlockedAt
        self.lastEligibility = lastEligibility
        self.closedAt = closedAt
    }
}
