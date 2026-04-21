// BatchNotificationEligibility.swift
// Whitelisted enum that gates batch-notification emission for
// "Download-Next-N" trip-context batches (playhead-zp0x).
//
// HARD CONTRACT: this enum is the ONLY thing the
// `BatchNotificationService` accepts on its emit API. The service does
// NOT take raw `SurfaceReason`, free-form text, dictionaries, or any
// other open-ended payload — copy lookup is a private switch on this
// enum inside the service.
//
// Cases (names are spec-verbatim — do not rename):
//   * `tripReady` — every child reached ready; fire exactly once.
//   * `blockedStorage` — any child surfaces `storageFull` (user-fixable).
//   * `blockedWifiPolicy` — any child surfaces `waitingForNetwork` AND
//     the cellular policy disallows (user-fixable in Settings).
//   * `blockedAnalysisUnavailable` — any child surfaces
//     `analysisUnavailable` AND the per-device unavailability reason is
//     either `appleIntelligenceDisabled` or `languageUnsupported`
//     (user-fixable). `hardwareUnsupported` and `regionUnsupported` are
//     NOT eligible — they are not user-fixable.
//   * `none` — still in progress, or only transient/non-fixable
//     blockers (thermal/power/transient-network). Never emits a
//     notification.

import Foundation

/// Whitelisted batch-notification trigger. The reducer
/// (`BatchNotificationReducer`) maps a per-pass child-summary set into
/// exactly one of these cases; the coordinator routes the result to the
/// service's `emit(eligibility:batch:)` API only when the cap and
/// persistence rules allow.
///
/// `String`-backed `rawValue` so coordinators / loggers can stamp the
/// last-eligibility marker on `DownloadBatch.lastEligibility` for
/// diagnostic correlation without needing a custom encoder.
enum BatchNotificationEligibility: String, Sendable, Hashable, Codable, CaseIterable {

    /// Every child in the batch reached the ready state (proximal
    /// coverage met AND download complete). Fired exactly once per
    /// batch lifetime; the coordinator short-circuits the blocker
    /// branches when this is the reducer's verdict.
    case tripReady

    /// At least one child surfaces `SurfaceReason.storageFull`. The
    /// user can clear the blocker by freeing storage, so the action-
    /// required notification copy nudges them toward Settings → Storage.
    case blockedStorage

    /// At least one child surfaces `SurfaceReason.waitingForNetwork`
    /// AND the user has cellular downloads disabled. Fixable by the
    /// user via Settings → Cellular toggle for Playhead, or by
    /// rejoining Wi-Fi.
    case blockedWifiPolicy

    /// At least one child surfaces `SurfaceReason.analysisUnavailable`
    /// AND the per-device unavailability reason is in the user-fixable
    /// set: `appleIntelligenceDisabled` (single Settings toggle) or
    /// `languageUnsupported` (device locale change). Hardware and
    /// region unsupported reasons are NOT eligible because the user
    /// cannot fix them without new hardware or a region change with
    /// broad side effects.
    case blockedAnalysisUnavailable

    /// Either the batch is still in progress (some children are
    /// neither ready nor blocked-and-fixable) or every blocker is in
    /// the transient / non-user-fixable set (thermal, power-limited,
    /// transient network, hardware/region unavailability). Neither
    /// `tripReady` nor an action-required notification fires for this
    /// reduction.
    case none

    /// Whether this case represents an action-required notification
    /// candidate (i.e. one of the three `blocked*` cases). The
    /// coordinator uses this to route emission through the
    /// persistence-rule check and the `actionRequiredNotified` cap;
    /// `tripReady` is routed through its own cap, and `.none` is a
    /// no-op. Pure reflection over the enum — no policy embedded.
    var isActionRequired: Bool {
        switch self {
        case .blockedStorage,
             .blockedWifiPolicy,
             .blockedAnalysisUnavailable:
            return true
        case .tripReady, .none:
            return false
        }
    }
}
