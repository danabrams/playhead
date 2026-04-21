// BatchNotificationCopy.swift
// Verbatim user-visible strings for the four batch-notification
// triggers (`tripReady`, `blockedStorage`, `blockedWifiPolicy`,
// `blockedAnalysisUnavailable`). playhead-zp0x.
//
// Every string here is snapshot-tested character-for-character; do NOT
// inline-literal any of these in the service body. Mirrors the pattern
// established by `DownloadNextCopy` (playhead-hkg8): copy is product
// surface area, edits go through the bd spec + tests.
//
// Trip-context phrasing (Flight / Commute / Workout) is folded into
// the copy via `tripContextPhrase(_:)` which returns nil for `.generic`
// — generic batches never reach this layer (the coordinator filters
// them upstream), but the helper stays exhaustive so a future opt-in
// has a place to plug in.

import Foundation

/// Verbatim user-visible strings for batch notifications. Snapshot-
/// tested in `BatchNotificationCopyTests`; any change here is a
/// product decision.
enum BatchNotificationCopy {

    // MARK: - Title / body

    /// Notification title for the `tripReady` trigger.
    static let tripReadyTitle: String = "Your downloads are ready"

    /// Notification body for the `tripReady` trigger. The trip context
    /// phrase ("for Flight" / "for Commute" / "for Workout") is folded
    /// in by the service so the body reads naturally; the constant
    /// here is the lead-in fragment with a `%@` placeholder for the
    /// phrase. Use `tripReadyBody(context:)` to compose.
    static func tripReadyBody(context: DownloadTripContext) -> String {
        if let phrase = tripContextPhrase(context) {
            return "Episodes you queued \(phrase) are downloaded and analyzed."
        }
        return "Episodes you queued are downloaded and analyzed."
    }

    /// Notification title for the `blockedStorage` trigger.
    static let blockedStorageTitle: String = "Downloads need more space"

    /// Notification body for the `blockedStorage` trigger. Mirrors the
    /// in-app amber "Free up space →" framing from `DownloadNextCopy`.
    static let blockedStorageBody: String =
        "Tap to free up space so your queued episodes can finish."

    /// Notification title for the `blockedWifiPolicy` trigger.
    static let blockedWifiPolicyTitle: String = "Downloads waiting for Wi‑Fi"

    /// Notification body for the `blockedWifiPolicy` trigger. The
    /// fix-path is either "rejoin Wi‑Fi" or "allow cellular in
    /// Settings"; we surface the cellular path because that is the
    /// in-app action the user can take without leaving the device.
    static let blockedWifiPolicyBody: String =
        "Connect to Wi‑Fi, or allow cellular for Playhead in Settings."

    /// Notification title for the `blockedAnalysisUnavailable` trigger.
    static let blockedAnalysisUnavailableTitle: String =
        "Analysis is paused"

    /// Notification body for the `blockedAnalysisUnavailable` trigger.
    /// Both fixable causes (Apple Intelligence disabled, language
    /// unsupported) route the user to Settings; we keep the copy
    /// generic so a single string covers both without ambiguity.
    static let blockedAnalysisUnavailableBody: String =
        "Open Settings to re-enable Apple Intelligence or change language."

    // MARK: - Helpers

    /// Trip-context phrase used inside the trip-ready body. Returns
    /// `nil` for `.generic` since generic batches never trigger
    /// notifications; the helper stays exhaustive so the codepath is
    /// type-safe even if a future opt-in changes the policy.
    static func tripContextPhrase(_ context: DownloadTripContext) -> String? {
        switch context {
        case .generic: return nil
        case .flight:  return "for your flight"
        case .commute: return "for your commute"
        case .workout: return "for your workout"
        }
    }
}
