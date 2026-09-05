//
//  UserCorrectionOutcome.swift
//  Playhead
//
//  playhead-yflz: one audit row per user-correction GESTURE, naming its
//  outcome — healthy rows included. On the `manual_veto_outcome` precedent
//  (playhead-zxqj): a refused correction used to leave a byte-identical
//  database and an identical screen, so a pull could not tell "refused" from
//  "never tapped". The fidelity ladder makes a user mark the highest-value
//  signal the app has; a silently refused one is that signal deleted.
//

import Foundation

enum UserCorrectionGesture: String, Sendable, Hashable, CaseIterable {
    /// The transcript "Mark ad" flow, the untranscribed-tail footer.
    case markAd
    /// The player's "Hearing an ad" button (`NowPlayingViewModel.reportHearingAd`).
    case hearingAd
    case acceptSuggestedSkip
    case declineSuggestedSkip
    case confirmAutoSkippedBanner
    case revertWindow
}

enum UserCorrectionOutcome: String, Sendable, Hashable, CaseIterable {
    /// The gesture did what the listener asked.
    case applied
    /// The mark already existed at that extent.
    case alreadyMarked
    /// The mark widened an existing one.
    case extended
    /// The card's revision/episode/generation no longer matched — the answer
    /// was to a card that had already changed underneath the tap.
    case staleRevision
    /// No window by that id (already finalized, or never this orchestrator's).
    case unknownWindow
    /// The window was already reverted or suppressed.
    case alreadyTerminal
    /// The runtime's identity guards refused the mark (asset/episode/generation).
    case refusedIdentity
    /// The store refused or failed to persist the mark.
    case refusedStore
    /// The hearing-ad debounce swallowed a second tap inside its window.
    case debounced
    /// The suggest card was dismissed (neutral x / auto-fade) without an answer.
    case dismissedWithoutAnswer
    /// A refusal this code did not classify. Still a row.
    case refused
}

struct UserCorrectionOutcomeAudit: Sendable, Hashable {
    let gesture: UserCorrectionGesture
    let outcome: UserCorrectionOutcome
    let analysisAssetId: String?
    let windowId: String?

    /// `key=value` pairs, one row per gesture. `asset` and `window` are
    /// written as `-` when unknown so the row's SHAPE never varies.
    var auditDescription: String {
        [
            "gesture=\(gesture.rawValue)",
            "outcome=\(outcome.rawValue)",
            "asset=\(analysisAssetId ?? "-")",
            "window=\(windowId ?? "-")",
        ].joined(separator: " ")
    }
}
