// DiagnosticsOptInResetPolicy.swift
// Pure rule for whether `Episode.diagnosticsOptIn` should reset after the
// mail composer dismisses. Decoupled from MessageUI so tests can drive
// every transition without spinning up `MFMailComposeViewController`
// (which is simulator-hostile).
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// The reset rule is intentionally a free function on a tiny enum so it
// can live in the test target without dragging UIKit / MessageUI across
// the seam. The thin adapter that converts a real `MFMailComposeResult`
// into this enum lives in ``DiagnosticsExportService``.

import Foundation

/// Mirror of `MessageUI.MFMailComposeResult` that does not require
/// importing MessageUI from test code or non-iOS targets.
enum DiagnosticsMailComposeResult: Sendable, Hashable, CaseIterable {
    /// User cancelled the composer. Reset rule: PRESERVE current opt-in.
    case cancelled
    /// User saved to Drafts. Reset rule: CLEAR opt-in (the artifact left
    /// the composer's sandbox into the user's mailbox).
    case saved
    /// User sent the message. Reset rule: CLEAR opt-in.
    case sent
    /// Composer reported a system failure. Reset rule: PRESERVE current
    /// opt-in (failure is not user intent — the user may immediately
    /// retry).
    case failed
}

enum DiagnosticsOptInResetPolicy {

    /// Returns the new value for `Episode.diagnosticsOptIn` after the
    /// composer dismisses. The function is total, pure, and idempotent
    /// when the current value is already `false`.
    ///
    /// Truth table:
    ///   - `(_, .sent)`       → `false`
    ///   - `(_, .saved)`      → `false`
    ///   - `(current, .cancelled)` → `current`
    ///   - `(current, .failed)`    → `current`
    static func newValue(
        current: Bool,
        result: DiagnosticsMailComposeResult
    ) -> Bool {
        switch result {
        case .sent, .saved:
            return false
        case .cancelled, .failed:
            return current
        }
    }

    /// Returns `true` when the composer result should trigger a reset of
    /// `Episode.diagnosticsOptIn` to `false`. Equivalent to the question
    /// "is this a delivery-confirming result?" and consistent with
    /// `newValue(current: true, result:) == false` for every case.
    ///
    /// Callers (e.g. `DiagnosticsExportCoordinator`) prefer this over
    /// the `!newValue(current: true, ...)` double-negation because the
    /// intent reads directly off the name.
    static func shouldReset(result: DiagnosticsMailComposeResult) -> Bool {
        switch result {
        case .sent, .saved:     return true
        case .cancelled, .failed: return false
        }
    }
}
