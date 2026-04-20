// EpisodeStatusLineCopy.swift
// Pure copy-resolution function for the single-line status surfaced
// above the episode body. Given an `EpisodeSurfaceStatus` (plus the
// originating `CoverageSummary` and playback anchor that drove the
// reducer), return the user-facing primary string and an optional
// secondary string.
//
// Scope: playhead-zp5y (Phase 2 deliverable 3 — "Episode detail status
// line sourced from EpisodeSurfaceStatus").
//
// Why a separate resolver rather than an extension on EpisodeSurfaceStatus?
//   * The resolver consults two additional inputs (`CoverageSummary`,
//     `anchor`) that aren't persisted on the status struct — those are
//     needed to compute the "first X min" vs "next X min" branch and
//     the covered-length number. Adding them to `EpisodeSurfaceStatus`
//     would couple the stored status to UI-only concerns.
//   * Keeping the function pure and free-standing means tests exercise
//     every golden string without any SwiftUI dependency.
//
// Precedence (distinct from the reducer's input-precedence ladder):
//   The reducer produces `(disposition, reason, hint, playbackReadiness)`.
//   For the status-line, disposition dominates for the four "something
//   is wrong" buckets — `.unavailable`, `.failed`, `.paused`, `.cancelled`
//   — and `.queued` falls through to the playback-readiness branch,
//   where the renderable bucket is driven by `playbackReadiness`:
//       .complete    → "Skip-ready · full episode"
//       .proximal    → "Skip-ready · first|next X min"
//       .deferredOnly→ "Downloaded · queued for analysis"
//       .none        → "Queued · [ResolutionHint copy]"
//
//   `.cancelled` (which the reducer emits for user-cancelled jobs) is
//   mapped onto the same "Couldn't analyze · Retry" shape the spec
//   prescribes for `failed` because (a) the spec's 7-case list does not
//   enumerate cancelled, and (b) both surface a retry affordance. The
//   alternative — dropping cancelled into the readiness branch — would
//   silently hide a user-initiated cancellation behind a "Queued …"
//   copy, which is worse.

import Foundation

// MARK: - EpisodeStatusLine

/// Fully-resolved status-line copy for an episode.
///
/// The primary string is the single line that sits above the episode
/// body; the optional secondary string is surfaced directly below when
/// a backfill is active and we want to telegraph that more coverage is
/// still landing.
struct EpisodeStatusLine: Sendable, Hashable {

    /// The user-visible primary copy. Never empty; the resolver emits a
    /// conservative fallback rather than a blank string.
    let primary: String

    /// Optional secondary line. Present when backfill is active so the
    /// surface can render "analyzing remainder" below the primary.
    let secondary: String?

    init(primary: String, secondary: String? = nil) {
        self.primary = primary
        self.secondary = secondary
    }
}

// MARK: - EpisodeStatusLineCopy

/// Pure resolver from `EpisodeSurfaceStatus` (plus the originating
/// coverage + anchor pair) to an `EpisodeStatusLine`.
///
/// Every call-site in the UI MUST route through `resolve(...)`; SwiftUI
/// views should never hand-build these strings. The resolver's inputs
/// are the reducer's output and the same coverage/anchor inputs the
/// reducer consumed — no additional scheduler/persistence types cross
/// the boundary.
enum EpisodeStatusLineCopy {

    /// Resolve the renderable status line for the supplied reducer output.
    ///
    /// - Parameters:
    ///   - status: The reducer's output. Drives every branch decision.
    ///   - coverage: The coverage summary the reducer consumed. Used
    ///     only when `status.playbackReadiness == .proximal` to compute
    ///     the covered-minutes integer and the first/next branch.
    ///   - anchor: The readiness anchor the reducer consumed. Used only
    ///     when `status.playbackReadiness == .proximal`.
    ///   - backfillActive: When `true` (and the primary line is a
    ///     playable state), the resolver emits the "analyzing remainder"
    ///     secondary string. Caller-supplied because the reducer does
    ///     not know whether a backfill is live.
    /// - Returns: The fully-resolved status line.
    static func resolve(
        status: EpisodeSurfaceStatus,
        coverage: CoverageSummary?,
        anchor: TimeInterval?,
        backfillActive: Bool = false
    ) -> EpisodeStatusLine {

        // MARK: Disposition-dominant branches
        switch status.disposition {
        case .unavailable:
            return EpisodeStatusLine(
                primary: unavailablePrimary(reason: status.analysisUnavailableReason)
            )
        case .failed:
            return EpisodeStatusLine(
                primary: failedPrimary()
            )
        case .paused:
            return EpisodeStatusLine(
                primary: pausedPrimary(reason: status.reason, hint: status.hint)
            )
        case .cancelled:
            // Spec does not enumerate cancelled; nearest neighbour is
            // the user-actionable retry affordance on `failed`.
            return EpisodeStatusLine(
                primary: failedPrimary()
            )
        case .queued:
            break
        }

        // MARK: Readiness branch (queued disposition only)
        switch status.playbackReadiness {
        case .complete:
            return EpisodeStatusLine(
                primary: "Skip-ready \(middot) full episode",
                secondary: backfillActive ? backfillSecondary : nil
            )
        case .proximal:
            let primary = proximalPrimary(coverage: coverage, anchor: anchor)
            return EpisodeStatusLine(
                primary: primary,
                secondary: backfillActive ? backfillSecondary : nil
            )
        case .deferredOnly:
            return EpisodeStatusLine(
                primary: "Downloaded \(middot) queued for analysis"
            )
        case .none:
            return EpisodeStatusLine(
                primary: "Queued \(middot) \(hintCopy(status.hint))"
            )
        }
    }

    // MARK: - Private copy builders

    /// "Skip-ready · first X min" or "Skip-ready · next X min" based on
    /// whether the covered region begins at (or very near) the start of
    /// the episode.
    private static func proximalPrimary(
        coverage: CoverageSummary?,
        anchor: TimeInterval?
    ) -> String {
        // The reducer derives `.proximal` when some range continuously
        // spans `[anchor, anchor + 15min]`. We surface the length of
        // that covered region (rounded to whole minutes) and pick the
        // lead word based on whether it starts at the beginning of the
        // episode.
        let minutes = proximalMinutes(coverage: coverage, anchor: anchor)
        let leadWord = proximalLeadWord(coverage: coverage)
        return "Skip-ready \(middot) \(leadWord) \(minutes) min"
    }

    /// "first" when the covered range starts at (or essentially at) 0;
    /// "next" otherwise. The `epsilon` guards against float jitter from
    /// anchor arithmetic; callers that set `firstCoveredOffset = 0`
    /// exactly still get "first".
    private static func proximalLeadWord(coverage: CoverageSummary?) -> String {
        let epsilon: TimeInterval = 0.5
        if let first = coverage?.firstCoveredOffset, first <= epsilon {
            return "first"
        }
        return "next"
    }

    /// Minutes covered by the first (anchor-containing) contiguous
    /// range, rounded to the nearest whole minute. Falls back to 15
    /// (the lookahead window spec) when inputs are missing — reaching
    /// this branch with nil inputs means a caller surfaced `.proximal`
    /// without the inputs, which is already a contract violation; the
    /// fallback keeps the line grammatical.
    private static func proximalMinutes(
        coverage: CoverageSummary?,
        anchor: TimeInterval?
    ) -> Int {
        let fallbackMinutes = Int(playbackReadinessProximalLookaheadSeconds / 60.0)
        guard let coverage, let range = coverage.coverageRanges.first else {
            return fallbackMinutes
        }
        // Prefer the length from the anchor forward when an anchor is
        // present and the range contains it — the user cares about how
        // far the covered region extends past their current position,
        // not the range's absolute length.
        let startSeconds: TimeInterval
        if let anchor, range.contains(anchor) {
            startSeconds = anchor
        } else {
            startSeconds = range.lowerBound
        }
        let coveredSeconds = max(0, range.upperBound - startSeconds)
        let minutes = Int((coveredSeconds / 60.0).rounded())
        return max(1, minutes)
    }

    /// "Analysis unavailable — [AnalysisUnavailableReason copy]". A nil
    /// reason (impossible per the reducer's invariants, but we guard
    /// rather than trap) falls back to the generic SurfaceReason copy.
    private static func unavailablePrimary(
        reason: AnalysisUnavailableReason?
    ) -> String {
        let tail = reason.map(unavailableReasonCopy) ??
            SurfaceReasonCopyTemplates.template(for: .analysisUnavailable)
        return "Analysis unavailable \(emdash) \(tail)"
    }

    /// "Couldn't analyze · Retry". The spec brackets "[Retry]" to
    /// indicate a CTA affordance; the pure string uses the literal word
    /// and the view is responsible for rendering the tap target.
    private static func failedPrimary() -> String {
        "Couldn't analyze \(middot) Retry"
    }

    /// "Paused — [SurfaceReason copy] · [ResolutionHint copy]".
    ///
    /// The SurfaceReason copy strings in `SurfaceReasonCopyTemplates`
    /// already prefix some reasons with "Paused —" (e.g. `phoneIsHot`,
    /// `powerLimited`); we strip that prefix when present so the final
    /// string has exactly one "Paused —" lead.
    private static func pausedPrimary(
        reason: SurfaceReason,
        hint: ResolutionHint
    ) -> String {
        let raw = SurfaceReasonCopyTemplates.template(for: reason)
        let reasonText = stripPausedPrefix(raw)
        return "Paused \(emdash) \(reasonText) \(middot) \(hintCopy(hint))"
    }

    /// Strip a leading "Paused — " / "Paused -" / "Paused: " so the
    /// outer template emits exactly one "Paused —" lead. Case-sensitive
    /// because SurfaceReasonCopyTemplates is canonical.
    private static func stripPausedPrefix(_ raw: String) -> String {
        let prefixes = [
            "Paused \(emdash) ",
            "Paused - ",
            "Paused: ",
        ]
        for prefix in prefixes {
            if raw.hasPrefix(prefix) {
                return String(raw.dropFirst(prefix.count))
            }
        }
        return raw
    }

    /// The optional secondary line emitted when a backfill is active
    /// (and the primary line is a playable-state copy).
    static let backfillSecondary = "analyzing remainder"

    /// Copy for a `ResolutionHint`. Localized strings will replace this
    /// in Phase 2; until then the strings are the canonical copy.
    static func hintCopy(_ hint: ResolutionHint) -> String {
        switch hint {
        case .none:
            return "waiting"
        case .wait:
            return "waiting"
        case .connectToWiFi:
            return "connect to Wi-Fi"
        case .chargeDevice:
            return "charge device"
        case .freeUpStorage:
            return "free up storage"
        case .enableAppleIntelligence:
            return "enable Apple Intelligence"
        case .openAppToResume:
            return "open Playhead to resume"
        case .retry:
            return "Retry"
        }
    }

    /// Copy for an `AnalysisUnavailableReason`. Short, user-facing
    /// fragments that slot into "Analysis unavailable — {reason}".
    static func unavailableReasonCopy(_ reason: AnalysisUnavailableReason) -> String {
        switch reason {
        case .hardwareUnsupported:
            return "this device isn't supported"
        case .regionUnsupported:
            return "unavailable in your region"
        case .languageUnsupported:
            return "your device language isn't supported"
        case .appleIntelligenceDisabled:
            return "turn on Apple Intelligence"
        case .modelTemporarilyUnavailable:
            return "model temporarily unavailable"
        }
    }

    // MARK: - Punctuation

    /// U+00B7 MIDDLE DOT. Pinned via escape rather than a literal so
    /// tests and the source agree on the exact codepoint; a
    /// lookalike (e.g. bullet U+2022) would not trip the compiler but
    /// would produce a subtly-different rendered string.
    private static let middot = "\u{00B7}"

    /// U+2014 EM DASH. Same rationale as `middot` — the spec uses an
    /// em dash, not a hyphen or an en dash.
    private static let emdash = "\u{2014}"
}
