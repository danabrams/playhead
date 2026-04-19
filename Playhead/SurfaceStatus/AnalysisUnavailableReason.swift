// AnalysisUnavailableReason.swift
// Per-device reason why on-device analysis is unavailable, derived
// purely from an `AnalysisEligibility` snapshot. Owned by the surface-
// status module; consumed by `EpisodeSurfaceStatus`.
//
// Scope: playhead-sueq (Phase 1.5 — "analysisUnavailableReason wiring").
//
// Shape rationale — why a flat raw-value enum (not an associated-value
// enum or a struct):
//   * Every case is a pure tag; there is no per-case payload to carry.
//   * The UI keys copy and CTAs off `rawValue`, so a `String` raw-value
//     form is the simplest Codable representation.
//   * A flat enum is exhaustively switchable, which the UI layer relies
//     on to guarantee every reason has a copy string.
//
// Derivation contract — see `derive(from:)`:
//   hardware > region > language > appleIntelligence > model
//
// Intuition: "most-permanent / least-fixable wins." A user whose device
// is simultaneously hardware-unsupported AND has AI toggled off should
// be told "hardware unsupported" — sending them to Settings first would
// only burn a round-trip before they hit the immovable wall. Conversely,
// a transient model-unavailable window is the lowest-priority signal:
// nothing is actually broken, the system will recover on its own.
//
// Scope contract with CauseAttributionPolicy (playhead-dfem):
//   * `AnalysisUnavailableReason` derives ONLY from `AnalysisEligibility`.
//     It does NOT consume `InternalMissCause` or any other runtime
//     signal. The cause→triple mapping lives in `CauseAttributionPolicy`;
//     this enum lives one layer lower and answers the different question
//     "why is this device ineligible at all".
//   * Downstream consumers that need a cause-aware picture must read
//     BOTH this reason and the cause — do not conflate them.
//
// Scope contract with playhead-ol05 (invariant enforcement):
//   * `ol05` will add the runtime invariant "analysisUnavailableReason
//     non-nil ⇔ disposition == .unavailable". This file does NOT assert
//     that invariant — it only exposes the derivation. Invariant wiring
//     belongs in `SurfaceStatusInvariantLogger` / the reducer.

import Foundation

/// Per-field derived reason why analysis is unavailable on this device.
///
/// Cases are ordered in the enum declaration to mirror the precedence
/// ladder (most-permanent / least-fixable first). The enum is a flat
/// `String` raw-value form so that Codable produces stable snake-case
/// tokens matching the field names on `AnalysisEligibility`.
enum AnalysisUnavailableReason: String, Sendable, Hashable, Codable, CaseIterable {

    /// Device hardware cannot run on-device analysis. Permanent for
    /// this device/OS pair; not user-fixable without new hardware.
    case hardwareUnsupported = "hardware_unsupported"

    /// Device region is outside the supported list. Permanent within the
    /// region; user-fixable only by changing their region setting, which
    /// has broad side effects we don't want to prescribe.
    case regionUnsupported = "region_unsupported"

    /// Device primary locale is not a supported language. Note this is
    /// the DEVICE locale, not a per-episode language — per-episode
    /// language gating is explicitly out of scope for this enum and is
    /// handled via `InternalMissCause.unsupportedEpisodeLanguage`
    /// further up the stack.
    case languageUnsupported = "language_unsupported"

    /// User has Apple Intelligence toggled off in Settings. User-fixable
    /// with a single toggle, which is why it ranks below the more-
    /// permanent gates above: the UI should nudge the user toward the
    /// fix rather than surfacing a more-alarming hardware story.
    case appleIntelligenceDisabled = "apple_intelligence_disabled"

    /// ML model assets are not currently resident. Transient by design;
    /// the system is expected to recover without user action as the
    /// model re-downloads / re-loads. Ranked last because it is the
    /// softest signal of the five — nothing is structurally wrong.
    case modelTemporarilyUnavailable = "model_temporarily_unavailable"

    // MARK: - Derivation

    /// Derive an `AnalysisUnavailableReason` from an `AnalysisEligibility`
    /// snapshot using the precedence ladder documented at the top of
    /// this file: hardware > region > language > appleIntelligence >
    /// model. Returns `nil` iff `eligibility.isFullyEligible` is true.
    ///
    /// This function is a pure static — same inputs always yield the
    /// same output, no side effects, no logging. The reducer
    /// (`episodeSurfaceStatus`) calls this once per reduction and
    /// plumbs the result into the output struct.
    static func derive(from eligibility: AnalysisEligibility) -> AnalysisUnavailableReason? {
        // Order of tests matches the declared precedence ladder. Short-
        // circuit at the first false field; every subsequent `if` is
        // implicitly guarded by the prior fields being true.
        if !eligibility.hardwareSupported { return .hardwareUnsupported }
        if !eligibility.regionSupported { return .regionUnsupported }
        if !eligibility.languageSupported { return .languageUnsupported }
        if !eligibility.appleIntelligenceEnabled { return .appleIntelligenceDisabled }
        if !eligibility.modelAvailableNow { return .modelTemporarilyUnavailable }
        // All gates pass — no reason to surface.
        return nil
    }
}
