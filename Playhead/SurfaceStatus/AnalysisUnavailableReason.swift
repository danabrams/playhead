// AnalysisUnavailableReason.swift
// Forward-declaration stub for `AnalysisUnavailableReason`. The enum's
// cases and derivation logic are owned by playhead-sueq (Phase 1.5 —
// "Analysis-unavailable reason + derivation from AnalysisEligibility"),
// which has not landed yet. This stub gives the `EpisodeSurfaceStatus`
// reducer a stable type to point at so consumers can compile against
// the reducer's public surface without depending on sueq's landing.
//
// TODO(playhead-sueq): Flesh out this enum with the full per-field
// derivation ladder (hardware unsupported > region unsupported > AI
// disabled > language unsupported > model-not-resident) and return a
// non-nil `AnalysisUnavailableReason` whenever `AnalysisEligibility`
// indicates at least one failing gate. Until then, `derive(from:)` is
// a no-op that returns nil — the reducer branches on the absence of a
// value to decide whether to emit `disposition = .unavailable`.
//
// Scope contract with playhead-5bb3 (this bead):
//   * The reducer is allowed to call `AnalysisUnavailableReason.derive`.
//   * The reducer MUST NOT pattern-match on any specific case — the
//     enum is empty today and adding cases here would make sueq's
//     landing a merge conflict.
//   * When sueq lands, the reducer's branch for
//     `.disposition == .unavailable` will be updated to plumb the
//     returned reason into the output struct. For now the output carries
//     `analysisUnavailableReason: AnalysisUnavailableReason?` which is
//     always nil.

import Foundation

/// Per-field derived reason why analysis is unavailable on this device.
/// Cases are intentionally empty in this bead — see the file-level TODO
/// and playhead-sueq for the canonical enum definition.
///
/// The empty enum form is a deliberate compile-time tripwire: any code
/// that tries to `switch` exhaustively on it is asserting that every
/// case is handled, and an empty enum forces the caller to route
/// through `derive(from:)` rather than pattern-match on the stub.
///
/// `Sendable` and `Hashable` are trivially satisfied by an empty enum
/// (no instances, no stored properties). We do NOT conform to
/// `Codable` on the stub: Swift's Codable synthesis for a case-less
/// enum would produce encode/decode bodies that either always throw or
/// are unreachable, and we would have to maintain them through sueq's
/// landing anyway. The downstream consumer (`EpisodeSurfaceStatus`)
/// handles the optional field by writing the raw-value string only when
/// a value is present; since `derive(from:)` always returns nil in
/// Phase 1.5, the Codable seam is exercised only on the nil side.
enum AnalysisUnavailableReason: Sendable, Hashable {

    // MARK: - Derivation (stub)

    /// Derive an `AnalysisUnavailableReason` from an `AnalysisEligibility`
    /// snapshot. Returns `nil` when the device is fully eligible. The
    /// stub implementation ALWAYS returns nil — playhead-sueq will
    /// replace this with the real per-field derivation ladder.
    ///
    /// The call-site contract (reducer in `EpisodeSurfaceStatusReducer.swift`)
    /// branches on `isFullyEligible` directly for the ineligible decision;
    /// the return value of `derive(from:)` is plumbed into the output
    /// struct purely as the "which reason won the ladder" signal, which
    /// Phase 1.5 does not need yet.
    static func derive(from eligibility: AnalysisEligibility) -> AnalysisUnavailableReason? {
        // TODO(playhead-sueq): fill in the per-field derivation ladder.
        _ = eligibility
        return nil
    }
}
