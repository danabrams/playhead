// FMBackfillMode.swift
// Phase 3 toggle that controls how the Foundation Model classifier participates
// in backfill. Shadow mode is the default: FM runs and persists results, but
// the lexical/classifier path keeps full control of skip cues.

import Foundation

/// Controls whether the Foundation Model classifier runs during backfill and
/// whether it is allowed to influence skip cues.
///
/// - `disabled`: FM is skipped entirely. Backfill is the legacy lexical path.
/// - `shadow`: FM runs and persists `SemanticScanResult` / `EvidenceEvent`
///   rows for telemetry, but cue computation is identical to `.disabled`.
///   This is the default during Phase 3 ramp-up.
/// - `enabled`: Reserved for Phase 6, when the decision-fusion layer learns
///   to incorporate FM evidence. Until that ships, the runner falls back to
///   `.shadow` behavior and emits a runtime warning.
enum FMBackfillMode: String, Codable, Sendable, CaseIterable, Equatable {
    case disabled
    case shadow
    case enabled
}
