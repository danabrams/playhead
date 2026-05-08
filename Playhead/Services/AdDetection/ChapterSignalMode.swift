// ChapterSignalMode.swift
// Controls whether the chapter-signal generation phase (playhead-au2v.1.10)
// runs and whether downstream consumers (CoveragePlanner audit-window
// selection — playhead-au2v.1.14, FM prompt context — playhead-au2v.1.16)
// read the resulting `ChapterPlan`.
//
// Mirrors the FMBackfillMode pattern (Playhead/Services/AdDetection/
// FMBackfillMode.swift) so config plumbing stays uniform across feature
// flags. This file ships the gate FIELD only; the phase shell, planner
// consumer, and prompt consumer land in subsequent beads.

import Foundation

/// Tri-state gate controlling chapter-signal generation and consumption.
///
/// - `off`: ChapterGenerationPhase never runs. Consumers (CoveragePlanner,
///   FM prompt builders) ignore any cached plan. Equivalent to today's
///   pre-au2v.1 behavior. THIS IS THE PRODUCTION DEFAULT.
/// - `shadow`: ChapterGenerationPhase runs and writes telemetry plus the
///   `ChapterPlan` cache, but consumers do NOT read the plan. Used for
///   plan-quality eval and FM-cost telemetry without affecting detection
///   behavior. Detection output must be byte-for-byte identical to `.off`
///   in this mode.
/// - `enabled`: Phase runs AND consumers read. Full activation.
///
/// Richer per-consumer states (`coverageOnly`, `promptOnly`) are deferred
/// per the bead notes — add only if shadow tuning surfaces a need. The
/// tri-state shape covers ship/safe-default/full-on, which is what the
/// eval harness (playhead-au2v.1.18) gate needs.
enum ChapterSignalMode: String, Codable, Sendable, CaseIterable, Equatable {
    case off
    case shadow
    case enabled

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let rawValue = try container.decode(String.self)

        switch rawValue {
        case Self.off.rawValue:
            self = .off
        case Self.shadow.rawValue:
            self = .shadow
        case Self.enabled.rawValue:
            self = .enabled
        case "disabled":
            // Legacy alias: an earlier draft of this bead used "disabled" in
            // a prototype JSON config. Map to .off so a stray serialized
            // payload from that prototype decodes without error.
            self = .off
        default:
            throw DecodingError.dataCorruptedError(
                in: container,
                debugDescription: "Unknown ChapterSignalMode raw value: \(rawValue)"
            )
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    /// `true` iff `ChapterGenerationPhase` (playhead-au2v.1.10, planned)
    /// should run. Used at the phase entry guard in the chapter-signal
    /// shell. Implemented as an exhaustive switch (rather than `self !=
    /// .off`) so adding a new case is a compile error that forces the
    /// author to choose the right semantics.
    var runsChapterGeneration: Bool {
        switch self {
        case .off:
            false
        case .shadow, .enabled:
            true
        }
    }

    /// `true` iff downstream consumers should READ the chapter plan when
    /// making decisions. Used by:
    ///   - CoveragePlanner audit-window selection (playhead-au2v.1.14, planned)
    ///   - FM prompt builders that inject chapter context
    ///     (playhead-au2v.1.16, planned)
    /// `.shadow` returns `false` here so detection behavior is identical
    /// to `.off` while telemetry continues to flow. Exhaustive switch for
    /// the same defensive reason as `runsChapterGeneration`.
    var consumersReadChapterPlan: Bool {
        switch self {
        case .off, .shadow:
            false
        case .enabled:
            true
        }
    }
}
