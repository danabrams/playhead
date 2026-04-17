// CauseTaxonomy.swift
// Four-layer taxonomy describing why a scheduled analysis did not complete
// or did not produce a skip for a given episode at a given moment.
//
// Layers:
//   1. InternalMissCause   — raw, engine-side reason (16 variants).
//   2. SurfaceDisposition  — how the condition presents in UI state.
//   3. SurfaceReason       — copy-stable reason bucket; localized strings
//                            anchor to these rawValues.
//   4. ResolutionHint      — the actionable hint surfaced alongside the
//                            reason; carries `userFixable` so UI can decide
//                            whether to render a call-to-action.
//
// Scope note:
// The full 16-row mapping table from InternalMissCause to
// (SurfaceDisposition, SurfaceReason, ResolutionHint) is implemented in a
// later bead (playhead-dfem, Phase 1.5). This file defines the enum
// variants and three worked-example mappings for the hardest
// context-dependent rows. See CauseAttributionPolicy for the precedence
// ladder and the worked examples.

import Foundation

// MARK: - Layer 1: InternalMissCause

/// Raw, engine-side reasons analysis did not complete or produce a skip for a
/// given episode at a given moment. These are the inputs to the attribution
/// policy; they are not user-facing.
///
/// Declaration order matters: the attribution policy uses it as the
/// deterministic tie-breaker when two causes share the same precedence tier.
///
/// ## Forward-compat `.unknown(String)` case (playhead-uzdq.1)
///
/// The `.unknown(String)` case exists so the read path in
/// `AnalysisStore.readWorkJournalEntry` can round-trip a schema-evolved
/// cause string (e.g. a value persisted by a newer build, then read back
/// by an older build, or a stale string that was never migrated). It is
/// **NOT** intended for production emission: every emitter should use one
/// of the 16 canonical cases.
///
/// `CaseIterable.allCases` intentionally omits `.unknown` — it enumerates
/// only the 16 canonical cases so exhaustiveness tests and attribution-
/// policy maps stay mechanical. `init?(rawValue:)` also returns `nil` for
/// unrecognized strings so callers that want `.unknown(s)` forward-compat
/// opt in explicitly via `?? .unknown(rawValue)`.
enum InternalMissCause: Sendable, Hashable, Codable {
    case noRuntimeGrant
    case taskExpired
    case thermal
    case lowPowerMode
    case batteryLowUnplugged
    case noNetwork
    case wifiRequired
    case mediaCap
    case analysisCap
    case userPreempted
    case userCancelled
    case modelTemporarilyUnavailable
    case unsupportedEpisodeLanguage
    case asrFailed
    case pipelineError
    case appForceQuitRequiresRelaunch

    /// Forward-compat sentinel for a schema-evolved cause string the
    /// current build does not recognize. `rawValue` returns the bare
    /// associated string (so `.unknown("futureCauseXYZ").rawValue ==
    /// "futureCauseXYZ"`), and a subsequent `init?(rawValue:)` on that
    /// same string returns `nil` — callers round-trip via
    /// `?? .unknown(rawValue)`.
    case unknown(String)
}

// MARK: InternalMissCause — RawRepresentable (hand-rolled)

extension InternalMissCause: RawRepresentable {
    /// The canonical string form persisted to `work_journal.cause`. For
    /// `.unknown(s)` this returns the bare `s`; round-trip semantics are
    /// documented on the enum itself.
    var rawValue: String {
        switch self {
        case .noRuntimeGrant: return "no_runtime_grant"
        case .taskExpired: return "task_expired"
        case .thermal: return "thermal"
        case .lowPowerMode: return "low_power_mode"
        case .batteryLowUnplugged: return "battery_low_unplugged"
        case .noNetwork: return "no_network"
        case .wifiRequired: return "wifi_required"
        case .mediaCap: return "media_cap"
        case .analysisCap: return "analysis_cap"
        case .userPreempted: return "user_preempted"
        case .userCancelled: return "user_cancelled"
        case .modelTemporarilyUnavailable: return "model_temporarily_unavailable"
        case .unsupportedEpisodeLanguage: return "unsupported_episode_language"
        case .asrFailed: return "asr_failed"
        case .pipelineError: return "pipeline_error"
        case .appForceQuitRequiresRelaunch: return "app_force_quit_requires_relaunch"
        case .unknown(let s): return s
        }
    }

    /// Matches the 16 canonical string rawValues. Returns `nil` for
    /// anything else — callers that want forward-compat fall through to
    /// `?? .unknown(rawValue)`.
    init?(rawValue: String) {
        switch rawValue {
        case "no_runtime_grant": self = .noRuntimeGrant
        case "task_expired": self = .taskExpired
        case "thermal": self = .thermal
        case "low_power_mode": self = .lowPowerMode
        case "battery_low_unplugged": self = .batteryLowUnplugged
        case "no_network": self = .noNetwork
        case "wifi_required": self = .wifiRequired
        case "media_cap": self = .mediaCap
        case "analysis_cap": self = .analysisCap
        case "user_preempted": self = .userPreempted
        case "user_cancelled": self = .userCancelled
        case "model_temporarily_unavailable": self = .modelTemporarilyUnavailable
        case "unsupported_episode_language": self = .unsupportedEpisodeLanguage
        case "asr_failed": self = .asrFailed
        case "pipeline_error": self = .pipelineError
        case "app_force_quit_requires_relaunch": self = .appForceQuitRequiresRelaunch
        default: return nil
        }
    }
}

// MARK: InternalMissCause — Codable (single-value rawValue string)
//
// We hand-roll `Codable` to mirror the string-backed encoding the pre-
// uzdq.1 `enum InternalMissCause: String` shape used. Swift only wires
// `RawRepresentable`-based Codable synthesis when the enum is declared
// `: String` directly; because we now lift `RawRepresentable` into an
// extension (to accommodate the `.unknown(String)` associated-value
// case), we have to provide the bridge ourselves. Encoding produces a
// single-value string container holding `rawValue`; decoding round-trips
// via `init?(rawValue:) ?? .unknown(raw)` so unknown strings survive.

extension InternalMissCause {
    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let raw = try container.decode(String.self)
        self = InternalMissCause(rawValue: raw) ?? .unknown(raw)
    }
}

// MARK: InternalMissCause — CaseIterable (hand-rolled, omits `.unknown`)

extension InternalMissCause: CaseIterable {
    /// The 16 canonical cases. `.unknown(_)` is deliberately absent: it
    /// is a read-path forward-compat sentinel, not a production emission
    /// site, and existing call-sites (attribution policy declaration
    /// order, exhaustiveness tests) assume exactly 16 entries.
    static var allCases: [InternalMissCause] {
        [
            .noRuntimeGrant,
            .taskExpired,
            .thermal,
            .lowPowerMode,
            .batteryLowUnplugged,
            .noNetwork,
            .wifiRequired,
            .mediaCap,
            .analysisCap,
            .userPreempted,
            .userCancelled,
            .modelTemporarilyUnavailable,
            .unsupportedEpisodeLanguage,
            .asrFailed,
            .pipelineError,
            .appForceQuitRequiresRelaunch,
        ]
    }
}

// MARK: - Layer 2: SurfaceDisposition

/// How the condition presents in the UI state machine.
enum SurfaceDisposition: String, Sendable, Hashable, Codable, CaseIterable {
    case queued
    case paused
    case unavailable
    case failed
    case cancelled
}

// MARK: - Layer 3: SurfaceReason

/// Copy-stable, user-visible reason buckets. Localized copy keys anchor to
/// these raw values so the UI layer can translate without depending on the
/// internal cause.
enum SurfaceReason: String, Sendable, Hashable, Codable, CaseIterable {
    case waitingForTime = "waiting_for_time"
    case phoneIsHot = "phone_is_hot"
    case powerLimited = "power_limited"
    case waitingForNetwork = "waiting_for_network"
    case storageFull = "storage_full"
    case analysisUnavailable = "analysis_unavailable"
    case resumeInApp = "resume_in_app"
    case cancelled = "cancelled"
    case couldntAnalyze = "couldnt_analyze"
}

// MARK: - Layer 4: ResolutionHint

/// The actionable hint presented alongside a `SurfaceReason`. `userFixable`
/// determines whether the UI should render a call-to-action (a CTA only makes
/// sense when the user can actually do something about the condition).
enum ResolutionHint: String, Sendable, Hashable, Codable, CaseIterable {
    case none
    case wait
    case connectToWiFi = "connect_to_wifi"
    case chargeDevice = "charge_device"
    case freeUpStorage = "free_up_storage"
    case enableAppleIntelligence = "enable_apple_intelligence"
    case openAppToResume = "open_app_to_resume"
    case retry

    /// Whether the hint corresponds to an action the user can take.
    ///
    /// `none` and `wait` are not user-fixable: the former offers no action at
    /// all, the latter asks the user to do nothing. Everything else surfaces
    /// a CTA.
    var userFixable: Bool {
        switch self {
        case .none, .wait:
            return false
        case .connectToWiFi,
             .chargeDevice,
             .freeUpStorage,
             .enableAppleIntelligence,
             .openAppToResume,
             .retry:
            return true
        }
    }
}

// MARK: - SurfaceAttribution

/// The triple a single `InternalMissCause` resolves to once context has been
/// applied. Produced by `CauseAttributionPolicy`.
struct SurfaceAttribution: Sendable, Hashable, Codable {
    let disposition: SurfaceDisposition
    let reason: SurfaceReason
    let hint: ResolutionHint
}
