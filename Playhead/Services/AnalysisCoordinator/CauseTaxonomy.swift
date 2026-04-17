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
enum InternalMissCause: String, Sendable, Hashable, Codable, CaseIterable {
    case noRuntimeGrant = "no_runtime_grant"
    case taskExpired = "task_expired"
    case thermal
    case lowPowerMode = "low_power_mode"
    case batteryLowUnplugged = "battery_low_unplugged"
    case noNetwork = "no_network"
    case wifiRequired = "wifi_required"
    case mediaCap = "media_cap"
    case analysisCap = "analysis_cap"
    case userPreempted = "user_preempted"
    case userCancelled = "user_cancelled"
    case modelTemporarilyUnavailable = "model_temporarily_unavailable"
    case unsupportedEpisodeLanguage = "unsupported_episode_language"
    case asrFailed = "asr_failed"
    case pipelineError = "pipeline_error"
    case appForceQuitRequiresRelaunch = "app_force_quit_requires_relaunch"
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
