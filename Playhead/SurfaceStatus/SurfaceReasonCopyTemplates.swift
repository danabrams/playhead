// SurfaceReasonCopyTemplates.swift
// The single approved copy template per `SurfaceReason`. Every UI surface
// that renders a `SurfaceReason` MUST source its user-visible string
// from `SurfaceReasonCopyTemplates.template(for:)` — ad-hoc copy outside
// this table is forbidden by `SurfaceReasonCopyTemplateTests`.
//
// Scope: playhead-ol05 (Phase 1.5 — "State-transition audit + impossible-
// state assertions + cross-target contract test"). Per the contract
// matrix item 3: "Each SurfaceReason case has exactly one approved copy
// template; test fails if any surface generates ad-hoc copy outside that
// table."
//
// The strings are intentionally short and product-tone; localization is
// out of scope for Phase 1.5 (Phase 2 will replace these with localized
// keys). The copy is pinned by an exhaustive switch so adding a future
// `SurfaceReason` case fails the build until a copy line is supplied.

import Foundation

// MARK: - SurfaceReasonCopyTemplates

enum SurfaceReasonCopyTemplates {

    /// The single approved English copy template for every `SurfaceReason`.
    /// Phase 2 will swap this for a localized key lookup; until then the
    /// string is the canonical copy the UI must render.
    static func template(for reason: SurfaceReason) -> String {
        switch reason {
        case .waitingForTime:
            return "Waiting to analyze"
        case .phoneIsHot:
            return "Paused — phone is too hot"
        case .powerLimited:
            return "Paused — low battery"
        case .waitingForNetwork:
            return "Waiting for network"
        case .storageFull:
            return "Storage is full"
        case .analysisUnavailable:
            return "Analysis unavailable on this device"
        case .resumeInApp:
            return "Open Playhead to resume"
        case .cancelled:
            return "Cancelled"
        case .couldntAnalyze:
            return "Couldn't analyze"
        }
    }
}
