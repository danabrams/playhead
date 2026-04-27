// DebugFlags.swift
// Single source of truth for `@AppStorage` keys that gate
// debug-only UI surfaces. Centralised so renames cannot drift
// between writers (e.g. the Settings toggle row) and readers
// (e.g. the Activity row views) — every site references the
// same constant.
//
// Scope: playhead-btoa.4 polish. Introduced when the
// `debug.showPipelineStrip` key was used in four places
// (one Settings toggle + three Activity row views) and the
// drift risk became real.

import Foundation

/// `@AppStorage` keys for debug-only feature flags. Internal
/// scope: only the in-app Settings toggle and the views it
/// gates should read these.
enum DebugFlagKeys {
    /// Drives the per-row `PipelineProgressStripView` rendering
    /// in `ActivityView`. Default `false`. Flipped from the
    /// Diagnostics toggle in `SettingsView`.
    static let showPipelineStrip = "debug.showPipelineStrip"
}
