// LightweightInventoryChecksSettings.swift
// playhead-xr3t — feature-flag persistence for the post-hoc
// inventory sanity filter.
//
// Persistence model mirrors `DownloadsSettings` (SettingsL274.swift):
// a small UserDefaults-backed struct with a stable namespaced key. The
// 24cm flag is the closest in spirit (real persistence behind a
// Diagnostics → Feature flags toggle); this bead follows the same
// pattern so the rollback UX is consistent across flags.
//
// Default: ON. The bead spec explicitly requires "Default ON for new
// builds". A new install (no value persisted) therefore evaluates as
// `enabled = true` and the filter runs from first launch. Existing
// installs that wrote a value before the bead shipped keep whatever
// they wrote — `UserDefaults.bool(forKey:)` returns `false` for an
// absent key, so the `object(forKey:)`-presence check below is what
// keeps the default at `true` for the absent-key case.

import Foundation

/// UserDefaults-backed persistence for the `lightweight_inventory_checks_enabled`
/// feature flag. Stateless filter, stateful toggle — the Diagnostics
/// surface mutates this and the production `InventorySanityFilter`
/// reads it at construction time.
struct LightweightInventoryChecksSettings: Sendable, Equatable {

    /// When `true`, the `InventorySanityFilter` is active and rejects
    /// invalid spans at the fusion → SkipOrchestrator boundary. When
    /// `false`, the filter is a no-op pass-through (pre-Phase-3
    /// behaviour).
    var enabled: Bool

    /// Default value for a fresh install (no value persisted). Bead
    /// spec: "Default ON for new builds."
    static let defaultEnabled: Bool = true

    /// UserDefaults key. Namespaced under `SettingsL274.featureFlags`
    /// so a future relocation to SwiftData is a single read-path
    /// change.
    static let enabledKey = "SettingsL274.featureFlags.lightweightInventoryChecksEnabled"

    init(enabled: Bool = LightweightInventoryChecksSettings.defaultEnabled) {
        self.enabled = enabled
    }

    /// Load the persisted value, falling back to the default when the
    /// key is absent. Presence-checking via `object(forKey:)` is
    /// load-bearing — `bool(forKey:)` returns `false` for an absent
    /// key, which would invert the spec default for new installs.
    static func load(from defaults: UserDefaults = .standard) -> LightweightInventoryChecksSettings {
        if defaults.object(forKey: enabledKey) == nil {
            return LightweightInventoryChecksSettings(enabled: defaultEnabled)
        }
        return LightweightInventoryChecksSettings(enabled: defaults.bool(forKey: enabledKey))
    }

    /// Persist this struct's `enabled` value. Idempotent.
    func save(to defaults: UserDefaults = .standard) {
        defaults.set(enabled, forKey: Self.enabledKey)
    }
}
