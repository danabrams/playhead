// StorageBudgetSettings.swift
// playhead-h7r: user-configurable `media_cap` setting (default 10 GB).
//
// The analysis cap (200 MB for warmResumeBundle + scratch) is NOT
// user-configurable by design — tuning it is a future engineering
// decision, not a user one. Only `mediaCapBytes` lives here.
//
// Persistence: UserDefaults-backed, matching the pattern of the existing
// `PreAnalysisConfig` (see `PreAnalysis/PreAnalysisConfig.swift`). A
// future Settings UI surface can bind to these accessors without
// changing the storage key. The Settings UI itself is out of scope for
// this bead — only the storage + accessors land here.

import Foundation

/// User-configurable storage-budget settings.
///
/// Current surface is intentionally minimal: just `mediaCapBytes`. If
/// later beads add further user-tunable storage knobs, extend here.
struct StorageBudgetSettings: Sendable {
    /// The user-configured media cap in bytes. Defaults to
    /// ``defaultMediaCapBytes`` (10 GB) when unset or invalid.
    var mediaCapBytes: Int64

    init(mediaCapBytes: Int64 = defaultMediaCapBytes) {
        self.mediaCapBytes = mediaCapBytes
    }

    // MARK: - UserDefaults persistence

    /// UserDefaults key for the persisted media-cap value (Int64 bytes).
    static let mediaCapBytesKey = "StorageBudget.mediaCapBytes"

    /// Load the current settings from a UserDefaults instance
    /// (defaults to `.standard`). Invalid or negative persisted values
    /// fall back to the default.
    static func load(from defaults: UserDefaults = .standard) -> StorageBudgetSettings {
        let raw = defaults.object(forKey: mediaCapBytesKey) as? NSNumber
        let candidate = raw?.int64Value ?? defaultMediaCapBytes
        // Clamp to a sane lower bound: anything below 100 MB is almost
        // certainly a bug or a corrupted preference. Fall back to
        // default rather than silently wedging the pipeline.
        let clamped = candidate >= 100 * 1_000_000 ? candidate : defaultMediaCapBytes
        return StorageBudgetSettings(mediaCapBytes: clamped)
    }

    /// Persist the current settings to a UserDefaults instance
    /// (defaults to `.standard`).
    func save(to defaults: UserDefaults = .standard) {
        defaults.set(NSNumber(value: mediaCapBytes), forKey: Self.mediaCapBytesKey)
    }
}
