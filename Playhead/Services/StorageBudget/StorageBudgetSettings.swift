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
import OSLog

/// Logger for configuration-load diagnostics. Module-private so the
/// ``StorageBudgetSettings`` static methods can use it without forcing
/// the struct itself to become non-Sendable.
private let storageBudgetSettingsLogger = Logger(
    subsystem: "com.playhead",
    category: "StorageBudgetSettings"
)

/// Map common Foundation toll-free-bridged class names to friendly
/// names so warning logs say "String" instead of "__NSCFString" or
/// "__StringStorage". Improves Console.app triage at scale; falls
/// back to the raw class name for unknown types.
private func friendlyTypeName(of value: Any) -> String {
    let raw = String(describing: type(of: value))
    switch raw {
    case "__NSCFString", "NSTaggedPointerString", "_NSCFConstantString":
        return "String"
    case "__StringStorage", "_StringStorage":
        return "String"
    case "__NSCFNumber", "NSNumber":
        return "NSNumber"
    case "__NSArrayI", "__NSArrayM", "__NSCFArray":
        return "Array"
    case "__NSDictionaryI", "__NSDictionaryM", "__NSCFDictionary":
        return "Dictionary"
    case "__NSCFData", "_NSInlineData":
        return "Data"
    case "__NSDate", "__NSTaggedDate":
        return "Date"
    default:
        return raw
    }
}

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
        // Cycle-3 hardening: distinguish "key truly absent" (normal
        // first-launch — silent fallback) from "key present but holds a
        // non-numeric value" (corrupted preference — log a warning so
        // we don't silently mask an upstream bad write). Without this
        // split, a stale string at the key would cast to nil and fall
        // through the `raw == nil` branch indistinguishably from a
        // brand-new install.
        let rawObject = defaults.object(forKey: mediaCapBytesKey)
        let raw: NSNumber?
        if let rawObject {
            raw = rawObject as? NSNumber
            if raw == nil {
                storageBudgetSettingsLogger.warning(
                    """
                    Persisted value at '\(mediaCapBytesKey, privacy: .public)' is not numeric \
                    (got \(friendlyTypeName(of: rawObject), privacy: .public)); \
                    falling back to default.
                    """
                )
            }
        } else {
            raw = nil
        }
        let candidate = raw?.int64Value ?? defaultMediaCapBytes
        // Clamp to a sane lower bound: anything below 100 MB is almost
        // certainly a bug or a corrupted preference. Fall back to
        // default rather than silently wedging the pipeline. Log a
        // warning so the clamp is visible in field logs and doesn't
        // hide a bad write upstream.
        let lowerBound: Int64 = 100 * 1_000_000
        if candidate < lowerBound {
            // L1 fix: surface the clamp via os.Logger. We only log when
            // a value was actually persisted (raw != nil) — an absent
            // key is the normal first-launch path, not corruption.
            if raw != nil {
                storageBudgetSettingsLogger.warning(
                    """
                    Persisted mediaCapBytes=\(candidate, privacy: .public) is below the \
                    \(lowerBound, privacy: .public)-byte sanity floor; \
                    falling back to default=\(defaultMediaCapBytes, privacy: .public).
                    """
                )
            }
            return StorageBudgetSettings(mediaCapBytes: defaultMediaCapBytes)
        }
        return StorageBudgetSettings(mediaCapBytes: candidate)
    }

    /// Persist the current settings to a UserDefaults instance
    /// (defaults to `.standard`).
    func save(to defaults: UserDefaults = .standard) {
        defaults.set(NSNumber(value: mediaCapBytes), forKey: Self.mediaCapBytesKey)
    }
}
