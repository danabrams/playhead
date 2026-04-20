// SettingsL274.swift
// playhead-l274 — Phase 2 Settings additions: Downloads, Storage, Diagnostics.
//
// Scope: this file houses all pure, testable surfaces for the l274 deliverable:
//   1. Verbatim copy for every user-visible string (pinned in tests).
//   2. Enumerations + defaults for every control (Auto-download, Cellular,
//      Download-Next-N, Episode storage cap).
//   3. `DownloadsSettings` — UserDefaults-backed persistence for the three
//      download controls. Storage cap persistence already lives in
//      `StorageBudgetSettings` (playhead-h7r); this file adds the Downloads
//      siblings.
//   4. `SettingsRoute` — deep-link identifier so the "Free up space"
//      affordance in `DownloadNextView` (playhead-hkg8) can jump directly
//      to the Storage group.
//   5. `DiagnosticsVersions` — resolves the pipeline/model/policy/feature-
//      schema version strings surfaced by the Diagnostics group.
//
// None of these types hold SwiftUI state. `SettingsView` composes them.
//
// Copy exactness is load-bearing: every string here is the canonical
// rendering the UI emits. Tests in `SettingsL274CopyTests` pin each one
// character-for-character. Any edit here is a product decision — update
// the bd spec and tests together.

import Foundation

// MARK: - Copy (verbatim)

/// Namespace for every user-visible string rendered by the Downloads /
/// Storage / Diagnostics settings groups. Test-pinned — do not inline any
/// of these literals into the SwiftUI body directly.
enum SettingsL274Copy {

    // MARK: Section headers
    static let downloadsHeader: String = "Downloads"
    static let storageHeader: String = "Storage"
    static let diagnosticsHeader: String = "Diagnostics"

    // MARK: Downloads group — control labels
    /// Global default for auto-downloading newly published episodes of
    /// subscribed shows. Matches UI design §F.
    static let autoDownloadOnSubscribeLabel: String = "Auto-download on subscribe"
    static let downloadOverCellularLabel: String = "Download over cellular"
    static let downloadNextDefaultCountLabel: String = "\"Download Next N\" default count"

    // MARK: Storage group — control labels
    static let episodeStorageCapLabel: String = "Episode storage cap"
    static let currentUsageLabel: String = "Current usage"
    static let keepAnalysisToggleLabel: String = "Keep analysis when removing downloads"
    /// Sub-line beneath the "Keep analysis" toggle. Verbatim per spec.
    static let keepAnalysisSubLine: String = "Keeps analysis for many episodes; exact count depends on episode size and retained bundle version."
    static let analysisCapLabel: String = "Analysis cap"
    /// Read-only line describing the auto-evict policy. Verbatim per spec.
    static let autoEvictPolicyLine: String = "Oldest played episodes are removed first."

    // MARK: Diagnostics group — control labels
    static let pipelineVersionLabel: String = "Pipeline version"
    static let modelVersionsLabel: String = "Model versions"
    static let policyVersionLabel: String = "Policy version"
    static let featureSchemaVersionLabel: String = "Feature-schema version"
    static let schedulerEventsLabel: String = "Last 50 scheduler events"
    static let perShowCapabilityProfileLabel: String = "Per-show capability profile"
    static let featureFlagsLabel: String = "Feature flags (rollback)"
    static let sendDiagnosticsButtonLabel: String = "Send diagnostics"
    /// Footer reassuring the user diagnostics are NEVER auto-uploaded.
    static let sendDiagnosticsFooter: String = "Opens Mail with a support-safe bundle attached. Never auto-uploads."
}

// MARK: - AutoDownloadOnSubscribe

/// Auto-download policy applied to newly published episodes from
/// subscribed shows. See UI design §F for the canonical four-option list.
enum AutoDownloadOnSubscribe: String, Codable, Sendable, CaseIterable, Hashable {
    case off
    case last1
    case last3
    case all

    /// Default value applied when no user preference is persisted.
    static let defaultValue: AutoDownloadOnSubscribe = .off

    /// Human-facing label for the picker. Verbatim per spec.
    var displayLabel: String {
        switch self {
        case .off:   return "Off"
        case .last1: return "Last 1"
        case .last3: return "Last 3"
        case .all:   return "All"
        }
    }
}

// MARK: - CellularPolicy

/// Policy for whether new downloads may proceed over cellular. Mirrors
/// (but is independent of) `UserPreferences.allowsCellular`, which the
/// download manager consumes directly. The `.askEachTime` branch is UX
/// only; the runtime gate still defaults to Off until the user answers.
enum CellularPolicy: String, Codable, Sendable, CaseIterable, Hashable {
    case off
    case askEachTime
    case on

    /// Default per the UI design doc (§F).
    static let defaultValue: CellularPolicy = .askEachTime

    /// Human-facing label. Verbatim per spec.
    var displayLabel: String {
        switch self {
        case .off:          return "Off"
        case .askEachTime:  return "Ask each time"
        case .on:           return "On"
        }
    }
}

// MARK: - DownloadNextDefaultCount

/// Default `N` for the "Download Next N" affordance on the show page.
/// Persisted independently of the one-off picker on that affordance —
/// the show-page picker loads this value as its initial selection.
enum DownloadNextDefaultCount: Int, Codable, Sendable, CaseIterable, Hashable {
    case one    = 1
    case three  = 3
    case five   = 5
    case ten    = 10

    /// Default per the UI design doc (§F).
    static let defaultValue: DownloadNextDefaultCount = .three

    /// Display label for the menu entry. Verbatim per spec.
    var displayLabel: String {
        "\(rawValue)"
    }
}

// MARK: - EpisodeStorageCap

/// User-selectable episode-storage-cap choices surfaced in Settings →
/// Storage. Persists through `StorageBudgetSettings.mediaCapBytes`
/// (playhead-h7r) — `.unlimited` encodes as `Int64.max`.
enum EpisodeStorageCap: Sendable, Equatable, Hashable, CaseIterable {
    case gb1
    case gb5
    case gb10
    case gb25
    case gb50
    case unlimited

    /// Default per the UI design doc (§F): 10 GB.
    static let defaultValue: EpisodeStorageCap = .gb10

    /// SI-byte representation. Matches `StorageBudget` which uses decimal
    /// gigabytes (1 GB = 1_000_000_000 bytes).
    var bytes: Int64 {
        switch self {
        case .gb1:        return 1 * 1_000_000_000
        case .gb5:        return 5 * 1_000_000_000
        case .gb10:       return 10 * 1_000_000_000
        case .gb25:       return 25 * 1_000_000_000
        case .gb50:       return 50 * 1_000_000_000
        case .unlimited:  return Int64.max
        }
    }

    /// Picker label. Verbatim per spec.
    var displayLabel: String {
        switch self {
        case .gb1:        return "1 GB"
        case .gb5:        return "5 GB"
        case .gb10:       return "10 GB"
        case .gb25:       return "25 GB"
        case .gb50:       return "50 GB"
        case .unlimited:  return "Unlimited"
        }
    }

    /// Round-trip from a persisted bytes value. Non-canonical byte counts
    /// snap to the nearest choice at or above the persisted size so
    /// admission control continues to honor the persisted cap.
    static func from(bytes: Int64) -> EpisodeStorageCap {
        if bytes >= Int64.max / 2 {
            return .unlimited
        }
        // Exact-match fast path.
        for choice in EpisodeStorageCap.allCases where choice.bytes == bytes {
            return choice
        }
        // Nearest-upward match so an admission cap never silently shrinks.
        let ordered: [EpisodeStorageCap] = [.gb1, .gb5, .gb10, .gb25, .gb50, .unlimited]
        for choice in ordered where bytes <= choice.bytes {
            return choice
        }
        return .unlimited
    }
}

// MARK: - DownloadsSettings persistence

/// UserDefaults-backed persistence for the three Downloads controls that
/// don't already live in `UserPreferences` / `StorageBudgetSettings`.
/// Keys are namespaced under `SettingsL274.downloads.*` so a future
/// relocation to SwiftData is a single read-path change.
struct DownloadsSettings: Sendable, Equatable {
    var autoDownloadOnSubscribe: AutoDownloadOnSubscribe
    var cellularPolicy: CellularPolicy
    var downloadNextDefaultCount: DownloadNextDefaultCount

    init(
        autoDownloadOnSubscribe: AutoDownloadOnSubscribe = .defaultValue,
        cellularPolicy: CellularPolicy = .defaultValue,
        downloadNextDefaultCount: DownloadNextDefaultCount = .defaultValue
    ) {
        self.autoDownloadOnSubscribe = autoDownloadOnSubscribe
        self.cellularPolicy = cellularPolicy
        self.downloadNextDefaultCount = downloadNextDefaultCount
    }

    // MARK: Keys
    static let autoDownloadKey = "SettingsL274.downloads.autoDownloadOnSubscribe"
    static let cellularPolicyKey = "SettingsL274.downloads.cellularPolicy"
    static let downloadNextDefaultCountKey = "SettingsL274.downloads.downloadNextDefaultCount"

    // MARK: Load / save
    static func load(from defaults: UserDefaults = .standard) -> DownloadsSettings {
        let auto = (defaults.string(forKey: autoDownloadKey))
            .flatMap(AutoDownloadOnSubscribe.init(rawValue:))
            ?? .defaultValue

        let cell = (defaults.string(forKey: cellularPolicyKey))
            .flatMap(CellularPolicy.init(rawValue:))
            ?? .defaultValue

        let countValue = defaults.object(forKey: downloadNextDefaultCountKey) as? Int
        let count = countValue
            .flatMap(DownloadNextDefaultCount.init(rawValue:))
            ?? .defaultValue

        return DownloadsSettings(
            autoDownloadOnSubscribe: auto,
            cellularPolicy: cell,
            downloadNextDefaultCount: count
        )
    }

    func save(to defaults: UserDefaults = .standard) {
        defaults.set(autoDownloadOnSubscribe.rawValue, forKey: Self.autoDownloadKey)
        defaults.set(cellularPolicy.rawValue, forKey: Self.cellularPolicyKey)
        defaults.set(downloadNextDefaultCount.rawValue, forKey: Self.downloadNextDefaultCountKey)
    }
}

// MARK: - SettingsRoute (deep-link)

/// Identifier for a deep-link target inside `SettingsView`. Today only
/// the `.storage` case is wired (for the `DownloadNextView` "Free up
/// space" CTA, closing the hkg8 TODO); the enum shape leaves room for
/// future destinations (`.downloads`, `.diagnostics`).
///
/// Routing semantics: when a route is pushed into the `SettingsRouter`,
/// the settings surface observes the change and scrolls/focuses to the
/// matching group anchor. The route is cleared after delivery so a
/// subsequent identical tap re-fires.
enum SettingsRoute: String, Sendable, Equatable, Hashable {
    case downloads
    case storage
    case diagnostics

    /// Scroll-to anchor id used by `SettingsView` to position each group.
    var anchorId: String { "settings.route.\(rawValue)" }
}

/// Shared router that lets deep-link entry points (the "Free up space"
/// affordance, URL schemes, etc.) request navigation to a specific
/// `SettingsRoute`. Observers on the Settings tab consume `pending` and
/// call `consume()` once they've honored it.
@MainActor
@Observable
final class SettingsRouter {
    private(set) var pending: SettingsRoute?

    init(initial: SettingsRoute? = nil) {
        self.pending = initial
    }

    /// Request the settings surface open at `route`. If a prior route is
    /// still unconsumed, it is replaced (the freshest request wins).
    func request(_ route: SettingsRoute) {
        pending = route
    }

    /// Called by the settings surface once it has delivered the route.
    func consume() {
        pending = nil
    }
}

// MARK: - SettingsRouter SwiftUI environment

import SwiftUI

/// SwiftUI environment key for the shared `SettingsRouter`. Tab-root
/// views (Library, Browse, Settings) install the same instance via
/// `.environment(\.settingsRouter, router)`; deep-link entry points
/// (e.g. `DownloadNextView.onFreeUpSpace`) read it to push a route.
private struct SettingsRouterKey: @preconcurrency EnvironmentKey {
    @MainActor static let defaultValue: SettingsRouter? = nil
}

extension EnvironmentValues {
    /// Shared deep-link router. Nil in previews/tests that don't install
    /// one; call sites guard with `if let router = ...`.
    var settingsRouter: SettingsRouter? {
        get { self[SettingsRouterKey.self] }
        set { self[SettingsRouterKey.self] = newValue }
    }
}

// MARK: - DiagnosticsVersions

/// Resolver for the four version strings surfaced in the Diagnostics
/// group. Kept as a nested namespace + one init-point struct so tests
/// can pin the shape without touching the real runtime.
///
/// Field semantics (playhead-7mq version columns):
///   - `pipelineVersion`       — overall analysis pipeline build id.
///     Sourced from the app's bundle short version, matching what
///     `DebugDiagnosticsHatch.appVersionString()` uses for the
///     default-bundle `app_version` field.
///   - `transcriptModelVersion` — `TranscriptEngineService.Config`
///     default (`"apple-speech-v1"` today).
///   - `adDetectionModelVersion` — `AdDetectionService` default id.
///   - `policyVersion`         — `SkipOrchestrator.Config` default
///     (`"skip-policy-v1"` today).
///   - `featureSchemaVersion`  — `FeatureWindow` schema version.
struct DiagnosticsVersions: Sendable, Equatable {
    let pipelineVersion: String
    let transcriptModelVersion: String
    let adDetectionModelVersion: String
    let policyVersion: String
    let featureSchemaVersion: String

    /// Default resolver wired from `Bundle.main` and the configured
    /// service defaults. Call sites that want a deterministic snapshot
    /// (tests, snapshot-style exports) construct the struct directly.
    static func current(bundle: Bundle = .main) -> DiagnosticsVersions {
        let short = (bundle.infoDictionary?["CFBundleShortVersionString"] as? String) ?? "unknown"
        return DiagnosticsVersions(
            pipelineVersion: short,
            transcriptModelVersion: "apple-speech-v1",
            adDetectionModelVersion: "hot-path-replay",
            policyVersion: "skip-policy-v1",
            featureSchemaVersion: "1"
        )
    }
}

// MARK: - Feature flag placeholders

/// Placeholder storage shape for the Diagnostics → Feature flags toggle
/// group. Each flag bead (xr3t, zx6i, 2hpn, 43ed) is OPEN — when those
/// beads land they will supply the real storage + rollback wiring and
/// this shim will be replaced at the call site. Defaults must remain
/// `false` across all flags.
///
/// Identifiers match the bd slugs so grep-cross-references are trivial:
/// a flag named `zx6i` in the UI maps to bd playhead-zx6i.
struct FeatureFlagPlaceholders: Sendable, Equatable {
    /// Stable, user-facing ordering of the flags in the Diagnostics
    /// group. Kept as an explicit array so tests can pin the render order.
    static let orderedSlugs: [String] = ["xr3t", "zx6i", "2hpn", "43ed"]

    /// Default (off) values, one per slug.
    static var defaultValues: [String: Bool] {
        var dict: [String: Bool] = [:]
        for slug in orderedSlugs {
            dict[slug] = false
        }
        return dict
    }
}
