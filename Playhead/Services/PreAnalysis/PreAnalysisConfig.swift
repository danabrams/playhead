// PreAnalysisConfig.swift
// User-configurable settings for the pre-analysis pipeline.
// Persisted to UserDefaults as JSON.
//
// Also hosts the playhead-dh9b device-class profile loader
// (`loadDeviceProfiles(...)`), which reads the bundled
// `PreAnalysisConfig.json` and falls back to the hard-coded table in
// `DeviceClassProfile.fallback(for:)` when the bundle resource is
// missing or malformed.

import Foundation
import OSLog

struct PreAnalysisConfig: Codable, Sendable {
    var isEnabled: Bool = true
    var defaultT0DepthSeconds: Double = 90
    var t1DepthSeconds: Double = 300
    var t2DepthSeconds: Double = 900

    /// playhead-24cm feature flag: when `true`, the download manager
    /// splits background transfers across two new URLSession
    /// configurations (`interactive` + `maintenance`). Defaults to
    /// `false` so production keeps using the single legacy session
    /// until the flag is flipped per-beta-cohort.
    var useDualBackgroundSessions: Bool = false

    /// playhead-44h1: nominal shard duration (seconds) used by the Live
    /// Activity ETA formula to estimate `totalShardsEstimate =
    /// ceil(episode.durationSec / nominalShardDurationSec)`. This is an
    /// estimator input only — the actual shard boundaries are still
    /// produced by the audio decoder during analysis. Default 20 s.
    var nominalShardDurationSec: Double = 20

    static let analysisVersion: Int = 1

    private static let key = "PreAnalysisConfig"

    init(
        isEnabled: Bool = true,
        defaultT0DepthSeconds: Double = 90,
        t1DepthSeconds: Double = 300,
        t2DepthSeconds: Double = 900,
        useDualBackgroundSessions: Bool = false,
        nominalShardDurationSec: Double = 20
    ) {
        self.isEnabled = isEnabled
        self.defaultT0DepthSeconds = defaultT0DepthSeconds
        self.t1DepthSeconds = t1DepthSeconds
        self.t2DepthSeconds = t2DepthSeconds
        self.useDualBackgroundSessions = useDualBackgroundSessions
        self.nominalShardDurationSec = nominalShardDurationSec
    }

    // Custom decoder so configs persisted before 24cm (which lack the
    // `useDualBackgroundSessions` key) still decode — absent keys fall
    // back to `false`, matching the new default. The 44h1
    // `nominalShardDurationSec` follows the same pattern: if the stored
    // config predates this bead, fall back to the 20 s default.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.isEnabled = try container.decodeIfPresent(Bool.self, forKey: .isEnabled) ?? true
        self.defaultT0DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .defaultT0DepthSeconds) ?? 90
        self.t1DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .t1DepthSeconds) ?? 300
        self.t2DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .t2DepthSeconds) ?? 900
        self.useDualBackgroundSessions = try container.decodeIfPresent(Bool.self, forKey: .useDualBackgroundSessions) ?? false
        self.nominalShardDurationSec = try container.decodeIfPresent(Double.self, forKey: .nominalShardDurationSec) ?? 20
    }

    private enum CodingKeys: String, CodingKey {
        case isEnabled
        case defaultT0DepthSeconds
        case t1DepthSeconds
        case t2DepthSeconds
        case useDualBackgroundSessions
        case nominalShardDurationSec
    }

    static func load() -> PreAnalysisConfig {
        guard let data = UserDefaults.standard.data(forKey: key),
              var config = try? JSONDecoder().decode(PreAnalysisConfig.self, from: data)
        else { return PreAnalysisConfig() }
        // Enforce ascending tier depths; reset to defaults if misconfigured.
        let defaults = PreAnalysisConfig()
        if !(config.defaultT0DepthSeconds < config.t1DepthSeconds
             && config.t1DepthSeconds < config.t2DepthSeconds) {
            config.defaultT0DepthSeconds = defaults.defaultT0DepthSeconds
            config.t1DepthSeconds = defaults.t1DepthSeconds
            config.t2DepthSeconds = defaults.t2DepthSeconds
        }
        return config
    }

    func save() {
        if let data = try? JSONEncoder().encode(self) {
            UserDefaults.standard.set(data, forKey: Self.key)
        }
    }

    // MARK: - playhead-dh9b: Device-Class Profile Loader

    /// The manifest schema version this binary understands. Must match
    /// the `version` field in `PreAnalysisConfig.json`. A mismatch is
    /// treated as a malformed bundle (fallback table used, loud log).
    static let deviceProfilesManifestVersion: Int = 1

    /// Bundle resource name for the device-class manifest (no extension).
    static let deviceProfilesResourceName: String = "PreAnalysisConfig"

    private static let deviceProfilesLogger = Logger(
        subsystem: "com.playhead",
        category: "PreAnalysisConfig"
    )

    /// Outcome of a `loadDeviceProfiles(...)` call, exposed for
    /// observability hooks and tests.
    enum DeviceProfilesLoadResult: Sendable, Equatable {
        /// Bundle JSON decoded cleanly and covered every DeviceClass
        /// case. No fallback values were used.
        case bundleJSON
        /// Bundle resource was missing. The hard-coded fallback table
        /// is returned; the caller should log a loud observability
        /// event (production expects the file to be present).
        case fallbackMissingResource
        /// Bundle resource was present but failed to decode, or the
        /// manifest version did not match `deviceProfilesManifestVersion`.
        case fallbackMalformedJSON(reason: String)
    }

    /// Loads the per-device-class profile table.
    ///
    /// Resolution order:
    ///   1. Bundled `PreAnalysisConfig.json` in `bundle` (default `.main`).
    ///   2. Hard-coded `DeviceClassProfile.fallback(for:)` for every
    ///      `DeviceClass` case.
    ///
    /// The return value always covers every `DeviceClass` case — even
    /// if the bundled JSON is incomplete, missing entries are patched
    /// in from the fallback table. This guarantees `result[someClass]`
    /// never returns nil at runtime.
    ///
    /// - Parameter bundle: Bundle to search. Tests pass a stripped
    ///   bundle (or `.init()`) to exercise the fallback path.
    /// - Returns: Tuple of (table, outcome). Callers that care about
    ///   which branch was taken (observability, tests) inspect
    ///   `outcome`; most callers just use `table`.
    static func loadDeviceProfiles(
        bundle: Bundle = .main
    ) -> (table: [DeviceClass: DeviceClassProfile], outcome: DeviceProfilesLoadResult) {
        guard let url = bundle.url(
            forResource: deviceProfilesResourceName,
            withExtension: "json"
        ) else {
            deviceProfilesLogger.error(
                "PreAnalysisConfig.json missing from bundle; using hard-coded fallback table"
            )
            return (DeviceClassProfile.fallbackTable(), .fallbackMissingResource)
        }

        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            let reason = "read failed: \(error.localizedDescription)"
            deviceProfilesLogger.error(
                "PreAnalysisConfig.json \(reason, privacy: .public); using fallback"
            )
            return (DeviceClassProfile.fallbackTable(), .fallbackMalformedJSON(reason: reason))
        }

        let manifest: DeviceClassProfilesManifest
        do {
            manifest = try JSONDecoder().decode(
                DeviceClassProfilesManifest.self,
                from: data
            )
        } catch {
            let reason = "decode failed: \(error.localizedDescription)"
            deviceProfilesLogger.error(
                "PreAnalysisConfig.json \(reason, privacy: .public); using fallback"
            )
            return (DeviceClassProfile.fallbackTable(), .fallbackMalformedJSON(reason: reason))
        }

        guard manifest.version == deviceProfilesManifestVersion else {
            let reason = "version mismatch (got \(manifest.version), expected \(deviceProfilesManifestVersion))"
            deviceProfilesLogger.error(
                "PreAnalysisConfig.json \(reason, privacy: .public); using fallback"
            )
            return (DeviceClassProfile.fallbackTable(), .fallbackMalformedJSON(reason: reason))
        }

        // Start from fallback so any DeviceClass not mentioned in the
        // JSON is still covered. Then overlay the JSON rows.
        var table = DeviceClassProfile.fallbackTable()
        for profile in manifest.profiles {
            guard let bucket = DeviceClass(rawValue: profile.deviceClass) else {
                // Unknown bucket in JSON (e.g., a row for a future
                // DeviceClass case this binary doesn't know about).
                // Ignored — the fallback row stays in place.
                deviceProfilesLogger.notice(
                    "PreAnalysisConfig.json contains unknown deviceClass=\(profile.deviceClass, privacy: .public); ignoring row"
                )
                continue
            }
            table[bucket] = profile
        }

        return (table, .bundleJSON)
    }
}
