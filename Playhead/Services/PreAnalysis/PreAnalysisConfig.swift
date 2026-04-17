// PreAnalysisConfig.swift
// User-configurable settings for the pre-analysis pipeline.
// Persisted to UserDefaults as JSON.

import Foundation

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

    static let analysisVersion: Int = 1

    private static let key = "PreAnalysisConfig"

    init(
        isEnabled: Bool = true,
        defaultT0DepthSeconds: Double = 90,
        t1DepthSeconds: Double = 300,
        t2DepthSeconds: Double = 900,
        useDualBackgroundSessions: Bool = false
    ) {
        self.isEnabled = isEnabled
        self.defaultT0DepthSeconds = defaultT0DepthSeconds
        self.t1DepthSeconds = t1DepthSeconds
        self.t2DepthSeconds = t2DepthSeconds
        self.useDualBackgroundSessions = useDualBackgroundSessions
    }

    // Custom decoder so configs persisted before 24cm (which lack the
    // `useDualBackgroundSessions` key) still decode — absent keys fall
    // back to `false`, matching the new default.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.isEnabled = try container.decodeIfPresent(Bool.self, forKey: .isEnabled) ?? true
        self.defaultT0DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .defaultT0DepthSeconds) ?? 90
        self.t1DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .t1DepthSeconds) ?? 300
        self.t2DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .t2DepthSeconds) ?? 900
        self.useDualBackgroundSessions = try container.decodeIfPresent(Bool.self, forKey: .useDualBackgroundSessions) ?? false
    }

    private enum CodingKeys: String, CodingKey {
        case isEnabled
        case defaultT0DepthSeconds
        case t1DepthSeconds
        case t2DepthSeconds
        case useDualBackgroundSessions
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
}
