// PreAnalysisConfig.swift
// User-configurable settings for the pre-analysis pipeline.
// Persisted to UserDefaults as JSON.

import Foundation

struct PreAnalysisConfig: Codable, Sendable {
    var isEnabled: Bool = true
    var defaultT0DepthSeconds: Double = 90
    var t1DepthSeconds: Double = 300
    var t2DepthSeconds: Double = 900
    static let analysisVersion: Int = 1

    private static let key = "PreAnalysisConfig"

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
