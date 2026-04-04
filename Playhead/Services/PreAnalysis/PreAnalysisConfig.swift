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
              let config = try? JSONDecoder().decode(PreAnalysisConfig.self, from: data)
        else { return PreAnalysisConfig() }
        return config
    }

    func save() {
        if let data = try? JSONEncoder().encode(self) {
            UserDefaults.standard.set(data, forKey: Self.key)
        }
    }
}
