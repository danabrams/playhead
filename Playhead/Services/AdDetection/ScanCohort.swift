// ScanCohort.swift
// Provenance for Foundation Models scan outputs. Any change here invalidates
// cached FM scan results and requires a rescan.

import Foundation

struct ScanCohort: Codable, Sendable, Hashable {
    let promptLabel: String
    let promptHash: String
    let schemaHash: String
    let scanPlanHash: String
    let normalizationHash: String
    let osBuild: String
    let locale: String
    let appBuild: String
}

extension ScanCohort {
    /// Canonical production cohort. All hash fields are static constants bumped
    /// whenever the prompt, schema, scan planner, or normalization pipeline
    /// changes; `osBuild`, `locale`, and `appBuild` are captured at runtime.
    /// Any field change invalidates the cached `semantic_scan_results` rows.
    static func production() -> ScanCohort {
        ScanCohort(
            promptLabel: "phase3-shadow-v1",
            promptHash: "phase3-prompt-2026-04-06",
            schemaHash: "phase3-schema-2026-04-06",
            scanPlanHash: "phase3-plan-2026-04-06",
            normalizationHash: "phase3-norm-2026-04-06",
            osBuild: {
                let v = ProcessInfo.processInfo.operatingSystemVersion
                return "\(v.majorVersion).\(v.minorVersion).\(v.patchVersion)"
            }(),
            locale: Locale.current.identifier,
            appBuild: Bundle.main.infoDictionary?["CFBundleVersion"] as? String ?? "unknown"
        )
    }

    /// JSON-encoded form of `production()` for persistence. Stable within a
    /// single binary run; changes automatically when the cohort fields change.
    static func productionJSON() -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        // `production()` is pure value types — encoding cannot fail.
        // swiftlint:disable:next force_try
        let data = try! encoder.encode(production())
        return String(data: data, encoding: .utf8) ?? "{}"
    }
}
