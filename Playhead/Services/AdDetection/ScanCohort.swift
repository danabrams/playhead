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
