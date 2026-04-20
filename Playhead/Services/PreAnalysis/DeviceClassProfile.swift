// DeviceClassProfile.swift
// playhead-dh9b B1 — per-device-class grant-window + slice-sizing config.
//
// Shipped as both a bundled JSON asset (PreAnalysisConfig.json) and a
// hard-coded Swift fallback (`DeviceClassProfile.fallback(for:)`) so a
// missing or malformed bundle resource is a loud bug but never a crash.
//
// Phase 3 (playhead-beh3) will replace this static table with an
// adaptive estimator. Until then the values are best-effort seeds; the
// empirical 95% slice-completion gate lives in Phase 1 acceptance (not
// this bead's landing gate).

import Foundation

/// One row of the device-class table.
///
/// Every integer field maps 1:1 to the schema in `PreAnalysisConfig.json`.
/// `deviceClass` is carried as a `String` so the JSON file is
/// human-readable and resilient to future enum cases being added
/// without bumping the manifest `version`.
struct DeviceClassProfile: Codable, Sendable, Equatable {
    let deviceClass: String
    let grantWindowMedianSeconds: Int
    let grantWindowP95Seconds: Int
    let nominalSliceSizeBytes: Int
    let cpuWindowSeconds: Int
    /// Consumed by playhead-bnrs slice-sizing formula.
    let bytesPerCpuSecond: Int
    /// Consumed by playhead-44h1 Live Activity ETA.
    let avgShardDurationMs: Int

    // MARK: - Hard-coded Fallback

    /// Fallback profile for every `DeviceClass` case. Used when the
    /// bundled `PreAnalysisConfig.json` is missing or malformed. Values
    /// match the seed table in the playhead-dh9b spec.
    ///
    /// Any new `DeviceClass` case MUST extend this switch. The compiler
    /// enforces exhaustiveness so the fallback path can never silently
    /// return `nil` for a known bucket.
    static func fallback(for deviceClass: DeviceClass) -> DeviceClassProfile {
        switch deviceClass {
        case .iPhone17Pro:
            return DeviceClassProfile(
                deviceClass: deviceClass.rawValue,
                grantWindowMedianSeconds: 45,
                grantWindowP95Seconds: 90,
                nominalSliceSizeBytes: 25_000_000,
                cpuWindowSeconds: 40,
                bytesPerCpuSecond: 625_000,
                avgShardDurationMs: 2500
            )
        case .iPhone17:
            return DeviceClassProfile(
                deviceClass: deviceClass.rawValue,
                grantWindowMedianSeconds: 40,
                grantWindowP95Seconds: 85,
                nominalSliceSizeBytes: 22_000_000,
                cpuWindowSeconds: 35,
                bytesPerCpuSecond: 628_000,
                avgShardDurationMs: 2800
            )
        case .iPhone16Pro:
            return DeviceClassProfile(
                deviceClass: deviceClass.rawValue,
                grantWindowMedianSeconds: 40,
                grantWindowP95Seconds: 85,
                nominalSliceSizeBytes: 20_000_000,
                cpuWindowSeconds: 35,
                bytesPerCpuSecond: 571_000,
                avgShardDurationMs: 2900
            )
        case .iPhone16:
            return DeviceClassProfile(
                deviceClass: deviceClass.rawValue,
                grantWindowMedianSeconds: 35,
                grantWindowP95Seconds: 75,
                nominalSliceSizeBytes: 18_000_000,
                cpuWindowSeconds: 30,
                bytesPerCpuSecond: 600_000,
                avgShardDurationMs: 3200
            )
        case .iPhone15Pro:
            return DeviceClassProfile(
                deviceClass: deviceClass.rawValue,
                grantWindowMedianSeconds: 35,
                grantWindowP95Seconds: 75,
                nominalSliceSizeBytes: 16_000_000,
                cpuWindowSeconds: 30,
                bytesPerCpuSecond: 533_000,
                avgShardDurationMs: 3400
            )
        case .iPhone15:
            // A16 Bionic: sits between A17 Pro (iPhone15Pro) and A15
            // (iPhone14andOlder). Seed values interpolate the adjacent
            // buckets; Phase 3 will replace this with empirical data.
            return DeviceClassProfile(
                deviceClass: deviceClass.rawValue,
                grantWindowMedianSeconds: 30,
                grantWindowP95Seconds: 65,
                nominalSliceSizeBytes: 12_000_000,
                cpuWindowSeconds: 25,
                bytesPerCpuSecond: 520_000,
                avgShardDurationMs: 4200
            )
        case .iPhoneSE3:
            return DeviceClassProfile(
                deviceClass: deviceClass.rawValue,
                grantWindowMedianSeconds: 25,
                grantWindowP95Seconds: 55,
                nominalSliceSizeBytes: 10_000_000,
                cpuWindowSeconds: 20,
                bytesPerCpuSecond: 500_000,
                avgShardDurationMs: 4500
            )
        case .iPhone14andOlder:
            return DeviceClassProfile(
                deviceClass: deviceClass.rawValue,
                grantWindowMedianSeconds: 20,
                grantWindowP95Seconds: 45,
                nominalSliceSizeBytes: 8_000_000,
                cpuWindowSeconds: 15,
                bytesPerCpuSecond: 533_000,
                avgShardDurationMs: 5500
            )
        }
    }

    /// Complete fallback table indexed by DeviceClass. Used when the
    /// bundled JSON resource is missing entirely.
    static func fallbackTable() -> [DeviceClass: DeviceClassProfile] {
        var table: [DeviceClass: DeviceClassProfile] = [:]
        for bucket in DeviceClass.allCases {
            table[bucket] = fallback(for: bucket)
        }
        return table
    }
}

/// Top-level manifest shape for `PreAnalysisConfig.json`.
///
/// `version` is tied to the app build; there are no OTA updates in
/// Phase 1. Bumping `version` is a schema change and must be
/// accompanied by a matching `PreAnalysisConfig.manifestVersion`
/// constant (so `loadDeviceProfiles()` can refuse to decode a manifest
/// from a newer app build that this binary doesn't understand).
struct DeviceClassProfilesManifest: Codable, Sendable, Equatable {
    let version: Int
    let profiles: [DeviceClassProfile]
}
