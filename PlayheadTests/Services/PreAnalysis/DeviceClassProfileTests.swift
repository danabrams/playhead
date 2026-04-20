// DeviceClassProfileTests.swift
// playhead-dh9b landing-gate tests for the device-class profile
// manifest, its JSON round-trip, the bundled seed file, and the
// fallback-to-hardcoded path triggered by a stripped bundle.

import Foundation
import Testing

@testable import Playhead

@Suite("DeviceClassProfile")
struct DeviceClassProfileTests {

    // MARK: - JSON round-trip

    @Test("Manifest + profile round-trip through JSONEncoder/Decoder")
    func testManifestJSONRoundTrip() throws {
        let seed = DeviceClassProfilesManifest(
            version: 1,
            profiles: DeviceClass.allCases.map { DeviceClassProfile.fallback(for: $0) }
        )

        let data = try JSONEncoder().encode(seed)
        let decoded = try JSONDecoder().decode(DeviceClassProfilesManifest.self, from: data)

        #expect(decoded == seed)
        #expect(decoded.version == 1)
        #expect(decoded.profiles.count == DeviceClass.allCases.count)
    }

    @Test("DeviceClassProfile preserves every integer field through JSON")
    func testProfileFieldPreservation() throws {
        let profile = DeviceClassProfile(
            deviceClass: DeviceClass.iPhone17Pro.rawValue,
            grantWindowMedianSeconds: 45,
            grantWindowP95Seconds: 90,
            nominalSliceSizeBytes: 25_000_000,
            cpuWindowSeconds: 40,
            bytesPerCpuSecond: 625_000,
            avgShardDurationMs: 2500
        )

        let data = try JSONEncoder().encode(profile)
        let decoded = try JSONDecoder().decode(DeviceClassProfile.self, from: data)

        #expect(decoded.deviceClass == "iPhone17Pro")
        #expect(decoded.grantWindowMedianSeconds == 45)
        #expect(decoded.grantWindowP95Seconds == 90)
        #expect(decoded.nominalSliceSizeBytes == 25_000_000)
        #expect(decoded.cpuWindowSeconds == 40)
        #expect(decoded.bytesPerCpuSecond == 625_000)
        #expect(decoded.avgShardDurationMs == 2500)
    }

    // MARK: - Fallback coverage

    @Test("fallback(for:) returns a non-nil profile for every DeviceClass")
    func testFallbackCoversEveryCase() {
        for bucket in DeviceClass.allCases {
            let profile = DeviceClassProfile.fallback(for: bucket)
            #expect(profile.deviceClass == bucket.rawValue)
            #expect(profile.grantWindowMedianSeconds > 0)
            #expect(profile.grantWindowP95Seconds > profile.grantWindowMedianSeconds)
            #expect(profile.nominalSliceSizeBytes > 0)
            #expect(profile.cpuWindowSeconds > 0)
            #expect(profile.bytesPerCpuSecond > 0)
            #expect(profile.avgShardDurationMs > 0)
        }
    }

    @Test("fallbackTable covers every DeviceClass case")
    func testFallbackTableCoversEveryCase() {
        let table = DeviceClassProfile.fallbackTable()
        #expect(table.count == DeviceClass.allCases.count)
        for bucket in DeviceClass.allCases {
            #expect(table[bucket] != nil, "fallbackTable missing bucket \(bucket)")
        }
    }

    @Test("Fallback values match the seed table in the bead spec")
    func testFallbackValuesMatchSpecTable() {
        let specTable: [(DeviceClass, Int, Int, Int, Int, Int, Int)] = [
            (.iPhone17Pro,      45, 90, 25_000_000, 40, 625_000, 2500),
            (.iPhone17,         40, 85, 22_000_000, 35, 628_000, 2800),
            (.iPhone16Pro,      40, 85, 20_000_000, 35, 571_000, 2900),
            (.iPhone16,         35, 75, 18_000_000, 30, 600_000, 3200),
            (.iPhone15Pro,      35, 75, 16_000_000, 30, 533_000, 3400),
            (.iPhone15,         30, 65, 12_000_000, 25, 520_000, 4200),
            (.iPhoneSE3,        25, 55, 10_000_000, 20, 500_000, 4500),
            (.iPhone14andOlder, 20, 45,  8_000_000, 15, 533_000, 5500),
        ]

        for (bucket, median, p95, sliceBytes, cpuWin, bytesPerCpu, shardMs) in specTable {
            let profile = DeviceClassProfile.fallback(for: bucket)
            #expect(profile.grantWindowMedianSeconds == median, "\(bucket) median")
            #expect(profile.grantWindowP95Seconds == p95, "\(bucket) p95")
            #expect(profile.nominalSliceSizeBytes == sliceBytes, "\(bucket) slice bytes")
            #expect(profile.cpuWindowSeconds == cpuWin, "\(bucket) cpuWindow")
            #expect(profile.bytesPerCpuSecond == bytesPerCpu, "\(bucket) bytesPerCpu")
            #expect(profile.avgShardDurationMs == shardMs, "\(bucket) shardMs")
        }
    }

    // MARK: - loadDeviceProfiles: bundle success path

    @Test("loadDeviceProfiles(bundle: Bundle(for: Playhead class)) loads bundled JSON")
    func testLoadDeviceProfilesFromHostBundle() {
        // The PlayheadTests bundle does not carry PreAnalysisConfig.json
        // itself — the resource is bundled into the host app (Playhead).
        // PlayheadFastTests runs with the host app present, so
        // `Bundle.main` during unit tests is the host app's bundle.
        let (table, outcome) = PreAnalysisConfig.loadDeviceProfiles(bundle: .main)

        #expect(outcome == .bundleJSON,
                "expected host bundle to carry PreAnalysisConfig.json; got \(outcome)")
        #expect(table.count == DeviceClass.allCases.count)

        // Every bucket must resolve to a profile with matching rawValue.
        for bucket in DeviceClass.allCases {
            let profile = table[bucket]
            #expect(profile != nil, "missing profile for \(bucket)")
            #expect(profile?.deviceClass == bucket.rawValue)
        }

        // Spot-check: bundle values must match the fallback table for
        // the Phase 1 seed manifest (Phase 3 adaptive tuning lives in
        // playhead-beh3, not this bead).
        #expect(table[.iPhone17Pro] == DeviceClassProfile.fallback(for: .iPhone17Pro))
        #expect(table[.iPhoneSE3] == DeviceClassProfile.fallback(for: .iPhoneSE3))
    }

    // MARK: - loadDeviceProfiles: fallback paths

    @Test("Stripped bundle (no PreAnalysisConfig.json) hits fallbackMissingResource")
    func testLoadDeviceProfilesFallbackMissing() {
        // `Bundle(for:)` on an NSObject subclass inside the test bundle
        // is NOT the app bundle, so the resource is not present there
        // unless we've also registered it into the test target — which
        // we intentionally do not do. That makes this a reliable
        // stripped-bundle seam.
        let testBundle = Bundle(for: StrippedBundleMarker.self)
        // Defensive: if someone ever does add PreAnalysisConfig.json to
        // the test bundle, this test would silently pass the bundleJSON
        // branch instead. Assert the precondition.
        #expect(
            testBundle.url(forResource: "PreAnalysisConfig", withExtension: "json") == nil,
            "PreAnalysisConfig.json should NOT be registered in the test bundle — the stripped-bundle fallback test relies on it being absent."
        )

        let (table, outcome) = PreAnalysisConfig.loadDeviceProfiles(bundle: testBundle)

        #expect(outcome == .fallbackMissingResource)
        #expect(table.count == DeviceClass.allCases.count)
        for bucket in DeviceClass.allCases {
            #expect(table[bucket] == DeviceClassProfile.fallback(for: bucket))
        }
    }

    @Test("Malformed JSON in a custom bundle triggers fallbackMalformedJSON")
    func testLoadDeviceProfilesFallbackMalformed() throws {
        // Write a malformed JSON file into a temp directory and point
        // a synthetic Bundle at it.
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("dh9b-malformed-\(UUID().uuidString)")
        try FileManager.default.createDirectory(
            at: tmpDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: tmpDir) }

        let url = tmpDir.appendingPathComponent("PreAnalysisConfig.json")
        try Data("{ not-valid-json".utf8).write(to: url)

        guard let bundle = Bundle(url: tmpDir) else {
            Issue.record("could not create Bundle at \(tmpDir.path)")
            return
        }

        let (table, outcome) = PreAnalysisConfig.loadDeviceProfiles(bundle: bundle)

        switch outcome {
        case .fallbackMalformedJSON:
            break
        default:
            Issue.record("expected .fallbackMalformedJSON, got \(outcome)")
        }
        #expect(table.count == DeviceClass.allCases.count)
        for bucket in DeviceClass.allCases {
            #expect(table[bucket] == DeviceClassProfile.fallback(for: bucket))
        }
    }

    @Test("Version mismatch in manifest triggers fallbackMalformedJSON")
    func testLoadDeviceProfilesVersionMismatch() throws {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("dh9b-version-\(UUID().uuidString)")
        try FileManager.default.createDirectory(
            at: tmpDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: tmpDir) }

        let futureManifest = DeviceClassProfilesManifest(
            version: PreAnalysisConfig.deviceProfilesManifestVersion + 42,
            profiles: DeviceClass.allCases.map { DeviceClassProfile.fallback(for: $0) }
        )
        let data = try JSONEncoder().encode(futureManifest)
        try data.write(to: tmpDir.appendingPathComponent("PreAnalysisConfig.json"))

        guard let bundle = Bundle(url: tmpDir) else {
            Issue.record("could not create Bundle at \(tmpDir.path)")
            return
        }

        let (table, outcome) = PreAnalysisConfig.loadDeviceProfiles(bundle: bundle)

        switch outcome {
        case .fallbackMalformedJSON(let reason):
            #expect(reason.contains("version mismatch"))
        default:
            Issue.record("expected version-mismatch fallback, got \(outcome)")
        }
        #expect(table.count == DeviceClass.allCases.count)
    }

    @Test("Unknown deviceClass rows in JSON are ignored; fallback fills the gap")
    func testLoadDeviceProfilesUnknownRowIgnored() throws {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("dh9b-unknown-row-\(UUID().uuidString)")
        try FileManager.default.createDirectory(
            at: tmpDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: tmpDir) }

        // Manifest has only 2 real rows + 1 unknown row.
        let json = """
        {
          "version": 1,
          "profiles": [
            {
              "deviceClass": "iPhone17Pro",
              "grantWindowMedianSeconds": 99,
              "grantWindowP95Seconds": 199,
              "nominalSliceSizeBytes": 99,
              "cpuWindowSeconds": 99,
              "bytesPerCpuSecond": 99,
              "avgShardDurationMs": 99
            },
            {
              "deviceClass": "iPhoneOfTheFuture",
              "grantWindowMedianSeconds": 1,
              "grantWindowP95Seconds": 2,
              "nominalSliceSizeBytes": 1,
              "cpuWindowSeconds": 1,
              "bytesPerCpuSecond": 1,
              "avgShardDurationMs": 1
            }
          ]
        }
        """
        try Data(json.utf8).write(
            to: tmpDir.appendingPathComponent("PreAnalysisConfig.json")
        )

        guard let bundle = Bundle(url: tmpDir) else {
            Issue.record("could not create Bundle at \(tmpDir.path)")
            return
        }

        let (table, outcome) = PreAnalysisConfig.loadDeviceProfiles(bundle: bundle)
        #expect(outcome == .bundleJSON)
        // iPhone17Pro was overridden by the JSON.
        #expect(table[.iPhone17Pro]?.grantWindowMedianSeconds == 99)
        // Other buckets still resolve (fallback).
        #expect(table[.iPhoneSE3] == DeviceClassProfile.fallback(for: .iPhoneSE3))
        // iPhoneOfTheFuture wasn't added — table size still matches.
        #expect(table.count == DeviceClass.allCases.count)
    }

    // MARK: - CapabilitySnapshot surface (playhead-dh9b: expose deviceClass)

    @Test("CapabilitySnapshot.deviceClass returns a valid DeviceClass case")
    func testCapabilitySnapshotExposesDeviceClass() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            foundationModelsUsable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        #expect(DeviceClass.allCases.contains(snapshot.deviceClass))
    }
}

/// Marker class used to scope `Bundle(for:)` to the test bundle —
/// guarantees `testLoadDeviceProfilesFallbackMissing` is targeting a
/// bundle that does NOT ship `PreAnalysisConfig.json`.
private final class StrippedBundleMarker {}
