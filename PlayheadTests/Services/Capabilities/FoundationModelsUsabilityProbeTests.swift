// FoundationModelsUsabilityProbeTests.swift
// Hermetic tests for the FM usability-probe cache. Verifies the TTL
// semantics that govern when a `usable == false` record stays trusted
// vs. forces a fresh probe. These tests construct an isolated
// `UserDefaults` suite per case, so they are safe to run in parallel
// and never touch the shared `.standard` defaults.
//
// Scope:
//   * `usable == true` records remain valid indefinitely until the
//     osBuild/boot pair changes.
//   * `usable == false` records are honored only within `falseCacheTTL`
//     of their `cachedAt`; older records read as `nil` (re-probe).
//   * The Codable cache record round-trips with the new `cachedAt`
//     field intact.
//   * Old-schema records persisted by the pre-TTL release (no
//     `cachedAt` field) decode cleanly: a stale `usable == true`
//     record stays valid (durable success), a stale `usable == false`
//     record reads as `nil` so the schedule gate re-probes rather
//     than trusting a possibly-permanent verdict.

import Foundation
import Testing

@testable import Playhead

@Suite("FoundationModelsUsabilityProbe cache TTL")
struct FoundationModelsUsabilityProbeCacheTests {

    /// Fresh, isolated UserDefaults suite per test so concurrent runs
    /// do not bleed cache state into each other.
    private static func isolatedDefaults() -> (UserDefaults, String) {
        let suiteName = "FoundationModelsUsabilityProbeTests.\(UUID().uuidString)"
        return (UserDefaults(suiteName: suiteName)!, suiteName)
    }

    private static let osBuild = "iOS 26.5 (23F99)"
    private static let bootEpoch = 1_700_000_000

    // MARK: - TTL behavior

    @Test("usable=false within TTL is honored")
    func falseRecordWithinTTLReadsFalse() {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        let written = Date(timeIntervalSince1970: 2_000_000_000)
        FoundationModelsUsabilityProbe.cache(
            usable: false,
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            now: written
        )

        // 14 minutes 59 seconds later — still inside the TTL.
        let now = written.addingTimeInterval(14 * 60 + 59)
        let result = FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            now: now
        )
        #expect(result == false,
                "Within-TTL false cache must stay trusted; got \(String(describing: result))")
    }

    /// Pin the exact boundary semantic. The reader uses
    /// `now - cachedAt > falseCacheTTL ? nil : false`, so at EXACTLY
    /// `falseCacheTTL` of elapsed time the comparison is false and the
    /// cached `false` verdict is still trusted. Flipping the operator
    /// to `>=` would break this assertion — that is the point. R1
    /// audit: the boundary was not previously pinned by any test, so a
    /// future inversion would silently change user-visible behavior.
    @Test("usable=false at exact TTL boundary still reads false")
    func falseRecordAtExactTTLBoundaryReadsFalse() {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        let written = Date(timeIntervalSince1970: 2_000_000_000)
        FoundationModelsUsabilityProbe.cache(
            usable: false,
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            now: written
        )

        let now = written.addingTimeInterval(FoundationModelsUsabilityProbe.falseCacheTTL)
        let result = FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            now: now
        )
        #expect(result == false,
                "At exactly falseCacheTTL elapsed, false cache must still be trusted; got \(String(describing: result))")
    }

    @Test("usable=false past TTL returns nil so probe re-fires")
    func falseRecordPastTTLReadsNil() {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        let written = Date(timeIntervalSince1970: 2_000_000_000)
        FoundationModelsUsabilityProbe.cache(
            usable: false,
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            now: written
        )

        // 15 minutes + 1 second later — just past the TTL.
        let now = written.addingTimeInterval(15 * 60 + 1)
        let result = FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            now: now
        )
        #expect(result == nil,
                "Past-TTL false cache must read as nil so the schedule gate re-probes; got \(String(describing: result))")
    }

    @Test("usable=true stays valid indefinitely")
    func trueRecordStaysValidIndefinitely() {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        let written = Date(timeIntervalSince1970: 2_000_000_000)
        FoundationModelsUsabilityProbe.cache(
            usable: true,
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            now: written
        )

        // Far past any reasonable TTL — three weeks later.
        let now = written.addingTimeInterval(21 * 24 * 60 * 60)
        let result = FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            now: now
        )
        #expect(result == true,
                "True cache must stay valid until osBuild/boot changes; got \(String(describing: result))")
    }

    /// The schedule gate in `CapabilitiesService` only fires a probe
    /// when `cachedUsability() == nil`. Verify that an OS/boot mismatch
    /// continues to read as nil (i.e. boot epoch invalidation still
    /// works alongside the new TTL behavior).
    @Test("osBuild or boot mismatch still invalidates cache")
    func osOrBootMismatchReadsNil() {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        FoundationModelsUsabilityProbe.cache(
            usable: true,
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch
        )

        #expect(
            FoundationModelsUsabilityProbe.cachedUsability(
                userDefaults: defaults,
                osBuild: "iOS 26.6 (23G01)",
                bootEpochSeconds: Self.bootEpoch
            ) == nil
        )
        #expect(
            FoundationModelsUsabilityProbe.cachedUsability(
                userDefaults: defaults,
                osBuild: Self.osBuild,
                bootEpochSeconds: Self.bootEpoch + 1
            ) == nil
        )
    }

    // MARK: - Codable round-trip

    @Test("Cache record round-trips with the new cachedAt field")
    func recordRoundTripsWithCachedAt() throws {
        let written = Date(timeIntervalSince1970: 2_000_000_000)
        let record = FoundationModelsUsabilityProbeCache(
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch,
            usable: false,
            cachedAt: written
        )

        let encoded = try JSONEncoder().encode(record)
        let decoded = try JSONDecoder().decode(
            FoundationModelsUsabilityProbeCache.self,
            from: encoded
        )

        #expect(decoded == record, "Record must round-trip equal")
        #expect(decoded.cachedAt == written, "cachedAt must survive round-trip")
        #expect(decoded.usable == false)
        #expect(decoded.osBuild == Self.osBuild)
        #expect(decoded.bootEpochSeconds == Self.bootEpoch)
    }

    // MARK: - Old-schema compatibility

    /// A record persisted by the pre-TTL release (no `cachedAt` field
    /// in the JSON). The decoder must accept it cleanly because the
    /// field is optional; the reader must then treat the missing
    /// `cachedAt` as "expired" for `usable == false` records so the
    /// schedule gate fires a fresh probe rather than trusting a
    /// possibly-permanent verdict.
    @Test("Old-schema false record without cachedAt reads as nil")
    func oldSchemaFalseRecordReadsNil() throws {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        // Emit the legacy JSON shape — three fields, no `cachedAt`.
        let legacyJSON = """
        {
          "osBuild": "\(Self.osBuild)",
          "bootEpochSeconds": \(Self.bootEpoch),
          "usable": false
        }
        """.data(using: .utf8)!
        defaults.set(legacyJSON, forKey: "foundationModels.usabilityProbe")

        let result = FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch
        )
        #expect(result == nil,
                "Old-schema false record must read as nil so we re-probe; got \(String(describing: result))")
    }

    /// Old-schema `usable == true` records should remain valid (the
    /// success verdict has always been treated as durable for the
    /// OS+boot pair, so dropping it on every install would force an
    /// unnecessary FM round-trip for healthy devices).
    @Test("Old-schema true record without cachedAt stays valid")
    func oldSchemaTrueRecordStaysValid() {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        let legacyJSON = """
        {
          "osBuild": "\(Self.osBuild)",
          "bootEpochSeconds": \(Self.bootEpoch),
          "usable": true
        }
        """.data(using: .utf8)!
        defaults.set(legacyJSON, forKey: "foundationModels.usabilityProbe")

        let result = FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch
        )
        #expect(result == true,
                "Old-schema true record must stay valid until osBuild/boot changes; got \(String(describing: result))")
    }

    /// Garbage JSON in the slot must read as nil rather than crash —
    /// the same path is exercised by an old-schema record we never
    /// wrote ourselves (e.g. an unrelated UserDefaults key collision).
    @Test("Malformed cache data reads as nil")
    func malformedDataReadsNil() {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        defaults.set(Data("not json".utf8), forKey: "foundationModels.usabilityProbe")

        let result = FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch
        )
        #expect(result == nil)
    }

    // MARK: - clearCache contract

    /// The Recheck button calls `clearCache()`; verify that path
    /// drops the entire record so the next reader returns nil.
    @Test("clearCache removes the record")
    func clearCacheDropsRecord() {
        let (defaults, suite) = Self.isolatedDefaults()
        defer { defaults.removePersistentDomain(forName: suite) }

        FoundationModelsUsabilityProbe.cache(
            usable: false,
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch
        )

        // Pre-condition: the record is honored within the TTL.
        #expect(FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch
        ) == false)

        FoundationModelsUsabilityProbe.clearCache(userDefaults: defaults)

        #expect(FoundationModelsUsabilityProbe.cachedUsability(
            userDefaults: defaults,
            osBuild: Self.osBuild,
            bootEpochSeconds: Self.bootEpoch
        ) == nil)
    }
}
