// FoundationModelsUsabilityProbe.swift
// First-call readiness probe for Apple's Foundation Models runtime.
//
// Cache shape
// -----------
// The probe persists a small `FoundationModelsUsabilityProbeCache` record
// in UserDefaults keyed on `(osBuild, bootEpochSeconds, usable, cachedAt)`.
//   * `usable == true` records stay valid indefinitely (until the
//     osBuild/boot pair changes — i.e. an OS update or device reboot).
//   * `usable == false` records expire after `falseCacheTTL` so a
//     transient failure (model warming up, guardrail rejection,
//     transient timeout) does not stick permanently across a single
//     boot session.
//
// Old-schema compatibility
// ------------------------
// The `cachedAt` field was added after a release that shipped without
// it. The field is optional in the Codable representation; an old
// record decodes successfully with `cachedAt == nil`. The
// `cachedUsability(...)` reader treats a nil `cachedAt` on a
// `usable == false` record the same way it treats an expired record:
// it returns `nil`, which causes the schedule gate in
// `CapabilitiesService.scheduleFoundationModelsProbeIfNeeded(...)` to
// fire a fresh probe rather than trusting a stale "unusable" verdict.
// Old `usable == true` records (which also lack `cachedAt`) remain
// valid — successful readiness has always been treated as durable for
// the OS+boot pair.

import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels

@available(iOS 26.0, *)
@Generable
private struct FoundationModelsReadinessProbe: Sendable {
    @Guide(description: "Return true when the model can successfully answer this readiness probe.")
    var ready: Bool
}
#endif

struct FoundationModelsUsabilityProbeCache: Codable, Sendable, Equatable {
    let osBuild: String
    let bootEpochSeconds: Int
    let usable: Bool
    /// Wall-clock time the record was written. Optional so records
    /// persisted by the pre-TTL release still decode cleanly — see the
    /// "Old-schema compatibility" note at the top of the file.
    let cachedAt: Date?

    init(
        osBuild: String,
        bootEpochSeconds: Int,
        usable: Bool,
        cachedAt: Date? = nil
    ) {
        self.osBuild = osBuild
        self.bootEpochSeconds = bootEpochSeconds
        self.usable = usable
        self.cachedAt = cachedAt
    }

    func matches(osBuild: String, bootEpochSeconds: Int) -> Bool {
        self.osBuild == osBuild && self.bootEpochSeconds == bootEpochSeconds
    }
}

enum FoundationModelsUsabilityProbe {
    private static let cacheKey = "foundationModels.usabilityProbe"

    /// How long a `usable == false` cache record is honored before the
    /// reader treats it as expired (returns `nil`, which causes the
    /// schedule gate to fire a fresh probe). 15 minutes is short enough
    /// that a user who hits the Settings screen after a transient
    /// guardrail/timeout failure recovers naturally; long enough that
    /// retrying does not hammer the model when the failure is durable.
    static let falseCacheTTL: TimeInterval = 15 * 60

    static func cachedUsability(
        userDefaults: UserDefaults = .standard,
        osBuild: String = osBuild(),
        bootEpochSeconds: Int = bootEpochSeconds(),
        now: Date = .now
    ) -> Bool? {
        guard let data = userDefaults.data(forKey: cacheKey),
              let record = try? JSONDecoder().decode(FoundationModelsUsabilityProbeCache.self, from: data),
              record.matches(osBuild: osBuild, bootEpochSeconds: bootEpochSeconds) else {
            return nil
        }

        // Successful probes stay valid indefinitely until the OS+boot
        // pair changes. Failed probes expire after `falseCacheTTL` so a
        // transient failure can heal without an OS update or reboot.
        if record.usable {
            return true
        }

        guard let cachedAt = record.cachedAt else {
            // Old-schema record lacking `cachedAt`. We cannot tell how
            // long ago it was written, so treat it as expired and let
            // the schedule gate fire a fresh probe. This is the right
            // call: pretending a possibly-stale "unusable" verdict is
            // still authoritative is exactly the bug this TTL fixes.
            return nil
        }

        // R4 audit: treat a negative elapsed interval (i.e. `cachedAt`
        // is in the FUTURE relative to `now`) as a corruption signal
        // and force a re-probe. This can happen after an NTP correction
        // pulls the clock backward, after a user manually rolls the
        // device clock back, or after a write from a process running
        // with a skewed clock. Honoring a future-dated `false` verdict
        // would otherwise keep the cache "fresh" for an arbitrarily
        // long window (until `now` catches up to `cachedAt`, then the
        // normal 15-minute TTL) — the exact stuck-Unavailable bug this
        // TTL exists to prevent.
        let elapsed = now.timeIntervalSince(cachedAt)
        if elapsed < 0 { return nil }
        return elapsed > falseCacheTTL ? nil : false
    }

    static func cache(
        usable: Bool,
        userDefaults: UserDefaults = .standard,
        osBuild: String = osBuild(),
        bootEpochSeconds: Int = bootEpochSeconds(),
        now: Date = .now
    ) {
        let record = FoundationModelsUsabilityProbeCache(
            osBuild: osBuild,
            bootEpochSeconds: bootEpochSeconds,
            usable: usable,
            cachedAt: now
        )

        guard let data = try? JSONEncoder().encode(record) else { return }
        userDefaults.set(data, forKey: cacheKey)
    }

    static func clearCache(userDefaults: UserDefaults = .standard) {
        userDefaults.removeObject(forKey: cacheKey)
    }

    static func osBuild(processInfo: ProcessInfo = .processInfo) -> String {
        processInfo.operatingSystemVersionString
    }

    static func bootEpochSeconds(
        now: Date = .now,
        processInfo: ProcessInfo = .processInfo
    ) -> Int {
        Int(now.timeIntervalSince1970 - processInfo.systemUptime)
    }

    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    static func probeIfNeeded(logger: Logger) async -> Bool {
        if let cached = cachedUsability() {
            return cached
        }

        let model = SystemLanguageModel.default
        guard case .available = model.availability, model.supportsLocale() else {
            return false
        }

        let session = LanguageModelSession(model: model)
        do {
            _ = try await session.respond(
                to: "Health check. Return ready=true if you can answer this request.",
                generating: FoundationModelsReadinessProbe.self
            )
            cache(usable: true)
            return true
        } catch {
            logger.warning("Foundation Models readiness probe failed: \(error.localizedDescription)")
            cache(usable: false)
            return false
        }
    }
    #endif
}
