// FoundationModelsUsabilityProbe.swift
// First-call readiness probe for Apple's Foundation Models runtime.

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

    func matches(osBuild: String, bootEpochSeconds: Int) -> Bool {
        self.osBuild == osBuild && self.bootEpochSeconds == bootEpochSeconds
    }
}

enum FoundationModelsUsabilityProbe {
    private static let cacheKey = "foundationModels.usabilityProbe"

    static func cachedUsability(
        userDefaults: UserDefaults = .standard,
        osBuild: String = osBuild(),
        bootEpochSeconds: Int = bootEpochSeconds()
    ) -> Bool? {
        guard let data = userDefaults.data(forKey: cacheKey),
              let record = try? JSONDecoder().decode(FoundationModelsUsabilityProbeCache.self, from: data),
              record.matches(osBuild: osBuild, bootEpochSeconds: bootEpochSeconds) else {
            return nil
        }

        return record.usable
    }

    static func cache(
        usable: Bool,
        userDefaults: UserDefaults = .standard,
        osBuild: String = osBuild(),
        bootEpochSeconds: Int = bootEpochSeconds()
    ) {
        let record = FoundationModelsUsabilityProbeCache(
            osBuild: osBuild,
            bootEpochSeconds: bootEpochSeconds,
            usable: usable
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
