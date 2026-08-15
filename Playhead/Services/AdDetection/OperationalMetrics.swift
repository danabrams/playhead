// OperationalMetrics.swift
// Stable persistence payload for Phase 11 FM backfill health metrics.
//
// THERE IS NO CACHE MEASUREMENT HERE, AND THERE USED TO BE ONE THAT READ 1.0
// (playhead-kvi1, sibling of playhead-exxc). Schema v1 carried
// `cacheReuseRate`, `counters.cacheLookupCount` and `counters.cacheReuseCount`.
// `recordFMOutput` incremented the denominator unconditionally and the
// numerator under the PASS-level `prewarmHit` flag, which
// `FoundationModelClassifier` sets from a compile-time constant
// (`let prewarmHit = true` on the coarse path; `prewarmHit = true`
// unconditionally after `makePrewarmedSessionBox` on the boundary and
// refinement paths). So for every pass that reached the model the two counters
// were equal and the ratio was 1.0 by construction, and on the arms where the
// flag is `false` the pass had run no FM work at all — a state `fmWindowCount`
// already reports. Measured on the 2026-08-11 virgin-DB overnight pull, all 26
// persisted events: `cacheReuseCount == cacheLookupCount` in every row, giving
// 1.0 on the six rows with FM work and 0.0 — the zero-denominator reading — on
// the other twenty.
//
// It could not have been made real here: `LanguageModelSession.prewarm` is
// fire-and-forget and never awaited, FoundationModels exposes no hit/miss, and
// nothing in this tree times the prewarm, so a prewarm that hit and one that
// missed are indistinguishable to every column this code writes.
//
// The app's real cache measures itself, honestly and elsewhere:
// `RepeatedAdCacheService` counts genuine hits and misses and publishes
// `RepeatedAdCacheHitRateSnapshot.hitRate` as a `Double?`, where nil is "no
// samples" and 0.0 is "samples exist, none hit" — and it is consumed, by the
// auto-disable rung. Do not add a second, fabricated one here: on a device pull
// the two are indistinguishable and the fabricated one reads better.

import Foundation

struct OperationalMetrics: Sendable, Codable, Equatable {
    /// v2 (playhead-kvi1) removed `cacheReuseRate` and its two counters. Old
    /// v1 events survive in `evidence_events`; they decode against this type
    /// unchanged, because Swift's synthesized `Decodable` ignores keys it does
    /// not know. Read the version before comparing payloads across pulls.
    static let schemaVersion = 2
    static let eventType = "backfillOperationalMetrics"

    let schemaVersion: Int
    let jobId: String
    let analysisAssetId: String
    let jobPhase: String
    let scanCohortIdentity: String
    let scanCohortJSON: String
    let wallTimeSeconds: Double
    let audioDurationSeconds: Double
    let wallTimePerAudioHour: Double
    let energyPerEpisode: Double
    let resumeSuccessRate: Double
    let perCohortDrift: Double
    let thermalDeferralRate: Double
    var counters: Counters

    init(
        jobId: String,
        analysisAssetId: String,
        jobPhase: String,
        scanCohortJSON: String,
        wallTimeSeconds: Double,
        audioDurationSeconds: Double,
        counters: Counters,
        schemaVersion: Int = OperationalMetrics.schemaVersion
    ) {
        self.schemaVersion = schemaVersion
        self.jobId = jobId
        self.analysisAssetId = analysisAssetId
        self.jobPhase = jobPhase
        self.scanCohortJSON = scanCohortJSON
        self.scanCohortIdentity = Self.scanCohortIdentity(from: scanCohortJSON)
        self.wallTimeSeconds = Self.finiteNonNegative(wallTimeSeconds)
        self.audioDurationSeconds = Self.finiteNonNegative(audioDurationSeconds)
        self.wallTimePerAudioHour = Self.perAudioHour(
            wallTimeSeconds: self.wallTimeSeconds,
            audioDurationSeconds: self.audioDurationSeconds
        )
        self.energyPerEpisode = Self.rate(
            numerator: counters.estimatedEnergyUnits,
            denominator: counters.episodeCount
        )
        self.resumeSuccessRate = Self.rate(
            numerator: counters.resumeSuccessCount,
            denominator: counters.resumeAttemptCount
        )
        self.perCohortDrift = Self.rate(
            numerator: counters.cohortDriftSignalCount,
            denominator: counters.cohortDriftEvaluationCount
        )
        self.thermalDeferralRate = Self.rate(
            numerator: counters.thermalDeferralCount,
            denominator: counters.admissionDecisionCount
        )
        self.counters = counters
    }

    struct Counters: Sendable, Codable, Equatable {
        var episodeCount: Int
        var fmPassCount: Int
        var fmWindowCount: Int
        var persistedScanResultCount: Int
        var persistedEvidenceEventCount: Int
        var estimatedEnergyUnits: Double
        var resumeAttemptCount: Int
        var resumeSuccessCount: Int
        var cohortDriftEvaluationCount: Int
        var cohortDriftSignalCount: Int
        var admissionDecisionCount: Int
        var thermalDeferralCount: Int
        var randomAuditCandidateCount: Int
        var randomAuditSelectedCount: Int

        init(
            episodeCount: Int = 0,
            fmPassCount: Int = 0,
            fmWindowCount: Int = 0,
            persistedScanResultCount: Int = 0,
            persistedEvidenceEventCount: Int = 0,
            estimatedEnergyUnits: Double = 0,
            resumeAttemptCount: Int = 0,
            resumeSuccessCount: Int = 0,
            cohortDriftEvaluationCount: Int = 0,
            cohortDriftSignalCount: Int = 0,
            admissionDecisionCount: Int = 0,
            thermalDeferralCount: Int = 0,
            randomAuditCandidateCount: Int = 0,
            randomAuditSelectedCount: Int = 0
        ) {
            self.episodeCount = max(0, episodeCount)
            self.fmPassCount = max(0, fmPassCount)
            self.fmWindowCount = max(0, fmWindowCount)
            self.persistedScanResultCount = max(0, persistedScanResultCount)
            self.persistedEvidenceEventCount = max(0, persistedEvidenceEventCount)
            self.estimatedEnergyUnits = OperationalMetrics.finiteNonNegative(estimatedEnergyUnits)
            self.resumeAttemptCount = max(0, resumeAttemptCount)
            self.resumeSuccessCount = max(0, resumeSuccessCount)
            self.cohortDriftEvaluationCount = max(0, cohortDriftEvaluationCount)
            self.cohortDriftSignalCount = max(0, cohortDriftSignalCount)
            self.admissionDecisionCount = max(0, admissionDecisionCount)
            self.thermalDeferralCount = max(0, thermalDeferralCount)
            self.randomAuditCandidateCount = max(0, randomAuditCandidateCount)
            self.randomAuditSelectedCount = max(0, randomAuditSelectedCount)
        }

        /// playhead-kvi1: this used to take `prewarmHit: Bool` and drive the
        /// two cache counters off it. The parameter is gone rather than
        /// ignored — an unused argument at three call sites is an invitation
        /// to wire it back to something.
        mutating func recordFMOutput(
            latencyMillis: Double,
            windowCount: Int
        ) {
            fmPassCount += 1
            fmWindowCount += max(0, windowCount)
            estimatedEnergyUnits += OperationalMetrics.finiteNonNegative(latencyMillis) / 1_000
        }

        mutating func add(_ other: Counters) {
            episodeCount += other.episodeCount
            fmPassCount += other.fmPassCount
            fmWindowCount += other.fmWindowCount
            persistedScanResultCount += other.persistedScanResultCount
            persistedEvidenceEventCount += other.persistedEvidenceEventCount
            estimatedEnergyUnits += other.estimatedEnergyUnits
            resumeAttemptCount += other.resumeAttemptCount
            resumeSuccessCount += other.resumeSuccessCount
            cohortDriftEvaluationCount += other.cohortDriftEvaluationCount
            cohortDriftSignalCount += other.cohortDriftSignalCount
            admissionDecisionCount += other.admissionDecisionCount
            thermalDeferralCount += other.thermalDeferralCount
            randomAuditCandidateCount += other.randomAuditCandidateCount
            randomAuditSelectedCount += other.randomAuditSelectedCount
        }
    }

    private static func scanCohortIdentity(from json: String) -> String {
        guard let data = json.data(using: .utf8),
              let cohort = try? JSONDecoder().decode(ScanCohortIdentity.self, from: data)
        else {
            return "invalid"
        }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let identity = OperationalCohortIdentity(scanCohort: cohort)
        return (try? encoder.encode(identity))
            .flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
    }

    private struct ScanCohortIdentity: Decodable {
        let promptLabel: String
        let promptHash: String
        let schemaHash: String
        let scanPlanHash: String
        let normalizationHash: String
        let locale: String
        let appBuild: String
    }

    private struct OperationalCohortIdentity: Codable {
        let promptLabel: String
        let promptHash: String
        let schemaHash: String
        let scanPlanHash: String
        let normalizationHash: String
        let locale: String
        let appBuild: String

        init(scanCohort: ScanCohortIdentity) {
            promptLabel = scanCohort.promptLabel
            promptHash = scanCohort.promptHash
            schemaHash = scanCohort.schemaHash
            scanPlanHash = scanCohort.scanPlanHash
            normalizationHash = scanCohort.normalizationHash
            locale = scanCohort.locale
            appBuild = scanCohort.appBuild
        }
    }

    private static func perAudioHour(
        wallTimeSeconds: Double,
        audioDurationSeconds: Double
    ) -> Double {
        guard audioDurationSeconds > 0 else { return 0 }
        return wallTimeSeconds / (audioDurationSeconds / 3_600)
    }

    private static func rate(numerator: Int, denominator: Int) -> Double {
        rate(numerator: Double(max(0, numerator)), denominator: denominator)
    }

    private static func rate(numerator: Double, denominator: Int) -> Double {
        let safeNumerator = finiteNonNegative(numerator)
        let safeDenominator = max(0, denominator)
        guard safeDenominator > 0 else { return 0 }
        return safeNumerator / Double(safeDenominator)
    }

    private static func finiteNonNegative(_ value: Double) -> Double {
        guard value.isFinite else { return 0 }
        return max(0, value)
    }
}
