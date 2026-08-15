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
//
// AND EVERY DERIVED RATE IN THIS FILE IS `Double?` FOR THAT SAME REASON
// (playhead-vev7). `rate()` used to answer a zero denominator with `return 0`,
// so "nothing was measured" and "measured, and it was zero" came out
// byte-identical — the manufacturing site for the reading kvi1 deleted one
// instance of. Measured on the same 26-event pull: one job had
// `resumeAttemptCount: 0` and therefore `resumeSuccessRate: 0`, which reads as
// "the resume failed" and is indistinguishable from the twenty rows where a
// resume genuinely was attempted and genuinely did fail.
//
// The other three `rate()` denominators read honestly on that pull only by
// ACCIDENT: `operationalCounters` in `BackfillJobRunner` forces
// `episodeCount`, `cohortDriftEvaluationCount` and `admissionDecisionCount` to
// 1 on every event it writes, so their denominators are never zero. Nothing
// declared that invariant and nothing checked it; make any of those three
// conditional and the fabrication spreads with no diff here at all. Hence the
// fix lives in the helper, not in the one field that happened to show it.
//
// `perAudioHour` carried the identical `return 0` and is fixed with it. It is
// not protected even by accident: `BackfillJobRunner` passes
// `audioSegments: []` on the thermal-deferral and no-anchor-sentinel paths, so
// `audioDurationSeconds` is genuinely 0 there and `wallTimePerAudioHour` read
// 0 — "infinitely fast". Those two paths simply produced no events on the
// 2026-08-11 pull, where all 26 rows carry real audio.
//
// nil is "the denominator was zero, so there is no rate". 0.0 is "there were
// observations and none of them fired". A reader that cannot tell those apart
// is the whole defect, so DO NOT `?? 0` any of these — encode the absence.

import Foundation

struct OperationalMetrics: Sendable, Codable, Equatable {
    /// v2 (playhead-kvi1) removed `cacheReuseRate` and its two counters.
    /// v3 (playhead-vev7) made every derived rate `Double?`, so an unmeasured
    /// rate is an ABSENT key rather than `0`.
    ///
    /// Old events survive in `evidence_events` and decode against this type
    /// unchanged: Swift's synthesized `Decodable` ignores keys it does not know
    /// (v1's three cache keys) and uses `decodeIfPresent` for optionals (v3's
    /// absent rates). Both are properties of the compiler rather than of this
    /// code, so both are pinned by tests rather than assumed.
    ///
    /// **Read the version before reading a rate.** A `0` in a v1 or v2 payload
    /// is ambiguous by construction — that shape could not express absence — and
    /// nothing can recover which it was after the fact. A `0` in a v3 payload is
    /// a measurement.
    static let schemaVersion = 3
    static let eventType = "backfillOperationalMetrics"

    let schemaVersion: Int
    let jobId: String
    let analysisAssetId: String
    let jobPhase: String
    let scanCohortIdentity: String
    let scanCohortJSON: String
    let wallTimeSeconds: Double
    let audioDurationSeconds: Double
    /// nil when `audioDurationSeconds == 0` — no audio ⇒ no rate.
    /// (Distinct from `0.0`, which is "there was audio and no wall time".)
    let wallTimePerAudioHour: Double?
    /// nil when `counters.episodeCount == 0`.
    /// (Distinct from `0.0`, which is "episodes ran and none cost anything".)
    let energyPerEpisode: Double?
    /// nil when `counters.resumeAttemptCount == 0` — NOTHING WAS RESUMED.
    /// (Distinct from `0.0`, which is "a resume was attempted and it failed".)
    let resumeSuccessRate: Double?
    /// nil when `counters.cohortDriftEvaluationCount == 0`.
    /// (Distinct from `0.0`, which is "drift was evaluated and none was found".)
    let perCohortDrift: Double?
    /// nil when `counters.admissionDecisionCount == 0`.
    /// (Distinct from `0.0`, which is "admission ran and deferred nothing".)
    let thermalDeferralRate: Double?
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

    /// nil when there is no audio to divide by. It returned 0 until
    /// playhead-vev7, which is "this job scanned an hour of audio in no time at
    /// all" — the flattering direction, and the one a health report is least
    /// likely to question.
    private static func perAudioHour(
        wallTimeSeconds: Double,
        audioDurationSeconds: Double
    ) -> Double? {
        guard audioDurationSeconds > 0 else { return nil }
        return wallTimeSeconds / (audioDurationSeconds / 3_600)
    }

    private static func rate(numerator: Int, denominator: Int) -> Double? {
        rate(numerator: Double(max(0, numerator)), denominator: denominator)
    }

    /// THE MANUFACTURING SITE (playhead-vev7). This answered a zero denominator
    /// with `return 0`, which every derived rate on this payload inherited: a
    /// quantity nobody observed and a quantity observed to be zero were the same
    /// bytes on the wire, and no reader could tell which they held. nil is the
    /// answer to "what is `x / 0`", and `Double?` is what makes the wrong answer
    /// untypeable rather than merely discouraged.
    private static func rate(numerator: Double, denominator: Int) -> Double? {
        let safeDenominator = max(0, denominator)
        guard safeDenominator > 0 else { return nil }
        return finiteNonNegative(numerator) / Double(safeDenominator)
    }

    private static func finiteNonNegative(_ value: Double) -> Double {
        guard value.isFinite else { return 0 }
        return max(0, value)
    }
}
