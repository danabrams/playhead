// SemanticScanResult.swift
// Persistence-facing FM scan result and evidence-event models.
//
// H16 resolved: `decisionCohortJSON` was removed from BackfillJob, the
// backfill_jobs column, and the reuse contract. Decision-time changes never
// invalidate FM scan output, so there is nothing to key on.

import Foundation
import OSLog

private let semanticScanLogger = Logger(
    subsystem: "com.playhead",
    category: "SemanticScanResult"
)

/// Rev3-M5: discriminator for `semantic_scan_results.phase` and
/// `evidence_events.phase`. Phase 3 shadow rows and Phase 5 targeted rows
/// are otherwise indistinguishable in those tables (only differ by
/// `reuseKeyHash`); the explicit phase tag lets queries filter without
/// reverse-engineering the hash inputs.
enum SemanticScanPhase: String, Sendable, Hashable, CaseIterable {
    case shadow
    case targeted
}

struct SemanticScanResult: Sendable, Equatable {
    let id: String
    let analysisAssetId: String
    let windowFirstAtomOrdinal: Int
    let windowLastAtomOrdinal: Int
    let windowStartTime: Double
    let windowEndTime: Double
    let scanPass: String
    let transcriptQuality: TranscriptQuality
    let disposition: CoarseDisposition
    let spansJSON: String
    let status: SemanticScanStatus
    let attemptCount: Int
    let errorContext: String?
    let inputTokenCount: Int?
    let outputTokenCount: Int?
    let latencyMs: Double?
    let prewarmHit: Bool
    let scanCohortJSON: String
    let transcriptVersion: String
    /// Optional stable scope included in persistence reuse hashing so
    /// logically distinct jobs/phases that share the same window bounds do
    /// not collapse each other. Nil preserves legacy reuse semantics.
    let reuseScope: String?
    /// Rev3-M5 (C4): run-mode discriminator persisted as a real column.
    /// Defaults to `.shadow` so existing call sites stay byte-identical.
    /// Targeted-narrowed rows opt-in by passing `.targeted`. Cycle-8
    /// reconciliation: renamed from `phase` → `runMode` to disambiguate
    /// from B6's `jobPhase` field.
    let runMode: SemanticScanPhase
    /// Cycle 6 B6 Rev3-M6: originating backfill phase (BackfillJobPhase.rawValue)
    /// or the sentinel `"shadow"` for rows persisted by callers that do not yet
    /// attribute phase. Stored in a distinct `jobPhase` column post cycle-8
    /// reconciliation and used by Rev3-M6 tests to verify that harvester
    /// and lexical narrowing phases actually produce strict-subset coverage.
    let jobPhase: String
    /// playhead-36t: model-generated refusal explanation captured from
    /// `LanguageModelSession.GenerationError.Refusal.explanation` when
    /// the FM classifier refuses this window. Nil for successful scans,
    /// for permissive-path scans, and when the async explanation fetch
    /// fails. Diagnostic only — does not affect routing or persistence
    /// schema.
    let refusalExplanation: String?
    /// playhead-eu1: true when the @Generable default path refused this
    /// window and the permissive string path was used as a fallback.
    let usedPermissiveFallback: Bool
    /// Model-generated explanation from `Refusal.explanation` at the time the permissive
    /// fallback was triggered. `nil` if explanation was unavailable or the fallback was not used.
    let permissiveFallbackReason: String?

    init(
        id: String,
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        windowStartTime: Double,
        windowEndTime: Double,
        scanPass: String,
        transcriptQuality: TranscriptQuality,
        disposition: CoarseDisposition,
        spansJSON: String,
        status: SemanticScanStatus,
        attemptCount: Int,
        errorContext: String?,
        inputTokenCount: Int?,
        outputTokenCount: Int?,
        latencyMs: Double?,
        prewarmHit: Bool,
        scanCohortJSON: String,
        transcriptVersion: String,
        reuseScope: String? = nil,
        runMode: SemanticScanPhase = .shadow,
        jobPhase: String = "shadow",
        refusalExplanation: String? = nil,
        usedPermissiveFallback: Bool = false,
        permissiveFallbackReason: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.windowFirstAtomOrdinal = windowFirstAtomOrdinal
        self.windowLastAtomOrdinal = windowLastAtomOrdinal
        self.windowStartTime = windowStartTime
        self.windowEndTime = windowEndTime
        self.scanPass = scanPass
        self.transcriptQuality = transcriptQuality
        self.disposition = disposition
        self.spansJSON = spansJSON
        self.status = status
        self.attemptCount = attemptCount
        self.errorContext = errorContext
        self.inputTokenCount = inputTokenCount
        self.outputTokenCount = outputTokenCount
        self.latencyMs = latencyMs
        self.prewarmHit = prewarmHit
        self.scanCohortJSON = scanCohortJSON
        self.transcriptVersion = transcriptVersion
        self.reuseScope = reuseScope
        self.runMode = runMode
        self.jobPhase = jobPhase
        self.refusalExplanation = refusalExplanation
        self.usedPermissiveFallback = usedPermissiveFallback
        self.permissiveFallbackReason = permissiveFallbackReason
    }

    func isReusable(
        scanCohortJSON: String,
        transcriptVersion: String
    ) -> Bool {
        // Decision cohort changes only affect downstream decisioning; the FM
        // scan remains reusable as long as the scan cohort and transcript match.
        Self.matchesScanCohortJSON(self.scanCohortJSON, scanCohortJSON) &&
        self.transcriptVersion == transcriptVersion
    }

    /// Internal hook so tests can observe decode-failure logging without
    /// scraping OSLog.
    nonisolated(unsafe) static var decodeFailureObserver: (@Sendable (String, String) -> Void)?

    static func matchesScanCohortJSON(_ lhs: String, _ rhs: String) -> Bool {
        if lhs == rhs {
            return true
        }

        let decoder = JSONDecoder()
        guard let lhsData = lhs.data(using: .utf8),
              let rhsData = rhs.data(using: .utf8) else {
            semanticScanLogger.debug("scan cohort comparison failed: invalid UTF-8")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        let lhsCohort: ScanCohort?
        let rhsCohort: ScanCohort?
        do {
            lhsCohort = try decoder.decode(ScanCohort.self, from: lhsData)
        } catch {
            semanticScanLogger.debug("scan cohort decode failed for stored value: \(String(describing: error), privacy: .public)")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        do {
            rhsCohort = try decoder.decode(ScanCohort.self, from: rhsData)
        } catch {
            semanticScanLogger.debug("scan cohort decode failed for query value: \(String(describing: error), privacy: .public)")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        return lhsCohort == rhsCohort
    }
}

enum EvidenceSourceType: String, Codable, Sendable, Hashable, CaseIterable {
    case fm
    case lexical
    case acoustic
    case catalog
    case classifier
}

struct EvidenceEvent: Sendable, Equatable {
    let id: String
    let analysisAssetId: String
    let eventType: String
    let sourceType: EvidenceSourceType
    let atomOrdinals: String
    let evidenceJSON: String
    let scanCohortJSON: String
    let createdAt: Double
    /// Rev3-M5 (C4): run-mode discriminator persisted as a real column.
    /// Defaults to `.shadow` so existing call sites stay byte-identical.
    /// Cycle-8 reconciliation: renamed from `phase` → `runMode`.
    let runMode: SemanticScanPhase
    /// Cycle 6 B6 Rev3-M6: originating backfill phase (BackfillJobPhase.rawValue)
    /// or the sentinel `"shadow"` for legacy rows. Stored in a distinct
    /// `jobPhase` column post cycle-8 reconciliation.
    let jobPhase: String

    init(
        id: String,
        analysisAssetId: String,
        eventType: String,
        sourceType: EvidenceSourceType,
        atomOrdinals: String,
        evidenceJSON: String,
        scanCohortJSON: String,
        createdAt: Double,
        runMode: SemanticScanPhase = .shadow,
        jobPhase: String = "shadow"
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.eventType = eventType
        self.sourceType = sourceType
        self.atomOrdinals = atomOrdinals
        self.evidenceJSON = evidenceJSON
        self.scanCohortJSON = scanCohortJSON
        self.createdAt = createdAt
        self.runMode = runMode
        self.jobPhase = jobPhase
    }
}
