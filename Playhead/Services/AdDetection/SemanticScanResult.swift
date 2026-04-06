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
}
