// SemanticScanResult.swift
// Persistence-facing FM scan result and evidence-event models.

import Foundation

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
        transcriptVersion: String,
        decisionCohortJSON: String? = nil
    ) -> Bool {
        // Decision cohort changes only affect downstream decisioning; the FM
        // scan remains reusable as long as the scan cohort and transcript match.
        self.scanCohortJSON == scanCohortJSON &&
        self.transcriptVersion == transcriptVersion
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
