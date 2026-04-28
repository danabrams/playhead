// TrainingExample.swift
// playhead-4my.10.1: Materialized training example snapshotted from the
// evidence + decision + correction ledger after each backfill. Survives
// future cohort prunes (the ledger tables get cohort-scoped GC; this one
// does not), so the labels remain useful even after we change the prompt
// or fusion configuration.
//
// Buckets:
//   - .positive       — high-confidence ad, user did not revert
//   - .negative       — correctly rejected non-ad (editorial/guest/organic)
//   - .uncertain      — unresolved or low-quality region
//   - .disagreement   — most valuable: lexical-vs-FM, model-vs-user,
//                       or FM-positive-but-user-reverted
//
// Schema is self-identifying: the encoder writes `schemaVersion` and emits
// JSON null for absent optional fields (decodeIfPresent + unconditional
// `encode(_:forKey:)`) so downstream tooling can rely on stable keys.

import Foundation

/// Bucket discriminator for `TrainingExample`. Strings (not ints) so that
/// the persisted SQLite column and the JSON snapshot read the same way and
/// future buckets can be added without renumbering.
enum TrainingExampleBucket: String, Codable, Sendable, Hashable, CaseIterable {
    case positive
    case negative
    case uncertain
    case disagreement
}

/// One materialized training row. Mirrors the bead-spec field list 1:1.
/// Field-naming tracks the source ledger names (`scanCohortJSON`,
/// `decisionCohortJSON`, `transcriptQuality`) so cohorting queries can be
/// written without translation tables.
struct TrainingExample: Sendable, Equatable, Codable {

    /// Schema version of the on-disk JSON envelope. Bump this when the
    /// shape changes incompatibly. `decodeIfPresent` lets older snapshots
    /// continue to decode without it (they default to v1) so we never
    /// have to run a destructive backfill.
    static let schemaVersion: Int = 1

    let id: String
    let analysisAssetId: String
    let startAtomOrdinal: Int
    let endAtomOrdinal: Int
    let transcriptVersion: String
    let startTime: Double
    let endTime: Double
    /// Stable hash over the redacted text payload. Always present; useful
    /// for dedupe against the durable corpus regardless of whether the
    /// raw snapshot was retained.
    let textSnapshotHash: String
    /// Optional verbatim snapshot. May be `nil` when storage policy elects
    /// to retain only the hash (e.g. PII redaction failed, or rebroadcast
    /// rights restrict full retention). Encodes as JSON `null` when absent.
    let textSnapshot: String?
    let bucket: TrainingExampleBucket
    let commercialIntent: String
    let ownership: String
    let evidenceSources: [String]
    let fmCertainty: Double
    let classifierConfidence: Double
    /// Recorded user action ("skipped" / "reverted" / "vetoed" / nil).
    let userAction: String?
    /// Eligibility gate string at decision time (matches
    /// `SkipEligibilityGate.rawValue`). Nil when no decision exists for
    /// the region (uncertain bucket).
    let eligibilityGate: String?
    let scanCohortJSON: String
    let decisionCohortJSON: String
    let transcriptQuality: String
    let createdAt: Double

    init(
        id: String,
        analysisAssetId: String,
        startAtomOrdinal: Int,
        endAtomOrdinal: Int,
        transcriptVersion: String,
        startTime: Double,
        endTime: Double,
        textSnapshotHash: String,
        textSnapshot: String?,
        bucket: TrainingExampleBucket,
        commercialIntent: String,
        ownership: String,
        evidenceSources: [String],
        fmCertainty: Double,
        classifierConfidence: Double,
        userAction: String?,
        eligibilityGate: String?,
        scanCohortJSON: String,
        decisionCohortJSON: String,
        transcriptQuality: String,
        createdAt: Double
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.startAtomOrdinal = startAtomOrdinal
        self.endAtomOrdinal = endAtomOrdinal
        self.transcriptVersion = transcriptVersion
        self.startTime = startTime
        self.endTime = endTime
        self.textSnapshotHash = textSnapshotHash
        self.textSnapshot = textSnapshot
        self.bucket = bucket
        self.commercialIntent = commercialIntent
        self.ownership = ownership
        self.evidenceSources = evidenceSources
        self.fmCertainty = fmCertainty
        self.classifierConfidence = classifierConfidence
        self.userAction = userAction
        self.eligibilityGate = eligibilityGate
        self.scanCohortJSON = scanCohortJSON
        self.decisionCohortJSON = decisionCohortJSON
        self.transcriptQuality = transcriptQuality
        self.createdAt = createdAt
    }

    // MARK: - Codable

    private enum CodingKeys: String, CodingKey {
        case schemaVersion
        case id
        case analysisAssetId
        case startAtomOrdinal
        case endAtomOrdinal
        case transcriptVersion
        case startTime
        case endTime
        case textSnapshotHash
        case textSnapshot
        case bucket
        case commercialIntent
        case ownership
        case evidenceSources
        case fmCertainty
        case classifierConfidence
        case userAction
        case eligibilityGate
        case scanCohortJSON
        case decisionCohortJSON
        case transcriptQuality
        case createdAt
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        // schemaVersion is informational; we accept any value (forward
        // compatibility) and fall back to the current version when absent.
        _ = try c.decodeIfPresent(Int.self, forKey: .schemaVersion)
            ?? Self.schemaVersion
        self.id = try c.decode(String.self, forKey: .id)
        self.analysisAssetId = try c.decode(String.self, forKey: .analysisAssetId)
        self.startAtomOrdinal = try c.decode(Int.self, forKey: .startAtomOrdinal)
        self.endAtomOrdinal = try c.decode(Int.self, forKey: .endAtomOrdinal)
        self.transcriptVersion = try c.decode(String.self, forKey: .transcriptVersion)
        self.startTime = try c.decode(Double.self, forKey: .startTime)
        self.endTime = try c.decode(Double.self, forKey: .endTime)
        self.textSnapshotHash = try c.decode(String.self, forKey: .textSnapshotHash)
        self.textSnapshot = try c.decodeIfPresent(String.self, forKey: .textSnapshot)
        self.bucket = try c.decode(TrainingExampleBucket.self, forKey: .bucket)
        self.commercialIntent = try c.decode(String.self, forKey: .commercialIntent)
        self.ownership = try c.decode(String.self, forKey: .ownership)
        self.evidenceSources = try c.decode([String].self, forKey: .evidenceSources)
        self.fmCertainty = try c.decode(Double.self, forKey: .fmCertainty)
        self.classifierConfidence = try c.decode(Double.self, forKey: .classifierConfidence)
        self.userAction = try c.decodeIfPresent(String.self, forKey: .userAction)
        self.eligibilityGate = try c.decodeIfPresent(String.self, forKey: .eligibilityGate)
        self.scanCohortJSON = try c.decode(String.self, forKey: .scanCohortJSON)
        self.decisionCohortJSON = try c.decode(String.self, forKey: .decisionCohortJSON)
        self.transcriptQuality = try c.decode(String.self, forKey: .transcriptQuality)
        self.createdAt = try c.decode(Double.self, forKey: .createdAt)
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(Self.schemaVersion, forKey: .schemaVersion)
        try c.encode(id, forKey: .id)
        try c.encode(analysisAssetId, forKey: .analysisAssetId)
        try c.encode(startAtomOrdinal, forKey: .startAtomOrdinal)
        try c.encode(endAtomOrdinal, forKey: .endAtomOrdinal)
        try c.encode(transcriptVersion, forKey: .transcriptVersion)
        try c.encode(startTime, forKey: .startTime)
        try c.encode(endTime, forKey: .endTime)
        try c.encode(textSnapshotHash, forKey: .textSnapshotHash)
        // Unconditional encode (NOT encodeIfPresent) so JSON null appears
        // for absent optionals — self-identifying schema convention.
        try c.encode(textSnapshot, forKey: .textSnapshot)
        try c.encode(bucket, forKey: .bucket)
        try c.encode(commercialIntent, forKey: .commercialIntent)
        try c.encode(ownership, forKey: .ownership)
        try c.encode(evidenceSources, forKey: .evidenceSources)
        try c.encode(fmCertainty, forKey: .fmCertainty)
        try c.encode(classifierConfidence, forKey: .classifierConfidence)
        try c.encode(userAction, forKey: .userAction)
        try c.encode(eligibilityGate, forKey: .eligibilityGate)
        try c.encode(scanCohortJSON, forKey: .scanCohortJSON)
        try c.encode(decisionCohortJSON, forKey: .decisionCohortJSON)
        try c.encode(transcriptQuality, forKey: .transcriptQuality)
        try c.encode(createdAt, forKey: .createdAt)
    }
}
