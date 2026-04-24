// DecodedSpan.swift
// Phase 5 (playhead-4my.5.2): A contiguous ad span produced by MinimalContiguousSpanDecoder.
//
// Design:
//   • Stable `id` is SHA256 prefix of "\(assetId):\(firstAtomOrdinal):\(lastAtomOrdinal)".
//   • Carries anchorProvenance all the way to the overlay UI for tap-to-explain.
//   • Persisted in `decoded_spans` SQLite table (new table, additive-only migration).

import CryptoKit
import Foundation

// MARK: - DecodedSpan

struct DecodedSpan: Sendable, Equatable, Identifiable {
    /// SHA256 prefix of "\(assetId):\(firstAtomOrdinal):\(lastAtomOrdinal)".
    /// Stable across re-runs — same inputs, same id.
    let id: String
    let assetId: String
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let startTime: Double
    let endTime: Double
    /// All anchor sources that contributed to any atom in this span.
    /// Serialized to JSON for persistence and restored on fetch.
    let anchorProvenance: [AnchorRef]

    var duration: Double { endTime - startTime }

    /// Compute the stable id from its components.
    static func makeId(assetId: String, firstAtomOrdinal: Int, lastAtomOrdinal: Int) -> String {
        let input = "\(assetId):\(firstAtomOrdinal):\(lastAtomOrdinal)"
        let hash = SHA256.hash(data: Data(input.utf8))
        return hash.prefix(16).map { String(format: "%02x", $0) }.joined()
    }
}

// MARK: - DecoderConstants

/// Universal duration caps for MinimalContiguousSpanDecoder.
/// Merge and snap radii now live in MinimalContiguousSpanDecoder.Configuration.
enum DecoderConstants {
    /// Minimum span duration (seconds). Spans below this are dropped.
    static let minDurationSeconds: Double = 5
    /// Maximum span duration (seconds). Spans above this are recursively split.
    static let maxDurationSeconds: Double = 180
}

// MARK: - AnchorRef Codable helpers

/// Used for JSON encoding/decoding of anchorProvenance in the `decoded_spans` table.
extension AnchorRef: Codable {
    private enum CodingKeys: String, CodingKey {
        case type, regionId, consensusStrength, entry, breakStrength, correctionId, reportedTime, score
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(String.self, forKey: .type)
        switch type {
        case "fmConsensus":
            let regionId = try container.decode(String.self, forKey: .regionId)
            let strength = try container.decode(Double.self, forKey: .consensusStrength)
            self = .fmConsensus(regionId: regionId, consensusStrength: strength)
        case "evidenceCatalog":
            let entry = try container.decode(EvidenceEntry.self, forKey: .entry)
            self = .evidenceCatalog(entry: entry)
        case "fmAcousticCorroborated":
            let regionId = try container.decode(String.self, forKey: .regionId)
            let strength = try container.decode(Double.self, forKey: .breakStrength)
            self = .fmAcousticCorroborated(regionId: regionId, breakStrength: strength)
        case "userCorrection":
            let correctionId = try container.decode(String.self, forKey: .correctionId)
            let reportedTime = try container.decode(Double.self, forKey: .reportedTime)
            self = .userCorrection(correctionId: correctionId, reportedTime: reportedTime)
        case "classifierSeed":
            let regionId = try container.decode(String.self, forKey: .regionId)
            let score = try container.decode(Double.self, forKey: .score)
            self = .classifierSeed(regionId: regionId, score: score)
        default:
            throw DecodingError.dataCorruptedError(forKey: .type, in: container, debugDescription: "Unknown AnchorRef type: \(type)")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .fmConsensus(let regionId, let strength):
            try container.encode("fmConsensus", forKey: .type)
            try container.encode(regionId, forKey: .regionId)
            try container.encode(strength, forKey: .consensusStrength)
        case .evidenceCatalog(let entry):
            try container.encode("evidenceCatalog", forKey: .type)
            try container.encode(entry, forKey: .entry)
        case .fmAcousticCorroborated(let regionId, let strength):
            try container.encode("fmAcousticCorroborated", forKey: .type)
            try container.encode(regionId, forKey: .regionId)
            try container.encode(strength, forKey: .breakStrength)
        case .userCorrection(let correctionId, let reportedTime):
            try container.encode("userCorrection", forKey: .type)
            try container.encode(correctionId, forKey: .correctionId)
            try container.encode(reportedTime, forKey: .reportedTime)
        case .classifierSeed(let regionId, let score):
            try container.encode("classifierSeed", forKey: .type)
            try container.encode(regionId, forKey: .regionId)
            try container.encode(score, forKey: .score)
        }
    }
}

extension EvidenceEntry: Codable {
    private enum CodingKeys: String, CodingKey {
        case evidenceRef, category, matchedText, normalizedText, atomOrdinal, count, firstTime, lastTime, startTime, endTime
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let count = try c.decodeIfPresent(Int.self, forKey: .count) ?? 1
        let firstTime = try c.decodeIfPresent(Double.self, forKey: .firstTime)
            ?? c.decode(Double.self, forKey: .startTime)
        let lastTime = try c.decodeIfPresent(Double.self, forKey: .lastTime)
            ?? c.decode(Double.self, forKey: .endTime)
        let startTime = try c.decodeIfPresent(Double.self, forKey: .startTime) ?? firstTime
        let endTime = try c.decodeIfPresent(Double.self, forKey: .endTime) ?? lastTime
        self.init(
            evidenceRef: try c.decode(Int.self, forKey: .evidenceRef),
            category: try c.decode(EvidenceCategory.self, forKey: .category),
            matchedText: try c.decode(String.self, forKey: .matchedText),
            normalizedText: try c.decode(String.self, forKey: .normalizedText),
            atomOrdinal: try c.decode(Int.self, forKey: .atomOrdinal),
            startTime: startTime,
            endTime: endTime,
            count: count,
            firstTime: firstTime,
            lastTime: lastTime
        )
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(evidenceRef, forKey: .evidenceRef)
        try c.encode(category, forKey: .category)
        try c.encode(matchedText, forKey: .matchedText)
        try c.encode(normalizedText, forKey: .normalizedText)
        try c.encode(atomOrdinal, forKey: .atomOrdinal)
        try c.encode(count, forKey: .count)
        try c.encode(firstTime, forKey: .firstTime)
        try c.encode(lastTime, forKey: .lastTime)
        // Emit legacy startTime/endTime so older builds can still decode persisted spans.
        try c.encode(startTime, forKey: .startTime)
        try c.encode(endTime, forKey: .endTime)
    }
}
