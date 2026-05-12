// RandomNegativeAuditSampler.swift
// Deterministic Phase 11 negative-audit sampling.

import CryptoKit
import Foundation

enum RandomNegativeAuditSampler {
    static let minimumSampleRate = 0.10
    static let maximumSampleRate = 0.15
    static let defaultSampleRate = 0.12
    private static let eligibleNoAdsDisposition = "noAds"

    struct Candidate: Sendable, Codable, Equatable {
        let stableId: String
        let firstAtomOrdinal: Int
        let lastAtomOrdinal: Int
        let fmDisposition: String?
        let wasFlagged: Bool

        init(
            stableId: String,
            firstAtomOrdinal: Int,
            lastAtomOrdinal: Int,
            fmDisposition: String?,
            wasFlagged: Bool
        ) {
            self.stableId = stableId
            self.firstAtomOrdinal = min(firstAtomOrdinal, lastAtomOrdinal)
            self.lastAtomOrdinal = max(firstAtomOrdinal, lastAtomOrdinal)
            self.fmDisposition = fmDisposition
            self.wasFlagged = wasFlagged
        }
    }

    struct Selection: Sendable, Codable, Equatable {
        let eligibleCount: Int
        let selected: [Candidate]
        let sampleRate: Double
    }

    struct EvidencePayload: Sendable, Codable, Equatable {
        let schemaVersion: Int
        let jobId: String
        let jobPhase: String
        let atomRange: AtomRange
        let fmDisposition: String?
        let manualReviewFoundAd: Bool?

        init(
            jobId: String,
            jobPhase: String,
            atomRange: AtomRange,
            fmDisposition: String?,
            manualReviewFoundAd: Bool? = nil,
            schemaVersion: Int = 1
        ) {
            self.schemaVersion = schemaVersion
            self.jobId = jobId
            self.jobPhase = jobPhase
            self.atomRange = atomRange
            self.fmDisposition = fmDisposition
            self.manualReviewFoundAd = manualReviewFoundAd
        }

        private enum CodingKeys: String, CodingKey {
            case schemaVersion
            case jobId
            case jobPhase
            case atomRange
            case fmDisposition
            case manualReviewFoundAd
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(schemaVersion, forKey: .schemaVersion)
            try container.encode(jobId, forKey: .jobId)
            try container.encode(jobPhase, forKey: .jobPhase)
            try container.encode(atomRange, forKey: .atomRange)
            if let fmDisposition {
                try container.encode(fmDisposition, forKey: .fmDisposition)
            } else {
                try container.encodeNil(forKey: .fmDisposition)
            }
            if let manualReviewFoundAd {
                try container.encode(manualReviewFoundAd, forKey: .manualReviewFoundAd)
            } else {
                try container.encodeNil(forKey: .manualReviewFoundAd)
            }
        }
    }

    struct AtomRange: Sendable, Codable, Equatable {
        let firstAtomOrdinal: Int
        let lastAtomOrdinal: Int

        init(firstAtomOrdinal: Int, lastAtomOrdinal: Int) {
            self.firstAtomOrdinal = min(firstAtomOrdinal, lastAtomOrdinal)
            self.lastAtomOrdinal = max(firstAtomOrdinal, lastAtomOrdinal)
        }
    }

    static func select(
        candidates: [Candidate],
        sampleRate: Double = defaultSampleRate,
        seedMaterial: String
    ) -> Selection {
        let eligible = candidates
            .filter { isEligibleForNegativeAudit($0) }
            .sorted { lhs, rhs in
                if lhs.stableId == rhs.stableId {
                    return lhs.firstAtomOrdinal < rhs.firstAtomOrdinal
                }
                return lhs.stableId < rhs.stableId
            }
        guard !eligible.isEmpty else {
            return Selection(
                eligibleCount: 0,
                selected: [],
                sampleRate: clampedSampleRate(sampleRate)
            )
        }

        let rate = clampedSampleRate(sampleRate)
        let targetCount = sampleCount(eligibleCount: eligible.count, sampleRate: rate)
        let selected = eligible
            .sorted { lhs, rhs in
                let lhsKey = stableRank(seedMaterial: seedMaterial, candidate: lhs)
                let rhsKey = stableRank(seedMaterial: seedMaterial, candidate: rhs)
                if lhsKey == rhsKey {
                    return lhs.stableId < rhs.stableId
                }
                return lhsKey < rhsKey
            }
            .prefix(targetCount)
            .sorted { lhs, rhs in
                if lhs.firstAtomOrdinal == rhs.firstAtomOrdinal {
                    return lhs.lastAtomOrdinal < rhs.lastAtomOrdinal
                }
                return lhs.firstAtomOrdinal < rhs.firstAtomOrdinal
            }

        return Selection(
            eligibleCount: eligible.count,
            selected: Array(selected),
            sampleRate: rate
        )
    }

    static func payload(
        for candidate: Candidate,
        jobId: String,
        jobPhase: String
    ) -> EvidencePayload {
        EvidencePayload(
            jobId: jobId,
            jobPhase: jobPhase,
            atomRange: AtomRange(
                firstAtomOrdinal: candidate.firstAtomOrdinal,
                lastAtomOrdinal: candidate.lastAtomOrdinal
            ),
            fmDisposition: candidate.fmDisposition
        )
    }

    static func missRate(auditHits: Int, totalAudits: Int) -> Double {
        let denominator = max(0, totalAudits)
        guard denominator > 0 else { return 0 }
        return Double(max(0, auditHits)) / Double(denominator)
    }

    static func isEligibleForNegativeAudit(_ candidate: Candidate) -> Bool {
        guard !candidate.wasFlagged else { return false }
        guard let disposition = candidate.fmDisposition else { return true }
        return disposition == eligibleNoAdsDisposition
    }

    private static func sampleCount(eligibleCount: Int, sampleRate: Double) -> Int {
        let requested = Int((Double(eligibleCount) * sampleRate).rounded())
        let minimum = max(1, Int(ceil(Double(eligibleCount) * minimumSampleRate)))
        let maximum = max(minimum, Int(floor(Double(eligibleCount) * maximumSampleRate)))
        return min(eligibleCount, max(minimum, min(maximum, requested)))
    }

    private static func clampedSampleRate(_ sampleRate: Double) -> Double {
        guard sampleRate.isFinite else { return defaultSampleRate }
        return min(max(sampleRate, minimumSampleRate), maximumSampleRate)
    }

    private static func stableRank(seedMaterial: String, candidate: Candidate) -> UInt64 {
        let material = [
            seedMaterial,
            candidate.stableId,
            "\(candidate.firstAtomOrdinal)",
            "\(candidate.lastAtomOrdinal)",
            candidate.fmDisposition ?? "null",
        ].joined(separator: "|")
        let digest = SHA256.hash(data: Data(material.utf8))
        return digest.prefix(8).reduce(into: UInt64(0)) { partial, byte in
            partial = (partial << 8) | UInt64(byte)
        }
    }
}
