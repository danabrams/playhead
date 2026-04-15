// MetadataReliabilityMatrix.swift
// ef2.2.3: Per-show reliability matrix tracking trust per source field × cue type
// using Beta distributions. Shadow mode only — accumulates trust observations
// without influencing any live ad detection decisions.
//
// Each cell in the matrix is a Beta(α, β) distribution representing the
// probability that a given (sourceField, cueType) combination produces
// correct cues. Observations update the distribution via Bayesian updating.
//
// Key behaviors:
// - Orthogonal update: corroboration only from a different evidence family
// - Recency weighting: observations >90 days old contribute at 0.5× weight
// - Per-show storage: each show has its own reliability matrix

import Foundation

// MARK: - Evidence Family

/// Groups cue types into evidence families for the orthogonal update rule.
/// Corroboration within the same family is self-corroboration and is rejected.
enum EvidenceFamily: String, Sendable, Codable {
    /// URL-based signals: external domains, show-owned domains, network-owned domains.
    case domain
    /// Text-based signals: disclosures, promo codes, sponsor aliases.
    case textual
}

extension MetadataCueType {
    /// The evidence family this cue type belongs to.
    var evidenceFamily: EvidenceFamily {
        switch self {
        case .externalDomain, .showOwnedDomain, .networkOwnedDomain:
            return .domain
        case .disclosure, .promoCode, .sponsorAlias:
            return .textual
        }
    }
}

// MARK: - BetaDistribution

/// A Beta(α, β) distribution representing Bayesian trust in a signal source.
/// Immutable value type — updates return a new instance.
struct BetaDistribution: Sendable, Codable, Equatable {
    /// Pseudo-count of successes (correct observations).
    let alpha: Float
    /// Pseudo-count of failures (incorrect observations).
    let beta: Float

    /// Posterior mean: α / (α + β).
    var mean: Float {
        guard alpha + beta > 0 else { return 0.5 }
        return alpha / (alpha + beta)
    }

    /// Posterior variance: αβ / ((α+β)² (α+β+1)).
    var variance: Float {
        let sum = alpha + beta
        guard sum > 0, sum + 1 > 0 else { return 0 }
        return (alpha * beta) / (sum * sum * (sum + 1))
    }

    /// Update with a weighted observation.
    /// - Parameters:
    ///   - success: Whether the observation was correct.
    ///   - weight: Observation weight (1.0 for recent, 0.5 for >90 days old).
    /// - Returns: Updated distribution.
    func updated(success: Bool, weight: Float = 1.0) -> BetaDistribution {
        if success {
            return BetaDistribution(alpha: alpha + weight, beta: beta)
        } else {
            return BetaDistribution(alpha: alpha, beta: beta + weight)
        }
    }

    init(alpha: Float, beta: Float) {
        // Clamp to non-negative to survive corrupted persistence data
        // without crashing in release builds.
        self.alpha = max(0, alpha)
        self.beta = max(0, beta)
    }
}

// MARK: - ReliabilityCell Key

/// Identifies a single cell in the reliability matrix: (sourceField × cueType).
struct ReliabilityCellKey: Sendable, Codable, Hashable {
    let sourceField: MetadataCueSourceField
    let cueType: MetadataCueType
}

// MARK: - ShowReliabilityMatrix

/// Per-show reliability matrix: Beta(α, β) for each (sourceField × cueType) cell.
struct ShowReliabilityMatrix: Sendable, Codable, Equatable {
    /// The cells of the matrix, keyed by (sourceField, cueType).
    var cells: [ReliabilityCellKey: BetaDistribution]

    /// Initialize with default priors for all cells.
    init() {
        cells = Self.defaultPriors()
    }

    /// Trust (posterior mean) for a specific cell.
    func trust(for cueType: MetadataCueType, sourceField: MetadataCueSourceField) -> Float {
        let key = ReliabilityCellKey(sourceField: sourceField, cueType: cueType)
        return cells[key]?.mean ?? Self.defaultPrior(for: cueType).mean
    }

    /// Default prior for a cue type, per spec.
    static func defaultPrior(for cueType: MetadataCueType) -> BetaDistribution {
        switch cueType {
        case .externalDomain:
            return BetaDistribution(alpha: 2, beta: 8)
        case .promoCode:
            return BetaDistribution(alpha: 2, beta: 8)
        case .disclosure:
            return BetaDistribution(alpha: 1, beta: 9)
        case .sponsorAlias:
            return BetaDistribution(alpha: 1, beta: 9)
        case .showOwnedDomain:
            return BetaDistribution(alpha: 1, beta: 4)
        case .networkOwnedDomain:
            return BetaDistribution(alpha: 1, beta: 4)
        }
    }

    /// Build default priors for all (sourceField × cueType) combinations.
    private static func defaultPriors() -> [ReliabilityCellKey: BetaDistribution] {
        var result: [ReliabilityCellKey: BetaDistribution] = [:]
        for sourceField in MetadataCueSourceField.allCases {
            for cueType in MetadataCueType.allCases {
                let key = ReliabilityCellKey(sourceField: sourceField, cueType: cueType)
                result[key] = defaultPrior(for: cueType)
            }
        }
        return result
    }
}

// MARK: - MetadataReliabilityMatrix

/// Per-show Bayesian reliability matrix tracking trust per (sourceField × cueType).
///
/// Shadow mode: observations accumulate but never influence live scoring.
/// All mutations are isolated via Swift actor for thread safety.
actor MetadataReliabilityMatrix {

    /// Recency threshold: observations older than this get reduced weight.
    static let recencyThresholdDays: Int = 90

    /// Weight applied to observations older than the recency threshold.
    static let decayedWeight: Float = 0.5

    /// Per-show matrices.
    private var matrices: [String: ShowReliabilityMatrix] = [:]

    // MARK: - Init

    init() {}

    /// Initialize with pre-loaded matrices (e.g., from persistence).
    init(matrices: [String: ShowReliabilityMatrix]) {
        self.matrices = matrices
    }

    // MARK: - Observe

    /// Record an observation that a cue was correct or incorrect.
    ///
    /// Enforces the orthogonal update rule: the corroborating evidence must
    /// come from a different evidence family than the cue being observed.
    /// Self-corroboration (same family) is silently rejected.
    ///
    /// - Parameters:
    ///   - showId: The podcast show identifier.
    ///   - cue: The cue being evaluated.
    ///   - wasCorrect: Whether the cue turned out to be correct.
    ///   - date: When the observation was made.
    ///   - corroboratingFamily: The evidence family that corroborated/refuted the cue.
    /// - Returns: `true` if the observation was applied, `false` if rejected
    ///   (same-family corroboration).
    @discardableResult
    func observe(
        showId: String,
        cue: EpisodeMetadataCue,
        wasCorrect: Bool,
        date: Date,
        corroboratingFamily: EvidenceFamily
    ) -> Bool {
        // Orthogonal update rule: reject same-family corroboration.
        guard cue.cueType.evidenceFamily != corroboratingFamily else {
            return false
        }

        let key = ReliabilityCellKey(sourceField: cue.sourceField, cueType: cue.cueType)
        let weight = Self.weight(for: date)

        var matrix = matrices[showId] ?? ShowReliabilityMatrix()
        let current = matrix.cells[key] ?? ShowReliabilityMatrix.defaultPrior(for: cue.cueType)
        matrix.cells[key] = current.updated(success: wasCorrect, weight: weight)
        matrices[showId] = matrix

        return true
    }

    // MARK: - Query

    /// Returns the current posterior mean trust for a (cueType, sourceField) cell.
    func trust(showId: String, for cueType: MetadataCueType, sourceField: MetadataCueSourceField) -> Float {
        let matrix = matrices[showId] ?? ShowReliabilityMatrix()
        return matrix.trust(for: cueType, sourceField: sourceField)
    }

    /// Returns the full matrix for a show, or default priors if none exists.
    func matrix(for showId: String) -> ShowReliabilityMatrix {
        matrices[showId] ?? ShowReliabilityMatrix()
    }

    // MARK: - Reset

    /// Clear a show's reliability matrix, resetting to default priors.
    func reset(showId: String) {
        matrices.removeValue(forKey: showId)
    }

    // MARK: - Persistence

    /// Export all matrices for persistence.
    func exportForPersistence() -> [String: ShowReliabilityMatrix] {
        matrices
    }

    /// Encode all matrices to JSON data.
    func encodeToJSON() throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(matrices)
    }

    /// Decode matrices from JSON data.
    static func decodeFromJSON(_ data: Data) throws -> MetadataReliabilityMatrix {
        let decoder = JSONDecoder()
        let decoded = try decoder.decode([String: ShowReliabilityMatrix].self, from: data)
        return MetadataReliabilityMatrix(matrices: decoded)
    }

    // MARK: - Private

    /// Compute the observation weight based on recency.
    /// Recent observations (≤90 days) get full weight (1.0).
    /// Older observations get decayed weight (0.5).
    /// Uses seconds-based comparison for timezone-agnostic determinism.
    private static let recencyThresholdSeconds: TimeInterval = TimeInterval(recencyThresholdDays) * 86400

    private static func weight(for date: Date, referenceDate: Date = Date()) -> Float {
        let elapsed = referenceDate.timeIntervalSince(date)
        return elapsed > recencyThresholdSeconds ? decayedWeight : 1.0
    }
}
