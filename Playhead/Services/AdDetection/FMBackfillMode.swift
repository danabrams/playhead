// FMBackfillMode.swift
// Controls how Foundation Model evidence participates in backfill and, once
// Phase 6 lands fully, how it flows into the decision ledger.

import Foundation

/// Controls whether the Foundation Model classifier runs during backfill and
/// how its evidence is intended to participate in the Phase 6 decision ledger.
///
/// - `off`: FM is skipped entirely. Backfill is the legacy lexical path.
/// - `shadow`: FM runs and persists telemetry, but fusion treats FM evidence
///   as absent for decision purposes.
/// - `rescoreOnly`: FM can contribute positive evidence to ledger entries for
///   existing candidates only.
/// - `proposalOnly`: FM can propose new regions, but does not rescore existing
///   candidates without quorum.
/// - `full`: FM can both rescore existing candidates and propose new regions.
enum FMBackfillMode: String, Codable, Sendable, CaseIterable, Equatable {
    case off
    case shadow
    case rescoreOnly
    case proposalOnly
    case full

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let rawValue = try container.decode(String.self)

        switch rawValue {
        case Self.off.rawValue:
            self = .off
        case Self.shadow.rawValue:
            self = .shadow
        case Self.rescoreOnly.rawValue:
            self = .rescoreOnly
        case Self.proposalOnly.rawValue:
            self = .proposalOnly
        case Self.full.rawValue:
            self = .full
        case "disabled":
            self = .off
        case "enabled":
            // The legacy `.enabled` mode still degraded to shadow behavior
            // before Phase 6 fusion existed, so preserve that observable contract.
            self = .shadow
        default:
            throw DecodingError.dataCorruptedError(
                in: container,
                debugDescription: "Unknown FMBackfillMode raw value: \(rawValue)"
            )
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    var runsFoundationModels: Bool {
        self != .off
    }

    var contributesToExistingCandidateLedger: Bool {
        switch self {
        case .off, .shadow, .proposalOnly:
            false
        case .rescoreOnly, .full:
            true
        }
    }

    var canProposeNewRegions: Bool {
        switch self {
        case .off, .shadow, .rescoreOnly:
            false
        case .proposalOnly, .full:
            true
        }
    }
}
