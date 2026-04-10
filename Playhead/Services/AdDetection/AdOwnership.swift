// AdOwnership.swift
// Phase 6 (playhead-4my.6.2): Ownership classification for SkipPolicyMatrix.
// v1: all spans default to .unknown; Phase 8 (SponsorKnowledgeStore) populates non-unknown values.

import Foundation

enum AdOwnership: String, Sendable, Codable, Hashable, CaseIterable {
    case thirdParty // external advertiser
    case show       // show-produced
    case network    // network-produced
    case unknown
}
