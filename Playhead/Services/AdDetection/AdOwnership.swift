// AdOwnership.swift
// Phase 6 (playhead-4my.6.2): Ownership classification for SkipPolicyMatrix.
// v1: all spans default to .unknown; Phase 8 (SponsorKnowledgeStore) populates non-unknown values.

import Foundation

enum AdOwnership: String, Sendable, Codable, Hashable, CaseIterable {
    case thirdParty // external advertiser
    case show       // show-produced
    case network    // network-produced
    case unknown

    // TODO(playhead-4my.8): FoundationModelClassifier.Ownership has a .guest case
    // (for guest-mention/endorsement) that has no counterpart here. When Phase 8
    // (SponsorKnowledgeStore) maps FM Ownership → AdOwnership, .guest will need
    // an explicit mapping decision (likely .detectOnly or a new case).
}
