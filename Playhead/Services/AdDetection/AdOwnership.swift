// AdOwnership.swift
// Phase 6 (playhead-4my.6.2): Ownership classification for SkipPolicyMatrix.
// v1: all spans default to .unknown; Phase 8 (SponsorKnowledgeStore) populates non-unknown values.

import Foundation

enum AdOwnership: String, Sendable, Codable, Hashable, CaseIterable {
    case thirdParty // external advertiser
    case show       // show-produced
    case network    // network-produced
    /// Guest mention/endorsement (e.g. host recommends a product in passing).
    /// Phase 8 (SponsorKnowledgeStore) maps FoundationModelClassifier.Ownership.guest here.
    /// Policy: .detectOnly — show a banner but never auto-skip; guest mentions are
    /// ambiguous by nature and require user confirmation before any action.
    case guest
    case unknown
}
