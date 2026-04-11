// SponsorKnowledgeStore.swift
// Phase 8 (playhead-4my.8.1): Structured per-show sponsor memory with
// quarantine lifecycle. Wraps AnalysisStore for SQLite persistence and
// exposes lifecycle promotion/demotion logic.
//
// Design:
//   - KnowledgeState: candidate → quarantined → active → decayed/blocked
//   - SponsorKnowledgeEntry: canonical sponsor entities + aliases, stats
//   - KnowledgeCandidateEvent: append-only provenance log
//   - SponsorKnowledgeStore (actor): lifecycle management + query APIs
//   - Only active entries are surfaced to matcher queries

import Foundation
import OSLog

// MARK: - KnowledgeState

/// Lifecycle state for a sponsor knowledge entry.
enum KnowledgeState: String, Sendable, Codable, CaseIterable {
    /// Initial extraction — not yet confirmed across episodes.
    case candidate
    /// One high-quality confirmation received; under observation.
    case quarantined
    /// Confirmed across multiple episodes; used for matching.
    case active
    /// Previously active but rollback spike or drift regression detected.
    case decayed
    /// Explicitly blocked due to correction conflict or persistent rollback.
    case blocked
}

// MARK: - KnowledgeEntityType

/// The type of entity tracked in sponsor knowledge.
enum KnowledgeEntityType: String, Sendable, Codable, CaseIterable {
    case sponsor
    case cta
    case url
    case disclosure
}

// MARK: - SponsorKnowledgeEntry

/// A canonical sponsor entity with lifecycle state and per-entity stats.
struct SponsorKnowledgeEntry: Sendable, Equatable {
    let id: String
    let podcastId: String
    let entityType: KnowledgeEntityType
    let entityValue: String
    let normalizedValue: String
    let state: KnowledgeState
    let confirmationCount: Int
    let rollbackCount: Int
    let firstSeenAt: Double
    let lastConfirmedAt: Double?
    let lastRollbackAt: Double?
    let decayedAt: Double?
    let blockedAt: Double?
    let aliases: [String]
    let metadata: [String: String]?

    init(
        id: String = UUID().uuidString,
        podcastId: String,
        entityType: KnowledgeEntityType,
        entityValue: String,
        normalizedValue: String? = nil,
        state: KnowledgeState = .candidate,
        confirmationCount: Int = 0,
        rollbackCount: Int = 0,
        firstSeenAt: Double = Date().timeIntervalSince1970,
        lastConfirmedAt: Double? = nil,
        lastRollbackAt: Double? = nil,
        decayedAt: Double? = nil,
        blockedAt: Double? = nil,
        aliases: [String] = [],
        metadata: [String: String]? = nil
    ) {
        self.id = id
        self.podcastId = podcastId
        self.entityType = entityType
        self.entityValue = entityValue
        self.normalizedValue = normalizedValue ?? entityValue.lowercased().trimmingCharacters(in: .whitespaces)
        self.state = state
        self.confirmationCount = confirmationCount
        self.rollbackCount = rollbackCount
        self.firstSeenAt = firstSeenAt
        self.lastConfirmedAt = lastConfirmedAt
        self.lastRollbackAt = lastRollbackAt
        self.decayedAt = decayedAt
        self.blockedAt = blockedAt
        self.aliases = aliases
        self.metadata = metadata
    }

    /// Rollback rate as a fraction of total observations (confirmations + rollbacks).
    var rollbackRate: Double {
        let total = confirmationCount + rollbackCount
        guard total > 0 else { return 0.0 }
        return Double(rollbackCount) / Double(total)
    }
}

// MARK: - KnowledgeCandidateEvent

/// Append-only event tracking candidate writes with provenance.
struct KnowledgeCandidateEvent: Sendable, Equatable {
    let id: String
    let analysisAssetId: String
    let entityType: KnowledgeEntityType
    let entityValue: String
    let sourceAtomOrdinals: [Int]
    let transcriptVersion: String
    let confidence: Double
    let scanCohortJSON: String?
    let createdAt: Double

    init(
        id: String = UUID().uuidString,
        analysisAssetId: String,
        entityType: KnowledgeEntityType,
        entityValue: String,
        sourceAtomOrdinals: [Int],
        transcriptVersion: String,
        confidence: Double,
        scanCohortJSON: String? = nil,
        createdAt: Double = Date().timeIntervalSince1970
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.entityType = entityType
        self.entityValue = entityValue
        self.sourceAtomOrdinals = sourceAtomOrdinals
        self.transcriptVersion = transcriptVersion
        self.confidence = confidence
        self.scanCohortJSON = scanCohortJSON
        self.createdAt = createdAt
    }
}

// MARK: - Promotion Thresholds

/// Constants governing lifecycle transitions.
enum KnowledgePromotionThresholds {
    /// Minimum confirmations to promote from quarantined → active.
    static let minConfirmationsForActive = 2
    /// Maximum rollback rate to allow quarantined → active promotion.
    static let maxRollbackRateForActive = 0.3
    /// Rollback rate that triggers active → decayed demotion.
    static let rollbackSpikeThreshold = 0.5
    /// Minimum confidence for initial candidate extraction.
    static let minCandidateConfidence = 0.5
}

// MARK: - SponsorKnowledgeStore

/// Actor-backed sponsor knowledge store with quarantine lifecycle.
/// Delegates SQLite persistence to AnalysisStore and exposes lifecycle
/// management + query APIs for SponsorKnowledgeMatcher.
actor SponsorKnowledgeStore {

    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "SponsorKnowledgeStore")

    init(store: AnalysisStore) {
        self.store = store
    }

    // MARK: - Write: Record Candidate

    /// Record a candidate entity extraction. Creates a new entry if one
    /// doesn't exist for this (podcastId, entityType, normalizedValue),
    /// or increments the confirmation count on the existing one.
    /// Also appends a KnowledgeCandidateEvent for provenance.
    func recordCandidate(
        podcastId: String,
        entityType: KnowledgeEntityType,
        entityValue: String,
        analysisAssetId: String,
        sourceAtomOrdinals: [Int],
        transcriptVersion: String,
        confidence: Double,
        scanCohortJSON: String? = nil
    ) async throws {
        guard confidence >= KnowledgePromotionThresholds.minCandidateConfidence else {
            logger.debug("recordCandidate: skipping low-confidence entity '\(entityValue)' (conf=\(confidence))")
            return
        }

        // Append the provenance event.
        let event = KnowledgeCandidateEvent(
            analysisAssetId: analysisAssetId,
            entityType: entityType,
            entityValue: entityValue,
            sourceAtomOrdinals: sourceAtomOrdinals,
            transcriptVersion: transcriptVersion,
            confidence: confidence,
            scanCohortJSON: scanCohortJSON
        )
        try await store.appendKnowledgeCandidateEvent(event)

        let normalized = entityValue.lowercased().trimmingCharacters(in: .whitespaces)

        // Try to load existing entry.
        if let existing = try await store.loadKnowledgeEntry(
            podcastId: podcastId,
            entityType: entityType,
            normalizedValue: normalized
        ) {
            // Increment confirmation count and apply promotion rules.
            let newCount = existing.confirmationCount + 1
            let now = Date().timeIntervalSince1970
            let newState = stablePromoteState(
                current: existing.state,
                confirmationCount: newCount,
                rollbackCount: existing.rollbackCount,
                hasActiveCorrection: false  // corrections applied at query time via activeEntriesWithNegativeMemory
            )
            var updated = SponsorKnowledgeEntry(
                id: existing.id,
                podcastId: existing.podcastId,
                entityType: existing.entityType,
                entityValue: existing.entityValue,
                normalizedValue: existing.normalizedValue,
                state: newState,
                confirmationCount: newCount,
                rollbackCount: existing.rollbackCount,
                firstSeenAt: existing.firstSeenAt,
                lastConfirmedAt: now,
                lastRollbackAt: existing.lastRollbackAt,
                decayedAt: newState == .decayed ? now : existing.decayedAt,
                blockedAt: newState == .blocked ? now : existing.blockedAt,
                aliases: existing.aliases,
                metadata: existing.metadata
            )
            // If there's an alias not in the list, add it.
            if entityValue != existing.entityValue && !existing.aliases.contains(entityValue) {
                var newAliases = existing.aliases
                newAliases.append(entityValue)
                updated = SponsorKnowledgeEntry(
                    id: updated.id,
                    podcastId: updated.podcastId,
                    entityType: updated.entityType,
                    entityValue: updated.entityValue,
                    normalizedValue: updated.normalizedValue,
                    state: updated.state,
                    confirmationCount: updated.confirmationCount,
                    rollbackCount: updated.rollbackCount,
                    firstSeenAt: updated.firstSeenAt,
                    lastConfirmedAt: updated.lastConfirmedAt,
                    lastRollbackAt: updated.lastRollbackAt,
                    decayedAt: updated.decayedAt,
                    blockedAt: updated.blockedAt,
                    aliases: newAliases,
                    metadata: updated.metadata
                )
            }
            try await store.upsertKnowledgeEntry(updated)
        } else {
            // Create new candidate entry and apply initial promotion.
            let initialState = stablePromoteState(
                current: .candidate,
                confirmationCount: 1,
                rollbackCount: 0,
                hasActiveCorrection: false
            )
            let now = Date().timeIntervalSince1970
            let entry = SponsorKnowledgeEntry(
                podcastId: podcastId,
                entityType: entityType,
                entityValue: entityValue,
                normalizedValue: normalized,
                state: initialState,
                confirmationCount: 1,
                firstSeenAt: now,
                lastConfirmedAt: now
            )
            try await store.upsertKnowledgeEntry(entry)
        }
    }

    // MARK: - Write: Record Rollback

    /// Record a rollback (user correction or drift regression) against an entity.
    /// Increments rollback count and may demote the entry.
    func recordRollback(
        podcastId: String,
        entityType: KnowledgeEntityType,
        entityValue: String
    ) async throws {
        let normalized = entityValue.lowercased().trimmingCharacters(in: .whitespaces)
        guard let existing = try await store.loadKnowledgeEntry(
            podcastId: podcastId,
            entityType: entityType,
            normalizedValue: normalized
        ) else {
            logger.debug("recordRollback: no entry found for '\(entityValue)' on podcast '\(podcastId)'")
            return
        }

        let now = Date().timeIntervalSince1970
        let newRollbackCount = existing.rollbackCount + 1
        let newState = demoteState(
            current: existing.state,
            confirmationCount: existing.confirmationCount,
            rollbackCount: newRollbackCount
        )

        let updated = SponsorKnowledgeEntry(
            id: existing.id,
            podcastId: existing.podcastId,
            entityType: existing.entityType,
            entityValue: existing.entityValue,
            normalizedValue: existing.normalizedValue,
            state: newState,
            confirmationCount: existing.confirmationCount,
            rollbackCount: newRollbackCount,
            firstSeenAt: existing.firstSeenAt,
            lastConfirmedAt: existing.lastConfirmedAt,
            lastRollbackAt: now,
            decayedAt: newState == .decayed ? now : existing.decayedAt,
            blockedAt: newState == .blocked ? now : existing.blockedAt,
            aliases: existing.aliases,
            metadata: existing.metadata
        )
        try await store.upsertKnowledgeEntry(updated)
    }

    // MARK: - Write: Block Entry (from UserCorrectionStore negative memory)

    /// Explicitly block an entity due to a user correction conflict.
    func blockEntry(
        podcastId: String,
        entityType: KnowledgeEntityType,
        entityValue: String
    ) async throws {
        let normalized = entityValue.lowercased().trimmingCharacters(in: .whitespaces)
        guard let existing = try await store.loadKnowledgeEntry(
            podcastId: podcastId,
            entityType: entityType,
            normalizedValue: normalized
        ) else { return }

        let now = Date().timeIntervalSince1970
        let blocked = SponsorKnowledgeEntry(
            id: existing.id,
            podcastId: existing.podcastId,
            entityType: existing.entityType,
            entityValue: existing.entityValue,
            normalizedValue: existing.normalizedValue,
            state: .blocked,
            confirmationCount: existing.confirmationCount,
            rollbackCount: existing.rollbackCount,
            firstSeenAt: existing.firstSeenAt,
            lastConfirmedAt: existing.lastConfirmedAt,
            lastRollbackAt: existing.lastRollbackAt,
            decayedAt: existing.decayedAt,
            blockedAt: now,
            aliases: existing.aliases,
            metadata: existing.metadata
        )
        try await store.upsertKnowledgeEntry(blocked)
    }

    // MARK: - Query: Active Entries for Matcher

    /// Returns only active entries for a podcast — the set that
    /// SponsorKnowledgeMatcher should use for matching.
    func activeEntries(forPodcast podcastId: String) async throws -> [SponsorKnowledgeEntry] {
        try await store.loadKnowledgeEntries(podcastId: podcastId, state: .active)
    }

    /// Returns all entries for a podcast regardless of state (for diagnostics).
    func allEntries(forPodcast podcastId: String) async throws -> [SponsorKnowledgeEntry] {
        try await store.loadAllKnowledgeEntries(podcastId: podcastId)
    }

    /// Returns a single entry by its natural key.
    func entry(
        podcastId: String,
        entityType: KnowledgeEntityType,
        normalizedValue: String
    ) async throws -> SponsorKnowledgeEntry? {
        try await store.loadKnowledgeEntry(
            podcastId: podcastId,
            entityType: entityType,
            normalizedValue: normalizedValue
        )
    }

    /// Returns candidate events for a given analysis asset.
    func candidateEvents(forAsset analysisAssetId: String) async throws -> [KnowledgeCandidateEvent] {
        try await store.loadKnowledgeCandidateEvents(analysisAssetId: analysisAssetId)
    }

    // MARK: - Query: Active Entries with Negative Memory Applied

    /// Returns active entries for a podcast, filtering out any that are
    /// negated by corrections in the AnalysisStore's correction_events table.
    /// Sponsor-type entries are checked against sponsorOnShow correction scopes.
    /// Note: corrections are queried from the same AnalysisStore that backs
    /// this knowledge store, since both tables share a single SQLite database.
    func activeEntriesWithNegativeMemory(
        forPodcast podcastId: String
    ) async throws -> [SponsorKnowledgeEntry] {
        let entries = try await activeEntries(forPodcast: podcastId)

        // Collect all sponsor correction scopes in one pass, then batch-check
        // the DB in a single query instead of O(n) individual lookups.
        let sponsorEntries = entries.filter { $0.entityType == .sponsor }
        let scopeStrings = sponsorEntries.map { entry in
            CorrectionScope.sponsorOnShow(
                podcastId: podcastId,
                sponsor: entry.normalizedValue
            ).serialized
        }
        let blockedScopes = try await store.correctionScopesPresent(from: scopeStrings)

        return entries.filter { entry in
            guard entry.entityType == .sponsor else { return true }
            let scope = CorrectionScope.sponsorOnShow(
                podcastId: podcastId,
                sponsor: entry.normalizedValue
            ).serialized
            return !blockedScopes.contains(scope)
        }
    }

    // MARK: - Promotion Logic

    /// Apply promotion transitions iteratively until the state stabilizes.
    /// This handles multi-step promotions (e.g., candidate → quarantined → active
    /// in a single recordCandidate call when counts satisfy both thresholds).
    func stablePromoteState(
        current: KnowledgeState,
        confirmationCount: Int,
        rollbackCount: Int,
        hasActiveCorrection: Bool
    ) -> KnowledgeState {
        var state = current
        for _ in 0..<5 { // bounded iteration to prevent infinite loops
            let next = promoteState(
                current: state,
                confirmationCount: confirmationCount,
                rollbackCount: rollbackCount,
                hasActiveCorrection: hasActiveCorrection
            )
            if next == state { break }
            state = next
        }
        return state
    }

    /// Compute the single next state transition when a confirmation is recorded.
    func promoteState(
        current: KnowledgeState,
        confirmationCount: Int,
        rollbackCount: Int,
        hasActiveCorrection: Bool
    ) -> KnowledgeState {
        guard !hasActiveCorrection else { return current }

        let rollbackRate = (confirmationCount + rollbackCount) > 0
            ? Double(rollbackCount) / Double(confirmationCount + rollbackCount)
            : 0.0

        switch current {
        case .candidate:
            // candidate → quarantined: at least one confirmation (already counted)
            return confirmationCount >= 1 ? .quarantined : .candidate
        case .quarantined:
            // quarantined → active: >=2 confirmations, rollback below threshold
            if confirmationCount >= KnowledgePromotionThresholds.minConfirmationsForActive
                && rollbackRate <= KnowledgePromotionThresholds.maxRollbackRateForActive
            {
                return .active
            }
            return .quarantined
        case .active:
            // Already active — stay active unless rollback spike detected.
            if rollbackRate > KnowledgePromotionThresholds.rollbackSpikeThreshold {
                return .decayed
            }
            return .active
        case .decayed:
            // Decayed entries can be re-promoted if new confirmations arrive
            // and rollback rate drops.
            if confirmationCount >= KnowledgePromotionThresholds.minConfirmationsForActive
                && rollbackRate <= KnowledgePromotionThresholds.maxRollbackRateForActive
            {
                return .active
            }
            return .decayed
        case .blocked:
            // Blocked entries cannot be promoted.
            return .blocked
        }
    }

    // MARK: - Demotion Logic

    /// Compute the next state when a rollback is recorded.
    func demoteState(
        current: KnowledgeState,
        confirmationCount: Int,
        rollbackCount: Int
    ) -> KnowledgeState {
        let total = confirmationCount + rollbackCount
        let rollbackRate = total > 0 ? Double(rollbackCount) / Double(total) : 0.0

        switch current {
        case .candidate, .quarantined:
            // High rollback rate on a pre-active entry → blocked.
            if rollbackRate > KnowledgePromotionThresholds.rollbackSpikeThreshold {
                return .blocked
            }
            return current
        case .active:
            // Rollback spike on active entry → decayed.
            if rollbackRate > KnowledgePromotionThresholds.rollbackSpikeThreshold {
                return .decayed
            }
            return .active
        case .decayed:
            // Persistent rollback → blocked.
            if rollbackRate > KnowledgePromotionThresholds.rollbackSpikeThreshold {
                return .blocked
            }
            return .decayed
        case .blocked:
            return .blocked
        }
    }
}
