// LearningArtifactIngestor.swift
// playhead-hygc.1.7: Wires deduped `correction_events` rows into the
// durable learning artifact surface (training_examples + sponsor knowledge).
//
// Why this exists:
//   playhead-hygc.1.6 tightened the read side via
//   `distinctSemanticCorrections`, but the *write* side still left side
//   effects scattered across SkipOrchestrator, AdDetectionService, and
//   ad-hoc backfill passes. As a result a user-confirmed false negative
//   landed in `correction_events` but the materializer might not run for
//   minutes, and a sponsor-scoped FP veto never reached
//   `SponsorKnowledgeStore.recordRollback`. This actor is the explicit
//   single seam those paths now go through after a correction is recorded.
//
// Contract:
//   • `ingest(correction:)` is idempotent: a row whose semantic identity
//     (asset, type, normalized scope) already exists is reported as
//     `.deduped` and produces no second-order side effects. The actor
//     remembers identities for the lifetime of the process so callers
//     don't re-rollback a sponsor on every replay.
//   • Every successful ingest triggers
//     `TrainingExampleMaterializer.materialize(forAsset:)`. The
//     materializer is itself idempotent (`replaceTrainingExamples` does
//     id-keyed upsert) so retriggers are cheap.
//   • Sponsor side effects only fire for `.sponsorOnShow` corrections.
//     FN → `recordCandidate` (one confirmation), FP → `recordRollback`.
//     Other scopes (exact spans, phrase/campaign/domain/jingle) flow
//     through to materialization but don't touch sponsor knowledge.
//
// Diagnostics:
//   `diagnostics()` returns running counters useful for the bead's
//   acceptance test and for post-hoc audits in dogfood diagnostics
//   exports. Counters are append-only over the actor's lifetime; callers
//   that want a per-session view should hold a fresh ingestor.

import Foundation
import OSLog

// MARK: - Outcomes

enum LearningIngestionOutcome: Sendable, Equatable {
    /// A new semantic identity was persisted. Materialization ran.
    case ingested
    /// The semantic identity was already known. No-op (no double rollback,
    /// no second materialization pass forced).
    case deduped
    /// The correction was rejected (e.g. malformed scope). Counted for
    /// diagnostics but not propagated to materializer or sponsor store.
    case skippedMalformed
}

struct LearningIngestionResult: Sendable, Equatable {
    let outcome: LearningIngestionOutcome
    /// The asset that owns the correction (nil only for malformed scopes).
    let analysisAssetId: String?
}

// MARK: - Diagnostics

struct LearningIngestionDiagnostics: Sendable, Equatable {
    /// Total ingest calls observed.
    var raw: Int = 0
    /// Calls that hit the in-process identity dedupe.
    var deduped: Int = 0
    /// Calls that produced new persisted artifacts.
    var ingested: Int = 0
    /// Calls rejected before any side effect (e.g. malformed scope).
    var skippedMalformed: Int = 0
    /// `.sponsorOnShow` FN corrections that triggered a knowledge
    /// candidate confirmation.
    var sponsorCandidatesConfirmed: Int = 0
    /// `.sponsorOnShow` FP corrections that triggered a knowledge
    /// rollback.
    var sponsorRollbacksApplied: Int = 0
}

// MARK: - LearningArtifactIngestor

actor LearningArtifactIngestor {

    private let store: AnalysisStore
    private let knowledgeStore: SponsorKnowledgeStore?
    private let materializer: TrainingExampleMaterializer
    private let logger = Logger(
        subsystem: "com.playhead",
        category: "LearningArtifactIngestor"
    )

    /// In-process semantic-identity set. The store's v23 UNIQUE INDEX
    /// already collapses duplicates on the persistence side, but
    /// remembering identities here lets the ingestor surface
    /// `.deduped` outcomes to callers and avoid re-applying sponsor
    /// rollbacks (which are *not* idempotent on `recordRollback`'s side
    /// — every call increments `rollbackCount`).
    private var seenIdentities: Set<String> = []

    private var counters = LearningIngestionDiagnostics()

    init(
        store: AnalysisStore,
        knowledgeStore: SponsorKnowledgeStore? = nil,
        materializer: TrainingExampleMaterializer = TrainingExampleMaterializer()
    ) {
        self.store = store
        self.knowledgeStore = knowledgeStore
        self.materializer = materializer
    }

    // MARK: - Public entry points

    /// Persist `correction` (idempotent), then run materialization and
    /// sponsor side effects when the row is new.
    @discardableResult
    func ingest(correction: CorrectionEvent) async throws -> LearningIngestionResult {
        counters.raw += 1
        guard let parsedScope = CorrectionScope.deserialize(correction.scope) else {
            counters.skippedMalformed += 1
            logger.warning(
                "ingest: rejecting correction with malformed scope: \(correction.scope, privacy: .public)"
            )
            return LearningIngestionResult(
                outcome: .skippedMalformed,
                analysisAssetId: nil
            )
        }

        let identityKey = Self.identityKey(
            analysisAssetId: correction.analysisAssetId,
            type: correction.correctionType ?? correction.source?.kind.correctionType ?? .falsePositive,
            scope: parsedScope
        )
        if seenIdentities.contains(identityKey) {
            counters.deduped += 1
            return LearningIngestionResult(
                outcome: .deduped,
                analysisAssetId: correction.analysisAssetId
            )
        }

        // Persist via the store's append path. The v23 UNIQUE INDEX
        // upsert collapses any pre-existing duplicate row to one — so
        // appending the same identity twice across processes is safe.
        try await store.appendCorrectionEvent(correction)

        // Sponsor side effects fire only for `.sponsorOnShow` (the
        // scope that names a sponsor). Other scopes contribute to
        // training examples via the materializer but don't touch
        // sponsor knowledge.
        switch parsedScope {
        case .sponsorOnShow(let podcastId, let sponsor):
            try await applySponsorSideEffect(
                podcastId: podcastId,
                sponsor: sponsor,
                kind: correction.source?.kind
                    ?? correction.correctionType.map(Self.kindFor)
                    ?? .falsePositive
            )
        default:
            break
        }

        // Materialize after every new correction so downstream
        // consumers see fresh artifacts without waiting for the next
        // backfill pass. `replaceTrainingExamples` is id-keyed upsert,
        // so a re-run is cheap and idempotent.
        try await materializer.materialize(
            forAsset: correction.analysisAssetId,
            store: store
        )

        seenIdentities.insert(identityKey)
        counters.ingested += 1
        return LearningIngestionResult(
            outcome: .ingested,
            analysisAssetId: correction.analysisAssetId
        )
    }

    /// Snapshot of running diagnostics. Useful for the bead's acceptance
    /// criterion and for diagnostics exports.
    func diagnostics() -> LearningIngestionDiagnostics {
        counters
    }

    // MARK: - Internals

    private func applySponsorSideEffect(
        podcastId: String,
        sponsor: String,
        kind: CorrectionKind
    ) async throws {
        guard let knowledgeStore else { return }
        switch kind {
        case .falseNegative:
            // FN on a sponsor scope means "every episode of this show
            // really did sponsor X, please remember that." Record a
            // confirmation against the entity. Use a confidence at the
            // `minCandidateConfidence` floor so we don't over-promote
            // on a single correction — the lifecycle still requires
            // multiple confirmations to reach `.active`.
            try await knowledgeStore.recordCandidate(
                podcastId: podcastId,
                entityType: .sponsor,
                entityValue: sponsor,
                analysisAssetId: "correction-\(UUID().uuidString)",
                sourceAtomOrdinals: [],
                transcriptVersion: "correction",
                confidence: KnowledgePromotionThresholds.minCandidateConfidence
            )
            counters.sponsorCandidatesConfirmed += 1
        case .falsePositive:
            try await knowledgeStore.recordRollback(
                podcastId: podcastId,
                entityType: .sponsor,
                entityValue: sponsor
            )
            counters.sponsorRollbacksApplied += 1
        }
    }

    private static func identityKey(
        analysisAssetId: String,
        type: CorrectionType,
        scope: CorrectionScope
    ) -> String {
        // Use the same normalization the store's v23 UNIQUE INDEX
        // applies, but live in-process so we don't need a round-trip
        // through SQLite to detect a duplicate.
        let normalizedScope: String
        switch scope {
        case .exactSpan(let assetId, let range):
            normalizedScope = "exactSpan:\(assetId):\(range.lowerBound):\(range.upperBound)"
        case .exactTimeSpan(let assetId, let start, let end):
            // Bucket to integer-millisecond precision to match v23's
            // dedupe convention (the persistence layer rounds to ms
            // before computing identity to absorb FP drift across
            // rebinds of the same UI gesture).
            let s = (start * 1000.0).rounded() / 1000.0
            let e = (end * 1000.0).rounded() / 1000.0
            normalizedScope = String(format: "exactTimeSpan:%@:%.3f:%.3f", assetId, s, e)
        case .sponsorOnShow(let podcastId, let sponsor):
            normalizedScope = "sponsorOnShow:\(podcastId):\(sponsor.lowercased())"
        case .phraseOnShow(let podcastId, let phrase):
            normalizedScope = "phraseOnShow:\(podcastId):\(phrase.lowercased())"
        case .campaignOnShow(let podcastId, let campaign):
            normalizedScope = "campaignOnShow:\(podcastId):\(campaign.lowercased())"
        case .domainOwnershipOnShow(let podcastId, let domain):
            normalizedScope = "domainOwnershipOnShow:\(podcastId):\(domain.lowercased())"
        case .jingleOnShow(let podcastId, let jingleId):
            normalizedScope = "jingleOnShow:\(podcastId):\(jingleId)"
        }
        return "\(analysisAssetId)|\(type.rawValue)|\(normalizedScope)"
    }

    private static func kindFor(_ type: CorrectionType) -> CorrectionKind {
        switch type {
        case .falseNegative: return .falseNegative
        case .falsePositive: return .falsePositive
        // Boundary-shift corrections are FP-shaped: the system marked
        // something the user disagreed with at the edges. Treat them
        // as suppressing signal rather than confirming.
        case .startTooEarly, .startTooLate, .endTooEarly, .endTooLate:
            return .falsePositive
        }
    }
}
