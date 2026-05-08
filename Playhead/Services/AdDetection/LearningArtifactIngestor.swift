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

    /// In-process semantic-identity set. Used as a fast-path to surface
    /// `.deduped` outcomes for repeat ingests within the same process —
    /// the durable source of truth for "is this a new persist?" lives in
    /// `AnalysisStore` (the v23 UNIQUE INDEX + `appendCorrectionEvent`'s
    /// `Bool` return).
    ///
    /// Concurrency contract: the identity is reserved BEFORE any `await`
    /// inside `ingest`, so concurrent re-entries on the actor observe
    /// the identity already taken and short-circuit to `.deduped`. The
    /// reservation is rolled back on throw so a transient SQLite hiccup
    /// doesn't permanently mask future retries.
    ///
    /// Durable guard: sponsor side effects gate on the persistence
    /// layer's `wasNewlyInserted` Bool, not on the in-process
    /// reservation. The store probes existence BEFORE the upsert
    /// (atomically inside its actor, no awaits between probe and
    /// write), so a row already on disk reports `false` and side
    /// effects are skipped — even when an across-process replay or a
    /// rolled-back reservation leaves the in-process set empty. The
    /// reservation is therefore a perf-only fast-path; the durable
    /// guard is the persistence Bool.
    ///
    /// Lifecycle: the set grows monotonically over the actor's
    /// lifetime. In practice corrections per process are O(10s–100s)
    /// and each entry is a short identity key (~80 bytes), so the
    /// memory cost is bounded by realistic usage. A long-running
    /// process that wants to bound memory explicitly should construct
    /// a fresh ingestor; the persistence-layer guard keeps that safe
    /// from a correctness standpoint.
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
            // Privacy contract: log only the scope's leading prefix (the
            // token before the first ':' — the *type* discriminator) and
            // the overall length. The remainder may carry user-typed text
            // (phrase scopes), podcast-identifying podcastIds, or sponsor
            // names. None of that may surface in os_log even when the
            // parse fails. The public prefix is bounded to 32 chars so a
            // malformed payload that lacks a ':' can't dump arbitrary
            // bytes into the system log.
            let scope = correction.scope
            let prefix = scope.split(separator: ":", maxSplits: 1).first.map(String.init) ?? scope
            let safePrefix = String(prefix.prefix(32))
            logger.warning(
                "ingest: rejecting correction with malformed scope (typePrefix=\(safePrefix, privacy: .public), len=\(scope.count, privacy: .public))"
            )
            return LearningIngestionResult(
                outcome: .skippedMalformed,
                analysisAssetId: nil
            )
        }

        // Use `effectiveCorrectionType` so the in-process identity key
        // agrees with the persistence-layer's identity tuple by construction
        // (`AnalysisStore.appendCorrectionEvent` keys the v23 UNIQUE INDEX
        // off `event.effectiveCorrectionType` via the same extension). A
        // divergent inline derivation here would split legacy nil-typed
        // rows into a different in-process bucket than the on-disk one.
        let identityKey = Self.identityKey(
            analysisAssetId: correction.analysisAssetId,
            type: correction.effectiveCorrectionType,
            scope: parsedScope
        )
        if seenIdentities.contains(identityKey) {
            counters.deduped += 1
            return LearningIngestionResult(
                outcome: .deduped,
                analysisAssetId: correction.analysisAssetId
            )
        }

        // Reserve the identity BEFORE any `await` so concurrent
        // re-entries on the actor see the identity already taken and
        // short-circuit to `.deduped`. This closes the TOCTOU window
        // between the contains-check above and the persistence /
        // side-effect calls below — without the reservation, N concurrent
        // ingests of the same correction would all pass the contains
        // check, all call the non-idempotent `recordRollback`, and
        // `rollbackCount` would jump to N for a single user gesture.
        seenIdentities.insert(identityKey)

        // Durable side-effect guard. `appendCorrectionEvent` returns
        // `true` only when it inserted a NEW row (its pre-upsert probe
        // saw nothing for this identity). If the row was already on
        // disk — including the rare case where the throw-rollback
        // branch below erased the in-process reservation after a
        // successful first append + downstream throw —
        // `wasNewlyInserted` is `false` and we MUST NOT re-fire the
        // non-idempotent sponsor side effects. Captured here so that a
        // `materializer.materialize` throw downstream still triggers
        // the catch, but the caller's retry sees the persistence
        // layer say "not new" and skips the sponsor side effects on
        // every future attempt.
        let wasNewlyInserted: Bool
        do {
            // Persist via the store's append path. The v23 UNIQUE INDEX
            // upsert collapses any pre-existing duplicate row to one — so
            // appending the same identity twice across processes is safe.
            wasNewlyInserted = try await store.appendCorrectionEvent(correction)

            // Sponsor side effects fire only for `.sponsorOnShow` (the
            // scope that names a sponsor) AND only when the persistence
            // layer says this is a fresh insert. Other scopes contribute
            // to training examples via the materializer but don't touch
            // sponsor knowledge.
            if wasNewlyInserted {
                switch parsedScope {
                case .sponsorOnShow(let podcastId, let sponsor):
                    // `source` is the canonical FN-vs-FP discriminator —
                    // `CorrectionSource` only has two shapes, falsePositive
                    // (`.manualVeto` / `.listenRevert`) and falseNegative
                    // (`.falseNegative`), and `source.kind` returns those
                    // directly. Prefer it when present.
                    //
                    // Fallback for legacy rows where `source == nil`: derive
                    // from `effectiveCorrectionType` via `kindFor`, which
                    // handles boundary-shift types (`.startTooEarly`,
                    // `.startTooLate`, `.endTooEarly`, `.endTooLate`) by
                    // collapsing them to `.falsePositive` — they are
                    // FP-shaped corrections (the user disagreed with the
                    // edges of a flagged span).
                    //
                    // The legacy nil-source / nil-type case defaults to
                    // `.falsePositive` via `effectiveCorrectionType`'s
                    // own fallback, preserving the prior
                    // `correctionPassthroughFactor` legacy assumption.
                    let kind: CorrectionKind = correction.source?.kind
                        ?? Self.kindFor(correction.effectiveCorrectionType)
                    try await applySponsorSideEffect(
                        podcastId: podcastId,
                        sponsor: sponsor,
                        kind: kind
                    )
                default:
                    break
                }
            }

            // Materialize after every new correction so downstream
            // consumers see fresh artifacts without waiting for the next
            // backfill pass. `replaceTrainingExamples` is id-keyed upsert,
            // so a re-run is cheap and idempotent. We materialize even
            // on `wasNewlyInserted == false` (the across-process replay
            // path) because the materializer's spine join is decoupled
            // from this single correction — a different correction may
            // have landed in the meantime that was never materialized.
            try await materializer.materialize(
                forAsset: correction.analysisAssetId,
                store: store
            )
        } catch {
            // Roll the reservation back on error so a transient SQLite
            // hiccup doesn't permanently mask future legitimate retries
            // of this identity. The caller sees the throw and decides
            // whether to retry. Even after this rollback a retry is
            // still safe: the durable persistence Bool reports `false`
            // if `appendCorrectionEvent` succeeded before the throw,
            // and the sponsor side-effect gate skips on that signal.
            seenIdentities.remove(identityKey)
            throw error
        }

        // A non-new persist (an across-process replay landing on an
        // already-deduped row) reports `.deduped` to the caller — same
        // observable shape as the in-process fast-path above — instead
        // of `.ingested`. This keeps the diagnostics counters honest
        // when an app restart drops the in-process `seenIdentities`
        // set and the user replays a previously-recorded correction.
        if !wasNewlyInserted {
            counters.deduped += 1
            return LearningIngestionResult(
                outcome: .deduped,
                analysisAssetId: correction.analysisAssetId
            )
        }

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
        // Route through `CorrectionScope.normalizedIdentityKey` so the
        // in-process `seenIdentities` set keys on EXACTLY the same
        // canonicalization as the on-disk v23 UNIQUE INDEX
        // (`appendCorrectionEvent` derives its `normalizedScopeKey`
        // from this same property). Sharing the canonical form
        // closes two failure modes that a divergent in-process
        // canonicalization could open:
        //
        //   1. Time-bucket drift: two `.exactTimeSpan` corrections
        //      at times 10.05 and 10.10 (50ms apart) collapse on
        //      disk (same 100ms bucket) but would produce DIFFERENT
        //      in-process keys under finer bucketing. Concurrent
        //      re-entrants would then both pass the `contains` check
        //      and both call `appendCorrectionEvent`; the persistence
        //      layer still gates sponsor side effects via the Bool
        //      return, but `submissionCount` would inflate beyond 1
        //      for one user gesture — visible in NARL exports.
        //
        //   2. Casing divergence: a sponsor reported as "Squarespace"
        //      and then "squarespace" would hash to ONE in-process
        //      key under lowercasing but TWO on-disk identities
        //      under case-preserving `serialized` form. The second
        //      call would short-circuit to `.deduped` in-process and
        //      never reach the persistence layer, silently dropping
        //      a correction that would have legitimately written a
        //      second row.
        return "\(analysisAssetId)|\(type.rawValue)|\(scope.normalizedIdentityKey)"
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
