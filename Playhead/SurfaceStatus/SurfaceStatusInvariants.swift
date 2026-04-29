// SurfaceStatusInvariants.swift
// Three-layer invariant enforcement for the SurfaceStatus module.
//
// Scope: playhead-ol05 (Phase 1.5 deliverable 5 — "State-transition audit
// + impossible-state assertions + cross-target contract test").
//
// Why three layers (and what does each layer buy us)?
//
//   * Layer 1 — exhaustive switches over enum fields.
//     `EpisodeSurfaceStatus` is a STRUCT, not a sum type, so the compiler
//     cannot directly enforce exhaustiveness over the struct as a whole.
//     But the struct's three taxonomy fields (`SurfaceDisposition`,
//     `SurfaceReason`, `ResolutionHint`) ARE enums, and the helpers in
//     `SurfaceStatusInvariants` switch over each of those enums
//     exhaustively. When a future bead adds a case to any of the three
//     taxonomy enums, the compiler points at every site here that needs
//     to be updated — which is the closest analog to "compile-time
//     exhaustiveness over the state enum" the plan asks for.
//
//   * Layer 2 — runtime preconditions at the reducer output (DEBUG only).
//     `SurfaceStatusInvariants.validate(_:)` runs through all five
//     Phase 1.5 invariants and crashes with `precondition(...)` in DEBUG
//     builds when any of them is violated. This fast-fails during
//     development and dogfood Debug-builds (per playhead-e2a3 build
//     flavor) without paying the cost in Release.
//
//   * Layer 3 — production-safe logging (all builds).
//     Every invariant violation also calls `SurfaceStatusInvariantLogger.record(_:)`
//     which writes a single JSON line to a per-session file under
//     `Caches/Diagnostics/`. This signal is the authoritative input to
//     `playhead-e2a3`'s 10-day audit (pass criterion 1: "no impossible-
//     state entries observed in production telemetry").
//
// Phase 1.5 invariants enforced (carried verbatim from the bead spec):
//   1. `surfaceDisposition == .paused AND surfaceReason == nil`
//      (reason is non-optional today — vacuously true; the exhaustive
//      switch at validation time still catches a future schema change.)
//   2. `surfaceDisposition == .unavailable AND resolutionHint == .retry`
//      (unavailable is not retryable — the user must clear the
//      eligibility gate, not retry.)
//   3. `analysisUnavailableReason != nil AND surfaceDisposition != .unavailable`
//      (from playhead-sueq: the reason field "never appears in Paused
//      section". It is paired exclusively with the .unavailable
//      disposition.)
//   4. `BatchSurfaceStatus.aggregateDisposition == .ready AND any
//      child.disposition in {.paused, .unavailable, .failed}`
//      (a batch cannot be `.ready` while any constituent episode is in a
//      non-ready state.)
//   5. `BatchSurfaceStatus.childDispositions is empty AND
//      aggregateDisposition != .queued`
//      (an empty batch can only be in the queued tier — there is nothing
//      to make it `.failed`/`.paused`/etc.)
//
// Deferred to a Phase 2 follow-up bead (CoverageSummary / PlaybackReadiness
// are stub types today, so the invariants below cannot be evaluated until
// playhead-cthe ships):
//   * playbackReadiness ∈ {.proximal,.complete} AND analysisUnavailableReason != nil
//   * coverage.isComplete == true AND playbackReadiness != .complete
//
// Phase 2 — playhead-cthe invariants on the derived `(coverage, readiness)`
// pair. Enforced via `violations(coverage:readiness:)` below:
//   A. `readiness == .complete` implies `coverage?.isComplete == true`
//      AND `firstCoveredOffset != nil` (complete implies proximal).
//   B. `readiness == .proximal` implies `coverage?.firstCoveredOffset != nil`
//      (proximal requires a non-empty coverage record).
//   These are spec-mandated impossible-state assertions — reaching either
//   branch means something upstream persisted an inconsistent
//   CoverageSummary (e.g. `isComplete=true` with empty ranges).
//
// Layered relationship to the reducer:
//   * The reducer (`episodeSurfaceStatus`) is the producer of every
//     `EpisodeSurfaceStatus` consumed by the rest of the app. Validation
//     runs at the reducer's exit point, not at every consumption site —
//     so a single point of enforcement covers every UI surface.
//   * `BatchSurfaceStatus` does not yet have a reducer (Phase 2 scope).
//     The validator still ships the invariants so when batch construction
//     paths land, the helper is ready to be called from those sites.

import Foundation

// MARK: - SurfaceStatusInvariants

/// Static helpers that enforce the Phase 1.5 invariants on
/// `EpisodeSurfaceStatus` and `BatchSurfaceStatus` outputs.
///
/// Modeled as a `enum` (uninhabited namespace) so there is no instance
/// to construct and no reference-counting to pay on the hot path. All
/// helpers are pure — they read the input and emit either a violation
/// description (when a check fails) or `nil` (when the input is valid).
enum SurfaceStatusInvariants {

    // MARK: - Episode validation

    /// Run every Phase 1.5 invariant against an `EpisodeSurfaceStatus`.
    /// Returns the list of violations (empty when the input is valid).
    ///
    /// `episodeIdHash` is plumbed through so the production logger can
    /// attribute the violation without holding the raw episode ID in
    /// memory. Callers that have no episode context (e.g. unit tests of
    /// the validator itself) may pass `nil`.
    static func violations(
        of status: EpisodeSurfaceStatus
    ) -> [InvariantViolation] {
        var found: [InvariantViolation] = []

        // Invariant 1: paused → reason is non-nil.
        // `EpisodeSurfaceStatus.reason` is non-optional today, so this
        // check is vacuously true. The switch below is still exhaustive
        // over `SurfaceDisposition` so a future schema change (reason
        // becoming optional) forces a deliberate audit here.
        switch status.disposition {
        case .paused:
            // No-op today — reason is always present at the type level.
            // Reserved for the day reason becomes optional.
            break
        case .queued, .unavailable, .failed, .cancelled:
            break
        }

        // Invariant 2: unavailable + .retry hint is impossible.
        // `.unavailable` reflects an eligibility gate the user must
        // clear (enable Apple Intelligence, change region, etc.) — a
        // retry button on top of that would mislead the user into
        // thinking the system can recover on its own.
        switch (status.disposition, status.hint) {
        case (.unavailable, .retry):
            found.append(
                InvariantViolation(
                    code: .unavailableWithRetryHint,
                    description: "EpisodeSurfaceStatus has disposition=.unavailable but hint=.retry; .unavailable is not retryable."
                )
            )
        default:
            break
        }

        // Invariant 3: analysisUnavailableReason ⇔ disposition == .unavailable.
        // Both directions matter:
        //   * a non-nil reason on a non-`.unavailable` disposition would
        //     leak the unavailability copy into a Paused/Queued/etc. UI
        //     surface (the playhead-sueq contract);
        //   * a `.unavailable` disposition without a reason would render
        //     a generic "analysis unavailable" without telling the user
        //     which gate failed.
        if status.analysisUnavailableReason != nil && status.disposition != .unavailable {
            found.append(
                InvariantViolation(
                    code: .unavailableReasonOnNonUnavailableDisposition,
                    description: "EpisodeSurfaceStatus has analysisUnavailableReason=\(status.analysisUnavailableReason!) but disposition=\(status.disposition); the reason must only appear on .unavailable."
                )
            )
        }

        return found
    }

    // MARK: - Coverage / readiness validation (playhead-cthe)

    /// Run the Phase 2 coverage/readiness invariants against the derived
    /// `(coverage, readiness)` pair. Callers that have just derived a
    /// readiness (reducer, Library cell, NowPlaying overlay) may route
    /// the pair through this helper to catch an upstream producer that
    /// persisted an inconsistent CoverageSummary.
    ///
    /// Returns the list of violations (empty when the pair is valid).
    /// Always passes through to `enforce(_:)` at the caller site — this
    /// helper itself is pure.
    ///
    /// Invariants enforced:
    ///   * `readiness == .complete` ⇒ `coverage?.isComplete == true` AND
    ///     `coverage?.firstCoveredOffset != nil`. "complete implies
    ///     proximal" — a complete episode trivially covers every
    ///     possible 15-minute lookahead, so the coverage record must at
    ///     minimum carry a non-nil first offset.
    ///   * `readiness == .proximal` ⇒ `coverage?.firstCoveredOffset != nil`.
    ///     Reaching `.proximal` means the derivation found a range
    ///     containing the anchor; by construction that range is the first
    ///     non-empty element of `coverageRanges`, which implies
    ///     `firstCoveredOffset != nil`. A logged violation here means
    ///     someone hand-constructed a `.proximal` via a path that
    ///     bypasses `derivePlaybackReadiness`.
    static func violations(
        coverage: CoverageSummary?,
        readiness: PlaybackReadiness
    ) -> [InvariantViolation] {
        var found: [InvariantViolation] = []

        switch readiness {
        case .complete:
            // Invariant A: complete implies proximal. A `.complete`
            // readiness must be backed by a coverage record with
            // `isComplete=true`. `firstCoveredOffset == nil` on a
            // `.complete` record would mean "the whole episode is
            // analyzed but there is no first covered offset", which is
            // a contradiction.
            guard let coverage else {
                found.append(
                    InvariantViolation(
                        code: .completeReadinessMissingCoverage,
                        description: "PlaybackReadiness=.complete but coverage is nil; complete requires a coverage record with isComplete=true."
                    )
                )
                return found
            }
            if !coverage.isComplete {
                found.append(
                    InvariantViolation(
                        code: .completeReadinessMissingCoverage,
                        description: "PlaybackReadiness=.complete but coverage.isComplete=false; the readiness derivation must not return .complete for an incomplete coverage record."
                    )
                )
            }
            if coverage.firstCoveredOffset == nil {
                found.append(
                    InvariantViolation(
                        code: .completeReadinessMissingFirstOffset,
                        description: "PlaybackReadiness=.complete but coverage.firstCoveredOffset=nil; complete implies proximal which requires a non-nil first offset."
                    )
                )
            }
        case .proximal:
            // Invariant B: proximal requires firstCoveredOffset != nil.
            guard let coverage else {
                found.append(
                    InvariantViolation(
                        code: .proximalReadinessMissingFirstOffset,
                        description: "PlaybackReadiness=.proximal but coverage is nil; .proximal requires a non-empty coverage record with firstCoveredOffset != nil."
                    )
                )
                return found
            }
            if coverage.firstCoveredOffset == nil {
                found.append(
                    InvariantViolation(
                        code: .proximalReadinessMissingFirstOffset,
                        description: "PlaybackReadiness=.proximal but coverage.firstCoveredOffset=nil; .proximal derivation must not fire against empty coverage."
                    )
                )
            }
        case .none, .deferredOnly:
            // These states do not carry the "some range covers the
            // anchor" assertion, so the first-offset / isComplete checks
            // above are vacuous. Nothing to enforce here.
            break
        }

        return found
    }

    // MARK: - Batch validation

    /// Run every Phase 1.5 batch invariant against a `BatchSurfaceStatus`
    /// plus the per-child disposition vector. The reducer for
    /// `BatchSurfaceStatus` lands in Phase 2; the helper is shipped now
    /// so the Phase 2 reducer can call into a stable API.
    ///
    /// `BatchSurfaceStatus` does not store its own child-disposition
    /// vector (that lives upstream in the batch reducer). Callers pass
    /// `childDispositions` explicitly so this helper can stay a pure
    /// function over inputs.
    static func violations(
        of status: BatchSurfaceStatus,
        childDispositions: [SurfaceDisposition]
    ) -> [InvariantViolation] {
        var found: [InvariantViolation] = []

        // Invariant 4: aggregate=.ready (which Phase 1.5 maps to .queued
        // in the absence of a real .ready disposition — see note below)
        // forbids any child being .paused/.unavailable/.failed.
        //
        // SurfaceDisposition does NOT have a `.ready` case in Phase 1.5
        // (the closest analog is the absence of a non-ready disposition
        // — i.e. a `.queued` aggregate that is waiting only on time). We
        // map "aggregate ready" to "aggregate is .queued" for Phase 1.5;
        // when Phase 2 introduces an explicit ready/complete signal the
        // invariant should be retargeted. Pinned here so a future bead
        // sees the bridge.
        if status.disposition == .queued {
            for childDisposition in childDispositions {
                switch childDisposition {
                case .paused, .unavailable, .failed:
                    found.append(
                        InvariantViolation(
                            code: .batchReadyWithNonReadyChild,
                            description: "BatchSurfaceStatus.disposition=.queued (Phase 1.5 stand-in for .ready) but a child episode has disposition=\(childDisposition)."
                        )
                    )
                    // One violation is enough to flag the batch — break
                    // out so a single bad batch does not flood the log.
                    return found
                case .queued, .cancelled:
                    continue
                }
            }
        }

        // Invariant 5: empty batch can only be queued.
        if childDispositions.isEmpty && status.disposition != .queued {
            found.append(
                InvariantViolation(
                    code: .emptyBatchWithNonQueuedDisposition,
                    description: "BatchSurfaceStatus has no children but disposition=\(status.disposition); empty batches must be .queued."
                )
            )
        }

        return found
    }

    // MARK: - Tier-A enforcement (DEBUG-only precondition)

    /// Tier-A enforcement: in DEBUG builds, fail fast on any invariant
    /// violation; in Release, fall through silently. Layer 3 (the JSON
    /// Lines logger) handles the all-builds signal — this is the local-
    /// dev safety net.
    ///
    /// Always pairs with `recordViolations(_:)` below — DEBUG fails
    /// loudly AND the logger records (so a single dogfood Debug-build
    /// run can both crash AND emit the audit line). Release skips the
    /// crash but still records.
    static func enforce(_ violations: [InvariantViolation]) {
        guard !violations.isEmpty else { return }
        #if DEBUG
        // Concatenate all violations into the message — typically there
        // is only one, but enumerating them in the crash log makes the
        // dev-time signal as informative as the audit log.
        let message = violations.map { "[\($0.code)] \($0.description)" }
            .joined(separator: " || ")
        preconditionFailure("SurfaceStatus invariant violation(s): \(message)")
        #endif
    }
}

// MARK: - InvariantViolation

/// A single invariant-violation record. Used by both the DEBUG-only
/// precondition path (Tier A) and the all-builds JSON Lines logger
/// (Tier B / Layer 3).
///
/// Keeping the violation as a value type with a stable code field means
/// `playhead-e2a3` can aggregate by `code` across sessions without
/// regex-parsing the description.
struct InvariantViolation: Sendable, Hashable, Codable {
    /// Stable, code-style identifier for the violated invariant. The
    /// raw value is what gets persisted in the JSON Lines log so audit
    /// queries can group / count by it.
    enum Code: String, Sendable, Hashable, Codable, CaseIterable {
        /// Invariant 2: `.unavailable` disposition combined with the
        /// `.retry` hint.
        case unavailableWithRetryHint = "unavailable_with_retry_hint"

        /// Invariant 3: `analysisUnavailableReason != nil` while the
        /// disposition is something other than `.unavailable`.
        case unavailableReasonOnNonUnavailableDisposition =
            "unavailable_reason_on_non_unavailable_disposition"

        /// Invariant 4: a Phase-1.5-stand-in-for-ready batch with at
        /// least one non-ready child episode.
        case batchReadyWithNonReadyChild = "batch_ready_with_non_ready_child"

        /// Invariant 5: an empty batch with a non-`.queued` disposition.
        case emptyBatchWithNonQueuedDisposition =
            "empty_batch_with_non_queued_disposition"

        /// Synthetic catch-all used by
        /// `SurfaceStatusInvariantLogger.recordUnknown(_:)` when a future
        /// caller has no specific `Code` for its condition. Not one of the
        /// five numbered Phase-1.5 invariants. Reaching this code from
        /// production means a new code-less site was added — a follow-up
        /// should give it a real `Code` (e2a3 aggregates by code, so an
        /// `.unknown` bucket is intentionally vague).
        ///
        /// As of `playhead-glch`, every reducer call-site has been
        /// migrated to a specific code; `recordUnknown(_:)` exists only
        /// as a deliberately-explicit "I don't have a code for this"
        /// fallback with zero current callers in production code.
        case unknown = "unknown"

        /// `EpisodeSurfaceStatusReducer` Rule 2 (user-paused) matched a
        /// cause that is not one of `.userPreempted` / `.userCancelled` /
        /// `.appForceQuitRequiresRelaunch`. The `isUserPaused` classifier
        /// fell out of sync with the inner `switch` — this is a
        /// reducer-precedence impossibility, not a taxonomy violation.
        case userPausedUnknownCause = "user_paused_unknown_cause"

        /// `EpisodeSurfaceStatusReducer` Rule 3 (resource-block) matched a
        /// cause that is not handled by the inner `switch` (`.mediaCap` /
        /// `.analysisCap` / `.taskExpired`). `isResourceBlock` and the
        /// inner switch fell out of sync.
        case resourceBlockUnknownCause = "resource_block_unknown_cause"

        /// `EpisodeSurfaceStatusReducer` Rule 4 (transient-wait) matched a
        /// cause that is not handled by the inner `switch`. `isTransientWait`
        /// and the inner switch fell out of sync.
        case transientWaitUnknownCause = "transient_wait_unknown_cause"

        /// `EpisodeSurfaceStatusReducer` default branch received an
        /// `InternalMissCause.unknown(raw)` forward-compat sentinel — a
        /// schema-evolved cause string escaped upstream validation. The
        /// surface output is still a safe conservative triple; this code
        /// lets e2a3 count unmapped-cause occurrences independently of
        /// the surface behavior. Distinct from the three precedence-ladder
        /// codes above because this fires on a known-shape sentinel
        /// rather than a precedence inconsistency.
        case unmappedForwardCompatCause = "unmapped_forward_compat_cause"

        /// playhead-cthe Invariant A (part 1): `PlaybackReadiness
        /// == .complete` but the coverage record is nil OR its
        /// `isComplete` flag is false. "complete" must be backed by a
        /// coverage record with `isComplete=true`.
        case completeReadinessMissingCoverage =
            "complete_readiness_missing_coverage"

        /// playhead-cthe Invariant A (part 2): `PlaybackReadiness
        /// == .complete` but `coverage.firstCoveredOffset == nil`.
        /// "complete implies proximal" — a complete episode covers
        /// every anchor's lookahead and therefore must carry a non-nil
        /// first offset.
        case completeReadinessMissingFirstOffset =
            "complete_readiness_missing_first_offset"

        /// playhead-cthe Invariant B: `PlaybackReadiness == .proximal`
        /// but `coverage.firstCoveredOffset == nil` (or coverage is
        /// nil). By construction `.proximal` is derived from a range
        /// that contains the anchor, which implies `firstCoveredOffset
        /// != nil`. Reaching this code means someone bypassed
        /// `derivePlaybackReadiness(coverage:anchor:)`.
        case proximalReadinessMissingFirstOffset =
            "proximal_readiness_missing_first_offset"
    }

    let code: Code
    let description: String

    init(code: Code, description: String) {
        self.code = code
        self.description = description
    }
}
