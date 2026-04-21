// BatchNotificationReducer.swift
// Pure reducer that maps a per-pass set of child episode surface
// summaries into a single `BatchNotificationEligibility` verdict.
// playhead-zp0x.
//
// Pure: same inputs always produce the same output; no side effects, no
// I/O, no logging, no global state. The only "stateful" inputs are the
// previous-pass eligibility and the persistence-rule fields on the
// caller-supplied `DownloadBatch` projection — both of which the
// coordinator threads in explicitly.
//
// Precedence (verbatim from the bead spec):
//   blockedAnalysisUnavailable > blockedStorage > blockedWifiPolicy
//
// Trip-ready short-circuit: when every child is ready, the result is
// `.tripReady` regardless of what blockers any child carries — a ready
// child by definition is no longer waiting for a blocker.
//
// Persistence rule (D from spec): action-required eligibility ONLY
// surfaces once the same blocker has held across ≥ 2 consecutive
// scheduler passes AND ≥ 30 minutes of wall-clock time. Pass count and
// wall-clock are AND, not OR: both bars must clear before the reducer
// promotes the result from `.none` to the matching `blocked*` case.
//
// User-fixability is delegated to `BatchChildSurfaceSummary.userFixable`
// — the reducer does NOT recompute it. The coordinator (which already
// has access to the full surface-status reducer machinery) is
// responsible for deriving `userFixable` from `(reason,
// analysisUnavailableReason)` and stamping the bool on each summary.
// Pinning the boolean at the boundary means this reducer stays free of
// any cross-module derivation policy.

import Foundation

// MARK: - BatchChildSurfaceSummary

/// Per-child input projection for `BatchNotificationReducer`. Built by
/// the coordinator from each batch episode's `EpisodeSurfaceStatus`.
///
/// `userFixable` is computed at the boundary by the coordinator (which
/// has access to the full surface-status taxonomy) so the reducer does
/// not re-derive it. The boundary is the right place for that decision:
/// the reducer's job is precedence + persistence + cap, not policy.
struct BatchChildSurfaceSummary: Sendable, Hashable, Codable {

    /// Canonical episode key (matches `Episode.canonicalEpisodeKey`).
    /// Used by tests and by coordinator log lines for correlation; the
    /// reducer itself only consumes the four downstream fields.
    let canonicalEpisodeKey: String

    /// Surface disposition produced by the episode-level reducer.
    let disposition: SurfaceDisposition

    /// Surface reason produced by the episode-level reducer. The
    /// reducer routes on this to map to the matching `blocked*` case.
    let reason: SurfaceReason

    /// Per-device unavailability reason. Required to disambiguate
    /// `analysisUnavailable` between user-fixable
    /// (`appleIntelligenceDisabled`, `languageUnsupported`) and
    /// non-user-fixable (`hardwareUnsupported`, `regionUnsupported`,
    /// `modelTemporarilyUnavailable`) cases.
    let analysisUnavailableReason: AnalysisUnavailableReason?

    /// True when the child has reached the ready state (proximal
    /// coverage met AND download complete). The reducer's trip-ready
    /// short-circuit requires every child in the batch to be ready.
    let isReady: Bool

    /// True when the child's blocker is user-fixable. Derived by the
    /// coordinator from `(reason, analysisUnavailableReason)`; the
    /// reducer trusts this boolean rather than re-deriving it.
    let userFixable: Bool
}

// MARK: - BatchNotificationReducer

/// Pure reducer that selects a single `BatchNotificationEligibility`
/// from a per-pass set of child surface summaries plus a small
/// projection of the persistent `DownloadBatch`.
///
/// All inputs are explicit; all outputs are deterministic. The
/// coordinator owns the side-effecting plumbing (persistence updates,
/// notification emission, cap enforcement); the reducer only decides
/// what the verdict for THIS pass would be if no caps were in effect.
enum BatchNotificationReducer {

    /// Persistence-rule constants. Public so the coordinator and tests
    /// can reference the same numbers and so a future tuning change has
    /// exactly one knob.
    enum PersistenceRule {
        /// Minimum number of consecutive scheduler passes a blocker
        /// must persist before action-required becomes eligible.
        static let minimumConsecutiveBlockedPasses: Int = 2

        /// Minimum wall-clock interval (in seconds) since the first
        /// blocked pass before action-required becomes eligible.
        /// 30 minutes per the bead spec.
        static let minimumWallClockInterval: TimeInterval = 30 * 60
    }

    /// Persistence-rule projection of `DownloadBatch`. The reducer
    /// stays decoupled from the SwiftData type so it can be exercised
    /// in pure unit tests without spinning up a `ModelContainer`.
    struct PersistenceState: Sendable, Hashable {
        /// Counter of consecutive prior passes that produced an
        /// action-required-eligible reduction. The coordinator passes
        /// the value as it stood ENTERING this pass; the reducer
        /// derives the post-pass increment internally.
        let consecutiveBlockedPasses: Int

        /// Wall-clock anchor for the first blocked pass in the current
        /// streak. `nil` if no streak is in progress.
        let firstBlockedAt: Date?
    }

    // MARK: - Reduction result

    /// Output of `reduce(...)`. Carries both the verdict (what the
    /// caller should fire, after cap rules) and the `pendingBlocker`
    /// candidate (the highest-precedence fixable blocker currently
    /// observed across the batch's children, regardless of whether the
    /// persistence rule has cleared yet).
    ///
    /// The coordinator uses `pendingBlocker` to advance the consecutive-
    /// blocked-pass counter and `firstBlockedAt` anchor on the
    /// persistent batch row — without it, a streak can never accumulate
    /// because the first pass always returns `.none` (persistence rule
    /// not yet satisfied) and a `.none` verdict alone is indistinguishable
    /// from "no blocker exists".
    struct Reduction: Sendable, Hashable {
        /// What the coordinator should consider firing this pass (after
        /// cap rules). `.none` when no blocker has cleared the
        /// persistence rule yet, OR when no blocker exists at all.
        let verdict: BatchNotificationEligibility

        /// The highest-precedence fixable blocker currently present in
        /// the child set, regardless of persistence-rule state. `nil`
        /// means there is no fixable blocker (so the streak should
        /// reset). Non-nil means the streak should advance, even if the
        /// verdict is still `.none`.
        let pendingBlocker: BatchNotificationEligibility?
    }

    // MARK: - Public entry point

    /// Reduce a per-pass set of child summaries into a `Reduction`
    /// (verdict + pending-blocker candidate).
    ///
    /// The reducer does NOT enforce notification caps. It returns the
    /// verdict that THIS pass would produce; the coordinator decides
    /// whether to fire based on the cap flags on the persistent
    /// `DownloadBatch` row.
    ///
    /// - Parameters:
    ///   - childSummaries: per-child surface summaries built by the
    ///     coordinator from each batch episode's `EpisodeSurfaceStatus`.
    ///   - persistence: counters carried over from prior passes; let
    ///     the reducer evaluate the persistence rule without reaching
    ///     into the SwiftData row directly.
    ///   - now: wall-clock at the time of the reduction. Always
    ///     supplied by the caller (so tests can pin time without
    ///     depending on `Date()`).
    static func reduce(
        childSummaries: [BatchChildSurfaceSummary],
        persistence: PersistenceState,
        now: Date
    ) -> Reduction {
        // Empty batch can never be ready or blocked. Treat as `.none`.
        guard !childSummaries.isEmpty else {
            return Reduction(verdict: .none, pendingBlocker: nil)
        }

        // Trip-ready short-circuits any blocker check. By spec a ready
        // child has no outstanding blocker, so this guard is safe even
        // when isReady children somehow also carry a SurfaceReason — we
        // route on isReady, not on reason.
        if childSummaries.allSatisfy({ $0.isReady }) {
            return Reduction(verdict: .tripReady, pendingBlocker: nil)
        }

        // Look for at least one fixable blocker per category. Precedence
        // is analysisUnavailable > storage > wifiPolicy.
        let hasAnalysisUnavailableFixable = childSummaries.contains { summary in
            guard summary.reason == .analysisUnavailable, summary.userFixable else { return false }
            // The reducer only considers the two user-fixable
            // unavailability reasons; everything else (hardware, region,
            // modelTemporarilyUnavailable, nil) is filtered out here so
            // an unexpected `userFixable=true` paired with an
            // unsupported reason cannot smuggle in a notification.
            switch summary.analysisUnavailableReason {
            case .appleIntelligenceDisabled, .languageUnsupported:
                return true
            case .hardwareUnsupported, .regionUnsupported, .modelTemporarilyUnavailable, .none:
                return false
            }
        }

        let hasStorageFixable = childSummaries.contains { summary in
            summary.reason == .storageFull && summary.userFixable
        }

        let hasWifiFixable = childSummaries.contains { summary in
            summary.reason == .waitingForNetwork && summary.userFixable
        }

        // Promote the highest-precedence blocker to a candidate. If
        // every blocker is transient / non-user-fixable, we land on
        // `.none` and the persistence rule has nothing to advance.
        let candidate: BatchNotificationEligibility
        if hasAnalysisUnavailableFixable {
            candidate = .blockedAnalysisUnavailable
        } else if hasStorageFixable {
            candidate = .blockedStorage
        } else if hasWifiFixable {
            candidate = .blockedWifiPolicy
        } else {
            return Reduction(verdict: .none, pendingBlocker: nil)
        }

        // Persistence rule: BOTH minimum-pass-count AND minimum-wall-
        // clock must clear before the action-required candidate is
        // promoted. Coordinator-side bookkeeping is responsible for
        // incrementing the pass counter and stamping `firstBlockedAt`
        // — the reducer just reads.
        //
        // pass count: the coordinator passes the count as of the
        // PRIOR pass; the current pass adds one. So `passesIncludingNow`
        // is `prior + 1`.
        let passesIncludingNow = persistence.consecutiveBlockedPasses + 1
        let passesOK = passesIncludingNow >= PersistenceRule.minimumConsecutiveBlockedPasses

        // Wall-clock: if no anchor is set yet (this is the first
        // blocked pass), the wall-clock bar cannot be cleared. The
        // coordinator will stamp `firstBlockedAt = now` for the next
        // pass, and the elapsed-since check will be evaluated then.
        let elapsedOK: Bool
        if let firstBlockedAt = persistence.firstBlockedAt {
            elapsedOK = now.timeIntervalSince(firstBlockedAt) >= PersistenceRule.minimumWallClockInterval
        } else {
            elapsedOK = false
        }

        let verdict: BatchNotificationEligibility = (passesOK && elapsedOK) ? candidate : .none
        return Reduction(verdict: verdict, pendingBlocker: candidate)
    }
}
