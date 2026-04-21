// BatchSummaryBuilder.swift
// Bridge from the persisted Episode + CapabilitySnapshot + work-journal
// cause to a `BatchChildSurfaceSummary`. playhead-0a0s.
//
// Why a dedicated module: the `BatchNotificationCoordinator` deliberately
// does NOT know how `EpisodeSurfaceStatus` is computed (it accepts an
// opaque `summaryBuilder` closure). This module owns the gather-and-
// reduce logic so the coordinator stays pure and so the batch path can
// be exercised end-to-end in tests without spinning up a runtime.
//
// Reuse contract:
//   * The per-episode reducer (`episodeSurfaceStatus(...)`) is the SOLE
//     authority that maps (state, cause, eligibility, coverage, anchor)
//     to a `EpisodeSurfaceStatus`. This file calls it; it does not
//     duplicate any of its logic.
//   * `userFixable` on the resulting `BatchChildSurfaceSummary` is
//     derived at this boundary from `(reason, analysisUnavailableReason,
//     hint)` per the documented contract on `BatchNotificationReducer`
//     ("the coordinator is responsible for deriving `userFixable`").
//     Centralising the derivation here means the downstream reducer
//     trusts the boolean rather than re-deriving it.
//
// Production wiring:
//   * `PlayheadRuntime.makeBatchSummaryBuilder` constructs a
//     `BatchSummaryBuilder` with closures bound to the live
//     `AnalysisStore`, `CapabilitiesService`, and the App-scope
//     `ModelContainer`, then hands the builder's `summaries(for:)`
//     method to the coordinator. Lives on the runtime (not on
//     `PlayheadApp`) so the SwiftUI `App` layer never references the
//     `AnalysisStore` type directly — `SurfaceStatusUILintTests`
//     forbids module-boundary persistence types in UI files.
//   * The closures keep the builder testable — unit tests inject
//     deterministic eligibility / cause / episode lookups without going
//     through SwiftData or SQLite at all.

import Foundation
import SwiftData

// MARK: - EpisodeProjection

/// Sendable snapshot of the four `Episode` fields the surface-status
/// reducer needs to evaluate a batch child. Lifted off the SwiftData
/// `Episode` row inside whatever actor owns the `ModelContext` (the
/// MainActor in production), then carried into the builder's actor-
/// agnostic projection logic.
///
/// `Episode` itself is `@PersistentModel` and not Sendable; this struct
/// is the boundary type that lets the builder stay free of MainActor
/// isolation while still operating on real persisted data.
struct EpisodeProjection: Sendable, Equatable {
    /// `true` when the episode's audio is fully downloaded.
    let downloaded: Bool

    /// `true` when an analysis run has been confirmed (`AnalysisSummary
    /// .hasAnalysis == true`).
    let analyzed: Bool

    /// Phase 2 coverage record. `nil` until the analysis pipeline writes
    /// one; the reducer treats `nil` as `PlaybackReadiness.none`.
    let coverageSummary: CoverageSummary?

    /// Phase 2 readiness anchor. Updated alongside `playbackPosition`
    /// at the play-loop commit points.
    let playbackAnchor: TimeInterval?

    init(
        downloaded: Bool,
        analyzed: Bool,
        coverageSummary: CoverageSummary?,
        playbackAnchor: TimeInterval?
    ) {
        self.downloaded = downloaded
        self.analyzed = analyzed
        self.coverageSummary = coverageSummary
        self.playbackAnchor = playbackAnchor
    }

    /// Lift a SwiftData `Episode` into a Sendable projection. Must be
    /// called from the actor that owns the `ModelContext` backing
    /// `episode` (the MainActor in production).
    init(_ episode: Episode) {
        self.downloaded = (episode.downloadState == .downloaded)
        self.analyzed = (episode.analysisSummary?.hasAnalysis == true)
        self.coverageSummary = episode.coverageSummary
        self.playbackAnchor = episode.playbackAnchor
    }
}

// MARK: - BatchSummaryBuilder

/// Builds `[BatchChildSurfaceSummary]` for a `BatchNotificationCoordinator`
/// pass by routing each batch child through the per-episode surface-status
/// reducer.
///
/// Sendable: the builder holds only `@Sendable` closures and immutable
/// state. The closures dispatch onto whatever isolation domain owns the
/// underlying resource (MainActor for `ModelContext`, AnalysisStore actor
/// for SQLite).
struct BatchSummaryBuilder: Sendable {

    /// Resolve a canonical episode key to a Sendable `EpisodeProjection`
    /// of the persisted `Episode` row. Returns `nil` when the row no
    /// longer exists (e.g. the episode was deleted between batch
    /// creation and this pass).
    let episodeLookup: @Sendable (_ canonicalEpisodeKey: String) async -> EpisodeProjection?

    /// Resolve a canonical episode key to the most-recent `cause` from
    /// the work-journal table. `nil` means "no terminal cause recorded
    /// yet" (the episode either never queued, or every prior pass
    /// finalized cleanly without a cause column).
    let causeLookup: @Sendable (_ canonicalEpisodeKey: String) async -> InternalMissCause?

    /// Resolve the live `AnalysisEligibility` snapshot. The coordinator
    /// pass evaluates eligibility once per pass and feeds the same
    /// snapshot to every child — that matches how the device's
    /// capability state is observed (it's a per-device, not per-episode,
    /// signal).
    let eligibilityProvider: @Sendable () async -> AnalysisEligibility

    /// Build the per-pass child summaries for the batch's `episodeKeys`.
    func summaries(for episodeKeys: [String]) async -> [BatchChildSurfaceSummary] {
        let eligibility = await eligibilityProvider()
        var summaries: [BatchChildSurfaceSummary] = []
        summaries.reserveCapacity(episodeKeys.count)
        for key in episodeKeys {
            let projection = await episodeLookup(key)
            let cause = await causeLookup(key)
            let summary = Self.makeSummary(
                canonicalEpisodeKey: key,
                episode: projection,
                cause: cause,
                eligibility: eligibility
            )
            summaries.append(summary)
        }
        return summaries
    }

    // MARK: - Pure projection

    /// Pure mapping from (episode projection, cause, eligibility) to
    /// `BatchChildSurfaceSummary`. Public-static (not on the instance)
    /// so tests can exercise the projection without constructing a full
    /// `BatchSummaryBuilder` — the four inputs are everything it needs.
    ///
    /// `episode == nil` means the episode row has been deleted; we
    /// surface a conservative non-ready / non-fixable summary so the
    /// reducer never promotes the missing child to a blocker case.
    static func makeSummary(
        canonicalEpisodeKey: String,
        episode: EpisodeProjection?,
        cause: InternalMissCause?,
        eligibility: AnalysisEligibility
    ) -> BatchChildSurfaceSummary {

        guard let episode else {
            // Episode row missing — treat as "not ready, no fixable
            // blocker". The coordinator's terminal check folds this into
            // the same bucket as `.cancelled` for batch-close purposes.
            return BatchChildSurfaceSummary(
                canonicalEpisodeKey: canonicalEpisodeKey,
                disposition: .cancelled,
                reason: .cancelled,
                analysisUnavailableReason: nil,
                isReady: false,
                userFixable: false
            )
        }

        let isReady = episode.downloaded && episode.analyzed

        // Per-episode reducer is the sole authority over the (disposition,
        // reason, hint, analysisUnavailableReason) tuple. We feed it a
        // minimal `AnalysisState` derived from the SwiftData row plus the
        // per-pass `cause` and per-device `eligibility` and trust its
        // output verbatim.
        let state = analysisState(from: episode)
        let status = episodeSurfaceStatus(
            state: state,
            cause: cause,
            eligibility: eligibility,
            coverage: episode.coverageSummary,
            readinessAnchor: episode.playbackAnchor
        )

        // `userFixable` is computed at the boundary per the
        // BatchNotificationReducer contract: the reducer trusts this
        // boolean rather than re-deriving from (reason, analysisUnavailableReason).
        // The hint already encodes user-fixability for most reasons; the
        // analysisUnavailable case requires the additional gate on the
        // per-device unavailability reason because the reducer's Rule 1
        // emits the same `.enableAppleIntelligence` hint regardless of
        // which gate failed, but only the appleIntelligenceDisabled and
        // languageUnsupported gates are actually user-fixable.
        let userFixable = deriveUserFixable(
            reason: status.reason,
            analysisUnavailableReason: status.analysisUnavailableReason,
            hint: status.hint
        )

        return BatchChildSurfaceSummary(
            canonicalEpisodeKey: canonicalEpisodeKey,
            disposition: status.disposition,
            reason: status.reason,
            analysisUnavailableReason: status.analysisUnavailableReason,
            isReady: isReady,
            userFixable: userFixable
        )
    }

    // MARK: - Episode → AnalysisState mapping

    /// Map an `EpisodeProjection` to the surface-status reducer's
    /// `AnalysisState` input.
    ///
    /// The projection carries `downloaded` + `analyzed` — the minimum
    /// the reducer needs to distinguish "nothing has analyzed yet" from
    /// "analysis is done". Force-quit / user-preempted flags are not
    /// yet plumbed onto Episode (those live in the work-journal row);
    /// the reducer tolerates both flags being `false` (Rule 5 fall-
    /// through to "queued / waitingForTime" when no cause is supplied),
    /// which is the correct minimum-scope behavior.
    static func analysisState(from episode: EpisodeProjection) -> AnalysisState {
        let persisted: AnalysisState.PersistedStatus
        if episode.analyzed {
            persisted = .done
        } else if episode.downloaded {
            persisted = .queued
        } else {
            persisted = .new
        }
        return AnalysisState(
            persistedStatus: persisted,
            hasUserPreemptedJob: false,
            hasAppForceQuitFlag: false,
            pendingSinceEnqueuedAt: nil,
            hasAnyConfirmedAnalysis: episode.analyzed
        )
    }

    // MARK: - userFixable derivation

    /// Derive the `userFixable` flag the BatchNotificationReducer expects.
    ///
    /// Rules:
    ///   * If `reason == .analysisUnavailable`, only treat as user-fixable
    ///     when the per-device `analysisUnavailableReason` is one of the
    ///     two settings-toggle cases (`appleIntelligenceDisabled`,
    ///     `languageUnsupported`). Hardware / region / model-temporary
    ///     are NOT user-fixable.
    ///   * Otherwise, defer to `hint.userFixable`. The hint already
    ///     encodes whether the reducer's selected reason has a CTA the
    ///     user can act on.
    static func deriveUserFixable(
        reason: SurfaceReason,
        analysisUnavailableReason: AnalysisUnavailableReason?,
        hint: ResolutionHint
    ) -> Bool {
        if reason == .analysisUnavailable {
            switch analysisUnavailableReason {
            case .appleIntelligenceDisabled, .languageUnsupported:
                return true
            case .hardwareUnsupported,
                 .regionUnsupported,
                 .modelTemporarilyUnavailable,
                 .none:
                return false
            }
        }
        return hint.userFixable
    }
}
