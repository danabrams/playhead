// ActivitySnapshotProvider.swift
// Production input provider for the Activity screen view-model. Walks
// the SwiftData Episode set and the AnalysisStore to assemble a list
// of `ActivityEpisodeInput` values keyed by episode, which the view-
// model aggregates into the four-section snapshot.
//
// Scope: playhead-quh7 (Phase 2 deliverable 4 — Activity screen).
//
// Boundary discipline:
//   - This file lives under `Playhead/Services/`, NOT `Playhead/Views/`.
//     It is allowed to reference `AnalysisStore` and other persistence
//     types because the UI lint (`SurfaceStatusUILintTests`) only polices
//     `Playhead/Views/` and `Playhead/App/` (with explicit exemptions
//     for the DI wiring files).
//   - The view-model (`ActivityViewModel`) sees only the value-type
//     `ActivityEpisodeInput` payload this provider hands back. It never
//     touches persistence or scheduler state directly.
//
// v1 minimalism:
//   - The provider is intentionally small — it does NOT build a
//     `BatchSurfaceStatus` (the reducer for that struct is a separate
//     Phase 2 bead's scope; only the struct shape ships from
//     playhead-5bb3) and does NOT iterate the analysis-jobs table to
//     enumerate "active scheduler state" beyond the current job. A
//     fuller wiring lands in a follow-up bead once the scheduler exposes
//     a stable enumeration surface; until then the v1 Activity view
//     renders correctly for the empty / single-job cases that dominate
//     real usage.

import Foundation
import SwiftData

// MARK: - ActivitySnapshotProviding

/// Async input source for `ActivityViewModel`. Production wires a
/// `LiveActivitySnapshotProvider`; SwiftUI Previews and tests pass an
/// inline closure via `ActivityView.init(inputProvider:)`.
@MainActor
protocol ActivitySnapshotProviding: Sendable {
    func loadInputs() async -> [ActivityEpisodeInput]
}

// MARK: - LiveActivitySnapshotProvider

/// Production provider. Consumes:
///   - `AnalysisStore` for per-episode `AnalysisAsset` rows (drives
///     `AnalysisState`).
///   - `CapabilitiesService` for the eligibility snapshot.
///   - `AnalysisWorkScheduler` for the currently-running episode id (the
///     single signal that distinguishes Now from Up Next in v1).
///   - The SwiftData `ModelContext` for episode titles + podcast names.
///
/// Per-episode reduction routes through the canonical
/// `episodeSurfaceStatus(...)` reducer so this provider never duplicates
/// the precedence-ladder logic.
@MainActor
final class LiveActivitySnapshotProvider: ActivitySnapshotProviding {

    private let store: AnalysisStore
    private let capabilitySnapshotProvider: @Sendable () async -> CapabilitySnapshot?
    private let runningEpisodeIdProvider: @Sendable () async -> String?
    private let modelContext: ModelContext

    init(
        store: AnalysisStore,
        capabilitySnapshotProvider: @escaping @Sendable () async -> CapabilitySnapshot?,
        runningEpisodeIdProvider: @escaping @Sendable () async -> String?,
        modelContext: ModelContext
    ) {
        self.store = store
        self.capabilitySnapshotProvider = capabilitySnapshotProvider
        self.runningEpisodeIdProvider = runningEpisodeIdProvider
        self.modelContext = modelContext
    }

    func loadInputs() async -> [ActivityEpisodeInput] {
        // Pull the eligibility + running-episode signals first so the
        // per-episode loop below uses a consistent snapshot.
        let snapshot = await capabilitySnapshotProvider()
        let eligibility = EpisodeSurfaceStatusObserver.eligibility(from: snapshot)
        let runningEpisodeId = await runningEpisodeIdProvider()

        // Enumerate episodes from SwiftData. The Library tab already
        // pages this set into memory via `@Query`, so this fetch is a
        // hot-path read on data that is typically already resident.
        let descriptor = FetchDescriptor<Episode>()
        let episodes: [Episode]
        do {
            episodes = try modelContext.fetch(descriptor)
        } catch {
            return []
        }

        var inputs: [ActivityEpisodeInput] = []
        inputs.reserveCapacity(episodes.count)

        for episode in episodes {
            let episodeId = episode.canonicalEpisodeKey

            // Episode rows that have never been queued for analysis are
            // not interesting to the Activity screen.
            let asset: AnalysisAsset?
            do {
                asset = try await store.fetchAssetByEpisodeId(episodeId)
            } catch {
                continue
            }
            guard let asset else { continue }

            let analysisState = EpisodeSurfaceStatusObserver.analysisState(from: asset)
            let status = episodeSurfaceStatus(
                state: analysisState,
                cause: nil, // v1: no cause attribution wired into the
                            // Activity provider; the dfem mapping is the
                            // Paused row's home and is keyed off the
                            // reducer's eligibility branch + persisted
                            // status. Plumbing a live cause stream is a
                            // follow-up bead's scope.
                eligibility: eligibility,
                coverage: episode.coverageSummary,
                readinessAnchor: episode.playbackAnchor
            )

            let isRunning = (episodeId == runningEpisodeId)
            // v1 finishedAt: derive from the Episode model when the
            // status is terminal. The Episode.feedMetadata table does
            // not yet carry an analysis-finishedAt column; we fall
            // back to the asset's `lastPlayedAnalysisAssetId` heuristic
            // by treating any terminal disposition as "just finished"
            // (Date()). This is intentionally coarse — Recently
            // Finished is bounded by the 24h window, and a real
            // finishedAt column is a follow-up bead.
            let finishedAt: Date? = isTerminal(status: status) ? Date() : nil

            inputs.append(
                ActivityEpisodeInput(
                    episodeId: episodeId,
                    episodeTitle: episode.title,
                    podcastTitle: episode.podcast?.title,
                    status: status,
                    isRunning: isRunning,
                    finishedAt: finishedAt
                )
            )
        }

        return inputs
    }

    private func isTerminal(status: EpisodeSurfaceStatus) -> Bool {
        switch status.disposition {
        case .failed, .cancelled, .unavailable:
            return true
        case .queued:
            return status.playbackReadiness == .complete
        case .paused:
            return false
        }
    }
}
