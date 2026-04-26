// ActivitySnapshotProvider.swift
// Production input provider for the Activity screen view-model. Walks
// the SwiftData Episode set and the AnalysisStore to assemble a list
// of `ActivityEpisodeInput` values keyed by episode, which the view-
// model aggregates into the four-section snapshot.
//
// Scope: playhead-quh7 (Phase 2 deliverable 4 — Activity screen).
//
// playhead-hkn1: previously the body ran wholly on the main actor —
// `ActivityView.refresh()` invokes a `@MainActor` closure that
// awaited a `@MainActor`-isolated `loadInputs()`, so the SwiftData
// fetch and the per-episode `AnalysisStore` round-trips all
// serialized on main. With Dan's library that froze the UI for
// seconds.
//
// Post-hkn1 the provider is non-isolated: `loadInputs()` constructs
// its own `ModelContext` from the injected `ModelContainer`, runs
// the fetch off-main, and uses a single bulk `AnalysisStore` query
// (`fetchAssetsByEpisodeIds`) plus
// `relationshipKeyPathsForPrefetching` to eliminate the N+1.
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
///
/// playhead-hkn1: protocol is no longer `@MainActor`-isolated so the
/// concrete implementation can run off-main. The view-side closure
/// in `ContentView` is still `@MainActor`, which is fine — awaiting
/// a non-isolated `async` method from a `@MainActor` context simply
/// hops off the main actor for the duration of the call and back at
/// the suspension point.
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
///   - The SwiftData `ModelContainer` for episode titles + podcast names.
///
/// playhead-hkn1: the provider takes a `ModelContainer` (Sendable)
/// rather than a `ModelContext` (not Sendable). Each `loadInputs()`
/// call constructs a fresh `ModelContext(container)` for its off-main
/// fetch — SwiftData's contract is that a `ModelContext` is bound to
/// whichever isolation domain creates it, so we cannot reuse the
/// view's main-actor context here.
///
/// Per-episode reduction routes through the canonical
/// `episodeSurfaceStatus(...)` reducer so this provider never duplicates
/// the precedence-ladder logic.
final class LiveActivitySnapshotProvider: ActivitySnapshotProviding {

    private let store: AnalysisStore
    private let capabilitySnapshotProvider: @Sendable () async -> CapabilitySnapshot?
    private let runningEpisodeIdProvider: @Sendable () async -> String?
    private let modelContainer: ModelContainer

    init(
        store: AnalysisStore,
        capabilitySnapshotProvider: @escaping @Sendable () async -> CapabilitySnapshot?,
        runningEpisodeIdProvider: @escaping @Sendable () async -> String?,
        modelContainer: ModelContainer
    ) {
        self.store = store
        self.capabilitySnapshotProvider = capabilitySnapshotProvider
        self.runningEpisodeIdProvider = runningEpisodeIdProvider
        self.modelContainer = modelContainer
    }

    func loadInputs() async -> [ActivityEpisodeInput] {
        // Pull the eligibility + running-episode signals first so the
        // per-episode loop below uses a consistent snapshot.
        let snapshot = await capabilitySnapshotProvider()
        let eligibility = EpisodeSurfaceStatusObserver.eligibility(from: snapshot)
        let runningEpisodeId = await runningEpisodeIdProvider()

        // playhead-6boz: AnalysisStore is now lazily-opened — the
        // first call to a public method blocks while open + DDL
        // complete. The Activity screen is reached during a
        // launch-window race against `PlayheadRuntime`'s deferred
        // `analysisStore.migrate()` Task. Refusing to drive that
        // first-open from the UI path means the activity surface
        // gracefully shows the empty state instead of stalling on the
        // SQLite handshake — exactly the freeze pattern hkn1 took out
        // of the loadInputs hot path. Once the deferred warmup
        // completes a subsequent UI refresh observes `isOpen == true`
        // and proceeds normally.
        let storeIsOpen = await store.isOpen
        guard storeIsOpen else { return [] }

        // playhead-hkn1: bulk-fetch every relevant analysis asset in a
        // single SQL round-trip. The pre-hkn1 path issued one
        // `fetchAssetByEpisodeId` per Episode row on the main actor;
        // for libraries with 50-200 episodes that compounded into a
        // multi-second freeze on the Activity screen.
        //
        // We pull the asset map FIRST (before the SwiftData fetch) so
        // the predicate below can filter Episodes to the set that
        // actually has an asset — the same set the post-loop guard
        // (`guard let asset else { continue }`) used to filter on the
        // hot path. Filtering at the descriptor level means SwiftData
        // does not have to materialize Episode rows that will be
        // immediately discarded.
        let allAssets: [String: AnalysisAsset]
        do {
            allAssets = try await store.fetchLatestAssetByEpisodeIdMap()
        } catch {
            return []
        }
        if allAssets.isEmpty { return [] }
        let eligibleEpisodeIds = Set(allAssets.keys)

        // playhead-hkn1: SwiftData fetch on a freshly-constructed
        // `ModelContext` so this work runs off the main actor. The
        // `relationshipKeyPathsForPrefetching = [\.podcast]` directive
        // materializes the related Podcast row in the same fetch —
        // before this, every `episode.podcast?.title` access in the
        // loop below was a lazy SwiftData round-trip on main.
        //
        // Non-`Sendable` types (`ModelContext`, `Episode`) stay
        // confined to this single async function, so we never violate
        // the per-context-per-isolation contract.
        let context = ModelContext(modelContainer)
        var descriptor = FetchDescriptor<Episode>(
            predicate: #Predicate { eligibleEpisodeIds.contains($0.canonicalEpisodeKey) }
        )
        descriptor.relationshipKeyPathsForPrefetching = [\Episode.podcast]
        let episodes: [Episode]
        do {
            episodes = try context.fetch(descriptor)
        } catch {
            return []
        }

        var inputs: [ActivityEpisodeInput] = []
        inputs.reserveCapacity(episodes.count)

        for episode in episodes {
            let episodeId = episode.canonicalEpisodeKey

            // Episode rows that have never been queued for analysis are
            // not interesting to the Activity screen. The descriptor
            // predicate above already filters to the asset-bearing
            // subset; the dictionary lookup below is the
            // belt-and-braces guard for the rare race where an asset
            // is deleted between the bulk-fetch and the SwiftData
            // fetch.
            guard let asset = allAssets[episodeId] else { continue }

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
                    finishedAt: finishedAt,
                    // playhead-cjqq: forward the persisted user
                    // ordering so the aggregator's Up Next sort
                    // (queuePosition asc, nil-last, episodeId
                    // tiebreak) reflects the user's drag-reorder
                    // history. The aggregator owns the sort comparator
                    // — this provider is purely a forwarder.
                    queuePosition: episode.queuePosition
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

