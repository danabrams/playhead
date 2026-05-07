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
// the fetch off-main, and uses bulk `AnalysisStore` queries plus
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
    /// playhead-btoa.3: per-episode foreground-download fraction map for
    /// the current refresh tick. Production wires
    /// `DownloadManager.progressSnapshot()` (an actor hop returning
    /// `episodeId → fractionCompleted` for in-flight foreground
    /// transfers); tests inject a stub returning whatever map they need.
    /// Episodes absent from the map may still render as 100% if the cached
    /// download provider reports completed media.
    private let downloadProgressProvider: @Sendable () async -> [String: Double]
    /// Fully-cached episode IDs for this refresh. Production wires a
    /// DownloadManager directory scan over the eligible episode IDs; tests
    /// inject a small set.
    private let downloadedEpisodeIdsProvider: @Sendable (Set<String>) async -> Set<String>
    private let modelContainer: ModelContainer

    init(
        store: AnalysisStore,
        capabilitySnapshotProvider: @escaping @Sendable () async -> CapabilitySnapshot?,
        runningEpisodeIdProvider: @escaping @Sendable () async -> String?,
        downloadProgressProvider: @escaping @Sendable () async -> [String: Double],
        downloadedEpisodeIdsProvider: @escaping @Sendable (Set<String>) async -> Set<String> = { _ in [] },
        modelContainer: ModelContainer
    ) {
        self.store = store
        self.capabilitySnapshotProvider = capabilitySnapshotProvider
        self.runningEpisodeIdProvider = runningEpisodeIdProvider
        self.downloadProgressProvider = downloadProgressProvider
        self.downloadedEpisodeIdsProvider = downloadedEpisodeIdsProvider
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
        //
        // skeptical-review-cycle-7 H1: the eligible-id list MUST be an
        // `Array`, not a `Set`. SwiftData's `#Predicate` macro accepts
        // `Set.contains` at compile time but its translation against
        // SQLite is fragile across toolchain revs — depending on the
        // build, it falls back to a per-row in-memory scan, fails to
        // translate (matching nothing — Activity widget appears empty),
        // or crashes. `Array.contains` is the only form that reliably
        // lowers to `IN (?, ?, …)`. PlayheadApp.swift:159-169 documents
        // this constraint at the other call site; that sweep missed
        // this one. Keep it `Array` and do not introduce a `Set` here
        // even if a future reader thinks the membership semantics
        // are nicer — the dictionary keys are already unique and
        // SwiftData's translator does not benefit from `Set`-ness.
        let allAssets: [String: AnalysisAsset]
        do {
            allAssets = try await store.fetchLatestAssetByEpisodeIdMap()
        } catch {
            return []
        }
        if allAssets.isEmpty { return [] }
        let eligibleEpisodeIds = Array(allAssets.keys)

        // playhead-btoa.3: snapshot the download manager's per-episode
        // foreground fraction map once per refresh. The closure is
        // injected so tests can drive the provider with arbitrary
        // states; production wires `DownloadManager.progressSnapshot()`
        // which only contains episodes with an in-flight foreground
        // transfer (size-known, non-zero `totalBytes`). Completed
        // downloads come from the cached-id provider below.
        let downloadFractions = await downloadProgressProvider()
        let downloadedEpisodeIds = await downloadedEpisodeIdsProvider(Set(eligibleEpisodeIds))

        // playhead-hygc.1.2: pull the canonical coverage summary (interval-
        // unioned fast-transcript seconds + high-water `MAX(endTime)` +
        // feature/final-pass/ad watermarks with provenance tags) once, then
        // drive the per-row display from it. Pre-hygc.1.2 the provider
        // SUMmed fast-chunk durations, which (a) double-counted overlapping
        // chunks and (b) under-explained gaps. Pre-hygc.1.2 it also fell
        // through to the asset's fast watermark when a chunk-derived value
        // was absent — a single canonical read model lets every coverage
        // scalar carry its own provenance so the UI never silently shows a
        // stale watermark when richer artifacts already exist.
        let assetIds = Set(allAssets.values.map(\.id))
        let coverageSummariesByAssetId: [String: AnalysisCoverageSummary]
        do {
            coverageSummariesByAssetId = try await store.fetchCoverageSummariesByAssetIds(
                assetIds
            )
        } catch {
            coverageSummariesByAssetId = [:]
        }
        let episodeIdSet = Set(eligibleEpisodeIds)
        let latestSessionsByAssetId = (
            try? await store.fetchLatestSessionByAssetIdMap(assetIds: assetIds)
        ) ?? [:]
        let latestJobsByEpisodeId = (
            try? await store.fetchLatestJobByEpisodeIdMap(episodeIds: episodeIdSet)
        ) ?? [:]
        let latestTerminalWorkJournalByEpisodeId = (
            try? await store.fetchLatestTerminalWorkJournalEntryByEpisodeIdMap(
                episodeIds: episodeIdSet
            )
        ) ?? [:]

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

        // playhead-btoa.3: hoisted out of the per-episode loop so we
        // don't reconstruct the closure on every iteration. Returns
        // `nil` when either the watermark is missing or the duration
        // is non-positive (legacy / placeholder rows pre-decode); a
        // non-nil result is already clamped into `[0, 1]`.
        func fraction(_ watermark: Double?, durationSec: Double) -> Double? {
            guard let watermark, durationSec > 0 else { return nil }
            return min(1.0, max(0.0, watermark / durationSec))
        }

        func maxKnown(_ lhs: Double?, _ rhs: Double?) -> Double? {
            switch (lhs, rhs) {
            case let (lhs?, rhs?):
                return max(lhs, rhs)
            case let (lhs?, nil):
                return lhs
            case let (nil, rhs?):
                return rhs
            case (nil, nil):
                return nil
            }
        }

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

            // playhead-btoa.3 / playhead-hygc.1.2: compute the three
            // pipeline-progress fractions from the canonical
            // ``AnalysisCoverageSummary`` read model. Clamping happens
            // here (in the provider) so the row struct's contract can be
            // "already in `[0, 1]` if non-nil" — the strip view never
            // has to defend against overflow.
            //
            // Transcript / analysis fractions are coverage-seconds /
            // duration ratios. The summary already reconciles "stale
            // watermark vs. real chunks": its `fastTranscriptCoveredSec`
            // is the interval-unioned chunk seconds when chunks landed,
            // and falls back to the asset's `fastTranscriptCoverageEndTime`
            // only when no chunks exist. Analysis uses the broad
            // feature/ad coverage watermark rather than detected-ad
            // existence alone. A missing or non-positive
            // `episodeDurationSec` (legacy rows, placeholder rows
            // pre-decode) collapses both to `nil` rather than
            // synthesising a fake 0% bar from a divide-by-zero.
            let summary = coverageSummariesByAssetId[asset.id]
            let durationSec = summary?.episodeDurationSec ?? asset.episodeDurationSec ?? 0
            let transcriptFraction = fraction(
                summary?.fastTranscriptCoveredSec,
                durationSec: durationSec
            )
            let analysisWatermark = maxKnown(
                summary?.featureCoverageEndSec ?? asset.featureCoverageEndTime,
                summary?.confirmedAdCoverageEndSec ?? asset.confirmedAdCoverageEndTime
            )
            let analysisFraction = fraction(analysisWatermark, durationSec: durationSec)
            // Download fraction comes from the (already-snapshotted)
            // `DownloadManager` live-progress map. Completed cached
            // audio renders as 100%; without that fallback the transfer
            // disappearing from the in-flight progress map made done
            // downloads look unknown (`DL --%`).
            //
            // Clamp live progress to `[0, 1]` defensively — the
            // manager's own arithmetic is bounded by `bytesWritten /
            // totalBytes` but a brief race where `totalBytes` falls
            // back to a smaller value than `bytesWritten` could in
            // principle yield > 1 mid-tick.
            let downloadFraction = downloadFractions[episodeId]
                .map { min(1.0, max(0.0, $0)) }
                ?? (downloadedEpisodeIds.contains(episodeId) ? 1.0 : nil)
            let latestSession = latestSessionsByAssetId[asset.id]
            let latestJob = latestJobsByEpisodeId[episodeId]
            let latestTerminalWorkJournal = latestTerminalWorkJournalByEpisodeId[episodeId]
            let finishedOutcome = activityFinishedOutcome(
                asset: asset,
                latestSession: latestSession,
                latestJob: latestJob,
                latestTerminalWorkJournal: latestTerminalWorkJournal
            )
            let finishedAt: Date?
            if finishedOutcome != nil || isTerminal(status: status) {
                finishedAt = activityFinishedAt(
                    latestSession: latestSession,
                    latestJob: latestJob,
                    latestTerminalWorkJournal: latestTerminalWorkJournal
                )
            } else {
                finishedAt = nil
            }

            inputs.append(
                ActivityEpisodeInput(
                    episodeId: episodeId,
                    episodeTitle: episode.title,
                    podcastTitle: episode.podcast?.title,
                    status: status,
                    isRunning: isRunning,
                    finishedAt: finishedAt,
                    finishedOutcome: finishedOutcome,
                    // playhead-cjqq: forward the persisted user
                    // ordering so the aggregator's Up Next sort
                    // (queuePosition asc, nil-last, episodeId
                    // tiebreak) reflects the user's drag-reorder
                    // history. The aggregator owns the sort comparator
                    // — this provider is purely a forwarder.
                    queuePosition: episode.queuePosition,
                    downloadFraction: downloadFraction,
                    transcriptFraction: transcriptFraction,
                    analysisFraction: analysisFraction
                )
            )
        }

        return inputs
    }

    func loadDogfoodDiagnosticsSnapshot(
        generatedAt: Date = Date(),
        episodeHashProvider: @escaping @Sendable (String) -> String
    ) async -> DogfoodDiagnosticsActivitySnapshot {
        let snapshot = await capabilitySnapshotProvider()
        let eligibility = EpisodeSurfaceStatusObserver.eligibility(from: snapshot)
        let runningEpisodeId = await runningEpisodeIdProvider()

        let storeIsOpen = await store.isOpen
        guard storeIsOpen else {
            return DogfoodDiagnosticsActivitySnapshot(
                generatedAt: generatedAt,
                rows: [],
                captureError: "analysis_store_unopened"
            )
        }

        let allAssets: [String: AnalysisAsset]
        do {
            allAssets = try await store.fetchLatestAssetByEpisodeIdMap()
        } catch {
            return DogfoodDiagnosticsActivitySnapshot(
                generatedAt: generatedAt,
                rows: [],
                captureError: "fetch_assets_failed: \(error)"
            )
        }
        if allAssets.isEmpty {
            return DogfoodDiagnosticsActivitySnapshot(generatedAt: generatedAt, rows: [])
        }
        let eligibleEpisodeIds = Array(allAssets.keys)

        let downloadFractions = await downloadProgressProvider()
        let downloadedEpisodeIds = await downloadedEpisodeIdsProvider(Set(eligibleEpisodeIds))

        // playhead-hygc.1.2: same canonical read model the UI rows
        // consume. The dogfood snapshot is the diagnostic surface where
        // provenance tags matter most — Activity already shows the
        // reconciled value, but operators inspecting a stuck device need
        // to know whether a 39% transcript bar came from real chunk data
        // or a stale watermark.
        let assetIds = Set(allAssets.values.map(\.id))
        let coverageSummariesByAssetId: [String: AnalysisCoverageSummary]
        do {
            coverageSummariesByAssetId = try await store.fetchCoverageSummariesByAssetIds(
                assetIds
            )
        } catch {
            coverageSummariesByAssetId = [:]
        }
        let episodeIdSet = Set(eligibleEpisodeIds)
        let latestSessionsByAssetId = (
            try? await store.fetchLatestSessionByAssetIdMap(assetIds: assetIds)
        ) ?? [:]
        let latestJobsByEpisodeId = (
            try? await store.fetchLatestJobByEpisodeIdMap(episodeIds: episodeIdSet)
        ) ?? [:]
        let latestTerminalWorkJournalByEpisodeId = (
            try? await store.fetchLatestTerminalWorkJournalEntryByEpisodeIdMap(
                episodeIds: episodeIdSet
            )
        ) ?? [:]

        let context = ModelContext(modelContainer)
        var descriptor = FetchDescriptor<Episode>(
            predicate: #Predicate { eligibleEpisodeIds.contains($0.canonicalEpisodeKey) }
        )
        descriptor.relationshipKeyPathsForPrefetching = [\Episode.podcast]
        let episodes: [Episode]
        do {
            episodes = try context.fetch(descriptor)
        } catch {
            return DogfoodDiagnosticsActivitySnapshot(
                generatedAt: generatedAt,
                rows: [],
                captureError: "fetch_episodes_failed: \(error)"
            )
        }

        var rows: [DogfoodDiagnosticsActivityRow] = []
        rows.reserveCapacity(episodes.count)

        for episode in episodes {
            let episodeId = episode.canonicalEpisodeKey
            guard let asset = allAssets[episodeId] else { continue }

            let analysisState = EpisodeSurfaceStatusObserver.analysisState(from: asset)
            let status = episodeSurfaceStatus(
                state: analysisState,
                cause: nil,
                eligibility: eligibility,
                coverage: episode.coverageSummary,
                readinessAnchor: episode.playbackAnchor
            )

            let isRunning = (episodeId == runningEpisodeId)
            // playhead-hygc.1.2: every coverage scalar comes from the
            // canonical summary. We fall back to the asset row only for
            // the very rare case where the summary lookup itself failed
            // (the catch above produced an empty dictionary) — and even
            // then the provenance tag is `unknown` so downstream
            // consumers never confuse a fallback for an authoritative
            // source. `fastTranscriptCoverageEndSec` is the high-water
            // `MAX(endTime)` reported alongside the unioned coverage
            // seconds; we surface it as the dogfood
            // `fast_transcript_watermark_sec` so the diagnostics still
            // expose "how far the runner reached" independently of how
            // much covered audio actually exists.
            let summary = coverageSummariesByAssetId[asset.id]
            let durationSec = summary?.episodeDurationSec ?? asset.episodeDurationSec ?? 0
            let transcriptCoveredSec = summary?.fastTranscriptCoveredSec
            let transcriptFraction = fraction(transcriptCoveredSec, durationSec: durationSec)
            let featureCoverageEndSec = summary?.featureCoverageEndSec ?? asset.featureCoverageEndTime
            let confirmedAdCoverageEndSec = summary?.confirmedAdCoverageEndSec ?? asset.confirmedAdCoverageEndTime
            let analysisWatermark = maxKnown(featureCoverageEndSec, confirmedAdCoverageEndSec)
            let analysisFraction = fraction(analysisWatermark, durationSec: durationSec)
            let fastTranscriptWatermarkSec = summary?.fastTranscriptCoverageEndSec ?? asset.fastTranscriptCoverageEndTime
            let finalPassCoverageEndSec = summary?.finalPassCoverageEndSec ?? asset.finalPassCoverageEndTime
            let liveDownloadFraction = downloadFractions[episodeId].map(clampFraction)
            let cachedAudioPresent = downloadedEpisodeIds.contains(episodeId)
            let downloadFraction = liveDownloadFraction ?? (cachedAudioPresent ? 1.0 : nil)
            let downloadSource = dogfoodDownloadSource(
                liveDownloadFraction: liveDownloadFraction,
                cachedAudioPresent: cachedAudioPresent
            )
            let transcriptSource = dogfoodTranscriptSource(summary: summary)
            let analysisSource = dogfoodAnalysisSource(summary: summary)

            let session = latestSessionsByAssetId[asset.id]
            let job = latestJobsByEpisodeId[episodeId]
            let terminalWorkJournal = latestTerminalWorkJournalByEpisodeId[episodeId]
            let finishedOutcome = activityFinishedOutcome(
                asset: asset,
                latestSession: session,
                latestJob: job,
                latestTerminalWorkJournal: terminalWorkJournal
            )

            rows.append(
                DogfoodDiagnosticsActivityRow(
                    episodeIdHash: episodeHashProvider(episodeId),
                    section: dogfoodActivitySection(
                        status: status,
                        isRunning: isRunning,
                        finishedOutcome: finishedOutcome
                    ),
                    status: statusSnapshot(status),
                    isRunning: isRunning,
                    finishedOutcome: finishedOutcome.map(dogfoodFinishedOutcomeName),
                    queuePosition: episode.queuePosition,
                    cachedAudioPresent: cachedAudioPresent,
                    liveDownloadFraction: liveDownloadFraction,
                    pipeline: DogfoodDiagnosticsPipelineSnapshot(
                        downloadFraction: downloadFraction,
                        downloadPercent: formatPercent(downloadFraction),
                        downloadSource: downloadSource,
                        transcriptFraction: transcriptFraction,
                        transcriptPercent: formatPercent(transcriptFraction),
                        transcriptSource: transcriptSource,
                        analysisFraction: analysisFraction,
                        analysisPercent: formatPercent(analysisFraction),
                        analysisSource: analysisSource,
                        episodeDurationSec: summary?.episodeDurationSec ?? asset.episodeDurationSec,
                        transcriptCoveredSec: transcriptCoveredSec,
                        transcriptWatermarkSec: transcriptCoveredSec,
                        fastTranscriptWatermarkSec: fastTranscriptWatermarkSec,
                        analysisWatermarkSec: analysisWatermark,
                        featureCoverageEndSec: featureCoverageEndSec,
                        confirmedAdCoverageEndSec: confirmedAdCoverageEndSec,
                        finalPassCoverageEndSec: finalPassCoverageEndSec,
                        fastTranscriptCoverageEndSource: dogfoodFastTranscriptEndSource(summary: summary),
                        finalPassCoverageEndSource: dogfoodFinalPassEndSource(summary: summary)
                    ),
                    analysisAsset: analysisAssetSnapshot(asset),
                    latestSession: session.map(analysisSessionSnapshot),
                    latestJob: job.map(analysisJobSnapshot),
                    latestTerminalWorkJournal: terminalWorkJournal.map(workJournalSnapshot)
                )
            )
        }

        return DogfoodDiagnosticsActivitySnapshot(generatedAt: generatedAt, rows: rows)
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

    private func dogfoodActivitySection(
        status: EpisodeSurfaceStatus,
        isRunning: Bool,
        finishedOutcome: ActivityFinishedOutcome?
    ) -> String {
        if finishedOutcome != nil || isTerminal(status: status) {
            return "recently_finished"
        }
        switch status.disposition {
        case .paused:
            return "paused"
        case .queued:
            return isRunning ? "now" : "up_next"
        case .failed, .cancelled, .unavailable:
            return "hidden_terminal"
        }
    }

    private func activityFinishedOutcome(
        asset: AnalysisAsset,
        latestSession: AnalysisSession?,
        latestJob: AnalysisJob?,
        latestTerminalWorkJournal: WorkJournalEntry?
    ) -> ActivityFinishedOutcome? {
        if let outcome = activityFinishedOutcome(sessionStateRaw: asset.analysisState) {
            return outcome
        }
        if let session = latestSession,
           let outcome = activityFinishedOutcome(sessionStateRaw: session.state) {
            return outcome
        }
        if let latestJob,
           latestJob.analysisAssetId == nil || latestJob.analysisAssetId == asset.id {
            if latestJob.state == "complete" {
                return .success
            }
            if latestJob.state == "superseded",
               let latestTerminalWorkJournal {
                switch latestTerminalWorkJournal.eventType {
                case .finalized:
                    return .success
                case .failed, .preempted:
                    return .couldntAnalyze
                case .acquired, .checkpointed:
                    return nil
                }
            }
        }
        return nil
    }

    private func activityFinishedOutcome(
        sessionStateRaw: String
    ) -> ActivityFinishedOutcome? {
        guard let state = SessionState(rawValue: sessionStateRaw) else { return nil }
        if state.isTerminalCompletion {
            return .success
        }
        if state.isTerminalFailure {
            return .couldntAnalyze
        }
        return nil
    }

    private func activityFinishedAt(
        latestSession: AnalysisSession?,
        latestJob: AnalysisJob?,
        latestTerminalWorkJournal: WorkJournalEntry?
    ) -> Date {
        let timestamp = [
            latestSession?.updatedAt,
            latestJob?.updatedAt,
            latestTerminalWorkJournal?.timestamp
        ]
        .compactMap { value -> Double? in
            guard let value, value.isFinite, value > 0 else { return nil }
            return value
        }
        .max()

        if let timestamp {
            return Date(timeIntervalSince1970: timestamp)
        }
        return Date()
    }

    private func dogfoodFinishedOutcomeName(
        _ outcome: ActivityFinishedOutcome
    ) -> String {
        switch outcome {
        case .success:
            return "success"
        case .couldntAnalyze:
            return "couldnt_analyze"
        case .analysisUnavailable(let reason):
            return "analysis_unavailable:\(reason.rawValue)"
        }
    }

    private func fraction(_ watermark: Double?, durationSec: Double) -> Double? {
        guard let watermark, durationSec > 0 else { return nil }
        return clampFraction(watermark / durationSec)
    }

    private func clampFraction(_ fraction: Double) -> Double {
        min(1.0, max(0.0, fraction))
    }

    private func maxKnown(_ lhs: Double?, _ rhs: Double?) -> Double? {
        switch (lhs, rhs) {
        case let (lhs?, rhs?):
            return max(lhs, rhs)
        case let (lhs?, nil):
            return lhs
        case let (nil, rhs?):
            return rhs
        case (nil, nil):
            return nil
        }
    }

    private func formatPercent(_ fraction: Double?) -> String {
        guard let fraction else { return "--%" }
        let clamped = clampFraction(fraction)
        return "\(Int((clamped * 100).rounded()))%"
    }

    private func dogfoodDownloadSource(
        liveDownloadFraction: Double?,
        cachedAudioPresent: Bool
    ) -> String {
        if liveDownloadFraction != nil {
            return "live_progress"
        }
        if cachedAudioPresent {
            return "cached_audio"
        }
        return "unknown"
    }

    /// playhead-hygc.1.2: derive the dogfood `transcript_source` wire
    /// string from the canonical ``AnalysisCoverageSummary``. Wire token
    /// vocabulary matches the bead's allowlist
    /// (`fast_transcript_chunks`, `asset_watermark`, `unknown`) so the
    /// `transcript_source` field and the `fast_transcript_coverage_end_source`
    /// field never disagree on the name of the same provenance.
    private func dogfoodTranscriptSource(summary: AnalysisCoverageSummary?) -> String {
        guard let summary else { return "unknown" }
        switch summary.fastTranscriptCoveredSource {
        case .fastTranscriptChunks:
            return "fast_transcript_chunks"
        case .assetWatermark:
            return "asset_watermark"
        case .unknown:
            return "unknown"
        case .finalPassChunks, .adWindows, .cachedAudio:
            // Not currently produced for `fastTranscriptCoveredSource`,
            // but keep the switch exhaustive without crashing if a
            // future pipeline change starts emitting one. The dogfood
            // wire format treats unfamiliar provenance the same as
            // unknown so downstream consumers stay forward-compatible.
            return "unknown"
        }
    }

    /// playhead-hygc.1.2: derive the dogfood `analysis_source` wire
    /// string. The historical wire enum was
    /// `feature_coverage | confirmed_ad_coverage | unknown`; we now also
    /// surface `final_pass_chunks` when the final-pass watermark exceeds
    /// both feature and confirmed-ad coverage (a real dogfood signal —
    /// final-pass re-transcribes ad-window ranges and can land coverage
    /// the feature window never reached).
    private func dogfoodAnalysisSource(summary: AnalysisCoverageSummary?) -> String {
        guard let summary else { return "unknown" }
        let feature = summary.featureCoverageEndSec
        let confirmed = summary.confirmedAdCoverageEndSec
        let finalPass = summary.finalPassCoverageEndSec
        if let finalPass,
           summary.finalPassCoverageEndSource == .finalPassChunks,
           finalPass > max(feature ?? 0, confirmed ?? 0) {
            return "final_pass_chunks"
        }
        switch (feature, confirmed) {
        case let (feature?, confirmed?):
            return confirmed >= feature ? "confirmed_ad_coverage" : "feature_coverage"
        case (_?, nil):
            return "feature_coverage"
        case (nil, _?):
            return "confirmed_ad_coverage"
        case (nil, nil):
            return "unknown"
        }
    }

    /// playhead-hygc.1.2: provenance string for the high-water
    /// `fast_transcript_watermark_sec` field surfaced in the dogfood
    /// snapshot. Distinguishes "we got this from chunk MAX(endTime)"
    /// from "we got this from the stale asset watermark", which is the
    /// load-bearing diagnostic when an asset is wedged.
    private func dogfoodFastTranscriptEndSource(summary: AnalysisCoverageSummary?) -> String {
        provenanceWireString(summary?.fastTranscriptCoverageEndSource)
    }

    /// playhead-hygc.1.2: same shape as
    /// ``dogfoodFastTranscriptEndSource(summary:)`` but for final-pass
    /// coverage. Acceptance criterion (g): final-pass coverage must
    /// appear in dogfood provenance.
    private func dogfoodFinalPassEndSource(summary: AnalysisCoverageSummary?) -> String {
        provenanceWireString(summary?.finalPassCoverageEndSource)
    }

    private func provenanceWireString(
        _ provenance: AnalysisCoverageSummary.CoverageProvenance?
    ) -> String {
        provenance?.rawValue ?? "unknown"
    }

    private func statusSnapshot(
        _ status: EpisodeSurfaceStatus
    ) -> DogfoodDiagnosticsStatusSnapshot {
        DogfoodDiagnosticsStatusSnapshot(
            disposition: status.disposition.rawValue,
            reason: status.reason.rawValue,
            hint: status.hint.rawValue,
            analysisUnavailableReason: status.analysisUnavailableReason?.rawValue,
            playbackReadiness: status.playbackReadiness.rawValue,
            readinessAnchor: status.readinessAnchor
        )
    }

    private func analysisAssetSnapshot(
        _ asset: AnalysisAsset
    ) -> DogfoodDiagnosticsAnalysisAssetSnapshot {
        DogfoodDiagnosticsAnalysisAssetSnapshot(
            analysisState: asset.analysisState,
            analysisVersion: asset.analysisVersion,
            artifactClass: asset.artifactClass.rawValue,
            terminalReason: Self.sanitizedDiagnosticString(asset.terminalReason),
            capabilitySnapshotPresent: asset.capabilitySnapshot != nil
        )
    }

    private func analysisSessionSnapshot(
        _ session: AnalysisSession
    ) -> DogfoodDiagnosticsAnalysisSessionSnapshot {
        DogfoodDiagnosticsAnalysisSessionSnapshot(
            state: session.state,
            startedAt: session.startedAt,
            updatedAt: session.updatedAt,
            failureReason: Self.sanitizedDiagnosticString(session.failureReason),
            needsShadowRetry: session.needsShadowRetry
        )
    }

    private func analysisJobSnapshot(
        _ job: AnalysisJob
    ) -> DogfoodDiagnosticsAnalysisJobSnapshot {
        DogfoodDiagnosticsAnalysisJobSnapshot(
            jobType: job.jobType,
            state: job.state,
            priority: job.priority,
            desiredCoverageSec: job.desiredCoverageSec,
            featureCoverageSec: job.featureCoverageSec,
            transcriptCoverageSec: job.transcriptCoverageSec,
            cueCoverageSec: job.cueCoverageSec,
            attemptCount: job.attemptCount,
            nextEligibleAt: job.nextEligibleAt,
            leasePresent: job.leaseOwner != nil,
            leaseExpiresAt: job.leaseExpiresAt,
            lastErrorCode: Self.sanitizedDiagnosticString(job.lastErrorCode),
            createdAt: job.createdAt,
            updatedAt: job.updatedAt,
            generationID: job.generationID,
            schedulerEpoch: job.schedulerEpoch,
            artifactClass: job.artifactClass.rawValue,
            estimatedWriteBytes: job.estimatedWriteBytes
        )
    }

    /// playhead-9ro7 cycle-3: scrub PII shapes and bound length on
    /// free-form diagnostic strings before they enter a dogfood
    /// diagnostics export. Producers (`AnalysisCoordinator`,
    /// `AnalysisWorkScheduler`) write `String(describing: error)` and
    /// `error.localizedDescription` directly into `terminalReason`,
    /// `failureReason`, and `lastErrorCode`; on iOS those strings can
    /// embed user-home paths (`/Users/...`, `/var/mobile/...`),
    /// container paths (`/private/var/mobile/Containers/...`), and
    /// remote URLs from `NSURLError.userInfo` — all of which would
    /// land in the JSON the user manually shares for support. The
    /// underlying SQLite columns retain the original strings (those
    /// stay on-device per the Playhead legal mandate); only the
    /// export layer scrubs them.
    private static func sanitizedDiagnosticString(_ raw: String?) -> String? {
        guard let raw, !raw.isEmpty else { return raw }
        let stripPatterns: [String] = [
            #"file://[^\s]*"#,
            #"https?://[^\s]*"#,
            #"/Users/[^\s]*"#,
            #"/var/mobile/[^\s]*"#,
            #"/private/var/[^\s]*"#
        ]
        var s = raw
        for pattern in stripPatterns {
            s = s.replacingOccurrences(
                of: pattern,
                with: "[redacted]",
                options: .regularExpression
            )
        }
        if s.count > 200 {
            s = String(s.prefix(200)) + "…"
        }
        return s
    }

    private func workJournalSnapshot(
        _ entry: WorkJournalEntry
    ) -> DogfoodDiagnosticsWorkJournalSnapshot {
        DogfoodDiagnosticsWorkJournalSnapshot(
            eventType: entry.eventType.rawValue,
            cause: entry.cause?.rawValue,
            timestamp: entry.timestamp,
            generationID: entry.generationID.uuidString,
            schedulerEpoch: entry.schedulerEpoch,
            artifactClass: entry.artifactClass.rawValue
        )
    }
}
