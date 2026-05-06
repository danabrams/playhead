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

        // playhead-3bv.2: the persisted fast-transcript watermark is a
        // scheduler high-water mark, not a reliable display summary of
        // actual transcript rows. Dogfood databases can have stale
        // `fastTranscriptCoverageEndTime` values even though
        // `transcript_chunks` covers nearly the whole episode. Aggregate
        // real fast-pass chunk seconds once and prefer that value when
        // present; fall back to the asset watermark for legacy rows or
        // rows whose chunks have not landed yet.
        let transcriptCoveredSecondsByAssetId: [String: Double]
        do {
            transcriptCoveredSecondsByAssetId = try await store.fetchFastTranscriptCoveredSecondsByAssetIds(
                Set(allAssets.values.map(\.id))
            )
        } catch {
            transcriptCoveredSecondsByAssetId = [:]
        }

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
            // v1 finishedAt: derive from the Episode model when the
            // status is terminal. The Episode.feedMetadata table does
            // not yet carry an analysis-finishedAt column; we fall
            // back to the asset's `lastPlayedAnalysisAssetId` heuristic
            // by treating any terminal disposition as "just finished"
            // (Date()). This is intentionally coarse — Recently
            // Finished is bounded by the 24h window, and a real
            // finishedAt column is a follow-up bead.
            let finishedAt: Date? = isTerminal(status: status) ? Date() : nil

            // playhead-btoa.3: compute the three pipeline-progress
            // fractions for the row payload. Clamping happens here (in
            // the provider) so the row struct's contract can be
            // "already in `[0, 1]` if non-nil" — the strip view never
            // has to defend against overflow.
            //
            // Transcript / analysis fractions are coverage-seconds /
            // duration ratios. Transcript prefers actual fast-pass
            // chunk seconds, then falls back to the asset watermark;
            // analysis uses the broad feature/ad coverage watermark
            // rather than detected-ad existence alone. A missing or
            // non-positive `episodeDurationSec` (legacy rows,
            // placeholder rows pre-decode) collapses both to `nil`
            // rather than synthesising a fake 0% bar from a
            // divide-by-zero.
            let durationSec = asset.episodeDurationSec ?? 0
            let transcriptWatermark = transcriptCoveredSecondsByAssetId[asset.id]
                ?? asset.fastTranscriptCoverageEndTime
            let transcriptFraction = fraction(transcriptWatermark, durationSec: durationSec)
            let analysisWatermark = maxKnown(
                asset.featureCoverageEndTime,
                asset.confirmedAdCoverageEndTime
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

        let transcriptCoveredSecondsByAssetId: [String: Double]
        do {
            transcriptCoveredSecondsByAssetId = try await store.fetchFastTranscriptCoveredSecondsByAssetIds(
                Set(allAssets.values.map(\.id))
            )
        } catch {
            transcriptCoveredSecondsByAssetId = [:]
        }

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
            let durationSec = asset.episodeDurationSec ?? 0
            let transcriptCoveredSec = transcriptCoveredSecondsByAssetId[asset.id]
            let transcriptWatermark = transcriptCoveredSec ?? asset.fastTranscriptCoverageEndTime
            let transcriptFraction = fraction(transcriptWatermark, durationSec: durationSec)
            let analysisWatermark = maxKnown(
                asset.featureCoverageEndTime,
                asset.confirmedAdCoverageEndTime
            )
            let analysisFraction = fraction(analysisWatermark, durationSec: durationSec)
            let liveDownloadFraction = downloadFractions[episodeId].map(clampFraction)
            let cachedAudioPresent = downloadedEpisodeIds.contains(episodeId)
            let downloadFraction = liveDownloadFraction ?? (cachedAudioPresent ? 1.0 : nil)
            let downloadSource = dogfoodDownloadSource(
                liveDownloadFraction: liveDownloadFraction,
                cachedAudioPresent: cachedAudioPresent
            )

            let session = try? await store.fetchLatestSessionForAsset(assetId: asset.id)
            let job = try? await store.fetchLatestJobForEpisode(episodeId)
            let terminalWorkJournal = try? await store.fetchLatestTerminalWorkJournalEntry(
                episodeId: episodeId
            )

            rows.append(
                DogfoodDiagnosticsActivityRow(
                    episodeIdHash: episodeHashProvider(episodeId),
                    section: dogfoodActivitySection(status: status, isRunning: isRunning),
                    status: statusSnapshot(status),
                    isRunning: isRunning,
                    queuePosition: episode.queuePosition,
                    cachedAudioPresent: cachedAudioPresent,
                    liveDownloadFraction: liveDownloadFraction,
                    pipeline: DogfoodDiagnosticsPipelineSnapshot(
                        downloadFraction: downloadFraction,
                        downloadPercent: formatPercent(downloadFraction),
                        downloadSource: downloadSource,
                        transcriptFraction: transcriptFraction,
                        transcriptPercent: formatPercent(transcriptFraction),
                        transcriptSource: dogfoodTranscriptSource(
                            transcriptCoveredSec: transcriptCoveredSec,
                            fastTranscriptWatermarkSec: asset.fastTranscriptCoverageEndTime
                        ),
                        analysisFraction: analysisFraction,
                        analysisPercent: formatPercent(analysisFraction),
                        analysisSource: dogfoodAnalysisSource(
                            featureCoverageEndSec: asset.featureCoverageEndTime,
                            confirmedAdCoverageEndSec: asset.confirmedAdCoverageEndTime
                        ),
                        episodeDurationSec: asset.episodeDurationSec,
                        transcriptCoveredSec: transcriptCoveredSec,
                        transcriptWatermarkSec: transcriptWatermark,
                        fastTranscriptWatermarkSec: asset.fastTranscriptCoverageEndTime,
                        analysisWatermarkSec: analysisWatermark,
                        featureCoverageEndSec: asset.featureCoverageEndTime,
                        confirmedAdCoverageEndSec: asset.confirmedAdCoverageEndTime,
                        finalPassCoverageEndSec: asset.finalPassCoverageEndTime
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
        isRunning: Bool
    ) -> String {
        if isTerminal(status: status) {
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

    private func dogfoodTranscriptSource(
        transcriptCoveredSec: Double?,
        fastTranscriptWatermarkSec: Double?
    ) -> String {
        if transcriptCoveredSec != nil {
            return "fast_transcript_chunks"
        }
        if fastTranscriptWatermarkSec != nil {
            return "asset_fast_watermark"
        }
        return "unknown"
    }

    private func dogfoodAnalysisSource(
        featureCoverageEndSec: Double?,
        confirmedAdCoverageEndSec: Double?
    ) -> String {
        switch (featureCoverageEndSec, confirmedAdCoverageEndSec) {
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
            terminalReason: asset.terminalReason,
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
            failureReason: session.failureReason,
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
            lastErrorCode: job.lastErrorCode,
            createdAt: job.createdAt,
            updatedAt: job.updatedAt,
            generationID: job.generationID,
            schedulerEpoch: job.schedulerEpoch,
            artifactClass: job.artifactClass.rawValue,
            estimatedWriteBytes: job.estimatedWriteBytes
        )
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
