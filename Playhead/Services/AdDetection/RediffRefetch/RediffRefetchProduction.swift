// RediffRefetchProduction.swift
// playhead-xsdz.36: the PRODUCTION conformers that turn the xsdz.28 re-fetch
// machinery + the xsdz.29/.57 rediff slot pass into a working on-device loop:
//
//   enumerator  — `AnalysisStoreRediffRefetchEnumerator`: candidates are the
//                 episodes with a CURRENT-version A-side fingerprint stream
//                 (i.e. analyzed after activation, played copy still on
//                 disk), joined with the persisted R2 attempt state and the
//                 CURRENT enclosure URL resolved live from SwiftData.
//   recorder    — `AnalysisStoreRediffRefetchRecorder`: persists the advanced
//                 `AttemptState` (the xsdz.28 R2 fix — failures now advance
//                 durable state) and accumulates the bandwidth ledger.
//   provider    — `RediffBSideStagingProvider`: the production
//                 `RediffBSideProvider`. Holds assetId → staged-B-file
//                 mappings for the (short) window between the re-fetch and
//                 the rediff pass consuming it.
//   consumer    — `RevalidatingRediffBSideConsumer`: stages the rotated
//                 B-copy, re-runs the rediff slot pass via
//                 `AdDetectionService.revalidateFromFeatures` (persisted
//                 chunks only — no ASR/decode re-run), and unstages. File
//                 DELETION stays with `RediffRefetchService` (never-persist-B).
//   decoder     — `AnalysisAudioBSideDecoder`: the `AudioFileDecoding`
//                 production conformer — decodes through the SAME
//                 `AnalysisAudioService` pipeline the A-side used (extractor
//                 identity, xsdz.27), against a synthetic episode id whose
//                 shard-cache entry is evicted immediately after (a process
//                 death in between is reclaimed by the per-fire orphan
//                 sweep — see the type's DISK NOTE).

import Foundation
import os
import OSLog

// MARK: - Enclosure resolver box

/// Late-binding holder for the episodeId → CURRENT enclosure-URL resolver.
/// The refetch service is constructed (and its BGTask registered) during
/// `PlayheadRuntime.init`, BEFORE the SwiftData `ModelContainer` exists;
/// `PlayheadApp.task` installs the real resolver once the container is
/// available (mirrors `ShadowRetryObserverHolder`). A nil resolver simply
/// yields zero candidates — a benign no-op sweep.
final class RediffEnclosureResolverBox: Sendable {
    private let storage = OSAllocatedUnfairLock<(@Sendable (String) async -> URL?)?>(initialState: nil)

    var resolver: (@Sendable (String) async -> URL?)? {
        get { storage.withLock { $0 } }
        set { storage.withLock { $0 = newValue } }
    }
}

// MARK: - Enumerator

/// Store-backed candidate enumeration. Candidacy = "has a CURRENT-version
/// A-side fingerprint stream" (spike §7 + xsdz.27): those rows exist only for
/// episodes analyzed with capture ON, which bounds the sweep to the
/// post-activation library. `downloadedAt` is the capture timestamp — capture
/// runs during analysis of the freshly downloaded copy, so it is ≥ the true
/// download time and therefore CONSERVATIVE against the ~3d first-attempt
/// gate. Resolved episodes are excluded in SQL (terminal steady state).
struct AnalysisStoreRediffRefetchEnumerator: RediffRefetchEnumerating {
    let store: AnalysisStore
    let enclosureResolver: RediffEnclosureResolverBox
    /// Seam so tests can fake the played-copy-on-disk check. The default is
    /// the SAME bf4a2383 anchor the byte differ applies to its A-side
    /// (`AdDetectionService.isAnchoredRegularFile`: regular, non-symlink,
    /// non-empty) — a bare existence check would admit a truncated/0-byte
    /// played copy whose garbage local sample reads as "rotated", spending a
    /// ~54 MB fetch on a candidate the byte differ is guaranteed to reject.
    var fileExists: @Sendable (URL) -> Bool = { AdDetectionService.isAnchoredRegularFile($0) }

    private static let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    func candidates() async -> [RediffRefetchCandidate] {
        guard let resolve = enclosureResolver.resolver else { return [] }
        let seeds: [RediffCandidateSeed]
        let stateRows: [RediffRefetchStateRow]
        do {
            seeds = try await store.fetchRediffCandidateSeeds()
            stateRows = try await store.fetchRediffRefetchStates()
        } catch {
            Self.logger.warning("rediff-refetch enumeration failed: \(String(describing: error), privacy: .public)")
            return []
        }
        let stateByAsset = Dictionary(stateRows.map { ($0.analysisAssetId, $0) }, uniquingKeysWith: { a, _ in a })

        var out: [RediffRefetchCandidate] = []
        out.reserveCapacity(seeds.count)
        for seed in seeds {
            // The played copy must still be an on-disk file — it is both the
            // local pre-check sample source and the byte differ's A input.
            guard let local = URL(string: seed.sourceURL), local.isFileURL, fileExists(local) else { continue }
            // CURRENT enclosure URL (spike §7: never a stale one).
            guard let enclosure = await resolve(seed.episodeId) else { continue }
            out.append(RediffRefetchCandidate(
                assetId: seed.analysisAssetId,
                enclosureURL: enclosure,
                downloadedAt: seed.capturedAt,
                localAudioURL: local,
                attemptState: stateByAsset[seed.analysisAssetId]?.attemptState ?? .initial
            ))
        }
        return out
    }
}

// MARK: - Recorder

/// Persists the advanced per-episode `AttemptState` into
/// `rediff_refetch_state` and accumulates `rediff_bandwidth_ledger` — the
/// durable halves the flag-OFF `LoggingRediffRefetchRecorder` deliberately
/// omitted. Also keeps the os_log breadcrumbs (same category) so dogfood
/// forensics keep working.
struct AnalysisStoreRediffRefetchRecorder: RediffRefetchRecording {
    let store: AnalysisStore
    /// Needed to derive the PARKED counter from a failed outcome's new state.
    var config: RediffRefetchPolicy.Configuration = .production
    var now: @Sendable () -> Double = { Date().timeIntervalSince1970 }

    private static let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    func recordOutcome(_ outcome: RediffRefetchPolicy.Outcome) async {
        switch outcome {
        case let .skippedIneligible(assetId, reason):
            // No state advance — nothing to persist. Debug level: this fires
            // for every not-yet-due episode every sweep.
            Self.logger.debug("rediff-refetch skip assetId=\(assetId, privacy: .public) reason=\(String(describing: reason), privacy: .public)")

        case let .unchanged(assetId, cost, newState):
            Self.logger.info("rediff-refetch unchanged assetId=\(assetId, privacy: .public) precheckBytes=\(cost.precheckBytes, privacy: .public)")
            await persist(assetId: assetId, state: newState, cost: cost, unchanged: 1, rotated: 0, failed: 0, parked: 0)

        case let .rotated(assetId, cost, fingerprintCount, newState):
            Self.logger.info("rediff-refetch ROTATED assetId=\(assetId, privacy: .public) precheckBytes=\(cost.precheckBytes, privacy: .public) fullFetchBytes=\(cost.fullFetchBytes, privacy: .public) fpCount=\(fingerprintCount, privacy: .public)")
            await persist(assetId: assetId, state: newState, cost: cost, unchanged: 0, rotated: 1, failed: 0, parked: 0)

        case let .failed(assetId, cost, failureClass, newState, error):
            Self.logger.error("rediff-refetch FAILED assetId=\(assetId, privacy: .public) bytes=\(cost.totalBytes, privacy: .public) class=\(failureClass.rawValue, privacy: .public) streak=\(newState.sameClassFailureStreak, privacy: .public) error=\(error, privacy: .public)")
            let parked = RediffRefetchPolicy.isParked(newState, config: config)
            await persist(assetId: assetId, state: newState, cost: cost, unchanged: 0, rotated: 0, failed: 1, parked: parked ? 1 : 0)
        }
    }

    private func persist(
        assetId: String,
        state: RediffRefetchPolicy.AttemptState,
        cost: RediffRefetchPolicy.BandwidthCost,
        unchanged: Int,
        rotated: Int,
        failed: Int,
        parked: Int
    ) async {
        do {
            try await store.upsertRediffRefetchState(RediffRefetchStateRow(
                analysisAssetId: assetId,
                attemptState: state,
                updatedAt: now()
            ))
            try await store.accumulateRediffBandwidth(
                precheckBytes: cost.precheckBytes,
                fullFetchBytes: cost.fullFetchBytes,
                unchangedCount: unchanged,
                rotatedCount: rotated,
                failedCount: failed,
                parkedCount: parked,
                at: now()
            )
        } catch {
            // Best-effort durability: a write failure costs one attempt's
            // bookkeeping (the episode retries next sweep), never the sweep.
            Self.logger.warning("rediff-refetch state persist failed assetId=\(assetId, privacy: .public): \(String(describing: error), privacy: .public)")
        }
    }
}

// MARK: - B-side staging provider (the production RediffBSideProvider)

/// Production `RediffBSideProvider`: an in-memory assetId → staged-B-file
/// map. `RevalidatingRediffBSideConsumer.stage(...)` populates it moments
/// before the rediff pass runs and unstages right after — the mapping (and
/// the file, owned by `RediffRefetchService`) never outlives the sweep's
/// candidate scope, so nothing here persists ~54 MB anywhere.
///
/// The byte-primary path (xsdz.57) reads `refetchedBSideFileURL`; the chroma
/// fallback asks for `refetchedBSideMono16kHz`, which decodes the staged file
/// through the injected `AudioFileDecoding` (the SAME pipeline decode the
/// A-side used), duration-capped by
/// `RediffActivation.maxBSideDecodeDurationSeconds`.
actor RediffBSideStagingProvider: RediffBSideProvider {
    /// playhead-xsdz.36.2 (k-way): assetId → the K staged B-side files (one per
    /// distinct-persona re-fetch). A single-fetch (K=1) asset holds a one-element
    /// list — `refetchedBSideFileURL` returns its first element, byte-identical
    /// to the pre-k-way single-URL map.
    private var staged: [String: [URL]] = [:]
    private let decoder: any AudioFileDecoding
    private let maxDecodeDurationSeconds: TimeInterval
    private let durationProbe: @Sendable (URL) async -> TimeInterval?
    private let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    init(
        decoder: any AudioFileDecoding,
        maxDecodeDurationSeconds: TimeInterval = RediffActivation.maxBSideDecodeDurationSeconds,
        durationProbe: @escaping @Sendable (URL) async -> TimeInterval? = { await AudioFileDurationProbe.probeDuration(at: $0) }
    ) {
        self.decoder = decoder
        self.maxDecodeDurationSeconds = maxDecodeDurationSeconds
        self.durationProbe = durationProbe
    }

    /// Single-B stage (the K=1 / pre-k-way path): replaces any prior mapping
    /// with a one-element list.
    func stage(assetId: String, fileURL: URL) {
        staged[assetId] = [fileURL]
    }

    /// playhead-xsdz.36.2 (k-way): stage ALL K distinct-persona B-copies at once
    /// so `computeByteAlignedPlayedSlots` can align A vs each and union. An empty
    /// list unstages (defensive — the consumer always stages ≥1).
    func stageAll(assetId: String, fileURLs: [URL]) {
        staged[assetId] = fileURLs.isEmpty ? nil : fileURLs
    }

    func unstage(assetId: String) {
        staged[assetId] = nil
    }

    /// Test/diagnostic surface: how many ASSETS currently have staged B-sides
    /// (not the total file count).
    var stagedCount: Int { staged.count }

    func refetchedBSideFileURL(assetId: String) async -> URL? {
        staged[assetId]?.first
    }

    func refetchedBSideFileURLs(assetId: String) async -> [URL] {
        staged[assetId] ?? []
    }

    func refetchedBSideMono16kHz(assetId: String) async -> [Float]? {
        // The chroma FALLBACK stays single-B: decode the PRIMARY (first-persona)
        // copy. k-way union is a byte-path concept (the byte differ aligns A vs
        // each staged B); the chroma path is only reached when the byte path is
        // unavailable/rejected for ALL of them.
        guard let url = staged[assetId]?.first else { return nil }
        // Cost bound for the chroma fallback: a >cap episode returns nil →
        // the pass falls through to status quo (the byte path has already
        // been tried by this point).
        if let duration = await durationProbe(url), duration > maxDecodeDurationSeconds {
            logger.info("rediff B-side PCM decode skipped (duration \(Int(duration), privacy: .public)s > cap) asset=\(assetId, privacy: .public)")
            return nil
        }
        do {
            return try await decoder.decodeMono16kHz(fileURL: url)
        } catch {
            logger.warning("rediff B-side PCM decode failed asset=\(assetId, privacy: .public): \(String(describing: error), privacy: .public)")
            return nil
        }
    }
}

// MARK: - Production AudioFileDecoding

/// Decodes an audio file to mono 16 kHz through `AnalysisAudioService` — the
/// EXACT decode machinery that produced the A-side shards, so A and B PCM
/// come from one decoder (extractor identity, xsdz.27 header). Uses a unique
/// synthetic episode id and evicts its shard-cache entry immediately, so the
/// B-side never pollutes the pipeline's shard cache for real episodes.
///
/// DISK NOTE (R2, wording corrected R3): `AnalysisAudioService.decode`
/// persists the decoded shards to the Application Support cache at decode
/// COMPLETION (`ShardCache.saveShards`, step 9 — skipped for truncated
/// files), so B-side PCM does touch disk transiently under the synthetic
/// id — the inline `evictCache` on both exits removes it within the same
/// call. A process death in the save→evict window (jetsam mid-consume)
/// strands the directory; the per-fire orphan sweep
/// (`FileManagerTempFileRemover.removeOrphanedBCopies`, prefix-scoped via
/// `syntheticEpisodeIDPrefix`) reclaims it on the next fire.
struct AnalysisAudioBSideDecoder: AudioFileDecoding {
    let audioService: AnalysisAudioService

    /// Prefix for the synthetic per-decode episode id. Shared with the
    /// per-fire shard-cache orphan sweep so cleanup can never drift from
    /// what this decoder actually names its cache entries.
    static let syntheticEpisodeIDPrefix = "rediff-bside-"

    struct NotAFileURLError: Error, Equatable {}

    func decodeMono16kHz(fileURL: URL) async throws -> [Float] {
        guard let local = LocalAudioURL(fileURL) else { throw NotAFileURLError() }
        let syntheticId = Self.syntheticEpisodeIDPrefix + UUID().uuidString
        do {
            let shards = try await audioService.decode(
                fileURL: local,
                episodeID: syntheticId
            )
            await audioService.evictCache(episodeID: syntheticId)
            let ordered = shards.sorted { $0.startTime < $1.startTime }
            var mono = [Float]()
            mono.reserveCapacity(ordered.reduce(0) { $0 + $1.samples.count })
            for shard in ordered { mono.append(contentsOf: shard.samples) }
            return mono
        } catch {
            await audioService.evictCache(episodeID: syntheticId)
            throw error
        }
    }
}

// MARK: - B-side consumer

/// Errors the consume path can raise, with their R2 failure classes: local
/// state that will not heal (asset row gone, no usable duration) is
/// `.staleAsset` (terminal → backoff → park); a store read failure is
/// `.transient` (retry next sweep).
enum RediffBSideConsumeError: RediffFailureClassifiable, Equatable {
    case assetMissing(assetId: String)
    case episodeDurationUnknown(assetId: String)
    case storeUnavailable(String)

    var rediffFailureClass: RediffRefetchPolicy.FailureClass {
        switch self {
        case .assetMissing, .episodeDurationUnknown: return .staleAsset
        case .storeUnavailable: return .transient
        }
    }
}

/// Production `RediffBSideConsuming`: stage → revalidate → unstage.
///
/// `revalidateFromFeatures` (playhead-zx6i) re-runs classifier + fusion +
/// boundary + the rediff slot pass over the PERSISTED transcript chunks — no
/// ASR, no feature re-extraction — so the whole consume fits comfortably in a
/// BGProcessingTask window. While the B-side is staged,
/// `computeRediffSlotPass` sees it through the provider and (byte-primary,
/// chroma-fallback) rewrites the episode's spans with `.rediffSlot`
/// provenance — the actual mark-only width marks.
struct RevalidatingRediffBSideConsumer: RediffBSideConsuming {
    let staging: RediffBSideStagingProvider
    let store: AnalysisStore
    let adDetection: any AdDetectionProviding

    private static let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    /// Single-B consume (K=1 / pre-k-way): stage the one B-copy and revalidate.
    /// Byte-identical to routing through `consumeRotatedBSides` with a
    /// one-element list.
    func consumeRotatedBSide(assetId: String, fileURL: URL) async throws {
        try await consumeRotatedBSides(assetId: assetId, fileURLs: [fileURL])
    }

    /// playhead-xsdz.36.2 (k-way): stage ALL K B-copies, run ONE revalidation
    /// (so `computeByteAlignedPlayedSlots` aligns A vs each and unions the
    /// divergent regions), then unstage on every exit.
    func consumeRotatedBSides(assetId: String, fileURLs: [URL]) async throws {
        guard !fileURLs.isEmpty else { return }
        let asset: AnalysisAsset
        do {
            guard let fetched = try await store.fetchAsset(id: assetId) else {
                throw RediffBSideConsumeError.assetMissing(assetId: assetId)
            }
            asset = fetched
        } catch let error as RediffBSideConsumeError {
            throw error
        } catch {
            throw RediffBSideConsumeError.storeUnavailable(String(describing: error))
        }

        // Duration: prefer the persisted column (populated by both pipelines
        // since gtt9.1.1); fall back to the A-side stream's own extent. Both
        // absent means the asset predates capture — it should never have been
        // a candidate (candidacy = fingerprint row exists), so treat as stale.
        let episodeDuration: Double
        if let persisted = asset.episodeDurationSec, persisted > 0 {
            episodeDuration = persisted
        } else {
            // A store READ failure here is `.transient` like the fetchAsset
            // one above (R2): it must not masquerade as the terminal
            // stale-asset class and walk the episode toward parking.
            let record: EpisodeFingerprintRecord?
            do {
                record = try await store.fetchEpisodeFingerprints(assetId: assetId)
            } catch {
                throw RediffBSideConsumeError.storeUnavailable(String(describing: error))
            }
            guard let record, record.secondsPerFingerprint > 0, !record.fingerprints.isEmpty else {
                throw RediffBSideConsumeError.episodeDurationUnknown(assetId: assetId)
            }
            episodeDuration = Double(record.fingerprints.count) * record.secondsPerFingerprint
        }

        // podcastId feeds show-scoped signals (stinger bank / negative bank);
        // the empty-string fallback matches the scheduler's own
        // `job.podcastId ?? ""` convention.
        let latestJob = (try? await store.fetchLatestJobForEpisode(asset.episodeId)) ?? nil
        let podcastId = latestJob?.podcastId ?? ""

        await staging.stageAll(assetId: assetId, fileURLs: fileURLs)
        Self.logger.info("rediff B-side staged for revalidation asset=\(assetId, privacy: .public) copies=\(fileURLs.count, privacy: .public)")
        do {
            try await adDetection.revalidateFromFeatures(
                analysisAssetId: assetId,
                podcastId: podcastId,
                episodeDuration: episodeDuration,
                sessionId: nil
            )
            await staging.unstage(assetId: assetId)
        } catch {
            // Unstage on EVERY exit — a stale mapping would point future
            // passes at a deleted file.
            await staging.unstage(assetId: assetId)
            throw error
        }
    }
}
