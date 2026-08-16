// FinalPassRetranscriptionRunner.swift
// Bug 9: charge-gated final-pass re-transcription phase.
//
// Background. `SpeechService.activeModelRole` only flips to `.asrFinal` when
// `loadFinalModel()` is called, but pre-Bug-9 production never invoked
// `loadFinalModel()`. As a result every persisted `transcript_chunks.pass`
// value was `"fast"` and downstream consumers that expected `pass='final'`
// rows either silently fell back to fast chunks (degraded text) or — at
// `AdDetectionService.retryShadowFMPhaseForSession` — bailed unconditionally.
// Part A of the bug fix added a fallback to the broken consumer; this file
// (Part B) adds the missing producer side: a sibling backfill phase that
// actually re-transcribes audio with the final-pass recogniser.
//
// Design (decisions documented inline so a reviewer can audit every edge
// without re-reading the bead prompt):
//
//  • Persistence — APPEND `pass='final'` rows; do NOT replace `pass='fast'`
//    rows. Existing FTS, search, and shadow-replay consumers keep working
//    unchanged on the fast rows; new consumers can opt in to the
//    higher-quality final rows by filtering on `pass='final'`, and
//    `TranscriptChunkCanonicalizer` is what merges the two for the display
//    and detection projections.
//
//    ⚠️ THE REASON THIS USED TO GIVE FOR WHY IT IS SAFE IS FALSE, and it
//    cost 7,248 duplicate rows (playhead-jc42). It read: "Fast and final
//    chunks have distinct `segmentFingerprint`s (computed from text +
//    timing, both of which differ across passes)". Measured on the
//    2026-08-15 device pull, text and timing DO NOT differ across passes —
//    7,247 of the 7,248 twins are byte-identical in both. The final pass
//    re-transcribing audio the fast pass already covered reproduces it
//    verbatim, and the fingerprints differ for one reason only: the
//    `fp-final-` prefix `computeFinalPassFingerprint` adds. So the prefix
//    was not observing a distinction, it was MANUFACTURING one, and it
//    manufactured it against `TranscriptEngineService` too — which also
//    emits `pass='final'` rows (17,632 of the 24,880 on that device),
//    unprefixed. Two producers, one pass, two hash namespaces: the
//    pre-insert read below could not see across the gap and neither could
//    the `(asset, pass, segmentFingerprint)` UNIQUE index, so 3,496 spans
//    ended up as two `pass='final'` rows that the canonicalizer retains in
//    full (it de-overlaps fast against final, never final against final)
//    and the overlay draws twice.
//
//    Identity here is CONTENT — `(asset, pass, startTime, endTime, text)` —
//    enforced by `idx_chunks_asset_pass_span_text` (AnalysisStore V53) and
//    read by `fetchTranscriptChunkBySpanText`. `segmentFingerprint` stays
//    what it always was: a stable per-writer row id, no longer load-bearing
//    for deduplication.
//
//  • State tracking — the `analysis_assets.finalPassCoverageEndTime`
//    column (added in this bead) carries the maximum `endTime` of any
//    AdWindow that has been re-transcribed. It is still WRITTEN, and it
//    still feeds `AnalysisCoverageSummary`'s final-pass provenance
//    fallback, but playhead-jzj0 removed it from the eligibility path.
//    It measures how far forward this runner has reached; it never
//    measured which spans were finished, and reading it as the latter
//    made every window minted behind the frontier permanently
//    unreachable — including a user's own manual mark. The resume guard
//    is now the per-window record that already existed: this window's own
//    `final_pass_jobs` row, resolved by canonical span key and retired
//    only when its status is `complete`.
//
//  • Job table — sibling table `final_pass_jobs`. We chose a sibling
//    rather than extending `backfill_jobs` with a new phase value because
//    (1) the unit of work is fundamentally different (audio decode + ASR
//    vs. FM text classification), (2) the runner shape is leaner, and
//    (3) keeping the two pipelines independent prevents one from
//    mass-deferring the other under thermal pressure. As a corollary, this
//    runner does NOT share the FM `AdmissionController` queue; gating is
//    derived inline from a `CapabilitySnapshot` + battery + charge read.
//
//  • Admission — three independent gates evaluated at runner-entry AND
//    once per window (so an unplug or thermal spike mid-drain terminates
//    promptly):
//      1. `isCharging == true` — re-transcription is heavy, the bead
//         spec scopes it to plugged-in devices.
//      2. `QualityProfile.derive(...) != .critical` (i.e. `pauseAllWork`
//         is false) — same thermal/LPM/low-battery gate the
//         `AdmissionController` uses, applied via the shared derivation.
//      3. `thermalState == .nominal` — the bead spec asks for a strict
//         "nominal thermal" floor, stricter than `pauseAllWork` (which
//         only blocks at `.critical`). Heavy ASR work is throttled out
//         even at `.fair` so we don't push warm devices into `.serious`.
//
//  • Confidence threshold — defaults to `0.5`, configurable via
//    `PreAnalysisConfig.finalPassRetranscriptionConfidenceFloor`. Only
//    AdWindows whose persisted `confidence` clears the floor are
//    re-transcribed. The whole point is high-quality text on the
//    candidate windows the classifier has already flagged; running the
//    final model over the entire episode would burn battery for no
//    classifier gain.
//
//  • Idempotency — the runner consults each window's own canonical
//    `final_pass_jobs` row before scheduling work for it AND consults
//    `transcript_chunks` to confirm there are no already-persisted
//    `pass='final'` rows that overlap. A second invocation against an
//    asset every one of whose confidence-cleared AdWindows has a
//    `complete` job is a guaranteed no-op. Unlike the pre-jzj0 watermark
//    form, that guarantee no longer depends on the order the candidates
//    arrived in.
//
//  • Wiring — composed in `PlayheadRuntime` alongside the existing
//    `BackfillJobRunner`. `AnalysisJobReconciler.reconcile()` enqueues
//    eligible assets at launch via `enqueueAssetsNeedingFinalPass`.
//
// Out of scope (per bead prompt): the existing `BackfillJobRunner` is
// untouched, the `pass` column and `loadFinalModel` enum case are
// preserved, and classifier thresholds (the *coarse* classifier's, not
// the per-window confidence floor introduced here) are not changed.

import CryptoKit
import Foundation
import OSLog

// MARK: - FinalPassDeferredMidWindow

/// Typed sentinel thrown by `retranscribeWindow` when a per-shard
/// admission gate (thermal / charge / LPM) trips mid-window. The outer
/// `runFinalPassBackfill` catches this distinct from `CancellationError`
/// and from a generic `retranscribeFailed` failure path:
///
///   • The current job has ALREADY been marked `.deferred` (with partial
///     chunks persisted) inside `retranscribeWindow`. The catch site
///     therefore MUST NOT call `markFinalPassJobFailed` (that would bump
///     `retryCount`) or `markFinalPassJobDeferred` again on the same row.
///   • Remaining sibling jobs in the same drain ARE still queued; the
///     catch site walks them and marks each `.deferred` with the same
///     reason. This prevents a row from being orphaned in `running`
///     state when the surrounding for-loop is broken out of.
///
/// `retryCount` is preserved across a clean defer (the runner makes no
/// `markFinalPassJobFailed` call on this path), so the launch retry
/// cap — if added later — does not see a spurious bump.
struct FinalPassDeferredMidWindow: Error, Equatable {
    let reason: AdmissionDeferReason
}

// MARK: - FinalPassRevalidationRequest

/// playhead-hc7e — payload handed to the post-final-pass revalidation hook
/// once at least one AdWindow has been re-transcribed to `pass='final'`.
///
/// Closing the final-ASR loop: the runner APPENDS higher-quality final
/// chunks but does not itself re-run detection. Without a trigger, the
/// improved transcript sits unused until the next full analysis pass. The
/// hook lets the composition root (`PlayheadRuntime`) invoke
/// `AdDetectionService.revalidateFromFeatures` for the affected asset, which
/// re-fetches the now-canonical fast+final chunk set and re-runs
/// classifier + fusion + boundary over it.
struct FinalPassRevalidationRequest: Sendable, Equatable {
    let analysisAssetId: String
    let podcastId: String?
    /// Total episode duration (seconds) resolved from the persisted asset.
    /// `0` when the asset row carries no durable duration.
    let episodeDuration: Double
    /// AdWindow ids whose audio was re-transcribed in the completed drain.
    let reTranscribedWindowIds: [String]
}

// MARK: - FinalPassRetranscriptionRunner

/// Runs the charge-gated final-pass re-transcription phase for one asset.
/// Idempotent against the persisted watermark + `pass='final'` chunks.
actor FinalPassRetranscriptionRunner {

    // MARK: - Production Defaults

    /// Default confidence floor for AdWindow eligibility. AdWindows whose
    /// `confidence` is strictly less than this value are skipped.
    /// Configurable via the `FinalPassRetranscriptionRunner.init`
    /// `confidenceFloor` parameter so future per-device cohort tuning can
    /// move it without touching the runner.
    static let defaultConfidenceFloor: Double = 0.5

    /// Distinct from the fast-pass `apple-speech-v1` so persisted
    /// `pass='final'` rows record which model produced them. Bumping this
    /// invalidates prior final-pass coverage; the watermark approach
    /// makes a re-run a one-shot reprocess rather than a destructive
    /// migration.
    static let defaultModelVersion: String = "apple-speech-final-v1"

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let speechService: SpeechService
    private let audioProvider: AnalysisAudioProviding
    private let capabilitySnapshotProvider: @Sendable () async -> CapabilitySnapshot
    private let batteryLevelProvider: @Sendable () async -> Float
    private let chargeStateProvider: @Sendable () async -> Bool
    private let confidenceFloor: Double
    private let modelVersion: String
    /// playhead-hc7e — invoked once at the end of a drain that re-transcribed
    /// at least one window, so the composition root can schedule a detector
    /// re-run / revalidation over the newly-improved transcript. `nil` (the
    /// default, and always in preview / most unit tests) makes the runner a
    /// pure producer with no loop-close — byte-identical to pre-hc7e.
    private let onFinalPassRetranscribed: (@Sendable (FinalPassRevalidationRequest) async -> Void)?
    private let logger = Logger(subsystem: "com.playhead", category: "FinalPassRetranscription")

    // MARK: - Inputs / Outputs

    struct AssetInput: Sendable, Equatable {
        let analysisAssetId: String
        let podcastId: String?
        /// Resolved local file URL for the asset's audio. The caller (the
        /// reconciler at launch time, or `runtime.runFinalPassBackfill`
        /// from a BG-task wakeup) is responsible for verifying that the
        /// file is on disk before constructing this input — if the file
        /// has been evicted, no AdWindow can be re-transcribed anyway.
        let audioURL: LocalAudioURL
        /// Episode identifier used as the audio shard cache key.
        let episodeId: String
    }

    struct RunResult: Sendable, Equatable {
        /// IDs of `final_pass_jobs` rows that were admitted and ran to
        /// completion in this drain. Empty when admission deferred or
        /// when the asset was already fully covered.
        let admittedJobIds: [String]
        /// IDs of windows that were re-transcribed during this run. A
        /// re-run against the same asset must produce an empty array
        /// here once `idempotency` has converged.
        let reTranscribedWindowIds: [String]
        /// IDs of `final_pass_jobs` rows that were enqueued but deferred
        /// by admission (charge / thermal / battery / LPM).
        let deferredJobIds: [String]
        /// Reason a top-level run did not start at all (e.g. not on
        /// charge). `nil` when the runner reached the per-window loop.
        let topLevelDeferReason: AdmissionDeferReason?

        static let empty = RunResult(
            admittedJobIds: [],
            reTranscribedWindowIds: [],
            deferredJobIds: [],
            topLevelDeferReason: nil
        )
    }

    // MARK: - Init

    init(
        store: AnalysisStore,
        speechService: SpeechService,
        audioProvider: AnalysisAudioProviding,
        capabilitySnapshotProvider: @escaping @Sendable () async -> CapabilitySnapshot,
        batteryLevelProvider: @escaping @Sendable () async -> Float,
        chargeStateProvider: @escaping @Sendable () async -> Bool,
        confidenceFloor: Double,
        modelVersion: String,
        onFinalPassRetranscribed: (@Sendable (FinalPassRevalidationRequest) async -> Void)? = nil
    ) {
        self.store = store
        self.speechService = speechService
        self.audioProvider = audioProvider
        self.capabilitySnapshotProvider = capabilitySnapshotProvider
        self.batteryLevelProvider = batteryLevelProvider
        self.chargeStateProvider = chargeStateProvider
        self.confidenceFloor = confidenceFloor
        self.modelVersion = modelVersion
        self.onFinalPassRetranscribed = onFinalPassRetranscribed
    }

    // MARK: - Entry Point

    /// Runs the final-pass re-transcription phase for one asset.
    ///
    /// Sequence:
    ///   1. Top-level admission gate: charge + nominal thermal + LPM=false +
    ///      QualityProfile permits work. Bails with a populated
    ///      `topLevelDeferReason` on the first failed gate.
    ///   2. Loads the asset's persisted AdWindows.
    ///   3. Filters to windows where `confidence >= confidenceFloor`
    ///      (playhead-jzj0 removed the `endTime > watermark` clause that
    ///      used to sit here; the idempotent skip is per-window, at step 4).
    ///   4. Groups survivors by canonical span, resolves each span to its
    ///      existing `final_pass_jobs` row (or enqueues one), and DROPS any
    ///      whose row is already `complete` — the per-window resume guard.
    ///      Per surviving job: re-checks the gates, calls
    ///      `loadFinalModel()` (idempotent via `activeModelRole`), slices
    ///      the relevant audio shards out of the cached decoded set,
    ///      transcribes each shard, and appends the resulting
    ///      `pass='final'` rows.
    ///   5. Advances `analysis_assets.finalPassCoverageEndTime` to the
    ///      maximum window endTime that ran in this drain.
    ///
    /// Idempotency contract: a second call with no thermal / battery /
    /// charge change must be a no-op (zero re-transcribed windows).
    @discardableResult
    func runFinalPassBackfill(for input: AssetInput) async throws -> RunResult {
        // Step 1 — top-level admission gate.
        if let reason = await currentDeferReason() {
            logger.info("Final-pass run deferred: reason=\(reason.rawValue, privacy: .public) (asset=\(input.analysisAssetId, privacy: .public))")
            return RunResult(
                admittedJobIds: [],
                reTranscribedWindowIds: [],
                deferredJobIds: [],
                topLevelDeferReason: reason
            )
        }

        // Step 2 — load AdWindows + watermark.
        let asset: AnalysisAsset?
        do {
            asset = try await store.fetchAsset(id: input.analysisAssetId)
        } catch {
            logger.warning("Final-pass: failed to fetch asset \(input.analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            throw error
        }
        guard let asset else {
            logger.debug("Final-pass: asset \(input.analysisAssetId, privacy: .public) not found")
            return .empty
        }
        let watermark = asset.finalPassCoverageEndTime ?? 0

        let allWindows: [AdWindow]
        do {
            allWindows = try await store.fetchAdWindows(assetId: input.analysisAssetId)
        } catch {
            logger.warning("Final-pass: failed to fetch ad windows for \(input.analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            throw error
        }

        // Step 3 — filter. A window is eligible iff:
        //   • its endTime is strictly greater than its startTime (a
        //     window with `endTime <= startTime` is structurally
        //     invalid — a zero-length or inverted span — and re-
        //     transcribing it is a no-op that would still mark a
        //     `complete` row, polluting coverage. Per the
        //     playhead-hygc.1.5 contract: zero-length windows are
        //     REJECTED at job creation time, not silently absorbed.
        //     Non-zero but tiny spans (e.g. 0.001 s) are valid and
        //     pass through; the 0-length cutoff is `endTime > startTime`,
        //     not a fixed minimum duration), AND
        //   • its confidence cleared the configured floor.
        //
        // playhead-jzj0 — THERE IS NO LONGER A THIRD CLAUSE, and the one
        // that was here is the whole of this bead. It read
        // `endTime > asset.finalPassCoverageEndTime`.
        //
        // That column holds the MAXIMUM END of any span this runner has
        // drained: a forward-progress marker whose numerator is "the
        // furthest point reached" and which has no denominator at all. The
        // filter read it as a COMPLETION claim — "everything at or before
        // this point is done". The two readings coincide only while
        // candidates arrive in increasing order of `endTime`, and they do
        // not: fusion backfill, semantic sweep and the user's own manual
        // marks mint AdWindows hours after the launch batch, at arbitrary
        // positions. Once a post-roll window drove the value to EOF, every
        // window ending inside the episode was excluded PERMANENTLY —
        // 6 of the 9 job-less windows on the 2026-08-02 device pull,
        // including both of Dan's `userMarked` pod windows at confidence
        // 1.0, and a `failed` job that could never be retried because an
        // unrelated later span had passed it.
        //
        // Nothing replaces it, because the per-window record it was
        // standing in for already exists and is already consulted a few
        // lines below: `findFinalPassJob(forAssetId:canonicalSpanKey:)`
        // resolves this window's own span to its own job, and
        // `guard job.status != .complete` retires it. That guard is
        // monotone, durable and indifferent to arrival order — a window
        // minted behind the frontier is now judged on its own record
        // rather than on a neighbour's.
        //
        // Deleting the clause rather than replacing it with a completed-span
        // SET is deliberate. A set-membership filter here would also drop
        // already-complete windows before the grouping loop, and that loop
        // is where playhead-hygc.1.5 records a same-span window as an ALIAS
        // of the canonical job covering it. Filtering early is cheaper and
        // silently narrows that audit contract; the cost is instead paid
        // back by deferring the `transcript_chunks` read below until the
        // job list is known to be non-empty.
        //
        // The per-row `pass='final'` chunk overlap check is in
        // `retranscribeWindow` so it can short-circuit the full audio
        // decode without an extra DB hop here.
        let eligibleWindows = allWindows.filter { window in
            // playhead-hygc.1.5: zero-length / inverted span guard.
            // The May 6 dogfood `final_pass_jobs` had several rows with
            // `windowStartTime == windowEndTime`; those produced
            // `.complete` rows that consumed audit slots without
            // re-transcribing anything. Filtering at job-creation time
            // means a downstream query like `SELECT COUNT(*) FROM
            // final_pass_jobs WHERE status='complete'` reflects real
            // transcription work, not bookkeeping noise.
            guard window.endTime > window.startTime else {
                logger.debug("Final-pass: rejecting zero-length window \(window.id, privacy: .public) [\(window.startTime, privacy: .public)..\(window.endTime, privacy: .public)]")
                return false
            }
            guard window.confidence >= confidenceFloor else { return false }
            return true
        }
        guard !eligibleWindows.isEmpty else {
            logger.debug("Final-pass: no eligible windows for \(input.analysisAssetId, privacy: .public) (windows=\(allWindows.count, privacy: .public), floor=\(self.confidenceFloor, privacy: .public), watermark=\(watermark, privacy: .public))")
            return .empty
        }

        // skeptical-review-cycle-5 M-Y1: track windows successfully
        // re-transcribed in this drain. If two eligible AdWindows are
        // spatially overlapping, the second iteration's `coversWindow`
        // check at line ~430 cannot see chunks the first iteration just
        // wrote (the snapshot above is loop-stale). Carry the in-drain
        // coverage forward as `(start, end)` intervals and OR it with
        // the persisted-chunks check so overlapping windows skip the
        // redundant ASR pass. Per-segment dedupe in
        // `hasTranscriptChunk(segmentFingerprint:)` was the only safety
        // net previously; this avoids the wasted FM/ASR work upstream.
        var inDrainCoveredIntervals: [(Double, Double)] = []

        // Step 4 — collapse same-span AdWindows + enqueue + run.
        //
        // playhead-hygc.1.5: the May 6 dogfood DB carried duplicate
        // final_pass_jobs rows because two AdWindow rows with different
        // `adWindowId` values represented the same time span. Pre-fix,
        // each one produced a distinct `fpj-<asset>-<adWindowId>` jobId
        // and `INSERT OR IGNORE` happily accepted both. We now group
        // eligible windows by `(start, end)` rounded to 1ms and pick
        // ONE canonical AdWindow per group (lowest `id` lexicographically
        // — deterministic, stable across re-enqueues). The non-canonical
        // ids are recorded in `final_pass_job_aliases` so audit visibility
        // is preserved.
        //
        // Canonical jobId is still `fpj-<asset>-<canonicalAdWindowId>`,
        // so existing diagnostics that key off jobId continue to work.
        // The store's `findFinalPassJob(forAssetId:canonicalSpanKey:)`
        // protects against the cross-launch case where a prior run already
        // landed a canonical row (possibly under a DIFFERENT
        // canonicalAdWindowId because the AdWindow set has shifted): we
        // attach the new contributing windows as aliases of THAT row
        // rather than enqueuing a competing canonical job.
        let now = Date().timeIntervalSince1970
        struct CanonicalGroup {
            let canonicalSpanKey: String
            let canonicalWindow: AdWindow
            let aliasWindowIds: [String]
        }
        // Group by canonical span key; sort the in-group windows by id
        // so the canonical pick is deterministic regardless of fetch
        // order.
        var groupsByKey: [String: [AdWindow]] = [:]
        for window in eligibleWindows {
            let key = AnalysisStore.canonicalSpanKey(
                start: window.startTime,
                end: window.endTime
            )
            groupsByKey[key, default: []].append(window)
        }
        // Stable iteration order over groups: sort by span key (which is
        // monotonic in (start,end)), then lexicographically by canonical
        // id. Without this, two test runs over the same input set could
        // pick different canonical AdWindow rows depending on dictionary
        // ordering, which would break the
        // "same windows produce one canonical job" invariant under
        // `Equatable` assertions.
        let canonicalGroups: [CanonicalGroup] = groupsByKey
            .map { (key, windows) -> CanonicalGroup in
                let sorted = windows.sorted { $0.id < $1.id }
                let canonical = sorted.first!
                let aliases = sorted.dropFirst().map(\.id)
                return CanonicalGroup(
                    canonicalSpanKey: key,
                    canonicalWindow: canonical,
                    aliasWindowIds: Array(aliases)
                )
            }
            .sorted { lhs, rhs in
                if lhs.canonicalSpanKey != rhs.canonicalSpanKey {
                    return lhs.canonicalSpanKey < rhs.canonicalSpanKey
                }
                return lhs.canonicalWindow.id < rhs.canonicalWindow.id
            }
        if canonicalGroups.count < eligibleWindows.count {
            logger.info("Final-pass: collapsed \(eligibleWindows.count, privacy: .public) eligible windows into \(canonicalGroups.count, privacy: .public) canonical span(s) for asset \(input.analysisAssetId, privacy: .public) (playhead-hygc.1.5 dedupe)")
        }

        var jobs: [FinalPassJob] = []
        // Track jobs we already added in this loop so two groups that
        // resolve to the same canonical jobId across the
        // `findFinalPassJob` lookup don't double-append the same row.
        var seenJobIds = Set<String>()
        for group in canonicalGroups {
            // Cross-launch lookup: did a prior run already land a
            // canonical row for this `(asset, span)`? If so, attach
            // ALL of this group's contributing AdWindow ids as aliases
            // of that pre-existing canonical and DO NOT enqueue a
            // competing row. Note this can happen when:
            //   • a thermal-deferred row from a prior process is
            //     waking up; the AdWindow set has not changed but
            //     `FinalPassThermalRecoveryObserver` re-walked the
            //     same windows;
            //   • the boundary detector has shifted and produced a
            //     fresh AdWindow row with a different id but the same
            //     span as a previously-completed job.
            let existing = (try? await store.findFinalPassJob(
                forAssetId: input.analysisAssetId,
                canonicalSpanKey: group.canonicalSpanKey
            )) ?? nil
            let job: FinalPassJob
            if let existing {
                job = existing
                // Record the canonical AdWindow + every alias against the
                // pre-existing row's jobId. The canonical of THIS group
                // may not equal the canonical the prior run picked, so
                // we record both: the prior canonical row's own
                // `adWindowId` is already the row's identity — we only
                // need to attach the new contributors.
                let contributors = [group.canonicalWindow.id] + group.aliasWindowIds
                for contributorId in contributors where contributorId != existing.adWindowId {
                    do {
                        try await store.recordFinalPassJobAlias(
                            jobId: existing.jobId,
                            adWindowId: contributorId,
                            addedAt: now
                        )
                    } catch {
                        logger.warning("Final-pass: failed to record alias \(contributorId, privacy: .public) → \(existing.jobId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                    }
                }
            } else {
                // playhead-jzj0 review R1 — JOB IDENTITY IS THE SPAN, BUT THE
                // jobId IS THE AdWindow ID, AND THE TWO CAN DISAGREE.
                //
                // `insertOrIgnoreFinalPassJob` is `INSERT OR IGNORE` against
                // `final_pass_jobs(jobId TEXT PRIMARY KEY)`, and the jobId is
                // derived from the canonical AdWindow's id — while the lookup
                // three lines above keys on the canonical SPAN. An AdWindow's
                // id is stable across a span change by design:
                // `AdDetectionService.reconcileHotPathWindows` builds its
                // `preservedWindow` with `id: existing.id` and
                // `startTime/endTime` from THIS run (the `sameGeometry`
                // branch exists precisely because the geometry may differ),
                // and `AnalysisStore.updateAdWindowHotPathCandidate` writes
                // the new bounds onto the same row.
                //
                // So after a window's span moves: the span lookup MISSES (new
                // key), the INSERT is silently IGNORED (old jobId already
                // present), and `newJob` — which says `.queued`, `retryCount 0`
                // — describes a row that does not exist. Trusting it made
                // `guard job.status != .complete` pass against a row that is
                // `complete`, `markFinalPassJobRunning` a silent no-op (its
                // IN-clause excludes `'complete'`), and the window re-decode +
                // re-ASR on EVERY admitted sweep, for ever — with
                // `onFinalPassRetranscribed` re-firing a full
                // classifier+fusion+boundary revalidation each time. Deleting
                // the `endTime > watermark` clause is what exposed this: the
                // frontier used to sit at EOF and suppress the whole path.
                // That is the same unbounded-churn shape this bead is about,
                // arrived at from the other side.
                //
                // Two steps close it:
                //   1. If a row already occupies the natural jobId under a
                //      DIFFERENT canonical span, qualify the id with the span
                //      so the new span gets its own row. Deterministic — the
                //      same (window id, span) always yields the same id, so a
                //      re-enqueue after a crash is still idempotent — and the
                //      `fpj-<asset>-<adWindowId>` prefix is preserved, so
                //      jobId-keyed diagnostics still resolve. The old span's
                //      completed row is left intact; it is real history.
                //   2. Read back whatever row the insert actually left behind
                //      and drive the drain off THAT, never off the local
                //      struct. This also covers the case where the span
                //      lookup above threw (it is `try?`) while a row for the
                //      span existed all along.
                let naturalJobId = "fpj-\(input.analysisAssetId)-\(group.canonicalWindow.id)"
                let clash = (try? await store.fetchFinalPassJob(byId: naturalJobId)) ?? nil
                let resolvedJobId: String
                if let clash,
                   AnalysisStore.canonicalSpanKey(
                       start: clash.windowStartTime,
                       end: clash.windowEndTime
                   ) != group.canonicalSpanKey {
                    resolvedJobId = "\(naturalJobId)@\(group.canonicalSpanKey)"
                    logger.info("Final-pass: jobId \(naturalJobId, privacy: .public) is held by span [\(clash.windowStartTime, privacy: .public)..\(clash.windowEndTime, privacy: .public)]; minting span-qualified id for [\(group.canonicalWindow.startTime, privacy: .public)..\(group.canonicalWindow.endTime, privacy: .public)]")
                } else {
                    resolvedJobId = naturalJobId
                }
                let newJob = FinalPassJob(
                    jobId: resolvedJobId,
                    analysisAssetId: input.analysisAssetId,
                    podcastId: input.podcastId,
                    adWindowId: group.canonicalWindow.id,
                    windowStartTime: group.canonicalWindow.startTime,
                    windowEndTime: group.canonicalWindow.endTime,
                    status: .queued,
                    retryCount: 0,
                    deferReason: nil,
                    createdAt: now
                )
                do {
                    try await store.insertOrIgnoreFinalPassJob(newJob)
                } catch {
                    logger.warning("Final-pass: failed to insert canonical job \(newJob.jobId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                    continue
                }
                // Record every collapsed-in alias so audit visibility
                // is preserved. The canonical AdWindow's own id is
                // captured by `final_pass_jobs.adWindowId`; aliases
                // are the remaining group members.
                for aliasId in group.aliasWindowIds {
                    do {
                        try await store.recordFinalPassJobAlias(
                            jobId: newJob.jobId,
                            adWindowId: aliasId,
                            addedAt: now
                        )
                    } catch {
                        logger.warning("Final-pass: failed to record alias \(aliasId, privacy: .public) → \(newJob.jobId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                    }
                }
                // Step 2: the persisted row, not the local struct. On the
                // happy path these are identical; when they are not, the
                // table is the authority on status and retryCount.
                job = ((try? await store.fetchFinalPassJob(byId: newJob.jobId)) ?? nil) ?? newJob
            }
            // Two distinct canonical groups CAN resolve to the same
            // existing jobId in pathological cases (e.g. an upstream
            // pre-v23 row that got both canonical span keys via the
            // backfill rounding). Filter at this layer rather than
            // trusting INSERT OR IGNORE so the loop below doesn't try
            // to mark the same row running twice in a drain.
            //
            // playhead-hygc.1.5: also skip canonical jobs that are
            // already `.complete`. Without this, the runner would
            // flip a complete row → running → complete again,
            // wasting a heartbeat write and churning audit logs even
            // though the inner `coversWindow` short-circuit means no
            // ASR work runs. (The aliases were already recorded above
            // — that's the only useful side-effect of finding a
            // pre-existing complete canonical row.)
            guard job.status != .complete else { continue }
            if seenJobIds.insert(job.jobId).inserted {
                jobs.append(job)
            }
        }

        // Materialize the existing pass='final' chunks so the per-window
        // inner check can run without an extra DB hop per window.
        // skeptical-review-cycle-16 L-3: declared `let` because neither
        // branch mutates the binding after assignment.
        //
        // playhead-jzj0 moved this read from before the eligibility filter
        // to here, after the job list is known. It materializes EVERY chunk
        // for the asset (fast and final — ~1,900 rows on the 2026-08-02
        // pull's largest episode) and the launch sweep walks every asset in
        // the library on every cold start. Before this bead the watermark
        // clause usually made `eligibleWindows` empty for a converged asset,
        // so the read was rarely reached; deleting that clause would have
        // left it running unconditionally. Gating on an empty `jobs` array
        // restores the same short-circuit from the honest signal — every
        // canonical span already has a `complete` row — instead of from a
        // frontier that was never evidence of completion.
        let existingFinalChunks: [TranscriptChunk]
        if jobs.isEmpty {
            existingFinalChunks = []
        } else {
            do {
                existingFinalChunks = try await store.fetchTranscriptChunks(
                    assetId: input.analysisAssetId
                ).filter { $0.pass == TranscriptPassType.final_.rawValue }
            } catch {
                existingFinalChunks = []
            }
        }

        var admittedJobIds: [String] = []
        var reTranscribedWindowIds: [String] = []
        var deferredJobIds: [String] = []
        var maxRetranscribedEnd: Double = watermark

        for job in jobs {
            // skeptical-review-cycle-1: cooperative cancellation. If the
            // surrounding Task is cancelled (app shutdown, bootstrap
            // teardown), abandon the drain promptly instead of churning
            // through every remaining window.
            try Task.checkCancellation()
            // Re-check the gates before every window. A device unplug or
            // thermal spike mid-drain must terminate the loop promptly
            // without loading a new shard.
            if let reason = await currentDeferReason() {
                logger.info("Final-pass: gate failed mid-drain (\(reason.rawValue, privacy: .public)), deferring remaining jobs")
                for remainingJob in jobs where !admittedJobIds.contains(remainingJob.jobId)
                    && !deferredJobIds.contains(remainingJob.jobId)
                {
                    do {
                        try await store.markFinalPassJobDeferred(
                            jobId: remainingJob.jobId,
                            reason: reason.rawValue
                        )
                        deferredJobIds.append(remainingJob.jobId)
                    } catch {
                        logger.warning("Final-pass: failed to mark deferred for \(remainingJob.jobId, privacy: .public)")
                    }
                }
                break
            }

            do {
                try await store.markFinalPassJobRunning(jobId: job.jobId)
                let didRun = try await retranscribeWindow(
                    job: job,
                    input: input,
                    existingFinalChunks: existingFinalChunks,
                    inDrainCoveredIntervals: inDrainCoveredIntervals
                )
                try await store.markFinalPassJobComplete(jobId: job.jobId)
                admittedJobIds.append(job.jobId)
                if didRun {
                    reTranscribedWindowIds.append(job.adWindowId)
                    maxRetranscribedEnd = max(maxRetranscribedEnd, job.windowEndTime)
                    inDrainCoveredIntervals.append((job.windowStartTime, job.windowEndTime))
                }
            } catch is CancellationError {
                // skeptical-review-cycle-1 / cycle-4 M2: cancellation
                // must NOT be logged as a job failure. Mark the row
                // `deferred` so the next launch sweep re-admits it via
                // `markFinalPassJobRunning`'s `IN ('queued','deferred',
                // 'failed')` clause — that path bypasses the
                // stranded-row reaper's 10-minute freshness floor
                // entirely. Then rethrow to break the drain promptly.
                try? await store.markFinalPassJobDeferred(
                    jobId: job.jobId,
                    reason: "cancelled"
                )
                throw CancellationError()
            } catch let deferred as FinalPassDeferredMidWindow {
                // playhead-5147: a per-shard gate trip mid-window. The
                // current job SHOULD have already been marked deferred
                // and any partial chunks SHOULD have already been
                // persisted by `retranscribeWindow`. Do NOT call
                // `markFinalPassJobFailed` here — that would bump
                // `retryCount` on a clean defer.
                //
                // Defensive double-tap: cycle-1 H-1 — if the inner
                // `markFinalPassJobDeferred` raised (e.g. SQLite I/O
                // error), the row would otherwise be left `running`
                // here, orphaning it in EXACTLY the way this bead is
                // supposed to fix. `markFinalPassJobDeferred` is
                // idempotent on a `deferred` row (its IN-clause covers
                // `'queued', 'running', 'deferred', 'failed'`), so a
                // second call is safe whether the first one succeeded
                // (no-op apart from refreshing `updatedAt`) or failed
                // (this is the rescue). Keep it `try?` because the
                // outer rescue is itself best-effort — if BOTH writes
                // fail, the stranded-row reaper is the final safety
                // net.
                try? await store.markFinalPassJobDeferred(
                    jobId: job.jobId,
                    reason: deferred.reason.rawValue
                )
                deferredJobIds.append(job.jobId)
                logger.info("Final-pass: gate failed mid-window (\(deferred.reason.rawValue, privacy: .public)) for \(job.jobId, privacy: .public); deferring remaining siblings")
                for remainingJob in jobs where !admittedJobIds.contains(remainingJob.jobId)
                    && !deferredJobIds.contains(remainingJob.jobId)
                {
                    do {
                        try await store.markFinalPassJobDeferred(
                            jobId: remainingJob.jobId,
                            reason: deferred.reason.rawValue
                        )
                        deferredJobIds.append(remainingJob.jobId)
                    } catch {
                        logger.warning("Final-pass: failed to mark sibling deferred for \(remainingJob.jobId, privacy: .public)")
                    }
                }
                break
            } catch {
                logger.warning("Final-pass: retranscribe failed for \(job.jobId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                try? await store.markFinalPassJobFailed(
                    jobId: job.jobId,
                    reason: "retranscribeFailed"
                )
            }
        }

        // Step 5 — advance watermark monotonically. Use the max END of
        // any window that re-transcribed in this drain (or the prior
        // watermark, whichever is greater — `advanceFinalPassCoverage`
        // enforces monotonicity in SQL).
        if maxRetranscribedEnd > watermark {
            do {
                try await store.advanceFinalPassCoverage(
                    id: input.analysisAssetId,
                    endTime: maxRetranscribedEnd
                )
            } catch {
                logger.warning("Final-pass: failed to advance watermark for \(input.analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)")
            }
        }

        // playhead-hc7e — close the final-ASR loop. New `pass='final'` chunks
        // were persisted for at least one window, so schedule a detector
        // re-run / revalidation over the now-canonical transcript. Gated on
        // `!reTranscribedWindowIds.isEmpty` so an idempotent no-op re-run (or a
        // fully-deferred drain) does NOT re-trigger detection. Fires after the
        // watermark advance so a revalidation that re-enters this runner sees a
        // converged watermark. No-op when no hook was injected (preview / most
        // unit tests) — the runner stays a pure producer.
        if let onFinalPassRetranscribed, !reTranscribedWindowIds.isEmpty {
            await onFinalPassRetranscribed(
                FinalPassRevalidationRequest(
                    analysisAssetId: input.analysisAssetId,
                    podcastId: input.podcastId,
                    episodeDuration: asset.episodeDurationSec ?? 0,
                    reTranscribedWindowIds: reTranscribedWindowIds
                )
            )
        }

        return RunResult(
            admittedJobIds: admittedJobIds,
            reTranscribedWindowIds: reTranscribedWindowIds,
            deferredJobIds: deferredJobIds,
            topLevelDeferReason: nil
        )
    }

    // MARK: - Gating

    /// Returns `nil` when all admission gates pass, or the most-specific
    /// `AdmissionDeferReason` enum case when at least one gate fails.
    /// Precedence mirrors `AdmissionController.deferReason`: thermal
    /// first, then low-battery-and-unplugged (here: not-charging is the
    /// blocker), then low-power-mode.
    private func currentDeferReason() async -> AdmissionDeferReason? {
        let snapshot = await capabilitySnapshotProvider()
        let battery = await batteryLevelProvider()
        let isCharging = await chargeStateProvider()

        // Bug 9 spec — strict thermal `.nominal` floor (stricter than
        // QualityProfile's `pauseAllWork` which only fires at `.critical`).
        if snapshot.thermalState != .nominal {
            return .thermalThrottled
        }
        // Bug 9 spec — must be on charge.
        if !isCharging {
            return .batteryTooLow
        }
        // Bug 9 spec — LPM must be off.
        if snapshot.isLowPowerMode {
            return .lowPowerMode
        }
        // Defensive: respect `QualityProfile.pauseAllWork` even though
        // the strict gates above subsume it for nominal devices. If a
        // future profile change demotes nominal devices, we honor it.
        let profile = snapshot.qualityProfile(
            batteryLevel: battery,
            isCharging: isCharging
        )
        if profile.schedulerPolicy.pauseAllWork {
            return .thermalThrottled
        }
        return nil
    }

    // MARK: - Per-window retranscribe

    /// Transcribes the audio range [windowStartTime, windowEndTime] using
    /// the final-pass model and appends `pass='final'` chunks. Returns
    /// `true` when at least one shard was processed (and the watermark
    /// should advance), `false` when the window was skipped because
    /// existing `pass='final'` chunks already covered it.
    private func retranscribeWindow(
        job: FinalPassJob,
        input: AssetInput,
        existingFinalChunks: [TranscriptChunk],
        inDrainCoveredIntervals: [(Double, Double)]
    ) async throws -> Bool {
        // Inner idempotency rail: skip if existing pass='final' chunks
        // already cover this window. The outer watermark check usually
        // already bails on this, but we keep the inner check as defense.
        // skeptical-review-cycle-5 M-Y1: also OR with `inDrainCoveredIntervals`
        // — windows the prior iterations of this same drain re-transcribed.
        // The persisted snapshot is loop-stale and won't reflect them.
        let coversWindow = existingFinalChunks.contains { chunk in
            chunk.startTime <= job.windowStartTime
                && chunk.endTime >= job.windowEndTime
        } || inDrainCoveredIntervals.contains { interval in
            interval.0 <= job.windowStartTime
                && interval.1 >= job.windowEndTime
        }
        if coversWindow {
            logger.debug("Final-pass: window \(job.adWindowId, privacy: .public) already covered by pass='final' chunks")
            return false
        }

        // Load the final model once per drain; the SpeechService caches
        // model state across calls so subsequent windows reuse it.
        if await speechService.activeModelRole != .asrFinal {
            try await speechService.loadFinalModel()
        }

        // Decode the asset's audio into shards (cache-hit if the audio
        // service already has them on disk).
        let allShards = try await audioProvider.decode(
            fileURL: input.audioURL,
            episodeID: input.episodeId,
            shardDuration: AnalysisAudioService.defaultShardDuration
        )
        // Slice to shards intersecting the window. We keep an entire
        // shard even if only the tail straddles the window — Apple
        // Speech's recogniser is window-context-sensitive, and clipping
        // the shard would degrade the very accuracy we're chasing.
        let intersectingShards = allShards.filter { shard in
            let shardEnd = shard.startTime + shard.duration
            return shard.startTime < job.windowEndTime
                && shardEnd > job.windowStartTime
        }
        guard !intersectingShards.isEmpty else {
            logger.debug("Final-pass: no shards intersect window \(job.adWindowId, privacy: .public) [\(job.windowStartTime, privacy: .public)..\(job.windowEndTime, privacy: .public)]")
            return false
        }

        var newChunks: [TranscriptChunk] = []
        var nextChunkIndex = await nextFinalChunkIndex(
            forAsset: input.analysisAssetId
        )
        // skeptical-review-cycle-3 M-E: lease-freshness heartbeat. The
        // stranded-row reaper flips a `running` row back to `queued`
        // after `strandedJobFreshnessSeconds` (10 min) without a touch.
        // On degraded hardware a multi-shard window can exceed that
        // floor, opening a duplicate-FM-call race window. Refreshing
        // `updatedAt` between shards keeps the lease alive without
        // changing semantics — `markFinalPassJobRunning` is idempotent
        // for an already-`running` row at the same jobId.
        var lastHeartbeatTick = ContinuousClock.now
        // cycle-1 L5: jitter the heartbeat interval by ±10% (±30s)
        // around the 300s base. Without jitter, two runners that
        // started in lockstep (e.g. two devices that synced their
        // schema migration on the same wall clock) would heartbeat
        // every 300s in lockstep, doubling the database write rate
        // at predictable beat moments. The random offset spreads
        // those writes across a 60s window. Re-rolled per cycle so
        // a long shard chain doesn't stay phase-locked.
        var heartbeatInterval = Self.jitteredHeartbeatInterval()
        for shard in intersectingShards {
            // skeptical-review-cycle-1: cooperative cancellation in the
            // hot per-shard loop. Speech transcription is the heaviest
            // operation in the runner; a missed cancellation here lets a
            // shutdown stall on a 30 s shard.
            //
            // Cancellation is checked BEFORE the thermal gate. If the
            // surrounding Task is already cancelled (e.g. app shutdown)
            // and the device is also throttling, callers expect a
            // `CancellationError` (the existing contract honored by
            // `runFinalPassBackfill`'s `catch is CancellationError`),
            // not a `FinalPassDeferredMidWindow`. Cancellation is
            // strictly stronger — once the Task is cancelled, no more
            // work should happen, period.
            try Task.checkCancellation()
            // playhead-5147: per-shard cooperative thermal/charge/LPM
            // check. Without this, a window that started on a nominal
            // device and warmed mid-transcribe would burn through every
            // remaining shard before the OUTER per-window gate could
            // notice. Worst case the row is left `running` until the
            // 10-min stranded-row reaper unsticks it on next cold
            // launch, surfacing as user-visible "doesn't finish
            // processing".
            //
            // Exit path on a tripped gate:
            //   1. Persist any chunks accumulated so far so partial
            //      work isn't lost (next admission wave re-enters the
            //      same window and de-dupes via segmentFingerprint).
            //   2. Mark THIS job `.deferred` with the gate reason.
            //   3. Throw `FinalPassDeferredMidWindow` so the outer
            //      catch site can defer remaining siblings WITHOUT
            //      bumping retryCount on the deferred row (which is
            //      what `markFinalPassJobFailed` would do).
            if let reason = await currentDeferReason() {
                logger.info("Final-pass: gate failed mid-window (\(reason.rawValue, privacy: .public)) for \(job.adWindowId, privacy: .public); persisting \(newChunks.count, privacy: .public) partial chunks and deferring")
                if !newChunks.isEmpty {
                    do {
                        try await store.insertTranscriptChunks(newChunks)
                    } catch {
                        logger.warning("Final-pass: failed to persist partial chunks on mid-window defer for \(job.adWindowId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                    }
                }
                // Mark the row deferred BEFORE throwing so the outer
                // catch path does not need to know whether this row's
                // bookkeeping was already done. (`markFinalPassJobDeferred`
                // is idempotent on a `deferred` row — see its IN-clause.)
                do {
                    try await store.markFinalPassJobDeferred(
                        jobId: job.jobId,
                        reason: reason.rawValue
                    )
                } catch {
                    logger.warning("Final-pass: failed to mark job deferred on mid-window gate trip for \(job.jobId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                }
                throw FinalPassDeferredMidWindow(reason: reason)
            }
            if ContinuousClock.now - lastHeartbeatTick >= heartbeatInterval {
                try? await store.markFinalPassJobRunning(jobId: job.jobId)
                lastHeartbeatTick = ContinuousClock.now
                // Re-jitter the next interval so a long shard chain
                // doesn't stay phase-locked to the first roll.
                heartbeatInterval = Self.jitteredHeartbeatInterval()
            }
            let segments = try await speechService.transcribe(
                shard: shard,
                podcastId: input.podcastId
            )
            for segment in segments {
                let chunk = TranscriptChunk(
                    id: UUID().uuidString,
                    analysisAssetId: input.analysisAssetId,
                    segmentFingerprint: Self.computeFinalPassFingerprint(
                        text: segment.text,
                        startTime: segment.startTime,
                        endTime: segment.endTime
                    ),
                    chunkIndex: nextChunkIndex,
                    startTime: segment.startTime,
                    endTime: segment.endTime,
                    text: segment.text,
                    // playhead-gjxf: THE CANONICAL NORMALIZER, not `.lowercased()`.
                    //
                    // This line read `segment.text.lowercased()` and therefore
                    // stored RAW text in the column whose name says normalized.
                    // Lowercasing is only the FIRST of the three things
                    // `TranscriptEngineService.normalizeText` does — it also
                    // strips every non-alphanumeric scalar and collapses the
                    // result on single spaces. Measured on the 2026-08-15 pull:
                    // 3,825 of the 24,880 `pass='final'` rows on disk held text
                    // this column is not supposed to hold, and all 3,825 are
                    // this writer's (`modelVersion='apple-speech-final-v1'`).
                    // The other 21,055 were "correct" only because their text
                    // happened to carry no punctuation — which is also why no
                    // fixture built from unpunctuated text can see this bug.
                    //
                    // It matters because `LexicalScanner.scanChunk` runs every
                    // built-in pattern group over `normalizedText`, and those
                    // patterns are written FOR normalized text (`go to \w+ com`,
                    // `\w+ com slash \w+`). Against a stored `wonderfulpistachios.com`
                    // they cannot match; against `wonderfulpistachios com` they
                    // can. `TranscriptChunkCanonicalizer` retains the FINAL row
                    // of a fast/final twin, so the un-normalized value is the one
                    // detection actually reads.
                    //
                    // `TranscriptEngineService` — which also writes `pass='final'`
                    // rows, 17,632 of them on that same pull — has always called
                    // the canonical normalizer here, and every one of its rows is
                    // correct. This is the one writer that diverged.
                    //
                    // Rows written before this fix are repaired by AnalysisStore
                    // schema V54, which calls this same function.
                    normalizedText: TranscriptEngineService.normalizeText(segment.text),
                    pass: TranscriptPassType.final_.rawValue,
                    modelVersion: modelVersion,
                    transcriptVersion: nil,
                    atomOrdinal: nil,
                    weakAnchorMetadata: segment.weakAnchorMetadata,
                    speakerId: segment.speakerId,
                    avgConfidence: segment.avgConfidence
                )
                // Skip insert if this asset already holds this SPAN AND TEXT in
                // the final pass (idempotent re-run guard), but still let a
                // later ASR result fill diarization/confidence an earlier run
                // lacked.
                //
                // playhead-jc42: this used to ask
                // `fetchTranscriptChunk(analysisAssetId:segmentFingerprint:)`
                // with `chunk.segmentFingerprint`, and that question could only
                // ever find rows THIS RUNNER wrote. `TranscriptEngineService`
                // also emits `pass='final'` rows, digesting the identical
                // `"\(text)|\(start)|\(end)"` WITHOUT the `fp-final-` prefix, so
                // its row for the very same audio was invisible here — as it was
                // to the `(asset, pass, segmentFingerprint)` UNIQUE index. Every
                // span the engine had already finalised got appended a second
                // time, and `TranscriptChunkCanonicalizer` keeps both (it
                // de-overlaps fast against final, never final against final), so
                // the overlay drew each one twice. Measured on the 2026-08-15
                // pull: 7,248 duplicated spans across all ten assets that had
                // ever run a final pass, 7,247 of them byte-identical in text
                // and timing; 3,496 were two `pass='final'` rows.
                //
                // The lookup is scoped to THIS pass on purpose. A fast row over
                // the same span is a different fact, read by a different
                // consumer (`readFastTranscriptRegions` vs
                // `readFinalTranscriptRegions`); matching it would make the
                // runner believe it had already stored a final row it had not,
                // and final-pass coverage would silently stop growing.
                //
                // The metadata upgrades are keyed on the EXISTING row's
                // fingerprint, not on `chunk.segmentFingerprint` — the whole
                // point is that the row we found may have been written by the
                // other producer under the other scheme, in which case
                // `chunk.segmentFingerprint` addresses nothing and both
                // upgrades silently update zero rows.
                //
                // One widening this carries, deliberately: the two update
                // helpers key on `(asset, segmentFingerprint)` WITHOUT `pass`,
                // so when the row we found was engine-written its fast twin
                // shares that fingerprint and is filled too. Both are
                // `…IfMissing` — they only ever write over a NULL, with a value
                // measured from the same text over the same span — so that is
                // the right answer for the fast row as well. Under the old
                // prefixed lookup it could not arise because only runner rows
                // could ever match.
                let existingFinalChunk = try? await store.fetchTranscriptChunkBySpanText(
                    analysisAssetId: input.analysisAssetId,
                    pass: TranscriptPassType.final_.rawValue,
                    startTime: segment.startTime,
                    endTime: segment.endTime,
                    text: segment.text
                )
                if let existing = existingFinalChunk {
                    if let speakerId = segment.speakerId {
                        _ = try await store.updateTranscriptChunkSpeakerIdIfMissing(
                            analysisAssetId: input.analysisAssetId,
                            segmentFingerprint: existing.segmentFingerprint,
                            speakerId: speakerId
                        )
                    }
                    _ = try await store.updateTranscriptChunkAvgConfidenceIfMissing(
                        analysisAssetId: input.analysisAssetId,
                        segmentFingerprint: existing.segmentFingerprint,
                        avgConfidence: segment.avgConfidence
                    )
                } else {
                    newChunks.append(chunk)
                    nextChunkIndex += 1
                }
            }
        }

        if !newChunks.isEmpty {
            try await store.insertTranscriptChunks(newChunks)
            logger.info("Final-pass: appended \(newChunks.count, privacy: .public) pass='final' chunks for window \(job.adWindowId, privacy: .public)")
        } else {
            logger.debug("Final-pass: no new chunks produced for window \(job.adWindowId, privacy: .public) — likely silence or all segments deduped")
        }
        return true
    }

    /// Compute the next `chunkIndex` for newly-inserted final-pass rows.
    /// Final-pass rows live alongside fast-pass rows in the same table;
    /// we pick a chunkIndex that is strictly greater than the highest
    /// existing index for the asset so positional ordering in
    /// `fetchTranscriptChunks(assetId:)` interleaves correctly.
    private func nextFinalChunkIndex(forAsset assetId: String) async -> Int {
        let existing = (try? await store.fetchTranscriptChunks(assetId: assetId)) ?? []
        return (existing.map { $0.chunkIndex }.max() ?? -1) + 1
    }

    /// Fingerprint scheme for final-pass chunks. Uses SHA-256 over a
    /// final-pass-prefixed key so the fingerprint is **stable across
    /// process launches** (Swift's `Hasher` re-seeds per process and
    /// would silently break the cross-launch idempotency guard). The
    /// prefix `fp-final-` cannot collide with `TranscriptEngineService`'s
    /// scheme (which uses `text|start|end` without a prefix).
    ///
    /// ⚠️ playhead-jc42 — READ THE NON-COLLISION AS A HAZARD, NOT A FEATURE.
    /// This comment used to end "Two chunks with identical text and timing but
    /// different passes therefore hash to different fingerprints — both rows
    /// persist, neither is confused for a duplicate of the other", and the
    /// premise it rests on ("but different passes") is not something the
    /// prefix can check. `TranscriptEngineService` writes `pass='final'` rows
    /// too, whenever `activeModelRole == .asrFinal`. So the guarantee actually
    /// delivered was that a runner row can never be recognised as a duplicate
    /// of an ENGINE row IN THE SAME PASS — which is how 3,496 doubled
    /// `pass='final'` spans reached the 2026-08-15 device pull while the
    /// `(asset, pass, segmentFingerprint)` UNIQUE index reported zero
    /// violations.
    ///
    /// The prefix is KEPT because cross-launch stability is a real requirement
    /// and rehashing every persisted row to drop it would buy nothing:
    /// deduplication no longer runs through this value at all. Identity is
    /// `(asset, pass, startTime, endTime, text)` —
    /// `AnalysisStore.fetchTranscriptChunkBySpanText` and
    /// `idx_chunks_asset_pass_span_text`. This is a row id now.
    static func computeFinalPassFingerprint(
        text: String,
        startTime: Double,
        endTime: Double
    ) -> String {
        let key = "fp-final-\(text)|\(startTime)|\(endTime)"
        let digest = SHA256.hash(data: Data(key.utf8))
        return digest.prefix(16).map { String(format: "%02x", $0) }.joined()
    }

    // MARK: - cycle-1 L5: heartbeat jitter

    /// Base heartbeat interval (seconds). Half the
    /// `strandedJobFreshnessSeconds` reaper floor (10 min) so a
    /// heartbeat fires twice per reaper cycle in the worst case,
    /// preventing a long shard chain from being mistakenly reaped as
    /// stranded.
    static let heartbeatBaseSeconds: Double = 300

    /// Maximum absolute jitter in seconds added to or subtracted from
    /// `heartbeatBaseSeconds`. ±30s = ±10% of the 300s base. Wide
    /// enough to spread heartbeat writes across a 60s window when
    /// many runners start in lockstep, narrow enough that the
    /// effective single-beat interval stays below the
    /// `AnalysisStore.strandedJobFreshnessSeconds` reaper floor
    /// (currently 600s — see `AnalysisStore.swift:767`).
    ///
    /// **Binding invariant** (review/v0.5-head-polish C3 M-2):
    ///   `heartbeatBaseSeconds + heartbeatJitterSeconds < strandedJobFreshnessSeconds`
    /// Today: `300 + 30 = 330 < 600` ✓
    ///
    /// The single-beat invariant alone is what guarantees a heartbeat
    /// row is never written *after* the reaper would already consider
    /// it stranded. The two-beats-vs-floor case (worst-case 660s
    /// between consecutive heartbeats) is a separate concern, but the
    /// reaper at `AnalysisStore.resetStrandedJobs` does NOT fire on a
    /// strict `strandedJobFreshnessSeconds` cadence — it runs at boot
    /// and when explicitly re-checked. So a 660s gap is tolerated as
    /// long as the next heartbeat lands before the next reaper sweep.
    /// If the reaper is ever wired to a wall-clock timer, this margin
    /// disappears and `heartbeatJitterSeconds` must be re-derived so
    /// that `2 * (heartbeatBaseSeconds + heartbeatJitterSeconds) <
    /// strandedJobFreshnessSeconds` also holds.
    static let heartbeatJitterSeconds: Double = 30

    /// Build a jittered heartbeat interval in [base-jitter, base+jitter].
    /// Uses the system RNG so each runner cycle re-rolls independently.
    /// Pure-functional variant `computeHeartbeatInterval(base:jitter:roll:)`
    /// below is what tests exercise — passing a deterministic `roll` in
    /// `[-1, +1]` lets the bounds be asserted without RNG flakiness.
    static func jitteredHeartbeatInterval() -> Duration {
        // Roll is normalized in [-1, +1]; computeHeartbeatInterval handles
        // the actual base ± jitter math and clamps any out-of-range input.
        let roll = Double.random(in: -1.0...1.0)
        return computeHeartbeatInterval(
            base: heartbeatBaseSeconds,
            jitter: heartbeatJitterSeconds,
            roll: roll
        )
    }

    /// Pure helper: `base + roll * jitter`, returned as a `Duration`.
    /// `roll` must be in `[-1, +1]`; values outside that range are
    /// clamped so a buggy caller can't produce a negative or
    /// reaper-floor-crossing interval.
    ///
    /// Test contract:
    ///   - `roll = -1` ⇒ 270s
    ///   - `roll = 0`  ⇒ 300s
    ///   - `roll = +1` ⇒ 330s
    static func computeHeartbeatInterval(
        base: Double,
        jitter: Double,
        roll: Double
    ) -> Duration {
        let clampedRoll = min(1.0, max(-1.0, roll))
        let seconds = base + clampedRoll * jitter
        return Duration.seconds(seconds)
    }
}

// MARK: - FinalPassJob

/// Persisted row in `final_pass_jobs`. Mirrors `BackfillJob` but uses a
/// per-AdWindow grain rather than per-asset.
struct FinalPassJob: Sendable, Equatable {
    let jobId: String
    let analysisAssetId: String
    let podcastId: String?
    let adWindowId: String
    let windowStartTime: Double
    let windowEndTime: Double
    let status: BackfillJobStatus
    let retryCount: Int
    let deferReason: String?
    let createdAt: Double
}
