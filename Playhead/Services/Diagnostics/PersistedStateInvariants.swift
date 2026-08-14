// PersistedStateInvariants.swift
// playhead-dgly — REPORT every persisted terminal state that is no longer
// true. Nothing here repairs anything; healing is playhead-gyhw and is
// sequenced behind this file on purpose.
//
// ----- Why a reporter, and why it may not heal -----
//
// Six beads found one shape: a state written once, believed forever, with no
// evidence-based path out. `playhead-1e86` (a hard-killed job left
// `status='running'` is invisible to the next grant), `playhead-wogi` (the
// coarse cursor advances over audio nothing scanned), `playhead-e6d3` (the
// coverage budget retires a job that was progressing), `playhead-1216`,
// `playhead-exy0`, `playhead-8ysk`. Every path into them is reachable by a
// listener who never sees a dev build: force-quit mid-scan, battery dies
// mid-pass, iOS reclaims the window.
//
// A reconciler that silently repaired 3C2FFE10's cursor last week would have
// HIDDEN `playhead-wogi` — we would have a phone that quietly re-scans and an
// unexplained FM bill, which is `playhead-ejr7`'s "41 % of FM compute produces
// nothing" arriving through a different door. Healing is earned by diagnosis.
//
// ----- The anti-vacuity contract -----
//
// REPORTING ZERO VIOLATIONS IS A POSITIVE CLAIM, NOT AN ABSENCE OF CHECKING.
// Two properties enforce it:
//
//  1. Every invariant emits a CENSUS line whether or not it fired, carrying
//     the numerator AND the denominator. A reporter that never ran emits no
//     lines at all, which is a different observation from `violations=0`, and
//     a reader can tell them apart. This is `playhead-isp5`'s
//     `ad_window_ingest_census` argument and `playhead-oa82`'s
//     `rediff_day_zero_kickoff_claim_attempted` argument, applied a third
//     time: only a row that is ALWAYS present can distinguish "the check ran
//     and found nothing" from "the check never ran".
//  2. `PersistedStateInvariant.allCases` is what the evaluator iterates, and
//     the evaluator returns one finding per case unconditionally. An invariant
//     whose predicate is deleted still reports — with `population=0`, which is
//     itself a claim a reader can challenge. `playhead-wwbr`'s canary read
//     `object["testTargets"] as? [[String: Any]] ?? []` and so passed on an
//     empty world for four months; the shape to avoid is a check whose
//     denominator can silently become zero without saying so.
//
// ----- What is NOT here -----
//
// No repair. No reset. No migration. If you find yourself writing one, it
// belongs in playhead-gyhw.

import Foundation

// MARK: - The invariants

/// The persisted-state invariants this reporter checks, one case each.
///
/// `CaseIterable` is load-bearing: ``PersistedStateInvariantEvaluator/evaluate(_:)``
/// iterates `allCases` and emits a finding for every one of them, so adding a
/// case without wiring its predicate produces a visibly empty census rather
/// than a silent gap.
enum PersistedStateInvariant: String, Sendable, Hashable, CaseIterable {

    /// **playhead-1e86.** A `backfill_jobs` row observed at `status='running'`
    /// at process start, before this process has reconciled anything.
    ///
    /// * QUANTITY: the count of coverage-lane rows claiming to be running.
    /// * WITNESS: the jobId, its asset, and how long ago the row was touched.
    /// * NULL READING: **zero**. No job of THIS process can be running yet —
    ///   the reporter runs before the scheduler loop starts — so every such
    ///   row is the corpse of a process iOS already killed. A run that ends
    ///   gracefully writes `queued` / `deferred` / `failed` / `complete`.
    ///
    /// Why it is terminal rather than transient: both candidate queries the
    /// FM/coarse phase uses exclude `status='running'`
    /// (`fetchAssetIdsWithResumableBackfillJobs` binds `status <> 'running'`;
    /// `fetchAssetIdsMissingCoverageLaneJobs` requires `NOT EXISTS` any row),
    /// and `handleBackfillTask` does not run the reaper. Two backfill grants
    /// with no launch, no scene activation and no pre-analysis recovery
    /// between them and the asset is simply absent from the sweep.
    case strandedRunningBackfillJob = "stranded_running_backfill_job"

    /// **playhead-wogi.** A coarse cursor claiming more of the episode than
    /// the asset's own EXAMINED `passA` rows support.
    ///
    /// * QUANTITY: `progressCursor.lastProcessedUpperBoundSec` minus
    ///   ``AnalysisCoverageMath/supportedScannedPrefix(examinedSpans:rescanThreshold:)``
    ///   — the same walk `playhead-wogi`'s V51 migration uses, deliberately
    ///   not a second expression of it.
    /// * WITNESS: jobId, asset, claimed, supported, excess seconds.
    /// * NULL READING: **zero**. A cursor is published only over audio a walk
    ///   actually covered, so `claimed <= supported` holds by construction
    ///   whenever the publication is sound. The 2026-08-14 pull's witness is
    ///   3C2FFE10: claimed 7,998.72 s, supported 659.46 s, excess 7,339.26 s
    ///   on a 7,999 s episode.
    ///
    /// Rows whose asset has NO examined `passA` row are EXCLUDED from the
    /// denominator rather than counted as clean — the cursor is supported by
    /// nothing in either direction, which is `playhead-5pyq`'s shape rather
    /// than this one. The excluded count travels in the census line so the
    /// denominator is never quietly narrowed.
    case coarseCursorBeyondScannedPrefix = "coarse_cursor_beyond_scanned_prefix"

    /// **playhead-e6d3** (forward fix merged; this is now a regression
    /// tripwire). A coverage-lane job retired at the retry cap while its own
    /// resume would still plan audio.
    ///
    /// * QUANTITY: the asset's transcript reach minus the job's cursor,
    ///   compared against ``RescanThresholdSec/adScanRescanWorthyGapSec``
    ///   (60 s — the same width that decides a hole is worth paying FM
    ///   wall-clock for).
    /// * WITNESS: jobId, asset, cursor, transcript reach, remaining seconds.
    /// * NULL READING: **zero**. Post-e6d3 `retryCount` counts CONSECUTIVE
    ///   attempts that did not advance the cursor, and e6d3's own saturating
    ///   argument is the legitimate way to reach the cap: once the cursor
    ///   reaches the last segment, `narrowedForResume` empties the plan list
    ///   and three attempts run no inference at all. So a job legitimately at
    ///   the cap has nothing left above its cursor, and `remaining` is zero.
    ///
    /// LIMIT, stated rather than hidden: the persisted row carries the FINAL
    /// cursor, not one per attempt, so "coverage was climbing across its
    /// attempts" is not directly readable from a snapshot. What IS readable is
    /// that the row is dead with reachable work remaining, which is the
    /// consequence the bead cares about. The transcript reach is a PROXY for
    /// what `narrowedForResume` would plan — it is the watermark, not the
    /// segment list — and it can only over-state the remainder on an asset
    /// whose transcript is gappy, so a violation is evidence and a zero is
    /// weaker evidence than it looks.
    case retryBudgetSpentWithWorkRemaining = "retry_budget_spent_with_work_remaining"

    /// **playhead-1216** (fixed; regression tripwire). An `analysis_assets` row
    /// still in the `new` registration state while its audio is on disk and
    /// its newest `analysis_jobs` row carries a terminal error.
    ///
    /// * QUANTITY: the count of such rows.
    /// * WITNESS: asset, episode, the job's state and `lastErrorCode`.
    /// * NULL READING: **zero**. Registration writes the asset row when the
    ///   bytes land (`playhead-fzrw`) and the pipeline drives it out of `new`
    ///   on the first pass; an asset that owns audio and has been worked on
    ///   cannot honestly still be in the state that means "nothing has looked
    ///   at this yet". That state drew the same library glyph as "no audio",
    ///   which is why the 2026-08-13 report read as a lost download.
    case newAssetWithAudioAndFailedJob = "new_asset_with_audio_and_failed_job"

    /// **playhead-exy0.** An `ad_windows` row that is `eligible` and belongs to
    /// a detector class seeded `.auto` independently of the show, which no
    /// delivery door has ever recorded touching.
    ///
    /// * QUANTITY: rows at `decisionState='candidate'` with `wasSkipped=0` and
    ///   `userDismissedBanner=0`, over the eligible + show-independent-auto
    ///   population.
    /// * WITNESS: window id, asset, span, boundary state, both edge anchors.
    /// * NULL READING: **zero**. Every delivery door leaves a mark on the row:
    ///   `decisionState` moves off `candidate` (to `confirmed` / `applied` /
    ///   `suppressed`), or `wasSkipped` / `userDismissedBanner` flips. A row
    ///   that was ever offered cannot be in this set, so a non-zero reading
    ///   says the population never arrived at a door — which is exactly what
    ///   exy0 established on the 2026-08-14 pull, where all four byte-exact
    ///   day-0 marks were minted while nothing was playing and dropped by
    ///   `ingest_door_dropped_not_playing`.
    ///
    /// The class is resolved through the SHARED
    /// ``SkipDetectorClass/classify(boundaryState:startAnchor:endAnchor:)``
    /// rather than by re-spelling `dayZeroRediffByteExact` here — 6qvf's
    /// lesson, a second expression that happens to agree is how the certainty
    /// tier and its consumers came apart.
    case eligibleAutoWindowNeverOffered = "eligible_auto_window_never_offered"
}

// MARK: - Snapshot

/// The persisted state the evaluator judges, read once and judged purely.
///
/// Deliberately a VALUE: the read happens in ``AnalysisStore``, the judgement
/// happens here, and a test can construct a snapshot field-for-field from a
/// device pull without a database. That split is what makes a mutation of the
/// predicate provable — a check that can only be exercised through SQLite is a
/// check nobody re-runs.
struct PersistedStateSnapshot: Sendable, Equatable {

    /// One `backfill_jobs` row, reduced to the columns the invariants read.
    struct BackfillJobRow: Sendable, Equatable {
        let jobId: String
        let assetId: String
        let status: String
        let retryCount: Int
        let deferReason: String?
        /// Seconds since the epoch, `backfill_jobs.updatedAt`.
        let updatedAt: Double
        /// `progressCursor.lastProcessedUpperBoundSec`, or nil when the row
        /// carries no cursor at all.
        let claimedUpperBoundSec: Double?

        init(
            jobId: String,
            assetId: String,
            status: String,
            retryCount: Int,
            deferReason: String?,
            updatedAt: Double,
            claimedUpperBoundSec: Double?
        ) {
            self.jobId = jobId
            self.assetId = assetId
            self.status = status
            self.retryCount = retryCount
            self.deferReason = deferReason
            self.updatedAt = updatedAt
            self.claimedUpperBoundSec = claimedUpperBoundSec
        }
    }

    /// One `analysis_assets` row plus the two derived quantities the reader
    /// computes in the store: the supported scanned prefix and the transcript
    /// reach.
    struct AssetRow: Sendable, Equatable {
        let assetId: String
        let episodeId: String
        let analysisState: String
        /// `AnalysisCoverageMath.supportedScannedPrefix` over this asset's
        /// EXAMINED `passA` rows. `nil` means the asset has no examined row at
        /// all — no evidence in either direction, which is NOT the same as a
        /// prefix of zero and must not be read as one.
        let supportedScannedPrefixSec: Double?
        /// The transcript high-water mark: the larger of
        /// `fastTranscriptCoverageEndTime` and `finalPassCoverageEndTime`.
        /// A WATERMARK, not an area — see the limit on
        /// ``PersistedStateInvariant/retryBudgetSpentWithWorkRemaining``.
        let transcriptReachSec: Double?
        /// Resolved by the reporter for assets in a registration state only,
        /// through the download manager's own cache oracle. `nil` means the
        /// question was not asked (because the asset is not in that state).
        let hasAudioOnDisk: Bool?
        /// The newest `analysis_jobs` row for this asset, when one exists.
        /// Read only for assets in a registration state.
        let newestJobState: String?
        let newestJobLastErrorCode: String?

        init(
            assetId: String,
            episodeId: String,
            analysisState: String,
            supportedScannedPrefixSec: Double?,
            transcriptReachSec: Double?,
            hasAudioOnDisk: Bool? = nil,
            newestJobState: String? = nil,
            newestJobLastErrorCode: String? = nil
        ) {
            self.assetId = assetId
            self.episodeId = episodeId
            self.analysisState = analysisState
            self.supportedScannedPrefixSec = supportedScannedPrefixSec
            self.transcriptReachSec = transcriptReachSec
            self.hasAudioOnDisk = hasAudioOnDisk
            self.newestJobState = newestJobState
            self.newestJobLastErrorCode = newestJobLastErrorCode
        }

        /// Fill in the newest `analysis_jobs` row. Read only for assets in a
        /// registration state, so the field stays `nil` for everything else and
        /// the invariant abstains rather than guessing.
        func resolvingNewestJob(state: String, lastErrorCode: String?) -> AssetRow {
            AssetRow(
                assetId: assetId,
                episodeId: episodeId,
                analysisState: analysisState,
                supportedScannedPrefixSec: supportedScannedPrefixSec,
                transcriptReachSec: transcriptReachSec,
                hasAudioOnDisk: hasAudioOnDisk,
                newestJobState: state,
                newestJobLastErrorCode: lastErrorCode
            )
        }

        /// Fill in whether the episode's audio is on disk. The reporter asks
        /// the download manager's own cache oracle — the same one the library
        /// asks — rather than re-deriving "is it downloaded" from the store,
        /// because the whole point of the `playhead-1216` shape is that two
        /// readers disagreed about it.
        func resolvingAudioPresence(_ present: Bool) -> AssetRow {
            AssetRow(
                assetId: assetId,
                episodeId: episodeId,
                analysisState: analysisState,
                supportedScannedPrefixSec: supportedScannedPrefixSec,
                transcriptReachSec: transcriptReachSec,
                hasAudioOnDisk: present,
                newestJobState: newestJobState,
                newestJobLastErrorCode: newestJobLastErrorCode
            )
        }
    }

    /// One `ad_windows` row, reduced to what the delivery-door invariant reads.
    struct AdWindowRow: Sendable, Equatable {
        let windowId: String
        let assetId: String
        let startTime: Double
        let endTime: Double
        let boundaryState: String
        let decisionState: String
        let eligibilityGate: String?
        let startEdgeAnchor: String
        let endEdgeAnchor: String
        let wasSkipped: Bool
        let userDismissedBanner: Bool

        init(
            windowId: String,
            assetId: String,
            startTime: Double,
            endTime: Double,
            boundaryState: String,
            decisionState: String,
            eligibilityGate: String?,
            startEdgeAnchor: String,
            endEdgeAnchor: String,
            wasSkipped: Bool,
            userDismissedBanner: Bool
        ) {
            self.windowId = windowId
            self.assetId = assetId
            self.startTime = startTime
            self.endTime = endTime
            self.boundaryState = boundaryState
            self.decisionState = decisionState
            self.eligibilityGate = eligibilityGate
            self.startEdgeAnchor = startEdgeAnchor
            self.endEdgeAnchor = endEdgeAnchor
            self.wasSkipped = wasSkipped
            self.userDismissedBanner = userDismissedBanner
        }
    }

    let backfillJobs: [BackfillJobRow]
    let assets: [AssetRow]
    /// Only rows with a non-nil `eligibilityGate` are read — the population the
    /// delivery-door invariant is about. The census reports the count it saw,
    /// so a filter that goes wrong shows up as a shrinking denominator rather
    /// than as a clean zero.
    let eligibilityGatedAdWindows: [AdWindowRow]
    /// The retry cap the coverage lane admits against, carried by value so the
    /// evaluator returns the admission model's answer rather than one of its
    /// own.
    let coverageLaneRetryCap: Int

    init(
        backfillJobs: [BackfillJobRow],
        assets: [AssetRow],
        eligibilityGatedAdWindows: [AdWindowRow],
        coverageLaneRetryCap: Int
    ) {
        self.backfillJobs = backfillJobs
        self.assets = assets
        self.eligibilityGatedAdWindows = eligibilityGatedAdWindows
        self.coverageLaneRetryCap = coverageLaneRetryCap
    }
}

// MARK: - Findings

/// One invariant's verdict: a numerator, a denominator, and the witnesses.
struct PersistedStateInvariantFinding: Sendable, Equatable {
    let invariant: PersistedStateInvariant
    /// How many rows VIOLATE. Always the true count over the whole population —
    /// never the length of ``witnesses``, which is capped.
    let violations: Int
    /// How many rows were JUDGED. Excludes rows the invariant deliberately
    /// abstains on; that count travels in ``abstained``.
    let population: Int
    /// Rows the invariant could not judge, with the reason. Reported rather
    /// than folded into either side, because a check that quietly narrows its
    /// own denominator reports a healthy ratio over a population it chose.
    let abstained: Int
    let abstainReason: String?
    /// Human-readable witness lines, capped at
    /// ``PersistedStateInvariantEvaluator/maxWitnessesPerInvariant``. The cap
    /// is declared in the census line so a reader knows the list is partial
    /// while the count above is not.
    let witnesses: [String]

    var isClean: Bool { violations == 0 }
}

// MARK: - Evaluator

/// Pure judgement over a ``PersistedStateSnapshot``. No I/O, no clock, no
/// actor state — every input arrives in the snapshot so a test can reproduce a
/// device pull field-for-field and a mutation of any predicate is visible.
enum PersistedStateInvariantEvaluator {

    /// How many witness lines one invariant may emit. The COUNT in the census
    /// is never capped; only the enumeration is.
    static let maxWitnessesPerInvariant = 8

    /// Float slack for comparing two persisted seconds values that were
    /// written by different writers. Small enough that the smallest violation
    /// on the 2026-08-14 pull (7,339.26 s) clears it by five orders of
    /// magnitude, and large enough that a cursor and a watermark written from
    /// the same walk do not disagree by rounding.
    static let secondsEpsilon = 0.001

    /// States an `analysis_assets` row can hold that mean "registered, nothing
    /// has advanced it yet". Read off ``AnalysisAsset/registeredNotQueuedState``
    /// rather than spelled here: playhead-fzrw named that value precisely so
    /// "a row exists so day-0 can resolve an A-side" would stop sharing a token
    /// with "the lane is working toward this", and a second spelling would put
    /// the two back together. Every `SessionState` case is a state the pipeline
    /// wrote deliberately and is therefore NOT in this set.
    static let registrationStates: Set<String> = [AnalysisAsset.registeredNotQueuedState]

    /// Terminal `analysis_jobs` states that mean the lane gave up.
    static let failedJobStates: Set<String> = ["failed", "cancelled"]

    /// Judge every invariant. Returns exactly `PersistedStateInvariant.allCases.count`
    /// findings, in `allCases` order, whether or not any fired.
    static func evaluate(_ snapshot: PersistedStateSnapshot) -> [PersistedStateInvariantFinding] {
        PersistedStateInvariant.allCases.map { invariant in
            switch invariant {
            case .strandedRunningBackfillJob:
                return strandedRunningBackfillJob(snapshot)
            case .coarseCursorBeyondScannedPrefix:
                return coarseCursorBeyondScannedPrefix(snapshot)
            case .retryBudgetSpentWithWorkRemaining:
                return retryBudgetSpentWithWorkRemaining(snapshot)
            case .newAssetWithAudioAndFailedJob:
                return newAssetWithAudioAndFailedJob(snapshot)
            case .eligibleAutoWindowNeverOffered:
                return eligibleAutoWindowNeverOffered(snapshot)
            }
        }
    }

    // MARK: Invariant 1

    /// The status string a coverage-lane row holds while a runner owns it.
    /// Spelled once here rather than at each comparison.
    static let runningBackfillStatus = "running"

    private static func strandedRunningBackfillJob(
        _ snapshot: PersistedStateSnapshot
    ) -> PersistedStateInvariantFinding {
        let offenders = snapshot.backfillJobs.filter { $0.status == runningBackfillStatus }
        return PersistedStateInvariantFinding(
            invariant: .strandedRunningBackfillJob,
            violations: offenders.count,
            population: snapshot.backfillJobs.count,
            abstained: 0,
            abstainReason: nil,
            witnesses: offenders.prefix(maxWitnessesPerInvariant).map { job in
                "job=\(job.jobId) asset=\(job.assetId) status=\(job.status)"
                    + " updated_at=\(format(job.updatedAt))"
            }
        )
    }

    // MARK: Invariant 2

    private static func coarseCursorBeyondScannedPrefix(
        _ snapshot: PersistedStateSnapshot
    ) -> PersistedStateInvariantFinding {
        let prefixByAsset = Dictionary(
            snapshot.assets.map { ($0.assetId, $0.supportedScannedPrefixSec) },
            uniquingKeysWith: { first, _ in first }
        )
        var violations: [String] = []
        var judged = 0
        var abstained = 0
        for job in snapshot.backfillJobs {
            guard let claimed = job.claimedUpperBoundSec, claimed.isFinite else { continue }
            // No examined coverage-lane row for this asset ⇒ no evidence in
            // EITHER direction. Abstaining is V51's own carve-out and the
            // reason is reported rather than absorbed.
            guard let supported = prefixByAsset[job.assetId] ?? nil else {
                abstained += 1
                continue
            }
            judged += 1
            guard claimed > supported + secondsEpsilon else { continue }
            violations.append(
                "job=\(job.jobId) asset=\(job.assetId) claimed=\(format(claimed))"
                    + " supported=\(format(supported)) excess=\(format(claimed - supported))"
            )
        }
        return PersistedStateInvariantFinding(
            invariant: .coarseCursorBeyondScannedPrefix,
            violations: violations.count,
            population: judged,
            abstained: abstained,
            abstainReason: abstained > 0 ? "no_examined_scan_row" : nil,
            witnesses: Array(violations.prefix(maxWitnessesPerInvariant))
        )
    }

    // MARK: Invariant 3

    private static func retryBudgetSpentWithWorkRemaining(
        _ snapshot: PersistedStateSnapshot
    ) -> PersistedStateInvariantFinding {
        let reachByAsset = Dictionary(
            snapshot.assets.map { ($0.assetId, $0.transcriptReachSec) },
            uniquingKeysWith: { first, _ in first }
        )
        let worthRescanning = RescanThresholdSec.adScanRescanWorthyGapSec
        var violations: [String] = []
        var judged = 0
        var abstained = 0
        for job in snapshot.backfillJobs where job.retryCount >= snapshot.coverageLaneRetryCap {
            // No transcript reach ⇒ nothing to compare the cursor against.
            guard let reach = reachByAsset[job.assetId] ?? nil, reach.isFinite else {
                abstained += 1
                continue
            }
            judged += 1
            // A row that never published a cursor claims nothing, so the whole
            // transcript is still above it.
            let cursor = job.claimedUpperBoundSec.flatMap { $0.isFinite ? $0 : nil } ?? 0
            let remaining = reach - cursor
            guard worthRescanning.warrantsRescan(gapSec: remaining) else { continue }
            violations.append(
                "job=\(job.jobId) asset=\(job.assetId) retry_count=\(job.retryCount)"
                    + "/\(snapshot.coverageLaneRetryCap) cursor=\(format(cursor))"
                    + " transcript_reach=\(format(reach)) remaining=\(format(remaining))"
                    + " defer_reason=\(job.deferReason.map(sanitize) ?? "none")"
            )
        }
        return PersistedStateInvariantFinding(
            invariant: .retryBudgetSpentWithWorkRemaining,
            violations: violations.count,
            population: judged,
            abstained: abstained,
            abstainReason: abstained > 0 ? "no_transcript_reach" : nil,
            witnesses: Array(violations.prefix(maxWitnessesPerInvariant))
        )
    }

    // MARK: Invariant 4

    private static func newAssetWithAudioAndFailedJob(
        _ snapshot: PersistedStateSnapshot
    ) -> PersistedStateInvariantFinding {
        let registered = snapshot.assets.filter { registrationStates.contains($0.analysisState) }
        var violations: [String] = []
        var judged = 0
        var abstained = 0
        for asset in registered {
            // The audio question is asked only of this population; an
            // unanswered one is abstained rather than assumed either way.
            guard let hasAudio = asset.hasAudioOnDisk else {
                abstained += 1
                continue
            }
            judged += 1
            guard hasAudio,
                  let jobState = asset.newestJobState,
                  failedJobStates.contains(jobState) || asset.newestJobLastErrorCode != nil else {
                continue
            }
            violations.append(
                "asset=\(asset.assetId) state=\(asset.analysisState) audio_on_disk=1"
                    + " job_state=\(jobState)"
                    + " job_error=\(asset.newestJobLastErrorCode.map(sanitize) ?? "none")"
            )
        }
        return PersistedStateInvariantFinding(
            invariant: .newAssetWithAudioAndFailedJob,
            violations: violations.count,
            population: judged,
            abstained: abstained,
            abstainReason: abstained > 0 ? "audio_presence_unresolved" : nil,
            witnesses: Array(violations.prefix(maxWitnessesPerInvariant))
        )
    }

    // MARK: Invariant 5

    /// The gate value that means "this row may be auto-skipped".
    static let eligibleGate = "eligible"
    /// The decision state a minted row holds until a delivery door touches it.
    static let candidateDecisionState = "candidate"

    private static func eligibleAutoWindowNeverOffered(
        _ snapshot: PersistedStateSnapshot
    ) -> PersistedStateInvariantFinding {
        var violations: [String] = []
        var judged = 0
        for window in snapshot.eligibilityGatedAdWindows {
            guard window.eligibilityGate == eligibleGate else { continue }
            let detectorClass = SkipDetectorClass.classify(
                boundaryState: window.boundaryState,
                startAnchor: AutoSkipEdgeAnchor(rawValue: window.startEdgeAnchor) ?? .unanchored,
                endAnchor: AutoSkipEdgeAnchor(rawValue: window.endEdgeAnchor) ?? .unanchored
            )
            // Only the classes whose mode is a show-INDEPENDENT `.auto` seed
            // belong to this population: for every other class a `candidate`
            // row may be waiting on the show's own trust history, which is a
            // decision rather than a loss. The mode is READ OFF the authority
            // rather than compared against a local `.auto` — a consumer that
            // hard-codes the constant is the defect `DetectorModeAuthority`
            // exists to make visible, and a retune of the seed must retune
            // this population with it.
            guard let authority = detectorClass.modeAuthority,
                  authority.declaredMode == .auto else { continue }
            judged += 1
            guard window.decisionState == candidateDecisionState,
                  !window.wasSkipped,
                  !window.userDismissedBanner else { continue }
            violations.append(
                "window=\(window.windowId) asset=\(window.assetId)"
                    + " span=\(format(window.startTime))-\(format(window.endTime))"
                    + " boundary=\(sanitize(window.boundaryState))"
                    + " class=\(detectorClass.rawValue)"
                    + " anchors=\(sanitize(window.startEdgeAnchor))/\(sanitize(window.endEdgeAnchor))"
            )
        }
        return PersistedStateInvariantFinding(
            invariant: .eligibleAutoWindowNeverOffered,
            violations: violations.count,
            population: judged,
            abstained: 0,
            abstainReason: nil,
            witnesses: Array(violations.prefix(maxWitnessesPerInvariant))
        )
    }

    // MARK: Formatting

    /// Two decimal places — the resolution the cursor and scan-window columns
    /// are actually written at (0.01 s), so a witness can be compared against a
    /// device pull without a unit conversion in the reader's head.
    static func format(_ value: Double) -> String {
        guard value.isFinite else { return "nonfinite" }
        return String(format: "%.2f", value)
    }

    /// The wire format is space-separated `key=value`, so a value that carries
    /// whitespace or an `=` would split one field into several. Defer reasons
    /// carry raw `NSError` descriptions; the FoundationModels one on the
    /// 2026-08-14 pull is 300 characters of embedded quotes and newlines.
    static func sanitize(_ value: String) -> String {
        let collapsed = value
            .replacingOccurrences(of: "=", with: "~")
            .split(whereSeparator: { $0.isWhitespace || $0.isNewline })
            .joined(separator: "_")
        let trimmed = collapsed.prefix(80)
        return trimmed.isEmpty ? "empty" : String(trimmed)
    }
}
