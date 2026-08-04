// SemanticScanClaim.swift
// playhead-fil5 — the durable record that an episode's audio is owed a
// semantic ad scan, and of which gate stopped one from being dispatched.

import Foundation
import os

/// playhead-fil5: a persisted CLAIM to a semantic ad scan.
///
/// **The defect this exists for.** Measured on the 2026-08-03 device pull: of
/// 12 episodes, 9 were transcribed to ~100 % and only 3 had ANY semantic ad
/// scan. Three assets (FCDDB309 at 100 % transcript, 4FF3A238 at 100 %,
/// 48E903D7 at 95 %) had ZERO `backfill_jobs` rows — and FCDDB309's
/// `decision_events` prove `runBackfill` ran for it TWICE and minted nothing.
/// Every path that requests a scan funnels through
/// `AdDetectionService.runShadowFMPhase`, which drops the work at four gates
/// (cohort mode `.off`, missing runner factory, `canUseFoundationModels`
/// false, empty `podcastId`) with nothing but a log line. A log line does not
/// survive the process, so "the scan was never requested" and "the scan was
/// requested and silently refused" were indistinguishable in the database.
///
/// **Why the claim is a `backfill_jobs` row and not a new table.** The row IS
/// the claim. `BackfillJobRunner.runPendingBackfill` currently mints its rows
/// only after every gate has already opened, which makes the row a receipt for
/// work that started rather than a request for work that is owed; minting it
/// first and gating only its DISPATCH inverts that without inventing a second
/// ledger. It also lands the asset in
/// ``AnalysisStore/fetchAssetIdsWithResumableBackfillJobs(limit:)``, which is
/// where `playhead-onn6`'s re-drive sweep starts — an asset with zero rows is
/// invisible to that sweep by construction, which is exactly why the three
/// field assets have no recovery path at all.
///
/// **Why the id is the runner's id.** ``jobId(analysisAssetId:transcriptVersion:)``
/// re-derives `BackfillJobRunner`'s deterministic `(asset, transcriptVersion,
/// phase, offset)` hash for `.fullEpisodeScan` at offset 0 — the row a
/// `fullCoverage` plan mints. So when a pass finally runs under an open gate,
/// the runner's M-5 idempotency branch finds the claim, re-drives it, and
/// completes it. A claim with a private id would linger `deferred` forever
/// next to the row that actually did the work.
///
/// **Where that resolution does NOT happen, and why it is still safe.**
/// `.fullEpisodeScan` at offset 0 is the whole of a `fullCoverage` or
/// `periodicFullRescan` plan, but ``CoveragePlanner/plan(for:)`` returns
/// `targetedWithAudit` once a podcast has been observed past the cold-start
/// threshold with stable recall, and THAT plan's phases
/// (`scanHarvesterProposals`, `scanLikelyAdSlots`, `scanRandomAuditWindows`)
/// derive three different ids. On such a podcast the claim is never completed
/// by the runner and stays `deferred` for the life of the asset.
///
/// That is a stale row, not a leak, and the bound is measured rather than
/// hoped for: ``AnalysisStore/countResumableBackfillJobs(assetId:)`` — the
/// authority `AnalysisJobReconciler.adScanRedriveCandidate` consults — scopes
/// to the NEWEST enqueue batch (`createdAt >= MAX(createdAt) -
/// ``AnalysisStore/backfillEnqueueBatchWindowSec``), so the moment the targeted
/// plan's own rows land the older claim stops counting, and the re-drive chain
/// stops on its own when those rows go terminal. The claim is still doing its
/// job in the meantime: it is what made the asset visible to the sweep that
/// minted the pass in the first place, and its `deferReason` is still the
/// device-pull answer to "which gate refused this episode a scan?".
enum SemanticScanClaim {

    // MARK: - Gates

    /// Prefix on `backfill_jobs.deferReason` that marks a row as a scan claim.
    ///
    /// Chosen so one query attributes every missing scan on a device pull:
    /// `SELECT analysisAssetId, deferReason FROM backfill_jobs
    ///  WHERE deferReason LIKE 'scan_claim:%'`. It does not collide with the
    /// admission controller's reasons, which are
    /// ``AdmissionDeferReason`` raw values (`thermal`, `battery`, …).
    static let deferReasonPrefix = "scan_claim:"

    /// The reason a semantic scan was owed but not dispatched.
    ///
    /// Every case names a DROP that used to be invisible. `noCoverageLaneRow`
    /// is the one that is not a `runShadowFMPhase` gate: it is written by the
    /// reconciler sweep, which does not observe a gate at all.
    ///
    /// **Each case is named for what its writer can OBSERVE.** The four gates
    /// each sit on the line that refused, so they can assert a cause. The sweep
    /// cannot: it sees an asset with no coverage-lane row and no way to know
    /// whether that is because nothing ever asked or because something asked on
    /// a build that had no way to record the refusal. `noCoverageLaneRow` says
    /// the second thing, and the distinction is not academic — FCDDB309, the
    /// asset this bead was measured on, provably ran `runBackfill` TWICE and
    /// still owns zero rows, so a reason reading `never_requested` would be
    /// false for the very episode the sweep exists to reach. On a build
    /// carrying this bead a refusal writes its own gate, so a sweep-written row
    /// means "no gate on any build ever recorded one here", which is what an
    /// operator can act on.
    enum Gate: String, Sendable, CaseIterable, Equatable {
        /// `effectiveFMBackfillMode == .off`. Either the build asked for it or
        /// the cohort registry demoted a `knownBad` cohort.
        case fmModeOff = "fm_mode_off"
        /// No `backfillJobRunnerFactory` was injected. In production this is a
        /// wiring defect; in preview runtimes it is expected.
        case runnerFactoryMissing = "runner_factory_missing"
        /// `canUseFoundationModels` read false. Frequently TRANSIENT — Apple
        /// Intelligence still downloading, a thermal probe, a locale flip — and
        /// `FoundationModelsUsabilityProbe` caches a false for 15 minutes, so
        /// one throttle can swallow every completion landing in that window.
        case foundationModelsUnavailable = "fm_unavailable"
        /// The caller supplied an empty `podcastId`. Reachable from the
        /// final-pass hook, which passes `request.podcastId ?? ""` — an absent
        /// podcast id rendered as a podcast whose id is the empty string.
        case podcastIdMissing = "podcast_id_missing"
        /// No coverage-lane row of ANY kind exists. Written by the reconciler
        /// sweep for an asset whose transcript cleared the finalize floor,
        /// whose measured ad scan is short, and whose episode has no analysis
        /// pass in flight — so nothing is going to mint one on its own.
        ///
        /// Deliberately NOT `neverRequested`: the sweep observes the absence of
        /// a row, never the absence of a request. See the note on ``Gate``.
        case noCoverageLaneRow = "no_coverage_lane_row"

        /// The string persisted in `backfill_jobs.deferReason`.
        var deferReason: String { "\(SemanticScanClaim.deferReasonPrefix)\(rawValue)" }
    }

    // There is deliberately NO Swift parser for a persisted `deferReason`.
    // Nothing in a shipped build reads one back — the consumer is the device
    // pull, and it is one line of SQL (`WHERE deferReason LIKE 'scan_claim:%'`).
    // A `gate(fromDeferReason:)` helper was written and removed: its only
    // callers were tests, which are stronger for comparing against
    // ``Gate/deferReason`` directly, since that is the value production
    // actually writes and cannot drift from itself.

    // MARK: - Is a scan actually owed?

    /// Is a semantic ad scan still owed for an asset measuring
    /// `adScanFraction`?
    ///
    /// The floor is ``AnalysisJobRunner/semanticBackfillSufficientAdScanFraction``
    /// — the SAME number the runner uses to decide it may skip the semantic
    /// backfill, the scheduler uses to decide whether to mint a re-drive, and
    /// the library ✓ uses to call an episode read. A claim minted below a
    /// different floor would solicit passes one of those three then declines.
    ///
    /// `nil` means UNMEASURED, not sufficient, and returns `true`. That is the
    /// same direction ``AnalysisWorkScheduler/shouldMintAdScanRedrive(adScanFraction:resumableCoverageJobCount:)``
    /// takes, and it is the load-bearing case here rather than an edge: an
    /// asset that has never been scanned has no `semantic_scan_results` rows,
    /// so ``AnalysisCoverageSummary/adScanFraction`` is `nil` — never a
    /// synthetic 0. Reading `nil` as "covered" would make the never-scanned
    /// asset the one case a claim is never minted for.
    static func isOwed(adScanFraction: Double?) -> Bool {
        guard let adScanFraction, adScanFraction.isFinite else { return true }
        return adScanFraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
    }

    /// Has the transcript reached far enough that a scan has something to read?
    ///
    /// ``AnalysisCoordinator/finalizeBackfillMinCoverageRatio`` (0.95), NOT
    /// ``episodePreparationCompleteThreshold`` (0.98). The two are different
    /// numbers for different quantities: 0.95 is the TRANSCRIPT floor,
    /// calibrated to tolerate the few seconds a decoder chops off the end of an
    /// episode. Using the ad-scan floor here would exclude 48E903D7 — one of
    /// the three assets this bead exists for, transcribed to 95 % — from its
    /// own fix.
    ///
    /// **The CONSTANT is shared with `finalizeBackfill`; the NUMERATOR is
    /// deliberately not.** ``AnalysisCoordinator/finalizeBackfillVerdict(chunks:episodeDuration:)``
    /// divides `chunks.map(\.endTime).max()` — a WATERMARK — by the duration.
    /// The caller here passes ``AnalysisCoverageSummary/fastTranscriptCoveredSec``,
    /// the interval UNION. They agree on a contiguous transcript and diverge on
    /// a gappy one, where the watermark reads 100 % over audio nobody
    /// transcribed: the same watermark-vs-union antipattern playhead-sd71 fixed
    /// on the Activity screen. The union is the right numerator for THIS
    /// question — a semantic scan can only read text that exists, so a
    /// transcript with holes has proportionally less for it to read — which
    /// makes this gate strictly stricter than `finalizeBackfill`'s. An asset
    /// that `finalizeBackfill` would complete can therefore fail here, on
    /// purpose, and it stays the transcript lane's problem until the holes fill.
    ///
    /// Unmeasurable inputs return `false`. This gate SUPPRESSES a mint, so the
    /// safe direction is the opposite of `isOwed`'s: an asset whose transcript
    /// reach cannot be established has not been shown to be ready for a scan,
    /// and the transcript lane will come back to it.
    static func transcriptClearsFinalizeFloor(
        coveredSec: Double?,
        episodeDurationSec: Double?
    ) -> Bool {
        guard let coveredSec, coveredSec.isFinite, coveredSec >= 0,
              let episodeDurationSec, episodeDurationSec.isFinite, episodeDurationSec > 0 else {
            return false
        }
        let ratio = coveredSec / episodeDurationSec
        return ratio + 1e-9 >= AnalysisCoordinator.finalizeBackfillMinCoverageRatio
    }

    // MARK: - Identity

    /// The coverage-lane job id a `fullCoverage` plan would derive for this
    /// asset — `.fullEpisodeScan` at plan offset 0.
    ///
    /// Delegates to `BackfillJobRunner` rather than restating its canonical
    /// string. A claim whose id merely LOOKS like the runner's would be a row
    /// the runner never finds, i.e. exactly the orphan this type exists to
    /// prevent.
    static func jobId(analysisAssetId: String, transcriptVersion: String) -> String {
        BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: analysisAssetId,
            transcriptVersion: transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )
    }

    /// The `transcriptVersion` for RAW persisted `transcript_chunks` rows.
    ///
    /// `runBackfill` hands `runShadowFMPhase` the CANONICALIZED chunk stream —
    /// final-pass chunks REPLACE the fast coverage they overlap — so hashing
    /// the raw rows would produce a different version, and therefore a
    /// different job id, for the same asset the moment a final pass had run.
    /// Every caller that starts from the store must come through here.
    static func transcriptVersion(forPersistedChunks chunks: [TranscriptChunk]) -> String {
        TranscriptAtomizer.transcriptVersionHash(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
        )
    }

    // MARK: - The row

    /// The claim row itself: `deferred`, retry budget untouched, carrying the
    /// gate in `deferReason`.
    ///
    /// `deferred` rather than `queued` because the row is a request that has
    /// already been refused once, and `deferReason` is only meaningful
    /// alongside a deferred status. Both are resumable
    /// (`status <> 'complete' AND status <> 'running'`), so either would reach
    /// the re-drive sweep; `deferred` is the one that does not claim the job is
    /// waiting its turn in an admission queue it was never handed to.
    ///
    /// `retryCount: 0` is load-bearing. Every resumability query also demands
    /// `retryCount < AdmissionController.maxRetries`, and a gate closing is not
    /// an attempt that failed — charging it as one would let three closed gates
    /// permanently retire an asset that has never once been scanned.
    static func claimRow(
        analysisAssetId: String,
        podcastId: String?,
        transcriptVersion: String,
        gate: Gate,
        createdAt: Double
    ) -> BackfillJob {
        BackfillJob(
            jobId: jobId(analysisAssetId: analysisAssetId, transcriptVersion: transcriptVersion),
            analysisAssetId: analysisAssetId,
            // An empty id is the ABSENCE of a podcast, not a podcast. Persisting
            // "" would hand the planner-state lookup a key that matches nothing
            // and reads as a real miss.
            podcastId: (podcastId?.isEmpty ?? true) ? nil : podcastId,
            phase: .fullEpisodeScan,
            // The policy a `fullCoverage` plan stamps. It is only ever read off
            // a row the runner re-drives, and the runner only re-drives this row
            // when its own plan derived the same id — which is exactly the
            // `fullCoverage`/`periodicFullRescan` case.
            coveragePolicy: .fullCoverage,
            priority: BackfillJobRunner.phasePriority(.fullEpisodeScan),
            progressCursor: nil,
            retryCount: 0,
            deferReason: gate.deferReason,
            status: .deferred,
            scanCohortJSON: nil,
            createdAt: createdAt
        )
    }

    // MARK: - Recording

    /// What ``record(gate:analysisAssetId:podcastId:transcriptVersion:store:clock:logger:)``
    /// did. Returned rather than logged-only so the guarantee is assertable
    /// without scraping `os_log`.
    enum Outcome: Equatable, Sendable {
        /// No row existed; a claim was inserted.
        case minted
        /// A claim (or any non-terminal coverage-lane row) already existed and
        /// its `deferReason` now names this gate.
        case refreshed
        /// A `complete` row already exists for this asset+transcriptVersion —
        /// the scan this claim would request has already run.
        case alreadySatisfied
        /// A `running` or `failed` row already exists and was left untouched.
        /// `running` means a pass is mid-flight and owns the row's bookkeeping;
        /// `failed` carries the reason the job actually lost, which is more
        /// specific than a gate name. Either way the row already IS the durable
        /// trace, and both remain resumable while `retryCount` is under budget.
        case leftInPlace
        /// Measured coverage says no scan is owed.
        case notOwed
        /// The store refused. Best-effort by contract; the caller continues.
        case failed
    }

    /// Persist (or refresh) the claim for one asset.
    ///
    /// Best-effort by contract: every store error is caught and reported as
    /// ``Outcome/failed``. This runs on the shadow path, whose invariant is
    /// that it can never affect cue computation — a claim that cannot be
    /// written must not turn a detection pass into a thrown error.
    ///
    /// Idempotent across repeated bails. The id is deterministic, so a second
    /// closed gate for the same asset+transcript refreshes the reason in place
    /// instead of inserting a duplicate or bumping `retryCount`.
    @discardableResult
    static func record(
        gate: Gate,
        analysisAssetId: String,
        podcastId: String?,
        transcriptVersion: String,
        store: AnalysisStore,
        clock: @Sendable () -> Double = { Date().timeIntervalSince1970 },
        logger: Logger
    ) async -> Outcome {
        let jobId = jobId(analysisAssetId: analysisAssetId, transcriptVersion: transcriptVersion)
        do {
            let fraction = try await store
                .fetchCoverageSummariesByAssetIds([analysisAssetId])[analysisAssetId]?
                .adScanFraction
            guard isOwed(adScanFraction: fraction) else {
                logger.debug(
                    "scan claim not minted for \(analysisAssetId, privacy: .public): ad scan already sufficient"
                )
                return .notOwed
            }

            if let existing = try await store.fetchBackfillJob(byId: jobId) {
                switch existing.status {
                case .complete:
                    return .alreadySatisfied
                case .running, .failed:
                    // `markBackfillJobDeferred` refuses `failed` on purpose:
                    // demoting it would erase the reason the job actually lost.
                    // `running` it would ACCEPT, which is worse — a pass is
                    // mid-flight and owns this row's state machine. Leave both.
                    return .leftInPlace
                case .queued, .deferred:
                    break
                }
                try await store.markBackfillJobDeferred(jobId: jobId, reason: gate.deferReason)
                logger.info(
                    """
                    scan_claim_refreshed asset=\(analysisAssetId, privacy: .public) \
                    gate=\(gate.rawValue, privacy: .public) job=\(jobId, privacy: .public)
                    """
                )
                return .refreshed
            }

            try await store.insertBackfillJob(claimRow(
                analysisAssetId: analysisAssetId,
                podcastId: podcastId,
                transcriptVersion: transcriptVersion,
                gate: gate,
                createdAt: clock()
            ))
            logger.info(
                """
                scan_claim_minted asset=\(analysisAssetId, privacy: .public) \
                gate=\(gate.rawValue, privacy: .public) job=\(jobId, privacy: .public)
                """
            )
            return .minted
        } catch {
            logger.warning(
                """
                scan_claim_write_failed asset=\(analysisAssetId, privacy: .public) \
                gate=\(gate.rawValue, privacy: .public) error=\(String(describing: error), privacy: .public)
                """
            )
            return .failed
        }
    }
}
