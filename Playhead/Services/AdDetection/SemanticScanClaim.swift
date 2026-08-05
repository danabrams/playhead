// SemanticScanClaim.swift
// playhead-fil5 — the durable record that an episode's audio is owed a
// semantic ad scan, and of which gate stopped one from being dispatched.

import Foundation
import os

/// playhead-fil5: a persisted CLAIM to a semantic ad scan.
///
/// **The defect this exists for.** Measured on the 2026-08-03 device pull: of
/// 12 assets, only 3 had ANY semantic ad scan and **4** had ZERO
/// `backfill_jobs` rows — 48E903D7, FCDDB309, 4FF3A238, 2C5C3699. Two of those
/// four are transcribed and stranded (FCDDB309 and 4FF3A238, 98.8 % and 98.9 %
/// of their duration as a bridged AREA) and are what this type recovers;
/// FCDDB309's `decision_events` prove `runBackfill` ran for it TWICE and minted
/// nothing.
///
/// The bead states that population as three assets with "48E903D7 at 95 %
/// transcript", and the count is four.
///
/// playhead-9y9e CORRECTS the second half of that note. It read 48E903D7's
/// 95.1 % as a watermark artifact, on the strength of a 36.9 % bridged area —
/// but that area was measured over the FAST pass alone, and 48E903D7's
/// transcript is mostly final-pass. Across both passes it covers 95.1 % as a
/// bridged area, which agrees with the watermark. The measurement was wrong,
/// not the watermark; see ``bridgedTranscriptCoveredSec(region:)``. The
/// principle in ``transcriptClearsFinalizeFloor(coveredSec:episodeDurationSec:)``
/// stands unchanged — this gate divides an area and never a watermark — and it
/// still stands on the pull as well as on its fixtures. Widening the area to
/// both passes removes 48E903D7 as an example; it does not empty the class.
/// D9B513CD reads 100.0 % by chunk-max watermark against an 88.3 % two-pass
/// area and flips THIS gate's own 0.95 floor. (R3 review: R2 wrote 58882C47
/// here, from the fast-pass COLUMN rather than the chunk max the gate divides;
/// R1 before it wrote that no asset diverges "by more than 0.8 pp", from a
/// three-asset sample.)
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
/// invisible to that sweep by construction, which is exactly why the four
/// zero-row field assets have no recovery path at all.
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
    static func isOwed(adScanFraction: ReachRatio?) -> Bool {
        // playhead-x0lb: `finiteValue` is absence and non-finiteness stated as
        // one fact, because every consumer of this quantity treats them as one.
        guard let adScanFraction = adScanFraction.finiteValue else { return true }
        return adScanFraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
    }

    /// The seconds of audio an asset's transcript actually backs, with
    /// sub-ad-width gaps BRIDGED — the numerator
    /// ``transcriptClearsFinalizeFloor(coveredSec:episodeDurationSec:)`` is
    /// meant to be handed.
    ///
    /// **playhead-9y9e: the region must span BOTH passes**
    /// (``AnalysisStore/fetchTranscribedRegion(assetId:)``), because the
    /// region this gate measures has to be the region
    /// ``AnalysisCoverageSummary/adScanCoveredSec`` is intersected with — that
    /// is the whole commensurability argument below. It was handed
    /// `fetchFastTranscriptCoveredRanges`, and on the 2026-08-03 device pull
    /// 48E903D7 read 36.9 % against a 0.95 floor while its two passes together
    /// cover 95.1 %: the sweep was refusing to claim a transcribed episode
    /// because most of its transcript came from the final pass. 0C2FC22E is the
    /// same shape and more extreme — its passes are DISJOINT, final `[0, 930]`
    /// and fast `[930, 2086]`, so the fast-only reading is 55.4 % of an episode
    /// transcribed end to end.
    ///
    /// **playhead-x0lb R5 review: that paragraph is now a TYPE.** It was still
    /// only a paragraph — the parameter took a bare
    /// `[(start: Double, end: Double)]`, and `fetchFastTranscriptCoveredRanges`
    /// returned the identical type, so the substitution the sentence forbids was
    /// writable at both of this function's production call sites. Both probes
    /// COMPILED before this change (rails TY32 / TY34), which is the same shape
    /// as every other finding on this bead: a sentence forbidding a substitution
    /// next to an expression that permits it.
    ///
    /// **The raw chunk union is not a reach measure and using it as one made
    /// this sweep mint nothing at all.** A `transcript_chunks` row spans the
    /// FIRST WORD's start to the LAST WORD's end, so every inter-utterance
    /// breath is a hole in `union(chunks)`. Measured on the 2026-08-03 device
    /// pull, across all 12 assets: raw union / duration runs 3.9 %–93.8 % and
    /// **not one asset clears 0.95** — FCDDB309 and 4FF3A238, the two the bead
    /// names as transcribed to ~100 %, read 87.3 % and 89.2 %, from 620 and 573
    /// gaps whose MEDIAN width is 0.12 s. A raw-union gate against a
    /// wall-clock floor is therefore not strict, it is unreachable: it measures
    /// SPEECH DENSITY and compares it to a number calibrated for REACH.
    ///
    /// playhead-pz32 had already found and removed exactly this shape one layer
    /// down (see ``AnalysisCoverageMath/bridgingShortGaps(_:upTo:)``, which
    /// records 0.68–0.976 on the 2026-04-25 capture and the same conclusion:
    /// "an under-claim so total it deletes the signal instead of correcting
    /// it"). Bridging at ``AnalysisCoverageMath/adScanBridgeableGapSec`` (5 s)
    /// restores the reach reading — FCDDB309 → 98.8 %, 4FF3A238 → 98.9 % — and
    /// cannot conceal an ad, because 5 s is shorter than the shortest span any
    /// detection lane will call one.
    ///
    /// It also makes this gate COMMENSURABLE with ``isOwed(adScanFraction:)``.
    /// ``AnalysisCoverageSummary/adScanCoveredSec`` is intersected with this
    /// very region, so the ad-scan fraction is bounded above by
    /// `bridgedTranscript / duration`. Gating on the raw union while judging
    /// sufficiency on the bridged one compares two different denominators —
    /// and gating on something LARGER than the bridged region (the watermark)
    /// would solicit passes whose scan fraction could never reach the floor
    /// that retires them.
    ///
    /// **playhead-x0lb R6 review: the RETURN type is now a
    /// ``BridgedTranscriptSeconds`` too, and R5 typing only the argument is why
    /// it had to be.** R5 closed which REGION goes in and left what comes out a
    /// bare `Double`, so at the call site the correct expression sat beside a
    /// watermark and a raw union of the same type — three probes, three
    /// COMPILED. That is R4's lesson at one more layer: a typed INPUT is not a
    /// typed OPERATION. Rails TY35–TY37.
    static func bridgedTranscriptCoveredSec(region: TranscribedRegion) -> BridgedTranscriptSeconds {
        region.bridgedSeconds(bridging: AnalysisCoverageMath.adScanBridgeableGapSec)
    }

    /// Has the transcript reached far enough that a scan has something to read?
    ///
    /// ``AnalysisCoordinator/finalizeBackfillMinCoverageRatio`` (0.95), NOT
    /// ``episodePreparationCompleteThreshold`` (0.98). The two are different
    /// numbers for different quantities: 0.95 is the TRANSCRIPT floor,
    /// calibrated to tolerate the few seconds a decoder chops off the end of an
    /// episode; 0.98 is the floor a completed AD SCAN is measured against, and
    /// borrowing it here would demand a transcript be more complete than the
    /// scan it is a prerequisite for.
    ///
    /// **The CONSTANT is shared with `finalizeBackfill`; the NUMERATOR is
    /// deliberately not.** ``AnalysisCoordinator/finalizeBackfillVerdict(chunks:episodeDuration:)``
    /// divides `chunks.map(\.endTime).max()` — a WATERMARK — by the duration.
    /// The caller here passes ``bridgedTranscriptCoveredSec(region:)``,
    /// which is an AREA. They agree on a contiguous transcript and diverge on a
    /// gappy one, where the watermark reads 100 % over audio nobody transcribed
    /// (the watermark-vs-union antipattern playhead-sd71 fixed on the Activity
    /// screen).
    ///
    /// **STATED HONESTLY: the 2026-08-03 pull DOES still exhibit the divergence
    /// with the area measured over both passes, and it flips THIS GATE.** NINE
    /// of the twelve assets read watermark ABOVE area, in percentage points of
    /// the declared duration:
    ///
    ///     D9B513CD  100.00 %  vs  88.33 %   11.66 pp   ← flips at 0.95 AND 0.98
    ///     58882C47   99.99 %  vs  97.45 %    2.54 pp   ← flips at 0.98
    ///     FCDDB309   99.92 %  vs  98.79 %    1.13 pp
    ///     4FF3A238   99.94 %  vs  98.85 %    1.08 pp
    ///     AD5F3A0A   99.99 %  vs  99.04 %    0.95 pp
    ///     44F076BB   81.91 %  vs  81.09 %    0.82 pp
    ///     53FC53E3   99.90 %  vs  99.29 %    0.61 pp
    ///     83592353  100.00 %  vs  99.50 %    0.49 pp
    ///     DE0784D8  100.00 %  vs  99.56 %    0.44 pp
    ///
    /// **D9B513CD is the one that bites, and it bites HERE**: 100.00 % by
    /// watermark clears this gate's own 0.95 floor and its 88.33 % two-pass area
    /// does not, so the two rulers return opposite verdicts for a real episode
    /// at the exact threshold this function applies. That is what makes dividing
    /// an area a live decision and not a stylistic one.
    ///
    /// **THE WATERMARK HERE IS `chunks.map(\.endTime).max()`, NOT
    /// `analysis_assets.fastTranscriptCoverageEndTime`** — the column is a
    /// FAST-pass field, and this paragraph is about the quantity
    /// `AnalysisCoordinator.classifyBackfillTerminal` actually divides, which it
    /// takes from the unfiltered `fetchTranscriptChunks` (both passes). Getting
    /// that wrong is what hid D9B513CD: by the fast column it reads 87.79 %,
    /// BELOW its own area, so it drops out of a "watermark above area" table
    /// altogether — the biggest divergence on the pull, invisible.
    ///
    /// Four drafts of this note have been wrong, every one of them the same
    /// defect this bead is about — a quantity read as though it named something
    /// else — so they are kept as the record rather than tidied away:
    ///
    ///  * R3 review corrected R2's five-asset table and its claim that "at this
    ///    gate's own 0.95 floor no asset on the pull flips". Both came from
    ///    measuring the FAST column instead of the chunk max. One asset flips,
    ///    and it flips by 11.66 pp.
    ///  * R2 review corrected R1's "the divergence has no field example left …
    ///    every one of the twelve assets reads within 0.8 percentage points of
    ///    its watermark": that was a THREE-asset sample (2C5C3699, 44F076BB,
    ///    48E903D7) generalised to twelve.
    ///  * R1 corrected playhead-9y9e's first draft, which cited 2C5C3699 as the
    ///    proof — "900 s of a 6,925 s episode against a 13.0 % area", where
    ///    900 / 6,925.5 IS 13.0 %, so the two numbers agree exactly and the
    ///    example showed the opposite of what it was cited for.
    ///  * Its predecessor cited 48E903D7 at 36.9 % and AD5F3A0A at 44.0 %, which
    ///    were the FAST pass alone — across both passes those read 95.1 % and
    ///    99.0 %, i.e. instances of the measurement bug this bead fixed, not of
    ///    watermark-vs-area divergence.
    ///
    /// The RAIL is still the fixture and not the corpus —
    /// `AnalysisJobRunner`'s `fullWatermarkOverGappyTranscriptStillFails` and
    /// mutation RT03 are what hold this, and they are synthetic on purpose. The
    /// field rows above say the shape is real; they are not a substitute for a
    /// fixture, because a single device over one fortnight never is.
    ///
    /// So this gate is strictly stricter than `finalizeBackfill`'s. An asset it
    /// would complete can fail here, on purpose, and it stays the transcript
    /// lane's problem until the holes fill. What the numerator must NOT be is
    /// the RAW union — see ``bridgedTranscriptCoveredSec(region:)`` for the
    /// measurement showing that reads 0/12 in the field.
    ///
    /// Unmeasurable inputs return `false`. This gate SUPPRESSES a mint, so the
    /// safe direction is the opposite of `isOwed`'s: an asset whose transcript
    /// reach cannot be established has not been shown to be ready for a scan,
    /// and the transcript lane will come back to it.
    ///
    /// **playhead-x0lb R6 review: BOTH parameters carry types, and this was the
    /// round's largest finding.** They were `Double?` and `Double?`, and the
    /// audit cleared them as latent instance L1 (playhead-fpnt) with an argument
    /// rather than a probe. Three substitutions were planted at
    /// `AnalysisJobRunner.transcriptCoverageOfCompletedTranscript` and all three
    /// COMPILED: the RAW union off the same region (playhead-fil5 R3's P0, and
    /// zero of twelve field assets clear 0.95 raw), the fast WATERMARK from three
    /// lines above (D9B513CD reads 100.0 % against an 88.3 % area — across this
    /// very floor), and the numerator and denominator EXCHANGED. See
    /// ``BridgedTranscriptSeconds``; rails TY35–TY37.
    ///
    /// **The arithmetic is unchanged and the guards are the same guards**, moved
    /// into ``BridgedTranscriptSeconds/fractionOfDeclaredDuration(_:)`` so the
    /// division is written once with the numerator as the receiver — R4's
    /// TY24/TY25 fix, applied here because a typed pair is not a typed operation.
    /// A negative numerator clamps to 0 and fails the floor exactly as
    /// `coveredSec >= 0` refused it; an absent or non-positive duration yields
    /// `nil` exactly as `episodeDurationSec > 0` refused it.
    ///
    /// **`finiteValue` on both terms is load-bearing and is NOT decoration.**
    /// ``BridgedTranscriptSeconds/fractionOfDeclaredDuration(_:)`` clamps into
    /// `[0, 1]`, and a `+∞` numerator clamps to `1.0` — which would CLEAR this
    /// floor, where `coveredSec.isFinite` refused it. Delegating the division
    /// without re-stating the finiteness guard was a behaviour change written
    /// while removing one, so it is spelled out rather than inherited.
    static func transcriptClearsFinalizeFloor(
        coveredSec: BridgedTranscriptSeconds?,
        episodeDurationSec: EpisodeSeconds?
    ) -> Bool {
        guard let ratio = coveredSec.finiteValue?
            .fractionOfDeclaredDuration(episodeDurationSec.finiteValue) else {
            return false
        }
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
