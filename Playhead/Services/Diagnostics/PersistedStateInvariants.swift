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
// ----- What is NOT here, and what playhead-gyhw decided -----
//
// No repair. No reset. No migration. `playhead-gyhw` — the healing half — took
// all five invariants one at a time and LICENSED NO NEW REPAIR. The reasons are
// recorded per case on ``PersistedStateInvariant/healLicence``, which is a total
// switch so a sixth invariant cannot be added without stating one. In summary:
//
//   1e86  no rule changed; the launch reaper already clears the row, and 1e86's
//         open gap is WHERE it is called from.
//   wogi  the rule changed and the repair SHIPPED as V51. 0 of 8 remain.
//   e6d3  the rule changed and the repair SHIPPED as V50. The 1 row it leaves
//         was retired by an arm whose rule did NOT change (59c8 / ronl).
//   1216  no rule changed; the defect was in the download oracle. 0 of 0.
//   exy0  REFUTED — `candidate` is correct for an episode nobody has played,
//         so a repair would fabricate a delivery.
//
// What gyhw DID ship is the thing that makes the next repair safe: V50 and V51
// now RECORD what they repaired, per row, from → to, on this reporter's own
// channel. See ``PersistedStateRepairRecord``.

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
    /// * WITNESS: jobId, asset, cursor, transcript reach, and BOTH remainders —
    ///   `remaining_to_plan` and `remaining_unexamined` (playhead-hii7).
    /// * NULL READING: **zero**. Post-e6d3 `retryCount` counts CONSECUTIVE
    ///   attempts that did not advance the cursor, and e6d3's own saturating
    ///   argument is the legitimate way to reach the cap: once the cursor
    ///   reaches the last segment, `narrowedForResume` empties the plan list
    ///   and three attempts run no inference at all. So a job legitimately at
    ///   the cap has nothing left above its cursor, and `remaining` is zero.
    ///
    /// **TWO REMAINDERS, AND THEY ARE NOT THE SAME NUMBER (playhead-hii7).**
    /// `reach - cursor` is honestly "what a resume would PLAN", because a
    /// resume plans from the cursor. It was read — in playhead-59c8's own
    /// description, and it is an easy read to make — as "transcribed audio
    /// above the cursor that NO SCAN HAS READ". Those differ whenever the
    /// cursor LAGS the asset's own examined prefix, which the pull of
    /// 2026-08-14 shows on A9F6DF05: cursor 2,882.94, supported prefix
    /// 6,036.84, transcript reach 6,874.25. The planning remainder is
    /// 3,991.31 s; the unexamined remainder is at most 837.41 s. A factor of
    /// 4.8, and the record already contained its own contradiction — the same
    /// row's adScanFraction 0.8866 against a 0.9965 ceiling implies ~756 s.
    ///
    /// So the witness carries both, named for what each one is. The invariant
    /// still FIRES on the planning remainder, because that is the quantity its
    /// claim is about (a dead row that would still plan work), and the second
    /// number is there so nobody reads the first as the other one.
    ///
    /// A cursor that lags its own asset's examined prefix is a cost in its own
    /// right — the re-drive re-plans audio already examined, playhead-ejr7's
    /// shape arriving through a cursor — and whether it deserves its own
    /// invariant is NOT decided here. Note what such an invariant would need:
    /// `supportedScannedPrefix` is per-ASSET while the cursor is per-JOB, so an
    /// asset carrying several coverage-lane rows would have to ABSTAIN rather
    /// than report every row but one as lagging.
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
    /// * NULL READING: **not zero — read it against `population=`**
    ///   (playhead-n4l2). Every delivery door does leave a mark on the row:
    ///   `decisionState` moves off `candidate` (to `confirmed` / `applied` /
    ///   `suppressed`), or `wasSkipped` / `userDismissedBanner` flips. Both
    ///   halves of that are true; the conclusion "so a non-zero reading says
    ///   the population was LOST" does not follow, because "never arrived at a
    ///   door" also covers the entirely healthy case where **the listener has
    ///   not played the episode yet**.
    ///
    ///   Measured: this reads 4/4 on db-pull10 with nothing broken. All five
    ///   day-0 kickoffs came from `download_and_analyze_tap` with nothing
    ///   playing, and neither episode was opened afterwards; driven through
    ///   `beginEpisode` the same four rows reach `.applied` and push a cue at
    ///   0.50 s (playhead-exy0, filed P0 and closed REFUTED with no production
    ///   change; `PersistedStateInvariantDevicePullTests.neverOfferedReadsFourOfFour`).
    ///
    ///   So the honest reading is a RATIO, not a count: `violations` over
    ///   `population` is "eligible show-independent-auto windows still waiting
    ///   for their first play". It is worth attention when it stays high on a
    ///   device whose episodes HAVE been played — which this evaluator cannot
    ///   see, and that is the residual: the playback position lives in the
    ///   SwiftData library store, not in `analysis.sqlite`, so narrowing the
    ///   population to "episodes played past the window's start" needs a
    ///   second source this snapshot does not have. Stated rather than
    ///   approximated, because a census whose null reading is wrong is a line
    ///   nobody reads — and dgly's whole argument for an always-present census
    ///   is that zero is a POSITIVE claim.
    ///
    /// The class is resolved through the SHARED
    /// ``SkipDetectorClass/classify(boundaryState:startAnchor:endAnchor:)``
    /// rather than by re-spelling `dayZeroRediffByteExact` here — 6qvf's
    /// lesson, a second expression that happens to agree is how the certainty
    /// tier and its consumers came apart.
    case eligibleAutoWindowNeverOffered = "eligible_auto_window_not_yet_offered"

    /// **playhead-6avxc.** An `ad_windows` row whose `boundaryState` is
    /// `dayZeroRediffByteExact` — the deterministic byte-differ splice the
    /// mint's own doc calls "DETERMINISTIC ground truth for the user's OWN
    /// played stitch" — yet carries `unanchored` on BOTH edges.
    ///
    /// * QUANTITY: the count of such rows, over EVERY row carrying that
    ///   boundaryState in ``PersistedStateSnapshot/eligibilityGatedAdWindows``
    ///   (rows with a non-nil `eligibilityGate` — every day-0 mint sets one,
    ///   so this is the whole population in practice). Deliberately not
    ///   narrowed to `eligibilityGate == "eligible"`: the demotion this
    ///   invariant polices can sit at EITHER gate value, and narrowing to
    ///   `eligible` would hide exactly the `markOnly` + both-unanchored rows
    ///   the bead measured.
    /// * WITNESS: window id, asset, span, both edge anchors, eligibility
    ///   gate, decision state.
    /// * NULL READING: **zero**. `AdDetectionService.mintByteExactDayZeroMarks`
    ///   is the ONLY production write site for this boundaryState (grep-
    ///   verified, playhead-6avxc), and every row it emits derives `anchor`
    ///   and `eligibilityGate` from the SAME `skipGrade` boolean — the two
    ///   cannot disagree from that site, in EITHER state of
    ///   `RediffActivation.dayZeroSegmentRecoveredAutoSkipEnabled`
    ///   (`RediffDayZeroAutoSkipPromotionTests
    ///   .recoveredSlotDispositionFollowsTheSwitch` pins both states
    ///   deliberately, as a tested ROLLBACK). A non-zero reading is residue
    ///   from an EARLIER version of that write path, predating
    ///   playhead-qs0d's tiering, that this reporter cannot reach — see
    ///   ``healLicence``.
    ///
    /// THE STANDING DEFECT: `unanchored` is read at the consumer
    /// (`AutoSkipEdgePadding.skipWindow`) as "no anchor was recorded", never
    /// as "the boundary is unknown" — the byte differ proved this row's
    /// geometry by CONSTRUCTION (the row would not exist otherwise), so the
    /// anchor columns UNDERSTATE what `boundaryState` already proves.
    /// `skipWindow` returns `nil` for an unanchored start regardless of
    /// `eligibilityGate`, so a row this shape describes is demoted to
    /// mark-only by its own metadata — a proven skip thrown away. Measured
    /// 2026-09-08: 7 such rows on the device, excluding user marks.
    case dayZeroByteExactBothEdgesUnanchored = "day_zero_byte_exact_both_edges_unanchored"

    /// **playhead-llne.** An asset carrying both `semantic_scan_results` rows
    /// and `transcript_chunks` rows, none of whose scan rows is at the version
    /// the asset's CURRENT chunk set hashes to — so no pull can say which text
    /// any of its scans read.
    ///
    /// # TWO COLUMNS, ONE NAME, DIFFERENT QUANTITIES
    ///
    /// Both tables carry a column named `transcriptVersion`, both hold a
    /// 32-hex SHA-256 prefix, and they are NOT the same quantity:
    ///
    ///  * `semantic_scan_results.transcriptVersion` is
    ///    `TranscriptAtomizer.transcriptVersionHash` over the CANONICALIZED
    ///    chunk SET the classifier consumed (final rows replace the fast rows
    ///    they cover, `canonicalTimeOrder`) — a per-SCAN projection hash,
    ///    stamped from `TranscriptVersion` at every `AdDetectionService` write
    ///    site and compared against ITSELF for reuse.
    ///  * `transcript_chunks.transcriptVersion` is written by NO producer:
    ///    `TranscriptEngineService` and `FinalPassRetranscriptionRunner` both
    ///    persist `nil`. Its only writer is the schema ladder's
    ///    `backfillLegacyTranscriptChunksPhase1IfNeeded`, which at every open
    ///    stamps EVERY `pass != 'fast'` row of an asset holding a NULL one with
    ///    `legacyTranscriptVersion` — the SAME hash function, over the
    ///    FINAL-ONLY subset, in the FROZEN `chunkIndex`/`id` order. Fast rows
    ///    stay NULL for ever.
    ///
    /// Same function, different population, different order: the two values
    /// are equal only when an asset is final-only AND its `chunkIndex` order
    /// is its time order. So `JOIN … ON (analysisAssetId, transcriptVersion)`
    /// is typeable, returns rows for exactly that coincidence, and returns
    /// NOTHING for everything else — silently, which is the dangerous
    /// direction: a coverage query written that way reports zero and reads as
    /// a clean answer. That is the standing defect class living in the
    /// schema's NAMING rather than in a value.
    ///
    /// MEASURED. 2026-08-16 pull: 13 of 13 scan-bearing assets, zero join rows
    /// each (the bead's table). 2026-09-08 pull, re-counted for this
    /// invariant: 41 assets carry scan rows; the column join is empty for 39
    /// and non-empty for 2 (0FF7EFF3, E30D13AB — both final-only, 0 fast rows,
    /// the coincidence above); 4 carry no stamp at all (all-fast, no final
    /// pass yet). The stamp equals a fresh legacy recompute on 32 of the 37
    /// stamped assets; the other 5 are STALE (the backfill re-stamps only when
    /// it finds a NULL final row, so a final set that later shrank keeps its
    /// old value) — so even where the join returns rows it can name rows whose
    /// text has changed.
    ///
    /// * QUANTITY: assets whose scan rows are ALL at a version other than
    ///   `SemanticScanClaim.transcriptVersion(forPersistedChunks:)` over the
    ///   asset's current rows — the ONE recoverable relation. Equal means the
    ///   scan read today's text and that text is on disk; unequal means the
    ///   text it read no longer exists in that form.
    /// * WITNESS: asset, scan row and version counts, chunk row / stamp / NULL
    ///   counts, the row count the naive column join returns, the current
    ///   chunk-set hash, and how many scan rows are at it (zero, by the
    ///   definition of a violation).
    /// * NULL READING: **zero only on a device that re-scanned every asset
    ///   after its last transcript change.** A re-transcription moves the hash
    ///   and orphans every earlier scan row (playhead-qjcf: 211 of 301 coarse
    ///   rows on the 2026-08-19 pull were at a superseded version), so read
    ///   the count against `population` — and read `abstained` FIRST: the
    ///   recompute reads every chunk row of every scan-bearing asset (183,640
    ///   rows / 2.67 MB of text on the 2026-09-08 pull) and is deliberately
    ///   NOT paid in the awaited launch chain, so at launch this invariant
    ///   abstains on its whole population and its abstain reason carries the
    ///   column-join count (`column_join_empty=39/41` on that pull). A test,
    ///   or a pull-side reader, passes `computingChunkSetHash: true` and
    ///   judges. `population=0 abstained=0` is an EMPTY population, which is a
    ///   third reading and the census tells all three apart.
    ///
    /// THE COLUMN JOIN IS NEVER EVIDENCE OF CLEANLINESS. A non-empty join is a
    /// coincidence of population (final-only) and can name stale rows; the
    /// recompute takes precedence whenever it is present, and without it the
    /// asset abstains rather than being read as related. What this invariant
    /// does NOT decide is the remedy — rename the scan column so the false
    /// join is untypeable, or keep the names and ship the recompute pull-side
    /// — see ``healLicence``.
    case semanticScanVersionUnrelatedToChunkSet = "semantic_scan_version_unrelated_to_chunk_set"
}

// MARK: - The heal licence (playhead-gyhw)

/// Why one ``PersistedStateInvariant`` is, or is not, REPAIRED.
///
/// `playhead-gyhw` enumerated all five and licensed no new repair. That is a
/// claim, so it is recorded where the next author must read it rather than in a
/// commit message nobody opens: ``PersistedStateInvariant/healLicence`` is a
/// total switch, so a sixth invariant does not COMPILE until it states one.
///
/// **The rule, from `playhead-e6d3`'s precedent and `playhead-59c8`'s refusal:**
/// a repair is principled exactly when the rows were failed under a rule that no
/// longer holds. Three corollaries this enum exists to keep straight:
///
///  * If the rule changed, the repair belongs WITH the forward fix, as a
///    one-shot version-guarded migration — that is what V50 and V51 are. A
///    SECOND, recurring healer for the same invariant can only ever fire on a
///    REGRESSION of that forward fix, and silently repairing a regression is
///    exactly what the reporter/healer split was built to prevent.
///  * If no rule changed, no repair is licensed, however dead the row looks.
///  * If the reading is not a defect, a "repair" would FABRICATE state — for
///    ``PersistedStateInvariant/eligibleAutoWindowNeverOffered`` it would record
///    a skip the listener was never offered.
enum PersistedStateHealLicence: Sendable, Equatable {

    /// The rule changed, and the repair SHIPPED with the forward fix as a
    /// one-shot migration. Nothing further is licensed here; what the migration
    /// left behind is stated so it can be challenged.
    case shippedWithTheForwardFix(bead: String, migration: String, residue: String)

    /// No rule changed, so no repair is licensed. `blockedBy` names the open
    /// bead that owns the forward fix — until it merges, a repair here would be
    /// a reconciler shipped ahead of its diagnosis.
    case noRuleChanged(reason: String, blockedBy: String?)

    /// The invariant fired, and the rows are not broken. Repairing them would
    /// write a state no evidence supports.
    case readingIsNotADefect(reason: String)

    /// The bead that would have to change a rule before any repair is licensed,
    /// when one is known.
    var blockingBead: String? {
        switch self {
        case .shippedWithTheForwardFix: return nil
        case let .noRuleChanged(_, blockedBy): return blockedBy
        case .readingIsNotADefect: return nil
        }
    }

    /// Whether this bead's launch path performs a repair for the invariant.
    /// **False for every case today**, and deliberately so — see the type's doc.
    var repairsAtLaunch: Bool { false }
}

extension PersistedStateInvariant {

    /// Why this invariant is or is not repaired. Total by construction.
    var healLicence: PersistedStateHealLicence {
        switch self {
        case .strandedRunningBackfillJob:
            // playhead-1e86 is OPEN. Nothing has changed the rule that leaves a
            // hard-killed row at `running`; the row is a corpse, not a stale
            // verdict, and `AnalysisJobReconciler.resetStrandedBackfillJobs`
            // already flips it back at launch, scene activation and
            // preAnalysisRecovery. What 1e86 names is that `handleBackfillTask`
            // runs NONE of those — a call-site gap, which is a forward fix and
            // 1e86's to make. A second reaper bolted on here would repair the
            // symptom on the one path that already has one.
            return .noRuleChanged(
                reason: "the row is a corpse rather than a stale verdict, and the launch reaper "
                    + "already clears it; 1e86's gap is WHERE the reaper is called, not that "
                    + "nothing repairs the row",
                blockedBy: "playhead-1e86"
            )

        case .coarseCursorBeyondScannedPrefix:
            // The rule changed (playhead-wogi: the cursor stops at a hole in
            // the run's OWN audio) and V51 repaired every device row under it,
            // measured at ONE row of nine on db-pull10. Post-V51 this invariant
            // reads 0 of 8 on that pull, so a recurring healer would fire only
            // on a regression of wogi's fix — and hiding that is the failure
            // mode `playhead-dgly`'s header is written against.
            return .shippedWithTheForwardFix(
                bead: "playhead-wogi",
                migration: "v51",
                residue: "0 of 8 on db-pull10 after the ladder; a later violation is a REGRESSION "
                    + "of wogi's forward fix and must be read, not repaired"
            )

        case .retryBudgetSpentWithWorkRemaining:
            // The rule changed (playhead-e6d3: `retryCount` counts CONSECUTIVE
            // attempts that did not advance the cursor) and V50 repaired
            // exactly the rows retired under the old one — the seven rows on
            // db-pull10 carrying `underCoverageBudgetSpent-%`. The one row it
            // leaves, A9F6DF05, was retired by the GENERIC exception arm, whose
            // rule playhead-59c8 examined and deliberately did not change;
            // playhead-ronl carries whether it should. Sweeping it in would be
            // a repair keyed on a consequence rather than on a cause.
            return .shippedWithTheForwardFix(
                bead: "playhead-e6d3",
                migration: "v50",
                residue: "1 of 1 on db-pull10 after the ladder — A9F6DF05, retired by the generic "
                    + "exception arm whose rule has NOT changed (playhead-59c8, playhead-ronl)"
            )

        case .newAssetWithAudioAndFailedJob:
            // playhead-1216 was a DOWNLOAD-oracle regression: the library asked
            // `DownloadManager.isCached` and got the wrong answer. No rule
            // about `analysis_assets.analysisState` changed, and the launch
            // chain's `reconcilePersistedTerminalStatesIfNeeded` already
            // rewrites that column. The population on db-pull10 is 0 of 0.
            return .noRuleChanged(
                reason: "1216's defect was in the download oracle, not in the persisted "
                    + "registration state; nothing changed the rule that writes `analysisState`, "
                    + "and the measured population is 0 of 0",
                blockedBy: nil
            )

        case .eligibleAutoWindowNeverOffered:
            // playhead-exy0 was REFUTED by measurement: driven through
            // `beginEpisode`, these rows reach `.applied` and push a cue at
            // 0.50 s. They are `candidate` because the episodes were never
            // played — every day-0 kickoff came from `download_and_analyze_tap`
            // with nothing playing. `candidate` is the CORRECT pre-offer state,
            // so a repair would record a delivery that did not happen.
            //
            // See playhead-n4l2: this invariant's stated null reading of zero
            // is wrong for that same reason, which is why it reads 4 of 4 on a
            // healthy device.
            return .readingIsNotADefect(
                reason: "playhead-exy0 measured these rows reaching `.applied` through "
                    + "`beginEpisode`; `candidate` is the correct state for a window on an "
                    + "episode nobody has played, so a repair would fabricate a delivery"
            )

        case .dayZeroByteExactBothEdgesUnanchored:
            // The write-path rule already changed — TWICE, by EARLIER beads,
            // not by this reporter. playhead-qs0d (measured 2026-07-31)
            // stopped emitting a fresh `dayZeroRediffByteExact` row with
            // `.unanchored` on both edges for any STRICT slot, and
            // playhead-pyq7 (measured 2026-08-14) widened that to every
            // SEGMENT-RECOVERED slot under
            // `dayZeroSegmentRecoveredAutoSkipEnabled` (shipped ON).
            // `AdDetectionService.mintByteExactDayZeroMarks` is the ONLY
            // production write site for this boundaryState, and its
            // `anchor`/`eligibilityGate` pair cannot disagree from that site
            // in EITHER flag state — `RediffDayZeroAutoSkipPromotionTests
            // .recoveredSlotDispositionFollowsTheSwitch` pins both states
            // deliberately, as a tested ROLLBACK that playhead-6avxc must
            // not and does not touch (see its report to Dan).
            //
            // No migration EVER ran against the rows this invariant measures:
            // unlike `coarseCursorBeyondScannedPrefix` /
            // `retryBudgetSpentWithWorkRemaining`, nothing swept the
            // pre-qs0d residue when the write path was fixed. The seven
            // device rows measured 2026-09-08 are that residue. Backfilling
            // them is a DEVICE action (recompute each row's anchor from its
            // own byte-exact geometry, then persist it) that playhead-6avxc
            // deliberately did not perform — it has no device access, and a
            // backfill is exactly the "repair earned by diagnosis" this
            // file's header reserves for a decision, not a reporter.
            return .noRuleChanged(
                reason: "the write-path rule already changed (playhead-qs0d, playhead-pyq7) and "
                    + "the one production write site cannot reproduce this shape going forward "
                    + "in either flag state; the seven rows measured 2026-09-08 are residue from "
                    + "an EARLIER version of that write path that no migration ever swept, and "
                    + "backfilling them on-device is deliberately deferred, not licensed here",
                blockedBy: nil
            )

        case .semanticScanVersionUnrelatedToChunkSet:
            // No rule changed, and no repair could exist in this shape: the
            // relation is RECOMPUTABLE, not repairable. A scan row at a
            // superseded version is a correct record of a transcript that no
            // longer exists; writing today's hash onto it would claim it read
            // text it never saw, and writing the scan hash onto chunk rows
            // would add a second producer to a column the legacy backfill
            // re-stamps at the next open. The forward fix is a REMEDY DECISION
            // playhead-llne surfaces and does not make: (a) rename the scan
            // column (`chunkSetHash`) so the false join is untypeable — a wide
            // rename across every reader, writer, the DDL and a migration; or
            // (b) keep the names, document the relation (done), and ship the
            // recompute as a pull-side script. Until that is decided, this
            // invariant reports.
            return .noRuleChanged(
                reason: "the relation is recomputable, not repairable — a scan row at a superseded "
                    + "version is a true record, and stamping either column with the other's value "
                    + "would fabricate a relation; the remedy (rename the scan column vs a pull-side "
                    + "recompute script) is a design decision playhead-llne surfaces and does not make",
                blockedBy: "playhead-llne"
            )
        }
    }
}

// MARK: - The repair record (playhead-gyhw)

/// One repair a MIGRATION performed on a persisted row.
///
/// **A repair that leaves no durable trace is indistinguishable from the bug not
/// having happened**, and both shipped repairs were in exactly that position:
/// V50 reset seven retry budgets and V51 withdrew a cursor by 7,339.26 s, each
/// announcing itself only through `Logger.notice`, which no device pull
/// collects. Worse, both repairs DESTROY the evidence they acted on — after V51
/// nothing anywhere says 3C2FFE10's cursor was ever 7,998.72, so the next pull
/// cannot tell a repaired row from one that was never broken.
///
/// Every field answers one of the four questions a pull must be able to ask:
/// WHAT changed (``field``), on WHICH row (``rowId``), FROM what TO what, and
/// WHICH invariant licensed it (``invariant`` / ``licensedBy``).
struct PersistedStateRepairRecord: Sendable, Equatable {
    /// The schema rung that performed it, e.g. `v50`.
    let migration: String
    /// The invariant whose violation this repair withdraws.
    let invariant: PersistedStateInvariant
    /// The bead whose RULE CHANGE licensed the repair.
    let licensedBy: String
    /// The row's identity in its own table — `backfill_jobs.jobId` for both.
    let rowId: String
    /// The column, spelled as a reader of the device DB would address it.
    let field: String
    let from: String
    let to: String
    /// The value of the predicate term that selected this row, so a reader can
    /// confirm the repair was keyed on a CAUSE rather than on a symptom.
    let cause: String

    /// The wire body, space-separated `key=value`, matching the census format
    /// so one parser reads both.
    var wireDescription: String {
        let sanitize = PersistedStateInvariantEvaluator.sanitize
        return "migration=\(migration)"
            + " invariant=\(invariant.rawValue)"
            + " licensed_by=\(licensedBy)"
            + " row=\(sanitize(rowId))"
            + " field=\(field)"
            + " from=\(sanitize(from))"
            + " to=\(sanitize(to))"
            + " cause=\(sanitize(cause))"
    }
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
    /// playhead-llne: one row per asset that carries `semantic_scan_results`
    /// rows — its two `transcriptVersion` populations side by side, and the
    /// one relation that can tie them together. See
    /// ``PersistedStateInvariant/semanticScanVersionUnrelatedToChunkSet``.
    ///
    /// Defaulted to `[]` in `init` so the dozens of test constructors that
    /// predate it compile unchanged. That default is exactly the silent-zero
    /// shape this reporter is built against, so the ONE production site that
    /// rebuilds a snapshot (`PersistedStateInvariantReporter
    /// .resolvingAudioPresence`) is pinned by a test that would read
    /// `population=0` if the field were dropped there.
    let transcriptVersionRelations: [TranscriptVersionRelationRow]

    init(
        backfillJobs: [BackfillJobRow],
        assets: [AssetRow],
        eligibilityGatedAdWindows: [AdWindowRow],
        coverageLaneRetryCap: Int,
        transcriptVersionRelations: [TranscriptVersionRelationRow] = []
    ) {
        self.backfillJobs = backfillJobs
        self.assets = assets
        self.eligibilityGatedAdWindows = eligibilityGatedAdWindows
        self.coverageLaneRetryCap = coverageLaneRetryCap
        self.transcriptVersionRelations = transcriptVersionRelations
    }

    /// playhead-llne: one asset's `transcriptVersion` populations. Every
    /// count is a ROW count in its own table; the two key spaces share a
    /// format (32 hex) and NOT a meaning.
    struct TranscriptVersionRelationRow: Sendable, Equatable {
        let assetId: String
        /// `semantic_scan_results` rows for this asset, keyed by the CHUNK-SET
        /// hash each carries as `transcriptVersion` (the canonical, time-ordered
        /// set the classifier consumed).
        let scanRowsByVersion: [String: Int]
        /// `transcript_chunks` rows carrying a NON-NULL stamp, keyed by it. The
        /// stamp is the legacy backfill's FINAL-ONLY hash in frozen
        /// `chunkIndex`/`id` order — never a producer's value.
        let chunkRowsByStamp: [String: Int]
        /// `transcript_chunks` rows carrying NULL — every fast row, by design.
        let chunkNullRows: Int
        /// `SemanticScanClaim.transcriptVersion(forPersistedChunks:)` over the
        /// asset's CURRENT rows — the recoverable relation. `nil` means NOT
        /// COMPUTED (the launch reader does not pay for it), never "no chunks".
        let chunkSetHash: String?

        init(
            assetId: String,
            scanRowsByVersion: [String: Int],
            chunkRowsByStamp: [String: Int],
            chunkNullRows: Int,
            chunkSetHash: String?
        ) {
            self.assetId = assetId
            self.scanRowsByVersion = scanRowsByVersion
            self.chunkRowsByStamp = chunkRowsByStamp
            self.chunkNullRows = chunkNullRows
            self.chunkSetHash = chunkSetHash
        }

        var scanRowCount: Int { scanRowsByVersion.values.reduce(0, +) }
        var chunkRowCount: Int { chunkRowsByStamp.values.reduce(0, +) + chunkNullRows }
        var scanVersions: Set<String> { Set(scanRowsByVersion.keys) }
        var chunkStamps: Set<String> { Set(chunkRowsByStamp.keys) }

        /// EXACTLY the row count that
        /// `SELECT COUNT(*) FROM semantic_scan_results s JOIN transcript_chunks c
        /// ON c.analysisAssetId = s.analysisAssetId AND c.transcriptVersion =
        /// s.transcriptVersion` returns for this asset — the false join, as a
        /// number a reader can compare against a pull. Zero on 39 of 41
        /// scan-bearing assets on the 2026-09-08 pull.
        var columnJoinRowCount: Int {
            scanRowsByVersion.reduce(0) { $0 + $1.value * (chunkRowsByStamp[$1.key] ?? 0) }
        }

        /// Scan rows whose version IS the current chunk-set hash — the rows
        /// whose text a pull can recover. `nil` when the hash was not computed.
        var scanRowsAtChunkSetHash: Int? {
            chunkSetHash.map { scanRowsByVersion[$0] ?? 0 }
        }
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
            case .dayZeroByteExactBothEdgesUnanchored:
                return dayZeroByteExactBothEdgesUnanchored(snapshot)
            case .semanticScanVersionUnrelatedToChunkSet:
                return semanticScanVersionUnrelatedToChunkSet(snapshot)
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
        // playhead-hii7: the examined prefix, for the SECOND remainder on the
        // witness line. Absent for an asset with no examined coverage-lane row,
        // and the witness then says `unknown` rather than guessing.
        let prefixByAsset = Dictionary(
            snapshot.assets.map { ($0.assetId, $0.supportedScannedPrefixSec) },
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
            // playhead-hii7: what a RESUME WOULD PLAN. The invariant fires on
            // this one, because its claim is about a dead row that still has
            // work to plan.
            let remainingToPlan = reach - cursor
            // And what NO SCAN HAS READ. These coincide only when the cursor
            // has kept up with the asset's own examined windows; on A9F6DF05
            // they differ by 4.8x. `supportedScannedPrefix` is per-ASSET, so
            // this is a bound on the unexamined remainder rather than a
            // per-job reading — which is exactly why it travels beside the
            // other number instead of replacing it.
            let examinedPrefix = prefixByAsset[job.assetId] ?? nil
            let remainingUnexamined = examinedPrefix.map { max(0, reach - max(cursor, $0)) }
            guard worthRescanning.warrantsRescan(gapSec: remainingToPlan) else { continue }
            violations.append(
                "job=\(job.jobId) asset=\(job.assetId) retry_count=\(job.retryCount)"
                    + "/\(snapshot.coverageLaneRetryCap) cursor=\(format(cursor))"
                    + " transcript_reach=\(format(reach))"
                    + " remaining_to_plan=\(format(remainingToPlan))"
                    + " remaining_unexamined=\(remainingUnexamined.map(format) ?? "unknown")"
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

    // MARK: Invariant 6

    /// The boundaryState literal a byte-exact day-0 mint stamps. Read off
    /// `AdDetectionService` rather than re-spelled here — 6qvf's lesson,
    /// applied a further time.
    static let dayZeroByteExactBoundaryState =
        AdDetectionService.dayZeroRediffByteExactBoundaryState

    private static func dayZeroByteExactBothEdgesUnanchored(
        _ snapshot: PersistedStateSnapshot
    ) -> PersistedStateInvariantFinding {
        var violations: [String] = []
        var judged = 0
        for window in snapshot.eligibilityGatedAdWindows
        where window.boundaryState == dayZeroByteExactBoundaryState {
            judged += 1
            let startAnchor = AutoSkipEdgeAnchor(rawValue: window.startEdgeAnchor) ?? .unanchored
            let endAnchor = AutoSkipEdgeAnchor(rawValue: window.endEdgeAnchor) ?? .unanchored
            guard startAnchor == .unanchored, endAnchor == .unanchored else { continue }
            violations.append(
                "window=\(window.windowId) asset=\(window.assetId)"
                    + " span=\(format(window.startTime))-\(format(window.endTime))"
                    + " start_anchor=\(sanitize(window.startEdgeAnchor))"
                    + " end_anchor=\(sanitize(window.endEdgeAnchor))"
                    + " eligibility_gate=\(sanitize(window.eligibilityGate ?? "nil"))"
                    + " decision_state=\(sanitize(window.decisionState))"
            )
        }
        return PersistedStateInvariantFinding(
            invariant: .dayZeroByteExactBothEdgesUnanchored,
            violations: violations.count,
            population: judged,
            abstained: 0,
            abstainReason: nil,
            witnesses: Array(violations.prefix(maxWitnessesPerInvariant))
        )
    }

    // MARK: Invariant 7

    /// The abstain reason's fixed prefix; the column-join count follows it so
    /// the launch census still carries the bead's number while the recompute
    /// is unpaid. No spaces and no `=` before the `;`, because the wire format
    /// splits on whitespace and a parser reads the first `=` as the key's.
    static let chunkSetHashNotComputed = "chunk_set_hash_not_computed"

    private static func semanticScanVersionUnrelatedToChunkSet(
        _ snapshot: PersistedStateSnapshot
    ) -> PersistedStateInvariantFinding {
        var violations: [String] = []
        var judged = 0
        var abstained = 0
        var inPopulation = 0
        var columnJoinEmpty = 0
        for row in snapshot.transcriptVersionRelations {
            // The population is assets carrying BOTH. Scan rows with no chunk
            // rows at all is a different shape (a transcript that was deleted
            // under its scans) and is not judged here; neither is an asset
            // nothing has scanned.
            guard row.scanRowCount > 0, row.chunkRowCount > 0 else { continue }
            inPopulation += 1
            let columnJoinRows = row.columnJoinRowCount
            if columnJoinRows == 0 { columnJoinEmpty += 1 }
            // No recompute ⇒ no evidence in either direction. The column join
            // is NOT consulted as a substitute: a non-empty one is a
            // coincidence of population and can name stale rows (5 of 37
            // stamps on the 2026-09-08 pull), so it may not clear an asset.
            guard let hash = row.chunkSetHash, let atCurrent = row.scanRowsAtChunkSetHash else {
                abstained += 1
                continue
            }
            judged += 1
            guard atCurrent == 0 else { continue }
            violations.append(
                "asset=\(sanitize(row.assetId)) scan_rows=\(row.scanRowCount)"
                    + " scan_versions=\(row.scanVersions.count)"
                    + " chunk_rows=\(row.chunkRowCount) chunk_stamps=\(row.chunkStamps.count)"
                    + " chunk_null_rows=\(row.chunkNullRows)"
                    + " column_join_rows=\(columnJoinRows)"
                    + " chunk_set_hash=\(sanitize(hash))"
                    + " scan_rows_at_chunk_set_hash=\(atCurrent)"
            )
        }
        return PersistedStateInvariantFinding(
            invariant: .semanticScanVersionUnrelatedToChunkSet,
            violations: violations.count,
            population: judged,
            abstained: abstained,
            abstainReason: abstained > 0
                ? "\(chunkSetHashNotComputed);column_join_empty=\(columnJoinEmpty)/\(inPopulation)"
                : nil,
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
