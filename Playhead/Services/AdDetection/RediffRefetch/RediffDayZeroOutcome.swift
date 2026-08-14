// RediffDayZeroOutcome.swift
// playhead-p70f: make the DAY-0 rediff path ACCOUNTABLE.
//
// THE BUG THIS EXISTS TO KILL. On the owner's device the day-0 path spent
// 299.6 MB of B-copy downloads and produced zero ad windows, zero state rows,
// and zero non-zero outcome counters — every bucket in
// `rediff_bandwidth_ledger` read 0 while ~300 MB went out the door. The cause
// was structural, not a single bad branch:
//
//   * `mintByteExactDayZeroMarks` had ELEVEN silent `return 0` / `continue`
//     exits, all of which collapsed into the one indistinguishable
//     `.dayZeroUnmarked` outcome.
//   * `.dayZeroUnmarked` routes to `accumulateBandwidthOnly`, which hardcodes
//     all four ledger counters to 0 and writes no per-asset row — so
//     `failedCount == 0` was never evidence of success, and `unchangedCount` /
//     `failedCount` / `parkedCount` were structurally UNREACHABLE from day-0.
//   * The day-0 fetch `catch` records `.dayZeroUnmarked` too, so a thrown
//     network error and a clean "diffed fine, nothing diverged" run left
//     byte-identical traces.
//
// So this file introduces the vocabulary that makes those outcomes DISTINCT,
// and the pure policy that stops the path re-spending ~108 MB on every replay
// of the same episode.
//
// WHY A SEPARATE RECORD, NOT `rediff_refetch_state`. The xsdz.36.4 "poisoning
// fix" deliberately does NOT advance `rediff_refetch_state` on an unmarked
// day-0 run: that table drives the LAGGED sweep's eligibility, and a day-0
// miss must leave the asset enumerable so the lagged sweep can still recover
// its ads later. That decision is preserved verbatim. Day-0 accountability
// therefore lands in its OWN per-asset table (`rediff_day_zero_attempts`),
// which no lagged query reads.

import Foundation

// MARK: - Exit taxonomy

/// The terminal reason ONE day-0 rediff attempt ended, one case per formerly
/// silent exit. Persisted by raw value in `rediff_day_zero_attempts.lastExit`
/// and surfaced in the diagnostics bundle, so "which of the exits fired?" is
/// answerable from a database pull or a support bundle — no sysdiagnose.
///
/// Ordering of the cases follows the order they can fire along the path:
/// pre-fetch local checks first, then the fetch, then the per-B diffs, then
/// the mint.
enum RediffDayZeroExit: String, Sendable, Equatable, Codable, CaseIterable {

    // MARK: Pre-fetch (FREE — these cost no bandwidth, playhead-p70f change 3)

    /// The service has no `dayZeroMinter` wired, so day-0 has no marking path
    /// at all. Defensive: `runDayZeroRefetch` guards on this before fetching.
    case minterUnavailable = "minter_unavailable"

    /// `store.fetchAsset` returned nil — no `analysis_assets` row for the id.
    /// Formerly `AdDetectionService.mintByteExactDayZeroMarks`'s "no asset row".
    ///
    /// THE ONE EXIT THAT CANNOT BE PERSISTED. `rediff_day_zero_attempts` is
    /// keyed `analysisAssetId … REFERENCES analysis_assets(id)` and the store
    /// runs with `PRAGMA foreign_keys = ON`, so by construction there is no row
    /// to hang the record off — the insert fails the FK check and the recorder
    /// logs it instead. That is a tautology of a per-asset table, not an
    /// oversight, and it is bounded: this exit is free (no fetch), the ledger
    /// is untouched, and the os_log breadcrumb still names it. Pinned by
    /// `RediffDayZeroAttemptStoreTests.assetRowMissingCannotBePersisted` so
    /// nobody later reads the taxonomy and assumes a DB pull would show it.
    case assetRowMissing = "asset_row_missing"

    /// `store.fetchAsset` THREW. Distinct from `assetRowMissing`: a store
    /// failure is transient, a missing row is not.
    case assetFetchFailed = "asset_fetch_failed"

    /// The asset row's `sourceURL` did not resolve to an anchored regular file
    /// (missing, symlink, empty, or not a file URL) — there is no A-side to
    /// diff against. This is the M4 candidate the p70f trace ranked second.
    case aSideNotAnchored = "a_side_not_anchored"

    /// The A-side file exists but could not be mmapped.
    case aSideReadFailed = "a_side_read_failed"

    /// The trigger declined to attempt because a recent attempt for this asset
    /// is still inside its backoff window, or the per-asset attempt budget is
    /// exhausted. Costs ZERO bytes — this is the replay-bleed fix.
    case suppressedByBackoff = "suppressed_by_backoff"

    /// A day-0 attempt for this asset is ALREADY running in this process. Two
    /// play paths (`PlayheadRuntime` streaming + local) and the
    /// "Download & Analyze" preparation kickoff can all reach the trigger for
    /// one episode; without this guard they each spend a full k-way fetch.
    case alreadyInFlight = "already_in_flight"

    // MARK: Gate refusals (playhead-4dqe — FREE, and formerly SILENT)
    //
    // Until playhead-4dqe the transport/power gate refused with a bare
    // `return SweepSummary()`. On a device where day-0 never ran, "the network
    // was wrong", "the phone was unplugged", "the flag is off" and "the wiring
    // was dropped" all looked exactly alike — from inside the app and from a
    // database pull. These cases make each refusal nameable. They cost no bytes
    // and are all retryable: the very next play on a different network, on a
    // charger, or after the budget window rolls, can succeed.

    /// No reachable network path at all.
    case deniedUnreachable = "denied_unreachable"

    /// iOS Low Data Mode is active on the current path. Refused on BOTH
    /// transports and regardless of the user's cellular setting — Low Data Mode
    /// is the user's OS-level instruction and an in-app toggle is not consent to
    /// override it.
    case deniedLowDataMode = "denied_low_data_mode"

    /// The path is cellular (or an "expensive" metered path such as a personal
    /// hotspot) and the user has not turned on cellular preparation. The
    /// shipping default is WiFi-only, so this is the EXPECTED refusal for a
    /// user who has never visited the setting — not a defect.
    case deniedCellularNotAllowed = "denied_cellular_not_allowed"

    /// Unplugged, with no deep-scan opt-in and no explicit
    /// "Download & Analyze" tap.
    case deniedPower = "denied_power"

    /// The rolling 24 h day-0 byte budget (`RediffDayZeroDailyBudget`) has no
    /// room for a full k-way attempt. Costs zero bytes by construction — this
    /// is the check that stops the newly-permitted cellular transport from
    /// having no ceiling at all.
    case deniedDailyBudget = "denied_daily_budget"

    // MARK: Fetch

    /// The k-way B-copy fetch threw. Formerly indistinguishable from every
    /// other unmarked exit; this is the E1 candidate in the p70f trace.
    case fetchFailed = "fetch_failed"

    /// Fewer B-copies came back than `RediffSlotOwnership.dayZeroMinKWayBCopies`
    /// — below the collision-recovery floor, so the mint refuses to diff.
    case tooFewBCopies = "too_few_b_copies"

    // MARK: Diff / mint

    /// B-copies arrived but NOT ONE produced an accepted byte diff: every one
    /// was unreadable, or the byte gate rejected it (no chained runs /
    /// non-monotonic beyond recovery / re-encode CDN).
    ///
    /// This is the case that distinguishes "the aligner found nothing to work
    /// with" from `noDivergentSlot` below. `RediffByteAligner` is an MP3-frame
    /// parser by construction; every `sourceURL` on device ends in `.mp3` but
    /// `DownloadManager` warns the suffix is normalized and "the filename is
    /// not evidence that the bytes are MP3". If the bytes are AAC/M4A the
    /// aligner finds no frames and EVERY diff gate-rejects — which shows up
    /// here, with `bSidesGateRejected == bSideCount`.
    case noAcceptedByteDiff = "no_accepted_byte_diff"

    /// At least one byte diff was ACCEPTED by the gate, but the union of the
    /// per-persona divergent slots was empty — the copies agreed. This is the
    /// M9 candidate the p70f trace ranked most likely, and it is now provably
    /// distinct from `noAcceptedByteDiff`.
    case noDivergentSlot = "no_divergent_slot"

    /// Divergent slots were found but every one overlapped an AdWindow that
    /// already existed (including a user-vetoed `.reverted` row), so the
    /// idempotency filter dropped them all. Nothing new to mint — NOT a failure.
    case allSlotsAlreadyCovered = "all_slots_already_covered"

    /// Windows were built but `upsertHotPathAdWindows` threw.
    case persistFailed = "persist_failed"

    /// ≥1 mark-only banner was minted. The success terminal.
    case marked = "marked"

    /// playhead-ug9m — THE SURFACED "PERMANENTLY FROZEN" STATE. This asset's
    /// day-0 marks are all degraded (no `.rediffByteExact` anchor on any of
    /// them, so every one banners and none can auto-skip) AND the one rescue
    /// re-attempt this generation grants has already been spent.
    ///
    /// It exists as its own case rather than folding into `.marked` because the
    /// two say opposite things about whether the user is getting what the
    /// detection earned. `.marked` means "day-0 is done here"; this means
    /// "day-0 is done here and the result is worse than the detector was capable
    /// of, and nothing further will be attempted until
    /// `DayZeroRediffAttemptPolicy.currentGeneration` moves". A support pull can
    /// count these; silence could not be counted.
    ///
    /// FREE (spends nothing — it is a pre-fetch refusal) and NOT retryable: it
    /// is a terminal statement about this generation by construction.
    case rescueExhausted = "rescue_exhausted"

    /// Whether this exit means the attempt spent (or could have spent) the
    /// ~54 MB × K full fetch. The pre-fetch exits are free by construction
    /// (playhead-p70f change 3 moved them ahead of the download); everything
    /// from `fetchFailed` on has already paid for bytes.
    var spentBandwidth: Bool {
        switch self {
        case .minterUnavailable, .assetRowMissing, .assetFetchFailed,
             .aSideNotAnchored, .aSideReadFailed, .suppressedByBackoff,
             .alreadyInFlight, .deniedUnreachable, .deniedLowDataMode,
             .deniedCellularNotAllowed, .deniedPower, .deniedDailyBudget,
             .rescueExhausted:
            return false
        case .fetchFailed, .tooFewBCopies, .noAcceptedByteDiff, .noDivergentSlot,
             .allSlotsAlreadyCovered, .persistFailed, .marked:
            return true
        }
    }

    /// Whether a LATER day-0 attempt for the same asset could plausibly
    /// succeed. `false` for the terminal states — `marked` (already done) and
    /// the local-state failures that will read identically next time.
    ///
    /// `aSideNotAnchored` is deliberately RETRYABLE: on the
    /// "Download & Analyze" path the pinned file genuinely materializes later,
    /// and on a play path an LRU eviction can be undone by a re-download. The
    /// backoff, not this flag, is what stops it spinning.
    var isRetryable: Bool {
        switch self {
        case .marked, .rescueExhausted:
            return false
        case .minterUnavailable, .assetRowMissing, .assetFetchFailed,
             .aSideNotAnchored, .aSideReadFailed, .suppressedByBackoff,
             .alreadyInFlight, .fetchFailed, .tooFewBCopies,
             .noAcceptedByteDiff, .noDivergentSlot, .allSlotsAlreadyCovered,
             .persistFailed, .deniedUnreachable, .deniedLowDataMode,
             .deniedCellularNotAllowed, .deniedPower, .deniedDailyBudget:
            return true
        }
    }
}

// MARK: - Gate refusal → exit (playhead-4dqe)

extension DayZeroTransportDecision {
    /// The exit a refusal RECORDS, or `nil` when there is nothing to record.
    ///
    /// `.allow` records nothing (the attempt's own outcome does). `.denyDisabled`
    /// records nothing either, and that one is deliberate: `triggerIfEligible`
    /// short-circuits on the inert flag BEFORE this decision is ever computed,
    /// so a row here would be unreachable — and an unreachable branch whose doc
    /// comment claims otherwise is a defect this arc has already been bitten by.
    /// The flag-off build stays byte-identical, writing nothing.
    var deniedExit: RediffDayZeroExit? {
        switch self {
        case .allow, .denyDisabled: return nil
        case .denyUnreachable: return .deniedUnreachable
        case .denyLowDataMode: return .deniedLowDataMode
        case .denyCellularNotAllowed: return .deniedCellularNotAllowed
        case .denyPower: return .deniedPower
        }
    }
}

// MARK: - Mint outcome

/// What ONE call to `mintByteExactDayZeroMarks` actually did. Replaces the
/// bare `Int` return, whose `0` collapsed nine distinct failure modes into one
/// indistinguishable value.
///
/// The per-B-side counters are the diagnostic payload: they are what separates
/// "the byte aligner rejected every copy" (`bSidesGateRejected == bSideCount`,
/// the AAC-bytes-behind-an-.mp3-suffix hypothesis) from "the copies were
/// diffed cleanly and simply agreed" (`bSidesAccepted > 0`,
/// `divergentSlotCount == 0`).
struct RediffDayZeroMintOutcome: Sendable, Equatable {
    /// Mark-only AdWindows actually persisted. `> 0` iff `exit == .marked`.
    var markCount: Int = 0
    /// Which exit fired.
    var exit: RediffDayZeroExit
    /// How many B-copies the mint was handed.
    var bSideCount: Int = 0
    /// B-copies whose byte diff the gate ACCEPTED (contributed a slot list).
    var bSidesAccepted: Int = 0
    /// B-copies the byte gate REJECTED (no chained runs / non-monotonic /
    /// re-encode). The aligner ran; it found nothing usable.
    var bSidesGateRejected: Int = 0
    /// B-copies that were not anchored regular files, or could not be mmapped.
    /// The aligner never ran on these.
    var bSidesUnreadable: Int = 0
    /// Size of the UNIONed divergent slot list, BEFORE the existing-window
    /// idempotency filter. `> 0` with `markCount == 0` means
    /// `allSlotsAlreadyCovered`.
    var divergentSlotCount: Int = 0
    /// playhead-ug9m: how many of `markCount` were STRICT monotonic-clean
    /// byte-exact slots. `markCount - strictMarkCount` is the 9s6q
    /// segment-recovered remainder.
    ///
    /// This is the quantity the whole bead turns on, and it was previously
    /// visible only in an os_log line: a mint of 4 marks, 0 of them strict, is
    /// the D9B513CD case — a `.marked` terminal that delivers no skip at all.
    ///
    /// playhead-pyq7 KEPT THIS MEANING EXACTLY. It names the ACCEPTANCE ARM
    /// (`alignment.monotonicClean`), not "how many marks auto-skip" — the two
    /// stopped being the same number when segment-recovered slots were
    /// promoted, and widening this counter to cover both would be one value
    /// standing for two different things, which is the defect class this repo
    /// keeps finding. The promotion population has its OWN counter below, and
    /// it is still `strictMarkCount` that the ug9m supersede guard and the
    /// rescue census are stated over.
    var strictMarkCount: Int = 0
    /// playhead-pyq7: how many of `markCount` were 9s6q SEGMENT-RECOVERED slots
    /// PROMOTED to skip grade — `.rediffByteExact` on both edges and, under
    /// `RediffActivation.dayZeroByteExactAutoSkipEnabled`, `.eligible`.
    ///
    /// Read it as a DISPOSITION, not as a population: with
    /// `RediffActivation.dayZeroSegmentRecoveredAutoSkipEnabled` off it is `0`
    /// on a mint whose every slot was recovered, and that zero is the switch
    /// being off rather than the arm not firing. The population itself is
    /// `markCount - strictMarkCount`, unchanged; the remainder
    /// `markCount - strictMarkCount - segmentRecoveredSkipGradeMarkCount` is
    /// what still banners.
    var segmentRecoveredSkipGradeMarkCount: Int = 0
    /// playhead-ug9m: degraded day-0 rows from an EARLIER attempt that this
    /// mint's strict slots superseded (retired and replaced). `0` on every
    /// first-listen mint; non-zero only on a rescue that actually improved
    /// something.
    var supersededMarkCount: Int = 0
    /// Free-text detail (an error description). Truncated by the recorder
    /// before it reaches the database.
    var detail: String?
    /// playhead-3zxd: what THIS mint's byte diffs saw — the phantom-slot
    /// instrumentation, aggregated across the personas the gate accepted. See
    /// `RediffByteMintDiagnostics`. `.empty` on every exit that never reached a
    /// diff, which is honest: no diff ran, so there is nothing to report.
    var byteDiagnostics: RediffByteMintDiagnostics = .empty

    /// Convenience for the pure pre-fetch / guard exits, which have no
    /// B-copies and no slots.
    static func blocked(_ exit: RediffDayZeroExit, detail: String? = nil) -> RediffDayZeroMintOutcome {
        RediffDayZeroMintOutcome(exit: exit, detail: detail)
    }
}

// MARK: - A-side resolution

/// Result of resolving the PINNED played A-side for a day-0 byte diff.
///
/// Deliberately its own two-case enum rather than `Result<Data, …>`: the
/// blocking value is a terminal EXIT, not an `Error` — `RediffDayZeroExit`
/// includes `.marked`, and conforming a success case to `Error` to satisfy a
/// generic constraint would be a lie in the type system.
enum RediffDayZeroASideResolution {
    /// The A-side file was read-only mmapped and is ready to diff.
    case ready(Data)
    /// The A-side is unavailable; this exit is what the attempt will record.
    case blocked(RediffDayZeroExit)
}

// MARK: - Durable attempt record

/// One row of `rediff_day_zero_attempts` — the per-asset day-0 history that
/// makes both accountability (change 1) and idempotency (change 2) possible.
///
/// Deliberately SEPARATE from `RediffRefetchStateRow`: writing day-0 outcomes
/// into `rediff_refetch_state` would resolve/advance the LAGGED sweep's
/// eligibility for an asset day-0 failed on, which is exactly the poisoning
/// the xsdz.36.4 fix prevents.
struct RediffDayZeroAttemptRecord: Sendable, Equatable {
    let analysisAssetId: String
    /// Total day-0 attempts made for this asset, including this one. Drives
    /// the escalating backoff and the hard attempt cap.
    let attemptCount: Int
    /// Unix seconds of the most recent attempt.
    let lastAttemptAt: Double
    /// The most recent attempt's terminal exit.
    let lastExit: RediffDayZeroExit
    let lastMarkCount: Int
    let lastBSideCount: Int
    let lastBSidesAccepted: Int
    let lastBSidesGateRejected: Int
    let lastBSidesUnreadable: Int
    let lastDivergentSlotCount: Int
    /// Bytes the most recent attempt's full fetch spent.
    let lastFullFetchBytes: Int
    /// Cumulative bytes every day-0 attempt for this asset has spent. This is
    /// the number that would have read ~108 MB × 3 on the owner's device and
    /// made the replay bleed obvious at a glance.
    let totalFullFetchBytes: Int
    /// How many times a day-0 kickoff for this asset was DECLINED without
    /// spending anything — the direct on-device evidence that the replay bleed
    /// is fixed. Deliberately separate from `attemptCount`: a suppression is
    /// not an attempt and must never consume the attempt budget.
    let suppressedCount: Int
    /// Unix seconds of the most recent suppression, `nil` when never suppressed.
    let lastSuppressedAt: Double?
    let lastDetail: String?
    /// Which `DayZeroRediffAttemptPolicy` generation spent this budget. A
    /// record from a DIFFERENT generation is not evidence about THIS build's
    /// day-0, so the budget starts over — see
    /// `DayZeroRediffAttemptPolicy.currentGeneration`.
    let policyGeneration: Int
    /// playhead-ug9m: how many RESCUE re-attempts this asset has spent in the
    /// current generation — a re-attempt made after day-0 had ALREADY marked the
    /// asset, granted only because those marks were all degraded.
    ///
    /// Separate from `attemptCount` because it bounds a different thing. The
    /// attempt budget bounds the FIRST-listen probe; this bounds the second
    /// chance, and it is checked whatever exit the previous attempt took, so a
    /// rescue that lands in `.noDivergentSlot` cannot then fall through into the
    /// ordinary three-attempt budget and spend another ~216 MB re-deriving
    /// nothing. Capped at `DayZeroRediffAttemptPolicy.maxRescueAttempts`.
    let rescueAttemptCount: Int
    /// playhead-3oyz: how many same-session retries have been CLAIMED for this
    /// asset — a claim is written BEFORE the retry's delay, so a retry the
    /// process never lived to run still leaves a row that says one was owed
    /// (the playhead-fil5 precedent: a dropped request must not read as "never
    /// happened"). `lastRetryClaimAt > lastAttemptAt` is the queryable
    /// signature of a claimed-but-dropped retry; an executed retry follows its
    /// claim with an ordinary attempt (own exit, own `lastAttemptAt`).
    let retryClaimCount: Int
    /// Unix seconds of the most recent retry claim, `nil` when none was ever
    /// made.
    let lastRetryClaimAt: Double?
    /// playhead-3zxd: the byte-diff instrumentation from the most recent
    /// attempt that RAN A DIFF — the six `rediff_day_zero_attempts` columns a
    /// device pull reads to answer "did the phantom fire, and did the fix
    /// prevent it?".
    ///
    /// THE INVARIANT, and it is the only thing that makes these columns
    /// readable: **this describes the attempt `lastAttemptAt` names.** So it is
    /// overwritten exactly when `lastAttemptAt` moves — see
    /// `DayZeroRediffAttemptPolicy.advance` — and NOT, as an earlier draft had
    /// it, by every attempt including the free ones.
    ///
    /// It is deliberately NOT grouped with `lastMarkCount` and the per-B census,
    /// which do update on a free exit. Those report something real about the
    /// decline (which exit fired, how many B-copies were on hand); these report
    /// a MEASUREMENT of a diff, and a decline ran none — so overwriting them
    /// destroys an observation and substitutes nothing. Worse than nothing: the
    /// validation query reads `lastRunsFound = 0` as VACUOUS ("re-encoding CDN,
    /// or the bytes are not MP3"), so the zeroes are read as an observation
    /// about the CDN that nobody made.
    let byteDiagnostics: RediffByteMintDiagnostics

    init(
        analysisAssetId: String,
        attemptCount: Int,
        lastAttemptAt: Double,
        lastExit: RediffDayZeroExit,
        lastMarkCount: Int = 0,
        lastBSideCount: Int = 0,
        lastBSidesAccepted: Int = 0,
        lastBSidesGateRejected: Int = 0,
        lastBSidesUnreadable: Int = 0,
        lastDivergentSlotCount: Int = 0,
        lastFullFetchBytes: Int = 0,
        totalFullFetchBytes: Int = 0,
        suppressedCount: Int = 0,
        lastSuppressedAt: Double? = nil,
        lastDetail: String? = nil,
        policyGeneration: Int = DayZeroRediffAttemptPolicy.currentGeneration,
        rescueAttemptCount: Int = 0,
        retryClaimCount: Int = 0,
        lastRetryClaimAt: Double? = nil,
        byteDiagnostics: RediffByteMintDiagnostics = .empty
    ) {
        self.analysisAssetId = analysisAssetId
        self.attemptCount = attemptCount
        self.lastAttemptAt = lastAttemptAt
        self.lastExit = lastExit
        self.lastMarkCount = lastMarkCount
        self.lastBSideCount = lastBSideCount
        self.lastBSidesAccepted = lastBSidesAccepted
        self.lastBSidesGateRejected = lastBSidesGateRejected
        self.lastBSidesUnreadable = lastBSidesUnreadable
        self.lastDivergentSlotCount = lastDivergentSlotCount
        self.lastFullFetchBytes = lastFullFetchBytes
        self.totalFullFetchBytes = totalFullFetchBytes
        self.suppressedCount = suppressedCount
        self.lastSuppressedAt = lastSuppressedAt
        self.lastDetail = lastDetail
        self.policyGeneration = policyGeneration
        self.rescueAttemptCount = rescueAttemptCount
        self.retryClaimCount = retryClaimCount
        self.lastRetryClaimAt = lastRetryClaimAt
        self.byteDiagnostics = byteDiagnostics
    }
}

// MARK: - Idempotency + backoff policy

/// The PURE decision "may day-0 spend ~54 MB × K on this asset right now?".
///
/// WHAT IT FIXES. Before playhead-p70f, day-0 had no idempotency, no backoff
/// and no cap: `DayZeroRediffTrigger` hardcoded `attemptState: .initial` and
/// consulted no prior state, `kickOffDayZeroRediff` fired from both play paths
/// with no per-episode guard, and an unmarked run wrote no state row — so
/// nothing existed for a later run to read. Replaying one episode cost ~108 MB
/// EVERY TIME, forever. The 299.6 MB on the owner's device is consistent with
/// a single episode replayed three times.
///
/// Pure over `(record, now)` so the whole truth table is unit-tested without a
/// store, a clock, or a network.
enum DayZeroRediffAttemptPolicy {

    /// Hard ceiling on day-0 attempts per asset PER `currentGeneration`, and
    /// only attempts that actually spent bandwidth count (see `advance`).
    /// Day-0 is an OPTIMISTIC probe;
    /// when it has failed this many times the lagged ≥3-day sweep is the
    /// backstop and is strictly cheaper (it pre-checks with a ~128 KB ranged
    /// sample before committing to a full fetch, which day-0 cannot do).
    static let maxAttempts = 3

    /// Backoff after the FIRST unsuccessful attempt. A same-day replay — the
    /// dominant real-world repeat — is fully suppressed by this alone.
    static let baseBackoff: TimeInterval = 24 * 60 * 60

    /// Ceiling on the doubling, so attempt 3's window stays bounded.
    static let maxBackoff: TimeInterval = 7 * 24 * 60 * 60

    /// The generation of day-0 behavior this build implements.
    ///
    /// WHY THIS EXISTS. `maxAttempts` is a HARD, permanent cap: once an asset
    /// has spent it, `decide` suppresses forever. That is the right bandwidth
    /// policy — but on its own it is also a trap, because playhead-p70f
    /// deliberately did NOT fix the reason day-0 mints nothing. Without an
    /// escape hatch, every episode the owner plays across three sessions burns
    /// its budget on a failure mode that is about to be fixed, and when the fix
    /// lands those episodes stay permanently dead: the owner would repair the
    /// mint and observe no change at all.
    ///
    /// So the budget is scoped to a GENERATION rather than to all time. A
    /// record stamped with a different generation describes a build whose
    /// outcome-determining day-0 behavior this one no longer has, and is
    /// therefore not evidence about this build — the budget starts over.
    ///
    /// **BUMP THIS whenever day-0's outcome-determining behavior changes** —
    /// the mint, the byte gate, the k-way persona staging, or the B-copy floor.
    /// Bumping costs at most `maxAttempts` fresh attempts per asset that is
    /// actually replayed; NOT bumping silently withholds the fix from every
    /// episode that already exhausted its budget.
    ///
    /// `.marked` is exempt and stays terminal across generations — EXCEPT for
    /// the narrow playhead-ug9m rescue below: the marks are already persisted,
    /// so a re-fetch would spend ~108 MB to mint nothing, UNLESS every one of
    /// those marks is degraded, in which case a re-fetch is the only way to
    /// mint what the detector was capable of.
    ///
    /// **2 (playhead-ug9m), was 1.** Bumped because day-0's outcome-determining
    /// behavior changed twice without a bump: playhead-qs0d changed what a mint
    /// PERSISTS (a strict slot now carries `.rediffByteExact` anchors and
    /// `eligibilityGate = .eligible` instead of `unanchored`/`.markOnly`), and
    /// this bead lets a strict re-mint supersede its own degraded rows. Assets
    /// that exhausted their budget under generation 1 were measuring a build
    /// whose day-0 could not stamp an anchor at all.
    static let currentGeneration = 2

    /// playhead-3oyz — the SAME-SESSION retry bound: how many immediate
    /// re-attempts one trigger invocation may make after a fetch failure that
    /// spent ~zero bytes.
    ///
    /// **1, derived from the attempt budget, not from optimism.** A retry is
    /// durably recorded as an attempt (its own exit, its own row advance), so
    /// every retry consumes the `maxAttempts = 3` generation budget. First
    /// attempt + 1 retry leaves exactly one budgeted attempt for a later
    /// (≥ 24 h) session — the existing cross-session ladder stays a real
    /// backstop. Two retries would let a single session-long outage burn the
    /// whole generation, converting "one bad session" into "day-0 dead until
    /// the generation moves", which is strictly worse than the defect this
    /// fixes.
    static let maxSameSessionRetries = 1

    /// playhead-3oyz — the delay before the same-session retry.
    ///
    /// 30 s, sized from the F4CE7F47 witness timeline. The download-time
    /// kickoff routinely waits minutes for its preconditions (the witness
    /// waited 120.4 s; the device pull shows 20.7–507 s), so the session
    /// demonstrably survives a 30 s pause — and the worst case this adds
    /// (30 s + one more timeout window, ~2.5 min) still finishes hours before
    /// any realistic first listen. Shorter would retry INTO the transient
    /// congestion a just-completed ~50 MB episode download leaves behind,
    /// which is the most plausible cause of a -1001 seconds after a
    /// successful download.
    static let sameSessionRetryDelaySeconds: TimeInterval = 30

    /// playhead-ug9m — the RESCUE bound. How many re-attempts, per generation,
    /// an asset that ALREADY has day-0 marks may spend.
    ///
    /// **1, and it is enforced by a persisted counter, not by hope.** A rescue
    /// costs a full k-way fetch (~108 MB at `dayZeroKWayFetchCount == 2`), and
    /// the failure mode this bead is fixing was itself created by a policy that
    /// looked bounded and was not. One is enough to be worth doing: the trapped
    /// populations are (a) rows minted before qs0d, which are unanchored even
    /// when strict and whose re-mint on this build stamps the anchor they
    /// earned, and (b) rows whose first attempt came out wholly non-strict,
    /// which get exactly one draw at different personas. If the second draw is
    /// also degraded, the asset is `.frozen` and SAYS SO
    /// (`RediffDayZeroExit.rescueExhausted`) rather than going quiet.
    ///
    /// Generation-scoped like `maxAttempts`, and for the same reason: a bump is
    /// a deliberate statement that day-0 behaves differently now, which is
    /// exactly when a frozen asset deserves another look.
    static let maxRescueAttempts = 1

    /// The decision, with the reason attached so the caller can RECORD a
    /// suppression rather than silently doing nothing (the original sin).
    enum Decision: Sendable, Equatable {
        /// Spend the bytes. `attemptNumber` is what the resulting record will
        /// carry (1 for a first attempt).
        case attempt(attemptNumber: Int)
        /// Do not spend. `.marked` already produced this asset's marks, or the
        /// attempt budget is spent, or we are inside the backoff window.
        case suppress(reason: RediffDayZeroExit, nextEligibleAt: Double?)

        var isAttempt: Bool {
            if case .attempt = self { return true }
            return false
        }
    }

    /// Backoff window applying AFTER `attemptCount` unsuccessful attempts.
    /// 24 h, 48 h, 96 h … capped at `maxBackoff`.
    static func backoff(afterAttempts attemptCount: Int) -> TimeInterval {
        guard attemptCount > 0 else { return 0 }
        // `min(_:62)` keeps the shift inside Double's exact-integer range even
        // if a corrupted row carries an absurd count.
        let doublings = Double(min(attemptCount - 1, 62))
        return min(baseBackoff * pow(2, doublings), maxBackoff)
    }

    /// May a day-0 attempt run for this asset now?
    ///
    /// - `nil` record ⇒ never attempted ⇒ attempt 1.
    /// - the asset already has day-0 marks AND its rescue budget is spent ⇒
    ///   suppress as `.rescueExhausted`, whatever exit the last attempt took
    ///   (playhead-ug9m; see `maxRescueAttempts`).
    /// - last exit `.marked` ⇒ NEVER again, with ONE exception. The marks are
    ///   already persisted and the mint's own overlap filter would drop
    ///   everything anyway; re-fetching would spend ~108 MB to mint zero
    ///   windows. This is the single most valuable suppression. The exception
    ///   (playhead-ug9m) is an asset from an OLDER generation whose every day-0
    ///   mark is DEGRADED — see `DayZeroMarkCensus.isRescuable` for why that is
    ///   a fact about persisted rows rather than a guess about provenance.
    /// - record from an OLDER generation ⇒ the budget starts over (see
    ///   `currentGeneration`); `.marked` is checked first and stays terminal
    ///   apart from the rescue.
    /// - attempt budget exhausted ⇒ suppress until the generation changes.
    /// - inside the backoff window ⇒ suppress until it elapses.
    ///
    /// - Parameter markCensus: what is ON DISK for this asset right now, read
    ///   in the SAME snapshot as `record` (see `DayZeroAttemptContext`). The
    ///   default is `.empty` — "no day-0 marks" — which reproduces the
    ///   pre-playhead-ug9m behavior exactly: no rescue is ever granted and no
    ///   ceiling ever fires. That is the CONSERVATIVE direction, so a caller
    ///   that omits it withholds a fix rather than spending bandwidth.
    static func decide(
        record: RediffDayZeroAttemptRecord?,
        markCensus: DayZeroMarkCensus = .empty,
        now: Double
    ) -> Decision {
        guard let record else { return .attempt(attemptNumber: 1) }
        // playhead-ug9m — THE RESCUE CEILING, checked before anything else that
        // could grant an attempt. Deliberately keyed on "this asset HAS day-0
        // marks" rather than on `lastExit == .marked`: a rescue that ended in
        // `.noDivergentSlot` leaves a RETRYABLE exit behind, and without this
        // it would fall straight through into the ordinary three-attempt budget
        // — turning a bounded second chance into ~324 MB.
        if markCensus.hasMarks, record.rescueAttemptCount >= maxRescueAttempts {
            return .suppress(reason: .rescueExhausted, nextEligibleAt: nil)
        }
        guard record.lastExit.isRetryable else {
            // playhead-ug9m — THE RESCUE. Three conjuncts, each load-bearing:
            //   * `.marked` — the only non-retryable exit a rescue applies to.
            //     `.rescueExhausted` falls through to the suppression below,
            //     which is what makes it terminal.
            //   * a FOREIGN generation — this build's day-0 behaves differently
            //     from the one that produced these marks. Within a generation
            //     `.marked` stays exactly as terminal as it was.
            //   * `isRescuable` — every day-0 mark on this asset is degraded.
            //     One anchored row proves the mint could stamp anchors and did,
            //     which makes the unanchored siblings a deliberate qs0d
            //     withholding, not a loss.
            if record.lastExit == .marked,
               record.policyGeneration != currentGeneration,
               markCensus.isRescuable {
                return .attempt(attemptNumber: 1)
            }
            return .suppress(reason: record.lastExit, nextEligibleAt: nil)
        }
        // Deliberately AFTER the `.marked` check and BEFORE the budget check:
        // a stale generation resets the budget, but never resurrects an asset
        // whose marks are already on disk.
        guard record.policyGeneration == currentGeneration else {
            return .attempt(attemptNumber: 1)
        }
        guard record.attemptCount < maxAttempts else {
            return .suppress(reason: .suppressedByBackoff, nextEligibleAt: nil)
        }
        let nextEligibleAt = record.lastAttemptAt + backoff(afterAttempts: record.attemptCount)
        guard now >= nextEligibleAt else {
            return .suppress(reason: .suppressedByBackoff, nextEligibleAt: nextEligibleAt)
        }
        return .attempt(attemptNumber: record.attemptCount + 1)
    }

    /// playhead-3oyz — may THIS trigger invocation retry the attempt it just
    /// watched fail, right now, in this session?
    ///
    /// The distinction is MEASURED, never inferred from the exit code alone
    /// (the standing defect class: an exit that names "failure" read as if it
    /// named "bandwidth spent"). `measuredFullFetchBytes` is the sum of
    /// `byteCount` over the B-copies that actually LANDED (completed
    /// downloads) in the attempt — there is no denominator; it is compared to
    /// zero, and the question it answers is "did ANY B-copy land?". If the
    /// fetch never started it reads 0, which is retry-eligible — correct,
    /// because a fetch that never started spent nothing. Any non-zero value
    /// proves a whole ~54 MB copy landed (the population the p70f 24 h
    /// backoff exists for), so the comparison is `== 0`, not a threshold:
    /// the quantity is quantized at whole copies, and partial-transfer wire
    /// bytes are unmeasured by construction (the "~zero" in the bead).
    ///
    /// Scoped to `.fetchFailed` ONLY. Every other zero-cost exit — the
    /// pre-fetch blockers, the gate denials — is a statement about LOCAL
    /// state or user policy that a 30 s delay cannot change; re-running those
    /// would be a spin, not a retry.
    ///
    /// Cross-session behavior is deliberately untouched: this is consulted by
    /// the trigger against the summary it is already holding, never by
    /// `decide`, so the 24 h backoff ladder reads exactly as before
    /// (shape ii/iii were explicitly not approved).
    static func grantsSameSessionRetry(
        exit: RediffDayZeroExit?,
        measuredFullFetchBytes: Int,
        retriesUsed: Int
    ) -> Bool {
        guard retriesUsed < maxSameSessionRetries else { return false }
        guard exit == .fetchFailed else { return false }
        return measuredFullFetchBytes == 0
    }

    /// Fold one attempt's outcome into the durable record. Additive over the
    /// prior row: `attemptCount` and `totalFullFetchBytes` accumulate so the
    /// per-asset bleed is visible without joining anything.
    ///
    /// TWO THINGS THE BUDGET DELIBERATELY IGNORES.
    ///
    /// 1. A FREE exit (`exit.spentBandwidth == false`) moves neither
    ///    `attemptCount` nor `lastAttemptAt`. The budget and the escalating
    ///    backoff exist to bound BANDWIDTH; an exit that spent none must not
    ///    consume either. This is load-bearing, not tidiness: playhead-p70f
    ///    change 3 hoisted the A-side checks ahead of the fetch, and on the
    ///    streaming play path day-0 fires BEFORE `downloadComplete()`, so
    ///    `.aSideNotAnchored` is a routine, transient, zero-cost decline. Were
    ///    it to consume the budget, three streaming starts would permanently
    ///    lock out an episode day-0 had never even attempted — and the exit's
    ///    own documentation ("deliberately RETRYABLE… the backoff, not this
    ///    flag, is what stops it spinning") would be false. `lastExit` and the
    ///    per-B census still update, so the decline stays diagnosable.
    ///
    ///    playhead-3zxd R2: `byteDiagnostics` moves with `lastAttemptAt`, NOT
    ///    with the census. The census reports the decline; the diagnostics
    ///    report a diff, and a free exit ran none. See the third bullet below.
    /// 2. A record from an older `currentGeneration` contributes no attempt
    ///    count: this build's day-0 has not spent anything yet. Cumulative
    ///    bytes and suppression history are historical facts and still carry.
    ///
    /// 3. playhead-3zxd R2 — a FREE exit does not overwrite `byteDiagnostics`
    ///    either, for the SAME reason and by the SAME discriminator. The six
    ///    V48 columns are a measurement of a byte diff; an exit that ran no
    ///    diff carries `.empty`, and the upsert's `ON CONFLICT` sets all six
    ///    from `excluded` — so writing them unconditionally ZEROED a prior real
    ///    diff while `lastAttemptAt` kept advertising that diff's timestamp.
    ///    The row then read `lastRunsFound = 0`, which the validation query
    ///    calls VACUOUS ("re-encoding CDN"), i.e. a re-encode observation
    ///    nobody made. That is the standing defect class — a value that names
    ///    one thing read as though it named another — landing inside the
    ///    instrument built to detect it.
    static func advance(
        record: RediffDayZeroAttemptRecord?,
        assetId: String,
        outcome: RediffDayZeroMintOutcome,
        fullFetchBytes: Int,
        at now: Double
    ) -> RediffDayZeroAttemptRecord {
        let budgeted = record?.policyGeneration == currentGeneration ? record : nil
        let spentBandwidth = outcome.exit.spentBandwidth
        // playhead-ug9m: THIS attempt is a rescue exactly when the record it
        // folds into already said `.marked` — `decide` grants an attempt over a
        // `.marked` record through one branch and one branch only, so the prior
        // exit IS the discriminator and no extra parameter has to be threaded
        // through the service to carry it. It counts only if bytes were spent,
        // for the same reason `attemptCount` does.
        let isRescue = record?.lastExit == .marked && spentBandwidth
        return RediffDayZeroAttemptRecord(
            analysisAssetId: assetId,
            attemptCount: (budgeted?.attemptCount ?? 0) + (spentBandwidth ? 1 : 0),
            lastAttemptAt: spentBandwidth ? now : (budgeted?.lastAttemptAt ?? now),
            lastExit: outcome.exit,
            lastMarkCount: outcome.markCount,
            lastBSideCount: outcome.bSideCount,
            lastBSidesAccepted: outcome.bSidesAccepted,
            lastBSidesGateRejected: outcome.bSidesGateRejected,
            lastBSidesUnreadable: outcome.bSidesUnreadable,
            lastDivergentSlotCount: outcome.divergentSlotCount,
            lastFullFetchBytes: fullFetchBytes,
            totalFullFetchBytes: (record?.totalFullFetchBytes ?? 0) + fullFetchBytes,
            // Suppression history is carried forward untouched — an attempt is
            // not a suppression and must not reset the evidence that earlier
            // replays were declined.
            suppressedCount: record?.suppressedCount ?? 0,
            lastSuppressedAt: record?.lastSuppressedAt,
            lastDetail: outcome.detail.map { String($0.prefix(detailCharCap)) },
            policyGeneration: currentGeneration,
            // Generation-scoped like `attemptCount`: a bump grants one fresh
            // rescue, and within a generation the counter only ever rises.
            rescueAttemptCount: (budgeted?.rescueAttemptCount ?? 0) + (isRescue ? 1 : 0),
            // playhead-3oyz: retry-claim history is carried like suppression
            // history — a historical fact `noteRediffDayZeroRetryClaim` owns
            // and increments in place; an attempt never resets it.
            retryClaimCount: record?.retryClaimCount ?? 0,
            lastRetryClaimAt: record?.lastRetryClaimAt,
            // playhead-3zxd: NEVER accumulated, and carried forward on exactly
            // the same test `lastAttemptAt` uses — so the two always describe
            // the SAME attempt.
            //
            //   spent bytes           -> THIS attempt's, whatever they are. A
            //                            running total could never fall back to
            //                            zero once a pre-fix row had
            //                            contributed, and a stale set would
            //                            read as evidence about the current
            //                            build's aligner.
            //   free, budgeted row    -> the prior attempt's, because
            //                            `lastAttemptAt` is the prior
            //                            attempt's. Zeroing here is what R2
            //                            found: a real diff erased by a
            //                            locally-blocked replay.
            //   free, no budgeted row -> `.empty`, because `lastAttemptAt`
            //                            becomes `now` and nothing was measured
            //                            at `now`. `budgeted`, not `record`:
            //                            a foreign-generation row does not
            //                            carry its timestamp forward, so
            //                            carrying its diagnostics would re-open
            //                            the mismatch one generation out.
            //
            // NOT `outcome.bSideCount > 0` as the discriminator, which is the
            // reading this round first proposed. `mintByteExactDayZeroMarks`
            // resolves the A-side AFTER the k-way fetch and returns
            // `.aSideNotAnchored` / `.aSideReadFailed` / `.assetRowMissing` /
            // `.assetFetchFailed` with `bSideCount: bSideURLs.count`, which is
            // ≥ 2 there (it is past the `tooFewBCopies` floor) — so that test
            // reads TRUE on precisely the four free exits it was meant to
            // exclude. `spentBandwidth` is a property of the exit and cannot
            // drift from the timestamp it is paired with.
            byteDiagnostics: spentBandwidth
                ? outcome.byteDiagnostics
                : (budgeted?.byteDiagnostics ?? .empty)
        )
    }

    /// Cap on the persisted `lastDetail` string. Error descriptions can carry
    /// a whole URL + userInfo dump; the database is not a log.
    static let detailCharCap = 200
}
