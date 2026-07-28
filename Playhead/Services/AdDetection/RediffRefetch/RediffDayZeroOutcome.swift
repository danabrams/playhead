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

    /// Whether this exit means the attempt spent (or could have spent) the
    /// ~54 MB × K full fetch. The pre-fetch exits are free by construction
    /// (playhead-p70f change 3 moved them ahead of the download); everything
    /// from `fetchFailed` on has already paid for bytes.
    var spentBandwidth: Bool {
        switch self {
        case .minterUnavailable, .assetRowMissing, .assetFetchFailed,
             .aSideNotAnchored, .aSideReadFailed, .suppressedByBackoff,
             .alreadyInFlight:
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
        case .marked:
            return false
        case .minterUnavailable, .assetRowMissing, .assetFetchFailed,
             .aSideNotAnchored, .aSideReadFailed, .suppressedByBackoff,
             .alreadyInFlight, .fetchFailed, .tooFewBCopies,
             .noAcceptedByteDiff, .noDivergentSlot, .allSlotsAlreadyCovered,
             .persistFailed:
            return true
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
    /// Free-text detail (an error description). Truncated by the recorder
    /// before it reaches the database.
    var detail: String?

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
        lastDetail: String? = nil
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

    /// Hard ceiling on day-0 attempts per asset. Day-0 is an OPTIMISTIC probe;
    /// when it has failed this many times the lagged ≥3-day sweep is the
    /// backstop and is strictly cheaper (it pre-checks with a ~128 KB ranged
    /// sample before committing to a full fetch, which day-0 cannot do).
    static let maxAttempts = 3

    /// Backoff after the FIRST unsuccessful attempt. A same-day replay — the
    /// dominant real-world repeat — is fully suppressed by this alone.
    static let baseBackoff: TimeInterval = 24 * 60 * 60

    /// Ceiling on the doubling, so attempt 3's window stays bounded.
    static let maxBackoff: TimeInterval = 7 * 24 * 60 * 60

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
    /// - last exit `.marked` ⇒ NEVER again. The marks are already persisted and
    ///   the mint's own overlap filter would drop everything anyway; re-fetching
    ///   would spend ~108 MB to mint zero windows. This is the single most
    ///   valuable suppression.
    /// - attempt budget exhausted ⇒ suppress permanently.
    /// - inside the backoff window ⇒ suppress until it elapses.
    static func decide(
        record: RediffDayZeroAttemptRecord?,
        now: Double
    ) -> Decision {
        guard let record else { return .attempt(attemptNumber: 1) }
        guard record.lastExit.isRetryable else {
            return .suppress(reason: record.lastExit, nextEligibleAt: nil)
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

    /// Fold one attempt's outcome into the durable record. Additive over the
    /// prior row: `attemptCount` and `totalFullFetchBytes` accumulate so the
    /// per-asset bleed is visible without joining anything.
    static func advance(
        record: RediffDayZeroAttemptRecord?,
        assetId: String,
        outcome: RediffDayZeroMintOutcome,
        fullFetchBytes: Int,
        at now: Double
    ) -> RediffDayZeroAttemptRecord {
        RediffDayZeroAttemptRecord(
            analysisAssetId: assetId,
            attemptCount: (record?.attemptCount ?? 0) + 1,
            lastAttemptAt: now,
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
            lastDetail: outcome.detail.map { String($0.prefix(detailCharCap)) }
        )
    }

    /// Cap on the persisted `lastDetail` string. Error descriptions can carry
    /// a whole URL + userInfo dump; the database is not a log.
    static let detailCharCap = 200
}
