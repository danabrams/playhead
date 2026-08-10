// RediffDayZeroKickoff.swift
// playhead-4dqe: make the DOWNLOAD-TIME day-0 kickoff accountable, and give
// background/auto downloads the same entry point the "Download & Analyze" tap
// has had since playhead-xsdz.36.4.
//
// THE SILENCE THIS FILE EXISTS TO KILL. `kickOffDayZeroRediffForPreparation`
// spawns a detached task that WAITS (bounded) for two preconditions — the
// pinned downloaded file and the registered `analysis_assets` row — and then,
// if either never materializes, does this:
//
//     guard let ready else { return }   // budget elapsed → lagged sweep backstops
//
// A bare `return`. No counter, no row, no breadcrumb. On the pre-ewag build the
// ASSET ROW was exactly the precondition that never materialized (dispatch was
// frozen by the thermal depth gate), so that `return` fired for EVERY download
// and the whole download-time day-0 feature was dead for weeks while every
// on-device table it writes to read a perfectly healthy zero. The field pull of
// 2026-07-31 — 5 completed downloads, 5 files on disk, ONE asset row, ZERO
// day-0 attempts for the other four — is what a silent give-up looks like from
// the outside: indistinguishable from a trigger that was never wired at all.
//
// WHY A SEPARATE, EPISODE-KEYED TABLE. The obvious home for this is
// `rediff_day_zero_attempts` (playhead-p70f), but it CANNOT hold it: that table
// is keyed `analysisAssetId … REFERENCES analysis_assets(id)` under
// `PRAGMA foreign_keys = ON`, and the give-up we most need to record is
// PRECISELY the one where no `analysis_assets` row exists to point at. The FK
// would reject the insert. `RediffDayZeroExit.assetRowMissing` already documents
// that tautology for the mint path; this file is what makes the same class of
// event recordable at the KICKOFF layer, where the only identity available is
// the episode id.
//
// So the two ledgers divide cleanly and neither is a substitute for the other:
//
//   `rediff_day_zero_attempts`  — per ASSET. "day-0 ran; here is what the mint
//                                 decided and what it cost." Requires an asset.
//   `rediff_day_zero_kickoffs`  — per EPISODE. "a kickoff was requested; did it
//                                 ever reach the trigger at all?" Requires
//                                 nothing but an episode id.
//
// A device where `kickoffCount` is large and `firedCount` is zero is the
// pre-ewag failure, readable at a glance from a database pull or a support
// bundle, with the blame (`no_pinned_file` vs `no_analysis_asset`) attached.

import Foundation

// MARK: - Source

/// WHICH download path asked for a day-0 kickoff.
///
/// Recorded because the two paths have materially different expectations and a
/// support engineer must be able to tell which one is broken: the tap fires
/// BEFORE the download starts (so it legitimately waits minutes for the file),
/// while the background hook fires AFTER the bytes have landed (so a
/// `no_pinned_file` give-up there is a real defect, not a slow network).
enum RediffDayZeroKickoffSource: String, Sendable, Equatable, Codable, CaseIterable {
    /// The explicit "Download & Analyze" tap (playhead-3xtw), via
    /// `PlayheadRuntime.prepareEpisodeForAnalysis`.
    case downloadAndAnalyzeTap = "download_and_analyze_tap"
    /// A plain / auto (subscription pre-cache, Download-Next-N, force-quit
    /// resume) background download completing — playhead-4dqe, Dan's
    /// "yes rediff on background".
    case backgroundDownload = "background_download"
}

// MARK: - Outcome

/// What ONE download-time day-0 kickoff actually did. One case per way the
/// bounded readiness wait can end, so `guard let ready else { return }` is no
/// longer expressible as a single indistinguishable silence.
enum RediffDayZeroKickoffOutcome: String, Sendable, Equatable, Codable, CaseIterable {
    /// Both preconditions resolved and `DayZeroRediffTrigger.triggerIfEligible`
    /// was invoked. Whatever the trigger then decided (gate, backoff, budget,
    /// mint) is recorded in `rediff_day_zero_attempts` against the asset — this
    /// case only claims the handoff happened.
    case fired

    /// The attempt budget elapsed and the pinned downloaded file never appeared.
    /// On the tap path this is a slow or failed download; on the background path
    /// it means the completion hook fired for bytes that are not servable.
    case noPinnedFile = "no_pinned_file"

    /// The pinned file DID appear but the `analysis_assets` row never did.
    ///
    /// **This is the pre-ewag signature.** Dispatch was frozen, so nothing
    /// registered an asset, so this wait expired for every download and day-0
    /// never ran. Post-ewag (#310) the asset registers within seconds; a device
    /// where this count is climbing again means dispatch has re-stalled, and
    /// that is a completely different repair from a networking problem.
    case noAnalysisAsset = "no_analysis_asset"

    /// The detached task was cancelled before the budget elapsed (app
    /// teardown / runtime shutdown). Not a defect — but not a success either,
    /// and lumping it in with either would misreport both.
    case cancelled

    /// Whether the kickoff failed to reach the trigger. The single predicate the
    /// ledger's `gaveUpCount` accumulates.
    var isGiveUp: Bool { self != .fired }

    /// The invariant code this outcome SURFACES on the JSON Lines session file
    /// that ships in the diagnostics bundle and that a device pull reads, or
    /// `nil` when there is nothing to surface.
    ///
    /// The two give-ups get DISTINCT codes because their remedies are unrelated
    /// — one is a networking problem, the other a stalled analysis dispatcher —
    /// and a single "day-0 kickoff failed" code would have been just as
    /// unattributable as the bare `return` it replaces.
    ///
    /// `.fired` is not a violation. `.cancelled` is teardown, not a defect: it
    /// is still COUNTED and RECORDED in the durable ledger, but logging app
    /// shutdown as an invariant violation would train a reader to ignore the
    /// stream, which costs more than the line is worth.
    var invariantCode: InvariantViolation.Code? {
        switch self {
        case .fired, .cancelled: return nil
        case .noPinnedFile: return .rediffDayZeroKickoffNoPinnedFile
        case .noAnalysisAsset: return .rediffDayZeroKickoffNoAnalysisAsset
        }
    }

    /// Ordering used ONLY to pick the furthest-observed blame across a bounded
    /// wait's probes (see `DayZeroReadinessOutcome`). A probe can genuinely go
    /// backwards — LRU eviction can delete a pinned file after the asset row
    /// registered — and "we never even saw the file" would then be a false
    /// report about a kickoff that got further than that.
    var readinessProgressRank: Int {
        switch self {
        case .noPinnedFile: return 0
        case .noAnalysisAsset: return 1
        case .cancelled: return 2
        case .fired: return 3
        }
    }
}

// MARK: - Readiness probe

/// One probe of the two download-time preconditions, ordered by how far the
/// kickoff got. Replaces the former `T?` resolver, whose `nil` collapsed "the
/// file is not here yet" and "the file is here but nothing registered an asset"
/// into the same value — the exact conflation that hid the pre-ewag failure.
enum DayZeroReadinessProbe<Ready: Sendable>: Sendable {
    /// Both preconditions hold; `Ready` carries what the trigger needs.
    case ready(Ready)
    /// The pinned downloaded file is not on disk (yet).
    case awaitingPinnedFile
    /// The file is on disk; the `analysis_assets` row is not registered (yet).
    case awaitingAnalysisAsset

    /// How far this probe got, as the outcome a give-up here would be blamed on.
    /// `.ready` maps to `.fired` because a resolved probe IS the handoff.
    var reachedOutcome: RediffDayZeroKickoffOutcome {
        switch self {
        case .ready: return .fired
        case .awaitingPinnedFile: return .noPinnedFile
        case .awaitingAnalysisAsset: return .noAnalysisAsset
        }
    }
}

/// The result of the bounded readiness wait: the resolved value when it
/// resolved, plus — always — the outcome to RECORD and the poll count that
/// witnesses how long the wait actually ran.
struct DayZeroReadinessOutcome<Ready: Sendable>: Sendable {
    /// Non-nil iff `outcome == .fired`.
    let ready: Ready?
    /// `.fired`, or the blame for the give-up.
    let outcome: RediffDayZeroKickoffOutcome
    /// How many probes ran. `1` with `.noPinnedFile` on the BACKGROUND path
    /// means the file vanished between the completion hook and the first probe;
    /// a full budget means it never came.
    let pollCount: Int
}

/// What the readiness probe hands the trigger once BOTH preconditions hold.
struct DayZeroKickoffReady: Sendable, Equatable {
    /// The registered `analysis_assets` row id — the mint key, and the source
    /// of the read-only pinned A-side.
    let analysisAssetId: String
    /// The pinned downloaded file. Informational for the day-0 path (which
    /// reads the A-side from the asset row) and never written.
    let playedFileURL: URL

    init(analysisAssetId: String, playedFileURL: URL) {
        self.analysisAssetId = analysisAssetId
        self.playedFileURL = playedFileURL
    }
}

/// The bounded WAIT for the two download-time preconditions.
///
/// Lifted out of `PlayheadRuntime` (where it was
/// `awaitDayZeroPreparationReadiness`, returning `T?`) for two reasons. It is
/// now shared by BOTH download entry points — the tap and the background hook —
/// and it must report WHY it gave up, which a `T?` structurally cannot.
enum DayZeroReadinessWait {

    /// Probe up to `maxAttempts` times, sleeping `pollNanos` between misses.
    ///
    /// Returns as soon as a probe is `.ready`. Otherwise returns the FURTHEST
    /// progress any probe observed — not the last one. A probe can genuinely
    /// regress (LRU eviction can delete a pinned file after the asset row
    /// registered), and reporting "we never even saw the file" about a kickoff
    /// that got further would send a reader to the wrong subsystem.
    ///
    /// Cancellation is its OWN outcome, checked before every probe and after
    /// every sleep, so app teardown is never miscounted as a defect.
    static func run<Ready: Sendable>(
        maxAttempts: Int,
        pollNanos: UInt64,
        sleep: @Sendable (UInt64) async -> Void = { try? await Task.sleep(nanoseconds: $0) },
        probe: @Sendable () async -> DayZeroReadinessProbe<Ready>
    ) async -> DayZeroReadinessOutcome<Ready> {
        var polls = 0
        var furthest = RediffDayZeroKickoffOutcome.noPinnedFile
        while polls < maxAttempts {
            if Task.isCancelled {
                return DayZeroReadinessOutcome(
                    ready: nil, outcome: .cancelled, pollCount: polls
                )
            }
            polls += 1
            let observed = await probe()
            if case let .ready(value) = observed {
                return DayZeroReadinessOutcome(
                    ready: value, outcome: .fired, pollCount: polls
                )
            }
            let reached = observed.reachedOutcome
            if reached.readinessProgressRank > furthest.readinessProgressRank {
                furthest = reached
            }
            if polls < maxAttempts { await sleep(pollNanos) }
        }
        // A zero/negative budget never probed, so it observed nothing; blaming
        // the file would be inventing an observation. Cancellation is the only
        // honest "we never looked" outcome available.
        guard polls > 0 else {
            return DayZeroReadinessOutcome(ready: nil, outcome: .cancelled, pollCount: 0)
        }
        return DayZeroReadinessOutcome(ready: nil, outcome: furthest, pollCount: polls)
    }
}

// MARK: - Durable kickoff record

/// One row of `rediff_day_zero_kickoffs` — the per-EPISODE record of whether a
/// download-time day-0 kickoff ever reached the trigger.
///
/// Deliberately NOT foreign-keyed to `analysis_assets`: the whole point is to
/// be able to record a kickoff for an episode that has no asset row, which is
/// the exact state the pre-ewag build left every download in.
struct RediffDayZeroKickoffRecord: Sendable, Equatable {
    let episodeId: String
    /// Which path requested the most recent kickoff.
    let lastSource: RediffDayZeroKickoffSource
    /// Total kickoffs requested for this episode, including the most recent.
    let kickoffCount: Int
    /// How many of them reached `triggerIfEligible`.
    let firedCount: Int
    /// How many gave up. `kickoffCount == gaveUpCount` with `firedCount == 0`
    /// is the pre-ewag failure for this episode.
    let gaveUpCount: Int
    let lastOutcome: RediffDayZeroKickoffOutcome
    /// Probes the most recent kickoff ran before settling.
    let lastPollCount: Int
    /// Wall-clock seconds the most recent kickoff waited.
    let lastWaitedSeconds: Double
    let updatedAt: Double

    init(
        episodeId: String,
        lastSource: RediffDayZeroKickoffSource,
        kickoffCount: Int,
        firedCount: Int,
        gaveUpCount: Int,
        lastOutcome: RediffDayZeroKickoffOutcome,
        lastPollCount: Int = 0,
        lastWaitedSeconds: Double = 0,
        updatedAt: Double
    ) {
        self.episodeId = episodeId
        self.lastSource = lastSource
        self.kickoffCount = kickoffCount
        self.firedCount = firedCount
        self.gaveUpCount = gaveUpCount
        self.lastOutcome = lastOutcome
        self.lastPollCount = lastPollCount
        self.lastWaitedSeconds = lastWaitedSeconds
        self.updatedAt = updatedAt
    }
}

/// One settled kickoff, as handed to whoever persists it. Separate from
/// `RediffDayZeroKickoffRecord` (the accumulated ROW) because a writer needs
/// the delta, not the total — the accumulation is the store's job and doing it
/// in two places is how two counters drift apart.
struct RediffDayZeroKickoffRecordUpdate: Sendable, Equatable {
    let episodeId: String
    let source: RediffDayZeroKickoffSource
    let outcome: RediffDayZeroKickoffOutcome
    /// Probes the wait ran before settling.
    let pollCount: Int
    /// Wall-clock seconds the wait ran.
    let waitedSeconds: Double
    let at: Double

    init(
        episodeId: String,
        source: RediffDayZeroKickoffSource,
        outcome: RediffDayZeroKickoffOutcome,
        pollCount: Int,
        waitedSeconds: Double,
        at: Double
    ) {
        self.episodeId = episodeId
        self.source = source
        self.outcome = outcome
        self.pollCount = pollCount
        self.waitedSeconds = waitedSeconds
        self.at = at
    }
}

// MARK: - Kickoff request ordering

/// One queued download-time day-0 kickoff.
///
/// Carries `publishedAt` because Dan's decision includes "newest-episode-first
/// ordering when the budget is contended", and contention is real: a
/// subscription batch can land five downloads inside a minute, each worth
/// ~130 MB of day-0 re-fetch, against a bounded daily budget. When only some of
/// them fit, the ones a listener will actually play first are the new ones.
struct RediffDayZeroKickoffRequest: Sendable, Equatable {
    let episodeId: String
    let enclosureURL: URL
    /// Episode publish date (unix seconds) when known. `nil` sorts LAST — an
    /// unknown date is not evidence of newness, and treating it as "now" would
    /// let every date-less feed jump the queue ahead of a genuine new drop.
    let publishedAt: Double?
    let source: RediffDayZeroKickoffSource
    /// When this request was enqueued — the tiebreak, so the ordering is TOTAL
    /// and therefore deterministic. A partial order would make the drain
    /// sequence depend on `sort`'s stability, which Swift does not guarantee.
    let enqueuedAt: Double

    init(
        episodeId: String,
        enclosureURL: URL,
        publishedAt: Double?,
        source: RediffDayZeroKickoffSource,
        enqueuedAt: Double
    ) {
        self.episodeId = episodeId
        self.enclosureURL = enclosureURL
        self.publishedAt = publishedAt
        self.source = source
        self.enqueuedAt = enqueuedAt
    }
}

// MARK: - Background-download kickoff facts (playhead-cnql)

/// The two things only SwiftData can answer about an episode whose background
/// download just completed: the CURRENT enclosure URL from the feed (which may
/// have moved since the download followed its own), and the publish date the
/// coordinator's drain orders by.
///
/// Optional as a whole, and optional in each field, because the process that
/// most needs to ask is the one least able to: a background URLSession relaunch
/// or a BGTask-only wake never runs `PlayheadApp.task`, so no SwiftData-backed
/// resolver is ever installed there. See `DayZeroBackgroundKickoff.request`.
struct DayZeroKickoffEpisodeFacts: Sendable, Equatable {
    let enclosureURL: URL?
    let publishedAt: Double?

    init(enclosureURL: URL?, publishedAt: Double?) {
        self.enclosureURL = enclosureURL
        self.publishedAt = publishedAt
    }
}

/// playhead-cnql: the pure "what kickoff does this completed background
/// download deserve?" decision.
///
/// WHY THIS IS A FUNCTION AND NOT THREE LINES IN A CLOSURE. Before this bead the
/// equivalent lines lived inside the observer that `PlayheadApp.task` installed,
/// and they could only run in a process where SwiftData had been attached — so
/// the fallback arm below (no facts at all) was unreachable and untested, and
/// every download that completed in a background-relaunch or BGTask-only
/// process was dropped by `guard let observer else { return }` one layer up. The
/// decision is lifted out so the SwiftData-less arm is the FIRST thing a test
/// can reach, not the last thing anyone thinks of.
enum DayZeroBackgroundKickoff {

    /// The kickoff request for a completed background download, or `nil` when
    /// no URL is knowable from either source — the one case where there is
    /// genuinely nothing to re-fetch.
    ///
    /// The feed's CURRENT enclosure URL wins when it is known (day-0 re-fetches
    /// the live enclosure K ways, exactly as the play path does); the URL the
    /// download itself followed is the fallback, and on a background relaunch it
    /// is the ONLY thing available.
    static func request(
        episodeId: String,
        facts: DayZeroKickoffEpisodeFacts?,
        fallbackURL: URL?,
        enqueuedAt: Double
    ) -> RediffDayZeroKickoffRequest? {
        guard let enclosureURL = facts?.enclosureURL ?? fallbackURL else { return nil }
        return RediffDayZeroKickoffRequest(
            episodeId: episodeId,
            enclosureURL: enclosureURL,
            publishedAt: facts?.publishedAt,
            source: .backgroundDownload,
            enqueuedAt: enqueuedAt
        )
    }
}

/// The pure "which pending kickoff runs next" decision — newest episode first.
///
/// Pure and total so the ordering is unit-tested without a queue, a clock, or a
/// network, and so it cannot silently depend on sort stability.
enum RediffDayZeroKickoffOrdering {

    /// Strict ordering: newer `publishedAt` first; a known date always beats an
    /// unknown one; ties broken by earlier `enqueuedAt` (FIFO), then by
    /// `episodeId` so the order is total.
    static func isOrderedBefore(
        _ lhs: RediffDayZeroKickoffRequest,
        _ rhs: RediffDayZeroKickoffRequest
    ) -> Bool {
        switch (lhs.publishedAt, rhs.publishedAt) {
        case let (left?, right?) where left != right:
            return left > right
        case (nil, .some):
            return false
        case (.some, nil):
            return true
        default:
            break
        }
        if lhs.enqueuedAt != rhs.enqueuedAt { return lhs.enqueuedAt < rhs.enqueuedAt }
        return lhs.episodeId < rhs.episodeId
    }

    /// The pending set in drain order.
    static func drainOrder(
        _ pending: [RediffDayZeroKickoffRequest]
    ) -> [RediffDayZeroKickoffRequest] {
        pending.sorted(by: isOrderedBefore)
    }
}
