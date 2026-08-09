// DayZeroRediffTrigger.swift
// playhead-xsdz.36.4: the PLAY-TIME (day-0, same-session) rediff trigger — the
// capstone of the rediff-activation series. Today rediff is a LAGGED oracle: a
// ≥24h WiFi+charging BGTask re-fetches a second copy of the enclosure and
// byte-aligns it against the played copy to reveal the DAI ad slots, so a
// drop-day listener only gets marks on a later re-analysis. Day-0 runs the SAME
// detection AT PLAY TIME: on first listen of an episode it kicks off an
// immediate k-way context-varied B-side fetch, byte-aligns each against the
// pinned played A-side, and marks the DAI slots IMMEDIATELY — marks on first
// listen.
//
// DEFAULT-OFF (`RediffActivation.dayZeroEnabledByDefault == false`): the trigger
// is INERT — `triggerIfEligible` returns before reading any power/network signal
// or touching the network. Flipping the flag on is the SEPARATE xsdz.36 day-0
// rollout go/no-go, not this change.
//
// SAFETY (wrj8 immutability invariant): the A-side of the diff is the PINNED
// played file (read-only — the byte differ mmaps the asset row's `sourceURL`);
// the day-0 B-side(s) are SEPARATE never-played temp copies the
// `RediffRefetchService` deletes on every exit (never-persist-B). Day-0 never
// writes, replaces, or rotates the pinned playback audio.
//
// This trigger is deliberately thin: it decides ELIGIBILITY (flag + live
// WiFi/charging/deep-scan gate) and builds a single candidate, then delegates to
// `RediffRefetchService.runDayZeroRefetch`, which reuses the exact k-way fetch →
// byte-align → RediffSlotOwnership marks → never-persist-B machinery Units 1 & 2
// shipped. Mark-only — auto-skip stays held.

import Foundation

// playhead-4dqe: the former `DayZeroRediffGate` — a `Bool`-returning,
// hardcoded-WiFi-only gate — was replaced by `DayZeroTransportPolicy` in
// `RediffDayZeroBandwidthPolicy.swift`. It returns a NAMED decision rather than
// a bare `false`, so a refusal is recordable; it reads the WiFi-vs-cellular leg
// from the user's setting (Dan 2026-08-01) instead of from code; and it honors
// iOS Low Data Mode on both transports.

/// The day-0 rediff trigger. `PlayheadRuntime.playEpisode` calls
/// `triggerIfEligible` (fire-and-forget, OFF the playback hot path) once it has
/// resolved the analysis asset id for the just-started episode.
///
/// `Sendable` value type: it holds the shared `RediffRefetchService` actor plus
/// the live-signal providers as closures, so it can be captured into a detached
/// background `Task` without crossing the runtime's `@MainActor`.
struct DayZeroRediffTrigger: Sendable {
    /// The SHARED rediff service (the same instance the lagged BGTask sweep
    /// uses) — its k-way fetcher, B-side staging consumer, temp-file remover,
    /// and bandwidth recorder are reused verbatim.
    let service: RediffRefetchService
    /// THE day-0 switch, captured at construction
    /// (`RediffActivation.dayZeroEnabledByDefault`). `false` ⇒ inert.
    let enabled: Bool
    /// The day-0 k-way fetch count (`RediffActivation.dayZeroKWayFetchCount`),
    /// independent of the lagged sweep's single-fetch default.
    let kWayFetchCount: Int
    /// playhead-4dqe: the live network context, read as ONE snapshot —
    /// reachability, iOS Low Data Mode, and the user's cellular preference
    /// (production: the shared `LiveTransportStatusProvider` +
    /// `UserPreferencesSnapshot`).
    ///
    /// One provider rather than three so the three legs cannot be sampled at
    /// three different instants; the gate then decides over a coherent view.
    let transportProvider: @Sendable () async -> DayZeroTransportSnapshot
    /// Live charging state (production: `UIDeviceBatteryProvider`).
    let chargeStateProvider: @Sendable () async -> Bool
    /// The user's "deep-scan" opt-in — lets a day-0 fetch run unplugged on WiFi.
    /// Defaults to `false` (no opt-in) until a settings toggle is wired.
    let deepScanOptInProvider: @Sendable () -> Bool
    /// playhead-p70f change 2: the DURABLE per-asset day-0 state this trigger
    /// consults before spending anything. Production reads
    /// `AnalysisStore.fetchDayZeroAttemptContext`.
    ///
    /// playhead-ug9m widened this from a bare attempt record to a
    /// `DayZeroAttemptContext`, because the rescue decision is a conjunction
    /// over the record AND the asset's day-0 marks and the two must come from
    /// one snapshot — see `DayZeroAttemptContext`.
    let attemptContextProvider: @Sendable (String) async -> DayZeroAttemptContext
    /// playhead-p70f: records that a kickoff was DECLINED without spending
    /// bytes. A suppression that leaves no trace is indistinguishable from a
    /// trigger that never fired, which is the mistake this whole bead exists to
    /// correct.
    let suppressionRecorder: @Sendable (String, RediffDayZeroExit, Double) async -> Void
    /// playhead-96ot: hand the just-minted asset id to whoever can deliver its
    /// new `AdWindow` rows to the LIVE session. Production passes
    /// `SkipOrchestrator.ingestPersistedAdWindows`, which no-ops unless the
    /// asset is the one currently playing.
    ///
    /// WHY IT LIVES HERE rather than at the two `PlayheadRuntime` call sites.
    /// The defect this bead fixes is precisely that BOTH call sites are
    /// fire-and-forget and drop the outcome on the floor. Putting the decision
    /// at the one place that already computes the outcome means a third caller
    /// cannot reintroduce the bug by forgetting, and it is the same reasoning
    /// (and the same required-parameter discipline) as `attemptContextProvider`
    /// above.
    let mintedMarkDelivery: @Sendable (String) async -> Void
    /// playhead-4dqe: the rolling 24 h day-0 byte window
    /// (`rediff_bandwidth_ledger`). Read AFTER the transport gate and the
    /// per-asset backoff, so a refused attempt costs no budget read.
    let budgetWindowProvider: @Sendable () async -> RediffDayZeroBudgetWindow
    /// playhead-4dqe: folds the bytes an attempt ACTUALLY spent into the
    /// rolling window. Called with the real `fullFetchBytes` from the sweep, not
    /// the pre-flight estimate — the estimate bounds admission, the ledger
    /// records truth.
    let budgetSpendRecorder: @Sendable (Int, Double) async -> Void
    /// playhead-3oyz: durably CLAIM a same-session retry BEFORE its delay, so
    /// a retry the process never lives to run still leaves a row saying one
    /// was owed (the playhead-fil5 precedent). Production writes
    /// `AnalysisStore.noteRediffDayZeroRetryClaim`, stamping its own live
    /// clock — the claim must sort AFTER the failed attempt's
    /// `lastAttemptAt`, which the recorder also stamps live, so the trigger's
    /// nominal `now` (captured before a fetch that can take minutes) is
    /// deliberately not threaded through.
    let retryClaimRecorder: @Sendable (String) async -> Void
    /// playhead-3oyz: the retry delay. Production sleeps for real; tests
    /// inject an instant recorder so no suite ever parks on a 30 s wall-clock
    /// wait. Cooperative: a real sleep ends early on task cancellation, and
    /// the loop re-checks `Task.isCancelled` right after it.
    let retryDelay: @Sendable (TimeInterval) async -> Void

    init(
        service: RediffRefetchService,
        enabled: Bool = RediffActivation.dayZeroEnabledByDefault,
        kWayFetchCount: Int = RediffActivation.dayZeroKWayFetchCount,
        transportProvider: @escaping @Sendable () async -> DayZeroTransportSnapshot,
        chargeStateProvider: @escaping @Sendable () async -> Bool,
        deepScanOptInProvider: @escaping @Sendable () -> Bool = { false },
        // DELIBERATELY NOT DEFAULTED (review round 1). The `nil`-returning /
        // no-op pair reproduces the pre-p70f "always attempt" behavior exactly
        // — i.e. the ~108 MB-per-replay bleed this bead exists to stop. As
        // defaults they were a silent footgun: nothing in the suite constructs
        // a real `PlayheadRuntime`, so deleting the wiring there would have
        // restored the bleed with every test still green. Required parameters
        // turn that regression into a compile error, which is a stronger
        // guarantee than any test could give. Callers that genuinely have no
        // store pass the opt-outs explicitly and say why.
        attemptContextProvider: @escaping @Sendable (String) async -> DayZeroAttemptContext,
        suppressionRecorder: @escaping @Sendable (String, RediffDayZeroExit, Double) async -> Void,
        // DELIBERATELY NOT DEFAULTED, for exactly the reason recorded above
        // `attemptContextProvider`. A `{ _ in }` default reproduces the
        // playhead-96ot defect verbatim — marks minted, nothing delivered, the
        // whole suite green — and nothing in the suite builds a real
        // `PlayheadRuntime`, so deleting the wiring there would be invisible.
        // A required parameter turns that regression into a compile error.
        mintedMarkDelivery: @escaping @Sendable (String) async -> Void,
        // DELIBERATELY NOT DEFAULTED, third instance of the same discipline
        // (playhead-4dqe). An `.empty`-returning / no-op pair IS "no daily
        // budget" — the state this bead exists to leave behind, now that the
        // transport setting can open the cellular door. Nothing in the suite
        // builds a real `PlayheadRuntime`, so a dropped wiring argument there
        // would restore an UNBOUNDED day-0 byte spend with the whole suite
        // green. A required parameter makes it a compile error.
        budgetWindowProvider: @escaping @Sendable () async -> RediffDayZeroBudgetWindow,
        budgetSpendRecorder: @escaping @Sendable (Int, Double) async -> Void,
        // DELIBERATELY NOT DEFAULTED, fourth instance of the same discipline
        // (playhead-3oyz). A `{ _ in }` default reproduces the playhead-fil5
        // defect verbatim for the retry lane — a retry dropped mid-delay
        // (teardown, jetsam) would read as "never happened" — and nothing in
        // the suite builds a real `PlayheadRuntime`, so dropping the wiring
        // there would be invisible. A required parameter makes it a compile
        // error. Callers with no store pass the opt-out explicitly and say why.
        retryClaimRecorder: @escaping @Sendable (String) async -> Void,
        // Defaulted, unlike the recorders above: a real sleep IS the correct
        // production behavior, not a silent opt-out — there is no store this
        // default could fail to reach. Tests that drive the retry path inject
        // an instant sleeper (the cooperative-time-bound rule).
        retryDelay: @escaping @Sendable (TimeInterval) async -> Void = { seconds in
            try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
        }
    ) {
        self.service = service
        self.enabled = enabled
        self.kWayFetchCount = kWayFetchCount
        self.transportProvider = transportProvider
        self.chargeStateProvider = chargeStateProvider
        self.deepScanOptInProvider = deepScanOptInProvider
        self.attemptContextProvider = attemptContextProvider
        self.suppressionRecorder = suppressionRecorder
        self.mintedMarkDelivery = mintedMarkDelivery
        self.budgetWindowProvider = budgetWindowProvider
        self.budgetSpendRecorder = budgetSpendRecorder
        self.retryClaimRecorder = retryClaimRecorder
        self.retryDelay = retryDelay
    }

    /// Fire an immediate day-0 rediff for this episode IF the flag is on AND
    /// three independent gates pass, in this order: the TRANSPORT gate
    /// (reachability + Low Data Mode + the user's cellular setting + power), the
    /// per-asset BACKOFF (playhead-p70f), and the daily BYTE BUDGET
    /// (playhead-4dqe). Every refusal names itself in
    /// `rediff_day_zero_attempts.lastExit`.
    ///
    /// The gate signals are read LAZILY (only when `enabled`), so the inert
    /// default path costs nothing — no `NWPathMonitor` / battery reads. Returns
    /// the sweep summary so tests can assert what happened; production ignores it
    /// (fire-and-forget).
    ///
    /// - Parameters:
    ///   - analysisAssetId: the resolved asset id — the key the B-side is staged
    ///     under and whose `sourceURL` supplies the read-only pinned A-side.
    ///   - enclosureURL: the CURRENT episode enclosure URL, fetched K ways under
    ///     distinct personas.
    ///   - playedFileURL: the pinned played file (informational — the day-0 path
    ///     reads the A-side from the asset row, never this URL, and never writes
    ///     it).
    ///   - forceDeepScanOptIn: when `true`, the deep-scan opt-in leg of the gate
    ///     is treated as GRANTED without reading `deepScanOptInProvider` — the
    ///     on-demand "Download & Analyze" trigger (playhead-3xtw) sets this
    ///     because the user EXPLICITLY requested analysis by tapping the control,
    ///     which satisfies "charging OR deep-scan opt-in" (so an unplugged WiFi
    ///     device still runs). It grants the POWER leg ONLY: the transport
    ///     decision, Low Data Mode, the daily budget and the `enabled` flag are
    ///     all UNAFFECTED. The play-time trigger leaves it `false` (reads the
    ///     provider), so its behavior is unchanged.
    @discardableResult
    func triggerIfEligible(
        analysisAssetId: String,
        enclosureURL: URL,
        playedFileURL: URL,
        forceDeepScanOptIn: Bool = false,
        at now: Double = Date().timeIntervalSince1970
    ) async -> RediffRefetchService.SweepSummary {
        guard enabled else { return SweepSummary() }
        let transport = await transportProvider()
        let isCharging = await chargeStateProvider()
        // The explicit "Download & Analyze" tap is itself the deep-scan opt-in —
        // don't even read the (settings-backed) provider in that case.
        let deepScanOptIn = forceDeepScanOptIn || deepScanOptInProvider()
        let transportDecision = DayZeroTransportPolicy.decide(
            enabled: enabled,
            transport: transport,
            isCharging: isCharging,
            deepScanOptIn: deepScanOptIn
        )
        // playhead-4dqe — A REFUSED GATE IS NO LONGER SILENT. This used to be a
        // bare `return SweepSummary()`: on a device where day-0 never ran, the
        // gate, a dropped wiring and a flag that was off were indistinguishable
        // from each other. Each refusal now names itself in
        // `rediff_day_zero_attempts.lastExit`, so "why has day-0 never fired on
        // this phone?" is answerable from a database pull.
        //
        // The write is bounded: an UPSERT on the asset's single row with an
        // incrementing counter, so a user who plays on cellular all week still
        // owns exactly one row per episode.
        guard transportDecision.isAllowed else {
            if let exit = transportDecision.deniedExit {
                await suppressionRecorder(analysisAssetId, exit, now)
            }
            return SweepSummary()
        }

        // playhead-p70f change 2 — THE BANDWIDTH BLEED FIX. Before this, the
        // trigger consulted NO prior state: `attemptState` was hardcoded
        // `.initial`, `kickOffDayZeroRediff` fired from both play paths with no
        // per-episode guard, and an unmarked run wrote nothing for a later run
        // to read. Replaying one episode cost ~108 MB every time, forever.
        //
        // The check runs AFTER the transport/power gate but BEFORE anything is
        // fetched.
        let prior = await attemptContextProvider(analysisAssetId)
        if case let .suppress(reason, _) = DayZeroRediffAttemptPolicy.decide(
            record: prior.record,
            markCensus: prior.markCensus,
            now: now
        ) {
            await suppressionRecorder(analysisAssetId, reason, now)
            return SweepSummary()
        }

        // playhead-4dqe — THE DAILY BYTE BUDGET. Separate question, separate
        // gate, deliberately AFTER the per-asset backoff (a replay that is
        // suppressed anyway must not consume a budget read) and BEFORE any
        // fetch. Applies on BOTH transports: the gate above chose the network,
        // this one chooses how much.
        let window = await budgetWindowProvider()
        let estimate = RediffDayZeroDailyBudget.estimatedAttemptBytes(
            kWayFetchCount: kWayFetchCount
        )
        guard RediffDayZeroDailyBudget.allows(window, estimatedCost: estimate, now: now) else {
            await suppressionRecorder(analysisAssetId, .deniedDailyBudget, now)
            return SweepSummary()
        }

        // `downloadedAt` / `attemptState` are unused by the day-0 path (no ≥24h
        // gate, no pre-check); a fresh `.initial` state is what a day-0 rotation
        // resolves or a day-0 failure advances from. Day-0's OWN attempt history
        // lives in `rediff_day_zero_attempts` (read just above), deliberately
        // NOT in the lagged `rediff_refetch_state` this field feeds.
        let candidate = RediffRefetchCandidate(
            assetId: analysisAssetId,
            enclosureURL: enclosureURL,
            downloadedAt: now,
            localAudioURL: playedFileURL,
            attemptState: .initial
        )
        var summary = await runAccountAndDeliver(
            candidate: candidate, assetId: analysisAssetId, at: now
        )

        // playhead-3oyz — THE SAME-SESSION RETRY (Dan's shape (i), 2026-08-08).
        // One -1001 timeout at download time used to darken the deterministic
        // pre-roll lane for the entire first-day listen: `.fetchFailed` pays
        // the same 24 h backoff as a full failed diff, though a timed-out
        // fetch landed ZERO B-copies (F4CE7F47: lastFullFetchBytes=0,
        // suppressedCount=2 mid-listen). The grant is decided by the pure
        // policy over the run's NAMED exit and its MEASURED landed bytes —
        // never the error code alone — and is bounded to
        // `maxSameSessionRetries` with a `sameSessionRetryDelaySeconds` pause.
        // A failure that landed real bytes takes zero trips through this loop
        // and keeps the p70f backoff untouched.
        var retriesUsed = 0
        while DayZeroRediffAttemptPolicy.grantsSameSessionRetry(
            exit: summary.dayZeroExit,
            measuredFullFetchBytes: summary.fullFetchBytes,
            retriesUsed: retriesUsed
        ) {
            retriesUsed += 1
            // The CLAIM precedes the delay (playhead-fil5): a process that
            // dies parked on the sleep — or mid-retry — leaves
            // `lastRetryClaimAt > lastAttemptAt`, the queryable signature of
            // "a retry was owed and never completed". Without this, a dropped
            // retry is byte-identical in the database to the pre-fix build.
            await retryClaimRecorder(analysisAssetId)
            await retryDelay(DayZeroRediffAttemptPolicy.sameSessionRetryDelaySeconds)
            if Task.isCancelled { break }
            // The two gates that admit a SPEND are re-read across the delay —
            // 30 s is plenty of time for the user to flip Low Data Mode on
            // (the OS-level instruction an in-app mechanism must never
            // override) or for a concurrent kickoff on another episode to
            // drain the daily window. The per-asset backoff is deliberately
            // NOT re-consulted: the record now names the failure this loop
            // exists to retry, and re-reading it would suppress every retry.
            let retryTransport = await transportProvider()
            let retryCharging = await chargeStateProvider()
            let retryDecision = DayZeroTransportPolicy.decide(
                enabled: enabled,
                transport: retryTransport,
                isCharging: retryCharging,
                deepScanOptIn: deepScanOptIn
            )
            guard retryDecision.isAllowed else {
                if let exit = retryDecision.deniedExit {
                    await suppressionRecorder(analysisAssetId, exit, now)
                }
                break
            }
            let retryWindow = await budgetWindowProvider()
            guard RediffDayZeroDailyBudget.allows(
                retryWindow, estimatedCost: estimate, now: now
            ) else {
                await suppressionRecorder(analysisAssetId, .deniedDailyBudget, now)
                break
            }
            summary = await runAccountAndDeliver(
                candidate: candidate, assetId: analysisAssetId, at: now
            )
        }
        return summary
    }

    /// One day-0 run plus its two accounting hops — shared verbatim by the
    /// first attempt and the playhead-3oyz same-session retry, so a retry can
    /// never spend unaccounted bytes or mint undelivered marks.
    private func runAccountAndDeliver(
        candidate: RediffRefetchCandidate,
        assetId: String,
        at now: Double
    ) async -> SweepSummary {
        let summary = await service.runDayZeroRefetch(
            for: candidate, kWayFetchCount: kWayFetchCount
        )

        // playhead-4dqe — charge the rolling window with what was ACTUALLY
        // spent, not the admission estimate. Recorded before the delivery hop
        // so a slow orchestrator read can never delay the accounting, and
        // guarded on `> 0` so a run that fetched nothing (a disabled service, an
        // immediate throw) does not stamp a window start it never used.
        if summary.fullFetchBytes > 0 {
            await budgetSpendRecorder(summary.fullFetchBytes, now)
        }

        // playhead-96ot — DELIVER WHAT WE JUST MINTED, in this session.
        //
        // The mint has already persisted its rows by the time
        // `runDayZeroRefetch` returns, so the delivery target's store read
        // cannot miss them. Gated on the MARK count rather than
        // `summary.rotatedCount`: a run that resolved the shared attempt state
        // and a run that put new rows on disk are two different facts, and only
        // the second is worth a re-read. An unmarked run — `noDivergentSlot`,
        // `allSlotsAlreadyCovered`, a thrown fetch — persisted nothing, so
        // re-reading would forward only rows the session already holds.
        if summary.dayZeroMarkCount > 0 {
            await mintedMarkDelivery(assetId)
        }
        return summary
    }

    private typealias SweepSummary = RediffRefetchService.SweepSummary
}
