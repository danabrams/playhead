// DayZeroDownloadTimeTests.swift
// playhead-4dqe: day-0 rediff at DOWNLOAD time.
//
// Three statements this file exists to pin, in the order the bead asks them:
//
//   1. THE TAP TRIGGER WORKS POST-EWAG, AND ITS GIVE-UP IS NO LONGER SILENT.
//      `kickOffDayZeroRediffForPreparation` waits (bounded) for a pinned file
//      and a registered `analysis_assets` row, then fires. On the pre-ewag
//      build the ASSET ROW never materialized, so the wait expired and the
//      trigger died on a bare `return` — no counter, no row, no log line, for
//      every download, for weeks. `readinessWait*` / `kickoff*` pin that the
//      two give-up causes are now DISTINGUISHABLE, counted per cause, durably
//      recorded, and written to the session file the device pull reads.
//
//   2. BACKGROUND / AUTO DOWNLOADS GET THE SAME ENTRY POINT (Dan: "yes rediff
//      on background"). `coordinator*` pins that a plain download completing
//      reaches the trigger with NO playback, and that a contended batch drains
//      newest-episode-first.
//
//   3. TRANSPORT IS A USER SETTING, AND THE BUDGET IS NOT THE SETTING.
//      `transport*` / `budget*` pin Dan's four sub-decisions: default WiFi
//      only, Low Data Mode wins on BOTH transports regardless of the setting,
//      the daily byte budget applies under BOTH transports, newest-first when
//      contended.
//
// Everything here is offline: pure policies, injected seams, a temp-dir store
// and a temp-dir logger. No network, no clock, no device.

import Foundation
import Testing

@testable import Playhead

// MARK: - 3a. The transport decision (Dan 2026-08-01)

@Suite("Day-0 transport policy — the user setting (playhead-4dqe)")
struct DayZeroTransportPolicyTests {

    private func snapshot(
        _ reachability: TransportSnapshot.Reachability,
        lowData: Bool = false,
        allowsCellular: Bool = false
    ) -> DayZeroTransportSnapshot {
        DayZeroTransportSnapshot(
            reachability: reachability,
            isLowDataMode: lowData,
            allowsCellular: allowsCellular
        )
    }

    @Test("THE DEFAULT IS WIFI ONLY — a metered user must never lose ~130 MB/episode before finding the toggle")
    func defaultIsWifiOnly() {
        #expect(RediffActivation.dayZeroAllowsCellularByDefault == false)
    }

    @Test("a disabled flag decides nothing — the inert build")
    func disabledIsInert() {
        for reach: TransportSnapshot.Reachability in [.wifi, .cellular, .unreachable] {
            for lowData in [true, false] {
                for allowsCellular in [true, false] {
                    for charging in [true, false] {
                        #expect(DayZeroTransportPolicy.decide(
                            enabled: false,
                            transport: snapshot(reach, lowData: lowData, allowsCellular: allowsCellular),
                            isCharging: charging,
                            deepScanOptIn: true
                        ) == .denyDisabled)
                    }
                }
            }
        }
    }

    @Test("no reachable path is denied before any policy question is asked")
    func unreachableDenied() {
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.unreachable, allowsCellular: true),
            isCharging: true,
            deepScanOptIn: true
        ) == .denyUnreachable)
    }

    @Test("LOW DATA MODE WINS ON WIFI, even with the setting on, charging, and opted in")
    func lowDataModeWinsOnWifi() {
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.wifi, lowData: true, allowsCellular: true),
            isCharging: true,
            deepScanOptIn: true
        ) == .denyLowDataMode)
    }

    @Test("LOW DATA MODE WINS ON CELLULAR too — it is an OS-level instruction, not a transport rule")
    func lowDataModeWinsOnCellular() {
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.cellular, lowData: true, allowsCellular: true),
            isCharging: true,
            deepScanOptIn: true
        ) == .denyLowDataMode)
    }

    @Test("Low Data Mode outranks the user setting: flipping the toggle cannot change the verdict")
    func lowDataModeOutranksSetting() {
        for allowsCellular in [true, false] {
            for reach: TransportSnapshot.Reachability in [.wifi, .cellular] {
                #expect(DayZeroTransportPolicy.decide(
                    enabled: true,
                    transport: snapshot(reach, lowData: true, allowsCellular: allowsCellular),
                    isCharging: true,
                    deepScanOptIn: true
                ) == .denyLowDataMode)
            }
        }
    }

    @Test("cellular WITHOUT the setting is denied — the shipping default")
    func cellularDeniedByDefault() {
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.cellular, allowsCellular: false),
            isCharging: true,
            deepScanOptIn: true
        ) == .denyCellularNotAllowed)
    }

    @Test("cellular WITH the setting is ALLOWED — this is what Dan moved out of code")
    func cellularAllowedBySetting() {
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.cellular, allowsCellular: true),
            isCharging: true,
            deepScanOptIn: false
        ) == .allow)
    }

    @Test("the power axis is unchanged: WiFi unplugged with no opt-in is denied on power, not transport")
    func powerAxisUnchanged() {
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.wifi),
            isCharging: false,
            deepScanOptIn: false
        ) == .denyPower)
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.wifi),
            isCharging: false,
            deepScanOptIn: true
        ) == .allow)
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.wifi),
            isCharging: true,
            deepScanOptIn: false
        ) == .allow)
    }

    @Test("a cellular denial is denied on the CELLULAR leg, not silently folded into power")
    func cellularDenialPrecedesPower() {
        #expect(DayZeroTransportPolicy.decide(
            enabled: true,
            transport: snapshot(.cellular, allowsCellular: false),
            isCharging: false,
            deepScanOptIn: false
        ) == .denyCellularNotAllowed)
    }

    @Test("every denial names an exit the trigger can RECORD; .allow names none")
    func everyDenialHasAnExit() {
        #expect(DayZeroTransportDecision.allow.deniedExit == nil)
        #expect(DayZeroTransportDecision.denyUnreachable.deniedExit == .deniedUnreachable)
        #expect(DayZeroTransportDecision.denyLowDataMode.deniedExit == .deniedLowDataMode)
        #expect(DayZeroTransportDecision.denyCellularNotAllowed.deniedExit == .deniedCellularNotAllowed)
        #expect(DayZeroTransportDecision.denyPower.deniedExit == .deniedPower)
        // `.denyDisabled` is the inert build, not a refusal worth a row.
        #expect(DayZeroTransportDecision.denyDisabled.deniedExit == nil)
    }

    @Test("the transport denial exits are FREE (no bytes) and RETRYABLE (the condition changes)")
    func transportExitsAreFreeAndRetryable() {
        for exit: RediffDayZeroExit in [
            .deniedUnreachable, .deniedLowDataMode, .deniedCellularNotAllowed,
            .deniedPower, .deniedDailyBudget
        ] {
            #expect(exit.spentBandwidth == false, "\(exit) declines BEFORE the fetch")
            #expect(exit.isRetryable, "\(exit) is a condition, not a verdict")
        }
    }
}

// MARK: - 3b. The daily byte budget (a DIFFERENT question from the transport)

@Suite("Day-0 daily byte budget (playhead-4dqe)")
struct RediffDayZeroDailyBudgetTests {

    typealias Budget = RediffDayZeroDailyBudget
    static let day: Double = 24 * 3600

    @Test("a never-spent window has the whole cap — and is NOT the same value as a window that started at time 0")
    func freshWindowHasFullCap() {
        let fresh = RediffDayZeroBudgetWindow.empty
        #expect(fresh.startedAt == nil)
        #expect(Budget.remainingBytes(fresh, now: 1_000_000) == Budget.dailyCapBytes)
        #expect(Budget.windowHasElapsed(fresh, now: 0))
    }

    @Test("spending inside the window reduces what is left")
    func spendingReducesRemaining() {
        let window = RediffDayZeroBudgetWindow(startedAt: 1_000, spentBytes: 400_000_000)
        #expect(Budget.remainingBytes(window, now: 1_000 + 3600)
            == Budget.dailyCapBytes - 400_000_000)
    }

    @Test("the window ROLLS after 24 h from FIRST SPEND (not calendar midnight — a timezone crossing must not gain or lose a day)")
    func windowRolls() {
        let window = RediffDayZeroBudgetWindow(startedAt: 1_000, spentBytes: Budget.dailyCapBytes)
        #expect(Budget.remainingBytes(window, now: 1_000 + Self.day - 1) == 0)
        #expect(Budget.remainingBytes(window, now: 1_000 + Self.day) == Budget.dailyCapBytes)
    }

    @Test("an attempt is admitted on the FULL estimate — never a partial fetch, which cannot mint at the k-way floor")
    func admitsOnlyFullAttempts() {
        let perCopy = Budget.estimatedBytesPerBCopy
        let window = RediffDayZeroBudgetWindow(
            startedAt: 1_000,
            spentBytes: Budget.dailyCapBytes - perCopy   // room for ONE copy
        )
        let twoWay = Budget.estimatedAttemptBytes(kWayFetchCount: 2)
        #expect(!Budget.allows(window, estimatedCost: twoWay, now: 1_100),
                "half an attempt buys nothing at the >=2 B-copy floor")
        #expect(Budget.allows(window, estimatedCost: perCopy, now: 1_100))
    }

    @Test("the k-way estimate scales with K and is never negative")
    func estimateScalesWithK() {
        #expect(Budget.estimatedAttemptBytes(kWayFetchCount: 2)
            == 2 * Budget.estimatedBytesPerBCopy)
        #expect(Budget.estimatedAttemptBytes(kWayFetchCount: 0) == 0)
        #expect(Budget.estimatedAttemptBytes(kWayFetchCount: -3) == 0)
    }

    @Test("THE BUDGET IS NOT THE TRANSPORT: `allows` takes no transport argument, so a WiFi user is capped identically")
    func budgetIsTransportBlind() {
        // Structural: the same window and cost yield the same answer no matter
        // what the transport setting is, because the setting cannot reach here.
        let window = RediffDayZeroBudgetWindow(startedAt: 1_000, spentBytes: Budget.dailyCapBytes)
        #expect(!Budget.allows(window, estimatedCost: 1, now: 1_100))
    }

    @Test("spending folds into the window; the first spend STARTS it")
    func spendStartsAndAccumulates() {
        let first = Budget.spend(.empty, bytes: 100, now: 5_000)
        #expect(first.startedAt == 5_000)
        #expect(first.spentBytes == 100)

        let second = Budget.spend(first, bytes: 250, now: 5_500)
        #expect(second.startedAt == 5_000, "the window keeps its original start")
        #expect(second.spentBytes == 350)
    }

    @Test("a spend after the window elapsed RESTARTS it rather than accumulating forever")
    func spendRestartsElapsedWindow() {
        let old = RediffDayZeroBudgetWindow(startedAt: 1_000, spentBytes: 900_000_000)
        let rolled = Budget.spend(old, bytes: 120_000_000, now: 1_000 + Self.day + 1)
        #expect(rolled.startedAt == 1_000 + Self.day + 1)
        #expect(rolled.spentBytes == 120_000_000)
    }

    @Test("a zero-byte spend does not start a window — an attempt that spent nothing has not opened a day")
    func zeroSpendDoesNotStartWindow() {
        #expect(Budget.spend(.empty, bytes: 0, now: 5_000) == .empty)
    }

    @Test("the cap admits ~7 measured attempts/day and is a real ceiling, not a formality")
    func capIsCalibratedToMeasuredCost() {
        // Field-measured day-0 attempts: 120.3 MB and 131.8 MB at K=2.
        let measured = 131_800_000
        #expect(Budget.dailyCapBytes / measured >= 7)
        #expect(Budget.dailyCapBytes / measured <= 9)
    }
}

// MARK: - 1. The give-up taxonomy: the bare `return` becomes nameable

@Suite("Day-0 kickoff give-up taxonomy (playhead-4dqe)")
struct RediffDayZeroKickoffOutcomeTests {

    @Test("only `.fired` is not a give-up")
    func onlyFiredIsSuccess() {
        #expect(RediffDayZeroKickoffOutcome.fired.isGiveUp == false)
        for outcome: RediffDayZeroKickoffOutcome in [.noPinnedFile, .noAnalysisAsset, .cancelled] {
            #expect(outcome.isGiveUp, "\(outcome) never reached the trigger")
        }
    }

    @Test("playhead-kg8h: `.requested` is NOT a give-up — work still owed is not work that failed")
    func requestedIsNeitherFiredNorGivenUp() {
        #expect(RediffDayZeroKickoffOutcome.requested.isGiveUp == false,
                "a claim counted as a give-up would report every queued kickoff as a failure")
        #expect(RediffDayZeroKickoffOutcome.requested.invariantCode == nil,
                "an outstanding claim is not a violation; only a settled give-up is")
        #expect(RediffDayZeroKickoffOutcome.requested.readinessProgressRank
            < RediffDayZeroKickoffOutcome.noPinnedFile.readinessProgressRank,
                "a claim precedes the first probe, so it can never be the furthest progress observed")
    }

    @Test("the claim's raw value is stable — a device pull greps for it")
    func requestedRawValueIsStable() {
        #expect(RediffDayZeroKickoffOutcome.requested.rawValue == "requested")
    }

    @Test("a probe names how far the kickoff got")
    func probeNamesProgress() {
        #expect(DayZeroReadinessProbe.ready(1).reachedOutcome == .fired)
        #expect(DayZeroReadinessProbe<Int>.awaitingPinnedFile.reachedOutcome == .noPinnedFile)
        #expect(DayZeroReadinessProbe<Int>.awaitingAnalysisAsset.reachedOutcome == .noAnalysisAsset)
    }

    @Test("progress ranks order the two give-up causes so the FURTHEST observed wins")
    func progressRanksOrderTheCauses() {
        #expect(RediffDayZeroKickoffOutcome.noPinnedFile.readinessProgressRank
            < RediffDayZeroKickoffOutcome.noAnalysisAsset.readinessProgressRank)
        #expect(RediffDayZeroKickoffOutcome.noAnalysisAsset.readinessProgressRank
            < RediffDayZeroKickoffOutcome.fired.readinessProgressRank)
    }

    @Test("THE PRE-EWAG SIGNATURE and a download failure map to DIFFERENT invariant codes — the remedies differ")
    func givesUpUnderDistinctCodes() {
        #expect(RediffDayZeroKickoffOutcome.noAnalysisAsset.invariantCode
            == .rediffDayZeroKickoffNoAnalysisAsset)
        #expect(RediffDayZeroKickoffOutcome.noPinnedFile.invariantCode
            == .rediffDayZeroKickoffNoPinnedFile)
        // `.fired` is not a violation; `.cancelled` is teardown, not a defect —
        // both are still COUNTED and RECORDED, just not logged as violations.
        #expect(RediffDayZeroKickoffOutcome.fired.invariantCode == nil)
        #expect(RediffDayZeroKickoffOutcome.cancelled.invariantCode == nil)
    }

    @Test("the two new codes carry the raw values a device pull greps for")
    func codeRawValuesAreStable() {
        #expect(InvariantViolation.Code.rediffDayZeroKickoffNoPinnedFile.rawValue
            == "rediff_day_zero_kickoff_no_pinned_file")
        #expect(InvariantViolation.Code.rediffDayZeroKickoffNoAnalysisAsset.rawValue
            == "rediff_day_zero_kickoff_no_analysis_asset")
    }

    @Test("both kickoff sources are distinguishable — the tap and the background hook fail differently")
    func sourcesAreDistinguishable() {
        #expect(RediffDayZeroKickoffSource.downloadAndAnalyzeTap.rawValue
            == "download_and_analyze_tap")
        #expect(RediffDayZeroKickoffSource.backgroundDownload.rawValue
            == "background_download")
    }
}

// MARK: - 1b. The bounded readiness wait

@Suite("Day-0 readiness wait (playhead-4dqe)")
struct DayZeroReadinessWaitTests {

    private static func noSleep(_ nanos: UInt64) async {}

    @Test("resolving on the first probe fires immediately")
    func resolvesImmediately() async {
        let result = await DayZeroReadinessWait.run(
            maxAttempts: 5, pollNanos: 1, sleep: Self.noSleep
        ) { .ready(42) }
        #expect(result.outcome == .fired)
        #expect(result.ready == 42)
        #expect(result.pollCount == 1)
    }

    @Test("resolving on a LATER probe still fires — the tap fires before the download even starts")
    func resolvesAfterWaiting() async {
        let probes = DayZeroProbeCounter()
        let result = await DayZeroReadinessWait.run(
            maxAttempts: 10, pollNanos: 1, sleep: Self.noSleep
        ) {
            await probes.increment() >= 4 ? .ready(7) : .awaitingPinnedFile
        }
        #expect(result.outcome == .fired)
        #expect(result.ready == 7)
        #expect(result.pollCount == 4)
    }

    @Test("a file that never lands gives up as .noPinnedFile — NOT a bare return")
    func noPinnedFileIsNamed() async {
        let result = await DayZeroReadinessWait.run(
            maxAttempts: 3, pollNanos: 1, sleep: Self.noSleep
        ) { DayZeroReadinessProbe<Int>.awaitingPinnedFile }
        #expect(result.outcome == .noPinnedFile)
        #expect(result.ready == nil)
        #expect(result.pollCount == 3)
    }

    @Test("THE PRE-EWAG SIGNATURE: the file lands but no asset row ever does → .noAnalysisAsset")
    func noAnalysisAssetIsNamed() async {
        let result = await DayZeroReadinessWait.run(
            maxAttempts: 3, pollNanos: 1, sleep: Self.noSleep
        ) { DayZeroReadinessProbe<Int>.awaitingAnalysisAsset }
        #expect(result.outcome == .noAnalysisAsset,
                "5 downloads, 5 files, ONE asset row — this is what that looked like")
        #expect(result.pollCount == 3)
    }

    @Test("the wait reports the FURTHEST progress it ever saw, not the last (an LRU eviction must not rewrite history)")
    func reportsFurthestObservedProgress() async {
        let probes = DayZeroProbeCounter()
        let result = await DayZeroReadinessWait.run(
            maxAttempts: 4, pollNanos: 1, sleep: Self.noSleep
        ) { () -> DayZeroReadinessProbe<Int> in
            // Probe 2 sees the asset wait (further along); probes 1, 3, 4 regress
            // to "no file" because the pinned copy was evicted.
            await probes.increment() == 2 ? .awaitingAnalysisAsset : .awaitingPinnedFile
        }
        #expect(result.outcome == .noAnalysisAsset)
    }

    @Test("a cancelled wait is .cancelled — teardown is not a defect and must not be reported as one")
    func cancellationIsItsOwnOutcome() async {
        let task = Task {
            await DayZeroReadinessWait.run(
                maxAttempts: 1_000_000,
                pollNanos: 1,
                sleep: { _ in await Task.yield() },
                probe: { DayZeroReadinessProbe<Int>.awaitingPinnedFile }
            )
        }
        task.cancel()
        let result = await task.value
        #expect(result.outcome == .cancelled)
        #expect(result.ready == nil)
    }

    @Test("the production wait budget covers a download plus the post-ewag asset registration")
    func productionBudgetIsBounded() {
        let seconds = Double(PlayheadRuntime.dayZeroPreparationReadinessMaxAttempts)
            * Double(PlayheadRuntime.dayZeroPreparationReadinessPollNanos) / 1_000_000_000
        #expect(seconds >= 300, "a real episode download needs minutes")
        #expect(seconds <= 900, "but the wait must be BOUNDED — the lagged sweep is the backstop")
    }
}

/// Minimal async counter for probe sequencing and concurrency probing.
actor DayZeroProbeCounter {
    private var value = 0
    private var highWaterMark = 0

    @discardableResult
    func increment() -> Int {
        value += 1
        highWaterMark = max(highWaterMark, value)
        return value
    }

    @discardableResult
    func decrement() -> Int {
        value -= 1
        return value
    }

    func current() -> Int { value }
    func peak() -> Int { highWaterMark }
}

// MARK: - 3d. Newest-episode-first when the budget is contended

@Suite("Day-0 kickoff ordering — newest episode first (playhead-4dqe)")
struct RediffDayZeroKickoffOrderingTests {

    private func request(
        _ id: String,
        publishedAt: Double?,
        enqueuedAt: Double = 0
    ) -> RediffDayZeroKickoffRequest {
        RediffDayZeroKickoffRequest(
            episodeId: id,
            enclosureURL: URL(string: "https://cdn.example.com/\(id).mp3")!,
            publishedAt: publishedAt,
            source: .backgroundDownload,
            enqueuedAt: enqueuedAt
        )
    }

    @Test("the newest episode drains first — that is the one a listener plays first")
    func newestFirst() {
        let order = RediffDayZeroKickoffOrdering.drainOrder([
            request("old", publishedAt: 100),
            request("new", publishedAt: 300),
            request("mid", publishedAt: 200)
        ])
        #expect(order.map(\.episodeId) == ["new", "mid", "old"])
    }

    @Test("an UNKNOWN publish date sorts LAST — a missing date is not evidence of newness")
    func unknownDateSortsLast() {
        let undated = request("undated", publishedAt: nil)
        let ancient = request("ancient", publishedAt: 1)
        let order = RediffDayZeroKickoffOrdering.drainOrder([undated, ancient])
        #expect(order.map(\.episodeId) == ["ancient", "undated"])

        // BOTH DIRECTIONS, and this is not belt-and-braces — it is the whole
        // test. Mutation K10 (`case (nil, .some): return true`) left the
        // `drainOrder` assertion above GREEN: with only two elements,
        // `sorted(by:)` reached its answer through the OTHER arm
        // (`isOrderedBefore(ancient, undated)`, still `true`) and swapped them
        // into the expected order anyway. A comparator that says "a precedes b"
        // AND "b precedes a" is not merely wrong, it is an INVALID predicate,
        // and `sorted(by:)`'s behaviour on one is undefined — so the list order
        // is not evidence about the comparator at all. Asserting the relation
        // itself is.
        #expect(RediffDayZeroKickoffOrdering.isOrderedBefore(ancient, undated),
                "a known date precedes an unknown one")
        #expect(!RediffDayZeroKickoffOrdering.isOrderedBefore(undated, ancient),
                "an unknown date NEVER precedes a known one — K10 survived on exactly this")
    }

    @Test("equal publish dates fall back to FIFO, then to episode id — the order is TOTAL")
    func tiebreaksAreTotal() {
        let order = RediffDayZeroKickoffOrdering.drainOrder([
            request("b", publishedAt: 100, enqueuedAt: 5),
            request("a", publishedAt: 100, enqueuedAt: 5),
            request("c", publishedAt: 100, enqueuedAt: 1)
        ])
        #expect(order.map(\.episodeId) == ["c", "a", "b"])
    }

    @Test("ordering is a STRICT order: nothing precedes itself")
    func orderingIsStrict() {
        let only = request("x", publishedAt: 100, enqueuedAt: 5)
        #expect(!RediffDayZeroKickoffOrdering.isOrderedBefore(only, only))
    }

    @Test("two undated requests still order deterministically (FIFO then id)")
    func undatedRequestsStillTotal() {
        let order = RediffDayZeroKickoffOrdering.drainOrder([
            request("z", publishedAt: nil, enqueuedAt: 2),
            request("y", publishedAt: nil, enqueuedAt: 1)
        ])
        #expect(order.map(\.episodeId) == ["y", "z"])
    }
}

// MARK: - 2. The coordinator: background downloads reach the trigger

/// Records what the coordinator did, so the tests assert behavior rather than
/// internals.
actor KickoffSpy {
    private(set) var fired: [(episodeId: String, assetId: String)] = []
    private(set) var claims: [RediffDayZeroKickoffClaim] = []
    private(set) var records: [RediffDayZeroKickoffRecordUpdate] = []
    private(set) var violations: [(code: InvariantViolation.Code, description: String)] = []
    /// playhead-kg8h: an ORDERED log across all three hooks. The claim's whole
    /// value is that it lands BEFORE the work, and a per-hook array cannot say
    /// which happened first.
    private(set) var timeline: [String] = []

    func noteFired(episodeId: String, assetId: String) {
        fired.append((episodeId, assetId))
        timeline.append("fire(\(episodeId))")
    }

    func noteClaim(_ claim: RediffDayZeroKickoffClaim) {
        claims.append(claim)
        timeline.append("claim(\(claim.episodeId))")
    }

    func noteRecord(_ update: RediffDayZeroKickoffRecordUpdate) {
        records.append(update)
        timeline.append("settle(\(update.episodeId))")
    }

    func noteViolation(code: InvariantViolation.Code, description: String) {
        violations.append((code, description))
    }
}

/// playhead-kg8h: lets a `claimKickoff` closure reach the coordinator that owns
/// it, so a test can drive a RE-ENTRANT `requestKickoff` from inside the claim's
/// suspension. The closure is built before the coordinator exists, so it needs a
/// box to read through at call time.
actor KickoffCoordinatorBox {
    private(set) var coordinator: RediffDayZeroKickoffCoordinator?

    func set(_ coordinator: RediffDayZeroKickoffCoordinator) {
        self.coordinator = coordinator
    }
}

@Suite("Day-0 kickoff coordinator (playhead-4dqe)")
struct RediffDayZeroKickoffCoordinatorTests {

    private static func request(
        _ id: String,
        source: RediffDayZeroKickoffSource = .backgroundDownload,
        publishedAt: Double? = nil,
        enqueuedAt: Double = 0
    ) -> RediffDayZeroKickoffRequest {
        RediffDayZeroKickoffRequest(
            episodeId: id,
            enclosureURL: URL(string: "https://cdn.example.com/\(id).mp3")!,
            publishedAt: publishedAt,
            source: source,
            enqueuedAt: enqueuedAt
        )
    }

    private static func makeCoordinator(
        spy: KickoffSpy,
        maxAttempts: Int = 3,
        probe: @escaping @Sendable (String) async -> DayZeroReadinessProbe<DayZeroKickoffReady>
    ) -> RediffDayZeroKickoffCoordinator {
        RediffDayZeroKickoffCoordinator(
            maxAttempts: maxAttempts,
            pollNanos: 1,
            probe: probe,
            fire: { ready, request in
                await spy.noteFired(episodeId: request.episodeId, assetId: ready.analysisAssetId)
            },
            claimKickoff: { await spy.noteClaim($0) },
            recordKickoff: { await spy.noteRecord($0) },
            reportViolation: { code, description in
                await spy.noteViolation(code: code, description: description)
            },
            episodeIdHasher: { "hash(\($0))" },
            sleep: { _ in },
            now: { 1_000 }
        )
    }

    private static func ready(_ assetId: String) -> DayZeroReadinessProbe<DayZeroKickoffReady> {
        .ready(DayZeroKickoffReady(
            analysisAssetId: assetId,
            playedFileURL: URL(fileURLWithPath: "/tmp/\(assetId).mp3")
        ))
    }

    @Test("A PLAIN BACKGROUND DOWNLOAD REACHES THE TRIGGER — no playback anywhere in this test")
    func backgroundDownloadFires() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in Self.ready("asset-bg") }
        await coordinator.requestKickoff(Self.request("ep-bg"))
        await coordinator.drainForTesting()

        let fired = await spy.fired
        #expect(fired.count == 1)
        #expect(fired.first?.episodeId == "ep-bg")
        #expect(fired.first?.assetId == "asset-bg")

        let records = await spy.records
        #expect(records.count == 1)
        #expect(records.first?.outcome == .fired)
        #expect(records.first?.source == .backgroundDownload)
    }

    @Test("VERIFY THE TAP POST-EWAG: an asset row that appears after a few polls still fires")
    func tapFiresOnceTheAssetRegisters() async {
        let spy = KickoffSpy()
        let probes = DayZeroProbeCounter()
        let coordinator = Self.makeCoordinator(spy: spy, maxAttempts: 10) { _ in
            await probes.increment() >= 3 ? Self.ready("asset-tap") : .awaitingAnalysisAsset
        }
        await coordinator.requestKickoff(Self.request("ep-tap", source: .downloadAndAnalyzeTap))
        await coordinator.drainForTesting()

        let fired = await spy.fired
        #expect(fired.count == 1, "post-ewag the asset registers in seconds — the wait outlives it")
        let records = await spy.records
        #expect(records.first?.outcome == .fired)
        #expect(records.first?.source == .downloadAndAnalyzeTap)
        #expect(records.first?.pollCount == 3)

        let violations = await spy.violations
        #expect(violations.isEmpty, "a firing kickoff is not a violation")
    }

    @Test("PRE-EWAG REPRODUCTION: the file lands, the asset never does → counted, recorded, and SURFACED")
    func preEwagGiveUpIsSurfaced() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in .awaitingAnalysisAsset }
        await coordinator.requestKickoff(Self.request("ep-dead"))
        await coordinator.drainForTesting()

        #expect(await spy.fired.isEmpty)

        let records = await spy.records
        #expect(records.count == 1)
        #expect(records.first?.outcome == .noAnalysisAsset)

        let violations = await spy.violations
        #expect(violations.count == 1, "the give-up that hid for a week now writes a line")
        #expect(violations.first?.code == .rediffDayZeroKickoffNoAnalysisAsset)
        #expect(violations.first?.description.contains("hash(ep-dead)") == true,
                "the episode id is HASHED, as every other producer on this stream does")

        // Counted PER CAUSE, never as one indistinguishable total (playhead-djl0).
        #expect(await coordinator.giveUpCount(.noAnalysisAsset) == 1)
        #expect(await coordinator.giveUpCount(.noPinnedFile) == 0)
        #expect(await coordinator.giveUpCount(.fired) == 0)
    }

    @Test("a download whose bytes never land is a DIFFERENT counted cause with a DIFFERENT code")
    func missingFileIsItsOwnCause() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in .awaitingPinnedFile }
        await coordinator.requestKickoff(Self.request("ep-nofile"))
        await coordinator.drainForTesting()

        #expect(await spy.records.first?.outcome == .noPinnedFile)
        #expect(await spy.violations.first?.code == .rediffDayZeroKickoffNoPinnedFile)
        #expect(await coordinator.giveUpCount(.noPinnedFile) == 1)
        #expect(await coordinator.giveUpCount(.noAnalysisAsset) == 0)
    }

    @Test("a SECOND kickoff for an episode already in flight does not double-spend the wait")
    func inFlightKickoffIsDeduplicated() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in Self.ready("asset-dup") }
        await coordinator.requestKickoff(Self.request("ep-dup"))
        await coordinator.requestKickoff(Self.request("ep-dup"))
        await coordinator.drainForTesting()

        #expect(await spy.fired.count == 1, "two play paths + the tap can all reach one episode")
    }

    @Test("A CONTENDED BATCH DRAINS NEWEST FIRST — Dan's ordering sub-decision, end to end")
    func contendedBatchDrainsNewestFirst() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { episodeId in
            Self.ready("asset-\(episodeId)")
        }
        // Enqueue oldest-first so a FIFO drain would produce the opposite order.
        await coordinator.suspendDrainForTesting()
        await coordinator.requestKickoff(Self.request("old", publishedAt: 100, enqueuedAt: 1))
        await coordinator.requestKickoff(Self.request("mid", publishedAt: 200, enqueuedAt: 2))
        await coordinator.requestKickoff(Self.request("new", publishedAt: 300, enqueuedAt: 3))
        await coordinator.resumeDrainForTesting()
        await coordinator.drainForTesting()

        #expect(await spy.fired.map(\.episodeId) == ["new", "mid", "old"])
    }

    @Test("the drain is SERIAL — two day-0 fetches must never race the daily budget")
    func drainIsSerial() async {
        let spy = KickoffSpy()
        let inFlight = DayZeroProbeCounter()
        let coordinator = RediffDayZeroKickoffCoordinator(
            maxAttempts: 3,
            pollNanos: 1,
            probe: { _ in Self.ready("asset") },
            fire: { _, _ in
                await inFlight.increment()
                await Task.yield()
                await inFlight.decrement()
            },
            claimKickoff: { await spy.noteClaim($0) },
            recordKickoff: { await spy.noteRecord($0) },
            reportViolation: { _, _ in },
            episodeIdHasher: { $0 },
            sleep: { _ in },
            now: { 1_000 }
        )
        for index in 0..<5 {
            await coordinator.requestKickoff(Self.request("ep-\(index)"))
        }
        await coordinator.drainForTesting()
        #expect(await inFlight.peak() == 1, "never two day-0 attempts at once")
        #expect(await spy.records.count == 5)
    }

    @Test("the recorded wait is measured, so a device pull can see HOW LONG a give-up waited")
    func recordsCarryTheWaitEvidence() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy, maxAttempts: 4) { _ in .awaitingPinnedFile }
        await coordinator.requestKickoff(Self.request("ep-slow"))
        await coordinator.drainForTesting()
        #expect(await spy.records.first?.pollCount == 4)
    }

    // MARK: - playhead-kg8h: the durable claim

    @Test("THE ACCEPTANCE: a kickoff that never settles STILL leaves a durable row")
    func aKickoffThatNeverSettlesStillLeavesARow() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in Self.ready("asset-x") }
        // The drain is held, standing in for every way the work never completes:
        // a background-URLSession wake whose budget expires inside the readiness
        // poll, jetsam during the ~66 MB k-way fetch, a force-quit. Before this
        // bead nothing at all was written until `settle`, so all of those were
        // byte-identical in the database to a download that never happened.
        await coordinator.suspendDrainForTesting()
        await coordinator.requestKickoff(Self.request("ep-unsettled"))

        let claims = await spy.claims
        #expect(claims.count == 1, "the row is owed the moment the kickoff is requested")
        #expect(claims.first?.episodeId == "ep-unsettled")
        #expect(claims.first?.source == .backgroundDownload)
        #expect(await spy.records.isEmpty, "nothing has settled — and that is the point")
        #expect(await spy.fired.isEmpty)
    }

    @Test("the claim lands BEFORE the trigger is fired, not after the re-fetch returns")
    func claimPrecedesTheFetch() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in Self.ready("asset-order") }
        await coordinator.requestKickoff(Self.request("ep-order"))
        await coordinator.drainForTesting()

        #expect(await spy.timeline == ["claim(ep-order)", "fire(ep-order)", "settle(ep-order)"],
                "a claim written after `fire` would be lost by exactly the failures it exists to record")
    }

    @Test("A CONTENDED BATCH claims ALL FIVE up front — the serial drain no longer hides four of them")
    func everyQueuedRequestIsClaimedEvenThoughTheDrainIsSerial() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in Self.ready("asset-batch") }
        // playhead-kxgh measured five day-0 requests taking 33 minutes through
        // this strictly serial drain, and `pending` is in-memory (playhead-jra6).
        // Claiming at enqueue is what makes the four still queued visible.
        await coordinator.suspendDrainForTesting()
        for index in 0..<5 {
            await coordinator.requestKickoff(Self.request("ep-batch-\(index)"))
        }

        #expect(await spy.claims.count == 5)
        #expect(await spy.records.isEmpty)
        #expect(Set(await spy.claims.map(\.episodeId))
            == Set((0..<5).map { "ep-batch-\($0)" }))
    }

    @Test("a DEDUPLICATED second request claims nothing — one download, one row, one count")
    func deduplicatedRequestDoesNotClaimTwice() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in Self.ready("asset-dup2") }
        await coordinator.suspendDrainForTesting()
        await coordinator.requestKickoff(Self.request("ep-dup2"))
        await coordinator.requestKickoff(Self.request("ep-dup2"))

        #expect(await spy.claims.count == 1,
                "the in-flight guard must run BEFORE the claim, or a doubled request inflates kickoffCount")
    }

    @Test("the in-flight guard is taken BEFORE the claim suspends — a RE-ENTRANT duplicate claims nothing")
    func inFlightGuardIsTakenBeforeTheClaimSuspends() async {
        let spy = KickoffSpy()
        let reentries = DayZeroProbeCounter()
        let box = KickoffCoordinatorBox()
        // The coordinator is an actor, so `requestKickoff` yields the actor at
        // the claim's store write and ANY other caller may enter while it is
        // parked there — and in production there really are several (the tap,
        // the background-completion observer, a force-quit resume). Moving
        // `inFlight.insert` below that await lets the re-entrant duplicate sail
        // past both guards and claim the same download twice, which is a
        // `kickoffCount` of 2 for one episode.
        let coordinator = RediffDayZeroKickoffCoordinator(
            maxAttempts: 1,
            pollNanos: 1,
            probe: { _ in Self.ready("asset-re") },
            fire: { _, _ in },
            claimKickoff: { claim in
                await spy.noteClaim(claim)
                guard await reentries.increment() == 1 else { return }
                guard let coordinator = await box.coordinator else { return }
                await coordinator.requestKickoff(Self.request("ep-re"))
            },
            recordKickoff: { await spy.noteRecord($0) },
            reportViolation: { _, _ in },
            episodeIdHasher: { $0 },
            sleep: { _ in },
            now: { 1_000 }
        )
        await box.set(coordinator)
        await coordinator.suspendDrainForTesting()
        await coordinator.requestKickoff(Self.request("ep-re"))

        #expect(await spy.claims.count == 1,
                "one download must produce one claim even when a duplicate arrives mid-write")
    }

    @Test("the claim carries the SOURCE, so a pull can still tell a tap from a background download")
    func claimCarriesItsSource() async {
        let spy = KickoffSpy()
        let coordinator = Self.makeCoordinator(spy: spy) { _ in Self.ready("asset-src") }
        await coordinator.suspendDrainForTesting()
        await coordinator.requestKickoff(Self.request("ep-tap-src", source: .downloadAndAnalyzeTap))
        #expect(await spy.claims.first?.source == .downloadAndAnalyzeTap)
    }
}

// MARK: - 3c. The setting is persisted, and defaults to WiFi only

@Suite("Day-0 transport setting persistence (playhead-4dqe)")
struct DayZeroTransportSettingTests {

    private func makeDefaults() -> UserDefaults {
        let suite = "playhead-4dqe-\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suite)!
        defaults.removePersistentDomain(forName: suite)
        return defaults
    }

    @Test("an untouched install reads WiFi only")
    func defaultsToWifiOnly() {
        let defaults = makeDefaults()
        #expect(UserPreferencesSnapshot.current(from: defaults).dayZeroAllowsCellular == false)
    }

    @Test("the setting round-trips, and is INDEPENDENT of the download cellular preference")
    func roundTripsIndependently() {
        let defaults = makeDefaults()
        UserPreferencesSnapshot.save(dayZeroAllowsCellular: true, to: defaults)
        let snapshot = UserPreferencesSnapshot.current(from: defaults)
        #expect(snapshot.dayZeroAllowsCellular)
        #expect(snapshot.allowsCellular, "downloading over cellular is a SEPARATE question")

        UserPreferencesSnapshot.save(allowsCellular: false, to: defaults)
        #expect(UserPreferencesSnapshot.current(from: defaults).dayZeroAllowsCellular,
                "changing the download preference must not move the preparation preference")
    }

    @Test("the SwiftData model carries the same default, so the two stores cannot disagree on day 1")
    func modelDefaultMatches() {
        let preferences = UserPreferences()
        #expect(preferences.dayZeroAllowsCellular == RediffActivation.dayZeroAllowsCellularByDefault)
    }

    @Test("the settings row has copy the user can act on")
    func settingsCopyExists() {
        #expect(!SettingsL274Copy.prepareOverCellularLabel.isEmpty)
        #expect(!SettingsL274Copy.prepareOverCellularSubLine.isEmpty)
        // The sub-line must warn about the cost — that is the whole reason the
        // default is off.
        #expect(SettingsL274Copy.prepareOverCellularSubLine.lowercased().contains("data"))
    }
}

// MARK: - 3e. The setting must reach the SOCKET, not just the gate

@Suite("Day-0 fetch transport plumbing (playhead-4dqe)")
struct DayZeroFetchTransportTests {

    @Test("the LAGGED path is unchanged: the default request is still WiFi-only")
    func laggedRequestStaysWifiOnly() {
        let request = RediffFetchRequest.makeBaseRequest(
            cacheBustedURL: URL(string: "https://cdn.example.com/a.mp3?_cb=1")!,
            persona: nil
        )
        #expect(request.allowsCellularAccess == false)
    }

    @Test("a day-0 request under the opted-in setting ACTUALLY permits cellular — a gate alone would just fail the fetch")
    func dayZeroRequestPermitsCellular() {
        let request = RediffFetchRequest.makeBaseRequest(
            cacheBustedURL: URL(string: "https://cdn.example.com/a.mp3?_cb=1")!,
            persona: nil,
            allowsCellular: true
        )
        #expect(request.allowsCellularAccess)
    }

    @Test("the WiFi-only session is unchanged — the lagged sweep's only WiFi enforcement")
    func wifiOnlySessionUnchanged() {
        let config = URLSessionRangedAudioSampler.makeSessionConfiguration(allowsCellular: false)
        #expect(config.allowsCellularAccess == false)
        #expect(config.allowsExpensiveNetworkAccess == false)
        #expect(config.allowsConstrainedNetworkAccess == false)
        #expect(config.urlCache == nil)
    }

    @Test("the cellular-capable session opens cellular and expensive paths — but NEVER the constrained one")
    func cellularSessionStillHonoursLowDataMode() {
        let config = URLSessionRangedAudioSampler.makeSessionConfiguration(allowsCellular: true)
        #expect(config.allowsCellularAccess)
        #expect(config.allowsExpensiveNetworkAccess)
        #expect(config.allowsConstrainedNetworkAccess == false,
                "Low Data Mode wins at the socket too, not only at the gate")
        #expect(config.urlCache == nil, "a rediff fetch is never served from cache")
    }

    @Test("a fetcher with no cellular session can never reach cellular, whatever the setting says")
    func fetcherWithoutCellularSessionStaysWifi() {
        let fetcher = URLSessionFullEpisodeFetcher(
            session: URLSessionRangedAudioSampler.makeWiFiOnlySession(),
            cellularSession: nil,
            allowsCellular: { true }
        )
        #expect(fetcher.usesCellularSessionForTesting() == false)
    }

    @Test("a fetcher with a cellular session follows the SETTING, both ways")
    func fetcherFollowsTheSetting() {
        let wifi = URLSessionRangedAudioSampler.makeWiFiOnlySession()
        let cellular = URLSessionRangedAudioSampler.makeSession(allowsCellular: true)
        #expect(URLSessionFullEpisodeFetcher(
            session: wifi, cellularSession: cellular, allowsCellular: { true }
        ).usesCellularSessionForTesting())
        #expect(URLSessionFullEpisodeFetcher(
            session: wifi, cellularSession: cellular, allowsCellular: { false }
        ).usesCellularSessionForTesting() == false)
    }

    @Test("the live transport provider reports iOS Low Data Mode, and the stub does not invent one")
    func lowDataModeIsAReadableSignal() async {
        #expect(await WifiTransportStatusProvider().isLowDataMode() == false)
    }
}
