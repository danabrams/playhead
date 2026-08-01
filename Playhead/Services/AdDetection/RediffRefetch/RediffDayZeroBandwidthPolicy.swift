// RediffDayZeroBandwidthPolicy.swift
// playhead-4dqe: the two SEPARATE bandwidth questions day-0 must answer, kept
// separate on purpose.
//
//   WHICH NETWORK may day-0 spend on?  → `DayZeroTransportPolicy` (a USER
//                                        SETTING as of Dan 2026-08-01, plus the
//                                        OS-level Low Data Mode override).
//   HOW MUCH may it spend per day?     → `RediffDayZeroDailyBudget` (a rolling
//                                        24 h byte window in the ledger).
//
// Dan's decision is explicit that these must not collapse into each other:
// "the setting governs *which network*, the ledger governs *how much*. They are
// different questions and one must not silently become the other." The failure
// mode being guarded against is one this codebase keeps re-discovering —
// letting a proxy stand in for the quantity it is not. WiFi-only was never a
// byte bound; it was a *transport* rule that happened to make bytes free for
// most people. Turning it into a user setting removes the accidental bound, so
// the real one has to exist explicitly.
//
// A NOTE ON WHAT WAS ACTUALLY THERE. The bead says to "keep the daily byte
// budget in the rediff_bandwidth_ledger". There was none to keep: before this
// change `rediff_bandwidth_ledger` was a pure cumulative ACCUMULATOR — seven
// counters and a timestamp, written after the fact, read by nothing that
// decides anything. No window, no cap, no enforcement path. The only thing
// bounding day-0's bytes was the per-asset attempt cap
// (`DayZeroRediffAttemptPolicy.maxAttempts`), which bounds REPLAYS of one
// episode and says nothing at all about a library-wide daily total. So this
// file BUILDS the budget the decision assumes, in the ledger, where the
// decision says it belongs.

import Foundation

// MARK: - Transport

/// The reason day-0 may not spend right now, or `.allow`.
///
/// A decision enum rather than a `Bool` for the same reason
/// `RediffDayZeroExit` exists: a gate that answers "no" without saying why is a
/// silent give-up, and this arc's standing bound is that a refusal must be
/// nameable, countable, and surfaceable. Each `deny*` case maps to a
/// `RediffDayZeroExit` the trigger records against the asset.
enum DayZeroTransportDecision: Sendable, Equatable {
    case allow
    /// `RediffActivation.dayZeroEnabledByDefault` is off — the inert build.
    case denyDisabled
    /// No reachable network path at all.
    case denyUnreachable
    /// iOS Low Data Mode is active on the current path. Wins on BOTH
    /// transports, regardless of the user's cellular setting.
    case denyLowDataMode
    /// The path is cellular (or a metered "expensive" path such as a personal
    /// hotspot) and the user has not allowed cellular preparation.
    case denyCellularNotAllowed
    /// Unplugged with no deep-scan opt-in — the battery axis, unchanged by the
    /// transport decision.
    case denyPower

    var isAllowed: Bool { self == .allow }
}

/// Everything about the network read at ONE instant, so the gate cannot see a
/// reachability from one moment and a Low Data Mode flag from another.
struct DayZeroTransportSnapshot: Sendable, Equatable {
    let reachability: TransportSnapshot.Reachability
    /// `NWPath.isConstrained` — iOS Low Data Mode.
    let isLowDataMode: Bool
    /// The USER SETTING (Dan 2026-08-01: "wifi vs 5g should be a user setting,
    /// most people have unlimited bandwidth"). Default `false` (WiFi only).
    let allowsCellular: Bool

    init(
        reachability: TransportSnapshot.Reachability,
        isLowDataMode: Bool,
        allowsCellular: Bool
    ) {
        self.reachability = reachability
        self.isLowDataMode = isLowDataMode
        self.allowsCellular = allowsCellular
    }
}

/// The PURE transport decision for a day-0 fetch.
///
/// PRECEDENCE, and why it is in this order:
///
/// 1. `enabled` — the inert build reads nothing and decides nothing.
/// 2. reachability `.unreachable` — there is no transport to have a policy about.
/// 3. **Low Data Mode** — checked BEFORE the user's cellular setting, and
///    applied on WiFi as well as cellular. Low Data Mode is the user's
///    OS-LEVEL instruction to every app on the device to stop doing
///    discretionary bulk transfers; a ~130 MB speculative re-fetch is exactly
///    what it means. An in-app toggle is not consent to override it, and the
///    toggle's own copy would be a lie if it could. This is the one leg the
///    user-facing setting cannot outrank.
/// 4. the user's cellular setting — the axis Dan moved out of code.
/// 5. power — unchanged from playhead-xsdz.36.4.
enum DayZeroTransportPolicy {
    static func decide(
        enabled: Bool,
        transport: DayZeroTransportSnapshot,
        isCharging: Bool,
        deepScanOptIn: Bool
    ) -> DayZeroTransportDecision {
        guard enabled else { return .denyDisabled }
        guard transport.reachability != .unreachable else { return .denyUnreachable }
        // Low Data Mode wins on BOTH transports, regardless of the setting.
        guard !transport.isLowDataMode else { return .denyLowDataMode }
        if transport.reachability == .cellular, !transport.allowsCellular {
            return .denyCellularNotAllowed
        }
        // The battery axis is orthogonal to the transport axis and Dan's
        // decision did not touch it: charging OR an explicit deep-scan /
        // "Download & Analyze" opt-in.
        guard isCharging || deepScanOptIn else { return .denyPower }
        return .allow
    }
}

// MARK: - Daily byte budget

/// The rolling 24 h day-0 byte window persisted in `rediff_bandwidth_ledger`.
///
/// `startedAt == nil` means the budget has never been spent against — the fresh
/// install, and deliberately NOT the same value as "a window that started at
/// time 0", which would look like an ancient window and roll on first read.
struct RediffDayZeroBudgetWindow: Sendable, Equatable {
    var startedAt: Double?
    var spentBytes: Int

    init(startedAt: Double? = nil, spentBytes: Int = 0) {
        self.startedAt = startedAt
        self.spentBytes = spentBytes
    }

    static let empty = RediffDayZeroBudgetWindow()
}

/// The PURE "does this attempt fit in today's byte budget?" decision.
///
/// Governs HOW MUCH, on BOTH transports — a WiFi user is subject to the same
/// daily cap as a cellular one. That is deliberate and is the second half of
/// keeping the two questions separate: if the budget only applied on cellular
/// it would be a transport rule wearing a budget's clothes, and flipping the
/// setting would silently change two things at once.
enum RediffDayZeroDailyBudget {

    /// Length of the rolling window. Rolling from FIRST SPEND rather than
    /// calendar-midnight: a calendar boundary needs a timezone, and a device
    /// that crosses one would either gain or lose a whole day's allowance for
    /// no reason a user could understand.
    static let windowSeconds: TimeInterval = 24 * 60 * 60

    /// Bytes day-0 may spend per rolling 24 h, across the whole library.
    ///
    /// **1 GB.** The arithmetic: a day-0 attempt measured 120.3 MB and
    /// 125.7–131.8 MB in the field (K=2 B-copies), so the cap admits ~7–8
    /// episodes a day — comfortably above a heavy subscriber's real daily
    /// intake, and far below what an unbounded retry pathology could spend.
    ///
    /// This number's job is NOT to be the right answer for every plan; it is to
    /// EXIST. Until this bead there was no ceiling of any kind, and the only
    /// reason that was survivable is that WiFi-only made the bytes free for
    /// most people. Dan's decision removes that accident, so the ceiling has to
    /// be real. A user on a metered plan who opts into cellular still cannot
    /// lose more than ~1 GB/day to speculative preparation, and the default
    /// (WiFi only) means they never opt in by accident.
    static let dailyCapBytes = 1_000_000_000

    /// Estimated bytes ONE B-copy costs, used for the pre-flight check.
    ///
    /// 65 MB, derived from the field measurement above (120.3–131.8 MB total at
    /// K=2 ⇒ ~60–66 MB per copy) rather than from `RediffRefetchService`'s
    /// internal 54 MB modelling constant — the admission check must bound what
    /// will ACTUALLY be spent, and under-estimating is the direction that
    /// overspends.
    static let estimatedBytesPerBCopy = 65_000_000

    /// Estimated cost of one k-way day-0 attempt.
    static func estimatedAttemptBytes(kWayFetchCount: Int) -> Int {
        max(0, kWayFetchCount) * estimatedBytesPerBCopy
    }

    /// Whether `window` has already elapsed at `now` (so the next spend starts
    /// a fresh window).
    static func windowHasElapsed(_ window: RediffDayZeroBudgetWindow, now: Double) -> Bool {
        guard let startedAt = window.startedAt else { return true }
        return now >= startedAt + windowSeconds
    }

    /// Bytes still available in the window containing `now`. A fresh or elapsed
    /// window has the whole cap.
    static func remainingBytes(_ window: RediffDayZeroBudgetWindow, now: Double) -> Int {
        guard !windowHasElapsed(window, now: now) else { return dailyCapBytes }
        return max(0, dailyCapBytes - window.spentBytes)
    }

    /// May an attempt estimated at `estimatedCost` bytes run now?
    ///
    /// Compares against the FULL estimate rather than admitting a partial fetch:
    /// a day-0 mint needs `RediffSlotOwnership.dayZeroMinKWayBCopies` copies to
    /// diff at all, so spending the last 60 MB of the budget on half an attempt
    /// buys nothing and is strictly worse than waiting for the window to roll.
    static func allows(
        _ window: RediffDayZeroBudgetWindow,
        estimatedCost: Int,
        now: Double
    ) -> Bool {
        estimatedCost <= remainingBytes(window, now: now)
    }
}
