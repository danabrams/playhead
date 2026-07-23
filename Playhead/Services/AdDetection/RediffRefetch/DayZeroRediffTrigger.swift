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

/// The pure eligibility decision for the day-0 (play-time) rediff trigger. A
/// day-0 fetch is a full ~54 MB × K second download at play time, so it is
/// WiFi-ONLY and gated on charging OR an explicit user "deep-scan" opt-in —
/// never cellular, never unplugged-without-opt-in. Pure so the truth table is
/// unit-tested without `NWPathMonitor` / `UIDevice`.
enum DayZeroRediffGate {
    /// Whether the day-0 trigger may run given the flag + the live power/network
    /// context. `false` for a disabled flag, any non-WiFi reachability
    /// (cellular / unreachable), or an unplugged device with no deep-scan
    /// opt-in.
    static func allows(
        enabled: Bool,
        reachability: TransportSnapshot.Reachability,
        isCharging: Bool,
        deepScanOptIn: Bool
    ) -> Bool {
        guard enabled else { return false }
        // WiFi-only: a ~54 MB × K second fetch must never run on cellular
        // (`.cellular`) or with no reachable path (`.unreachable`).
        guard reachability == .wifi else { return false }
        // On WiFi: charging is the default power gate; a deep-scan opt-in lets a
        // user who wants marks-on-first-listen accept the battery cost unplugged.
        return isCharging || deepScanOptIn
    }
}

/// The play-time day-0 rediff trigger. `PlayheadRuntime.playEpisode` calls
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
    /// Live network reachability (production: the shared
    /// `LiveTransportStatusProvider`, `NWPathMonitor`-backed).
    let reachabilityProvider: @Sendable () async -> TransportSnapshot.Reachability
    /// Live charging state (production: `UIDeviceBatteryProvider`).
    let chargeStateProvider: @Sendable () async -> Bool
    /// The user's "deep-scan" opt-in — lets a day-0 fetch run unplugged on WiFi.
    /// Defaults to `false` (no opt-in) until a settings toggle is wired.
    let deepScanOptInProvider: @Sendable () -> Bool

    init(
        service: RediffRefetchService,
        enabled: Bool = RediffActivation.dayZeroEnabledByDefault,
        kWayFetchCount: Int = RediffActivation.dayZeroKWayFetchCount,
        reachabilityProvider: @escaping @Sendable () async -> TransportSnapshot.Reachability,
        chargeStateProvider: @escaping @Sendable () async -> Bool,
        deepScanOptInProvider: @escaping @Sendable () -> Bool = { false }
    ) {
        self.service = service
        self.enabled = enabled
        self.kWayFetchCount = kWayFetchCount
        self.reachabilityProvider = reachabilityProvider
        self.chargeStateProvider = chargeStateProvider
        self.deepScanOptInProvider = deepScanOptInProvider
    }

    /// Fire an immediate day-0 rediff for the just-started episode IF the flag is
    /// on AND the live WiFi + (charging OR deep-scan) gate passes.
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
    @discardableResult
    func triggerIfEligible(
        analysisAssetId: String,
        enclosureURL: URL,
        playedFileURL: URL,
        at now: Double = Date().timeIntervalSince1970
    ) async -> RediffRefetchService.SweepSummary {
        guard enabled else { return SweepSummary() }
        let reachability = await reachabilityProvider()
        let isCharging = await chargeStateProvider()
        let deepScanOptIn = deepScanOptInProvider()
        guard DayZeroRediffGate.allows(
            enabled: enabled,
            reachability: reachability,
            isCharging: isCharging,
            deepScanOptIn: deepScanOptIn
        ) else {
            return SweepSummary()
        }

        // `downloadedAt` / `attemptState` are unused by the day-0 path (no ≥24h
        // gate, no pre-check); a fresh `.initial` state is what a day-0 rotation
        // resolves or a day-0 failure advances from.
        let candidate = RediffRefetchCandidate(
            assetId: analysisAssetId,
            enclosureURL: enclosureURL,
            downloadedAt: now,
            localAudioURL: playedFileURL,
            attemptState: .initial
        )
        return await service.runDayZeroRefetch(for: candidate, kWayFetchCount: kWayFetchCount)
    }

    private typealias SweepSummary = RediffRefetchService.SweepSummary
}
