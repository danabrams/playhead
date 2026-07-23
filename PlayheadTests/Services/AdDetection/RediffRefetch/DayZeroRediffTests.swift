// DayZeroRediffTests.swift
// playhead-xsdz.36.4: offline coverage for the DAY-0 (immediate, play-time)
// rediff trigger — the pure WiFi+charging/opt-in gate AND the trigger that fires
// an immediate k-way rediff for a just-started episode through the SHARED
// re-fetch service (reusing the Unit 1 & 2 fetch → byte-align → marks →
// never-persist-B machinery). Pins the acceptance criteria:
//
//   • fires on play when enabled + gated (WiFi + charging/opt-in)  → `fires*`
//   • does NOT fire flag-off / cellular / unplugged-no-optin       → `inert*` /
//                                                                     `doesNotFire*`
//   • the day-0 B-side(s) are DELETED (never-persist-B)            → `deletes*`
//   • bandwidth is accounted in the same ledger                    → the recorder
//                                                                     `.rotated` cost
//   • the day-0 k-way count is its own constant, independent of    → `dayZeroKWay*`
//     the lagged single-fetch default
//
// The doubles (KWaySpyFullFetcher, SpyKWayBSideConsumer, SpyTempFileRemover,
// SpyRefetchRecorder, Stub*Sampler, StubBSideFingerprinter, StubRefetchEnumerator,
// StubTaskScheduler) are the same ones RediffRefetchTests.swift / Stubs.swift
// define — reused, not re-declared.

import Foundation
import Testing
@testable import Playhead

// MARK: - Pure gate truth table

@Suite("DayZeroRediffGate (playhead-xsdz.36.4)")
struct DayZeroRediffGateTests {

    @Test("a disabled flag is inert regardless of power/network")
    func disabledIsInert() {
        for reachability: TransportSnapshot.Reachability in [.wifi, .cellular, .unreachable] {
            for charging in [true, false] {
                for optIn in [true, false] {
                    #expect(!DayZeroRediffGate.allows(
                        enabled: false, reachability: reachability,
                        isCharging: charging, deepScanOptIn: optIn
                    ))
                }
            }
        }
    }

    @Test("WiFi + charging allows (the default power gate)")
    func wifiChargingAllows() {
        #expect(DayZeroRediffGate.allows(
            enabled: true, reachability: .wifi, isCharging: true, deepScanOptIn: false
        ))
    }

    @Test("WiFi + unplugged + deep-scan opt-in allows")
    func wifiOptInAllows() {
        #expect(DayZeroRediffGate.allows(
            enabled: true, reachability: .wifi, isCharging: false, deepScanOptIn: true
        ))
    }

    @Test("WiFi + unplugged + no opt-in is rejected")
    func wifiUnpluggedNoOptInRejected() {
        #expect(!DayZeroRediffGate.allows(
            enabled: true, reachability: .wifi, isCharging: false, deepScanOptIn: false
        ))
    }

    @Test("cellular is rejected even charging + opt-in (a ~54 MB × K fetch is never on cellular)")
    func cellularRejected() {
        #expect(!DayZeroRediffGate.allows(
            enabled: true, reachability: .cellular, isCharging: true, deepScanOptIn: true
        ))
    }

    @Test("no reachable network is rejected")
    func unreachableRejected() {
        #expect(!DayZeroRediffGate.allows(
            enabled: true, reachability: .unreachable, isCharging: true, deepScanOptIn: true
        ))
    }
}

// MARK: - Trigger: fires vs inert, through the shared service

@Suite("DayZeroRediffTrigger (playhead-xsdz.36.4)")
struct DayZeroRediffTriggerTests {

    static let day = RediffRefetchPolicy.Configuration.secondsPerDay
    static let enclosure = URL(string: "https://cdn.example.com/current.mp3")!
    static let played = URL(fileURLWithPath: "/tmp/played-a.mp3")

    /// Build a trigger over a REAL service wired with k-way spy doubles. Day-0
    /// routes marks through the `dayZeroMinter` (byte-exact mint), NOT the lagged
    /// `bsideConsumer`/revalidate path — so these doubles exercise the reworked
    /// first-listen path.
    private func makeTrigger(
        enabled: Bool,
        reachability: TransportSnapshot.Reachability,
        isCharging: Bool,
        deepScanOptIn: Bool = false,
        serviceEnabled: Bool = true,
        kWayFetchCount: Int = RediffActivation.dayZeroKWayFetchCount,
        fetcher: KWaySpyFullFetcher,
        minter: SpyDayZeroMinter,
        remover: any RediffTempFileRemoving,
        recorder: SpyRefetchRecorder
    ) -> DayZeroRediffTrigger {
        let service = RediffRefetchService(
            enabled: serviceEnabled,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: remover,
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: minter,
            now: { 100 * Self.day }
        )
        return DayZeroRediffTrigger(
            service: service,
            enabled: enabled,
            kWayFetchCount: kWayFetchCount,
            reachabilityProvider: { reachability },
            chargeStateProvider: { isCharging },
            deepScanOptInProvider: { deepScanOptIn }
        )
    }

    @discardableResult
    private func fire(
        _ trigger: DayZeroRediffTrigger,
        forceDeepScanOptIn: Bool = false
    ) async -> RediffRefetchService.SweepSummary {
        await trigger.triggerIfEligible(
            analysisAssetId: "asset-day0",
            enclosureURL: Self.enclosure,
            playedFileURL: Self.played,
            forceDeepScanOptIn: forceDeepScanOptIn
        )
    }

    @Test("fires when enabled + WiFi + charging: k-way (=3) distinct-persona fetch, all handed to the minter, all deleted, bandwidth accounted, .dayZeroMarked (resolved)")
    func firesWhenEnabledAndGated() async {
        let fetcher = KWaySpyFullFetcher()
        let minter = SpyDayZeroMinter()   // returns 1 mark by default
        let remover = SpyTempFileRemover()
        let recorder = SpyRefetchRecorder()
        let trigger = makeTrigger(
            enabled: true, reachability: .wifi, isCharging: true,
            fetcher: fetcher, minter: minter, remover: remover, recorder: recorder
        )
        let summary = await fire(trigger)

        // K=3 distinct-persona fetch in the divergence-reliable order.
        #expect(fetcher.calls.count == 3, "day-0 uses dayZeroKWayFetchCount = 3")
        #expect(fetcher.calls.map { $0.persona?.name }
            == ["applecoremedia-iphone", "applecoremedia-macintosh", "overcast"])
        // The enclosure URL is the one the trigger passed.
        #expect(fetcher.calls.allSatisfy { $0.url == Self.enclosure })
        // The minter receives ALL 3 B-copies at once (one k-way byte-exact mint).
        #expect(minter.calls.count == 1)
        #expect(minter.calls.first?.bSideURLs.count == 3)
        #expect(minter.calls.first?.assetId == "asset-day0")
        // Never-persist-B: every fetched copy is deleted.
        let expected = (0..<3).map { URL(fileURLWithPath: "/tmp/kway-bcopy-\($0).mp3") }
        #expect(Set(remover.removed) == Set(expected))
        // Bandwidth accounted; a MARK ⇒ .dayZeroMarked (resolved) — day-0 K≥3
        // supersets the lagged K=1 sweep, so a mark may resolve the shared state.
        guard case let .dayZeroMarked(_, cost, markCount, newState) = recorder.outcomes.first else {
            Issue.record("expected .dayZeroMarked, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(markCount == 1)
        #expect(cost.fullFetchBytes == 3 * 54_000_000)
        #expect(cost.precheckBytes == 0, "day-0 does no pre-check — zero pre-check bytes")
        #expect(newState.resolved)
        #expect(summary.rotatedCount == 1)
        #expect(summary.fullFetchBytes == 3 * 54_000_000)
    }

    @Test("POISONING FIX: a day-0 run that mints NO marks records .dayZeroUnmarked — bytes accounted, but NO resolve / NO state advance")
    func unmarkedDayZeroDoesNotResolve() async {
        let fetcher = KWaySpyFullFetcher()
        let minter = SpyDayZeroMinter()
        minter.markCountToReturn = 0   // nothing byte-exact/≥2-persona-robust found
        let remover = SpyTempFileRemover()
        let recorder = SpyRefetchRecorder()
        let trigger = makeTrigger(
            enabled: true, reachability: .wifi, isCharging: true,
            fetcher: fetcher, minter: minter, remover: remover, recorder: recorder
        )
        let summary = await fire(trigger)

        #expect(minter.calls.count == 1, "the minter still ran (byte-exact attempt)")
        // Still fetched + deleted (bandwidth is spent regardless of the verdict).
        #expect(fetcher.calls.count == 3)
        let expected = (0..<3).map { URL(fileURLWithPath: "/tmp/kway-bcopy-\($0).mp3") }
        #expect(Set(remover.removed) == Set(expected))
        // The outcome is .dayZeroUnmarked: bytes accounted, NO AttemptState — the
        // asset stays a lagged candidate (fetchRediffCandidateSeeds still sees it).
        guard case let .dayZeroUnmarked(_, cost, error) = recorder.outcomes.first else {
            Issue.record("expected .dayZeroUnmarked, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(cost.fullFetchBytes == 3 * 54_000_000, "bytes spent are still accounted")
        #expect(error == nil, "a clean no-mark run carries no error")
        #expect(summary.rotatedCount == 0, "no mark ⇒ nothing resolved")
        #expect(summary.failedCount == 0, "a clean no-mark run is not a failure")
    }

    @Test("fires unplugged when the deep-scan opt-in is set")
    func firesUnpluggedWithDeepScanOptIn() async {
        let fetcher = KWaySpyFullFetcher()
        let trigger = makeTrigger(
            enabled: true, reachability: .wifi, isCharging: false, deepScanOptIn: true,
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            remover: SpyTempFileRemover(), recorder: SpyRefetchRecorder()
        )
        await fire(trigger)
        #expect(fetcher.calls.count == 3, "opt-in permits an unplugged WiFi day-0 fetch")
    }

    @Test("Download & Analyze (playhead-3xtw): forceDeepScanOptIn fires unplugged on WiFi even with the settings opt-in OFF (the tap IS the opt-in)")
    func downloadAndAnalyzeForcedOptInFiresUnplugged() async {
        let fetcher = KWaySpyFullFetcher()
        // Unplugged, settings deep-scan opt-in OFF — only the forced (tap) opt-in
        // can permit this fetch.
        let trigger = makeTrigger(
            enabled: true, reachability: .wifi, isCharging: false, deepScanOptIn: false,
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            remover: SpyTempFileRemover(), recorder: SpyRefetchRecorder()
        )
        await fire(trigger, forceDeepScanOptIn: true)
        #expect(fetcher.calls.count == 3, "the explicit Download & Analyze tap grants the deep-scan opt-in on unplugged WiFi")
    }

    @Test("Download & Analyze: forceDeepScanOptIn NEVER overrides the WiFi requirement (cellular stays rejected)")
    func downloadAndAnalyzeForcedOptInStillRequiresWiFi() async {
        let fetcher = KWaySpyFullFetcher()
        let trigger = makeTrigger(
            enabled: true, reachability: .cellular, isCharging: true, deepScanOptIn: false,
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            remover: SpyTempFileRemover(), recorder: SpyRefetchRecorder()
        )
        await fire(trigger, forceDeepScanOptIn: true)
        #expect(fetcher.calls.isEmpty, "the tap grants the opt-in leg only — WiFi is still required, never cellular")
    }

    /// Records whether a gate-signal provider was ever consulted, so the
    /// flag-off short-circuit's "reads no signal" claim is WITNESSED, not just
    /// implied by source order.
    private final class CallFlag: @unchecked Sendable {
        private let lock = NSLock()
        private var called = false
        func mark() { lock.lock(); called = true; lock.unlock() }
        var wasCalled: Bool { lock.lock(); defer { lock.unlock() }; return called }
    }

    @Test("INERT when the day-0 flag is off — no power/network signal is read, no fetch")
    func inertWhenFlagOff() async {
        let fetcher = KWaySpyFullFetcher()
        let minter = SpyDayZeroMinter()
        let recorder = SpyRefetchRecorder()
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: minter,
            now: { 100 * Self.day }
        )
        // Providers that would pass the gate (WiFi + charging) AND record if
        // consulted — the flag-off guard must short-circuit BEFORE reading them.
        let reachRead = CallFlag()
        let chargeRead = CallFlag()
        let trigger = DayZeroRediffTrigger(
            service: service,
            enabled: false,
            kWayFetchCount: RediffActivation.dayZeroKWayFetchCount,
            reachabilityProvider: { reachRead.mark(); return .wifi },
            chargeStateProvider: { chargeRead.mark(); return true },
            deepScanOptInProvider: { false }
        )
        let summary = await fire(trigger)

        #expect(!reachRead.wasCalled, "flag off ⇒ reachability is never read")
        #expect(!chargeRead.wasCalled, "flag off ⇒ charge state is never read")
        #expect(fetcher.calls.isEmpty, "flag off ⇒ no play-time fetch")
        #expect(minter.calls.isEmpty)
        #expect(recorder.outcomes.isEmpty)
        #expect(summary == RediffRefetchService.SweepSummary())
    }

    @Test("does NOT fire on cellular (even charging + opt-in)")
    func doesNotFireOnCellular() async {
        let fetcher = KWaySpyFullFetcher()
        let trigger = makeTrigger(
            enabled: true, reachability: .cellular, isCharging: true, deepScanOptIn: true,
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            remover: SpyTempFileRemover(), recorder: SpyRefetchRecorder()
        )
        await fire(trigger)
        #expect(fetcher.calls.isEmpty, "cellular is never a day-0 transport")
    }

    @Test("does NOT fire unplugged with no opt-in")
    func doesNotFireUnpluggedNoOptIn() async {
        let fetcher = KWaySpyFullFetcher()
        let trigger = makeTrigger(
            enabled: true, reachability: .wifi, isCharging: false, deepScanOptIn: false,
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            remover: SpyTempFileRemover(), recorder: SpyRefetchRecorder()
        )
        await fire(trigger)
        #expect(fetcher.calls.isEmpty, "unplugged + no opt-in is rejected")
    }

    @Test("a disabled SERVICE is a no-op even when the day-0 gate passes (OFF byte-identity)")
    func disabledServiceIsNoOp() async {
        let fetcher = KWaySpyFullFetcher()
        let trigger = makeTrigger(
            enabled: true, reachability: .wifi, isCharging: true,
            serviceEnabled: false,
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            remover: SpyTempFileRemover(), recorder: SpyRefetchRecorder()
        )
        let summary = await fire(trigger)
        #expect(fetcher.calls.isEmpty, "a disabled service touches no network even on the day-0 path")
        #expect(summary.candidateCount == 0)
    }

    @Test("day-0 DELETES the real B-copy temp file (never-persist-B, real filesystem)")
    func deletesRealBCopyFile() async throws {
        let dir = try makeTempDir(prefix: "DayZeroRediff-delete")
        defer { try? FileManager.default.removeItem(at: dir) }
        let bCopy = dir.appendingPathComponent("downloaded-b.mp3")
        try Data(repeating: 7, count: 4096).write(to: bCopy)
        #expect(FileManager.default.fileExists(atPath: bCopy.path))

        // A single-fetch day-0 with a stub fetcher returning the REAL file, and
        // the REAL FileManager remover. (The minter is a spy — this test pins
        // never-persist-B deletion, which happens regardless of the mark verdict.)
        let fetcher = StubFullFetcher()
        fetcher.fileToReturn = bCopy
        fetcher.byteCount = 4096
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: FileManagerTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: SpyDayZeroMinter(),
            now: { 100 * Self.day }
        )
        let trigger = DayZeroRediffTrigger(
            service: service, enabled: true, kWayFetchCount: 1,
            reachabilityProvider: { .wifi },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false }
        )
        await fire(trigger)
        #expect(!FileManager.default.fileExists(atPath: bCopy.path),
                "the day-0 B-copy must be deleted on exit")
    }

    @Test("the day-0 k-way count is its own constant (3), the flag defaults OFF, and neither touches the lagged single-fetch default")
    func dayZeroConstantsAreIndependent() {
        #expect(RediffActivation.dayZeroEnabledByDefault == false,
                "day-0 ships inert — flipping it on is the rollout go/no-go")
        #expect(RediffActivation.dayZeroKWayFetchCount == 3,
                "day-0 draws the iPhone+Mac+Overcast divergence core")
        #expect(RediffActivation.productionKWayFetchCount == 1,
                "the lagged sweep's single-fetch default is untouched")
    }
}

// MARK: - Download & Analyze readiness WAIT (playhead-xsdz.36.4)

/// The on-demand "Download & Analyze" (playhead-3xtw) day-0 kickoff cannot fire
/// synchronously after `prepare()` — on a genuine first listen the download runs
/// async and the analysis asset is registered LATER, so neither the pinned file
/// nor the asset row exists yet. `awaitDayZeroPreparationReadiness` is the
/// bounded WAIT that closes that gap; these pin its wait-then-fire / give-up
/// behavior directly (the seam the earlier trigger-only test could not reach).
@Suite("PlayheadRuntime.awaitDayZeroPreparationReadiness (playhead-xsdz.36.4)")
struct DayZeroPreparationReadinessTests {

    private final class Counter: @unchecked Sendable {
        private let lock = NSLock()
        private var n = 0
        func bump() { lock.lock(); n += 1; lock.unlock() }
        var count: Int { lock.lock(); defer { lock.unlock() }; return n }
    }

    @Test("ready on the first attempt → returns immediately, never sleeps")
    func readyImmediately() async {
        let sleeps = Counter()
        let result = await PlayheadRuntime.awaitDayZeroPreparationReadiness(
            maxAttempts: 5, pollNanos: 1,
            sleep: { _ in sleeps.bump() },
            resolveReady: { "ready" }
        )
        #expect(result == "ready")
        #expect(sleeps.count == 0, "a first-attempt hit never sleeps")
    }

    @Test("WAITS across misses then fires: resolves after 2 nil polls (the D&A download + asset-registration lag)")
    func readyAfterMisses() async {
        let sleeps = Counter()
        let attempts = Counter()
        let result = await PlayheadRuntime.awaitDayZeroPreparationReadiness(
            maxAttempts: 10, pollNanos: 1,
            sleep: { _ in sleeps.bump() },
            resolveReady: { () -> String? in
                attempts.bump()
                return attempts.count >= 3 ? "ready" : nil   // nil, nil, ready
            }
        )
        #expect(result == "ready")
        #expect(attempts.count == 3)
        #expect(sleeps.count == 2, "slept between the two misses, not after the hit")
    }

    @Test("never ready → gives up (nil) after the attempt budget; the lagged sweep is the backstop")
    func neverReadyGivesUp() async {
        let sleeps = Counter()
        let result: String? = await PlayheadRuntime.awaitDayZeroPreparationReadiness(
            maxAttempts: 4, pollNanos: 1,
            sleep: { _ in sleeps.bump() },
            resolveReady: { nil }
        )
        #expect(result == nil, "budget elapsed ⇒ give up (fail-safe)")
        #expect(sleeps.count == 3, "sleeps between attempts (maxAttempts - 1), never after the last")
    }
}
