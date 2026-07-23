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

    /// Build a trigger over a REAL service wired with k-way spy doubles.
    private func makeTrigger(
        enabled: Bool,
        reachability: TransportSnapshot.Reachability,
        isCharging: Bool,
        deepScanOptIn: Bool = false,
        serviceEnabled: Bool = true,
        kWayFetchCount: Int = RediffActivation.dayZeroKWayFetchCount,
        fetcher: KWaySpyFullFetcher,
        consumer: SpyKWayBSideConsumer,
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
            bsideConsumer: consumer,
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
    private func fire(_ trigger: DayZeroRediffTrigger) async -> RediffRefetchService.SweepSummary {
        await trigger.triggerIfEligible(
            analysisAssetId: "asset-day0",
            enclosureURL: Self.enclosure,
            playedFileURL: Self.played
        )
    }

    @Test("fires when enabled + WiFi + charging: k-way (=3) distinct-persona fetch, all consumed, all deleted, bandwidth accounted")
    func firesWhenEnabledAndGated() async {
        let fetcher = KWaySpyFullFetcher()
        let consumer = SpyKWayBSideConsumer()
        let remover = SpyTempFileRemover()
        let recorder = SpyRefetchRecorder()
        let trigger = makeTrigger(
            enabled: true, reachability: .wifi, isCharging: true,
            fetcher: fetcher, consumer: consumer, remover: remover, recorder: recorder
        )
        let summary = await fire(trigger)

        // K=3 distinct-persona fetch in the divergence-reliable order.
        #expect(fetcher.calls.count == 3, "day-0 uses dayZeroKWayFetchCount = 3")
        #expect(fetcher.calls.map { $0.persona?.name }
            == ["applecoremedia-iphone", "applecoremedia-macintosh", "overcast"])
        // The enclosure URL is the one the trigger passed.
        #expect(fetcher.calls.allSatisfy { $0.url == Self.enclosure })
        // The consumer stages ALL 3 at once (one k-way handoff → one revalidation).
        #expect(consumer.consumedFileURLs.count == 1)
        #expect(consumer.consumedFileURLs.first?.count == 3)
        // Never-persist-B: every fetched copy is deleted.
        let expected = (0..<3).map { URL(fileURLWithPath: "/tmp/kway-bcopy-\($0).mp3") }
        #expect(Set(remover.removed) == Set(expected))
        // Bandwidth accounted in the same ledger (recorder .rotated cost).
        guard case let .rotated(_, cost, _, newState) = recorder.outcomes.first else {
            Issue.record("expected .rotated, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(cost.fullFetchBytes == 3 * 54_000_000)
        #expect(cost.precheckBytes == 0, "day-0 does no pre-check — zero pre-check bytes")
        #expect(newState.resolved)
        #expect(summary.rotatedCount == 1)
        #expect(summary.fullFetchBytes == 3 * 54_000_000)
    }

    @Test("fires unplugged when the deep-scan opt-in is set")
    func firesUnpluggedWithDeepScanOptIn() async {
        let fetcher = KWaySpyFullFetcher()
        let trigger = makeTrigger(
            enabled: true, reachability: .wifi, isCharging: false, deepScanOptIn: true,
            fetcher: fetcher, consumer: SpyKWayBSideConsumer(),
            remover: SpyTempFileRemover(), recorder: SpyRefetchRecorder()
        )
        await fire(trigger)
        #expect(fetcher.calls.count == 3, "opt-in permits an unplugged WiFi day-0 fetch")
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
        let consumer = SpyKWayBSideConsumer()
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
            bsideConsumer: consumer,
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
        #expect(consumer.consumedFileURLs.isEmpty)
        #expect(recorder.outcomes.isEmpty)
        #expect(summary == RediffRefetchService.SweepSummary())
    }

    @Test("does NOT fire on cellular (even charging + opt-in)")
    func doesNotFireOnCellular() async {
        let fetcher = KWaySpyFullFetcher()
        let trigger = makeTrigger(
            enabled: true, reachability: .cellular, isCharging: true, deepScanOptIn: true,
            fetcher: fetcher, consumer: SpyKWayBSideConsumer(),
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
            fetcher: fetcher, consumer: SpyKWayBSideConsumer(),
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
            fetcher: fetcher, consumer: SpyKWayBSideConsumer(),
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
        // the REAL FileManager remover.
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
            bsideConsumer: SpyKWayBSideConsumer(),
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
