// CapabilitiesServiceStreamTests.swift
// playhead-zqhz: pin the CapabilitiesService stream contract that the
// Settings capability-observer used to cover only indirectly (and
// flakily, via a MainActor child task racing a 1 s deadline).
//
// `SettingsViewModel.observeCapabilitySnapshots` — and the runtime's
// eligibility-cache follower in `PlayheadRuntime.init` — both rely on
// `capabilityUpdates()` yielding the CURRENT snapshot immediately on
// subscribe, so consumers render a verdict without waiting for the next
// device-state change. The Settings tests are now stream-driven against
// a scripted provider, so this test keeps the real actor's seed-emission
// contract pinned directly. The wait below is event-driven (await the
// first stream element); there is no polling deadline — the suite time
// limit is the only backstop.

import Foundation
import Testing

@testable import Playhead

@Suite("CapabilitiesService stream contract (playhead-zqhz)")
struct CapabilitiesServiceStreamTests {

    @Test("capabilityUpdates() yields the current snapshot immediately on subscribe",
          .timeLimit(.minutes(1)))
    func capabilityUpdatesYieldsSeedSnapshotOnSubscribe() async throws {
        let service = CapabilitiesService()
        let expected = await service.currentSnapshot

        let stream = await service.capabilityUpdates()
        var iterator = stream.makeAsyncIterator()
        let first = await iterator.next()

        let seed = try #require(first, "Stream must yield a seed snapshot without waiting for a device-state change")
        // The seed is the snapshot the actor held at subscribe time. A
        // concurrent refreshSnapshot (scheduled by init) may have
        // replaced the snapshot we read above, so the seed is either
        // that snapshot or a FRESHER one — never a stale/garbage value.
        #expect(seed.capturedAt >= expected.capturedAt,
                "Seed snapshot must be the subscribe-time snapshot or fresher")
    }
}
