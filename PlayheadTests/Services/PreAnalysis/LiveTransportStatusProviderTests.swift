// LiveTransportStatusProviderTests.swift
// playhead-ml96: smoke tests for the NWPathMonitor-backed
// `LiveTransportStatusProvider`. The full reachability truth-table is
// already exercised at the admission-gate level by
// AdmissionGateTests.swift via `StubTransportStatusProvider`; these
// tests cover the pieces that are 100% live-provider-specific:
//   1. `init` doesn't crash and the provider can answer
//      `currentReachability()` without blocking.
//   2. The injected `allowsCellularProvider` closure is honored — when
//      the user has opted out, `userAllowsCellular()` returns `false`
//      regardless of network conditions.
//   3. The pure `reachability(from:)` mapping function returns the
//      correct case for `nil` (treated as unreachable) — the only
//      branch we can drive deterministically without an `NWPath`
//      instance (Network.framework forbids constructing them).

import Foundation
import Network
import os.lock
import Testing
@testable import Playhead

@Suite("LiveTransportStatusProvider — smoke + user-pref honoring")
struct LiveTransportStatusProviderTests {

    @Test("init succeeds and currentReachability returns a valid case")
    func testInitAndSnapshotReadable() async {
        let provider = LiveTransportStatusProvider(
            allowsCellularProvider: { true }
        )
        let reachability = await provider.currentReachability()
        // Whatever the simulator host reports is fine — we only care
        // that the provider is wired and answers without blocking
        // forever or trapping.
        switch reachability {
        case .wifi, .cellular, .unreachable:
            // OK — exhaustively a valid case.
            break
        }
    }

    @Test("userAllowsCellular honors injected pref closure: true → true")
    func testUserAllowsCellularTrue() async {
        let provider = LiveTransportStatusProvider(
            allowsCellularProvider: { true }
        )
        let allows = await provider.userAllowsCellular()
        #expect(allows == true)
    }

    @Test("userAllowsCellular honors injected pref closure: false → false")
    func testUserAllowsCellularFalse() async {
        // Regression for the playhead-ml96 acceptance criterion:
        // "On a cellular-only device with allowsCellular=false, scheduler
        // admission rejects maintenance-session jobs with .wifiRequired."
        // The admission gate consults this method on every cellular pass;
        // if the live provider doesn't propagate the pref, the cellular-
        // blocked path can never fire.
        let provider = LiveTransportStatusProvider(
            allowsCellularProvider: { false }
        )
        let allows = await provider.userAllowsCellular()
        #expect(allows == false)
    }

    @Test("userAllowsCellular re-reads the closure on every call")
    func testUserAllowsCellularReReadsClosure() async {
        // The default production closure reads `UserPreferencesSnapshot`
        // (UserDefaults). The live provider must NOT cache the value at
        // init time, or pref toggles in Settings won't take effect
        // until app relaunch.
        let storage = OSAllocatedUnfairLock<Bool>(initialState: true)
        let provider = LiveTransportStatusProvider(
            allowsCellularProvider: {
                storage.withLock { $0 }
            }
        )
        #expect(await provider.userAllowsCellular() == true)
        storage.withLock { $0 = false }
        #expect(await provider.userAllowsCellular() == false)
        storage.withLock { $0 = true }
        #expect(await provider.userAllowsCellular() == true)
    }

    @Test("reachability(from: nil) maps to .unreachable")
    func testReachabilityNilPathIsUnreachable() {
        // Pure-mapping coverage. The `nil` branch fires before
        // `NWPathMonitor`'s first update lands; we treat that as
        // unreachable so admission doesn't accidentally succeed during
        // the boot-time race.
        #expect(LiveTransportStatusProvider.reachability(from: nil) == .unreachable)
    }
}
