// TransportCensusTests.swift
// playhead-1ueyd — the rail for the instrument, not for the product.
//
// `PlaybackTransport.swift` states "Production shares one process-wide player —
// there is one transport". Since 2026-09-07 the device writes TWO session files
// per launch, both carrying the persisted-state census, and that census has
// exactly one call site — `PlayheadRuntime`'s bootstrap. So two runtimes reach
// bootstrap, and each builds a transport eagerly in its own init.
//
// The census added under that bead reports `constructed`, `released` and their
// difference from a device pull. THE RELEASE COUNT IS THE LOAD-BEARING HALF: a
// bootstrap reading says `live=2` whether or not the discarded runtime's
// transport is later collected, and only the difference between "still alive"
// and "released" decides whether every audio interruption is handled twice.
//
// So the deinit hook needs a rail of its own. If it were silently never called,
// the device would report `live=2` forever and that reading would be believed —
// an instrument reporting a defect it invented. This suite makes both ends of
// the count fire.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-1ueyd — the transport census counts both ends", .serialized)
struct TransportCensusTests {

    @Test("constructing a transport increments the census, releasing it increments the other half",
          .timeLimit(.minutes(1)))
    func censusCountsConstructionAndRelease() async throws {
        let before = PlaybackService.transportCensus.withLock { $0 }

        // Scope the service so ARC can release it at the end of the block.
        do {
            let service = await PlaybackService(
                audioSession: FakeAudioSessionProvider(),
                nowPlayingInfo: FakeNowPlayingInfoProvider(),
                notificationCenter: NotificationCenter()
            )
            let during = PlaybackService.transportCensus.withLock { $0 }
            #expect(
                during.constructed == before.constructed + 1,
                "the init hook did not count this transport — the device reading would undercount"
            )
            #expect(during.released == before.released, "nothing has been released yet")
            await service.tearDown()
        }

        // The release is ARC's to schedule, so poll rather than assume it has
        // already happened. A bounded wait that never satisfies is itself the
        // finding: it would mean transports are never released, which is a
        // different defect from the one this instrument is aimed at, and one
        // the device reading would otherwise present as "two live transports".
        var released = PlaybackService.transportCensus.withLock { $0.released }
        let deadline = ContinuousClock.now.advanced(by: .seconds(5))
        while released == before.released, ContinuousClock.now < deadline {
            try? await Task.sleep(for: .milliseconds(25))
            released = PlaybackService.transportCensus.withLock { $0.released }
        }

        #expect(
            released == before.released + 1,
            """
            the transport was scoped, torn down and dropped, and `deinit` never counted it. \
            Either the hook is not wired or a transport outlives its owner — and until this \
            passes, a `live=2` reading from a device pull cannot be trusted to mean anything.
            """
        )
    }
}
