// LivePlaybackSignalProviderTests.swift
// playhead-narl.2 continuation: focused coverage for the synchronous
// getters and `assetIdProvider` semantics of the live playback signal
// provider. Swift Testing suite.
//
// Coverage strategy:
//   - Construct the provider with a real `PlaybackService` (built via the
//     fake seams InterruptionHandlingTests uses), then drive the internal
//     snapshot via the `#if DEBUG` test hook `_testingSetSnapshot`.
//   - Synchronous reads follow each `_testingSetSnapshot` with NO `await`
//     between set and read so the background `observeStates()` consumer
//     Task cannot interleave and clobber the test's value.
//   - `currentAsset()` uses the `assetIdProvider` closure: returning `nil`
//     short-circuits to `nil`; returning a String wraps the snapshot's
//     `currentTime` into a `ShadowActiveAsset`.

import Foundation
import os
import Testing

@testable import Playhead

@Suite("LivePlaybackSignalProvider (playhead-narl.2)")
struct LivePlaybackSignalProviderTests {

    // MARK: - isStrictlyPlaying

    @Test("isStrictlyPlaying reflects the snapshot's isPlaying leg")
    func isStrictlyPlayingReflectsSnapshot() async throws {
        let service = await makeService()
        let provider = LivePlaybackSignalProvider(
            playbackService: service,
            assetIdProvider: { nil }
        )
        // Overwrite any value the background observer may have written at
        // init time. No `await` between set and read so the observer Task
        // cannot interleave.
        provider._testingSetSnapshot(isPlaying: true, currentTime: 42)
        #expect(provider.isStrictlyPlaying() == true)

        provider._testingSetSnapshot(isPlaying: false, currentTime: 42)
        #expect(provider.isStrictlyPlaying() == false)
    }

    // MARK: - currentAsset

    @Test("currentAsset returns nil when assetIdProvider returns nil")
    func currentAssetNilWhenNoAssetId() async throws {
        let service = await makeService()
        let provider = LivePlaybackSignalProvider(
            playbackService: service,
            assetIdProvider: { nil }
        )
        provider._testingSetSnapshot(isPlaying: true, currentTime: 123)
        #expect(provider.currentAsset() == nil)
    }

    @Test("currentAsset wraps snapshot currentTime when assetId is available")
    func currentAssetWrapsSnapshot() async throws {
        let service = await makeService()
        let provider = LivePlaybackSignalProvider(
            playbackService: service,
            assetIdProvider: { "asset-xyz" }
        )
        provider._testingSetSnapshot(isPlaying: true, currentTime: 99)
        let asset = provider.currentAsset()
        #expect(asset?.assetId == "asset-xyz")
        #expect(asset?.playheadSeconds == 99)
    }

    @Test("currentAsset sees mutations to the assetIdProvider's underlying value")
    func currentAssetReadsProviderOnEachCall() async throws {
        // Use a reference box so the closure can observe changes without a
        // shared actor hop. `OSAllocatedUnfairLock` matches the pattern the
        // production runtime uses.
        let box = OSAllocatedUnfairLock<String?>(initialState: nil)
        let service = await makeService()
        let provider = LivePlaybackSignalProvider(
            playbackService: service,
            assetIdProvider: { box.withLock { $0 } }
        )
        provider._testingSetSnapshot(isPlaying: true, currentTime: 5)
        // Initially nil → no asset.
        #expect(provider.currentAsset() == nil)
        // Mutate the box; next currentAsset() call sees the new value.
        box.withLock { $0 = "asset-after-mutation" }
        #expect(provider.currentAsset()?.assetId == "asset-after-mutation")
    }
}

// MARK: - Service factory

/// Build a minimal `PlaybackService` using the same fake seams
/// InterruptionHandlingTests uses so this suite doesn't hit real audio
/// hardware.
private func makeService() async -> PlaybackService {
    let center = NotificationCenter()
    return await PlaybackService(
        audioSession: FakeAudioSessionProvider(),
        nowPlayingInfo: FakeNowPlayingInfoProvider(),
        notificationCenter: center
    )
}
