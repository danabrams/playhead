// LivePlaybackSignalProvider.swift
// playhead-narl.2: Live `ShadowPlaybackSignalProvider` backed by
// `PlaybackService`.
//
// The coordinator's protocol requires SYNCHRONOUS getters
// (`isStrictlyPlaying()` and `currentAsset()`) because the coordinator
// samples the signals at tick time with no actor hop. `PlaybackService`
// lives on `@PlaybackServiceActor`, so the live provider maintains a
// lock-protected snapshot that a long-running consumer task refreshes
// from `PlaybackService.observeStates()`.
//
// Q1 decision (narl.2 continuation): the provider wraps `PlaybackService`.
// `isStrictlyPlaying` returns true iff `state.status == .playing`.
// `currentAsset()` returns a `ShadowActiveAsset` built from the currently
// loaded episode's analysis asset id (threaded in via a closure) and the
// latest `currentTime`.

import Foundation
import os

/// Live `ShadowPlaybackSignalProvider` backed by `PlaybackService`.
///
/// Snapshots are written by a long-running async task that subscribes to
/// `PlaybackService.observeStates()`; synchronous protocol getters read the
/// snapshot from an `OSAllocatedUnfairLock` so the coordinator never hops
/// onto `PlaybackServiceActor` to sample the tick.
///
/// The `assetIdProvider` closure is supplied by the host runtime
/// (`PlayheadRuntime`) so the provider can translate the currently loaded
/// episode into the analysis asset id the shadow capture pipeline keys on.
/// Returning `nil` from the closure (or when no episode is loaded) causes
/// `currentAsset()` to return `nil`, which makes Lane A no-op for the tick.
final class LivePlaybackSignalProvider: ShadowPlaybackSignalProvider,
    @unchecked Sendable
{
    private struct Snapshot: Sendable {
        var isPlaying: Bool = false
        var currentTime: TimeInterval = 0
    }

    private let state: OSAllocatedUnfairLock<Snapshot>
    private let assetIdProvider: @Sendable () -> String?
    private let observerTask: Task<Void, Never>

    /// - Parameters:
    ///   - playbackService: The transport whose state we mirror.
    ///   - assetIdProvider: Closure that returns the analysis-asset id for
    ///     the currently loaded episode. `nil` means "no asset loaded / no
    ///     mapping yet"; Lane A no-ops for the tick.
    init(
        playbackService: PlaybackService,
        assetIdProvider: @escaping @Sendable () -> String?
    ) {
        let initial = Snapshot()
        let box = OSAllocatedUnfairLock(initialState: initial)
        self.state = box
        self.assetIdProvider = assetIdProvider
        // Subscribe to PlaybackService state updates in the background.
        // `observeStates()` immediately yields the current snapshot on
        // subscription (see PlaybackService.observeStates), so the first
        // tick after construction reflects transport truth without
        // waiting for a change event.
        self.observerTask = Task { [box, playbackService] in
            let stream = await playbackService.observeStates()
            for await playState in stream {
                let isPlaying: Bool
                switch playState.status {
                case .playing:
                    isPlaying = true
                default:
                    isPlaying = false
                }
                box.withLock { snap in
                    snap.isPlaying = isPlaying
                    snap.currentTime = playState.currentTime
                }
                if Task.isCancelled { break }
            }
        }
    }

    deinit {
        observerTask.cancel()
    }

    // MARK: - ShadowPlaybackSignalProvider

    func isStrictlyPlaying() -> Bool {
        state.withLock { $0.isPlaying }
    }

    func currentAsset() -> ShadowActiveAsset? {
        guard let assetId = assetIdProvider() else { return nil }
        let seconds = state.withLock { $0.currentTime }
        return ShadowActiveAsset(assetId: assetId, playheadSeconds: seconds)
    }

    // MARK: - Test hooks

    #if DEBUG
    /// Test-only: override the snapshot directly. Production callers should
    /// always drive the provider through `PlaybackService.observeStates()`.
    func _testingSetSnapshot(isPlaying: Bool, currentTime: TimeInterval) {
        state.withLock { snap in
            snap.isPlaying = isPlaying
            snap.currentTime = currentTime
        }
    }
    #endif
}
