// PlaybackSystemSeams.swift
// Injectable seams for the two process-global singletons PlaybackService
// touches: AVAudioSession.sharedInstance() and MPNowPlayingInfoCenter.default().
//
// Motivation: playhead-86s. Before these seams existed, constructing two
// PlaybackService instances in parallel tests clobbered each other's state
// via the shared singletons, which in turn forced the test suite to run
// serially (parallelizable: false in project.yml). Production still wires
// in the real singletons via the defaulted initializer, so non-test callers
// see no behavior change.

@preconcurrency import AVFoundation
import Foundation
import MediaPlayer

// MARK: - AudioSessionProviding

/// The subset of AVAudioSession that PlaybackService actually uses. Keeping
/// the protocol narrow makes fakes trivial and ensures PlaybackService can't
/// quietly grow a new dependency on the shared singleton.
protocol AudioSessionProviding: Sendable {
    func setCategory(
        _ category: AVAudioSession.Category,
        mode: AVAudioSession.Mode,
        policy: AVAudioSession.RouteSharingPolicy
    ) throws

    func setActive(_ active: Bool) throws
}

/// Production implementation that forwards to AVAudioSession.sharedInstance().
/// Crucially this is the ONLY place in the app that touches the real singleton
/// on PlaybackService's behalf.
struct SystemAudioSessionProvider: AudioSessionProviding {
    static let shared = SystemAudioSessionProvider()

    func setCategory(
        _ category: AVAudioSession.Category,
        mode: AVAudioSession.Mode,
        policy: AVAudioSession.RouteSharingPolicy
    ) throws {
        try AVAudioSession.sharedInstance().setCategory(category, mode: mode, policy: policy)
    }

    func setActive(_ active: Bool) throws {
        try AVAudioSession.sharedInstance().setActive(active)
    }
}

// MARK: - NowPlayingInfoProviding

/// The subset of MPNowPlayingInfoCenter that PlaybackService uses: a simple
/// get/set on the nowPlayingInfo dictionary. Implementations must be safe to
/// access from PlaybackServiceActor — the production wrapper just forwards to
/// the main-thread-backed MPNowPlayingInfoCenter.default(), and fakes store
/// the dictionary behind their own synchronization.
protocol NowPlayingInfoProviding: Sendable {
    func getNowPlayingInfo() -> [String: Any]?
    func setNowPlayingInfo(_ info: [String: Any]?)
}

/// Production implementation backed by MPNowPlayingInfoCenter.default().
/// MPNowPlayingInfoCenter reads/writes are themselves main-thread-backed
/// inside MediaPlayer; this wrapper exists purely so PlaybackService never
/// mentions the singleton directly.
struct SystemNowPlayingInfoProvider: NowPlayingInfoProviding {
    static let shared = SystemNowPlayingInfoProvider()

    func getNowPlayingInfo() -> [String: Any]? {
        MPNowPlayingInfoCenter.default().nowPlayingInfo
    }

    func setNowPlayingInfo(_ info: [String: Any]?) {
        MPNowPlayingInfoCenter.default().nowPlayingInfo = info
    }
}
