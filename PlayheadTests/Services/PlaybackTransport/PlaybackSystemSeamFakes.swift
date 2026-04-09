// PlaybackSystemSeamFakes.swift
// Test fakes for the PlaybackService system seams introduced by playhead-86s.
//
// Each fake records the calls PlaybackService makes, backed by an internal
// lock so tests can assert from any isolation domain without data races.
// They are intentionally dumb: no behavior, just recording.

@preconcurrency import AVFoundation
import Foundation
@testable import Playhead

// MARK: - FakeAudioSessionProvider

/// Records setCategory/setActive calls without touching the process-global
/// AVAudioSession. Lets parallel PlaybackService instances operate without
/// clobbering each other.
final class FakeAudioSessionProvider: AudioSessionProviding, @unchecked Sendable {
    struct CategoryCall: Sendable, Equatable {
        let category: String
        let mode: String
        let policy: UInt
    }

    private let lock = NSLock()
    private var _categoryCalls: [CategoryCall] = []
    private var _setActiveCalls: [Bool] = []

    var categoryCalls: [CategoryCall] {
        lock.lock(); defer { lock.unlock() }
        return _categoryCalls
    }

    var setActiveCalls: [Bool] {
        lock.lock(); defer { lock.unlock() }
        return _setActiveCalls
    }

    func setCategory(
        _ category: AVAudioSession.Category,
        mode: AVAudioSession.Mode,
        policy: AVAudioSession.RouteSharingPolicy
    ) throws {
        lock.lock(); defer { lock.unlock() }
        _categoryCalls.append(CategoryCall(
            category: category.rawValue,
            mode: mode.rawValue,
            policy: policy.rawValue
        ))
    }

    func setActive(_ active: Bool) throws {
        lock.lock(); defer { lock.unlock() }
        _setActiveCalls.append(active)
    }
}

// MARK: - FakeNowPlayingInfoProvider

/// Local dictionary store that stands in for MPNowPlayingInfoCenter.default().
/// Each PlaybackService instance can hold its own fake so parallel tests don't
/// clobber each other's now-playing metadata.
final class FakeNowPlayingInfoProvider: NowPlayingInfoProviding, @unchecked Sendable {
    private let lock = NSLock()
    private var _info: [String: Any]?

    var info: [String: Any]? {
        lock.lock(); defer { lock.unlock() }
        return _info
    }

    func getNowPlayingInfo() -> [String: Any]? {
        lock.lock(); defer { lock.unlock() }
        return _info
    }

    func setNowPlayingInfo(_ info: [String: Any]?) {
        lock.lock(); defer { lock.unlock() }
        _info = info
    }
}
