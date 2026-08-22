// AdSkipCue.swift
// playhead-nqwr: the mechanism that makes an auto-skip AUDIBLE.
//
// Dan, 2026-08-21: "I think there also needs to be a chime or sound to indicate
// that an ad is being skipped. Something fitting with the subdued aesthetics of
// the app."
//
// WHY A SOUND AND NOT MORE BANNER. A banner makes a CLAIM about a specific span
// and invites a judgement; when it is wrong it costs trust, which is what
// playhead-bwxi cost. A cue asserts only "I acted" — the listener can already
// hear whether the show resumed correctly, so the cue cannot be wrong about
// anything they cannot check. It is also the ONLY feedback channel that reaches
// a listener whose Now Playing surface is not mounted (lock screen, CarPlay),
// which is the state playhead-2d6i leaves with no announcement at all. That
// bead is cited, not absorbed: this one does not add banner replay.
//
// ─────────────────────────────────────────────────────────────────────────────
// THE SESSION IS THE HARD PART, NOT THE SOUND
// ─────────────────────────────────────────────────────────────────────────────
//
// The app owns one `AVAudioSession`, configured once in
// `PlaybackService.configureAudioSession`. Everything here is built so a cue
// cannot touch it:
//
//   * NOTHING in this file or `AdSkipCueSound.swift` names `AVAudioSession`,
//     `setActive` or `setCategory`. A second `AVAudioPlayer` mixes into the
//     session the transport already activated; it never configures one.
//     `AdSkipCueSourceCanaryTests` pins that absence, because a session mistake
//     is invisible at runtime on the simulator.
//   * The cue is fired and stopped ONLY from `PlaybackServiceActor`, so
//     "is the transport playing?" and "start the cue" are one atomic decision.
//     See `PlaybackService.emitAdSkipCue`.
//   * `playAdSkipCue()` and `stopAdSkipCue()` must return without blocking:
//     they are called from the actor that owns the seam. Every audio call is
//     hopped onto this type's own serial queue.
//
// ─────────────────────────────────────────────────────────────────────────────
// DEFAULTS SET HERE, ALL OF THEM DAN'S TO OVERTURN
// ─────────────────────────────────────────────────────────────────────────────
//
//   1. ONE sound at the skip, not a two-part in/out. "Subdued" was his word and
//      a bracket draws more attention than the event deserves.
//      → to overturn: a second call in `PlaybackService.duckSeekRelease` before
//        the duck, using a second `AdSkipCueSound` source.
//   2. Fires on EVERY completed auto-skip, whatever the span's length. A
//      duration threshold is a product threshold and is his.
//      → to overturn: one predicate in `PlaybackService.emitAdSkipCue`.
//   3. ON by default (`AdSkipCueSettings.defaultValue`), with a Settings
//      toggle to silence it.
//      → to overturn: flip that one constant.
//   4. Reserved for actual CUTS. A mark or a banner gets nothing audible:
//      nothing happened to the audio, so there is nothing to acknowledge.
//      → to overturn: the mark tier would need its own emit site; there is
//        deliberately none, and the canary counts the call sites.

import AVFoundation
import Foundation

// MARK: - AdSkipCuePlaying

/// The transport's view of the cue: start one, or silence one that is
/// sounding. Both are FIRE-AND-FORGET and must not block — the only caller is
/// `PlaybackServiceActor`, at the seam of a skip it has just performed.
protocol AdSkipCuePlaying: Sendable {
    /// Sound the cue. Implementations decide whether a request that arrives
    /// while a cue is already sounding is admitted; see `AdSkipCuePlayer`.
    func playAdSkipCue()

    /// Silence a sounding cue immediately. Called whenever the transport stops
    /// being the thing the listener is hearing — an interruption, a route that
    /// vanished, a user pause, teardown. A cue left ringing after the episode
    /// has been pulled out from under it is the "alert" reading this whole
    /// feature is trying to avoid.
    func stopAdSkipCue()
}

// MARK: - AdSkipCueSettings

/// The user-facing switch. UserDefaults rather than a `UserPreferences`
/// (SwiftData) column because the reader is `PlaybackServiceActor` at a seam:
/// it must answer synchronously, off the main actor, with no store hop.
enum AdSkipCueSettings {

    /// `@AppStorage` key. Namespaced under `playback.` to sit with transport
    /// behaviour rather than with the detection settings.
    static let userDefaultsKey = "playback.adSkipCueEnabled"

    /// Default #3: ON. A correct auto-skip is currently imperceptible, which is
    /// the whole defect; shipping the fix off by default would leave it that
    /// way for everyone who never opens Settings.
    static let defaultValue = true

    /// Read the switch.
    ///
    /// `object(forKey:) as? Bool ?? defaultValue` and NOT `bool(forKey:)`:
    /// `bool(forKey:)` answers `false` for a key that was never written, so it
    /// silently inverts an ON-by-default flag for every user until the day they
    /// toggle it. Same idiom as `UserPreferencesSnapshot.current(from:)`.
    static func isEnabled(_ defaults: UserDefaults = .standard) -> Bool {
        defaults.object(forKey: userDefaultsKey) as? Bool ?? defaultValue
    }

    /// Write the switch. `@AppStorage` in Settings writes the same key; this
    /// exists for tests and for any non-SwiftUI caller.
    static func setEnabled(_ enabled: Bool, in defaults: UserDefaults = .standard) {
        defaults.set(enabled, forKey: userDefaultsKey)
    }
}

// MARK: - AdSkipCuePlayer

/// Production cue player: an `AVAudioPlayer` on a private serial queue, plus
/// the one policy decision this type owns — what happens when two skips land on
/// top of each other.
///
/// `@unchecked Sendable` with the usual justification: every mutable field is
/// guarded by `lock`, and the `AVAudioPlayer` itself is confined to `queue`.
final class AdSkipCuePlayer: AdSkipCuePlaying, @unchecked Sendable {

    /// The process-wide cue. One transport, one cue player — and constructing
    /// it lazily keeps the audio object off `PlaybackService.init`, which is on
    /// the launch path (playhead-xul6 is the cautionary tale: one synchronous
    /// framework read there held the main actor for up to 2 s).
    static let shared: AdSkipCuePlayer = .system()

    private let lock = NSLock()
    /// Instant after which a new request is admitted again. `nil` means no cue
    /// is believed to be sounding.
    private var admitAgainAt: ContinuousClock.Instant?

    private let retriggerWindow: Duration
    private let now: @Sendable () -> ContinuousClock.Instant
    private let startSound: @Sendable () -> Void
    private let stopSound: @Sendable () -> Void

    /// Designated initialiser. Every collaborator is injected so the
    /// re-trigger policy can be driven deterministically without an audio
    /// device — the policy is the part that can be wrong, the `AVAudioPlayer`
    /// is the part the simulator cannot verify anyway.
    init(
        retriggerWindow: Duration = AdSkipCueSound.retriggerWindow,
        now: @escaping @Sendable () -> ContinuousClock.Instant = { ContinuousClock.now },
        startSound: @escaping @Sendable () -> Void,
        stopSound: @escaping @Sendable () -> Void
    ) {
        self.retriggerWindow = retriggerWindow
        self.now = now
        self.startSound = startSound
        self.stopSound = stopSound
    }

    /// Wire the real audio path. The `AVAudioPlayer` is built ON the serial
    /// queue, so this returns after two allocations and a `queue.async` — no
    /// decoding, no file I/O and no tone synthesis happens on the caller.
    static func system(sound: AdSkipCueSound = .resolve()) -> AdSkipCuePlayer {
        let queue = DispatchQueue(label: "com.playhead.ad-skip-cue", qos: .userInitiated)
        let box = AdSkipCueAudioBox()
        queue.async { box.load(sound) }
        return AdSkipCuePlayer(
            startSound: { queue.async { box.restart() } },
            stopSound: { queue.async { box.stop() } }
        )
    }

    // MARK: AdSkipCuePlaying

    /// Sound the cue unless one is already sounding.
    ///
    /// DROPPING the second request — rather than overlapping it or queueing it
    /// — is deliberate and is the mechanism half of "several skips land close
    /// together". Overlapping two copies of one short tone comb-filters its
    /// attack into something that is neither subdued nor recognisable;
    /// queueing smears the cue away from the seam it exists to mark, so it
    /// would announce a skip that had already finished. One acknowledgement per
    /// burst of adjacent cuts is the honest reading, and adjacent cuts are one
    /// break as far as the listener is concerned.
    ///
    /// The window is `AdSkipCueSound.retriggerWindow`, which is also the length
    /// bound on the asset — so it can only ever drop a request that WOULD have
    /// overlapped.
    func playAdSkipCue() {
        let instant = now()
        lock.lock()
        if let admitAgainAt, instant < admitAgainAt {
            lock.unlock()
            return
        }
        admitAgainAt = instant.advanced(by: retriggerWindow)
        lock.unlock()
        startSound()
    }

    func stopAdSkipCue() {
        lock.lock()
        admitAgainAt = nil
        lock.unlock()
        stopSound()
    }
}

// MARK: - AdSkipCueAudioBox

/// Holder for the `AVAudioPlayer`. Every method is called ONLY from
/// `AdSkipCuePlayer`'s serial queue, which is what makes the unguarded stored
/// property safe; it is `@unchecked Sendable` on that confinement alone.
private final class AdSkipCueAudioBox: @unchecked Sendable {
    private var player: AVAudioPlayer?

    func load(_ sound: AdSkipCueSound) {
        player = sound.makePreparedPlayer()
    }

    /// Rewind and play. `currentTime = 0` matters: `AVAudioPlayer.play()` on a
    /// player that has finished resumes from the end and sounds like nothing.
    func restart() {
        guard let player else { return }
        player.currentTime = 0
        player.play()
    }

    func stop() {
        player?.stop()
        player?.currentTime = 0
    }
}
