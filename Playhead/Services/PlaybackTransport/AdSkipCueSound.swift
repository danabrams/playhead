// AdSkipCueSound.swift
// playhead-nqwr: WHAT the ad-skip cue sounds like. This is THE ONE FILE to
// change when the real sound arrives — nothing else in the app knows what a
// cue is made of.
//
// ─────────────────────────────────────────────────────────────────────────────
// DROPPING IN THE REAL SOUND (Dan's call — the tone below is a PLACEHOLDER)
// ─────────────────────────────────────────────────────────────────────────────
//
//   1. Put the file at `Playhead/Resources/AdSkipCue.caf` (or `.wav`, `.aiff`,
//      `.aif`, `.m4a`, `.mp3` — the lookup tries them in that order).
//   2. `xcodegen generate` — `Playhead/Resources` is bundled by a recursive
//      walk in `project.yml`, so no target edit is needed.
//   3. That is all. `AdSkipCueSound.resolve()` prefers a bundled asset over the
//      synthesised placeholder, so the tone below stops being reachable the
//      moment the file exists. No code change, no constant to flip.
//
//   FORMAT the asset must satisfy, and why each bound exists:
//     * MONO. The cue is positional information about the app, not about the
//       show; a stereo cue would move in the image while the episode does not.
//     * 44.1 kHz or 48 kHz, 16-bit or 24-bit PCM (`.caf`/`.wav`/`.aiff`), or
//       AAC in `.m4a`. Anything `AVAudioPlayer` decodes will play; PCM is
//       preferred because it needs no decoder warm-up at the seam.
//     * **≤ 600 ms** (`AdSkipCueSound.retriggerWindow`). This is the only HARD
//       bound: two skips landing closer together than the cue is long would
//       overlap, and `AdSkipCuePlayer` drops the second request for exactly
//       this window. A longer asset means a cue can talk over itself; if the
//       sound Dan picks needs longer, raise `retriggerWindow` with it.
//     * Peak around −18 dBFS. It plays UNDER an episode that has just been
//       ducked to 0.15 and is about to come back to full — it is an
//       acknowledgement, not an alert.
//     * Fade the tail to digital silence. A hard truncation reads as a glitch,
//       which is the exact experience this cue exists to distinguish itself
//       from.
//
// ─────────────────────────────────────────────────────────────────────────────
// WHAT THIS FILE MAY NOT DO
// ─────────────────────────────────────────────────────────────────────────────
//
// It must never mention `AVAudioSession`. The app owns one playback session
// configured `.playback` / `.spokenAudio` / `.longFormAudio` in
// `PlaybackService.configureAudioSession`, and a cue that reconfigures or
// re-activates it can duck, interrupt or reroute the episode at the one moment
// the listener is paying attention. A cue that wedges the session is strictly
// worse than the silence it replaces. `AdSkipCueSourceCanaryTests` enforces the
// absence, because on the simulator a session mistake is invisible at runtime.

import AVFoundation
import Foundation

// MARK: - AdSkipCueSound

/// The material an ad-skip cue is played from: either a bundled asset, or the
/// synthesised placeholder that ships until one exists.
struct AdSkipCueSound: Sendable, Equatable {

    enum Source: Sendable, Equatable {
        /// A real sound file found in the app bundle.
        case bundledAsset(URL)
        /// The synthesised stand-in. Deliberately plain: it is meant to be
        /// replaced, and a placeholder anyone could mistake for a finished
        /// design is a placeholder nobody replaces.
        case placeholderTone
    }

    let source: Source

    // MARK: Bundle lookup

    /// Base name of the bundled asset. Kept as a constant rather than inlined
    /// so the canary can pin the spelling the instructions above promise.
    static let resourceBaseName = "AdSkipCue"

    /// Extensions tried, in order. PCM containers first: they need no decoder
    /// warm-up, and the cue is started from a seam that has already spent
    /// 150 ms ducked.
    static let resourceExtensions = ["caf", "wav", "aiff", "aif", "m4a", "mp3"]

    /// How long one cue may occupy the channel. Two purposes, and they are the
    /// same number on purpose: it is the maximum length a dropped-in asset may
    /// be, and it is the window `AdSkipCuePlayer` refuses a second request in.
    ///
    /// A cue that outlives this window can overlap itself when two skips land
    /// close together, which comb-filters the attack into something that is
    /// neither subdued nor recognisable.
    static let retriggerWindow: Duration = .milliseconds(600)

    /// Playback level for the cue, independent of the episode's own volume
    /// (which the skip transition ducks to `PlaybackService.duckVolume`). The
    /// cue is NOT ducked with the episode — that is the point of it being a
    /// separate player — so it is authored quiet instead.
    static let level: Float = 0.35

    /// The bundled asset, if the real sound has been dropped in.
    static func bundledAssetURL(in bundle: Bundle = .main) -> URL? {
        for ext in resourceExtensions {
            if let url = bundle.url(forResource: resourceBaseName, withExtension: ext) {
                return url
            }
        }
        return nil
    }

    /// Prefer a bundled asset; fall back to the placeholder.
    static func resolve(in bundle: Bundle = .main) -> AdSkipCueSound {
        if let url = bundledAssetURL(in: bundle) {
            return AdSkipCueSound(source: .bundledAsset(url))
        }
        return AdSkipCueSound(source: .placeholderTone)
    }

    // MARK: Placeholder synthesis

    /// Placeholder length. Comfortably inside `retriggerWindow`.
    static let placeholderDurationSeconds: Double = 0.45

    /// Placeholder partials: a fundamental and its perfect fifth (3:2). A
    /// consonant interval decaying like a struck bar reads as punctuation
    /// rather than as a notification, which is the only quality this stand-in
    /// is trying to get approximately right.
    static let placeholderFundamentalHz: Double = 784.0
    private static let placeholderPartialRatio: Double = 1.5
    private static let placeholderPartialLevel: Double = 0.35
    private static let placeholderPeak: Double = 0.22
    private static let placeholderDecaySeconds: Double = 0.13
    private static let placeholderAttackSeconds: Double = 0.006
    /// Linear release over the tail so the last sample is digital silence.
    /// An exponential decay alone still leaves a step at the buffer's end —
    /// measured at 112/32767 for these constants — and a step is a click,
    /// which is exactly the "glitch" reading the cue exists to be the opposite
    /// of. The asset spec in the header asks a dropped-in sound for the same
    /// thing, so the placeholder had better do it too.
    private static let placeholderReleaseSeconds: Double = 0.02
    static let placeholderSampleRate: Int = 44_100

    /// A complete little-endian 16-bit mono PCM WAV file for the placeholder.
    ///
    /// Synthesised rather than shipped as bytes so the repo carries no binary
    /// nobody chose, and so the thing that gets deleted when the real asset
    /// lands is a function rather than a file somebody has to remember to
    /// remove.
    static func placeholderToneWAVData() -> Data {
        let sampleRate = Double(placeholderSampleRate)
        let frameCount = Int(sampleRate * placeholderDurationSeconds)
        var samples: [Int16] = []
        samples.reserveCapacity(frameCount)
        let normalisation = 1.0 + placeholderPartialLevel
        for frame in 0..<frameCount {
            let seconds = Double(frame) / sampleRate
            let attack = min(1.0, seconds / placeholderAttackSeconds)
            let decay = exp(-seconds / placeholderDecaySeconds)
            let fundamental = sin(2.0 * .pi * placeholderFundamentalHz * seconds)
            let fifth = sin(
                2.0 * .pi * placeholderFundamentalHz * placeholderPartialRatio * seconds
            )
            let mixed = (fundamental + placeholderPartialLevel * fifth) / normalisation
            let remaining = placeholderDurationSeconds - seconds
            let release = min(1.0, max(0.0, remaining / placeholderReleaseSeconds))
            let value = placeholderPeak * attack * decay * release * mixed
            samples.append(Int16(clamping: Int((value * 32_767.0).rounded())))
        }
        return wavData(samples: samples, sampleRate: placeholderSampleRate)
    }

    /// Minimal canonical RIFF/WAVE writer: 16-bit signed little-endian PCM,
    /// one channel. Only ever fed the placeholder above.
    static func wavData(samples: [Int16], sampleRate: Int) -> Data {
        let bitsPerSample = 16
        let channels = 1
        let blockAlign = channels * bitsPerSample / 8
        let byteRate = sampleRate * blockAlign
        let dataByteCount = samples.count * blockAlign

        var data = Data()
        data.reserveCapacity(44 + dataByteCount)

        func appendASCII(_ text: String) {
            data.append(contentsOf: Array(text.utf8))
        }
        func appendUInt32(_ value: UInt32) {
            withUnsafeBytes(of: value.littleEndian) { data.append(contentsOf: $0) }
        }
        func appendUInt16(_ value: UInt16) {
            withUnsafeBytes(of: value.littleEndian) { data.append(contentsOf: $0) }
        }

        appendASCII("RIFF")
        appendUInt32(UInt32(36 + dataByteCount))
        appendASCII("WAVE")
        appendASCII("fmt ")
        appendUInt32(16)
        appendUInt16(1)                       // PCM
        appendUInt16(UInt16(channels))
        appendUInt32(UInt32(sampleRate))
        appendUInt32(UInt32(byteRate))
        appendUInt16(UInt16(blockAlign))
        appendUInt16(UInt16(bitsPerSample))
        appendASCII("data")
        appendUInt32(UInt32(dataByteCount))
        for sample in samples {
            withUnsafeBytes(of: sample.littleEndian) { data.append(contentsOf: $0) }
        }
        return data
    }

    /// Build a prepared `AVAudioPlayer` for this sound, or `nil` if the asset
    /// cannot be decoded (a corrupt drop-in must degrade to silence, never to a
    /// crash or a session change).
    ///
    /// Called only from `AdSkipCuePlayer`'s private serial queue.
    func makePreparedPlayer() -> AVAudioPlayer? {
        let player: AVAudioPlayer?
        switch source {
        case .bundledAsset(let url):
            player = try? AVAudioPlayer(contentsOf: url)
        case .placeholderTone:
            player = try? AVAudioPlayer(data: Self.placeholderToneWAVData())
        }
        player?.volume = Self.level
        player?.numberOfLoops = 0
        player?.prepareToPlay()
        return player
    }
}
