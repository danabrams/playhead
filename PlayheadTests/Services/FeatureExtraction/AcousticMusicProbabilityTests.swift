// AcousticMusicProbabilityTests.swift
// playhead-riiz: the acoustic music-playout discriminator.
//
// These tests exercise the sub-frame feature extractor and the composite
// probability on deterministic synthesized signals. They pin the whole point
// of playhead-riiz: the OLD spectral-flatness→tonalness formula SATURATED near
// 1.0 on all podcast audio (measured music 0.738 ≈ speech 0.722, AUC 0.576 =
// chance) and false-fired on music-under-speech. The NEW composite —
// pauseFraction + fine sub-spectral-flux + steadiness + tonalness + loudness —
// discriminates (AUC 0.997 at this 2 s window, recalibration verdict
// GO_PORT_AS_IS).
//
// All signals are 2.0 s @ 16 kHz (32 000 samples) — the pipeline's real,
// non-overlapping analysis window — so the 1024/512 sub-FFTs actually run
// (≈61 sub-frames). Randomness is seeded (a fixed LCG) so the tests are
// byte-deterministic and non-flaky.

import Foundation
import Testing
@testable import Playhead

@Suite("Acoustic music-playout discriminator (playhead-riiz)")
struct AcousticMusicProbabilityTests {

    private let sampleRate = 16_000.0
    private var windowSampleCount: Int { Int(2.0 * sampleRate) } // 32 000

    // MARK: - Deterministic signal synthesis

    /// Small deterministic LCG so "noise" is reproducible across runs/machines.
    private struct LCG {
        var state: UInt64
        mutating func nextSymmetric() -> Double {
            state = state &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
            // Top 53 bits → [0,1), then map to [-1,1).
            return (Double(state >> 11) / Double(1 << 53)) * 2.0 - 1.0
        }
    }

    private func silence() -> [Float] {
        [Float](repeating: 0, count: windowSampleCount)
    }

    /// Steady, loud, single-frequency tone: the canonical "sustained tonal
    /// play-out" — no pauses, stationary spectrum, constant amplitude.
    private func steadyTone(frequency: Double, amplitude: Double) -> [Float] {
        (0..<windowSampleCount).map { i in
            Float(amplitude * sin(2.0 * Double.pi * frequency * Double(i) / sampleRate))
        }
    }

    /// Steady, loud chord (a few stationary harmonics). Reads less like a
    /// synthetic sine than a single tone but is still stationary + tonal, so it
    /// is the "music-playout" positive.
    private func steadyChord(frequencies: [Double], amplitude: Double) -> [Float] {
        (0..<windowSampleCount).map { i in
            let t = Double(i) / sampleRate
            let sum = frequencies.reduce(0.0) { $0 + sin(2.0 * Double.pi * $1 * t) }
            return Float(amplitude * sum / Double(frequencies.count))
        }
    }

    /// Speech-like signal: broadband (noise-carried) energy, amplitude-modulated
    /// by a ~4 Hz syllabic envelope, with hard sub-second silent gaps. High
    /// sub-spectral-flux (broadband, decorrelated frame-to-frame) AND non-zero
    /// pauseFraction (the micro-gaps). Low tonalness. Starts inside a gap-ramp
    /// so the window is unambiguously speech-like from the first sub-frame.
    private func speechLikeAMNoise() -> [Float] {
        var rng = LCG(state: 0xA11CE_BEEF)
        var out = [Float](repeating: 0, count: windowSampleCount)
        for i in 0..<windowSampleCount {
            let t = Double(i) / sampleRate
            let syllable = 0.5 * (1.0 - cos(2.0 * Double.pi * 4.0 * t)) // 0…1 @ 4 Hz
            // Hard silent gap for 200 ms of every 500 ms.
            let phase = t.truncatingRemainder(dividingBy: 0.5)
            let voiced = phase < 0.3
            let envelope = voiced ? syllable : 0.0
            out[i] = Float(0.4 * envelope * rng.nextSymmetric())
        }
        return out
    }

    /// Music-UNDER-speech — the hard negative and the whole reason for the
    /// change. A loud tonal signal whose pitch WANDERS every 16 ms (a fast
    /// melodic run / speech formant transitions) over a light broadband floor,
    /// with 50 % silent gaps. The spectrum stays peaky (HIGH tonalness ≈ 0.9 —
    /// above the 0.88 knee that made the OLD flatness formula fire) yet
    /// consecutive sub-frames decorrelate (elevated sub-flux) and the gaps give
    /// a speech-like pauseFraction / RMS coefficient-of-variation. The composite
    /// must reject it because pause + flux + steadiness break the predicate
    /// despite the high tonalness.
    private func musicUnderSpeech() -> [Float] {
        var rng = LCG(state: 0xC0FFEE_D00D)
        var out = [Float](repeating: 0, count: windowSampleCount)
        let dwell = 256        // 16 ms pitch dwell (< the 512-sample sub-FFT hop)
        let pitches: [Double] = [262, 330, 392, 494, 587, 698, 440, 523, 349, 622]
        for i in 0..<windowSampleCount {
            let t = Double(i) / sampleRate
            // 50 % duty cycle: 250 ms voiced, 250 ms silent gap.
            let voiced = t.truncatingRemainder(dividingBy: 0.5) < 0.25
            guard voiced else { out[i] = 0; continue }
            let frequency = pitches[(i / dwell) % pitches.count]
            let tone = 0.3 * sin(2.0 * Double.pi * frequency * t)
            let floor = 0.05 * rng.nextSymmetric() // lift tonalness off 1.0
            out[i] = Float(tone + floor)
        }
        return out
    }

    // MARK: - Sub-frame feature correctness

    @Test("pure silence → pauseFraction 1.0, rmsCoV 0, and ≈61 sub-frames")
    func silenceFeatures() {
        let features = FeatureSignalExtraction.subFrameMusicFeatures(samples: silence())
        // 32 000 samples, 1024-wide sub-FFTs at 512 hop → starts 0…30720 → 61.
        #expect(features.subFrameCount == 61)
        #expect(features.pauseFraction == 1.0)
        #expect(features.rmsCoV == 0.0)
        #expect(features.subflux == 0.0)
    }

    @Test("steady loud tone → no pauses, low sub-flux, low rmsCoV")
    func steadyToneFeatures() {
        let features = FeatureSignalExtraction.subFrameMusicFeatures(
            samples: steadyTone(frequency: 440, amplitude: 0.3)
        )
        #expect(features.subFrameCount == 61)
        #expect(features.pauseFraction == 0.0)
        #expect(features.subflux < 0.15)  // stationary spectrum → ≈0
        #expect(features.rmsCoV < 0.05)   // constant amplitude → ≈0
    }

    @Test("speech-like AM/noise → higher sub-flux AND pauseFraction than steady music")
    func speechLikeFeaturesExceedMusic() {
        let speech = FeatureSignalExtraction.subFrameMusicFeatures(samples: speechLikeAMNoise())
        let music = FeatureSignalExtraction.subFrameMusicFeatures(
            samples: steadyTone(frequency: 440, amplitude: 0.3)
        )
        // Both discriminators move the right way relative to steady music. The
        // absolute floors are generous (a steady tone sits near ~0.03 sub-flux
        // and 0.0 pauseFraction) — the load-bearing claim is the ordering.
        #expect(speech.subflux > 0.15)
        #expect(speech.subflux > music.subflux)
        #expect(speech.pauseFraction > 0.20)
        #expect(speech.pauseFraction > music.pauseFraction)
    }

    @Test("music-under-speech is speech-like on pause/flux even though it is tonal")
    func musicUnderSpeechFeaturesAreSpeechLike() {
        let hard = FeatureSignalExtraction.subFrameMusicFeatures(samples: musicUnderSpeech())
        // Non-zero pauses (the gaps) and elevated flux (wandering pitch) are the
        // features that break the predicate despite a tonal, peaky spectrum.
        #expect(hard.pauseFraction > 0.25)
        #expect(hard.subflux > 0.20)  // ≈9× a steady tone's sub-flux
        #expect(hard.rmsCoV > 0.5)    // loud-voiced-vs-silence → high variation
    }

    @Test("a window too short for a sub-FFT yields empty features")
    func shortWindowYieldsEmptyFeatures() {
        let short = [Float](repeating: 0.2, count: 500) // < 1024
        let features = FeatureSignalExtraction.subFrameMusicFeatures(samples: short)
        #expect(features == .empty)
        #expect(features.subFrameCount == 0)
        // The composite has no pause/flux evidence → cannot claim music.
        #expect(FeatureSignalExtraction.acousticMusicProbability(windowSamples: short) == 0)
    }

    // MARK: - Composite discrimination (the fix)

    @Test("steady tonal play-out scores HIGH; speech-with-gaps scores LOW; Δ ≥ 0.3")
    func compositeSeparatesMusicFromSpeech() {
        let music = FeatureSignalExtraction.acousticMusicProbability(
            windowSamples: steadyChord(frequencies: [220, 330, 440], amplitude: 0.3)
        )
        let speech = FeatureSignalExtraction.acousticMusicProbability(
            windowSamples: speechLikeAMNoise()
        )

        // Music-playout must fire; speech must not; the gap must be wide. The
        // OLD formula gave these ≈EQUAL (music 0.738 ≈ speech 0.722, AUC 0.576);
        // this assertion is exactly the saturation that playhead-riiz fixes.
        #expect(music >= 0.76)
        #expect(speech < 0.5)
        #expect(music - speech >= 0.3)
    }

    @Test("music-UNDER-speech is NOT scored as music (the hard case)")
    func compositeRejectsMusicUnderSpeech() {
        // High window-level tonalness (the OLD flatness formula fired on this,
        // ~0.589 AUC = barely above chance) but pauseFraction + sub-flux break
        // the predicate. This is the single most important assertion in the
        // bead: a tonal carrier alone must not read as sustained play-out.
        let hard = FeatureSignalExtraction.acousticMusicProbability(
            windowSamples: musicUnderSpeech()
        )
        let music = FeatureSignalExtraction.acousticMusicProbability(
            windowSamples: steadyChord(frequencies: [220, 330, 440], amplitude: 0.3)
        )

        #expect(hard < 0.5)
        #expect(music - hard >= 0.3)
    }
}
