// SyntheticFixtureGenerator.swift
// Byte-deterministic synthetic audio generator for the playhead-ym57
// locked-core 8 fixture placeholders. Produces .wav files (canonical 16-bit
// little-endian PCM, mono, 16 kHz) whose exact bytes are a pure function of
// (slot, durationSamples, frequencyHz, amplitude).
//
// Rationale:
//   * Real podcast audio requires legal sign-off; see FIXTURES_LICENSING.md.
//   * Synthetic fixtures let the device-lab substrate ship TODAY while
//     preserving every integrity-gate property: files exist, SHA-256 matches,
//     taxonomy slots are filled, rotation tooling works end-to-end.
//   * WAV + 16-bit PCM is used instead of .m4a/.caf because generating it
//     requires no encoder dependency (pure-Swift byte layout) so the output
//     is stable across Xcode/SDK versions.
//
// This module is compiled into the test target and is also usable by the
// regenerate-fixtures shell helper via `swift run` (see scripts/).

import Foundation

// MARK: - Synthetic Fixture Generator

enum SyntheticFixtureGenerator {

    /// Spec for one synthetic fixture. `slot` drives the sine-wave frequency so
    /// that each slot has a distinct byte signature; `durationSamples` keeps
    /// placeholder files small (default ~0.5 s * 16 kHz = 8000 samples).
    struct Spec: Sendable, Equatable {
        let slot: Int
        let durationSamples: Int
        let sampleRate: Int
        let amplitude: Int16

        static let defaultSampleRate = 16_000
        static let defaultDurationSamples = 8_000      // 0.5s @ 16 kHz
        static let defaultAmplitude: Int16 = 4_000     // -18 dBFS-ish

        init(
            slot: Int,
            durationSamples: Int = Self.defaultDurationSamples,
            sampleRate: Int = Self.defaultSampleRate,
            amplitude: Int16 = Self.defaultAmplitude
        ) {
            self.slot = slot
            self.durationSamples = durationSamples
            self.sampleRate = sampleRate
            self.amplitude = amplitude
        }

        /// Sine-wave frequency derived from slot. Each slot gets a distinct
        /// pitch so byte signatures diverge even if slots share other params.
        /// 220 Hz A3 + 55 Hz * (slot-1). Slots 1..8 yield 220..605 Hz.
        var frequencyHz: Double { 220.0 + 55.0 * Double(slot - 1) }
    }

    /// Produce the canonical .wav bytes for a given spec. Byte-deterministic.
    static func makeWAV(spec: Spec) -> Data {
        let samples = sineSamples(
            count: spec.durationSamples,
            frequencyHz: spec.frequencyHz,
            sampleRate: spec.sampleRate,
            amplitude: spec.amplitude
        )
        return encodeWAV(samples: samples, sampleRate: spec.sampleRate)
    }

    // MARK: - Sample generation

    /// Generate `count` samples of a sine wave. Uses `Double` for the phase
    /// accumulator and rounds via truncation to `Int16` at the end. Results
    /// are identical across Darwin/Linux Swift (same libm sin on IEEE 754).
    static func sineSamples(
        count: Int,
        frequencyHz: Double,
        sampleRate: Int,
        amplitude: Int16
    ) -> [Int16] {
        var samples = [Int16](repeating: 0, count: count)
        let twoPi = 2.0 * Double.pi
        let step = frequencyHz / Double(sampleRate)
        let amp = Double(amplitude)
        for i in 0..<count {
            let phase = twoPi * step * Double(i)
            let v = sin(phase) * amp
            // Round to nearest and clamp to Int16 range.
            let rounded = (v >= 0) ? Int(v + 0.5) : Int(v - 0.5)
            let clamped = max(Int(Int16.min), min(Int(Int16.max), rounded))
            samples[i] = Int16(clamped)
        }
        return samples
    }

    // MARK: - WAV encoder (RIFF / PCM / 16-bit / mono)

    /// Encode 16-bit PCM mono samples into a canonical WAV file. Byte layout
    /// follows the standard 44-byte RIFF header + samples. All multi-byte
    /// fields are little-endian.
    static func encodeWAV(samples: [Int16], sampleRate: Int) -> Data {
        let numChannels: UInt16 = 1
        let bitsPerSample: UInt16 = 16
        let byteRate = UInt32(sampleRate) * UInt32(numChannels) * UInt32(bitsPerSample / 8)
        let blockAlign: UInt16 = numChannels * (bitsPerSample / 8)
        let dataSize = UInt32(samples.count) * UInt32(blockAlign)
        let chunkSize = 36 + dataSize

        var data = Data()
        data.reserveCapacity(44 + Int(dataSize))

        // RIFF header
        data.append(contentsOf: [0x52, 0x49, 0x46, 0x46]) // "RIFF"
        data.appendLE(uint32: chunkSize)
        data.append(contentsOf: [0x57, 0x41, 0x56, 0x45]) // "WAVE"

        // fmt  subchunk
        data.append(contentsOf: [0x66, 0x6D, 0x74, 0x20]) // "fmt "
        data.appendLE(uint32: 16)                          // PCM subchunk size
        data.appendLE(uint16: 1)                           // AudioFormat = 1 (PCM)
        data.appendLE(uint16: numChannels)
        data.appendLE(uint32: UInt32(sampleRate))
        data.appendLE(uint32: byteRate)
        data.appendLE(uint16: blockAlign)
        data.appendLE(uint16: bitsPerSample)

        // data subchunk
        data.append(contentsOf: [0x64, 0x61, 0x74, 0x61]) // "data"
        data.appendLE(uint32: dataSize)
        for s in samples {
            data.appendLE(int16: s)
        }

        return data
    }
}

// MARK: - Little-endian helpers

private extension Data {

    mutating func appendLE(uint16 v: UInt16) {
        append(UInt8(v & 0xFF))
        append(UInt8((v >> 8) & 0xFF))
    }

    mutating func appendLE(int16 v: Int16) {
        appendLE(uint16: UInt16(bitPattern: v))
    }

    mutating func appendLE(uint32 v: UInt32) {
        append(UInt8(v & 0xFF))
        append(UInt8((v >> 8) & 0xFF))
        append(UInt8((v >> 16) & 0xFF))
        append(UInt8((v >> 24) & 0xFF))
    }
}
