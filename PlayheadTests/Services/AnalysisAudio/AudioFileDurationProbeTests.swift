// AudioFileDurationProbeTests.swift
// playhead-gyvb.2: pin the cheap container-metadata duration probe used as
// the source of truth for `analysis_assets.episodeDurationSec` once a
// download lands. The probe must:
//
//   - return a finite, positive duration (within tolerance) for a real audio
//     file, matching what `afinfo` would report
//   - return `nil` (not throw) on a non-audio file
//   - return `nil` for a missing file or a non-file URL
//
// These tests fail on `main` because the probe helper does not yet exist.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("AudioFileDurationProbe (playhead-gyvb.2)")
struct AudioFileDurationProbeTests {

    /// Writes a synthetic mono 32-bit float CAF of the requested duration.
    /// CAF avoids container quirks (matches the AnalysisAudioStreamingTests
    /// pattern); AVURLAsset reads container metadata for `.duration` without
    /// decoding the payload.
    private func writeSynthAudio(
        seconds: TimeInterval,
        sampleRate: Double = 44_100
    ) throws -> URL {
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory())
        let fileURL = tempDir.appendingPathComponent("gyvb2-\(UUID().uuidString).caf")

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sampleRate,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "AudioFileDurationProbeTests", code: -1)
        }

        let file = try AVAudioFile(
            forWriting: fileURL,
            settings: format.settings,
            commonFormat: .pcmFormatFloat32,
            interleaved: false
        )

        let totalFrames = AVAudioFramePosition(seconds * sampleRate)
        let chunkFrames = AVAudioFrameCount(sampleRate)
        var written = AVAudioFramePosition(0)
        while written < totalFrames {
            let remaining = AVAudioFrameCount(totalFrames - written)
            let frames = min(chunkFrames, remaining)
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "AudioFileDurationProbeTests", code: -2)
            }
            buffer.frameLength = frames
            // zeros are fine — duration probe ignores payload
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }

        return fileURL
    }

    @Test("returns finite duration matching synthetic-fixture length within 0.5 s")
    func realAudioFileReturnsFiniteDuration() async throws {
        let url = try writeSynthAudio(seconds: 12.5)
        defer { try? FileManager.default.removeItem(at: url) }

        let probed = await AudioFileDurationProbe.probeDuration(at: url)
        try #require(probed != nil, "probe should return a value for a valid audio file")
        let actual = probed!
        #expect(abs(actual - 12.5) < 0.5,
                "probed duration \(actual) should be within 0.5s of expected 12.5s")
    }

    @Test("returns nil for a non-audio file (no throw)")
    func nonAudioFileReturnsNil() async throws {
        let dir = try makeTempDir(prefix: "AudioFileDurationProbe-NonAudio")
        let fileURL = dir.appendingPathComponent("not-audio.bin")
        try Data("this is not an audio file, just some bytes".utf8).write(to: fileURL)

        let probed = await AudioFileDurationProbe.probeDuration(at: fileURL)
        #expect(probed == nil, "non-audio file should return nil, not throw")
    }

    @Test("returns nil for a missing file")
    func missingFileReturnsNil() async throws {
        let dir = try makeTempDir(prefix: "AudioFileDurationProbe-Missing")
        let fileURL = dir.appendingPathComponent("does-not-exist.mp3")

        let probed = await AudioFileDurationProbe.probeDuration(at: fileURL)
        #expect(probed == nil)
    }

    @Test("returns nil for a non-file (remote) URL")
    func remoteURLReturnsNil() async throws {
        let url = URL(string: "https://example.com/audio.mp3")!
        let probed = await AudioFileDurationProbe.probeDuration(at: url)
        #expect(probed == nil, "remote URL must not be probed")
    }

    @Test("returns nil for an empty file")
    func emptyFileReturnsNil() async throws {
        let dir = try makeTempDir(prefix: "AudioFileDurationProbe-Empty")
        let fileURL = dir.appendingPathComponent("empty.mp3")
        try Data().write(to: fileURL)

        let probed = await AudioFileDurationProbe.probeDuration(at: fileURL)
        #expect(probed == nil)
    }
}
