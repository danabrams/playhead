// Streaming-decode regression tests for playhead-s8dq.
//
// Covers the bead's acceptance criteria:
//   - peak source buffer stays bounded (<<50 MB) regardless of asset duration
//   - decoded sample count matches expected duration × 16 kHz within tolerance
//   - the actor's instrumentation seam reports plausible values

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisAudio — streaming-convert (playhead-s8dq)")
struct AnalysisAudioStreamingTests {

    /// Writes a synthetic mono 32-bit float WAV to a temp file. AVAudioFile
    /// only supports caf/wav/aif natively for non-interleaved Float32, so
    /// we use .caf to dodge container quirks.
    private func writeSynthFile(
        seconds: TimeInterval,
        sampleRate: Double = 44_100,
        frequency: Double = 440
    ) throws -> URL {
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory())
        let fileURL = tempDir.appendingPathComponent("s8dq-\(UUID().uuidString).caf")

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sampleRate,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "test", code: -1)
        }

        let file = try AVAudioFile(
            forWriting: fileURL,
            settings: format.settings,
            commonFormat: .pcmFormatFloat32,
            interleaved: false
        )

        // Write in 1 s chunks to keep test memory bounded too.
        let chunkFrames = AVAudioFrameCount(sampleRate)
        var totalFrames = AVAudioFramePosition(0)
        let totalNeeded = AVAudioFramePosition(seconds * sampleRate)

        while totalFrames < totalNeeded {
            let remaining = AVAudioFrameCount(totalNeeded - totalFrames)
            let frames = min(chunkFrames, remaining)
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "test", code: -2)
            }
            buffer.frameLength = frames

            let channel = buffer.floatChannelData![0]
            let phaseStep = 2.0 * .pi * frequency / sampleRate
            for i in 0..<Int(frames) {
                let phase = phaseStep * Double(Int(totalFrames) + i)
                channel[i] = Float(sin(phase) * 0.25)
            }

            try file.write(from: buffer)
            totalFrames += AVAudioFramePosition(frames)
        }

        return fileURL
    }

    @Test("60s synth file: peak source buffer stays under ceiling, output sample count matches duration")
    func bounded_peak_for_one_minute_synth() async throws {
        let assetSeconds: TimeInterval = 60
        let url = try writeSynthFile(seconds: assetSeconds)
        defer { try? FileManager.default.removeItem(at: url) }

        guard let local = LocalAudioURL(url) else {
            Issue.record("Could not wrap test fixture in LocalAudioURL")
            return
        }
        let service = AnalysisAudioService()
        let episodeID = "s8dq-test-\(UUID().uuidString)"
        let shards = try await service.decode(
            fileURL: local,
            episodeID: episodeID,
            shardDuration: 30
        )

        // Sanity: shards cover the asset.
        let totalSamples = shards.reduce(0) { $0 + $1.sampleCount }
        let expected = Int(assetSeconds * AnalysisAudioService.targetSampleRate)
        // AVAudioConverter may drop or pad up to a few hundred frames at the
        // tail due to FIR-tap ramp-up; tolerate ±0.5%.
        let tol = expected / 200
        #expect(abs(totalSamples - expected) <= tol,
                "decoded samples=\(totalSamples) expected=\(expected) (±\(tol))")

        // Peak in-flight source buffer must be far below the 50 MB ceiling.
        // For a 60s 44.1kHz mono Float32 file, one CMSampleBuffer is typically
        // 4096–8192 frames (~16–32 KB). We assert a generous 1 MB upper bound
        // to leave room for codec quirks while still proving streaming works.
        let peak = await service.peakSourceBufferBytes
        #expect(peak < 1 * 1024 * 1024,
                "peak source buffer = \(peak) bytes (must be < 1 MB to prove streaming)")
        #expect(peak > 0, "peak source buffer should be > 0 if any decode occurred")

        // cumulativeOutputSamples mirrors what we counted in shards.
        let cumulative = await service.cumulativeOutputSamples
        #expect(cumulative == totalSamples,
                "cumulativeOutputSamples=\(cumulative) shardTotal=\(totalSamples)")

        // Evict to keep the temp ShardCache from polluting other test runs.
        await service.evictCache(episodeID: episodeID)
    }

    @Test("Tail short shard preserved: a 75s file produces final shard < 30s without dropping samples")
    func tail_shard_preserved() async throws {
        let assetSeconds: TimeInterval = 75
        let url = try writeSynthFile(seconds: assetSeconds)
        defer { try? FileManager.default.removeItem(at: url) }

        guard let local = LocalAudioURL(url) else {
            Issue.record("Could not wrap test fixture")
            return
        }
        let service = AnalysisAudioService()
        let episodeID = "s8dq-tail-\(UUID().uuidString)"
        let shards = try await service.decode(
            fileURL: local,
            episodeID: episodeID,
            shardDuration: 30
        )

        // 30 + 30 + 15 = 75
        #expect(shards.count == 3, "expected 3 shards for 75s @30s, got \(shards.count)")
        if let last = shards.last {
            // Last shard ~15s ⇒ ~240k samples (with codec slack).
            #expect(last.sampleCount > 200_000 && last.sampleCount < 260_000,
                    "tail shard sampleCount = \(last.sampleCount), expected ~240k")
        }

        await service.evictCache(episodeID: episodeID)
    }
}
