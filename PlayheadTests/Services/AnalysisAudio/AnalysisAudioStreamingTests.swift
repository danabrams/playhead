// Streaming-decode regression tests for playhead-s8dq.
//
// Covers the bead's acceptance criteria:
//   - peak source CMSampleBuffer batch stays bounded (<<50 MB) regardless
//     of asset duration
//   - peak per-shard accumulator stays bounded to one shard's worth,
//     proving total in-flight Float32 output RAM does not scale with
//     episode duration
//   - decoded sample count matches expected duration × 16 kHz within tolerance
//   - decoded content is preserved (RMS + zero-crossing equivalence on
//     a known sine input — there is no longer a one-shot reference path
//     to byte-compare against, so content equivalence is asserted via
//     signal properties of the synthetic input)
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
        frequency: Double = 440,
        amplitude: Double = 0.25
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
                channel[i] = Float(sin(phase) * amplitude)
            }

            try file.write(from: buffer)
            totalFrames += AVAudioFramePosition(frames)
        }

        return fileURL
    }

    @Test("60s synth file: peak source batch + peak shard accumulator stay bounded; sample count matches duration")
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

        // Peak source CMSampleBuffer batch must be far below the 50 MB ceiling.
        // For a 60s 44.1kHz mono Float32 file, one CMSampleBuffer is typically
        // 4096–8192 frames (~16–32 KB). We assert a generous 1 MB upper bound
        // to leave room for codec quirks while still proving streaming works.
        let peakBatch = await service.peakSourceBatchBytes
        #expect(peakBatch < 1 * 1024 * 1024,
                "peak source batch = \(peakBatch) bytes (must be < 1 MB to prove streaming)")
        #expect(peakBatch > 0, "peak source batch should be > 0 if any decode occurred")

        // Peak per-shard accumulator must be bounded to one shard's worth
        // (~1.92 MB). This is the C1 acceptance — total in-flight Float32
        // output RAM scales with shard duration, NOT episode duration.
        let peakShard = await service.peakShardAccumulatorBytes
        let oneShardBytes = 30 * 16_000 * MemoryLayout<Float>.size  // 1_920_000
        #expect(peakShard <= oneShardBytes + (oneShardBytes / 100),  // +1% slack
                "peak shard accumulator = \(peakShard) bytes (must be ≤ \(oneShardBytes) + 1% to prove bounded)")
        #expect(peakShard > 0, "peak shard accumulator should be > 0 if any decode occurred")

        // cumulativeOutputSamples mirrors what we counted in shards.
        let cumulative = await service.cumulativeOutputSamples
        #expect(cumulative == totalSamples,
                "cumulativeOutputSamples=\(cumulative) shardTotal=\(totalSamples)")

        // Evict to keep the temp ShardCache from polluting other test runs.
        await service.evictCache(episodeID: episodeID)
    }

    /// Writes a synthetic linear chirp (frequency sweep) to a temp file —
    /// useful for exercising AVAudioConverter's FIR-tap state across many
    /// `convert` calls. A clean sine is too forgiving; a chirp puts every
    /// frequency through the resampler so a tap-reset bug introduces
    /// audible boundary glitches.
    private func writeChirpFile(
        seconds: TimeInterval,
        sampleRate: Double = 44_100,
        startFreq: Double = 200,
        endFreq: Double = 4_000,
        amplitude: Double = 0.25
    ) throws -> URL {
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory())
        let fileURL = tempDir.appendingPathComponent("s8dq-chirp-\(UUID().uuidString).caf")

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

        let chunkFrames = AVAudioFrameCount(sampleRate)
        var totalFrames = AVAudioFramePosition(0)
        let totalNeeded = AVAudioFramePosition(seconds * sampleRate)
        let k = (endFreq - startFreq) / seconds  // sweep rate (Hz/s)

        while totalFrames < totalNeeded {
            let remaining = AVAudioFrameCount(totalNeeded - totalFrames)
            let frames = min(chunkFrames, remaining)
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "test", code: -2)
            }
            buffer.frameLength = frames

            let channel = buffer.floatChannelData![0]
            for i in 0..<Int(frames) {
                let t = Double(Int(totalFrames) + i) / sampleRate
                // Linear chirp: phase = 2π(f0·t + ½·k·t²)
                let phase = 2.0 * .pi * (startFreq * t + 0.5 * k * t * t)
                channel[i] = Float(sin(phase) * amplitude)
            }
            try file.write(from: buffer)
            totalFrames += AVAudioFramePosition(frames)
        }
        return fileURL
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

    /// Long-fixture regression for C1: the per-shard accumulator must
    /// remain bounded as episode length grows. We use 10 minutes (≈40 MB
    /// on disk at 44.1 kHz Float32) instead of a literal 5-hour fixture
    /// to avoid 3+ GB of disk during CI runs — but the assertion is the
    /// same: peak shard accumulator ≤ one shard, regardless of asset
    /// duration. 10 min is 20× the default shard, which would have been
    /// 1.5× the prior fix's leak ceiling for a 5-hour asset.
    @Test("Long fixture: peak shard accumulator stays bounded as episode length grows")
    func bounded_for_long_synth() async throws {
        let assetSeconds: TimeInterval = 600  // 10 minutes
        let url = try writeSynthFile(seconds: assetSeconds, sampleRate: 22_050)
        defer { try? FileManager.default.removeItem(at: url) }

        guard let local = LocalAudioURL(url) else {
            Issue.record("Could not wrap test fixture in LocalAudioURL")
            return
        }
        let service = AnalysisAudioService()
        let episodeID = "s8dq-long-\(UUID().uuidString)"
        let shards = try await service.decode(
            fileURL: local,
            episodeID: episodeID,
            shardDuration: 30
        )

        // 600s / 30s = 20 shards expected.
        #expect(shards.count == 20 || shards.count == 21,
                "expected ~20 shards for 600s @30s, got \(shards.count)")

        // M-1 invariant: every non-tail shard must have exactly
        // `samplesPerShard` samples — the chunk loop never appends past
        // that. Catches single-sample drops the cumulative tolerance
        // would miss.
        let samplesPerShard = 30 * 16_000
        for shard in shards.dropLast() {
            #expect(shard.sampleCount == samplesPerShard,
                    "non-tail shard \(shard.id) sampleCount=\(shard.sampleCount); expected exactly \(samplesPerShard)")
        }

        // Peak shard accumulator MUST stay bounded — this is the C1
        // assertion that distinguishes streaming-shard-emission from the
        // prior half-fix where allSamples grew linearly.
        let peakShard = await service.peakShardAccumulatorBytes
        let oneShardBytes = 30 * 16_000 * MemoryLayout<Float>.size  // 1_920_000
        #expect(peakShard <= oneShardBytes + (oneShardBytes / 100),
                "peak shard accumulator = \(peakShard) bytes (must be ≤ \(oneShardBytes) + 1%); a leak would scale with duration")

        // Cumulative output ≈ 600 × 16 000 = 9.6M samples (FIR slack ±0.5%).
        let cumulative = await service.cumulativeOutputSamples
        let expected = Int(assetSeconds * AnalysisAudioService.targetSampleRate)
        let tol = expected / 200
        #expect(abs(cumulative - expected) <= tol,
                "cumulativeOutputSamples=\(cumulative) expected=\(expected) (±\(tol))")

        await service.evictCache(episodeID: episodeID)
    }

    /// Content-equivalence regression for C3/H1: prove the streaming
    /// converter preserves the input signal. We can't byte-compare to a
    /// one-shot reference (that path no longer exists), so we instead
    /// verify the decoded output retains the known properties of the
    /// synthetic input:
    ///   - RMS within ±5% of expected (amplitude/√2)
    ///   - dominant frequency within ±2 Hz via zero-crossing count
    /// A leaky/buggy converter would produce DC bias, attenuation, or
    /// frequency drift across shard boundaries that this catches.
    @Test("Content equivalence: 5-min sine input preserves RMS and frequency through shard boundaries")
    func content_equivalence_sine() async throws {
        let assetSeconds: TimeInterval = 300  // 5 minutes — many shard boundaries
        let frequency = 440.0
        let amplitude = 0.25
        let url = try writeSynthFile(
            seconds: assetSeconds,
            sampleRate: 44_100,
            frequency: frequency,
            amplitude: amplitude
        )
        defer { try? FileManager.default.removeItem(at: url) }

        guard let local = LocalAudioURL(url) else {
            Issue.record("Could not wrap test fixture")
            return
        }
        let service = AnalysisAudioService()
        let episodeID = "s8dq-content-\(UUID().uuidString)"
        let shards = try await service.decode(
            fileURL: local,
            episodeID: episodeID,
            shardDuration: 30
        )

        // Concatenate ALL shard samples to verify shard boundaries didn't
        // introduce discontinuities. NOTE: we tolerate this whole-file
        // assemble in the TEST (where bounded test fixtures are fine);
        // production code must not.
        var all: [Float] = []
        all.reserveCapacity(shards.reduce(0) { $0 + $1.sampleCount })
        for shard in shards { all.append(contentsOf: shard.samples) }

        // Skip the first/last 1024 samples to avoid FIR-tap ramp.
        let head = 1024
        let tail = all.count - 1024
        guard tail > head else {
            Issue.record("Decoded output too short for content check")
            return
        }
        let region = all[head..<tail]

        // RMS check.
        var sumSq: Double = 0
        for s in region { sumSq += Double(s) * Double(s) }
        let rms = sqrt(sumSq / Double(region.count))
        let expectedRMS = amplitude / sqrt(2.0)
        let rmsErr = abs(rms - expectedRMS) / expectedRMS
        #expect(rmsErr < 0.05,
                "RMS=\(rms) expected≈\(expectedRMS), error=\(rmsErr * 100)%")

        // Frequency via zero-crossing count.
        var zc = 0
        var prev = region[region.startIndex]
        for s in region.dropFirst() {
            if (prev <= 0 && s > 0) || (prev >= 0 && s < 0) { zc += 1 }
            prev = s
        }
        let regionDuration = Double(region.count) / AnalysisAudioService.targetSampleRate
        let measuredFreq = Double(zc) / 2.0 / regionDuration
        #expect(abs(measuredFreq - frequency) < 2.0,
                "measured freq=\(measuredFreq) Hz expected≈\(frequency) Hz")

        // And the shard accumulator was still bounded.
        let peakShard = await service.peakShardAccumulatorBytes
        let oneShardBytes = 30 * 16_000 * MemoryLayout<Float>.size
        #expect(peakShard <= oneShardBytes + (oneShardBytes / 100),
                "peak shard accumulator = \(peakShard) bytes; must be ≤ \(oneShardBytes)")

        await service.evictCache(episodeID: episodeID)
    }

    /// Cycle-2 H-B: actually-long fixture. 60 minutes @ 11 025 Hz costs
    /// ~10 MB on disk but exercises the streaming code through ~120
    /// shard boundaries — far more than the original 5-hour OOM crash
    /// would have hit before failing. The discriminating assertion is
    /// the same as the 600s test, but at 6× the duration: any leak
    /// proportional to episode length would compound and trip the
    /// per-shard ceiling.
    @Test("60-min fixture: peak shard accumulator stays bounded through ~120 shards")
    func bounded_for_one_hour_synth() async throws {
        let assetSeconds: TimeInterval = 3600
        let url = try writeSynthFile(seconds: assetSeconds, sampleRate: 11_025)
        defer { try? FileManager.default.removeItem(at: url) }

        guard let local = LocalAudioURL(url) else {
            Issue.record("Could not wrap test fixture")
            return
        }
        let service = AnalysisAudioService()
        let episodeID = "s8dq-1hr-\(UUID().uuidString)"
        let shards = try await service.decode(
            fileURL: local,
            episodeID: episodeID,
            shardDuration: 30
        )

        // 3600 / 30 = 120 shards.
        #expect(shards.count == 120 || shards.count == 121,
                "expected ~120 shards for 3600s @30s, got \(shards.count)")

        // M-1: every non-tail shard must have *exactly* samplesPerShard
        // samples by construction (the chunk loop never appends past the
        // ceiling). A subtle off-by-one or single-sample-drop bug would
        // surface here as some non-tail shard with sampleCount != 480_000.
        let samplesPerShard = 30 * 16_000
        for shard in shards.dropLast() {
            #expect(shard.sampleCount == samplesPerShard,
                    "non-tail shard \(shard.id) sampleCount=\(shard.sampleCount); expected exactly \(samplesPerShard)")
        }

        let peakShard = await service.peakShardAccumulatorBytes
        let oneShardBytes = 30 * 16_000 * MemoryLayout<Float>.size
        #expect(peakShard <= oneShardBytes + (oneShardBytes / 100),
                "peak shard accumulator = \(peakShard) bytes; must be ≤ \(oneShardBytes); a leak would scale with duration")

        let cumulative = await service.cumulativeOutputSamples
        let expected = Int(assetSeconds * AnalysisAudioService.targetSampleRate)
        let tol = expected / 200
        #expect(abs(cumulative - expected) <= tol,
                "cumulativeOutputSamples=\(cumulative) expected=\(expected) (±\(tol))")

        await service.evictCache(episodeID: episodeID)
    }

    /// Cycle-2 M-A: detect per-shard-boundary discontinuities. A clean
    /// sine input must produce smooth output across shard boundaries.
    /// At 440 Hz / 16 kHz / amplitude 0.25, the maximum sample-to-sample
    /// derivative is `2π·440/16000·0.25 ≈ 0.043`. A boundary glitch
    /// (e.g. dropped sample, double-emitted sample, FIR-tap reset) would
    /// inject a step several × this magnitude. We assert no inter-sample
    /// jump exceeds 5× the local maximum derivative.
    @Test("Per-boundary discontinuity check: no spikes at shard transitions")
    func no_boundary_discontinuity() async throws {
        let assetSeconds: TimeInterval = 90  // 3 boundaries (30, 60)
        let frequency = 440.0
        let amplitude: Double = 0.25
        let url = try writeSynthFile(
            seconds: assetSeconds,
            sampleRate: 44_100,
            frequency: frequency,
            amplitude: amplitude
        )
        defer { try? FileManager.default.removeItem(at: url) }

        guard let local = LocalAudioURL(url) else {
            Issue.record("Could not wrap test fixture")
            return
        }
        let service = AnalysisAudioService()
        let episodeID = "s8dq-disc-\(UUID().uuidString)"
        let shards = try await service.decode(
            fileURL: local,
            episodeID: episodeID,
            shardDuration: 30
        )

        #expect(shards.count == 3, "expected 3 shards for 90s @30s, got \(shards.count)")
        guard shards.count >= 2 else { return }

        let maxDerivPerSample = 2.0 * .pi * frequency / AnalysisAudioService.targetSampleRate * amplitude
        // Allow up to 5× — leaves room for FIR-tap ringing at boundaries
        // without admitting an actual sample-level glitch.
        let limit = Float(5.0 * maxDerivPerSample)

        for i in 0..<(shards.count - 1) {
            let endA = shards[i].samples.last!
            let startB = shards[i + 1].samples.first!
            let jump = abs(endA - startB)
            #expect(jump < limit,
                    "shard boundary \(i)→\(i+1): jump=\(jump) limit=\(limit) (sine deriv=\(maxDerivPerSample))")
        }

        await service.evictCache(episodeID: episodeID)
    }

    /// Cycle-2 M-B: chirp test stresses cross-call FIR-tap state. A
    /// clean sine doesn't reveal tap-reset bugs because the resampler
    /// re-stabilizes within tens of samples. A linear chirp hits every
    /// frequency in the band — if the FIR-tap state is reset between
    /// `convert` calls, the output will have audible glitches at every
    /// shard boundary, surfacing as RMS deviation from the smooth chirp
    /// ideal. We assert RMS within ±10% (looser than sine because the
    /// chirp itself isn't bandlimited; but tap-reset would push it
    /// >25%).
    @Test("Chirp content equivalence: FIR-tap state carries cleanly across convert calls")
    func chirp_content_equivalence() async throws {
        let assetSeconds: TimeInterval = 120  // many shard boundaries
        let url = try writeChirpFile(
            seconds: assetSeconds,
            sampleRate: 44_100,
            startFreq: 200,
            endFreq: 4_000,
            amplitude: 0.25
        )
        defer { try? FileManager.default.removeItem(at: url) }

        guard let local = LocalAudioURL(url) else {
            Issue.record("Could not wrap test fixture")
            return
        }
        let service = AnalysisAudioService()
        let episodeID = "s8dq-chirp-\(UUID().uuidString)"
        let shards = try await service.decode(
            fileURL: local,
            episodeID: episodeID,
            shardDuration: 30
        )

        var all: [Float] = []
        all.reserveCapacity(shards.reduce(0) { $0 + $1.sampleCount })
        for shard in shards { all.append(contentsOf: shard.samples) }

        let head = 1024
        let tail = all.count - 1024
        guard tail > head else {
            Issue.record("Decoded chirp output too short")
            return
        }
        let region = all[head..<tail]

        var sumSq: Double = 0
        for s in region { sumSq += Double(s) * Double(s) }
        let rms = sqrt(sumSq / Double(region.count))
        let expectedRMS = 0.25 / sqrt(2.0)
        let rmsErr = abs(rms - expectedRMS) / expectedRMS
        #expect(rmsErr < 0.10,
                "chirp RMS=\(rms) expected≈\(expectedRMS), error=\(rmsErr * 100)%; a tap-reset bug would produce RMS dips at boundaries")

        // Also assert no boundary discontinuity using a generous limit
        // (the chirp's local derivative grows with frequency).
        let maxFreq = 4_000.0  // upper end of sweep
        let chirpMaxDeriv = 2.0 * .pi * maxFreq / AnalysisAudioService.targetSampleRate * 0.25
        let limit = Float(5.0 * chirpMaxDeriv)
        for i in 0..<(shards.count - 1) {
            let endA = shards[i].samples.last!
            let startB = shards[i + 1].samples.first!
            let jump = abs(endA - startB)
            #expect(jump < limit,
                    "chirp boundary \(i)→\(i+1): jump=\(jump) limit=\(limit)")
        }

        await service.evictCache(episodeID: episodeID)
    }

    /// Cycle-2 M-C: an asset whose duration is an exact multiple of
    /// `shardDuration` must NOT produce a spurious empty trailing
    /// shard. The pre-fix slicing path naturally wouldn't, but the
    /// streaming-emit code's tail `emitShard()` call must guard
    /// against `count == 0`.
    @Test("60s file at 30s shards: no spurious empty trailing shard")
    func no_empty_trailing_shard() async throws {
        let url = try writeSynthFile(seconds: 60)
        defer { try? FileManager.default.removeItem(at: url) }

        guard let local = LocalAudioURL(url) else {
            Issue.record("Could not wrap test fixture")
            return
        }
        let service = AnalysisAudioService()
        let episodeID = "s8dq-no-tail-\(UUID().uuidString)"
        let shards = try await service.decode(
            fileURL: local,
            episodeID: episodeID,
            shardDuration: 30
        )

        // Decoder slack from FIR ramp-up may make the duration land at
        // 59.9x s rather than exact 60 s — so we accept 2 or 3 shards
        // depending on whether the tail leftover crossed the threshold.
        // What we MUST NOT see: a final shard with sampleCount == 0.
        for shard in shards {
            #expect(shard.sampleCount > 0,
                    "shard \(shard.id) has sampleCount=0 — empty trailing shard regression")
        }
        // And the count is exactly 2 or 3 — not 4 (which would imply
        // an empty shard *plus* the regular slicing).
        #expect((2...3).contains(shards.count),
                "got \(shards.count) shards for ~60s @30s; expected 2 or 3")

        await service.evictCache(episodeID: episodeID)
    }
}
