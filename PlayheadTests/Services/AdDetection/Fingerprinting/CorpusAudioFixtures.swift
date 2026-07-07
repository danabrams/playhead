// CorpusAudioFixtures.swift
// playhead-xsdz.26: staged-corpus audio access + decode/transcode helpers
// for the ChromaFingerprinter validation and perf suites. TEST TARGET ONLY —
// AVFoundation is allowed here (decode/transcode); the production
// fingerprinter itself never imports it.
//
// Corpus location: the ~30 real podcast episodes live ONLY in the MAIN
// checkout (bead worktrees do not carry the multi-GB audio). Resolution
// order (each candidate must actually contain the required files):
//   1. PLAYHEAD_CORPUS_AUDIO_DIR from the test-process environment (pass
//      via xcodebuild as TEST_RUNNER_PLAYHEAD_CORPUS_AUDIO_DIR=...).
//   2. <this repo root>/TestFixtures/Corpus/Audio (works when the suite
//      runs from the main checkout itself).
//   3. The documented absolute default /Users/dabrams/playhead/
//      TestFixtures/Corpus/Audio (the main checkout on the dev machine,
//      reachable from worktree runs; simulator tests read host paths).
// When nothing resolves, suites SKIP cleanly (ConditionTrait), keeping the
// suite green on machines without the corpus.

import AVFoundation
import Foundation
@testable import Playhead

enum CorpusAudioFixtures {

    /// Absolute default: the main checkout's staged corpus on the dev
    /// machine (worktrees deliberately do not duplicate the audio).
    private static let defaultMainCheckoutAudioDir =
        URL(fileURLWithPath: "/Users/dabrams/playhead/TestFixtures/Corpus/Audio", isDirectory: true)

    /// This repo root derived from THIS file's #filePath (five parents up:
    /// Fingerprinting/ -> AdDetection/ -> Services/ -> PlayheadTests/ ->
    /// repo root), mirroring the LexicalAutoAdCorpusEvalTests pattern.
    private static func repoRootAudioDir(filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()  // Fingerprinting/
            .deletingLastPathComponent()  // AdDetection/
            .deletingLastPathComponent()  // Services/
            .deletingLastPathComponent()  // PlayheadTests/
            .deletingLastPathComponent()  // <repo root>
            .appendingPathComponent("TestFixtures/Corpus/Audio", isDirectory: true)
    }

    /// First candidate directory that contains ALL `requiredFiles`, or nil.
    static func audioDirectory(containing requiredFiles: [String]) -> URL? {
        var candidates: [URL] = []
        if let override = ProcessInfo.processInfo.environment["PLAYHEAD_CORPUS_AUDIO_DIR"] {
            candidates.append(URL(fileURLWithPath: override, isDirectory: true))
        }
        candidates.append(repoRootAudioDir())
        candidates.append(defaultMainCheckoutAudioDir)
        let fm = FileManager.default
        return candidates.first { dir in
            requiredFiles.allSatisfy { fm.fileExists(atPath: dir.appendingPathComponent($0).path) }
        }
    }

    // MARK: - Decode

    /// Decode a segment of any AVFoundation-readable audio file to mono
    /// Float32 at the fingerprinter's 11025 Hz.
    static func decodeMono11025(
        url: URL, startSeconds: Double, durationSeconds: Double
    ) throws -> [Float] {
        let file = try AVAudioFile(forReading: url)
        let source = file.processingFormat
        guard let target = AVAudioFormat(
                commonFormat: .pcmFormatFloat32,
                sampleRate: Double(ChromaFingerprinter.requiredSampleRate),
                channels: 1, interleaved: false),
              let converter = AVAudioConverter(from: source, to: target) else {
            throw FixtureError.converterSetupFailed
        }
        file.framePosition = AVAudioFramePosition(startSeconds * source.sampleRate)
        var remaining = AVAudioFrameCount(durationSeconds * source.sampleRate)
        return try convert(converter: converter, target: target) { inputCapacity in
            guard remaining > 0 else { return nil }
            guard let buffer = AVAudioPCMBuffer(pcmFormat: source, frameCapacity: inputCapacity) else {
                throw FixtureError.bufferAllocationFailed
            }
            do {
                try file.read(into: buffer, frameCount: min(inputCapacity, remaining))
            } catch let error as NSError
                where error.domain == NSOSStatusErrorDomain && error.code == kAudioFileEndOfFileError {
                // Reading past the last packet throws eofErr (-39) on iOS;
                // a shorter-than-requested decode is normal end-of-stream
                // here (callers assert the decoded length they need).
                return nil
            }
            guard buffer.frameLength > 0 else { return nil }
            remaining -= buffer.frameLength
            return buffer
        }
    }

    /// OSStatus for "read past end of audio file" (eofErr, -39).
    private static let kAudioFileEndOfFileError = -39

    /// Sample-rate convert an 11025 Hz mono buffer to another rate.
    static func resample(_ samples: [Float], from sourceRate: Double, to targetRate: Double) throws -> [Float] {
        guard let source = AVAudioFormat(
                commonFormat: .pcmFormatFloat32, sampleRate: sourceRate, channels: 1, interleaved: false),
              let target = AVAudioFormat(
                commonFormat: .pcmFormatFloat32, sampleRate: targetRate, channels: 1, interleaved: false),
              let converter = AVAudioConverter(from: source, to: target) else {
            throw FixtureError.converterSetupFailed
        }
        var fed = false
        return try convert(converter: converter, target: target) { _ in
            guard !fed else { return nil }
            fed = true
            return try pcmBuffer(samples, format: source)
        }
    }

    /// Megaphone-class re-encode: 11025 mono -> 44100 -> AAC 64 kbps m4a
    /// on disk -> decode -> back to 11025 mono. In-test transcode chosen
    /// over pre-generated fixtures ([OWN CHOICE], documented in the bead
    /// docs): Process/afconvert is unavailable in the simulator, and this
    /// keeps the acid test hermetic given corpus audio.
    static func aacRoundTrip(_ samples: [Float]) throws -> [Float] {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("xsdz26-reencode-\(UUID().uuidString).m4a")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        let at44100 = try resample(
            samples, from: Double(ChromaFingerprinter.requiredSampleRate), to: 44100)
        let settings: [String: Any] = [
            AVFormatIDKey: kAudioFormatMPEG4AAC,
            AVSampleRateKey: 44100.0,
            AVNumberOfChannelsKey: 1,
            AVEncoderBitRateKey: 64000,
        ]
        // Scope the writer so the file is finalized before reading back.
        do {
            let writer = try AVAudioFile(
                forWriting: tempURL, settings: settings,
                commonFormat: .pcmFormatFloat32, interleaved: false)
            try writer.write(from: pcmBuffer(at44100, format: writer.processingFormat))
        }
        return try decodeMono11025(
            url: tempURL, startSeconds: 0,
            durationSeconds: Double(at44100.count) / 44100.0 + 1.0)
    }

    // MARK: - Internals

    enum FixtureError: Error {
        case converterSetupFailed
        case bufferAllocationFailed
        case conversionFailed
    }

    private static func pcmBuffer(_ samples: [Float], format: AVAudioFormat) throws -> AVAudioPCMBuffer {
        guard !samples.isEmpty,
              format.sampleRate > 0,
              let buffer = AVAudioPCMBuffer(
                pcmFormat: format, frameCapacity: AVAudioFrameCount(samples.count)),
              let channel = buffer.floatChannelData?[0] else {
            throw FixtureError.bufferAllocationFailed
        }
        samples.withUnsafeBufferPointer { pointer in
            channel.update(from: pointer.baseAddress!, count: samples.count)
        }
        buffer.frameLength = AVAudioFrameCount(samples.count)
        return buffer
    }

    /// Shared AVAudioConverter drain loop. `nextInput` returns nil at end
    /// of stream; thrown errors abort the conversion.
    private static func convert(
        converter: AVAudioConverter,
        target: AVAudioFormat,
        nextInput: @escaping (AVAudioFrameCount) throws -> AVAudioPCMBuffer?
    ) throws -> [Float] {
        guard let outBuffer = AVAudioPCMBuffer(pcmFormat: target, frameCapacity: 65536) else {
            throw FixtureError.bufferAllocationFailed
        }
        var inputError: Error?
        var ended = false
        let inputBlock: AVAudioConverterInputBlock = { _, status in
            do {
                if let buffer = try nextInput(65536) {
                    status.pointee = .haveData
                    return buffer
                }
            } catch {
                inputError = error
            }
            ended = true
            status.pointee = .endOfStream
            return nil
        }
        var output: [Float] = []
        while true {
            outBuffer.frameLength = 0
            var conversionError: NSError?
            let status = converter.convert(to: outBuffer, error: &conversionError, withInputFrom: inputBlock)
            if let inputError { throw inputError }
            if let conversionError { throw conversionError }
            if outBuffer.frameLength > 0, let channel = outBuffer.floatChannelData?[0] {
                output.append(contentsOf: UnsafeBufferPointer(start: channel, count: Int(outBuffer.frameLength)))
            }
            switch status {
            case .endOfStream:
                return output
            case .error:
                // .error with no populated NSError (should not happen):
                // fail loudly rather than return a silent truncation.
                throw FixtureError.conversionFailed
            case .inputRanDry where ended && outBuffer.frameLength == 0:
                return output
            default:
                continue
            }
        }
    }
}
