// ChapterFeatureSnapshotBuilderTests.swift
// playhead-au2v.1.24 (playhead-nbmj): Regression tests for the
// raw-audio → ChapterFeatureSnapshot helper.
//
// Scope (smallest sufficient set):
//   * End-to-end: a small synthetic mono .caf written to a temp
//     file flows through the builder and yields a non-empty
//     `ChapterFeatureSnapshot` with the expected window cadence
//     (music + pause arrays cover the audio at the feature-extraction
//     window stride; lexical hits drawn from the supplied transcript;
//     speaker windows shaped by transcript chunks).
//   * URL gate: a non-`file://` URL throws `BuildError.audioURLNotLocal`.
//   * Pure mapping: `snapshot(from:transcript:episodeDuration:)` is a
//     pure function — given a fixed `[FeatureWindow]` + transcript it
//     produces a deterministic snapshot. This is the seam shared with
//     any future production wiring that needs the same projection
//     (see file header in `ChapterFeatureSnapshotBuilder.swift`).
//
// What we deliberately do NOT cover here:
//   * Full feature-extraction DSP shape (`FeatureExtractionService` is
//     covered by its own suite; we only need to know the new
//     `extract(shards:analysisAssetId:)` actor entry composes
//     correctly with `extractAndPersist`'s shared private path).
//   * Boundary-detector behavior (covered by
//     `ChapterBoundaryDetectorTests`).
//   * FM labeling (covered by `ChapterLabelingServiceTests`).
//
// Note on synthetic audio: we generate a 6 s mono Float32 sine WAV
// (well above one feature window of ~2 s but small enough to keep
// the test fast). 6 s also exceeds the upstream
// `AnalysisAudioService.defaultShardDuration` slicing threshold so the
// builder exercises the multi-window slice path inside one shard.
// Going larger (60+ s, multi-shard) is the streaming suite's job —
// here we only need a non-empty windows array out of feature
// extraction.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("ChapterFeatureSnapshotBuilder (au2v.1.24)")
struct ChapterFeatureSnapshotBuilderTests {

    // MARK: - Fixtures

    /// Writes a synthetic mono Float32 .caf of the requested duration.
    /// Matches the recipe used by `AnalysisAudioStreamingTests`; .caf
    /// dodges format quirks for non-interleaved Float32.
    private func writeSynthAudio(
        seconds: TimeInterval,
        sampleRate: Double = 44_100,
        frequency: Double = 440,
        amplitude: Double = 0.25
    ) throws -> URL {
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory())
        let fileURL = tempDir.appendingPathComponent(
            "au2v124-\(UUID().uuidString).caf"
        )

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sampleRate,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "ChapterFeatureSnapshotBuilderTests", code: -1)
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

        while totalFrames < totalNeeded {
            let remaining = AVAudioFrameCount(totalNeeded - totalFrames)
            let frames = min(chunkFrames, remaining)
            guard let buffer = AVAudioPCMBuffer(
                pcmFormat: format,
                frameCapacity: frames
            ) else {
                throw NSError(
                    domain: "ChapterFeatureSnapshotBuilderTests", code: -2
                )
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

    /// Build a tiny transcript covering the [0, audioDuration] range.
    /// Two chunks separated by a sponsor-like phrase so the lexical
    /// scanner can produce at least one hit when wired in.
    private func makeTranscript(
        analysisAssetId: String,
        audioDuration: TimeInterval
    ) -> [TranscriptChunk] {
        let mid = audioDuration / 2
        let chunks: [(Double, Double, String)] = [
            (0.0, mid,
             "Welcome to the show, today we have a great guest."),
            (mid, audioDuration,
             "This podcast is brought to you by ExampleSponsor.")
        ]
        return chunks.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(analysisAssetId)",
                analysisAssetId: analysisAssetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    // MARK: - End-to-end

    @Test("build(audioURL:transcript:fmAvailable:) produces a populated snapshot")
    func buildEndToEnd_populatesSnapshot() async throws {
        let seconds: TimeInterval = 6
        let url = try writeSynthAudio(seconds: seconds)
        defer { try? FileManager.default.removeItem(at: url) }

        let transcript = makeTranscript(
            analysisAssetId: "au2v124-builder-e2e",
            audioDuration: seconds
        )

        let snapshot = try await ChapterFeatureSnapshotBuilder.build(
            audioURL: url,
            transcript: transcript,
            fmAvailable: false
        )

        // Episode duration tracks the decoded shard span. With one
        // shard of 6 s in, duration is exactly the shard's
        // (startTime + duration). Allow a generous tolerance because
        // shard duration is set by the converter to the nearest frame
        // boundary, not the requested seconds.
        #expect(
            abs(snapshot.episodeDuration - seconds) < 0.5,
            "episodeDuration tracks decoded shard span; got \(snapshot.episodeDuration)"
        )

        // Music + pause windows are emitted by feature extraction at
        // the configured window stride. 6 s of audio should yield at
        // least one window per signal (the FeatureExtractionConfig
        // default window stride is well below 6 s).
        #expect(
            !snapshot.musicWindows.isEmpty,
            "musicWindows must be populated from feature extraction"
        )
        #expect(
            !snapshot.pauseWindows.isEmpty,
            "pauseWindows must be populated from feature extraction"
        )
        #expect(
            snapshot.musicWindows.count == snapshot.pauseWindows.count,
            "music + pause arrays come from the same FeatureWindow set; counts must match"
        )

        // Speaker windows are one per transcript chunk.
        #expect(
            snapshot.speakerWindows.count == transcript.count,
            "one speaker window per transcript chunk; got \(snapshot.speakerWindows.count) vs \(transcript.count)"
        )
        for (window, chunk) in zip(snapshot.speakerWindows, transcript) {
            #expect(window.startTime == chunk.startTime)
            #expect(window.endTime == chunk.endTime)
            // Transcript chunks built here have no speakerId — the
            // snapshot preserves nil-cluster windows so the time
            // coverage stays intact even without a label.
            #expect(window.clusterId == nil)
        }

        // Lexical wiring: the synthetic transcript contains a
        // sponsor-like phrase ("brought to you by ExampleSponsor"),
        // which the built-in `LexicalScanner` patterns catch. This
        // assertion guards against `build(...)` accidentally dropping
        // its `lexicalScanner` invocation in a future refactor — the
        // pure-mapping test (`snapshotMappingIsDeterministic`) only
        // covers `snapshot(...)` direct calls, not the end-to-end
        // `build(...)` wiring.
        #expect(
            !snapshot.lexicalHits.isEmpty,
            "build(...) must thread the lexical scanner into the snapshot mapping; got 0 hits on sponsor transcript"
        )

        // Window arrays are sorted by start time. Defensive sort in the
        // builder is exercised by the end-to-end path because feature
        // extraction emits in time order already; this assertion
        // documents the invariant.
        let musicStarts = snapshot.musicWindows.map(\.startTime)
        #expect(
            musicStarts == musicStarts.sorted(),
            "musicWindows must be sorted by startTime ascending"
        )
        let pauseStarts = snapshot.pauseWindows.map(\.startTime)
        #expect(
            pauseStarts == pauseStarts.sorted(),
            "pauseWindows must be sorted by startTime ascending"
        )
    }

    // MARK: - URL gate

    @Test("non-file URL raises BuildError.audioURLNotLocal")
    func nonFileURL_throws() async throws {
        let httpURL = URL(string: "https://example.com/audio.mp3")!

        do {
            _ = try await ChapterFeatureSnapshotBuilder.build(
                audioURL: httpURL,
                transcript: [],
                fmAvailable: false
            )
            Issue.record("expected BuildError.audioURLNotLocal")
        } catch let error as ChapterFeatureSnapshotBuilder.BuildError {
            switch error {
            case .audioURLNotLocal(let url):
                #expect(url == httpURL)
            }
        } catch {
            Issue.record("unexpected error \(error)")
        }
    }

    // MARK: - Pure mapping

    @Test("snapshot(from:transcript:episodeDuration:) is a deterministic pure mapping")
    func snapshotMappingIsDeterministic() {
        // Fixed FeatureWindow set covering 0..6s at 2s stride.
        var windows: [FeatureWindow] = []
        for idx in 0..<3 {
            let startTime: Double = Double(idx) * 2.0
            let endTime: Double = Double(idx + 1) * 2.0
            let music: Double = (idx == 0) ? 0.95 : 0.05
            let pause: Double = (idx == 1) ? 0.9 : 0.1
            windows.append(FeatureWindow(
                analysisAssetId: "fake-asset",
                startTime: startTime,
                endTime: endTime,
                rms: 0.2,
                spectralFlux: 0.5,
                musicProbability: music,
                speakerChangeProxyScore: 0.0,
                pauseProbability: pause,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            ))
        }

        let transcript = [
            TranscriptChunk(
                id: "c0",
                analysisAssetId: "fake-asset",
                segmentFingerprint: "fp-0",
                chunkIndex: 0,
                startTime: 0.0,
                endTime: 3.0,
                text: "This podcast is brought to you by ExampleSponsor.",
                normalizedText: "this podcast is brought to you by examplesponsor",
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                speakerId: 1
            )
        ]

        let first = ChapterFeatureSnapshotBuilder.snapshot(
            from: windows,
            transcript: transcript,
            episodeDuration: 6.0
        )
        let second = ChapterFeatureSnapshotBuilder.snapshot(
            from: windows,
            transcript: transcript,
            episodeDuration: 6.0
        )

        #expect(first.episodeDuration == 6.0)
        #expect(first.musicWindows.count == 3)
        #expect(first.pauseWindows.count == 3)
        #expect(first.speakerWindows.count == 1)
        #expect(first.speakerWindows.first?.clusterId == 1)
        // Lexical scanner should find at least one sponsor hit in
        // the supplied transcript text. If the scanner is later
        // retuned to suppress this specific phrase, update the
        // expected count; until then this catches accidental
        // de-wiring of the scanner.
        #expect(
            !first.lexicalHits.isEmpty,
            "lexical scanner must produce at least one hit on the sponsor transcript"
        )

        // Deterministic: same inputs → identical output arrays.
        #expect(first.musicWindows == second.musicWindows)
        #expect(first.pauseWindows == second.pauseWindows)
        #expect(first.speakerWindows == second.speakerWindows)
        #expect(first.lexicalHits == second.lexicalHits)
    }

    @Test("snapshot(...) clamps negative episodeDuration to zero")
    func snapshotClampsNegativeDuration() {
        let snapshot = ChapterFeatureSnapshotBuilder.snapshot(
            from: [],
            transcript: [],
            episodeDuration: -1.0
        )
        #expect(snapshot.episodeDuration == 0)
        #expect(snapshot.musicWindows.isEmpty)
        #expect(snapshot.pauseWindows.isEmpty)
        #expect(snapshot.speakerWindows.isEmpty)
        #expect(snapshot.lexicalHits.isEmpty)
    }
}
