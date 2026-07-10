// ChromaFingerprinterPerfTests.swift
// playhead-xsdz.26: wall-clock budget for fingerprinting a full 60-minute
// episode (decode + fingerprint < 60 s in the test process).
//
// PerfGate-gated (playhead-zx0l): asserts an absolute wall-clock budget
// that is only meaningful on a quiescent CPU, so it runs ONLY in the
// dedicated serial perf pass (scripts/perf-tests.sh, which sets
// PLAYHEAD_RUN_PERF=1) and skips in the parallel fast/integration suites.
// It is listed in MEASUREMENT_TESTS in scripts/perf-tests.sh.
//
// Measured numbers and the on-device extrapolation caveat (this Mac is
// faster than an iPhone) are recorded in
// docs/xsdz26-fingerprinter-validation.md.

import Foundation
import Testing
@testable import Playhead

/// A corpus episode that is at least 60 minutes long.
private let perfEpisodeFile = "rest-history-2026-05-06-667-the-mystery-of-the-mona-lisa.mp3"

private func perfCorpusAvailable() -> Bool {
    CorpusAudioFixtures.audioDirectory(containing: [perfEpisodeFile]) != nil
}

private func seconds(_ duration: Duration) -> Double {
    Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
}

@Suite("ChromaFingerprinter 60-minute perf budget (playhead-xsdz.26)")
struct ChromaFingerprinterPerfTests {

    @Test(
        "60-minute episode: decode + fingerprint under 60 s",
        .enabled(if: PerfGate.runsMeasurementTests, "perf pass only — see playhead-zx0l"),
        .enabled(if: perfCorpusAvailable(),
                 "corpus audio not staged — expects the main checkout at /Users/dabrams/playhead/TestFixtures/Corpus/Audio or TEST_RUNNER_PLAYHEAD_CORPUS_AUDIO_DIR"))
    func sixtyMinuteEpisodeUnderBudget() throws {
        let audioDir = try #require(CorpusAudioFixtures.audioDirectory(containing: [perfEpisodeFile]))
        let targetSamples = 3600 * ChromaFingerprinter.requiredSampleRate
        let clock = ContinuousClock()

        let decodeStart = clock.now
        var samples = try CorpusAudioFixtures.decodeMono11025(
            url: audioDir.appendingPathComponent(perfEpisodeFile),
            startSeconds: 0,
            durationSeconds: 3600)
        // #require: an empty decode would spin the tiling loop forever.
        try #require(!samples.isEmpty, "perf episode decoded to zero samples")
        // Defensive tiling: keep the measurement honest at exactly 60
        // minutes of samples even if the fixture episode is trimmed.
        while samples.count < targetSamples {
            samples.append(contentsOf: samples.prefix(targetSamples - samples.count))
        }
        samples.removeLast(max(0, samples.count - targetSamples))
        let decodeDuration = clock.now - decodeStart

        let fingerprintStart = clock.now
        let fingerprints = ChromaFingerprinter.fingerprint(monoSamples11025: samples)
        let fingerprintDuration = clock.now - fingerprintStart

        let totalSeconds = seconds(decodeDuration) + seconds(fingerprintDuration)
        print(String(
            format: "PERF xsdz.26: 60-min episode decode %.2fs + fingerprint %.2fs = %.2fs (%d subfingerprints)",
            seconds(decodeDuration), seconds(fingerprintDuration), totalSeconds, fingerprints.count))

        // ~29,059 fps expected for 60 min at 1365/11025 s per fingerprint.
        #expect(fingerprints.count > 28_000, "unexpectedly few subfingerprints: \(fingerprints.count)")
        #expect(totalSeconds < 60.0,
                "60-minute episode took \(totalSeconds)s (budget 60s; decode \(seconds(decodeDuration))s, fingerprint \(seconds(fingerprintDuration))s)")
    }
}
