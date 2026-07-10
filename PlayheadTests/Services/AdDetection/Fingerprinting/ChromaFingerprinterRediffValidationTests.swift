// ChromaFingerprinterRediffValidationTests.swift
// playhead-xsdz.26: end-to-end validation of the clean-room
// ChromaFingerprinter THROUGH the validated rediff differ
// (RediffPrototype, xsdz.16 spike — not modified here), on staged corpus
// audio. Ground truth is exact-by-construction: known segments are spliced
// at known offsets, so recovered slot boundaries can be asserted directly.
//
// Scope (per the bead):
//   (a) synthetic splice pairs over 3 shows with distinct audio character
//       (conversational / produced+music / true-crime narration);
//   (b) re-encode robustness (Megaphone-class): AAC 64 kbps round trip via
//       AVFoundation IN-TEST (Process/afconvert is unavailable in the
//       simulator) — identical content must stay aligned
//       (alignedFractionB >= 0.8) and a spliced+re-encoded pair must still
//       recover boundaries;
//   (c) real played-arm-vs-fresh-arm pairs: OUT OF SCOPE here (no fresh-B
//       audio is kept locally); deferred to the integration bead.
//
// Corpus audio lives only in the main checkout; suites SKIP cleanly when
// it is absent (see CorpusAudioFixtures).
//
// BOUNDARY GEOMETRY (windowBias): subfingerprint i covers chroma frames
// i..<i+16, so every window whose SPAN touches inserted audio mismatches.
// The last clean fingerprint before a splice therefore sits ~15 hops
// (= (windowFrameCount-1) * secondsPerFingerprint ≈ 1.86 s) BEFORE the
// splice, and reported gap STARTS carry that systematic early bias, while
// gap ENDS are unbiased (the first clean window starts exactly at the
// insert's end). Assertions below correct for it; the width-oracle
// integration bead must apply the same correction.

import Foundation
import Testing
@testable import Playhead

// MARK: - Pinned operating point

/// The differ operating point chosen for ChromaFingerprinter fingerprints
/// (tuned in the xsdz.26 calibration harness; see
/// docs/xsdz26-fingerprinter-validation.md). fpcalc-era defaults used
/// hammingTol=2; our re-implementation needs hammingTol=5 because a splice
/// shifts the downstream STFT grid by a sub-hop phase (insert lengths are
/// not hop multiples), costing a few bits per subfingerprint on
/// phase-sensitive (music-heavy) content. Run seeding stays anchor-EXACT,
/// so higher tolerance widens run EXTENSION without loosening seeding.
enum ChromaRediffOperatingPoint {
    static let hammingTol = 5
    static let minRunLen = 8
    static let offsetSlack = 2
    static let gapDiffSlack = 2
    static let minAdSeconds = 5.0
    /// The exact granularity handed to the differ — the fingerprinter's
    /// own exposed rate (granularity-pair consistency pinned by test).
    static let secondsPerFp = ChromaFingerprinter.secondsPerFingerprint
}

// MARK: - Cases and helpers

struct ChromaSpliceValidationCase: CustomStringConvertible, Sendable {
    let show: String
    let file: String
    /// Segment start inside the episode (past any preroll ads).
    let contentStart: Double
    var description: String { show }
}

private let spliceCases: [ChromaSpliceValidationCase] = [
    ChromaSpliceValidationCase(show: "smartless-conversational", file: "smartless-2026-05-18-quot-sting-quot.mp3", contentStart: 300),
    ChromaSpliceValidationCase(show: "radiolab-produced-music", file: "radiolab-2026-05-29-this-american-roach.mp3", contentStart: 300),
    ChromaSpliceValidationCase(show: "casefile-truecrime-narration", file: "casefile-true-crime-2026-05-30-case-340-elisabeth-membrey.mp3", contentStart: 300),
]

/// The "inserted ad" donor: a DIFFERENT show entirely.
private let insertDonorFile = "techcrunch-daily-crunch-2026-05-26-spotify-s-ai-bet-more-of-everything-less.mp3"

private let allValidationFiles = spliceCases.map(\.file) + [insertDonorFile]

private func corpusAvailable() -> Bool {
    CorpusAudioFixtures.audioDirectory(containing: allValidationFiles) != nil
}

/// Content segment length per case (4 min — short by design, fast suite).
private let contentSeconds = 240.0
/// Insert (fake DAI fill) length and position within the content.
private let insertSeconds = 30.0
private let insertAtSeconds = 120.0

/// Systematic early bias on gap starts; see file header. NOTE this is an
/// EMPIRICALLY-CENTERED correction, not exact geometry: a subfingerprint's
/// true audio span is 15 hops + one 4096-sample STFT frame (~2.23 s, plus
/// smoothing bleed), but Hann edge taper + hammingTol=5 let runs survive
/// partway into contaminated windows, so measured raw start errors center
/// around ~15 hops (~1.86 s) with roughly +-0.5 s of model error. The
/// startTolerance absorbs that spread.
private let windowBiasSeconds =
    Double(ChromaFingerprinter.windowFrameCount - 1) * ChromaFingerprinter.secondsPerFingerprint

/// Boundary bars (xsdz.16 spike hit ~1-3 s with fpcalc fingerprints; the
/// harness measured <= 0.5 s end error and <= 0.5 s bias-corrected start
/// error at THIS suite's splice geometry — tolerances leave decode-jitter
/// room). COUPLING WARNING: end-boundary error grows with the insert's
/// sub-hop phase on music-heavy content (harness worst case: 2.06 s at
/// phase ~0.47 hop). Do NOT change insertSeconds/insertAtSeconds without
/// re-checking the worst-phase end error against endTolerance
/// (docs/xsdz26-fingerprinter-validation.md).
private let startTolerance = 1.5
private let endTolerance = 2.0

private func rediff(_ a: [Float], _ b: [Float]) -> RediffPrototype.Result {
    RediffPrototype.rediff(
        fingerprintA: ChromaFingerprinter.fingerprint(monoSamples11025: a),
        secondsPerFpA: ChromaRediffOperatingPoint.secondsPerFp,
        fingerprintB: ChromaFingerprinter.fingerprint(monoSamples11025: b),
        secondsPerFpB: ChromaRediffOperatingPoint.secondsPerFp,
        hammingTol: ChromaRediffOperatingPoint.hammingTol,
        minRunLen: ChromaRediffOperatingPoint.minRunLen,
        offsetSlack: ChromaRediffOperatingPoint.offsetSlack,
        gapDiffSlack: ChromaRediffOperatingPoint.gapDiffSlack,
        minAdSeconds: ChromaRediffOperatingPoint.minAdSeconds)
}

/// Assert one recovered B-side slot matching the constructed insert.
private func expectInsertRecovered(
    _ result: RediffPrototype.Result,
    label: String,
    sourceLocation: SourceLocation = #_sourceLocation
) {
    #expect(result.slotsB.count == 1, "\(label): expected exactly one B slot, got \(result.slotsB)",
            sourceLocation: sourceLocation)
    guard let slot = result.slotsB.first else { return }
    let expectedStart = insertAtSeconds - windowBiasSeconds
    let expectedEnd = insertAtSeconds + insertSeconds
    #expect(abs(slot.startSeconds - expectedStart) <= startTolerance,
            "\(label): slot start \(slot.startSeconds) vs expected \(expectedStart) (bias-corrected)",
            sourceLocation: sourceLocation)
    #expect(abs(slot.endSeconds - expectedEnd) <= endTolerance,
            "\(label): slot end \(slot.endSeconds) vs insert end \(expectedEnd)",
            sourceLocation: sourceLocation)
    // The played arm (A) is pure content: no phantom A-side ad spans.
    #expect(result.slotsA.isEmpty, "\(label): phantom slotsA \(result.slotsA)",
            sourceLocation: sourceLocation)
}

// MARK: - Suite

@Suite(
    "ChromaFingerprinter through rediff differ (playhead-xsdz.26, corpus)",
    .enabled(if: corpusAvailable(),
             "corpus audio not staged — expects the main checkout at /Users/dabrams/playhead/TestFixtures/Corpus/Audio or TEST_RUNNER_PLAYHEAD_CORPUS_AUDIO_DIR"))
struct ChromaFingerprinterRediffValidationTests {

    /// (a) splice recovery, (b1) re-encode alignment on identical content,
    /// (b2) splice + re-encode — one pass per show so each episode is
    /// decoded once (keeps the fast suite fast).
    @Test("splice + re-encode validation per show", arguments: spliceCases)
    func validate(spliceCase: ChromaSpliceValidationCase) throws {
        let audioDir = try #require(CorpusAudioFixtures.audioDirectory(containing: allValidationFiles))
        let content = try CorpusAudioFixtures.decodeMono11025(
            url: audioDir.appendingPathComponent(spliceCase.file),
            startSeconds: spliceCase.contentStart,
            durationSeconds: contentSeconds)
        let insert = try CorpusAudioFixtures.decodeMono11025(
            url: audioDir.appendingPathComponent(insertDonorFile),
            startSeconds: 30,
            durationSeconds: insertSeconds)
        // #require (not #expect): a truncated decode would otherwise trap
        // on the splice slicing below and crash the whole test runner.
        try #require(Double(content.count) > (contentSeconds - 1) * 11025, "content decode came up short")
        try #require(Double(insert.count) > (insertSeconds - 1) * 11025, "insert decode came up short")

        // Ground truth by construction: B = content with `insert` spliced
        // in at exactly insertAtSeconds.
        let cut = Int(insertAtSeconds * Double(ChromaFingerprinter.requiredSampleRate))
        var spliced = Array(content[0..<cut])
        spliced.append(contentsOf: insert)
        spliced.append(contentsOf: content[cut...])

        // (a) plain splice: boundaries within the bar.
        let spliceResult = rediff(content, spliced)
        expectInsertRecovered(spliceResult, label: "\(spliceCase) splice")
        #expect(spliceResult.alignedFractionB >= 0.8,
                "\(spliceCase) splice alignedFractionB \(spliceResult.alignedFractionB)")

        // (b1) identical content vs AAC-64k re-encode: stays aligned, no
        // phantom slots on either side.
        let reencoded = try CorpusAudioFixtures.aacRoundTrip(content)
        let identicalResult = rediff(content, reencoded)
        #expect(identicalResult.alignedFractionB >= 0.8,
                "\(spliceCase) re-encode-identical alignedFractionB \(identicalResult.alignedFractionB)")
        #expect(identicalResult.slotsB.isEmpty,
                "\(spliceCase) re-encode-identical phantom slotsB \(identicalResult.slotsB)")
        #expect(identicalResult.slotsA.isEmpty,
                "\(spliceCase) re-encode-identical phantom slotsA \(identicalResult.slotsA)")

        // (b2) splice + re-encode (the Megaphone-class acid test): the
        // insert is still recovered within the same boundary bar.
        let splicedReencoded = try CorpusAudioFixtures.aacRoundTrip(spliced)
        let acidResult = rediff(content, splicedReencoded)
        expectInsertRecovered(acidResult, label: "\(spliceCase) splice+re-encode")
        #expect(acidResult.alignedFractionB >= 0.8,
                "\(spliceCase) splice+re-encode alignedFractionB \(acidResult.alignedFractionB)")
    }
}

// MARK: - Operating point pins (hermetic — run even without corpus audio)

@Suite("ChromaFingerprinter/rediff operating point pins (playhead-xsdz.26)")
struct ChromaRediffOperatingPointPinTests {

    @Test("granularity pair: validation hands the differ the fingerprinter's exact rate")
    func granularityPairConsistency() {
        #expect(ChromaRediffOperatingPoint.secondsPerFp == ChromaFingerprinter.secondsPerFingerprint)
        #expect(ChromaRediffOperatingPoint.secondsPerFp == 1365.0 / 11025.0)
    }

    @Test("chosen differ knobs are pinned")
    func operatingPointPinned() {
        // Tuned 2026-07-07 on the corpus harness; changing any of these
        // requires re-running the validation matrix (see
        // docs/xsdz26-fingerprinter-validation.md).
        #expect(ChromaRediffOperatingPoint.hammingTol == 5)
        #expect(ChromaRediffOperatingPoint.minRunLen == 8)
        #expect(ChromaRediffOperatingPoint.offsetSlack == 2)
        #expect(ChromaRediffOperatingPoint.gapDiffSlack == 2)
        #expect(ChromaRediffOperatingPoint.minAdSeconds == 5.0)
    }
}
