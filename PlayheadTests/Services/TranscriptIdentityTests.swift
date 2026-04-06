// TranscriptIdentityTests.swift
// Unit tests for Phase 1 transcript identity types: TranscriptAtomizer,
// TranscriptSegmenter, TranscriptQualityEstimator, and TranscriptChunk migration.

import Foundation
import SQLite3
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeChunk(
    id: String = UUID().uuidString,
    assetId: String = "asset-1",
    chunkIndex: Int = 0,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String = "hello world",
    pass: String = "final"
) -> TranscriptChunk {
    TranscriptChunk(
        id: id,
        analysisAssetId: assetId,
        segmentFingerprint: "fp-\(id)",
        chunkIndex: chunkIndex,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: text.lowercased(),
        pass: pass,
        modelVersion: "speech-v1",
        transcriptVersion: nil,
        atomOrdinal: nil
    )
}

private func makeAtom(
    assetId: String = "asset-1",
    version: String = "abc123",
    ordinal: Int = 0,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String = "hello world"
) -> TranscriptAtom {
    TranscriptAtom(
        atomKey: TranscriptAtomKey(
            analysisAssetId: assetId,
            transcriptVersion: version,
            atomOrdinal: ordinal
        ),
        contentHash: "deadbeef",
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: ordinal
    )
}

private func makeFeatureWindow(
    assetId: String = "asset-1",
    startTime: Double,
    endTime: Double,
    pauseProbability: Double = 0.1,
    speakerClusterId: Int?
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        rms: 0.4,
        spectralFlux: 0.1,
        musicProbability: 0.0,
        pauseProbability: pauseProbability,
        speakerClusterId: speakerClusterId,
        jingleHash: nil,
        featureVersion: 1
    )
}

// MARK: - TranscriptAtomizer Tests

@Suite("TranscriptAtomizer")
struct TranscriptAtomizerTests {

    @Test("Atomize produces one atom per chunk with correct ordinals")
    func atomizeBasic() {
        let chunks = (0..<5).map { i in
            makeChunk(chunkIndex: i, startTime: Double(i) * 10, endTime: Double(i) * 10 + 9)
        }

        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: "asset-1",
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )

        #expect(atoms.count == 5)
        for (i, atom) in atoms.enumerated() {
            #expect(atom.atomKey.atomOrdinal == i)
            #expect(atom.atomKey.analysisAssetId == "asset-1")
            #expect(atom.atomKey.transcriptVersion == version.transcriptVersion)
            #expect(atom.chunkIndex == i)
        }
        #expect(!version.transcriptVersion.isEmpty)
        #expect(version.normalizationHash == "norm-v1")
        #expect(version.sourceHash == "asr-v1")
    }

    @Test("Atomize is deterministic — same input produces same version hash")
    func atomizeDeterministic() {
        let chunks = [makeChunk(chunkIndex: 0, text: "foo"), makeChunk(chunkIndex: 1, text: "bar")]

        let (_, v1) = TranscriptAtomizer.atomize(chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")
        let (_, v2) = TranscriptAtomizer.atomize(chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")

        #expect(v1.transcriptVersion == v2.transcriptVersion)
    }

    @Test("Atomize sorts by chunkIndex regardless of input order")
    func atomizeSortsInput() {
        let chunks = [
            makeChunk(chunkIndex: 2, text: "third"),
            makeChunk(chunkIndex: 0, text: "first"),
            makeChunk(chunkIndex: 1, text: "second"),
        ]

        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )

        #expect(atoms[0].text == "first")
        #expect(atoms[1].text == "second")
        #expect(atoms[2].text == "third")
    }

    @Test("Atomize empty chunks returns empty")
    func atomizeEmpty() {
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: [], analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )
        #expect(atoms.isEmpty)
    }

    @Test("Different content produces different version hashes")
    func atomizeDifferentContent() {
        let chunks1 = [makeChunk(chunkIndex: 0, text: "foo")]
        let chunks2 = [makeChunk(chunkIndex: 0, text: "bar")]

        let (_, v1) = TranscriptAtomizer.atomize(chunks: chunks1, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")
        let (_, v2) = TranscriptAtomizer.atomize(chunks: chunks2, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")

        #expect(v1.transcriptVersion != v2.transcriptVersion)
    }

    @Test("Content hashes are non-empty and differ for different content")
    func contentHashesCorrect() {
        let chunks = [
            makeChunk(chunkIndex: 0, text: "alpha"),
            makeChunk(chunkIndex: 1, text: "beta"),
        ]

        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )

        #expect(!atoms[0].contentHash.isEmpty)
        #expect(!atoms[1].contentHash.isEmpty)
        #expect(atoms[0].contentHash != atoms[1].contentHash)
    }

    @Test("Reordered input produces identical version hash")
    func reorderDeterminism() {
        let a = makeChunk(chunkIndex: 0, text: "first")
        let b = makeChunk(chunkIndex: 1, text: "second")
        let c = makeChunk(chunkIndex: 2, text: "third")

        let (_, v1) = TranscriptAtomizer.atomize(chunks: [a, b, c], analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")
        let (_, v2) = TranscriptAtomizer.atomize(chunks: [c, a, b], analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")

        #expect(v1.transcriptVersion == v2.transcriptVersion)
    }

    @Test("Different chunk boundaries produce different version hashes")
    func chunkBoundaryAmbiguity() {
        // ["ab", "cd"] vs ["a", "bcd"] — must NOT collide
        let chunks1 = [makeChunk(chunkIndex: 0, text: "ab"), makeChunk(chunkIndex: 1, text: "cd")]
        let chunks2 = [makeChunk(chunkIndex: 0, text: "a"), makeChunk(chunkIndex: 1, text: "bcd")]

        let (_, v1) = TranscriptAtomizer.atomize(chunks: chunks1, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")
        let (_, v2) = TranscriptAtomizer.atomize(chunks: chunks2, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")

        #expect(v1.transcriptVersion != v2.transcriptVersion)
    }

    @Test("Single chunk produces single atom with ordinal 0")
    func singleChunk() {
        let chunks = [makeChunk(chunkIndex: 0, text: "only one")]

        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )

        #expect(atoms.count == 1)
        #expect(atoms[0].atomKey.atomOrdinal == 0)
        #expect(!version.transcriptVersion.isEmpty)
    }
}

// MARK: - TranscriptSegmenter Tests

@Suite("TranscriptSegmenter")
struct TranscriptSegmenterTests {

    @Test("Segments split on pause threshold")
    func splitOnPause() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 5, text: "Hello."),
            makeAtom(ordinal: 1, startTime: 5, endTime: 10, text: "World."),
            // 3-second gap — exceeds 2s default threshold
            makeAtom(ordinal: 2, startTime: 13, endTime: 18, text: "New segment."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.count == 2)
        #expect(segments[0].atoms.count == 2)
        #expect(segments[1].atoms.count == 1)
        #expect(segments[1].firstAtomOrdinal == 2)
    }

    @Test("Default pause threshold splits on gaps above 1.5 seconds")
    func defaultPauseThresholdMatchesBeadSpec() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 4, text: "First part"),
            makeAtom(ordinal: 1, startTime: 5.6, endTime: 9, text: "Second part after a 1.6 second gap")
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.count == 2)
        #expect(segments[0].firstAtomOrdinal == 0)
        #expect(segments[1].firstAtomOrdinal == 1)
    }

    @Test("Single atom produces single segment")
    func singleAtom() {
        let atoms = [makeAtom(ordinal: 0, startTime: 0, endTime: 5)]
        let segments = TranscriptSegmenter.segment(atoms: atoms)
        #expect(segments.count == 1)
        #expect(segments[0].atoms.count == 1)
    }

    @Test("Empty input produces no segments")
    func emptyInput() {
        let segments = TranscriptSegmenter.segment(atoms: [])
        #expect(segments.isEmpty)
    }

    @Test("Max duration forces hard break even below min segment duration")
    func maxDurationBreak() {
        // Create atoms spanning 130s with no pauses — should break at 120s
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        var atoms: [TranscriptAtom] = []
        for i in 0..<26 {
            atoms.append(makeAtom(
                ordinal: i,
                startTime: Double(i) * 5,
                endTime: Double(i) * 5 + 4.9,
                text: "Word number \(i)."
            ))
        }

        let segments = TranscriptSegmenter.segment(atoms: atoms, config: config)

        #expect(segments.count == 2)
        // First segment should end at exactly the atom before 120s
        // Atom 24 starts at 120.0s, so it triggers the break. First segment = atoms 0-23.
        #expect(segments[0].atoms.count == 24)
        #expect(segments[1].atoms.count == 2)
        #expect(segments.allSatisfy { $0.duration <= config.maxSegmentDuration })
    }

    @Test("Discourse marker triggers break after minor pause")
    func discourseMarkerBreak() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10, text: "End of first topic."),
            makeAtom(ordinal: 1, startTime: 10, endTime: 20, text: "More content here."),
            // 0.6s gap + discourse marker
            makeAtom(ordinal: 2, startTime: 20.6, endTime: 30, text: "Anyway let me tell you about."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.count == 2)
        #expect(segments[0].atoms.count == 2)
        #expect(segments[1].atoms.count == 1)
        #expect(segments[1].atoms.first?.text.hasPrefix("Anyway") == true)
    }

    @Test("Discourse marker prefix does not false-positive on common words")
    func discourseMarkerWordBoundary() {
        // Use text WITHOUT sentence-ending punctuation to isolate the discourse marker check
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10, text: "First part here"),
            // 0.6s gap but "somebody" should NOT trigger "so" marker
            makeAtom(ordinal: 1, startTime: 10.6, endTime: 20, text: "Somebody told me about this"),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        // Should stay as one segment — "somebody" is not the discourse marker "so"
        #expect(segments.count == 1)
    }

    @Test("Sentence punctuation triggers soft break when min duration met")
    func sentencePunctuationBreak() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 8, text: "This is a good topic."),
            makeAtom(ordinal: 1, startTime: 8, endTime: 11, text: "End of thought."),
            // 0.4s gap + sentence punctuation + segment > 10s minDuration
            makeAtom(ordinal: 2, startTime: 11.4, endTime: 20, text: "New thought here."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms, config: config)

        #expect(segments.count == 2)
        #expect(segments[0].atoms.count == 2)
        #expect(segments[1].firstAtomOrdinal == 2)
    }

    @Test("Sentence punctuation does not split when continuation is lowercase")
    func sentencePunctuationRequiresCapitalization() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 8, text: "This is a good topic."),
            makeAtom(ordinal: 1, startTime: 8, endTime: 11, text: "End of thought."),
            makeAtom(ordinal: 2, startTime: 11.4, endTime: 20, text: "and this keeps the same thought going."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms, config: config)

        #expect(segments.count == 1)
    }

    @Test("Segment indices are sequential")
    func sequentialIndices() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 5),
            makeAtom(ordinal: 1, startTime: 8, endTime: 13), // 3s gap
            makeAtom(ordinal: 2, startTime: 16, endTime: 21), // 3s gap
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        for (i, seg) in segments.enumerated() {
            #expect(seg.segmentIndex == i)
        }
    }

    @Test("Min duration prevents micro-segments from soft breaks")
    func minDurationPreventsFragments() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        // Sentence punctuation + 0.4s gap at 3s — below minSegmentDuration
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 3, text: "Short sentence."),
            makeAtom(ordinal: 1, startTime: 3.4, endTime: 8, text: "Still same segment."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms, config: config)

        #expect(segments.count == 1)
    }

    @Test("Speaker change triggers soft break when stable clusters differ")
    func speakerChangeBreak() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 6, text: "we are still talking here"),
            makeAtom(ordinal: 1, startTime: 6, endTime: 12, text: "same speaker keeps going"),
            makeAtom(ordinal: 2, startTime: 12, endTime: 18, text: "different voice starts now"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 0, endTime: 4, speakerClusterId: 1),
            makeFeatureWindow(startTime: 4, endTime: 8, speakerClusterId: 1),
            makeFeatureWindow(startTime: 8, endTime: 12, speakerClusterId: 1),
            makeFeatureWindow(startTime: 12, endTime: 15, speakerClusterId: 2),
            makeFeatureWindow(startTime: 15, endTime: 18, speakerClusterId: 2),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 2)
        #expect(segments[0].startAtomOrdinal == 0)
        #expect(segments[0].endAtomOrdinal == 1)
        #expect(segments[1].startAtomOrdinal == 2)
        #expect(segments[1].boundaryReason == .speakerTurn)
        #expect(segments[1].boundaryConfidence >= 0.8)
        #expect(segments[1].segmentType == .speech)
    }

    @Test("Speaker change respects min segment duration to avoid micro segments")
    func speakerChangeRespectsMinDuration() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 4, text: "short opening"),
            makeAtom(ordinal: 1, startTime: 4, endTime: 8, text: "new speaker starts"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 0, endTime: 4, speakerClusterId: 1),
            makeFeatureWindow(startTime: 4, endTime: 8, speakerClusterId: 2),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 1)
        #expect(segments[0].atoms.count == 2)
    }

    @Test("High pause probability window triggers a hard break without a literal atom gap")
    func featurePauseProbabilityBreak() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 6, text: "first thought continues"),
            makeAtom(ordinal: 1, startTime: 6, endTime: 12, text: "new topic starts here"),
            makeAtom(ordinal: 2, startTime: 12, endTime: 18, text: "same topic continues"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 0, endTime: 4, pauseProbability: 0.1, speakerClusterId: 1),
            makeFeatureWindow(startTime: 4, endTime: 8, pauseProbability: 0.9, speakerClusterId: 1),
            makeFeatureWindow(startTime: 8, endTime: 12, pauseProbability: 0.1, speakerClusterId: 1),
            makeFeatureWindow(startTime: 12, endTime: 18, pauseProbability: 0.1, speakerClusterId: 1),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 2)
        #expect(segments[0].startAtomOrdinal == 0)
        #expect(segments[0].endAtomOrdinal == 0)
        #expect(segments[1].startAtomOrdinal == 1)
        #expect(segments[1].endAtomOrdinal == 2)
        #expect(segments[1].boundaryReason == .pause)
        #expect(segments[1].boundaryConfidence >= 0.9)
        #expect(segments[1].segmentType == .speech)
    }

    @Test("Every atom appears exactly once across emitted segments")
    func exactAtomCoverage() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 5, text: "intro"),
            makeAtom(ordinal: 1, startTime: 5, endTime: 10, text: "still intro."),
            makeAtom(ordinal: 2, startTime: 13, endTime: 18, text: "new section"),
            makeAtom(ordinal: 3, startTime: 18.6, endTime: 24, text: "Anyway sponsor break"),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)
        let flattenedOrdinals = segments.flatMap(\.atoms).map(\.atomKey.atomOrdinal)

        #expect(flattenedOrdinals == atoms.map(\.atomKey.atomOrdinal))
    }
}

// MARK: - TranscriptQualityEstimator Tests

@Suite("TranscriptQualityEstimator")
struct TranscriptQualityEstimatorTests {

    private func makeSegment(
        text: String,
        startTime: Double = 0,
        duration: Double = 30,
        segmentIndex: Int = 0
    ) -> AdTranscriptSegment {
        // Create enough atoms to cover the duration
        let wordsPerAtom = 10
        let words = text.split(whereSeparator: \.isWhitespace)
        let atomCount = max(1, words.count / wordsPerAtom)
        let atomDuration = duration / Double(atomCount)

        let atoms = (0..<atomCount).map { i in
            let atomStart = startTime + Double(i) * atomDuration
            let atomEnd = atomStart + atomDuration
            let wordSlice = words[
                min(i * wordsPerAtom, words.count)..<min((i + 1) * wordsPerAtom, words.count)
            ]
            return makeAtom(
                ordinal: i,
                startTime: atomStart,
                endTime: atomEnd,
                text: wordSlice.joined(separator: " ")
            )
        }

        return AdTranscriptSegment(atoms: atoms, segmentIndex: segmentIndex)
    }

    @Test("Good quality transcript scores as good with reasonable signals")
    func goodQuality() {
        // Natural speech with punctuation, normal word density (~2.5 wps)
        let text = "Welcome back to the show. Today we have a really exciting guest. She has been working in artificial intelligence for over ten years. Let me introduce Doctor Sarah Chen."
        // 30 words in 12 seconds ≈ 2.5 wps (optimal range)
        let segment = makeSegment(text: text, duration: 12)

        let assessment = TranscriptQualityEstimator.assess(segment: segment)

        #expect(assessment.quality == .good)
        #expect(assessment.qualityScore == assessment.compositeScore)
        #expect(assessment.compositeScore >= 0.65)
        #expect(assessment.punctuationScore > 0.3)
        #expect(assessment.tokenDensityScore > 0.5)
        #expect(assessment.wordLengthScore > 0.5)
    }

    @Test("Repetitive text scores as degraded or unusable")
    func repetitiveText() {
        // ASR hallucination pattern — extreme repetition
        let text = (0..<30).map { _ in "the the the the" }.joined(separator: " ")
        let segment = makeSegment(text: text, duration: 30)

        let assessment = TranscriptQualityEstimator.assess(segment: segment)

        #expect(assessment.quality != .good)
        #expect(assessment.repetitionScore < 0.5)
    }

    @Test("Garbled text scores as unusable")
    func unusableQuality() {
        // Simulate garbled ASR: no punctuation, uniform short words, wrong density
        let text = (0..<80).map { _ in "xx" }.joined(separator: " ")
        // 80 words of "xx" in 5 seconds = 16 wps (way too fast)
        let segment = makeSegment(text: text, duration: 5)

        let assessment = TranscriptQualityEstimator.assess(segment: segment)

        #expect(assessment.quality == .unusable)
        #expect(assessment.compositeScore < 0.35)
    }

    @Test("OOV-like noise does not score as good")
    func oovLikeNoiseDoesNotScoreAsGood() {
        let clean = makeSegment(
            text: "This conversation stays coherent and natural with complete sentences and clear transitions between each part of the discussion.",
            duration: 12
        )
        let noisy = makeSegment(
            text: "brxq9 tzzk4 qlmn8 vvvr rrrr zyxw7 pttt3 kqrx8 blorf99 snnn qrxl5 mmmn.",
            duration: 12
        )

        let cleanAssessment = TranscriptQualityEstimator.assess(segment: clean)
        let noisyAssessment = TranscriptQualityEstimator.assess(segment: noisy)

        #expect(noisyAssessment.quality != .good)
        #expect(noisyAssessment.qualityScore < cleanAssessment.qualityScore)
        #expect(noisyAssessment.wordLengthScore < cleanAssessment.wordLengthScore)
    }

    @Test("Bad region ranks below clean region when density matches")
    func badRegionRanksBelowCleanRegionWhenDensityMatches() {
        let clean = makeSegment(
            text: "We are explaining the topic clearly with normal phrasing and enough punctuation to keep the transcript easy to follow throughout.",
            duration: 14
        )
        let noisy = makeSegment(
            text: "mrrp qzxv9 blrtt snnn qqqq vvvv tktk4 rxxm zplk9 and uh qrrt mnop7 tsss.",
            duration: 14
        )

        let cleanAssessment = TranscriptQualityEstimator.assess(segment: clean)
        let noisyAssessment = TranscriptQualityEstimator.assess(segment: noisy)

        #expect(noisyAssessment.qualityScore < cleanAssessment.qualityScore)
        #expect(noisyAssessment.quality != .good)
        #expect(noisyAssessment.confidenceProxyScore < cleanAssessment.confidenceProxyScore)
    }

    @Test("Batch assess processes all segments with correct indices")
    func batchAssess() {
        let segments = (0..<3).map { i in
            makeSegment(text: "Segment \(i) has some text. It is fine.", duration: 15, segmentIndex: i)
        }

        let assessments = TranscriptQualityEstimator.assess(segments: segments)

        #expect(assessments.count == 3)
        for (i, assessment) in assessments.enumerated() {
            #expect(assessment.segmentIndex == i)
            #expect(assessment.compositeScore >= 0.0)
            #expect(assessment.compositeScore <= 1.0)
        }
    }

    @Test("Composite score is bounded 0-1 for extreme inputs")
    func compositeBounded() {
        // Normal text
        let normal = makeSegment(text: "Test text here.", duration: 10)
        let a1 = TranscriptQualityEstimator.assess(segment: normal)
        #expect(a1.compositeScore >= 0.0)
        #expect(a1.compositeScore <= 1.0)

        // Empty-ish text
        let minimal = makeSegment(text: "hi", duration: 1)
        let a2 = TranscriptQualityEstimator.assess(segment: minimal)
        #expect(a2.compositeScore >= 0.0)
        #expect(a2.compositeScore <= 1.0)

        // Very long repetitive text
        let long = makeSegment(text: (0..<200).map { _ in "word" }.joined(separator: " "), duration: 60)
        let a3 = TranscriptQualityEstimator.assess(segment: long)
        #expect(a3.compositeScore >= 0.0)
        #expect(a3.compositeScore <= 1.0)
    }
}

// MARK: - TranscriptChunk Migration Tests

@Suite("TranscriptChunk Schema Migration")
struct TranscriptChunkMigrationTests {

    @Test("New columns persist through insert and fetch")
    func newColumnsRoundTrip() async throws {
        let store = try await makeTestStore()

        let asset = AnalysisAsset(
            id: "asset-migration", episodeId: "ep-1",
            assetFingerprint: "fp", weakFingerprint: nil,
            sourceURL: "file:///test.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "running", analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let chunk = TranscriptChunk(
            id: "chunk-1", analysisAssetId: "asset-migration",
            segmentFingerprint: "fp-chunk-1", chunkIndex: 0,
            startTime: 0, endTime: 10,
            text: "hello world", normalizedText: "hello world",
            pass: "final", modelVersion: "v1",
            transcriptVersion: "abc123def456",
            atomOrdinal: 42
        )
        try await store.insertTranscriptChunk(chunk)

        let fetched = try await store.fetchTranscriptChunks(assetId: "asset-migration")
        #expect(fetched.count == 1)
        #expect(fetched[0].transcriptVersion == "abc123def456")
        #expect(fetched[0].atomOrdinal == 42)
    }

    @Test("ALTER TABLE migration adds columns to pre-existing database")
    func alterTableMigration() async throws {
        // Simulate a database created by an older app version without the new columns.
        let dir = try makeTempDir(prefix: "MigrationTest")
        let dbURL = dir.appendingPathComponent("analysis.sqlite")

        // Create old-schema database directly
        var db: OpaquePointer?
        let rc = sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nil)
        #expect(rc == SQLITE_OK)

        // Create the old transcript_chunks table WITHOUT the new columns
        let oldDDL = """
            CREATE TABLE analysis_assets (
                id TEXT PRIMARY KEY, episodeId TEXT NOT NULL,
                assetFingerprint TEXT NOT NULL, weakFingerprint TEXT,
                sourceURL TEXT NOT NULL, featureCoverageEndTime REAL,
                fastTranscriptCoverageEndTime REAL, confirmedAdCoverageEndTime REAL,
                analysisState TEXT NOT NULL DEFAULT 'new',
                analysisVersion INTEGER NOT NULL DEFAULT 1,
                capabilitySnapshot TEXT,
                createdAt REAL NOT NULL DEFAULT (strftime('%s', 'now'))
            );
            CREATE TABLE transcript_chunks (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                segmentFingerprint TEXT NOT NULL, chunkIndex INTEGER NOT NULL,
                startTime REAL NOT NULL, endTime REAL NOT NULL,
                text TEXT NOT NULL, normalizedText TEXT NOT NULL,
                pass TEXT NOT NULL DEFAULT 'fast', modelVersion TEXT NOT NULL
            );
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL) VALUES ('a1', 'ep', 'fp', 'url');
            INSERT INTO transcript_chunks (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime, text, normalizedText, modelVersion)
            VALUES ('old-chunk', 'a1', 'fp-old', 0, 0.0, 10.0, 'old text', 'old text', 'v0');
            """
        var errMsg: UnsafeMutablePointer<CChar>?
        sqlite3_exec(db, oldDDL, nil, nil, &errMsg)
        sqlite3_close(db)

        // Now open via AnalysisStore which runs migration
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        // Old row should have nil for new columns
        let fetched = try await store.fetchTranscriptChunks(assetId: "a1")
        #expect(fetched.count == 1)
        #expect(fetched[0].id == "old-chunk")
        #expect(fetched[0].transcriptVersion == nil)
        #expect(fetched[0].atomOrdinal == nil)

        // New row with non-nil values should round-trip
        let newChunk = TranscriptChunk(
            id: "new-chunk", analysisAssetId: "a1",
            segmentFingerprint: "fp-new", chunkIndex: 1,
            startTime: 10, endTime: 20,
            text: "new text", normalizedText: "new text",
            pass: "final", modelVersion: "v1",
            transcriptVersion: "version-hash",
            atomOrdinal: 7
        )
        try await store.insertTranscriptChunk(newChunk)

        let all = try await store.fetchTranscriptChunks(assetId: "a1")
        #expect(all.count == 2)
        let newRow = all.first { $0.id == "new-chunk" }!
        #expect(newRow.transcriptVersion == "version-hash")
        #expect(newRow.atomOrdinal == 7)

        try? FileManager.default.removeItem(at: dir)
    }

    @Test("Nil values for new columns on fast-pass chunks")
    func nilColumnsRoundTrip() async throws {
        let store = try await makeTestStore()

        let asset = AnalysisAsset(
            id: "asset-nil", episodeId: "ep-2",
            assetFingerprint: "fp2", weakFingerprint: nil,
            sourceURL: "file:///test.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "running", analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let chunk = TranscriptChunk(
            id: "chunk-nil", analysisAssetId: "asset-nil",
            segmentFingerprint: "fp-nil", chunkIndex: 0,
            startTime: 0, endTime: 10,
            text: "fast pass text", normalizedText: "fast pass text",
            pass: "fast", modelVersion: "v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
        try await store.insertTranscriptChunk(chunk)

        let fetched = try await store.fetchTranscriptChunks(assetId: "asset-nil")
        #expect(fetched.count == 1)
        #expect(fetched[0].transcriptVersion == nil)
        #expect(fetched[0].atomOrdinal == nil)
    }
}
