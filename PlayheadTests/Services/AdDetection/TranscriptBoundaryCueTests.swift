// TranscriptBoundaryCueTests.swift
// playhead-kgby: Coverage for the transcript-aware boundary cue.
//
// Three layers under test:
//   1. `TranscriptBoundaryCueBuilder.buildHits` — chunk-to-hit extraction.
//   2. `TimeBoundaryResolver.scoredCandidates` / `snap` with non-empty
//      `transcriptHits` — proximity decay, cue-blend integration.
//   3. `BoundaryRefiner.computeAdjustments` — gate behaviour: legacy
//      output when transcript hits are absent, transcript-aware output
//      when present.

import Foundation
import Testing

@testable import Playhead

@Suite("TranscriptBoundaryCueBuilder")
struct TranscriptBoundaryCueBuilderTests {

    @Test("empty chunks yield empty hits")
    func emptyChunks() {
        let hits = TranscriptBoundaryCueBuilder.buildHits(from: [])
        #expect(hits.isEmpty)
    }

    @Test("chunk without sentence terminators yields no hits")
    func unpunctuatedChunkProducesNoHits() {
        let chunk = makeChunk(
            startTime: 0,
            endTime: 8,
            text: "this is a chunk with no punctuation at all"
        )
        let hits = TranscriptBoundaryCueBuilder.buildHits(from: [chunk])
        #expect(hits.isEmpty)
    }

    @Test("each terminator produces one hit")
    func terminatorsProduceHits() throws {
        // 6 words across 3 seconds = 2 wps (within natural-speech band)
        // and 3 terminators / 6 words is high but not pathological — well
        // above the chunk-quality gate so the apportioned hits reach the
        // caller.
        let chunk = makeChunk(
            startTime: 100,
            endTime: 103,
            text: "First sentence. Second sentence? Third sentence!"
        )
        let hits = TranscriptBoundaryCueBuilder.buildHits(from: [chunk])
        try #require(hits.count == 3)
        #expect(hits.contains(where: { $0.terminalKind == .period }))
        #expect(hits.contains(where: { $0.terminalKind == .question }))
        #expect(hits.contains(where: { $0.terminalKind == .exclamation }))

        // All hits must fall inside the chunk's time interval.
        for hit in hits {
            #expect(hit.time >= chunk.startTime)
            #expect(hit.time <= chunk.endTime)
        }

        // Hits are sorted by time ascending.
        let times = hits.map(\.time)
        #expect(times == times.sorted())
    }

    @Test("hit time scales linearly with character offset")
    func hitTimeApportionment() throws {
        // Build a chunk with two terminators and a natural-speech word
        // density (~2.5 wps) so it survives the quality gate. The point
        // of this test is the apportionment math, not the gate.
        // 8 words across 3.2 seconds = 2.5 wps; one terminator per ~4 words
        // matches a healthy punctuation ratio.
        let chunk = makeChunk(
            startTime: 0,
            endTime: 3.2,
            text: "Stop here now. Keep going forward please."
        )
        let hits = TranscriptBoundaryCueBuilder.buildHits(from: [chunk])
        // Recompute expected positions from the actual character offsets so
        // the assertion stays robust to text edits above.
        let total = Double(chunk.text.count)
        let firstFraction = Double(chunk.text.distance(
            from: chunk.text.startIndex,
            to: chunk.text.firstIndex(of: ".")!
        ) + 1) / total
        let lastFraction = Double(chunk.text.distance(
            from: chunk.text.startIndex,
            to: chunk.text.lastIndex(of: ".")!
        ) + 1) / total

        try #require(hits.count == 2)
        #expect(abs(hits[0].time - (chunk.startTime + firstFraction * (chunk.endTime - chunk.startTime))) < 0.001)
        #expect(abs(hits[1].time - (chunk.startTime + lastFraction * (chunk.endTime - chunk.startTime))) < 0.001)
    }

    @Test("low-quality chunk is suppressed")
    func lowQualityChunkSuppressed() {
        // 0.4 wps (way under the natural-speech 1.5 wps floor) and one
        // word per terminator — synthetic "garbled" output. The builder's
        // chunk-quality gate should drop this entirely.
        let chunk = makeChunk(
            startTime: 0,
            endTime: 60,
            text: "yes. ok. no. eh. uh."
        )
        let hits = TranscriptBoundaryCueBuilder.buildHits(from: [chunk])
        #expect(hits.isEmpty,
                "expected low-quality chunk (very low word density) to be dropped, got \(hits.count) hits")
    }

    @Test("hits across multiple chunks are merged and sorted")
    func multipleChunks() {
        let early = makeChunk(
            startTime: 0,
            endTime: 6,
            text: "Quick first chunk with a period."
        )
        let later = makeChunk(
            startTime: 100,
            endTime: 110,
            text: "Later chunk follows. Another sentence here."
        )

        // Pass them out of order to confirm the builder sorts the result.
        let hits = TranscriptBoundaryCueBuilder.buildHits(from: [later, early])

        // Must be sorted ascending by time across chunks.
        let times = hits.map(\.time)
        #expect(times == times.sorted())
        // Must contain both chunks' contributions. The early-chunk hit
        // lands inside [early.startTime, early.endTime] (i.e. ≤ 6); the
        // later-chunk hits land inside [later.startTime, later.endTime]
        // (i.e. ≥ 100).
        #expect(hits.contains(where: { $0.time <= early.endTime }))
        #expect(hits.contains(where: { $0.time >= later.startTime }))
    }

    @Test("zero-duration or empty-text chunk yields no hits")
    func degenerateChunks() {
        let zeroDuration = makeChunk(startTime: 50, endTime: 50, text: "Sentence ends.")
        let emptyText = makeChunk(startTime: 0, endTime: 5, text: "")
        let hits = TranscriptBoundaryCueBuilder.buildHits(from: [zeroDuration, emptyText])
        #expect(hits.isEmpty)
    }

    // MARK: - Helpers

    private func makeChunk(
        startTime: Double,
        endTime: Double,
        text: String
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "chunk-\(startTime)",
            analysisAssetId: "asset-test",
            segmentFingerprint: "fp",
            chunkIndex: 0,
            startTime: startTime,
            endTime: endTime,
            text: text,
            normalizedText: text.lowercased(),
            pass: "final",
            modelVersion: "v1",
            transcriptVersion: "vv",
            atomOrdinal: 0
        )
    }
}

// MARK: - Resolver Integration

@Suite("TimeBoundaryResolver+TranscriptCue")
struct TimeBoundaryResolverTranscriptCueTests {

    private let resolver = TimeBoundaryResolver()

    @Test("transcript hits with zero weight do not move the snap")
    func zeroWeightIsBitIdentical() {
        let windows = makeUniformWindows()
        let hits = [
            TranscriptBoundaryHit(time: 102, confidence: 1.0, terminalKind: .period)
        ]

        // Default config has transcriptBoundary weight 0 — the new cue
        // exists in the data structures but contributes nothing to the
        // blend. Confirms the backward-compatibility contract.
        let withHits = resolver.snap(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            transcriptHits: hits
        )
        let withoutHits = resolver.snap(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            transcriptHits: []
        )

        #expect(withHits == withoutHits)
    }

    @Test("transcript cue contributes proportional to confidence and proximity")
    func transcriptCueContribution() throws {
        let windows = [
            makeWindow(start: 99, end: 101, pause: 0.1, spectralFlux: 0.05),
            makeWindow(start: 101.5, end: 103.5, pause: 0.1, spectralFlux: 0.05),
            makeWindow(start: 105, end: 107, pause: 0.1, spectralFlux: 0.05),
        ]
        // Place a strong sentence terminal exactly at window 2's start
        // boundary (101.5). With transcript weight 0.5 and confidence 1.0,
        // the cue should carry that window above the others.
        let hits = [
            TranscriptBoundaryHit(time: 101.5, confidence: 1.0, terminalKind: .period)
        ]
        let config = BoundarySnappingConfig(
            startWeights: StartBoundaryCueWeights(
                pauseVAD: 0.2,
                speakerChangeProxy: 0.1,
                musicBedChange: 0.1,
                spectralChange: 0.1,
                lexicalDensityDelta: 0.0,
                transcriptBoundary: 0.5
            ),
            endWeights: EndBoundaryCueWeights(
                pauseVAD: 0.4,
                speakerChangeProxy: 0.1,
                musicBedChange: 0.1,
                spectralChange: 0.1,
                explicitReturnMarker: 0.3
            ),
            lambda: 0.05,
            minBoundaryScore: 0.2,
            minImprovementOverOriginal: -0.5
        )

        let scored = resolver.scoredCandidates(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            transcriptHits: hits,
            config: config
        )
        let onTerminal = try #require(scored.first(where: { $0.boundaryTime == 101.5 }))
        let offTerminal = try #require(scored.first(where: { $0.boundaryTime == 99 }))
        #expect(onTerminal.transcriptBoundary == 1.0)
        #expect(offTerminal.transcriptBoundary == 0.0)
        #expect(onTerminal.score > offTerminal.score)
    }

    @Test("low-confidence transcript hit attenuates the cue")
    func lowConfidenceAttenuates() {
        let windows = [
            makeWindow(start: 99, end: 101, pause: 0.1, spectralFlux: 0.05),
            makeWindow(start: 101.5, end: 103.5, pause: 0.1, spectralFlux: 0.05),
        ]
        let hits = [
            TranscriptBoundaryHit(time: 101.5, confidence: 0.10, terminalKind: .period)
        ]
        let config = BoundarySnappingConfig(
            startWeights: StartBoundaryCueWeights(
                pauseVAD: 0.4,
                speakerChangeProxy: 0.1,
                musicBedChange: 0.1,
                spectralChange: 0.1,
                lexicalDensityDelta: 0.0,
                transcriptBoundary: 0.3
            ),
            endWeights: EndBoundaryCueWeights(
                pauseVAD: 0.4,
                speakerChangeProxy: 0.1,
                musicBedChange: 0.1,
                spectralChange: 0.1,
                explicitReturnMarker: 0.3
            ),
            lambda: 0.05,
            minBoundaryScore: 0.2,
            minImprovementOverOriginal: -0.5
        )

        let scored = resolver.scoredCandidates(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            transcriptHits: hits,
            config: config
        )
        // The transcriptBoundary value at the on-hit window equals the
        // confidence (0.10), not 1.0 — so the cue is attenuated 10×.
        if let onHit = scored.first(where: { $0.boundaryTime == 101.5 }) {
            #expect(abs(onHit.transcriptBoundary - 0.10) < 0.001)
        } else {
            Issue.record("expected scored candidate at 101.5")
        }
    }

    @Test("hits beyond hit radius do not contribute")
    func hitsBeyondRadiusContributeZero() {
        let windows = [
            makeWindow(start: 99, end: 101, pause: 0.1, spectralFlux: 0.05),
            makeWindow(start: 101.5, end: 103.5, pause: 0.1, spectralFlux: 0.05),
        ]
        // Hit 5s away from any candidate window's boundary — well outside
        // the default 1.5s radius.
        let hits = [
            TranscriptBoundaryHit(time: 110, confidence: 1.0, terminalKind: .period)
        ]
        let config = BoundarySnappingConfig(
            startWeights: StartBoundaryCueWeights(
                pauseVAD: 0.4,
                speakerChangeProxy: 0.1,
                musicBedChange: 0.1,
                spectralChange: 0.1,
                lexicalDensityDelta: 0.0,
                transcriptBoundary: 0.3
            ),
            endWeights: EndBoundaryCueWeights(
                pauseVAD: 0.4,
                speakerChangeProxy: 0.1,
                musicBedChange: 0.1,
                spectralChange: 0.1,
                explicitReturnMarker: 0.3
            ),
            lambda: 0.05,
            minBoundaryScore: 0.2,
            minImprovementOverOriginal: -0.5
        )

        let scored = resolver.scoredCandidates(
            candidateTime: 100,
            boundaryType: .start,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            transcriptHits: hits,
            config: config
        )
        for candidate in scored {
            #expect(candidate.transcriptBoundary == 0)
        }
    }

    @Test("end-boundary cue uses end-side weight")
    func endBoundaryUsesEndWeights() throws {
        let windows = [
            makeBoundaryWindow(boundaryTime: 152, boundaryType: .end),
            makeBoundaryWindow(boundaryTime: 154, boundaryType: .end),
        ]
        let hits = [
            TranscriptBoundaryHit(time: 154, confidence: 1.0, terminalKind: .question)
        ]
        let config = BoundarySnappingConfig(
            startWeights: StartBoundaryCueWeights(
                pauseVAD: 0.5,
                speakerChangeProxy: 0.1,
                musicBedChange: 0.1,
                spectralChange: 0.1,
                lexicalDensityDelta: 0.2,
                transcriptBoundary: 0.0
            ),
            endWeights: EndBoundaryCueWeights(
                pauseVAD: 0.2,
                speakerChangeProxy: 0.1,
                musicBedChange: 0.1,
                spectralChange: 0.1,
                explicitReturnMarker: 0.0,
                transcriptBoundary: 0.5
            ),
            lambda: 0.05,
            minBoundaryScore: 0.2,
            minImprovementOverOriginal: -0.5
        )

        let scored = resolver.scoredCandidates(
            candidateTime: 151.5,
            boundaryType: .end,
            anchorType: .fmPositive,
            featureWindows: windows,
            lexicalHits: [],
            transcriptHits: hits,
            config: config
        )
        let onTerminal = try #require(scored.first(where: { $0.boundaryTime == 154 }))
        let offTerminal = try #require(scored.first(where: { $0.boundaryTime == 152 }))
        #expect(onTerminal.transcriptBoundary == 1.0)
        #expect(offTerminal.transcriptBoundary == 0.0)
    }

    // MARK: - Helpers

    private func makeUniformWindows() -> [FeatureWindow] {
        (0..<5).map { i in
            let s = Double(99 + i)
            return makeWindow(start: s, end: s + 1, pause: 0.1, spectralFlux: 0.05)
        }
    }

    private func makeWindow(
        start: Double,
        end: Double,
        pause: Double,
        spectralFlux: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-kgby",
            startTime: start,
            endTime: end,
            rms: 0.1,
            spectralFlux: spectralFlux,
            musicProbability: 0,
            speakerChangeProxyScore: 0,
            musicBedChangeScore: 0,
            pauseProbability: pause,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 4
        )
    }

    private func makeBoundaryWindow(
        boundaryTime: Double,
        boundaryType: BoundaryType
    ) -> FeatureWindow {
        switch boundaryType {
        case .start:
            return makeWindow(start: boundaryTime, end: boundaryTime + 1, pause: 0.5, spectralFlux: 0.5)
        case .end:
            return makeWindow(start: boundaryTime - 1, end: boundaryTime, pause: 0.5, spectralFlux: 0.5)
        }
    }
}

// MARK: - BoundaryRefiner Integration

@Suite("BoundaryRefiner+TranscriptCue")
struct BoundaryRefinerTranscriptCueTests {

    @Test("empty transcript hits produce legacy output (bit-identical)")
    func emptyHitsAreBitIdentical() {
        let windows = makeWindows()
        let legacy = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 9.5,
            candidateEnd: 20.5
        )
        let withEmptyHits = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 9.5,
            candidateEnd: 20.5,
            transcriptHits: []
        )
        #expect(legacy.startAdjust == withEmptyHits.startAdjust)
        #expect(legacy.endAdjust == withEmptyHits.endAdjust)
    }

    @Test("transcript hit can shift boundary toward sentence terminal")
    func transcriptHitInfluencesBoundary() {
        // Geometry: candidate at 10 sits between two acoustic peaks —
        // a strong one at window-start 8 (pause 0.50) and a weaker one
        // at window-start 12 (pause 0.40). Legacy weights make 8 the
        // only candidate that clears the 0.50 minBoundaryScore floor,
        // so the snap lands at 8 (adjustment -2). When a strong sentence
        // terminal hit lands on window-start 12, the transcript-aware
        // weights (pauseVAD 0.70 + transcriptBoundary 0.20) pull 12
        // *above* 8 (which loses pauseVAD weight to make room for the
        // transcript term and now falls below 0.50), so the snap moves
        // to 12 (adjustment +2). The two adjustments differ — proving
        // the transcript cue is non-inert in the live config.
        let windows: [FeatureWindow] = [
            makeWindow(start: 7,  end: 8,  pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 8,  end: 9,  pause: 0.50, spectralFlux: 0.50),
            makeWindow(start: 9,  end: 10, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 10, end: 11, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 11, end: 12, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 12, end: 13, pause: 0.40, spectralFlux: 0.40),
            makeWindow(start: 13, end: 14, pause: 0.05, spectralFlux: 0.05),
        ]
        let hits = [
            TranscriptBoundaryHit(time: 12.0, confidence: 1.0, terminalKind: .period)
        ]

        let withCue = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 10.0,
            candidateEnd: 30.0,
            transcriptHits: hits
        )
        let withoutCue = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 10.0,
            candidateEnd: 30.0,
            transcriptHits: []
        )

        #expect(withCue.startAdjust != withoutCue.startAdjust,
                "expected transcript cue to alter start adjustment; got withCue=\(withCue.startAdjust), withoutCue=\(withoutCue.startAdjust)")
    }

    @Test("transcript cue cannot move boundary beyond ±3s")
    func transcriptCueRespectsClamp() {
        // Hit far outside the snap radius — even with the cue, the
        // refiner must stay clamped within ±3s.
        let windows: [FeatureWindow] = [
            makeWindow(start: 6, end: 7, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 7, end: 8, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
        ]
        let hits = [
            TranscriptBoundaryHit(time: 50, confidence: 1.0, terminalKind: .period)
        ]
        let result = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 9.5,
            candidateEnd: 30,
            transcriptHits: hits
        )
        #expect(abs(result.startAdjust) <= 3.0)
        #expect(abs(result.endAdjust) <= 3.0)
    }

    @Test("transcript cue is no-op when fewer than 3 windows")
    func tooFewWindowsBypass() {
        let windows: [FeatureWindow] = [
            makeWindow(start: 7, end: 8, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
        ]
        let hits = [
            TranscriptBoundaryHit(time: 8.5, confidence: 1.0, terminalKind: .period)
        ]
        let result = BoundaryRefiner.computeAdjustments(
            windows: windows,
            candidateStart: 8.5,
            candidateEnd: 8.5,
            transcriptHits: hits
        )
        #expect(result.startAdjust == 0)
        #expect(result.endAdjust == 0)
    }

    // MARK: - Helpers

    private func makeWindows() -> [FeatureWindow] {
        [
            makeWindow(start: 7, end: 8, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 8, end: 9, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 9, end: 10, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 19, end: 20, pause: 0.05, spectralFlux: 0.05),
            makeWindow(start: 20, end: 21, pause: 0.95, spectralFlux: 0.95),
            makeWindow(start: 21, end: 22, pause: 0.05, spectralFlux: 0.05),
        ]
    }

    private func makeWindow(
        start: Double,
        end: Double,
        pause: Double,
        spectralFlux: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-kgby-refiner",
            startTime: start,
            endTime: end,
            rms: 0.05,
            spectralFlux: spectralFlux,
            musicProbability: 0,
            speakerChangeProxyScore: 0,
            musicBedChangeScore: 0,
            pauseProbability: pause,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }
}
