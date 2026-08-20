// FastFinalTwinEvidenceCanaryTests.swift
//
// playhead-99yt — a fast/final twin is ONE observation, and the consumers
// that count observations must say so.
//
// THE DEFECT. `AnalysisStore.fetchTranscriptChunks(assetId:)` returns every
// row for an asset, both passes. When the final pass re-transcribes audio the
// fast pass already covered, the store holds two rows over one utterance —
// 3,751 such pairs on the 2026-08-15 device pull, every one of them an
// EXACT-span pair with byte-identical `text`. Those rows are DESIGN-INTENDED
// and playhead-jc42's V53 migration deliberately keeps them: the two coverage
// unions read them separately, and a coverage union is idempotent under
// duplication, so keeping them costs those consumers nothing.
//
// It costs the consumers that COUNT. `LexicalScanner.buildCandidate` refuses
// to emit unless `promotionHitCount >= config.minHitsForCandidate` (2) — a bar
// whose entire purpose is to require two INDEPENDENT hits — and then sums the
// same hits' weights into `rawConfidence = 1 - 1/(1 + totalWeight * 0.3)`. One
// phrase, transcribed twice, clears a bar that one phrase does not, and lands
// with roughly double the weight.
//
// WHAT THESE TESTS PIN, and why each is a separate one. The defect is a
// property of the CONSUMER, not of the scanner: `LexicalScanner` is a pure
// function of the array it is handed and is behaving correctly. So there is
// nothing to fix inside it and nothing to assert about it beyond the first
// test here, which exists to prove the defect is real and that this fixture
// can see it. Every other test names a call site that reads the raw store
// array and asks whether canonicalizing changed its answer.
//
// EACH RAIL CARRIES ITS OWN CONTROL, because the failure mode of a rail like
// this is silent vacuity — a fixture that produces no candidate for reasons
// unrelated to duplication passes the "no phantom candidate" assertion
// forever. playhead-jc42's JC04 is the worked example: its fixture used
// unpunctuated text, which is precisely the input on which the two code paths
// it was distinguishing return the same string. So every "the twin does not
// promote" assertion is paired with a "two genuinely independent hits DO
// promote" assertion over the same code path.
//
// MEASURED, on the nine mixed-pass assets of the 2026-08-15 pull
// (`db-pull12`), through `LexicalScanner.scan` — the lane the hot path's
// lexical fallback uses:
//
//     raw chunks:       44 candidates
//     canonical chunks: 39 candidates
//
// All five removed candidates sat at exactly `hitCount == 2`; a further 15
// survivors carried inflated confidence, worst delta +0.170.

import Foundation
import Testing

@testable import Playhead

@Suite("Fast/final twins are one observation (playhead-99yt)")
struct FastFinalTwinEvidenceCanaryTests {

    // MARK: - Fixture

    /// A single `purchaseLanguage` phrase — weight 0.9, which is BELOW
    /// `highWeightBypassThreshold` (0.95).
    ///
    /// The weight is the fixture's load-bearing choice. A `sponsor` phrase
    /// (1.0) or a literal-TLD URL (0.95) clears `highWeightBypassThreshold` on
    /// its own, so a single hit promotes and the duplication changes nothing
    /// that can be observed — the test would pass for the wrong reason, in
    /// both directions, forever. 0.9 is the highest weight that still requires
    /// the two-hit bar, which makes this the tightest fixture that can see the
    /// defect.
    private static let promotableText = "It comes with a free trial for new listeners"
    private static let promotableNormalized = "it comes with a free trial for new listeners"

    /// A second, DIFFERENT `purchaseLanguage` phrase for the control. Distinct
    /// text so the two hits are genuinely independent evidence rather than the
    /// same evidence twice — which is the whole distinction under test.
    private static let secondPromotableText = "Listeners can sign up today at the link"
    private static let secondPromotableNormalized = "listeners can sign up today at the link"

    private static func chunk(
        id: String,
        assetId: String = "asset-99yt",
        index: Int,
        start: Double,
        end: Double,
        text: String,
        normalized: String,
        pass: String
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: id,
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(id)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: normalized,
            pass: pass,
            modelVersion: "test.v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// The twin: one fast row and one final row over an identical span with
    /// byte-identical text — the shape all 3,751 device-pull pairs have.
    ///
    /// Returned final-first so the array is NOT already in a helpful order; a
    /// consumer that happens to read only `first` must not accidentally pass.
    private static func twinPair(assetId: String = "asset-99yt") -> [TranscriptChunk] {
        [
            chunk(
                id: "final-1", assetId: assetId, index: 900, start: 100, end: 105,
                text: promotableText, normalized: promotableNormalized, pass: "final"
            ),
            chunk(
                id: "fast-1", assetId: assetId, index: 3, start: 100, end: 105,
                text: promotableText, normalized: promotableNormalized, pass: "fast"
            ),
        ]
    }

    /// Two genuinely independent hits, 10 s apart — inside the scanner's
    /// 30 s `mergeGapThreshold`, so they merge into one group and clear the
    /// two-hit bar honestly. Single-pass, so canonicalization is a byte-identical
    /// passthrough and cannot be what makes the control pass.
    private static func independentPair(assetId: String = "asset-99yt") -> [TranscriptChunk] {
        [
            chunk(
                id: "fast-1", assetId: assetId, index: 3, start: 100, end: 105,
                text: promotableText, normalized: promotableNormalized, pass: "fast"
            ),
            chunk(
                id: "fast-2", assetId: assetId, index: 4, start: 110, end: 115,
                text: secondPromotableText, normalized: secondPromotableNormalized,
                pass: "fast"
            ),
        ]
    }

    // MARK: - The defect, and that this fixture can see it

    /// **The fixture is not vacuous, in three separate directions.**
    ///
    /// If any one of these stops holding, every "no phantom candidate"
    /// assertion below becomes unfalsifiable, and would stay green through a
    /// full removal of every canonicalization this bead added.
    @Test("A twin pair clears minHitsForCandidate on evidence one row provides")
    func twinPairClearsTheTwoHitBarUncanonicalized() throws {
        let scanner = LexicalScanner()

        // (1) ONE row of the pair is not enough. This is the answer the
        // consumers should be giving.
        let single = scanner.scan(
            chunks: [Self.twinPair()[0]],
            analysisAssetId: "asset-99yt",
            // playhead-2kxd: this service was built with no profile, so
            // "no show identity" is the same input it always had here.
            podcastId: nil
        )
        #expect(single.isEmpty, """
            one row carrying one weight-0.9 hit must not clear \
            minHitsForCandidate — if it does, the fixture's phrase is being \
            matched more than once or is above the high-weight bypass, and \
            every assertion below is measuring something else
            """)

        // (2) The SAME utterance, as two rows, does clear it. This is the
        // defect, quantified.
        let raw = scanner.scan(chunks: Self.twinPair(), analysisAssetId: "asset-99yt")
        #expect(raw.count == 1)
        let phantom = try #require(raw.first)
        #expect(phantom.hitCount == 2, """
            the duplicated row must contribute a SECOND promotion hit — that \
            is the quantity `minHitsForCandidate` is read as measuring and is \
            not what it counts
            """)

        // (3) The canonicalizer removes exactly the duplicate, and the
        // candidate goes with it.
        let canonicalization = TranscriptChunkCanonicalizer.canonicalize(Self.twinPair())
        #expect(canonicalization.diagnostics.droppedFastCount == 1)
        #expect(canonicalization.diagnostics.residualFastFinalOverlapCount == 0)
        #expect(canonicalization.chunks.count == 1)
        #expect(scanner.scan(
            chunks: canonicalization.chunks,
            analysisAssetId: "asset-99yt",
            // playhead-2kxd: this service was built with no profile, so
            // "no show identity" is the same input it always had here.
            podcastId: nil
        ).isEmpty)

        // (4) Control: two INDEPENDENT hits still promote, through the same
        // canonicalization. Canonicalizing must remove duplicate evidence, not
        // evidence.
        let independent = TranscriptChunkCanonicalizer
            .canonicalize(Self.independentPair()).chunks
        let independentCandidates = scanner.scan(
            chunks: independent,
            analysisAssetId: "asset-99yt",
            // playhead-2kxd: this service was built with no profile, so
            // "no show identity" is the same input it always had here.
            podcastId: nil
        )
        #expect(independentCandidates.count == 1)
        #expect(independentCandidates.first?.hitCount == 2)
    }

    /// The confidence half of the same defect. `promotionHitCount` is the
    /// boolean gate; `totalWeight` is the continuous one, and it is inflated
    /// even when the candidate would have been emitted anyway.
    @Test("Duplicated weight inflates rawConfidence, and canonicalizing undoes it")
    func duplicatedWeightInflatesConfidence() throws {
        let scanner = LexicalScanner()
        // Three rows: an independent pair PLUS a twin of the first, so the
        // group promotes in both worlds and only the confidence moves.
        let withTwin = Self.independentPair() + [
            Self.chunk(
                id: "final-1", index: 900, start: 100, end: 105,
                text: Self.promotableText, normalized: Self.promotableNormalized,
                pass: "final"
            ),
        ]
        let raw = try #require(scanner.scan(
            chunks: withTwin, analysisAssetId: "asset-99yt",
            // playhead-2kxd: this service was built with no profile, so
            // "no show identity" is the same input it always had here.
            podcastId: nil
        ).first)
        let canonical = try #require(scanner.scan(
            chunks: TranscriptChunkCanonicalizer.canonicalize(withTwin).chunks,
            analysisAssetId: "asset-99yt",
            // playhead-2kxd: this service was built with no profile, so
            // "no show identity" is the same input it always had here.
            podcastId: nil
        ).first)

        #expect(raw.hitCount == 3)
        #expect(canonical.hitCount == 2)
        #expect(canonical.confidence < raw.confidence, """
            the third hit is the second row of one utterance; dropping it must \
            lower the score, not leave it alone
            """)
    }

    // MARK: - Consumer: the hot path

    /// **The named consumer.** `AnalysisJobRunner` hands `runHotPath` the raw
    /// `fetchTranscriptChunks` array, and it reaches `LexicalScanner` through
    /// `AdDetectionService.hotPathCandidates`. This is where a listener's
    /// first banner comes from.
    @Test("Hot path does not promote a candidate from a fast/final twin")
    func hotPathIgnoresTheTwin() async throws {
        let store = try await makeTestStore()
        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor()
        )

        let fromTwin = try await service.hotPathCandidatesForTesting(
            from: Self.twinPair(),
            analysisAssetId: "asset-99yt",
            // playhead-2kxd: this service was built with no profile, so
            // "no show identity" is the same input it always had here.
            podcastId: nil
        )
        #expect(fromTwin.isEmpty, """
            one utterance transcribed twice is one observation; the hot path \
            must not emit a candidate that a single row would not have produced
            """)

        // Control, through the identical call: independent evidence promotes.
        let fromIndependent = try await service.hotPathCandidatesForTesting(
            from: Self.independentPair(),
            analysisAssetId: "asset-99yt",
            // playhead-2kxd: this service was built with no profile, so
            // "no show identity" is the same input it always had here.
            podcastId: nil
        )
        #expect(fromIndependent.count == 1)
        #expect(fromIndependent.first?.hitCount == 2)
    }

    // MARK: - Consumer: the user's own "Hearing an ad" widening

    /// `NowPlayingViewModel.reportHearingAd` fetches the raw array and hands it
    /// to `BoundaryExpander.expand`, whose `makeLexicalContext` both scans for
    /// candidates and builds the flat hit stream `BoundaryResolver.snap` reads.
    /// A phantom candidate here moves the edges of a mark the user asked for.
    ///
    /// The assertion is on `source` rather than on the times, because that is
    /// the categorical fact: with a lexical candidate the expander returns
    /// `.acousticAndLexical` and the candidate's own span; without one it falls
    /// back to seed ± a fixed width. The two are different answers to the
    /// user's tap.
    @Test("Boundary expansion from a twin matches expansion from the final row alone")
    func boundaryExpansionIgnoresTheTwin() {
        let expander = BoundaryExpander()
        let seed = 102.0

        let fromTwin = expander.expand(
            seed: seed,
            featureWindows: [],
            transcriptChunks: Self.twinPair(),
            adWindows: []
        )
        let fromFinalOnly = expander.expand(
            seed: seed,
            featureWindows: [],
            transcriptChunks: [Self.twinPair()[0]],
            adWindows: []
        )
        #expect(fromTwin.source == fromFinalOnly.source)
        #expect(fromTwin.startTime == fromFinalOnly.startTime)
        #expect(fromTwin.endTime == fromFinalOnly.endTime)
        #expect(fromTwin.source == .fallback, """
            one weight-0.9 hit is not a lexical candidate, so the expander has \
            no lexical signal and must say so — if this reads \
            .acousticAndLexical the duplication is still promoting
            """)

        // Control: independent evidence DOES give the expander a lexical
        // boundary, so `.fallback` above is a verdict rather than a floor.
        let fromIndependent = expander.expand(
            seed: seed,
            featureWindows: [],
            transcriptChunks: Self.independentPair(),
            adWindows: []
        )
        #expect(fromIndependent.source == .acousticAndLexical)
        #expect(fromIndependent.startTime < fromIndependent.endTime)
    }

    // MARK: - Consumer: the shadow prompts

    /// Both shadow dispatchers join chunk `text` with newlines after sorting by
    /// `canonicalTimeOrder` — which ranks final BEFORE fast at an identical
    /// span, so an un-canonicalized twin repeats the sentence on ADJACENT
    /// lines. That is the position in which a repetition reads as emphasis.
    @Test("Specialist shadow prompt carries each utterance once")
    func specialistShadowPromptDeduplicatesTheTwin() async throws {
        let store = try await makeTestStore()
        try await seedTwinAsset(store: store, assetId: "asset-99yt-prompt")

        let recorder = TwinPromptRecorder()
        let dispatcher = LiveSpecialistShadowDispatcher(
            store: store,
            runtime: SpecialistAdClassifier.Runtime(
                makeSession: {
                    SpecialistAdClassifier.Runtime.Session(
                        classify: { _ in SpecialistVerdict(isAd: false, confidence: 0.1) }
                    )
                }
            ),
            record: recorder.sink
        )
        _ = try await dispatcher.dispatchShadowCall(
            assetId: "asset-99yt-prompt",
            window: ShadowWindow(start: 90, end: 120),
            configVariant: .allEnabledShadow
        )
        let prompt = try #require(recorder.payloads.first?.promptText)
        #expect(prompt == Self.promotableText, """
            the prompt must be the utterance once — got \(prompt.debugDescription)
            """)
        #expect(prompt.components(separatedBy: "\n").count == 1)
    }

    @Test("FM shadow prompt carries each utterance once")
    func foundationModelShadowPromptDeduplicatesTheTwin() async throws {
        let store = try await makeTestStore()
        try await seedTwinAsset(store: store, assetId: "asset-99yt-fm")

        let dispatcher = LiveShadowFMDispatcher(
            store: store,
            runtime: FoundationModelClassifier.Runtime(
                availabilityStatus: { _ in nil },
                contextSize: { 4_096 },
                tokenCount: { prompt in
                    max(1, prompt.split(whereSeparator: \.isWhitespace).count)
                },
                coarseSchemaTokenCount: { 16 },
                refinementSchemaTokenCount: { 32 },
                boundarySchemaTokenCount: { 32 },
                makeSession: {
                    FoundationModelClassifier.Runtime.Session(
                        prewarm: { _ in },
                        respondCoarse: { _ in
                            CoarseScreeningSchema(disposition: .noAds, support: nil)
                        },
                        respondRefinement: { _ in RefinementWindowSchema(spans: []) }
                    )
                }
            ),
            modelVersion: "test-fm.v1"
        )
        let result = try await dispatcher.dispatchShadowCall(
            assetId: "asset-99yt-fm",
            window: ShadowWindow(start: 90, end: 120),
            configVariant: .allEnabledShadow
        )
        let payload = try JSONDecoder().decode(
            ShadowFMPayload.self, from: result.fmResponse
        )
        #expect(payload.promptText == Self.promotableText)
        #expect(payload.promptText.components(separatedBy: "\n").count == 1)
    }

    // MARK: - Consumer: the episode-summary prompt budget

    /// `EpisodeSummarySampler` de-duplicates by `chunk.id` — a per-ROW UUID —
    /// so the twins are two different ids over identical text and BOTH survive
    /// it, each spending one of the 80 prompt slots. Canonicalizing at the
    /// fetch is what makes the budget count utterances.
    @Test("Episode-summary hydration hands the sampler each utterance once")
    func episodeSummaryHydrationDeduplicatesTheTwin() async throws {
        let store = try await makeTestStore()
        try await seedTwinAsset(store: store, assetId: "asset-99yt-summary")

        let provider = AnalysisStoreEpisodeSummaryBackfillCandidateProvider(store: store)
        let input = try #require(try await provider.hydrate(assetId: "asset-99yt-summary"))
        #expect(input.chunks.count == 1, """
            two rows over one utterance must not spend two of the sampler's 80 \
            slots — got \(input.chunks.map(\.pass))
            """)

        // Control: the sampler's own id-dedupe cannot do this, which is why the
        // fix had to be at the fetch. Both rows carry distinct ids.
        let raw = try await store.fetchTranscriptChunks(assetId: "asset-99yt-summary")
        #expect(raw.count == 2)
        #expect(Set(raw.map(\.id)).count == 2)
        #expect(EpisodeSummarySampler.sample(chunks: raw).count == 2, """
            if this ever reads 1 the sampler has started de-duplicating by \
            content and this rail is measuring the wrong thing
            """)
    }

    // MARK: - Seeding

    private func seedTwinAsset(store: AnalysisStore, assetId: String) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "episode-\(assetId)",
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(assetId).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
        for chunk in Self.twinPair(assetId: assetId) {
            try await store.insertTranscriptChunk(chunk)
        }
    }
}

// MARK: - Test helpers (file-scoped)

/// Thread-safe sink capturing every specialist payload the dispatcher records.
private final class TwinPromptRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [SpecialistShadowPayload] = []

    var sink: @Sendable (SpecialistShadowPayload) -> Void {
        { [self] payload in
            lock.lock()
            storage.append(payload)
            lock.unlock()
        }
    }

    var payloads: [SpecialistShadowPayload] {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }
}
