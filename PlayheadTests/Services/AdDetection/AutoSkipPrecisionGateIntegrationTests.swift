// AutoSkipPrecisionGateIntegrationTests.swift
// playhead-gtt9.11: integration tests asserting the precision gate is wired
// into the production hot path and that `SkipOrchestrator` honors the
// `AdWindow.eligibilityGate` stamp.
//
// Unlike the unit tests in AutoSkipPrecisionGateTests, these tests drive
// `AdDetectionService.runHotPath` end-to-end (aggregator + single-window
// paths) and then push the persisted AdWindows through `SkipOrchestrator`
// in auto mode. They are the RED→GREEN driver for the gate's four
// touchpoints:
//
//   1. `runSegmentAggregation` — aggregator-promoted segments must go
//      through the gate before persistence.
//   2. `buildAdWindow` — every persisted AdWindow must carry an
//      eligibilityGate stamp ("markOnly" or "autoSkip").
//   3. Single-window fast path — high-confidence windows must also pass
//      the safety-signal conjunction before becoming auto-skippable.
//   4. `SkipOrchestrator.receiveAdWindows` — must refuse to auto-skip
//      windows stamped `eligibilityGate = "markOnly"`.
//
// Scope guardrails:
//   - No threshold calibration (gtt9.3 owns).
//   - No new safety signals (gtt9.12/9.13 own).
//   - No changes to SegmentAggregator internals.

import CoreMedia
import Foundation
import Testing
@testable import Playhead

@Suite("AutoSkipPrecisionGate — wired into hot path + orchestrator")
struct AutoSkipPrecisionGateIntegrationTests {

    // MARK: - Fixtures

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// Plain 2 s uniform feature grid with zero music probability — ensures
    /// the acoustic safety signal never fires unless a test explicitly adds
    /// music-bed-level windows.
    private func insertFeatureGrid(
        store: AnalysisStore,
        assetId: String,
        duration: Double,
        musicBedLevel: MusicBedLevel = .none
    ) async throws {
        var windows: [FeatureWindow] = []
        var t = 0.0
        while t < duration {
            let end = min(t + 2.0, duration)
            windows.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: t,
                endTime: end,
                rms: 0.3,
                spectralFlux: 0.2,
                musicProbability: musicBedLevel == .none ? 0.05 : 0.8,
                musicBedLevel: musicBedLevel,
                pauseProbability: 0.1,
                speakerClusterId: 1,
                jingleHash: nil,
                featureVersion: 1
            ))
            t = end
        }
        try await store.insertFeatureWindows(windows)
    }

    /// - Parameter blocksUnanchoredExtent: playhead-bllt's gate, which ships
    ///   `true` and which this suite DEFAULTS OFF. That is deliberate and it is
    ///   the opposite of hiding from it.
    ///
    ///   This suite's subject is the PRECISION gate — "does the presence
    ///   evidence admit auto-skip?" — and with bllt on, every hot-path row is
    ///   `"markOnly"` whatever the precision gate decided, so every
    ///   `== "markOnly"` assertion here would become true by construction and
    ///   stop discriminating. A rail that cannot fail is the defect
    ///   playhead-le02 spent a bead removing; adding seven more of them while
    ///   fixing a different problem would be a poor trade.
    ///
    ///   The SHIPPING behaviour is not untested — it is pinned by the two
    ///   `(playhead-bllt)` tests below, which pass `true` explicitly and say so
    ///   in their names, and by `HotPathExtentGateMonotonicityTests`.
    private func makeService(
        store: AnalysisStore,
        classifier: ClassifierService,
        classifierCalibrationProfile: ClassifierCalibrationProfile =
            .production,
        blocksUnanchoredExtent: Bool = false
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "gtt9.11-test",
            fmBackfillMode: .off,
            autoSkipConfidenceThreshold: 0.80,
            unanchoredExtentBlocksAutoSkip: blocksUnanchoredExtent
        )
        return AdDetectionService(
            store: store,
            classifier: classifier,
            metadataExtractor: FallbackExtractor(),
            config: config,
            classifierCalibrationProfile:
                classifierCalibrationProfile
        )
    }

    @Test("malformed persisted music-bed level rejects the entire acoustic cohort")
    func malformedPersistedMusicBedLevelFailsClosed() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.11-malformed-music-level"
        try await store.insertAsset(makeAsset(id: assetId))
        try await insertFeatureGrid(
            store: store,
            assetId: assetId,
            duration: 14,
            musicBedLevel: .background
        )
        try await store.execForTesting(
            """
            UPDATE feature_windows
            SET musicBedLevelRaw = 'corrupt-level'
            WHERE analysisAssetId = '\(assetId)' AND startTime = 12
            """
        )

        await #expect(throws: AnalysisStoreError.self) {
            _ = try await store.fetchFeatureWindows(
                assetId: assetId,
                from: 0,
                to: 14,
                minimumFeatureVersion: nil
            )
        }
        await #expect(throws: AnalysisStoreError.self) {
            _ = try await store.fetchAllFeatureWindows(
                assetId: assetId,
                minimumFeatureVersion: nil
            )
        }
    }

    /// Maps every finite raw score to 0.2, below the UI candidate threshold.
    /// Matching both producer revision keys proves the production calibration
    /// path—not a low raw classifier score—drives detection-only.
    private func detectionOnlyCalibrationProfile()
        -> ClassifierCalibrationProfile
    {
        ClassifierCalibrationProfile(fits: [
            .init(
                detectorVersion: "gtt9.11-test",
                buildCommitSHA: BuildInfo.commitSHA,
                coefficients: PlattCoefficients(
                    a: 0,
                    b: log(4)
                ),
                corpusLabel: "round3-detection-only",
                trainingSampleCount: 2
            )
        ])
    }

    /// Helper to fabricate a Tier-1 slot-scoring pattern that fuses into ONE
    /// promoted aggregator segment whose duration-weighted mean sits in a
    /// prescribed bucket. We use six 30 s Tier 1 slots; two carry a higher
    /// "spike" score to complete the N=2-nearby start criterion, the other
    /// four carry a continuation-grade baseline.
    ///
    /// - Parameters:
    ///   - baseline: per-window score for the 4 non-spike slots (must be
    ///     < 0.35 candidateThreshold and > 0.28 continuationThreshold to
    ///     keep the segment open without seeding new starts).
    ///   - spike: per-window score for the 2 spike slots (drives the
    ///     weighted-mean segmentScore; must be ≥ 0.35 candidateThreshold).
    /// - Returns: a (scoresByStartTime, segmentScore) pair. `segmentScore`
    ///   is the duration-weighted mean over all 6 slots (all have equal
    ///   duration so it's a plain arithmetic mean).
    private func aggregatorPatternForSegmentScore(
        baseline: Double,
        spike: Double
    ) -> (scores: [Double: Double], mean: Double) {
        let scores: [Double: Double] = [
            0.0:   baseline,
            30.0:  spike,
            60.0:  baseline,
            90.0:  baseline,
            120.0: spike,
            150.0: baseline
        ]
        let mean = (baseline * 4 + spike * 2) / 6.0
        return (scores, mean)
    }

    @Test("single-window detection-only gate result is not persisted")
    func singleWindowDetectionOnlyIsNotPersisted() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-round3-single-detection-only"
        try await store.insertAsset(makeAsset(id: assetId))
        try await insertFeatureGrid(
            store: store,
            assetId: assetId,
            duration: 3600
        )
        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.1,
            chunkScore: 0.99
        )
        let service = makeService(
            store: store,
            classifier: classifier,
            classifierCalibrationProfile:
                detectionOnlyCalibrationProfile()
        )
        let text =
            "brought to you by squarespace use promo code playhead"
        let chunk = TranscriptChunk(
            id: "round3-single-detection-only",
            analysisAssetId: assetId,
            segmentFingerprint: "round3-single-fingerprint",
            chunkIndex: 0,
            startTime: 600,
            endTime: 660,
            text: text,
            normalizedText: text,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: 3600
        )

        #expect(
            try await store.fetchAdWindows(assetId: assetId).isEmpty,
            "a calibrated detection-only result must not become a legacy nil-gated AdWindow"
        )
    }

    @Test("aggregator detection-only gate result is not persisted")
    func aggregatorDetectionOnlyIsNotPersisted() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-round3-aggregator-detection-only"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration = 3600.0
        try await insertFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration
        )
        var scores: [Double: Double] = [:]
        for time in stride(from: 0.0, to: duration, by: 30.0) {
            scores[time] = 0.1
        }
        scores[1500] = 0.85
        scores[1530] = 0.85
        scores[1560] = 0.85
        let service = makeService(
            store: store,
            classifier: SlotScoringClassifier(
                scoresByStartTime: scores,
                defaultScore: 0.1
            ),
            classifierCalibrationProfile:
                detectionOnlyCalibrationProfile()
        )

        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        #expect(
            try await store.fetchAdWindows(assetId: assetId).isEmpty,
            "the aggregator must honor detection-only instead of persisting a nil gate"
        )
    }

    // MARK: - 1. segmentScore ≥ autoSkip, ZERO safety signals → markOnly

    @Test("aggregator segment scoring ≥ autoSkipThreshold with ZERO safety signals persists as eligibilityGate=markOnly and is NOT auto-skipped")
    func highScoreSegmentWithNoSafetySignalsStaysMarkOnly() async throws {
        // Three adjacent Tier 1 slots at 0.85 opens via high-confidence
        // branch; one trailing sub-continuation slot (0.10) closes the
        // segment. Mean ≈ (0.85·3 + 0.10)/4 = 0.6625, duration = 90 s
        // (inclusive of gate upper bound). Segment centered at mid-
        // episode (~1545 s in a 3600 s episode) — NOT in the first/last
        // 10% slot-prior window. No music grid (MusicBedLevel.none), no
        // user corrections, no chunks → no strong lexical signal. Result:
        // gate returns uiCandidate(.noSafetySignals) → markOnly.
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.11-markonly-no-signals"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        var scores = [Double: Double]()
        for t in stride(from: 0.0, to: duration, by: 30.0) { scores[t] = 0.10 }
        scores[1500.0] = 0.85
        scores[1530.0] = 0.85
        scores[1560.0] = 0.85

        let classifier = SlotScoringClassifier(scoresByStartTime: scores, defaultScore: 0.10)
        let service = makeService(store: store, classifier: classifier)

        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "one aggregator segment expected; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "zero safety signals → must stamp eligibilityGate=markOnly; got \(String(describing: persisted.first?.eligibilityGate))")

        // Push to SkipOrchestrator in auto mode — must NOT fire a skip cue.
        try await assertNoSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
    }

    // MARK: - 2. segmentScore ≥ autoSkip, ONE safety signal → autoSkip

    /// playhead-bllt: the PRESENCE half, isolated.
    ///
    /// This test's original assertion — a slot+lexical hot-path segment
    /// persists `"autoSkip"` — is now false at the shipping config, because the
    /// aggregator invents its edges and bllt demotes an unanchored `"autoSkip"`
    /// at the emission site. The claim gtt9.11 made is still true and still
    /// worth pinning, so it is pinned HERE with the extent block off, and the
    /// shipping behaviour is pinned by the companion test below. Two arms, one
    /// fixture, so a regression in either half is attributable.
    @Test("hot path: ≥0.55 score in pre-roll WITH a non-slot signal (sponsor/promoCode/URL lexical) reaches the autoSkip PRESENCE verdict (extent block off)")
    func highScoreSegmentWithSafetySignalAutoSkips() async throws {
        // playhead-9ro7 cycle-2 follow-up: a slot prior on its own no
        // longer admits autoSkip — the service-level helper
        // `AdDetectionService.precisionGateLabel` demotes slot-only
        // autoSkip to mark-only (the pure gate's existing contract is
        // preserved via `AutoSkipPrecisionGateTests
        // .autoSkipAdmittedBySlotPriorPreRoll`). To pin the real
        // autoSkip path we co-fire `strongLexicalAdPhrase` alongside
        // `metadataSlotPrior` by placing a sponsor/promoCode/URL chunk
        // in the pre-roll slot — same lexical pattern as the single-
        // window autoSkip test at §5, just shifted into the slot
        // region. firedSignals = {metadataSlotPrior, strongLexicalAdPhrase}
        // ≠ {.metadataSlotPrior}, so the helper stamps autoSkip.
        let store = try await makeTestStore()
        let assetId = "asset-9ro7-cycle3-slot-plus-lexical"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        let service = makeService(store: store, classifier: classifier)

        // Pre-roll chunk [60, 120]: center 90 s ÷ 3600 s = 2.5% < 10%
        // slotFraction → metadataSlotPrior fires. The same sponsor /
        // promoCode / urlCTA cluster used at §5 fires
        // strongLexicalAdPhrase. Duration 60 ∈ [30, 90] (typical ad).
        let normalized = "brought to you by squarespace use promo code playhead at squarespace.com"
        let chunk = TranscriptChunk(
            id: "chunk-9ro7-cycle3-slot-lexical",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-slot-lexical",
            chunkIndex: 0,
            startTime: 60,
            endTime: 120,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "one AdWindow expected; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "autoSkip",
                "slot+lexical safety signals fired → must stamp eligibilityGate=autoSkip; got \(String(describing: persisted.first?.eligibilityGate))")

        try await assertSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
    }

    /// playhead-bllt: the SHIPPING behaviour of the same fixture.
    ///
    /// Byte-identical to the test above except for the flag. The presence
    /// verdict is still `"autoSkip"` (proved there); the persisted row is
    /// `"markOnly"`, because the segment's edges are wherever the scoring
    /// windows happened to fall and nothing observed either one.
    @Test("hot path: the same slot+lexical segment persists markOnly at the SHIPPING config — its edges are invented (playhead-bllt)")
    func highScoreSegmentWithSafetySignalDemotesOnUnanchoredExtent() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-bllt-slot-plus-lexical-demoted"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        // The SHIPPING config, opted into explicitly — this suite defaults the
        // extent block OFF so its precision-gate assertions keep discriminating.
        let service = makeService(
            store: store,
            classifier: classifier,
            blocksUnanchoredExtent: true
        )

        let normalized = "brought to you by squarespace use promo code playhead at squarespace.com"
        let chunk = TranscriptChunk(
            id: "chunk-bllt-slot-lexical",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-bllt-slot-lexical",
            chunkIndex: 0,
            startTime: 60,
            endTime: 120,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "one AdWindow expected; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "an unanchored hot-path row must demote to markOnly; got \(String(describing: persisted.first?.eligibilityGate))")
        // The demotion must be attributable: the row's own anchors are what
        // caused it, and they are persisted, so a device pull can check.
        #expect(persisted.first?.extentSupport == .unanchored,
                "the row's persisted anchors must be the unanchored pair the gate read")
        // PRESENCE is untouched. A demotion that also moved the score would be
        // the presence/extent conflation playhead-2350 removed.
        #expect(persisted.first?.confidence == 0.85,
                "the demotion must not touch the score; got \(String(describing: persisted.first?.confidence))")

        try await assertNoSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
        // The POSITIVE witness (playhead-le02): a demoted row must still reach
        // the listener as a banner. "No cue" alone is also what a dropped row
        // looks like, and dropping it would be a different decision.
        try await assertArmedInSuggestTierWithUnanchoredExtentCensus(
            store: store,
            windows: persisted,
            assetId: assetId
        )
    }

    // MARK: - 3. segmentScore ∈ [uiCandidate, autoSkip) → markOnly

    @Test("segmentScore in [uiCandidateThreshold, autoSkipThreshold) persists as eligibilityGate=markOnly (below autoSkipThreshold)")
    func segmentBelowAutoSkipButAboveUIStaysMarkOnly() async throws {
        // Use the DF5C1832-style N=2-nearby start pattern, sized so that
        // end-of-stream flush is the closer. Episode duration matches the
        // pattern length (180 s) so no trailing 0.10 slot drags the mean.
        //
        //   slot 0..30    baseline 0.33 (> 0.28 continuation, < 0.35 cand.)
        //   slot 30..60   spike 0.595   (> 0.35 candidate, < 0.60 hiConf)
        //   slot 60..90   baseline 0.33
        //   slot 90..120  baseline 0.33
        //   slot 120..150 spike 0.595
        //   slot 150..180 baseline 0.33
        //   mean = (0.33*4 + 0.595*2) / 6 = 2.51 / 6 = 0.4183  ← < 0.55
        //
        // Segment center = 90 s in a 180 s episode → 50% (mid-roll, no slot
        // prior). No music grid, no lexical chunks, no user correction →
        // zero safety signals — but score < 0.55 autoSkipThreshold demotes
        // it via the .belowAutoSkipThreshold branch regardless.
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.11-below-autoskip-threshold"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 180
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        let scores: [Double: Double] = [
            0.0:   0.33,
            30.0:  0.595,
            60.0:  0.33,
            90.0:  0.33,
            120.0: 0.595,
            150.0: 0.33
        ]
        let classifier = SlotScoringClassifier(scoresByStartTime: scores, defaultScore: 0.33)
        let service = makeService(store: store, classifier: classifier)

        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "one aggregator segment expected (mean 0.4183 > 0.40 promotion); got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "segmentScore 0.4183 < 0.55 autoSkipThreshold → markOnly; got \(String(describing: persisted.first?.eligibilityGate))")

        try await assertNoSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
    }

    // MARK: - 4. segmentScore < uiCandidate → no AdWindow persisted

    @Test("segmentScore < uiCandidateThreshold (0.40) persists ZERO AdWindows (detection-only, telemetry)")
    func segmentBelowUICandidateThresholdIsDetectionOnly() async throws {
        // In practice, SegmentAggregator.promotionThreshold = 0.40 already
        // filters out sub-0.40 segments, so the precision gate never sees
        // them from the aggregator path. We force the aggregator's input
        // below 0.35 (candidateThreshold) on every window so no segment
        // even opens — the expected result is zero persisted AdWindows.
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.11-below-ui"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        // All windows at 0.34 — below 0.35 candidateThreshold for the
        // aggregator start. Segment cannot open. No AdWindow persisted.
        var scores = [Double: Double]()
        for t in stride(from: 0.0, to: duration, by: 30.0) { scores[t] = 0.34 }

        let classifier = SlotScoringClassifier(scoresByStartTime: scores, defaultScore: 0.34)
        let service = makeService(store: store, classifier: classifier)

        _ = try await service.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.isEmpty,
                "no AdWindow should be persisted when all scores are below aggregator candidateThreshold; got \(persisted.count)")
    }

    // MARK: - 5. Single 0.85 window WITH ≥1 safety signal → autoSkip

    /// playhead-bllt: the PRESENCE half of the SINGLE-WINDOW producer,
    /// isolated with the extent block off. See the aggregator pair at §2 for
    /// the argument; the shipping behaviour is the companion test below.
    @Test("single-window fast path: 0.85 window with ≥1 safety signal reaches the autoSkip PRESENCE verdict and is auto-skipped (extent block off)")
    func singleHighConfidenceWindowWithSafetySignalAutoSkips() async throws {
        // Chunk carrying sponsor + promoCode + strong-URL lexical hits
        // → strongLexicalAdPhrase safety signal fires via the {sponsor,
        // promoCode, urlCTA} category set. ClassifierService pins the
        // score at 0.85 via the SlotScoringClassifier's chunkScore branch.
        //
        // Chunk text chosen so lexical hits are densely distributed
        // (all pairwise gaps ≤ 30 s mergeGapThreshold) and the merged
        // LexicalCandidate spans ≥ 30 s (gate's typicalAdDuration floor).
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.11-single-window-autoskip"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        let service = makeService(store: store, classifier: classifier)

        // 60 s chunk at mid-episode (center 1530 — no slot prior).
        //  - "brought to you by" at chars 0-17
        //  - "promo code PLAYHEAD" at chars 35-54
        //  - "squarespace.com" at chars 59-74
        // Interpolated hit timings within [1500, 1560):
        //  - ~1500..1508, ~1524..1536, ~1540..1552. All gaps < 30.
        // Merged LexicalCandidate spans ~48 s → within [30, 90]. ✓
        let normalized = "brought to you by squarespace use promo code playhead at squarespace.com"
        let chunk = TranscriptChunk(
            id: "chunk-gtt9.11-sponsor",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-sponsor",
            chunkIndex: 0,
            startTime: 1500,
            endTime: 1560,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "exactly one AdWindow from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "autoSkip",
                "lexical sponsor signal fires → must stamp eligibilityGate=autoSkip; got \(String(describing: persisted.first?.eligibilityGate))")

        try await assertSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
    }

    /// playhead-bllt — THE CENTREPIECE RAIL. An unanchored hot-path row must
    /// not reach auto-skip.
    ///
    /// This is the single-window producer at the SHIPPING config, with the
    /// strongest presence evidence the suite can manufacture: a 0.85 classifier
    /// score and a firing `strongLexicalAdPhrase`. Before this bead the row
    /// persisted `"autoSkip"` and `SkipOrchestrator` cut the audio at
    /// boundaries a lexical seed had guessed — the THEMOVE failure playhead-2350
    /// fixed for the fusion producer and left open for this one.
    @Test("single-window fast path: the same 0.85 + strong-lexical window persists markOnly at the SHIPPING config — its edges are invented (playhead-bllt)")
    func singleHighConfidenceWindowDemotesOnUnanchoredExtent() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-bllt-single-window-demoted"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        // The SHIPPING config, opted into explicitly — this suite defaults the
        // extent block OFF so its precision-gate assertions keep discriminating.
        let service = makeService(
            store: store,
            classifier: classifier,
            blocksUnanchoredExtent: true
        )

        let normalized = "brought to you by squarespace use promo code playhead at squarespace.com"
        let chunk = TranscriptChunk(
            id: "chunk-bllt-sponsor",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-bllt-sponsor",
            chunkIndex: 0,
            startTime: 1500,
            endTime: 1560,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "exactly one AdWindow from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "an unanchored hot-path row must demote to markOnly; got \(String(describing: persisted.first?.eligibilityGate))")
        #expect(persisted.first?.extentSupport == .unanchored,
                "the row's persisted anchors must be the unanchored pair the gate read")
        #expect(persisted.first?.confidence == 0.85,
                "the demotion must not touch the score; got \(String(describing: persisted.first?.confidence))")

        try await assertNoSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
        try await assertArmedInSuggestTierWithUnanchoredExtentCensus(
            store: store,
            windows: persisted,
            assetId: assetId
        )
    }

    /// playhead-bllt: an ANCHORED row is unaffected — the negative direction of
    /// the acceptance criteria, stated at the producer's own emission site.
    ///
    /// The hot path has no way to produce an anchored row today (that is the
    /// whole finding), so this drives `HotPathExtentGate` with the anchors a
    /// future producer would carry. Without it, "unanchored rows demote" is
    /// indistinguishable from "hot-path rows always demote", and the day
    /// somebody teaches the hot path to read a byte-exact edge, the gate would
    /// silently keep demoting it.
    @Test("an anchored hot-path row keeps its autoSkip verdict — the gate reads the extent, not the producer (playhead-bllt)")
    func anchoredHotPathRowIsUnaffected() {
        for anchor in [AutoSkipEdgeAnchor.rediffByteExact, .stingerSnapped] {
            let anchored = SpanExtentSupport(startAnchor: anchor, endAnchor: anchor)
            #expect(
                HotPathExtentGate.gatedLabel(
                    HotPathExtentGate.autoSkipLabel,
                    extent: anchored,
                    blockingUnanchoredAutoSkip: true
                ) == HotPathExtentGate.autoSkipLabel,
                "\(anchor.rawValue) on BOTH edges must pass the gate untouched"
            )
        }
        // And the asymmetric cases are NOT "half safe": one invented edge is
        // enough to clip the show, so a mixed pair demotes like a bare one.
        for (start, end) in [
            (AutoSkipEdgeAnchor.rediffByteExact, AutoSkipEdgeAnchor.unanchored),
            (AutoSkipEdgeAnchor.unanchored, AutoSkipEdgeAnchor.stingerSnapped),
        ] {
            #expect(
                HotPathExtentGate.gatedLabel(
                    HotPathExtentGate.autoSkipLabel,
                    extent: SpanExtentSupport(startAnchor: start, endAnchor: end),
                    blockingUnanchoredAutoSkip: true
                ) == HotPathExtentGate.markOnlyLabel,
                "\(start.rawValue)/\(end.rawValue): one unanchored edge must demote"
            )
        }
    }

    // MARK: - 6. Single 0.85 window with ZERO safety signals → markOnly (NEW)

    @Test("single-window fast path: 0.85 window with ZERO safety signals persists eligibilityGate=markOnly (no auto-skip)")
    func singleHighConfidenceWindowWithoutSafetySignalsStaysMarkOnly() async throws {
        // Chunk text uses ONLY transitionMarker lexical patterns (weak —
        // not counted as `strongLexicalAdPhrase`). ≥2 hits ensure a
        // LexicalCandidate is emitted, so the single-window classification
        // path fires. Mid-episode placement avoids slot-prior. No music
        // grid → acoustic signal silent. No user correction → boost = 1.0.
        // Result: 0 safety signals → gate demotes 0.85 score to markOnly.
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.11-single-window-markonly"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        let service = makeService(store: store, classifier: classifier)

        // 60 s chunk at mid-episode. transitionMarker hits:
        //  "anyway" (0-6), "back to the show" (7-23), "without further ado"
        //  (24-43). Interpolated timings in [1500, 1560): ~1500..1508,
        //  ~1510..1531, ~1533..1558. All gaps ≤ 30. Merged candidate
        //  [1500, 1558], duration 58 s → within [30, 90]. ✓ Categories
        //  = {transitionMarker} only → strongLexicalAdPhrase does NOT fire.
        let normalized = "anyway back to the show without further ado"
        let chunk = TranscriptChunk(
            id: "chunk-gtt9.11-nonad",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-nonad",
            chunkIndex: 0,
            startTime: 1500,
            endTime: 1560,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "exactly one AdWindow from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "0.85 score but zero safety signals → must stamp eligibilityGate=markOnly (NEW precision gate); got \(String(describing: persisted.first?.eligibilityGate))")

        try await assertNoSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
    }

    // MARK: - 7. Single-window in slot with metadataSlotPrior alone → markOnly

    @Test("single-window fast path: 0.85 window in pre-roll with ONLY metadataSlotPrior signal persists eligibilityGate=markOnly (no auto-skip)")
    func singleHighConfidenceWindowInSlotWithOnlySlotSignalStaysMarkOnly() async throws {
        // playhead-9ro7 cycle-2: pin the symmetric single-window leak.
        // The pure gate (`AutoSkipPrecisionGate.classify`) admits any
        // non-empty signal set — a slot-prior alone clears the bar (see
        // `AutoSkipPrecisionGateTests.autoSkipAdmittedBySlotPriorPreRoll`).
        // The service-level helper `precisionGateLabel` overlays a
        // stricter policy: when `metadataSlotPrior` is the ONLY firing
        // signal, demote to mark-only. This test drives that policy via
        // the single-window path so future drift on EITHER call site
        // (single-window line ~1345 OR aggregator line ~4674) is caught.
        let store = try await makeTestStore()
        let assetId = "asset-9ro7-single-window-slot-only"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        let service = makeService(store: store, classifier: classifier)

        // Pre-roll chunk [60, 120]: center 90 s ÷ 3600 s = 2.5% < 10%
        // slotFraction → metadataSlotPrior fires by construction.
        // transitionMarker-only lexical hits emit a LexicalCandidate
        // (so the single-window path actually fires) but do NOT trip
        // strongLexicalAdPhrase. No music grid → acoustic silent. No
        // user correction → boost = 1.0. So firedSignals == {.metadataSlotPrior}.
        let normalized = "anyway back to the show without further ado"
        let chunk = TranscriptChunk(
            id: "chunk-9ro7-slot-only",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-slot-only",
            chunkIndex: 0,
            startTime: 60,
            endTime: 120,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "exactly one AdWindow from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "0.85 score with metadataSlotPrior as the ONLY firing safety signal must demote to markOnly; got \(String(describing: persisted.first?.eligibilityGate))")

        try await assertNoSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
    }

    // MARK: - 8. Post-roll variant: single-window slot-only → markOnly

    @Test("single-window fast path: 0.85 window in POST-roll with ONLY metadataSlotPrior signal persists eligibilityGate=markOnly")
    func singleHighConfidenceWindowInPostRollSlotWithOnlySlotSignalStaysMarkOnly() async throws {
        // playhead-9ro7 cycle-3: symmetric coverage of the helper's
        // bound to `metadataSlotPrior`. The slot prior fires for both
        // pre-roll AND post-roll segments (center within first OR last
        // `slotFraction` of the episode), so the demotion rule must
        // apply on both edges. Without this test, a future regression
        // that forgets to demote on the post-roll path would slip
        // through — the pre-roll-only test would still pass.
        let store = try await makeTestStore()
        let assetId = "asset-9ro7-single-window-postroll-slot-only"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        let classifier = SlotScoringClassifier(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        let service = makeService(store: store, classifier: classifier)

        // Post-roll chunk [3540, 3600]: center 3570 s ÷ 3600 s = 99.2%
        // → within last 10% slotFraction → metadataSlotPrior fires by
        // construction. transitionMarker-only lexical hits emit a
        // LexicalCandidate (so the single-window path actually fires)
        // but do NOT trip strongLexicalAdPhrase. No music grid →
        // acoustic silent. No user correction → boost = 1.0. So
        // firedSignals == {.metadataSlotPrior}.
        let normalized = "anyway back to the show without further ado"
        let chunk = TranscriptChunk(
            id: "chunk-9ro7-postroll-slot-only",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-postroll-slot-only",
            chunkIndex: 0,
            startTime: 3540,
            endTime: 3600,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        #expect(persisted.count == 1,
                "exactly one AdWindow from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "0.85 score with metadataSlotPrior as the ONLY firing safety signal in POST-roll must demote to markOnly; got \(String(describing: persisted.first?.eligibilityGate))")

        try await assertNoSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
    }

    // MARK: - Orchestrator helpers

    /// Hand persisted AdWindows to a SkipOrchestrator in auto mode and
    /// assert NO CMTimeRange skip cue was emitted. A window stamped
    /// `eligibilityGate = "markOnly"` must not reach .applied.
    private func assertNoSkipCueEmitted(
        store: AnalysisStore,
        windows: [AdWindow],
        assetId: String
    ) async throws {
        let orchestrator = try await buildAutoOrchestrator(store: store, assetId: assetId)
        let captured = CueCaptor()
        await orchestrator.setSkipCueHandler { cues in captured.append(cues) }
        await orchestrator.receiveAdWindows(windows)

        #expect(captured.nonEmptyBatchCount == 0,
                "markOnly windows must not emit a skip cue; got \(captured.totalCueCount) cue(s) across \(captured.nonEmptyBatchCount) non-empty batches")
    }

    /// Hand persisted AdWindows to a SkipOrchestrator in auto mode and
    /// assert at least one skip cue was emitted. A window stamped
    /// `eligibilityGate = "autoSkip"` must reach .applied and surface a cue.
    private func assertSkipCueEmitted(
        store: AnalysisStore,
        windows: [AdWindow],
        assetId: String
    ) async throws {
        let orchestrator = try await buildAutoOrchestrator(store: store, assetId: assetId)
        let captured = CueCaptor()
        await orchestrator.setSkipCueHandler { cues in captured.append(cues) }
        await orchestrator.receiveAdWindows(windows)

        #expect(captured.nonEmptyBatchCount >= 1,
                "autoSkip windows must emit at least one skip cue; got \(captured.nonEmptyBatchCount) non-empty batches")
    }

    /// playhead-bllt, using the playhead-le02 idiom: the POSITIVE witness a
    /// demotion needs, and that `assertNoSkipCueEmitted` cannot supply.
    ///
    /// "No skip cue" is also what a row that was dropped, filtered, or never
    /// delivered looks like. A demotion is a specific claim — the row reached
    /// the listener, in the suggest tier, for a NAMED reason — and each of the
    /// three checks below fails for a different mutation:
    ///
    ///   * `activeSuggestWindowIDs()` reads the real collection the suggest
    ///     tier lands in, so a demotion that turned into a drop reddens here.
    ///   * the isp5 census OUTCOME distinguishes armed from every other way a
    ///     row can be absent from the managed tier.
    ///   * the census DETAIL names the extent as the cause, so a row demoted
    ///     for some unrelated reason is not mistaken for this one.
    private func assertArmedInSuggestTierWithUnanchoredExtentCensus(
        store: AnalysisStore,
        windows: [AdWindow],
        assetId: String,
        sourceLocation: SourceLocation = #_sourceLocation
    ) async throws {
        let orchestrator = try await buildAutoOrchestrator(store: store, assetId: assetId)
        await orchestrator.receiveAdWindows(windows)

        let suggested = await orchestrator.activeSuggestWindowIDs()
        let managed = await orchestrator.activeWindowIDs()
        for window in windows {
            #expect(
                suggested.contains(window.id),
                "\(window.id): a demoted row must still reach the listener as a banner — it is in NO tier",
                sourceLocation: sourceLocation
            )
            #expect(
                !managed.contains(window.id),
                "\(window.id): a demoted row must not be in the managed (auto-skip) tier",
                sourceLocation: sourceLocation
            )
            let ingest = await orchestrator.lastAdWindowIngestOutcome(
                forWindowId: window.id
            )
            #expect(
                ingest?.outcome == .armedSuggest,
                "\(window.id): census must record `ingest_armed_suggest`; got \(String(describing: ingest?.outcome))",
                sourceLocation: sourceLocation
            )
            #expect(
                ingest?.detail == "unanchored_extent_start+end",
                "\(window.id): census must NAME the extent as the cause — absence from the managed tier for an unstated reason is what this bead exists to stop; got \(String(describing: ingest?.detail))",
                sourceLocation: sourceLocation
            )
        }
    }

    private func buildAutoOrchestrator(
        store: AnalysisStore,
        assetId: String
    ) async throws -> SkipOrchestrator {
        let trust = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orch = SkipOrchestrator(store: store, trustService: trust)
        await orch.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "ep-\(assetId)",
            podcastId: "podcast-1"
        )
        return orch
    }
}

// MARK: - Test doubles

/// Deterministic classifier reused from SegmentAggregatorWiringTests' shape.
private final class SlotScoringClassifier: @unchecked Sendable, ClassifierService {
    private let scoresByStartTime: [Double: Double]
    private let defaultScore: Double
    private let chunkScore: Double?

    init(
        scoresByStartTime: [Double: Double],
        defaultScore: Double,
        chunkScore: Double? = nil
    ) {
        self.scoresByStartTime = scoresByStartTime
        self.defaultScore = defaultScore
        self.chunkScore = chunkScore
    }

    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
        inputs.map { classify(input: $0, priors: priors) }
    }

    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
        let probability: Double
        if let chunkScore, !input.candidate.id.hasPrefix("tier1-") {
            probability = chunkScore
        } else {
            probability = scoresByStartTime[input.candidate.startTime] ?? defaultScore
        }
        return ClassifierResult(
            candidateId: input.candidate.id,
            analysisAssetId: input.candidate.analysisAssetId,
            startTime: input.candidate.startTime,
            endTime: input.candidate.endTime,
            adProbability: probability,
            startAdjustment: 0,
            endAdjustment: 0,
            signalBreakdown: SignalBreakdown(
                lexicalScore: 0,
                rmsDropScore: 0,
                spectralChangeScore: 0,
                musicScore: 0,
                speakerChangeScore: 0,
                priorScore: 0
            )
        )
    }
}

/// Captures CMTimeRange batches pushed by SkipOrchestrator's skipCueHandler.
private final class CueCaptor: @unchecked Sendable {
    private let lock = NSLock()
    private var batches: [[CMTimeRange]] = []

    func append(_ batch: [CMTimeRange]) {
        lock.lock(); defer { lock.unlock() }
        batches.append(batch)
    }

    var nonEmptyBatchCount: Int {
        lock.lock(); defer { lock.unlock() }
        return batches.filter { !$0.isEmpty }.count
    }

    var totalCueCount: Int {
        lock.lock(); defer { lock.unlock() }
        return batches.reduce(0) { $0 + $1.count }
    }
}
