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

    private func makeService(
        store: AnalysisStore,
        classifier: ClassifierService
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "gtt9.11-test",
            fmBackfillMode: .off,
            autoSkipConfidenceThreshold: 0.80
        )
        return AdDetectionService(
            store: store,
            classifier: classifier,
            metadataExtractor: FallbackExtractor(),
            config: config
        )
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

    @Test("aggregator segment scoring ≥ autoSkipThreshold with ≥1 safety signal persists as eligibilityGate=autoSkip and SkipOrchestrator emits a skip cue")
    func highScoreSegmentWithSafetySignalAutoSkips() async throws {
        // Same three-slot high-confidence pattern, but centered in the
        // PRE-ROLL slot (first 10% of the episode → metadataSlotPrior
        // fires). That's our ≥1 safety signal. Score ≥ 0.55, duration
        // ≤ 90 s, signal fires → autoSkip.
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.11-autoskip-slot-signal"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        try await insertFeatureGrid(store: store, assetId: assetId, duration: duration)

        // Three adjacent slots at 0..90 s — center 45 s < 360 s (10% of 3600).
        var scores = [Double: Double]()
        for t in stride(from: 0.0, to: duration, by: 30.0) { scores[t] = 0.10 }
        scores[0.0] = 0.85
        scores[30.0] = 0.85
        scores[60.0] = 0.85

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
        #expect(persisted.first?.eligibilityGate == "autoSkip",
                "slot-prior safety signal fired → must stamp eligibilityGate=autoSkip; got \(String(describing: persisted.first?.eligibilityGate))")

        try await assertSkipCueEmitted(store: store, windows: persisted, assetId: assetId)
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

    @Test("single-window fast path: 0.85 window with ≥1 safety signal persists eligibilityGate=autoSkip and is auto-skipped")
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
