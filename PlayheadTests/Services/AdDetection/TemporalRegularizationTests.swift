// TemporalRegularizationTests.swift
// playhead-xsdz.10: Tests for lightweight temporal regularization — a
// deterministic, post-fusion SOFT penalty on `skipConfidence` that exploits the
// CONTIGUOUS / CLUSTERED nature of real ads vs the ISOLATED nature of false
// positives.
//
// Two halves:
//   • The pure scoring lives in `TemporalRegularizer` (an enum of static
//     helpers operating over `[Detection]`), so we test the isolation penalty,
//     min-dwell, the anti-contagion (one-sided, high-confidence-gated) guarantee,
//     determinism, and edge cases directly without spinning up the actor
//     (mirrors `AdDetectionServiceFragilityGateTests`).
//   • The OFF-by-default flag + conservative scalar defaults are asserted on
//     `AdDetectionConfig.default` and its derived parameter set.

import Foundation
import Testing
@testable import Playhead

@Suite("Temporal regularization (playhead-xsdz.10)")
struct TemporalRegularizationTests {

    // MARK: - Helpers

    private func detection(
        _ id: String,
        start: Double,
        end: Double,
        conf: Double
    ) -> TemporalRegularizer.Detection {
        TemporalRegularizer.Detection(id: id, startTime: start, endTime: end, skipConfidence: conf)
    }

    /// Default-parameter regularize, returning a lookup by id for terse asserts.
    private func adjustedById(
        _ detections: [TemporalRegularizer.Detection],
        parameters: TemporalRegularizer.Parameters = .default
    ) -> [String: TemporalRegularizer.Adjustment] {
        let out = TemporalRegularizer.regularize(detections: detections, parameters: parameters)
        return Dictionary(uniqueKeysWithValues: out.map { ($0.id, $0) })
    }

    // MARK: - (a) Config defaults / flag-off identity

    @Test("AdDetectionConfig.default keeps temporal regularization OFF with conservative defaults")
    func configDefaultsAreOffAndConservative() {
        let config = AdDetectionConfig.default
        #expect(config.temporalRegularizationEnabled == false,
                "OFF-by-default is load-bearing: main must stay behavior-neutral")
        #expect(config.temporalNeighborWindowSeconds == 120.0)
        #expect(config.temporalHighConfidenceNeighborThreshold == 0.80)
        #expect(config.temporalIsolationPenaltyFactor == 0.85)
        #expect(config.temporalMinDwellSeconds == 10.0)
        #expect(config.temporalMinDwellPenaltyFactor == 0.90)
    }

    @Test("config.temporalRegularizerParameters mirrors the config scalars")
    func parametersMirrorConfig() {
        let config = AdDetectionConfig.default
        let params = config.temporalRegularizerParameters
        #expect(params.neighborWindowSeconds == config.temporalNeighborWindowSeconds)
        #expect(params.highConfidenceNeighborThreshold == config.temporalHighConfidenceNeighborThreshold)
        #expect(params.isolationPenaltyFactor == config.temporalIsolationPenaltyFactor)
        #expect(params.minDwellSeconds == config.temporalMinDwellSeconds)
        #expect(params.minDwellPenaltyFactor == config.temporalMinDwellPenaltyFactor)
    }

    // MARK: - (b) Isolated detection is penalized

    @Test("Enabled: an isolated detection (no high-confidence neighbor in window) is penalized")
    func isolatedDetectionIsPenalized() {
        // Two detections 1000s apart — far outside the 120s window. Neither is
        // the other's neighbor, so both are isolated.
        let dets = [
            detection("lonely", start: 100, end: 130, conf: 0.82),
            detection("faraway", start: 5000, end: 5030, conf: 0.95)
        ]
        let out = adjustedById(dets)
        let lonely = out["lonely"]!
        #expect(lonely.isIsolated, "no high-confidence neighbor within 120s")
        #expect(lonely.isolationPenaltyApplied)
        #expect(lonely.adjustedSkipConfidence == 0.82 * 0.85,
                "isolated confidence must be multiplied by the isolation factor (got \(lonely.adjustedSkipConfidence))")
        #expect(lonely.adjustedSkipConfidence < lonely.originalSkipConfidence)
    }

    // MARK: - (c) Clustered detection is unaffected

    @Test("Enabled: a clustered detection (high-confidence neighbor in window) is NOT penalized")
    func clusteredDetectionUnaffected() {
        // Three back-to-back creatives, each long enough to clear min-dwell, each
        // high confidence. Every one has a high-confidence neighbor within 120s.
        let dets = [
            detection("ad1", start: 100, end: 130, conf: 0.88),
            detection("ad2", start: 132, end: 165, conf: 0.91),
            detection("ad3", start: 167, end: 200, conf: 0.86)
        ]
        let out = adjustedById(dets)
        for id in ["ad1", "ad2", "ad3"] {
            let a = out[id]!
            #expect(!a.isIsolated, "\(id) has a high-confidence neighbor in the cluster")
            #expect(!a.changed, "clustered detection \(id) must be unchanged (got \(a.adjustedSkipConfidence))")
        }
    }

    // MARK: - (d) Anti-contagion: a low-confidence neighbor does NOT count as support

    @Test("Anti-contagion: two adjacent WEAK detections do NOT rescue each other")
    func twoWeakNeighborsDoNotRescueEachOther() {
        // Two adjacent detections, both below the 0.80 high-confidence gate.
        // Neither qualifies as the other's supporting neighbor, so BOTH are
        // treated as isolated and penalized. This is the core correctness
        // requirement: naive contagion would have cemented these correlated FPs.
        let dets = [
            detection("weakA", start: 100, end: 130, conf: 0.55),
            detection("weakB", start: 135, end: 165, conf: 0.60)
        ]
        let out = adjustedById(dets)
        let a = out["weakA"]!
        let b = out["weakB"]!
        #expect(a.isIsolated, "a weak neighbor must NOT count as support")
        #expect(b.isIsolated, "a weak neighbor must NOT count as support")
        #expect(a.adjustedSkipConfidence == 0.55 * 0.85)
        #expect(b.adjustedSkipConfidence == 0.60 * 0.85)
        #expect(a.adjustedSkipConfidence < 0.55 && b.adjustedSkipConfidence < 0.60,
                "the pass is one-sided — it only ever lowers, so weak FPs cannot be raised above threshold")
    }

    @Test("Anti-contagion: a weak detection adjacent to a STRONG one IS supported (one-sided gate is on the neighbor, not the subject)")
    func weakDetectionAdjacentToStrongIsSupported() {
        // The gate is on the NEIGHBOR's confidence: a strong neighbor (>=0.80)
        // supports a weaker subject. The subject keeps its (low) confidence —
        // the pass still never RAISES it; it just declines to penalize.
        let dets = [
            detection("weak", start: 100, end: 130, conf: 0.55),
            detection("strong", start: 135, end: 165, conf: 0.92)
        ]
        let out = adjustedById(dets)
        let weak = out["weak"]!
        #expect(!weak.isIsolated, "a high-confidence neighbor within the window supports the subject")
        #expect(!weak.changed, "supported subject is unchanged — but note the pass never RAISES it either")
        #expect(weak.adjustedSkipConfidence == 0.55)
        // The strong span also has a neighbor (the weak one is within window),
        // but the weak one does NOT count as ITS support — so `strong` is
        // supported only if it qualifies someone else. Here `strong` sees no
        // high-confidence neighbor, so it is isolated and penalized.
        let strong = out["strong"]!
        #expect(strong.isIsolated, "the weak span is below the gate, so it is NOT support for the strong span")
        #expect(strong.adjustedSkipConfidence == 0.92 * 0.85)
    }

    // MARK: - (e) Min-dwell

    @Test("Min-dwell: a too-short, uncorroborated island is additionally down-weighted")
    func minDwellDownweightsShortUncorroboratedIsland() {
        // A 4s lonely island (< 10s min-dwell), no high-confidence neighbor.
        // Both the isolation penalty AND the min-dwell penalty stack.
        let dets = [
            detection("blip", start: 100, end: 104, conf: 0.82),
            detection("faraway", start: 5000, end: 5040, conf: 0.95)
        ]
        let out = adjustedById(dets)
        let blip = out["blip"]!
        #expect(blip.isolationPenaltyApplied)
        #expect(blip.minDwellPenaltyApplied)
        #expect(blip.adjustedSkipConfidence == 0.82 * 0.85 * 0.90,
                "isolation and min-dwell penalties compound (got \(blip.adjustedSkipConfidence))")
    }

    @Test("Min-dwell does NOT touch a short detection that has a high-confidence neighbor")
    func minDwellSkipsCorroboratedShortSpan() {
        // A 4s creative inside a real cluster (high-confidence neighbor present)
        // keeps full confidence — min-dwell only down-weights UNCORROBORATED
        // islands.
        let dets = [
            detection("shortAd", start: 100, end: 104, conf: 0.83),
            detection("bigAd", start: 106, end: 150, conf: 0.90)
        ]
        let out = adjustedById(dets)
        let shortAd = out["shortAd"]!
        #expect(!shortAd.isIsolated)
        #expect(!shortAd.minDwellPenaltyApplied)
        #expect(!shortAd.changed, "a short creative inside a cluster is untouched")
    }

    // MARK: - (f) Determinism / order-independence

    @Test("Determinism: result is independent of input order")
    func resultIsOrderIndependent() {
        let a = detection("a", start: 100, end: 130, conf: 0.55)
        let b = detection("b", start: 5000, end: 5030, conf: 0.95)
        let c = detection("c", start: 105, end: 140, conf: 0.50)
        let forward = adjustedById([a, b, c])
        let reversed = adjustedById([c, b, a])
        for id in ["a", "b", "c"] {
            #expect(forward[id]!.adjustedSkipConfidence == reversed[id]!.adjustedSkipConfidence,
                    "adjusted confidence for \(id) must not depend on input order")
        }
    }

    @Test("Determinism: penalties never cascade — neighbor test uses ORIGINAL confidences")
    func penaltiesDoNotCascade() {
        // `mid` is a strong span (0.85) flanked by two weak spans within window.
        // After `mid` is penalized (it has no high-conf neighbor of its own), the
        // weak spans must STILL see `mid`'s ORIGINAL 0.85 as their supporting
        // neighbor — the pass must not let one span's penalty change whether it
        // supports another. So the weak spans are corroborated and unchanged.
        let dets = [
            detection("weakL", start: 60, end: 90, conf: 0.50),
            detection("mid", start: 100, end: 130, conf: 0.85),
            detection("weakR", start: 140, end: 170, conf: 0.55)
        ]
        let out = adjustedById(dets)
        #expect(!out["weakL"]!.isIsolated, "weakL sees mid's original 0.85 as support")
        #expect(!out["weakR"]!.isIsolated, "weakR sees mid's original 0.85 as support")
        #expect(!out["weakL"]!.changed)
        #expect(!out["weakR"]!.changed)
        // `mid` itself has no high-confidence neighbor (both flanks are < 0.80),
        // so it IS penalized — confirming the support test is one-sided.
        #expect(out["mid"]!.isIsolated)
        #expect(out["mid"]!.adjustedSkipConfidence == 0.85 * 0.85)
    }

    // MARK: - (g) Edge cases

    @Test("Edge: a single detection in the episode is returned unchanged")
    func singleDetectionUnchanged() {
        let dets = [detection("solo", start: 100, end: 104, conf: 0.82)]
        let out = TemporalRegularizer.regularize(detections: dets)
        #expect(out.count == 1)
        #expect(!out[0].changed,
                "a lone detection has no neighbors to compare against — the pass is a no-op")
        #expect(out[0].adjustedSkipConfidence == 0.82)
    }

    @Test("Edge: empty input returns empty output")
    func emptyInputIsEmptyOutput() {
        let out = TemporalRegularizer.regularize(detections: [])
        #expect(out.isEmpty)
    }

    @Test("Edge: a neighbor EXACTLY at the window boundary counts as support")
    func neighborExactlyAtWindowBoundaryCounts() {
        // Gap is edge-to-edge: subject ends at 130, neighbor starts at 250 →
        // gap == 120 == the window. `<=` means this counts as support.
        let dets = [
            detection("subject", start: 100, end: 130, conf: 0.55),
            detection("boundary", start: 250, end: 280, conf: 0.90)
        ]
        let out = adjustedById(dets)
        #expect(TemporalRegularizer.gap(dets[0], dets[1]) == 120.0)
        #expect(!out["subject"]!.isIsolated, "a neighbor exactly at the window edge is in-window (<=)")
        #expect(!out["subject"]!.changed)
    }

    @Test("Edge: a neighbor JUST past the window boundary does NOT count as support")
    func neighborJustPastBoundaryDoesNotCount() {
        // Gap == 120.001 > 120 → out of window → subject is isolated.
        let dets = [
            detection("subject", start: 100, end: 130, conf: 0.55),
            detection("tooFar", start: 250.001, end: 280, conf: 0.90)
        ]
        let out = adjustedById(dets)
        #expect(TemporalRegularizer.gap(dets[0], dets[1]) > 120.0)
        #expect(out["subject"]!.isIsolated, "a neighbor past the window edge is NOT support")
        #expect(out["subject"]!.adjustedSkipConfidence == 0.55 * 0.85)
    }

    @Test("Edge: overlapping detections have gap 0 (always in window)")
    func overlappingDetectionsHaveZeroGap() {
        let a = detection("a", start: 100, end: 150, conf: 0.55)
        let b = detection("b", start: 140, end: 200, conf: 0.90)
        #expect(TemporalRegularizer.gap(a, b) == 0.0)
        let out = adjustedById([a, b])
        #expect(!out["a"]!.isIsolated, "an overlapping high-confidence neighbor always supports")
    }

    @Test("Edge: factors > 1 are clamped — the pass can never raise a confidence")
    func factorsAreClampedNeverBoost() {
        let params = TemporalRegularizer.Parameters(
            neighborWindowSeconds: 120,
            highConfidenceNeighborThreshold: 0.80,
            isolationPenaltyFactor: 5.0,    // absurd — must clamp to 1.0
            minDwellSeconds: 10,
            minDwellPenaltyFactor: 5.0      // absurd — must clamp to 1.0
        )
        let dets = [
            detection("lonely", start: 100, end: 104, conf: 0.50),
            detection("faraway", start: 9000, end: 9040, conf: 0.95)
        ]
        let out = adjustedById(dets, parameters: params)
        #expect(out["lonely"]!.adjustedSkipConfidence == 0.50,
                "clamped factors of 1.0 leave the confidence unchanged — never boosted")
        #expect(!out["lonely"]!.changed)
    }

    @Test("Edge: non-finite confidence is passed through untouched")
    func nonFiniteConfidencePassThrough() {
        let dets = [
            detection("nan", start: 100, end: 130, conf: .nan),
            detection("ok", start: 5000, end: 5030, conf: 0.95)
        ]
        let out = adjustedById(dets)
        #expect(out["nan"]!.adjustedSkipConfidence.isNaN,
                "NaN confidence is a data-integrity error; pass it through unchanged")
        #expect(!out["nan"]!.isolationPenaltyApplied)
        #expect(!out["nan"]!.minDwellPenaltyApplied)
        #expect(!out["nan"]!.changed,
                "a passed-through NaN must report `changed == false` — `NaN != NaN` is true in IEEE 754, so `changed` must derive from the penalty flags, not a value comparison, or the service emits a spurious no-op rebuild + 'NaN → NaN' log")
    }

    @Test("Edge: a non-finite neighbor cannot count as support")
    func nonFiniteNeighborIsNotSupport() {
        // The only other detection has a NaN confidence — it must not qualify as
        // a high-confidence neighbor, so the subject is isolated.
        let dets = [
            detection("subject", start: 100, end: 130, conf: 0.55),
            detection("nanNeighbor", start: 135, end: 165, conf: .nan)
        ]
        let out = adjustedById(dets)
        #expect(out["subject"]!.isIsolated, "a NaN-confidence neighbor is not valid support")
        #expect(out["subject"]!.adjustedSkipConfidence == 0.55 * 0.85)
    }

    @Test("Edge: every output confidence is in [0, 1] and <= its input (one-sided)")
    func outputsAreClampedAndOneSided() {
        let dets = [
            detection("a", start: 0, end: 4, conf: 0.50),
            detection("b", start: 200, end: 204, conf: 0.999),
            detection("c", start: 400, end: 460, conf: 0.0),
            detection("d", start: 405, end: 408, conf: 1.0)
        ]
        let out = TemporalRegularizer.regularize(detections: dets)
        for a in out {
            #expect(a.adjustedSkipConfidence >= 0.0 && a.adjustedSkipConfidence <= 1.0,
                    "\(a.id) adjusted confidence must stay in [0, 1]")
            #expect(a.adjustedSkipConfidence <= a.originalSkipConfidence,
                    "\(a.id) the pass is one-sided — it must never raise a confidence")
        }
    }
}

// MARK: - Service-level wiring

/// playhead-xsdz.10: exercises the collect-then-regularize seam inside
/// `runBackfill`. The unit suite above pins the algorithm; these tests pin the
/// two-loop restructure: with the flag OFF the persisted decisions are
/// identical across runs (the restructure is byte-stable), and with the flag ON
/// the pass is wired through without crashing the pipeline.
@Suite("Temporal regularization — service wiring (playhead-xsdz.10)")
struct TemporalRegularizationServiceWiringTests {

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

    private func adChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome back to the show today.",
            "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.",
            "Back to our conversation about technology and the future of podcasting."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func config(temporalEnabled: Bool) -> AdDetectionConfig {
        AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "xsdz10-test",
            fmBackfillMode: .off,
            temporalRegularizationEnabled: temporalEnabled
        )
    }

    /// A per-window fingerprint of the DETERMINISTIC side effects the emission
    /// loop is responsible for: it is NOT just `confidence`. The flag-off
    /// restructure must reproduce the pre-xsdz.10 AdWindow build verbatim — same
    /// bounds, decision/boundary state, gate, detector version, and persisted
    /// confidence — in the same order. Comparing this whole tuple (rather than
    /// confidence alone) catches a reordered, dropped, duplicated, or
    /// differently-built window, which a bare `confidence` list would miss.
    ///
    /// `id` (a fresh `UUID().uuidString` per window) and `analysisAssetId` (the
    /// per-run asset id) are DELIBERATELY excluded — they are non-deterministic
    /// across runs by design and were never a behaviour the restructure could
    /// have changed; including them would make any cross-run comparison vacuous.
    private struct WindowFingerprint: Equatable {
        let startTime: Double
        let endTime: Double
        let confidence: Double
        let decisionState: String
        let boundaryState: String
        let eligibilityGate: String?
        let detectorVersion: String
        let catalogStoreMatchSimilarity: Double?
    }

    private func runAndReadWindows(temporalEnabled: Bool) async throws -> [WindowFingerprint] {
        let store = try await makeTestStore()
        let assetId = "asset-xsdz10-\(temporalEnabled ? "on" : "off")"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config(temporalEnabled: temporalEnabled)
        )
        try await service.runBackfill(
            chunks: adChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-xsdz10",
            episodeDuration: 90.0
        )
        // Stable order so two runs are directly comparable; the persisted set is
        // independent of asset id, so two runs differing only in assetId still
        // produce identical fingerprints once the (id-suffixed) window/asset ids
        // are excluded below.
        return try await store.fetchAdWindows(assetId: assetId)
            .sorted { ($0.startTime, $0.endTime, $0.id) < ($1.startTime, $1.endTime, $1.id) }
            .map { w in
                WindowFingerprint(
                    startTime: w.startTime,
                    endTime: w.endTime,
                    confidence: w.confidence,
                    decisionState: w.decisionState,
                    boundaryState: w.boundaryState,
                    eligibilityGate: w.eligibilityGate,
                    detectorVersion: w.detectorVersion,
                    catalogStoreMatchSimilarity: w.catalogStoreMatchSimilarity
                )
            }
    }

    @Test("Flag OFF: two independent runs persist identical windows (restructure is byte-stable across ALL window side effects, not just confidence)")
    func flagOffIsDeterministicAndStable() async throws {
        let first = try await runAndReadWindows(temporalEnabled: false)
        let second = try await runAndReadWindows(temporalEnabled: false)
        #expect(!first.isEmpty,
                "the fixture must persist at least one ad window or this test proves nothing")
        #expect(first == second,
                "with the flag off the collect-then-emit restructure must be deterministic / byte-stable across the full AdWindow build (bounds, decisionState, boundaryState, gate, confidence), not just confidence")
    }

    @Test("Flag ON is one-sided end-to-end: every persisted confidence is <= its flag-OFF counterpart, and identical for a degenerate (<= 1 window) corpus")
    func flagOnIsOneSidedRelativeToOff() async throws {
        let off = try await runAndReadWindows(temporalEnabled: false)
        let on = try await runAndReadWindows(temporalEnabled: true)
        // The pass only ever lowers confidence; it never adds, drops, or moves a
        // window. Both runs persist windows over the same deterministic span set
        // in the same (startTime, endTime) order, so we pair by sorted index and
        // assert (a) the window count matches and (b) the one-sided property on
        // confidence, with bounds/state identical.
        #expect(on.count == off.count,
                "the temporal pass must not add or drop windows — only adjust confidence (off=\(off.count) on=\(on.count))")
        for (onWin, offWin) in zip(on, off) {
            #expect(onWin.startTime == offWin.startTime && onWin.endTime == offWin.endTime,
                    "the pass must not move a window's bounds")
            #expect(onWin.confidence <= offWin.confidence,
                    "flag ON is one-sided: confidence must never exceed its flag-OFF value (off=\(offWin.confidence) on=\(onWin.confidence))")
        }
        // For a degenerate corpus (0 or 1 detection) the regularizer's `count > 1`
        // guard makes the pass a strict no-op, so ON must EQUAL OFF exactly.
        if off.count <= 1 {
            #expect(on == off,
                    "with <= 1 detection the temporal pass is a no-op — ON and OFF must persist identical windows")
        }
    }

    @Test("Flag ON: runBackfill completes end-to-end without throwing (pass is wired)")
    func flagOnCompletesCleanly() async throws {
        await #expect(throws: Never.self) {
            _ = try await self.runAndReadWindows(temporalEnabled: true)
        }
    }

    @Test("AdDetectionService.default config leaves temporal regularization OFF")
    func serviceDefaultConfigIsOff() {
        #expect(AdDetectionConfig.default.temporalRegularizationEnabled == false,
                "no production config or A/B arm may enable temporal regularization")
    }
}
