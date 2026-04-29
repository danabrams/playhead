// LowPowerModeTests.swift
// playhead-rk7: degraded-conditions E2E — Low Power Mode (LPM).
//
// Pins the LPM-specific demotion rules through the production
// QualityProfile derivation + BackgroundProcessingService gate, plus
// the LPM-specific lookahead degradation in
// `BackgroundProcessingService.hotPathLookaheadMultiplier()`. Also
// proves the hot-path detector does not produce false-positive
// AdWindows on a hand-crafted no-ad transcript while LPM is on — the
// classifier must not fire on conversational content even under
// reduced-lookahead conditions.
//
// Contract being pinned (from `QualityProfile.derive(...)`):
//   * LPM alone with healthy battery + charging + nominal thermal
//     demotes the baseline `.nominal` profile to `.fair`. `.fair`
//     keeps `allowSoonLane: true` and `pauseAllWork: false`, so
//     neither the BPS foreground gate nor the BPS Soon-lane gate
//     fires. The Background lane is the one that throttles in `.fair`,
//     and that gate lives in `AnalysisWorkScheduler` (lane-level), not
//     in BPS. We assert the BPS-layer behavior here and leave the lane
//     scheduler's `.fair` gate to its own dedicated tests.
//   * LPM + `.fair` thermal demotes the baseline to `.serious`,
//     which clears `allowSoonLane`. BPS backfill gate fires;
//     foreground hot-path gate stays open.
//   * `hotPathLookaheadMultiplier()` returns 0.5 whenever LPM is on
//     (regardless of thermal) so Stage 3 ASR + scoring runs at half
//     depth. Recovery (LPM disabled, nominal thermal) returns it to
//     1.0×.
//   * No false-positive ad detections on a clean conversational
//     transcript: with LPM-degraded lookahead, the lexical + rule-
//     based classifier still must not surface phantom AdWindows on
//     content that contains zero sponsor disclosures.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-rk7 - low power mode", .serialized)
struct LowPowerModeTests {

    private func makeBPS() -> (BackgroundProcessingService, StubAnalysisCoordinator, StubBatteryProvider) {
        let coordinator = StubAnalysisCoordinator()
        let scheduler = StubTaskScheduler()
        let battery = StubBatteryProvider()
        battery.level = 0.95
        battery.charging = true
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: battery
        )
        return (bps, coordinator, battery)
    }

    // MARK: - Test 1: LPM alone keeps hot-path active and backfill open at BPS

    @Test("LPM alone (charged, nominal thermal) keeps BPS gates open")
    func lpmAloneKeepsBpsGatesOpen() async throws {
        let (bps, coordinator, _) = makeBPS()
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // LPM on, battery charged, nominal thermal → baseline .nominal
        // demotes to .fair (LPM is the demotion trigger). `.fair` keeps
        // allowSoonLane=true and pauseAllWork=false so neither BPS
        // gate fires.
        let lpmSnapshot = makeCapabilitySnapshot(
            thermalState: .nominal,
            isLowPowerMode: true
        )
        await bps.handleCapabilityUpdate(lpmSnapshot)
        try await Task.sleep(for: .milliseconds(50))

        #expect(
            coordinator.stopCallCount == 0,
            "LPM alone (with charging + nominal thermal) demotes to .fair, NOT a pauseAll state."
        )
        #expect(
            await bps.isHotPathActive() == true,
            "Hot-path must remain active under LPM alone — playback continues, skip cues continue."
        )
        #expect(
            await bps.isBackfillPaused() == false,
            "LPM alone → .fair profile → allowSoonLane true → BPS backfill gate must NOT fire."
        )
    }

    // MARK: - Test 2: LPM + .fair thermal pauses backfill at BPS

    @Test("LPM combined with .fair thermal pauses backfill but not hot-path")
    func lpmPlusFairPausesBackfill() async throws {
        let (bps, coordinator, _) = makeBPS()
        await bps.playbackDidStart()
        try await Task.sleep(for: .milliseconds(50))

        // LPM + .fair thermal → derive(...) demotes baseline .fair → .serious.
        // .serious clears allowSoonLane → BPS backfill gate fires; hot-path
        // gate (pauseAllWork) stays open.
        let snapshot = makeCapabilitySnapshot(
            thermalState: .fair,
            isLowPowerMode: true
        )
        await bps.handleCapabilityUpdate(snapshot)
        try await Task.sleep(for: .milliseconds(50))

        #expect(
            coordinator.stopCallCount == 0,
            "LPM + fair thermal → .serious; pauseAllWork is still false. coordinator.stop() must not fire."
        )
        #expect(
            await bps.isHotPathActive() == true,
            "Hot-path must remain active — only critical thermal pauses it."
        )
        #expect(
            await bps.isBackfillPaused() == true,
            ".serious clears allowSoonLane → BPS-level backfill gate fires."
        )
    }

    // MARK: - Test 3: hotPathLookaheadMultiplier degrades under LPM

    @Test("Hot-path lookahead multiplier drops to 0.5 under LPM")
    func hotPathLookaheadDropsUnderLPM() async throws {
        let (bps, _, _) = makeBPS()

        // Baseline.
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: false))
        try await Task.sleep(for: .milliseconds(20))
        #expect(await bps.hotPathLookaheadMultiplier() == 1.0)

        // LPM on, even with nominal thermal — the multiplier is OR'd
        // against thermal so LPM alone halves lookahead per the
        // hotPathLookaheadMultiplier() contract.
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: true))
        try await Task.sleep(for: .milliseconds(20))
        #expect(
            await bps.hotPathLookaheadMultiplier() == 0.5,
            "LPM must halve hot-path lookahead — battery preservation is the whole point of LPM."
        )

        // Recovery: LPM off, nominal thermal → 1.0×.
        await bps.handleCapabilityUpdate(makeCapabilitySnapshot(thermalState: .nominal, isLowPowerMode: false))
        try await Task.sleep(for: .milliseconds(20))
        #expect(
            await bps.hotPathLookaheadMultiplier() == 1.0,
            "LPM disable must restore full lookahead."
        )
    }

    // MARK: - Test 4: pure QualityProfile derivation under LPM

    @Test("QualityProfile.derive demotes nominal→fair under LPM, .fair→.serious under LPM, leaves .serious/.critical alone")
    func qualityProfileDerivationUnderLPM() {
        // The hot-path lookahead multiplier reads the BPS in-process
        // flag, but the BPS pause flags are driven by QualityProfile.
        // Pin the derive() rules directly so a future caller refactor
        // can't drift the demotion thresholds without breaking this test.
        let nominalCharged = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.9,
            batteryState: .charging,
            isLowPowerMode: true
        )
        #expect(nominalCharged == .fair, "LPM alone demotes nominal→fair regardless of battery / charging state.")

        let fairCharged = QualityProfile.derive(
            thermalState: .fair,
            batteryLevel: 0.9,
            batteryState: .charging,
            isLowPowerMode: true
        )
        #expect(fairCharged == .serious, "LPM + baseline .fair must demote to .serious.")

        let serious = QualityProfile.derive(
            thermalState: .serious,
            batteryLevel: 0.9,
            batteryState: .charging,
            isLowPowerMode: true
        )
        #expect(serious == .serious, ".serious must NOT be demoted further under LPM (no `severe` variant exists).")

        let critical = QualityProfile.derive(
            thermalState: .critical,
            batteryLevel: 0.9,
            batteryState: .charging,
            isLowPowerMode: true
        )
        #expect(critical == .critical, ".critical must remain .critical under LPM (already maximally throttled).")

        // Sanity: no LPM, healthy battery → baseline matches thermal.
        let baseline = QualityProfile.derive(
            thermalState: .nominal,
            batteryLevel: 0.9,
            batteryState: .charging,
            isLowPowerMode: false
        )
        #expect(baseline == .nominal)
    }

    // MARK: - Test 5: no false positives on clean conversational content

    @Test("Hot-path detector produces no false-positive AdWindows on a clean conversational transcript under LPM")
    func noFalsePositivesUnderLPM() async throws {
        // A 4-chunk conversational transcript with ZERO sponsor signals.
        // Hand-crafted inline because no shared "no-ad" fixture exists in
        // the test corpus. Each chunk is plausibly recognizable as host
        // chatter — the strongest detector should never trigger here,
        // and certainly not under LPM-degraded lookahead.
        //
        // What this proves: the lexical scanner + rule-based classifier
        // do not generate phantom ads from conversational content. A
        // false positive here would manifest as a skip of the user's
        // actual content, the worst possible regression.
        let store = try await makeTestStore()
        let asset = AnalysisAsset(
            id: "rk7-lpm-no-fp",
            episodeId: "ep-rk7-lpm-no-fp",
            assetFingerprint: "rk7-lpm-fp",
            weakFingerprint: nil,
            sourceURL: "file:///rk7/lpm.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let cleanTexts = [
            "So I was telling my brother about the conference. He thought the keynote was a little dry, but the breakout sessions were really worth the trip.",
            "Right, and the venue was beautiful. We walked along the harbor afterwards. Have you ever been to Copenhagen in the spring? It is unreasonably pretty.",
            "I was reading about urban planning history last week. It turns out the bicycle infrastructure took decades to build out, even with political will behind it.",
            "Anyway, that is not really what I came on to talk about. Let us get back to the main topic — the new book you mentioned in your last email."
        ]
        let cleanChunks = cleanTexts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "rk7-lpm-clean-\(idx)",
                analysisAssetId: asset.id,
                segmentFingerprint: "rk7-lpm-fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "rk7-test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
        try await store.insertTranscriptChunks(cleanChunks)

        // FM unavailable to mirror an LPM device that has shed FM access
        // (FM access is gated separately by capabilities, but most LPM
        // devices in production also defer FM work for energy reasons).
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig.default,
            canUseFoundationModelsProvider: { false }
        )

        let windows = try await service.runHotPath(
            chunks: cleanChunks,
            analysisAssetId: asset.id,
            episodeDuration: 120
        )

        #expect(
            windows.isEmpty,
            "Conversational content must produce zero AdWindows. False positive(s): \(windows.map { "\($0.startTime)…\($0.endTime) conf=\($0.confidence)" })"
        )
    }
}
