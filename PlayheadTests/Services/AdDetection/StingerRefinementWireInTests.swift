// StingerRefinementWireInTests.swift
// playhead-l2f.6: tests for the StingerBank boundary-refinement wire-in
// into `AdDetectionService.runBackfill`.
//
// Mirrors the `SpanFinalizerWireInTests` (playhead-p56a) structure:
//   (a) Flag default / config-init plumbing — `stingerRefinementEnabled`
//       defaults to `false` in `AdDetectionConfig.default`, and the init
//       carries the arg through.
//   (b) Flag-OFF byte-identity — running `runBackfill` twice on the same
//       deterministic fixture (once with the flag explicitly OFF AND a
//       live bank + snapping PCM provider injected, once with the config
//       left at its default and nothing injected) produces identical
//       persisted AdWindow rows. Injecting the bank on the OFF arm is the
//       stronger contract: even with everything wired, the flag gate alone
//       must keep the refiner unreachable. Also pins that both stinger
//       trace maps stay empty when the flag is OFF.
//   (c) Flag-ON wire-up — a synthetic bank entry keyed to the runBackfill
//       `podcastId` (the production join key) plus a deterministic PCM
//       provider drives a real end-to-end snap: the persisted window's
//       start edge moves to the planted stinger, the window count is
//       unchanged (the refiner never splits or merges), and both trace
//       maps are populated. A second flag-ON arm with NO available PCM
//       pins the graceful no-snap degradation.

import Foundation
import Testing
@testable import Playhead

@Suite("StingerRefinement wire-in (playhead-l2f.6)")
struct StingerRefinementWireInTests {

    // MARK: - Synthetic audio world
    //
    // PCM is generated as a pure function of ABSOLUTE episode time:
    // silence everywhere except a 7 s amplitude-stepped burst at
    // 20.0–27.0 s (the "stinger"). Because the function is absolute, the
    // provider returns consistent audio for whatever search span the
    // service requests, and the bank template built from [14, 21) matches
    // the burst onset at exactly 20.0 s.

    private static let sampleRate = 16_000
    private static let burstStart = 20.0
    private static let burstPattern: [Float] = [
        0.9, 0.1, 0.7, 0.2, 0.8, 0.05, 0.6,
        0.3, 0.95, 0.15, 0.5, 0.25, 0.85, 0.4,
    ]

    private static func amplitude(at t: Double) -> Float {
        guard t >= burstStart, t < burstStart + 7.0 else { return 0 }
        let segment = Int((t - burstStart) / 0.5)
        return burstPattern[min(segment, burstPattern.count - 1)]
    }

    private static func syntheticSamples(from start: Double, to end: Double) -> [Float] {
        let count = Int(((end - start) * Double(sampleRate)).rounded())
        guard count > 0 else { return [] }
        return (0..<count).map { i in
            amplitude(at: start + Double(i) / Double(sampleRate))
        }
    }

    /// PCM provider that always serves the synthetic world.
    private static let snappingPCMProvider: StingerPCMProvider = { _, start, end in
        StingerPCMSlice(
            samples: syntheticSamples(from: start, to: end),
            startSeconds: start
        )
    }

    /// PCM provider modelling "shard cache absent" — no PCM anywhere.
    private static let noPCMProvider: StingerPCMProvider = { _, _, _ in nil }

    /// Bank with one show keyed by the EXACT `podcastId` the tests pass to
    /// `runBackfill` — pinning the production join-key contract (exact
    /// string match on the runtime show key). Built through
    /// `StingerBank.decode` so the wire-in consumes a bank that went
    /// through the same validation path production uses.
    private static func makeBank(showKey: String) throws -> StingerBank {
        let templateSamples = syntheticSamples(
            from: burstStart - 6.0,
            to: burstStart + 1.0
        )
        let template = StingerEnvelope.compute(samples: templateSamples)
        let payload: [String: Any] = [
            "schemaVersion": 1,
            "envelopeHz": 50,
            "pcmSampleRate": 16_000,
            "shows": [
                [
                    "showKeys": [showKey],
                    "showName": "Wire-In Test Show",
                    "pre": [
                        "template": template.map(Double.init),
                        "edgeSampleIndex": 300,
                        "edgeOffsetSeconds": 0.0,
                        "confidence": 0.9,
                        "support": 3,
                    ],
                ],
            ],
        ]
        let data = try JSONSerialization.data(withJSONObject: payload)
        return try StingerBank.decode(data)
    }

    // MARK: - Fixture (mirrors SpanFinalizerWireInTests)

    private func makeAdSignalChunks(assetId: String) -> [TranscriptChunk] {
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

    private func makeService(
        store: AnalysisStore,
        stingerRefinementEnabled: Bool,
        stingerBank: StingerBank? = nil,
        stingerPCMProvider: StingerPCMProvider? = nil
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            stingerRefinementEnabled: stingerRefinementEnabled
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            stingerBank: stingerBank,
            stingerPCMProvider: stingerPCMProvider
        )
    }

    private static let podcastId = "podcast-test"

    // MARK: - (a) Config defaults

    @Test("AdDetectionConfig.default keeps stinger refinement OFF")
    func configDefaultsAreOff() {
        let config = AdDetectionConfig.default
        #expect(
            config.stingerRefinementEnabled == false,
            "OFF-by-default is load-bearing: production must stay behavior-neutral until the Catalyst dump + gold-scorer measurement confirms the lift"
        )
    }

    @Test("AdDetectionConfig init carries stingerRefinementEnabled through")
    func configInitCarriesFlag() {
        let off = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1",
            stingerRefinementEnabled: false
        )
        #expect(off.stingerRefinementEnabled == false)

        let on = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1",
            stingerRefinementEnabled: true
        )
        #expect(on.stingerRefinementEnabled == true)

        // The init's default value MUST match `.default` so callers that
        // omit the arg get the OFF path.
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1"
        )
        #expect(omitted.stingerRefinementEnabled == false, "init default must match .default")
    }

    // MARK: - (b) Flag-OFF byte-identity

    @Test("Flag OFF: runBackfill is byte-identical to the default config even with a bank + PCM provider wired")
    func flagOffMatchesDefaultBaseline() async throws {
        // Two independent stores, two independent service instances, same
        // synthetic transcript. The first runs with the flag explicitly
        // OFF but a LIVE bank (keyed to this very podcastId) and a PCM
        // provider that WOULD snap the start edge to 20.0s if the refiner
        // were ever consulted. The second leaves the config at its (OFF)
        // default with nothing injected. If any persisted field diverges,
        // the flag gate is leaking.
        let storeExplicit = try await makeTestStore()
        let storeDefault = try await makeTestStore()
        let assetId = "asset-l2f6-off"
        try await storeExplicit.insertAsset(makeAsset(id: assetId))
        try await storeDefault.insertAsset(makeAsset(id: assetId))

        let serviceExplicit = makeService(
            store: storeExplicit,
            stingerRefinementEnabled: false,
            stingerBank: try Self.makeBank(showKey: Self.podcastId),
            stingerPCMProvider: Self.snappingPCMProvider
        )
        let defaultConfig = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off
            // stingerRefinementEnabled omitted → init default applies → false
        )
        let serviceDefault = AdDetectionService(
            store: storeDefault,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: defaultConfig
        )

        let chunks = makeAdSignalChunks(assetId: assetId)

        try await serviceExplicit.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 90.0
        )
        try await serviceDefault.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 90.0
        )

        let windowsExplicit = try await storeExplicit.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsDefault = try await storeDefault.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        #expect(
            windowsExplicit.count == windowsDefault.count,
            "explicit-OFF arm produced \(windowsExplicit.count) windows; default arm produced \(windowsDefault.count) — flag OFF must be byte-identical"
        )

        // Same persisted-field sweep the p56a byte-identity test pins:
        // every field the wire-in could plausibly mutate when ON.
        for (a, b) in zip(windowsExplicit, windowsDefault) {
            #expect(a.startTime == b.startTime, "startTime mismatch under flag OFF")
            #expect(a.endTime == b.endTime, "endTime mismatch under flag OFF")
            #expect(a.confidence == b.confidence, "confidence mismatch under flag OFF")
            #expect(a.decisionState == b.decisionState, "decisionState mismatch under flag OFF")
            #expect(a.eligibilityGate == b.eligibilityGate, "eligibilityGate mismatch under flag OFF")
            #expect(a.wasSkipped == b.wasSkipped, "wasSkipped mismatch under flag OFF")
            #expect(a.boundaryState == b.boundaryState, "boundaryState mismatch under flag OFF")
            #expect(a.detectorVersion == b.detectorVersion, "detectorVersion mismatch under flag OFF")
            #expect(a.metadataSource == b.metadataSource, "metadataSource mismatch under flag OFF")
            #expect(a.metadataConfidence == b.metadataConfidence, "metadataConfidence mismatch under flag OFF")
            #expect(a.evidenceStartTime == b.evidenceStartTime, "evidenceStartTime mismatch under flag OFF")
        }

        // Both stinger trace maps must be empty under flag OFF — no
        // consult, no allocation. Asserts the no-cost contract documented
        // on the fields.
        let traceBySpan = await serviceExplicit.stingerRefinementTraceBySpanIdForTesting()
        let traceByWindow = await serviceExplicit.stingerRefinementTraceByWindowIdForTesting()
        #expect(traceBySpan.isEmpty, "flag OFF must leave the spanId trace empty")
        #expect(traceByWindow.isEmpty, "flag OFF must leave the windowId trace empty")
    }

    // MARK: - (c) Flag-ON wire-up

    @Test("Flag ON: a planted stinger snaps the persisted window start end-to-end and stamps both trace maps")
    func flagOnSnapsWindowStartEndToEnd() async throws {
        // OFF baseline first, so the snap assertion is relative to what
        // the pipeline persists without refinement.
        let storeOff = try await makeTestStore()
        let storeOn = try await makeTestStore()
        let assetId = "asset-l2f6-on"
        try await storeOff.insertAsset(makeAsset(id: assetId))
        try await storeOn.insertAsset(makeAsset(id: assetId))
        let chunks = makeAdSignalChunks(assetId: assetId)

        let serviceOff = makeService(store: storeOff, stingerRefinementEnabled: false)
        try await serviceOff.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 90.0
        )
        let windowsOff = try await storeOff.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        try #require(!windowsOff.isEmpty, "fixture must produce at least one window for the snap pin to be meaningful")
        let baselineStart = windowsOff[0].startTime
        try #require(
            abs(baselineStart - Self.burstStart) <= StingerRefiner.maxEdgeMoveSeconds,
            "fixture invariant: baseline start \(baselineStart) must be within the move cap of the planted stinger"
        )

        let serviceOn = makeService(
            store: storeOn,
            stingerRefinementEnabled: true,
            stingerBank: try Self.makeBank(showKey: Self.podcastId),
            stingerPCMProvider: Self.snappingPCMProvider
        )
        try await serviceOn.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 90.0
        )
        let windowsOn = try await storeOn.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        // Never split, never merge: exactly the same number of windows.
        #expect(
            windowsOn.count == windowsOff.count,
            "stinger refinement must never split or merge windows (\(windowsOff.count) OFF vs \(windowsOn.count) ON)"
        )

        // The first window's start snapped to the planted stinger onset
        // (20.0s ± one 20ms envelope frame); its end is untouched (the
        // bank has no post template and no grid).
        let snapped = try #require(windowsOn.first)
        #expect(
            abs(snapped.startTime - Self.burstStart) < 0.05,
            "window start must snap to the planted stinger at \(Self.burstStart)s (got \(snapped.startTime), baseline \(baselineStart))"
        )
        #expect(
            snapped.endTime == windowsOff[0].endTime,
            "end edge has no post template and must not move (got \(snapped.endTime) vs \(windowsOff[0].endTime))"
        )
        #expect(snapped.endTime > snapped.startTime, "refined window must keep end > start")

        // Both trace maps populated; the per-window entry records the
        // start snap with a near-perfect correlation peak.
        let traceBySpan = await serviceOn.stingerRefinementTraceBySpanIdForTesting()
        let traceByWindow = await serviceOn.stingerRefinementTraceByWindowIdForTesting()
        #expect(!traceBySpan.isEmpty, "flag ON must populate the spanId trace map")
        #expect(!traceByWindow.isEmpty, "flag ON must populate the windowId trace map")
        let windowTrace = try #require(
            traceByWindow[snapped.id],
            "the snapped window's id must resolve a trace (ids: \(traceByWindow.keys.sorted()))"
        )
        #expect(windowTrace.startSnapped, "trace must record the start snap")
        #expect(!windowTrace.endSnapped)
        #expect(!windowTrace.gridApplied)
        #expect(!windowTrace.revertedNoOverlap)
        let peak = try #require(windowTrace.startPeak)
        #expect(peak > 0.99, "planted template must correlate at ~1.0 (got \(peak))")
        let delta = try #require(windowTrace.startDeltaSeconds)
        #expect(
            abs(delta - (Self.burstStart - baselineStart)) < 0.05,
            "recorded delta \(delta) must equal snap-minus-baseline \(Self.burstStart - baselineStart)"
        )
    }

    @Test("Flag ON without PCM: bounds match the OFF baseline and the trace records the no-snap consult")
    func flagOnWithoutPCMDegradesToNoSnap() async throws {
        let storeOff = try await makeTestStore()
        let storeOn = try await makeTestStore()
        let assetId = "asset-l2f6-nopcm"
        try await storeOff.insertAsset(makeAsset(id: assetId))
        try await storeOn.insertAsset(makeAsset(id: assetId))
        let chunks = makeAdSignalChunks(assetId: assetId)

        let serviceOff = makeService(store: storeOff, stingerRefinementEnabled: false)
        let serviceOn = makeService(
            store: storeOn,
            stingerRefinementEnabled: true,
            stingerBank: try Self.makeBank(showKey: Self.podcastId),
            stingerPCMProvider: Self.noPCMProvider
        )

        try await serviceOff.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 90.0
        )
        try await serviceOn.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 90.0
        )

        let windowsOff = try await storeOff.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsOn = try await storeOn.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        #expect(windowsOn.count == windowsOff.count)
        for (a, b) in zip(windowsOn, windowsOff) {
            #expect(a.startTime == b.startTime, "no PCM ⇒ no snap ⇒ bounds must match the OFF baseline")
            #expect(a.endTime == b.endTime, "no PCM ⇒ no snap ⇒ bounds must match the OFF baseline")
        }

        // The refiner WAS consulted (bank entry resolved), so the trace
        // records the no-snap outcome — distinguishing "flag OFF" from
        // "flag ON but nothing snapped" for the measurement dump.
        let traceBySpan = await serviceOn.stingerRefinementTraceBySpanIdForTesting()
        #expect(!traceBySpan.isEmpty, "a consulted refiner must record a trace even when nothing snapped")
        for trace in traceBySpan.values {
            #expect(trace == StingerRefinementTrace(), "no PCM ⇒ pristine no-snap trace (got \(trace))")
        }
    }

    @Test("Flag ON with no bank entry for the show: refiner never consulted, trace stays empty")
    func flagOnUnknownShowLeavesTraceEmpty() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-l2f6-unknown-show"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(
            store: store,
            stingerRefinementEnabled: true,
            stingerBank: try Self.makeBank(showKey: "some-other-show"),
            stingerPCMProvider: Self.snappingPCMProvider
        )
        try await service.runBackfill(
            chunks: makeAdSignalChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 90.0
        )
        let traceBySpan = await service.stingerRefinementTraceBySpanIdForTesting()
        let traceByWindow = await service.stingerRefinementTraceByWindowIdForTesting()
        #expect(traceBySpan.isEmpty, "no bank entry for this show ⇒ refiner never consulted")
        #expect(traceByWindow.isEmpty)
    }
}
