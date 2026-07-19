// MusicOffsetCoverageFlagsWireInTests.swift
// playhead-ncv6: tests for the production-enablement plumbing of the three
// coverage-program flags — `sustainedMusicProposerEnabled` (t1py proposer),
// `musicOffsetLexicalGateEnabled` (eki3 gate), and
// `musicOffsetFMRecoveryEnabled` (r2vz recovery) — from `AdDetectionConfig`
// through `AdDetectionService.runBackfill` into `RegionShadowPhase.Input`.
//
// Mirrors the LexicalAnchorRefinementWireInTests structure:
//   (a) Flag default / config-init plumbing — all three flags default to
//       `false` in `AdDetectionConfig.default` AND when omitted from the init,
//       and the init stores each flag verbatim (one-at-a-time, so a swapped
//       assignment cannot slip through).
//   (b) Flag-OFF byte-identity — running `runBackfill` on the same
//       deterministic fixture (a real ad transcript PLUS a live sustained-music
//       run in the stored feature windows PLUS a wired recovery dispatcher)
//       with the config left at its default vs the three flags explicitly
//       `false` produces identical persisted AdWindow rows, no
//       `.sustainedMusicOffset` provenance, and zero dispatcher consults.
//       Wiring the music run + dispatcher on BOTH arms is the stronger
//       contract: even fully wired, the config gates alone must keep the
//       proposer / gate / recovery unreachable.
//   (c) Flag-ON wire-up — each flag flipped in CONFIG (not at the Input site)
//       observably changes `runBackfill` output at the decoded-span seam:
//       proposer-only ⇒ a music-anchored span decodes and persists; +gate ⇒
//       the cue-less music-only span is suppressed WITHOUT consulting the
//       wired dispatcher (recovery stays false at the Input); +recovery ⇒ the
//       dispatcher is consulted and an `.ad` verdict restores the span.
//       The gate-on/recovery-off arm doubles as the swap detector: if the
//       call site crossed the two flags, the span would survive and the test
//       would fail.

import Foundation
import Testing

@testable import Playhead

@Suite("Music-offset coverage flags config wire-in (playhead-ncv6)")
struct MusicOffsetCoverageFlagsWireInTests {

    private static let podcastId = "podcast-ncv6"
    private static let episodeDuration = 90.0

    // MARK: - Fixtures

    /// Ad-FREE transcript: 30 contiguous 3s chunks across [0, 90). All text is
    /// deliberately ad-free so the ONLY proposal source under test is the music
    /// proposer (no lexical / sponsor / fingerprint / classifier co-firing).
    /// Same shape as the SustainedMusicOffsetSeamIntegrationTests fixture, so
    /// the proposer / gate / recovery behavior at the RegionShadowPhase layer
    /// is already pinned — these tests pin the CONFIG threading on top.
    private func makeAdFreeChunks(assetId: String) -> [TranscriptChunk] {
        let chunkDuration = 3.0
        let count = Int(Self.episodeDuration / chunkDuration)  // 30
        return (0..<count).map { idx in
            let start = Double(idx) * chunkDuration
            let text = "Segment \(idx) of ordinary spoken conversation about coastal tide pools and slow patient observation."
            return TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: start,
                endTime: start + chunkDuration,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "v",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    /// Ad-SIGNAL transcript (3 × 30s, ad in [30, 60)) — produces a persisted
    /// AdWindow so the byte-identity sweep in (b) has a nontrivial baseline.
    /// Same fixture the xsdz.37 wire-in tests use.
    private func makeAdSignalChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "This is the introduction to the program with our host and guest.",
            "We will be right back. This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.",
            "Now we continue our discussion about technology and the future."
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

    /// Feature windows: flat RMS / flux / pause everywhere (so the acoustic
    /// break detector finds NOTHING and cannot confound the test), with a
    /// sustained high `musicProbability` play-out run in [60, 74) that the
    /// t1py proposer WOULD seed a region from if the config flag reaches the
    /// Input as `true`.
    private func makeMusicRunFeatureWindows(assetId: String) -> [FeatureWindow] {
        var windows: [FeatureWindow] = []
        var t: Double = 0
        while t < Self.episodeDuration {
            let inMusicRun = t >= 60 && t < 74
            windows.append(
                FeatureWindow(
                    analysisAssetId: assetId,
                    startTime: t,
                    endTime: t + 2.0,
                    rms: 0.3,
                    spectralFlux: 0.05,
                    musicProbability: inMusicRun ? 0.9 : 0.0,
                    pauseProbability: 0.0,
                    speakerClusterId: 1,
                    jingleHash: nil,
                    featureVersion: 5
                )
            )
            t += 2.0
        }
        return windows
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

    /// Store pre-loaded with the asset row and the music-run feature windows,
    /// so `runBackfill`'s `fetchFeatureWindows` sees the sustained-music run.
    private func makeSeededStore(assetId: String) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(makeMusicRunFeatureWindows(assetId: assetId))
        return store
    }

    private func makeService(
        store: AnalysisStore,
        config: AdDetectionConfig,
        dispatcher: FMRegionRecoveryDispatcher? = nil
    ) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            fmRegionRecoveryDispatcher: dispatcher
        )
    }

    private func musicAnchoredSpans(_ spans: [DecodedSpan]) -> [DecodedSpan] {
        spans.filter { span in
            span.anchorProvenance.contains {
                if case .sustainedMusicOffset = $0 { return true }
                return false
            }
        }
    }

    // MARK: - (a) Config defaults

    @Test("AdDetectionConfig.default ships all three coverage flags OFF")
    func configDefaultsAreOff() {
        let config = AdDetectionConfig.default
        #expect(config.sustainedMusicProposerEnabled == false,
                "t1py proposer ships OFF; enablement is a separate product decision")
        #expect(config.musicOffsetLexicalGateEnabled == false,
                "eki3 gate ships OFF; enablement is a separate product decision")
        #expect(config.musicOffsetFMRecoveryEnabled == false,
                "r2vz recovery ships OFF; enablement is a separate product decision")
    }

    @Test("AdDetectionConfig.init defaults all three coverage flags to false when omitted")
    func configInitOmittedDefaultsAreOff() {
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1"
        )
        #expect(omitted.sustainedMusicProposerEnabled == false, "init default must match .default")
        #expect(omitted.musicOffsetLexicalGateEnabled == false, "init default must match .default")
        #expect(omitted.musicOffsetFMRecoveryEnabled == false, "init default must match .default")
    }

    @Test("AdDetectionConfig.init carries each coverage flag through verbatim, one at a time")
    func configInitCarriesEachFlagIndependently() {
        // One flag at a time: a swapped assignment in the init (or at the
        // runBackfill call site later) cannot pass an all-true probe, so each
        // arm flips exactly one flag and asserts the other two stayed false.
        let proposerOnly = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            sustainedMusicProposerEnabled: true
        )
        #expect(proposerOnly.sustainedMusicProposerEnabled == true)
        #expect(proposerOnly.musicOffsetLexicalGateEnabled == false)
        #expect(proposerOnly.musicOffsetFMRecoveryEnabled == false)

        let gateOnly = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            musicOffsetLexicalGateEnabled: true
        )
        #expect(gateOnly.sustainedMusicProposerEnabled == false)
        #expect(gateOnly.musicOffsetLexicalGateEnabled == true)
        #expect(gateOnly.musicOffsetFMRecoveryEnabled == false)

        let recoveryOnly = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            musicOffsetFMRecoveryEnabled: true
        )
        #expect(recoveryOnly.sustainedMusicProposerEnabled == false)
        #expect(recoveryOnly.musicOffsetLexicalGateEnabled == false)
        #expect(recoveryOnly.musicOffsetFMRecoveryEnabled == true)
    }

    // MARK: - (b) Flag-OFF byte-identity

    @Test("Default config: runBackfill is byte-identical to explicit-false flags even with a live music run and a wired dispatcher")
    func defaultConfigMatchesExplicitFalseBaseline() async throws {
        let assetId = "asset-ncv6-off"
        let storeDefault = try await makeSeededStore(assetId: assetId)
        let storeExplicit = try await makeSeededStore(assetId: assetId)

        // Both arms get a live dispatcher: with all three flags false at the
        // Input, the recovery closure must be unreachable even though it is
        // fully wired and FM availability defaults to true in tests.
        let dispatcherDefault = CountingRecoveryDispatcher(verdict: .ad)
        let dispatcherExplicit = CountingRecoveryDispatcher(verdict: .ad)

        // Default arm: the three flags OMITTED — the acceptance contract that
        // "no config change" carries the same three false values as today.
        let defaultConfig = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off
            // sustainedMusicProposerEnabled / musicOffsetLexicalGateEnabled /
            // musicOffsetFMRecoveryEnabled omitted → default OFF.
        )
        let explicitConfig = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            sustainedMusicProposerEnabled: false,
            musicOffsetLexicalGateEnabled: false,
            musicOffsetFMRecoveryEnabled: false
        )

        let serviceDefault = makeService(store: storeDefault, config: defaultConfig, dispatcher: dispatcherDefault)
        let serviceExplicit = makeService(store: storeExplicit, config: explicitConfig, dispatcher: dispatcherExplicit)

        let chunks = makeAdSignalChunks(assetId: assetId)
        try await serviceDefault.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        try await serviceExplicit.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )

        let windowsDefault = try await storeDefault.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsExplicit = try await storeExplicit.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        try #require(!windowsDefault.isEmpty, "fixture must produce a window so the byte-identity sweep is meaningful")
        #expect(
            windowsDefault.count == windowsExplicit.count,
            "default \(windowsDefault.count) vs explicit-false \(windowsExplicit.count) — flags OFF must be byte-identical"
        )
        // Same persisted-field sweep the p56a / l2f.6 / xsdz.37 byte-identity tests pin.
        for (a, b) in zip(windowsDefault, windowsExplicit) {
            #expect(a.startTime == b.startTime, "startTime mismatch under flags OFF")
            #expect(a.endTime == b.endTime, "endTime mismatch under flags OFF")
            #expect(a.confidence == b.confidence, "confidence mismatch under flags OFF")
            #expect(a.decisionState == b.decisionState, "decisionState mismatch under flags OFF")
            #expect(a.eligibilityGate == b.eligibilityGate, "eligibilityGate mismatch under flags OFF")
            #expect(a.wasSkipped == b.wasSkipped, "wasSkipped mismatch under flags OFF")
            #expect(a.boundaryState == b.boundaryState, "boundaryState mismatch under flags OFF")
            #expect(a.detectorVersion == b.detectorVersion, "detectorVersion mismatch under flags OFF")
            #expect(a.metadataSource == b.metadataSource, "metadataSource mismatch under flags OFF")
            #expect(a.metadataConfidence == b.metadataConfidence, "metadataConfidence mismatch under flags OFF")
            #expect(a.evidenceStartTime == b.evidenceStartTime, "evidenceStartTime mismatch under flags OFF")
        }

        // No music-origin span decodes on either arm — the proposer never ran.
        let spansDefault = try await storeDefault.fetchDecodedSpans(assetId: assetId)
        let spansExplicit = try await storeExplicit.fetchDecodedSpans(assetId: assetId)
        #expect(musicAnchoredSpans(spansDefault).isEmpty,
                "default config must not seed a .sustainedMusicOffset span")
        #expect(musicAnchoredSpans(spansExplicit).isEmpty,
                "explicit-false config must not seed a .sustainedMusicOffset span")

        // The wired dispatcher was never consulted on either arm.
        #expect(await dispatcherDefault.callCount == 0,
                "flags OFF must keep the recovery dispatcher unreachable (default arm)")
        #expect(await dispatcherExplicit.callCount == 0,
                "flags OFF must keep the recovery dispatcher unreachable (explicit arm)")
    }

    // MARK: - (c) Flag-ON wire-up

    @Test("Config proposer ON: the sustained-music run decodes to a persisted music-anchored span; default config does not")
    func configProposerOnSeedsMusicSpanEndToEnd() async throws {
        let assetId = "asset-ncv6-proposer"
        let storeOn = try await makeSeededStore(assetId: assetId)
        let storeDefault = try await makeSeededStore(assetId: assetId)

        // ONLY the proposer flag flips — if the call site read a neighboring
        // config field for this Input parameter, no span would decode.
        let configOn = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            sustainedMusicProposerEnabled: true
        )
        let configDefault = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off
        )

        let chunks = makeAdFreeChunks(assetId: assetId)
        try await makeService(store: storeOn, config: configOn).runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        try await makeService(store: storeDefault, config: configDefault).runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )

        let musicSpansOn = musicAnchoredSpans(try await storeOn.fetchDecodedSpans(assetId: assetId))
        #expect(musicSpansOn.count == 1,
                "config proposer ON must thread to the Input and decode exactly one music-anchored span")
        if let span = musicSpansOn.first {
            #expect(span.startTime >= 59.0 && span.endTime <= Self.episodeDuration,
                    "the decoded span must sit at the post-roll music run (got [\(span.startTime), \(span.endTime)))")
            #expect(span.anchorProvenance.allSatisfy {
                if case .sustainedMusicOffset = $0 { return true }
                return false
            }, "the decoded span must be music-ONLY (no other anchor) on the ad-free fixture")
        }

        let musicSpansDefault = musicAnchoredSpans(try await storeDefault.fetchDecodedSpans(assetId: assetId))
        #expect(musicSpansDefault.isEmpty,
                "default config must leave the identical fixture without a music-anchored span")
    }

    @Test("Config gate ON (recovery OFF): the cue-less music-only span is suppressed and the wired dispatcher is NOT consulted")
    func configGateOnSuppressesWithoutConsultingDispatcher() async throws {
        let assetId = "asset-ncv6-gate"
        let store = try await makeSeededStore(assetId: assetId)

        // Recovery stays FALSE in config while a live .ad dispatcher is wired:
        // if the call site swapped the gate/recovery flags, the Input would get
        // gate=false and the music span would survive — failing this test. If
        // the recovery flag leaked true, the dispatcher would be consulted —
        // also failing this test.
        let dispatcher = CountingRecoveryDispatcher(verdict: .ad)
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            sustainedMusicProposerEnabled: true,
            musicOffsetLexicalGateEnabled: true
        )

        try await makeService(store: store, config: config, dispatcher: dispatcher).runBackfill(
            chunks: makeAdFreeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: Self.episodeDuration
        )

        let musicSpans = musicAnchoredSpans(try await store.fetchDecodedSpans(assetId: assetId))
        #expect(musicSpans.isEmpty,
                "gate ON must suppress the cue-less music-only span (PR1 drop path)")
        #expect(await dispatcher.callCount == 0,
                "recovery OFF in config must keep the wired dispatcher unreachable")
    }

    @Test("Config recovery ON (all three flags): the dispatcher is consulted and an .ad verdict restores the music span")
    func configRecoveryOnConsultsDispatcherAndRestores() async throws {
        let assetId = "asset-ncv6-recovery"
        let store = try await makeSeededStore(assetId: assetId)

        let dispatcher = CountingRecoveryDispatcher(verdict: .ad)
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            sustainedMusicProposerEnabled: true,
            musicOffsetLexicalGateEnabled: true,
            musicOffsetFMRecoveryEnabled: true
        )

        try await makeService(store: store, config: config, dispatcher: dispatcher).runBackfill(
            chunks: makeAdFreeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: Self.episodeDuration
        )

        #expect(await dispatcher.callCount >= 1,
                "recovery ON in config must reach the dispatcher for the gate-suppressed span")

        let musicSpans = musicAnchoredSpans(try await store.fetchDecodedSpans(assetId: assetId))
        #expect(musicSpans.count == 1,
                "the .ad verdict must restore exactly the one music-only span")
        if let span = musicSpans.first {
            #expect(span.anchorProvenance.allSatisfy {
                if case .sustainedMusicOffset = $0 { return true }
                return false
            }, "the restored span must stay music-ONLY (recovery is an admit/drop gate, never an FM anchor)")
        }
    }
}

/// Counts recovery-dispatcher invocations and returns a fixed verdict.
/// Conforms to the production `FMRegionRecoveryDispatcher` protocol so the
/// wire-in exercises the same adapter path `PlayheadRuntime` uses.
private actor CountingRecoveryDispatcher: FMRegionRecoveryDispatcher {
    private(set) var callCount = 0
    private let verdict: FMRegionVerdict

    init(verdict: FMRegionVerdict) {
        self.verdict = verdict
    }

    func classify(region: ProposedRegion, atoms: [TranscriptAtom]) async -> FMRegionVerdict {
        callCount += 1
        return verdict
    }
}
