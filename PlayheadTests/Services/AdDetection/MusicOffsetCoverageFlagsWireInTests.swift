// MusicOffsetCoverageFlagsWireInTests.swift
// playhead-ncv6: tests for the production-enablement plumbing of the three
// coverage-program flags — `sustainedMusicProposerEnabled` (t1py proposer),
// `musicOffsetLexicalGateEnabled` (eki3 gate), and
// `musicOffsetFMRecoveryEnabled` (r2vz recovery) — from `AdDetectionConfig`
// through `AdDetectionService.runBackfill` into `RegionShadowPhase.Input`.
//
// Mirrors the LexicalAnchorRefinementWireInTests structure. Since Dan's
// 2026-07-19 Ship Gate 1 enablement (playhead-lq6f) all three flags default
// to `true` in `AdDetectionConfig.default` AND the init, so the arms pin the
// PRODUCTION-ON state:
//   (a) Flag default / config-init plumbing — all three flags default to
//       `true` in `AdDetectionConfig.default` AND when omitted from the init,
//       and the init stores each flag verbatim (one-at-a-time explicit-FALSE
//       probes against the all-true default, so a swapped assignment cannot
//       slip through).
//   (b) Default byte-identity + OFF still works — running `runBackfill` on
//       the same deterministic fixture (a real ad transcript PLUS a live
//       sustained-music run in the stored feature windows PLUS a wired
//       recovery dispatcher) with the config left at its default vs the three
//       flags explicitly `true` produces identical persisted AdWindow rows
//       and identical dispatcher-consult counts (default == explicit-ON). A
//       third explicit-FALSE arm pins that OFF still works and DIFFERS: no
//       `.sustainedMusicOffset` provenance and zero dispatcher consults even
//       with the music run + dispatcher fully wired.
//   (c) Flag wire-up — each flag flipped in CONFIG (not at the Input site)
//       observably changes `runBackfill` output at the decoded-span seam:
//       proposer-only (gate+recovery explicitly OFF) ⇒ a music-anchored span
//       decodes and persists, and an explicit proposer-OFF arm does not;
//       +gate (recovery explicitly OFF) ⇒ the cue-less music-only span is
//       suppressed WITHOUT consulting the wired dispatcher; +recovery ⇒ the
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

    @Test("AdDetectionConfig.default ships all three coverage flags ON")
    func configDefaultsAreOn() {
        let config = AdDetectionConfig.default
        #expect(config.sustainedMusicProposerEnabled == true,
                "flipped ON 2026-07-19 (playhead-lq6f, Ship Gate 1): certified config measured 47.5% cov / 91.7% true prec / 6.0% false-banner; markOnly-only")
        #expect(config.musicOffsetLexicalGateEnabled == true,
                "flipped ON 2026-07-19 (playhead-lq6f, Ship Gate 1, same certified measurement)")
        #expect(config.musicOffsetFMRecoveryEnabled == true,
                "flipped ON 2026-07-19 (playhead-lq6f, Ship Gate 1, same certified measurement)")
    }

    @Test("AdDetectionConfig.init defaults all three coverage flags to true when omitted")
    func configInitOmittedDefaultsAreOn() {
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1"
        )
        #expect(omitted.sustainedMusicProposerEnabled == true, "init default must match .default (ON post Gate 1)")
        #expect(omitted.musicOffsetLexicalGateEnabled == true, "init default must match .default (ON post Gate 1)")
        #expect(omitted.musicOffsetFMRecoveryEnabled == true, "init default must match .default (ON post Gate 1)")
    }

    @Test("AdDetectionConfig.init carries each coverage flag through verbatim, one at a time")
    func configInitCarriesEachFlagIndependently() {
        // One flag at a time, EXPLICIT-FALSE probes against the now-all-true
        // default: a swapped assignment in the init (or at the runBackfill
        // call site later) cannot pass an all-false probe, so each arm flips
        // exactly one flag OFF and asserts the other two stayed true.
        let proposerOff = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            sustainedMusicProposerEnabled: false
        )
        #expect(proposerOff.sustainedMusicProposerEnabled == false)
        #expect(proposerOff.musicOffsetLexicalGateEnabled == true)
        #expect(proposerOff.musicOffsetFMRecoveryEnabled == true)

        let gateOff = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            musicOffsetLexicalGateEnabled: false
        )
        #expect(gateOff.sustainedMusicProposerEnabled == true)
        #expect(gateOff.musicOffsetLexicalGateEnabled == false)
        #expect(gateOff.musicOffsetFMRecoveryEnabled == true)

        let recoveryOff = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            musicOffsetFMRecoveryEnabled: false
        )
        #expect(recoveryOff.sustainedMusicProposerEnabled == true)
        #expect(recoveryOff.musicOffsetLexicalGateEnabled == true)
        #expect(recoveryOff.musicOffsetFMRecoveryEnabled == false)
    }

    // MARK: - (b) Default byte-identity (== explicit-ON) + explicit-OFF still works and differs

    @Test("Default config: runBackfill is byte-identical to explicit-true flags; explicit-false still works and observably differs")
    func defaultConfigMatchesExplicitTrueAndExplicitFalseDiffers() async throws {
        let assetId = "asset-ncv6-default"
        let storeDefault = try await makeSeededStore(assetId: assetId)
        let storeExplicitOn = try await makeSeededStore(assetId: assetId)
        let storeExplicitOff = try await makeSeededStore(assetId: assetId)

        // Every arm gets a live dispatcher. On the two ON arms the recovery
        // path is reachable and must behave IDENTICALLY (default == explicit
        // true). On the OFF arm the closure must stay unreachable even though
        // it is fully wired and FM availability defaults to true in tests.
        let dispatcherDefault = CountingRecoveryDispatcher(verdict: .ad)
        let dispatcherExplicitOn = CountingRecoveryDispatcher(verdict: .ad)
        let dispatcherExplicitOff = CountingRecoveryDispatcher(verdict: .ad)

        // Default arm: the three flags OMITTED — the acceptance contract that
        // "no config change" carries the production-ON state (Ship Gate 1,
        // playhead-lq6f, 2026-07-19).
        let defaultConfig = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off
            // sustainedMusicProposerEnabled / musicOffsetLexicalGateEnabled /
            // musicOffsetFMRecoveryEnabled omitted → default ON post Gate 1.
        )
        let explicitOnConfig = AdDetectionConfig(
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
        let explicitOffConfig = AdDetectionConfig(
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

        let chunks = makeAdSignalChunks(assetId: assetId)
        try await makeService(store: storeDefault, config: defaultConfig, dispatcher: dispatcherDefault).runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        try await makeService(store: storeExplicitOn, config: explicitOnConfig, dispatcher: dispatcherExplicitOn).runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        try await makeService(store: storeExplicitOff, config: explicitOffConfig, dispatcher: dispatcherExplicitOff).runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )

        let windowsDefault = try await storeDefault.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsExplicitOn = try await storeExplicitOn.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsExplicitOff = try await storeExplicitOff.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        try #require(!windowsDefault.isEmpty, "fixture must produce a window so the byte-identity sweep is meaningful")
        try #require(!windowsExplicitOff.isEmpty, "the OFF arm must still detect the transcript ad — OFF must keep working")
        #expect(
            windowsDefault.count == windowsExplicitOn.count,
            "default \(windowsDefault.count) vs explicit-true \(windowsExplicitOn.count) — omitted flags must equal explicit-ON"
        )
        // Same persisted-field sweep the p56a / l2f.6 / xsdz.37 byte-identity
        // tests pin, now between the default and explicit-TRUE arms.
        for (a, b) in zip(windowsDefault, windowsExplicitOn) {
            #expect(a.startTime == b.startTime, "startTime mismatch default vs explicit-ON")
            #expect(a.endTime == b.endTime, "endTime mismatch default vs explicit-ON")
            #expect(a.confidence == b.confidence, "confidence mismatch default vs explicit-ON")
            #expect(a.decisionState == b.decisionState, "decisionState mismatch default vs explicit-ON")
            #expect(a.eligibilityGate == b.eligibilityGate, "eligibilityGate mismatch default vs explicit-ON")
            #expect(a.wasSkipped == b.wasSkipped, "wasSkipped mismatch default vs explicit-ON")
            #expect(a.boundaryState == b.boundaryState, "boundaryState mismatch default vs explicit-ON")
            #expect(a.detectorVersion == b.detectorVersion, "detectorVersion mismatch default vs explicit-ON")
            #expect(a.metadataSource == b.metadataSource, "metadataSource mismatch default vs explicit-ON")
            #expect(a.metadataConfidence == b.metadataConfidence, "metadataConfidence mismatch default vs explicit-ON")
            #expect(a.evidenceStartTime == b.evidenceStartTime, "evidenceStartTime mismatch default vs explicit-ON")
        }

        // The proposer RAN on both ON arms (identically) and did NOT run on
        // the OFF arm — this is the discriminating "ON differs from OFF"
        // proof that keeps the byte-identity sweep above from going vacuous.
        let spansDefault = try await storeDefault.fetchDecodedSpans(assetId: assetId)
        let spansExplicitOn = try await storeExplicitOn.fetchDecodedSpans(assetId: assetId)
        let spansExplicitOff = try await storeExplicitOff.fetchDecodedSpans(assetId: assetId)
        #expect(!musicAnchoredSpans(spansDefault).isEmpty,
                "default (ON) config must seed a .sustainedMusicOffset span from the live music run")
        #expect(musicAnchoredSpans(spansDefault).count == musicAnchoredSpans(spansExplicitOn).count,
                "default and explicit-true arms must seed the same music-anchored spans")
        #expect(musicAnchoredSpans(spansExplicitOff).isEmpty,
                "explicit-false config must not seed a .sustainedMusicOffset span — OFF must keep the proposer unreachable")

        // Dispatcher consults: identical between the ON arms (whatever the
        // gate decides for this fixture, it must decide it the same way), and
        // ZERO on the OFF arm even though the dispatcher is fully wired.
        let defaultCalls = await dispatcherDefault.callCount
        let explicitOnCalls = await dispatcherExplicitOn.callCount
        #expect(defaultCalls == explicitOnCalls,
                "default (\(defaultCalls)) vs explicit-true (\(explicitOnCalls)) dispatcher consults must match")
        #expect(await dispatcherExplicitOff.callCount == 0,
                "flags OFF must keep the recovery dispatcher unreachable (explicit-false arm)")
    }

    // MARK: - (c) Flag-ON wire-up

    @Test("Config proposer ON (gate+recovery OFF): the sustained-music run decodes to a persisted music-anchored span; proposer OFF does not")
    func configProposerOnSeedsMusicSpanEndToEnd() async throws {
        let assetId = "asset-ncv6-proposer"
        let storeOn = try await makeSeededStore(assetId: assetId)
        let storeOff = try await makeSeededStore(assetId: assetId)

        // ONLY the proposer flag differs between the arms; gate + recovery
        // are explicitly OFF on both (post Gate 1 they default ON, and the
        // gate would suppress this cue-less music-only span with no
        // dispatcher wired). If the call site read a neighboring config field
        // for the proposer Input parameter, no span would decode on the ON
        // arm and this test would fail.
        let configOn = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            sustainedMusicProposerEnabled: true,
            musicOffsetLexicalGateEnabled: false,
            musicOffsetFMRecoveryEnabled: false
        )
        let configOff = AdDetectionConfig(
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

        let chunks = makeAdFreeChunks(assetId: assetId)
        try await makeService(store: storeOn, config: configOn).runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        try await makeService(store: storeOff, config: configOff).runBackfill(
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

        let musicSpansOff = musicAnchoredSpans(try await storeOff.fetchDecodedSpans(assetId: assetId))
        #expect(musicSpansOff.isEmpty,
                "explicit proposer-OFF config must leave the identical fixture without a music-anchored span")
    }

    @Test("Config gate ON (recovery OFF): the cue-less music-only span is suppressed and the wired dispatcher is NOT consulted")
    func configGateOnSuppressesWithoutConsultingDispatcher() async throws {
        let assetId = "asset-ncv6-gate"
        let store = try await makeSeededStore(assetId: assetId)

        // Recovery is EXPLICITLY false in config (post Gate 1 the omitted
        // default is true) while a live .ad dispatcher is wired: if the call
        // site swapped the gate/recovery flags, the Input would get
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
            musicOffsetLexicalGateEnabled: true,
            musicOffsetFMRecoveryEnabled: false
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
