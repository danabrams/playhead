// CertaintyTieredSkipFlagsWireInTests.swift
// playhead-wraj (SURFACING slice): tests for the production-enablement plumbing
// of the three certainty-tiered auto-skip gate flags — `certaintyTieredSkipEnabled`
// (master switch), `hostReadConfidenceFloor` (0.9 themove host-read calibration),
// and `postRollGuardSeconds` (90.0 post-roll guard) — from `AdDetectionConfig`
// through `AdDetectionService.runBackfill` into the `FusionWeightConfig` the
// `BackfillEvidenceFusion` `DecisionMapper` consumes.
//
// Mirrors the MusicOffsetCoverageFlagsWireInTests (playhead-ncv6) structure.
// The gate LOGIC (the post-gate `.eligible → .markOnly` downgrade) shipped in
// PR #237 inside `FusionWeightConfig` / `DecisionMapper` and is exhaustively
// covered by BackfillEvidenceFusionTests; this suite pins the CONFIG THREADING
// on top — that the three new `AdDetectionConfig` fields reach the fusion config
// at the real `runBackfill` construction site (the ONLY production path to the
// gate; `runBackfill` previously built a bare `FusionWeightConfig()`):
//   (a) Config default / init plumbing — the three flags default to
//       false / 0.9 / 90.0 in `AdDetectionConfig.default` AND when omitted from
//       the init, and the init stores each verbatim (one-at-a-time probes so a
//       swapped assignment cannot slip through).
//   (c) Default-OFF byte-identity at the decision seam — running `runBackfill`
//       on a gate-SENSITIVE fixture (a lexical ad that decodes to an eligible,
//       non-rediff, host-read span the gate WOULD demote) with the config left
//       at its default vs the three flags explicitly at their default values
//       produces byte-identical persisted AdWindow rows (default == explicit-
//       false; the OFF gate never fires). Non-vacuous because the SAME fixture
//       demotes under the flag-ON arms in (d).
//   (d) Flag-ON threading — each of the three fields, flipped in CONFIG (not at
//       the `FusionWeightConfig` call site), observably changes `runBackfill`
//       output at the persisted-gate seam. Four arms on one fixture, robust to
//       the fixture's exact `skipConfidence`:
//         OFF        (default, enabled=false)        → eligible span survives.
//         ON-inert   (enabled, floor=0.0, guard=0.0) → survives (both demoters
//                                                       inert) == OFF eligible set.
//         ON-floor   (enabled, floor=2.0, guard=0.0) → demoted; the ONLY change
//                                                       vs ON-inert is the floor,
//                                                       so `hostReadConfidenceFloor`
//                                                       is threaded + consumed.
//         ON-guard   (enabled, floor=0.0, guard=1e6) → demoted; the ONLY change
//                                                       vs ON-inert is the guard,
//                                                       so `postRollGuardSeconds`
//                                                       is threaded + consumed.
//       Enabling the master switch is necessary for ANY demotion (OFF vs
//       ON-floor differ), proving `certaintyTieredSkipEnabled` threads too.

import Foundation
import Testing

@testable import Playhead

@Suite("Certainty-tiered skip flags config wire-in (playhead-wraj)")
struct CertaintyTieredSkipFlagsWireInTests {

    private static let podcastId = "podcast-wraj"
    // Episode duration is deliberately LONG so the early [30, 60) lexical ad
    // sits ~1740s from the end: the default 90s post-roll guard (and the
    // ON-inert 0.0 guard) is inert for it, and ONLY the ON-guard arm's 1e6
    // window can reach it. This isolates the floor demotion from the guard
    // demotion.
    private static let episodeDuration = 1800.0

    // MARK: - Fixtures

    /// Ad-SIGNAL transcript (3 × 30s, lexical ad in [30, 60)) — the same
    /// Squarespace fixture the ncv6 / xsdz.37 wire-in suites use. Produces a
    /// persisted AdWindow whose span is non-FM, non-rediff (no B-side provider
    /// is wired, so the rediff pass never stamps `.rediffSlot`), and carries
    /// in-audio (lexical + classifier) corroboration → `computeGate()` returns
    /// `.eligible`. That eligible, non-rediff span is exactly what the wraj
    /// host-read floor and post-roll guard demote, so this fixture is
    /// gate-SENSITIVE (keeps the byte-identity sweep in (c) honest).
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

    private func makeSeededStore(assetId: String) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        return store
    }

    private func makeService(store: AnalysisStore, config: AdDetectionConfig) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    /// Base config with the flags OMITTED — proves "no config change" carries
    /// the production-OFF state. `fmBackfillMode: .off` keeps the pipeline
    /// deterministic (no FoundationModels dependence in the harness).
    private func makeBaseConfig() -> AdDetectionConfig {
        AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off
            // certaintyTieredSkipEnabled / hostReadConfidenceFloor /
            // postRollGuardSeconds omitted → default false / 0.9 / 90.0.
        )
    }

    /// Config with the three certainty-tiered fields set explicitly. All other
    /// fields match `makeBaseConfig()`, so the ONLY variable across arms is the
    /// trio under test.
    private func makeTieredConfig(
        enabled: Bool,
        floor: Double,
        guardSeconds: Double
    ) -> AdDetectionConfig {
        AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            certaintyTieredSkipEnabled: enabled,
            hostReadConfidenceFloor: floor,
            postRollGuardSeconds: guardSeconds
        )
    }

    private func fetchWindows(_ store: AnalysisStore, assetId: String) async throws -> [AdWindow] {
        try await store.fetchAdWindows(assetId: assetId).sorted { $0.startTime < $1.startTime }
    }

    /// Sorted start times of the persisted windows whose gate is `.eligible`
    /// (raw value "eligible"). The wraj downgrade turns exactly these into
    /// `.markOnly`, so the set shrinking is the observable of a fired gate.
    private func eligibleStarts(_ windows: [AdWindow]) -> [Double] {
        windows
            .filter { $0.eligibilityGate == SkipEligibilityGate.eligible.rawValue }
            .map(\.startTime)
            .sorted()
    }

    // MARK: - (a) Config defaults + init threading

    @Test("AdDetectionConfig.default ships the certainty-tiered gate OFF at the calibrated floor + guard")
    func configDefaultsAreOff() {
        let config = AdDetectionConfig.default
        #expect(config.certaintyTieredSkipEnabled == false,
                "the certainty-tiered auto-skip gate ships OFF — enablement is Dan's Gate-2 decision")
        #expect(config.hostReadConfidenceFloor == 0.9,
                "T=0.9 themove host-read calibration (2026-07-17)")
        #expect(config.postRollGuardSeconds == 90.0,
                "90s post-roll guard window (Dan 2026-07-19)")
    }

    @Test("AdDetectionConfig.init defaults the three certainty-tiered fields when omitted")
    func configInitOmittedDefaults() {
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1"
        )
        #expect(omitted.certaintyTieredSkipEnabled == false, "init default must match .default (OFF)")
        #expect(omitted.hostReadConfidenceFloor == 0.9, "init default must match .default (0.9)")
        #expect(omitted.postRollGuardSeconds == 90.0, "init default must match .default (90.0)")
    }

    @Test("AdDetectionConfig.init carries each certainty-tiered field through verbatim, one at a time")
    func configInitCarriesEachFieldIndependently() {
        // One field at a time, distinct non-default sentinels, so a swapped
        // assignment in the init cannot pass (each probe pins its own field to
        // a unique value AND asserts the other two stayed at their defaults).
        let enabledOn = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            certaintyTieredSkipEnabled: true
        )
        #expect(enabledOn.certaintyTieredSkipEnabled == true)
        #expect(enabledOn.hostReadConfidenceFloor == 0.9)
        #expect(enabledOn.postRollGuardSeconds == 90.0)

        let customFloor = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            hostReadConfidenceFloor: 0.42
        )
        #expect(customFloor.certaintyTieredSkipEnabled == false)
        #expect(customFloor.hostReadConfidenceFloor == 0.42)
        #expect(customFloor.postRollGuardSeconds == 90.0)

        let customGuard = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            postRollGuardSeconds: 123.5
        )
        #expect(customGuard.certaintyTieredSkipEnabled == false)
        #expect(customGuard.hostReadConfidenceFloor == 0.9)
        #expect(customGuard.postRollGuardSeconds == 123.5)
    }

    // MARK: - (c) Default-OFF byte-identity at the decision seam

    @Test("Default config: runBackfill is byte-identical to explicit-default (false/0.9/90.0) flags")
    func defaultConfigMatchesExplicitDefaults() async throws {
        let assetId = "asset-wraj-byteid"
        let storeDefault = try await makeSeededStore(assetId: assetId)
        let storeExplicit = try await makeSeededStore(assetId: assetId)

        let chunks = makeAdSignalChunks(assetId: assetId)
        try await makeService(store: storeDefault, config: makeBaseConfig()).runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        try await makeService(
            store: storeExplicit,
            config: makeTieredConfig(enabled: false, floor: 0.9, guardSeconds: 90.0)
        ).runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )

        let windowsDefault = try await fetchWindows(storeDefault, assetId: assetId)
        let windowsExplicit = try await fetchWindows(storeExplicit, assetId: assetId)

        try #require(!windowsDefault.isEmpty, "fixture must produce a window so the byte-identity sweep is meaningful")
        // Non-vacuity anchor: the fixture must produce a gate-SENSITIVE window
        // (an eligible, non-rediff span the flag-ON arms in (d) demote). If this
        // ever regresses to zero eligible windows, (c) would be trivially equal
        // and (d) would have nothing to demote — this require catches that.
        try #require(!eligibleStarts(windowsDefault).isEmpty,
                     "fixture must yield an eligible non-rediff window — the byte-identity sweep is only meaningful on a span the gate WOULD demote")

        #expect(
            windowsDefault.count == windowsExplicit.count,
            "default \(windowsDefault.count) vs explicit-default \(windowsExplicit.count) — omitted flags must equal explicit-default"
        )
        for (a, b) in zip(windowsDefault, windowsExplicit) {
            #expect(a.startTime == b.startTime, "startTime mismatch default vs explicit-default")
            #expect(a.endTime == b.endTime, "endTime mismatch default vs explicit-default")
            #expect(a.confidence == b.confidence, "confidence mismatch default vs explicit-default")
            #expect(a.decisionState == b.decisionState, "decisionState mismatch default vs explicit-default")
            #expect(a.eligibilityGate == b.eligibilityGate, "eligibilityGate mismatch default vs explicit-default")
            #expect(a.wasSkipped == b.wasSkipped, "wasSkipped mismatch default vs explicit-default")
            #expect(a.boundaryState == b.boundaryState, "boundaryState mismatch default vs explicit-default")
            #expect(a.detectorVersion == b.detectorVersion, "detectorVersion mismatch default vs explicit-default")
            #expect(a.metadataSource == b.metadataSource, "metadataSource mismatch default vs explicit-default")
            #expect(a.metadataConfidence == b.metadataConfidence, "metadataConfidence mismatch default vs explicit-default")
            #expect(a.evidenceStartTime == b.evidenceStartTime, "evidenceStartTime mismatch default vs explicit-default")
        }
    }

    // MARK: - (d) Flag-ON threading through runBackfill

    @Test("Config ON: the three certainty-tiered fields thread from AdDetectionConfig to the fusion gate")
    func configOnThreadsEachFieldToTheFusionGate() async throws {
        let idOff = "asset-wraj-off"
        let idInert = "asset-wraj-inert"
        let idFloor = "asset-wraj-floor"
        let idGuard = "asset-wraj-guard"

        let storeOff = try await makeSeededStore(assetId: idOff)
        let storeInert = try await makeSeededStore(assetId: idInert)
        let storeFloor = try await makeSeededStore(assetId: idFloor)
        let storeGuard = try await makeSeededStore(assetId: idGuard)

        // OFF: master switch off — the gate never fires; the eligible span
        // survives. This is the baseline eligible set the demotions shrink.
        try await makeService(store: storeOff, config: makeBaseConfig()).runBackfill(
            chunks: makeAdSignalChunks(assetId: idOff), analysisAssetId: idOff,
            podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        // ON-inert: master switch ON but BOTH demoters neutralized — floor 0.0
        // (no `skipConfidence` is < 0.0) and guard 0.0 (the early span ends
        // ~1740s before the end, so `duration - endTime <= 0` is false). No
        // demotion; must equal the OFF eligible set. Proves enabling ALONE,
        // with inert params, changes nothing.
        try await makeService(store: storeInert, config: makeTieredConfig(enabled: true, floor: 0.0, guardSeconds: 0.0)).runBackfill(
            chunks: makeAdSignalChunks(assetId: idInert), analysisAssetId: idInert,
            podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        // ON-floor: identical to ON-inert EXCEPT the floor is raised to 2.0 —
        // above any possible `skipConfidence` (<= 1.0) — so EVERY eligible
        // non-rediff span demotes to `.markOnly`. The ONLY changed input vs
        // ON-inert is `hostReadConfidenceFloor`, so a shrink here proves the
        // floor VALUE is threaded and consumed (a hardcoded/ignored floor
        // could not respond to 2.0).
        try await makeService(store: storeFloor, config: makeTieredConfig(enabled: true, floor: 2.0, guardSeconds: 0.0)).runBackfill(
            chunks: makeAdSignalChunks(assetId: idFloor), analysisAssetId: idFloor,
            podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )
        // ON-guard: identical to ON-inert EXCEPT the guard window is widened to
        // 1e6s — so the early span (1740s from the end) demotes via the
        // post-roll guard (floor 0.0 stays inert). The ONLY changed input vs
        // ON-inert is `postRollGuardSeconds`, so a shrink here proves the guard
        // VALUE is threaded and consumed (the default 90s would NOT reach a
        // span 1740s from the end).
        try await makeService(store: storeGuard, config: makeTieredConfig(enabled: true, floor: 0.0, guardSeconds: 1_000_000.0)).runBackfill(
            chunks: makeAdSignalChunks(assetId: idGuard), analysisAssetId: idGuard,
            podcastId: Self.podcastId, episodeDuration: Self.episodeDuration
        )

        let eligibleOff = eligibleStarts(try await fetchWindows(storeOff, assetId: idOff))
        let eligibleInert = eligibleStarts(try await fetchWindows(storeInert, assetId: idInert))
        let eligibleFloor = eligibleStarts(try await fetchWindows(storeFloor, assetId: idFloor))
        let eligibleGuard = eligibleStarts(try await fetchWindows(storeGuard, assetId: idGuard))

        // Non-vacuity: there IS an eligible non-rediff span to demote.
        try #require(!eligibleOff.isEmpty,
                     "fixture must yield an eligible non-rediff window for the threading proof to be non-vacuous")

        // certaintyTieredSkipEnabled + inert params == OFF (enabling alone is a no-op).
        #expect(eligibleInert == eligibleOff,
                "enabled with floor 0.0 / guard 0.0 must leave the eligible set unchanged (both demoters inert): OFF \(eligibleOff) vs ON-inert \(eligibleInert)")

        // hostReadConfidenceFloor threaded: raising ONLY the floor demotes.
        #expect(eligibleFloor.isEmpty,
                "floor 2.0 must demote every eligible non-rediff span → hostReadConfidenceFloor is threaded and consumed (still eligible: \(eligibleFloor))")
        #expect(eligibleFloor != eligibleInert,
                "the ONLY change from ON-inert is the floor; the eligible set MUST differ or the floor is not threaded")

        // postRollGuardSeconds threaded: widening ONLY the guard demotes.
        #expect(eligibleGuard.isEmpty,
                "guard 1e6 must demote the early eligible span → postRollGuardSeconds is threaded and consumed (still eligible: \(eligibleGuard))")
        #expect(eligibleGuard != eligibleInert,
                "the ONLY change from ON-inert is the guard; the eligible set MUST differ or the guard is not threaded")

        // Structure preserved: the demotions only change the gate, never drop a
        // window. Window counts are identical across all four arms.
        let countOff = try await fetchWindows(storeOff, assetId: idOff).count
        let countInert = try await fetchWindows(storeInert, assetId: idInert).count
        let countFloor = try await fetchWindows(storeFloor, assetId: idFloor).count
        let countGuard = try await fetchWindows(storeGuard, assetId: idGuard).count
        #expect(countOff == countInert && countInert == countFloor && countFloor == countGuard,
                "the wraj downgrade must only change the gate, never add/drop windows: OFF \(countOff), inert \(countInert), floor \(countFloor), guard \(countGuard)")
    }
}
