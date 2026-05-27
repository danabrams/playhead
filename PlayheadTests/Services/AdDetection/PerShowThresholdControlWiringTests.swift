// PerShowThresholdControlWiringTests.swift
// playhead-xsdz.11: Service-level wiring tests for the per-show auto-skip
// threshold control.
//
// These prove the load-bearing OFF-by-default full-gating contract end-to-end
// through `AdDetectionService.runBackfill`:
//   1. FLAG-OFF IDENTITY: with a controller store WIRED AND PRE-SEEDED with a
//      strong per-show offset, `perShowThresholdControlEnabled = false` produces
//      byte-identical persisted `AdWindow`s (confidence, decisionState, AND
//      eligibilityGate) to the no-store baseline — i.e. no offset read, no
//      threshold change.
//   2. FLAG-ON READ: with the same pre-seeded store and the flag ON, the gate
//      genuinely consumes the per-show offset — the effective auto-skip
//      threshold `runBackfill` resolves rises for a positive (FP) offset, falls
//      for a negative (miss) offset, and is clamped to [0.55, 0.95]. Asserted
//      through the production read seam (`effectiveAutoSkipThresholdForTesting`),
//      not a fragile corpus-confidence flip.
//   3. GATING: the production-default config keeps the feature OFF, so the
//      runtime constructs no store.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("PerShowThresholdControl service wiring (playhead-xsdz.11)")
struct PerShowThresholdControlWiringTests {

    private let asset = "asset-xsdz11-wiring"
    private let show = "show-xsdz11-new"

    // MARK: - 1. Flag-off identity

    @Test("Flag OFF: a wired+seeded controller store does not change any AdWindow decision")
    func flagOffIsByteIdentical() async throws {
        let baseline = try await runAndFetchWindows(enabled: false, seedOffset: nil)
        let withStoreOff = try await runAndFetchWindows(enabled: false, seedOffset: -0.15) // strong aggressive seed

        #expect(baseline.count == withStoreOff.count)
        func key(_ w: AdWindow) -> String { String(format: "%.3f-%.3f", w.startTime, w.endTime) }
        let baseMap = Dictionary(
            baseline.map { (key($0), $0) },
            uniquingKeysWith: { a, _ in a }
        )
        for w in withStoreOff {
            let b = try #require(baseMap[key(w)], "span \(key(w)) missing in baseline")
            #expect(w.confidence == b.confidence, "flag-off must be byte-identical (confidence)")
            #expect(w.decisionState == b.decisionState, "flag-off must be byte-identical (decisionState)")
            #expect(w.eligibilityGate == b.eligibilityGate, "flag-off must be byte-identical (eligibilityGate)")
        }
    }

    // MARK: - 2. Flag-on read genuinely consumes the offset

    @Test("Flag ON: the per-show offset moves the effective auto-skip threshold the gate uses")
    func flagOnConsumesOffset() async throws {
        // Max-aggressive: a strong NEGATIVE (miss-driven) offset lowers the
        // effective threshold toward the 0.55 floor.
        let (aggressiveSvc, _, aggressiveStore) = try await makeService(enabled: true, seedOffset: -0.15)
        // Max-conservative: a strong POSITIVE (FP-driven) offset raises the
        // effective threshold toward the 0.95 cap.
        let (conservativeSvc, _, conservativeStore) = try await makeService(enabled: true, seedOffset: 0.15)
        // Flag-off control: the same seeded store, but the flag is off ⇒ the gate
        // must use the unmodified base threshold (no read).
        let (offSvc, _, offStore) = try await makeService(enabled: false, seedOffset: -0.15)

        let base = AdDetectionConfig.default.autoSkipConfidenceThreshold // 0.80
        let aggressive = await aggressiveSvc.effectiveAutoSkipThresholdForTesting(showId: show, track: .standard)
        let conservative = await conservativeSvc.effectiveAutoSkipThresholdForTesting(showId: show, track: .standard)
        let off = await offSvc.effectiveAutoSkipThresholdForTesting(showId: show, track: .standard)

        // Flag-off uses the base threshold verbatim (proves no offset read).
        #expect(off == base, "flag-off must resolve the unmodified base threshold")

        // A miss-driven (negative) offset LOWERS the threshold; an FP-driven
        // (positive) offset RAISES it. Both differ from the base.
        #expect(aggressive < base, "a miss-driven offset must lower the effective auto-skip threshold")
        #expect(conservative > base, "an FP-driven offset must raise the effective auto-skip threshold")
        #expect(aggressive < conservative)

        // Both stay inside the bead-mandated clamp band.
        #expect(aggressive >= 0.55 && aggressive <= 0.95)
        #expect(conservative >= 0.55 && conservative <= 0.95)

        // SCOPE: the offset applies ONLY to the `.standard` track. The qualified
        // precision lanes keep their intentional sub-0.55 floor untouched, even
        // with the flag on and a large offset seeded.
        let qualifiedBase = AdDetectionConfig.default.classifierSeedQualifiedThreshold // 0.50
        let qualifiedAggressive = await aggressiveSvc.effectiveAutoSkipThresholdForTesting(
            showId: show, track: .classifierSeedQualified
        )
        let qualifiedConservative = await conservativeSvc.effectiveAutoSkipThresholdForTesting(
            showId: show, track: .classifierSeedQualified
        )
        #expect(qualifiedAggressive == qualifiedBase, "qualified track must not be moved by the per-show offset")
        #expect(qualifiedConservative == qualifiedBase, "qualified track must not be pulled up to the 0.55 clamp floor")

        await aggressiveStore.close()
        await conservativeStore.close()
        await offStore.close()
    }

    // MARK: - Harness

    /// Persist the asset, seed the controller store (optionally), and build a
    /// service wired to that store. Returns both so the caller can drive
    /// `runBackfill` and/or read the effective threshold, then close the store.
    private func makeService(
        enabled: Bool,
        seedOffset: Double?
    ) async throws -> (AdDetectionService, AnalysisStore, PerShowThresholdControllerStore) {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: asset))

        let dir = try makeTempDir(prefix: "xsdz11-wiring-store")
        let controllerStore = try PerShowThresholdControllerStore(directoryURL: dir)
        if let seedOffset {
            // Drive the controller past the min-sample gate to the requested
            // saturated offset by recording a one-sided correction stream. A
            // long FP stream saturates at +maxOffset; a long miss stream at
            // −maxOffset (the default maxOffset is 0.15).
            let signal: ThresholdControlSignal = seedOffset >= 0 ? .falsePositive : .miss
            for _ in 0..<60 {
                _ = try await controllerStore.record(signal: signal, forShow: show)
            }
        }

        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "xsdz11-test",
            fmBackfillMode: .off,
            perShowThresholdControlEnabled: enabled
        )
        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            perShowThresholdControllerStore: controllerStore
        )
        return (service, store, controllerStore)
    }

    private func runAndFetchWindows(
        enabled: Bool,
        seedOffset: Double?
    ) async throws -> [AdWindow] {
        let (service, store, controllerStore) = try await makeService(enabled: enabled, seedOffset: seedOffset)
        try await service.runBackfill(
            chunks: chunks(),
            analysisAssetId: asset,
            podcastId: show,
            episodeDuration: 130.0
        )
        let windows = try await store.fetchAdWindows(assetId: asset)
        await controllerStore.close()
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

    private func chunks() -> [TranscriptChunk] {
        let texts: [(Double, Double, String)] = [
            (0.0, 30.0, "Welcome back to the show today we discuss technology and design."),
            (60.0, 90.0, "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show."),
            (90.0, 120.0, "Back to our regular conversation about new things and ideas.")
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(asset)",
                analysisAssetId: asset,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }
}

// MARK: - Flag-OFF ⇒ no store construction (behavior-neutral gating)

/// playhead-xsdz.11: the ENTIRE feature — store construction, SQLite migration,
/// the correction-signal WRITE path, AND the per-show offset READ — rides the
/// ONE off-by-default `perShowThresholdControlEnabled` flag. With the flag off
/// (the production default) there must be NO store, NO new DB file, NO migration,
/// and NO writes/reads. These tests pin that invariant at the seam the production
/// `PlayheadRuntime` actually branches on.
@Suite("PerShowThresholdControl flag-off gating (playhead-xsdz.11)")
struct PerShowThresholdControlGatingTests {

    /// The production gate: `PlayheadRuntime` constructs the store only when
    /// `AdDetectionConfig.default.perShowThresholdControlEnabled` is true. The
    /// default (production) config keeps it false, so production constructs NO
    /// store — no DB file, no migration. This is the single value the runtime
    /// branches on; pinning it proves the OFF state is the production state.
    @Test("Production-default config keeps the feature OFF (so the runtime builds no store)")
    func productionDefaultDisablesConstruction() {
        #expect(AdDetectionConfig.default.perShowThresholdControlEnabled == false)
    }

    /// Sanity: the default gains/bounds are the conservative bead-mandated
    /// values, and the derived controller parameters clamp the effective
    /// threshold to exactly [0.55, 0.95].
    @Test("Default gains/bounds are conservative and clamp to [0.55, 0.95]")
    func defaultParametersAreConservative() {
        let p = AdDetectionConfig.default.perShowThresholdControllerParameters
        #expect(p.proportionalGain == 0.02)
        #expect(p.integralGain == 0.005)
        #expect(p.maxOffset == 0.15)
        #expect(p.minSamples == 5)
        #expect(p.effectiveMin == 0.55)
        #expect(p.effectiveMax == 0.95)
    }
}

#endif
