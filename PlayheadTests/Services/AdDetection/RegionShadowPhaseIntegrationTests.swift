// RegionShadowPhaseIntegrationTests.swift
// playhead-xba (Phase 4 shadow wire-up):
//
// Pins the end-to-end shadow wiring of `RegionProposalBuilder` +
// `RegionFeatureExtractor` into `AdDetectionService.runBackfill`. The
// assertions here are the regression rail for the fact that Phase 4 is
// actually invoked in production (when a `RegionShadowObserver` is
// injected) — prior to this wire-up both types had zero live call sites.

import Foundation
import Testing

@testable import Playhead

@Suite("Region shadow phase integration (playhead-xba)")
struct RegionShadowPhaseIntegrationTests {

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

    private func makeChunks(assetId: String) -> [TranscriptChunk] {
        // Chunk index 1 is a textbook lexical ad: known sponsor + promo
        // code + explicit URL. LexicalScanner will latch onto this one and
        // emit at least one candidate.
        let texts = [
            "Welcome to the show. Today we're chatting with our guest about new research in behavioral economics.",
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off your first purchase at squarespace dot com slash show.",
            "Now back to our interview. So, tell us about your experiments with cooperative games."
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

    private func makeFeatureWindows(assetId: String, episodeDuration: Double) -> [FeatureWindow] {
        // Build 2-second windows across the full episode with a quiet
        // segment during the ad (chunk index 1, 30..60s) so the acoustic
        // break detector has a meaningful drop to find.
        var windows: [FeatureWindow] = []
        var t: Double = 0
        while t < episodeDuration {
            let inAd = t >= 28 && t < 62
            windows.append(
                FeatureWindow(
                    analysisAssetId: assetId,
                    startTime: t,
                    endTime: t + 2.0,
                    rms: inAd ? 0.15 : 0.55,
                    spectralFlux: inAd ? 0.3 : 0.08,
                    musicProbability: inAd ? 0.2 : 0.0,
                    pauseProbability: inAd ? 0.1 : 0.0,
                    speakerClusterId: inAd ? 2 : 1,
                    jingleHash: nil,
                    featureVersion: 1
                )
            )
            t += 2.0
        }
        return windows
    }

    // MARK: - Tests

    @Test("runBackfill records Phase 4 bundles in the injected observer")
    func runBackfillPopulatesRegionShadowObserver() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-region-shadow"
        try await store.insertAsset(makeAsset(id: assetId))

        let episodeDuration: Double = 90
        try await store.insertFeatureWindows(
            makeFeatureWindows(assetId: assetId, episodeDuration: episodeDuration)
        )

        let observer = RegionShadowObserver()
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "test-region-shadow",
                // Disable the Phase 3 FM shadow so this test focuses on the
                // Phase 4 wire-up without requiring an FM runner factory.
                fmBackfillMode: .disabled
            ),
            regionShadowObserver: observer
        )

        let chunks = makeChunks(assetId: assetId)
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-region-shadow",
            episodeDuration: episodeDuration
        )

        // The observer must have been invoked exactly once for this asset:
        // the shadow phase records once per runBackfill call.
        let recordCount = await observer.recordCount(for: assetId)
        #expect(recordCount == 1)

        let bundles = await observer.latestBundles(for: assetId)
        #expect(bundles != nil, "observer should have a bundle entry for the asset")

        guard let bundles else { return }

        // The lexical scan should find at least one candidate on the ad
        // chunk, and `RegionShadowPhase.run` should turn that into at least
        // one feature bundle. Anything less means either the wire-up is
        // absent or the lexical origin path of `RegionProposalBuilder` is
        // broken — both regressions worth failing on.
        #expect(!bundles.isEmpty, "Phase 4 shadow should produce at least one region bundle")

        // Each bundle's lexical origin flag should be set AND its lexical
        // score (re-computed via `LexicalScanner.rescoreRegionText` inside
        // `RegionFeatureExtractor`) should be non-zero. Any future refactor
        // that silently drops the rescoring hop would fail here.
        let anyLexical = bundles.contains { bundle in
            bundle.region.origins.contains(.lexical)
        }
        #expect(anyLexical, "at least one bundle should carry the lexical origin flag")

        let anyWithLexicalScore = bundles.contains { $0.lexicalScore > 0 }
        #expect(anyWithLexicalScore, "at least one bundle should have a non-zero rescored lexical score")

        // And the transcript-quality field should have been populated by
        // the heuristic path (FM transcript-quality windows are empty in
        // this test), not left at whatever default the observer starts at.
        #expect(
            bundles.allSatisfy { $0.transcriptQuality.source == .heuristic },
            "all bundles should use the heuristic transcript-quality source when no FM windows are supplied"
        )
    }

    @Test("runBackfill is a no-op for the Phase 4 path when no observer is injected")
    func runBackfillSkipsRegionPhaseWhenObserverIsNil() async throws {
        // Pins the production release-build behavior: with observer == nil
        // (matching release PlayheadRuntime), the Phase 4 shadow phase must
        // not touch anything. This test exists because the primary gate
        // for Phase 4 is the observer reference itself — a regression that
        // defaults the observer to a live instance would change release
        // behavior, which this assertion prevents.
        let store = try await makeTestStore()
        let assetId = "asset-no-observer"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(
            makeFeatureWindows(assetId: assetId, episodeDuration: 90)
        )

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "test-region-shadow-disabled",
                fmBackfillMode: .disabled
            )
            // regionShadowObserver intentionally omitted — default nil.
        )

        // Capture the baseline call log so we can assert a *delta* rather
        // than an absolute count — other fetchFeatureWindows calls from
        // `classifyCandidates` (steps 1-9 of runBackfill) are expected and
        // don't tell us anything about the shadow path.
        let baselineLog = await store.fetchFeatureWindowsCallLog

        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-no-observer",
            episodeDuration: 90
        )

        // The Phase 4 shadow phase fetches the whole episode with
        // `from: 0, to: episodeDuration`. If a regression ever defaulted
        // `regionShadowObserver` to a live instance (e.g. via
        // `regionShadowObserver ?? RegionShadowObserver()`), that exact
        // fetch would appear in the call log below. Assert it never
        // happens for this asset. Other fetchFeatureWindows calls (from
        // per-candidate classifyCandidates) are permitted.
        let newCalls = await store.fetchFeatureWindowsCallLog.dropFirst(baselineLog.count)
        let sawFullEpisodeShadowFetch = newCalls.contains { call in
            call.assetId == assetId && call.from == 0 && call.to == 90
        }
        #expect(
            !sawFullEpisodeShadowFetch,
            "Phase 4 shadow phase must not fetch the full episode when regionShadowObserver is nil"
        )
    }

    @Test("RegionShadowPhase.run threads FM refinement windows through RegionProposalBuilder")
    func regionShadowPhaseRunExposesFMOriginWhenWindowsProvided() throws {
        // playhead-xba follow-up: pins the end-to-end property that
        // `RegionShadowPhase.Input.fmWindows` (threaded from
        // `BackfillJobRunner.RunResult.fmRefinementWindows` through
        // `AdDetectionService.runBackfill`) reaches
        // `RegionProposalBuilder.makeFMProposals` and produces a
        // `.foundationModel`-origin region whose `fmConsensusStrength`
        // is non-zero and whose bundle exposes populated `fmEvidence`.
        //
        // This test exercises the helper directly rather than driving a
        // full `runBackfill` so that the assertion does not require the
        // real FM stack nor a deterministic coarse→refine→span flow
        // through `TestFMRuntime`. The integration coverage for the
        // runner-side field (`RunResult.fmRefinementWindows`) lives in
        // `BackfillJobRunnerTests`; this test pins the Phase 4 side.
        let assetId = "asset-fm-origin"

        // Two chunks with distinct content so the atomizer produces
        // enough atom ordinals to host two FM-refinement spans at
        // different atom ranges.
        let chunks: [TranscriptChunk] = (0..<6).map { idx in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 10,
                endTime: Double(idx + 1) * 10,
                text: "Line \(idx) synthetic content about topic \(idx).",
                normalizedText: "line \(idx) synthetic content about topic \(idx).",
                pass: "final",
                modelVersion: "v",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }

        // Build a baseline ShowPriors from a nil profile — identical to
        // what `AdDetectionService.runRegionShadowPhase` does in shadow
        // mode when no profile is loaded.
        let priors = ShowPriors.from(profile: nil)

        // Feature windows covering the whole fixture episode, nominal
        // values. Acoustic break detection does not drive this test;
        // the FM clustering path is what we're pinning.
        var featureWindows: [FeatureWindow] = []
        var t: Double = 0
        while t < 60 {
            featureWindows.append(
                FeatureWindow(
                    analysisAssetId: assetId,
                    startTime: t,
                    endTime: t + 2.0,
                    rms: 0.5,
                    spectralFlux: 0.1,
                    musicProbability: 0.0,
                    pauseProbability: 0.0,
                    speakerClusterId: 1,
                    jingleHash: nil,
                    featureVersion: 1
                )
            )
            t += 2.0
        }

        // Atomize the chunks the same way RegionShadowPhase will so we
        // can pick atom ordinals that actually exist and feed the FM
        // window fixture exactly aligned atom refs.
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: assetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        #expect(atoms.count >= 4, "atomizer should produce at least 4 atoms for the fixture")
        let midOrdinals = atoms.sorted {
            $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal
        }.map(\.atomKey.atomOrdinal)
        let lo = midOrdinals[1]
        let hi = midOrdinals[midOrdinals.count - 2]

        // Two synthetic refinement windows at different windowIndex
        // values and different centers so the clustering pass sees two
        // unique windows spanning the same atom range. Both carry a
        // single resolved evidence anchor so `consensusStrength` can
        // promote above `.low` when the rest of the guards pass.
        let sharedAnchor = ResolvedEvidenceAnchor(
            entry: nil,
            lineRef: lo,
            kind: .brandSpan,
            certainty: .strong,
            resolutionSource: .evidenceRef,
            memoryWriteEligible: true
        )
        let span = RefinedAdSpan(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: lo,
            lastLineRef: hi,
            firstAtomOrdinal: lo,
            lastAtomOrdinal: hi,
            certainty: .strong,
            boundaryPrecision: .usable,
            resolvedEvidenceAnchors: [sharedAnchor],
            memoryWriteEligible: true,
            alternativeExplanation: .none,
            reasonTags: []
        )
        let fmWindows: [FMRefinementWindowOutput] = [
            FMRefinementWindowOutput(
                windowIndex: 1,
                sourceWindowIndex: 1,
                lineRefs: Array(lo...hi),
                spans: [span],
                latencyMillis: 10
            ),
            FMRefinementWindowOutput(
                windowIndex: 2,
                sourceWindowIndex: 2,
                lineRefs: Array(lo...hi),
                spans: [span],
                latencyMillis: 10
            )
        ]

        let bundles = RegionShadowPhase.run(
            RegionShadowPhase.Input(
                analysisAssetId: assetId,
                chunks: chunks,
                lexicalCandidates: [],
                featureWindows: featureWindows,
                episodeDuration: 60,
                priors: priors,
                podcastProfile: nil,
                fmWindows: fmWindows
            )
        )

        #expect(!bundles.isEmpty, "FM-only shadow input should still produce at least one bundle")

        // The wire-up property: at least one bundle must carry the
        // `.foundationModel` origin flag. Before playhead-xba's
        // follow-up plumbing this would fail because the shadow helper
        // always passed `fmWindows: []`.
        let fmBundles = bundles.filter { $0.region.origins.contains(.foundationModel) }
        #expect(
            !fmBundles.isEmpty,
            "at least one bundle should carry the .foundationModel origin flag when fmWindows are supplied"
        )

        // Consensus strength must be non-zero. `.low` (0.35) is the
        // weakest non-zero band and is the expected floor for two
        // clustered windows with shared anchors.
        #expect(
            fmBundles.contains { $0.region.fmConsensusStrength.value > 0 },
            "FM-origin region should have non-zero fmConsensusStrength after clustering"
        )

        // FM evidence must be populated (non-nil) on at least one bundle
        // so downstream Phase 5+ consumers have something to read.
        #expect(
            fmBundles.contains { $0.region.fmEvidence != nil },
            "at least one FM-origin bundle should expose populated fmEvidence"
        )
    }

    @Test("RegionShadowPhase.run produces uniform feature bundles from synthetic inputs")
    func regionShadowPhaseRunDirect() throws {
        // Unit-level pin on the composition helper itself: given a known
        // lexical candidate and feature windows, `RegionShadowPhase.run`
        // must return at least one bundle whose region pulls in the
        // lexical origin. This is the smallest possible proof that
        // `RegionProposalBuilder` and `RegionFeatureExtractor` are wired
        // together end-to-end, independent of the `AdDetectionService`
        // flow above.
        let assetId = "asset-direct"
        let chunks: [TranscriptChunk] = [
            TranscriptChunk(
                id: "c0",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-0",
                chunkIndex: 0,
                startTime: 0,
                endTime: 30,
                text: "Welcome back listeners.",
                normalizedText: "welcome back listeners.",
                pass: "final",
                modelVersion: "v",
                transcriptVersion: nil,
                atomOrdinal: nil
            ),
            TranscriptChunk(
                id: "c1",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-1",
                chunkIndex: 1,
                startTime: 30,
                endTime: 60,
                text: "This episode is brought to you by Squarespace. Go to squarespace dot com slash show for 20 percent off.",
                normalizedText: "this episode is brought to you by squarespace. go to squarespace dot com slash show for 20 percent off.",
                pass: "final",
                modelVersion: "v",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ]
        let scanner = LexicalScanner(podcastProfile: nil)
        let lexical = scanner.scan(chunks: chunks, analysisAssetId: assetId)
        #expect(!lexical.isEmpty, "lexical scanner fixture should produce at least one candidate")

        var featureWindows: [FeatureWindow] = []
        var t: Double = 0
        while t < 60 {
            featureWindows.append(
                FeatureWindow(
                    analysisAssetId: assetId,
                    startTime: t,
                    endTime: t + 2.0,
                    rms: 0.5,
                    spectralFlux: 0.1,
                    musicProbability: 0.0,
                    pauseProbability: 0.0,
                    speakerClusterId: 1,
                    jingleHash: nil,
                    featureVersion: 1
                )
            )
            t += 2.0
        }

        let bundles = RegionShadowPhase.run(
            RegionShadowPhase.Input(
                analysisAssetId: assetId,
                chunks: chunks,
                lexicalCandidates: lexical,
                featureWindows: featureWindows,
                episodeDuration: 60,
                priors: ShowPriors.from(profile: nil),
                podcastProfile: nil
            )
        )

        #expect(!bundles.isEmpty, "direct Phase 4 composition should return at least one bundle")
        #expect(bundles.allSatisfy { $0.region.analysisAssetId == assetId })
        #expect(bundles.contains { $0.region.origins.contains(.lexical) })
    }
}
