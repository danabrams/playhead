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
                fmBackfillMode: .off
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
                fmBackfillMode: .off
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

    @Test("RegionShadowPhase.run consults podcastProfile for per-show sponsor rescoring")
    func regionShadowPhasePodcastProfilePlumbing() throws {
        // playhead-8n1: pins the end-to-end property that
        // `RegionShadowPhase.Input.podcastProfile` reaches the
        // `LexicalScanner` that `RegionFeatureExtractor` constructs to
        // rescore each region. Prior to 8n1, `AdDetectionService`
        // hard-coded `podcastProfile: nil` here, silently dropping
        // per-show sponsor patterns from every shadow bundle.
        //
        // The load-bearing assertion is the DIFF between a run with a
        // profile that contains a custom sponsor name and a run with
        // `podcastProfile: nil`. The sponsor name ("Frobozzcola") is
        // synthetic specifically so it is impossible for any built-in
        // `LexicalScanner` pattern group to match it — the only way
        // `lexicalHitCount` can grow is if the profile-driven sponsor
        // lexicon is actually being compiled and scanned against the
        // region text.
        let assetId = "asset-profile-diff"

        // Two chunks: a neutral intro and a sponsor read whose host
        // built-in patterns ("brought to you by", "dot com slash",
        // "use code") guarantee the upstream `LexicalScanner` emits
        // a lexical candidate so `RegionProposalBuilder` has a region
        // to rescore. Inside that region text, the custom sponsor
        // "Frobozzcola" appears twice, so the profile-driven scanner
        // should contribute two extra sponsor hits on top of whatever
        // the built-in patterns produce.
        let chunks: [TranscriptChunk] = [
            TranscriptChunk(
                id: "c0",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-0",
                chunkIndex: 0,
                startTime: 0,
                endTime: 30,
                text: "Welcome back listeners, today we talk cooperative games.",
                normalizedText: "welcome back listeners today we talk cooperative games",
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
                text: "This episode is brought to you by Frobozzcola. Visit frobozzcola dot com slash show and use code SHOW for 20 percent off.",
                normalizedText: "this episode is brought to you by frobozzcola visit frobozzcola dot com slash show and use code show for 20 percent off",
                pass: "final",
                modelVersion: "v",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ]

        // Upstream lexical scan is profile-free here on purpose: we
        // want both pipeline invocations below to start from the
        // same `lexicalCandidates` input so the ONLY degree of
        // freedom between them is `Input.podcastProfile`.
        let upstreamScanner = LexicalScanner(podcastProfile: nil)
        let lexical = upstreamScanner.scan(chunks: chunks, analysisAssetId: assetId)
        #expect(!lexical.isEmpty, "built-in patterns should produce an upstream lexical candidate for the ad chunk")

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

        // The profile's sponsorLexicon is a comma-separated list; a
        // single entry is enough to prove plumbing. "Frobozzcola" is
        // fabricated so it cannot collide with any built-in pattern.
        let profile = PodcastProfile(
            podcastId: "podcast-8n1",
            sponsorLexicon: "Frobozzcola",
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 0,
            mode: "shadow",
            recentFalseSkipSignals: 0
        )

        func makeInput(profile: PodcastProfile?) -> RegionShadowPhase.Input {
            RegionShadowPhase.Input(
                analysisAssetId: assetId,
                chunks: chunks,
                lexicalCandidates: lexical,
                featureWindows: featureWindows,
                episodeDuration: 60,
                priors: ShowPriors.from(profile: nil),
                podcastProfile: profile
            )
        }

        let bundlesWithoutProfile = RegionShadowPhase.run(makeInput(profile: nil))
        let bundlesWithProfile = RegionShadowPhase.run(makeInput(profile: profile))

        #expect(!bundlesWithoutProfile.isEmpty, "baseline run (nil profile) should still produce bundles")
        #expect(
            bundlesWithProfile.count == bundlesWithoutProfile.count,
            "profile should not change which regions are proposed, only how they score"
        )

        // Match bundles pairwise by region identity. Proposals are
        // deterministic for identical inputs so the ordering is
        // stable; key by (firstAtomOrdinal, lastAtomOrdinal) anyway
        // to survive any future sort-order changes.
        let withoutByKey = Dictionary(
            uniqueKeysWithValues: bundlesWithoutProfile.map { bundle in
                ("\(bundle.region.firstAtomOrdinal)-\(bundle.region.lastAtomOrdinal)", bundle)
            }
        )

        var sawStrictHitIncrease = false
        for withBundle in bundlesWithProfile {
            let key = "\(withBundle.region.firstAtomOrdinal)-\(withBundle.region.lastAtomOrdinal)"
            guard let withoutBundle = withoutByKey[key] else {
                Issue.record("profile run produced a region missing from the baseline run (key=\(key))")
                continue
            }
            // The load-bearing assertion: at least one region's
            // rescored lexical hit count must strictly increase when
            // the profile is supplied. If ANY bundle satisfies this,
            // the profile has provably reached the rescoring scanner.
            if withBundle.lexicalHitCount > withoutBundle.lexicalHitCount {
                sawStrictHitIncrease = true
                // The profile-driven hits are category `.sponsor`, so
                // the category set must also gain `.sponsor` when it
                // wasn't already present. (Built-in phrases like
                // "brought to you by" ARE sponsor-category too, so
                // this is an additional sanity check rather than a
                // strictly load-bearing one.)
                #expect(
                    withBundle.lexicalCategories.contains(.sponsor),
                    "profile-rescored bundle with extra hits should expose the .sponsor category"
                )
                // The lexical score is a monotonically-increasing
                // function of hit count in `buildCandidate`, so a
                // strict hit increase must also imply a strict score
                // increase (or at worst, equality via clamping). Use
                // >= so any future confidence ceiling doesn't flake
                // the test.
                #expect(
                    withBundle.lexicalScore >= withoutBundle.lexicalScore,
                    "profile-rescored bundle should not lose lexical score"
                )
            }
        }
        #expect(
            sawStrictHitIncrease,
            "at least one region bundle must show a strictly higher lexicalHitCount when a profile with a custom sponsor is supplied — otherwise the profile is not reaching the rescoring LexicalScanner"
        )
    }
}
