// Phase6ComprehensiveTests.swift
// playhead-4my.6.5: Comprehensive tests for Phase 6 coverage gaps.
//
// Covers items not already tested in BackfillEvidenceFusionTests,
// SkipPolicyMatrixTests, AdDecisionResultTests, or BackfillFusionPipelineTests:
//
//   1.  Evidence ledger explicitly accumulates all 5 source types in one fusion
//   6.  FM-only without quorum stays blockedByEvidenceQuorum, score NOT clamped
//  13.  DecisionCohort change triggers recomputation from cached results (no FM rescan)
//  14.  FMBackfillMode.proposalOnly: does NOT contribute to existing-candidate ledger
//  15.  Full backfill integration: transcript→atoms→harvesters→classifier→
//           proposals→features→decode→fuse→refine→decide (end-to-end)
//  16.  Borderline candidate (0.42 confidence) promoted above skip threshold with FM
//  17.  FM-only conversational ad → skip-eligible when quorum satisfied
//  18.  Old promote/suppress path removed — no resolveDecision() calls remain
//
//  Key gaps (from spec):
//   G1. Hysteresis stay/exit characterization for receiveAdDecisionResults path
//   G2. Multi-span hysteresis: enter/stay/below-stay for AdDecisionResult path

import Foundation
import Testing
@testable import Playhead

// MARK: - Shared helpers

private func makePhase6Span(
    assetId: String = "asset-p6",
    startTime: Double = 10.0,
    endTime: Double = 70.0,
    anchorProvenance: [AnchorRef] = []
) -> DecodedSpan {
    DecodedSpan(
        id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 10, lastAtomOrdinal: 80),
        assetId: assetId,
        firstAtomOrdinal: 10,
        lastAtomOrdinal: 80,
        startTime: startTime,
        endTime: endTime,
        anchorProvenance: anchorProvenance
    )
}

private func makePhase6Asset(id: String = "asset-p6") -> AnalysisAsset {
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

// MARK: - 1. Multi-source ledger accumulation

@Suite("Phase6 — Evidence ledger accumulates all 5 source types")
struct Phase6LedgerAccumulationTests {

    /// Verifies that a single BackfillEvidenceFusion call with entries from all 5
    /// sources produces a ledger that contains all 5 source types simultaneously.
    @Test("Ledger contains classifier, fm, lexical, acoustic, and catalog entries in one build")
    func allFiveSourceTypesPresent() {
        let span = makePhase6Span()

        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.35,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )
        let lexEntry = EvidenceLedgerEntry(
            source: .lexical,
            weight: 0.18,
            detail: .lexical(matchedCategories: ["url", "promoCode"])
        )
        let acEntry = EvidenceLedgerEntry(
            source: .acoustic,
            weight: 0.15,
            detail: .acoustic(breakStrength: 0.72)
        )
        let catEntry = EvidenceLedgerEntry(
            source: .catalog,
            weight: 0.12,
            detail: .catalog(entryCount: 2)
        )

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.65,
            fmEntries: [fmEntry],
            lexicalEntries: [lexEntry],
            acousticEntries: [acEntry],
            catalogEntries: [catEntry],
            mode: .full,
            config: FusionWeightConfig()
        )

        let ledger = fusion.buildLedger()
        let sources = Set(ledger.map { $0.source })

        #expect(sources.contains(.classifier), "Ledger must contain .classifier entry")
        #expect(sources.contains(.fm), "Ledger must contain .fm entry (mode=.full)")
        #expect(sources.contains(.lexical), "Ledger must contain .lexical entry")
        #expect(sources.contains(.acoustic), "Ledger must contain .acoustic entry")
        #expect(sources.contains(.catalog), "Ledger must contain .catalog entry")
        #expect(sources.count == 5, "All 5 source types must appear in the ledger simultaneously")
    }

    /// Old RuleBasedClassifier score always produces exactly one .classifier entry
    /// regardless of mode.
    @Test("Classifier entry is always present and has correct weight formula")
    func classifierEntryAlwaysPresentWithCorrectWeight() {
        let span = makePhase6Span()
        let score = 0.75
        let config = FusionWeightConfig()

        for mode in FMBackfillMode.allCases {
            let fusion = BackfillEvidenceFusion(
                span: span,
                classifierScore: score,
                fmEntries: [],
                lexicalEntries: [],
                acousticEntries: [],
                catalogEntries: [],
                mode: mode,
                config: config
            )
            let ledger = fusion.buildLedger()
            let classifierEntries = ledger.filter { $0.source == .classifier }
            #expect(classifierEntries.count == 1, "Exactly one .classifier entry in mode=\(mode.rawValue)")

            // Weight = min(score * classifierCap, classifierCap)
            let expectedWeight = min(score * config.classifierCap, config.classifierCap)
            #expect(
                abs(classifierEntries[0].weight - expectedWeight) < 0.0001,
                "Classifier weight must equal min(score * cap, cap) in mode=\(mode.rawValue)"
            )
        }
    }
}

// MARK: - 6. FM-only without quorum — gate does not clamp score

@Suite("Phase6 — FM-only quorum block does not clamp score")
struct Phase6FMOnlyQuorumTests {

    /// A span with fmConsensus provenance but only 1 distinct evidence kind
    /// (FM only) blocks by quorum — but the score remains the honest sum.
    @Test("fmConsensus span with only FM evidence: blockedByEvidenceQuorum but score is honest")
    func fmConsensusOnlyOneKindBlockedButScoreHonest() {
        let span = makePhase6Span(
            anchorProvenance: [.fmConsensus(regionId: "r1", consensusStrength: 0.9)]
        )
        // Only FM evidence — 1 distinct kind, not 2+
        let entries: [EvidenceLedgerEntry] = [
            .init(
                source: .fm,
                weight: 0.38,
                detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
            )
        ]

        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let result = mapper.map()

        // Gate must block (only 1 distinct evidence kind, not 2+)
        #expect(
            result.eligibilityGate == .blockedByEvidenceQuorum,
            "fmConsensus with < 2 distinct evidence kinds must be blockedByEvidenceQuorum"
        )

        // Score must NOT be clamped — it must reflect the honest weight sum
        #expect(
            result.skipConfidence > 0.0,
            "Score must be honest (> 0) even when gate blocks"
        )
        // The weight sum is 0.38 (capped at fmCap=0.4), so score should be ~0.38
        #expect(
            result.skipConfidence >= 0.3,
            "Score must reflect actual evidence weight, not be zero-clamped by gate"
        )
    }

    /// An fmAcousticCorroborated span with NO external sources is blocked,
    /// but its score must still reflect the raw FM weight.
    @Test("fmAcousticCorroborated with no external sources: blocked but score not clamped to zero")
    func fmAcousticOnlyNoCorroboration_ScoreNotClamped() {
        let span = makePhase6Span(
            anchorProvenance: [.fmAcousticCorroborated(regionId: "r2", breakStrength: 0.8)]
        )
        // FM-only: no classifier/lexical/catalog/acoustic corroboration
        let entries: [EvidenceLedgerEntry] = [
            .init(
                source: .fm,
                weight: 0.40,
                detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
            )
        ]

        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let result = mapper.map()

        #expect(result.eligibilityGate == .blockedByEvidenceQuorum, "No external corroboration must block")
        #expect(result.skipConfidence > 0.0, "Score must be > 0 when gate blocks without score-clamping")
        // proposalConfidence must also be honest (equal to skip since v1 is identity)
        #expect(result.proposalConfidence > 0.0)
        #expect(result.proposalConfidence == result.skipConfidence)
    }
}

// MARK: - 13. DecisionCohort change triggers recomputation (no FM rescan)

@Suite("Phase6 — DecisionCohort change triggers recomputation without FM rescan")
struct Phase6DecisionCohortRecomputationTests {

    /// DecisionCohort.production() returns different cohorts for different appBuild strings.
    /// Changing any field (featurePipelineHash, fusionHash, policyHash, stabilityHash, appBuild)
    /// produces a different cohort, signaling that downstream consumers must recompute.
    /// This is a pure-value-type test — no FM is involved.
    @Test("Two DecisionCohorts differ when featurePipelineHash differs")
    func differentFeaturePipelineHashProducesDifferentCohort() {
        let cohortA = DecisionCohort(
            featurePipelineHash: "feature-v1",
            fusionHash: "fusion-v1",
            policyHash: "policy-v1",
            stabilityHash: "stability-v1",
            appBuild: "100"
        )
        let cohortB = DecisionCohort(
            featurePipelineHash: "feature-v2",  // changed
            fusionHash: "fusion-v1",
            policyHash: "policy-v1",
            stabilityHash: "stability-v1",
            appBuild: "100"
        )
        #expect(cohortA != cohortB, "Changed featurePipelineHash must produce different cohort")
    }

    @Test("Two DecisionCohorts differ when fusionHash differs")
    func differentFusionHashProducesDifferentCohort() {
        let cohortA = DecisionCohort(
            featurePipelineHash: "fp1",
            fusionHash: "fusion-v1",
            policyHash: "p1",
            stabilityHash: "s1",
            appBuild: "100"
        )
        let cohortB = DecisionCohort(
            featurePipelineHash: "fp1",
            fusionHash: "fusion-v2",  // changed
            policyHash: "p1",
            stabilityHash: "s1",
            appBuild: "100"
        )
        #expect(cohortA != cohortB, "Changed fusionHash must produce different cohort")
    }

    @Test("Two DecisionCohorts differ when policyHash differs")
    func differentPolicyHashProducesDifferentCohort() {
        let cohortA = DecisionCohort(
            featurePipelineHash: "fp1",
            fusionHash: "fu1",
            policyHash: "policy-v1",
            stabilityHash: "s1",
            appBuild: "100"
        )
        let cohortB = DecisionCohort(
            featurePipelineHash: "fp1",
            fusionHash: "fu1",
            policyHash: "policy-v2",  // changed
            stabilityHash: "s1",
            appBuild: "100"
        )
        #expect(cohortA != cohortB, "Changed policyHash must produce different cohort")
    }

    @Test("Same DecisionCohort on same inputs stays stable across re-runs")
    func stableCohortForSameInputs() {
        let first = DecisionCohort.production(appBuild: "200")
        let second = DecisionCohort.production(appBuild: "200")
        #expect(first == second, "DecisionCohort.production must be stable for same inputs")
    }

    /// The key invariant: changing ONLY the DecisionCohort (same transcript, same scan results)
    /// should allow decision recomputation by diffing the cohort — without re-running FM.
    /// This test pins the protocol: cohort A != cohort B when any hash changes, so consumers
    /// can detect the change with a simple equality check.
    @Test("DecisionCohort inequality signals recomputation needed without FM re-run")
    func cohortInequalitySignalsRecomputationWithoutFMRescan() {
        // Same transcript version; different decision pipeline version
        let scanTimeCohort = DecisionCohort(
            featurePipelineHash: "feature-v1",
            fusionHash: "fusion-v1",
            policyHash: "policy-v1",
            stabilityHash: "stability-v1",
            appBuild: "1"
        )
        let newDecisionCohort = DecisionCohort(
            featurePipelineHash: "feature-v1",
            fusionHash: "fusion-v2",  // fusion policy changed — recompute decisions
            policyHash: "policy-v1",
            stabilityHash: "stability-v1",
            appBuild: "1"
        )

        // Different cohort → consumer must recompute from cached scan results
        let mustRecompute = scanTimeCohort != newDecisionCohort
        #expect(mustRecompute, "Changed fusionHash must trigger recomputation flag")

        // FM scan cohort was NOT changed (featurePipelineHash still v1) so no rescan needed.
        // Only the decision pipeline changed — this is the key isolation guarantee.
        #expect(
            scanTimeCohort.featurePipelineHash == newDecisionCohort.featurePipelineHash,
            "FM scan hash unchanged — no FM rescan required"
        )
    }
}

// MARK: - 14. FMBackfillMode.proposalOnly

@Suite("Phase6 — FMBackfillMode.proposalOnly ledger behavior")
struct Phase6ProposalOnlyModeTests {

    /// .proposalOnly should NOT contribute FM entries to existing-candidate ledger
    /// (contributesToExistingCandidateLedger == false).
    @Test("proposalOnly mode: FM positive entries excluded from existing-candidate ledger")
    func proposalOnlyExcludesFMFromExistingCandidateLedger() {
        let span = makePhase6Span()
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.4,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.6,
            fmEntries: [fmEntry],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .proposalOnly,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let fmInLedger = ledger.filter { $0.source == .fm }

        // .proposalOnly: contributesToExistingCandidateLedger == false
        // FM must NOT appear in the decision ledger for existing candidates
        #expect(
            fmInLedger.isEmpty,
            "FMBackfillMode.proposalOnly must NOT contribute FM to existing-candidate ledger"
        )
    }

    /// Sanity check: contributesToExistingCandidateLedger property is false for .proposalOnly
    @Test("FMBackfillMode.proposalOnly.contributesToExistingCandidateLedger is false")
    func proposalOnlyContributesToExistingCandidateLedgerIsFalse() {
        #expect(!FMBackfillMode.proposalOnly.contributesToExistingCandidateLedger)
    }

    /// .proposalOnly CAN propose new regions (canProposeNewRegions == true)
    @Test("FMBackfillMode.proposalOnly.canProposeNewRegions is true")
    func proposalOnlyCanProposeNewRegions() {
        #expect(FMBackfillMode.proposalOnly.canProposeNewRegions)
    }

    /// Compare all modes' contributesToExistingCandidateLedger values
    @Test("Only rescoreOnly and full contribute FM to existing-candidate ledger")
    func onlyRescoreOnlyAndFullContributeToLedger() {
        // Modes that contribute: rescoreOnly, full
        #expect(!FMBackfillMode.off.contributesToExistingCandidateLedger)
        #expect(!FMBackfillMode.shadow.contributesToExistingCandidateLedger)
        #expect(!FMBackfillMode.proposalOnly.contributesToExistingCandidateLedger)
        #expect(FMBackfillMode.rescoreOnly.contributesToExistingCandidateLedger)
        #expect(FMBackfillMode.full.contributesToExistingCandidateLedger)
    }
}

// MARK: - 15. Full backfill integration test

@Suite("Phase6 — Full backfill integration (transcript → decide)")
struct Phase6FullBackfillIntegrationTests {

    /// Makes AdDetectionService with specified FMBackfillMode and no BackfillJobRunner
    private func makeService(store: AnalysisStore, mode: FMBackfillMode = .off) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "p6-integration-v1",
                fmBackfillMode: mode
            )
        )
    }

    /// Full 16-step pipeline for a transcript that contains an ad read.
    /// Verifies:
    ///   • transcript → atoms (TranscriptAtomizer)
    ///   • atoms → evidence catalog (EvidenceCatalogBuilder)
    ///   • evidence → lexical candidates (LexicalScanner / harvesters)
    ///   • candidates → classifier (RuleBasedClassifier as ledger entry)
    ///   • FM is mocked (mode=.off, no FM call)
    ///   • proposals → features → decode → spans (DecodedSpan pipeline)
    ///   • spans → fuse → decision (BackfillEvidenceFusion + DecisionMapper)
    ///   • decision → refine → persist (AdWindow + DecisionEvent)
    @Test("Full pipeline: ad-signal transcript produces AdWindows and DecisionEvents")
    func fullPipelineWithAdSignalTranscript() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-p6-full"
        try await store.insertAsset(makePhase6Asset(id: assetId))

        let service = makeService(store: store, mode: .off)

        // Transcript with a clear ad read
        let chunks = [
            TranscriptChunk(
                id: "c0-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-0",
                chunkIndex: 0,
                startTime: 0,
                endTime: 30,
                text: "Welcome to the show. Today we are talking about fitness.",
                normalizedText: "welcome to the show today we are talking about fitness",
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            ),
            TranscriptChunk(
                id: "c1-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-1",
                chunkIndex: 1,
                startTime: 30,
                endTime: 60,
                text: "This episode is brought to you by FitSupp. Visit fitsuppl.com and use promo code PODCAST for twenty percent off your first order.",
                normalizedText: "this episode is brought to you by fitsuppl visit fitsuppl dot com and use promo code podcast for twenty percent off your first order",
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            ),
            TranscriptChunk(
                id: "c2-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-2",
                chunkIndex: 2,
                startTime: 60,
                endTime: 90,
                text: "And we are back. Let us get into our topic for today.",
                normalizedText: "and we are back let us get into our topic for today",
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ]

        // Run the full pipeline — must not throw
        await #expect(throws: Never.self) {
            try await service.runBackfill(
                chunks: chunks,
                analysisAssetId: assetId,
                podcastId: "podcast-p6-test",
                episodeDuration: 90.0
            )
        }

        // The pipeline must produce windows for the ad-signal chunk
        let windows = try await store.fetchAdWindows(assetId: assetId)
        // With lexical signals, we expect at least one window
        #expect(!windows.isEmpty, "Full pipeline with ad-signal transcript must produce at least one AdWindow")

        // Every window must have valid fields
        for window in windows {
            #expect(window.analysisAssetId == assetId, "Window must be associated with correct asset")
            #expect(window.startTime >= 0, "Window startTime must be non-negative")
            #expect(window.endTime > window.startTime, "Window endTime must exceed startTime")
            #expect(!window.id.isEmpty, "Window must have a non-empty id")
        }
    }

    /// Clean transcript produces no AdWindows (no false positives from the pipeline)
    @Test("Full pipeline: clean transcript produces no AdWindows")
    func fullPipelineWithCleanTranscript() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-p6-clean"
        try await store.insertAsset(makePhase6Asset(id: assetId))

        let service = makeService(store: store, mode: .off)

        let chunks = [
            TranscriptChunk(
                id: "c0-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-0",
                chunkIndex: 0,
                startTime: 0,
                endTime: 30,
                text: "Hello and welcome to today's episode on neuroscience.",
                normalizedText: "hello and welcome to today's episode on neuroscience",
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            ),
            TranscriptChunk(
                id: "c1-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-1",
                chunkIndex: 1,
                startTime: 30,
                endTime: 60,
                text: "The brain is a fascinating organ that processes information in complex ways.",
                normalizedText: "the brain is a fascinating organ that processes information in complex ways",
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ]

        await #expect(throws: Never.self) {
            try await service.runBackfill(
                chunks: chunks,
                analysisAssetId: assetId,
                podcastId: "podcast-p6-test",
                episodeDuration: 60.0
            )
        }

        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows.isEmpty, "Clean transcript must produce zero AdWindows")
    }

    /// Decision events are written after the pipeline runs
    @Test("Full pipeline writes DecisionEvents for every span produced")
    func fullPipelineWritesDecisionEvents() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-p6-events"
        try await store.insertAsset(makePhase6Asset(id: assetId))

        let service = makeService(store: store, mode: .off)

        let chunks = [
            TranscriptChunk(
                id: "c0-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-0",
                chunkIndex: 0,
                startTime: 0,
                endTime: 30,
                text: "This episode is brought to you by AcmeCorp. Use code SHOW for discount at acme.com.",
                normalizedText: "this episode is brought to you by acmecorp use code show for discount at acme dot com",
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            ),
            TranscriptChunk(
                id: "c1-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-1",
                chunkIndex: 1,
                startTime: 30,
                endTime: 60,
                text: "Now back to the interview.",
                normalizedText: "now back to the interview",
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ]

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-p6-test",
            episodeDuration: 60.0
        )

        // The fusion path must produce at least one AdWindow for the ad-signal transcript.
        // This is an unconditional guard — if it fails, the pipeline regressed.
        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(!windows.isEmpty, "Full pipeline with ad-signal transcript must produce at least one AdWindow (fusion path regression)")

        let events = try await store.loadDecisionEvents(for: assetId)
        #expect(!events.isEmpty, "Fusion path must write DecisionEvents for every span produced")
        #expect(events.count == windows.count, "One DecisionEvent per AdWindow")

        for event in events {
            #expect(!event.windowId.isEmpty, "DecisionEvent must reference a non-empty windowId")
            #expect(!event.eligibilityGate.isEmpty, "DecisionEvent must have non-empty eligibilityGate")
            #expect(!event.policyAction.isEmpty, "DecisionEvent must have non-empty policyAction")
            #expect(event.proposalConfidence >= 0 && event.proposalConfidence <= 1.0)
            #expect(event.skipConfidence >= 0 && event.skipConfidence <= 1.0)
        }
    }
}

// MARK: - 16. Borderline candidate promoted with FM evidence

@Suite("Phase6 — Borderline candidate promoted with FM evidence")
struct Phase6BorderlinePromotionTests {

    private func makeSpanFMConsensus(startTime: Double = 10.0, endTime: Double = 70.0) -> DecodedSpan {
        makePhase6Span(
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: [.fmConsensus(regionId: "r-border", consensusStrength: 0.75)]
        )
    }

    /// A span at 0.42 classifier confidence (below auto-skip enterThreshold=0.65)
    /// gets boosted above the threshold when FM evidence is added in .full mode.
    @Test("Borderline classifier score (0.42) promoted above enterThreshold by FM evidence")
    func borderlineScorePromotedByFMEvidence() {
        let span = makeSpanFMConsensus()

        // Borderline: classifier score below enterThreshold (0.65)
        let borderlineClassifierScore = 0.42
        let classifierWeightContribution = min(
            borderlineClassifierScore * FusionWeightConfig().classifierCap,
            FusionWeightConfig().classifierCap
        )

        // FM evidence boosts the ledger
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.38,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )
        // Additional lexical corroboration to satisfy fmConsensus quorum (2+ kinds)
        let lexEntry = EvidenceLedgerEntry(
            source: .lexical,
            weight: 0.18,
            detail: .lexical(matchedCategories: ["url"])
        )

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: borderlineClassifierScore,
            fmEntries: [fmEntry],
            lexicalEntries: [lexEntry],
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()

        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let result = mapper.map()

        // Classifier alone was 0.42 * 0.3 = 0.126 (well below enterThreshold)
        // After FM boost: classifier(0.126) + FM(0.38) + lexical(0.18) = 0.686 > 0.65
        let classifierAlone = classifierWeightContribution
        #expect(classifierAlone < SkipPolicyConfig.default.enterThreshold,
                "Classifier alone must be below enterThreshold to confirm borderline scenario")

        // With FM: promoted above enterThreshold
        #expect(
            result.skipConfidence >= SkipPolicyConfig.default.enterThreshold,
            "Borderline candidate at 0.42 must be promoted above enterThreshold (\(SkipPolicyConfig.default.enterThreshold)) with FM evidence"
        )

        // Gate must be eligible (fmConsensus + 2+ kinds + good quality + valid duration)
        #expect(result.eligibilityGate == .eligible,
                "Promoted span with fmConsensus and sufficient evidence must be eligible")
    }

    /// Without FM, the same borderline span remains below threshold.
    @Test("Borderline score (0.42) stays below enterThreshold without FM evidence")
    func borderlineScoreStaysBelowThresholdWithoutFM() {
        let span = makeSpanFMConsensus()

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.42,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .off,  // FM excluded
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let result = mapper.map()

        // Without FM, classifier at 0.42 contributes 0.42 * 0.3 = 0.126 to the ledger
        // This is well below enterThreshold (0.65)
        #expect(
            result.skipConfidence < SkipPolicyConfig.default.enterThreshold,
            "Borderline score without FM must remain below enterThreshold"
        )
    }
}

// MARK: - 17. FM-only conversational ad → skip-eligible with quorum

@Suite("Phase6 — FM-only conversational ad skip-eligible with evidence quorum")
struct Phase6FMOnlyConversationalAdTests {

    /// A conversational ad detected primarily by FM (no lexical URL/promo signals)
    /// is skip-eligible when the evidence quorum is satisfied for fmConsensus.
    ///
    /// The quorum check for fmConsensus requires:
    ///   - 2+ distinct evidence kinds in ledger
    ///   - transcript quality == .good
    ///   - span duration in [5s, 180s]
    ///
    /// The FM entry (from BackfillEvidenceFusion) and the classifier entry together
    /// satisfy the 2-kind requirement. "Skip-eligible" here means eligibilityGate == .eligible;
    /// whether the score is above the SkipOrchestrator's hysteresis threshold is orthogonal.
    @Test("FM-only conversational ad with quorum satisfied is skip-eligible (gate == .eligible)")
    func fmOnlyConversationalAdEligibleWithQuorum() {
        // fmConsensus provenance — multi-window consensus already satisfied by the span decoder
        let span = makePhase6Span(
            startTime: 60.0,
            endTime: 120.0,  // 60s span — valid duration [5, 180]
            anchorProvenance: [.fmConsensus(regionId: "r-conv", consensusStrength: 0.88)]
        )

        // FM evidence (conversational ad read — no URL/promo codes)
        // Use high weights so the score is meaningfully above zero.
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.40,  // at fmCap to maximize signal
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )

        // Classifier fires on conversational language patterns. The classifier entry comes
        // from BackfillEvidenceFusion (always included); we pass it explicitly to make
        // the ledger building path clear.
        // classifierScore=1.0 → weight=min(1.0*0.3, 0.3)=0.3. Together with FM=0.4: total=0.7.

        // This provides 2 distinct evidence kinds: .fm + .classifier → quorum satisfied for fmConsensus
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 1.0,  // strong classifier signal: contributes 0.3 weight
            fmEntries: [fmEntry],
            lexicalEntries: [],   // no lexical URL/promo signals
            acousticEntries: [],
            catalogEntries: [],
            mode: .full,
            config: FusionWeightConfig()
        )

        let ledger = fusion.buildLedger()
        let sources = Set(ledger.map { $0.source })

        // Must have at least 2 distinct sources for fmConsensus quorum
        #expect(sources.count >= 2, "FM-only conversational ad needs 2+ distinct evidence kinds for fmConsensus quorum")
        #expect(sources.contains(.fm), "Ledger must contain FM evidence")
        #expect(sources.contains(.classifier), "Ledger must contain classifier evidence (always included)")

        let mapper = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let result = mapper.map()

        // Gate must be eligible: fmConsensus + 2 kinds (.fm + .classifier) + good quality + valid duration
        #expect(
            result.eligibilityGate == .eligible,
            "FM conversational ad with quorum satisfied must be skip-eligible (gate == .eligible)"
        )

        // Score must be > 0 (honest evidence accumulation)
        #expect(result.skipConfidence > 0.0, "FM conversational ad must have non-zero skip confidence")

        // With FM=0.4 and classifier=0.3, total=0.7 which exceeds enterThreshold (0.65)
        // confirming this is actionable by the SkipOrchestrator as well
        #expect(
            result.skipConfidence >= SkipPolicyConfig.default.enterThreshold,
            "FM conversational ad with strong FM+classifier should exceed enterThreshold (\(SkipPolicyConfig.default.enterThreshold))"
        )
    }

    /// FM-only (no classifier) conversational ad: fmConsensus with only 1 source kind
    /// blocks by quorum. This ensures FM alone is insufficient for skip eligibility.
    @Test("FM-only conversational ad without classifier: blocked by quorum (only 1 kind)")
    func fmOnlyConversationalAdBlockedWithoutClassifier() {
        let span = makePhase6Span(
            startTime: 60.0,
            endTime: 120.0,
            anchorProvenance: [.fmConsensus(regionId: "r-conv-blocked", consensusStrength: 0.88)]
        )

        // Only FM, no classifier, no other sources
        let fmEntry = EvidenceLedgerEntry(
            source: .fm,
            weight: 0.38,
            detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")
        )

        // Build ledger WITHOUT classifier (FM-only case)
        let entries: [EvidenceLedgerEntry] = [fmEntry]

        let mapper = DecisionMapper(
            span: span,
            ledger: entries,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        )
        let result = mapper.map()

        // fmConsensus with only 1 kind (fm) fails the 2+ kinds requirement
        #expect(
            result.eligibilityGate == .blockedByEvidenceQuorum,
            "FM-only (no classifier) with fmConsensus must be blocked: only 1 distinct evidence kind"
        )
        // Score must remain honest (not zero-clamped)
        #expect(result.skipConfidence > 0.0, "Score must remain honest even when blocked")
    }
}

// MARK: - 18. Old promote/suppress path removed

@Suite("Phase6 — resolveDecision() removed from production code")
struct Phase6ResolveDecisionRemovedTests {

    /// Verifies the old promote/suppress `resolveDecision()` method no longer exists
    /// in AdDetectionService by confirming the backfill pipeline works and only uses
    /// the fusion path. This is a behavioral test — we check that the old code path's
    /// signature is absent by running the new path and confirming it doesn't hit
    /// the legacy branch.
    ///
    /// NOTE: The actual grep-level source check is in BackfillFusionPipelineTests
    /// ("resolveDecision path is absent"). This test is the behavioral complement:
    /// the backfill now runs without errors through the fusion path only.
    @Test("runBackfill uses fusion path (no legacy resolveDecision branch)")
    func runBackfillUsesFusionPath() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-no-resolve-decision"
        try await store.insertAsset(makePhase6Asset(id: assetId))

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "p6-no-legacy-v1",
                fmBackfillMode: .off
            )
        )

        let chunks = [
            TranscriptChunk(
                id: "c0",
                analysisAssetId: assetId,
                segmentFingerprint: "fp",
                chunkIndex: 0,
                startTime: 0,
                endTime: 30,
                text: "Brought to you by Acme. Visit acme.com with promo code TEST.",
                normalizedText: "brought to you by acme visit acme dot com with promo code test",
                pass: "final",
                modelVersion: "v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        ]

        // If resolveDecision() still existed and the old path fired, it would
        // call store.updateAdWindowDecision which doesn't exist for fusion windows yet.
        // The fusion path completes cleanly.
        await #expect(throws: Never.self) {
            try await service.runBackfill(
                chunks: chunks,
                analysisAssetId: assetId,
                podcastId: "podcast-no-legacy",
                episodeDuration: 30.0
            )
        }

        // Fusion path produces AdWindows and DecisionEvents.
        // The old resolveDecision path never wrote DecisionEvents — it only called
        // updateAdWindowDecision on existing rows. The fusion path writes DecisionEvents
        // for every span it processes. So DecisionEvents being written is the behavioral
        // proof that the new fusion path ran (not resolveDecision).
        let windows = try await store.fetchAdWindows(assetId: assetId)
        let events = try await store.loadDecisionEvents(for: assetId)

        if !windows.isEmpty {
            // The new fusion path must have written DecisionEvents for each window.
            // resolveDecision wrote no events — so if windows exist without events,
            // the old path ran.
            #expect(
                !events.isEmpty,
                "Fusion path must write DecisionEvents — absence would indicate resolveDecision (which writes no events) ran instead"
            )

            // Each event must reference one of the produced windows
            let windowIds = Set(windows.map { $0.id })
            for event in events {
                #expect(
                    windowIds.contains(event.windowId),
                    "DecisionEvent.windowId must match a produced AdWindow id"
                )
            }
        }
    }
}

// MARK: - G1 + G2. Hysteresis for receiveAdDecisionResults path

@Suite("Phase6 — AdDecisionResult hysteresis stay/exit characterization")
struct Phase6AdDecisionHysteresisTests {

    private func makeTrustService(mode: String) async throws -> TrustScoringService {
        let store = try await makeTestStore()
        try await store.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-hysteresis",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: 0.9,
                observationCount: 10,
                mode: mode,
                recentFalseSkipSignals: 0
            )
        )
        return TrustScoringService(store: store)
    }

    private func makeDecisionResult(
        id: String,
        startTime: Double,
        endTime: Double,
        skipConfidence: Double
    ) -> AdDecisionResult {
        AdDecisionResult(
            id: id,
            analysisAssetId: "asset-hysteresis",
            startTime: startTime,
            endTime: endTime,
            skipConfidence: skipConfidence,
            eligibilityGate: .eligible,
            recomputationRevision: 1
        )
    }

    // MARK: - Stay threshold

    /// A span above enterThreshold is applied; a second sequential span above stayThreshold
    /// but below enterThreshold stays applied (hysteresis stay behavior).
    @Test("Eligible span above stayThreshold after entering ad state: stays applied")
    func spanAboveStayThresholdAfterEnter() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hysteresis"
        try await store.insertAsset(makePhase6Asset(id: assetId))
        let trustService = try await makeTrustService(mode: "auto")

        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(analysisAssetId: assetId, podcastId: "podcast-hysteresis")

        // Enter span: above enterThreshold (0.65)
        let enterSpan = makeDecisionResult(
            id: "enter-span",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.80
        )

        // Stay span: above stayThreshold (0.45) but below enterThreshold (0.65)
        let staySpan = makeDecisionResult(
            id: "stay-span",
            startTime: 121,
            endTime: 180,
            skipConfidence: 0.55
        )

        // Verify preconditions
        #expect(enterSpan.skipConfidence > SkipPolicyConfig.default.enterThreshold)
        #expect(staySpan.skipConfidence > SkipPolicyConfig.default.stayThreshold)
        #expect(staySpan.skipConfidence < SkipPolicyConfig.default.enterThreshold)

        // Send enter span first, then stay span
        await orchestrator.receiveAdDecisionResults([enterSpan])
        await orchestrator.receiveAdDecisionResults([staySpan])

        let log = await orchestrator.getDecisionLog()
        let enterApplied = log.filter { $0.adWindowId == "enter-span" && $0.decision == .applied }
        let stayApplied = log.filter { $0.adWindowId == "stay-span" && $0.decision == .applied }

        #expect(!enterApplied.isEmpty, "Enter span (above enterThreshold) must be applied")
        #expect(
            !stayApplied.isEmpty,
            "Stay span (above stayThreshold, hysteresis active) must stay applied"
        )
    }

    /// A span below stayThreshold causes the orchestrator to exit ad state.
    /// The span must be suppressed.
    @Test("Eligible span below stayThreshold after entering ad state: suppressed (exit ad state)")
    func spanBelowStayThresholdExitsAdState() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hysteresis"
        try await store.insertAsset(makePhase6Asset(id: assetId))
        let trustService = try await makeTrustService(mode: "auto")

        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(analysisAssetId: assetId, podcastId: "podcast-hysteresis")

        // Enter span: above enterThreshold
        let enterSpan = makeDecisionResult(
            id: "enter-for-exit",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.80
        )

        // Below-stay span: below stayThreshold → exit ad state
        let belowStaySpan = makeDecisionResult(
            id: "below-stay-span",
            startTime: 181,
            endTime: 240,
            skipConfidence: 0.35
        )

        #expect(enterSpan.skipConfidence > SkipPolicyConfig.default.enterThreshold)
        #expect(belowStaySpan.skipConfidence < SkipPolicyConfig.default.stayThreshold)

        await orchestrator.receiveAdDecisionResults([enterSpan])
        await orchestrator.receiveAdDecisionResults([belowStaySpan])

        let log = await orchestrator.getDecisionLog()
        let belowStaySuppressed = log.filter { $0.adWindowId == "below-stay-span" && $0.decision == .suppressed }

        #expect(
            !belowStaySuppressed.isEmpty,
            "Span below stayThreshold must be suppressed (exits ad state)"
        )
    }

    /// Multi-span test: send enter, stay, below-stay in one receiveAdDecisionResults call.
    /// Verifies the temporal ordering of hysteresis evaluation.
    @Test("Multi-span: enter+stay+below-stay in single call — enter applied, stay applied, below-stay suppressed")
    func multiSpanHysteresisEnterStayBelowStay() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hysteresis"
        try await store.insertAsset(makePhase6Asset(id: assetId))
        let trustService = try await makeTrustService(mode: "auto")

        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(analysisAssetId: assetId, podcastId: "podcast-hysteresis")

        // Three sequential spans with decreasing confidence
        let enterSpan = makeDecisionResult(
            id: "ms-enter",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.80  // > enterThreshold (0.65)
        )
        let staySpan = makeDecisionResult(
            id: "ms-stay",
            startTime: 122,
            endTime: 180,
            skipConfidence: 0.52  // > stayThreshold (0.45), < enterThreshold
        )
        let belowStaySpan = makeDecisionResult(
            id: "ms-below-stay",
            startTime: 182,
            endTime: 240,
            skipConfidence: 0.38  // < stayThreshold (0.45)
        )

        // All preconditions
        #expect(enterSpan.skipConfidence > SkipPolicyConfig.default.enterThreshold)
        #expect(staySpan.skipConfidence > SkipPolicyConfig.default.stayThreshold)
        #expect(staySpan.skipConfidence < SkipPolicyConfig.default.enterThreshold)
        #expect(belowStaySpan.skipConfidence < SkipPolicyConfig.default.stayThreshold)

        // Send all three in one call — they must be evaluated in temporal order
        await orchestrator.receiveAdDecisionResults([enterSpan, staySpan, belowStaySpan])

        let log = await orchestrator.getDecisionLog()

        let enterApplied = log.filter { $0.adWindowId == "ms-enter" && $0.decision == .applied }
        let stayApplied = log.filter { $0.adWindowId == "ms-stay" && $0.decision == .applied }
        let belowStaySuppressed = log.filter { $0.adWindowId == "ms-below-stay" && $0.decision == .suppressed }

        #expect(!enterApplied.isEmpty, "Enter span must be applied (above enterThreshold)")
        #expect(
            !stayApplied.isEmpty,
            "Stay span must be applied (above stayThreshold with hysteresis active)"
        )
        #expect(
            !belowStaySuppressed.isEmpty,
            "Below-stay span must be suppressed (exits ad state when below stayThreshold)"
        )
    }

    /// Span below enterThreshold in auto mode, with no prior ad state: suppressed.
    @Test("Eligible span below enterThreshold without prior ad state: suppressed")
    func spanBelowEnterThresholdNoPriorAdState() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hysteresis"
        try await store.insertAsset(makePhase6Asset(id: assetId))
        let trustService = try await makeTrustService(mode: "auto")

        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(analysisAssetId: assetId, podcastId: "podcast-hysteresis")

        let belowEnterSpan = makeDecisionResult(
            id: "below-enter",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.55  // > stayThreshold but < enterThreshold
        )

        #expect(belowEnterSpan.skipConfidence > SkipPolicyConfig.default.stayThreshold)
        #expect(belowEnterSpan.skipConfidence < SkipPolicyConfig.default.enterThreshold)

        await orchestrator.receiveAdDecisionResults([belowEnterSpan])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.adWindowId == "below-enter" && $0.decision == .applied }

        // Without prior ad state, must meet enterThreshold (not just stayThreshold)
        #expect(
            applied.isEmpty,
            "Span below enterThreshold without prior ad state must NOT be applied (hysteresis requires enterThreshold to enter)"
        )
    }
}
