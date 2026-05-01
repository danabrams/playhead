// AdDetectionServiceShadowModeTests.swift
// The shadow invariant: with fmBackfillMode = .shadow, the AdWindows produced
// by AdDetectionService.runBackfill must be byte-identical to the AdWindows
// produced with fmBackfillMode = .off. The only observable difference is
// rows in semantic_scan_results / evidence_events.

import Foundation
import Testing

@testable import Playhead

@Suite("AdDetectionService shadow-mode invariant")
struct AdDetectionServiceShadowModeTests {

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
        let texts = [
            "Welcome to the show. Today we're discussing podcasts and how to find them.",
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off your first purchase at squarespace dot com slash show.",
            "Now back to our interview with our guest about technology trends."
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

    private func makeService(
        store: AnalysisStore,
        config: AdDetectionConfig,
        factory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)? = nil
    ) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            backfillJobRunnerFactory: factory
        )
    }

    /// Builds a deterministic `BackfillJobRunnerFactory` that routes FM work
    /// through `TestFMRuntime` with a canned coarse response. Both the
    /// shadow and off services in test #7 receive the same factory so
    /// the only observable difference is the `fmBackfillMode` gate in
    /// `AdDetectionService.runShadowFMPhase`.
    private func makeDeterministicFactory() -> @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner {
        return { store, mode in
            BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(
                    runtime: TestFMRuntime(
                        coarseResponses: [
                            CoarseScreeningSchema(
                                disposition: .containsAd,
                                support: CoarseSupportSchema(
                                    supportLineRefs: [1],
                                    certainty: .strong
                                )
                            )
                        ]
                    ).runtime
                ),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON()
            )
        }
    }

    private func adWindowSignature(_ window: AdWindow) -> String {
        // Strip ID/UUID-like fields so signatures stay byte-identical across runs.
        "\(window.startTime)|\(window.endTime)|\(window.confidence)|\(window.boundaryState)|\(window.decisionState)|\(window.detectorVersion)|\(window.advertiser ?? "-")|\(window.product ?? "-")|\(window.evidenceText ?? "-")|\(window.evidenceSources ?? "-")|\(window.eligibilityGate ?? "-")"
    }

    @Test("H-R3-3 (Phase 6 update): shadow and off produce identical AdWindow signatures")
    func shadowAndOffProduceIdenticalAdWindows() async throws {
        // playhead-4my.6.4: Phase 6 fusion has landed. The old invariant
        // ("shadow must not touch AdWindow rows") is superseded by:
        //   1. Both off and shadow run the FULL fusion pipeline and write
        //      AdWindows. FM evidence is excluded from both ledgers, so the
        //      resulting window signatures are identical.
        //   2. Shadow mode additionally persists FM telemetry (SemanticScanResult
        //      rows), while off mode does not.
        //   3. Off mode must not persist FM telemetry.
        //
        // We use two independent stores with identical inputs to compare
        // signatures across modes, rather than running sequentially on the same
        // store (which would double-insert windows on the second run).
        let offStore = try await makeTestStore()
        let shadowStore = try await makeTestStore()
        let offAssetId = "asset-off"
        let shadowAssetId = "asset-shadow"
        try await offStore.insertAsset(makeAsset(id: offAssetId))
        try await shadowStore.insertAsset(makeAsset(id: shadowAssetId))
        let factory = makeDeterministicFactory()

        // Identical chunks for both assets (different asset IDs, same content).
        let offChunks = makeChunks(assetId: offAssetId)
        let shadowChunks = makeChunks(assetId: shadowAssetId)

        let offService = makeService(
            store: offStore,
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .off
            ),
            factory: factory
        )
        let shadowService = makeService(
            store: shadowStore,
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            factory: factory
        )

        try await offService.runBackfill(
            chunks: offChunks,
            analysisAssetId: offAssetId,
            podcastId: "podcast-shared",
            episodeDuration: 90
        )
        try await shadowService.runBackfill(
            chunks: shadowChunks,
            analysisAssetId: shadowAssetId,
            podcastId: "podcast-shared",
            episodeDuration: 90
        )

        let offWindows = try await offStore.fetchAdWindows(assetId: offAssetId)
        let shadowWindows = try await shadowStore.fetchAdWindows(assetId: shadowAssetId)

        // Phase 6 invariant: both modes produce the same number of AdWindows
        // with identical signatures (FM excluded from both ledgers).
        let offSigs = offWindows.map(adWindowSignature).sorted()
        let shadowSigs = shadowWindows.map(adWindowSignature).sorted()
        #expect(offSigs == shadowSigs,
                "off and shadow modes must produce identical AdWindow signatures (FM excluded from both ledgers). off=\(offSigs.count), shadow=\(shadowSigs.count)")

        // Off mode must not have written FM telemetry.
        let offScans = try await offStore.fetchSemanticScanResults(analysisAssetId: offAssetId)
        let offEvents = try await offStore.fetchEvidenceEvents(analysisAssetId: offAssetId)
        #expect(offScans.isEmpty, "off mode must not persist semantic scan rows")
        #expect(offEvents.isEmpty, "off mode must not persist evidence events")

        // Shadow mode MUST have exercised the FM pipeline (if factory was invoked).
        // Note: the FM pipeline only runs if the factory is invoked AND the
        // canUseFoundationModels guard passes. In test context both are true.
        let shadowScans = try await shadowStore.fetchSemanticScanResults(analysisAssetId: shadowAssetId)
        #expect(!shadowScans.isEmpty,
                "shadow service must have exercised the FM pipeline and persisted scan rows")
    }

    @Test("M-D: shadow phase short-circuits when canUseFoundationModels is false")
    func shadowPhaseSkipsWhenFMUnavailable() async throws {
        // Inject a provider that reports FM is unavailable. The runner
        // factory should never be invoked; no semantic scan rows should
        // land in the store even though fmBackfillMode == .shadow.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-md"))

        // Use a factory that would PANIC if invoked, to prove the guard
        // runs before the factory is touched.
        nonisolated(unsafe) var factoryCallCount = 0
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: { store, mode in
                factoryCallCount += 1
                return BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON()
                )
            },
            canUseFoundationModelsProvider: { false }
        )

        try await service.runBackfill(
            chunks: makeChunks(assetId: "asset-md"),
            analysisAssetId: "asset-md",
            podcastId: "podcast-md",
            episodeDuration: 90
        )

        #expect(factoryCallCount == 0, "factory must not be invoked when FM is unavailable")
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-md")
        #expect(scans.isEmpty, "no semantic scan rows should be written")
    }

    @Test("missing podcastId skips priors and shadow telemetry writes")
    func missingPodcastIdSkipsProfileAndShadowTelemetry() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-missing-podcast"
        try await store.insertAsset(makeAsset(id: assetId))

        nonisolated(unsafe) var factoryCallCount = 0
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: { store, mode in
                factoryCallCount += 1
                return BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON()
                )
            }
        )

        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "",
            episodeDuration: 90
        )

        let profile = try await store.fetchProfile(podcastId: "")
        #expect(profile == nil, "missing podcast id must not create an empty-key profile")
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(scans.isEmpty, "missing podcast id must not start shadow FM telemetry")
        #expect(factoryCallCount == 0, "shadow FM factory must not be invoked without a podcast id")
    }

    /// Bug 7 regression: `runBackfill` end-to-end on an FM-capable fixture
    /// MUST land at least one row in `training_examples`. The captured
    /// xcappdata bundle from 2026-04-30 had 0 rows in `training_examples`
    /// despite real classifier decisions and ad windows on disk; the
    /// proximate cause (Bug 11) was that `BackfillJobRunner.runJob` could
    /// return early with zero `semantic_scan_results` rows, which is the
    /// spine the `TrainingExampleMaterializer` requires. Bug 11 was fixed
    /// in the previous commit on this branch's history; this test pins the
    /// production wiring so a future regression that removes the
    /// `materializeTrainingExamples` call site or breaks the spine
    /// invariant trips a fast-test.
    ///
    /// We deliberately use the same factory shape as
    /// `shadowModeWritesScanTelemetry` (real-canned coarse `.containsAd`
    /// response → real `.success` row) so the materializer's
    /// `status == .success` filter is satisfied. Sentinel rows
    /// (`.noAds` status, written by Bug 11's defensive backstop) do NOT
    /// produce training examples and are not what we are asserting here.
    @Test("Bug 7: shadow mode runBackfill materializes at least one training_examples row")
    func shadowModeMaterializesTrainingExamples() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-bug7-train"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: { store, mode in
                BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(
                        runtime: TestFMRuntime(
                            coarseResponses: [
                                CoarseScreeningSchema(
                                    disposition: .containsAd,
                                    support: CoarseSupportSchema(
                                        supportLineRefs: [1],
                                        certainty: .strong
                                    )
                                )
                            ]
                        ).runtime
                    ),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON()
                )
            }
        )

        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-bug7-train",
            episodeDuration: 90
        )

        // Pre-condition the materializer depends on: there MUST be at
        // least one .success-status scan row, otherwise the materializer
        // produces nothing (and our assertion below would silently pass
        // for the wrong reason).
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        let successScans = scans.filter { $0.status == .success }
        #expect(!successScans.isEmpty,
                "fixture must produce at least one success scan row so the materializer has a spine to read from (got \(scans.count) total, \(successScans.count) success)")

        // Bug 7 invariant: training_examples MUST be populated for an
        // FM-capable run that produced success scan rows. The
        // materializer is invoked from `runShadowFMPhase` after
        // `runner.runPendingBackfill` returns success — if that wiring
        // breaks, this assertion fails.
        let examples = try await store.loadTrainingExamples(forAsset: assetId)
        #expect(!examples.isEmpty,
                "training_examples MUST be populated when shadow FM phase runs to completion against success scan rows (got \(examples.count) for \(successScans.count) success scans)")
    }

    @Test("shadow mode actually writes semantic_scan_results telemetry")
    func shadowModeWritesScanTelemetry() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-tel"))
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: { store, mode in
                BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(
                        runtime: TestFMRuntime(
                            coarseResponses: [
                                CoarseScreeningSchema(
                                    disposition: .containsAd,
                                    support: CoarseSupportSchema(
                                        supportLineRefs: [1],
                                        certainty: .strong
                                    )
                                )
                            ]
                        ).runtime
                    ),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON()
                )
            }
        )

        try await service.runBackfill(
            chunks: makeChunks(assetId: "asset-tel"),
            analysisAssetId: "asset-tel",
            podcastId: "podcast-tel",
            episodeDuration: 90
        )

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-tel")
        #expect(!scans.isEmpty, "shadow mode must persist FM scan results to telemetry")
        // And shadow mode must STILL not have promoted any FM source to AdWindows.
        let windows = try await store.fetchAdWindows(assetId: "asset-tel")
        #expect(windows.allSatisfy { $0.detectorVersion == "detection-v1" })
    }

    @Test("live runBackfill path flips planner into targetedWithAudit and executes targeted jobs")
    func runBackfillFlipsPlannerAndExecutesTargetedJobs() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-live-targeted"
        let runtime = TestFMRuntime(
            coarseResponses: Array(
                repeating: CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        // Cycle 2 C5: align with the 30-line fixture's
                        // ad copy at lines 14-15 so the recall sample
                        // computed by the runner is non-zero and the
                        // planner ring fills with passing samples.
                        supportLineRefs: [14],
                        certainty: .strong
                    )
                ),
                count: 24
            )
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
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: { store, mode in
                BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(runtime: runtime.runtime),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON()
                )
            }
        )

        func makeEpisodeChunks(_ assetId: String) -> [TranscriptChunk] {
            // Cycle 2 C5 / Cycle 4 B4 Rev3-M6: 30-line fixture so the
            // per-anchor narrowing model produces a strict subset
            // (anchors land at lines 14/15, narrowed envelope ~[9..20]
            // = 12 segments, strictly less than the 30-segment full
            // episode). The legacy 8-line fixture was too small for the
            // new model: a single anchor with default padding=5 already
            // covers the full episode and the Rev3-M6 narrowing rail
            // can't fire.
            //
            // Cycle 4 B4: the chunks MUST include time gaps between
            // adjacent pieces so `TranscriptSegmenter` splits them
            // into distinct segments rather than collapsing everything
            // into a handful of big segments. The segmenter's default
            // pause threshold is 1.5s — use a 5s gap per chunk to
            // clear it comfortably. Without this, 30 chunks collapse
            // into 2-3 segments and the per-anchor narrowing envelope
            // (padding=5 in segment index space) covers the entire
            // episode, making the harvester / lexical phase rows look
            // like full-rescan rows and silently defeating Rev3-M6.
            var lines: [String] = []
            for idx in 0..<30 {
                switch idx {
                case 14:
                    lines.append("Before we continue, this episode is brought to you by ExampleCo.")
                case 15:
                    lines.append("Visit example.com slash deal and use promo code PLAYHEAD.")
                default:
                    lines.append("Editorial line \(idx) about the topic of the day.")
                }
            }
            return lines.enumerated().map { index, text in
                // 10s of content + 5s pause per chunk. The 5s gap
                // exceeds the segmenter's default 1.5s pause threshold
                // and forces a hard break between every chunk.
                let chunkStart = Double(index) * 15.0
                let chunkEnd = chunkStart + 10.0
                return TranscriptChunk(
                    id: "\(assetId)-chunk-\(index)",
                    analysisAssetId: assetId,
                    segmentFingerprint: "\(assetId)-fp-\(index)",
                    chunkIndex: index,
                    startTime: chunkStart,
                    endTime: chunkEnd,
                    text: text,
                    normalizedText: text.lowercased(),
                    pass: "final",
                    modelVersion: "test-v1",
                    transcriptVersion: nil,
                    atomOrdinal: nil
                )
            }
        }

        for episode in 1...6 {
            let assetId = "asset-live-targeted-\(episode)"
            try await store.insertAsset(makeAsset(id: assetId))
            try await service.runBackfill(
                chunks: makeEpisodeChunks(assetId),
                analysisAssetId: assetId,
                podcastId: podcastId,
                episodeDuration: 300
            )
        }

        let plannerState = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(plannerState.observedEpisodeCount == 6)
        #expect(plannerState.stableRecallFlag)

        let targetedAssetId = "asset-live-targeted-6"
        let passA = try await store.fetchSemanticScanResults(
            analysisAssetId: targetedAssetId,
            scanPass: "passA"
        )
        #expect(passA.count == 3, "targeted episode should produce one passA row per targeted phase")

        let episodeChunks = makeEpisodeChunks(targetedAssetId)
        let (_, version) = TranscriptAtomizer.atomize(
            chunks: episodeChunks,
            analysisAssetId: targetedAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )

        let expectedTargetedPhases: [BackfillJobPhase] = [
            .scanHarvesterProposals,
            .scanLikelyAdSlots,
            .scanRandomAuditWindows,
        ]
        for (offset, phase) in expectedTargetedPhases.enumerated() {
            let jobId = BackfillJobRunner.makeJobIdForTesting(
                analysisAssetId: targetedAssetId,
                transcriptVersion: version.transcriptVersion,
                phase: phase,
                offset: offset
            )
            let job = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(job.status == .complete)
            #expect(job.phase == phase)
        }

        let fullEpisodeJobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: targetedAssetId,
            transcriptVersion: version.transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )
        let fullEpisodeJob = try await store.fetchBackfillJob(byId: fullEpisodeJobId)
        #expect(fullEpisodeJob == nil)

        // Cycle 6 B6 Rev3-M6: now that BackfillJobRunner persists the
        // originating phase into `semantic_scan_results.phase`, filter
        // rows by phase and assert that BOTH harvester AND lexical
        // narrowing phases produced at least one row that is a strict
        // subset of the full episode. The legacy `>= 2` assertion would
        // pass even if only audit + one of the heavy-lifting phases
        // narrowed — this is the condition the cycle-5 reviewer flagged.
        let (allAtoms, _) = TranscriptAtomizer.atomize(
            chunks: makeEpisodeChunks(targetedAssetId),
            analysisAssetId: targetedAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        let firstOrdinal = allAtoms.first?.atomKey.atomOrdinal ?? 0
        let lastOrdinal = allAtoms.last?.atomKey.atomOrdinal ?? 0
        let episodeAtomCount = lastOrdinal - firstOrdinal
        func isStrictSubset(_ row: SemanticScanResult) -> Bool {
            (row.windowLastAtomOrdinal - row.windowFirstAtomOrdinal) < episodeAtomCount
        }

        let harvesterRows = passA.filter { $0.jobPhase == BackfillJobPhase.scanHarvesterProposals.rawValue }
        let lexicalRows = passA.filter { $0.jobPhase == BackfillJobPhase.scanLikelyAdSlots.rawValue }
        let auditRows = passA.filter { $0.jobPhase == BackfillJobPhase.scanRandomAuditWindows.rawValue }

        #expect(!harvesterRows.isEmpty, "harvester phase must persist at least one passA row")
        #expect(!lexicalRows.isEmpty, "lexical phase must persist at least one passA row")
        #expect(!auditRows.isEmpty, "audit phase must persist at least one passA row")

        #expect(
            harvesterRows.contains(where: isStrictSubset),
            "Cycle 6 B6 Rev3-M6: HARVESTER phase must produce a strict-subset row (\(harvesterRows.count) rows, none narrowed)"
        )
        #expect(
            lexicalRows.contains(where: isStrictSubset),
            "Cycle 6 B6 Rev3-M6: LEXICAL phase must produce a strict-subset row (\(lexicalRows.count) rows, none narrowed)"
        )
        #expect(
            auditRows.contains(where: isStrictSubset),
            "Cycle 6 B6 Rev3-M6: AUDIT phase must produce a strict-subset row (\(auditRows.count) rows, none narrowed)"
        )
    }
}

// MARK: - playhead-ux6r: fusion path eligibilityGate persistence
//
// `buildFusionAdWindow` previously omitted `eligibilityGate` from the
// persisted row. The live-decision path forwards the gate via
// `AdDecisionResult` so in-process runs are correct, but on app restart
// (`SkipOrchestrator.beginEpisode`) the row is reloaded from
// `ad_windows` and the NULL gate silently re-arms a previously-demoted
// span for auto-skip.
//
// Direction: under-restriction. These tests pin the producer side: the
// row written by `runBackfill` MUST carry the same gate string the
// in-flight `DecisionResult` carried, and that string MUST survive a
// store close/reopen (the closest analog to an app restart available
// without a real launch).

@Suite("playhead-ux6r — fusion AdWindow eligibilityGate persistence")
struct FusionEligibilityGatePersistenceTests {

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

    /// Strong lexical ad chunk that drives a fusion window with
    /// `eligibilityGate == .eligible` (no FM provenance, lexical
    /// in-audio corroboration → `metadataCorroborationGate` short-
    /// circuits to `.eligible`).
    private func makeAdChunks(assetId: String) -> [TranscriptChunk] {
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

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "ux6r-test-v1",
                fmBackfillMode: .off
            )
        )
    }

    /// Detector-version filter that survives `extractAndPersistMetadata`
    /// rewrites of `metadataSource`. The detector version is set once at
    /// the producer (`buildFusionAdWindow`) and never updated by
    /// downstream metadata extraction, making it the stable provenance
    /// stamp for "this row came from the fusion path".
    private func fusionWindows(_ windows: [AdWindow]) -> [AdWindow] {
        windows.filter { $0.detectorVersion == "ux6r-test-v1" }
    }

    // MARK: - Primary regression (was failing pre-fix)

    @Test("runBackfill stamps eligibilityGate on every persisted fusion AdWindow")
    func runBackfillStampsEligibilityGateOnFusionWindows() async throws {
        let dir = try makeTempDir(prefix: "ux6r-stamp")
        let store = try await AnalysisStore.open(directory: dir)
        let assetId = "asset-ux6r-stamp"
        try await store.insertAsset(makeAsset(id: assetId))

        let service = makeService(store: store)
        try await service.runBackfill(
            chunks: makeAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-ux6r-stamp",
            episodeDuration: 90.0
        )

        let persisted = fusionWindows(try await store.fetchAdWindows(assetId: assetId))
        #expect(!persisted.isEmpty,
                "Squarespace lexical fixture must produce at least one fusion window")
        for window in persisted {
            // Pre-fix: `eligibilityGate` was nil for every fusion row.
            // Post-fix: every row carries the SkipEligibilityGate.rawValue
            // from the in-flight DecisionResult.
            #expect(
                window.eligibilityGate != nil,
                "Fusion window \(window.id) must carry a non-nil eligibilityGate after runBackfill (was the bug)"
            )
            // Defensive: the stamped value must round-trip through
            // SkipEligibilityGate. A garbage string would silently
            // bypass the SkipOrchestrator's `== \"markOnly\"` guard.
            if let raw = window.eligibilityGate {
                #expect(
                    SkipEligibilityGate(rawValue: raw) != nil,
                    "Persisted gate \"\(raw)\" must decode back to a known SkipEligibilityGate case"
                )
            }
        }
    }

    // MARK: - Edge cases (autoSkip-shaped + markOnly-shaped)

    /// `decision.eligibilityGate == .eligible` is the fusion-path
    /// equivalent of "autoSkip-eligible" — the consumer gate
    /// (`SkipOrchestrator.receiveAdWindows`) only demotes on
    /// `eligibilityGate == \"markOnly\"`, so any other value (including
    /// `\"eligible\"`) leaves the span on the auto-skip path. This test
    /// pins that an eligible fusion window is not silently re-stamped
    /// nor stripped on store reopen.
    @Test("fusion window decided as eligible (autoSkip-shaped) survives store reopen with the same gate")
    func eligibleFusionWindowSurvivesReopen() async throws {
        let dir = try makeTempDir(prefix: "ux6r-eligible-reopen")
        let assetId = "asset-ux6r-eligible"

        // First open: run the pipeline to write fusion windows.
        do {
            let store = try await AnalysisStore.open(directory: dir)
            try await store.insertAsset(makeAsset(id: assetId))
            let service = makeService(store: store)
            try await service.runBackfill(
                chunks: makeAdChunks(assetId: assetId),
                analysisAssetId: assetId,
                podcastId: "podcast-ux6r-eligible",
                episodeDuration: 90.0
            )
        }

        // Second open: same on-disk directory, fresh actor handle —
        // this is the closest stand-in for an app restart available
        // without spinning up the full launch path.
        let reopened = try await AnalysisStore.open(directory: dir)
        let persisted = fusionWindows(try await reopened.fetchAdWindows(assetId: assetId))
        #expect(!persisted.isEmpty, "expected at least one fusion window after reopen")

        // The Squarespace fixture has no FM provenance and at least one
        // in-audio corroborating source (lexical), so DecisionMapper's
        // metadataCorroborationGate returns .eligible.
        for window in persisted {
            #expect(
                window.eligibilityGate == SkipEligibilityGate.eligible.rawValue,
                "eligible fusion window must round-trip as \"eligible\"; got \(String(describing: window.eligibilityGate))"
            )
        }
    }

    /// Forensic fixture: a synthetic `markOnly`-stamped fusion row is
    /// written, the store is closed and reopened, and we assert the
    /// gate field survives the reopen byte-identically. This mirrors
    /// the SkipOrchestrator preload path: `beginEpisode` calls
    /// `fetchAdWindows` and forwards the rows to `receiveAdWindows`,
    /// which gates on `eligibilityGate == \"markOnly\"`. If the field
    /// were dropped on reopen (the ux6r bug, but at the read layer),
    /// auto-skip would re-arm.
    ///
    /// We use `metadataSource = \"fusion-v1\"` so the row is shaped
    /// exactly like a row produced by `buildFusionAdWindow`, making
    /// this a regression for the read-side too.
    @Test("markOnly-stamped fusion row survives close/reopen as \"markOnly\"")
    func markOnlyFusionRowSurvivesReopen() async throws {
        let dir = try makeTempDir(prefix: "ux6r-markonly-reopen")
        let assetId = "asset-ux6r-markonly"

        do {
            let store = try await AnalysisStore.open(directory: dir)
            try await store.insertAsset(makeAsset(id: assetId))
            // A fusion-shaped row stamped markOnly. This is the row
            // shape `buildFusionAdWindow` now emits when the upstream
            // decision is markOnly (e.g. via SpanFinalizer chapter
            // penalty, FM-suppression cap, or any future demotion).
            let row = AdWindow(
                id: "win-ux6r-markonly",
                analysisAssetId: assetId,
                startTime: 30.0,
                endTime: 60.0,
                confidence: 0.85,           // ≥ preload threshold
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "ux6r-test-v1",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: 30.0,
                metadataSource: "fusion-v1",
                metadataConfidence: 0.85,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: SkipEligibilityGate.markOnly.rawValue
            )
            try await store.insertAdWindow(row)
        }

        let reopened = try await AnalysisStore.open(directory: dir)
        let persisted = try await reopened.fetchAdWindows(assetId: assetId)
        let row = try #require(persisted.first { $0.id == "win-ux6r-markonly" })
        #expect(
            row.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
            "markOnly fusion row must round-trip across reopen; got \(String(describing: row.eligibilityGate))"
        )
        #expect(
            row.eligibilityGate == "markOnly",
            "Consumer-side check (SkipOrchestrator.receiveAdWindows compares to the literal \"markOnly\") must still match after reopen"
        )
    }

    /// Bug-5 preload regression coverage with a fusion-shaped row:
    /// the SkipOrchestrator's `beginEpisode` path must NOT auto-skip a
    /// markOnly-stamped fusion row after reopen. This is the load-
    /// bearing invariant ux6r protects: producer (buildFusionAdWindow)
    /// stamps the gate, consumer (SkipOrchestrator) honors it, and the
    /// persistence layer carries the field across a process boundary.
    @Test("SkipOrchestrator preload of a reopened markOnly fusion row routes to suggest tier, not auto-skip")
    func preloadOfReopenedMarkOnlyFusionRowDoesNotAutoSkip() async throws {
        let dir = try makeTempDir(prefix: "ux6r-preload")
        let assetId = "asset-ux6r-preload"

        do {
            let store = try await AnalysisStore.open(directory: dir)
            try await store.insertAsset(makeAsset(id: assetId))
            let row = AdWindow(
                id: "win-ux6r-preload",
                analysisAssetId: assetId,
                startTime: 30.0,
                endTime: 60.0,
                confidence: 0.85,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "ux6r-test-v1",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: 30.0,
                metadataSource: "fusion-v1",
                metadataConfidence: 0.85,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: SkipEligibilityGate.markOnly.rawValue
            )
            try await store.insertAdWindow(row)
        }

        let reopened = try await AnalysisStore.open(directory: dir)
        let orchestrator = SkipOrchestrator(store: reopened)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "ep-\(assetId)"
        )

        // The orchestrator must NOT confirm a markOnly window; it
        // belongs in the suggest tier per SkipOrchestrator
        // receiveAdWindows.
        let confirmed = await orchestrator.confirmedWindows()
        #expect(
            !confirmed.contains(where: { $0.id == "win-ux6r-preload" }),
            "markOnly fusion row must NOT register as a confirmed/auto-skip window after preload"
        )
    }
}
