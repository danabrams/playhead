// AdDetectionServiceShadowModeTests.swift
// The shadow invariant: with fmBackfillMode = .shadow, the AdWindows produced
// by AdDetectionService.runBackfill must be byte-identical to the AdWindows
// produced with fmBackfillMode = .disabled. The only observable difference is
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
    /// shadow and disabled services in test #7 receive the same factory so
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
        "\(window.startTime)|\(window.endTime)|\(window.confidence)|\(window.boundaryState)|\(window.decisionState)|\(window.detectorVersion)|\(window.advertiser ?? "-")|\(window.product ?? "-")|\(window.evidenceText ?? "-")"
    }

    @Test("H-R3-3: shadow mode produces byte-identical cues to disabled mode (same store, same asset)")
    func shadowAndDisabledProduceIdenticalAdWindows() async throws {
        // H-R3-3: strengthened from the test #7 fix. The original
        // comparison used two different stores + two different asset ids,
        // which drifted the comparison: any global state or ordering issue
        // would be hidden by the store split. Now we run BOTH modes
        // against the SAME store + SAME asset id, toggling
        // `fmBackfillMode` by swapping services between calls. The
        // assertion is that AdWindow rows present on-disk before the
        // shadow FM run are identical to the rows present after — shadow
        // mode must never touch AdWindows.
        let store = try await makeTestStore()
        let assetId = "asset-shared"
        try await store.insertAsset(makeAsset(id: assetId))
        let factory = makeDeterministicFactory()
        let chunks = makeChunks(assetId: assetId)

        // Run #1: disabled — shadow gate short-circuits, no FM work, and
        // the classical backfill pipeline writes the baseline AdWindows.
        let disabledService = makeService(
            store: store,
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .disabled
            ),
            factory: factory
        )
        try await disabledService.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-shared",
            episodeDuration: 90
        )
        let preShadowWindows = try await store.fetchAdWindows(assetId: assetId)
        let preShadowSigs = preShadowWindows.map(adWindowSignature).sorted()

        // Confirm the baseline: disabled mode must not have written FM
        // telemetry.
        let preShadowScans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        let preShadowEvents = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        #expect(preShadowScans.isEmpty, "disabled mode must not persist semantic scan rows")
        #expect(preShadowEvents.isEmpty, "disabled mode must not persist evidence events")

        // Run #2: shadow — same store, same asset id. The shadow FM path
        // must write telemetry but MUST NOT mutate the AdWindow rows we
        // just captured.
        let shadowService = makeService(
            store: store,
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
        try await shadowService.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-shared",
            episodeDuration: 90
        )
        let postShadowWindows = try await store.fetchAdWindows(assetId: assetId)
        let postShadowSigs = postShadowWindows.map(adWindowSignature).sorted()

        // The AdWindow set must be byte-identical before vs after the
        // shadow FM run. This is the shadow invariant: FM is observation-
        // only until Phase 6 fusion lands.
        #expect(preShadowSigs == postShadowSigs,
                "shadow FM run mutated AdWindow rows (pre=\(preShadowSigs), post=\(postShadowSigs))")
        #expect(preShadowWindows.map(\.id).sorted() == postShadowWindows.map(\.id).sorted(),
                "shadow FM run changed AdWindow row identities")

        // And the shadow run MUST have actually exercised the FM pipeline —
        // otherwise the invariant is vacuous.
        let postShadowScans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(!postShadowScans.isEmpty,
                "shadow service must have exercised the FM pipeline")
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
            // Cycle 2 C5/Rev3-M6: 30-line fixture so the per-anchor
            // narrowing model produces a strict subset (anchors land at
            // lines 14/15, narrowed envelope ~[9..20] = 12 segments,
            // strictly less than the 30-segment full episode). The
            // legacy 8-line fixture was too small for the new model: a
            // single anchor with default padding=5 already covers the
            // full episode and the Rev3-M6 narrowing rail can't fire.
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
                TranscriptChunk(
                    id: "\(assetId)-chunk-\(index)",
                    analysisAssetId: assetId,
                    segmentFingerprint: "\(assetId)-fp-\(index)",
                    chunkIndex: index,
                    startTime: Double(index) * 10,
                    endTime: Double(index + 1) * 10,
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

        // Cycle 2 Rev3-M6: prove at least one targeted phase row was
        // actually narrowed — i.e. the persisted scan window's
        // atom-ordinal span is strictly smaller than the full episode's
        // atom-ordinal span. The legacy assertion only checked that 3
        // passA rows existed, which would also pass if the runner had
        // quietly fallen back to full-episode coverage on every targeted
        // phase.
        let (allAtoms, _) = TranscriptAtomizer.atomize(
            chunks: makeEpisodeChunks(targetedAssetId),
            analysisAssetId: targetedAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        let firstOrdinal = allAtoms.first?.atomKey.atomOrdinal ?? 0
        let lastOrdinal = allAtoms.last?.atomKey.atomOrdinal ?? 0
        let episodeAtomCount = lastOrdinal - firstOrdinal
        let narrowedRowExists = passA.contains { row in
            (row.windowLastAtomOrdinal - row.windowFirstAtomOrdinal) < episodeAtomCount
        }
        #expect(narrowedRowExists, "Cycle 2 Rev3-M6: at least one targeted phase row must be a strict subset of the full episode")
    }
}
