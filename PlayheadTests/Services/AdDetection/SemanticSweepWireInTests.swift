// SemanticSweepWireInTests.swift
// playhead-y3ya: end-to-end wire-in coverage for the semantic-sweep mark
// compose inside `AdDetectionService.runBackfill` (Step 18c).
//
// The extent policy is pinned in `SemanticSweepMarkComposerTests` and the
// arming in `SemanticSweepMarkSurfacingTests`. This suite guards the WIRING —
// the failure mode where a correct composer ships never being called — and it
// does so by running the SAME deterministic transcript twice, once with
// `semanticSweepMarkEnabled: false` and once `true`, over a fixture that
// reproduces the field shape:
//
//   a plain narration transcript with NO ad cue anywhere, plus one persisted
//   `semantic_scan_results` row saying `containsAd` over 40–100 s.
//
// That is DE0784D8 in miniature. Fusion has nothing to seed a span with, so it
// emits no window over that region — exactly the state in which the verdict was
// discarded — and the only thing that can produce a candidate there is the
// sweep.
//
//   • FLAG OFF is inert: zero rows carry the sweep detector version. Shipping
//     this ON by accident fails here.
//   • FLAG ON composes: the verdict becomes a persisted row. Deleting the
//     Step 18c call — the "ships inert" failure mode a pure-composer test
//     cannot see — fails here.
//   • ADDITIVE ONLY: every row that is NOT a sweep row is byte-identical
//     between the two arms, so no existing window's geometry, gate, anchors or
//     id moved.
//   • BANNER TIER: every persisted sweep row is `markOnly` / `candidate` with
//     both edge anchors `.unanchored`.

import Foundation
import Testing
@testable import Playhead

@Suite("Semantic-sweep mark wire-in (playhead-y3ya)")
struct SemanticSweepWireInTests {

    private static let podcastId = "podcast-semantic-sweep"
    private static let assetId = "asset-semantic-sweep"
    private static let episodeDuration = 200.0

    /// The verdict FM returned and fusion could not attach.
    private static let verdict = (start: 40.0, end: 100.0)

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

    /// Plain narration end to end. No sponsor disclosure, no promo code, no
    /// URL — nothing any deterministic detector can seed a span with. The point
    /// of the fixture is that the transcript alone yields NO candidate over the
    /// verdict's window.
    private func makeChunks(assetId: String) -> [TranscriptChunk] {
        let spans: [(start: Double, end: Double, text: String)] = [
            (0.0, 40.0, "We were talking about the history of the neighborhood and how the old market building changed hands three times before the war."),
            (40.0, 70.0, "So there is this thing I have been meaning to tell you about, and honestly it took me a while to come around to it at all."),
            (70.0, 100.0, "It changed how the mornings go, which is not nothing, and the people I have mentioned it to have said much the same thing."),
            (100.0, 150.0, "Anyway, you were telling me about the market building and the family that ran the produce stall on the corner for forty years."),
            (150.0, 200.0, "That says something about how these places hold a neighborhood together, and about who gets to decide what a street looks like.")
        ]
        return spans.enumerated().map { idx, span in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: span.start,
                endTime: span.end,
                text: span.text,
                normalizedText: span.text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeScanRow() -> SemanticScanResult {
        SemanticScanResult(
            id: "scan-wirein-1",
            analysisAssetId: Self.assetId,
            windowFirstAtomOrdinal: 1,
            windowLastAtomOrdinal: 3,
            windowStartTime: Self.verdict.start,
            windowEndTime: Self.verdict.end,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeCohortJSON(promptLabel: "y3ya-wirein"),
            transcriptVersion: "tv-1"
        )
    }

    private func makeService(
        store: AnalysisStore,
        semanticSweepMarkEnabled: Bool,
        fmBackfillMode: FMBackfillMode
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            // `.proposalOnly` is the default arm rather than `.off`: Step 18c is
            // gated on `canProposeNewRegions`, and this mode grants exactly that
            // while leaving `contributesToExistingCandidateLedger` FALSE — so the
            // verdict still reaches fusion as nothing, which is the state it died
            // in. `runShadowFMPhase` returns `.skipped` immediately (no runner
            // factory is injected here), so no FM runs and both arms stay
            // deterministic.
            fmBackfillMode: fmBackfillMode,
            semanticSweepMarkEnabled: semanticSweepMarkEnabled
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    private func runArm(
        semanticSweepMarkEnabled: Bool,
        fmBackfillMode: FMBackfillMode = .proposalOnly
    ) async throws -> [AdWindow] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: Self.assetId))
        try await store.insertSemanticScanResult(makeScanRow())
        let service = makeService(
            store: store,
            semanticSweepMarkEnabled: semanticSweepMarkEnabled,
            fmBackfillMode: fmBackfillMode
        )
        try await service.runBackfill(
            chunks: makeChunks(assetId: Self.assetId),
            analysisAssetId: Self.assetId,
            podcastId: Self.podcastId,
            episodeDuration: Self.episodeDuration
        )
        return try await store.fetchAdWindows(assetId: Self.assetId)
            .sorted { lhs, rhs in
                lhs.startTime != rhs.startTime
                    ? lhs.startTime < rhs.startTime
                    : lhs.id < rhs.id
            }
    }

    private func sweepRows(_ windows: [AdWindow]) -> [AdWindow] {
        windows.filter { $0.detectorVersion == SemanticSweepMarkComposer.detectorVersion }
    }

    /// A comparable projection of every field the sweep must not touch. String
    /// rather than a tuple so a mismatch prints readably, and `bitPattern` on
    /// the doubles so a sub-epsilon rewrite cannot slip through.
    private func fingerprint(_ window: AdWindow) -> String {
        [
            window.id,
            String(window.startTime.bitPattern),
            String(window.endTime.bitPattern),
            String(window.confidence.bitPattern),
            window.boundaryState,
            window.decisionState,
            window.detectorVersion,
            window.eligibilityGate ?? "nil",
            window.startEdgeAnchor,
            window.endEdgeAnchor
        ].joined(separator: "|")
    }

    /// THE STATE THE VERDICT DIED IN, asserted rather than assumed. If the
    /// fixture's plain narration ever starts producing a window over 40–100 s,
    /// every other test in this suite becomes a test of something else.
    @Test("control: the transcript alone produces no window over the verdict")
    func theTranscriptAloneProducesNothingThere() async throws {
        let windows = try await runArm(semanticSweepMarkEnabled: false)

        #expect(!windows.contains {
            $0.startTime < Self.verdict.end && $0.endTime > Self.verdict.start
        }, "fixture drifted — a detector now seeds this region: \(windows.map(\.id))")
    }

    @Test("flag OFF: no sweep rows are written")
    func flagOffIsInert() async throws {
        let windows = try await runArm(semanticSweepMarkEnabled: false)

        #expect(sweepRows(windows).isEmpty)
    }

    /// THE WIRE. A pure composer that is never called passes every test in
    /// `SemanticSweepMarkComposerTests` and ships inert.
    @Test("flag ON: the verdict is persisted as a sweep row")
    func flagOnComposesTheVerdict() async throws {
        let windows = try await runArm(semanticSweepMarkEnabled: true)
        let rows = sweepRows(windows)

        #expect(rows.count == 1)
        #expect(rows.first?.startTime == Self.verdict.start)
        #expect(rows.first?.endTime == Self.verdict.end)
    }

    @Test("a persisted sweep row is markOnly, candidate and unanchored")
    func aPersistedSweepRowIsBannerTier() async throws {
        let rows = sweepRows(try await runArm(semanticSweepMarkEnabled: true))

        #expect(rows.allSatisfy {
            $0.eligibilityGate == SkipEligibilityGate.markOnly.rawValue
                && $0.decisionState == AdDecisionState.candidate.rawValue
                && $0.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue
                && $0.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue
        })
    }

    /// ADDITIVE ONLY, end to end. Every non-sweep row is byte-identical across
    /// the arms, so nothing the rest of the pipeline decided was perturbed.
    /// THE SHADOW CONTRACT at the service site. `ApprovedCohortRegistry`
    /// collapses any unapproved prompt / schema / scan-plan / locale / appBuild
    /// cohort to `.shadow`, and PlayheadRuntime's bootstrap calls that "exactly
    /// the protection the registry was designed to provide". A banner composed
    /// from an unapproved cohort's verdicts would defeat it, so the compose is
    /// gated on `canProposeNewRegions` and NOT on the feature flag alone.
    @Test("shadow mode composes nothing at the service site")
    func shadowModeComposesNothingAtTheService() async throws {
        let windows = try await runArm(
            semanticSweepMarkEnabled: true, fmBackfillMode: .shadow
        )

        #expect(sweepRows(windows).isEmpty)
    }

    @Test("no existing row changes between the two arms")
    func theSweepIsPurelyAdditive() async throws {
        let off = try await runArm(semanticSweepMarkEnabled: false)
        let on = try await runArm(semanticSweepMarkEnabled: true)

        #expect(off.map(fingerprint) == on.filter {
            $0.detectorVersion != SemanticSweepMarkComposer.detectorVersion
        }.map(fingerprint))
    }

    /// A second backfill over unchanged inputs must not churn: content-addressed
    /// ids mean the version-scoped reconcile retires nothing and the row keeps
    /// its identity. Without this a listener would meet the same banner twice.
    @Test("a second backfill mints the same sweep row id")
    func recomposeIsIdempotentEndToEnd() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: Self.assetId))
        try await store.insertSemanticScanResult(makeScanRow())
        let service = makeService(
            store: store,
            semanticSweepMarkEnabled: true,
            fmBackfillMode: .proposalOnly
        )
        let chunks = makeChunks(assetId: Self.assetId)

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: Self.assetId,
            podcastId: Self.podcastId,
            episodeDuration: Self.episodeDuration
        )
        let first = sweepRows(try await store.fetchAdWindows(assetId: Self.assetId)).map(\.id)
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: Self.assetId,
            podcastId: Self.podcastId,
            episodeDuration: Self.episodeDuration
        )
        let second = sweepRows(try await store.fetchAdWindows(assetId: Self.assetId)).map(\.id)

        #expect(first.count == 1, "control: the first run composed a row")
        #expect(first == second)
    }
}

// MARK: - The runner-tail site

/// playhead-y3ya: the TIMELY half of the wire. Step 18c only composes when a
/// backfill runs; on the field episode the FM sweep spent fifteen hours writing
/// rows in background jobs while nothing re-fused. `BackfillJobRunner.runJob`
/// therefore composes from the asset's persisted rows before it returns, so a
/// verdict becomes a durable mark within one job cycle of the model producing
/// it.
///
/// The fixture pre-inserts the verdict rather than driving the fake model to
/// produce it, and that is deliberate: the tail reads PERSISTED rows, so a row
/// written by an earlier job — the field shape, where the job that wrote the
/// verdict later ended `cancelled` — is exactly the input under test.
@Suite("Semantic-sweep mark compose at the runner tail (playhead-y3ya)")
struct SemanticSweepRunnerTailTests {

    private static let assetId = "asset-sweep-runner"
    private static let transcriptVersion = "tx-sweep-runner-v1"

    /// A verdict inside the transcript's span with no `ad_window` near it —
    /// the state in which the sweep lane's output was discarded.
    private static let verdict = (start: 30.0, end: 60.0)

    private func makeAsset() -> AnalysisAsset {
        AnalysisAsset(
            id: Self.assetId,
            episodeId: "ep-\(Self.assetId)",
            assetFingerprint: "fp-\(Self.assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(Self.assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeInputs() -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: Self.assetId,
            transcriptVersion: Self.transcriptVersion,
            lines: [
                (0, 30, "Welcome to the show. Today we're discussing podcasts."),
                (30, 60, "Use code SHOW for 20 percent off at example dot com."),
                (60, 90, "Now back to the interview with our guest.")
            ]
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: Self.assetId,
            podcastId: "podcast-sweep-runner",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: Self.assetId,
                transcriptVersion: Self.transcriptVersion
            ),
            transcriptVersion: Self.transcriptVersion,
            plannerContext: CoveragePlannerContext(
                observedEpisodeCount: 0,
                stableRecall: false,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: 0,
                periodicFullRescanIntervalEpisodes: 10
            )
        )
    }

    /// A verdict an EARLIER job persisted, keyed to this asset and transcript.
    private func makePriorVerdictRow() -> SemanticScanResult {
        SemanticScanResult(
            id: "scan-runner-tail-1",
            analysisAssetId: Self.assetId,
            windowFirstAtomOrdinal: 1,
            windowLastAtomOrdinal: 1,
            windowStartTime: Self.verdict.start,
            windowEndTime: Self.verdict.end,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeTestScanCohortJSON(),
            transcriptVersion: Self.transcriptVersion
        )
    }

    private func runArm(
        semanticSweepMarkEnabled: Bool,
        mode: FMBackfillMode = .full
    ) async throws -> [AdWindow] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(makePriorVerdictRow())
        let fmRuntime = TestFMRuntime()
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: mode,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            semanticSweepMarkEnabled: semanticSweepMarkEnabled
        )

        _ = try await runner.runPendingBackfill(for: makeInputs())

        return try await store.fetchAdWindows(assetId: Self.assetId)
            .filter { $0.detectorVersion == SemanticSweepMarkComposer.detectorVersion }
    }

    /// THE TIMELY WIRE. Deleting the runner-tail compose leaves Step 18c green
    /// and the field episode silent until something re-fuses.
    @Test("flag ON: a job run composes the persisted verdict into a mark")
    func theRunnerTailComposes() async throws {
        let rows = try await runArm(semanticSweepMarkEnabled: true)

        #expect(rows.count == 1)
        #expect(rows.first?.startTime == Self.verdict.start)
        #expect(rows.first?.endTime == Self.verdict.end)
        #expect(rows.first?.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
    }

    /// The rollback, and the control for the test above: a green
    /// `theRunnerTailComposes` with no OFF arm could be satisfied by Step 18c
    /// running somewhere else in the same process.
    @Test("flag OFF: a job run composes nothing")
    func theRunnerTailIsInertWhenOff() async throws {
        let rows = try await runArm(semanticSweepMarkEnabled: false)

        #expect(rows.isEmpty)
    }

    /// THE SHADOW CONTRACT at the runner site — the one this file's neighbour
    /// `BackfillJobRunnerTests` states outright ("shadow mode never inserts
    /// AdWindows"). It held only accidentally before this gate: `TestFMRuntime`
    /// defaults to `.noAds`, so no fixture there produced a verdict to compose.
    @Test("shadow mode composes nothing at the runner tail")
    func shadowModeComposesNothingAtTheRunnerTail() async throws {
        let rows = try await runArm(semanticSweepMarkEnabled: true, mode: .shadow)

        #expect(rows.isEmpty)
    }
}
