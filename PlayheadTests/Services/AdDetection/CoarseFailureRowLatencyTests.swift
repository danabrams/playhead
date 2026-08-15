// CoarseFailureRowLatencyTests.swift
// playhead-ejr7: `semantic_scan_results.latencyMs` on a per-window FAILURE row
// is THAT ATTEMPT's own elapsed time, or NULL. It is never the pass's.
//
// THE FIELD CASE. On the 2026-08-10/11 virgin-DB overnight pull
// (`scratchpad/db-overnight5/analysis.sqlite`, 95 `semantic_scan_results` rows)
// thirteen rows produced no verdict and appeared to have cost 938.4 s — read as
// "41 % of all FM compute", which is why playhead-ejr7 was filed P0. The number
// is arithmetic over a column that meant two different things per row.
// `BackfillJobRunner` stamped `failure.latencyMillis ?? coarse.latencyMillis`,
// and `coarse.latencyMillis` is the WHOLE PASS's wall clock:
//
//   * `cancelled`, 613.9 s, 65.4 % of the figure — CERTAINLY not per-call cost.
//     The only two constructors of a `.cancelled` `CoarseWindowFailure` omit
//     `latencyMillis`, so four rows took the fallback and three took the
//     literal 0 of a never-attempted plan;
//   * `permissive_decoding_failure`, 296.9 s, 31.6 % — UNDETERMINED. Two of the
//     sites that can produce that status thread their own timing and three do
//     not, and no persisted column tells them apart;
//   * `refusal`, 27.5 s, 2.9 % — certainly per-call cost;
//   * at least 319.0 s of the 613.9 s is provably the same wall clock already
//     inside the 1,354.6 s "productive" figure: a cancelled row's number IS its
//     pass's elapsed time, so the pass spans exactly
//     `[createdAt - latencyMs, createdAt]`, and 26 same-asset success rows were
//     written inside those exact spans carrying their own costs;
//   * the largest cost that pull can vouch for on ONE call is 74.1 s.
//
// It also inverted the SHAPE of the finding. A `cancelled` row's number is the
// pass's elapsed-until-cancellation and a pass is cancelled at grant end, so a
// LARGE number means the pass started EARLY. The three biggest began 2.2-3.2 s
// into grants of 265-295 s — with essentially the entire window in hand.
//
// The twins in the very same call already had the right rule (playhead-rkfp:
// "a failure that did not measure itself gets NULL, not a number from a
// different span"). `latencyMs` was the last field still taking a number from a
// different span, and the fix is that the call site no longer has a parameter
// through which the pass total could reach the row at all.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-ejr7: a failure row's cost is its own attempt's, or nothing")
struct CoarseFailureRowLatencyTests {

    private static let segmentSeconds = 30.0

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
            capabilitySnapshot: nil,
            episodeDurationSec: nil
        )
    }

    private func makeInputs(
        assetId: String,
        lineCount: Int
    ) -> BackfillJobRunner.AssetInputs {
        let transcriptVersion = "tx-ejr7-v1"
        var lines: [(Double, Double, String)] = []
        for idx in 0..<lineCount {
            let start = Double(idx) * Self.segmentSeconds
            lines.append(
                (
                    start,
                    start + Self.segmentSeconds,
                    "Editorial line \(idx) about the topic of the day."
                )
            )
        }
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-ejr7",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion
            ),
            transcriptVersion: transcriptVersion,
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

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
    }

    private func passARows(
        _ store: AnalysisStore,
        assetId: String
    ) async throws -> [SemanticScanResult] {
        try await store.fetchSemanticScanResults(analysisAssetId: assetId)
            .filter { $0.scanPass == "passA" }
    }

    // MARK: - 1. The direction that killed the field number

    /// A window abandoned WITHOUT being timed must persist NULL.
    ///
    /// The abandonment used here is the over-budget arm of
    /// `coarsePassAUnbounded` (`plan.promptTokenCount > budget`), which builds
    /// its `CoarseWindowFailure` with `latencyMillis: nil` — the same nil the
    /// `.cancelled` and safety-blocked arms produce, and the population the
    /// field rows came from. Driven by starving `contextSize` so the prompt
    /// budget collapses; no FM call is made for an over-budget window, which is
    /// exactly why there is no timing to record.
    ///
    /// **This is the mutation rail.** Restore `?? coarse.latencyMillis` at the
    /// call site and every row below carries the pass's wall clock instead of
    /// NULL. Nothing else in the suite can see that: with a MEASURED failure
    /// the coalesce never fires, so a test that only ever produces one passes
    /// on both versions.
    @Test("an unmeasured failure persists NULL, never the pass's wall clock")
    func unmeasuredFailurePersistsNull() async throws {
        let assetId = "asset-ejr7-unmeasured"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(contextSize: 64, coarseSchemaTokenCount: 8)

        _ = try? await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: makeInputs(assetId: assetId, lineCount: 6))

        let rows = try await passARows(store, assetId: assetId)
        let unmeasured = rows.filter { $0.status != .success && $0.status != .noAds }
        #expect(
            !unmeasured.isEmpty,
            "vacuity: the starved budget produced no failure row, so nothing below is asserted"
        )
        // The premise, read out of the DATABASE rather than restated: these
        // windows never reached the model, which is why nobody timed them.
        #expect(
            await fmRuntime.coarseCallCount == 0,
            "vacuity: a window that DID call the model would have been timed, and the nil arm is not what is under test"
        )
        for row in unmeasured {
            #expect(
                row.latencyMs == nil,
                """
                \(row.status.rawValue) row \(row.id) claims \(row.latencyMs ?? -1) ms of cost \
                for an attempt nobody timed. That number can only have come from a different \
                span — the pass's — which is the 938.4 s the field pull mis-read as FM compute.
                """
            )
            // The twins already obeyed this rule; the point of the bead is that
            // all three fields now agree about the same unmeasured attempt.
            #expect(row.suspendingLatencyMs == nil)
            #expect(row.daemonPeersAtStart == nil)
        }
    }

    // MARK: - 2. The mirror: a measured failure keeps its own number

    /// The opposite direction, so the fix cannot be "write NULL always".
    ///
    /// A `.guardrailViolation` is a whole-window attempt that DID reach the
    /// model, so `coarsePassA` stamps it with its own `FMClockPair` reading. The
    /// persisted row must carry that reading, not NULL — a runner that simply
    /// stopped recording failure cost would pass test 1 and fail here.
    @Test("a failure that DID time itself keeps its own number")
    func measuredFailureKeepsItsOwnNumber() async throws {
        let assetId = "asset-ejr7-measured"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        // One window answers, the next violates the guardrail: a mixed pass, so
        // the failure row is not the only row and cannot be confused with a
        // whole-pass row.
        let fmRuntime = TestFMRuntime(
            coarseFailures: [nil, .guardrailViolation],
            tokenCountRule: { $0.count }
        )

        _ = try? await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: makeInputs(assetId: assetId, lineCount: 40))

        let rows = try await passARows(store, assetId: assetId)
        let violations = rows.filter { $0.status == .guardrailViolation }
        #expect(
            !violations.isEmpty,
            "vacuity: the injected guardrail violation produced no failure row"
        )
        #expect(
            rows.contains { $0.status == .success },
            "vacuity: a pass with no successful window cannot show that the failure row is not the pass"
        )
        for row in violations {
            let measured = try #require(
                row.latencyMs,
                "a failure that reached the model must persist the cost it measured — NULL here would delete the one failure class the pull could read"
            )
            #expect(measured >= 0)
            // Its twin comes off the same `FMClockPair`, so both are present or
            // neither is. A row with one and not the other is a wiring break.
            #expect(row.suspendingLatencyMs != nil)
            #expect(row.daemonPeersAtStart != nil)
        }
    }

    // MARK: - 3. The reader that depends on it

    /// `SemanticScanThroughputSplit` is the only production reader of this
    /// column, and it computes compute-per-audio-second. It filters to
    /// `.success` with a non-nil latency, so a NULL failure row must contribute
    /// nothing — the arrangement that makes NULL, rather than 0, the right
    /// value. A 0 would be eligible-looking to any future reader that dropped
    /// the status filter; NULL is not a number at all.
    @Test("an unmeasured failure row contributes nothing to the throughput split")
    func unmeasuredFailureContributesNothingToThroughput() {
        let unmeasured = SemanticScanThroughputSplitFixture.row(
            status: .cancelled,
            latencyMs: nil
        )
        let passTotalStamped = SemanticScanThroughputSplitFixture.row(
            status: .cancelled,
            latencyMs: 292_000
        )
        #expect(!SemanticScanThroughputSplit.isEligible(unmeasured))
        // And the row shape the OLD code produced is rejected too — by the
        // status filter, not by the latency one. Stated so nobody reads test 1
        // as "the reader was broken": it was not, which is precisely why the
        // 938.4 s was only ever wrong in an analyst's SQL and never in the app.
        #expect(!SemanticScanThroughputSplit.isEligible(passTotalStamped))
    }
}

/// Minimal `SemanticScanResult` fixture for the eligibility rule above.
private enum SemanticScanThroughputSplitFixture {
    static func row(status: SemanticScanStatus, latencyMs: Double?) -> SemanticScanResult {
        let latencyLabel: String = latencyMs == nil ? "nil" : "measured"
        return SemanticScanResult(
            id: "scan-ejr7-fixture-\(status.rawValue)-\(latencyLabel)",
            analysisAssetId: "asset-ejr7-fixture",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: 0,
            windowEndTime: 30,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: latencyMs,
            prewarmHit: false,
            scanCohortJSON: makeTestScanCohortJSON(),
            transcriptVersion: "tx-ejr7-v1"
        )
    }
}
