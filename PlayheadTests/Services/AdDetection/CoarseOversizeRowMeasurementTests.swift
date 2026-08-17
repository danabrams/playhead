// playhead-hzpa: the measurement reaches the DATABASE, not just the struct.
//
// THE FIELD ROW. On the 2026-08-16 device pull (sha256 b3ec720d…), asset
// AA6CD430's head window `[0.0, 42.9]` is `exceededContextWindow`, and 43.3 s
// of a pre-roll Dan confirmed twice — `falseNegative/bannerSuggestionConfirmed`
// over `[0.00, 60.32]` and `[0.00, 54.18]` — was never read by any scan. Only
// 28.3 % / 20.2 % of those spans overlap a window that produced a verdict, and
// the first EXAMINED passA window on the asset starts at 43.26 s.
//
// The row could not say why. Measured over the same bytes:
//   * `inputTokenCount` is NULL on all 961 `semantic_scan_results` rows;
//   * `errorContext` is non-null on exactly 25, every one of them a
//     `noWork:emptySegments` sentinel — never a real failure.
//
// So the ONE status whose entire meaning is a comparison of two numbers
// persisted neither of them, and the numbers were not missing: they were
// logged, at the moment of abandonment, into an os_log that is long gone by
// the time anyone pulls the file.
//
// WHY THIS SUITE EXISTS SEPARATELY FROM
// `CoarseOversizeAbandonmentMeasurementTests`. That suite asserts on the
// in-memory `CoarseWindowFailure`. Passing it proves the classifier BUILT the
// measurement — it proves nothing about whether the measurement survives
// `makeCoarseFailureScanResult` -> `makeFailureScanResult` -> the row, which
// is exactly the seam that was hardcoding `errorContext: nil,
// inputTokenCount: nil`. A suite named "carries its measurement" that only
// ever looked at the struct would pass for a reason other than the one its
// name claims. This one reads the columns back out of the store.
//
// Structure and the starved-`contextSize` technique are borrowed from
// `CoarseFailureRowLatencyTests` (playhead-ejr7), which drives the same
// over-budget arm to prove the twin `latencyMs` rule.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-hzpa: an oversize row carries the size it was rejected for")
struct CoarseOversizeRowMeasurementTests {

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
        let transcriptVersion = "tx-hzpa-v1"
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
            podcastId: "podcast-hzpa",
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

    /// Parse `oversize:<case> budget=<n>` back out of the column, so the test
    /// reads the DURABLE STRING rather than an in-memory value that happens to
    /// agree with it.
    private func parseOversize(_ context: String?) -> (cause: String, budget: Int)? {
        guard let context, context.hasPrefix("oversize:") else { return nil }
        let parts = context.split(separator: " ")
        guard let head = parts.first else { return nil }
        let cause = String(head.dropFirst("oversize:".count))
        guard let budgetPart = parts.first(where: { $0.hasPrefix("budget=") }),
              let budget = Int(budgetPart.dropFirst("budget=".count)) else { return nil }
        return (cause, budget)
    }

    // MARK: - 1. The direction the field row was missing

    @Test("an over-budget abandonment persists its token count, its budget and its cause")
    func oversizeRowPersistsItsMeasurement() async throws {
        let assetId = "asset-hzpa-oversize"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        // Starve the context so every planned window is over budget — the same
        // arm the field row came from, and the reason no FM call is made.
        let fmRuntime = TestFMRuntime(contextSize: 64, coarseSchemaTokenCount: 8)

        _ = try? await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: makeInputs(assetId: assetId, lineCount: 6))

        let rows = try await passARows(store, assetId: assetId)
        let oversize = rows.filter { $0.status == .exceededContextWindow }
        #expect(
            !oversize.isEmpty,
            "vacuity: the starved budget produced no exceededContextWindow row, so nothing below is asserted"
        )
        #expect(
            await fmRuntime.coarseCallCount == 0,
            "vacuity: an over-budget window is rejected pre-flight; a run that called the model is a different population"
        )

        let validCauses = Set(CoarseOversizeAbandonment.allCases.map(\.rawValue))
        for row in oversize {
            let tokens = try #require(
                row.inputTokenCount,
                """
                row \(row.id) says exceededContextWindow — literally "the prompt was \
                bigger than the budget" — and records no prompt size. That is the state \
                all 961 rows of the 2026-08-16 pull were in.
                """
            )
            let parsed = try #require(
                parseOversize(row.errorContext),
                "row \(row.id) errorContext=\(row.errorContext ?? "nil") does not name a cause and a budget"
            )
            #expect(
                validCauses.contains(parsed.cause),
                "row \(row.id) names cause '\(parsed.cause)', which is not a CoarseOversizeAbandonment case"
            )
            // THE INVARIANT THAT MAKES THE PAIR MEAN SOMETHING. If these two
            // numbers were ever swapped, or either taken from a different
            // question, the row would claim a prompt that FIT was rejected for
            // not fitting.
            #expect(
                tokens > parsed.budget,
                """
                row \(row.id) was abandoned as oversize with tokens=\(tokens) \
                budget=\(parsed.budget). A prompt at or under budget is not an \
                oversize, so one of these two numbers is measuring something else.
                """
            )
        }
    }

    // MARK: - 2. The mirror, so the fix cannot be "write a number always"

    @Test("a failure that made no size comparison still persists NULL in both columns")
    func nonOversizeRowPersistsNoMeasurement() async throws {
        let assetId = "asset-hzpa-guardrail"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        // Windows FIT — the model is reached and refuses. Nothing on this path
        // compares a prompt to a budget, so both columns must stay NULL. One
        // window answers first so the pass is mixed, exactly as the ejr7 twin
        // arranges it.
        let fmRuntime = TestFMRuntime(
            coarseFailures: [nil, .guardrailViolation],
            tokenCountRule: { $0.count }
        )

        _ = try? await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: makeInputs(assetId: assetId, lineCount: 40))

        let rows = try await passARows(store, assetId: assetId)
        let blocked = rows.filter { $0.status == .guardrailViolation }
        #expect(
            !blocked.isEmpty,
            "vacuity: no guardrailViolation row was produced, so nothing below is asserted"
        )
        #expect(
            rows.contains { $0.status == .success },
            "vacuity: a pass with no successful window is not the mixed pass this asserts over"
        )
        for row in blocked {
            #expect(
                row.inputTokenCount == nil,
                """
                row \(row.id) is a guardrail block and claims a prompt size of \
                \(row.inputTokenCount ?? -1). No size comparison ended this attempt, so \
                any number here came from a different question.
                """
            )
            #expect(
                row.errorContext == nil,
                "row \(row.id) spells a guardrail block as '\(row.errorContext ?? "nil")'"
            )
        }
    }
}
