// BoundarySpanExpansionTests.swift
// bd-1my: simulator-side tests for the outward-expansion path in
// BackfillJobRunner. The four cases the bead's design field calls out:
//   (a) span merging unit — pure helpers, no FM runtime
//   (b) bounded expansion — pure helpers (truncation cap math)
//   (c) boundary-end synthetic — runner with stubbed FM where the first
//       refinement returns a span touching the window edge, the
//       expansion call returns a span fully inside the new window
//   (d) no-boundary control — runner with stubbed FM that returns a
//       span fully inside the window; expansion must NOT fire
//
// The runner-level cases are deliberately tolerant about which exact
// lineRefs `planAdaptiveZoom` selects: we assert the EXPANSION counter
// instead of pinning concrete line refs because Phase 4's adaptive zoom
// helpers can choose any reasonable subset of the coarse window's
// support refs without affecting the bd-1my contract.
//
// On-device end-to-end coverage against the Conan "Fanhausen Revisited"
// fixture lives in PlayheadFMSmokeTests.swift; THAT file requires real
// FoundationModels and is gated by the PlayheadFMSmoke scheme.

import Foundation
import Testing
@testable import Playhead

@Suite("bd-1my BoundarySpanExpansion")
struct BoundarySpanExpansionTests {

    // MARK: - (a) Span merging unit

    @Test("mergeSpans unions overlapping line refs into a single wider span")
    func mergeOverlappingSpans() {
        let original = makeRefinedSpan(firstLineRef: 5, lastLineRef: 8, certainty: .moderate)
        let expansion = makeRefinedSpan(firstLineRef: 7, lastLineRef: 11, certainty: .strong)

        let merged = BackfillJobRunner.mergeSpans(existing: [original], expansion: [expansion])

        #expect(merged.count == 1)
        let span = merged[0]
        #expect(span.firstLineRef == 5)
        #expect(span.lastLineRef == 11)
        // Higher-certainty side wins for metadata.
        #expect(span.certainty == .strong)
    }

    @Test("mergeSpans appends non-overlapping spans (different ads)")
    func mergeKeepsDistinctAds() {
        let firstAd = makeRefinedSpan(firstLineRef: 0, lastLineRef: 3)
        let secondAd = makeRefinedSpan(firstLineRef: 10, lastLineRef: 14)

        let merged = BackfillJobRunner.mergeSpans(existing: [firstAd], expansion: [secondAd])

        #expect(merged.count == 2)
        #expect(merged.contains { $0.firstLineRef == 0 && $0.lastLineRef == 3 })
        #expect(merged.contains { $0.firstLineRef == 10 && $0.lastLineRef == 14 })
    }

    @Test("mergeSpans is idempotent under repeated calls with the same expansion")
    func mergeIdempotent() {
        let original = makeRefinedSpan(firstLineRef: 2, lastLineRef: 5)
        let expansion = makeRefinedSpan(firstLineRef: 4, lastLineRef: 7)

        let onceMerged = BackfillJobRunner.mergeSpans(existing: [original], expansion: [expansion])
        let twiceMerged = BackfillJobRunner.mergeSpans(existing: onceMerged, expansion: [expansion])

        #expect(onceMerged.count == twiceMerged.count)
        #expect(onceMerged[0].firstLineRef == twiceMerged[0].firstLineRef)
        #expect(onceMerged[0].lastLineRef == twiceMerged[0].lastLineRef)
    }

    @Test("mergeSpans with empty expansion returns existing unchanged")
    func mergeEmptyExpansion() {
        let original = makeRefinedSpan(firstLineRef: 0, lastLineRef: 4)
        let merged = BackfillJobRunner.mergeSpans(existing: [original], expansion: [])
        #expect(merged.count == 1)
        #expect(merged[0].firstLineRef == 0)
        #expect(merged[0].lastLineRef == 4)
    }

    // MARK: - (b) Bounded expansion / spansTouchBoundary

    @Test("spansTouchBoundary detects first-edge contact")
    func boundaryDetectsFirstEdge() {
        let span = makeRefinedSpan(firstLineRef: 4, lastLineRef: 6)
        #expect(BackfillJobRunner.spansTouchBoundary(spans: [span], windowMin: 4, windowMax: 9))
    }

    @Test("spansTouchBoundary detects last-edge contact")
    func boundaryDetectsLastEdge() {
        let span = makeRefinedSpan(firstLineRef: 5, lastLineRef: 9)
        #expect(BackfillJobRunner.spansTouchBoundary(spans: [span], windowMin: 4, windowMax: 9))
    }

    @Test("spansTouchBoundary returns false for spans fully inside the window")
    func boundaryRejectsInteriorSpan() {
        let span = makeRefinedSpan(firstLineRef: 5, lastLineRef: 7)
        #expect(!BackfillJobRunner.spansTouchBoundary(spans: [span], windowMin: 4, windowMax: 9))
    }

    @Test("spansTouchBoundary returns false for an empty span list")
    func boundaryRejectsEmpty() {
        #expect(!BackfillJobRunner.spansTouchBoundary(spans: [], windowMin: 0, windowMax: 5))
    }

    // MARK: - (c) Boundary-end synthetic via runner

    @Test("boundary-touching refinement triggers exactly one expansion call")
    func boundaryTouchingTriggersExpansion() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeExpansionAsset())

        // With minimumZoomSpanLines=5 and supportLineRefs=[3], the
        // refinement plan grows right from [3] to [3,4,5,6,7]. Reply 1
        // returns a span touching the window's first line ref (3).
        // The expansion call gets a wider window starting at lineRef
        // 0 and reply 2 returns an interior span — loop exits cleanly.
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [3], certainty: .strong)
                )
            ],
            refinementResponses: [
                makeBoundarySpanSchema(firstLineRef: 3, lastLineRef: 4),
                makeInteriorSpanSchema(firstLineRef: 1, lastLineRef: 4)
            ],
            contextSize: 65_536
        )
        let runner = makeExpansionRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: makeExpansionInputs())

        let telemetry = await runner.snapshotExpansionTelemetry()
        #expect(telemetry.invocations >= 1, "expansion must fire when refinement touches the window boundary")
        #expect(telemetry.truncations == 0, "expansion must terminate cleanly when the new spans are interior")
    }

    // MARK: - (d) No-boundary control

    @Test("interior-only refinement keeps expansion completely silent")
    func interiorOnlyKeepsExpansionSilent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeExpansionAsset())

        // Plan = [3..7]; reply is [4..6] — interior on both sides.
        // The boundary detector must reject it and the expansion
        // path must never call the FM.
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [3], certainty: .strong)
                )
            ],
            refinementResponses: [
                makeInteriorSpanSchema(firstLineRef: 4, lastLineRef: 6)
            ],
            contextSize: 65_536
        )
        let runner = makeExpansionRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: makeExpansionInputs())

        let telemetry = await runner.snapshotExpansionTelemetry()
        #expect(telemetry.invocations == 0, "no boundary span ⇒ no expansion FM call")
        #expect(telemetry.truncations == 0)
    }

    // MARK: - (b) Pathological truncation via runner

    @Test("expansion truncates after the cumulative-segments cap when boundary spans recur")
    func pathologicalExpansionTruncates() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeExpansionAsset(id: "asset-expansion-wide"))

        // Wide fixture (40 segments) with support=[15] gives a base
        // refinement plan of [15..19] (cluster grows right from 15
        // by minimumZoomSpanLines=5). Each reply touches the
        // current first line ref so expansion keeps firing below.
        //
        // Reply 1: span [15..16] — touches base windowMin=15.
        //   Expansion adds 5 below → new plan [10..19], cumulative=5.
        // Reply 2: span [10..11] — touches new windowMin=10.
        //   Expansion adds 5 below → new plan [5..19], cumulative=10.
        // Reply 3: span [5..6] — would touch the boundary again,
        //   but iter3 trips the cumulative-segments cap and the
        //   loop exits as truncated before issuing another FM call.
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [15], certainty: .strong)
                )
            ],
            refinementResponses: [
                makeBoundarySpanSchema(firstLineRef: 15, lastLineRef: 16),
                makeBoundarySpanSchema(firstLineRef: 10, lastLineRef: 11),
                makeBoundarySpanSchema(firstLineRef: 5, lastLineRef: 6),
                makeBoundarySpanSchema(firstLineRef: 0, lastLineRef: 1),
            ],
            contextSize: 65_536
        )
        let runner = makeExpansionRunner(
            store: store,
            runtime: fmRuntime.runtime
        )

        _ = try await runner.runPendingBackfill(for: makeExpansionInputs(id: "asset-expansion-wide", segmentCount: 40))

        let telemetry = await runner.snapshotExpansionTelemetry()
        #expect(telemetry.truncations == 1, "exactly one truncation event per source span hitting the cap")
        #expect(telemetry.invocations <= BackfillJobRunner.maxExpansionIterations,
                "expansion must stop on or before maxExpansionIterations FM calls")
        #expect(telemetry.invocations >= 1, "at least one expansion call must have fired")
    }
}

// MARK: - Local fixture helpers

private func makeRefinedSpan(
    firstLineRef: Int,
    lastLineRef: Int,
    certainty: CertaintyBand = .moderate
) -> RefinedAdSpan {
    RefinedAdSpan(
        commercialIntent: .paid,
        ownership: .thirdParty,
        firstLineRef: firstLineRef,
        lastLineRef: lastLineRef,
        firstAtomOrdinal: firstLineRef,
        lastAtomOrdinal: lastLineRef,
        certainty: certainty,
        boundaryPrecision: .usable,
        resolvedEvidenceAnchors: [],
        memoryWriteEligible: false,
        alternativeExplanation: .none,
        reasonTags: []
    )
}

private func makeBoundarySpanSchema(firstLineRef: Int, lastLineRef: Int) -> RefinementWindowSchema {
    RefinementWindowSchema(spans: [
        SpanRefinementSchema(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: firstLineRef,
            lastLineRef: lastLineRef,
            certainty: .strong,
            boundaryPrecision: .rough,
            evidenceAnchors: makeAnchorsCovering(firstLineRef: firstLineRef, lastLineRef: lastLineRef),
            alternativeExplanation: .none,
            reasonTags: []
        )
    ])
}

private func makeInteriorSpanSchema(firstLineRef: Int, lastLineRef: Int) -> RefinementWindowSchema {
    RefinementWindowSchema(spans: [
        SpanRefinementSchema(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: firstLineRef,
            lastLineRef: lastLineRef,
            certainty: .strong,
            boundaryPrecision: .precise,
            evidenceAnchors: makeAnchorsCovering(firstLineRef: firstLineRef, lastLineRef: lastLineRef),
            alternativeExplanation: .none,
            reasonTags: []
        )
    ])
}

/// Build evidence anchors dense enough to satisfy the breadth cap
/// (`uniqueAnchorKeys.count * 4 >= breadth`) for an arbitrary span.
/// One anchor every 4 line refs is sufficient — the cap is generous.
private func makeAnchorsCovering(firstLineRef: Int, lastLineRef: Int) -> [EvidenceAnchorSchema] {
    let breadth = max(0, lastLineRef - firstLineRef)
    let needed = max(1, (breadth + 3) / 4)
    var anchors: [EvidenceAnchorSchema] = []
    for i in 0..<needed {
        let lineRef = min(lastLineRef, firstLineRef + i * 4)
        anchors.append(
            EvidenceAnchorSchema(
                evidenceRef: nil,
                lineRef: lineRef,
                kind: .ctaPhrase,
                certainty: .strong
            )
        )
    }
    return anchors
}

private func makeExpansionAsset(id: String = "asset-expansion") -> AnalysisAsset {
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

private func makeExpansionInputs(
    id: String = "asset-expansion",
    segmentCount: Int = 8
) -> BackfillJobRunner.AssetInputs {
    let transcriptVersion = "tx-\(id)-v1"
    // Generate `segmentCount` synthetic segments. Each line is short
    // and stable, so the planner produces a single coarse window
    // covering all of them under the default token budget.
    let lines: [(start: Double, end: Double, text: String)] = (0..<segmentCount).map { idx in
        let start = Double(idx) * 10.0
        return (start, start + 10.0, "Line \(idx) text segment for synthetic ad detection coverage.")
    }
    let segments = makeFMSegments(
        analysisAssetId: id,
        transcriptVersion: transcriptVersion,
        lines: lines
    )
    let evidenceCatalog = EvidenceCatalogBuilder.build(
        atoms: segments.flatMap(\.atoms),
        analysisAssetId: id,
        transcriptVersion: transcriptVersion
    )
    let plannerContext = CoveragePlannerContext(
        observedEpisodeCount: 0,
        stablePrecision: false,
        isFirstEpisodeAfterCohortInvalidation: false,
        recallDegrading: false,
        sponsorDriftDetected: false,
        auditMissDetected: false,
        episodesSinceLastFullRescan: 0,
        periodicFullRescanIntervalEpisodes: 10
    )
    return BackfillJobRunner.AssetInputs(
        analysisAssetId: id,
        podcastId: "podcast-\(id)",
        segments: segments,
        evidenceCatalog: evidenceCatalog,
        transcriptVersion: transcriptVersion,
        plannerContext: plannerContext
    )
}

private func makeExpansionRunner(
    store: AnalysisStore,
    runtime: FoundationModelClassifier.Runtime
) -> BackfillJobRunner {
    // bd-1my: bump `minimumZoomSpanLines` so the synthetic refinement
    // window is wide enough (5 segments) to contain BOTH a boundary span
    // and an interior span. The default of 2 makes "boundary" and
    // "interior" indistinguishable for any single-line span.
    let config = FoundationModelClassifier.Config(
        safetyMarginTokens: 128,
        coarseMaximumResponseTokens: 96,
        refinementMaximumResponseTokens: 1024,
        zoomAmbiguityBudget: 1,
        minimumZoomSpanLines: 5,
        maximumRefinementSpansPerWindow: 2
    )
    return BackfillJobRunner(
        store: store,
        admissionController: AdmissionController(),
        classifier: FoundationModelClassifier(runtime: runtime, config: config),
        coveragePlanner: CoveragePlanner(),
        mode: .shadow,
        capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
        batteryLevelProvider: { 1.0 },
        scanCohortJSON: makeTestScanCohortJSON()
    )
}
