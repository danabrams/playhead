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

// MARK: - playhead-kbqw

/// playhead-kbqw: what a CANCELLED coarse attempt cost, which playhead-ejr7
/// left unanswerable.
///
/// ejr7 removed `?? coarse.latencyMillis` and the `.cancelled` rows went
/// honestly NULL. That is strictly better than the pass total it replaced, and
/// it means the question the P0 rested on — "what does a cancelled call
/// actually cost?" — had no answer from a device pull at all. The largest
/// per-call cost the 2026-08-10/11 pull can vouch for is **74.1 s, and that is
/// a success.**
///
/// Five `CoarseWindowFailure` sites passed no `latencyMillis`. **Two of them
/// could measure themselves and now do; three still cannot, and their NULL is
/// the measurement rather than a gap in it.** The split is not a matter of
/// effort — it is whether an attempt was ISSUED for the range the row covers:
///
/// | site | what it stands for | cost |
/// |---|---|---|
/// | 1 | line refs that resolved to no segment | NULL — no prompt was built |
/// | 2 | recovery declined before its first call | NULL — nothing was issued |
/// | 3 | whole-window permissive attempt, cancelled | **MEASURED** |
/// | 4 | a half of a split the loop never reached | NULL — never issued |
/// | 5 | one half's permissive attempt, cancelled | **MEASURED** |
///
/// THE SPAN, because a duration that does not name its span is how ejr7 became
/// a P0: **from the instant the permissive attempt was ISSUED to the instant
/// `CancellationError` was OBSERVED.** Not how long the model took — it never
/// answered. Not how long cancellation took to propagate through the pass. It
/// is what the attempt occupied before it was cut short, which is why an
/// attempt started on an already-dead grant honestly reads ~0 ms and one killed
/// at grant end reads the work it burned.
///
/// SITES 4 AND 5 SHARE A STATUS AND MUST NOT SHARE A NUMBER, which is why
/// `cancelledSplitDistinguishesTheHalfThatRanFromTheHalfThatDidNot` is the
/// load-bearing test here rather than a completeness exercise: both rows
/// persist `status='cancelled'`, one carries a measurement and one carries
/// NULL, and a SQL reader that treats "cancelled" as one population gets the
/// wrong answer in both directions.
@Suite("playhead-kbqw: a cancelled coarse attempt records what it spent")
struct CancelledCoarseAttemptCostTests {

    /// Wall-clock the injected permissive attempt burns before it throws.
    ///
    /// A LOWER bound is the only kind of timing assertion that is safe in this
    /// repo's parallel gate: the box's load can only make an elapsed span
    /// longer, never shorter. `Thread.sleep` guarantees at least this interval,
    /// so `>= assertedFloorMs` holds under any load — and without it the tests
    /// could not tell a real measurement from a mutant that writes the literal
    /// `0`, which is the failure mode ejr7 spent a P0 on.
    private static let injectedSpendSeconds = 0.03
    private static let assertedFloorMs = 20.0

    private func makeSegments(count: Int) -> [AdTranscriptSegment] {
        makeFMSegments(
            analysisAssetId: "asset-kbqw",
            transcriptVersion: "tx-kbqw-v1",
            lines: (0..<count).map { idx in
                (
                    Double(idx) * 100,
                    Double(idx + 1) * 100,
                    "Segment \(idx) mentions a sponsor."
                )
            }
        )
    }

    /// One coarse window over every segment, refused on the standard path — the
    /// safety block that sends the window into qbib permissive recovery.
    private func makeSafetyBlockedRuntime() -> TestFMRuntime {
        TestFMRuntime(coarseFailures: [.refusal], contextSize: 4_096)
    }

    // MARK: - SITE 3: the whole-window recovery attempt

    /// The population the bead was filed about. A cancelled whole-window
    /// permissive attempt must carry ITS OWN span — not NULL, and not a number
    /// borrowed from anywhere else.
    ///
    /// **This is the mutation rail for site 3.** Delete the three arguments
    /// from that `CoarseWindowFailure` and this fails on `latencyMillis`;
    /// restore any borrowed number (the pass total, the whole recovery, a
    /// sibling attempt) and the floor assertion still passes but the ejr7 suite
    /// is what catches it. The floor is what kills a literal `0`.
    @available(iOS 26.0, *)
    @Test("a cancelled whole-window recovery attempt carries its own span")
    func cancelledWholeWindowAttemptCarriesItsOwnSpan() async throws {
        let segments = makeSegments(count: 4)
        let runtime = makeSafetyBlockedRuntime()
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)
        let permissive = PermissiveAdClassifier()
        await permissive.installClassifyOverrideForTesting { _ in
            Thread.sleep(forTimeInterval: Self.injectedSpendSeconds)
            throw CancellationError()
        }

        let output = try await classifier.coarsePassA(
            segments: segments,
            sensitiveRouter: SensitiveWindowRouter.noop,
            permissiveClassifier: permissive
        )

        #expect(
            await runtime.coarseCallCount >= 1,
            "vacuity: the standard path never ran, so nothing was safety-blocked and recovery was never entered"
        )
        let hole = try #require(
            output.failedWindows.first { $0.status == .cancelled },
            "vacuity: no cancelled failure row was produced, so nothing below is asserted"
        )
        let measured = try #require(
            hole.latencyMillis,
            """
            a cancelled recovery attempt persisted NULL. Nothing was JUDGED, but \
            something was SPENT, and NULL is what made "what does a cancelled call \
            cost?" unanswerable from a device pull after playhead-ejr7.
            """
        )
        #expect(
            measured >= Self.assertedFloorMs,
            """
            \(measured) ms for an attempt that provably burned \
            \(Self.injectedSpendSeconds * 1_000) ms — a span that cannot see the work \
            is not a measurement of it.
            """
        )
        // The twins come off the SAME `FMClockPair` and the SAME instant, so
        // all three are present or none is. One present and one nil is a
        // wiring break — the rule playhead-rkfp set and ejr7 pinned.
        let suspending = try #require(hole.suspendingLatencyMillis)
        #expect(hole.daemonPeersAtStart != nil)
        // Same span, two clocks: the suspending reading excludes device sleep,
        // so it can never meaningfully exceed the continuous one. A crossed
        // wire — two readings of DIFFERENT spans — is what this catches.
        #expect(suspending <= measured + 1.0)
    }

    // MARK: - SITES 5 and 4: the split, and the half that never ran

    /// The load-bearing test. Both halves persist `status == .cancelled`; only
    /// the one that was ATTEMPTED may carry a number.
    ///
    /// The whole window stays blocked, so recovery splits it. The first half is
    /// attempted and cancelled mid-flight (site 5). The second half is never
    /// reached, because the split stops on a cancellation (site 4) — and a row
    /// for a call that was never issued must say so with NULL.
    ///
    /// **This is the mutation rail for both sites, in both directions.** Drop
    /// site 5's arguments and the measured half goes NULL; give site 4 any
    /// number at all — including one taken from its cancelled sibling, which is
    /// the most tempting wrong answer — and the never-attempted half stops
    /// being distinguishable from the one that burned a deadline.
    @available(iOS 26.0, *)
    @Test("a cancelled split records the half that ran and NOT the half that never did")
    func cancelledSplitDistinguishesTheHalfThatRanFromTheHalfThatDidNot() async throws {
        let segments = makeSegments(count: 4)
        let runtime = makeSafetyBlockedRuntime()
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)
        let permissive = PermissiveAdClassifier()
        await permissive.installClassifyOverrideForTesting { windowSegments in
            // The whole window stays blocked, which is what forces the split.
            guard windowSegments.count < 4 else {
                throw PermissiveClassificationError.failed(
                    reason: .permissiveRefusal,
                    underlyingDescription: "kbqw-whole-window-still-blocked"
                )
            }
            // The FIRST half is attempted, burns time, and is then cancelled.
            Thread.sleep(forTimeInterval: Self.injectedSpendSeconds)
            throw CancellationError()
        }

        let output = try await classifier.coarsePassA(
            segments: segments,
            sensitiveRouter: SensitiveWindowRouter.noop,
            permissiveClassifier: permissive
        )

        let cancelled = output.failedWindows.filter { $0.status == .cancelled }
        #expect(
            cancelled.count == 2,
            "vacuity: the split did not produce both an attempted and an unattempted cancelled half"
        )
        let attemptedHalf = try #require(cancelled.first { $0.lineRefs == [0, 1] })
        let neverAttemptedHalf = try #require(cancelled.first { $0.lineRefs == [2, 3] })

        let measured = try #require(
            attemptedHalf.latencyMillis,
            "the half that was attempted and cancelled must carry the span it burned"
        )
        #expect(measured >= Self.assertedFloorMs)
        #expect(attemptedHalf.suspendingLatencyMillis != nil)
        #expect(attemptedHalf.daemonPeersAtStart != nil)

        #expect(
            neverAttemptedHalf.latencyMillis == nil,
            """
            the half the split never reached claims \
            \(neverAttemptedHalf.latencyMillis ?? -1) ms of cost. No call was issued \
            for it, so any number here can only have come from a different span — \
            most plausibly its cancelled sibling's, which is the exact substitution \
            playhead-ejr7 removed one layer up.
            """
        )
        #expect(neverAttemptedHalf.suspendingLatencyMillis == nil)
        #expect(neverAttemptedHalf.daemonPeersAtStart == nil)
    }

    // MARK: - The persisted column, which is what a device pull reads

    /// End to end: the bead's own check query must return rows.
    ///
    /// Everything above is in-memory. The question this bead exists to make
    /// answerable is asked in SQL against `semantic_scan_results`, so the rail
    /// that matters is that a cancelled recovery attempt's span survives
    /// `BackfillJobRunner` and lands in `latencyMs`. Asserted here in exactly
    /// the shape the pull will ask it:
    ///
    ///     SELECT ... WHERE status='cancelled' AND latencyMs IS NOT NULL
    @available(iOS 26.0, *)
    @Test("a cancelled attempt's span reaches the persisted latencyMs column")
    func cancelledAttemptSpanIsPersisted() async throws {
        let assetId = "asset-kbqw-persisted"
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: assetId,
                episodeId: "ep-\(assetId)",
                assetFingerprint: "fp-\(assetId)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(assetId).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: nil
            )
        )

        let transcriptVersion = "tx-kbqw-v1"
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<4).map { idx in
                (
                    Double(idx) * 30,
                    Double(idx + 1) * 30,
                    "Editorial line \(idx) mentions a sponsor."
                )
            }
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-kbqw",
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

        let permissive = PermissiveAdClassifier()
        await permissive.installClassifyOverrideForTesting { _ in
            Thread.sleep(forTimeInterval: Self.injectedSpendSeconds)
            throw CancellationError()
        }
        let runtime = TestFMRuntime(coarseFailures: [.refusal], contextSize: 4_096)
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            // BOTH are required: `runJob` selects the recovery-capable
            // `coarsePassA` overload only when the router AND the box are
            // present. `.noop` has no rules, so no window routes `.sensitive`
            // and the permissive classifier is reached ONLY through qbib
            // safety recovery — which is the path under test.
            sensitiveRouter: SensitiveWindowRouter.noop,
            permissiveClassifier: BackfillJobRunner.PermissiveClassifierBox { permissive }
        )

        _ = try? await runner.runPendingBackfill(for: inputs)

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
            .filter { $0.scanPass == "passA" }
        let cancelledRows = rows.filter { $0.status == .cancelled }
        #expect(
            !cancelledRows.isEmpty,
            "vacuity: the run persisted no cancelled row, so the query below asks about nothing"
        )
        // The bead's check, in the shape the device pull asks it.
        let measuredRows = cancelledRows.filter { $0.latencyMs != nil }
        #expect(
            !measuredRows.isEmpty,
            """
            every persisted cancelled row is still NULL. This query returning zero rows \
            is exactly the state playhead-ejr7 left behind and this bead exists to end.
            """
        )
        for row in measuredRows {
            let latency = try #require(row.latencyMs)
            #expect(
                latency >= Self.assertedFloorMs,
                "\(latency) ms cannot cover an attempt that provably burned \(Self.injectedSpendSeconds * 1_000) ms"
            )
            // playhead-rkfp / playhead-ezmv: the triple travels together or the
            // wiring is broken. This is the persisted mirror of the in-memory
            // assertion above.
            #expect(row.suspendingLatencyMs != nil)
            #expect(row.daemonPeersAtStart != nil)
        }
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
