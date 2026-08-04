// BackfillStallBoundTests.swift
// playhead-qk44: a `fullEpisodeScan` that stops making progress must become a
// NAMED terminal, in the session that wedged it.
//
// THE FIELD CASE THESE TESTS ENCODE. On 2026-07-31, on a clean install, one
// episode's `backfill_jobs` row read `phase=fullEpisodeScan, status=running`
// for 23 minutes with `progressCursor` EMPTY, `retryCount 0`, an empty
// `work_journal`, zero `semantic_scan_results`, no error and no terminal —
// while `Documents/bg-task-log.jsonl` shows the app continuously `active`
// for 22 of those minutes.
//
// Three separate properties had to hold before that state could be reached,
// and each has a test here:
//
//   1. `coarsePassA` wrote NOTHING until it returned, so a pass in flight was
//      invisible in the database. The pass now reports each unit of work it
//      finishes, which is what gives both the in-process bound and the
//      `backfill_jobs` lease something real to measure.
//      **`zero semantic_scan_results` is no longer part of the signature**, as
//      of playhead-26od: the pass now checkpoints each screened window while it
//      runs, so a wedge that got past its first window leaves rows behind. That
//      does not weaken this bound — a wedge in the PRE-WINDOW stretch (probe,
//      tokenisation, `planPassA`), which is where the 2026-07-31 field case sat,
//      still produces no row to observe, because there is no screened window
//      yet. It does mean an operator triaging a future stall must read the LEASE
//      and the cursor, not the row count.
//   2. Nothing bounded a pass that produced no units at all. It does now, and
//      the outcome is a named terminal rather than silence.
//   3. Nothing swept a stranded row during a foregrounded session. The reaper
//      is now reachable from one, and its freshness predicate finally means
//      what it says.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-qk44: backfill stall bound")
struct BackfillStallBoundTests {

    // MARK: - Fixtures

    private func makeAsset(id: String = "asset-stall") -> AnalysisAsset {
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
        assetId: String = "asset-stall",
        lineCount: Int = 6
    ) -> BackfillJobRunner.AssetInputs {
        let transcriptVersion = "tx-stall-v1"
        var lines: [(Double, Double, String)] = []
        for idx in 0..<lineCount {
            let start = Double(idx) * 30.0
            lines.append((start, start + 30.0, "Editorial line \(idx) about the topic of the day."))
        }
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-stall",
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

    /// A runtime whose `tokenCount` never returns.
    ///
    /// This is not a contrived fault. `SystemLanguageModel.tokenCount(for:)` is
    /// an XPC round trip to the same on-device daemon `respond` goes to, it was
    /// unbounded until this bead, and `planPassA` makes one per candidate
    /// window — so it is the single most likely place for the observed wedge to
    /// have been, and the one place no per-call inference deadline reached.
    private func wedgedPlanningRuntime() -> FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4_096 },
            tokenCount: { _ in
                await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
                return 1
            },
            coarseSchemaTokenCount: { 16 },
            refinementSchemaTokenCount: { 32 },
            boundarySchemaTokenCount: { 32 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in CoarseScreeningSchema(disposition: .noAds, support: nil) },
                    respondRefinement: { _ in RefinementWindowSchema(spans: []) }
                )
            },
            backoffSleep: { _ in }
        )
    }

    /// Shrinks the stall bound so the whole watchdog path runs at gate speed,
    /// exactly as the suites for `inferenceDeadline` do. The assertions below
    /// never measure elapsed time — only which outcome was reached.
    private func stallBoundConfig(
        interval: Duration = .milliseconds(10),
        limit: Int = 2
    ) -> FoundationModelClassifier.Config {
        FoundationModelClassifier.Config(
            safetyMarginTokens: 128,
            coarseMaximumResponseTokens: 96,
            refinementMaximumResponseTokens: 1024,
            coarseNoProgressInterval: interval,
            coarseNoProgressIntervalLimit: limit
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime,
        config: FoundationModelClassifier.Config
    ) -> BackfillJobRunner {
        BackfillJobRunner(
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

    // MARK: - 1. The pass reports the work it finishes

    /// The tick contract, stated exactly: one for the planning prologue plus
    /// one per window after the first, which is `plans.count` for any pass that
    /// planned at least one window.
    ///
    /// Before this bead there was no signal here at all — which is why a
    /// healthy 45-minute coarse pass and a permanently wedged one presented
    /// identically in the database, and why no threshold on `updatedAt` could
    /// have separated them.
    @Test("the coarse pass reports one unit of work per resolved window")
    func coarsePassReportsProgressPerWindow() async throws {
        // Counting prompt CHARACTERS rather than words is what forces the
        // planner to cut several windows out of this transcript. Without it the
        // whole fixture fits one window, `max(1, plans.count)` collapses to 1,
        // and the per-window half of the contract goes untested — a vacuous
        // pass that survives deleting the per-window tick outright.
        let fmRuntime = TestFMRuntime(tokenCountRule: { $0.count })
        let classifier = FoundationModelClassifier(runtime: fmRuntime.runtime)
        let inputs = makeInputs(lineCount: 40)

        let observed = FMProgressTicker()
        let output = try await classifier.coarsePassA(
            segments: inputs.segments,
            onProgress: { _ in observed.note() }
        )

        // The vacuity guard. A single-window pass cannot distinguish the
        // prologue tick from the per-window ticks.
        #expect(output.plans.count > 1)
        // One for the planning prologue, then one per window after the first.
        #expect(observed.observed == max(1, output.plans.count))
    }

    @Test("an empty pass still reports the planning prologue")
    func emptyPassReportsThePrologue() async throws {
        let fmRuntime = TestFMRuntime()
        let classifier = FoundationModelClassifier(runtime: fmRuntime.runtime)
        let observed = FMProgressTicker()

        _ = try await classifier.coarsePassA(
            segments: [],
            onProgress: { _ in observed.note() }
        )

        // Reaching the end of planning IS the unit of work here — it proves the
        // availability probe and the token counts returned. Without this tick
        // the watchdog could not tell "still planning" from "wedged before the
        // first window", which is the conflation the bead is about.
        #expect(observed.observed == 1)
    }

    // MARK: - 2. A pass that produces nothing becomes a named terminal

    @Test("a coarse pass wedged in planning ends the job with a named terminal")
    func wedgedPlanningEndsWithANamedTerminal() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let runner = makeRunner(
            store: store,
            runtime: wedgedPlanningRuntime(),
            config: stallBoundConfig()
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        let jobId = try #require(result.admittedJobIds.first)
        let job = try #require(await store.fetchBackfillJob(byId: jobId))
        // The whole point: NOT `running`. A job that cannot make progress must
        // leave the state that says it is making progress.
        #expect(job.status == .failed)
        // NAMED. `deferReason` is what an operator reads off the device without
        // scraping logs; "the model stopped answering" and "SQLite rejected a
        // write" must not look alike there.
        let reason = try #require(job.deferReason)
        #expect(reason.contains("FMNoProgressError"))
    }

    @Test("a healthy pass is never ended by the stall bound")
    func healthyPassIsNotEndedByTheStallBound() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [1], certainty: .strong)
                )
            ]
        )
        // The same aggressive bound the wedged test uses. A pass that answers
        // must survive it — otherwise the bound is measuring duration, not
        // silence, and would kill the 12-45 minute passes it is meant to spare.
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            config: stallBoundConfig()
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        let jobId = try #require(result.admittedJobIds.first)
        let job = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(job.status != .failed)
        #expect(!result.scanResultIds.isEmpty)
    }

    // MARK: - 3. The reaper is reachable from a foregrounded session

    @Test("the in-session sweep reaps a stranded row and spares a fresh one")
    func inSessionSweepReapsOnlyStrandedRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-sweep"))

        // `insertBackfillJob` seeds `updatedAt` from `createdAt`, so an aged
        // `createdAt` is how a row that predates the freshness floor is built.
        let now = Date().timeIntervalSince1970
        let stranded = now - Double(AnalysisStore.strandedJobFreshnessSeconds) - 60
        try await store.insertBackfillJob(
            makeRunningJob(jobId: "stale-job", createdAt: stranded)
        )
        try await store.insertBackfillJob(
            makeRunningJob(jobId: "fresh-job", createdAt: now)
        )

        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider()
        )
        let count = await reconciler.sweepStrandedBackfillJobsInSession()

        #expect(count == 1)
        let staleAfter = try #require(await store.fetchBackfillJob(byId: "stale-job"))
        #expect(staleAfter.status == .queued)
        // The freshness floor is the only thing standing between this sweep and
        // a live runner mid-pass. It has to hold.
        let freshAfter = try #require(await store.fetchBackfillJob(byId: "fresh-job"))
        #expect(freshAfter.status == .running)
    }

    private func makeRunningJob(jobId: String, createdAt: Double) -> BackfillJob {
        BackfillJob(
            jobId: jobId,
            analysisAssetId: "asset-sweep",
            podcastId: "podcast-stall",
            phase: .fullEpisodeScan,
            coveragePolicy: .fullCoverage,
            priority: 5,
            progressCursor: nil,
            retryCount: 0,
            deferReason: nil,
            status: .running,
            scanCohortJSON: nil,
            createdAt: createdAt
        )
    }
}
