// BackfillCoarseCheckpointTests.swift
// playhead-26od: a coarse pass killed mid-flight must keep the windows it
// already screened.
//
// THE FIELD CASE THESE TESTS ENCODE. The 2026-08-03 device pull
// (`scratchpad/db-aug3/analysis.sqlite`) recorded, across ten `backfill_jobs`
// rows, roughly 4.6 hours of `createdAt`->`updatedAt` running time and ZERO
// durable `semantic_scan_results`. One asset (AD5F3A0A, a 4280 s episode
// transcribed to 100 %) had two `fullEpisodeScan` runs of 37.4 and 29.8 minutes
// that both ended reset to `queued` with no scan row past `windowEndTime` 900.0;
// all 45 of its rows came from the single run that RETURNED. `updatedAt` was
// advancing the whole time — playhead-qk44's per-window lease touch — so the
// windows demonstrably WERE being screened, and every one of them was thrown
// away.
//
// The cause was structural, not a bug in any branch: `coarsePassA` banked its
// windows in a local array and had exactly one durability event, its `return`.
// A healthy pass is 12-45 minutes of FM wall clock; an app-lifecycle window or a
// background grant is ~30 minutes. So a pass that dies mid-flight — jetsam,
// suspend, or the no-progress watchdog abandoning a wedge — lost all of it, and
// the next run paid for the same FM seconds again.
//
// playhead-t1kq does not cover this. It salvages the CURSOR, it fires only on a
// graceful `CancellationError`, and it needs the pass to have returned.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-26od: coarse pass checkpointing")
struct BackfillCoarseCheckpointTests {

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
            capabilitySnapshot: nil,
            episodeDurationSec: nil
        )
    }

    /// Each line is one segment and one atom, 30 s long, so a line ref `i`
    /// covers `[30i, 30i + 30)` and a coverage cursor is directly readable as a
    /// line count.
    private static let segmentSeconds = 30.0

    private func makeInputs(
        assetId: String,
        lineCount: Int
    ) -> BackfillJobRunner.AssetInputs {
        let transcriptVersion = "tx-26od-v1"
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
            podcastId: "podcast-26od",
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

    /// The no-progress bound, shrunk enough to run at gate speed but NOT to the
    /// millisecond.
    ///
    /// The bound is on SILENCE, and the silence these tests need to produce is a
    /// window that never answers. Every other window here answers from an
    /// in-memory queue, so the real gap between ticks is microseconds of work
    /// plus a couple of SQLite writes — but the gate runs ~8,300 tests
    /// concurrently and a 10 ms budget is close enough to a loaded machine's
    /// scheduling jitter to be a coin flip. 250 ms x 3 leaves three orders of
    /// magnitude of headroom over the legitimate gap while still firing in under
    /// a second. No assertion here measures elapsed time.
    private func stallBoundConfig() -> FoundationModelClassifier.Config {
        FoundationModelClassifier.Config(
            safetyMarginTokens: 128,
            coarseMaximumResponseTokens: 96,
            refinementMaximumResponseTokens: 1024,
            coarseNoProgressInterval: .milliseconds(250),
            coarseNoProgressIntervalLimit: 3
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

    /// A runtime that answers `answeringWindows` coarse calls and then never
    /// answers again.
    ///
    /// This is the death mode the bead is about, expressed in the one form a
    /// test can produce deterministically. A real jetsam runs no in-process code
    /// at all; a wedged window runs none of the code that matters either,
    /// because `FMNoProgressWatchdog` ABANDONS the pass rather than awaiting it
    /// — so the banked windows never reach the `return` that used to be their
    /// only route into the database. Same observable: the pass's work exists
    /// only in memory that nobody will ever read.
    private func wedgingAfter(_ answeringWindows: Int) -> TestFMRuntime {
        TestFMRuntime(
            tokenCountRule: { $0.count },
            onCoarseRespond: { ordinal in
                guard ordinal > answeringWindows else { return }
                await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
            }
        )
    }

    private func passARows(
        _ store: AnalysisStore,
        assetId: String
    ) async throws -> [SemanticScanResult] {
        try await store.fetchSemanticScanResults(analysisAssetId: assetId)
            .filter { $0.scanPass == "passA" }
    }

    // MARK: - 1. The work survives the death

    /// The headline. Before this bead the expected row count here was ZERO, for
    /// any number of successfully screened windows, because the only write site
    /// was downstream of a `return` this pass never reaches.
    @Test("a coarse pass killed mid-flight keeps the windows it already screened")
    func midFlightDeathKeepsTheScreenedWindows() async throws {
        let assetId = "asset-26od-durable"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let answeringWindows = 3
        let runner = makeRunner(
            store: store,
            runtime: wedgingAfter(answeringWindows).runtime,
            config: stallBoundConfig()
        )

        _ = try? await runner.runPendingBackfill(
            for: makeInputs(assetId: assetId, lineCount: 40)
        )

        let rows = try await passARows(store, assetId: assetId)
        let successes = rows.filter { $0.status == .success }
        // Every window that answered, and only those. A pass that banked three
        // windows and lost them is the measured field state; a pass that banked
        // three and persisted five would be fabricating coverage, which is the
        // failure mode qbib and pmp9 both exist to prevent.
        #expect(successes.count == answeringWindows)
        // The vacuity guard: a single-window pass could not distinguish
        // "checkpointed as it went" from "wrote one row at the end".
        #expect(answeringWindows > 1)
        // Every row is a real screening of real audio, not a sentinel.
        for row in successes {
            #expect(row.windowEndTime > row.windowStartTime)
            #expect(row.analysisAssetId == assetId)
        }
    }

    /// The other half of "a subsequent run does not re-do them": the durable
    /// rows are useless if nothing records HOW FAR the pass got, because
    /// `narrowedForResume` is the only sub-episode skip in production and it
    /// reads `backfill_jobs.progressCursor`.
    ///
    /// Note what is asserted about the cursor's VALUE. It is not a number picked
    /// by this test; it is exactly the audio the persisted rows cover. A cursor
    /// past that would strand unscanned audio forever — the pmp9 defect — and a
    /// cursor short of it would re-scan work that is already durable.
    @Test("a coarse pass killed mid-flight checkpoints an honest resume cursor")
    func midFlightDeathCheckpointsTheHonestCursor() async throws {
        let assetId = "asset-26od-cursor"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let runner = makeRunner(
            store: store,
            runtime: wedgingAfter(3).runtime,
            config: stallBoundConfig()
        )

        let result = try? await runner.runPendingBackfill(
            for: makeInputs(assetId: assetId, lineCount: 40)
        )

        let jobId = try #require(result?.admittedJobIds.first)
        let job = try #require(await store.fetchBackfillJob(byId: jobId))
        let cursor = try #require(job.progressCursor?.lastProcessedUpperBoundSec)

        let successes = try await passARows(store, assetId: assetId)
            .filter { $0.status == .success }
        let coveredThrough = try #require(successes.map(\.windowEndTime).max())
        #expect(cursor == coveredThrough)
        // Positive, so it can actually narrow anything. `cursorAdvanced` treats
        // a cursor of 0 as no progress.
        #expect(cursor > 0)
    }

    // MARK: - 2. The next run does not pay for it again

    /// End-to-end composition: the state a mid-flight death leaves behind is
    /// state the NEXT run consumes.
    ///
    /// The seam in the middle is the reaper. A jetsam leaves the row `running`
    /// and `AnalysisStore.resetStrandedBackfillJobs` flips it to `queued`,
    /// touching only `status` and `updatedAt` — the `progressCursor` survives by
    /// construction, which is what makes this composition work at all. That flip
    /// has its own test (`BackfillStallBoundTests`), so this one carries the
    /// durable row across to a fresh store and re-queues it, which is what the
    /// next app launch sees.
    ///
    /// Nothing here is hand-fed: the cursor and the job are read out of run 1's
    /// database, not written by the test.
    @Test("a run resuming after a mid-flight death does not re-scan the covered prefix")
    func resumeAfterMidFlightDeathSkipsTheCoveredPrefix() async throws {
        let assetId = "asset-26od-resume"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)

        // --- Run 1: dies mid-flight.
        let firstStore = try await makeTestStore()
        try await firstStore.insertAsset(makeAsset(id: assetId))
        let firstRunner = makeRunner(
            store: firstStore,
            runtime: wedgingAfter(3).runtime,
            config: stallBoundConfig()
        )
        let firstResult = try? await firstRunner.runPendingBackfill(for: inputs)
        let jobId = try #require(firstResult?.admittedJobIds.first)
        let diedJob = try #require(await firstStore.fetchBackfillJob(byId: jobId))
        let cursor = try #require(diedJob.progressCursor?.lastProcessedUpperBoundSec)

        // --- The reaper: same row, same cursor, back in the queue.
        let secondStore = try await makeTestStore()
        try await secondStore.insertAsset(makeAsset(id: assetId))
        try await secondStore.insertBackfillJob(
            BackfillJob(
                jobId: diedJob.jobId,
                analysisAssetId: diedJob.analysisAssetId,
                podcastId: diedJob.podcastId,
                phase: diedJob.phase,
                coveragePolicy: diedJob.coveragePolicy,
                priority: diedJob.priority,
                progressCursor: diedJob.progressCursor,
                retryCount: 0,
                deferReason: nil,
                status: .queued,
                scanCohortJSON: diedJob.scanCohortJSON,
                createdAt: diedJob.createdAt
            )
        )

        // --- Run 2: healthy, and must not re-screen covered audio.
        let secondRuntime = TestFMRuntime(tokenCountRule: { $0.count })
        let secondRunner = makeRunner(
            store: secondStore,
            runtime: secondRuntime.runtime,
            config: stallBoundConfig()
        )
        _ = try await secondRunner.runPendingBackfill(for: inputs)

        let submitted = await secondRuntime.snapshotSubmittedCoarseLineRefs()
        let resubmitted = Set(submitted.flatMap { $0 })
        #expect(!resubmitted.isEmpty, "run 2 did no work at all — the assertion below would be vacuous")
        // Line ref `i` covers `[30i, 30i+30)`, so every ref whose window ends at
        // or below the cursor is audio run 1 already screened and banked.
        let coveredRefs = resubmitted.filter { Double($0 + 1) * Self.segmentSeconds <= cursor }
        #expect(coveredRefs.isEmpty, "run 2 re-scanned already-durable audio: \(coveredRefs.sorted())")
    }

    // MARK: - 3. The negative: a pass that finishes is unchanged

    /// A normally-completing pass now writes each success row twice — once by
    /// the checkpoint and once by the end-of-pass digest, which is untouched.
    /// That is only safe because the row id is deterministic and
    /// `UNIQUE(reuseKeyHash)` + `INSERT OR REPLACE` make the second write an
    /// exact replace.
    ///
    /// This test is the proof, and it is deliberately three assertions rather
    /// than one, because three different things could go wrong: a duplicate ROW
    /// (id collision broken), a duplicate ENTRY in the returned id list (the
    /// runner's own accounting), and an inflated `attemptCount` (the store's
    /// non-success retry counter firing on a path it should not).
    @Test("a pass that completes normally persists exactly one row per window")
    func normalPassPersistsExactlyOneRowPerWindow() async throws {
        let assetId = "asset-26od-normal"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(tokenCountRule: { $0.count })
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            config: stallBoundConfig()
        )

        let result = try await runner.runPendingBackfill(
            for: makeInputs(assetId: assetId, lineCount: 40)
        )

        let windowCount = await fmRuntime.coarseCallCount
        #expect(windowCount > 1, "single-window pass cannot detect a duplicate")

        let rows = try await passARows(store, assetId: assetId)
        let successes = rows.filter { $0.status == .success }
        // One row per screened window — not two.
        #expect(successes.count == windowCount)
        #expect(Set(successes.map(\.id)).count == successes.count)
        // The double write must not read as a retry. `insertSemanticScanResult`
        // bumps `attemptCount` for NON-success rows only, which is precisely why
        // the checkpoint banks successes and leaves failure rows to the digest.
        for row in successes {
            #expect(row.attemptCount == 1)
        }
        // The runner's own accounting stays exact: the ids it reports are the
        // ids in the database, once each.
        let reportedPassAIds = Set(result.scanResultIds).intersection(Set(rows.map(\.id)))
        #expect(reportedPassAIds.count == rows.count)
        #expect(Set(result.scanResultIds).count == result.scanResultIds.count)
    }

    /// Re-running the identical job against a database that already holds its
    /// rows must converge, not accumulate. The reuse key is per-job
    /// (`reuseScope` is the `jobId`), so this is the only re-run shape that can
    /// collide at all — and it is exactly the shape a reaped-and-redriven job
    /// produces.
    @Test("re-running a completed pass adds no rows")
    func rerunningTheSamePassIsIdempotent() async throws {
        let assetId = "asset-26od-idempotent"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))

        let firstRunner = makeRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
            config: stallBoundConfig()
        )
        _ = try await firstRunner.runPendingBackfill(for: inputs)
        let afterFirst = try await passARows(store, assetId: assetId)
        #expect(!afterFirst.isEmpty)

        // A fresh runner over the same store is what a second app launch is.
        let secondRunner = makeRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
            config: stallBoundConfig()
        )
        _ = try await secondRunner.runPendingBackfill(for: inputs)
        let afterSecond = try await passARows(store, assetId: assetId)

        #expect(afterSecond.count == afterFirst.count)
        #expect(Set(afterSecond.map(\.id)) == Set(afterFirst.map(\.id)))
    }

    // MARK: - 4. The classifier's half of the contract

    /// The pass hands out its banked windows BEFORE it returns, and it hands out
    /// the plan list with them.
    ///
    /// The prefix property is what makes the checkpoint cheap: because each
    /// snapshot is the whole append-only prefix, the caller can slice off what
    /// it has already written instead of rebuilding every row on every window.
    /// A snapshot that ever shrank or reordered would silently turn that slice
    /// into a hole.
    @Test("the coarse pass reports its banked windows before it returns")
    func coarsePassReportsBankedWindowsBeforeReturning() async throws {
        let assetId = "asset-26od-classifier"
        let fmRuntime = TestFMRuntime(tokenCountRule: { $0.count })
        let classifier = FoundationModelClassifier(runtime: fmRuntime.runtime)
        let inputs = makeInputs(assetId: assetId, lineCount: 40)

        let recorder = BankedWindowRecorder()
        let output = try await classifier.coarsePassA(
            segments: inputs.segments,
            onWindowsBanked: { await recorder.record($0) }
        )

        #expect(output.plans.count > 1, "single-window pass makes the prefix claims vacuous")
        let snapshots = await recorder.snapshots
        // One per window after the first: the checkpoint rides the same tick
        // `onProgress` does, taken at the top of iteration N once window N-1 has
        // resolved.
        #expect(snapshots.count == output.plans.count - 1)
        // Strictly growing, append-only prefixes of the final window list.
        var previousCount = -1
        for snapshot in snapshots {
            #expect(snapshot.windows.count > previousCount)
            previousCount = snapshot.windows.count
            #expect(Array(output.windows.prefix(snapshot.windows.count)) == snapshot.windows)
            // The denominator is complete from the first snapshot: planning
            // finishes before any window is attempted, so a caller can compute
            // coverage without waiting for the pass.
            #expect(snapshot.plans == output.plans)
        }
        // The last snapshot carries everything except the final window, which is
        // still in flight when it is delivered. That is the bound on what a
        // mid-flight death can cost: one window, never the whole pass.
        let last = try #require(snapshots.last)
        #expect(last.windows.count == output.windows.count - 1)
    }

    /// A pass with no observer must behave exactly as it did before this bead.
    @Test("a coarse pass with no checkpoint observer is unaffected")
    func coarsePassWithoutObserverIsUnaffected() async throws {
        let assetId = "asset-26od-no-observer"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)

        let withObserver = try await FoundationModelClassifier(
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime
        ).coarsePassA(segments: inputs.segments, onWindowsBanked: { _ in })
        let without = try await FoundationModelClassifier(
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime
        ).coarsePassA(segments: inputs.segments)

        #expect(withObserver.windows == without.windows)
        #expect(withObserver.plans == without.plans)
        #expect(withObserver.status == without.status)
        #expect(withObserver.failedWindows == without.failedWindows)
    }
}

/// Collects the checkpoint snapshots a coarse pass emits.
private actor BankedWindowRecorder {
    private(set) var snapshots: [FMCoarseBankedWindows] = []

    func record(_ banked: FMCoarseBankedWindows) {
        snapshots.append(banked)
    }
}

/// playhead-26od extracted the coarse coverage walk out of `runJob` so the
/// mid-flight checkpoint and the end-of-pass digest compute the resume cursor by
/// the IDENTICAL rule. These pin that rule directly, which the runner-level
/// tests could only do through an FM pass.
///
/// The rule is not "how much did we scan". It is "how much CONTIGUOUS audio from
/// the start of the episode did we scan cleanly" — because the only consumer,
/// `narrowedForResume`, drops everything at or below the cursor, so a cursor
/// that jumps a hole strands that audio permanently. That is the pmp9 defect,
/// and it is what these cases are guarding.
@Suite("playhead-26od: coarse coverage walk")
struct CoarseCoverageWalkTests {

    private func plan(_ index: Int) -> CoarsePassWindowPlan {
        CoarsePassWindowPlan(
            windowIndex: index,
            lineRefs: [index],
            prompt: "prompt-\(index)",
            promptTokenCount: 10,
            startTime: Double(index) * 30.0,
            endTime: Double(index + 1) * 30.0,
            transcriptQuality: .high
        )
    }

    private func window(_ index: Int) -> FMCoarseWindowOutput {
        FMCoarseWindowOutput(
            windowIndex: index,
            lineRefs: [index],
            startTime: Double(index) * 30.0,
            endTime: Double(index + 1) * 30.0,
            transcriptQuality: .high,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )
    }

    private func failure(_ index: Int) -> CoarseWindowFailure {
        CoarseWindowFailure(
            planWindowIndex: index,
            lineRefs: [index],
            startTime: Double(index) * 30.0,
            endTime: Double(index + 1) * 30.0,
            status: .rateLimited
        )
    }

    @Test("an unbroken run of successes advances the cursor to its end")
    func unbrokenPrefixAdvances() {
        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: (0..<4).map(plan),
            windows: (0..<3).map(window),
            failedWindows: []
        )
        #expect(walk.contiguousUpperBoundSec == 90.0)
        #expect(walk.fullyCovered == false)
        #expect(walk.unattemptedPlans(from: (0..<4).map(plan)).map(\.windowIndex) == [3])
    }

    @Test("every plan covered reads as fully covered")
    func allPlansCovered() {
        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: (0..<3).map(plan),
            windows: (0..<3).map(window),
            failedWindows: []
        )
        #expect(walk.fullyCovered)
        #expect(walk.contiguousUpperBoundSec == 90.0)
    }

    /// The cursor must STOP at a hole even when later audio was screened
    /// successfully — playhead-lxkq attempts ad-likely windows out of episode
    /// order, so a success past a hole is the normal case, not a corner one.
    @Test("a hole stops the walk even when later windows succeeded")
    func holeStopsTheWalk() {
        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: (0..<4).map(plan),
            windows: [window(0), window(2), window(3)],
            failedWindows: [failure(1)]
        )
        #expect(walk.contiguousUpperBoundSec == 30.0)
        #expect(walk.fullyCovered == false)
    }

    /// playhead-qbib: a plan that produced BOTH a recovered window and an
    /// un-recovered remainder is not covered, and must not advance the cursor
    /// past itself.
    @Test("a plan with both a window and a failure is not covered")
    func partiallyRecoveredPlanIsNotCovered() {
        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: (0..<3).map(plan),
            windows: [window(0), window(1)],
            failedWindows: [failure(1)]
        )
        #expect(walk.contiguousUpperBoundSec == 30.0)
        #expect(walk.fullyCovered == false)
        #expect(walk.succeededPlanIndices == [0, 1])
        #expect(walk.failedPlanIndices == [1])
    }

    /// playhead-bkhc: `count == count` is vacuously true at zero. An empty plan
    /// list is a missing denominator, and certifying complete coverage of audio
    /// nobody screened is the exact failure the walk exists to prevent.
    @Test("an empty plan list never reads as fully covered")
    func emptyPlansAreNotFullyCovered() {
        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: [],
            windows: [],
            failedWindows: []
        )
        #expect(walk.fullyCovered == false)
        #expect(walk.contiguousUpperBoundSec == nil)
    }

    @Test("a failure on the very first plan leaves the cursor unset")
    func failureOnFirstPlanLeavesNoCursor() {
        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: (0..<3).map(plan),
            windows: [window(1), window(2)],
            failedWindows: [failure(0)]
        )
        #expect(walk.contiguousUpperBoundSec == nil)
    }

    /// Attribution is structural (line-ref subset), not positional, so the walk
    /// is indifferent to the order outcomes arrive in — which is what makes it
    /// safe to run against playhead-lxkq's permuted ATTEMPT order.
    @Test("the walk is indifferent to the order outcomes arrive in")
    func outcomeOrderDoesNotMatter() {
        let plans = (0..<4).map(plan)
        let forward = BackfillJobRunner.coarseCoverageWalk(
            plans: plans,
            windows: [window(0), window(1), window(2)],
            failedWindows: []
        )
        let shuffled = BackfillJobRunner.coarseCoverageWalk(
            plans: plans,
            windows: [window(2), window(0), window(1)],
            failedWindows: []
        )
        #expect(forward == shuffled)
    }
}
