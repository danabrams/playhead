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

    /// The stall bound is left at its PRODUCTION value (180 s x 3) in every test
    /// in this suite, deliberately.
    ///
    /// The first version of this suite shrank it to run the wedge fast, and the
    /// full gate destroyed it: under ~10,000 concurrently-running tests a
    /// perfectly healthy pass answered nothing for longer than the shrunken
    /// budget, the no-progress watchdog abandoned it, and four tests failed on
    /// `windowCount > 1` — a pass with ZERO windows. A silence bound cannot be
    /// shrunk into a saturated machine's scheduling jitter, and there is no
    /// constant that is both small enough to be fast and large enough to be
    /// safe.
    ///
    /// So no test below measures, or depends on, elapsed time. Durability is
    /// observed the only way that is actually load-independent: by reading the
    /// database FROM INSIDE the pass, at a point where the pass demonstrably has
    /// not returned. See `CoarseInFlightObserver`.
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

    /// Samples the DATABASE at the start of every coarse window, from inside the
    /// running pass.
    ///
    /// This is the whole trick that makes the suite deterministic. The contract
    /// under test is "the windows a pass has screened are durable BEFORE the
    /// pass returns" — and a database row observed while the pass is provably
    /// still running is that contract, directly. Whatever kills the pass after
    /// that instant, and however long it takes to do it, cannot un-write a
    /// committed row. So the test needs no death and no clock, and asserting on
    /// what survives a real death becomes a corollary rather than a measurement.
    private actor CoarseInFlightObserver {
        struct Sample: Sendable {
            let respondOrdinal: Int
            let durableSuccessRows: Int
            let durableCursor: Double?
            let maxDurableWindowEndTime: Double?
        }

        private let store: AnalysisStore
        private let assetId: String
        private let transcriptVersion: String
        private var resolvedJobId: String?
        private(set) var samples: [Sample] = []

        init(store: AnalysisStore, assetId: String, transcriptVersion: String) {
            self.store = store
            self.assetId = assetId
            self.transcriptVersion = transcriptVersion
        }

        /// The job id is a pure function of (asset, transcriptVersion, phase,
        /// offset), so the running job can be FOUND rather than guessed at.
        /// Resolved once and cached — searching every phase on every window
        /// would swamp the samples with reads.
        private func runningJobId() async -> String? {
            if let resolvedJobId { return resolvedJobId }
            for phase in BackfillJobPhase.allCases {
                for offset in 0..<4 {
                    let candidate = BackfillJobRunner.makeJobIdForTesting(
                        analysisAssetId: assetId,
                        transcriptVersion: transcriptVersion,
                        phase: phase,
                        offset: offset
                    )
                    guard let job = try? await store.fetchBackfillJob(byId: candidate),
                          job.status == .running else { continue }
                    resolvedJobId = candidate
                    return candidate
                }
            }
            return nil
        }

        func sample(respondOrdinal: Int) async {
            let rows = (try? await store.fetchSemanticScanResults(analysisAssetId: assetId)) ?? []
            let successes = rows.filter { $0.scanPass == "passA" && $0.status == .success }
            var cursor: Double?
            if let jobId = await runningJobId(),
               let job = try? await store.fetchBackfillJob(byId: jobId) {
                cursor = job.progressCursor?.lastProcessedUpperBoundSec
            }
            samples.append(
                Sample(
                    respondOrdinal: respondOrdinal,
                    durableSuccessRows: successes.count,
                    durableCursor: cursor,
                    maxDurableWindowEndTime: successes.map(\.windowEndTime).max()
                )
            )
        }
    }

    // MARK: - 1. The work is durable BEFORE the pass returns

    /// The headline, asserted at the only instant that proves it: while the pass
    /// is still running.
    ///
    /// Before this bead the observed row count at every one of these instants was
    /// ZERO, for any number of already-screened windows, because the only write
    /// site was downstream of a `return`. That is exactly the field state — the
    /// 2026-08-03 pull's ten jobs had a live `updatedAt` and no rows.
    ///
    /// The sequence asserted is exact, not a lower bound: at the start of the
    /// N-th coarse call, precisely N-1 windows are durable. One fewer would mean
    /// the checkpoint lags a window behind what it claims; one more would mean it
    /// is persisting a window that has not resolved.
    @Test("every screened window is durable before the pass returns")
    func screenedWindowsAreDurableBeforeThePassReturns() async throws {
        let assetId = "asset-26od-durable"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let observer = CoarseInFlightObserver(
            store: store,
            assetId: assetId,
            transcriptVersion: inputs.transcriptVersion
        )
        let fmRuntime = TestFMRuntime(
            tokenCountRule: { $0.count },
            onCoarseRespond: { ordinal in await observer.sample(respondOrdinal: ordinal) }
        )

        _ = try await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: inputs)

        let samples = await observer.samples
        // The vacuity guard. With one window there is no "before the return" to
        // observe, and every claim below holds trivially.
        #expect(samples.count > 2, "need several windows for an in-flight claim to mean anything")
        for sample in samples {
            #expect(
                sample.durableSuccessRows == sample.respondOrdinal - 1,
                "at coarse call \(sample.respondOrdinal) expected \(sample.respondOrdinal - 1) durable rows, saw \(sample.durableSuccessRows)"
            )
        }
        // Stated once more as the thing the bead is actually about, so a future
        // reader does not have to derive it from the loop: real screened windows
        // were in the database while the pass was still running.
        let lastInFlight = try #require(samples.last)
        #expect(lastInFlight.durableSuccessRows > 0)
    }

    /// The other half of "a subsequent run does not re-do them": the durable rows
    /// are useless if nothing records HOW FAR the pass got, because
    /// `narrowedForResume` is the only sub-episode skip in production and it
    /// reads `backfill_jobs.progressCursor`.
    ///
    /// Note what is asserted about the cursor's VALUE. It is never a number this
    /// test picked; at every instant it is exactly the audio the rows durable at
    /// that same instant cover. A cursor past that would strand unscanned audio
    /// forever — the pmp9 defect — and a cursor short of it would re-scan work
    /// that is already durable.
    @Test("the resume cursor is checkpointed in step with the durable rows")
    func resumeCursorIsCheckpointedInStepWithTheRows() async throws {
        let assetId = "asset-26od-cursor"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let observer = CoarseInFlightObserver(
            store: store,
            assetId: assetId,
            transcriptVersion: inputs.transcriptVersion
        )
        let fmRuntime = TestFMRuntime(
            tokenCountRule: { $0.count },
            onCoarseRespond: { ordinal in await observer.sample(respondOrdinal: ordinal) }
        )

        _ = try await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: inputs)

        let samples = await observer.samples
        #expect(samples.count > 2)
        var previousCursor = 0.0
        var sawAPositiveCursor = false
        for sample in samples {
            guard let cursor = sample.durableCursor else { continue }
            // Never past audio nobody screened. This is the invariant the whole
            // coverage walk exists to hold.
            let covered = try #require(sample.maxDurableWindowEndTime)
            #expect(cursor <= covered)
            // Monotonic: a resume must never be told to go back and re-scan, and
            // must never be told to skip forward over a hole.
            #expect(cursor >= previousCursor)
            previousCursor = cursor
            if cursor > 0 { sawAPositiveCursor = true }
        }
        // `cursorAdvanced` treats a cursor of 0 as no progress, so a cursor that
        // never becomes positive would narrow nothing on the next run.
        #expect(sawAPositiveCursor, "no positive resume cursor was ever durable mid-pass")
    }

    // MARK: - 2. The next run does not pay for it again

    /// End-to-end composition: the state a mid-flight death leaves behind is
    /// state the NEXT run consumes.
    ///
    /// The seam in the middle is a JETSAM, and it is captured live rather than
    /// simulated. Run 1 is stopped being observed at a chosen window, and the
    /// job row as it stood AT THAT INSTANT — status, cursor and all — is what
    /// run 2 is handed. That row is precisely what a process death at that
    /// instant would leave on disk, because a committed row is not undone by the
    /// process that wrote it going away.
    ///
    /// The reaper is the only step modelled rather than executed: it flips the
    /// row `running` -> `queued` and touches nothing else
    /// (`AnalysisStore.resetStrandedBackfillJobs` SETs only `status` and
    /// `updatedAt`, which is what lets a mid-flight `progressCursor` survive at
    /// all), and it has its own test in `BackfillStallBoundTests`.
    ///
    /// Nothing here is hand-fed: the cursor is read out of run 1's live
    /// database, never written by the test.
    @Test("a run resuming after a mid-flight death does not re-scan the covered prefix")
    func resumeAfterMidFlightDeathSkipsTheCoveredPrefix() async throws {
        let assetId = "asset-26od-resume"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)

        // --- Run 1, observed from inside: capture the job row exactly as it
        //     stands part-way through the coarse pass.
        let firstStore = try await makeTestStore()
        try await firstStore.insertAsset(makeAsset(id: assetId))
        let captured = CapturedJobBox()
        let captureAtWindow = 4
        let firstRuntime = TestFMRuntime(
            tokenCountRule: { $0.count },
            onCoarseRespond: { ordinal in
                guard ordinal == captureAtWindow else { return }
                for phase in BackfillJobPhase.allCases {
                    for offset in 0..<4 {
                        let candidate = BackfillJobRunner.makeJobIdForTesting(
                            analysisAssetId: assetId,
                            transcriptVersion: inputs.transcriptVersion,
                            phase: phase,
                            offset: offset
                        )
                        if let job = try? await firstStore.fetchBackfillJob(byId: candidate),
                           job.status == .running {
                            await captured.set(job)
                            return
                        }
                    }
                }
            }
        )
        _ = try await makeRunner(store: firstStore, runtime: firstRuntime.runtime)
            .runPendingBackfill(for: inputs)

        let diedJob = try #require(await captured.job, "never observed a running job mid-pass")
        let cursor = try #require(
            diedJob.progressCursor?.lastProcessedUpperBoundSec,
            "no cursor was durable at the captured instant — nothing for a resume to use"
        )
        #expect(cursor > 0)

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
        _ = try await makeRunner(store: secondStore, runtime: secondRuntime.runtime)
            .runPendingBackfill(for: inputs)

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
        )
        _ = try await firstRunner.runPendingBackfill(for: inputs)
        let afterFirst = try await passARows(store, assetId: assetId)
        #expect(!afterFirst.isEmpty)

        // A fresh runner over the same store is what a second app launch is.
        let secondRunner = makeRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
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

    /// A pass with no observer must reach the same verdicts as one with an
    /// observer. The observer is a durability mechanism; it is not allowed to
    /// change what the pass CONCLUDES.
    ///
    /// Compared field by field rather than by whole-struct equality, and
    /// deliberately: `FMCoarseWindowOutput.latencyMillis` is measured wall clock,
    /// so `==` on the struct compares two stopwatches and fails on any machine.
    /// Everything that is a verdict or a coordinate is compared; the stopwatch
    /// is not.
    @Test("a coarse pass with no checkpoint observer reaches the same verdicts")
    func coarsePassWithoutObserverIsUnaffected() async throws {
        let assetId = "asset-26od-no-observer"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)

        let withObserver = try await FoundationModelClassifier(
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime
        ).coarsePassA(segments: inputs.segments, onWindowsBanked: { _ in })
        let without = try await FoundationModelClassifier(
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime
        ).coarsePassA(segments: inputs.segments)

        #expect(withObserver.status == without.status)
        #expect(withObserver.plans == without.plans)
        #expect(withObserver.failedWindows == without.failedWindows)
        #expect(withObserver.windows.count == without.windows.count)
        #expect(withObserver.windows.count > 1, "single-window pass makes this vacuous")
        for (observed, plain) in zip(withObserver.windows, without.windows) {
            #expect(observed.windowIndex == plain.windowIndex)
            #expect(observed.lineRefs == plain.lineRefs)
            #expect(observed.startTime == plain.startTime)
            #expect(observed.endTime == plain.endTime)
            #expect(observed.transcriptQuality == plain.transcriptQuality)
            #expect(observed.screening == plain.screening)
        }
    }
}

/// The two properties that bound this bead's write cost.
///
/// Neither is observable from the database, which is exactly why they are
/// asserted here: a lost dedupe rewrites rows that are already durable and a
/// lost cursor guard rewrites a cursor that has not moved. Both converge on the
/// same correct state, so every row-level test still passes — while the pass
/// quietly goes from O(n) writes to O(n^2), on a phone, once per window, for
/// 12-45 minutes.
@Suite("playhead-26od: checkpoint write cost")
struct CoarseCheckpointBoxTests {

    /// Each checkpoint carries the whole banked prefix, so without this the
    /// pass would re-offer every earlier window on every window.
    @Test("a window already offered to the store is never offered again")
    func processedWindowsAreNotReoffered() {
        let box = CoarseCheckpointBox()
        #expect(box.processedWindowCount == 0)

        // Checkpoint 1 sees one window.
        box.markProcessed(throughCount: 1)
        #expect(box.processedWindowCount == 1)
        // Checkpoint 2 sees the same window plus one more; only the new one is
        // work.
        box.markProcessed(throughCount: 2)
        #expect(box.processedWindowCount == 2)
    }

    /// A snapshot that somehow arrived stale must not rewind the counter and
    /// resurrect writes that are already done.
    @Test("the processed count never goes backwards")
    func processedCountIsMonotonic() {
        let box = CoarseCheckpointBox()
        box.markProcessed(throughCount: 5)
        box.markProcessed(throughCount: 2)
        #expect(box.processedWindowCount == 5)
    }

    /// playhead-lxkq attempts ad-likely windows first, so most early windows do
    /// NOT extend the contiguous prefix. Those checkpoints must cost no
    /// `backfill_jobs` write at all.
    @Test("only a strictly advancing cursor is worth a write")
    func onlyStrictAdvancesAreWorthAWrite() {
        let box = CoarseCheckpointBox()
        #expect(box.advanceCursor(to: 90.0))
        // Same prefix — the window that just resolved was out of episode order.
        #expect(box.advanceCursor(to: 90.0) == false)
        // Behind the prefix — must never regress the cursor.
        #expect(box.advanceCursor(to: 30.0) == false)
        #expect(box.advanceCursor(to: 120.0))
    }

    /// `cursorAdvanced` treats a cursor of 0 as no progress, so a zero upper
    /// bound is not worth a write either.
    @Test("a zero upper bound is not an advance")
    func zeroIsNotAnAdvance() {
        let box = CoarseCheckpointBox()
        #expect(box.advanceCursor(to: 0.0) == false)
    }
}

/// Holds the `backfill_jobs` row as it stood at a chosen instant inside a
/// running coarse pass — the state a jetsam at that instant would leave behind.
private actor CapturedJobBox {
    private(set) var job: BackfillJob?

    func set(_ job: BackfillJob) {
        self.job = job
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
            transcriptQuality: .good
        )
    }

    private func window(_ index: Int) -> FMCoarseWindowOutput {
        FMCoarseWindowOutput(
            windowIndex: index,
            lineRefs: [index],
            startTime: Double(index) * 30.0,
            endTime: Double(index + 1) * 30.0,
            transcriptQuality: .good,
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
