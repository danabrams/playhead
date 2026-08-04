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
    /// So no test that runs in the DEFAULT plans measures, or depends on,
    /// elapsed time. Durability is observed the only way that is actually
    /// load-independent: by reading the database FROM INSIDE the pass, at a
    /// point where the pass demonstrably has not returned. See
    /// `CoarseInFlightObserver`.
    ///
    /// The one exception is stated rather than hidden:
    /// `abandonedPassLeavesItsScreenedWindowsBehind` needs a pass to OUTLIVE
    /// `runJob`, and the only thing that produces one is the no-progress
    /// watchdog, whose bound is wall-clock and whose injectable `sleep` seam
    /// `coarsePassA` does not expose. It shrinks the bound, is therefore
    /// PerfGate'd out of the default plans, and is registered in
    /// `scripts/perf-tests.sh`.
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

    /// The runner as PRODUCTION builds it: with a `SensitiveWindowRouter` and a
    /// `PermissiveClassifierBox` (`PlayheadRuntime`, the
    /// `backfillJobRunnerFactory`, passes both unconditionally on iOS 26+).
    ///
    /// That combination is not cosmetic — it selects a DIFFERENT `coarsePassA`
    /// overload inside `runJob`, and therefore a different argument list, which
    /// is where the mid-flight checkpoint is handed to the pass. `makeRunner`
    /// above takes the other branch, so every other test in this suite exercises
    /// the one production never runs.
    ///
    /// The trigger rule deliberately matches nothing in the fixture text, so no
    /// window routes `.sensitive` and the pass's per-window behaviour is
    /// identical to the plain path. The only thing this configuration changes is
    /// WHICH call site has to carry the observer.
    ///
    /// `adLikelihoodScanOrderEnabled` is set for the same reason and with the
    /// same honesty about its reach: `BackfillJobRunner`'s default is `false`
    /// while `AdDetectionConfig` ships `true`, so a runner built without it is
    /// production-shaped in name only. It does NOT permute this fixture — the
    /// editorial filler produces no acoustic breaks and no lexical candidates,
    /// so `adLikelihoodScanOrderSeeds` returns empty and `planPassA` falls back
    /// to episode order. What a permuted ATTEMPT order does to the cursor is
    /// asserted where it can be made deterministic, in
    /// `checkpointCursorStopsAtTheFirstUnscannedPlan`, rather than left to a
    /// fixture that would have to fire a seed channel by accident.
    @available(iOS 26.0, *)
    private func makeProductionShapedRunner(
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
            scanCohortJSON: makeTestScanCohortJSON(),
            sensitiveRouter: SensitiveWindowRouter(
                triggerRules: [
                    PromptRedactor.RedactionRule(
                        pattern: "ZZTHISNEVERAPPEARSZZ",
                        isRegex: true
                    )
                ]
            ),
            permissiveClassifier: BackfillJobRunner.PermissiveClassifierBox {
                PermissiveAdClassifier()
            },
            adLikelihoodScanOrderEnabled: true
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

    /// The same claim, on the call site production actually takes.
    ///
    /// `runJob` chooses between two `coarsePassA` overloads: the bd-1en
    /// DISPATCHING one when a `sensitiveRouter` AND a `permissiveClassifierBox`
    /// are both present, and the legacy one otherwise. `PlayheadRuntime` supplies
    /// both on iOS 26+, so the dispatching overload is the only one a shipped
    /// build ever calls — and the observer has to be threaded through BOTH.
    ///
    /// Every other test in this suite builds the runner without a router, so all
    /// of them ride the legacy branch. Deleting `onWindowsBanked:` from the
    /// dispatching call site therefore left the whole suite green while the
    /// feature was dead in production: a correct rule nothing invokes, which is
    /// the same defect shape playhead-26od's review round II caught one level
    /// down in the coverage walk.
    ///
    /// The assertion is deliberately the exact sequence the headline test makes,
    /// not a weaker "some rows exist": a lower bound would still pass if the
    /// dispatching branch checkpointed only at the end.
    @available(iOS 26.0, *)
    @Test("the production runner shape checkpoints too — both coarse call sites carry the observer")
    func productionShapedRunnerAlsoCheckpointsMidFlight() async throws {
        let assetId = "asset-26od-dispatching"
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

        _ = try await makeProductionShapedRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: inputs)

        let samples = await observer.samples
        #expect(samples.count > 2, "need several windows for an in-flight claim to mean anything")
        for sample in samples {
            #expect(
                sample.durableSuccessRows == sample.respondOrdinal - 1,
                "at coarse call \(sample.respondOrdinal) expected \(sample.respondOrdinal - 1) durable rows, saw \(sample.durableSuccessRows)"
            )
        }
        let lastInFlight = try #require(samples.last)
        #expect(lastInFlight.durableSuccessRows > 0)
        // The router/permissive pair must not have changed what the pass did:
        // this is the same episode the legacy-branch test screens, so the same
        // windows must end up in the database.
        let successes = try await passARows(store, assetId: assetId)
            .filter { $0.status == .success }
        #expect(successes.count == (await fmRuntime.coarseCallCount))
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
    /// Nothing here is hand-fed: the cursor AND the rows are read out of run 1's
    /// live database, never written by the test. Both are carried across, so
    /// what run 2 faces is the whole durable state of the death instant — which
    /// also means run 2 has to co-exist with rows it did not write.
    @Test("a run resuming after a mid-flight death does not re-scan the covered prefix")
    func resumeAfterMidFlightDeathSkipsTheCoveredPrefix() async throws {
        let assetId = "asset-26od-resume"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)

        // --- Run 1, observed from inside: capture the job row AND the scan rows
        //     exactly as they stand part-way through the coarse pass.
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
                            let rows = (try? await firstStore.fetchSemanticScanResults(
                                analysisAssetId: assetId
                            )) ?? []
                            await captured.set(job, rows: rows)
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

        // --- The reaper: the same row and the same rows the death left on disk,
        //     with status flipped `running` -> `queued` and nothing else touched.
        let carriedRows = await captured.rows
        #expect(!carriedRows.isEmpty, "the death instant carried no durable rows — nothing to preserve")
        let secondStore = try await makeTestStore()
        try await secondStore.insertAsset(makeAsset(id: assetId))
        for row in carriedRows {
            try await secondStore.insertSemanticScanResult(row)
        }
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

        // The work run 1 banked is still there, once each, after run 2 wrote its
        // own rows alongside it. This is the half a two-store seam cannot see:
        // the resume co-exists with rows it did not write.
        let afterSecond = try await passARows(secondStore, assetId: assetId)
        let survivingIds = Set(afterSecond.map(\.id))
        for row in carriedRows where row.scanPass == "passA" {
            #expect(survivingIds.contains(row.id), "run 2 destroyed a row run 1 had banked")
        }
        #expect(Set(afterSecond.map(\.id)).count == afterSecond.count)
    }

    // MARK: - 2b. The literal death, in the serial lane

    /// An ACTUAL abandoned pass, end to end: the runtime stops answering, the
    /// no-progress watchdog abandons the pass, and the banked windows never
    /// reach the `return` that used to be their only route into the database.
    ///
    /// Everything this asserts is entailed by the in-flight tests above — a
    /// committed row is not un-written by the process that wrote it giving up —
    /// so this is a belt on their braces. What it adds is the segment of the
    /// path they cannot reach: that the runner's own failure handling, which
    /// runs after the abandonment and marks the job `.failed`, does not delete
    /// or overwrite the rows on its way out.
    ///
    /// PerfGate'd because it is the one test here that MUST shrink the silence
    /// bound, and a shrunken silence bound is exactly what the parallel gate
    /// destroys — see the note on `makeRunner`. In the serial pass a healthy
    /// window's gap is microseconds and 250 ms x 3 is four orders of magnitude
    /// of headroom. Registered in `scripts/perf-tests.sh`.
    @Test(
        "an abandoned coarse pass leaves its screened windows in the database",
        .enabled(if: PerfGate.runsMeasurementTests, "shrinks the FM no-progress bound — perf pass only (playhead-zx0l)")
    )
    func abandonedPassLeavesItsScreenedWindowsBehind() async throws {
        let assetId = "asset-26od-abandoned"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let answeringWindows = 3
        let fmRuntime = TestFMRuntime(
            tokenCountRule: { $0.count },
            onCoarseRespond: { ordinal in
                guard ordinal > answeringWindows else { return }
                // A park that NOTHING can wake, deliberately. `FMNoProgressWatchdog`
                // abandons rather than awaits, and its `work.cancel()` cannot reach a
                // task suspended on an unresumed continuation — which is the point: a
                // cancellable `Task.sleep` would unwind on that cancel, the "wedged"
                // pass would resume answering, and the test would no longer be about a
                // pass that never returns. The cost is one suspended task plus its
                // fixtures held for the process's life, once per run of the perf plan.
                await withUnsafeContinuation { (_: UnsafeContinuation<Void, Never>) in }
            }
        )
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(
                runtime: fmRuntime.runtime,
                config: FoundationModelClassifier.Config(
                    safetyMarginTokens: 128,
                    coarseMaximumResponseTokens: 96,
                    refinementMaximumResponseTokens: 1024,
                    coarseNoProgressInterval: .milliseconds(250),
                    coarseNoProgressIntervalLimit: 3
                )
            ),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        _ = try? await runner.runPendingBackfill(
            for: makeInputs(assetId: assetId, lineCount: 40)
        )

        let successes = try await passARows(store, assetId: assetId)
            .filter { $0.status == .success }
        // Every window that answered, and only those. Before this bead the count
        // here was zero. A count ABOVE `answeringWindows` would be worse than
        // zero — it would be coverage claimed for audio nobody screened, which
        // is the failure qbib and pmp9 both exist to prevent.
        #expect(successes.count == answeringWindows)
        // The premise, stated against the fixture rather than against itself: an
        // earlier version compared two literals here and could not fail. What
        // has to be true for the count above to mean anything is that the pass
        // was cut SHORT — several windows durable, and the episode's later audio
        // not among them. 40 lines x 30 s is 1200 s of episode.
        #expect(answeringWindows > 1, "a one-window pass cannot distinguish this from a final write")
        let reached = successes.map(\.windowEndTime).max() ?? 0
        #expect(
            reached > 0 && reached < 40 * Self.segmentSeconds,
            "the pass was not truncated (reached \(reached)s of 1200s), so nothing here is about an abandonment"
        )
        for row in successes {
            #expect(row.windowEndTime > row.windowStartTime)
            #expect(row.analysisAssetId == assetId)
        }
    }

    // MARK: - 3. The negative: a pass that finishes is unchanged

    /// A normally-completing pass now writes each success row twice — once by
    /// the checkpoint and once by the end-of-pass digest, which is untouched.
    /// That is only safe because the row id is deterministic and
    /// `UNIQUE(reuseKeyHash)` + `INSERT OR REPLACE` make the second write an
    /// exact replace.
    ///
    /// Two things this test can actually catch, stated precisely because a
    /// looser claim was wrong. It catches a **duplicate row** — two rows where a
    /// window should have one — and an **inflated `attemptCount`**, the store's
    /// non-success retry counter firing on a path it should not. It also catches
    /// a duplicate ENTRY in `result.scanResultIds`, which is the runner's own
    /// in-memory accounting and genuinely can repeat.
    ///
    /// What it does NOT catch, despite an earlier version of this comment
    /// claiming otherwise: a non-deterministic row id. `UNIQUE(reuseKeyHash)` +
    /// `INSERT OR REPLACE` collapses the second write on the reuse key whatever
    /// the id is, so a random id still yields one row per window here. The
    /// deterministic id is load-bearing for OTHER reasons (an orphan row from a
    /// crashed run, and the C-R3-2 transcriptVersion argument in
    /// `makeScanResult`); it is not what this test measures, and pretending it
    /// is would leave the real mechanism — the reuse key — unnamed.
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

    /// A `.complete` job is skipped outright on re-invocation, so a second run
    /// does no FM work and writes nothing.
    ///
    /// **What this does and does not pin.** An earlier version of this test
    /// asserted only that the row set was unchanged, and was therefore vacuous:
    /// run 2 short-circuits at `if existing.status == .complete { continue }`
    /// before touching the classifier, so "no new rows" was true no matter what
    /// the persistence layer did — a plain `INSERT`, no dedupe box and a random
    /// row id would all have passed. The `coarseCallCount == 0` assertion below
    /// is what makes the claim real: it pins the M-5 short-circuit itself, which
    /// is the actual contract on this path.
    ///
    /// Row-level idempotency under a repeated write is pinned where it actually
    /// happens — `normalPassPersistsExactlyOneRowPerWindow`, where the checkpoint
    /// and the end-of-pass digest both write every success row.
    @Test("a completed pass is skipped entirely on re-invocation")
    func rerunningTheSamePassIsIdempotent() async throws {
        let assetId = "asset-26od-idempotent"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))

        let firstRunner = makeRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime
        )
        _ = try await firstRunner.runPendingBackfill(for: inputs)
        let afterFirst = try await passARows(store, assetId: assetId)
        #expect(!afterFirst.isEmpty)

        // A fresh runner over the same store is what a second app launch is.
        let secondFMRuntime = TestFMRuntime(tokenCountRule: { $0.count })
        let secondRunner = makeRunner(store: store, runtime: secondFMRuntime.runtime)
        _ = try await secondRunner.runPendingBackfill(for: inputs)
        // The non-vacuity guard: run 2 reached the classifier ZERO times.
        // Without this the row assertions below hold for any implementation.
        #expect(await secondFMRuntime.coarseCallCount == 0)
        let afterSecond = try await passARows(store, assetId: assetId)

        #expect(afterSecond.count == afterFirst.count)
        #expect(Set(afterSecond.map(\.id)) == Set(afterFirst.map(\.id)))
    }

    /// The "successes only" rule, pinned.
    ///
    /// It is the load-bearing reason the checkpoint can write a row that the
    /// end-of-pass digest will write again: `insertSemanticScanResult` leaves
    /// `attemptCount` alone for a `.success` row but sets it to
    /// `max(existing + 1, incoming)` for a NON-success one. So a failure row
    /// written twice would silently report a single failed window as two
    /// attempts, corrupting exactly the failure accounting playhead-qbib and
    /// playhead-8d5r exist to make trustworthy.
    @Test("a coarse window that FAILED is written once, not twice")
    func failedWindowsAreNotDoubleWritten() async throws {
        let assetId = "asset-26od-failure"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        // One window refuses; its siblings answer. bd-34e Fix B v3 makes a
        // refusal a tolerable per-window failure, so the pass continues and the
        // digest persists a failure row for it.
        let fmRuntime = TestFMRuntime(
            coarseFailures: [nil, .refusal],
            tokenCountRule: { $0.count }
        )

        _ = try await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: makeInputs(assetId: assetId, lineCount: 40))

        let rows = try await passARows(store, assetId: assetId)
        let failures = rows.filter { $0.status != .success }
        #expect(!failures.isEmpty, "no failure row was produced — the assertion below would be vacuous")
        for row in failures {
            #expect(
                row.attemptCount == 1,
                "failure row \(row.id) reports \(row.attemptCount) attempts — it was written more than once"
            )
        }
        // And still exactly one row per logical window, successes included.
        #expect(Set(rows.map(\.id)).count == rows.count)
    }

    /// A checkpoint whose row write FAILED must not advance the resume cursor
    /// past that window.
    ///
    /// The row loop deliberately steps over a failed write — retrying it on
    /// every subsequent checkpoint would spend the rest of the pass on a row a
    /// permanent validator error rejects every time, and would block every later
    /// window from being banked at all. The CURSOR must not step over it. A
    /// cursor is a contiguous prefix and `narrowedForResume` DROPS everything at
    /// or below it, so crediting a window whose row is missing tells the next run
    /// to skip audio that nothing covers — a permanent, silent coverage hole,
    /// and the exact defect (pmp9) the whole coverage walk exists to prevent.
    ///
    /// The failure injected here is a real one: `insertSemanticScanResult`
    /// validates `scanCohortJSON` before anything else, so a runner configured
    /// with a malformed cohort cannot persist a scan row at all. Nothing lands,
    /// so nothing may be credited, so the cursor must stay unset.
    @Test("a checkpoint whose write failed does not advance the resume cursor")
    func aFailedCheckpointWriteDoesNotAdvanceTheCursor() async throws {
        let assetId = "asset-26od-writefail"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let fmRuntime = TestFMRuntime(tokenCountRule: { $0.count })
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: "{ this is not valid cohort json"
        )

        _ = try? await runner.runPendingBackfill(for: inputs)

        // The OTHER half of the premise, and the one an empty database cannot
        // state: the pass really screened windows whose rows then failed to
        // land. Without this a build whose planning produced nothing at all —
        // no windows, no writes, no cursor — satisfies every assertion below
        // while proving none of them.
        #expect(
            await fmRuntime.coarseCallCount > 1,
            "the pass screened nothing, so 'a failed write earns no cursor' is untested here"
        )
        // Nothing was persisted — the premise of the test.
        let rows = try await passARows(store, assetId: assetId)
        #expect(rows.isEmpty, "the injected failure did not actually stop the writes")

        // ... so no cursor may claim any audio. A cursor here would send the
        // next run past windows that were screened but never stored.
        for phase in BackfillJobPhase.allCases {
            for offset in 0..<4 {
                let jobId = BackfillJobRunner.makeJobIdForTesting(
                    analysisAssetId: assetId,
                    transcriptVersion: inputs.transcriptVersion,
                    phase: phase,
                    offset: offset
                )
                guard let job = try await store.fetchBackfillJob(byId: jobId) else { continue }
                let cursor = job.progressCursor?.lastProcessedUpperBoundSec ?? 0
                #expect(
                    cursor == 0,
                    "job \(phase.rawValue) claims coverage through \(cursor)s with zero durable rows"
                )
            }
        }
    }

    // MARK: - 3b. The checkpoint's LIFETIME is the job's

    /// `attributed(_:jobId:)` calls `scenePhaseProvider` exactly once per scan-row
    /// insert attempt (`BackfillJobRunner.attributed`), so an injected provider is
    /// two things at once: an exact COUNT of the row writes a checkpoint performs,
    /// and a deterministic place to reach in BETWEEN two of them.
    ///
    /// That second property is what makes the defuse contract testable at all.
    /// `defuse()` matters only when it lands mid-loop, and a real abandoned pass
    /// arrives there by racing — which is exactly the kind of test this suite
    /// refuses to write. Driving it from inside the write loop makes the same
    /// event ordered rather than raced. Nothing here reads a clock.
    private actor RowWriteProbe {
        private(set) var writes = 0
        private let onWrite: @Sendable (Int) -> Void

        init(onWrite: @escaping @Sendable (Int) -> Void = { _ in }) {
            self.onWrite = onWrite
        }

        func next() -> String {
            writes += 1
            onWrite(writes)
            return "background"
        }
    }

    private func makeProbedRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime,
        probe: RowWriteProbe
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            scenePhaseProvider: { await probe.next() }
        )
    }

    /// A snapshot with `windows` screened out of `plans` planned, one window per
    /// plan — the shape a healthy pass banks part-way through. `windows < plans`
    /// so the walk is never `fullyCovered`, which is the state a mid-flight
    /// checkpoint is only ever called in.
    private func bankedSnapshot(windows: Int, plans: Int) -> FMCoarseBankedWindows {
        FMCoarseBankedWindows(
            plans: (0..<plans).map { idx in
                CoarsePassWindowPlan(
                    windowIndex: idx,
                    lineRefs: [idx],
                    prompt: "p\(idx)",
                    promptTokenCount: 10,
                    startTime: Double(idx) * Self.segmentSeconds,
                    endTime: Double(idx + 1) * Self.segmentSeconds,
                    transcriptQuality: .good
                )
            },
            windows: (0..<windows).map { idx in
                FMCoarseWindowOutput(
                    windowIndex: idx,
                    lineRefs: [idx],
                    startTime: Double(idx) * Self.segmentSeconds,
                    endTime: Double(idx + 1) * Self.segmentSeconds,
                    transcriptQuality: .good,
                    screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
                    latencyMillis: 1.0
                )
            },
            failedWindows: []
        )
    }

    /// One plan, one window, at a chosen episode position — the pieces a
    /// snapshot in a NON-episode attempt order is built from.
    private func planAt(_ idx: Int) -> CoarsePassWindowPlan {
        CoarsePassWindowPlan(
            windowIndex: idx,
            lineRefs: [idx],
            prompt: "p\(idx)",
            promptTokenCount: 10,
            startTime: Double(idx) * Self.segmentSeconds,
            endTime: Double(idx + 1) * Self.segmentSeconds,
            transcriptQuality: .good
        )
    }

    private func windowAt(_ idx: Int) -> FMCoarseWindowOutput {
        FMCoarseWindowOutput(
            windowIndex: idx,
            lineRefs: [idx],
            startTime: Double(idx) * Self.segmentSeconds,
            endTime: Double(idx + 1) * Self.segmentSeconds,
            transcriptQuality: .good,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )
    }

    private func failureAt(_ idx: Int) -> CoarseWindowFailure {
        CoarseWindowFailure(
            planWindowIndex: idx,
            lineRefs: [idx],
            startTime: Double(idx) * Self.segmentSeconds,
            endTime: Double(idx + 1) * Self.segmentSeconds,
            status: .rateLimited
        )
    }

    private func seedRunningJob(
        _ store: AnalysisStore,
        assetId: String,
        jobId: String,
        status: BackfillJobStatus = .running
    ) async throws {
        try await store.insertBackfillJob(
            BackfillJob(
                jobId: jobId,
                analysisAssetId: assetId,
                podcastId: "podcast-26od",
                phase: .fullEpisodeScan,
                coveragePolicy: .targetedWithAudit,
                priority: 0,
                progressCursor: nil,
                retryCount: 0,
                deferReason: nil,
                status: status,
                scanCohortJSON: makeTestScanCohortJSON(),
                createdAt: Date().timeIntervalSince1970
            )
        )
    }

    /// A `defuse()` that arrives MID-LOOP stops the checkpoint where it is.
    ///
    /// This is the property review round I added and nothing pinned: the loop
    /// suspends on every write, so a checkpoint that entered while the job was
    /// alive can be resumed after `runJob` has already exited. Checking the flag
    /// only on ENTRY would let an orphaned pass keep writing rows — and, worse,
    /// keep writing a cursor — onto a row whose run is over, undoing the t1kq
    /// rewind that same exit path may have just performed.
    ///
    /// `aDefusedBoxAcceptsNothing` cannot see any of that: it sets a flag and
    /// reads it back. The three guards in `checkpointCoarseWindows` were all
    /// individually deletable with a green suite.
    @Test("a defuse that lands mid-loop stops the checkpoint at that window")
    func aMidLoopDefuseStopsTheCheckpoint() async throws {
        let assetId = "asset-26od-defuse-midloop"
        let jobId = "job-26od-defuse-midloop"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        try await seedRunningJob(store, assetId: assetId, jobId: jobId)
        let inputs = makeInputs(assetId: assetId, lineCount: 8)
        let box = CoarseCheckpointBox()
        // Defused while the FIRST row is being built — after that row's guard
        // has already been passed, before the second window's.
        let probe = RowWriteProbe { count in
            if count == 1 { box.defuse() }
        }
        let runner = makeProbedRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
            probe: probe
        )

        await runner.checkpointCoarseWindows(
            bankedSnapshot(windows: 3, plans: 4),
            box: box,
            inputs: inputs,
            jobId: jobId,
            jobPhase: .fullEpisodeScan,
            priorCursor: nil,
            runMode: .shadow
        )

        let rows = try await passARows(store, assetId: assetId)
        #expect(rows.count == 1, "the checkpoint kept writing after it was defused: \(rows.count) rows")
        #expect(box.processedWindowCount == 1)
        let job = try #require(try await store.fetchBackfillJob(byId: jobId))
        #expect(job.progressCursor?.lastProcessedUpperBoundSec == nil)
    }

    /// A `defuse()` that arrives after the LAST row still stops the CURSOR.
    ///
    /// The cursor is the one thing this function writes that a terminal path may
    /// deliberately have moved somewhere else — playhead-t1kq REWINDS it to the
    /// admission-time value when a cancellation arrives with the coarse pass
    /// fully covered. A late checkpoint landing after that rewind puts the
    /// partial cursor back and strands the refinement the rewind exists to
    /// preserve, so the guard AFTER the row loop is a separate rule from the one
    /// inside it, and deleting it is invisible to the test above.
    @Test("a defuse that lands after the last row still withholds the cursor")
    func aLateDefuseStillWithholdsTheCursor() async throws {
        let assetId = "asset-26od-defuse-late"
        let jobId = "job-26od-defuse-late"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        try await seedRunningJob(store, assetId: assetId, jobId: jobId)
        let inputs = makeInputs(assetId: assetId, lineCount: 8)
        let box = CoarseCheckpointBox()
        let probe = RowWriteProbe { count in
            if count == 3 { box.defuse() }
        }
        let runner = makeProbedRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
            probe: probe
        )

        await runner.checkpointCoarseWindows(
            bankedSnapshot(windows: 3, plans: 4),
            box: box,
            inputs: inputs,
            jobId: jobId,
            jobPhase: .fullEpisodeScan,
            priorCursor: nil,
            runMode: .shadow
        )

        // Every row landed — the premise. The defuse arrived too late to stop
        // any of them, which is exactly the case the post-loop guard is for.
        let rows = try await passARows(store, assetId: assetId)
        #expect(rows.count == 3)
        let job = try #require(try await store.fetchBackfillJob(byId: jobId))
        #expect(
            job.progressCursor?.lastProcessedUpperBoundSec == nil,
            "a defused checkpoint wrote a cursor onto a row whose run is over"
        )

        // The control, from the same fixture: an UNdefused box does advance the
        // cursor here, so the assertion above is about the defuse and not about
        // a checkpoint that never had a cursor to write.
        let liveAssetId = "asset-26od-defuse-live"
        let liveJobId = "job-26od-defuse-live"
        try await store.insertAsset(makeAsset(id: liveAssetId))
        try await seedRunningJob(store, assetId: liveAssetId, jobId: liveJobId)
        let liveRunner = makeProbedRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
            probe: RowWriteProbe()
        )
        await liveRunner.checkpointCoarseWindows(
            bankedSnapshot(windows: 3, plans: 4),
            box: CoarseCheckpointBox(),
            inputs: makeInputs(assetId: liveAssetId, lineCount: 8),
            jobId: liveJobId,
            jobPhase: .fullEpisodeScan,
            priorCursor: nil,
            runMode: .shadow
        )
        let liveJob = try #require(try await store.fetchBackfillJob(byId: liveJobId))
        #expect(liveJob.progressCursor?.lastProcessedUpperBoundSec == 90.0)
    }

    /// An already-defused box writes NOTHING — not even the first row.
    @Test("a checkpoint on an already-defused box writes nothing at all")
    func anAlreadyDefusedCheckpointWritesNothing() async throws {
        let assetId = "asset-26od-defuse-entry"
        let jobId = "job-26od-defuse-entry"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        try await seedRunningJob(store, assetId: assetId, jobId: jobId)
        let probe = RowWriteProbe()
        let box = CoarseCheckpointBox()
        box.defuse()

        await makeProbedRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
            probe: probe
        ).checkpointCoarseWindows(
            bankedSnapshot(windows: 3, plans: 4),
            box: box,
            inputs: makeInputs(assetId: assetId, lineCount: 8),
            jobId: jobId,
            jobPhase: .fullEpisodeScan,
            priorCursor: nil,
            runMode: .shadow
        )

        #expect(await probe.writes == 0, "a defused checkpoint still built rows")
        #expect(try await passARows(store, assetId: assetId).isEmpty)
        let job = try #require(try await store.fetchBackfillJob(byId: jobId))
        #expect(job.progressCursor?.lastProcessedUpperBoundSec == nil)
    }

    /// The OTHER observer `runJob` installs on the pass is bounded by the same
    /// token, and its guard is asserted here for the reason the checkpoint's
    /// three are: it fires only from a pass that outlived `runJob`, which only
    /// the PerfGate'd abandonment test produces, so as two lines inside a
    /// closure it was a rule nothing could see deleted. `touchCoarseLeaseIfLive`
    /// exists so the same claim can be made directly and deterministically.
    ///
    /// The state that makes it matter is a NON-TERMINAL row.
    /// `markBackfillJobRunning` accepts `queued`, `deferred` and `running`, and
    /// `resetStrandedBackfillJobs` only sweeps `running` rows whose `updatedAt`
    /// is STALE — so a touch from a pass whose own run is over pulls a job a
    /// later run left `deferred` back into `running` and keeps it fresh there,
    /// where the reaper will not take it and the drain will not re-admit it.
    /// That is the stall qk44 exists to prevent, re-entered through qk44's own
    /// observer.
    ///
    /// Both directions, deliberately: a guard that refused everything would
    /// satisfy the negative half on its own while silently retiring the lease
    /// heartbeat that is the whole point of qk44.
    @Test("a defused lease touch cannot resurrect a deferred job — and a live one still leases")
    func aDefusedLeaseTouchDoesNotResurrectTheJob() async throws {
        let assetId = "asset-26od-lease"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let runner = makeRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime
        )

        let orphanedJobId = "job-26od-lease-orphaned"
        try await seedRunningJob(store, assetId: assetId, jobId: orphanedJobId, status: .deferred)
        let defused = CoarseCheckpointBox()
        defused.defuse()
        await runner.touchCoarseLeaseIfLive(box: defused, jobId: orphanedJobId)
        let afterOrphan = try #require(try await store.fetchBackfillJob(byId: orphanedJobId))
        #expect(
            afterOrphan.status == .deferred,
            "an orphaned pass pulled a deferred job back into running: \(afterOrphan.status)"
        )

        let liveJobId = "job-26od-lease-live"
        try await seedRunningJob(store, assetId: assetId, jobId: liveJobId, status: .deferred)
        await runner.touchCoarseLeaseIfLive(box: CoarseCheckpointBox(), jobId: liveJobId)
        let afterLive = try #require(try await store.fetchBackfillJob(byId: liveJobId))
        #expect(
            afterLive.status == .running,
            "a live pass stopped leasing — playhead-qk44's work clock is dead"
        )
    }

    /// The cursor rule, asserted through the WIRING rather than against the
    /// static walk.
    ///
    /// `CoarseCoverageWalkTests` calls `coarseCoverageWalk` directly, so it pins
    /// the RULE and says nothing about what `checkpointCoarseWindows` hands it —
    /// the same "the rule is pinned, the wiring is not" split R3 closed one level
    /// down at `coarseCheckpointWalk`. And the end-to-end tests cannot close it
    /// either: their fixtures are hole-free and in episode order, so
    /// "contiguous prefix" and "the largest `endTime` I have banked" are the same
    /// number and a pmp9-shaped implementation passes both.
    ///
    /// Three snapshots, each a state a healthy production pass really reaches and
    /// none of which an in-order fixture can produce:
    ///
    /// 1. **A PROMOTED window.** playhead-lxkq ships ON and reorders the attempt
    ///    sequence by ad-likelihood, so the pass's FIRST banked window is
    ///    routinely a late-episode one. Its row is durable; the cursor must still
    ///    stop at the unscanned head.
    /// 2. **A HOLE behind a success**, which is what a promoted order produces as
    ///    soon as anything fails.
    /// 3. **A FAILURE whose plan also produced a success** — playhead-qbib's
    ///    partially-recovered window. This is the one that needs
    ///    `coarseCheckpointWalk` to actually forward `banked.failedWindows`;
    ///    dropping that argument is invisible in every other test here.
    @Test("the checkpoint's cursor is the contiguous prefix, not the furthest window banked")
    func checkpointCursorStopsAtTheFirstUnscannedPlan() async throws {
        let store = try await makeTestStore()

        /// Runs one snapshot against a fresh job and returns the cursor it left.
        func cursorAfter(
            _ label: String,
            plans: [CoarsePassWindowPlan],
            windows: [FMCoarseWindowOutput],
            failedWindows: [CoarseWindowFailure]
        ) async throws -> Double? {
            let assetId = "asset-26od-wiring-\(label)"
            let jobId = "job-26od-wiring-\(label)"
            try await store.insertAsset(makeAsset(id: assetId))
            try await seedRunningJob(store, assetId: assetId, jobId: jobId)
            let box = CoarseCheckpointBox()
            await makeProbedRunner(
                store: store,
                runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
                probe: RowWriteProbe()
            ).checkpointCoarseWindows(
                FMCoarseBankedWindows(plans: plans, windows: windows, failedWindows: failedWindows),
                box: box,
                inputs: makeInputs(assetId: assetId, lineCount: 8),
                jobId: jobId,
                jobPhase: .fullEpisodeScan,
                priorCursor: nil,
                runMode: .shadow
            )
            // Every window landed — otherwise the cursor is short for the wrong
            // reason and the assertion below proves nothing.
            #expect(box.durableWindowCount == windows.count, "\(label): a row failed to write")
            let job = try #require(try await store.fetchBackfillJob(byId: jobId))
            return job.progressCursor?.lastProcessedUpperBoundSec
        }

        let plans = (0..<4).map(planAt)

        // 1. lxkq promoted plan 3 to the front. Banking it must not carry the
        //    cursor over plans 1 and 2, which nobody has looked at.
        let promoted = try await cursorAfter(
            "promoted",
            plans: plans,
            windows: [windowAt(3), windowAt(0)],
            failedWindows: []
        )
        #expect(
            promoted == 30.0,
            "the cursor followed the furthest window banked (\(promoted as Double?)) instead of the contiguous prefix"
        )

        // 2. A hole with a success behind it — the same claim, reached through a
        //    recorded failure rather than through an unattempted plan.
        let hole = try await cursorAfter(
            "hole",
            plans: plans,
            windows: [windowAt(0), windowAt(2)],
            failedWindows: [failureAt(1)]
        )
        #expect(hole == 30.0, "the cursor stepped over a failed window: \(hole as Double?)")

        // 3. playhead-qbib: plan 0 produced BOTH a window and a failure, so it is
        //    not covered and the cursor cannot start at all. Only reachable if the
        //    checkpoint forwards `failedWindows` to the walk.
        let partial = try await cursorAfter(
            "partial",
            plans: plans,
            windows: [windowAt(0), windowAt(1)],
            failedWindows: [failureAt(0)]
        )
        #expect(
            partial == nil,
            "a partially-recovered plan was credited: cursor \(partial as Double?)"
        )
    }

    /// A checkpoint must never LOWER a cursor an earlier run already reached.
    ///
    /// The guard is the `monotonic(from: priorCursor)` merge, and it is reachable
    /// for the same reason the cursor cap is: a plan's `startTime` is a MIN over
    /// its segments, and `narrowedForResume` keeps a segment whose `endTime`
    /// exceeds the cursor even when its `startTime` does not. So a resumed pass
    /// can plan a window that BEGINS below the cursor it resumed from, the cap
    /// pulls the walk value down to that start, and the raw walk output is behind
    /// the database.
    ///
    /// Every other test in this suite passes `priorCursor: nil`, so deleting the
    /// merge left them all green — and a regressed cursor is not merely wasted
    /// work: it re-opens audio the previous run already certified, which is the
    /// treadmill this bead exists to end.
    @Test("a checkpoint never lowers a cursor an earlier run already reached")
    func checkpointNeverRegressesAPriorCursor() async throws {
        let assetId = "asset-26od-monotonic"
        let jobId = "job-26od-monotonic"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        try await seedRunningJob(store, assetId: assetId, jobId: jobId)

        // The walk over this snapshot reaches 30; the prior run reached 300.
        await makeProbedRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
            probe: RowWriteProbe()
        ).checkpointCoarseWindows(
            bankedSnapshot(windows: 1, plans: 4),
            box: CoarseCheckpointBox(),
            inputs: makeInputs(assetId: assetId, lineCount: 8),
            jobId: jobId,
            jobPhase: .fullEpisodeScan,
            priorCursor: BackfillProgressCursor(
                processedPhaseCount: 0,
                lastProcessedUpperBoundSec: 300.0
            ),
            runMode: .shadow
        )

        let job = try #require(try await store.fetchBackfillJob(byId: jobId))
        #expect(
            job.progressCursor?.lastProcessedUpperBoundSec == 300.0,
            "the checkpoint rewound the cursor to \(job.progressCursor?.lastProcessedUpperBoundSec as Double?)"
        )
    }

    /// A checkpointed row must be the row the end-of-pass digest would have
    /// written, column for column — because on a killed pass it is the ONLY row
    /// that window will ever have.
    ///
    /// `normalPassPersistsExactlyOneRowPerWindow` cannot see this: there the
    /// digest's `INSERT OR REPLACE` overwrites whatever the checkpoint wrote, so
    /// every discriminator could be wrong and the final database still correct.
    /// The three asserted here are the ones a checkpoint could plausibly get
    /// wrong by hardcoding — `runMode` and `jobPhase` are arguments threaded from
    /// `runJob`, and every other test in this suite passes the same
    /// `.shadow`/`.fullEpisodeScan` pair, so a constant would satisfy them all.
    @Test("a checkpointed row carries the job's own runMode and phase, not a constant")
    func checkpointedRowsCarryTheirJobsDiscriminators() async throws {
        let assetId = "asset-26od-columns"
        let jobId = "job-26od-columns"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        try await seedRunningJob(store, assetId: assetId, jobId: jobId)

        await makeProbedRunner(
            store: store,
            runtime: TestFMRuntime(tokenCountRule: { $0.count }).runtime,
            probe: RowWriteProbe()
        ).checkpointCoarseWindows(
            bankedSnapshot(windows: 2, plans: 4),
            box: CoarseCheckpointBox(),
            inputs: makeInputs(assetId: assetId, lineCount: 8),
            jobId: jobId,
            // Deliberately NOT the pair every other test uses.
            jobPhase: .scanLikelyAdSlots,
            priorCursor: nil,
            runMode: .targeted
        )

        let rows = try await passARows(store, assetId: assetId)
        #expect(rows.count == 2)
        for row in rows {
            #expect(row.runMode == .targeted, "checkpoint row carries runMode \(row.runMode)")
            #expect(row.jobPhase == BackfillJobPhase.scanLikelyAdSlots.rawValue,
                    "checkpoint row carries jobPhase \(row.jobPhase as String?)")
            #expect(row.status == .success)
            #expect(row.scanPass == "passA")
            #expect(row.analysisAssetId == assetId)
            #expect(row.windowEndTime > row.windowStartTime)
        }
    }

    // MARK: - 3c. The write COST, measured over a whole pass

    /// The bead's write-cost claim, asserted end to end instead of argued.
    ///
    /// Every checkpoint carries the whole banked PREFIX, not a delta. Without
    /// the `processedWindowCount` slice the checkpoint rewrites every earlier
    /// window on every window — O(W^2) row writes and O(W^2) JSON encodings
    /// across a pass, on a thermally-constrained phone, in the middle of the
    /// pass this bead exists to keep alive.
    ///
    /// `CoarseCheckpointBoxTests` cannot see that: it asserts a counter
    /// increments, which is true of an implementation that ignores the counter
    /// entirely. Counting `scenePhaseProvider` calls counts scan-row INSERT
    /// attempts directly (`attributed` is on the path of every one of them), so
    /// the quadratic implementation and the linear one differ by an
    /// unmistakable margin — at the 13 windows this fixture plans, 25 writes
    /// against 91.
    ///
    /// The count is exact, not a bound, and it is derived rather than observed:
    /// W-1 checkpoints each contributing exactly the one window that resolved
    /// since the last one, plus the end-of-pass digest's W. A `.noAds` fixture
    /// plans no zoom windows, so passB adds nothing.
    @Test("a pass writes one row per window per checkpoint, not the whole prefix")
    func checkpointWriteCostIsLinearInTheWindowCount() async throws {
        let assetId = "asset-26od-writecost"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let probe = RowWriteProbe()
        let fmRuntime = TestFMRuntime(tokenCountRule: { $0.count })

        let result = try await makeProbedRunner(store: store, runtime: fmRuntime.runtime, probe: probe)
            .runPendingBackfill(for: makeInputs(assetId: assetId, lineCount: 40))

        // The identity below is per-JOB: two jobs would spend two digests and
        // two checkpoint runs and read `2W - 2`, blaming the checkpoint for a
        // planner change. One-job-ness is decided three files away by
        // `observedEpisodeCount: 0`, so pin it where the arithmetic depends on it.
        #expect(result.admittedJobIds.count == 1, "the write-cost identity assumes exactly one job")
        let windowCount = await fmRuntime.coarseCallCount
        #expect(windowCount > 2, "too few windows for a quadratic cost to be distinguishable")
        let writes = await probe.writes
        #expect(
            writes == 2 * windowCount - 1,
            "expected \(2 * windowCount - 1) row writes for \(windowCount) windows, saw \(writes)"
        )
        // ... and the rows themselves are still one per window.
        let successes = try await passARows(store, assetId: assetId).filter { $0.status == .success }
        #expect(successes.count == windowCount)
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

    // MARK: - 5. What a mid-flight-durable coarse row SURFACES

    /// Capture the scan rows a jetsam at coarse window `captureAtWindow` would
    /// leave on disk, plus the job row as it stood at that instant.
    ///
    /// Every coarse window after the first returns `containsAd`, so the rows
    /// this yields are the WORST case for surfacing: a whole prefix of
    /// presence verdicts that no refinement pass ever visited. Window 0 is
    /// `noAds` so the composed extents all lie ahead of a playhead at 0 and
    /// the surfacing question is about arming, not about a mark the listener
    /// has already passed.
    private func captureDeathInstant(
        assetId: String,
        inputs: BackfillJobRunner.AssetInputs,
        captureAtWindow: Int = 5
    ) async throws -> (job: BackfillJob, rows: [SemanticScanResult]) {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let captured = CapturedJobBox()
        let runtime = TestFMRuntime(
            coarseResponses: [CoarseScreeningSchema(disposition: .noAds, support: nil)]
                + Array(
                    repeating: CoarseScreeningSchema(
                        disposition: .containsAd,
                        support: nil
                    ),
                    count: 64
                ),
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
                        if let job = try? await store.fetchBackfillJob(byId: candidate),
                           job.status == .running {
                            let rows = (try? await store.fetchSemanticScanResults(
                                analysisAssetId: assetId
                            )) ?? []
                            await captured.set(job, rows: rows)
                            return
                        }
                    }
                }
            }
        )
        _ = try await makeRunner(store: store, runtime: runtime.runtime)
            .runPendingBackfill(for: inputs)
        let job = try #require(await captured.job, "never observed a running job mid-pass")
        return (job, await captured.rows)
    }

    /// Stand up the production surfacing door over a fresh store and deliver
    /// `windows` through it, exactly as `SemanticSweepMarkSurfacingTests` does.
    private func deliverToSuggestTier(
        _ windows: [AdWindow],
        assetId: String,
        episodeDuration: Double
    ) async throws -> SkipOrchestrator {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: assetId,
                episodeId: "ep-\(assetId)",
                episodeDurationSec: episodeDuration
            )
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store),
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: "ep-\(assetId)",
            podcastId: "podcast-26od"
        )
        await orchestrator.setEpisodeDuration(episodeDuration, analysisAssetId: assetId)
        await orchestrator.updatePlayheadTime(0)
        try await store.upsertHotPathAdWindows(windows, existingIDs: [], retiredIDs: [])
        _ = await orchestrator.ingestPersistedAdWindows(analysisAssetId: assetId)
        return orchestrator
    }

    /// A span the inventory filter genuinely rejects — 2.0 s wholly inside the
    /// 3.0 s head margin — carried in the SAME delivery as the marks under
    /// test. playhead-le02 measured 39 rails that were blind on this axis
    /// because "no banner emitted" is true by construction when the door never
    /// ran; this row makes "the door ran and made a decision" a positive
    /// observation rather than an inference from silence.
    private func headArtifact(assetId: String) -> AdWindow {
        AdWindow(
            id: "artifact-26od-head",
            analysisAssetId: assetId,
            startTime: 0.0,
            endTime: 2.0,
            confidence: 1.0,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 0.0,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    /// A `passB` row that localizes an ad inside `coarse` — what the refinement
    /// pass would have written had the job lived long enough to run it.
    private func refinementRow(
        narrowing coarse: SemanticScanResult,
        start: Double,
        end: Double
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "scan-26od-passB-\(Int(start))",
            analysisAssetId: coarse.analysisAssetId,
            windowFirstAtomOrdinal: coarse.windowFirstAtomOrdinal,
            windowLastAtomOrdinal: coarse.windowLastAtomOrdinal,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: "passB",
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
            scanCohortJSON: coarse.scanCohortJSON,
            transcriptVersion: coarse.transcriptVersion
        )
    }

    /// THE MEASUREMENT, not an assumption: what does a coarse-only row banked
    /// by the mid-flight checkpoint actually put in front of a listener?
    ///
    /// This is the axis playhead-26od moves and nothing pinned. The checkpoint
    /// makes coarse verdicts durable that a mid-pass death used to discard, and
    /// playhead-y3ya (`SemanticSweepMarkComposer`, shipped ON) turns a durable
    /// `containsAd` verdict with NO pass-B refinement into a mark-only
    /// `AdWindow` at the COARSE extent. So the rows this bead preserves reach
    /// the suggest tier, at ~coarse-window width, and the resume cursor means
    /// the prefix they cover is never re-scanned and therefore never refined.
    ///
    /// Asserted with playhead-le02's idiom — the isp5 census outcome AND the
    /// live suggest set, plus a control row that MUST still drop in the same
    /// delivery — so the answer is a positive observation either way. If the
    /// policy is later changed so an unrefined coarse verdict does not surface,
    /// this test is where that decision becomes visible: the expected outcome
    /// flips from `.armedSuggest` to no mark at all, and the control row's drop
    /// is what proves the door still ran.
    @Test("a coarse-only row banked mid-flight reaches the suggest tier at coarse extent")
    func midFlightCoarseOnlyRowsReachTheSuggestTier() async throws {
        let assetId = "asset-26od-surface"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let episodeDuration = Double(40) * Self.segmentSeconds
        let death = try await captureDeathInstant(assetId: assetId, inputs: inputs)

        // Durability, restated here so bullet 3 is asserted alongside bullet 1
        // rather than in a different file: the death instant carried rows AND a
        // cursor. Everything below is about what those rows then do.
        #expect(!death.rows.isEmpty, "the death instant carried no durable rows")
        let cursor = try #require(death.job.progressCursor?.lastProcessedUpperBoundSec)
        #expect(cursor > 0)
        let coarsePresence = death.rows.filter {
            $0.scanPass == "passA" && $0.disposition == .containsAd && $0.status == .success
        }
        #expect(!coarsePresence.isEmpty, "no durable presence verdict — the claim below is vacuous")
        #expect(
            !death.rows.contains { $0.scanPass == "passB" },
            "the pass died before refinement, so no row may be a refinement"
        )

        let marks = SemanticSweepMarkComposer.compose(
            scanRows: death.rows,
            existingWindows: [],
            analysisAssetId: assetId
        )
        #expect(!marks.isEmpty, "the composer emitted nothing — nothing to observe")

        let orchestrator = try await deliverToSuggestTier(
            marks + [headArtifact(assetId: assetId)],
            assetId: assetId,
            episodeDuration: episodeDuration
        )

        // The control: the door ran and made a decision.
        let dropped = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: "artifact-26od-head"
        )
        #expect(dropped?.outcome == .droppedInventorySanity)
        #expect(dropped?.detail == InventorySanityRejectionReason.tooEarly.rawValue)

        // The measurement: every unrefined coarse mark arms a suggestion.
        let armed = await orchestrator.activeSuggestWindowIDs()
        for mark in marks {
            let outcome = await orchestrator.lastAdWindowIngestOutcome(forWindowId: mark.id)
            #expect(outcome?.outcome == .armedSuggest, "mark \(mark.id) at \(mark.startTime)-\(mark.endTime)")
            #expect(armed.contains(mark.id))
        }
        #expect(
            await orchestrator.adWindowIngestOutcomeCount(.armedSuggest) == marks.count
        )
        // What this actually proves is that nothing here AUTO-skips: the marks
        // are `markOnly`/`.candidate` with both edges `.unanchored`, so none of
        // them is admitted as managed and no cut happens without a tap.
        //
        // It is NOT the stronger claim that nothing here can cost show. A
        // suggest banner is a skip affordance — a listener who taps it on an
        // unrefined coarse extent loses whatever show that extent overhangs.
        // Bounded, not eliminated: `SemanticSweepMarkComposer` caps a mark at
        // `maximumMarkDurationSeconds` (300 s) and refuses to emit over an
        // existing window. That exposure is the settled product decision
        // recorded in `.notes/playhead-26od-investigation.md`, with the
        // refinement side filed as playhead-o98e.
        #expect(await orchestrator.adWindowIngestOutcomeCount(.admittedManaged) == 0)
    }

    /// The other direction, which a suppression rule would have to preserve: a
    /// refinement narrows the SAME durable coarse row and the mark surfaces at
    /// the refined extent, not the coarse one.
    ///
    /// Both directions matter — a rule that suppressed everything would satisfy
    /// "no coarse-extent banner" and be exactly as broken as one that suppressed
    /// nothing.
    @Test("once refinement localizes it, the same durable row surfaces at the REFINED extent")
    func aRefinedRowSurfacesAtTheRefinedExtent() async throws {
        let assetId = "asset-26od-surface-refined"
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let episodeDuration = Double(40) * Self.segmentSeconds
        let death = try await captureDeathInstant(assetId: assetId, inputs: inputs)

        let coarse = try #require(
            death.rows
                .filter { $0.scanPass == "passA" && $0.disposition == .containsAd }
                .min(by: { $0.windowStartTime < $1.windowStartTime })
        )
        // A refinement strictly inside the coarse window, and strictly narrower
        // than it, so "the refined extent won" is observable rather than
        // coincidental.
        let refinedStart = coarse.windowStartTime + Self.segmentSeconds
        let refinedEnd = refinedStart + Self.segmentSeconds
        #expect(refinedEnd < coarse.windowEndTime, "fixture: the refinement must be narrower")

        let marks = SemanticSweepMarkComposer.compose(
            scanRows: death.rows + [
                refinementRow(narrowing: coarse, start: refinedStart, end: refinedEnd)
            ],
            existingWindows: [],
            analysisAssetId: assetId
        )
        let refinedMark = try #require(
            marks.first { $0.startTime >= refinedStart && $0.endTime <= refinedEnd },
            "no mark carries the refined extent: \(marks.map { ($0.startTime, $0.endTime) })"
        )
        #expect(refinedMark.startTime == refinedStart)
        #expect(refinedMark.endTime == refinedEnd)

        let orchestrator = try await deliverToSuggestTier(
            marks + [headArtifact(assetId: assetId)],
            assetId: assetId,
            episodeDuration: episodeDuration
        )
        #expect(
            await orchestrator.lastAdWindowIngestOutcome(
                forWindowId: refinedMark.id
            )?.outcome == .armedSuggest
        )
        #expect(await orchestrator.activeSuggestWindowIDs().contains(refinedMark.id))
        // Same control, same delivery.
        #expect(
            await orchestrator.lastAdWindowIngestOutcome(
                forWindowId: "artifact-26od-head"
            )?.outcome == .droppedInventorySanity
        )
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
        box.noteWrite(index: 0, succeeded: true)
        #expect(box.processedWindowCount == 1)
        // Checkpoint 2 sees the same window plus one more; only the new one is
        // work.
        box.noteWrite(index: 1, succeeded: true)
        #expect(box.processedWindowCount == 2)
    }

    /// A snapshot that somehow arrived stale must not rewind the counter and
    /// resurrect writes that are already done.
    @Test("the processed count never goes backwards")
    func processedCountIsMonotonic() {
        let box = CoarseCheckpointBox()
        box.noteWrite(index: 4, succeeded: true)
        box.noteWrite(index: 1, succeeded: true)
        #expect(box.processedWindowCount == 5)
    }

    /// playhead-lxkq attempts ad-likely windows first, so most early windows do
    /// NOT extend the contiguous prefix. Those checkpoints must cost no
    /// `backfill_jobs` write at all.
    @Test("only a strictly advancing cursor is worth a write")
    func onlyStrictAdvancesAreWorthAWrite() {
        let box = CoarseCheckpointBox()
        #expect(box.shouldAdvanceCursor(to: 90.0))
        box.noteCursorWritten(90.0)
        // Same prefix — the window that just resolved was out of episode order.
        #expect(box.shouldAdvanceCursor(to: 90.0) == false)
        // Behind the prefix — must never regress the cursor.
        #expect(box.shouldAdvanceCursor(to: 30.0) == false)
        #expect(box.shouldAdvanceCursor(to: 120.0))
    }

    /// `cursorAdvanced` treats a cursor of 0 as no progress, so a zero upper
    /// bound is not worth a write either.
    @Test("a zero upper bound is not an advance")
    func zeroIsNotAnAdvance() {
        let box = CoarseCheckpointBox()
        #expect(box.shouldAdvanceCursor(to: 0.0) == false)
    }

    /// A cursor write that THREW is not a cursor the database holds, so the
    /// next checkpoint must be free to offer it again.
    ///
    /// Recording the intent instead of the outcome is the same defect
    /// `noteWrite` exists to avoid one level down, with a smaller blast radius:
    /// the cursor only ever ends up BEHIND the truth, so it costs re-scanned
    /// audio rather than a coverage hole. It still costs the thing this bead is
    /// about — durable minutes — whenever the failure is transient and the pass
    /// dies before the prefix advances again.
    ///
    /// GAP, STATED RATHER THAN PAPERED OVER. This pins the box's contract, not
    /// the call site: moving `box.noteCursorWritten` in `checkpointCoarseWindows`
    /// to BEFORE the `store.checkpointBackfillJobProgress` it guards would pass
    /// every test in this repo, because nothing can make that store call throw —
    /// `BackfillJobRunner` holds a concrete `AnalysisStore`, not a protocol, and
    /// `checkpointBackfillJobProgress` is a bare UPDATE with no reachable
    /// rejection. Closing it means a store fault seam, which is an architecture
    /// change this bead's non-goals exclude. The exposure is bounded to one
    /// repeated `backfill_jobs` UPDATE: the cursor is never written AHEAD of what
    /// rows cover either way, so no coverage claim depends on this ordering.
    @Test("a cursor whose write failed is offered again")
    func aFailedCursorWriteIsRetried() {
        let box = CoarseCheckpointBox()
        #expect(box.shouldAdvanceCursor(to: 90.0))
        // ... the store threw, so nothing is noted.
        #expect(box.shouldAdvanceCursor(to: 90.0))
        box.noteCursorWritten(90.0)
        #expect(box.shouldAdvanceCursor(to: 90.0) == false)
    }

    /// The distinction between "offered to the store" and "actually in the
    /// store", which is what stops a failed write from becoming a permanent,
    /// silent coverage hole.
    ///
    /// The row loop steps over a failed write on purpose — retrying it every
    /// checkpoint would spend the rest of the pass on a row a permanent
    /// validator error rejects every time. The CURSOR must not step over it: it
    /// is a contiguous prefix, and crediting a window whose row is missing tells
    /// the next run to skip audio that nothing covers.
    @Test("a failed write freezes the durable prefix but not the processed count")
    func aFailedWriteFreezesTheDurablePrefix() {
        let box = CoarseCheckpointBox()
        box.noteWrite(index: 0, succeeded: true)
        box.noteWrite(index: 1, succeeded: true)
        #expect(box.processedWindowCount == 2)
        #expect(box.durableWindowCount == 2)

        // Window 2's row did not land.
        box.noteWrite(index: 2, succeeded: false)
        #expect(box.processedWindowCount == 3, "the row loop must move on")
        #expect(box.durableWindowCount == 2, "the cursor must not credit a row that is not there")

        // And nothing after the hole can be credited either, however well it
        // writes — the prefix is contiguous.
        box.noteWrite(index: 3, succeeded: true)
        #expect(box.processedWindowCount == 4)
        #expect(box.durableWindowCount == 2)
    }

    /// A pass that outlives its job must not write to it.
    @Test("a defused box accepts nothing further")
    func aDefusedBoxAcceptsNothing() {
        let box = CoarseCheckpointBox()
        #expect(box.isDefused == false)
        box.defuse()
        #expect(box.isDefused)
    }
}

/// Holds the `backfill_jobs` row as it stood at a chosen instant inside a
/// running coarse pass — the state a jetsam at that instant would leave behind.
private actor CapturedJobBox {
    private(set) var job: BackfillJob?
    private(set) var rows: [SemanticScanResult] = []

    func set(_ job: BackfillJob, rows: [SemanticScanResult]) {
        self.job = job
        self.rows = rows
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

    /// A plan whose windows straddle the end of the durable prefix is NOT
    /// covered, even though one of them landed.
    ///
    /// A plan is not one window. `runCoarseRetry` banks one window per
    /// sub-prompt of a shrunk plan and permissive recovery banks one per
    /// recovered chunk, so "the durable prefix ends at window k" and "every plan
    /// under windows 0..<k is durable" are different statements. The plan that
    /// straddles k has a sibling row missing from the store; crediting it
    /// because of the sibling that landed advances the cursor over audio no row
    /// covers, and `narrowedForResume` then drops that audio permanently — pmp9,
    /// through the one door a window-index prefix cannot close.
    ///
    /// Both directions are asserted from ONE fixture: the same three windows
    /// with nothing unpersisted DO carry the cursor to 90 s. Without that
    /// control an implementation that simply never advanced the cursor would
    /// pass the interesting half.
    @Test("a plan straddling the durable prefix is not covered")
    func straddlingPlanIsNotCovered() {
        // P1 is one plan covering two line refs, screened as two windows.
        let plans = [
            CoarsePassWindowPlan(
                windowIndex: 0,
                lineRefs: [0],
                prompt: "p0",
                promptTokenCount: 10,
                startTime: 0.0,
                endTime: 30.0,
                transcriptQuality: .good
            ),
            CoarsePassWindowPlan(
                windowIndex: 1,
                lineRefs: [1, 2],
                prompt: "p1",
                promptTokenCount: 10,
                startTime: 30.0,
                endTime: 90.0,
                transcriptQuality: .good
            )
        ]
        let firstHalf = window(1)
        let secondHalf = window(2)

        let straddled = BackfillJobRunner.coarseCoverageWalk(
            plans: plans,
            windows: [window(0), firstHalf],
            failedWindows: [],
            unpersistedWindows: [secondHalf]
        )
        #expect(straddled.contiguousUpperBoundSec == 30.0)
        #expect(straddled.fullyCovered == false)
        #expect(straddled.unpersistedPlanIndices == [1])
        // The sibling that DID land still reads as a success — the plan is
        // disqualified for coverage, not erased from the attempt record, so the
        // end-of-pass `unattemptedPlans` denominator is unaffected.
        #expect(straddled.succeededPlanIndices == [0, 1])
        #expect(straddled.unattemptedPlans(from: plans).isEmpty)

        // The control: with both halves durable the same plans DO carry the
        // cursor past P1.
        let whole = BackfillJobRunner.coarseCoverageWalk(
            plans: plans,
            windows: [window(0), firstHalf, secondHalf],
            failedWindows: []
        )
        #expect(whole.contiguousUpperBoundSec == 90.0)
        #expect(whole.fullyCovered)
        #expect(whole.unpersistedPlanIndices.isEmpty)
    }

    /// The cursor must never pass where an UNCOVERED plan BEGINS, even when the
    /// covered prefix demonstrably runs past it.
    ///
    /// "Stop at the first uncovered plan in start-time order" and "everything
    /// below this timestamp is covered" are the same statement only while plan
    /// time ranges do not overlap. `planPassA` slices plans out of a list sorted
    /// by `segmentIndex` and takes each plan's bounds as the min/max over its
    /// segments — precisely because playhead-csbq measured that the atom
    /// sequence is NOT time-monotone on 27 of 30 device assets. So a covered
    /// plan can end long after an uncovered one begins.
    ///
    /// The consumer knows nothing about plans: `narrowedForResume` drops every
    /// segment whose `endTime <= cursor`. So an uncapped cursor of 600 here
    /// deletes both of P1's segments from the resume, no row covers 65–125, and
    /// nothing ever plans them again — permanent, silent, and invisible in every
    /// coverage number. The assertion below is the consumer's rule applied to
    /// this fixture's real segment times, not a magic number.
    @Test("the cursor never passes where an uncovered plan begins")
    func cursorNeverPassesAnUncoveredPlanStart() {
        // P0's segments are (0,30), (30,60), (60,600) — the third runs long.
        let covered = CoarsePassWindowPlan(
            windowIndex: 0,
            lineRefs: [0, 1, 2],
            prompt: "p0",
            promptTokenCount: 10,
            startTime: 0.0,
            endTime: 600.0,
            transcriptQuality: .good
        )
        // P1's segments are (65,95) and (95,125) — wholly inside P0's span.
        let uncovered = CoarsePassWindowPlan(
            windowIndex: 1,
            lineRefs: [3, 4],
            prompt: "p1",
            promptTokenCount: 10,
            startTime: 65.0,
            endTime: 125.0,
            transcriptQuality: .good
        )
        let coveredWindow = FMCoarseWindowOutput(
            windowIndex: 0,
            lineRefs: [0, 1, 2],
            startTime: 0.0,
            endTime: 600.0,
            transcriptQuality: .good,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )

        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: [covered, uncovered],
            windows: [coveredWindow],
            failedWindows: []
        )
        #expect(walk.fullyCovered == false)
        #expect(walk.contiguousUpperBoundSec == 65.0)
        // The consumer's rule, restated over the segments that have no row: a
        // resume must keep every one of them.
        let cursor = walk.contiguousUpperBoundSec ?? 0
        for unscannedSegmentEnd in [95.0, 125.0] {
            #expect(
                unscannedSegmentEnd > cursor,
                "a resume at cursor \(cursor) drops unscanned audio ending at \(unscannedSegmentEnd)"
            )
        }
    }

    /// The straddle at the head of the episode: the cursor must be UNSET, not
    /// merely short.
    ///
    /// Called out separately because it is the case with no safe fallback. Every
    /// other straddle leaves an earlier covered plan to fall back to, so an
    /// implementation that only half-applied the rule would still look sane;
    /// here the only honest answer is nil, and a cursor of 30 s would tell the
    /// next run to skip the first half of a plan it never durably screened.
    @Test("a straddle on the very first plan leaves the cursor unset")
    func straddleOnFirstPlanLeavesNoCursor() {
        let plans = [
            CoarsePassWindowPlan(
                windowIndex: 0,
                lineRefs: [0, 1],
                prompt: "p0",
                promptTokenCount: 10,
                startTime: 0.0,
                endTime: 60.0,
                transcriptQuality: .good
            ),
            plan(2)
        ]
        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: plans,
            windows: [window(0), window(2)],
            failedWindows: [],
            unpersistedWindows: [window(1)]
        )
        #expect(walk.contiguousUpperBoundSec == nil)
        #expect(walk.fullyCovered == false)
        #expect(walk.unpersistedPlanIndices == [0])
    }

    /// The MID-FLIGHT composition itself, not just the rule it applies.
    ///
    /// The walk cases above all hand-build their `unpersistedWindows`, so they
    /// pin the RULE and say nothing about whether the checkpoint supplies it.
    /// `coarseCheckpointWalk` is the one place that splits a banked snapshot
    /// into a durable prefix and an unpersisted tail, and dropping either half
    /// is invisible in every row a test can read: pass only the prefix and a
    /// plan whose sibling window is missing is credited; pass the whole list and
    /// the window that failed to write is credited. Both cases are asserted here
    /// from the same snapshot, so neither half can be deleted quietly.
    @Test("the mid-flight walk splits the snapshot at the durable count")
    func checkpointWalkSplitsAtTheDurableCount() {
        // One plan, two windows — the shape `runCoarseRetry` produces when a
        // shrunk plan is screened as two sub-prompts.
        let split = CoarsePassWindowPlan(
            windowIndex: 1,
            lineRefs: [1, 2],
            prompt: "p1",
            promptTokenCount: 10,
            startTime: 30.0,
            endTime: 90.0,
            transcriptQuality: .good
        )
        let banked = FMCoarseBankedWindows(
            plans: [plan(0), split, plan(3)],
            windows: [window(0), window(1), window(2)],
            failedWindows: []
        )

        // Everything landed: the split plan is covered and the cursor clears it.
        let allDurable = BackfillJobRunner.coarseCheckpointWalk(
            banked: banked,
            durableWindowCount: 3
        )
        #expect(allDurable.contiguousUpperBoundSec == 90.0)
        #expect(allDurable.unpersistedPlanIndices.isEmpty)

        // The second half of the split plan did not land. The TAIL is what says
        // so — the prefix alone still shows a window for plan 1.
        let straddled = BackfillJobRunner.coarseCheckpointWalk(
            banked: banked,
            durableWindowCount: 2
        )
        #expect(straddled.contiguousUpperBoundSec == 30.0)
        #expect(straddled.unpersistedPlanIndices == [1])
        #expect(straddled.succeededPlanIndices.contains(1))

        // And nothing at all landed: no cursor, and the PREFIX is what says so.
        let nothingDurable = BackfillJobRunner.coarseCheckpointWalk(
            banked: banked,
            durableWindowCount: 0
        )
        #expect(nothingDurable.contiguousUpperBoundSec == nil)
        #expect(nothingDurable.succeededPlanIndices.isEmpty)
    }

    /// An outcome with NO line refs attributes to NO plan.
    ///
    /// The rail is worth a test because the arithmetic runs the wrong way by
    /// default: attribution is `needle.isSubset(of: planRefs)`, and the empty set
    /// is a subset of every set — so an empty needle matches `plans.first`, i.e.
    /// windowIndex 0, the FIRST plan of the episode. A success carrying no refs
    /// would therefore certify the head of the episode as screened on the
    /// strength of an outcome that says nothing, and the contiguous walk starts
    /// from there: the cursor claims audio nobody looked at.
    ///
    /// No `FMCoarseWindowOutput` or `CoarseWindowFailure` constructor can emit
    /// one today — all four build from a plan's own refs — so this pins a rail
    /// against a refactor, not a live defect. The failure half is asserted too,
    /// because there the honest answer is not "nothing": `planWindowIndex` says
    /// which plan the attempt came from, and the fallback must still use it.
    ///
    /// **The ONE-PLAN case is what actually kills this guard, and it is asserted
    /// first for that reason.** With two or more plans the empty needle is a
    /// subset of ALL of them, so the ambiguity guard added alongside this one
    /// already returns nil and the empty guard is redundant. Exactly one plan is
    /// the only shape where the empty set has a UNIQUE superset — so a multi-plan
    /// fixture, which is what this test used to be, would keep passing with the
    /// empty guard deleted.
    @Test("an outcome with no line refs credits no plan")
    func emptyLineRefsAttributeToNothing() {
        let lonePlan = [plan(0)]
        let reflessAgainstOnePlan = FMCoarseWindowOutput(
            windowIndex: 0,
            lineRefs: [],
            startTime: 0.0,
            endTime: 0.0,
            transcriptQuality: .good,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )
        let single = BackfillJobRunner.coarseCoverageWalk(
            plans: lonePlan,
            windows: [reflessAgainstOnePlan],
            failedWindows: []
        )
        #expect(
            single.succeededPlanIndices.isEmpty,
            "the episode's only plan was certified as screened by an outcome that says nothing"
        )
        #expect(single.contiguousUpperBoundSec == nil)
        #expect(single.fullyCovered == false)

        let plans = (0..<3).map(plan)
        let refless = FMCoarseWindowOutput(
            windowIndex: 0,
            lineRefs: [],
            startTime: 0.0,
            endTime: 0.0,
            transcriptQuality: .good,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )

        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: plans,
            windows: [refless],
            failedWindows: []
        )
        #expect(walk.succeededPlanIndices.isEmpty, "an empty needle was credited to a plan")
        #expect(walk.contiguousUpperBoundSec == nil, "the cursor advanced on an outcome that says nothing")
        #expect(walk.fullyCovered == false)

        // A refless FAILURE still lands on the plan it came from, because
        // `CoarseWindowFailure` carries that plan's index explicitly.
        let reflessFailure = CoarseWindowFailure(
            planWindowIndex: 2,
            lineRefs: [],
            startTime: 60.0,
            endTime: 90.0,
            status: .rateLimited
        )
        let withFailure = BackfillJobRunner.coarseCoverageWalk(
            plans: plans,
            windows: [window(0), window(1)],
            failedWindows: [reflessFailure]
        )
        #expect(withFailure.failedPlanIndices == [2], "a refless failure lost its plan")
        #expect(withFailure.contiguousUpperBoundSec == 60.0)
        #expect(withFailure.fullyCovered == false)
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

    /// An AMBIGUOUS needle credits no plan — the sixth instance of this bead's
    /// recurring shape, and the one an empty needle's rail does not cover.
    ///
    /// The subset test identifies a plan only while the plans' line-ref sets are
    /// DISJOINT. `planPassA` partitions the segment LIST, which partitions the
    /// `segmentIndex` VALUES only while those values are unique — and the
    /// classifier deliberately does not require that (it builds its segment
    /// lookup with `uniquingKeysWith`, and
    /// `coarse pass tolerates duplicate segmentIndex without crashing` pins the
    /// tolerance with two segments both carrying index 0). Two segments sharing
    /// an index that fall either side of a packing boundary put the same line ref
    /// in two plans.
    ///
    /// The damage lands here, not there. `runCoarseRetry` banks one window per
    /// sub-prompt of a shrunk plan and a sub-prompt's refs are a strict SUBSET of
    /// its plan's, so a sub-window of the LATER plan is also a subset of the
    /// EARLIER plan's refs — and `first(where:)` credited the earlier one. Below,
    /// plan 0 was never attempted; crediting it would advance the cursor to 30
    /// and `narrowedForResume` would then delete plan 0's segments forever.
    @Test("an outcome matching two plans credits neither")
    func ambiguousLineRefsAttributeToNothing() {
        // Two plans sharing line ref 0 — the duplicate-`segmentIndex` shape.
        let shared = [
            CoarsePassWindowPlan(
                windowIndex: 0,
                lineRefs: [0],
                prompt: "p0",
                promptTokenCount: 10,
                startTime: 0.0,
                endTime: 30.0,
                transcriptQuality: .good
            ),
            CoarsePassWindowPlan(
                windowIndex: 1,
                lineRefs: [0, 1],
                prompt: "p1",
                promptTokenCount: 10,
                startTime: 30.0,
                endTime: 90.0,
                transcriptQuality: .good
            )
        ]
        // A shrunk sub-window of plan 1 covering only the shared ref. `{0}` is a
        // subset of BOTH plans' refs.
        let subWindow = FMCoarseWindowOutput(
            windowIndex: 0,
            lineRefs: [0],
            startTime: 30.0,
            endTime: 60.0,
            transcriptQuality: .good,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )

        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: shared,
            windows: [subWindow],
            failedWindows: []
        )
        #expect(
            walk.succeededPlanIndices.isEmpty,
            "an ambiguous needle was credited to \(walk.succeededPlanIndices) — the cursor now claims audio nobody screened"
        )
        #expect(walk.contiguousUpperBoundSec == nil)
        #expect(walk.fullyCovered == false)

        // A FAILURE resolves the ambiguity from its own `planWindowIndex`, which
        // every construction site fills with the OUTER plan's index — the shrunk
        // sub-plans carry `windowIndex: 0` and are never the source of it. So the
        // failure disqualifies plan 1, not plan 0, and the cursor stops at plan
        // 0's end rather than running past the hole.
        let ambiguousFailure = CoarseWindowFailure(
            planWindowIndex: 1,
            lineRefs: [0],
            startTime: 30.0,
            endTime: 60.0,
            status: .rateLimited
        )
        let wholePlanZero = FMCoarseWindowOutput(
            windowIndex: 0,
            lineRefs: [0],
            startTime: 0.0,
            endTime: 30.0,
            transcriptQuality: .good,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )
        let withFailure = BackfillJobRunner.coarseCoverageWalk(
            plans: shared,
            windows: [wholePlanZero],
            failedWindows: [ambiguousFailure]
        )
        #expect(withFailure.failedPlanIndices == [1], "an ambiguous failure disqualified the wrong plan")
        #expect(withFailure.contiguousUpperBoundSec == nil)
        #expect(withFailure.fullyCovered == false)

        // An UNPERSISTED ambiguous window resolves the other way: crediting is
        // conservative when it credits nobody, but disqualifying is conservative
        // only when it disqualifies EVERYBODY. Here plan 1 is credited by an
        // unambiguous window covering ref 1, while an ambiguous sibling covering
        // ref 0 never reached the store — so plan 1 is only PARTLY persisted and
        // the cursor must not be issued over it.
        let unambiguousForPlanOne = FMCoarseWindowOutput(
            windowIndex: 0,
            lineRefs: [1],
            startTime: 60.0,
            endTime: 90.0,
            transcriptQuality: .good,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )
        let withUnpersisted = BackfillJobRunner.coarseCoverageWalk(
            plans: shared,
            windows: [unambiguousForPlanOne],
            failedWindows: [],
            unpersistedWindows: [subWindow]
        )
        #expect(
            withUnpersisted.unpersistedPlanIndices == [0, 1],
            "an ambiguous unpersisted window disqualified \(withUnpersisted.unpersistedPlanIndices) — it must disqualify every plan it could belong to"
        )
        #expect(
            withUnpersisted.contiguousUpperBoundSec == nil,
            "the cursor was issued over a plan whose sibling window never reached the store"
        )
        #expect(withUnpersisted.fullyCovered == false)
    }

    /// The no-op half: with distinct line refs a needle has at most one superset,
    /// so requiring a UNIQUE match changes nothing on any healthy episode.
    ///
    /// Stated as its own case because "credit nobody when it is ambiguous" is one
    /// `guard` away from "credit nobody, ever", and that mutation would leave the
    /// case above green while silently retiring the whole coverage cursor.
    @Test("a strict subset of exactly one plan still credits that plan")
    func unambiguousSubsetStillCredits() {
        let plans = [
            CoarsePassWindowPlan(
                windowIndex: 0,
                lineRefs: [0, 1],
                prompt: "p0",
                promptTokenCount: 10,
                startTime: 0.0,
                endTime: 60.0,
                transcriptQuality: .good
            ),
            CoarsePassWindowPlan(
                windowIndex: 1,
                lineRefs: [2, 3],
                prompt: "p1",
                promptTokenCount: 10,
                startTime: 60.0,
                endTime: 120.0,
                transcriptQuality: .good
            )
        ]
        // The shrink case that MUST keep working: one sub-prompt of plan 0.
        let subWindow = FMCoarseWindowOutput(
            windowIndex: 0,
            lineRefs: [1],
            startTime: 30.0,
            endTime: 60.0,
            transcriptQuality: .good,
            screening: CoarseScreeningSchema(disposition: .noAds, support: nil),
            latencyMillis: 1.0
        )
        let walk = BackfillJobRunner.coarseCoverageWalk(
            plans: plans,
            windows: [subWindow],
            failedWindows: []
        )
        #expect(walk.succeededPlanIndices == [0], "a unique subset match stopped crediting its plan")
        #expect(walk.contiguousUpperBoundSec == 60.0)
    }
}
