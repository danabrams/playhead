// BackfillJobRunnerTests.swift
// Phase 3 shadow-mode runner. These tests pin the orchestration contract:
// plan -> enqueue via AdmissionController -> run FM coarse pass -> persist
// SemanticScanResult / EvidenceEvent rows. None of the tests boot the real
// Foundation Models stack; they use TestFMRuntime.

import Foundation
import Testing

@testable import Playhead

@Suite("BackfillJobRunner")
struct BackfillJobRunnerTests {

    // MARK: - Fixtures

    private func makeAsset(
        id: String = "asset-runner",
        episodeDurationSec: Double? = nil,
        fastTranscriptCoverageEndTime: Double? = nil
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    }

    /// playhead-15d0 R4: `plannerContext` defaults to `nil`, which means "the
    /// cold-start context this fixture has always used" — every pre-existing
    /// caller is byte-identical. It is overridable so a test can pin a policy
    /// OTHER than `fullCoverage` against this same three-line transcript; see
    /// `periodicFullRescanIsNarrowedByItsOwnDurableRows`.
    private func makeInputs(
        assetId: String = "asset-runner",
        podcastId: String = "podcast-runner",
        transcriptVersion: String = "tx-runner-v1",
        plannerContext: CoveragePlannerContext? = nil
    ) -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 30, "Welcome to the show. Today we're discussing podcasts."),
                (30, 60, "Use code SHOW for 20 percent off at example dot com."),
                (60, 90, "Now back to the interview with our guest.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion
        )
        let coldStartContext = CoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: podcastId,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: transcriptVersion,
            plannerContext: plannerContext ?? coldStartContext
        )
    }

    private func makeTargetedInputs(
        assetId: String = "asset-targeted",
        podcastId: String = "podcast-targeted",
        transcriptVersion: String = "tx-targeted-v1",
        plannerContext: CoveragePlannerContext
    ) -> BackfillJobRunner.AssetInputs {
        // Cycle 2 C5: the per-anchor narrowing model uses padding=5 by
        // default, so a 5-segment-wide window centered on every anchor
        // covers ~11 segments. The legacy 8-segment fixture is smaller
        // than that envelope, which made every "narrowed" phase devolve
        // back to the full transcript and broke the strict-subset
        // invariant this test pins. Use a 30-segment fixture with the
        // ad lines clustered near the middle so the narrowed envelope
        // is meaningfully smaller than the full transcript.
        var lines: [(Double, Double, String)] = []
        for idx in 0..<30 {
            let start = Double(idx) * 10.0
            let text: String
            switch idx {
            case 12:
                text = "Before we continue, this episode is brought to you by ExampleCo."
            case 13:
                text = "Visit example.com slash deal and use promo code PLAYHEAD."
            default:
                text = "Editorial line \(idx) about the topic of the day."
            }
            lines.append((start, start + 10.0, text))
        }
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: podcastId,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: transcriptVersion,
            plannerContext: plannerContext
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime,
        snapshot: CapabilitySnapshot = makePermissiveCapabilitySnapshot(),
        mode: FMBackfillMode = .shadow,
        classifierConfig: FoundationModelClassifier.Config = .default,
        // playhead-hvk0: defaults to the PRE-hvk0 behaviour so every existing
        // test in this suite keeps its meaning; the gate's own tests opt in.
        plannerPromotionRequiresMeasuredCoverage: Bool = false
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime, config: classifierConfig),
            coveragePlanner: CoveragePlanner(),
            mode: mode,
            capabilitySnapshotProvider: { snapshot },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            plannerPromotionRequiresMeasuredCoverage: plannerPromotionRequiresMeasuredCoverage
        )
    }

    // MARK: - Tests

    @Test("off mode runs no FM jobs and writes nothing")
    func offModeIsNoOp() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            mode: .off
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(result.admittedJobIds.isEmpty)
        #expect(result.scanResultIds.isEmpty)
        #expect(result.evidenceEventIds.isEmpty)
        #expect(await fmRuntime.coarseCallCount == 0)
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(scans.isEmpty)
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(evidence.isEmpty)
    }

    @Test("shadow mode admits planned jobs, runs FM, persists scan results")
    func shadowModePersistsResults() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        #expect(!result.scanResultIds.isEmpty)
        let coarseCalls = await fmRuntime.coarseCallCount
        #expect(coarseCalls >= 1)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(!scans.isEmpty)
        #expect(scans.allSatisfy { $0.scanPass == "passA" || $0.scanPass == "passB" })

        // playhead-qjcf (V66): THE WRITE PATH, BEHAVIOURALLY. The fixture above
        // returns `supportLineRefs: [1]`, so the coarse row this pass persists
        // must carry the SECONDS line 1 covered in the segmentation the model
        // was shown — projected by `makeScanResult`, carried through
        // `attributed`, and bound by `insertSemanticScanResult`.
        //
        // It lives here rather than in `SupportLineSecondsTests` because
        // `makeScanResult` is `private` and this is the nearest test that
        // already drives the whole chain into the store. Without it the ONLY
        // guard on "the writer actually projects" was a `contains` over the
        // source text, and the QJ05 mutant that deletes the projection had
        // exactly one victim: that substring match. A source canary is the right
        // instrument for "the call site is spelled"; it is the wrong one for
        // "the value reaches disk", and this bead's whole subject is the second.
        let coarseWithRefs = scans.filter {
            $0.scanPass == "passA" && $0.disposition == .containsAd
        }
        #expect(!coarseWithRefs.isEmpty, "precondition: the fixture must produce a coarse hit")
        for row in coarseWithRefs {
            let projected = SupportLineIndex.decodeSupportLineSpans(row.supportLineSpansJSON)
            #expect(projected?.map(\.lineRef) == [1],
                    "the coarse row must record the seconds its supportLineRefs named")
            #expect(projected?.allSatisfy { $0.end > $0.start } == true)
        }
        // Shadow mode never inserts AdWindows. Two independent reasons now,
        // and the second is playhead-y3ya's: `TestFMRuntime` falls back to
        // `.noAds` once its one queued `containsAd` response is drained — so
        // this fixture composes at most one verdict, and the block above is
        // what asserts that verdict exists (the older wording said the fixture
        // "has no verdict to compose", which stopped being true when a
        // `containsAd` response was queued); AND the
        // semantic-sweep compose is gated on `canProposeNewRegions`, which
        // `.shadow` is not. The sibling `phase6ModesPersistWithoutAdWindowWrites`
        // is where the gate itself is exercised against a `containsAd` fixture.
        let windows = try await store.fetchAdWindows(assetId: "asset-runner")
        #expect(windows.isEmpty)
    }

    @Test("playhead-nlh: targeted phases persist distinct passA rows on narrowed subsets")
    func targetedPhasesRunOnNarrowedSubsets() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-targeted-narrow"
        try await store.insertAsset(makeAsset(id: assetId))

        let runtime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: runtime.runtime)
        let targetedContext = CoveragePlannerContext(
            observedEpisodeCount: 20,
            stableRecall: true,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 1,
            periodicFullRescanIntervalEpisodes: 10
        )
        let inputs = makeTargetedInputs(
            assetId: assetId,
            podcastId: "podcast-targeted-narrow",
            transcriptVersion: "tx-targeted-narrow-v1",
            plannerContext: targetedContext
        )

        let result = try await runner.runPendingBackfill(for: inputs)

        #expect(result.admittedJobIds.count == 3, "targetedWithAudit should admit one job per targeted phase")
        let passA = try await store.fetchSemanticScanResults(
            analysisAssetId: assetId,
            scanPass: "passA"
        )
        #expect(passA.count == 3, "targeted phases must persist distinct passA rows (no cross-phase row collisions)")

        let fullFirst = try #require(inputs.segments.first?.firstAtomOrdinal)
        let fullLast = try #require(inputs.segments.last?.lastAtomOrdinal)
        let fullWidth = fullLast - fullFirst
        let fullLineRefs = Set(inputs.segments.map(\.segmentIndex))
        let submittedLineRefs = await runtime.snapshotSubmittedCoarseLineRefs()
        #expect(submittedLineRefs.count == 3, "targeted mode should submit one coarse window per targeted phase for this fixture")

        for refs in submittedLineRefs {
            let scannedSet = Set(refs)
            #expect(!refs.isEmpty)
            #expect(scannedSet.isSubset(of: fullLineRefs))
            #expect(scannedSet.count < fullLineRefs.count, "targeted phase should submit a strict subset of transcript lines")
        }
        // playhead-xsdz.3: the audit phase (`.scanRandomAuditWindows`) is no
        // longer guaranteed contiguous. It now points its budget at the
        // lexically-nominated ad-likely regions (here the segment-12/13 sponsor
        // cluster) and tops up any remaining budget with a random block — a
        // UNION that is intentionally non-contiguous (e.g. [4, 12, 13, 14]).
        // The harvester / likely-ad-slot phases still produce contiguous
        // per-window envelopes for this fixture, so assert that AT LEAST the
        // two anchor-driven phases submit a contiguous envelope while leaving
        // the audit phase free to be the nominated union.
        let contiguousCount = submittedLineRefs.filter { refs in
            let sorted = refs.sorted()
            guard let lo = sorted.first, let hi = sorted.last else { return false }
            return sorted == Array(lo...hi)
        }.count
        #expect(
            contiguousCount >= 2,
            "the anchor-driven phases should submit contiguous envelopes; the audit phase may be a nominated union, got submissions \(submittedLineRefs.map { $0.sorted() })"
        )

        for row in passA {
            #expect(row.windowFirstAtomOrdinal >= fullFirst)
            #expect(row.windowLastAtomOrdinal <= fullLast)
            #expect(
                row.windowLastAtomOrdinal - row.windowFirstAtomOrdinal < fullWidth,
                "targeted phase should scan a strict subset, got full-episode range \(row.windowFirstAtomOrdinal)-\(row.windowLastAtomOrdinal)"
            )
        }
    }

    /// playhead-15d0 R2: a TARGETED phase must not be narrowed by its OWN
    /// durable rows on a re-drive.
    ///
    /// **This test exists because the guard it pins had NO rail.** R2 mutation
    /// M1 deleted the call-site guard in `runJob`
    /// (`job.phase != .fullEpisodeScan → screenedRows = []`) outright, and the
    /// whole 82-test scoped gate stayed GREEN. The row-side clause R1 added
    /// cannot stand in for it: `screenedSpans` refuses a job the rows of
    /// ANOTHER phase, and here the phase is its OWN — `row.jobPhase ==
    /// jobPhase.rawValue` matches, every other clause matches, and the phase's
    /// entire narrowed subset vanishes.
    ///
    /// Why that loses audio rather than saving work.
    /// `TargetedWindowNarrower.narrow` hands the classifier the union of
    /// DISJOINT per-anchor intervals, and `planPassA` packs each window from a
    /// contiguous slice of that sparse array while stamping
    /// `startTime = min(...)`, `endTime = max(...)` over the slice. One window
    /// over a non-contiguous subset therefore persists a span covering every
    /// segment BETWEEN the intervals — segments no prompt ever contained.
    /// Crediting that span on the next grant deletes audio nobody read, and for
    /// `.scanRandomAuditWindows` it additionally corrupts the population the
    /// audit exists to measure.
    ///
    /// The assertion is over the LINE REFS the classifier was handed, for the
    /// reason `growingTranscriptContinuesTheScan` spells out at length:
    /// scan-result ids are deterministic, so a re-drive that re-read the same
    /// windows writes rows that dedupe onto the first run's. The persisted row
    /// set, the row count and the job count are all IDENTICAL either way;
    /// `TestFMRuntime`'s prompt log is the only place the difference is visible.
    @Test("playhead-15d0: a targeted phase's re-drive is narrowed by nothing — not even its own rows")
    func targetedPhaseReDriveIsNotNarrowedByItsOwnRows() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-targeted-redrive"
        try await store.insertAsset(makeAsset(id: assetId))

        let runtime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: runtime.runtime)
        let targetedContext = CoveragePlannerContext(
            observedEpisodeCount: 20,
            stableRecall: true,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 1,
            periodicFullRescanIntervalEpisodes: 10
        )
        let inputs = makeTargetedInputs(
            assetId: assetId,
            podcastId: "podcast-targeted-redrive",
            transcriptVersion: "tx-targeted-redrive-v1",
            plannerContext: targetedContext
        )

        let first = try await runner.runPendingBackfill(for: inputs)
        #expect(first.admittedJobIds.count == 3, "targetedWithAudit should admit one job per targeted phase")
        let refsAfterFirst = await runtime.snapshotSubmittedCoarseLineRefs()
        #expect(!refsAfterFirst.isEmpty, "run 1 must submit coarse windows, or this rail is vacuous")

        // The rows run 1 banked must be REUSABLE under run 2's predicate.
        // Without this the mutant could survive for a reason that has nothing
        // to do with the guard — a stale cohort or an unexamined status would
        // refuse them anyway and the test would pass while proving nothing.
        let rowsAfterFirst = try await store.fetchSemanticScanResults(
            analysisAssetId: assetId,
            scanPass: SemanticScanResult.presenceScanPass
        )
        #expect(!rowsAfterFirst.isEmpty, "run 1 must bank passA rows, or this rail is vacuous")
        for row in rowsAfterFirst {
            #expect(
                row.didExamineWindow,
                "row \(row.id) is not examined, so the phase clause is not what would refuse it"
            )
            #expect(
                row.isReusable(
                    scanCohortJSON: makeTestScanCohortJSON(),
                    transcriptVersion: inputs.transcriptVersion
                ),
                "row \(row.id) is not reusable, so the phase clause is not what would refuse it"
            )
            #expect(
                row.windowEndTime > row.windowStartTime,
                "row \(row.id) is degenerate, so the phase clause is not what would refuse it"
            )
        }

        // Re-drive the same three jobs. `.queued` with no cursor is the
        // orphan-recovery shape: rows banked, job never marked complete.
        for jobId in first.admittedJobIds {
            try await store.forceBackfillJobStateForTesting(
                jobId: jobId,
                status: .queued,
                progressCursor: nil
            )
        }

        let second = try await runner.runPendingBackfill(for: inputs)
        #expect(
            Set(second.admittedJobIds) == Set(first.admittedJobIds),
            "run 2 must re-drive the same three targeted jobs, or this rail is vacuous"
        )

        let resubmitted = Array(
            (await runtime.snapshotSubmittedCoarseLineRefs()).dropFirst(refsAfterFirst.count)
        )
        #expect(
            resubmitted.count == refsAfterFirst.count,
            """
            the targeted re-drive submitted \(resubmitted.count) coarse windows where run 1 \
            submitted \(refsAfterFirst.count). A targeted phase was narrowed by its own rows — \
            whose spans cover the gaps BETWEEN its disjoint per-anchor intervals, so the audio \
            in those gaps is now deleted from every future attempt without ever having been read.
            """
        )
        let shape: ([[Int]]) -> [[Int]] = { refs in
            refs.map { $0.sorted() }.sorted { $0.lexicographicallyPrecedes($1) }
        }
        #expect(
            shape(resubmitted) == shape(refsAfterFirst),
            "the re-drive submitted \(shape(resubmitted)) where run 1 submitted \(shape(refsAfterFirst))"
        )
    }

    /// playhead-15d0 R4: **THE CALL-SITE GUARD READS `phase`, AND NOT
    /// `coveragePolicy`.** This is the other direction of
    /// `targetedPhaseReDriveIsNotNarrowedByItsOwnRows` above — that one pins
    /// which jobs the narrowing must REFUSE, this one pins which it must SERVE.
    ///
    /// **Why it exists.** R3 mutation M-A rewrote the guard in `runJob` from
    /// `job.phase != .fullEpisodeScan` to `job.coveragePolicy != .fullCoverage`
    /// and SURVIVED — the whole 125-test scoped gate stayed green. The two
    /// predicates agree on `fullCoverage` and on all three `targetedWithAudit`
    /// phases, so nothing that existed could tell them apart. They disagree on
    /// exactly one plan, and `CoveragePlanner.plan` really does emit it:
    /// `policy: .periodicFullRescan, phases: [.fullEpisodeScan]`. Under the
    /// mutant that job reads NO rows, is narrowed by nothing, and re-infers
    /// audio it already banked — 15d0 goes inert on the one policy whose entire
    /// job is to re-scan a whole episode, which is also the policy that runs
    /// most often against an asset that already has durable rows.
    ///
    /// **The consequence is a lost SAVING, not lost audio**, and that is why R3
    /// left the production code alone: the mutant cannot strand audio, advance a
    /// cursor or corrupt a population. The rail is owed anyway. A guard whose
    /// mutation nobody notices is a guard a future author will "simplify" to the
    /// policy spelling — which reads more naturally than the phase spelling, and
    /// is wrong.
    ///
    /// **The cursor is cleared on purpose.** With `progressCursor: nil` the
    /// `narrowedForResume` half of the composition is an identity, so the ROWS
    /// are the only thing that can suppress run 2's windows and the assertion
    /// cannot be satisfied by the cursor doing the work.
    ///
    /// The assertion is over the LINE REFS the classifier was handed, for the
    /// reason `growingTranscriptContinuesTheScan` spells out: scan-result ids
    /// are deterministic, so a re-drive that re-read the same windows writes
    /// rows that dedupe onto run 1's. `TestFMRuntime`'s prompt log is the only
    /// place the difference is visible.
    @Test("playhead-15d0: a PERIODIC FULL RESCAN is narrowed by its own rows — the guard reads phase, not policy")
    func periodicFullRescanIsNarrowedByItsOwnDurableRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: (0..<20).map { _ in
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            }
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        // `shouldUseFullCoverage` false (well past the cold-start threshold, no
        // cohort invalidation, no recall degradation, no audit miss) AND
        // `shouldUsePeriodicFullRescan` true (the interval has elapsed) is the
        // one context in which the policy and the phase disagree.
        let rescanContext = CoveragePlannerContext(
            observedEpisodeCount: 20,
            stableRecall: true,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 10,
            periodicFullRescanIntervalEpisodes: 10
        )
        let plan = CoveragePlanner().plan(for: rescanContext)
        #expect(
            plan.policy == .periodicFullRescan,
            "the fixture must actually produce a periodic full rescan, or this rail is vacuous"
        )
        #expect(
            plan.phases == [.fullEpisodeScan],
            """
            the whole discriminating power of this rail is that a
            `.periodicFullRescan` plan runs a `.fullEpisodeScan` phase. If the
            planner stops emitting that pair, M-A becomes equivalent and this
            test proves nothing — fix the rail, do not delete it.
            """
        )

        let inputs = makeInputs(
            transcriptVersion: "tx-periodic-rescan",
            plannerContext: rescanContext
        )
        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-runner",
            phase: .fullEpisodeScan,
            offset: 0
        )

        let first = try await runner.runPendingBackfill(for: inputs)
        #expect(first.admittedJobIds == [jobId], "run 1 must admit the rescan job, or this rail is vacuous")
        let refsAfterFirst = await fmRuntime.snapshotSubmittedCoarseLineRefs()
        #expect(!refsAfterFirst.isEmpty, "run 1 must submit coarse windows, or this rail is vacuous")

        // The persisted job is what the guard reads, and its two fields must
        // genuinely disagree here — otherwise M-A is equivalent on this fixture.
        let job = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(job.phase == .fullEpisodeScan)
        #expect(
            job.coveragePolicy == .periodicFullRescan,
            "the stamped policy must NOT be `fullCoverage`, or the mutant's predicate agrees with the real one"
        )

        // Run 1's rows must be REUSABLE under run 2's predicate, or the mutant
        // could survive for a reason that has nothing to do with the guard.
        let rowsAfterFirst = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: SemanticScanResult.presenceScanPass
        )
        let usableRows = rowsAfterFirst.filter { row in
            row.didExamineWindow
                && row.isReusable(
                    scanCohortJSON: makeTestScanCohortJSON(),
                    transcriptVersion: inputs.transcriptVersion
                )
                && row.windowEndTime > row.windowStartTime
                && row.jobPhase == BackfillJobPhase.fullEpisodeScan.rawValue
        }
        #expect(
            !usableRows.isEmpty,
            "run 1 must bank a reusable, examined passA row for this phase, or this rail is vacuous"
        )

        // Rows banked, job never marked complete, and NO CURSOR: the
        // orphan-recovery shape, with the cursor's half of the composition
        // deliberately neutralised.
        try await store.forceBackfillJobStateForTesting(
            jobId: jobId,
            status: .queued,
            progressCursor: nil
        )

        let second = try await runner.runPendingBackfill(for: inputs)
        #expect(second.admittedJobIds == [jobId], "run 2 must re-drive the same job, or this rail is vacuous")

        let resubmitted = Array(
            (await fmRuntime.snapshotSubmittedCoarseLineRefs()).dropFirst(refsAfterFirst.count)
        )
        #expect(
            resubmitted.isEmpty,
            """
            the periodic-full-rescan re-drive submitted \(resubmitted) — audio its OWN durable \
            rows had already screened. The cursor is nil, so the row-side narrowing is the only \
            thing that can suppress those windows, and it is reached only when the call-site \
            guard in `runJob` tests the job's PHASE. A guard written over `coveragePolicy` \
            waves `.periodicFullRescan` through as "not full coverage", reads no rows, and \
            leaves playhead-15d0 inert on this policy.
            """
        )
    }

    // Cycle 10 Rev3-M5: production-path rail for the `runMode` discriminator.
    //
    // The schema column, struct field, and decoder were wired in cycle 2, but
    // until cycle 10 no production call site in `BackfillJobRunner` ever
    // passed `runMode: .targeted` — every row defaulted to `.shadow`, making
    // the column dead storage. This test drives the runner end-to-end under a
    // `targetedWithAudit` plan and asserts that the persisted scan-result +
    // evidence-event rows carry `.targeted` so a query like
    // `WHERE runMode = 'targeted'` returns the rows the planner produced.
    //
    // Mapping: `runMode = .targeted` iff `job.coveragePolicy == .targetedWithAudit`.
    // This matches the original Rev3-M5 intent (distinguish Phase 3 shadow
    // validation rows from Phase 5 targeted execution rows via planner policy,
    // not via FMBackfillMode). A non-targeted plan (fullCoverage) must write
    // `.shadow` regardless of FMBackfillMode so existing readers stay stable.
    @Test("cycle10 Rev3-M5: runner writes runMode=targeted under targetedWithAudit plans")
    func targetedPlanWritesTargetedRunMode() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-runmode-targeted"
        try await store.insertAsset(makeAsset(id: assetId))

        // Seed the refinement path so at least one passB row + evidence row
        // gets persisted — we want coverage on both tables, not just passA.
        let spanSchema = SpanRefinementSchema(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: 12,
            lastLineRef: 13,
            certainty: .strong,
            boundaryPrecision: .precise,
            evidenceAnchors: [
                EvidenceAnchorSchema(
                    evidenceRef: nil,
                    lineRef: 12,
                    kind: .ctaPhrase,
                    certainty: .strong
                )
            ],
            alternativeExplanation: .none,
            reasonTags: [.callToAction]
        )
        let runtime = TestFMRuntime(
            coarseResponses: (0..<4).map { _ in
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [12],
                        certainty: .strong
                    )
                )
            },
            refinementResponses: (0..<4).map { _ in
                RefinementWindowSchema(spans: [spanSchema])
            }
        )
        let runner = makeRunner(store: store, runtime: runtime.runtime)
        let targetedContext = CoveragePlannerContext(
            observedEpisodeCount: 20,
            stableRecall: true,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 1,
            periodicFullRescanIntervalEpisodes: 10
        )
        // Sanity: the planner under this context must actually produce the
        // targetedWithAudit policy — otherwise the test would be green against
        // a shadow-only run and provide no coverage.
        let plan = CoveragePlanner().plan(for: targetedContext)
        #expect(plan.policy == .targetedWithAudit)

        let inputs = makeTargetedInputs(
            assetId: assetId,
            podcastId: "podcast-runmode-targeted",
            transcriptVersion: "tx-runmode-targeted-v1",
            plannerContext: targetedContext
        )

        let result = try await runner.runPendingBackfill(for: inputs)
        #expect(!result.scanResultIds.isEmpty)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(!scans.isEmpty, "targeted plan should persist at least one scan result row")

        // Every row written under a targetedWithAudit plan must carry
        // runMode = .targeted. Zero rows with the default .shadow are
        // permitted — the planner policy is the discriminator and every
        // job in this plan was admitted under that policy.
        let targetedScans = scans.filter { $0.runMode == .targeted }
        let shadowScans = scans.filter { $0.runMode == .shadow }
        #expect(
            targetedScans.count == scans.count,
            "all \(scans.count) rows should carry runMode=.targeted, got \(targetedScans.count) targeted / \(shadowScans.count) shadow"
        )
        #expect(shadowScans.isEmpty, "no rows should fall through to the default .shadow under a targetedWithAudit plan")

        // Evidence events produced by passB under the targeted plan must
        // also be tagged targeted so `WHERE runMode = 'targeted'` queries
        // on `evidence_events` return them.
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        if !evidence.isEmpty {
            let targetedEvidence = evidence.filter { $0.runMode == .targeted }
            #expect(
                targetedEvidence.count == evidence.count,
                "all \(evidence.count) evidence rows should carry runMode=.targeted, got \(targetedEvidence.count)"
            )
        }
    }

    // Cycle 10 Rev3-M5: the non-targeted control rail. A fullCoverage plan
    // (the default Phase 3 shadow-validation path) must keep writing
    // runMode=.shadow so existing shadow-mode consumers and the Rev3-M5
    // store-level round-trip rail stay green. This pins the semantics of
    // the mapping: `.targeted` only when the PLANNER policy is
    // `.targetedWithAudit`, independent of FMBackfillMode.
    @Test("cycle10 Rev3-M5: runner writes runMode=shadow under fullCoverage plans")
    func fullCoveragePlanWritesShadowRunMode() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let inputs = makeInputs()
        // Sanity: default plannerContext must still map to fullCoverage so
        // this test exercises the control rail rather than silently
        // following the same path as the targeted test above.
        let plan = CoveragePlanner().plan(for: inputs.plannerContext)
        #expect(plan.policy == .fullCoverage)

        _ = try await runner.runPendingBackfill(for: inputs)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(!scans.isEmpty)
        #expect(
            scans.allSatisfy { $0.runMode == .shadow },
            "fullCoverage plan must write runMode=.shadow for every row"
        )
    }

    @Test("playhead-nlh: full-rescan path records non-nil precision samples and unlocks targetedWithAudit")
    func fullRescanPersistsPrecisionSamplesAndUnlocksTargetedCoverage() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-planner-live"
        // Cycle 2 C5: under the new makeTargetedInputs fixture the ad
        // copy lives at line refs 12-13 (so the harvester anchors land
        // there). The fake coarse FM must report a support line ref that
        // overlaps the narrower's predicted window so the recall sample
        // is non-zero.
        let coarseResponses = (0..<5).map { _ in
            CoarseScreeningSchema(
                disposition: .containsAd,
                support: CoarseSupportSchema(
                    supportLineRefs: [12],
                    certainty: .strong
                )
            )
        }
        let runtime = TestFMRuntime(coarseResponses: coarseResponses)
        let runner = makeRunner(store: store, runtime: runtime.runtime)

        for episode in 1...5 {
            let assetId = "asset-planner-live-\(episode)"
            try await store.insertAsset(makeAsset(id: assetId))

            let state = try await store.fetchPodcastPlannerState(podcastId: podcastId)
            let context = CoveragePlannerContext(
                observedEpisodeCount: state?.observedEpisodeCount ?? 0,
                stableRecall: state?.stableRecallFlag ?? false,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: state?.episodesSinceLastFullRescan ?? 0,
                periodicFullRescanIntervalEpisodes: 10
            )

            let inputs = makeTargetedInputs(
                assetId: assetId,
                podcastId: podcastId,
                transcriptVersion: "tx-planner-live-v\(episode)",
                plannerContext: context
            )
            _ = try await runner.runPendingBackfill(for: inputs)
        }

        let finalState = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(finalState.observedEpisodeCount == 5)
        #expect(!finalState.recallSamples.isEmpty, "live full-rescan path should persist precision samples")
        #expect(finalState.stableRecallFlag, "stable precision should flip true once sample and episode thresholds are met")

        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: finalState.observedEpisodeCount,
            stableRecall: finalState.stableRecallFlag,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: finalState.episodesSinceLastFullRescan,
            periodicFullRescanIntervalEpisodes: 10
        )
        let plan = CoveragePlanner().plan(for: plannerContext)
        #expect(plan.policy == .targetedWithAudit)
    }

    // MARK: - playhead-hvk0: the promotion gate, driven through the live runner

    /// playhead-hvk0: builds the `CoveragePlannerContext` the production shadow
    /// phase would build from persisted planner state, so these tests exercise
    /// the real translation rather than a parallel one.
    private func liveContext(_ state: PodcastPlannerState?) -> CoveragePlannerContext {
        CoveragePlannerContext(
            observedEpisodeCount: state?.observedEpisodeCount ?? 0,
            stableRecall: state?.stableRecallFlag ?? false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: state?.episodesSinceLastFullRescan ?? 0,
            periodicFullRescanIntervalEpisodes: 10
        )
    }

    /// playhead-hvk0: a coverage-lane row asserting that some earlier pass
    /// already read `[start, end]` of the asset. Written under a DIFFERENT
    /// `transcriptVersion` than the run under test so it can never be picked up
    /// as a reusable result — it exists only to move the measured
    /// `adScanFraction` the gate reads.
    private func makeReadEvidenceScanRow(
        assetId: String,
        start: Double,
        end: Double
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "\(assetId)-prior-read",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 999,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: SemanticScanCoverage.coverageScanPass,
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: makeTestScanCohortJSON(),
            transcriptVersion: "tx-prior-read-only",
            reuseScope: "\(assetId)-prior-read"
        )
    }

    /// The device shape this bead exists for. `fullRescanPersistsPrecisionSamples…`
    /// above is the same five-episode drive with the gate OFF and is the
    /// pre-hvk0 parity rail; this is the same drive with the gate ON, over
    /// episodes whose full rescans do not read them.
    ///
    /// Measured on the 2026-07-29 device pull: every `fullEpisodeScan` on
    /// `feeds.simplecast.com/dHoohVNH` examined 82–167 s of episodes 1,503–4,379 s
    /// long, and three 1.0 recall samples from those runs promoted the show.
    @Test("playhead-hvk0: full rescans that do not read the episode never promote the show")
    func shortFullRescansDoNotPromoteShow() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-hvk0-short"
        let coarseResponses = (0..<40).map { _ in
            CoarseScreeningSchema(
                disposition: .containsAd,
                support: CoarseSupportSchema(supportLineRefs: [12], certainty: .strong)
            )
        }
        let runtime = TestFMRuntime(coarseResponses: coarseResponses)
        let runner = makeRunner(
            store: store,
            runtime: runtime.runtime,
            plannerPromotionRequiresMeasuredCoverage: true
        )

        for episode in 1...5 {
            let assetId = "asset-hvk0-short-\(episode)"
            // The fixture transcribes 0–300 s; the episode is 3,000 s long. The
            // rescan can read at most ~10% of it, which is the device ratio.
            try await store.insertAsset(
                makeAsset(id: assetId, episodeDurationSec: 3000, fastTranscriptCoverageEndTime: 3000)
            )
            let state = try await store.fetchPodcastPlannerState(podcastId: podcastId)
            _ = try await runner.runPendingBackfill(
                for: makeTargetedInputs(
                    assetId: assetId,
                    podcastId: podcastId,
                    transcriptVersion: "tx-hvk0-short-v\(episode)",
                    plannerContext: liveContext(state)
                )
            )
            // Prove the premise rather than assuming it: the run really was a
            // full rescan that really did fall short.
            let fraction = try await store
                .fetchCoverageSummariesByAssetIds([assetId])[assetId]?.adScanFraction
            let measured = try #require(fraction, "episode \(episode) must have a MEASURED fraction")
            #expect(
                measured < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction,
                "episode \(episode) measured \(measured) — fixture no longer models a short rescan"
            )
        }

        let finalState = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(finalState.observedEpisodeCount == 5)
        #expect(
            finalState.recallSamples.isEmpty,
            "a rescan that did not read its episode must not enter the recall ring"
        )
        #expect(finalState.stableRecallFlag == false)
        #expect(CoveragePlanner().plan(for: liveContext(finalState)).policy == .fullCoverage)
    }

    /// UNMEASURABLE is not the same as sufficient. An asset with no declared
    /// duration (legacy rows, placeholder rows pre-decode) yields a `nil`
    /// `adScanFraction`, and a rescan whose read cannot be measured has not
    /// demonstrated anything. Under-claiming routes the next episode to
    /// `fullCoverage` — more audio read, never less.
    ///
    /// This is the exact fixture `fullRescanPersistsPrecisionSamples…` uses
    /// (assets with no duration), so the two tests differ ONLY in the gate.
    @Test("playhead-hvk0: an unmeasurable rescan does not certify the targeted policy")
    func unmeasurableRescanDoesNotPromoteShow() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-hvk0-unmeasurable"
        let coarseResponses = (0..<40).map { _ in
            CoarseScreeningSchema(
                disposition: .containsAd,
                support: CoarseSupportSchema(supportLineRefs: [12], certainty: .strong)
            )
        }
        let runtime = TestFMRuntime(coarseResponses: coarseResponses)
        let runner = makeRunner(
            store: store,
            runtime: runtime.runtime,
            plannerPromotionRequiresMeasuredCoverage: true
        )

        for episode in 1...5 {
            let assetId = "asset-hvk0-unmeasurable-\(episode)"
            try await store.insertAsset(makeAsset(id: assetId))
            let state = try await store.fetchPodcastPlannerState(podcastId: podcastId)
            _ = try await runner.runPendingBackfill(
                for: makeTargetedInputs(
                    assetId: assetId,
                    podcastId: podcastId,
                    transcriptVersion: "tx-hvk0-unmeasurable-v\(episode)",
                    plannerContext: liveContext(state)
                )
            )
            // Prove the premise: the fraction really is unmeasurable here.
            let fraction = try await store
                .fetchCoverageSummariesByAssetIds([assetId])[assetId]?.adScanFraction
            #expect(fraction == nil, "episode \(episode) is supposed to be UNMEASURABLE")
        }

        let finalState = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(finalState.observedEpisodeCount == 5)
        #expect(finalState.recallSamples.isEmpty)
        #expect(finalState.stableRecallFlag == false)
        #expect(CoveragePlanner().plan(for: liveContext(finalState)).policy == .fullCoverage)
    }

    /// The floor itself, pinned just below. Without this the whole gate is
    /// satisfied by any threshold in `(0.10, 1.00]` — the other fixtures sit at
    /// ~0.10 and 1.00, so an implementation with the floor at 0.50, or 0.11,
    /// passes every other test here.
    ///
    /// It matters more than a normal boundary test: 0.95 is ABOVE every value
    /// this pipeline achieves in the field (max measured `adScanFraction` on the
    /// 2026-07-29 device pull is 0.943 across all 19 assets with a coverage-lane
    /// row), so this test is also the executable statement of why the gate ships
    /// OFF. If a future change makes 0.95 certify, that is a threshold decision
    /// and it has to break this test to happen.
    @Test("playhead-hvk0: a 0.95-coverage rescan is still short of the 0.98 floor")
    func nearMissRescanDoesNotCertify() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hvk0-nearmiss"
        // 950 s of a 1,000 s episode read: better than anything the device has
        // ever achieved, and still not certification.
        try await store.insertAsset(
            makeAsset(id: assetId, episodeDurationSec: 1000, fastTranscriptCoverageEndTime: 1000)
        )
        try await store.insertSemanticScanResult(
            makeReadEvidenceScanRow(assetId: assetId, start: 0, end: 950)
        )
        let fraction = try #require(
            await store.fetchCoverageSummariesByAssetIds([assetId])[assetId]?.adScanFraction
        )
        #expect(abs(fraction.rawValue - 0.95) < 0.001, "fixture drifted: measured \(fraction)")
        #expect(fraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction)

        // And one second under the floor is the same answer as ninety-five
        // percent under it — the gate is a floor, not a gradient.
        let atFloorId = "asset-hvk0-atfloor"
        try await store.insertAsset(
            makeAsset(id: atFloorId, episodeDurationSec: 1000, fastTranscriptCoverageEndTime: 1000)
        )
        try await store.insertSemanticScanResult(
            makeReadEvidenceScanRow(assetId: atFloorId, start: 0, end: 980)
        )
        let atFloor = try #require(
            await store.fetchCoverageSummariesByAssetIds([atFloorId])[atFloorId]?.adScanFraction
        )
        #expect(atFloor >= AnalysisJobRunner.semanticBackfillSufficientAdScanFraction)
    }

    /// The other side of the same gate: read evidence is NECESSARY, not
    /// punitive. A show whose rescans do read their episodes is promoted
    /// exactly as it was pre-hvk0, so the targeted policy's savings survive.
    @Test("playhead-hvk0: full rescans that DO read the episode still promote the show")
    func completeFullRescansStillPromoteShow() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-hvk0-complete"
        let coarseResponses = (0..<40).map { _ in
            CoarseScreeningSchema(
                disposition: .containsAd,
                support: CoarseSupportSchema(supportLineRefs: [12], certainty: .strong)
            )
        }
        let runtime = TestFMRuntime(coarseResponses: coarseResponses)
        let runner = makeRunner(
            store: store,
            runtime: runtime.runtime,
            plannerPromotionRequiresMeasuredCoverage: true
        )

        for episode in 1...5 {
            let assetId = "asset-hvk0-complete-\(episode)"
            try await store.insertAsset(
                makeAsset(id: assetId, episodeDurationSec: 300, fastTranscriptCoverageEndTime: 300)
            )
            // An earlier pass read the whole episode. Deterministic, so the
            // assertion does not depend on the FM window geometry.
            try await store.insertSemanticScanResult(
                makeReadEvidenceScanRow(assetId: assetId, start: 0, end: 300)
            )
            let state = try await store.fetchPodcastPlannerState(podcastId: podcastId)
            _ = try await runner.runPendingBackfill(
                for: makeTargetedInputs(
                    assetId: assetId,
                    podcastId: podcastId,
                    transcriptVersion: "tx-hvk0-complete-v\(episode)",
                    plannerContext: liveContext(state)
                )
            )
            let fraction = try await store
                .fetchCoverageSummariesByAssetIds([assetId])[assetId]?.adScanFraction
            let measured = try #require(fraction, "episode \(episode) must have a MEASURED fraction")
            #expect(
                measured >= AnalysisJobRunner.semanticBackfillSufficientAdScanFraction,
                "episode \(episode) measured \(measured) — fixture no longer models a complete rescan"
            )
        }

        let finalState = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(finalState.observedEpisodeCount == 5)
        #expect(!finalState.recallSamples.isEmpty, "a rescan that read its episode must certify")
        #expect(finalState.stableRecallFlag == true)
        #expect(CoveragePlanner().plan(for: liveContext(finalState)).policy == .targetedWithAudit)
    }

    /// The bounded / no-progress rail at the RUNNER level.
    ///
    /// Every cycle re-transcribes (a fresh `transcriptVersion`), which is the
    /// real producer of repeat coverage-lane batches — measured on the
    /// 2026-07-29 device pull, six assets carried 2–3 distinct transcript
    /// versions each. So every cycle here re-drives the asset's coverage-lane
    /// work under a new transcript and records a real observation; none of them
    /// gains coverage.
    ///
    /// What a broken implementation would do: promote on some later cycle, let
    /// per-cycle job or scan-row counts grow super-linearly, or let the recall
    /// ring fill from runs that read nothing.
    ///
    /// **playhead-wxsv tightened the growth bound from linear to flat, and that
    /// is the bead's prize measured.** Before it, each new version derived a new
    /// jobId, so every cycle minted a fresh row with a nil cursor and re-read
    /// the whole episode: 2 scan rows and a full FM pass per cycle, 30 of each.
    /// Now the ONE row is re-opened with its cursor intact, `narrowedForResume`
    /// drops everything at or below it, and cycles 2–30 find nothing left to
    /// read — one cheap no-anchors sentinel row apiece and ZERO further FM
    /// calls. The two assertions below are written as upper bounds against the
    /// pre-wxsv numbers so a regression that clears the cursor on re-open (the
    /// tempting `INSERT OR REPLACE` spelling of the same fix) lands here as
    /// `first * cycle` rows and a 30-fold FM bill.
    @Test("playhead-hvk0: 30 no-progress runner cycles never promote and never run away")
    func noProgressRunnerCyclesStayBounded() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-hvk0-spin"
        let assetId = "asset-hvk0-spin"
        let coarseResponses = (0..<400).map { _ in
            CoarseScreeningSchema(
                disposition: .containsAd,
                support: CoarseSupportSchema(supportLineRefs: [12], certainty: .strong)
            )
        }
        let runtime = TestFMRuntime(coarseResponses: coarseResponses)
        let runner = makeRunner(
            store: store,
            runtime: runtime.runtime,
            plannerPromotionRequiresMeasuredCoverage: true
        )
        try await store.insertAsset(
            makeAsset(id: assetId, episodeDurationSec: 3000, fastTranscriptCoverageEndTime: 3000)
        )

        let cycles = 30
        var scanRowsAfterFirst: Int?
        var coarseCallsAfterFirst: Int?
        for cycle in 1...cycles {
            let state = try await store.fetchPodcastPlannerState(podcastId: podcastId)
            // Asserted on the phases, not the policy name: `fullCoverage` and
            // `periodicFullRescan` are both full-episode plans and which fires
            // depends on the re-validation clock. What must never happen is a
            // narrowed plan.
            let plan = CoveragePlanner().plan(for: liveContext(state))
            #expect(plan.policy != .targetedWithAudit, "cycle \(cycle) must not narrow")
            #expect(plan.phases == [.fullEpisodeScan], "cycle \(cycle)")
            _ = try await runner.runPendingBackfill(
                for: makeTargetedInputs(
                    assetId: assetId,
                    podcastId: podcastId,
                    transcriptVersion: "tx-hvk0-spin-v\(cycle)",
                    plannerContext: liveContext(state)
                )
            )
            let after = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
            #expect(after.stableRecallFlag == false, "cycle \(cycle) must not promote")
            #expect(after.recallSamples.isEmpty, "cycle \(cycle) must not enter the ring")

            // Runaway check: FM work per cycle must not GROW.
            let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
            if cycle == 1 {
                // Guard the rail against going vacuous: an `n <= …` bound holds
                // for every cycle if the fixture ever stops producing rows.
                #expect(scans.count > 0, "cycle 1 wrote no scan rows — the rail would be vacuous")
                scanRowsAfterFirst = scans.count
                coarseCallsAfterFirst = await runtime.coarseCallCount
                #expect(coarseCallsAfterFirst ?? 0 > 0, "cycle 1 called no FM — the rail would be vacuous")
            } else if let first = scanRowsAfterFirst, let firstCalls = coarseCallsAfterFirst {
                let cap = first + (cycle - 1)
                #expect(
                    scans.count <= cap,
                    """
                    cycle \(cycle) wrote \(scans.count) scan rows; a re-driven cycle may add at \
                    most one sentinel, so the cap is \(cap). Pre-wxsv this was \(first * cycle) \
                    — a cleared cursor re-reads the episode.
                    """
                )
                #expect(
                    await runtime.coarseCallCount == firstCalls,
                    "cycle \(cycle) called FM again: a transcript that grew past nothing must read no audio"
                )
            }
        }

        let final = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(final.observedEpisodeCount == cycles)
        #expect(final.stableRecallFlag == false)
        #expect(final.recallSamples.isEmpty)
        let fraction = try await store
            .fetchCoverageSummariesByAssetIds([assetId])[assetId]?.adScanFraction
        let measured = try #require(fraction)
        #expect(
            measured < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction,
            "the episode never gained coverage — that is the premise of this rail"
        )
    }

    @available(iOS 26.0, *)
    @Test("coarse guardrail failures persist a terminal passA row even without windows")
    func coarseGuardrailFailuresPersistFailureRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(coarseFailures: [.guardrailViolation])
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        #expect(result.scanResultIds.count == 1)
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(scans.count == 1, "runner must persist a synthetic failure row for blocked coarse scans")
        let failure = try #require(scans.first)
        #expect(failure.scanPass == "passA")
        #expect(failure.status == .guardrailViolation)
        #expect(failure.disposition == .abstain)
        #expect(failure.windowFirstAtomOrdinal == 0)
        #expect(failure.windowLastAtomOrdinal == 2)
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(evidence.filter { !$0.sourceType.isObservabilityOnly }.isEmpty)
        #expect(evidence.filter { $0.eventType == OperationalMetrics.eventType }.count == 1)
    }

    @available(iOS 26.0, *)
    @Test("playhead-pmp9: an all-windows-rate-limited coarse pass DEFERS the job, not complete-with-holes")
    func coarseRateLimitAllWindowsDefersJob() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        // The single coarse window rate-limits on the initial call AND every
        // capped-exponential backoff retry (full budget) → it is abandoned, so
        // the pass claims ZERO coverage. Before pmp9 the runner marked the job
        // `complete` with the episode-end cursor and the M-5 idempotency gate
        // then skipped it FOREVER, stranding the unscanned audio.
        let budget = 1 + FoundationModelClassifier.rateLimitBackoffBaseNanos.count
        let fmRuntime = TestFMRuntime(coarseFailures: Array(repeating: .rateLimited, count: budget))
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        // NEW: the rate-limited job is DEFERRED (resumable), not silently completed.
        #expect(!result.deferredJobIds.isEmpty)
        #expect(result.scanResultIds.count == 1)
        #expect(await fmRuntime.coarseCallCount == budget,
            "runner should exhaust the full capped-exponential backoff budget before abandoning the window")

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(scans.count == 1)
        let failure = try #require(scans.first)
        #expect(failure.scanPass == "passA")
        #expect(failure.status == .rateLimited)
        #expect(failure.disposition == .abstain)

        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        // Non-terminal DEFERRED so M-5 re-drives it; NOT `.complete`.
        #expect(row.status == .deferred)
        #expect(row.deferReason == "rateLimited-backoff")
        // HONEST cursor: nothing was successfully scanned, so no coverage claimed.
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == nil)
        // Deferral is not a retry-budget failure — retryCount stays 0.
        #expect(row.retryCount == 0)
    }

    /// POSTROLL (playhead-qbib): the runner-level regression that a
    /// mid-episode coarse guardrail cannot suppress scanning of everything
    /// after it. This test used to pin the OPPOSITE contract — the pass
    /// aborted on window 2 of 3, persisted 2 rows, and reported the job
    /// complete with the final third of the episode never screened. The
    /// blocking-failure row is still asserted; what changed is that it no
    /// longer costs the postroll.
    @available(iOS 26.0, *)
    @Test("playhead-qbib: a mid-episode coarse guardrail still scans the postroll window")
    func partialCoarseGuardrailStillScansPostroll() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseFailures: [nil, .guardrailViolation],
            contextSize: 431,
            coarseSchemaTokenCount: 4,
            refinementSchemaTokenCount: 8,
            tokenCountRule: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 8
            }
        )
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            classifierConfig: .init(safetyMarginTokens: 5, maximumResponseTokens: 6)
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        // Three planned windows, three FM calls: the guardrail costs its own
        // window and nothing else.
        #expect(await fmRuntime.coarseCallCount == 3)
        #expect(result.scanResultIds.count == 3)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        let passA = scans.filter { $0.scanPass == "passA" }
        #expect(passA.count == 3)
        #expect(passA.filter { $0.status == .success }.count == 2)
        #expect(passA.filter { $0.status == .guardrailViolation }.count == 1)

        let failure = try #require(passA.first { $0.status == .guardrailViolation })
        #expect(failure.disposition == .abstain)

        // The postroll — the LAST window of the episode — was screened, and it
        // sits after the guardrailed window in time.
        let lastWindow = try #require(passA.max(by: { $0.windowEndTime < $1.windowEndTime }))
        #expect(lastWindow.status == .success)
        #expect(lastWindow.windowStartTime >= failure.windowEndTime)
        #expect(lastWindow.windowEndTime == 90, "coverage must reach transcript end")

        // DENOMINATOR: the guardrailed window is reported as audio we could
        // NOT look at, not as audio screened clean.
        let coverage = SemanticScanCoverage.compute(rows: scans, episodeDuration: 90)
        #expect(!coverage.isComplete)
        #expect(coverage.unexaminedRanges == [failure.windowStartTime ... failure.windowEndTime])
        #expect(coverage.examinedSeconds == 90 - (failure.windowEndTime - failure.windowStartTime))
    }

    @available(iOS 26.0, *)
    @Test("partial refinement guardrail persists success rows and a blocking failure row")
    func partialRefinementGuardrailPersistsBlockingFailureRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [0], certainty: .strong)
                ),
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [1], certainty: .strong)
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [])
            ],
            refinementFailures: [
                nil,
                .guardrailViolation
            ],
            contextSize: 431,
            coarseSchemaTokenCount: 4,
            refinementSchemaTokenCount: 8,
            tokenCountRule: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 8
            }
        )
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            classifierConfig: .init(safetyMarginTokens: 5, maximumResponseTokens: 6)
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        #expect(await fmRuntime.refinementCallCount == 2)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        let passAScans = scans.filter { $0.scanPass == "passA" }
        let passBScans = scans.filter { $0.scanPass == "passB" }

        #expect(passAScans.count == 3)
        #expect(passAScans.allSatisfy { $0.status == .success })
        #expect(passBScans.count == 2)
        #expect(passBScans.filter { $0.status == .success }.count == 1)
        #expect(passBScans.filter { $0.status == .guardrailViolation }.count == 1)

        let failure = try #require(passBScans.first { $0.status == .guardrailViolation })
        #expect(failure.disposition == .abstain)
        #expect(result.scanResultIds.count == scans.count)

        // playhead-qbib: a passB window that found no ads still has to say
        // WHERE it looked. These coordinates used to be derived solely from
        // the returned spans, so a no-ad window persisted `0.0 - 0.0` — a row
        // that cannot locate itself is useless to every consumer downstream.
        let noAdWindow = try #require(passBScans.first { $0.status == .success })
        #expect(noAdWindow.disposition == .noAds)
        #expect(noAdWindow.windowEndTime > noAdWindow.windowStartTime)
        #expect(noAdWindow.windowEndTime <= 90)
    }

    @available(iOS 26.0, *)
    @Test("playhead-avbn: a passB row reports the MEASURED transcript quality, never a hardcoded good")
    func refinementRowReportsMeasuredTranscriptQuality() async throws {
        // `makeRefinementScanResult` used to assert `transcriptQuality: .good`
        // on every pass-B row it wrote. Measured on the surviving device pulls,
        // 100 % of pass-B rows read `good` while pass-A on the SAME assets read
        // `degraded` 8 times in 11 — the column carried no information.
        //
        // It is not cosmetic: `FMSuppressionWindow.votingWindows` bands a window
        // `transcriptQuality == .good ? .moderate : .weak` and
        // `FMSuppressionGuard` counts only `.moderate`+, so the hardcode
        // promoted every pass-B row to a full vote regardless of the transcript
        // under it.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // A deliberately bad transcript: no sentence punctuation, heavy
        // repetition. Every line is written the same way so any window the
        // runner chooses aggregates to the same level.
        let degradedLine =
            "um um so like uh you know um so like uh you know um so like uh you know um so like uh"
        let segments = makeFMSegments(
            analysisAssetId: "asset-runner",
            transcriptVersion: "tx-runner-v1",
            lines: [
                (0, 30, degradedLine),
                (30, 60, degradedLine),
                (60, 90, degradedLine)
            ]
        )

        // The rail proves nothing unless the fixture is genuinely not `.good` —
        // assert that first, so a future tweak to the estimator's thresholds
        // fails HERE with a clear message instead of silently turning the real
        // assertion into a tautology.
        let measured = TranscriptQualityEstimator.assess(segments: segments).map(\.quality)
        #expect(
            measured.allSatisfy { $0 != .good },
            "fixture regression: the degraded transcript now assesses as good (\(measured)), so this test can no longer distinguish a measured value from the old hardcode"
        )

        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: "asset-runner",
                transcriptVersion: "tx-runner-v1"
            ),
            transcriptVersion: "tx-runner-v1",
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

        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [0], certainty: .strong)
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: inputs)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        let passB = scans.filter { $0.scanPass == "passB" && $0.status == .success }
        #expect(!passB.isEmpty, "the fixture must produce at least one successful passB row")
        #expect(
            passB.allSatisfy { $0.transcriptQuality != .good },
            "a passB row over a degraded transcript still claims good — the hardcode is back"
        )
    }

    @Test("admission throttling defers the job and records the reason")
    func thermalThrottleIsDeferred() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            snapshot: makeThermalThrottledSnapshot()
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(result.admittedJobIds.isEmpty)
        #expect(!result.deferredJobIds.isEmpty)
        let coarseCalls = await fmRuntime.coarseCallCount
        #expect(coarseCalls == 0)
        // Persisted jobs should be marked deferred with a reason.
        let job = try await store.fetchBackfillJob(byId: result.deferredJobIds.first!)
        #expect(job?.status == .deferred)
        #expect(job?.deferReason == "thermalThrottled")
    }

    @Test("task cancellation between jobs aborts the run")
    func cancellationBetweenJobsAborts() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let task = Task { [runner] in
            try await runner.runPendingBackfill(for: makeInputs())
        }
        task.cancel()

        do {
            _ = try await task.value
        } catch is CancellationError {
            // expected
            return
        } catch {
            // Acceptable: also valid for runner to bail with a thrown error
            // wrapping cancellation, but we expect the canonical CancellationError.
            #expect(Bool(false), "Expected CancellationError, got \(error)")
        }
    }

    @Test("refinement-pass persists evidence events with JSON-array atomOrdinals")
    func refinementPassPersistsEvidenceEvents() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: 1,
                        lastLineRef: 1,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: nil,
                                lineRef: 1,
                                kind: .ctaPhrase,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.callToAction]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        // The refinement pass must actually persist evidence rows. Before the
        // C-1 fix this failed because the runner emitted comma-joined ordinals
        // like "1,2,3" which AnalysisStore.validateAtomOrdinalsJSON rejected.
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let refinementEvidence = evidence.filter { $0.eventType == "fm.spanRefinement" }
        #expect(!refinementEvidence.isEmpty, "refinement pass must persist evidence_events rows")
        #expect(result.evidenceEventIds.count == evidence.count)
        #expect(Set(result.evidenceEventIds) == Set(evidence.map(\.id)))
        // Every persisted row's atomOrdinals must be a JSON array parseable as [Int].
        for event in refinementEvidence {
            let data = Data(event.atomOrdinals.utf8)
            let parsed = try JSONDecoder().decode([Int].self, from: data)
            #expect(!parsed.isEmpty)
        }
    }

    // R4-Fix7: The catch arms in the drain loop called
    // `markBackfillJobFailed` with no surrounding try/catch. If the typed
    // store guard threw `invalidStateTransition` (e.g. another runner
    // marked the row `.complete` first), the throw escaped the catch arm,
    // aborted the for-loop, and stranded the rest of the batch.
    //
    // We engineer the race deterministically: the classifier marks the
    // row `.complete` BEFORE throwing. By the time the catch arm calls
    // `markBackfillJobFailed`, the C-R3-2 guard sees the row in
    // `.complete` and throws `invalidStateTransition`. After R4-Fix7 the
    // wrap absorbs that throw, finishes the admission ticket, and the
    // runner returns normally instead of propagating the throw out of
    // `runPendingBackfill`.
    @Test("R4-Fix7: markBackfillJobFailed throw is absorbed and the runner returns normally")
    func markFailedThrowIsAbsorbedByCatchArm() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let inputs = makeInputs()
        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-runner",
            phase: .fullEpisodeScan,
            offset: 0
        )

        // Custom Runtime: respondCoarse marks the row .complete BEFORE
        // returning a successful screening. Combined with a malformed
        // scanCohortJSON, the runner's `insertSemanticScanResult` then
        // throws `invalidScanCohortJSON`, the catch arm fires, and the
        // catch arm's `markBackfillJobFailed` hits a `.complete` row
        // and throws `invalidStateTransition`. Without R4-Fix7 the throw
        // escapes the for-loop and `runPendingBackfill` re-throws.
        let runtime = FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4_096 },
            tokenCount: { prompt in
                max(1, prompt.split(whereSeparator: \.isWhitespace).count)
            },
            coarseSchemaTokenCount: { 16 },
            refinementSchemaTokenCount: { 32 },
            boundarySchemaTokenCount: { 32 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in
                        // Race: flip the row to `.complete` before
                        // returning. The runner's subsequent store write
                        // will fail (malformed cohort) and the catch
                        // arm's markBackfillJobFailed will then see a
                        // `.complete` row and throw.
                        try? await store.markBackfillJobComplete(
                            jobId: jobId,
                            progressCursor: nil
                        )
                        return CoarseScreeningSchema(disposition: .noAds, support: nil)
                    },
                    respondRefinement: { _ in
                        RefinementWindowSchema(spans: [])
                    }
                )
            }
        )

        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            // Malformed cohort: insertSemanticScanResult will throw
            // invalidScanCohortJSON, sending the runner into the catch arm.
            scanCohortJSON: "not-json"
        )

        // Must NOT throw — the wrap absorbs the invalidStateTransition
        // from the racing markBackfillJobFailed call and lets the loop
        // wind down cleanly.
        let result = try await runner.runPendingBackfill(for: inputs)

        // The classifier ran for the only planned job.
        #expect(result.admittedJobIds.contains(jobId))

        // The row reflects the racing classifier write. The fix
        // tolerates the markBackfillJobFailed throw and does not wedge
        // the loop.
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete,
                "row reflects the racing classifier write; runner must not have crashed before returning")
    }

    // R4-Fix4: `memoryWriteEligible` was computed on RefinedAdSpan but never
    // serialized into the persisted EvidencePayload. The H-R3-1 in-memory
    // protection had no production consumer. Persist the flag so a future
    // sponsor-memory writer reading evidence_events.evidenceJSON sees the
    // eligibility decision.
    @Test("R4-Fix4: persisted evidence JSON encodes memoryWriteEligible (true case)")
    func evidenceJSONIncludesMemoryWriteEligibleTrue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let inputs = makeInputs()
        // The default segments include "Use code SHOW for 20 percent off at
        // example dot com." which yields catalog entries. Pick one to cite.
        let entry = try #require(inputs.evidenceCatalog.entries.first,
                                  "test fixture must yield at least one catalog entry")
        // Map EvidenceCategory -> EvidenceAnchorKind (raw values match).
        let anchorKind: EvidenceAnchorKind
        switch entry.category {
        case .url: anchorKind = .url
        case .promoCode: anchorKind = .promoCode
        case .ctaPhrase: anchorKind = .ctaPhrase
        case .disclosurePhrase: anchorKind = .disclosurePhrase
        case .brandSpan: anchorKind = .brandSpan
        }

        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [entry.atomOrdinal],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: entry.atomOrdinal,
                        lastLineRef: entry.atomOrdinal,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: entry.evidenceRef,
                                lineRef: entry.atomOrdinal,
                                kind: anchorKind,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.promoCode]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: inputs)

        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let refinementRows = evidence.filter { $0.eventType == "fm.spanRefinement" }
        #expect(!refinementRows.isEmpty, "expected at least one refinement evidence row")
        for row in refinementRows {
            #expect(row.evidenceJSON.contains("\"memoryWriteEligible\":true"),
                    "evidenceJSON must encode memoryWriteEligible=true; got: \(row.evidenceJSON)")
        }
    }

    @Test("R4-Fix4: persisted evidence JSON encodes memoryWriteEligible (false case)")
    func evidenceJSONIncludesMemoryWriteEligibleFalse() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Anchor with evidenceRef=nil resolves via the lineRefFallback path,
        // which (per the C8 contract) marks the span as memoryWriteEligible=false.
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: 1,
                        lastLineRef: 1,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: nil,
                                lineRef: 1,
                                kind: .ctaPhrase,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.callToAction]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: makeInputs())

        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let refinementRows = evidence.filter { $0.eventType == "fm.spanRefinement" }
        #expect(!refinementRows.isEmpty)
        for row in refinementRows {
            #expect(row.evidenceJSON.contains("\"memoryWriteEligible\":false"),
                    "evidenceJSON must encode memoryWriteEligible=false; got: \(row.evidenceJSON)")
        }
    }

    @available(iOS 26.0, *)
    @Test("refinement refusal persists a terminal passB row when no spans are returned")
    func refinementRefusalPersistsFailureRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ],
            refinementFailures: [.refusal]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        #expect(passB.count == 1, "runner must persist a synthetic passB failure row")
        let failure = try #require(passB.first)
        #expect(failure.status == .refusal)
        #expect(failure.disposition == .abstain)
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(
            evidence.filter { $0.eventType == "fm.spanRefinement" }.isEmpty,
            "no refinement evidence should be written for a refused prompt"
        )
        #expect(evidence.filter { $0.eventType == OperationalMetrics.eventType }.count == 1)
    }

    @Test("refinement writes scan row and evidence events atomically")
    func refinementPassWritesAtomically() async throws {
        // C-3 regression. The runner previously wrote Pass-B scan rows and
        // evidence events with separate `insertSemanticScanResult` /
        // `insertEvidenceEvent` calls across `await` points, so a crash
        // between them would leave orphan scan rows. After the fix the
        // runner calls `recordSemanticScanResult(_:evidenceEvents:)` which
        // wraps both writes in a single SQLite transaction with rollback on
        // failure.
        //
        // We pin the happy-path invariant here: for each persisted passB
        // scan result, the count of evidence events attributed to the same
        // asset is non-zero, and every evidence row parses back to a valid
        // JSON array of integers (also pinning C-1).
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: 1,
                        lastLineRef: 1,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: nil,
                                lineRef: 1,
                                kind: .ctaPhrase,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.callToAction]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: makeInputs())

        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(!passB.isEmpty, "passB scan row must persist")
        #expect(evidence.count >= passB.count, "each passB row must have at least one evidence event")
    }

    @Test("runPendingBackfill is idempotent across invocations for the same asset")
    func runPendingBackfillIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .noAds,
                    support: nil
                ),
                CoarseScreeningSchema(
                    disposition: .noAds,
                    support: nil
                )
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        // First run should enqueue and admit the planned jobs.
        let first = try await runner.runPendingBackfill(for: makeInputs())
        #expect(!first.admittedJobIds.isEmpty, "first run must admit jobs")

        // Second run must not throw `duplicateJobId` — it should reuse the
        // existing rows. Jobs already completed stay off the queue; anything
        // deferred can be re-driven.
        let second = try await runner.runPendingBackfill(for: makeInputs())
        // After the fix we expect zero *new* admitted jobs, because the first
        // run completed every planned job.
        #expect(second.admittedJobIds.isEmpty, "completed jobs must not be re-admitted")
    }

    // playhead-59c8 REWROTE THIS TEST'S ASSERTION, and finding it is what the
    // mutation battery's own baseline check bought: it was the one test in the
    // tree pinning the behaviour `playhead-v7q6` forbids.
    //
    // C-2's claim is right and unchanged — a job that dies on an unclassifiable
    // throw must leave a row a human can diagnose without a device attached.
    // Its EVIDENCE was `deferReason.contains("synthetic classifier failure")`,
    // i.e. that the column carries `String(describing: error)`. On the
    // 2026-08-14 pull that is 300 characters of `NSError` prose which cannot be
    // grouped, cannot be counted, and changes shape with the OS; identifying
    // A9F6DF05's cause cost somebody a hand-read of all 300.
    //
    // The column now carries a named token plus the error's IDENTITY, so the
    // assertion moved from the prose to the identity — and the prose's absence
    // is asserted too, because "the token is present" would still pass if the
    // description were appended to it.
    @Test("C-2: failing classifier persists a NAMED, identity-carrying deferReason on .failed")
    func failedClassifierPersistsDeferReason() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        struct CoarseFailure: Error, CustomStringConvertible {
            let description = "synthetic classifier failure"
        }

        // Build a Runtime whose coarse pass always throws. We cannot mutate
        // TestFMRuntime without crossing ownership boundaries, so build the
        // Runtime struct inline.
        // We make `tokenCount` throw so `planPassA` → `coarsePassA` throws
        // out of the runtime entirely (bypassing the per-window failure
        // mapping) and trips the runner's terminal-failure catch branch.
        let failingRuntime = FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4_096 },
            tokenCount: { _ in throw CoarseFailure() },
            coarseSchemaTokenCount: { 16 },
            refinementSchemaTokenCount: { 32 },
            boundarySchemaTokenCount: { 32 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in
                        CoarseScreeningSchema(disposition: .noAds, support: nil)
                    },
                    respondRefinement: { _ in
                        RefinementWindowSchema(spans: [])
                    }
                )
            }
        )
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: failingRuntime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty, "runner must have admitted at least one job")
        // After all admitted jobs failed, the stored row must reflect the
        // failure with a non-nil deferReason describing the cause.
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed)
        let reason = try #require(row.deferReason)
        // COUNTABLE: one prefix, nothing else in this runner answers to it.
        #expect(reason.hasPrefix("\(UnclassifiedModelFailure.causePrefix)-"))
        #expect(reason.contains(BackfillJobPhase.fullEpisodeScan.rawValue))
        // DIAGNOSABLE: the throw's identity, which for a native Swift error is
        // its reflected type name and its case index. This is the end-to-end
        // half of `UnclassifiedModelFailureTests.aNativeSwiftErrorStillGetsAnIdentity`
        // — the pure test proves the read is total, this proves the runner
        // persists what it read.
        #expect(reason.contains("CoarseFailure"), "the identity must name the thrown type: \(reason)")
        #expect(reason.contains("code="))
        // A native error carries no underlying chain, and the record says so in
        // a word rather than by omitting the field.
        #expect(reason.contains("under=\(UnclassifiedModelFailure.noUnderlyingToken)"))
        // AND THE PROSE IS GONE. Without this, appending `String(describing:)`
        // to the token would satisfy every assertion above while restoring the
        // ungroupable column the token exists to replace.
        #expect(!reason.contains("synthetic classifier failure"))

        let events = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let metricEvent = try #require(events.first { $0.eventType == OperationalMetrics.eventType })
        #expect(result.evidenceEventIds.contains(metricEvent.id))
        let metrics = try JSONDecoder().decode(
            OperationalMetrics.self,
            from: Data(metricEvent.evidenceJSON.utf8)
        )
        #expect(metrics.jobId == jobId)
        #expect(metrics.counters.admissionDecisionCount == 1)
        #expect(metrics.counters.resumeSuccessCount == 0)
        #expect(metrics.counters.thermalDeferralCount == 0)
    }

    @Test("C-B: runs do not re-admit jobs that have exhausted the retry budget")
    func exhaustedRetryBudgetIsNotReAdmitted() async throws {
        // The factory in PlayheadRuntime allocates a fresh AdmissionController
        // per invocation, so the controller's in-memory retry budget resets
        // between runs. The persisted retryCount on the backfill_jobs row is
        // the only source of truth. When a prior run has left a row in
        // `.failed` with `retryCount >= AdmissionController.maxRetries`, the
        // runner must skip it entirely — no re-enqueue, no FM call, no status
        // change.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Seed a failed job matching the deterministic jobId format the
        // runner synthesizes for a cold-start fullEpisodeScan plan.
        let exhausted = BackfillJob(
            jobId: BackfillJobRunner.makeJobId(
                analysisAssetId: "asset-runner",
                phase: .fullEpisodeScan,
                offset: 0
            ),
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            phase: .fullEpisodeScan,
            coveragePolicy: .fullCoverage,
            priority: 5,
            progressCursor: nil,
            retryCount: AdmissionController.maxRetries,
            deferReason: "prior failure",
            status: .failed,
            scanCohortJSON: makeTestScanCohortJSON(),
            createdAt: Date().timeIntervalSince1970
        )
        try await store.insertBackfillJob(exhausted)

        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(result.admittedJobIds.isEmpty, "exhausted job must not be admitted")
        #expect(result.deferredJobIds.isEmpty, "exhausted job must not be re-deferred")
        #expect(result.scanResultIds.isEmpty)
        #expect(await fmRuntime.coarseCallCount == 0, "FM must not be called")
        let row = try #require(await store.fetchBackfillJob(byId: exhausted.jobId))
        #expect(row.status == .failed, "status must remain .failed")
        #expect(row.retryCount == AdmissionController.maxRetries)
    }

    /// playhead-wxsv SPEC 1, at the RUNNER level: a `failed` row under the
    /// retry budget is RE-DRIVEN, and the retry actually reaches the
    /// classifier.
    ///
    /// **This test asserted the opposite until playhead-wxsv.** It pinned the
    /// C3-1 catch arm swallowing an `invalidStateTransition` on exactly this
    /// row, and that swallow was the defect: the M-5 branch re-enqueued a
    /// failed-under-budget row, `markBackfillJobRunning` refused it, the arm
    /// logged and continued, and the retry budget bought nothing. The store
    /// guard now accepts the row (see
    /// `BackfillJobStoreTests.markBackfillJobRunning_restartsFailedRowUnderBudget`),
    /// so the observable claim is the one that matters to the user: the asset
    /// gets its scan.
    ///
    /// A broken implementation that would still pass the OLD test: the shipped
    /// one. A broken implementation that would still pass THIS test: one that
    /// also restarts a row at the budget — which
    /// `exhaustedRetryBudgetIsNotReAdmitted` refuses.
    @Test("playhead-wxsv: a failed row under the retry budget is re-driven and reaches FM")
    func preFailedRowUnderBudgetIsRedriven() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let failedJob = BackfillJob(
            jobId: BackfillJobRunner.makeJobId(
                analysisAssetId: "asset-runner",
                phase: .fullEpisodeScan,
                offset: 0
            ),
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            phase: .fullEpisodeScan,
            coveragePolicy: .fullCoverage,
            priority: 5,
            progressCursor: nil,
            retryCount: 0,
            deferReason: "seeded failure",
            status: .failed,
            scanCohortJSON: makeTestScanCohortJSON(),
            createdAt: Date().timeIntervalSince1970
        )
        try await store.insertBackfillJob(failedJob)

        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(await fmRuntime.coarseCallCount > 0,
                "the retry must reach the classifier — a budget nothing can spend is not a budget")
        #expect(result.admittedJobIds.contains(failedJob.jobId))

        let row = try #require(await store.fetchBackfillJob(byId: failedJob.jobId))
        #expect(row.status != .failed,
                "the row must have left .failed; it stayed there for the whole pre-wxsv life of the fleet")
        #expect(row.attemptTranscriptVersion == makeInputs().transcriptVersion,
                "the attempt must record the transcript it actually read")
    }

    @Test("H-1: thermal defer marks ALL planned jobs, not just the first")
    func thermalDeferMarksAllPlannedJobs() async throws {
        // Use a planner context that emits the 3-phase targeted plan
        // (harvester, likely-ad-slots, audit). Previously the runner broke
        // out of the drain loop after marking only the first queued job as
        // deferred, leaving the other two stuck in `.queued` forever.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime()
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makeThermalThrottledSnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: 20,
            stableRecall: true,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        let base = makeInputs()
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: base.analysisAssetId,
            podcastId: base.podcastId,
            segments: base.segments,
            evidenceCatalog: base.evidenceCatalog,
            transcriptVersion: base.transcriptVersion,
            plannerContext: plannerContext
        )

        let result = try await runner.runPendingBackfill(for: inputs)

        #expect(result.admittedJobIds.isEmpty)
        #expect(result.deferredJobIds.count == 3, "all 3 planned jobs must be marked deferred (got \(result.deferredJobIds.count))")

        for jobId in result.deferredJobIds {
            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.status == .deferred, "job \(jobId) must be .deferred, got \(row.status)")
            #expect(row.deferReason == "thermalThrottled")
        }
    }

    // R4-Fix1: The H-1 defer-all-jobs loop unconditionally called
    // markBackfillJobDeferred for every non-admitted candidate. When the M-5
    // idempotency path re-enqueued a `.failed` row (retryCount<maxRetries),
    // the C-R3-1 status guard rejected the `.failed -> .deferred` write with
    // `invalidStateTransition`. The throw was unhandled inside the loop and
    // aborted mid-iteration, leaving subsequent jobs stranded in `.queued`.
    @Test("R4-Fix1: defer loop tolerates terminal pre-failed rows and continues marking the rest")
    func deferLoopHandlesTerminalRowsGracefully() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Pre-insert a `.failed` row at the deterministic jobId the runner
        // will synthesize for the first targeted phase. retryCount=0 keeps
        // it under the C-B exhaustion gate, so the M-5 idempotency path
        // re-enqueues it instead of skipping.
        // Targeted plan emits phases in order: scanHarvesterProposals(0),
        // scanLikelyAdSlots(1), scanRandomAuditWindows(2). Seed the FIRST
        // phase as `.failed` so the M-5 idempotency probe matches by jobId
        // and re-enqueues the existing terminal row.
        let failedJob = BackfillJob(
            jobId: BackfillJobRunner.makeJobId(
                analysisAssetId: "asset-runner",
                phase: .scanHarvesterProposals,
                offset: 0
            ),
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            phase: .scanHarvesterProposals,
            coveragePolicy: .targetedWithAudit,
            priority: 20,
            progressCursor: nil,
            retryCount: 0,
            deferReason: "seeded prior failure",
            status: .failed,
            scanCohortJSON: makeTestScanCohortJSON(),
            createdAt: Date().timeIntervalSince1970
        )
        try await store.insertBackfillJob(failedJob)

        let fmRuntime = TestFMRuntime()
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makeThermalThrottledSnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: 20,
            stableRecall: true,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        let base = makeInputs()
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: base.analysisAssetId,
            podcastId: base.podcastId,
            segments: base.segments,
            evidenceCatalog: base.evidenceCatalog,
            transcriptVersion: base.transcriptVersion,
            plannerContext: plannerContext
        )

        // Must not throw: the defer-loop must absorb the C-R3-1
        // invalidStateTransition on the seeded `.failed` row and keep going.
        let result = try await runner.runPendingBackfill(for: inputs)

        // The pre-failed row stays exactly as seeded.
        let preFailed = try #require(await store.fetchBackfillJob(byId: failedJob.jobId))
        #expect(preFailed.status == .failed, "seeded .failed row must remain .failed")
        #expect(preFailed.deferReason == "seeded prior failure",
                "seeded deferReason must be preserved")
        #expect(preFailed.retryCount == 0)

        // The other 2 planned phases (auditWindows + harvesterProposals)
        // must have been marked deferred — proving the loop did not abort
        // after the invalidStateTransition on the .failed row.
        let otherDeferredIds = result.deferredJobIds.filter { $0 != failedJob.jobId }
        #expect(otherDeferredIds.count == 2,
                "expected 2 sibling phases marked deferred, got \(otherDeferredIds.count)")
        for jobId in otherDeferredIds {
            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.status == .deferred, "sibling \(jobId) must be .deferred")
            #expect(row.deferReason == "thermalThrottled")
        }
    }

    /// R4-Fix6's requirement, under playhead-wxsv's mechanism: a version bump
    /// must still REPROCESS — the completed row must not silently absorb the
    /// new transcript by colliding with it.
    ///
    /// **What changed and why.** R4-Fix6 met that requirement by hashing the
    /// version into the jobId, so a bump minted a whole new row. That is the
    /// defect this bead removes: the old row was orphaned with its cursor, one
    /// per transcription session, measured on the 2026-08-07 pull as one stale
    /// row per version and four abandoned full-episode scans on `CDD611C4`. The
    /// requirement is now met by re-opening the SAME row, so the assertion
    /// moves from "a second row exists" to "the one row was re-opened and
    /// re-driven, keeping what it had".
    ///
    /// A broken implementation that would still pass this: one that re-opens
    /// unconditionally, including at an unchanged version. `rerunProducesNoDuplicateScanRows`
    /// and `completedRowAtTheSameVersionIsNotResurrected` refuse that.
    @Test("playhead-wxsv: a transcriptVersion bump re-opens the SAME row and re-drives it")
    func transcriptVersionBumpReopensTheSameRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil),
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)
        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-runner",
            phase: .fullEpisodeScan,
            offset: 0
        )

        _ = try await runner.runPendingBackfill(
            for: makeInputs(transcriptVersion: "tx-runner-v1")
        )
        let afterV1 = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(afterV1.status == .complete)
        #expect(afterV1.attemptTranscriptVersion == "tx-runner-v1",
                "the row must record the version its attempt actually read")
        #expect(await fmRuntime.coarseCallCount >= 1, "v1 must invoke the classifier")

        let result = try await runner.runPendingBackfill(
            for: makeInputs(transcriptVersion: "tx-runner-v2")
        )

        // Re-driven, not skipped: the row was admitted again under v2.
        #expect(result.admittedJobIds.contains(jobId),
                "a version bump must not be absorbed by the completed row")
        let afterV2 = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(afterV2.attemptTranscriptVersion == "tx-runner-v2",
                "the second attempt must re-stamp the row")
        #expect(afterV2.createdAt == afterV1.createdAt,
                "re-opening must not move createdAt — see spec item 3")

        // And exactly ONE row exists for this asset+phase, where the pre-wxsv
        // runner would have left two: the live one and an orphan.
        #expect(try await store.countResumableBackfillJobs(assetId: "asset-runner") <= 1)
    }

    /// **playhead-wxsv, THE PRIZE, end to end: a growing transcript CONTINUES
    /// the scan.**
    ///
    /// Session 1 transcribes 0–90 s and the coverage lane scans it. Session 2
    /// transcribes on to 180 s, which is a new `transcriptVersion` because the
    /// version is a content hash. What must happen is that the SAME row resumes
    /// and reads only 90–180 s.
    ///
    /// Neither prior implementation could express that. Main derives a new
    /// jobId from the new version, so it inserts a second row with a nil cursor
    /// and re-reads 0–180 — and the first row is orphaned forever, because no
    /// invocation will ever re-derive its id again. The abandoned
    /// `playhead-7x1s` branch orphaned it too and adopted it later. Measured on
    /// the 2026-08-07 pull, that is one stale row per transcript version,
    /// exactly, and `CDD611C4` with four abandoned full-episode scans and zero
    /// results between them.
    ///
    /// **The assertion is about the LINE REFS the classifier was handed, and
    /// that is not a stylistic preference — a row count cannot see this at
    /// all.** Scan-result ids are deterministic, so a session that re-read
    /// 0-90 s writes rows that `insertSemanticScanResult` dedupes onto the ones
    /// session 1 already wrote. The persisted set is IDENTICAL whether the
    /// cursor was honoured or cleared, and so is the row count, the job count,
    /// `createdAt`, and "the cursor advanced". `TestFMRuntime` records every
    /// coarse prompt it was given, which is the only place the difference is
    /// observable.
    @Test("playhead-wxsv: a transcript that grows resumes the scan instead of restarting it")
    func growingTranscriptContinuesTheScan() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: (0..<20).map { _ in
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            }
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)
        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-runner",
            phase: .fullEpisodeScan,
            offset: 0
        )

        // Session 1: the transcript reaches 90 s.
        _ = try await runner.runPendingBackfill(for: makeInputs(transcriptVersion: "tx-grow-90"))
        let afterFirst = try #require(await store.fetchBackfillJob(byId: jobId))
        let firstCursor = try #require(afterFirst.progressCursor?.lastProcessedUpperBoundSec)
        #expect(firstCursor.rawValue > 0, "session 1 must leave a cursor, or the rail is vacuous")
        let refsAfterFirst = await fmRuntime.snapshotSubmittedCoarseLineRefs()
        let firstSubmitted = Set(refsAfterFirst.flatMap { $0 })
        #expect(!firstSubmitted.isEmpty, "session 1 must submit line refs, or the rail is vacuous")

        // Session 2: the SAME audio, transcribed further — one longer version.
        let grownSegments = makeFMSegments(
            analysisAssetId: "asset-runner",
            transcriptVersion: "tx-grow-180",
            lines: [
                (0, 30, "Welcome to the show. Today we're discussing podcasts."),
                (30, 60, "Use code SHOW for 20 percent off at example dot com."),
                (60, 90, "Now back to the interview with our guest."),
                (90, 120, "Our guest explains how the field changed after 2015."),
                (120, 150, "This segment is supported by ExampleCo, visit example dot com."),
                (150, 180, "And that wraps the second half of the conversation.")
            ]
        )
        let grown = BackfillJobRunner.AssetInputs(
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            segments: grownSegments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: grownSegments.flatMap(\.atoms),
                analysisAssetId: "asset-runner",
                transcriptVersion: "tx-grow-180"
            ),
            transcriptVersion: "tx-grow-180",
            plannerContext: makeInputs().plannerContext
        )
        let result = try await runner.runPendingBackfill(for: grown)

        // ONE row, re-driven — not a second row, and not an orphan.
        #expect(result.admittedJobIds == [jobId])
        #expect(try await store.countResumableBackfillJobs(assetId: "asset-runner") <= 1)
        let afterSecond = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(afterSecond.createdAt == afterFirst.createdAt)
        #expect(afterSecond.attemptTranscriptVersion == "tx-grow-180")

        // And it read only the NEW span. The line refs session 2 submitted are
        // disjoint from session 1's: the first three lines (0-90 s, refs 0-2)
        // were already scanned and must not be handed to FM again.
        let refsAfterSecond = await fmRuntime.snapshotSubmittedCoarseLineRefs()
        let secondSubmitted = Set(refsAfterSecond.flatMap { $0 }).subtracting(firstSubmitted)
        #expect(!secondSubmitted.isEmpty,
                "the grown transcript must be scanned — session 2 submitted nothing new")
        let resubmitted = Set(refsAfterSecond.dropFirst(refsAfterFirst.count).flatMap { $0 })
            .intersection(firstSubmitted)
        #expect(resubmitted.isEmpty,
                """
                session 2 re-submitted line refs \(resubmitted.sorted()) that session 1 had \
                already scanned — the cursor was discarded and the episode was re-read. This is \
                the ONLY assertion in this test that can see that: the persisted rows dedupe.
                """)
        #expect(try #require(afterSecond.progressCursor?.lastProcessedUpperBoundSec).rawValue
                > firstCursor.rawValue,
                "the cursor must advance across the growth, not reset")
    }

    /// playhead-wxsv SPEC 2: a completed row is not resurrected at the version
    /// it completed against.
    ///
    /// The store-level guard is `reopenBackfillJob` refusing an unchanged
    /// version; this is the runner-level consequence, and it is the half that
    /// keeps "re-open on a version change" from degenerating into "re-open
    /// every launch". A broken implementation that would still pass
    /// `transcriptVersionBumpReopensTheSameRow`: one that re-opens
    /// unconditionally.
    @Test("playhead-wxsv: a completed row at the SAME version is not resurrected")
    func completedRowAtTheSameVersionIsNotResurrected() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil),
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)
        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-runner",
            phase: .fullEpisodeScan,
            offset: 0
        )

        _ = try await runner.runPendingBackfill(for: makeInputs(transcriptVersion: "tx-same"))
        let callsAfterFirst = await fmRuntime.coarseCallCount
        #expect(callsAfterFirst >= 1)

        let result = try await runner.runPendingBackfill(for: makeInputs(transcriptVersion: "tx-same"))

        #expect(result.admittedJobIds.isEmpty, "the same version has nothing new to process")
        #expect(await fmRuntime.coarseCallCount == callsAfterFirst, "FM must not be called again")
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete, "the row must still be complete, not re-queued")
    }

    @Test("H-3: re-run with deterministic inputs produces no duplicate scan rows")
    func rerunProducesNoDuplicateScanRows() async throws {
        // Deterministic fake: same asset, same transcript, two runs in
        // sequence. The persisted scan_results count must equal the unique
        // (assetId, scanPass, windowIndex) triples. Before the fix the
        // runner stamped a random UUID suffix into each row id, so a crash
        // mid-run could leave an orphan row under a different id.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil),
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: makeInputs())
        let firstRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")

        // Force the job row back to `.queued` so the second run actually
        // reprocesses it rather than skipping via the `.complete` fast path.
        // This simulates the orphan-recovery scenario the fix targets: a
        // prior run wrote scan rows but did not mark the job complete.
        for jobId in firstRows.map(\.id) {
            _ = jobId // silence warning; we reuse the variable below
        }
        // We need a job row that still allows re-enqueue. Use the
        // DEBUG-only force helper to drop the status back to .queued —
        // direct `markBackfillJobDeferred` can no longer demote a terminal
        // row after the C-R3-1 guard fix.
        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-runner",
            phase: .fullEpisodeScan,
            offset: 0
        )
        try await store.forceBackfillJobStateForTesting(
            jobId: jobId,
            status: .queued,
            progressCursor: nil
        )

        _ = try await runner.runPendingBackfill(for: makeInputs())
        let secondRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")

        // Count unique logical keys across all persisted rows.
        var uniqueKeys = Set<String>()
        for row in secondRows {
            uniqueKeys.insert("\(row.analysisAssetId)|\(row.scanPass)|\(row.windowFirstAtomOrdinal)|\(row.windowLastAtomOrdinal)")
        }
        #expect(secondRows.count == uniqueKeys.count,
                "expected no duplicate rows: \(secondRows.count) rows vs \(uniqueKeys.count) unique keys")
        // And the re-run must not have grown the table beyond the first
        // run's unique-key count.
        //
        // playhead-15d0: compared over the SCAN rows, excluding the no-work
        // sentinel. The re-run is now narrowed by the first run's own durable
        // rows, so it has nothing left to screen, reaches the empty-segments
        // short-circuit and writes the `noWork:` sentinel Bug 11 requires of
        // every admitted job. That sentinel is a legitimately NEW logical key
        // and it is not a scan row — `didExamineWindow` excludes it everywhere,
        // including from the narrowing that produced it, so it can never be
        // read as coverage.
        func scanKeys(_ rows: [SemanticScanResult]) -> Set<String> {
            Set(
                rows
                    .filter { !$0.isNoWorkSentinel }
                    .map { "\($0.analysisAssetId)|\($0.scanPass)|\($0.windowFirstAtomOrdinal)|\($0.windowLastAtomOrdinal)" }
            )
        }
        #expect(scanKeys(secondRows) == scanKeys(firstRows), "second run introduced new logical keys")
        // Stronger than the original assertion, and the reason the sentinel is
        // tolerated above: the re-run must have made NO new FM screening at all.
        #expect(
            secondRows.filter { !$0.isNoWorkSentinel }.count == firstRows.filter { !$0.isNoWorkSentinel }.count,
            "the re-run must not have added a scan row — every window was already screened"
        )
    }

    @Test("H-R3-2: permanent store errors exhaust retries immediately, not after maxRetries attempts")
    func permanentStoreErrorsExhaustRetriesImmediately() async throws {
        // H-R3-2: `AnalysisStoreError.invalidEvidenceEvent`,
        // `.evidenceEventBodyMismatch`, `.invalidScanCohortJSON`, and
        // `.invalidRow` are permanent — replaying the same inputs against
        // the same schema will always fail the same validator. Burning
        // through the retry budget on them is wasted work. The runner must
        // classify them as permanent and short-circuit the retry counter
        // to `maxRetries`, so the next run's C-B gate skips the row.
        //
        // We inject a classifier whose refinement pass returns a span with
        // an evidence anchor whose line refs point outside the segment
        // window. The runner's `makeEvidenceEvents` builder encodes this
        // into an atomOrdinals JSON array that the store's validator
        // rejects with `invalidEvidenceEvent` when the transcript version
        // mismatches the catalog's expected version (H-1 integrity check).
        //
        // Simpler path: use an invalid scanCohortJSON. That is rejected by
        // `insertSemanticScanResult` as `invalidScanCohortJSON` and is
        // genuinely permanent — the cohort is fixed at runner init, so
        // every retry will hit the same error.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            // Malformed cohort JSON: not a valid JSON object. The store's
            // `validateScanCohortJSON` rejects this with
            // `AnalysisStoreError.invalidScanCohortJSON`.
            scanCohortJSON: "not-json"
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())
        #expect(!result.admittedJobIds.isEmpty, "runner must have admitted at least one job")

        // After the permanent failure path the row must be `.failed` with
        // `retryCount == maxRetries`, so the C-B gate skips it on re-runs.
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed)
        #expect(row.retryCount == AdmissionController.maxRetries,
                "permanent error must short-circuit retryCount to maxRetries, got \(row.retryCount)")

        // Second run must skip the exhausted row entirely.
        let fmCallsBefore = await fmRuntime.coarseCallCount
        let second = try await runner.runPendingBackfill(for: makeInputs())
        let fmCallsAfter = await fmRuntime.coarseCallCount
        #expect(second.admittedJobIds.isEmpty, "exhausted row must not be re-admitted")
        #expect(fmCallsAfter == fmCallsBefore,
                "FM must not be called again for an exhausted permanent failure")
    }

    @Test("C-R3-2: scan results from different transcript versions coexist under distinct ids")
    func scanResultsFromDifferentTranscriptVersionsCoexist() async throws {
        // C-R3-2: the deterministic scan id was `scan-{assetId}-{pass}-{idx}`,
        // which omitted the transcriptVersion. The `semantic_scan_results`
        // PK is `id`; UNIQUE is on `reuseKeyHash`, which DOES include the
        // transcriptVersion. Two runs with different transcript versions
        // therefore produced distinct reuseKeyHash values but a colliding
        // PK — the INSERT OR REPLACE then silently nuked the prior run's
        // row, defeating H-1's success-protection guard (which only probes
        // by reuseKeyHash).
        //
        // Fix: include the transcriptVersion in the id itself. Two runs of
        // the same asset under different transcriptVersions must produce
        // two persisted rows, neither overwriting the other.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil),
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        // First run under transcriptVersion "v1".
        _ = try await runner.runPendingBackfill(
            for: makeInputs(transcriptVersion: "tx-runner-v1")
        )
        let v1Rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(!v1Rows.isEmpty, "v1 run must persist at least one scan row")

        // Force the job row back so the second run actually re-runs the
        // passA pipeline; the job ids don't depend on transcriptVersion.
        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-runner",
            phase: .fullEpisodeScan,
            offset: 0
        )
        try await store.forceBackfillJobStateForTesting(
            jobId: jobId,
            status: .queued,
            progressCursor: nil
        )

        // Second run under a new transcriptVersion. Same asset, same window
        // indices — only the transcriptVersion differs.
        _ = try await runner.runPendingBackfill(
            for: makeInputs(transcriptVersion: "tx-runner-v2")
        )
        let allRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")

        // Every v1 row must still exist after the v2 run.
        let idsAfter = Set(allRows.map(\.id))
        for v1 in v1Rows {
            #expect(idsAfter.contains(v1.id),
                    "v1 row \(v1.id) must survive a v2 re-run (C-R3-2 regression)")
        }

        // Both transcript versions must be represented in the stored rows.
        let versions = Set(allRows.map(\.transcriptVersion))
        #expect(versions.contains("tx-runner-v1"), "v1 rows must be present after v2 run")
        #expect(versions.contains("tx-runner-v2"), "v2 rows must be present after v2 run")
    }

    @Test("HIGH-1: concurrent runBackfill calls with per-call controllers do not mass-defer each other")
    func concurrentRunBackfillsDoNotMassDeferEachOther() async throws {
        // HIGH-1 regression: the round-2 M-B hoist made the runtime factory
        // capture a single shared AdmissionController. Because
        // AdDetectionService is actor-reentrant on `await`, two concurrent
        // `runBackfill` calls on different episodes would each hit the
        // shared controller. The second call saw `runningJob != nil`,
        // mass-deferred its whole batch with `serialBusy`, and lost
        // telemetry. The correct wiring is per-call controllers (one per
        // `runBackfill` invocation), which this test pins by mimicking the
        // runtime factory: allocate a fresh controller for each runner.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-concurrent-A"))
        try await store.insertAsset(makeAsset(id: "asset-concurrent-B"))

        let fmRuntimeA = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let fmRuntimeB = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )

        // Per-call admission controllers, matching the corrected factory.
        let runnerA = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntimeA.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
        let runnerB = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntimeB.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        async let a = runnerA.runPendingBackfill(
            for: makeInputs(assetId: "asset-concurrent-A", transcriptVersion: "tx-A")
        )
        async let b = runnerB.runPendingBackfill(
            for: makeInputs(assetId: "asset-concurrent-B", transcriptVersion: "tx-B")
        )
        let (resultA, resultB) = try await (a, b)

        #expect(!resultA.admittedJobIds.isEmpty, "runner A must admit its jobs")
        #expect(!resultB.admittedJobIds.isEmpty, "runner B must admit its jobs")
        #expect(resultA.deferredJobIds.isEmpty, "runner A must not mass-defer")
        #expect(resultB.deferredJobIds.isEmpty, "runner B must not mass-defer")

        // Neither run should have left a `serialBusy` defer reason behind.
        for jobId in resultA.admittedJobIds + resultB.admittedJobIds {
            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.deferReason != "serialBusy",
                    "concurrent runs must not poison each other with serialBusy")
        }
    }

    /// playhead-y3ya RESTATED THIS CONTRACT, deliberately, and the restatement
    /// is stronger than what it replaces.
    ///
    /// It used to read "no non-off mode writes AdWindows", full stop. That was
    /// true only because a coarse `containsAd` verdict with no narrow seed under
    /// it was DISCARDED — which is the defect y3ya exists to fix. FM's verdict
    /// now becomes a mark in the modes that may propose a region, so the claim
    /// worth keeping is not "no windows" but:
    ///
    ///   * `.shadow` / `.rescoreOnly` — still ZERO windows. Neither may propose,
    ///     and `.shadow` in particular is the state `ApprovedCohortRegistry`
    ///     collapses an unapproved cohort to, so surfacing its verdicts would
    ///     defeat the registry's whole purpose. This half of the assertion is
    ///     now LOAD-BEARING where it used to be incidental.
    ///   * `.proposalOnly` / `.full` — the only rows written are
    ///     `semantic-sweep-v1` MARK-ONLY candidates with both edges
    ///     `.unanchored`. The FM scan path still writes no auto-skip material,
    ///     which is what "without writing AdWindows directly" was protecting.
    @Test("non-off Phase 6 modes write only mark-only sweep rows, and only when they may propose")
    func phase6ModesPersistWithoutAdWindowWrites() async throws {
        let modes: [FMBackfillMode] = [.shadow, .rescoreOnly, .proposalOnly, .full]

        for mode in modes {
            let assetId = "asset-\(mode.rawValue)"
            let store = try await makeTestStore()
            try await store.insertAsset(makeAsset(id: assetId))
            let fmRuntime = TestFMRuntime(
                coarseResponses: [
                    CoarseScreeningSchema(
                        disposition: .containsAd,
                        support: CoarseSupportSchema(supportLineRefs: [1], certainty: .strong)
                    )
                ]
            )
            let runner = makeRunner(
                store: store,
                runtime: fmRuntime.runtime,
                mode: mode
            )

            let result = try await runner.runPendingBackfill(
                for: makeInputs(
                    assetId: assetId,
                    podcastId: "podcast-\(mode.rawValue)",
                    transcriptVersion: "tx-\(mode.rawValue)"
                )
            )

            #expect(!result.scanResultIds.isEmpty, "\(mode.rawValue) should persist FM output")
            #expect(await fmRuntime.coarseCallCount >= 1, "\(mode.rawValue) should run FM")
            let windows = try await store.fetchAdWindows(assetId: assetId)
            if mode.canProposeNewRegions {
                #expect(
                    windows.allSatisfy {
                        $0.detectorVersion == SemanticSweepMarkComposer.detectorVersion
                            && $0.eligibilityGate == SkipEligibilityGate.markOnly.rawValue
                            && $0.decisionState == AdDecisionState.candidate.rawValue
                            && $0.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue
                            && $0.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue
                    },
                    "\(mode.rawValue) may write ONLY mark-only sweep rows: \(windows.map(\.detectorVersion))"
                )
                #expect(
                    !windows.isEmpty,
                    "\(mode.rawValue) proposes, so the containsAd verdict must have composed"
                )
            } else {
                #expect(
                    windows.isEmpty,
                    "\(mode.rawValue) may not propose, so it must write no AdWindows"
                )
            }
        }
    }

    // MARK: - R7-Fix11: scan id / job id hashing

    @Test("R7-Fix11: scan IDs are stable hashes immune to separator collision")
    func scanIdsHashCollisionImmune() {
        // Two distinct tuples that would collide under naive `-` joining:
        //   ("abc", "def-123") vs ("abc-def", "123")
        // both produce "scan-abc-def-123-passA-0" with the old format.
        let a = BackfillJobRunner.makeScanResultIdForTesting(
            assetId: "abc", transcriptVersion: "def-123", pass: "passA", windowKey: "atoms=0-0"
        )
        let b = BackfillJobRunner.makeScanResultIdForTesting(
            assetId: "abc-def", transcriptVersion: "123", pass: "passA", windowKey: "atoms=0-0"
        )
        #expect(a != b, "distinct tuples must produce distinct hashed ids")
    }

    @Test("R7-Fix11: scan ID is deterministic for same inputs")
    func scanIdsAreDeterministic() {
        let a = BackfillJobRunner.makeScanResultIdForTesting(
            assetId: "asset-1", transcriptVersion: "v1", pass: "passA", windowKey: "atoms=0-0"
        )
        let b = BackfillJobRunner.makeScanResultIdForTesting(
            assetId: "asset-1", transcriptVersion: "v1", pass: "passA", windowKey: "atoms=0-0"
        )
        #expect(a == b)
        #expect(a.hasPrefix("scan-"))
        #expect(a.count == "scan-".count + 16) // 16-char hex hash
    }

    @Test("R7-Fix11: job IDs are stable hashes immune to separator collision")
    func jobIdsHashCollisionImmune() {
        // Analogous to the scan-id collision test: a hyphen drifting
        // between assetId and transcriptVersion must not collapse two
        // logical tuples onto the same jobId.
        let a = BackfillJobRunner.makeJobId(
            analysisAssetId: "abc",
            phase: .fullEpisodeScan,
            offset: 0
        )
        let b = BackfillJobRunner.makeJobId(
            analysisAssetId: "abc-def",
            phase: .fullEpisodeScan,
            offset: 0
        )
        #expect(a != b, "distinct tuples must produce distinct hashed jobIds")
        #expect(a.hasPrefix("fm-"))
        #expect(a.count == "fm-".count + 16)
    }

    @Test("R7-Fix11: job ID is deterministic for same inputs")
    func jobIdsAreDeterministic() {
        let a = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-1",
            phase: .scanHarvesterProposals,
            offset: 2
        )
        let b = BackfillJobRunner.makeJobId(
            analysisAssetId: "asset-1",
            phase: .scanHarvesterProposals,
            offset: 2
        )
        #expect(a == b)
    }

    @Test("Rev1-L5: every AnalysisStoreError case has a defined permanence classification")
    func isPermanentExhaustivenessRail() {
        // Mirrors `caseNameCoversEveryCase`: this test exists so any new
        // `AnalysisStoreError` case fails compilation here BEFORE it
        // ships with an undefined permanence classification. The switch
        // below must enumerate every case explicitly — `default:` would
        // defeat the rail.
        //
        // Cycle 4 M3: the cycle-2 version of this test only enumerated
        // cases at compile time; it never called the real production
        // `isPermanent` so drift between this table and the real switch
        // would have gone undetected. Now every case is paired with its
        // expected classification and the test calls
        // `BackfillJobRunner.isPermanentForTesting(_:)` for real.
        let cases: [(AnalysisStoreError, Bool)] = [
            (.openFailed(code: 1, message: "x"), false),
            (.migrationFailed("x"), false),
            (.queryFailed("x"), false),
            (.insertFailed("x"), false),
            (.insertFailed("payloadTooLarge: 999"), true),
            (.notFound, false),
            (.duplicateJobId("x"), false),
            (.invalidRow(column: 0), true),
            (.invalidEvidenceEvent("x"), true),
            (.invalidScanCohortJSON("x"), true),
            (.invalidStateTransition(jobId: "j", fromStatus: nil, toStatus: "running"), false),
            (.evidenceEventBodyMismatch(id: "x"), true),
            (.staleAdWindowRevision(id: "window"), false),
            // playhead-4my.10.1 L5: encoder failures while persisting a
            // training example are permanent — the row will fail again on
            // identical input.
            (.encodingFailure("x"), true),
        ]
        // Force the switch to be exhaustive against the enum so a new
        // case fails compilation here.
        for (error, _) in cases {
            switch error {
            case .openFailed,
                 .migrationFailed,
                 .queryFailed,
                 .insertFailed,
                 .notFound,
                 .duplicateJobId,
                 .invalidRow,
                 .invalidEvidenceEvent,
                 .invalidScanCohortJSON,
                 .invalidStateTransition,
                 .evidenceEventBodyMismatch,
                 .staleAdWindowRevision,
                 .encodingFailure:
                continue
            }
        }
        // Real production call — any drift between this table and the
        // real `isPermanent(_:)` switch lights up here.
        for (error, expected) in cases {
            let actual = BackfillJobRunner.isPermanentForTesting(error)
            #expect(
                actual == expected,
                "isPermanent(\(error)) expected \(expected) got \(actual)"
            )
        }
        #expect(cases.count == 14)
    }

    @Test("bd-1tl: caseName covers every AnalysisStoreError case with a stable token")
    func caseNameCoversEveryCase() {
        // bd-1tl: the on-device run reported `AnalysisStoreError error 9`
        // — Swift's NSError-bridge ordinal — which is unhelpful for triage.
        // Production telemetry now logs the case name via
        // `BackfillJobRunner.caseName(of:)`. This test pins every case to
        // its stable token and exercises every switch arm so a future
        // case addition fails compilation here (the switch is exhaustive)
        // before it can ship a "case=unknown" log line.
        let cases: [(AnalysisStoreError, String)] = [
            (.openFailed(code: 1, message: "x"), "openFailed"),
            (.migrationFailed("x"), "migrationFailed"),
            (.queryFailed("x"), "queryFailed"),
            (.insertFailed("x"), "insertFailed"),
            (.notFound, "notFound"),
            (.duplicateJobId("x"), "duplicateJobId"),
            (.invalidRow(column: 0), "invalidRow"),
            (.invalidEvidenceEvent("x"), "invalidEvidenceEvent"),
            (.invalidScanCohortJSON("x"), "invalidScanCohortJSON"),
            (.invalidStateTransition(jobId: "j", fromStatus: nil, toStatus: "running"), "invalidStateTransition"),
            (.evidenceEventBodyMismatch(id: "x"), "evidenceEventBodyMismatch"),
            (.encodingFailure("x"), "encodingFailure"),
        ]
        for (error, expected) in cases {
            #expect(BackfillJobRunner.caseName(of: error) == expected,
                    "caseName(\(expected)) returned the wrong token")
        }
    }

    @Test("bd-1tl: backfill runner persists results when refinement evidence shares a natural key")
    func runnerPersistsAcrossEvidenceNaturalKeyCollision() async throws {
        // bd-1tl: end-to-end repro of the on-device persistence failure.
        // Two refined spans returned by the FM cover the same line range
        // (firstLineRef == lastLineRef == 1) but with different bodies
        // (`commercialIntent`, `certainty`). Both spans flow through
        // `BackfillJobRunner.makeEvidenceEvents` and produce evidence events
        // with the same atomOrdinals JSON, the same scanCohortJSON, and
        // different evidenceJSON. Pre-playhead-fn0 this aborted the entire
        // refinement-pass batch or silently collapsed the second span.
        // Post-fix the scan row commits and BOTH distinct evidence rows
        // survive.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: 1,
                        lastLineRef: 1,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: nil,
                                lineRef: 1,
                                kind: .ctaPhrase,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.callToAction]
                    ),
                    // Second span: same line range, different body. The
                    // FM is well within its rights to emit overlapping
                    // refined spans for the same atoms — H3-1's throw
                    // turned that into a P0 persistence failure.
                    SpanRefinementSchema(
                        commercialIntent: .affiliate,
                        ownership: .thirdParty,
                        firstLineRef: 1,
                        lastLineRef: 1,
                        certainty: .moderate,
                        boundaryPrecision: .usable,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: nil,
                                lineRef: 1,
                                kind: .ctaPhrase,
                                certainty: .moderate
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.callToAction]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        #expect(passB.count >= 1, "passB scan row must persist; pre-fix this was 0")
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let refinementEvidence = evidence.filter { $0.eventType == "fm.spanRefinement" }
        #expect(refinementEvidence.count == 2, "distinct same-range evidence rows must both persist")
        #expect(evidence.filter { $0.eventType == OperationalMetrics.eventType }.count == 1)
        #expect(Set(refinementEvidence.map(\.id)).isSubset(of: Set(result.evidenceEventIds)))
        #expect(Set(result.evidenceEventIds) == Set(evidence.map(\.id)))
    }

    @Test("playhead-fn0: partial refinement refusals persist failure rows alongside surviving success rows")
    func partialRefinementRefusalsPersistFailureRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let segments = makeFMSegments(
            analysisAssetId: "asset-runner",
            transcriptVersion: "tx-runner-v1",
            lines: [
                (0, 8, "The hosts catch up before the break."),
                (8, 16, "This episode is brought to you by ExampleCo."),
                (16, 24, "Use code SAVE for twenty percent off.")
            ]
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: "asset-runner",
                transcriptVersion: "tx-runner-v1"
            ),
            transcriptVersion: "tx-runner-v1",
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

        let runtime = WindowedTestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                ),
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [2],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [])
            ],
            refinementFailures: [
                .refusal,
                nil
            ]
        ).runtime

        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(
                runtime: runtime,
                config: .init(
                    safetyMarginTokens: 5,
                    coarseMaximumResponseTokens: 6,
                    refinementMaximumResponseTokens: 16
                )
            ),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let result = try await runner.runPendingBackfill(for: inputs)

        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        #expect(passB.count == 2, "expected one surviving success row and one persisted refusal row")
        #expect(passB.contains { $0.status == .success })
        #expect(passB.contains { $0.status == .refusal && $0.disposition == .abstain })
        #expect(result.scanResultIds.count >= 3, "passA rows plus both passB outcomes should be reported")
    }

    @Test("partial coarse refusals persist failure rows alongside surviving success rows")
    func partialCoarseRefusalsPersistFailureRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let runtime = WindowedTestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .noAds,
                    support: nil
                )
            ],
            coarseFailures: [
                .refusal,
                nil
            ]
        ).runtime

        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(
                runtime: runtime,
                config: .init(
                    safetyMarginTokens: 5,
                    coarseMaximumResponseTokens: 6,
                    refinementMaximumResponseTokens: 16
                )
            ),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        let passA = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passA"
        )
        #expect(passA.count == 2, "expected one surviving success row and one persisted refusal row")
        #expect(passA.contains { $0.status == .success })
        #expect(passA.contains { $0.status == .refusal && $0.disposition == .abstain })
        #expect(result.scanResultIds.count >= 2)
    }

    // MARK: - bd-3vm: anchor encoding in spansJSON / EvidencePayload

    /// bd-3vm: round-trip an encoded refined span with anchors. The encoder
    /// must persist per-anchor identity tuples matching
    /// `BackfillJobRunner.anchorIdentityKey` (evidenceRef, lineRef, kind,
    /// resolutionSource) plus certainty, so downstream analytics and
    /// debugging can observe exactly which anchors justified a span.
    @Test("bd-3vm: encodeRefinedSpans round-trips anchor identity tuples")
    func encodeRefinedSpansRoundTripsAnchorIdentity() throws {
        let entry = EvidenceEntry(
            evidenceRef: 7,
            category: .url,
            matchedText: "example.com",
            normalizedText: "example.com",
            atomOrdinal: 3,
            startTime: 12,
            endTime: 15
        )
        let anchor1 = ResolvedEvidenceAnchor(
            entry: entry,
            lineRef: 3,
            kind: .url,
            certainty: .strong,
            resolutionSource: .evidenceRef,
            memoryWriteEligible: true
        )
        let anchor2 = ResolvedEvidenceAnchor(
            entry: nil,
            lineRef: 4,
            kind: .ctaPhrase,
            certainty: .moderate,
            resolutionSource: .lineRefFallback,
            memoryWriteEligible: false
        )
        let span = RefinedAdSpan(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: 3,
            lastLineRef: 4,
            firstAtomOrdinal: 3,
            lastAtomOrdinal: 4,
            certainty: .strong,
            boundaryPrecision: .precise,
            resolvedEvidenceAnchors: [anchor1, anchor2],
            memoryWriteEligible: false,
            alternativeExplanation: .none,
            reasonTags: [.promoCode]
        )

        let json = BackfillJobRunner.encodeRefinedSpansForTesting([span])
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(json)

        #expect(decoded.count == 1)
        let encodedSpan = try #require(decoded.first)
        #expect(encodedSpan.firstLineRef == 3)
        #expect(encodedSpan.lastLineRef == 4)
        #expect(encodedSpan.commercialIntent == "paid")
        #expect(encodedSpan.ownership == "thirdParty")
        #expect(encodedSpan.certainty == "strong")

        let anchors = try #require(encodedSpan.anchors)
        #expect(anchors.count == 2)

        let first = anchors[0]
        #expect(first.evidenceRef == 7)
        #expect(first.lineRef == 3)
        #expect(first.kind == "url")
        #expect(first.resolutionSource == "evidenceRef")
        #expect(first.certainty == "strong")

        let second = anchors[1]
        #expect(second.evidenceRef == nil)
        #expect(second.lineRef == 4)
        #expect(second.kind == "ctaPhrase")
        #expect(second.resolutionSource == "lineRefFallback")
        #expect(second.certainty == "moderate")
    }

    /// bd-3vm: rows persisted before this change lack the `anchors` field.
    /// The decoder must parse them without throwing, with anchors == nil.
    @Test("bd-3vm: decodeRefinedSpans accepts legacy JSON without anchors")
    func decodeRefinedSpansAcceptsLegacyJSONWithoutAnchors() throws {
        let legacy = #"""
        [{"firstLineRef":10,"lastLineRef":12,"commercialIntent":"paid","ownership":"thirdParty","certainty":"moderate"}]
        """#
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(legacy)
        #expect(decoded.count == 1)
        let span = try #require(decoded.first)
        #expect(span.firstLineRef == 10)
        #expect(span.lastLineRef == 12)
        #expect(span.commercialIntent == "paid")
        #expect(span.ownership == "thirdParty")
        #expect(span.certainty == "moderate")
        #expect(span.anchors == nil,
                "legacy rows have no anchors field; decoder must produce nil")
    }

    /// bd-3vm: same back-compat, but for EvidencePayload. Pre-change
    /// evidence_events rows never encoded an `anchors` field.
    @Test("bd-3vm: EvidencePayload decodes legacy JSON without anchors")
    func evidencePayloadDecodesLegacyJSONWithoutAnchors() throws {
        let legacy = #"""
        {"commercialIntent":"paid","ownership":"thirdParty","certainty":"strong","boundaryPrecision":"precise","firstLineRef":1,"lastLineRef":2,"jobId":"job-42","memoryWriteEligible":true}
        """#
        let payload = try BackfillJobRunner.decodeEvidencePayloadForTesting(legacy)
        #expect(payload.commercialIntent == "paid")
        #expect(payload.jobId == "job-42")
        #expect(payload.memoryWriteEligible == true)
        #expect(payload.anchors == nil)
    }

    /// bd-3vm: end-to-end. A refinement pass with a catalog-backed anchor
    /// must produce a persisted passB row whose spansJSON carries the
    /// anchor identity tuple. This is the observability path bd-1my's
    /// anchor-upgrade merge fix unblocked.
    @Test("bd-3vm: persisted spansJSON encodes anchor identity tuple")
    func persistedSpansJSONEncodesAnchorIdentityTuple() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let inputs = makeInputs()
        let entry = try #require(inputs.evidenceCatalog.entries.first,
                                 "test fixture must yield at least one catalog entry")
        let anchorKind: EvidenceAnchorKind
        switch entry.category {
        case .url: anchorKind = .url
        case .promoCode: anchorKind = .promoCode
        case .ctaPhrase: anchorKind = .ctaPhrase
        case .disclosurePhrase: anchorKind = .disclosurePhrase
        case .brandSpan: anchorKind = .brandSpan
        }

        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [entry.atomOrdinal],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: entry.atomOrdinal,
                        lastLineRef: entry.atomOrdinal,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: entry.evidenceRef,
                                lineRef: entry.atomOrdinal,
                                kind: anchorKind,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.promoCode]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: inputs)

        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        let successRows = passB.filter { $0.status == .success && $0.disposition == .containsAd }
        #expect(!successRows.isEmpty, "expected at least one containsAd passB row")
        let row = try #require(successRows.first)
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(row.spansJSON)
        #expect(!decoded.isEmpty, "spansJSON must contain at least one encoded span")
        let encodedSpan = try #require(decoded.first)
        let anchors = try #require(encodedSpan.anchors,
                                    "spansJSON must encode anchors field after bd-3vm")
        #expect(!anchors.isEmpty, "encoded span must carry at least one anchor")
        let firstAnchor = try #require(anchors.first)
        #expect(firstAnchor.evidenceRef == entry.evidenceRef)
        #expect(firstAnchor.lineRef == entry.atomOrdinal)
        #expect(firstAnchor.kind == anchorKind.rawValue)
        #expect(firstAnchor.resolutionSource == "evidenceRef")
        #expect(firstAnchor.certainty == "strong")

        // And the same anchor identity must reach evidence_events.evidenceJSON
        // (the EvidencePayload path).
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let refinementRows = evidence.filter { $0.eventType == "fm.spanRefinement" }
        #expect(!refinementRows.isEmpty)
        let evRow = try #require(refinementRows.first)
        let payload = try BackfillJobRunner.decodeEvidencePayloadForTesting(evRow.evidenceJSON)
        let payloadAnchors = try #require(payload.anchors,
                                           "EvidencePayload must encode anchors field after bd-3vm")
        #expect(!payloadAnchors.isEmpty)
        let payloadAnchor = try #require(payloadAnchors.first)
        #expect(payloadAnchor.evidenceRef == entry.evidenceRef)
        #expect(payloadAnchor.lineRef == entry.atomOrdinal)
        #expect(payloadAnchor.kind == anchorKind.rawValue)
        #expect(payloadAnchor.resolutionSource == "evidenceRef")
        #expect(payloadAnchor.certainty == "strong")
    }
}

// MARK: - Bug 11: Job-complete must imply at least one persisted scan row
//
// Captured xcappdata from a real device showed 7 backfill_jobs with
// status='complete' but 0 rows in semantic_scan_results — the runner marked
// jobs done without recording any audit trail. Cohort tracking, planner
// state, and forensic queries all assume a 1:N relationship from completed
// job to scan rows; a 1:0 relationship is a wiring bug.
//
// These tests pin the invariant: every admitted backfill job that reaches
// markBackfillJobComplete MUST have produced at least one row in
// semantic_scan_results keyed to the job's analysisAssetId+jobId. A
// "no work was performed" outcome is recorded as a sentinel row, not as
// silent absence.
@Suite("BackfillJobRunner — job-complete persistence invariant (Bug 11)")
struct BackfillJobRunnerJobCompletePersistenceInvariantTests {

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
            capabilitySnapshot: nil
        )
    }

    private func makePlannerContext() -> CoveragePlannerContext {
        CoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime,
        mode: FMBackfillMode = .shadow
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime, config: .default),
            coveragePlanner: CoveragePlanner(),
            mode: mode,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
    }

    // MARK: - Tests

    /// Captured-DB symptom: a backfill job runs through the admission
    /// pipeline with empty input segments (e.g. a transcript that
    /// atomized/segmented to nothing on a corner-case input). Today the
    /// runner marks the job complete without persisting any scan row,
    /// leaving a job with `status='complete'` and zero corresponding
    /// rows in `semantic_scan_results`. The fix must write a sentinel
    /// row so the asset+job pair appears in scan-result queries.
    @Test("admitted job with empty segments persists at least one scan row keyed by asset")
    func emptySegmentsAdmittedJobStillPersistsScanRow() async throws {
        let assetId = "asset-empty-segments"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime()

        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: assetId,
            transcriptVersion: "tx-empty-v1",
            entries: []
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-empty",
            segments: [],
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: "tx-empty-v1",
            plannerContext: makePlannerContext()
        )

        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)
        let result = try await runner.runPendingBackfill(for: inputs)

        // The job must be admitted — we want to test the persistence
        // path, not the device-defer path. CoveragePlanner emits at
        // least the fullEpisodeScan phase for cold-start state.
        #expect(!result.admittedJobIds.isEmpty,
                "test setup: at least one job should be admitted")

        // The captured-DB symptom: jobs marked complete with no
        // scan rows. After the fix, every admitted+completed job
        // must produce at least one scan row keyed to the asset.
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(!scans.isEmpty,
                "Bug 11: admitted+completed job left semantic_scan_results empty for asset=\(assetId)")

        // The sentinel row carries a structured `errorContext` so
        // forensic queries can distinguish "no work was done" from a
        // genuine FM scan that produced zero windows. This locks that
        // contract: at least one row must carry the noWork marker.
        // (`reuseScope` is not surfaced back through `fetchSemanticScanResults`
        // — it's an INSERT-time-only field that contributes to reuseKeyHash —
        // so we use the persisted `errorContext` column instead.)
        let noWorkRows = scans.filter {
            ($0.errorContext ?? "").hasPrefix("noWork:")
        }
        #expect(!noWorkRows.isEmpty,
                "Bug 11: empty-segments path must persist a noWork sentinel row")

        // Backfill_jobs row should reflect status=complete (the bug
        // pre-fix: complete row, zero scan rows).
        let job = try #require(
            try await store.fetchBackfillJob(byId: result.admittedJobIds[0]),
            "admitted job should be persisted"
        )
        #expect(job.status == .complete,
                "admitted job should reach .complete after runner returns")
    }

    /// Edge case 1: the FM scan succeeds with zero windows of interest
    /// (e.g. the @Generable path returns "noAds" for every plan and
    /// somehow no windows get recorded, simulated here by a runtime
    /// that returns success but the runner's coarse path produces
    /// only no-ad rows). The test asserts the strict invariant that
    /// every admitted job must produce at least one scan row.
    @Test("admitted job with no-ads coarse output still persists scan rows for every plan")
    func noAdsCoarseOutputPersistsScanRowPerPlan() async throws {
        let assetId = "asset-noads"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime()

        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: "tx-noads-v1",
            lines: [
                (0, 30, "Welcome to the show. Today's topic is craftsmanship."),
                (30, 60, "Our guest has been working in the field for decades."),
                (60, 90, "We talked about technique, mistakes, and recovery.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: "tx-noads-v1"
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-noads",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: "tx-noads-v1",
            plannerContext: makePlannerContext()
        )

        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)
        let result = try await runner.runPendingBackfill(for: inputs)

        #expect(!result.admittedJobIds.isEmpty)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(!scans.isEmpty,
                "Bug 11: noAds coarse output left semantic_scan_results empty")

        // Every coarse window must produce a passA row, even when
        // disposition is .noAds (current behavior at line 852-872).
        // This locks that contract.
        let passARows = scans.filter { $0.scanPass == "passA" }
        #expect(!passARows.isEmpty,
                "Bug 11: at least one passA row must be persisted per admitted job")
    }

    /// Edge case 2: foreign-key wiring. The asset must exist in
    /// `analysis_assets` before any `semantic_scan_results` row can be
    /// inserted (FK on analysisAssetId). The runner already relies on
    /// this — the test pins the assumption so a future schema change
    /// that drops the FK doesn't silently skew the persistence path.
    @Test("scan-result FK to analysis_assets is enforced — runner errors are surfaced not swallowed")
    func scanResultForeignKeyFailureIsSurfaced() async throws {
        // Intentionally do NOT insert the asset. The store's FK on
        // semantic_scan_results.analysisAssetId should reject any
        // insert attempt. The runner's catch arms must propagate or
        // mark the job .failed — not silently mark .complete with 0
        // rows (that would re-introduce Bug 11 via the FK path).
        let assetId = "asset-orphan"
        let store = try await makeTestStore()
        // (no insertAsset)

        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [0],
                        certainty: .strong
                    )
                )
            ]
        )
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: "tx-orphan-v1",
            lines: [
                (0, 30, "Use code DEAL for 20 percent off at example dot com.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: "tx-orphan-v1"
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-orphan",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: "tx-orphan-v1",
            plannerContext: makePlannerContext()
        )

        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        // The runner's outer call should NOT throw — its catch arms
        // convert store errors into markBackfillJobFailed transitions
        // and continue. But the resulting backfill_jobs row must NOT
        // be `.complete` with zero scan rows. It must be `.failed`
        // (or `.queued` if backfill_jobs FK also rejects), because
        // that's the only honest signal: "we tried, the store said
        // no, we recorded the failure".
        do {
            _ = try await runner.runPendingBackfill(for: inputs)
        } catch {
            // Acceptable: the FK rejection on backfill_jobs itself
            // prevents enqueue. Either way the contract holds —
            // we did NOT silently complete with 0 rows.
            return
        }

        // If the runner returned successfully, every admitted job
        // must NOT be `.complete` with zero scan rows for the asset.
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        if scans.isEmpty {
            // The bug: at least one job marked .complete with 0 rows.
            // Walk every backfill_jobs row that targeted this orphan
            // asset and assert none of them is `.complete`.
            // We can't fetch by asset directly without a helper, so
            // we reconstruct the deterministic ids the runner would
            // emit and check each one. If any are .complete, that's
            // Bug 11 reappearing.
            for phase in BackfillJobPhase.allCases {
                let jobId = BackfillJobRunner.makeJobId(
                    analysisAssetId: assetId,
                    phase: phase,
                    offset: 0
                )
                if let job = try await store.fetchBackfillJob(byId: jobId) {
                    #expect(job.status != .complete,
                            "Bug 11 (FK path): job \(jobId) marked .complete despite 0 scan rows for orphan asset")
                }
            }
        }
    }

    /// Edge case 3: a job that's deferred at admission time must NOT
    /// produce a sentinel row — only admitted+completed jobs should.
    /// This is the negative companion to the main test: it locks the
    /// fix's scope so the sentinel write doesn't fire on non-admitted
    /// paths.
    @Test("deferred job (thermal) does not insert a sentinel row")
    func deferredJobDoesNotInsertSentinelRow() async throws {
        let assetId = "asset-deferred"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime()

        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: "tx-deferred-v1",
            lines: [(0, 30, "Editorial line.")]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: "tx-deferred-v1"
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-deferred",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: "tx-deferred-v1",
            plannerContext: makePlannerContext()
        )

        // Use a thermal-throttled snapshot so AdmissionController defers
        // every job before runJob is called.
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime, config: .default),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makeThermalThrottledSnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let result = try await runner.runPendingBackfill(for: inputs)
        #expect(!result.deferredJobIds.isEmpty,
                "test setup: jobs should defer under thermal throttle")
        #expect(result.admittedJobIds.isEmpty,
                "test setup: no jobs should be admitted under thermal throttle")

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(scans.isEmpty,
                "deferred jobs must NOT produce sentinel rows (the fix only applies to admitted+completed)")
    }
}

private actor WindowedTestFMRuntime {
    private var coarseQueue: [CoarseScreeningSchema]
    private var refinementQueue: [RefinementWindowSchema]
    private var coarseFailureQueue: [TestFMRuntimeFailure?]
    private var refinementFailureQueue: [TestFMRuntimeFailure?]

    init(
        coarseResponses: [CoarseScreeningSchema] = [],
        refinementResponses: [RefinementWindowSchema] = [],
        coarseFailures: [TestFMRuntimeFailure?] = [],
        refinementFailures: [TestFMRuntimeFailure?] = []
    ) {
        self.coarseQueue = coarseResponses
        self.refinementQueue = refinementResponses
        self.coarseFailureQueue = coarseFailures
        self.refinementFailureQueue = refinementFailures
    }

    nonisolated var runtime: FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 295 },
            tokenCount: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 5
            },
            coarseSchemaTokenCount: { 4 },
            refinementSchemaTokenCount: { 8 },
            boundarySchemaTokenCount: { 8 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in try await self.nextCoarse() },
                    respondRefinement: { _ in try await self.nextRefinement() }
                )
            },
            // playhead-pmp9: no-op backoff sleep so rate-limit retries are instant.
            backoffSleep: { _ in }
        )
    }

    private func nextCoarse() throws -> CoarseScreeningSchema {
        if !coarseFailureQueue.isEmpty {
            let failure = coarseFailureQueue.removeFirst()
            if let failure {
                throw failure.error
            }
        }
        if coarseQueue.isEmpty {
            return CoarseScreeningSchema(disposition: .noAds, support: nil)
        }
        return coarseQueue.removeFirst()
    }

    private func nextRefinement() throws -> RefinementWindowSchema {
        if !refinementFailureQueue.isEmpty {
            let failure = refinementFailureQueue.removeFirst()
            if let failure {
                throw failure.error
            }
        }
        if refinementQueue.isEmpty {
            return RefinementWindowSchema(spans: [])
        }
        return refinementQueue.removeFirst()
    }
}
