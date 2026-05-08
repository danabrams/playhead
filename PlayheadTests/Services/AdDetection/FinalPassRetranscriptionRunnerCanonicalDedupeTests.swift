// FinalPassRetranscriptionRunnerCanonicalDedupeTests.swift
// playhead-hygc.1.5: pin the canonical-span dedupe and zero-length
// rejection at the runner level. The May 6 dogfood DB carried duplicate
// `final_pass_jobs` rows because two AdWindow rows with different ids
// represented the same time span; this test suite proves the runner
// now collapses them into one canonical job + N alias rows BEFORE any
// re-transcription work runs.
//
// These tests are deliberately separate from the original
// `FinalPassRetranscriptionRunnerTests` suite so that:
//   * the existing 948-line file stays readable, and
//   * the dedupe contract has its own discoverable home.
//
// Coverage matrix:
//   1. duplicate-span fanout (May 6 fixture-shape) → ONE canonical job,
//      N-1 aliases.
//   2. zero-length / inverted-span windows are rejected at filter time;
//      no job row ever lands.
//   3. cross-launch dedupe: a pre-existing canonical row absorbs new
//      contributing AdWindow ids as aliases (no competing job created).
//   4. progress derivation: `canonicalCompleteFinalPassSpans` collapses
//      the row count to canonical-span count after a full drain.

import Foundation
import Testing

@testable import Playhead

@Suite("FinalPassRetranscriptionRunner canonical-span dedupe (playhead-hygc.1.5)")
struct FinalPassRetranscriptionRunnerCanonicalDedupeTests {

    // MARK: - Fixtures (mirror the originals so tests can be read in isolation)

    private func makeAsset(
        id: String = "asset-fp",
        finalPassCoverageEndTime: Double? = nil
    ) -> AnalysisAsset {
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
            finalPassCoverageEndTime: finalPassCoverageEndTime
        )
    }

    private func makeAdWindow(
        id: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        confidence: Double = 0.9
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            boundaryState: "tentative",
            decisionState: "pending",
            detectorVersion: "v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fixture",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    private func makeSnapshot() -> CapabilitySnapshot {
        CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10 * 1024 * 1024 * 1024,
            capturedAt: .now
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        audio: AnalysisAudioProviding
    ) -> FinalPassRetranscriptionRunner {
        FinalPassRetranscriptionRunner(
            store: store,
            speechService: SpeechService(recognizer: StubSpeechRecognizer()),
            audioProvider: audio,
            capabilitySnapshotProvider: { self.makeSnapshot() },
            batteryLevelProvider: { 0.85 },
            chargeStateProvider: { true },
            confidenceFloor: 0.5,
            modelVersion: "test-final-v1"
        )
    }

    private func makeInput(
        assetId: String = "asset-fp"
    ) -> FinalPassRetranscriptionRunner.AssetInput {
        let url = LocalAudioURL(URL(fileURLWithPath: "/tmp/\(assetId).m4a"))!
        return FinalPassRetranscriptionRunner.AssetInput(
            analysisAssetId: assetId,
            podcastId: "pod-1",
            audioURL: url,
            episodeId: "ep-\(assetId)"
        )
    }

    // MARK: - Duplicate-span dedupe (May 6 fixture shape)

    @Test("four AdWindows with the same span produce ONE canonical final_pass_jobs row + 3 aliases")
    func duplicateSpanCollapsesToOneCanonicalJob() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // May 6 dogfood pattern: same span persisted as 4 distinct
        // AdWindow rows. (asset 1874D961 actually had this shape on
        // span 3386.0-3394.14.)
        for id in ["w-a", "w-b", "w-c", "w-d"] {
            try await store.insertAdWindow(
                makeAdWindow(
                    id: id,
                    analysisAssetId: "asset-fp",
                    startTime: 3386.0,
                    endTime: 3394.14
                )
            )
        }

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(
                id: 0,
                episodeID: "ep-asset-fp",
                startTime: 3386.0,
                duration: 8.14,
                samples: []
            )
        ]
        let runner = makeRunner(store: store, audio: audio)
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        // Exactly ONE canonical job admitted.
        #expect(result.topLevelDeferReason == nil)
        #expect(result.admittedJobIds.count == 1, "4 same-span AdWindows must collapse to 1 admitted job (admitted=\(result.admittedJobIds))")

        // The persisted row count proves the dedupe happened at insert
        // time, not just in the in-memory result.
        let persistedJobs = try await store.fetchFinalPassJobs(forAsset: "asset-fp")
        #expect(persistedJobs.count == 1, "exactly one final_pass_jobs row should persist; got \(persistedJobs.map(\.jobId))")

        // Canonical AdWindow id is the lexicographically-lowest of the
        // group ("w-a"). The 3 aliases are "w-b", "w-c", "w-d".
        let canonical = try #require(persistedJobs.first)
        #expect(canonical.adWindowId == "w-a")
        let aliases = try await store.fetchFinalPassJobAliases(jobId: canonical.jobId)
        #expect(aliases.sorted() == ["w-b", "w-c", "w-d"],
                "the 3 non-canonical contributing AdWindow ids must be recorded as aliases for audit visibility")
    }

    @Test("canonical pick is lex-lowest id regardless of insertion / fetch order")
    func canonicalPickIsLexLowestRegardlessOfInsertionOrder() async throws {
        // Adversarial: insert windows in REVERSE lex order so a buggy
        // implementation that picks "first encountered" / "first by
        // insertion order" / "first by SQLite return order" would pick
        // "z-window" instead of "a-window". The canonical-pick rule is
        // "lex-lowest id" — the only impl that passes is one that
        // sorts by id BEFORE picking. Run twice to also catch any
        // residual order dependency.
        for run in 0..<2 {
            let store = try await makeTestStore()
            let assetId = "asset-fp-run\(run)"
            try await store.insertAsset(makeAsset(id: assetId))

            // Reverse-lex insertion. fetchAdWindows ORDER BY startTime
            // ties leave row order undefined, so we cannot assume any
            // particular fetch order — the dedupe MUST sort.
            let ids = ["z-window", "m-window", "a-window", "p-window"]
            for id in ids {
                try await store.insertAdWindow(
                    makeAdWindow(
                        id: id,
                        analysisAssetId: assetId,
                        startTime: 100.0,
                        endTime: 130.0
                    )
                )
            }

            let audio = StubAnalysisAudioProvider()
            audio.shardsToReturn = [
                AnalysisShard(id: 0, episodeID: "ep-\(assetId)", startTime: 100.0, duration: 30.0, samples: [])
            ]
            let runner = makeRunner(store: store, audio: audio)
            _ = try await runner.runFinalPassBackfill(
                for: FinalPassRetranscriptionRunner.AssetInput(
                    analysisAssetId: assetId,
                    podcastId: "pod-1",
                    audioURL: LocalAudioURL(URL(fileURLWithPath: "/tmp/\(assetId).m4a"))!,
                    episodeId: "ep-\(assetId)"
                )
            )

            let persisted = try await store.fetchFinalPassJobs(forAsset: assetId)
            #expect(persisted.count == 1)
            let canonical = try #require(persisted.first)
            #expect(canonical.adWindowId == "a-window",
                    "lex-lowest pick must select 'a-window' regardless of insertion order; got \(canonical.adWindowId)")
            // jobId encodes the canonical adWindowId — verify the
            // canonical bookkeeping is consistent.
            #expect(canonical.jobId == "fpj-\(assetId)-a-window")
            let aliases = try await store.fetchFinalPassJobAliases(jobId: canonical.jobId)
            #expect(aliases.sorted() == ["m-window", "p-window", "z-window"],
                    "the 3 non-canonical ids must be recorded as aliases")
        }
    }

    @Test("two distinct spans on the same asset produce two canonical jobs (no over-collapsing)")
    func distinctSpansAreNotCollapsed() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Span A duplicated 2× and span B duplicated 2×. Result must
        // be exactly TWO canonical jobs (one per span), each with one
        // alias.
        try await store.insertAdWindow(makeAdWindow(id: "a-1", analysisAssetId: "asset-fp", startTime: 100, endTime: 130))
        try await store.insertAdWindow(makeAdWindow(id: "a-2", analysisAssetId: "asset-fp", startTime: 100, endTime: 130))
        try await store.insertAdWindow(makeAdWindow(id: "b-1", analysisAssetId: "asset-fp", startTime: 200, endTime: 230))
        try await store.insertAdWindow(makeAdWindow(id: "b-2", analysisAssetId: "asset-fp", startTime: 200, endTime: 230))

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 100, duration: 30, samples: []),
            AnalysisShard(id: 1, episodeID: "ep-asset-fp", startTime: 200, duration: 30, samples: [])
        ]
        let runner = makeRunner(store: store, audio: audio)
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.topLevelDeferReason == nil)
        #expect(result.admittedJobIds.count == 2, "2 distinct spans must NOT be collapsed; got \(result.admittedJobIds)")

        let persisted = try await store.fetchFinalPassJobs(forAsset: "asset-fp")
        #expect(persisted.count == 2)
        let spans = persisted.map { AnalysisStore.canonicalSpanKey(start: $0.windowStartTime, end: $0.windowEndTime) }.sorted()
        #expect(spans == ["100.000-130.000", "200.000-230.000"])
    }

    // MARK: - Zero-length rejection

    @Test("zero-length AdWindow (startTime == endTime) is rejected and creates no job")
    func zeroLengthWindowIsRejected() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // A pathological AdWindow whose start and end are identical —
        // the May 6 fixture had a handful of these and the pre-fix
        // runner happily marked them complete, polluting coverage.
        try await store.insertAdWindow(
            makeAdWindow(
                id: "w-degenerate",
                analysisAssetId: "asset-fp",
                startTime: 42.0,
                endTime: 42.0
            )
        )

        let audio = StubAnalysisAudioProvider()
        let runner = makeRunner(store: store, audio: audio)
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.topLevelDeferReason == nil)
        #expect(result.admittedJobIds.isEmpty,
                "zero-length window must not produce a final_pass_jobs row")
        #expect(result.reTranscribedWindowIds.isEmpty)

        let persisted = try await store.fetchFinalPassJobs(forAsset: "asset-fp")
        #expect(persisted.isEmpty,
                "no final_pass_jobs row should land for a degenerate window")
    }

    @Test("inverted-span AdWindow (endTime < startTime) is rejected and creates no job")
    func invertedSpanWindowIsRejected() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.insertAdWindow(
            makeAdWindow(
                id: "w-inverted",
                analysisAssetId: "asset-fp",
                startTime: 100.0,
                endTime: 90.0
            )
        )

        let runner = makeRunner(store: store, audio: StubAnalysisAudioProvider())
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.admittedJobIds.isEmpty)
        let persisted = try await store.fetchFinalPassJobs(forAsset: "asset-fp")
        #expect(persisted.isEmpty)
    }

    // MARK: - Cross-launch dedupe

    @Test("cross-launch: a pre-existing canonical row absorbs new contributing windows as aliases")
    func crossLaunchAbsorbsNewWindowsAsAliases() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Simulate a prior process having already landed the canonical
        // row for this span (queued, not complete — so the runner
        // should still consider it; the canonical span lookup happens
        // BEFORE the status filter).
        let priorCanonical = FinalPassJob(
            jobId: "fpj-asset-fp-prior",
            analysisAssetId: "asset-fp",
            podcastId: "pod-1",
            adWindowId: "prior",
            windowStartTime: 500.0,
            windowEndTime: 530.0,
            status: .queued,
            retryCount: 0,
            deferReason: nil,
            createdAt: 1.0
        )
        try await store.insertOrIgnoreFinalPassJob(priorCanonical)

        // The "current process" sees TWO new AdWindow rows for the same
        // span; without cross-launch dedupe these would each create
        // their own competing canonical row.
        try await store.insertAdWindow(makeAdWindow(id: "fresh-1", analysisAssetId: "asset-fp", startTime: 500, endTime: 530))
        try await store.insertAdWindow(makeAdWindow(id: "fresh-2", analysisAssetId: "asset-fp", startTime: 500, endTime: 530))

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 500, duration: 30, samples: [])
        ]
        let runner = makeRunner(store: store, audio: audio)
        _ = try await runner.runFinalPassBackfill(for: makeInput())

        // Still exactly ONE canonical job — the prior one. No new row
        // landed.
        let persisted = try await store.fetchFinalPassJobs(forAsset: "asset-fp")
        #expect(persisted.count == 1, "cross-launch lookup must absorb new same-span windows as aliases, not enqueue competing jobs; persisted=\(persisted.map(\.jobId))")
        #expect(persisted.first?.jobId == "fpj-asset-fp-prior")

        // Both fresh AdWindow ids are recorded as aliases.
        let aliases = try await store.fetchFinalPassJobAliases(jobId: "fpj-asset-fp-prior")
        #expect(aliases.sorted() == ["fresh-1", "fresh-2"])
    }

    @Test("cross-launch with COMPLETE prior canonical: no new work runs (short-circuit)")
    func crossLaunchCompleteCanonicalShortCircuits() async throws {
        // Acceptance criterion: "if every (start,end) bucket already
        // has a complete job, no new work is enqueued." Specifically:
        //   * the canonical row stays `.complete` (no flip to running)
        //   * `reTranscribedWindowIds` is empty
        //   * `admittedJobIds` is empty (the runner skips the loop body
        //     for already-complete canonicals)
        //   * BUT new contributing AdWindow ids are still recorded as
        //     aliases for audit visibility (the only useful side-effect
        //     of finding a pre-existing complete canonical row).
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Land a prior canonical row, then force it complete.
        let priorCanonical = FinalPassJob(
            jobId: "fpj-asset-fp-prior",
            analysisAssetId: "asset-fp",
            podcastId: "pod-1",
            adWindowId: "prior",
            windowStartTime: 600.0,
            windowEndTime: 630.0,
            status: .queued,
            retryCount: 0,
            deferReason: nil,
            createdAt: 1.0
        )
        try await store.insertOrIgnoreFinalPassJob(priorCanonical)
        try await store.forceFinalPassJobStateForTesting(
            jobId: priorCanonical.jobId,
            status: .complete
        )

        // New AdWindow rows for the same span land in this process.
        try await store.insertAdWindow(makeAdWindow(id: "fresh-c1", analysisAssetId: "asset-fp", startTime: 600, endTime: 630))
        try await store.insertAdWindow(makeAdWindow(id: "fresh-c2", analysisAssetId: "asset-fp", startTime: 600, endTime: 630))

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 600, duration: 30, samples: [])
        ]
        let runner = makeRunner(store: store, audio: audio)
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        // Short-circuit: NO ASR work ran, no admittance, no re-transcription.
        #expect(result.admittedJobIds.isEmpty,
                "complete canonical short-circuits the loop body — no admittance should occur; got \(result.admittedJobIds)")
        #expect(result.reTranscribedWindowIds.isEmpty,
                "no re-transcription should run when canonical is already complete; got \(result.reTranscribedWindowIds)")

        // The complete row stays complete.
        let persisted = try await store.fetchFinalPassJobs(forAsset: "asset-fp")
        #expect(persisted.count == 1)
        #expect(persisted.first?.status == .complete,
                "complete canonical must NOT be flipped to running/queued by the runner")

        // Aliases ARE still recorded (audit visibility for the new
        // contributing windows).
        let aliases = try await store.fetchFinalPassJobAliases(jobId: priorCanonical.jobId)
        #expect(aliases.sorted() == ["fresh-c1", "fresh-c2"],
                "new contributors must still be recorded as aliases even when canonical is complete")
    }

    // MARK: - Progress derivation post-drain

    @Test("after dedupe, canonicalCompleteFinalPassSpans returns one entry per canonical span")
    func progressDerivationCollapsesAfterDrain() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Same May 6 pattern as the duplicate-span test, plus a
        // second distinct span also duplicated. After the runner
        // drains, the canonical-progress view returns exactly 2 rows.
        for id in ["a-1", "a-2", "a-3"] {
            try await store.insertAdWindow(makeAdWindow(id: id, analysisAssetId: "asset-fp", startTime: 3386.0, endTime: 3394.14))
        }
        for id in ["b-1", "b-2"] {
            try await store.insertAdWindow(makeAdWindow(id: id, analysisAssetId: "asset-fp", startTime: 24.0, endTime: 38.16))
        }

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 24.0, duration: 14.16, samples: []),
            AnalysisShard(id: 1, episodeID: "ep-asset-fp", startTime: 3386.0, duration: 8.14, samples: [])
        ]
        let runner = makeRunner(store: store, audio: audio)
        _ = try await runner.runFinalPassBackfill(for: makeInput())

        let spans = try await store.canonicalCompleteFinalPassSpans(forAsset: "asset-fp")
        #expect(spans.count == 2, "2 distinct canonical spans, regardless of contributing-row count")
        let keys = spans.map(\.canonicalSpanKey).sorted()
        #expect(keys == ["24.000-38.160", "3386.000-3394.140"])

        // The canonical row's adWindowId + every alias must surface in
        // the contributing-id list. Span 3386 had three contributors;
        // span 24 had two.
        let span3386 = try #require(spans.first { $0.canonicalSpanKey == "3386.000-3394.140" })
        #expect(span3386.adWindowIds.sorted() == ["a-1", "a-2", "a-3"])
        let span24 = try #require(spans.first { $0.canonicalSpanKey == "24.000-38.160" })
        #expect(span24.adWindowIds.sorted() == ["b-1", "b-2"])
    }
}
