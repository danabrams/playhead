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

    // MARK: - playhead-jzj0 review R1: jobId is the AdWindow id, identity is the span

    @Test("an AdWindow whose span moves under a stable id converges instead of re-ASRing every launch")
    func spanShiftUnderStableIdConverges() async throws {
        // The AdWindow id is stable across a span change BY DESIGN:
        // `AdDetectionService.reconcileHotPathWindows` reuses
        // `existing.id` while taking `startTime`/`endTime` from the
        // fresh run, and `AnalysisStore.updateAdWindowHotPathCandidate`
        // writes the new bounds onto that same row.
        //
        // The final-pass job's IDENTITY is the canonical span, but its
        // jobId is derived from the AdWindow id. After a shift the span
        // lookup misses (new key) while `INSERT OR IGNORE` is silently
        // ignored (old jobId) — so an implementation that trusts the
        // locally-constructed `FinalPassJob` sees `.queued` for a row
        // that is `complete`, re-decodes and re-transcribes the window,
        // and does it again on EVERY subsequent launch because nothing
        // it writes changes the outcome.
        //
        // Before playhead-jzj0 the `endTime > watermark` clause hid this
        // whenever the frontier had passed the window — which is the
        // normal state. Removing that clause is what exposes it, so the
        // convergence pin belongs to this bead.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(
                id: "w-1",
                analysisAssetId: "asset-fp",
                startTime: 100.0,
                endTime: 160.0
            )
        )

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 100.0, duration: 80.0, samples: [])
        ]
        let runner = makeRunner(store: store, audio: audio)
        let input = makeInput()

        // Drain 1 — the original span.
        let first = try await runner.runFinalPassBackfill(for: input)
        #expect(first.reTranscribedWindowIds == ["w-1"])
        #expect(audio.decodeCallCount == 1)

        // The detector refines the boundary: SAME row id, wider span.
        let widened = makeAdWindow(
            id: "w-1",
            analysisAssetId: "asset-fp",
            startTime: 100.0,
            endTime: 175.0
        )
        try await store.updateAdWindowHotPathCandidate(widened)

        // Drain 2 — the new span is genuinely new audio, so it SHOULD
        // run exactly once. (An implementation that reads the persisted
        // row but never disambiguates the jobId would drop it here and
        // silently never transcribe [160, 175] — the permanent-exclusion
        // class this bead closed, arrived at from the other side.)
        let second = try await runner.runFinalPassBackfill(for: input)
        #expect(second.reTranscribedWindowIds == ["w-1"],
                "the widened span is new audio and must get a final pass")
        #expect(audio.decodeCallCount == 2)

        // Both spans must now hold their own `complete` row — the old
        // span's completed work is real history and is not overwritten.
        let spans = try await store.canonicalCompleteFinalPassSpans(forAsset: "asset-fp")
        #expect(spans.map(\.canonicalSpanKey).sorted()
                == ["100.000-160.000", "100.000-175.000"],
                "got \(spans.map(\.canonicalSpanKey).sorted())")

        // Drain 3 — THE PIN. Nothing changed, so nothing may run. A
        // runner that trusts the local struct decodes and re-ASRs here,
        // and on every launch after it, for ever.
        let third = try await runner.runFinalPassBackfill(for: input)
        #expect(third.reTranscribedWindowIds.isEmpty,
                "a converged asset must be a no-op; got \(third.reTranscribedWindowIds)")
        #expect(third.admittedJobIds.isEmpty)
        #expect(audio.decodeCallCount == 2,
                "no audio decode may run for a converged asset; decodeCallCount=\(audio.decodeCallCount)")
    }

    @Test("a span-shifted jobId collision does not resurrect a complete row as queued")
    func spanShiftDoesNotMisreportPersistedStatus() async throws {
        // Narrower pin on the same defect, stated in terms of the row
        // rather than the drain: after the shift, the ORIGINAL row must
        // still read `complete` (its jobId was never re-inserted) and
        // the new span must occupy a DIFFERENT jobId that still carries
        // the `fpj-<asset>-<adWindowId>` prefix diagnostics key off.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w-1", analysisAssetId: "asset-fp", startTime: 100.0, endTime: 160.0)
        )
        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 100.0, duration: 80.0, samples: [])
        ]
        let runner = makeRunner(store: store, audio: audio)
        _ = try await runner.runFinalPassBackfill(for: makeInput())

        try await store.updateAdWindowHotPathCandidate(
            makeAdWindow(id: "w-1", analysisAssetId: "asset-fp", startTime: 100.0, endTime: 175.0)
        )
        _ = try await runner.runFinalPassBackfill(for: makeInput())

        let rows = try await store.fetchFinalPassJobs(forAsset: "asset-fp")
        #expect(rows.count == 2, "one row per canonical span; got \(rows.map(\.jobId))")
        let original = try #require(rows.first { $0.jobId == "fpj-asset-fp-w-1" })
        #expect(original.status == .complete)
        #expect(original.windowEndTime == 160.0,
                "the original row's span must not be rewritten under it")
        let shifted = try #require(rows.first { $0.jobId != "fpj-asset-fp-w-1" })
        #expect(shifted.jobId.hasPrefix("fpj-asset-fp-w-1"),
                "the span-qualified id keeps the jobId prefix diagnostics resolve on; got \(shifted.jobId)")
        #expect(shifted.windowEndTime == 175.0)
        #expect(shifted.status == .complete)
    }

    @Test("the enqueue branch drives off the PERSISTED row, not the struct it just built")
    func enqueueBranchReadsBackThePersistedRow() async throws {
        // playhead-jzj0 review R2 — a pin on the SECOND half of R1's fix.
        //
        // R1 closed the span-shift defect with two independent steps:
        //   1. disambiguate the jobId when a row already holds it under a
        //      different canonical span, and
        //   2. read back whatever row `INSERT OR IGNORE` actually left
        //      behind and drive the drain off THAT.
        //
        // Step 1 alone satisfies every span-shift assertion in this file,
        // so deleting step 2 was a SURVIVING mutant at R2 (`job = newJob`,
        // 91/91 green). It is not redundant, though: step 1 only fires when
        // the occupying row's span DIFFERS. When a row occupies the natural
        // jobId at the SAME span but the span lookup cannot see it, no
        // disambiguation happens, `INSERT OR IGNORE` is ignored, and only
        // the read-back stands between the runner and a `.queued`-shaped
        // local struct describing a row that is `complete` — which is
        // exactly the H1 shape: `markFinalPassJobRunning` silently no-ops
        // (its IN-clause excludes `'complete'`), a full decode + ASR runs,
        // and it runs again on every admitted sweep, for ever.
        //
        // In production that corridor is a pre-v25 row whose
        // `canonicalSpanKey` backfill was missed — `findFinalPassJob`
        // returns nil for a NULL key by documented contract. That state is
        // not reachable through the store's API, so the fixture below
        // constructs the same PRECONDITION through the other documented
        // route into it: `findFinalPassJob` filters on `analysisAssetId`
        // while `fetchFinalPassJob(byId:)` does not, so a row whose jobId
        // is the one THIS asset would mint is invisible to the span lookup
        // and visible to the id lookup. The runner cannot tell the two
        // corridors apart — both hand it an occupied PK at a matching span.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAsset(makeAsset(id: "asset-other"))
        try await store.insertAdWindow(
            makeAdWindow(id: "w-1", analysisAssetId: "asset-fp", startTime: 10, endTime: 30)
        )
        // Occupies `fpj-asset-fp-w-1` — the id the runner will compute for
        // (asset-fp, w-1) — at the SAME span, but owned by another asset.
        try await store.insertOrIgnoreFinalPassJob(
            FinalPassJob(
                jobId: "fpj-asset-fp-w-1",
                analysisAssetId: "asset-other",
                podcastId: "pod-1",
                adWindowId: "w-1",
                windowStartTime: 10,
                windowEndTime: 30,
                status: .queued,
                retryCount: 0,
                deferReason: nil,
                createdAt: 1_000.0
            )
        )
        try await store.markFinalPassJobComplete(jobId: "fpj-asset-fp-w-1")

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]
        let runner = makeRunner(store: store, audio: audio)
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        // THE PIN. The occupied row is `complete`, so nothing may run —
        // and `decodeCallCount` is what says the expensive work
        // specifically did not happen, rather than that the drain merely
        // reported nothing. A runner that trusts its local struct decodes
        // and re-ASRs here, and on every launch after it.
        #expect(result.topLevelDeferReason == nil)
        #expect(audio.decodeCallCount == 0,
                "a complete row at the natural jobId must retire the window without a decode; decodeCallCount=\(audio.decodeCallCount)")
        #expect(result.reTranscribedWindowIds.isEmpty,
                "got \(result.reTranscribedWindowIds)")
        #expect(result.admittedJobIds.isEmpty)

        // And no competing row was minted for the span — the PK was
        // already taken, and the runner accepted that rather than
        // inventing a second identity for the same work.
        let rows = try await store.fetchFinalPassJobs(forAsset: "asset-fp")
        #expect(rows.isEmpty, "got \(rows.map(\.jobId))")
    }
}
