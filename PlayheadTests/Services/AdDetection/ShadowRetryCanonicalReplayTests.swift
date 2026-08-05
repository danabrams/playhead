// ShadowRetryCanonicalReplayTests.swift
// playhead-iu0t — the shadow-retry drain replays the CANONICAL transcript.
//
// THE FIELD ROW THESE FIXTURES ARE SHAPED FROM. On the 2026-08-03 device pull,
// asset 53FC53E3 carried 2,949 transcript chunks: 2,917 `fast` covering
// [0, 2490] and 32 `final` covering [2490, 2525.82]. Its one `backfill_jobs`
// row is `fm-041dedcf8293523e`, and re-deriving
// `BackfillJobRunner.makeJobIdForTesting` over each candidate chunk set says
// which set minted it:
//
//     V(final-only) = 55afd3e8bb41833c004ee7d4b1be7589 -> fm-041dedcf8293523e  <- the row
//     V(canonical)  = 61872a4d3e718e8bb4b8156dc2099444 -> fm-46ea5fad09da03b2
//     V(fast-only)  = 766ea8ba068ffce5e3f48be66c9fa13f -> fm-af6aeeb497745eca
//
// So the dispatcher was `retryShadowFMPhaseForSession`, whose
// `chunksForReplay = finalChunks.isEmpty ? chunks : finalChunks` handed the FM
// phase 32 chunks and threw 2,917 away. The job scanned one 36 s window, called
// itself `complete` 23 s after creation at an `adScanFraction` of 0.0142, and
// `countResumableBackfillJobs` then read 0 — no path back.
//
// THE FIXTURE SHAPE IS THE WHOLE POINT, and it is what the pre-existing
// mixed-pass test did not have. `ShadowRetryTests.testBug9A_mixedPassPrefersFinal`
// gave its fast and final chunks IDENTICAL spans, so canonicalization drops
// every fast chunk and the final-only set and the canonical set are the same
// array — a fixture in which the defect is unobservable. Here the final chunks
// sit at the TAIL, disjoint from the fast coverage, exactly as they do in the
// field: `FinalPassRetranscriptionRunner` only re-transcribes around detected
// candidate windows, so the final set is candidate-local by construction.
// `fixtureDistinguishesTheTwoChunkSets` below is the control that keeps it that
// way.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-iu0t: the shadow-retry drain replays the canonical transcript")
struct ShadowRetryCanonicalReplayTests {

    private static let assetId = "asset-iu0t"
    private static let sessionId = "sess-iu0t"
    private static let podcastId = "pod-iu0t"

    /// Fast coverage ends here; the final pass picks up from it.
    private static let fastReachSec: Double = 360
    private static let episodeDurationSec: Double = 420

    // MARK: - Fixtures

    /// The 53FC53E3 shape in miniature: a long fast prefix and a short,
    /// candidate-local final tail that does not overlap it.
    private func mixedPassChunks(assetId: String = assetId) -> [TranscriptChunk] {
        let fastTexts = [
            "Welcome to the show. Today we're discussing podcasts and how to find them.",
            "Our guest has been working on this problem for the better part of a decade.",
            "Before we get into it, a quick word about how the numbers actually break down.",
            "The interesting part is what happens when you look at the second cohort.",
            "That result held up across every replication anyone has published since.",
            "Which brings us to the question everybody asks at this point in the story.",
            "There is a version of this that works, and a version that very much does not.",
            "We came back to the studio the next morning and ran the whole thing again.",
            "What surprised me was how little the ordering mattered in the end.",
            "So that is the setup, and here is where the conversation really starts.",
            "Back to our interview with our guest about technology trends and the future.",
            "We will pick that thread up again right after this short break."
        ]
        let finalTexts = [
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off.",
            "Sign up today at squarespace dot com slash show and make your own website."
        ]
        var chunks: [TranscriptChunk] = fastTexts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "fast-\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-fast-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "fast",
                modelVersion: "test-fast-v1",
                transcriptVersion: nil,
                atomOrdinal: idx
            )
        }
        // Persisted with a chunkIndex strictly above every fast row, the way
        // `FinalPassRetranscriptionRunner.nextFinalChunkIndex` writes them.
        chunks += finalTexts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "final-\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-final-\(idx)",
                chunkIndex: 1_000 + idx,
                startTime: Self.fastReachSec + Double(idx) * 30,
                endTime: Self.fastReachSec + Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-final-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
        return chunks
    }

    private func asset(id: String = assetId) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: Self.episodeDurationSec,
            fastTranscriptCoverageEndTime: Self.fastReachSec,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: Self.episodeDurationSec
        )
    }

    private func flaggedSession(id: String = sessionId, assetId: String = assetId) -> AnalysisSession {
        let now = Date().timeIntervalSince1970
        return AnalysisSession(
            id: id,
            analysisAssetId: assetId,
            state: "complete",
            startedAt: now,
            updatedAt: now,
            failureReason: nil,
            needsShadowRetry: true,
            shadowRetryPodcastId: Self.podcastId
        )
    }

    private func seededStore(
        chunks: [TranscriptChunk],
        assetId: String = assetId,
        sessionId: String = sessionId
    ) async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(asset(id: assetId))
        try await store.insertSession(flaggedSession(id: sessionId, assetId: assetId))
        try await store.insertTranscriptChunks(chunks)
        return store
    }

    private func shadowFactory() -> @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner {
        { store, mode in
            BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(
                    runtime: TestFMRuntime(
                        coarseResponses: [
                            CoarseScreeningSchema(
                                disposition: .containsAd,
                                support: CoarseSupportSchema(
                                    supportLineRefs: [1],
                                    certainty: .strong
                                )
                            )
                        ]
                    ).runtime
                ),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON()
            )
        }
    }

    private func service(
        store: AnalysisStore,
        mode: FMBackfillMode = .shadow,
        factory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)? = nil
    ) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: mode
            ),
            backfillJobRunnerFactory: factory ?? shadowFactory(),
            canUseFoundationModelsProvider: { true }
        )
    }

    // MARK: - Identity helpers, derived the way production derives them

    private func canonicalVersion(_ chunks: [TranscriptChunk]) -> String {
        SemanticScanClaim.transcriptVersion(forPersistedChunks: chunks)
    }

    private func finalOnlyVersion(_ chunks: [TranscriptChunk]) -> String {
        TranscriptAtomizer.transcriptVersionHash(
            chunks: chunks.filter { $0.pass == TranscriptPassType.final_.rawValue }
        )
    }

    // MARK: - The control that makes every rail below mean something

    /// **The fixture control.** Every assertion in this suite distinguishes the
    /// canonical chunk set from the final-only one, which is only possible if
    /// the fixture makes them different. A future edit that gave the final
    /// chunks the same spans as the fast ones would make canonicalization drop
    /// every fast chunk, collapse the two sets into one array, and turn this
    /// whole suite green against the unfixed code. That is exactly how the
    /// pre-iu0t mixed-pass test failed to see this bug for months.
    @Test("control: the fixture's final-only and canonical chunk sets are genuinely different")
    func fixtureDistinguishesTheTwoChunkSets() throws {
        let chunks = mixedPassChunks()
        let canonicalization = TranscriptChunkCanonicalizer.canonicalize(chunks)

        #expect(canonicalization.diagnostics.isPassthrough == false,
                "a mixed-pass fixture must not take the single-pass passthrough")
        #expect(canonicalization.diagnostics.droppedFastCount == 0,
                "the final tail must not overlap the fast prefix — a candidate-local final set")
        #expect(canonicalization.chunks.count == chunks.count)
        #expect(canonicalVersion(chunks) != finalOnlyVersion(chunks),
                "the two chunk sets must hash differently or nothing below can fail")

        // And the reach the defect discards, named as a numerator over a
        // denominator: 360 of 420 declared seconds live only in the fast pass.
        let finalReach = chunks
            .filter { $0.pass == TranscriptPassType.final_.rawValue }
            .map(\.startTime)
            .min()
        #expect(finalReach == Self.fastReachSec)
    }

    // MARK: - Rail 1: reach

    /// The reach failure, end to end. Pre-fix the drain atomizes 2 chunks and
    /// screens one tail window; the 360 s of transcribed audio in front of it is
    /// discarded and no later pass can reach it, because the job the drain mints
    /// is `complete` under an id nothing else derives.
    @Test("the drain screens the whole transcript, not just the candidate-local final tail")
    func drainScreensTheWholeTranscript() async throws {
        let chunks = mixedPassChunks()
        let store = try await seededStore(chunks: chunks)

        let didRun = await service(store: store)
            .retryShadowFMPhaseForSession(sessionId: Self.sessionId)
        #expect(didRun, "the drain must execute")

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: Self.assetId)
        #expect(!scans.isEmpty, "the drain must have written semantic scan rows")

        let earliest = try #require(scans.map(\.windowStartTime).min())
        #expect(earliest < Self.fastReachSec,
                """
                every screened window started at or after \(Self.fastReachSec)s — \
                the fast prefix was discarded (earliest=\(earliest))
                """)

        // The union of screened spans, against the declared duration. This is
        // the same shape as the field's 36-of-2,528 s = 0.0142.
        let unionSec = TranscriptChunkCanonicalizer
            .mergeIntervals(scans.map { ($0.windowStartTime, $0.windowEndTime) })
            .reduce(0) { $0 + ($1.1 - $1.0) }
        #expect(unionSec > Self.episodeDurationSec * 0.5,
                "screened \(unionSec)s of a \(Self.episodeDurationSec)s episode")
    }

    // MARK: - Rail 2: one transcriptVersion id space

    /// The persisted rows must carry the version every OTHER path derives.
    /// A second id space is not a cosmetic problem: `BackfillJobRunner`'s job id
    /// embeds `transcriptVersion`, so rows minted under the drain's private
    /// version can never dedupe against — or be resumed by — `runBackfill`.
    @Test("scan rows are stamped with the canonical transcriptVersion, not a drain-private one")
    func scanRowsCarryTheCanonicalTranscriptVersion() async throws {
        let chunks = mixedPassChunks()
        let store = try await seededStore(chunks: chunks)

        _ = await service(store: store).retryShadowFMPhaseForSession(sessionId: Self.sessionId)

        let persisted = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        let canonical = canonicalVersion(persisted)
        let finalOnly = finalOnlyVersion(persisted)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: Self.assetId)
        #expect(!scans.isEmpty)
        #expect(scans.allSatisfy { $0.transcriptVersion == canonical })
        #expect(scans.allSatisfy { $0.transcriptVersion != finalOnly })
    }

    /// The identity that decides whether a later pass can find this work at all.
    @Test("the drain mints the job id the runBackfill path would mint")
    func drainMintsTheSharedJobId() async throws {
        let chunks = mixedPassChunks()
        let store = try await seededStore(chunks: chunks)

        _ = await service(store: store).retryShadowFMPhaseForSession(sessionId: Self.sessionId)

        let persisted = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        let canonicalId = SemanticScanClaim.jobId(
            analysisAssetId: Self.assetId,
            transcriptVersion: canonicalVersion(persisted)
        )
        let finalOnlyId = SemanticScanClaim.jobId(
            analysisAssetId: Self.assetId,
            transcriptVersion: finalOnlyVersion(persisted)
        )
        #expect(canonicalId != finalOnlyId)

        #expect(try await store.fetchBackfillJob(byId: canonicalId) != nil,
                "the coverage-lane row must be the one `runBackfill` would also derive")
        #expect(try await store.fetchBackfillJob(byId: finalOnlyId) == nil,
                "no row may exist in the drain-private id space")
    }

    // MARK: - Rail 3: the fil5 claim rides the same identity

    /// `runShadowFMPhase`'s mode-off gate is reachable in a shipped build from
    /// exactly one place — this drain — and it records a durable claim naming
    /// the job its caller WOULD have minted. Pre-fix that was the final-only
    /// job, so the rescue row pointed at a job no other dispatcher derives: a
    /// durable claim for work nothing could pick up.
    @Test("a mode-off bail on the drain claims under the canonical job id")
    func modeOffClaimUsesTheCanonicalJobId() async throws {
        let chunks = mixedPassChunks()
        let store = try await seededStore(chunks: chunks)

        let didRun = await service(store: store, mode: .off)
            .retryShadowFMPhaseForSession(sessionId: Self.sessionId)
        #expect(didRun == false, "a mode-off phase does not execute")

        let persisted = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        let canonicalId = SemanticScanClaim.jobId(
            analysisAssetId: Self.assetId,
            transcriptVersion: canonicalVersion(persisted)
        )
        let finalOnlyId = SemanticScanClaim.jobId(
            analysisAssetId: Self.assetId,
            transcriptVersion: finalOnlyVersion(persisted)
        )

        let claim = try #require(try await store.fetchBackfillJob(byId: canonicalId),
                                 "the claim must name the canonical job")
        #expect(claim.deferReason == SemanticScanClaim.Gate.fmModeOff.deferReason)
        #expect(try await store.fetchBackfillJob(byId: finalOnlyId) == nil)
    }

    // MARK: - Rail 4: the no-regression pin

    /// Production persists only `pass='fast'` rows for most assets, and the
    /// single-pass path must be byte-identical to before the fix — canonicalize
    /// returns an all-fast array unchanged, so the drain's identity is exactly
    /// what the old fallback produced. Without this, "canonicalize everything"
    /// could have silently moved every existing all-fast asset's job id.
    @Test("an all-fast transcript replays byte-identically to the pre-fix fallback")
    func allFastReplayIsUnchanged() async throws {
        let assetId = "asset-iu0t-allfast"
        let sessionId = "sess-iu0t-allfast"
        let fastOnly = mixedPassChunks(assetId: assetId)
            .filter { $0.pass != TranscriptPassType.final_.rawValue }
        let store = try await seededStore(chunks: fastOnly, assetId: assetId, sessionId: sessionId)

        // The pre-fix expression, evaluated over the same input: with no final
        // chunks it falls through to the raw array.
        let preFixReplay = fastOnly.filter {
            $0.pass == TranscriptPassType.final_.rawValue
        }.isEmpty ? fastOnly : fastOnly.filter { $0.pass == TranscriptPassType.final_.rawValue }
        #expect(TranscriptAtomizer.transcriptVersionHash(chunks: preFixReplay)
                == canonicalVersion(fastOnly),
                "an all-fast asset's replay identity must not move")

        let didRun = await service(store: store)
            .retryShadowFMPhaseForSession(sessionId: sessionId)
        #expect(didRun)

        let expectedId = SemanticScanClaim.jobId(
            analysisAssetId: assetId,
            transcriptVersion: TranscriptAtomizer.transcriptVersionHash(chunks: preFixReplay)
        )
        #expect(try await store.fetchBackfillJob(byId: expectedId) != nil,
                "the all-fast drain must still mint the id it always did")
    }
}
