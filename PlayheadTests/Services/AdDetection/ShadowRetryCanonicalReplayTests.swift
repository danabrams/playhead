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
// array — a fixture in which the defect is unobservable.
//
// The fixture here separates FOUR candidate chunk sets, because there are four
// plausible things this line could have been written as and only one of them is
// right. 12 fast chunks over [0, 360] and 4 final chunks over [300, 420]:
//
//   canonical  14 chunks, [0, 420]  final text replaces the two fast chunks it
//                                   covers; the other ten survive        <- correct
//   final-only  4 chunks, [300, 420]  candidate-local — the shipped defect
//   fast-only  12 chunks, [0, 360]    ignores the higher-accuracy re-transcription
//   raw        16 chunks, [0, 420]    right reach, but the overlapped audio is
//                                     present TWICE and the version drifts from
//                                     `runBackfill`'s
//
// All four hash differently, so every rail below can fail. The overlap is not
// decoration: `FinalPassRetranscriptionRunner` re-transcribes AROUND detected
// candidate windows, so a real final set is both candidate-local (which is what
// makes final-only catastrophic) and partly overlapping (which is what makes raw
// wrong). `fixtureDistinguishesTheCandidateChunkSets` is the control that keeps
// all four apart.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-iu0t: the shadow-retry drain replays the canonical transcript")
struct ShadowRetryCanonicalReplayTests {

    private static let assetId = "asset-iu0t"
    private static let sessionId = "sess-iu0t"
    private static let podcastId = "pod-iu0t"

    /// Where the fast pass stops.
    private static let fastReachSec: Double = 360
    /// Where the candidate-local final pass starts — inside the fast coverage,
    /// so it REPLACES two fast chunks and extends past the rest.
    private static let finalRegionStartSec: Double = 300
    private static let episodeDurationSec: Double = 420

    // MARK: - Fixtures

    /// The 53FC53E3 shape in miniature: a long fast prefix and a short,
    /// candidate-local final region that overlaps its tail and runs past it.
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
            "Sign up today at squarespace dot com slash show and make your own website.",
            "And we are also supported by listeners like you who back the show directly.",
            "That is all for this week, we will see you again on Thursday morning."
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
                startTime: Self.finalRegionStartSec + Double(idx) * 30,
                endTime: Self.finalRegionStartSec + Double(idx + 1) * 30,
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

    private func fastOnlyVersion(_ chunks: [TranscriptChunk]) -> String {
        TranscriptAtomizer.transcriptVersionHash(
            chunks: chunks.filter { $0.pass != TranscriptPassType.final_.rawValue }
        )
    }

    private func rawVersion(_ chunks: [TranscriptChunk]) -> String {
        TranscriptAtomizer.transcriptVersionHash(chunks: chunks)
    }

    // MARK: - The control that makes every rail below mean something

    /// **The fixture control.** Every assertion in this suite distinguishes the
    /// canonical chunk set from one of the three wrong ones, which is only
    /// possible if the fixture keeps all four apart. A future edit that gave the
    /// final chunks the same spans as the fast ones would make canonicalization
    /// drop every fast chunk, collapse canonical into final-only, and turn this
    /// whole suite green against the unfixed code. That is exactly how the
    /// pre-iu0t mixed-pass test failed to see this bug for months.
    @Test("control: the fixture's four candidate chunk sets are all genuinely different")
    func fixtureDistinguishesTheCandidateChunkSets() throws {
        let chunks = mixedPassChunks()
        let canonicalization = TranscriptChunkCanonicalizer.canonicalize(chunks)
        let diagnostics = canonicalization.diagnostics

        #expect(diagnostics.isPassthrough == false,
                "a mixed-pass fixture must not take the single-pass passthrough")
        #expect(diagnostics.fastCount == 12)
        #expect(diagnostics.finalCount == 4)
        #expect(diagnostics.droppedFastCount == 2,
                "the final region must REPLACE two fast chunks — the overlap raw would double-count")
        #expect(diagnostics.retainedFastCount == 10,
                "and must leave the other ten standing — the coverage final-only discards")
        #expect(diagnostics.coverageRetained,
                "canonicalization must not lose a second of audio")
        #expect(canonicalization.chunks.count == 14)

        let canonical = canonicalVersion(chunks)
        let versions = [
            "final-only": finalOnlyVersion(chunks),
            "fast-only": fastOnlyVersion(chunks),
            "raw": rawVersion(chunks)
        ]
        for (name, version) in versions {
            #expect(canonical != version,
                    "the canonical set must hash differently from \(name) or its rail cannot fail")
        }
        #expect(Set(versions.values).count == 3, "and the three wrong sets differ from each other")

        // The reach the shipped defect discards, as a numerator over a named
        // denominator: the final region begins at 300 s of a 420 s episode, so
        // replaying it alone screens at most 120/420 = 0.286 of the audio. The
        // field row's equivalent was 36/2528 = 0.0142.
        #expect(chunks.filter { $0.pass == TranscriptPassType.final_.rawValue }
                    .map(\.startTime).min() == Self.finalRegionStartSec)
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
        #expect(earliest < Self.finalRegionStartSec,
                """
                every screened window started at or after \(Self.finalRegionStartSec)s — \
                the fast-only coverage in front of the final region was discarded \
                (earliest=\(earliest))
                """)

        // The union of screened spans over the declared duration — the same
        // ratio the field row reads 36/2528 = 0.0142 on, and the same one
        // `AnalysisCoverageSummary.adScanFraction` divides.
        //
        // EXACT, not a threshold. Measured: this drain screens 420.0 of 420.0 s,
        // because the canonical transcript is contiguous over the whole episode
        // and `fullEpisodeScan` plans across all of it. Replaying the final-only
        // set screens 120 s — the [300, 420] region — for 0.286. A `> 0.5`-style
        // bound would separate those two but would also quietly accept a future
        // change that lost a third of the episode, and the whole point of this
        // bead is that a plausible-looking coverage number is exactly what
        // nobody checked.
        let unionSec = TranscriptChunkCanonicalizer
            .mergeIntervals(scans.map { ($0.windowStartTime, $0.windowEndTime) })
            .reduce(0) { $0 + ($1.1 - $1.0) }
        #expect(unionSec == Self.episodeDurationSec,
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

    /// The OTHER direction, and the reason "just don't filter" is not the fix.
    /// Replaying the RAW persisted rows reaches the whole episode and would pass
    /// the reach rail above, but it feeds the FM phase both the fast and the
    /// final text for the audio the final pass re-transcribed — the duplicate
    /// evidence hc7e removed — and hashes to a version `runBackfill` never
    /// derives, which puts the drain back in its own id space by a different
    /// route.
    @Test("the replay is the canonical set, not the raw persisted rows")
    func replayIsNotTheRawPersistedRows() async throws {
        let chunks = mixedPassChunks()
        let store = try await seededStore(chunks: chunks)

        _ = await service(store: store).retryShadowFMPhaseForSession(sessionId: Self.sessionId)

        let persisted = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: Self.assetId)
        #expect(!scans.isEmpty)
        #expect(scans.allSatisfy { $0.transcriptVersion != rawVersion(persisted) })
        #expect(scans.allSatisfy { $0.transcriptVersion != fastOnlyVersion(persisted) })
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
