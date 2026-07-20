// RediffRefetchProductionTests.swift
// playhead-xsdz.36: the production conformers behind the activation wiring —
// V28 store persistence (attempt state + bandwidth ledger), the store-backed
// enumerator, the store-backed recorder, the B-side staging provider, and the
// revalidating consumer (stage → revalidate → unstage on every exit).

import Foundation
import SQLite3
import Testing

@testable import Playhead

// MARK: - Shared fixtures

private func makeAsset(
    id: String,
    episodeId: String? = nil,
    sourceURL: String = "file:///tmp/nonexistent.mp3",
    duration: Double? = nil
) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId ?? "ep-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: sourceURL,
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil,
        episodeDurationSec: duration
    )
}

private func makeFingerprintRecord(
    assetId: String,
    capturedAt: Double,
    fingerprints: [UInt32] = [1, 2, 3, 4],
    version: UInt32 = ChromaFingerprinter.algorithmVersion
) -> EpisodeFingerprintRecord {
    EpisodeFingerprintRecord(
        analysisAssetId: assetId,
        algorithmVersion: version,
        secondsPerFingerprint: ChromaFingerprinter.secondsPerFingerprint,
        fingerprints: fingerprints,
        sourceAudioIdentity: "fp-\(assetId)",
        capturedAt: capturedAt
    )
}

private final class StubBSideDecoder: AudioFileDecoding, @unchecked Sendable {
    var pcm: [Float] = [0.25, -0.5, 0.75]
    var errorToThrow: Error?
    private(set) var calls: [URL] = []
    func decodeMono16kHz(fileURL: URL) async throws -> [Float] {
        calls.append(fileURL)
        if let errorToThrow { throw errorToThrow }
        return pcm
    }
}

/// Records `revalidateFromFeatures` calls; can inspect the staging provider
/// MID-revalidation to prove the B-side is visible exactly when the rediff
/// pass would run.
private final class SpyAdDetection: AdDetectionProviding, @unchecked Sendable {
    struct RevalidateCall: Equatable {
        let assetId: String
        let podcastId: String
        let episodeDuration: Double
    }
    var errorToThrow: Error?
    var onRevalidate: (@Sendable () async -> Void)?
    private(set) var revalidateCalls: [RevalidateCall] = []

    func runHotPath(chunks: [TranscriptChunk], analysisAssetId: String, episodeDuration: Double) async throws -> [AdWindow] { [] }
    func runBackfill(chunks: [TranscriptChunk], analysisAssetId: String, podcastId: String, episodeDuration: Double, sessionId: String?) async throws {}
    func revalidateFromFeatures(analysisAssetId: String, podcastId: String, episodeDuration: Double, sessionId: String?) async throws {
        revalidateCalls.append(RevalidateCall(assetId: analysisAssetId, podcastId: podcastId, episodeDuration: episodeDuration))
        await onRevalidate?()
        if let errorToThrow { throw errorToThrow }
    }
}

// MARK: - V28 store persistence

@Suite("rediff_refetch_state V28 store (playhead-xsdz.36)")
struct RediffRefetchStateV28MigrationTests {

    @Test("fresh DB migrate() lands both V28 tables at head")
    func freshDbHasV28Tables() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(AnalysisStore.currentSchemaVersion == 28)
        // Probe by using the API — both tables must be queryable.
        #expect(try await store.fetchRediffRefetchStates().isEmpty)
        #expect(try await store.fetchRediffBandwidthTotals() == RediffBandwidthTotals())
    }

    @Test("attempt-state roundtrip preserves every field incl. NULLs")
    func stateRoundtrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))

        // Fresh state: nil lastAttemptAt, nil failure class.
        let initial = RediffRefetchStateRow(
            analysisAssetId: "a1",
            attemptState: .initial,
            updatedAt: 111
        )
        try await store.upsertRediffRefetchState(initial)
        #expect(try await store.fetchRediffRefetchState(assetId: "a1") == initial)

        // Full state: failure fields populated; upsert replaces in place.
        let advanced = RediffRefetchStateRow(
            analysisAssetId: "a1",
            attemptState: RediffRefetchPolicy.AttemptState(
                unchangedAttempts: 2,
                lastAttemptAt: 1_234.5,
                resolved: false,
                lastFailureClass: .resourceGone,
                sameClassFailureStreak: 3
            ),
            updatedAt: 222
        )
        try await store.upsertRediffRefetchState(advanced)
        #expect(try await store.fetchRediffRefetchState(assetId: "a1") == advanced)
        #expect(try await store.fetchRediffRefetchStates() == [advanced])
    }

    @Test("an unknown persisted failure class decodes as nil class (conservative retry, never a phantom park)")
    func unknownFailureClassDecodesNil() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset(id: "a1"))
        try await store.upsertRediffRefetchState(RediffRefetchStateRow(
            analysisAssetId: "a1",
            attemptState: RediffRefetchPolicy.AttemptState(
                unchangedAttempts: 0, lastAttemptAt: 10, resolved: false,
                lastFailureClass: .decodeFailure, sameClassFailureStreak: 9
            ),
            updatedAt: 1
        ))
        // Rewrite the class to a string this build does not know.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        #expect(sqlite3_exec(db, "UPDATE rediff_refetch_state SET lastFailureClass = 'future_class'", nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        let row = try #require(try await store.fetchRediffRefetchState(assetId: "a1"))
        #expect(row.attemptState.lastFailureClass == nil)
        #expect(row.attemptState.sameClassFailureStreak == 9)
        #expect(!RediffRefetchPolicy.isParked(row.attemptState), "nil class can never read as parked")
    }

    @Test("FK cascade: deleting the asset removes its re-fetch state row")
    func cascadeDelete() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        try await store.upsertRediffRefetchState(RediffRefetchStateRow(
            analysisAssetId: "a1", attemptState: .initial, updatedAt: 1
        ))
        try await store.deleteAsset(id: "a1")
        #expect(try await store.fetchRediffRefetchState(assetId: "a1") == nil)
    }

    @Test("bandwidth ledger accumulates across writes and reads back as totals")
    func bandwidthAccumulates() async throws {
        let store = try await makeTestStore()
        try await store.accumulateRediffBandwidth(
            precheckBytes: 131_072, fullFetchBytes: 0,
            unchangedCount: 1, rotatedCount: 0, failedCount: 0, parkedCount: 0, at: 10
        )
        try await store.accumulateRediffBandwidth(
            precheckBytes: 131_072, fullFetchBytes: 54_000_000,
            unchangedCount: 0, rotatedCount: 1, failedCount: 0, parkedCount: 0, at: 20
        )
        try await store.accumulateRediffBandwidth(
            precheckBytes: 131_072, fullFetchBytes: 54_000_000,
            unchangedCount: 0, rotatedCount: 0, failedCount: 1, parkedCount: 1, at: 30
        )
        let totals = try await store.fetchRediffBandwidthTotals()
        #expect(totals.precheckBytesTotal == 3 * 131_072)
        #expect(totals.fullFetchBytesTotal == 2 * 54_000_000)
        #expect(totals.unchangedCount == 1)
        #expect(totals.rotatedCount == 1)
        #expect(totals.failedCount == 1)
        #expect(totals.parkedCount == 1)
        #expect(totals.lastUpdatedAt == 30)
        #expect(totals.totalBytes == Int64(3 * 131_072) + Int64(2 * 54_000_000))
    }

    @Test("byte counters accept a >Int32.max per-write value (R2: no 32-bit bind trap on an oversized enclosure)")
    func bandwidthBindsAreInt64() async throws {
        let store = try await makeTestStore()
        // A single CDN-controlled full fetch can exceed Int32.max; the bind
        // must be a true 64-bit bind, not the trapping Int32 helper.
        let oversized = Int(Int32.max) + 1_000_000
        try await store.accumulateRediffBandwidth(
            precheckBytes: oversized, fullFetchBytes: oversized,
            unchangedCount: 0, rotatedCount: 0, failedCount: 1, parkedCount: 0, at: 40
        )
        let totals = try await store.fetchRediffBandwidthTotals()
        #expect(totals.precheckBytesTotal == Int64(oversized))
        #expect(totals.fullFetchBytesTotal == Int64(oversized))
        #expect(totals.totalBytes == 2 * Int64(oversized))
    }

    @Test("candidate seeds = current-version fingerprint rows minus resolved; capturedAt is the age baseline")
    func candidateSeeds() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a-fresh", sourceURL: "file:///tmp/a-fresh.mp3"))
        try await store.insertAsset(makeAsset(id: "a-resolved"))
        try await store.insertAsset(makeAsset(id: "a-stale"))
        try await store.insertAsset(makeAsset(id: "a-nofp"))

        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(assetId: "a-fresh", capturedAt: 1_000))
        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(assetId: "a-resolved", capturedAt: 2_000))
        // Stale algorithm version → not a candidate.
        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(
            assetId: "a-stale", capturedAt: 3_000, version: ChromaFingerprinter.algorithmVersion &+ 1
        ))
        // Resolved state → excluded.
        try await store.upsertRediffRefetchState(RediffRefetchStateRow(
            analysisAssetId: "a-resolved",
            attemptState: RediffRefetchPolicy.AttemptState(unchangedAttempts: 0, lastAttemptAt: 5, resolved: true),
            updatedAt: 5
        ))

        let seeds = try await store.fetchRediffCandidateSeeds()
        #expect(seeds.count == 1)
        let seed = try #require(seeds.first)
        #expect(seed.analysisAssetId == "a-fresh")
        #expect(seed.episodeId == "ep-a-fresh")
        #expect(seed.sourceURL == "file:///tmp/a-fresh.mp3")
        #expect(seed.capturedAt == 1_000)
    }
}

// MARK: - Enumerator

@Suite("AnalysisStoreRediffRefetchEnumerator (playhead-xsdz.36)")
struct RediffRefetchEnumeratorTests {

    @Test("builds candidates from seeds + persisted state + resolved enclosure; skips missing files and unresolvable episodes")
    func buildsCandidates() async throws {
        let store = try await makeTestStore()
        let dir = try makeTempDir(prefix: "RediffEnum")
        defer { try? FileManager.default.removeItem(at: dir) }

        // Episode with a real on-disk played copy.
        let playedURL = dir.appendingPathComponent("played.mp3")
        try Data(repeating: 1, count: 128).write(to: playedURL)
        try await store.insertAsset(makeAsset(id: "a1", episodeId: "ep-1", sourceURL: playedURL.absoluteString))
        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(assetId: "a1", capturedAt: 42))
        try await store.upsertRediffRefetchState(RediffRefetchStateRow(
            analysisAssetId: "a1",
            attemptState: RediffRefetchPolicy.AttemptState(unchangedAttempts: 1, lastAttemptAt: 43, resolved: false),
            updatedAt: 43
        ))

        // Played copy missing on disk → skipped.
        try await store.insertAsset(makeAsset(id: "a-gone", episodeId: "ep-gone", sourceURL: dir.appendingPathComponent("missing.mp3").absoluteString))
        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(assetId: "a-gone", capturedAt: 42))

        // Enclosure unresolvable → skipped.
        let orphanURL = dir.appendingPathComponent("orphan.mp3")
        try Data(repeating: 2, count: 64).write(to: orphanURL)
        try await store.insertAsset(makeAsset(id: "a-orphan", episodeId: "ep-orphan", sourceURL: orphanURL.absoluteString))
        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(assetId: "a-orphan", capturedAt: 42))

        let box = RediffEnclosureResolverBox()
        box.resolver = { @Sendable episodeId in
            episodeId == "ep-1" ? URL(string: "https://cdn.example.com/ep1-current.mp3") : nil
        }
        let enumerator = AnalysisStoreRediffRefetchEnumerator(store: store, enclosureResolver: box)

        let candidates = await enumerator.candidates()
        #expect(candidates.count == 1)
        let candidate = try #require(candidates.first)
        #expect(candidate.assetId == "a1")
        #expect(candidate.enclosureURL == URL(string: "https://cdn.example.com/ep1-current.mp3"))
        #expect(candidate.downloadedAt == 42, "capture time is the age baseline")
        #expect(candidate.localAudioURL == playedURL)
        #expect(candidate.attemptState.unchangedAttempts == 1)
    }

    @Test("a 0-byte played copy fails the anchored-file default and is not a candidate (no doomed 54 MB fetch)")
    func emptyPlayedCopyIsNotACandidate() async throws {
        let store = try await makeTestStore()
        let dir = try makeTempDir(prefix: "RediffEnumEmpty")
        defer { try? FileManager.default.removeItem(at: dir) }

        // Present on disk, but empty — the byte differ's bf4a2383 anchor
        // (regular, non-symlink, NON-EMPTY) would reject it as an A-side, so
        // the enumerator must not admit it either.
        let emptyURL = dir.appendingPathComponent("empty.mp3")
        try Data().write(to: emptyURL)
        try await store.insertAsset(makeAsset(id: "a-empty", episodeId: "ep-empty", sourceURL: emptyURL.absoluteString))
        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(assetId: "a-empty", capturedAt: 42))

        let box = RediffEnclosureResolverBox()
        box.resolver = { @Sendable _ in URL(string: "https://cdn.example.com/current.mp3") }
        let enumerator = AnalysisStoreRediffRefetchEnumerator(store: store, enclosureResolver: box)
        #expect(await enumerator.candidates().isEmpty)
    }

    @Test("nil resolver (pre-attach window) yields zero candidates — a benign no-op sweep")
    func nilResolverYieldsNothing() async throws {
        let store = try await makeTestStore()
        let dir = try makeTempDir(prefix: "RediffEnumNil")
        defer { try? FileManager.default.removeItem(at: dir) }
        let playedURL = dir.appendingPathComponent("played.mp3")
        try Data(repeating: 1, count: 16).write(to: playedURL)
        try await store.insertAsset(makeAsset(id: "a1", sourceURL: playedURL.absoluteString))
        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(assetId: "a1", capturedAt: 1))

        let enumerator = AnalysisStoreRediffRefetchEnumerator(store: store, enclosureResolver: RediffEnclosureResolverBox())
        #expect(await enumerator.candidates().isEmpty)
    }
}

// MARK: - Recorder

@Suite("AnalysisStoreRediffRefetchRecorder (playhead-xsdz.36)")
struct RediffRefetchRecorderTests {

    @Test("unchanged / rotated / failed outcomes persist the advanced state and accumulate bandwidth; skips persist nothing")
    func persistsStateAndBandwidth() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        let recorder = AnalysisStoreRediffRefetchRecorder(store: store, config: .production, now: { 999 })

        await recorder.recordOutcome(.skippedIneligible(assetId: "a1", reason: .tooSoonSinceDownload(ageSeconds: 5)))
        #expect(try await store.fetchRediffRefetchState(assetId: "a1") == nil, "skips advance nothing")
        #expect(try await store.fetchRediffBandwidthTotals() == RediffBandwidthTotals())

        let unchangedState = RediffRefetchPolicy.advanceUnchanged(.initial, at: 100)
        await recorder.recordOutcome(.unchanged(
            assetId: "a1",
            cost: RediffRefetchPolicy.BandwidthCost(precheckBytes: 131_072, fullFetchBytes: 0),
            newState: unchangedState
        ))
        #expect(try await store.fetchRediffRefetchState(assetId: "a1")?.attemptState == unchangedState)

        let rotatedState = RediffRefetchPolicy.markResolved(unchangedState, at: 200)
        await recorder.recordOutcome(.rotated(
            assetId: "a1",
            cost: RediffRefetchPolicy.BandwidthCost(precheckBytes: 131_072, fullFetchBytes: 54_000_000),
            fingerprintCount: 0,
            newState: rotatedState
        ))
        #expect(try await store.fetchRediffRefetchState(assetId: "a1")?.attemptState == rotatedState)

        let totals = try await store.fetchRediffBandwidthTotals()
        #expect(totals.precheckBytesTotal == 2 * 131_072)
        #expect(totals.fullFetchBytesTotal == 54_000_000)
        #expect(totals.unchangedCount == 1)
        #expect(totals.rotatedCount == 1)
        #expect(totals.failedCount == 0)
    }

    @Test("a parking failure bumps the parked counter")
    func parkedCounter() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        let recorder = AnalysisStoreRediffRefetchRecorder(store: store, config: .production, now: { 999 })

        // A streak-5 same-class state is parked under the production config.
        let parkedState = RediffRefetchPolicy.AttemptState(
            unchangedAttempts: 0, lastAttemptAt: 500, resolved: false,
            lastFailureClass: .resourceGone, sameClassFailureStreak: 5
        )
        await recorder.recordOutcome(.failed(
            assetId: "a1",
            cost: .zero,
            failureClass: .resourceGone,
            newState: parkedState,
            error: "404"
        ))
        #expect(try await store.fetchRediffRefetchState(assetId: "a1")?.attemptState == parkedState)
        let totals = try await store.fetchRediffBandwidthTotals()
        #expect(totals.failedCount == 1)
        #expect(totals.parkedCount == 1)
    }
}

// MARK: - Staging provider

@Suite("RediffBSideStagingProvider (playhead-xsdz.36)")
struct RediffBSideStagingProviderTests {

    @Test("serves the staged file URL for exactly its asset, and nothing once unstaged")
    func stageServeUnstage() async throws {
        let provider = RediffBSideStagingProvider(decoder: StubBSideDecoder(), durationProbe: { _ in nil })
        let url = URL(fileURLWithPath: "/tmp/rediff-b.mp3")

        #expect(await provider.refetchedBSideFileURL(assetId: "a1") == nil)
        await provider.stage(assetId: "a1", fileURL: url)
        #expect(await provider.refetchedBSideFileURL(assetId: "a1") == url)
        #expect(await provider.refetchedBSideFileURL(assetId: "other") == nil)
        #expect(await provider.stagedCount == 1)
        await provider.unstage(assetId: "a1")
        #expect(await provider.refetchedBSideFileURL(assetId: "a1") == nil)
        #expect(await provider.stagedCount == 0)
    }

    @Test("PCM fallback decodes the staged file through the injected decoder; unstaged or failing decode → nil")
    func pcmFallback() async throws {
        let decoder = StubBSideDecoder()
        decoder.pcm = [0.5, 0.25]
        let provider = RediffBSideStagingProvider(decoder: decoder, durationProbe: { _ in 60 })
        let url = URL(fileURLWithPath: "/tmp/rediff-b.mp3")

        #expect(await provider.refetchedBSideMono16kHz(assetId: "a1") == nil, "not staged → nil, decoder untouched")
        #expect(decoder.calls.isEmpty)

        await provider.stage(assetId: "a1", fileURL: url)
        #expect(await provider.refetchedBSideMono16kHz(assetId: "a1") == [0.5, 0.25])
        #expect(decoder.calls == [url])

        decoder.errorToThrow = NSError(domain: "decode", code: 1)
        #expect(await provider.refetchedBSideMono16kHz(assetId: "a1") == nil, "decode failure degrades to nil (status quo)")
    }

    @Test("the duration cap gates the PCM decode (cost bound), not the byte-path URL")
    func durationCap() async throws {
        let decoder = StubBSideDecoder()
        let provider = RediffBSideStagingProvider(
            decoder: decoder,
            maxDecodeDurationSeconds: 100,
            durationProbe: { _ in 101 }
        )
        let url = URL(fileURLWithPath: "/tmp/rediff-b.mp3")
        await provider.stage(assetId: "a1", fileURL: url)
        #expect(await provider.refetchedBSideMono16kHz(assetId: "a1") == nil)
        #expect(decoder.calls.isEmpty, "over-cap file must not be decoded")
        #expect(await provider.refetchedBSideFileURL(assetId: "a1") == url, "byte path unaffected by the cap")
    }
}

// MARK: - Consumer

@Suite("RevalidatingRediffBSideConsumer (playhead-xsdz.36)")
struct RevalidatingRediffBSideConsumerTests {

    private func makeStaging() -> RediffBSideStagingProvider {
        RediffBSideStagingProvider(decoder: StubBSideDecoder(), durationProbe: { _ in nil })
    }

    @Test("happy path: stages during revalidation, passes store-derived podcastId/duration, unstages after")
    func happyPath() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1", episodeId: "ep-1", duration: 280))
        let staging = makeStaging()
        let spy = SpyAdDetection()
        // Prove the B-side is visible to the rediff pass DURING revalidation.
        let url = URL(fileURLWithPath: "/tmp/rediff-b.mp3")
        let stagingForClosure = staging
        spy.onRevalidate = { @Sendable in
            let visible = await stagingForClosure.refetchedBSideFileURL(assetId: "a1")
            #expect(visible == url, "B-side must be staged while the pass runs")
        }
        let consumer = RevalidatingRediffBSideConsumer(staging: staging, store: store, adDetection: spy)

        try await consumer.consumeRotatedBSide(assetId: "a1", fileURL: url)

        #expect(spy.revalidateCalls == [SpyAdDetection.RevalidateCall(assetId: "a1", podcastId: "", episodeDuration: 280)])
        #expect(await staging.stagedCount == 0, "unstaged after consumption — no stale mapping")
    }

    @Test("episode duration falls back to the A-side stream extent when the column is NULL")
    func durationFallbackFromFingerprints() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1", duration: nil))
        try await store.upsertEpisodeFingerprints(makeFingerprintRecord(
            assetId: "a1", capturedAt: 1, fingerprints: Array(repeating: 7, count: 800)
        ))
        let staging = makeStaging()
        let spy = SpyAdDetection()
        let consumer = RevalidatingRediffBSideConsumer(staging: staging, store: store, adDetection: spy)

        try await consumer.consumeRotatedBSide(assetId: "a1", fileURL: URL(fileURLWithPath: "/tmp/b.mp3"))

        let call = try #require(spy.revalidateCalls.first)
        let expected = 800 * ChromaFingerprinter.secondsPerFingerprint
        #expect(abs(call.episodeDuration - expected) < 0.001)
    }

    @Test("missing asset throws stale-asset class without staging")
    func missingAssetThrows() async throws {
        let store = try await makeTestStore()
        let staging = makeStaging()
        let spy = SpyAdDetection()
        let consumer = RevalidatingRediffBSideConsumer(staging: staging, store: store, adDetection: spy)

        await #expect(throws: RediffBSideConsumeError.assetMissing(assetId: "ghost")) {
            try await consumer.consumeRotatedBSide(assetId: "ghost", fileURL: URL(fileURLWithPath: "/tmp/b.mp3"))
        }
        #expect(spy.revalidateCalls.isEmpty)
        #expect(await staging.stagedCount == 0)
    }

    @Test("a revalidation throw still unstages, then rethrows (failure → retry, no stale mapping)")
    func revalidationThrowUnstages() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1", duration: 100))
        let staging = makeStaging()
        let spy = SpyAdDetection()
        spy.errorToThrow = NSError(domain: "revalidate", code: 3)
        let consumer = RevalidatingRediffBSideConsumer(staging: staging, store: store, adDetection: spy)

        await #expect(throws: (any Error).self) {
            try await consumer.consumeRotatedBSide(assetId: "a1", fileURL: URL(fileURLWithPath: "/tmp/b.mp3"))
        }
        #expect(await staging.stagedCount == 0, "unstage on the throw path too")
    }
}
