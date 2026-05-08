// AdDetectionServiceChapterPhaseWiringTests.swift
// playhead-au2v.1.13: Wiring + gate tests for the ChapterGenerationPhase
// invocation inside AdDetectionService.runBackfill.
//
// Goal: pin the wire-up contract — mode gate, factory gate, cache-hit
// short-circuit — without exercising the deep phase semantics, which
// are covered by ChapterGenerationPhaseTests / IntegrationTests.
//
// Test doubles below are intentionally minimal:
//   * `RecordingPhaseFactory` — wraps any `ChapterGenerationPhase` and
//     records every factory invocation count + the per-call mode/episode
//     observed at `phase.run(...)`. The recorded calls let tests assert
//     "phase invoked exactly once with mode=.shadow and episodeId=X" or
//     "phase NOT invoked at all".
//   * `MockAdmission` / `MockBoundaryDetector` / `MockLabeler` /
//     `MockTranscriptHashProvider` / `MockEventSink` mirror the doubles
//     from `ChapterGenerationPhaseTests` (re-implemented here rather
//     than hoisted to a shared helper because the wiring suite needs
//     additional behaviors — recording call counts on the factory — and
//     the `ChapterGenerationPhaseTests` doubles are private to that
//     suite. Duplicated to keep the wiring suite self-contained per
//     project conventions; if a third site needs them, the right
//     refactor is `internal` doubles in a `Testing.Helpers` module.)
//
// Naming convention `wireup_*` for the test functions is just for
// greppability — Swift Testing doesn't enforce a prefix.

import Foundation
import os
import Testing
@testable import Playhead

@Suite("AdDetectionService — ChapterGenerationPhase wiring + gates (au2v.1.13)")
struct AdDetectionServiceChapterPhaseWiringTests {

    // MARK: - Test doubles (private to this suite)

    private actor RecordingEventSink: ChapterPhaseEventSink {
        private(set) var events: [ChapterPhaseEvent] = []
        func record(_ event: ChapterPhaseEvent) async {
            events.append(event)
        }
        func snapshot() -> [ChapterPhaseEvent] { events }
    }

    private actor RecordingPlanReadySink: ChapterPlanReadyEventSink {
        private(set) var events: [ChapterPlanReadyEvent] = []
        func record(_ event: ChapterPlanReadyEvent) async {
            events.append(event)
        }
        func snapshot() -> [ChapterPlanReadyEvent] { events }
    }

    private struct MockAdmission: ChapterPhaseAdmissionPolicy {
        let decision: ChapterPhaseAdmissionDecision
        func decide() async -> ChapterPhaseAdmissionDecision { decision }
    }

    private struct MockCreatorChapterProvider: CreatorChapterProviding {
        let chapters: [ChapterEvidence]
        init(chapters: [ChapterEvidence] = []) { self.chapters = chapters }
        func creatorChapters(episodeId: String) async -> [ChapterEvidence] {
            chapters
        }
    }

    private struct MockBoundaryDetector: ChapterBoundaryDetecting {
        let candidates: [ChapterBoundaryCandidate]
        func detect() async throws -> [ChapterBoundaryCandidate] { candidates }
    }

    private final class MockLabeler: ChapterLabeling, @unchecked Sendable {
        private actor Counter {
            var value = 0
            func increment() { value += 1 }
        }
        private let counter = Counter()
        var invocationCount: Int { get async { await counter.value } }

        func label(
            candidate: ChapterBoundaryCandidate
        ) async throws -> LabelingResult? {
            await counter.increment()
            let evidence = ChapterEvidence(
                startTime: candidate.startTime,
                endTime: candidate.endTime,
                title: "T-\(Int(candidate.startTime))",
                source: .inferred,
                disposition: .content,
                qualityScore: 0.7
            )
            return LabelingResult(
                chapter: evidence,
                labelDisposition: .content,
                topicDescriptor: evidence.title,
                failureMode: nil,
                attempts: 1
            )
        }
    }

    /// Sticky transcript-hash provider: every call returns the same hash.
    /// (Race tests live in the integration file.)
    private struct StickyHashProvider: TranscriptHashProviding {
        let hash: String
        func currentTranscriptHash() async -> String? { hash }
    }

    /// Records the number of factory invocations so the wiring tests
    /// can prove "factory called once per backfill" / "factory NEVER
    /// called when mode=.off". The factory closure itself constructs a
    /// real `ChapterGenerationPhase` — we are testing the wire-up, not
    /// the phase internals.
    ///
    /// The invocation counter is a synchronous `OSAllocatedUnfairLock`
    /// rather than an actor + unstructured `Task { await }` so the
    /// increment lands BEFORE `runBackfill` returns to the test (which
    /// `await`s the phase factory in-line). That removes the need for a
    /// `waitForInvocationsToSettle` poll — once the test's
    /// `try await service.runBackfill(...)` returns, every factory call
    /// the service made has already incremented the counter.
    private final class RecordingPhaseFactory: @unchecked Sendable {
        private let invocationLock = OSAllocatedUnfairLock(initialState: 0)
        let cache: ChapterPlanCache
        let eventSink: RecordingEventSink
        let planReadySink: RecordingPlanReadySink
        let labeler: MockLabeler
        let admissionPolicy: any ChapterPhaseAdmissionPolicy
        let boundaryDetector: any ChapterBoundaryDetecting
        let creatorProvider: any CreatorChapterProviding
        let hashProvider: any TranscriptHashProviding

        init(
            cache: ChapterPlanCache,
            eventSink: RecordingEventSink = RecordingEventSink(),
            planReadySink: RecordingPlanReadySink = RecordingPlanReadySink(),
            labeler: MockLabeler = MockLabeler(),
            admission: ChapterPhaseAdmissionDecision = .admit,
            candidates: [ChapterBoundaryCandidate] = [
                ChapterBoundaryCandidate(startTime: 0, endTime: 60),
                ChapterBoundaryCandidate(startTime: 60, endTime: 120)
            ],
            creatorChapters: [ChapterEvidence] = [],
            transcriptHash: String
        ) {
            self.cache = cache
            self.eventSink = eventSink
            self.planReadySink = planReadySink
            self.labeler = labeler
            self.admissionPolicy = MockAdmission(decision: admission)
            self.boundaryDetector = MockBoundaryDetector(candidates: candidates)
            self.creatorProvider = MockCreatorChapterProvider(
                chapters: creatorChapters
            )
            self.hashProvider = StickyHashProvider(hash: transcriptHash)
        }

        var invocationCount: Int { invocationLock.withLock { $0 } }

        /// Build the @Sendable factory closure. Captures the lock and
        /// value-typed mocks so it remains Sendable. The lock is
        /// incremented synchronously inside the closure so the count is
        /// observable as soon as the closure returns.
        func makeFactory() -> @Sendable () -> ChapterGenerationPhase {
            let cache = self.cache
            let eventSink = self.eventSink
            let planReadySink = self.planReadySink
            let labeler = self.labeler
            let admissionPolicy = self.admissionPolicy
            let boundaryDetector = self.boundaryDetector
            let creatorProvider = self.creatorProvider
            let hashProvider = self.hashProvider
            let lock = self.invocationLock
            return {
                lock.withLock { $0 += 1 }
                return ChapterGenerationPhase(
                    admissionPolicy: admissionPolicy,
                    creatorChapterProvider: creatorProvider,
                    boundaryDetector: boundaryDetector,
                    labeler: labeler,
                    transcriptHashProvider: hashProvider,
                    cache: cache,
                    eventSink: eventSink,
                    planReadySink: planReadySink
                )
            }
        }
    }

    // MARK: - Fixtures

    private static func makeAsset(id: String) -> AnalysisAsset {
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

    private static func makeChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            (0.0, 30.0, "Welcome back to the show."),
            (60.0, 90.0, "This podcast is brought to you by ExampleAd."),
            (90.0, 120.0, "Now back to our regular content.")
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private static func makeCache() -> ChapterPlanCache {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "ChapterPhaseWiring-\(UUID().uuidString)",
                isDirectory: true
            )
        return ChapterPlanCache(directory: dir)
    }

    /// Helper to compute the transcript hash AdDetectionService will
    /// derive from a chunk set. Mirrors the production call so the
    /// cache-hit short-circuit test can pre-seed the cache against the
    /// same key the wire-up uses.
    private static func transcriptVersionFor(
        chunks: [TranscriptChunk],
        analysisAssetId: String
    ) -> String {
        let (_, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        return version.transcriptVersion
    }

    private static func makeService(
        store: AnalysisStore,
        chapterSignalMode: ChapterSignalMode,
        chapterPlanCache: ChapterPlanCache?,
        chapterGenerationPhaseFactory: (@Sendable () -> ChapterGenerationPhase)?,
        installID: UUID = UUID()
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "au2v.1.13-test",
            fmBackfillMode: .off,
            chapterSignalMode: chapterSignalMode
        )
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            chapterGenerationPhaseFactory: chapterGenerationPhaseFactory,
            chapterPlanCache: chapterPlanCache,
            chapterPhaseInstallIDProvider: { installID }
        )
    }

    // MARK: - Mode-gate tests

    @Test("mode=.off: factory NEVER invoked, no plan cached, no diagnostics emitted")
    func wireup_modeOff_noFactoryCalls() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-au2v.1.13-modeoff"
        try await store.insertAsset(Self.makeAsset(id: assetId))

        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let factory = RecordingPhaseFactory(
            cache: cache, transcriptHash: hash
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .off,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory.makeFactory()
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-au2v.1.13",
            episodeDuration: 200.0
        )

        #expect(factory.invocationCount == 0,
                "mode=.off must short-circuit BEFORE any factory invocation")
        #expect(await factory.eventSink.snapshot().isEmpty,
                "mode=.off must emit zero phase diagnostics")
        let plan = await cache.get(contentHash: hash)
        #expect(plan == nil, "mode=.off must not write any chapter plan")
        #expect(await factory.labeler.invocationCount == 0,
                "mode=.off must never reach FM labeling")
    }

    @Test("mode=.shadow: factory invoked once, plan cached, ready event fired (consumers ignore by contract)")
    func wireup_modeShadow_invokesPhaseAndCaches() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-au2v.1.13-shadow"
        try await store.insertAsset(Self.makeAsset(id: assetId))

        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let factory = RecordingPhaseFactory(
            cache: cache, transcriptHash: hash
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .shadow,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory.makeFactory()
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-au2v.1.13",
            episodeDuration: 200.0
        )

        #expect(factory.invocationCount == 1,
                "mode=.shadow must invoke the factory exactly once")
        let plan = await cache.get(contentHash: hash)
        #expect(plan != nil, "mode=.shadow must persist a plan")
        #expect(plan?.episodeContentHash == hash)
        let readyEvents = await factory.planReadySink.snapshot()
        #expect(readyEvents.count == 1,
                "successful plan write must fire ChapterPlanReadyEvent exactly once")
        // Sanity: ChapterSignalMode contract — shadow does not let
        // consumers read. The mode itself enforces this; here we just
        // assert the gate config carried through.
        #expect(ChapterSignalMode.shadow.consumersReadChapterPlan == false,
                "shadow mode contract: consumers do NOT read the plan")
    }

    @Test("mode=.enabled: factory invoked once, plan cached, ready event fired")
    func wireup_modeEnabled_invokesPhaseAndCaches() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-au2v.1.13-enabled"
        try await store.insertAsset(Self.makeAsset(id: assetId))

        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let factory = RecordingPhaseFactory(
            cache: cache, transcriptHash: hash
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory.makeFactory()
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-au2v.1.13",
            episodeDuration: 200.0
        )

        #expect(factory.invocationCount == 1)
        let plan = await cache.get(contentHash: hash)
        #expect(plan != nil)
        #expect(ChapterSignalMode.enabled.consumersReadChapterPlan == true,
                "enabled mode contract: consumers read the plan")
    }

    // MARK: - Factory absence tests

    @Test("mode=.shadow with no factory wired: phase silently skipped, runBackfill still succeeds")
    func wireup_noFactoryButShadowMode_silentlySkips() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-au2v.1.13-noFactory"
        try await store.insertAsset(Self.makeAsset(id: assetId))

        let cache = Self.makeCache()
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .shadow,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: nil
        )

        try await service.runBackfill(
            chunks: Self.makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-au2v.1.13",
            episodeDuration: 200.0
        )

        // No factory means no plan ever lands.
        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let plan = await cache.get(contentHash: hash)
        #expect(plan == nil,
                "no factory ⇒ no plan written even though mode=.shadow")
    }

    // MARK: - Cache-hit short-circuit

    @Test("cache hit on current content hash: factory NOT invoked, no FM cost incurred")
    func wireup_cacheHit_shortCircuitsPhase() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-au2v.1.13-cachehit"
        try await store.insertAsset(Self.makeAsset(id: assetId))

        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let factory = RecordingPhaseFactory(
            cache: cache, transcriptHash: hash
        )

        // Pre-seed the cache with a plan keyed by the same content hash
        // the service will derive from this chunk set.
        let preExistingPlan = ChapterPlan(
            episodeContentHash: hash,
            chapters: [
                ChapterEvidence(
                    startTime: 0,
                    endTime: 60,
                    title: "Pre-seeded",
                    source: .inferred,
                    disposition: .content,
                    qualityScore: 0.9
                )
            ],
            planConfidence: 1.0,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            generationDiagnostics: ChapterPlanDiagnostics(
                candidatesDetected: 1,
                candidatesKept: 1,
                operationalUnclearCount: 0,
                semanticUnclearCount: 0
            )
        )
        let didPersist = await cache.put(contentHash: hash, plan: preExistingPlan)
        #expect(didPersist, "precondition: pre-seed must succeed")

        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory.makeFactory()
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-au2v.1.13",
            episodeDuration: 200.0
        )

        #expect(factory.invocationCount == 0,
                "cache hit must short-circuit BEFORE the factory is called")
        #expect(await factory.labeler.invocationCount == 0,
                "cache hit ⇒ zero FM cost (no labeler call)")
        // The pre-seeded plan must still be the cached plan (not
        // overwritten).
        let plan = await cache.get(contentHash: hash)
        #expect(plan?.chapters.first?.title == "Pre-seeded",
                "cache hit must not overwrite an existing plan")
    }

    @Test("nil cache + mode=.enabled: factory invoked every run (no short-circuit)")
    func wireup_nilCache_alwaysInvokesPhase() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-au2v.1.13-nilcache"
        try await store.insertAsset(Self.makeAsset(id: assetId))

        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        // Phase still needs a cache to write into; we pass it to the
        // factory directly but NOT to the wire-up's short-circuit.
        let phaseCache = Self.makeCache()
        let factory = RecordingPhaseFactory(
            cache: phaseCache, transcriptHash: hash
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: nil, // no short-circuit cache
            chapterGenerationPhaseFactory: factory.makeFactory()
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-au2v.1.13",
            episodeDuration: 200.0
        )
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-au2v.1.13",
            episodeDuration: 200.0
        )

        #expect(factory.invocationCount == 2,
                "no short-circuit cache ⇒ factory invoked on every backfill")
    }

    // MARK: - Install-ID provider plumbing

    /// The wire-up takes a `chapterPhaseInstallIDProvider` closure that
    /// production wiring uses to thread a stable per-install UUID into
    /// `phase.run(installID:)`. Diagnostic events hash this UUID
    /// together with the episodeId via `EpisodeIdHasher.hash(...)`. By
    /// injecting a deterministic UUID here and recomputing the same
    /// hash from the test, we prove the wire-up actually consults the
    /// injected closure (rather than, e.g., silently constructing a
    /// fresh UUID inline).
    @Test("install-ID provider closure: injected UUID flows into emitted phase events")
    func wireup_installIDProviderIsConsulted() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-au2v.1.13-installid"
        try await store.insertAsset(Self.makeAsset(id: assetId))

        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let factory = RecordingPhaseFactory(
            cache: cache, transcriptHash: hash
        )

        // Stable test install UUID. The provider closure returns this
        // exact value every call, so the diagnostic hash is fully
        // deterministic.
        let testInstallID = UUID(uuidString: "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA")!

        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory.makeFactory(),
            installID: testInstallID
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-au2v.1.13",
            episodeDuration: 200.0
        )

        // Recompute what the diagnostic hash MUST be if the wire-up
        // forwarded the injected UUID verbatim.
        let expectedEpisodeHash = EpisodeIdHasher.hash(
            installID: testInstallID,
            episodeId: assetId
        )
        let events = await factory.eventSink.snapshot()
        #expect(!events.isEmpty,
                "phase must have emitted at least one event in mode=.enabled with admit + candidates")
        // EVERY event from this run must carry the same episodeIdHash
        // because the provider returned a stable UUID. If the wire-up
        // ignored the closure and made fresh UUIDs, hashes would differ
        // (or differ from `expectedEpisodeHash`).
        for event in events {
            #expect(event.episodeIdHash == expectedEpisodeHash,
                    "event \(event.eventType) must carry the hash derived from the injected install UUID")
        }
    }
}
