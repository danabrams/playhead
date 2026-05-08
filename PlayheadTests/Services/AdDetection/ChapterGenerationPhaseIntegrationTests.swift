// ChapterGenerationPhaseIntegrationTests.swift
// playhead-au2v.1.13: End-to-end integration tests for the chapter
// generation phase running inside `AdDetectionService.runBackfill`.
//
// Scope vs. the wiring suite (AdDetectionServiceChapterPhaseWiringTests):
//   * Wiring tests pin mode-gate / factory-presence / cache-hit
//     short-circuit semantics with deterministic mocks.
//   * THIS suite exercises the full phase end-to-end against
//     `AdDetectionService.runBackfill` to cover:
//        - happy path (mode=.enabled): phase runs, plan persists,
//          ready event fires, no exception bubbles up.
//        - creator-chapter precedence (P2): when the creator-chapter
//          provider returns a creator-supplied chapter, the phase
//          short-circuits with `chapter_phase_skipped_creator_chapters`
//          and writes NO inferred plan.
//        - cancellation: a cancelled parent task tears the phase down
//          cleanly without crashing the surrounding backfill.
//        - transcript-revision race: an entry/recheck hash mismatch
//          aborts the run, no plan written.
//        - admission denial: phase emits skipped_admission, no plan
//          written.
//        - boundary detector failure: phase emits noCandidates, no
//          plan written.
//        - labeler operational-rate exceeded: phase aborts with
//          op-rate diagnostic, no plan written.
//
// Doubles in this file mirror those in
// AdDetectionServiceChapterPhaseWiringTests (deliberately duplicated to
// keep each suite self-contained — the project convention is "duplicate
// before three sites need it"; if a third needs them, hoist into a
// shared `Internal` helper).

import Foundation
import os
import Testing
@testable import Playhead

@Suite("ChapterGenerationPhase end-to-end via AdDetectionService.runBackfill (au2v.1.13)")
struct ChapterGenerationPhaseIntegrationTests {

    // MARK: - Test doubles

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
        let result: Result<[ChapterBoundaryCandidate], Error>
        init(candidates: [ChapterBoundaryCandidate]) {
            self.result = .success(candidates)
        }
        init(error: Error) {
            self.result = .failure(error)
        }
        func detect() async throws -> [ChapterBoundaryCandidate] {
            try result.get()
        }
    }

    /// Labeler: by default emits a confident inferred chapter per
    /// candidate. `throwingForAll: true` causes every label call to
    /// throw a non-cancellation error — folded into operational results
    /// by the phase, which trips the op-rate gate when the rate
    /// exceeds 30%.
    private final class StubLabeler: ChapterLabeling, @unchecked Sendable {
        let throwsAlways: Bool
        init(throwsAlways: Bool = false) { self.throwsAlways = throwsAlways }

        func label(
            candidate: ChapterBoundaryCandidate
        ) async throws -> LabelingResult? {
            if throwsAlways {
                struct OpFail: Error {}
                throw OpFail()
            }
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

    /// Hash provider with two scriptable behaviors:
    ///  * `.sticky(hash)` — every call returns `hash`. Default for happy
    ///    path tests; entry == recheck so no race fires.
    ///  * `.race(entry:recheck:)` — first call returns `entry`,
    ///    subsequent calls return `recheck`. Drives the
    ///    transcript-revision race exit.
    ///  * `.unavailable` — every call returns nil.
    private actor ScriptedHashProvider: TranscriptHashProviding {
        enum Behavior: Sendable {
            case sticky(String)
            case race(entry: String, recheck: String)
            case unavailable
        }

        private var behavior: Behavior
        private var calls: Int = 0

        init(_ behavior: Behavior) { self.behavior = behavior }

        func currentTranscriptHash() async -> String? {
            calls += 1
            switch behavior {
            case .sticky(let hash):
                return hash
            case .race(let entry, let recheck):
                return calls == 1 ? entry : recheck
            case .unavailable:
                return nil
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
                "ChapterPhaseIntegration-\(UUID().uuidString)",
                isDirectory: true
            )
        return ChapterPlanCache(directory: dir)
    }

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

    /// Build a phase factory closing over the supplied dependencies.
    /// Returns the factory plus the recorders so tests can assert
    /// emitted events / cached plans without re-deriving the dep graph.
    private static func makeFactory(
        cache: ChapterPlanCache,
        admission: ChapterPhaseAdmissionDecision = .admit,
        creatorChapters: [ChapterEvidence] = [],
        candidates: [ChapterBoundaryCandidate] = [
            ChapterBoundaryCandidate(startTime: 0, endTime: 60),
            ChapterBoundaryCandidate(startTime: 60, endTime: 120)
        ],
        boundaryError: Error? = nil,
        labeler: StubLabeler = StubLabeler(),
        hashBehavior: ScriptedHashProvider.Behavior
    ) -> (
        factory: @Sendable () -> ChapterGenerationPhase,
        eventSink: RecordingEventSink,
        planReadySink: RecordingPlanReadySink
    ) {
        let eventSink = RecordingEventSink()
        let planReadySink = RecordingPlanReadySink()
        let admissionPolicy = MockAdmission(decision: admission)
        let creatorProvider = MockCreatorChapterProvider(chapters: creatorChapters)
        let boundaryDetector: any ChapterBoundaryDetecting = {
            if let err = boundaryError {
                return MockBoundaryDetector(error: err)
            }
            return MockBoundaryDetector(candidates: candidates)
        }()
        let hashProvider = ScriptedHashProvider(hashBehavior)

        let factory: @Sendable () -> ChapterGenerationPhase = {
            ChapterGenerationPhase(
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
        return (factory, eventSink, planReadySink)
    }

    private static func makeService(
        store: AnalysisStore,
        chapterSignalMode: ChapterSignalMode,
        chapterPlanCache: ChapterPlanCache?,
        chapterGenerationPhaseFactory: (@Sendable () -> ChapterGenerationPhase)?
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "au2v.1.13-int",
            fmBackfillMode: .off,
            chapterSignalMode: chapterSignalMode
        )
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            chapterGenerationPhaseFactory: chapterGenerationPhaseFactory,
            chapterPlanCache: chapterPlanCache,
            chapterPhaseInstallIDProvider: { UUID() }
        )
    }

    // MARK: - Happy path

    @Test("mode=.enabled end-to-end: phase runs, plan persists, ready event fires, runBackfill returns")
    func integration_happyPath_enabled() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-int-happy"
        try await store.insertAsset(Self.makeAsset(id: assetId))
        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let (factory, eventSink, planReadySink) = Self.makeFactory(
            cache: cache, hashBehavior: .sticky(hash)
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-int",
            episodeDuration: 200.0
        )

        let plan = await cache.get(contentHash: hash)
        #expect(plan != nil, "happy path must persist a plan")
        #expect(plan?.chapters.count == 2)
        let ready = await planReadySink.snapshot()
        #expect(ready.count == 1, "ready event must fire exactly once")
        #expect(ready.first?.episodeContentHash == hash)
        let events = await eventSink.snapshot()
        // Expect exactly one .completed terminal event.
        let completed = events.filter { $0.eventType == .completed }
        #expect(completed.count == 1)
        let started = events.filter { $0.eventType == .started }
        #expect(started.count == 1, "exactly one .started event per run")
    }

    // MARK: - Creator-chapter precedence (P2)

    @Test("creator-chapter precedence: phase short-circuits, no inferred plan, no ready event")
    func integration_creatorChapterPrecedence() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-int-creator"
        try await store.insertAsset(Self.makeAsset(id: assetId))
        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let creatorChapters: [ChapterEvidence] = [
            ChapterEvidence(
                startTime: 0,
                endTime: 60,
                title: "Real Creator Chapter",
                source: .id3,
                disposition: .content,
                qualityScore: 0.9
            )
        ]
        let (factory, eventSink, planReadySink) = Self.makeFactory(
            cache: cache,
            creatorChapters: creatorChapters,
            hashBehavior: .sticky(hash)
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-int",
            episodeDuration: 200.0
        )

        let plan = await cache.get(contentHash: hash)
        #expect(plan == nil,
                "creator chapters present ⇒ phase writes NO inferred plan")
        let ready = await planReadySink.snapshot()
        #expect(ready.isEmpty,
                "creator-chapter precedence skip must NOT fire ready event")
        let events = await eventSink.snapshot()
        let skipEvents = events.filter { $0.eventType == .skippedCreatorChapters }
        #expect(skipEvents.count == 1,
                "exactly one chapter_phase_skipped_creator_chapters event")
        // Started should NOT fire on this exit path (phase never truly began).
        let started = events.filter { $0.eventType == .started }
        #expect(started.isEmpty,
                ".started must not fire on creator-chapter precedence skip")
    }

    // MARK: - Transcript-revision race

    @Test("transcript revision race: entry/recheck hash mismatch ⇒ raceAborted, no plan, no ready event")
    func integration_transcriptRevisionRace() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-int-race"
        try await store.insertAsset(Self.makeAsset(id: assetId))
        let chunks = Self.makeChunks(assetId: assetId)
        let cache = Self.makeCache()
        // The phase's TranscriptHashProviding is the in-phase race
        // detector. Returning different hashes between the entry
        // snapshot and the post-labeling recheck triggers the race
        // abort. The service-level cache short-circuit uses the atom-
        // derived `transcriptVersion` hash, which we deliberately do
        // NOT pre-seed, so the phase actually runs (which is what we
        // want — we're testing the in-phase race protection).
        let (factory, eventSink, planReadySink) = Self.makeFactory(
            cache: cache,
            hashBehavior: .race(entry: "race-entry-hash", recheck: "race-recheck-hash")
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-int",
            episodeDuration: 200.0
        )

        // Plan was discarded due to race. Cache stays empty for any
        // hash the phase observed.
        let entryPlan = await cache.get(contentHash: "race-entry-hash")
        let recheckPlan = await cache.get(contentHash: "race-recheck-hash")
        #expect(entryPlan == nil)
        #expect(recheckPlan == nil)
        let ready = await planReadySink.snapshot()
        #expect(ready.isEmpty, "race abort must NOT fire ready event")
        let events = await eventSink.snapshot()
        let preempted = events.filter { $0.eventType == .preempted }
        #expect(preempted.count == 1,
                "exactly one chapter_phase_preempted on race abort")
    }

    // MARK: - Admission denial

    @Test("admission deny: phase emits skipped_admission, no plan, no ready event")
    func integration_admissionDenied() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-int-admit-deny"
        try await store.insertAsset(Self.makeAsset(id: assetId))
        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let (factory, eventSink, planReadySink) = Self.makeFactory(
            cache: cache,
            admission: .deny(reason: "thermal_pressure"),
            hashBehavior: .sticky(hash)
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-int",
            episodeDuration: 200.0
        )

        let plan = await cache.get(contentHash: hash)
        #expect(plan == nil)
        let ready = await planReadySink.snapshot()
        #expect(ready.isEmpty)
        let events = await eventSink.snapshot()
        let skipped = events.filter { $0.eventType == .skippedAdmission }
        #expect(skipped.count == 1)
    }

    // MARK: - Boundary detector failure → noCandidates

    @Test("boundary detector throws non-cancellation: phase emits noCandidates, no plan")
    func integration_boundaryDetectorFailure() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-int-boundary-fail"
        try await store.insertAsset(Self.makeAsset(id: assetId))
        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        struct BoundaryError: Error {}
        let (factory, eventSink, planReadySink) = Self.makeFactory(
            cache: cache,
            boundaryError: BoundaryError(),
            hashBehavior: .sticky(hash)
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-int",
            episodeDuration: 200.0
        )

        let plan = await cache.get(contentHash: hash)
        #expect(plan == nil)
        let ready = await planReadySink.snapshot()
        #expect(ready.isEmpty)
        let events = await eventSink.snapshot()
        let noCandidates = events.filter { $0.eventType == .noCandidates }
        #expect(noCandidates.count == 1)
    }

    // MARK: - Operational rate exceeded

    @Test("labeler throws on every call: op-rate exceeded, no plan, no ready event")
    func integration_operationalRateExceeded() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-int-op-rate"
        try await store.insertAsset(Self.makeAsset(id: assetId))
        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
        let (factory, eventSink, planReadySink) = Self.makeFactory(
            cache: cache,
            labeler: StubLabeler(throwsAlways: true),
            hashBehavior: .sticky(hash)
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-int",
            episodeDuration: 200.0
        )

        let plan = await cache.get(contentHash: hash)
        #expect(plan == nil)
        let ready = await planReadySink.snapshot()
        #expect(ready.isEmpty,
                "op-rate exceeded must NOT fire ready event")
        let events = await eventSink.snapshot()
        let opRateExceeded = events.filter {
            $0.eventType == .operationalUnclearRateExceeded
        }
        #expect(opRateExceeded.count == 1)
    }

    // MARK: - Cancellation

    /// Labeler that yields cooperatively until the parent task is
    /// cancelled, then throws `CancellationError`. This pins a
    /// deterministic synchronization point INSIDE the phase: the phase
    /// is guaranteed to be mid-`labeler.label(...)` when the test
    /// cancels the parent task, so cancellation reliably hits the
    /// phase's `Task.isCancelled` rail.
    ///
    /// Implementation is a `Task.yield`-loop rather than a continuation
    /// so we never get into "store-then-resume" races against the
    /// cancellation handler — `Task.isCancelled` is sampled directly
    /// each iteration, which Swift Concurrency guarantees observes the
    /// cancellation flag deterministically after `Task.cancel()`.
    private final class BlockingLabeler: ChapterLabeling, @unchecked Sendable {
        private let entered = OSAllocatedUnfairLock(initialState: false)

        /// True once the phase has entered `label(...)` at least once.
        var hasEntered: Bool { entered.withLock { $0 } }

        func label(
            candidate: ChapterBoundaryCandidate
        ) async throws -> LabelingResult? {
            entered.withLock { $0 = true }
            // Spin yielding until the parent task is cancelled. Bounded
            // to ~5s of wall time as a safety net so a regression in
            // cancellation propagation surfaces as a test failure, not
            // a CI hang. With normal test execution the cancellation
            // arrives within a handful of yields.
            let deadline = Date().addingTimeInterval(5.0)
            while !Task.isCancelled {
                if Date() > deadline {
                    Issue.record("BlockingLabeler timed out waiting for cancellation")
                    return nil
                }
                await Task.yield()
            }
            throw CancellationError()
        }
    }

    @Test("parent task cancellation: runBackfill propagates cancellation; phase tears down cleanly")
    func integration_cancellationPropagates() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-int-cancel"
        try await store.insertAsset(Self.makeAsset(id: assetId))
        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()

        // Custom factory using BlockingLabeler so the phase suspends
        // mid-label until we cancel the parent task.
        let blockingLabeler = BlockingLabeler()
        let eventSink = RecordingEventSink()
        let planReadySink = RecordingPlanReadySink()
        let admissionPolicy = MockAdmission(decision: .admit)
        let creatorProvider = MockCreatorChapterProvider(chapters: [])
        let boundaryDetector = MockBoundaryDetector(candidates: [
            ChapterBoundaryCandidate(startTime: 0, endTime: 60),
            ChapterBoundaryCandidate(startTime: 60, endTime: 120)
        ])
        let hashProvider = ScriptedHashProvider(.sticky(hash))

        let factory: @Sendable () -> ChapterGenerationPhase = {
            ChapterGenerationPhase(
                admissionPolicy: admissionPolicy,
                creatorChapterProvider: creatorProvider,
                boundaryDetector: boundaryDetector,
                labeler: blockingLabeler,
                transcriptHashProvider: hashProvider,
                cache: cache,
                eventSink: eventSink,
                planReadySink: planReadySink
            )
        }

        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory
        )

        let task = Task { () -> Result<Void, Error> in
            do {
                try await service.runBackfill(
                    chunks: chunks,
                    analysisAssetId: assetId,
                    podcastId: "show-int",
                    episodeDuration: 200.0
                )
                return .success(())
            } catch {
                return .failure(error)
            }
        }

        // Synchronize: wait until the phase has actually entered the
        // labeler. After this point we know runBackfill has progressed
        // PAST the chapter-phase wire-up's pre-checks and is suspended
        // inside the phase. We poll a sync flag with `Task.yield()`
        // rather than a continuation to avoid store/resume races
        // against the cancellation handler. Bounded poll: a regression
        // that prevents the phase from reaching the labeler surfaces
        // here as a fail, not a hang.
        let entryDeadline = Date().addingTimeInterval(5.0)
        while !blockingLabeler.hasEntered {
            if Date() > entryDeadline {
                Issue.record("phase did not enter labeler within deadline")
                task.cancel()
                _ = await task.value
                return
            }
            await Task.yield()
        }

        // Now cancel — guaranteed to land while the labeler is
        // suspended, so the phase's cancellation rail is the path that
        // unwinds.
        task.cancel()
        let outcome = await task.value

        // The cancellation MUST surface as an error from runBackfill.
        // Without the post-phase `try Task.checkCancellation()`, the
        // call would have returned `.success` after the phase emitted
        // `.preempted` — so this assertion specifically pins that the
        // wire-up forwards cancellation upward rather than swallowing
        // it.
        switch outcome {
        case .success:
            Issue.record("runBackfill must propagate cancellation; got success")
        case .failure(let err):
            #expect(err is CancellationError,
                    "expected CancellationError, got \(err)")
        }

        // Plan must NOT have been written because the phase observed
        // cancellation before its cache write.
        let plan = await cache.get(contentHash: hash)
        #expect(plan == nil,
                "cancelled phase must not persist a plan")
        let ready = await planReadySink.snapshot()
        #expect(ready.isEmpty,
                "cancellation must NOT fire a ready event")
    }

    // MARK: - Cache hit short-circuit (integration angle)

    @Test("integration cache hit: pre-seeded plan ⇒ phase NOT invoked end-to-end")
    func integration_cacheHitShortCircuit() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-int-cachehit"
        try await store.insertAsset(Self.makeAsset(id: assetId))
        let chunks = Self.makeChunks(assetId: assetId)
        let hash = Self.transcriptVersionFor(
            chunks: chunks, analysisAssetId: assetId
        )
        let cache = Self.makeCache()
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
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let didPersist = await cache.put(contentHash: hash, plan: preExistingPlan)
        #expect(didPersist, "precondition: pre-seed must succeed")

        let (factory, eventSink, planReadySink) = Self.makeFactory(
            cache: cache, hashBehavior: .sticky(hash)
        )
        let service = Self.makeService(
            store: store,
            chapterSignalMode: .enabled,
            chapterPlanCache: cache,
            chapterGenerationPhaseFactory: factory
        )

        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "show-int",
            episodeDuration: 200.0
        )

        // Plan still the pre-seeded one (not overwritten).
        let plan = await cache.get(contentHash: hash)
        #expect(plan?.chapters.first?.title == "Pre-seeded")
        // No phase events emitted because the phase was never invoked.
        let events = await eventSink.snapshot()
        #expect(events.isEmpty,
                "cache hit ⇒ no phase events emitted (phase was never invoked)")
        let ready = await planReadySink.snapshot()
        #expect(ready.isEmpty,
                "cache hit ⇒ no ready event (phase was never invoked)")
    }
}
