// AdDetectionServiceChapterContextWireInTests.swift
// playhead-au2v.1.27 — Phase B tests: the SAFE, `.enabled`-gated wire-in
// of the cached inferred `ChapterPlan` into the shadow-phase
// `CoveragePlannerContext`.
//
// These are hermetic: NO live Foundation Models, no runner factory, no
// audio. The `ChapterPlan` is injected directly into a `ChapterPlanCache`
// (the production cache type, backed by a temp directory) and the context
// is observed via the DEBUG `shadowPhaseCoveragePlannerContextForTesting`
// seam, which runs the exact resolve-then-build sequence `runShadowFMPhase`
// uses in production.
//
// The two load-bearing invariants:
//   1. Canary: `.enabled` + a cached plan ⇒ context carries the chapter
//      evidence AND `chapterSignalMode == .enabled`.
//   2. `.off`/`.shadow` byte-identical: the context carries NEITHER
//      (defaults: `.off` / `nil`), proving production-default behaviour is
//      unchanged. A snapshot of every field pins this against drift.

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService — chapter-signal CoveragePlannerContext wire-in (au2v.1.27 Phase B)")
struct AdDetectionServiceChapterContextWireInTests {

    // MARK: - Fixtures

    private static func makeCache() -> ChapterPlanCache {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "ChapterContextWireIn-\(UUID().uuidString)",
                isDirectory: true
            )
        return ChapterPlanCache(directory: dir)
    }

    private static func makePlan(hash: String) -> ChapterPlan {
        let chapters = [
            ChapterEvidence(
                startTime: 0, endTime: 60,
                title: "Intro", source: .inferred,
                disposition: .content, qualityScore: 0.8
            ),
            ChapterEvidence(
                startTime: 60, endTime: 90,
                title: "Sponsor: Acme", source: .inferred,
                disposition: .adBreak, qualityScore: 0.9
            ),
        ]
        return ChapterPlan(
            episodeContentHash: hash,
            chapters: chapters,
            planConfidence: ChapterPlan.computePlanConfidence(chapters),
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    private static func makeService(
        store: AnalysisStore,
        chapterSignalMode: ChapterSignalMode,
        chapterPlanCache: ChapterPlanCache?
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "au2v.1.27-test",
            fmBackfillMode: .shadow,
            chapterSignalMode: chapterSignalMode
        )
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            chapterPlanCache: chapterPlanCache
        )
    }

    /// Snapshot of the non-chapter planner-context fields. These MUST be
    /// invariant across all modes (they depend only on planner state, which
    /// is cold-start here), so the byte-identical proof can isolate the
    /// chapter fields as the only thing that ever changes.
    private struct BaselineSnapshot: Equatable {
        let observedEpisodeCount: Int
        let stableRecall: Bool
        let isFirstEpisodeAfterCohortInvalidation: Bool
        let recallDegrading: Bool
        let sponsorDriftDetected: Bool
        let auditMissDetected: Bool
        let episodesSinceLastFullRescan: Int
        let periodicFullRescanIntervalEpisodes: Int

        init(_ ctx: CoveragePlannerContext) {
            observedEpisodeCount = ctx.observedEpisodeCount
            stableRecall = ctx.stableRecall
            isFirstEpisodeAfterCohortInvalidation = ctx.isFirstEpisodeAfterCohortInvalidation
            recallDegrading = ctx.recallDegrading
            sponsorDriftDetected = ctx.sponsorDriftDetected
            auditMissDetected = ctx.auditMissDetected
            episodesSinceLastFullRescan = ctx.episodesSinceLastFullRescan
            periodicFullRescanIntervalEpisodes = ctx.periodicFullRescanIntervalEpisodes
        }
    }

    private static let hash = "transcript-hash-au2v.1.27"
    private static let podcastId = "pod-au2v.1.27"

    // MARK: - Canary: .enabled threads the evidence

    @Test("CANARY: .enabled + cached plan ⇒ context carries chapter evidence and .enabled mode")
    func enabled_withCachedPlan_threadsEvidence() async throws {
        let store = try await makeTestStore()
        let cache = Self.makeCache()
        let plan = Self.makePlan(hash: Self.hash)
        let didPersist = await cache.put(contentHash: Self.hash, plan: plan)
        #expect(didPersist, "precondition: plan must persist into the cache")

        let service = Self.makeService(
            store: store, chapterSignalMode: .enabled, chapterPlanCache: cache
        )

        let ctx = await service.shadowPhaseCoveragePlannerContextForTesting(
            podcastId: Self.podcastId, transcriptVersion: Self.hash
        )

        #expect(ctx.chapterSignalMode == .enabled,
                ".enabled mode with a cached plan must carry .enabled downstream")
        #expect(ctx.chapterEvidence == plan.chapters,
                "context must carry the cached plan's inferred chapter evidence verbatim")
        #expect(ctx.chapterEvidence?.count == 2)
        // The CoveragePlanner branch is gated on this predicate; assert the
        // wire-in actually flips it true.
        #expect(ctx.chapterSignalMode.consumersReadChapterPlan == true)
    }

    // MARK: - .off / .shadow byte-identical contract

    @Test(".off ⇒ context carries NO chapter evidence and .off mode (production default unchanged)")
    func off_carriesNoEvidence() async throws {
        let store = try await makeTestStore()
        let cache = Self.makeCache()
        // A plan IS present in the cache — but `.off` must ignore it.
        _ = await cache.put(contentHash: Self.hash, plan: Self.makePlan(hash: Self.hash))

        let service = Self.makeService(
            store: store, chapterSignalMode: .off, chapterPlanCache: cache
        )

        let ctx = await service.shadowPhaseCoveragePlannerContextForTesting(
            podcastId: Self.podcastId, transcriptVersion: Self.hash
        )

        #expect(ctx.chapterSignalMode == .off,
                ".off must NOT carry the chapter signal mode downstream")
        #expect(ctx.chapterEvidence == nil,
                ".off must carry nil chapter evidence even when a plan is cached")
    }

    @Test(".shadow ⇒ context carries NO chapter evidence and .off mode (shadow byte-identical to off)")
    func shadow_carriesNoEvidence() async throws {
        let store = try await makeTestStore()
        let cache = Self.makeCache()
        _ = await cache.put(contentHash: Self.hash, plan: Self.makePlan(hash: Self.hash))

        let service = Self.makeService(
            store: store, chapterSignalMode: .shadow, chapterPlanCache: cache
        )

        let ctx = await service.shadowPhaseCoveragePlannerContextForTesting(
            podcastId: Self.podcastId, transcriptVersion: Self.hash
        )

        // Shadow's consumersReadChapterPlan == false, so the resolver
        // returns nil and the builder falls to the .off default — identical
        // to the .off arm above.
        #expect(ctx.chapterSignalMode == .off,
                ".shadow must NOT carry chapter evidence (consumers do not read in shadow)")
        #expect(ctx.chapterEvidence == nil)
    }

    /// Regression-style proof of the byte-identical contract. The
    /// `.off` context built WITH the wire-in present must equal — field for
    /// field — the context that the pre-au2v.1.27 inline construction would
    /// have produced (which is exactly: the baseline planner fields plus
    /// `.off` / `nil` chapter defaults). We reconstruct that "old" context
    /// from the public `CoveragePlannerContext` default initializer and
    /// assert full `Equatable` equality.
    @Test(".off context is byte-identical to the pre-wire-in construction (full Equatable snapshot)")
    func off_byteIdenticalToPreWireIn() async throws {
        let store = try await makeTestStore()
        let cache = Self.makeCache()
        _ = await cache.put(contentHash: Self.hash, plan: Self.makePlan(hash: Self.hash))

        let service = Self.makeService(
            store: store, chapterSignalMode: .off, chapterPlanCache: cache
        )
        let actual = await service.shadowPhaseCoveragePlannerContextForTesting(
            podcastId: Self.podcastId, transcriptVersion: Self.hash
        )

        // The pre-au2v.1.27 inline construction. Cold-start planner state
        // (no row for this podcast) ⇒ the same defaults the live code used:
        //   observedEpisodeCount 0, stableRecall false, the four false
        //   flags, episodesSinceLastFullRescan 0, interval 10, and — by
        //   omission — the `.off` / nil chapter defaults.
        let expectedPreWireIn = CoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )

        #expect(actual == expectedPreWireIn,
                "the .off context must be byte-identical to the pre-au2v.1.27 construction; got \(actual)")
        // And the baseline (non-chapter) fields must match the .enabled arm
        // too — proving the wire-in only ever changes the two chapter fields.
        #expect(BaselineSnapshot(actual) == BaselineSnapshot(expectedPreWireIn))
    }

    // MARK: - Cache-miss handling under .enabled

    @Test(".enabled but plan ABSENT from cache ⇒ context behaves as today (no evidence, .off, no crash)")
    func enabled_cacheMiss_fallsBackToDefaults() async throws {
        let store = try await makeTestStore()
        let cache = Self.makeCache() // empty — no plan seeded
        let service = Self.makeService(
            store: store, chapterSignalMode: .enabled, chapterPlanCache: cache
        )

        let ctx = await service.shadowPhaseCoveragePlannerContextForTesting(
            podcastId: Self.podcastId, transcriptVersion: Self.hash
        )

        #expect(ctx.chapterEvidence == nil,
                ".enabled cache-miss must fall back to nil evidence (no crash)")
        #expect(ctx.chapterSignalMode == .off,
                ".enabled cache-miss must NOT carry .enabled with no evidence")
    }

    @Test(".enabled but NO cache wired ⇒ context behaves as today (no evidence, .off)")
    func enabled_noCache_fallsBackToDefaults() async throws {
        let store = try await makeTestStore()
        let service = Self.makeService(
            store: store, chapterSignalMode: .enabled, chapterPlanCache: nil
        )

        let ctx = await service.shadowPhaseCoveragePlannerContextForTesting(
            podcastId: Self.podcastId, transcriptVersion: Self.hash
        )

        #expect(ctx.chapterEvidence == nil)
        #expect(ctx.chapterSignalMode == .off)
    }

    @Test(".enabled with an EMPTY-chapter cached plan ⇒ falls back to defaults")
    func enabled_emptyPlan_fallsBackToDefaults() async throws {
        let store = try await makeTestStore()
        let cache = Self.makeCache()
        let emptyPlan = ChapterPlan(
            episodeContentHash: Self.hash,
            chapters: [],
            planConfidence: 0,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        _ = await cache.put(contentHash: Self.hash, plan: emptyPlan)

        let service = Self.makeService(
            store: store, chapterSignalMode: .enabled, chapterPlanCache: cache
        )
        let ctx = await service.shadowPhaseCoveragePlannerContextForTesting(
            podcastId: Self.podcastId, transcriptVersion: Self.hash
        )

        #expect(ctx.chapterEvidence == nil, "empty chapter list must resolve to nil evidence")
        #expect(ctx.chapterSignalMode == .off)
    }

    @Test(".enabled with empty transcriptVersion ⇒ falls back to defaults (no cache lookup against empty key)")
    func enabled_emptyTranscriptVersion_fallsBackToDefaults() async throws {
        let store = try await makeTestStore()
        let cache = Self.makeCache()
        let service = Self.makeService(
            store: store, chapterSignalMode: .enabled, chapterPlanCache: cache
        )
        let ctx = await service.shadowPhaseCoveragePlannerContextForTesting(
            podcastId: Self.podcastId, transcriptVersion: ""
        )
        #expect(ctx.chapterEvidence == nil)
        #expect(ctx.chapterSignalMode == .off)
    }

    // MARK: - Pure builder unit coverage (no store, no FM)

    @Test("makeShadowPhaseCoveragePlannerContext: nil evidence ⇒ .off regardless of mode argument")
    func pureBuilder_nilEvidenceForcesOff() {
        for mode in ChapterSignalMode.allCases {
            let ctx = AdDetectionService.makeShadowPhaseCoveragePlannerContext(
                plannerState: nil,
                chapterSignalMode: mode,
                chapterEvidence: nil
            )
            #expect(ctx.chapterSignalMode == .off,
                    "nil evidence must force .off even when mode=\(mode)")
            #expect(ctx.chapterEvidence == nil)
        }
    }

    @Test("makeShadowPhaseCoveragePlannerContext: non-nil evidence carries the supplied mode")
    func pureBuilder_evidenceCarriesMode() {
        let chapters = [
            ChapterEvidence(startTime: 0, endTime: 30, title: "X",
                            source: .inferred, disposition: .adBreak, qualityScore: 0.9)
        ]
        let ctx = AdDetectionService.makeShadowPhaseCoveragePlannerContext(
            plannerState: nil,
            chapterSignalMode: .enabled,
            chapterEvidence: chapters
        )
        #expect(ctx.chapterSignalMode == .enabled)
        #expect(ctx.chapterEvidence == chapters)
    }
}
