// AnalysisWorkSchedulerCascadeOrderingTests.swift
// playhead-swws: end-to-end ordering tests that the scheduler's
// production selector (`selectNextDispatchableSlice()`, which is
// the same selector `runLoop()` consumes) returns the cascade's
// first window (sponsor-chapter ahead of proximal) for an episode
// that has been seeded — proving that the cascade-aware dispatch
// path overrides the store's `priority DESC, createdAt ASC` (FIFO
// at equal priority) ordering.
//
// The headline ordering test
// --------------------------
// Two episodes are enqueued in deliberately inverted FIFO ↔
// cascade order:
//
//   * Episode A is enqueued FIRST (older `createdAt`) at the same
//     `priority`. FIFO would pick A.
//   * Episode B is enqueued SECOND but seeded with a sponsor
//     chapter, which the cascade ranks above the proximal-only
//     anchor of A.
//
// The selector must pick B's job. This proves the cascade does
// genuinely override FIFO — which would be impossible to demonstrate
// with a one-job seed (a one-job test asserts only that the cascade
// does not break FIFO when there is nothing to override).
//
// Supporting tests retained
// -------------------------
//   * Single-job sponsor seed: confirms a sponsor window survives
//     end-to-end through the production selector even with no FIFO
//     competitor.
//   * Unseeded-episode fallback: an episode the cascade has never
//     seeded must still be dispatched with `cascadeWindow == nil`,
//     i.e. the existing depth-driven [0, desiredCoverageSec]
//     behavior is preserved for the long tail of episodes that
//     have not been seeded.
//   * No-cascade legacy path: when no cascade is wired at all, the
//     selector behaves as a thin wrapper around
//     `fetchNextEligibleJob`.
//   * Empty queue: returns nil (no dispatchable slice).

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-swws: AnalysisWorkScheduler dispatches in candidate-window order")
struct AnalysisWorkSchedulerCascadeOrderingTests {

    // MARK: - Construction helpers

    private func makeRunner(store: AnalysisStore) -> AnalysisJobRunner {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        return AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider(),
            cueMaterializer: SkipCueMaterializer(store: store)
        )
    }

    private func makeScheduler(
        store: AnalysisStore,
        cascade: CandidateWindowCascade?
    ) -> AnalysisWorkScheduler {
        AnalysisWorkScheduler(
            store: store,
            jobRunner: makeRunner(store: store),
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: StubDownloadProvider(),
            batteryProvider: {
                let b = StubBatteryProvider()
                b.level = 0.9
                b.charging = true
                return b
            }(),
            candidateWindowCascade: cascade,
            config: PreAnalysisConfig()
        )
    }

    private func sponsor(
        start: TimeInterval,
        end: TimeInterval,
        title: String = "Sponsor"
    ) -> ChapterEvidence {
        ChapterEvidence(
            startTime: start,
            endTime: end,
            title: title,
            source: .pc20,
            disposition: .adBreak,
            qualityScore: 0.9
        )
    }

    // MARK: - Cascade-aware dispatch

    @Test("Cascade override: sponsor-seeded episode beats FIFO-older proximal-only episode")
    func cascadeSponsorOverridesFifoAcrossTwoEpisodes() async throws {
        // The headline ordering test. Two episodes; FIFO order says
        // pick A; cascade order says pick B (sponsor > proximal).
        // The production selector must pick B.
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        // FIFO is `priority DESC, createdAt ASC` so equal-priority
        // jobs tie-break by `createdAt`. Stamp A's createdAt
        // strictly earlier so FIFO unambiguously prefers A.
        let now = Date().timeIntervalSince1970
        let aCreatedAt = now - 100   // older — FIFO would pick this
        let bCreatedAt = now - 1     // newer

        let episodeA = "ep-swws-A-fifo-older"
        let episodeB = "ep-swws-B-sponsor-seeded"

        // workKey is UNIQUE in the analysis_jobs schema and is
        // derived from sourceFingerprint + analysisVersion + jobType,
        // so each job needs a distinct fingerprint to actually
        // insert (an `INSERT OR IGNORE` collision would silently
        // drop the second row and the test would degenerate to a
        // one-job FIFO check).
        let jobA = makeAnalysisJob(
            jobId: "job-A-fifo",
            jobType: "preAnalysis",
            episodeId: episodeA,
            sourceFingerprint: "fp-A-fifo",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            createdAt: aCreatedAt,
            updatedAt: aCreatedAt
        )
        let jobB = makeAnalysisJob(
            jobId: "job-B-sponsor",
            jobType: "preAnalysis",
            episodeId: episodeB,
            sourceFingerprint: "fp-B-sponsor",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            createdAt: bCreatedAt,
            updatedAt: bCreatedAt
        )
        try await store.insertJob(jobA)
        try await store.insertJob(jobB)

        // Sanity: the underlying store would pick A under pure
        // FIFO. If this assertion ever flips, the test no longer
        // demonstrates "cascade overrides FIFO" — fail loudly.
        let storePick = try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
            t0ThresholdSec: 60,
            now: now
        )
        #expect(
            storePick?.jobId == "job-A-fifo",
            "FIFO precondition violated: store should have picked A first; got \(String(describing: storePick?.jobId))"
        )

        // Seed both episodes:
        //   * A only gets the proximal window (no chapter
        //     evidence) — cascade tier `.proximal` (rank 1).
        //   * B gets a sponsor chapter — cascade tier
        //     `.sponsorChapter` (rank 2).
        let sponsorStart: TimeInterval = 10 * 60
        let sponsorEnd: TimeInterval = 11 * 60
        let episodeDuration: TimeInterval = 60 * 60
        let expectedSponsorRange: ClosedRange<TimeInterval> = sponsorStart ... sponsorEnd

        _ = await scheduler.seedCandidateWindows(
            episodeId: episodeA,
            episodeDuration: episodeDuration,
            playbackAnchor: nil,
            chapterEvidence: []
        )
        let seededB = await scheduler.seedCandidateWindows(
            episodeId: episodeB,
            episodeDuration: episodeDuration,
            playbackAnchor: nil,
            chapterEvidence: [sponsor(start: sponsorStart, end: sponsorEnd)]
        )
        #expect(seededB.first?.kind == .sponsorChapter)
        #expect(seededB.first?.range == expectedSponsorRange)

        // Production selector — same surface `runLoop()` consumes.
        let slice = await scheduler.selectNextDispatchableSlice()
        #expect(slice != nil)
        #expect(
            slice?.jobId == "job-B-sponsor",
            "Cascade override failed: expected job B (sponsor-seeded) but selector returned \(String(describing: slice?.jobId)) — FIFO-only behavior was not overridden"
        )
        #expect(slice?.episodeId == episodeB)
        #expect(slice?.cascadeWindow?.kind == .sponsorChapter)
        #expect(slice?.cascadeWindow?.range == expectedSponsorRange)
    }

    @Test("Single-job sponsor seed: dispatched slice carries the cascade's first window")
    func dispatchedSliceMatchesCascadeFirstWindow() async throws {
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        // Insert a single queued ASR job for the seeded episode.
        let episodeId = "ep-swws-ordering"
        let job = makeAnalysisJob(
            jobId: "job-swws-1",
            jobType: "preAnalysis",
            episodeId: episodeId,
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        // Seed the cascade with a sponsor window at [10m, 11m] +
        // the unplayed proximal window. CandidateWindowSelector
        // orders sponsors first, so the cascade's first window is
        // the sponsor — this is what the dispatched slice must
        // carry, NOT the proximal window or a FIFO/depth-driven
        // fallback range. Constants are pre-resolved as
        // TimeInterval here to keep `#expect` macro expansion
        // cheap on the type-checker (otherwise the literal-laden
        // expressions trip the "unable to type-check" timeout).
        let sponsorStart: TimeInterval = 10 * 60
        let sponsorEnd: TimeInterval = 11 * 60
        let proximalStart: TimeInterval = 0
        let proximalEnd: TimeInterval = 20 * 60
        let episodeDuration: TimeInterval = 60 * 60
        let expectedSponsorRange: ClosedRange<TimeInterval> = sponsorStart ... sponsorEnd
        let expectedProximalRange: ClosedRange<TimeInterval> = proximalStart ... proximalEnd

        let sponsorEvidence = sponsor(start: sponsorStart, end: sponsorEnd)
        let seededWindows = await scheduler.seedCandidateWindows(
            episodeId: episodeId,
            episodeDuration: episodeDuration,
            playbackAnchor: nil,
            chapterEvidence: [sponsorEvidence]
        )
        #expect(seededWindows.count == 2)
        #expect(seededWindows[0].kind == .sponsorChapter)
        #expect(seededWindows[0].range == expectedSponsorRange)
        #expect(seededWindows[1].kind == .proximal)
        #expect(seededWindows[1].range == expectedProximalRange)

        // Production selector — single-job seed must surface the
        // sponsor window cleanly even with nothing to outrank.
        let slice = await scheduler.selectNextDispatchableSlice()
        #expect(slice != nil)
        #expect(slice?.jobId == "job-swws-1")
        #expect(slice?.episodeId == episodeId)
        let firstWindow = seededWindows[0]
        #expect(
            slice?.cascadeWindow == firstWindow,
            "Dispatched slice must carry the cascade's FIRST window (sponsor at [10m,11m]) — proximal-first FIFO was not overridden"
        )
        #expect(slice?.cascadeWindow?.kind == .sponsorChapter)
        #expect(slice?.cascadeWindow?.range == expectedSponsorRange)
    }

    @Test("Dispatched slice falls back to nil cascadeWindow when episode is not seeded")
    func dispatchFallsBackToFIFOWhenEpisodeNotSeeded() async throws {
        let store = try await makeTestStore()
        // Cascade present but the episode is never seeded — the
        // dispatched slice must still surface the FIFO-picked job
        // with cascadeWindow == nil (i.e. existing depth-driven
        // behavior is preserved for unseeded episodes).
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        let job = makeAnalysisJob(
            jobId: "job-swws-fallback",
            jobType: "preAnalysis",
            episodeId: "ep-swws-unseeded",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let slice = await scheduler.selectNextDispatchableSlice()
        #expect(slice != nil)
        #expect(slice?.jobId == "job-swws-fallback")
        #expect(
            slice?.cascadeWindow == nil,
            "Unseeded episode must yield cascadeWindow == nil so the runner falls back to its depth-driven [0, desiredCoverageSec] processing"
        )
    }

    @Test("Dispatched slice has no cascadeWindow when no cascade is wired (legacy path preserved)")
    func dispatchHasNoCascadeWhenNoCascadeWired() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store, cascade: nil)

        let job = makeAnalysisJob(
            jobId: "job-swws-legacy",
            jobType: "preAnalysis",
            episodeId: "ep-swws-legacy",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let slice = await scheduler.selectNextDispatchableSlice()
        #expect(slice != nil)
        #expect(slice?.jobId == "job-swws-legacy")
        #expect(slice?.cascadeWindow == nil)
    }

    @Test("selectNextDispatchableSlice returns nil when there are no eligible jobs")
    func selectReturnsNilWhenQueueEmpty() async throws {
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        let slice = await scheduler.selectNextDispatchableSlice()
        #expect(slice == nil)
    }
}
