// AnalysisWorkSchedulerCascadeOrderingTests.swift
// playhead-swws: end-to-end ordering test that the scheduler's
// `peekNextDispatchableSlice()` returns the cascade's first window
// (sponsor-chapter ahead of proximal) for an episode that has been
// seeded — proving that the cascade-aware dispatch path overrides
// the store's `priority DESC, createdAt ASC` (FIFO at equal priority)
// ordering.
//
// The test seeds a single episode with two cascade windows:
//   1. A sponsor-chapter window at [10min, 11min]
//   2. The proximal window at [0, 20min] (default unplayed depth)
//
// CandidateWindowSelector orders sponsor windows ahead of the
// proximal window — so cascade.currentWindows[0] is the sponsor.
// Without cascade-aware dispatch, the runner would consume the job
// from the store and process [0, desiredCoverageSec] depth-first;
// the proximal window range would dominate. With cascade-aware
// dispatch, the dispatched slice's `cascadeWindow` matches the
// sponsor window — proximal-first order is overridden by the
// cascade's higher-priority sponsor.
//
// A second test asserts the FIFO fallback: an episode that has NOT
// been seeded must still be dispatched (with `cascadeWindow == nil`)
// so the long tail of unseeded episodes does not regress to "no
// work picked up".

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

    @Test("Dispatched slice carries the cascade's first window (sponsor wins over proximal)")
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

        // Peek the next dispatchable slice. With the cascade
        // wired and seeded, the dispatched slice's cascadeWindow
        // must match the cascade's first window — the sponsor.
        let slice = await scheduler.peekNextDispatchableSlice()
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

        let slice = await scheduler.peekNextDispatchableSlice()
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

        let slice = await scheduler.peekNextDispatchableSlice()
        #expect(slice != nil)
        #expect(slice?.jobId == "job-swws-legacy")
        #expect(slice?.cascadeWindow == nil)
    }

    @Test("peekNextDispatchableSlice returns nil when there are no eligible jobs")
    func peekReturnsNilWhenQueueEmpty() async throws {
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        let slice = await scheduler.peekNextDispatchableSlice()
        #expect(slice == nil)
    }
}
