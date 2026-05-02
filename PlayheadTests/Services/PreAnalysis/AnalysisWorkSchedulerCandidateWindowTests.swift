// AnalysisWorkSchedulerCandidateWindowTests.swift
// playhead-c3pi: scheduler-side wiring of the CandidateWindowCascade.
// Tests that the scheduler's seedCandidateWindows / noteCommittedPlayhead
// entry points propagate to the injected cascade and are no-ops when no
// cascade is wired (preserves existing scheduler behavior for the many
// tests that don't supply one).

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — candidate-window cascade wiring")
struct AnalysisWorkSchedulerCandidateWindowTests {

    // MARK: - Construction helpers

    private func makeRunner(store: AnalysisStore) -> AnalysisJobRunner {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        return AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
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
            transportStatusProvider: StubTransportStatusProvider(),
            candidateWindowCascade: cascade,
            config: PreAnalysisConfig()
        )
    }

    // MARK: - With cascade injected

    @Test("seedCandidateWindows propagates to the cascade and returns the ordered windows")
    func testSeedPropagatesToCascade() async throws {
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        let windows = await scheduler.seedCandidateWindows(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: []
        )
        #expect(windows.count == 1)
        #expect(windows[0].kind == .proximal)

        // Cascade also shows the windows via direct query.
        let stored = await cascade.currentWindows(for: "ep-1")
        #expect(stored == windows)
    }

    @Test("noteCommittedPlayhead returns nil for sub-threshold seeks")
    func testNoteCommittedPlayheadBelowThreshold() async throws {
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        _ = await scheduler.seedCandidateWindows(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: []
        )

        let result = await scheduler.noteCommittedPlayhead(
            episodeId: "ep-1",
            newPosition: 10 * 60 + 25,
            episodeDuration: 60 * 60,
            chapterEvidence: []
        )
        #expect(result == nil)
    }

    @Test("noteCommittedPlayhead re-latches and returns rebased windows on >30s seek")
    func testNoteCommittedPlayheadAboveThreshold() async throws {
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        _ = await scheduler.seedCandidateWindows(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: []
        )

        let result = await scheduler.noteCommittedPlayhead(
            episodeId: "ep-1",
            newPosition: 40 * 60,
            episodeDuration: 60 * 60,
            chapterEvidence: []
        )
        #expect(result?.count == 1)
        #expect(result?[0].range.lowerBound == TimeInterval(40 * 60))
    }

    @Test("currentCandidateWindows surfaces the cascade's stored ordering")
    func testCurrentCandidateWindowsReturnsCascadeState() async throws {
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        let sponsor = ChapterEvidence(
            startTime: 5 * 60,
            endTime: 6 * 60,
            title: "Sponsor",
            source: .pc20,
            disposition: .adBreak,
            qualityScore: 0.9
        )
        _ = await scheduler.seedCandidateWindows(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: [sponsor]
        )

        let windows = await scheduler.currentCandidateWindows(for: "ep-1")
        #expect(windows.count == 2)
        #expect(windows[0].kind == .sponsorChapter)
        #expect(windows[1].kind == .proximal)
    }

    @Test("episodeDeleted forgets the cascade entry")
    func testEpisodeDeletedForgetsCascade() async throws {
        let store = try await makeTestStore()
        let cascade = CandidateWindowCascade(config: PreAnalysisConfig())
        let scheduler = makeScheduler(store: store, cascade: cascade)

        _ = await scheduler.seedCandidateWindows(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: []
        )
        await scheduler.episodeDeleted(episodeId: "ep-1")
        let windows = await scheduler.currentCandidateWindows(for: "ep-1")
        #expect(windows.isEmpty)
    }

    // MARK: - Without cascade injected (default behavior preserved)

    @Test("seedCandidateWindows returns empty when no cascade is wired")
    func testSeedNoCascadeNoOp() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store, cascade: nil)

        let windows = await scheduler.seedCandidateWindows(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: []
        )
        #expect(windows.isEmpty)
    }

    @Test("noteCommittedPlayhead returns nil when no cascade is wired")
    func testNoteCommittedPlayheadNoCascadeNoOp() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store, cascade: nil)

        let result = await scheduler.noteCommittedPlayhead(
            episodeId: "ep-1",
            newPosition: 12 * 60,
            episodeDuration: 60 * 60,
            chapterEvidence: []
        )
        #expect(result == nil)
    }
}
