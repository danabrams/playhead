// CandidateWindowCascadeTests.swift
// playhead-c3pi: exercise the CandidateWindowCascade actor's seed/seek
// lifecycle. The pure-function behavior is covered by
// CandidateWindowSelectorTests; these tests focus on the latch state
// machine (seed → seek below threshold → seek above threshold).

import Foundation
import Testing
@testable import Playhead

@Suite("CandidateWindowCascade — seed and seek re-latch")
struct CandidateWindowCascadeTests {

    // MARK: - Helpers

    private func makeCascade(config: PreAnalysisConfig = PreAnalysisConfig()) -> CandidateWindowCascade {
        CandidateWindowCascade(config: config)
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
            qualityScore: 0.85
        )
    }

    // MARK: - Seed

    @Test("seed returns proximal window for unplayed episode")
    func testSeedUnplayed() async {
        let cascade = makeCascade()
        let windows = await cascade.seed(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: []
        )
        #expect(windows.count == 1)
        #expect(windows[0].kind == .proximal)
        #expect(windows[0].range == 0...(20 * 60))
    }

    @Test("seed with chapter evidence places sponsors before proximal")
    func testSeedWithSponsors() async {
        let cascade = makeCascade()
        let windows = await cascade.seed(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 30 * 60,
            chapterEvidence: [sponsor(start: 15 * 60, end: 16 * 60)]
        )
        #expect(windows.count == 2)
        #expect(windows[0].kind == .sponsorChapter)
        #expect(windows[1].kind == .proximal)
    }

    @Test("seed records the anchor for later seek comparisons")
    func testSeedRecordsAnchor() async {
        let cascade = makeCascade()
        _ = await cascade.seed(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: []
        )
        let anchor = await cascade.currentAnchor(for: "ep-1")
        #expect(anchor == .some(10 * 60))
    }

    // MARK: - Seek below threshold

    @Test("seek below threshold does not re-latch (returns nil, windows unchanged)")
    func testSeekBelowThresholdIsNoOp() async {
        let cascade = makeCascade()
        let seeded = await cascade.seed(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: []
        )
        let result = await cascade.noteSeek(
            episodeId: "ep-1",
            newPosition: 10 * 60 + 25,
            episodeDuration: 60 * 60,
            chapterEvidence: []
        )
        #expect(result == nil)

        let current = await cascade.currentWindows(for: "ep-1")
        #expect(current == seeded)

        // Anchor unchanged.
        let anchor = await cascade.currentAnchor(for: "ep-1")
        #expect(anchor == .some(10 * 60))
    }

    // MARK: - Seek above threshold

    @Test("seek above threshold re-latches and rebases windows on new position")
    func testSeekAboveThresholdRelatches() async {
        let cascade = makeCascade()
        _ = await cascade.seed(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: []
        )
        let result = await cascade.noteSeek(
            episodeId: "ep-1",
            newPosition: 40 * 60,
            episodeDuration: 60 * 60,
            chapterEvidence: []
        )

        #expect(result?.count == 1)
        #expect(result?[0].kind == .proximal)
        #expect(result?[0].range == TimeInterval(40 * 60)...TimeInterval(55 * 60))

        let anchor = await cascade.currentAnchor(for: "ep-1")
        #expect(anchor == .some(TimeInterval(40 * 60)))
    }

    @Test("seek above threshold carries sponsor chapters through to the new window list")
    func testRelatchPreservesSponsors() async {
        let cascade = makeCascade()
        let evidence = [sponsor(start: 5 * 60, end: 6 * 60)]
        _ = await cascade.seed(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: evidence
        )
        let result = await cascade.noteSeek(
            episodeId: "ep-1",
            newPosition: 40 * 60,
            episodeDuration: 60 * 60,
            chapterEvidence: evidence
        )
        #expect(result?.count == 2)
        #expect(result?[0].kind == .sponsorChapter)
        #expect(result?[1].kind == .proximal)
        #expect(result?[1].range.lowerBound == TimeInterval(40 * 60))
    }

    // MARK: - Forget

    @Test("forget clears both anchor and windows for the episode")
    func testForgetClears() async {
        let cascade = makeCascade()
        _ = await cascade.seed(
            episodeId: "ep-1",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: []
        )
        await cascade.forget(episodeId: "ep-1")
        let windows = await cascade.currentWindows(for: "ep-1")
        let anchor = await cascade.currentAnchor(for: "ep-1")
        #expect(windows == nil)
        #expect(anchor == nil)
    }

    // MARK: - noteSeek without prior seed

    @Test("noteSeek before seed establishes anchor on first call (nil previous -> relatch)")
    func testNoteSeekFromFreshState() async {
        let cascade = makeCascade()
        let result = await cascade.noteSeek(
            episodeId: "ep-1",
            newPosition: 12 * 60,
            episodeDuration: 60 * 60,
            chapterEvidence: []
        )
        // First seek with no prior anchor should always relatch.
        #expect(result != nil)
        let anchor = await cascade.currentAnchor(for: "ep-1")
        #expect(anchor == .some(12 * 60))
    }

    // MARK: - Multiple episodes

    @Test("cascade tracks episodes independently")
    func testMultipleEpisodesIndependent() async {
        let cascade = makeCascade()
        _ = await cascade.seed(
            episodeId: "ep-a",
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: []
        )
        _ = await cascade.seed(
            episodeId: "ep-b",
            episodeDuration: 30 * 60,
            playbackAnchor: nil,
            chapterEvidence: []
        )

        let a = await cascade.currentAnchor(for: "ep-a")
        let b = await cascade.currentAnchor(for: "ep-b")
        #expect(a == .some(10 * 60))
        #expect(b == .some(nil))

        // Seek on ep-a does not touch ep-b.
        _ = await cascade.noteSeek(
            episodeId: "ep-a",
            newPosition: 45 * 60,
            episodeDuration: 60 * 60,
            chapterEvidence: []
        )
        let aAfter = await cascade.currentAnchor(for: "ep-a")
        let bAfter = await cascade.currentAnchor(for: "ep-b")
        #expect(aAfter == .some(45 * 60))
        #expect(bAfter == .some(nil))
    }
}
