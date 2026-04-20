// CandidateWindowSelectorTests.swift
// playhead-c3pi: pure-function tests for the candidate-window selector.
//
// The selector is a deterministic, side-effect-free function:
//
//     (episodeDuration, playbackAnchor, chapterEvidence, config)
//         -> [CandidateWindow]
//
// Returned windows MUST be in the cascade-execution order required by the
// bead spec:
//   1. Sponsor/ad chapter windows (in episode-time order)
//   2. The single playhead-proximal window
//      - Unplayed (anchor == nil) → first 20 minutes from 0.
//      - Resumed                 → next 15 minutes from anchor, clamped to
//        episode end.

import Foundation
import Testing
@testable import Playhead

@Suite("CandidateWindowSelector — pure window-selection function")
struct CandidateWindowSelectorTests {

    // MARK: - Helpers

    /// Default config for tests; centralises the spec's named constants so
    /// a magic-number drift breaks here, not in window assertions.
    private let config = PreAnalysisConfig()

    private func chapter(
        startTime: TimeInterval,
        endTime: TimeInterval?,
        title: String?,
        disposition: ChapterDisposition,
        source: ChapterSource = .pc20,
        quality: Float = 0.85
    ) -> ChapterEvidence {
        ChapterEvidence(
            startTime: startTime,
            endTime: endTime,
            title: title,
            source: source,
            disposition: disposition,
            qualityScore: quality
        )
    }

    // MARK: - Proximal window selection

    @Test("unplayed episode → single proximal window covering first 20 minutes")
    func testUnplayedFirst20Minutes() {
        let windows = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: [],
            config: config
        )

        #expect(windows.count == 1)
        #expect(windows[0].kind == .proximal)
        #expect(windows[0].range.lowerBound == 0)
        #expect(windows[0].range.upperBound == config.unplayedCandidateWindowSeconds)
        #expect(config.unplayedCandidateWindowSeconds == 20 * 60)
    }

    @Test("unplayed short episode → proximal window clamped to episode end")
    func testUnplayedShortEpisodeClamped() {
        let windows = CandidateWindowSelector.select(
            episodeDuration: 5 * 60,
            playbackAnchor: nil,
            chapterEvidence: [],
            config: config
        )

        #expect(windows.count == 1)
        #expect(windows[0].kind == .proximal)
        #expect(windows[0].range.lowerBound == 0)
        #expect(windows[0].range.upperBound == 5 * 60)
    }

    @Test("resumed at minute 30 → next 15 min window (30:00–45:00)")
    func testResumedNext15Minutes() {
        let windows = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 30 * 60,
            chapterEvidence: [],
            config: config
        )

        #expect(windows.count == 1)
        #expect(windows[0].kind == .proximal)
        #expect(windows[0].range.lowerBound == 30 * 60)
        #expect(windows[0].range.upperBound == 45 * 60)
        #expect(config.resumedCandidateWindowSeconds == 15 * 60)
    }

    @Test("resumed near end → window clamped to episode end, no overflow")
    func testResumedClampedToEpisodeEnd() {
        // Episode 60 min long, user is at 57 min — only 3 min remain.
        let windows = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 57 * 60,
            chapterEvidence: [],
            config: config
        )

        #expect(windows.count == 1)
        #expect(windows[0].kind == .proximal)
        #expect(windows[0].range.lowerBound == 57 * 60)
        #expect(windows[0].range.upperBound == 60 * 60)
    }

    @Test("resumed at episode end → no proximal window")
    func testResumedAtOrPastEnd() {
        let atEnd = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 60 * 60,
            chapterEvidence: [],
            config: config
        )
        let pastEnd = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 99 * 60,
            chapterEvidence: [],
            config: config
        )

        #expect(atEnd.isEmpty)
        #expect(pastEnd.isEmpty)
    }

    @Test("nil duration falls back to unbounded proximal window from anchor")
    func testNilDurationFallback() {
        let unplayed = CandidateWindowSelector.select(
            episodeDuration: nil,
            playbackAnchor: nil,
            chapterEvidence: [],
            config: config
        )
        let resumed = CandidateWindowSelector.select(
            episodeDuration: nil,
            playbackAnchor: 30 * 60,
            chapterEvidence: [],
            config: config
        )

        #expect(unplayed.count == 1)
        #expect(unplayed[0].range == 0...(20 * 60))
        #expect(resumed.count == 1)
        #expect(resumed[0].range == (30 * 60)...(45 * 60))
    }

    // MARK: - Sponsor-chapter seeding

    @Test("single sponsor chapter is seeded ahead of the proximal window")
    func testSingleSponsorChapterSeededFirst() {
        let sponsor = chapter(
            startTime: 25 * 60,
            endTime: 27 * 60,
            title: "Sponsor: Acme",
            disposition: .adBreak
        )
        let windows = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: [sponsor],
            config: config
        )

        #expect(windows.count == 2)
        #expect(windows[0].kind == .sponsorChapter)
        #expect(windows[0].range == (25 * 60)...(27 * 60))
        #expect(windows[1].kind == .proximal)
        #expect(windows[1].range == 0...(20 * 60))
    }

    @Test("multiple sponsor chapters are seeded in episode-time order")
    func testMultipleSponsorChaptersOrderedByStartTime() {
        // Provide them out-of-order to confirm the selector sorts them.
        let later = chapter(
            startTime: 40 * 60,
            endTime: 42 * 60,
            title: "Mid-roll",
            disposition: .adBreak
        )
        let earlier = chapter(
            startTime: 10 * 60,
            endTime: 11 * 60,
            title: "Pre-roll",
            disposition: .adBreak
        )
        let middle = chapter(
            startTime: 22 * 60,
            endTime: 24 * 60,
            title: "Sponsor: Beta",
            disposition: .adBreak
        )

        let windows = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 30 * 60,
            chapterEvidence: [later, earlier, middle],
            config: config
        )

        // 3 sponsor windows + 1 proximal.
        #expect(windows.count == 4)
        #expect(windows[0].kind == .sponsorChapter)
        #expect(windows[0].range.lowerBound == 10 * 60)
        #expect(windows[1].kind == .sponsorChapter)
        #expect(windows[1].range.lowerBound == 22 * 60)
        #expect(windows[2].kind == .sponsorChapter)
        #expect(windows[2].range.lowerBound == 40 * 60)
        #expect(windows[3].kind == .proximal)
    }

    @Test("non-adBreak chapters are ignored")
    func testNonAdBreakChaptersIgnored() {
        let content = chapter(
            startTime: 5 * 60,
            endTime: 10 * 60,
            title: "Interview begins",
            disposition: .content
        )
        let ambiguous = chapter(
            startTime: 50 * 60,
            endTime: 51 * 60,
            title: nil,
            disposition: .ambiguous
        )
        let windows = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: [content, ambiguous],
            config: config
        )

        #expect(windows.count == 1)
        #expect(windows[0].kind == .proximal)
    }

    @Test("sponsor chapter without endTime is skipped (no derivable range)")
    func testSponsorWithoutEndTimeSkipped() {
        // We deliberately do NOT invent a duration for an open-ended
        // sponsor chapter — downstream consumers expect a closed range.
        let openEnded = chapter(
            startTime: 30 * 60,
            endTime: nil,
            title: "Sponsor",
            disposition: .adBreak
        )
        let windows = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: [openEnded],
            config: config
        )

        #expect(windows.count == 1)
        #expect(windows[0].kind == .proximal)
    }

    @Test("sponsor chapter outside episode bounds is clamped or dropped")
    func testSponsorChapterClampedToEpisode() {
        let pastEnd = chapter(
            startTime: 70 * 60,
            endTime: 75 * 60,
            title: "Sponsor (past end)",
            disposition: .adBreak
        )
        let straddling = chapter(
            startTime: 58 * 60,
            endTime: 65 * 60,
            title: "Sponsor (overflows)",
            disposition: .adBreak
        )
        let windows = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: nil,
            chapterEvidence: [pastEnd, straddling],
            config: config
        )

        // pastEnd starts beyond the episode and is dropped. straddling
        // is clamped to the episode end.
        let sponsors = windows.filter { $0.kind == .sponsorChapter }
        #expect(sponsors.count == 1)
        #expect(sponsors[0].range == (58 * 60)...(60 * 60))
    }

    // MARK: - Determinism

    @Test("repeated calls produce identical orderings")
    func testDeterministicOrdering() {
        let a = chapter(startTime: 5 * 60, endTime: 6 * 60, title: "Sponsor A", disposition: .adBreak)
        let b = chapter(startTime: 5 * 60, endTime: 6 * 60, title: "Sponsor B", disposition: .adBreak)

        let first = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 12 * 60,
            chapterEvidence: [a, b],
            config: config
        )
        let second = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 12 * 60,
            chapterEvidence: [b, a],
            config: config
        )

        #expect(first == second)
    }

    // MARK: - Re-latch helper

    @Test("seek delta below threshold does not relatch")
    func testSeekBelowThresholdNoRelatch() {
        #expect(
            CandidateWindowSelector.shouldRelatch(
                previousAnchor: 30 * 60,
                newPosition: 30 * 60 + 25,
                threshold: config.seekRelatchThresholdSeconds
            ) == false
        )
    }

    @Test("seek delta at exactly threshold does not relatch (strictly greater)")
    func testSeekAtThresholdNoRelatch() {
        #expect(
            CandidateWindowSelector.shouldRelatch(
                previousAnchor: 30 * 60,
                newPosition: 30 * 60 + 30,
                threshold: config.seekRelatchThresholdSeconds
            ) == false
        )
    }

    @Test("seek delta above threshold relatches (forward and backward)")
    func testSeekAboveThresholdRelatches() {
        let forward = CandidateWindowSelector.shouldRelatch(
            previousAnchor: 30 * 60,
            newPosition: 30 * 60 + 31,
            threshold: config.seekRelatchThresholdSeconds
        )
        let backward = CandidateWindowSelector.shouldRelatch(
            previousAnchor: 30 * 60,
            newPosition: 30 * 60 - 60,
            threshold: config.seekRelatchThresholdSeconds
        )
        #expect(forward == true)
        #expect(backward == true)
    }

    @Test("nil previousAnchor always relatches when a position is supplied")
    func testNilPreviousAnchorRelatches() {
        #expect(
            CandidateWindowSelector.shouldRelatch(
                previousAnchor: nil,
                newPosition: 12 * 60,
                threshold: config.seekRelatchThresholdSeconds
            ) == true
        )
    }

    @Test("re-latched anchor rebases the proximal window from the new position")
    func testRelatchRebasesProximalWindow() {
        // First selection at minute 10.
        let initial = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 10 * 60,
            chapterEvidence: [],
            config: config
        )
        #expect(initial[0].range == (10 * 60)...(25 * 60))

        // User seeks to minute 40 — > 30s away → relatch.
        let relatched = CandidateWindowSelector.select(
            episodeDuration: 60 * 60,
            playbackAnchor: 40 * 60,
            chapterEvidence: [],
            config: config
        )
        #expect(relatched[0].range == (40 * 60)...(55 * 60))
    }
}
