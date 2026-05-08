// ChapterBoundaryDetectorTests.swift
// playhead-au2v.1.4: Unit tests for `ChapterBoundaryDetector`.
//
// Coverage (bead-4):
//   - synthetic t=0 boundary always present (including for empty input)
//   - music-onset and music-offset both detected
//   - speaker shift sustained >5s detected
//   - speaker shift <5s filtered out (crosstalk)
//   - lexical category jump detected
//   - long pause >2s detected
//   - combined-signals matrix
//   - monologue (sparse output)
//   - short episode (<5min)
//   - chronological ordering invariant
//   - per-episode <50ms perf bound on a 60-min show
//
// Coverage (bead-5 density gates):
//   - pathological-rate gate fires above 1/90s and aborts to empty
//   - boundary-rate exactly at 1/90s does NOT abort (strict >)
//   - cap-and-merge retains top-N by confidence
//   - sparse input (below cap) passes through unchanged
//   - floor of 8 enforced for episodes ≥40 min
//   - floor of 8 NOT enforced for episodes <40 min
//   - episode <5 min skips BOTH gates byte-for-byte
//   - merge picks the neighbor with higher signal-overlap
//   - synthetic t=0 boundary is always retained even after a tight cap

import Foundation
import Testing
@testable import Playhead

// MARK: - Test helpers

private enum BoundaryFixtures {

    /// Build a synthetic music-window track with `count` 2s windows
    /// starting at t=0, all carrying the same `probability`.
    static func musicTrack(
        count: Int,
        probability: Double,
        windowDuration: TimeInterval = 2.0
    ) -> [ChapterMusicWindow] {
        (0..<count).map { index in
            let start = TimeInterval(index) * windowDuration
            return ChapterMusicWindow(
                startTime: start,
                endTime: start + windowDuration,
                musicProbability: probability
            )
        }
    }

    /// Build a synthetic speaker track of `count` windows with a fixed
    /// cluster ID.
    static func speakerTrack(
        count: Int,
        clusterId: Int?,
        windowDuration: TimeInterval = 2.0,
        startOffset: TimeInterval = 0
    ) -> [ChapterSpeakerWindow] {
        (0..<count).map { index in
            let start = startOffset + TimeInterval(index) * windowDuration
            return ChapterSpeakerWindow(
                startTime: start,
                endTime: start + windowDuration,
                clusterId: clusterId
            )
        }
    }

    /// Build a synthetic pause-window track of `count` windows, all with
    /// the same pause probability.
    static func pauseTrack(
        count: Int,
        pauseProbability: Double,
        windowDuration: TimeInterval = 2.0,
        startOffset: TimeInterval = 0
    ) -> [ChapterPauseWindow] {
        (0..<count).map { index in
            let start = startOffset + TimeInterval(index) * windowDuration
            return ChapterPauseWindow(
                startTime: start,
                endTime: start + windowDuration,
                pauseProbability: pauseProbability
            )
        }
    }
}

// MARK: - t=0 invariant + empty input

@Suite("ChapterBoundaryDetector / t=0 invariant")
struct ChapterBoundaryDetectorT0Tests {

    @Test("empty snapshot produces only the synthetic t=0 boundary")
    func emptySnapshotProducesOnlyT0() {
        let detector = ChapterBoundaryDetector()
        let snapshot = ChapterFeatureSnapshot(episodeDuration: 0)
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1)
        #expect(result.first?.startTime == 0)
        #expect(result.first?.triggeringSignals.isEmpty == true)
    }

    @Test("snapshot with positive duration but no signals still emits t=0")
    func noSignalsButPositiveDurationEmitsT0() {
        let detector = ChapterBoundaryDetector()
        let snapshot = ChapterFeatureSnapshot(episodeDuration: 600)
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1)
        #expect(result.first?.startTime == 0)
    }

    @Test("t=0 boundary has confidence 1.0")
    func t0HasConfidenceOne() {
        let detector = ChapterBoundaryDetector()
        let snapshot = ChapterFeatureSnapshot(episodeDuration: 60)
        let result = detector.detect(features: snapshot)
        #expect(result.first?.boundaryConfidence == 1.0)
    }
}

// MARK: - Music transitions

@Suite("ChapterBoundaryDetector / music transitions")
struct ChapterBoundaryDetectorMusicTests {

    @Test("music onset (low → high) emits boundary")
    func musicOnsetEmitsBoundary() {
        let detector = ChapterBoundaryDetector()
        // Windows: 0-2s @ 0.05 (silence), 2-4s @ 0.95 (full music) →
        // onset at t=2.
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 4, endTime: 6, musicProbability: 0.95),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        let onset = result.last!
        #expect(onset.startTime == 2.0)
        #expect(onset.triggeringSignals == [.musicTransition])
        // 0.4 weight, normalized to [0,1] → 0.4
        #expect(abs(onset.boundaryConfidence - 0.4) < 0.0001)
    }

    @Test("music offset (high → low) emits boundary")
    func musicOffsetEmitsBoundary() {
        let detector = ChapterBoundaryDetector()
        // 0-2s @ 0.95, 2-4s @ 0.95, 4-6s @ 0.05 → offset at t=4.
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 4, endTime: 6, musicProbability: 0.05),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        let offset = result.last!
        #expect(offset.startTime == 4.0)
        #expect(offset.triggeringSignals == [.musicTransition])
    }

    @Test("delta exactly at threshold does not trigger (strict >)")
    func musicDeltaAtThresholdDoesNotTrigger() {
        let detector = ChapterBoundaryDetector()
        // Spec calls for delta > 0.5; 0.5 exactly should not trigger.
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: 0.0),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 0.5),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1)
    }

    @Test("small deltas below threshold do not trigger")
    func smallDeltasIgnored() {
        let detector = ChapterBoundaryDetector()
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: 0.10),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 0.30),
            ChapterMusicWindow(startTime: 4, endTime: 6, musicProbability: 0.50),
            ChapterMusicWindow(startTime: 6, endTime: 8, musicProbability: 0.70),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1, "no individual delta exceeds 0.5; only t=0 should emit")
    }

    @Test("out-of-range probabilities are clamped before delta computation")
    func clampingMakesOutOfRangeSafe() {
        let detector = ChapterBoundaryDetector()
        // Bogus inputs (-2 and +2) should clamp to (0, 1) — delta = 1.0,
        // which exceeds the 0.5 threshold.
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: -2.0),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 2.0),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 2.0)
    }
}

// MARK: - Speaker shifts

@Suite("ChapterBoundaryDetector / speaker shifts")
struct ChapterBoundaryDetectorSpeakerTests {

    @Test("shift sustained well past 5s emits boundary")
    func sustainedShiftEmits() {
        let detector = ChapterBoundaryDetector()
        // cluster 1 for first 10s, then cluster 2 for 10s → shift at t=10.
        let speakers =
            BoundaryFixtures.speakerTrack(count: 5, clusterId: 1) +
            BoundaryFixtures.speakerTrack(count: 5, clusterId: 2, startOffset: 10)
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 10.0)
        #expect(result.last?.triggeringSignals == [.speakerShift])
    }

    @Test("shift sustained exactly minSpeakerRunDuration is filtered (strict >)")
    func shiftAtExactBoundaryFiltered() {
        let detector = ChapterBoundaryDetector()
        // cluster 1 for 6s, then cluster 2 for EXACTLY 5s. Spec says
        // ">5s", so 5s exactly should be treated as crosstalk.
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 6, clusterId: 1),
            ChapterSpeakerWindow(startTime: 6, endTime: 11, clusterId: 2),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1, "exactly-5s shift is below the strict > threshold")
    }

    @Test("shift sustained just over minSpeakerRunDuration emits boundary")
    func shiftJustOverBoundaryEmits() {
        let detector = ChapterBoundaryDetector()
        // cluster 1 for 6s, then cluster 2 for 5.001s — strict-greater
        // boundary clears.
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 6, clusterId: 1),
            ChapterSpeakerWindow(startTime: 6, endTime: 11.001, clusterId: 2),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 6.0)
    }

    @Test("brief crosstalk (<5s shift) is filtered out and does not fabricate a 'shift back' event")
    func briefShiftFilteredOut() {
        let detector = ChapterBoundaryDetector()
        // Cluster 1 for 10s, cluster 2 for ONLY 2s (interruption),
        // then cluster 1 resumes for 18s. The 2s interruption is
        // filtered. The "return" to cluster 1 at t=12 must NOT fabricate
        // a shift event — cluster 1 was the established baseline before
        // the crosstalk and remains so. Only the synthetic t=0 boundary
        // is expected.
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 10, clusterId: 1),
            ChapterSpeakerWindow(startTime: 10, endTime: 12, clusterId: 2),
            ChapterSpeakerWindow(startTime: 12, endTime: 30, clusterId: 1),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1, "synthetic t=0 only; brief crosstalk should not surface as two shifts")
        #expect(result.first?.startTime == 0.0)
    }

    @Test("single short shift only (<5s) emits no shift boundary")
    func onlyBriefShiftEmitsNothing() {
        let detector = ChapterBoundaryDetector()
        // Two clusters appear in episode but neither sustains >5s.
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 4, clusterId: 1),
            ChapterSpeakerWindow(startTime: 4, endTime: 7, clusterId: 2),
            ChapterSpeakerWindow(startTime: 7, endTime: 10, clusterId: 3),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1, "only synthetic t=0 should emit")
    }

    @Test("first non-nil cluster does not count as a shift")
    func initialClusterIsNotAShift() {
        let detector = ChapterBoundaryDetector()
        // Episode begins with cluster 1 sustained 30s. There is no
        // PRIOR cluster to shift FROM, so no shift event should emit.
        let speakers = BoundaryFixtures.speakerTrack(count: 15, clusterId: 1)
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1)
    }

    @Test("nil cluster gaps inside a sustained run do not create a false shift")
    func nilGapsDoNotBreakSustainedRun() {
        let detector = ChapterBoundaryDetector()
        // cluster 1 for 6s, then a 2s nil gap, then cluster 1 for 6s
        // more, then cluster 2 sustained 10s. Expect ONE shift, at the
        // start of cluster 2's run — the nil gap should not register
        // as a shift event.
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 6, clusterId: 1),
            ChapterSpeakerWindow(startTime: 6, endTime: 8, clusterId: nil),
            ChapterSpeakerWindow(startTime: 8, endTime: 14, clusterId: 1),
            ChapterSpeakerWindow(startTime: 14, endTime: 24, clusterId: 2),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 14.0)
    }
}

// MARK: - Lexical category jumps

@Suite("ChapterBoundaryDetector / lexical category jumps")
struct ChapterBoundaryDetectorLexicalTests {

    @Test("category change between bins emits boundary")
    func categoryChangeEmits() {
        let detector = ChapterBoundaryDetector()
        // Bin 0 (0-30s): all transitionMarkers (3 hits).
        // Bin 1 (30-60s): all sponsor (3 hits).
        // → jump at t=30.
        let hits: [ChapterLexicalHit] = [
            .init(startTime: 5, category: .transitionMarker),
            .init(startTime: 12, category: .transitionMarker),
            .init(startTime: 25, category: .transitionMarker),
            .init(startTime: 35, category: .sponsor),
            .init(startTime: 42, category: .sponsor),
            .init(startTime: 55, category: .sponsor),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            lexicalHits: hits
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 30.0)
        #expect(result.last?.triggeringSignals == [.lexicalCategoryJump])
    }

    @Test("same-category bins do not emit jump")
    func sameCategoryNoJump() {
        let detector = ChapterBoundaryDetector()
        let hits: [ChapterLexicalHit] = [
            .init(startTime: 5, category: .sponsor),
            .init(startTime: 12, category: .sponsor),
            .init(startTime: 35, category: .sponsor),
            .init(startTime: 45, category: .sponsor),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            lexicalHits: hits
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1)
    }

    @Test("first non-empty bin is not a jump from nothing")
    func firstBinNotJump() {
        let detector = ChapterBoundaryDetector()
        let hits: [ChapterLexicalHit] = [
            .init(startTime: 5, category: .promoCode),
            .init(startTime: 10, category: .promoCode),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            lexicalHits: hits
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1, "single bin should not produce a jump boundary")
    }

    @Test("non-adjacent bins still emit jump using most-recent prior dominant")
    func nonAdjacentBinJumpDetected() {
        let detector = ChapterBoundaryDetector()
        // Bin 0 (0-30s): sponsor.
        // Bins 1,2,3 (30-120s): empty.
        // Bin 4 (120-150s): promoCode.
        // Expect a jump at the start of bin 4 (t=120) because the
        // most-recent prior non-empty bin (bin 0) had a different
        // dominant.
        let hits: [ChapterLexicalHit] = [
            .init(startTime: 5, category: .sponsor),
            .init(startTime: 15, category: .sponsor),
            .init(startTime: 125, category: .promoCode),
            .init(startTime: 140, category: .promoCode),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            lexicalHits: hits
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 120.0)
    }

    @Test("negative-time hits are filtered out defensively")
    func negativeTimeHitsFiltered() {
        let detector = ChapterBoundaryDetector()
        // Garbage upstream might emit hits with negative timestamps
        // (e.g. from a chunking edge case). These should be dropped
        // before binning rather than collapsed into bin 0.
        let hits: [ChapterLexicalHit] = [
            .init(startTime: -10, category: .sponsor),
            .init(startTime: -5, category: .sponsor),
            .init(startTime: 35, category: .promoCode),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            lexicalHits: hits
        )
        let result = detector.detect(features: snapshot)
        // Only one valid hit remains → not enough for a jump.
        #expect(result.count == 1)
    }

    @Test("hits in unsorted order still produce correct boundaries")
    func unsortedHitsHandled() {
        let detector = ChapterBoundaryDetector()
        // Same as categoryChangeEmits but with shuffled order.
        let hits: [ChapterLexicalHit] = [
            .init(startTime: 35, category: .sponsor),
            .init(startTime: 5, category: .transitionMarker),
            .init(startTime: 55, category: .sponsor),
            .init(startTime: 12, category: .transitionMarker),
            .init(startTime: 42, category: .sponsor),
            .init(startTime: 25, category: .transitionMarker),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            lexicalHits: hits
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 30.0)
    }
}

// MARK: - Long pauses

@Suite("ChapterBoundaryDetector / long pauses")
struct ChapterBoundaryDetectorPauseTests {

    @Test("contiguous run >= minLongPauseDuration emits boundary")
    func longPauseEmits() {
        let detector = ChapterBoundaryDetector()
        // 0-4s: speech (low pauseProb)
        // 4-8s: silence (high pauseProb), 4s contiguous → exceeds 2s
        // 8-12s: speech
        let pauses = [
            ChapterPauseWindow(startTime: 0, endTime: 2, pauseProbability: 0.1),
            ChapterPauseWindow(startTime: 2, endTime: 4, pauseProbability: 0.1),
            ChapterPauseWindow(startTime: 4, endTime: 6, pauseProbability: 0.95),
            ChapterPauseWindow(startTime: 6, endTime: 8, pauseProbability: 0.95),
            ChapterPauseWindow(startTime: 8, endTime: 10, pauseProbability: 0.1),
        ]
        // Use a config with a more permissive min-confidence so the
        // 0.1-weight long-pause-alone signal can emit (default gates
        // single longPause out as below-threshold).
        let config = ChapterBoundaryDetectorConfig(
            musicTransitionWeight: 0.4,
            speakerShiftWeight: 0.3,
            lexicalCategoryJumpWeight: 0.2,
            longPauseWeight: 0.1,
            musicProbabilityDelta: 0.5,
            minSpeakerRunDuration: 5.0,
            lexicalBinDuration: 30.0,
            pauseThreshold: 0.5,
            minLongPauseDuration: 2.0,
            minBoundaryConfidence: 0.05,
            minBoundarySpacing: 1.0
        )
        let permissiveDetector = ChapterBoundaryDetector(config: config)
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            pauseWindows: pauses
        )
        let result = permissiveDetector.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 4.0)
        #expect(result.last?.triggeringSignals == [.longPause])

        // Default detector with min-confidence 0.10 gates this one
        // out (longPause alone is exactly 0.1; gate is >= 0.10 so it
        // emits — sanity-check).
        let defaultResult = detector.detect(features: snapshot)
        #expect(defaultResult.count == 2, "0.1 weight at >= 0.10 gate should pass")
    }

    @Test("short pause (<2s) does not emit")
    func shortPauseFiltered() {
        let detector = ChapterBoundaryDetector()
        // Single 2s window — exactly 2s, but spec says >2s. With our
        // implementation we use >= for the duration; one 2s window has
        // duration = endTime - startTime = 2.0 which equals min, so
        // it triggers. To ensure short pauses get filtered, use a
        // single 1s window.
        let pauses = [
            ChapterPauseWindow(startTime: 0, endTime: 1, pauseProbability: 0.95),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            pauseWindows: pauses
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1, "1s pause is below 2s minimum")
    }

    @Test("non-contiguous pauses do not coalesce across speech")
    func nonContiguousPausesDoNotCoalesce() {
        let detector = ChapterBoundaryDetector()
        // Two 3s pauses (each individually >2s) separated by 2s of
        // speech. Each run separately clears the >2s threshold and
        // emits a boundary; the speech window between them must NOT
        // bridge the two pauses into a single 8s run that produces a
        // single boundary.
        let pauses = [
            ChapterPauseWindow(startTime: 0, endTime: 3, pauseProbability: 0.9),
            ChapterPauseWindow(startTime: 3, endTime: 5, pauseProbability: 0.1),
            ChapterPauseWindow(startTime: 5, endTime: 8, pauseProbability: 0.9),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            pauseWindows: pauses
        )
        let result = detector.detect(features: snapshot)
        // Boundary at t=0 is filtered (synthetic dedup), so we expect
        // synthetic t=0 + boundary at t=5 only. The first pause's
        // event lands at t=0 and is dropped by the synthetic-boundary
        // dedup gate.
        #expect(result.count == 2)
        #expect(result.last?.startTime == 5.0)
        #expect(result.last?.triggeringSignals == [.longPause])
    }

    @Test("brief pause (==2s) does not emit (strict >)")
    func exactlyTwoSecondPauseFiltered() {
        let detector = ChapterBoundaryDetector()
        let pauses = [
            ChapterPauseWindow(startTime: 0, endTime: 2, pauseProbability: 0.95),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            pauseWindows: pauses
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 1, "exactly-2s pause is at the strict-> threshold; should not emit")
    }

    @Test("trailing pause exactly at threshold does not emit (strict >, parity with mid-loop)")
    func exactlyTwoSecondTrailingPauseFiltered() {
        // R3 regression pin: prior code used `>=` in the trailing
        // flush, so a 2s pause that *ends the episode* would emit
        // while the same 2s pause followed by speech would not. This
        // test reproduces the trailing-only path: speech, then a 2s
        // pause that runs to the end of the input, away from t=0 so
        // the synthetic-boundary dedup gate can't mask the bug.
        let pauses = [
            ChapterPauseWindow(startTime: 0, endTime: 5, pauseProbability: 0.1),
            ChapterPauseWindow(startTime: 5, endTime: 7, pauseProbability: 0.95),
        ]
        let config = ChapterBoundaryDetectorConfig(
            musicTransitionWeight: 0.4,
            speakerShiftWeight: 0.3,
            lexicalCategoryJumpWeight: 0.2,
            longPauseWeight: 0.1,
            musicProbabilityDelta: 0.5,
            minSpeakerRunDuration: 5.0,
            lexicalBinDuration: 30.0,
            pauseThreshold: 0.5,
            minLongPauseDuration: 2.0,
            minBoundaryConfidence: 0.05,  // permissive so we test the duration gate, not the confidence gate
            minBoundarySpacing: 1.0
        )
        let permissive = ChapterBoundaryDetector(config: config)
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            pauseWindows: pauses
        )
        let result = permissive.detect(features: snapshot)
        #expect(result.count == 1, "trailing 2s pause is at the strict-> threshold; should not emit even though it terminates the input array")
    }

    @Test("trailing pause strictly greater than threshold emits (parity with mid-loop)")
    func trailingPauseAboveThresholdEmits() {
        // Companion to `exactlyTwoSecondTrailingPauseFiltered`: a 3s
        // trailing pause clears the strict-> threshold and should
        // emit, confirming the trailing flush is wired up at all.
        let pauses = [
            ChapterPauseWindow(startTime: 0, endTime: 5, pauseProbability: 0.1),
            ChapterPauseWindow(startTime: 5, endTime: 8, pauseProbability: 0.95),
        ]
        let config = ChapterBoundaryDetectorConfig(
            musicTransitionWeight: 0.4,
            speakerShiftWeight: 0.3,
            lexicalCategoryJumpWeight: 0.2,
            longPauseWeight: 0.1,
            musicProbabilityDelta: 0.5,
            minSpeakerRunDuration: 5.0,
            lexicalBinDuration: 30.0,
            pauseThreshold: 0.5,
            minLongPauseDuration: 2.0,
            minBoundaryConfidence: 0.05,
            minBoundarySpacing: 1.0
        )
        let permissive = ChapterBoundaryDetector(config: config)
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            pauseWindows: pauses
        )
        let result = permissive.detect(features: snapshot)
        #expect(result.count == 2)
        #expect(result.last?.startTime == 5.0)
        #expect(result.last?.triggeringSignals == [.longPause])
    }
}

// MARK: - Combined signals

@Suite("ChapterBoundaryDetector / combined signals")
struct ChapterBoundaryDetectorCombinedTests {

    @Test("co-located music + speaker + lexical + pause stack into one boundary")
    func allFourSignalsStack() {
        let detector = ChapterBoundaryDetector()
        let dur: TimeInterval = 600

        // Engineer all four signal events to land at t=30 exactly:
        //   * Music: probability jumps low→high at t=30 (event lands at
        //     start of the second window, which is t=30).
        //   * Speaker: cluster 1 sustained 0..30, cluster 2 starting at
        //     t=30 sustained ≥5s.
        //   * Lexical: bin 0 (0-30s) dominated by transitionMarker, bin 1
        //     (30-60s) dominated by sponsor. Jump event lands at t=30.
        //   * Long pause: 2s silence run starting at t=30.
        let music = [
            ChapterMusicWindow(startTime: 28, endTime: 30, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 30, endTime: 32, musicProbability: 0.95),
        ]
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 30, clusterId: 1),
            ChapterSpeakerWindow(startTime: 30, endTime: 40, clusterId: 2),
        ]
        let hits: [ChapterLexicalHit] = [
            .init(startTime: 5, category: .transitionMarker),
            .init(startTime: 15, category: .transitionMarker),
            .init(startTime: 25, category: .transitionMarker),
            .init(startTime: 35, category: .sponsor),
            .init(startTime: 45, category: .sponsor),
            .init(startTime: 55, category: .sponsor),
        ]
        let pauses = [
            ChapterPauseWindow(startTime: 28, endTime: 30, pauseProbability: 0.1),
            ChapterPauseWindow(startTime: 30, endTime: 32, pauseProbability: 0.95),
            ChapterPauseWindow(startTime: 32, endTime: 34, pauseProbability: 0.95),
            ChapterPauseWindow(startTime: 34, endTime: 36, pauseProbability: 0.1),
        ]

        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            musicWindows: music,
            speakerWindows: speakers,
            lexicalHits: hits,
            pauseWindows: pauses
        )
        let result = detector.detect(features: snapshot)
        // Expect: synthetic t=0 + a single combined boundary at t=30
        // carrying all four signals.
        let combined = result.first { $0.startTime == 30.0 }
        #expect(combined != nil, "expected a combined boundary at t=30")
        let signals = Set(combined?.triggeringSignals ?? [])
        #expect(signals.contains(.musicTransition))
        #expect(signals.contains(.speakerShift))
        #expect(signals.contains(.lexicalCategoryJump))
        #expect(signals.contains(.longPause))
        // Confidence: 0.4 + 0.3 + 0.2 + 0.1 = 1.0 (all four normalize).
        #expect(abs((combined?.boundaryConfidence ?? 0) - 1.0) < 0.0001)
    }

    @Test("partial signal stacks produce intermediate confidence")
    func threeOfFourSignalsStack() {
        let detector = ChapterBoundaryDetector()
        // Music + speaker + lexical at t=30 (no pause).
        let music = [
            ChapterMusicWindow(startTime: 28, endTime: 30, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 30, endTime: 32, musicProbability: 0.95),
        ]
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 30, clusterId: 1),
            ChapterSpeakerWindow(startTime: 30, endTime: 40, clusterId: 2),
        ]
        let hits: [ChapterLexicalHit] = [
            .init(startTime: 5, category: .transitionMarker),
            .init(startTime: 25, category: .transitionMarker),
            .init(startTime: 35, category: .sponsor),
            .init(startTime: 55, category: .sponsor),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            musicWindows: music,
            speakerWindows: speakers,
            lexicalHits: hits
        )
        let result = detector.detect(features: snapshot)
        let combined = result.first { $0.startTime == 30.0 }
        #expect(combined != nil)
        // 0.4 + 0.3 + 0.2 = 0.9.
        #expect(abs((combined?.boundaryConfidence ?? 0) - 0.9) < 0.0001)
    }

    @Test("all-four-signals confidence sums to exactly 1.0 (no Float drift)")
    func confidenceSumsToOne() {
        let detector = ChapterBoundaryDetector()
        let music = [
            ChapterMusicWindow(startTime: 28, endTime: 30, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 30, endTime: 32, musicProbability: 0.95),
        ]
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 30, clusterId: 1),
            ChapterSpeakerWindow(startTime: 30, endTime: 40, clusterId: 2),
        ]
        let hits: [ChapterLexicalHit] = [
            .init(startTime: 5, category: .transitionMarker),
            .init(startTime: 25, category: .transitionMarker),
            .init(startTime: 35, category: .sponsor),
            .init(startTime: 55, category: .sponsor),
        ]
        let pauses = [
            ChapterPauseWindow(startTime: 30, endTime: 33, pauseProbability: 0.95),
            ChapterPauseWindow(startTime: 33, endTime: 35, pauseProbability: 0.1),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            musicWindows: music,
            speakerWindows: speakers,
            lexicalHits: hits,
            pauseWindows: pauses
        )
        let result = detector.detect(features: snapshot)
        let combined = result.first { $0.startTime == 30.0 }
        #expect(combined?.boundaryConfidence == 1.0,
                "Float accumulation of 0.4+0.3+0.2+0.1 must clamp to exactly 1.0")
    }

    @Test("output is deterministic across repeated calls")
    func outputIsDeterministic() {
        let detector = ChapterBoundaryDetector()
        // Setup with multiple co-timed events to stress-test sort
        // stability.
        let music = [
            ChapterMusicWindow(startTime: 8, endTime: 10, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 10, endTime: 12, musicProbability: 0.95),
        ]
        let speakers = [
            ChapterSpeakerWindow(startTime: 0, endTime: 10, clusterId: 1),
            ChapterSpeakerWindow(startTime: 10, endTime: 20, clusterId: 2),
        ]
        let pauses = [
            ChapterPauseWindow(startTime: 10, endTime: 13, pauseProbability: 0.95),
            ChapterPauseWindow(startTime: 13, endTime: 15, pauseProbability: 0.1),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music,
            speakerWindows: speakers,
            pauseWindows: pauses
        )
        // Run many times — assertions must hold every time.
        let reference = detector.detect(features: snapshot)
        for _ in 0..<20 {
            let next = detector.detect(features: snapshot)
            #expect(next == reference, "detector output must be byte-stable across runs")
        }
    }

    @Test("output is in chronological order")
    func outputIsChronological() {
        let detector = ChapterBoundaryDetector()
        // Place independent signal events at t=10, t=50, t=200.
        let music = [
            ChapterMusicWindow(startTime: 8, endTime: 10, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 10, endTime: 12, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 198, endTime: 200, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 200, endTime: 202, musicProbability: 0.95),
        ]
        let speakers =
            BoundaryFixtures.speakerTrack(count: 25, clusterId: 1) +
            BoundaryFixtures.speakerTrack(count: 25, clusterId: 2, startOffset: 50)
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 600,
            musicWindows: music,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        let times = result.map(\.startTime)
        let sorted = times.sorted()
        #expect(times == sorted)
        #expect(times.first == 0)
    }

    @Test("transitions further than minBoundarySpacing emit as separate boundaries")
    func separatedTransitionsEmitSeparately() {
        let detector = ChapterBoundaryDetector()
        // Two music transitions 2s apart with 1s default min spacing
        // → they DO emit as separate boundaries. Each carries its
        // single .musicTransition contribution (confidence 0.4).
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 4, endTime: 6, musicProbability: 0.05),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 3, "two separated transitions emit two boundaries")
        for candidate in result.dropFirst() {
            #expect(abs(candidate.boundaryConfidence - 0.4) < 0.0001)
        }
    }

    @Test("repeated same-signal events within minBoundarySpacing dedupe to one contribution")
    func repeatedSameSignalDeduplicates() {
        // R5 regression pin: the cluster algorithm uses a Set to
        // deduplicate signal contributions per cluster. Two music
        // transitions 0.5s apart (well within the 1s
        // minBoundarySpacing) must collapse into ONE boundary
        // carrying ONE .musicTransition contribution (confidence
        // 0.4), not two contributions (which would push confidence to
        // 0.8 and double-weight a single oscillation as if it were
        // two distinct events).
        //
        // The events are placed AFTER t=minBoundarySpacing (1.0s) so
        // that they land in the post-synthetic-dedup window and we
        // test the cluster-merge path, not the t=0 dedup path. Test
        // geometry uses 0.5s windows so two probability flips can
        // happen 0.5s apart:
        //   [10.0-10.5] prob 0.05 (silent)
        //   [10.5-11.0] prob 0.95 (music)   delta > 0.5 → event @ t=10.5
        //   [11.0-11.5] prob 0.05 (silent)  delta > 0.5 → event @ t=11.0
        //   [11.5-12.0] prob 0.05 (silent)  no delta
        // Events at t=10.5 and t=11.0 are 0.5s apart — well within
        // 1.0s minBoundarySpacing → cluster together.
        let detector = ChapterBoundaryDetector()
        let music = [
            ChapterMusicWindow(startTime: 10.0, endTime: 10.5, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 10.5, endTime: 11.0, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 11.0, endTime: 11.5, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 11.5, endTime: 12.0, musicProbability: 0.05),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        // Synthetic t=0 + one merged boundary at t=10.5 (the earlier
        // of the two co-clustered events). Confidence = 0.4
        // (single-signal weight), NOT 0.8 (would indicate
        // double-counting).
        #expect(result.count == 2, "two co-clustered same-signal events should produce ONE boundary, not two")
        let merged = result.last
        #expect(merged?.startTime == 10.5)
        #expect(abs((merged?.boundaryConfidence ?? 0) - 0.4) < 0.0001,
                "co-clustered same-signal events must deduplicate to a single 0.4-weight contribution, not double-count to 0.8")
        #expect(merged?.triggeringSignals == [.musicTransition])
    }
}

// MARK: - Edge cases: monologue + short episode

@Suite("ChapterBoundaryDetector / edge cases")
struct ChapterBoundaryDetectorEdgeTests {

    @Test("monologue (single speaker, no music) produces sparse output")
    func monologueIsSparse() {
        let detector = ChapterBoundaryDetector()
        // 60-min episode, single speaker, no music, no lexical, no
        // pauses long enough.
        let dur: TimeInterval = 3600
        let speakers = BoundaryFixtures.speakerTrack(count: 1800, clusterId: 1)
        let music = BoundaryFixtures.musicTrack(count: 1800, probability: 0.0)
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            musicWindows: music,
            speakerWindows: speakers
        )
        let result = detector.detect(features: snapshot)
        // Sparse: only the synthetic t=0 boundary.
        #expect(result.count == 1)
    }

    @Test("short episode (<5min) still runs and emits t=0")
    func shortEpisodeRunsCleanly() {
        let detector = ChapterBoundaryDetector()
        let dur: TimeInterval = 240 // 4 minutes
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 200, endTime: 202, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 202, endTime: 204, musicProbability: 0.05),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        #expect(result.count == 3)
        #expect(result.first?.startTime == 0)
        #expect(result.last!.startTime <= dur)
    }

    @Test("boundaries past episode duration are filtered out")
    func boundariesPastDurationFiltered() {
        let detector = ChapterBoundaryDetector()
        let dur: TimeInterval = 60
        // Music transition at t=100 (past episode end). The test feeds
        // the windows in anyway to verify the duration clamp.
        let music = [
            ChapterMusicWindow(startTime: 98, endTime: 100, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 100, endTime: 102, musicProbability: 0.95),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            musicWindows: music
        )
        let result = detector.detect(features: snapshot)
        // Boundary at t=100 is past 60s episode → filtered out.
        #expect(result.count == 1)
    }

    @Test("detect is idempotent (same snapshot → same output)")
    func detectIsIdempotent() {
        let detector = ChapterBoundaryDetector()
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 0.95),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 60,
            musicWindows: music
        )
        let r1 = detector.detect(features: snapshot)
        let r2 = detector.detect(features: snapshot)
        #expect(r1 == r2)
    }
}

// MARK: - Performance

@Suite("ChapterBoundaryDetector / performance")
struct ChapterBoundaryDetectorPerfTests {

    @Test("60-minute episode with realistic input completes within budget")
    func sixtyMinuteEpisodeUnderBudget() {
        let detector = ChapterBoundaryDetector()

        // Realistic 60-min show:
        //   - 1800 music windows (2s each)
        //   - 1800 speaker windows (2s each), with cluster ID toggling
        //     every ~30s to exercise the speaker-shift lookahead path
        //   - ~120 lexical hits sprinkled across the episode (typical
        //     density is well under this for a 60-min show)
        //   - 1800 pause windows
        let windowCount = 1800
        let dur = TimeInterval(windowCount * 2)

        var music: [ChapterMusicWindow] = []
        music.reserveCapacity(windowCount)
        for index in 0..<windowCount {
            let t = TimeInterval(index) * 2.0
            // Simulate 4 music intros / outros per episode.
            let phase = (index / 450) % 2 == 0 ? 0.1 : 0.9
            music.append(ChapterMusicWindow(
                startTime: t,
                endTime: t + 2,
                musicProbability: phase
            ))
        }

        var speakers: [ChapterSpeakerWindow] = []
        speakers.reserveCapacity(windowCount)
        for index in 0..<windowCount {
            let t = TimeInterval(index) * 2.0
            speakers.append(ChapterSpeakerWindow(
                startTime: t,
                endTime: t + 2,
                clusterId: (index / 15) % 3 // toggle every ~30s
            ))
        }

        var hits: [ChapterLexicalHit] = []
        hits.reserveCapacity(120)
        let categories: [LexicalPatternCategory] = [
            .sponsor, .promoCode, .urlCTA, .purchaseLanguage, .transitionMarker
        ]
        for index in 0..<120 {
            hits.append(ChapterLexicalHit(
                startTime: TimeInterval(index) * 30.0,
                category: categories[index % categories.count]
            ))
        }

        var pauses: [ChapterPauseWindow] = []
        pauses.reserveCapacity(windowCount)
        for index in 0..<windowCount {
            let t = TimeInterval(index) * 2.0
            // Roughly 5% pauses sprinkled.
            let p = (index % 19 == 0) ? 0.95 : 0.05
            pauses.append(ChapterPauseWindow(
                startTime: t,
                endTime: t + 2,
                pauseProbability: p
            ))
        }

        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            musicWindows: music,
            speakerWindows: speakers,
            lexicalHits: hits,
            pauseWindows: pauses
        )

        // Warm up — JIT/codegen pages.
        _ = detector.detect(features: snapshot)

        let start = ContinuousClock.now
        let result = detector.detect(features: snapshot)
        let elapsed = ContinuousClock.now - start

        // Spec calls for <50ms on target device. Simulator under
        // parallel test execution is ~2-3x slower than device; allow
        // 200ms here so the test is deterministic and not flaky on CI
        // simulators (matches the simulator-tolerance pattern in
        // MinimalContiguousSpanDecoderTests perf test).
        #expect(elapsed < .milliseconds(200))
        // Sanity: detector ran and produced at least the synthetic
        // t=0 boundary.
        #expect(result.first?.startTime == 0)
    }

    @Test("detectRefined on a 60-minute episode completes within budget")
    func detectRefinedSixtyMinuteUnderBudget() {
        // 60-min snapshot exercised through the bead-5
        // `detectRefined` entry point (adds two density gates on top
        // of `detect`). The added work is O(n log n) for the cap
        // sort plus O(retainedCap × dropped) for merge selection;
        // both bounded by `ceil(episodeMinutes/5)` (≤24 for a 2h
        // show). Microseconds on top.
        //
        // The synthetic input is intentionally less-dense than the
        // bare-`detect` perf test so the candidate density stays
        // below the pathological-rate threshold (1/90s avg). The
        // bare-detect test packs many same-second lexical/speaker
        // hits and produces ≥40 candidates on a 60-min episode →
        // 40/3600 = 0.011 ≈ 1/90s, on the threshold. We use sparser
        // signals here so refinement runs cap-and-merge (or noChange),
        // not pathological abort, which is the path we actually want
        // perf coverage for.
        let detector = ChapterBoundaryDetector()
        let windowCount = 1800
        let dur = TimeInterval(windowCount * 2)

        var music: [ChapterMusicWindow] = []
        music.reserveCapacity(windowCount)
        for index in 0..<windowCount {
            let t = TimeInterval(index) * 2.0
            // 4 music intros / outros per episode (same as bare-detect
            // test). These dominate the candidate set and produce
            // ~4 boundaries.
            let phase = (index / 450) % 2 == 0 ? 0.1 : 0.9
            music.append(ChapterMusicWindow(
                startTime: t,
                endTime: t + 2,
                musicProbability: phase
            ))
        }
        var speakers: [ChapterSpeakerWindow] = []
        speakers.reserveCapacity(windowCount)
        for index in 0..<windowCount {
            let t = TimeInterval(index) * 2.0
            // Speaker cluster toggles every ~5 minutes (300s = 150
            // windows) so we get ~12 speaker shifts on a 60-min show.
            speakers.append(ChapterSpeakerWindow(
                startTime: t,
                endTime: t + 2,
                clusterId: (index / 150) % 2
            ))
        }
        // 12 sparse lexical hits (every 5 min). Enough to exercise the
        // lexical signal path without dominating the candidate count.
        var hits: [ChapterLexicalHit] = []
        hits.reserveCapacity(12)
        let categories: [LexicalPatternCategory] = [
            .sponsor, .promoCode, .urlCTA, .purchaseLanguage, .transitionMarker
        ]
        for index in 0..<12 {
            hits.append(ChapterLexicalHit(
                startTime: TimeInterval(index) * 300.0,
                category: categories[index % categories.count]
            ))
        }
        // ~15 long-pause spikes (every 4 min).
        var pauses: [ChapterPauseWindow] = []
        pauses.reserveCapacity(windowCount)
        for index in 0..<windowCount {
            let t = TimeInterval(index) * 2.0
            let p = (index % 120 == 0) ? 0.95 : 0.05
            pauses.append(ChapterPauseWindow(
                startTime: t,
                endTime: t + 2,
                pauseProbability: p
            ))
        }
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            musicWindows: music,
            speakerWindows: speakers,
            lexicalHits: hits,
            pauseWindows: pauses
        )

        // Warm up.
        _ = detector.detectRefined(features: snapshot)

        let start = ContinuousClock.now
        let refined = detector.detectRefined(features: snapshot)
        let elapsed = ContinuousClock.now - start

        // Same simulator-tolerant 200ms budget as the bare detect
        // perf test. The gate work is microseconds on top.
        #expect(elapsed < .milliseconds(200))
        // Sanity: refined result is non-empty (sparse-enough input
        // does NOT trigger the pathological-rate gate) and still has
        // the synthetic t=0 at head.
        #expect(refined.candidates.first?.startTime == 0,
                "sparse-enough perf snapshot must survive density gates with t=0 at head")
        // The whole point of this test is to exercise the post-detect
        // path. Pathological-rate gate firing would be a regression in
        // the test fixture (we'd be measuring an O(1) abort, not the
        // sort + merge path). Assert a non-pathological outcome.
        if case .pathologicalRate = refined.outcome {
            Issue.record("perf test fixture must not trigger pathological-rate gate; got \(refined.outcome)")
        }
    }
}

// MARK: - Density gates (playhead-au2v.1.5)

private enum DensityFixtures {

    /// Build a synthetic candidate list with a t=0 boundary plus
    /// `nonZeroCount` boundaries spread evenly across the episode
    /// duration. Each non-zero boundary carries the supplied
    /// `confidence` and `signals`.
    static func candidates(
        episodeDuration: TimeInterval,
        nonZeroCount: Int,
        confidence: Float = 0.5,
        signals: [BoundarySignal] = [.musicTransition]
    ) -> [ChapterCandidate] {
        var out: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        guard nonZeroCount > 0 else { return out }
        let spacing = episodeDuration / Double(nonZeroCount + 1)
        for index in 1...nonZeroCount {
            out.append(
                ChapterCandidate(
                    startTime: spacing * Double(index),
                    boundaryConfidence: confidence,
                    triggeringSignals: signals
                )
            )
        }
        return out
    }
}

@Suite("ChapterBoundaryDetector / density gates / pathological rate")
struct ChapterBoundaryDetectorPathologicalRateTests {

    @Test("rate strictly above 1/90s aborts to empty + pathologicalRate outcome")
    func aboveThresholdAborts() {
        // 600s episode at 1/90s threshold → max ≤ 600/90 ≈ 6.67. So 7
        // candidates already exceeds 1/90s. Use 30 to be unambiguous.
        let dur: TimeInterval = 600
        let cands = DensityFixtures.candidates(episodeDuration: dur, nonZeroCount: 30)
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        #expect(result.candidates.isEmpty, "pathological rate must abort to empty list")
        if case let .pathologicalRate(detected, episodeDur, perSec) = result.outcome {
            #expect(detected == cands.count)
            #expect(episodeDur == dur)
            #expect(perSec > 1.0 / 90.0)
        } else {
            Issue.record("expected .pathologicalRate outcome, got \(result.outcome)")
        }
    }

    @Test("rate exactly at 1/90s does NOT abort (strict >)")
    func exactlyAtThresholdDoesNotAbort() {
        // Engineer count / duration == 1/90 exactly: 100 candidates over
        // 9000 seconds. Density gates are applied to the whole list
        // including t=0; we put 99 non-zero candidates so the total is
        // 100 → rate = 100/9000 = 1/90 exactly.
        let dur: TimeInterval = 9000
        // Use confidence 0.9 so cap-and-merge still applies but no
        // abort.
        let cands = DensityFixtures.candidates(
            episodeDuration: dur,
            nonZeroCount: 99,
            confidence: 0.9
        )
        #expect(cands.count == 100)
        // Rate = 100/9000 = 1/90 exactly → strict-> threshold misses.
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        if case .pathologicalRate = result.outcome {
            Issue.record("rate exactly at 1/90s must not trigger pathological-rate gate")
        }
    }

    @Test("rate just over 1/90s triggers gate")
    func justOverThresholdAborts() {
        // 9000s episode with 101 candidates → 101/9000 > 1/90.
        let dur: TimeInterval = 9000
        let cands = DensityFixtures.candidates(
            episodeDuration: dur,
            nonZeroCount: 100
        )
        #expect(cands.count == 101)
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        #expect(result.candidates.isEmpty)
        if case .pathologicalRate = result.outcome {
            // ok
        } else {
            Issue.record("expected pathologicalRate outcome")
        }
    }

    @Test("normal density (well below 1/90s) does not trigger pathological-rate gate")
    func normalDensitySafe() {
        // 60-min episode, 6 chapters → rate = 6/3600 = 1/600 ≪ 1/90.
        let dur: TimeInterval = 3600
        let cands = DensityFixtures.candidates(episodeDuration: dur, nonZeroCount: 6)
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        if case .pathologicalRate = result.outcome {
            Issue.record("normal density must not trigger pathological-rate gate")
        }
    }
}

@Suite("ChapterBoundaryDetector / density gates / cap-and-merge")
struct ChapterBoundaryDetectorCapAndMergeTests {

    @Test("dense candidate set keeps top-N by confidence")
    func denseCapKeepsTopN() {
        // 60-min episode → target = ceil(60/5) = 12, floor 8 (≥40min)
        // applies but is below the target so the cap is 12. Hand 20
        // non-zero candidates with descending confidences so we can
        // verify the top-12 retained.
        let dur: TimeInterval = 3600
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        for index in 0..<20 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(60 + index * 30), // every 30s starting at 60s
                    boundaryConfidence: 0.99 - Float(index) * 0.04,
                    triggeringSignals: [.musicTransition]
                )
            )
        }
        // Total candidates = 21; rate = 21/3600 = 1/171 < 1/90. Safe.
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        // Outcome should be capApplied with retained = 13 (12 cap +
        // synthetic t=0).
        guard case let .capApplied(detected, retained, _, mergeRecords) = result.outcome else {
            Issue.record("expected capApplied outcome, got \(result.outcome)")
            return
        }
        #expect(detected == 21)
        #expect(retained == 13)
        #expect(result.candidates.count == 13)
        // First survivor must be t=0; rest in startTime order.
        #expect(result.candidates.first?.startTime == 0)
        let times = result.candidates.map(\.startTime)
        #expect(times == times.sorted())
        // The top 12 non-zero by confidence are the FIRST 12 we
        // appended (highest confidences). Their times: 60, 90, ..., 60+11*30 = 390.
        let nonZeroTimes = result.candidates.dropFirst().map(\.startTime)
        let expectedTimes = (0..<12).map { TimeInterval(60 + $0 * 30) }
        #expect(Array(nonZeroTimes) == expectedTimes)
        // 8 boundaries dropped → 8 merge records.
        #expect(mergeRecords.count == 8)
        // Each merge record's dropped time matches a non-retained
        // candidate's time.
        let droppedTimes = mergeRecords.map(\.droppedStartTime).sorted()
        let expectedDroppedTimes = (12..<20).map { TimeInterval(60 + $0 * 30) }
        #expect(droppedTimes == expectedDroppedTimes)
    }

    @Test("sparse candidate set passes through unchanged")
    func sparsePassesThrough() {
        // 60-min episode, only 4 non-zero candidates → well under the
        // cap of 12, no floor relevance.
        let dur: TimeInterval = 3600
        let cands = DensityFixtures.candidates(
            episodeDuration: dur,
            nonZeroCount: 4
        )
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        #expect(result.candidates.count == cands.count)
        #expect(result.candidates == cands)
        if case .noChange = result.outcome {
            // ok
        } else {
            Issue.record("expected noChange outcome, got \(result.outcome)")
        }
    }

    @Test("synthetic t=0 boundary is always retained even after a tight cap")
    func t0AlwaysRetained() {
        // Build a 60-min episode with 30 non-zero candidates, all at
        // confidence 1.0 (ties). After the cap we should still have
        // exactly 13 = 12 + t=0 survivors AND the t=0 must be at the
        // head. This stresses the load-bearing invariant: even when
        // every non-zero boundary "ties" the synthetic t=0 on
        // confidence, t=0 cannot be crowded out.
        let dur: TimeInterval = 3600
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        for index in 0..<30 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(60 + index * 30),
                    boundaryConfidence: 1.0,
                    triggeringSignals: [.musicTransition, .speakerShift]
                )
            )
        }
        // Total 31; rate = 31/3600 < 1/90. Safe from pathological.
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        #expect(result.candidates.first?.startTime == 0,
                "synthetic t=0 boundary must always be at head of survivor list")
        #expect(result.candidates.count == 13)
    }

    @Test("floor of 8 enforced for episodes ≥40 min")
    func floorEnforcedAt40Min() {
        // 40-min episode → ceil(40/5) = 8. Floor 8 applies. Total
        // cap = max(8,8) = 8. Detected 12 → retained 9 (8 + t=0).
        let dur: TimeInterval = 40 * 60
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        for index in 0..<12 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(60 + index * 60),
                    boundaryConfidence: 0.5 + Float(index) * 0.01,
                    triggeringSignals: [.musicTransition]
                )
            )
        }
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        guard case let .capApplied(_, retained, _, _) = result.outcome else {
            Issue.record("expected capApplied outcome, got \(result.outcome)")
            return
        }
        #expect(retained == 9, "8 non-zero + synthetic t=0 = 9")
    }

    @Test("floor of 8 NOT enforced for episodes <40 min")
    func floorNotEnforcedBelow40Min() {
        // 30-min episode → ceil(30/5) = 6. Floor does NOT apply
        // (30<40). Detected 12 → retained 7 (6 + t=0).
        let dur: TimeInterval = 30 * 60
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        for index in 0..<12 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(60 + index * 60),
                    boundaryConfidence: 0.5 + Float(index) * 0.01,
                    triggeringSignals: [.musicTransition]
                )
            )
        }
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        guard case let .capApplied(_, retained, _, _) = result.outcome else {
            Issue.record("expected capApplied outcome, got \(result.outcome)")
            return
        }
        #expect(retained == 7, "6 non-zero + synthetic t=0 = 7 (floor 8 must NOT apply)")
    }

    @Test("episode ≥40 min, detected count just under floor of 8 → keep all")
    func belowFloorKeepsAll() {
        // 50-min episode → ceil(50/5) = 10. But detected only 7
        // non-zero + 1 t=0 = 8. Cap = min(8, max(8,10)) = 8. So all
        // 8 are kept (no drop). Outcome should be noChange because
        // detected ≤ cap.
        let dur: TimeInterval = 50 * 60
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        for index in 0..<7 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(60 + index * 300),
                    boundaryConfidence: 0.5,
                    triggeringSignals: [.musicTransition]
                )
            )
        }
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        #expect(result.candidates.count == 8)
        if case .noChange = result.outcome {
            // ok
        } else {
            Issue.record("expected noChange when detected ≤ cap, got \(result.outcome)")
        }
    }

    @Test("episode ≥40 min, detected just at floor of 8 → keep all")
    func exactlyAtFloorKeepsAll() {
        // 40-min episode → cap = 8. Detected 8 non-zero + 1 t=0 = 9.
        // Cap is 8 for non-zero; so we'd cap to 8 = 7 non-zero + 1
        // t=0. Wait — the spec says "detected count exactly at floor
        // → keep all" assumes detected COUNT == 8. But total
        // including t=0 is 9. Re-read: "Cap: keep top-N by
        // boundaryConfidence where N = min(detected, max(8, ceil(...)))".
        // The bead spec's "detected" is the upstream-detector count
        // which conventionally INCLUDES t=0 (everything `detect`
        // returns). So detected==8 means 7 non-zero + 1 t=0.
        let dur: TimeInterval = 40 * 60
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        for index in 0..<7 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(60 + index * 300),
                    boundaryConfidence: 0.5,
                    triggeringSignals: [.musicTransition]
                )
            )
        }
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        #expect(result.candidates.count == 8, "exactly at floor: keep all 8")
        if case .noChange = result.outcome {
            // ok
        } else {
            Issue.record("expected noChange when detected ≤ cap, got \(result.outcome)")
        }
    }

    @Test("episode ≥40 min, detected > target but < floor → keep all")
    func detectedAboveTargetBelowFloorKeepsAll() {
        // 40-min episode: ceil(40/5) = 8, floor = 8. cap = 8.
        // Construct case where target < floor. 41-min episode → ceil(41/5)
        // = 9. Floor = 8. cap = max(8,9) = 9. We need detected
        // between target and floor, but here floor (8) ≤ target (9),
        // so we can't make detected sit between them with the spec's
        // formula on a 41-min episode.
        //
        // The spec wording "detected count > target but < floor"
        // describes a case where target < floor — which only happens
        // when ceil(min/5) < 8 AND floor applies. E.g. 40-min
        // episode is borderline: target = 8 exactly, floor = 8.
        // There's no ≥40min episode where target < 8.
        //
        // Reinterpret: the case is meaningful for the EARLY-RETURN
        // path. 40-min episode, detected = 7 (above some implicit
        // "low" target but below the floor of 8) → cap = min(7,8) =
        // 7 → noChange. We test that the floor of 8 does NOT
        // ratchet up to fabricate an 8th boundary.
        let dur: TimeInterval = 40 * 60
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        for index in 0..<6 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(60 + index * 300),
                    boundaryConfidence: 0.5,
                    triggeringSignals: [.musicTransition]
                )
            )
        }
        // Total = 7 (1 t=0 + 6 non-zero), all retained because cap =
        // min(7, max(8,8)) = 7.
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        #expect(result.candidates.count == 7,
                "floor must not fabricate boundaries beyond detected count")
        if case .noChange = result.outcome {
            // ok
        } else {
            Issue.record("expected noChange when detected ≤ cap, got \(result.outcome)")
        }
    }

    @Test("merge picks the neighbor with higher signal-overlap")
    func mergeBySignalOverlap() {
        // Construct a dense set where one boundary's drop has
        // adjacent retained neighbors with clearly different
        // signal-overlaps to the dropped boundary's signals. The
        // dropped boundary should be merged into the
        // higher-similarity neighbor.
        //
        // Episode 60 min → cap = 12. Build 13 non-zero candidates.
        // Confidence configured so the boundary at t=300 is the
        // weakest and gets dropped. Neighbors at t=240 (prev) and
        // t=360 (next) carry disjoint signal sets; the dropped
        // boundary's signal set overlaps perfectly with t=240
        // (prev), so the merge should pick prev.
        let dur: TimeInterval = 3600
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]

        // 12 strong boundaries at t=60..720 step 60, but t=300 is
        // the WEAKEST so it gets dropped.
        let strongConfidence: Float = 0.99
        for index in 0..<12 {
            let time = TimeInterval(60 + index * 60)
            let signals: [BoundarySignal]
            let confidence: Float
            if time == 240 {
                signals = [.musicTransition, .speakerShift] // prev neighbor
                confidence = strongConfidence
            } else if time == 360 {
                signals = [.lexicalCategoryJump, .longPause] // next neighbor
                confidence = strongConfidence
            } else if time == 300 {
                // The to-be-dropped boundary; lower confidence so it's
                // selected for drop. Signals overlap with prev's set.
                signals = [.musicTransition, .speakerShift]
                confidence = 0.10 // weakest
            } else {
                signals = [.musicTransition]
                confidence = strongConfidence
            }
            cands.append(
                ChapterCandidate(
                    startTime: time,
                    boundaryConfidence: confidence,
                    triggeringSignals: signals
                )
            )
        }
        // Add a 13th boundary at t=780 with high confidence, also
        // strong, so that 12 strong boundaries survive and the
        // weakest one (at t=300) drops.
        cands.append(
            ChapterCandidate(
                startTime: 780,
                boundaryConfidence: strongConfidence,
                triggeringSignals: [.musicTransition]
            )
        )
        // Total 14 = 1 t=0 + 13 non-zero. Cap = 12 → 1 drop (t=300).
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        guard case let .capApplied(_, _, _, mergeRecords) = result.outcome else {
            Issue.record("expected capApplied outcome, got \(result.outcome)")
            return
        }
        #expect(mergeRecords.count == 1)
        let pick = mergeRecords[0]
        #expect(pick.droppedStartTime == 300)
        #expect(pick.absorbedIntoStartTime == 240,
                "prev neighbor (t=240) shares both signals with dropped; should be picked")
        // Jaccard for {music,speaker} vs {music,speaker} = 2/2 = 1.0;
        // for {music,speaker} vs {lexical,longPause} = 0/4 = 0.0.
        #expect(pick.signalOverlap == 1.0)
    }

    @Test("merge falls back to higher confidence on equal signal-overlap")
    func mergeFallsBackOnEqualOverlap() {
        // Both neighbors share equal (zero) signal-overlap with the
        // dropped boundary. The neighbor with higher confidence
        // should win the merge.
        let dur: TimeInterval = 3600
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]

        // 12 strong boundaries; the to-be-dropped at t=300 has
        // signals disjoint from both neighbors at t=240 and t=360.
        // Neighbor at t=240 has higher confidence than t=360.
        for index in 0..<12 {
            let time = TimeInterval(60 + index * 60)
            let signals: [BoundarySignal]
            let confidence: Float
            if time == 240 {
                signals = [.musicTransition]
                confidence = 0.99 // higher
            } else if time == 360 {
                signals = [.musicTransition]
                confidence = 0.80 // lower
            } else if time == 300 {
                signals = [.lexicalCategoryJump] // disjoint from both
                confidence = 0.10 // weakest
            } else {
                signals = [.musicTransition]
                confidence = 0.99
            }
            cands.append(
                ChapterCandidate(
                    startTime: time,
                    boundaryConfidence: confidence,
                    triggeringSignals: signals
                )
            )
        }
        cands.append(
            ChapterCandidate(
                startTime: 780,
                boundaryConfidence: 0.99,
                triggeringSignals: [.musicTransition]
            )
        )
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        guard case let .capApplied(_, _, _, mergeRecords) = result.outcome else {
            Issue.record("expected capApplied outcome, got \(result.outcome)")
            return
        }
        #expect(mergeRecords.count == 1)
        let pick = mergeRecords[0]
        #expect(pick.droppedStartTime == 300)
        #expect(pick.absorbedIntoStartTime == 240,
                "equal signal-overlap → higher-confidence neighbor (240, 0.99) wins over (360, 0.80)")
        #expect(pick.signalOverlap == 0.0)
    }

    @Test("merge records carry partial Jaccard overlap when neighbor sets overlap partially")
    func mergePartialOverlapRecorded() {
        // Build a dense set where the dropped boundary has a HALF
        // overlap with one neighbor and ZERO with the other — the
        // merge record should adopt the higher-overlap neighbor and
        // the recorded `signalOverlap` should be 0.5 (not 0 or 1).
        // Episode 60 min → cap 12. Build 13 non-zero so exactly 1
        // drop happens.
        let dur: TimeInterval = 3600
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        let strongConfidence: Float = 0.99
        for index in 0..<12 {
            let time = TimeInterval(60 + index * 60)
            let signals: [BoundarySignal]
            let confidence: Float
            if time == 240 {
                // Prev neighbor: shares ONE signal with dropped t=300.
                // Jaccard(t=300, t=240) = |{music}| / |{music,speaker,lex}|
                //                       = 1 / 3 ≈ 0.333.
                signals = [.musicTransition]
                confidence = strongConfidence
            } else if time == 360 {
                // Next neighbor: disjoint from t=300's signal set.
                signals = [.longPause]
                confidence = strongConfidence
            } else if time == 300 {
                signals = [.musicTransition, .speakerShift, .lexicalCategoryJump]
                confidence = 0.10 // weakest → dropped
            } else {
                signals = [.musicTransition]
                confidence = strongConfidence
            }
            cands.append(
                ChapterCandidate(
                    startTime: time,
                    boundaryConfidence: confidence,
                    triggeringSignals: signals
                )
            )
        }
        cands.append(
            ChapterCandidate(
                startTime: 780,
                boundaryConfidence: strongConfidence,
                triggeringSignals: [.musicTransition]
            )
        )
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        guard case let .capApplied(_, _, _, mergeRecords) = result.outcome else {
            Issue.record("expected capApplied outcome, got \(result.outcome)")
            return
        }
        #expect(mergeRecords.count == 1)
        let pick = mergeRecords[0]
        #expect(pick.droppedStartTime == 300)
        #expect(pick.absorbedIntoStartTime == 240,
                "prev neighbor (t=240) shares 1/3 of dropped signals; should be picked over disjoint next")
        // Jaccard for {music,speaker,lex} ∩ {music} = 1; ∪ = 3 → 1/3.
        #expect(abs(pick.signalOverlap - (1.0 / 3.0)) < 1e-9,
                "partial overlap should record exact Jaccard, not be rounded to 0 or 1")
    }

    @Test("merge falls back to time-distance when sim and confidence are equal")
    func mergeFallsBackOnTimeDistance() {
        // Both neighbors at equal sim (0) AND equal confidence. The
        // dropped boundary is closer in time to `next` than to `prev`,
        // so the merge should pick `next`.
        let dur: TimeInterval = 3600
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        // Place the dropped at t=350; prev (kept) at t=240, next (kept)
        // at t=360. Distance: 110 vs 10 → next wins on time-closer.
        for index in 0..<12 {
            let time = TimeInterval(60 + index * 60)
            let signals: [BoundarySignal]
            let confidence: Float
            if time == 240 || time == 360 {
                signals = [.musicTransition]
                confidence = 0.99 // equal between the two neighbors
            } else {
                signals = [.musicTransition]
                confidence = 0.99
            }
            cands.append(
                ChapterCandidate(
                    startTime: time,
                    boundaryConfidence: confidence,
                    triggeringSignals: signals
                )
            )
        }
        // Replace t=300 with t=350 (asymmetric position) and lower
        // its confidence so it's the one dropped.
        cands.removeAll { $0.startTime == 300 }
        cands.append(
            ChapterCandidate(
                startTime: 350,
                boundaryConfidence: 0.10,
                triggeringSignals: [.lexicalCategoryJump] // disjoint from neighbors
            )
        )
        // Add a 13th non-zero so the cap of 12 forces exactly the
        // weakest drop.
        cands.append(
            ChapterCandidate(
                startTime: 780,
                boundaryConfidence: 0.99,
                triggeringSignals: [.musicTransition]
            )
        )
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        guard case let .capApplied(_, _, _, mergeRecords) = result.outcome else {
            Issue.record("expected capApplied outcome, got \(result.outcome)")
            return
        }
        #expect(mergeRecords.count == 1)
        let pick = mergeRecords[0]
        #expect(pick.droppedStartTime == 350)
        #expect(pick.absorbedIntoStartTime == 360,
                "equal sim + equal confidence → time-closer neighbor (360, dist 10) wins over (240, dist 110)")
    }

    @Test("cap-and-merge survivors are sorted by startTime ascending")
    func survivorsSortedByStartTime() {
        // Construct candidates in a non-time-sorted confidence order so
        // the cap step's confidence sort can't accidentally produce
        // the right startTime order.
        let dur: TimeInterval = 3600
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        // Reverse-sorted by time, varying confidences.
        cands.append(ChapterCandidate(startTime: 600, boundaryConfidence: 0.8, triggeringSignals: [.musicTransition]))
        cands.append(ChapterCandidate(startTime: 480, boundaryConfidence: 0.9, triggeringSignals: [.musicTransition]))
        cands.append(ChapterCandidate(startTime: 360, boundaryConfidence: 0.7, triggeringSignals: [.musicTransition]))
        cands.append(ChapterCandidate(startTime: 240, boundaryConfidence: 0.6, triggeringSignals: [.musicTransition]))
        // Add 10 more strong ones to overflow the cap of 12.
        for index in 0..<10 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(700 + index * 60),
                    boundaryConfidence: 0.99,
                    triggeringSignals: [.musicTransition]
                )
            )
        }
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        let times = result.candidates.map(\.startTime)
        #expect(times == times.sorted(),
                "survivors must be sorted by startTime ascending")
        #expect(result.candidates.first?.startTime == 0,
                "synthetic t=0 must be at head")
    }
}

@Suite("ChapterBoundaryDetector / density gates / short-episode skip")
struct ChapterBoundaryDetectorShortEpisodeSkipTests {

    @Test("episode <5 min skips both gates byte-for-byte")
    func shortEpisodeSkipsBothGates() {
        // 4-min episode with WAY too many candidates. Both gates
        // should skip; the candidate list passes through.
        let dur: TimeInterval = 240 // 4 min
        var cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: [])
        ]
        // 50 candidates on a 240s episode — rate = 51/240 = 0.21 per
        // second, which is WAY above 1/90 = 0.011. If the gate
        // ran, this would abort. Spec says short-episode skips
        // BOTH, so nothing aborts.
        for index in 0..<50 {
            cands.append(
                ChapterCandidate(
                    startTime: TimeInterval(2 + index * 4),
                    boundaryConfidence: 0.9,
                    triggeringSignals: [.musicTransition]
                )
            )
        }
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        #expect(result.candidates == cands, "short-episode skip must pass candidates through unchanged")
        if case .skippedShortEpisode = result.outcome {
            // ok
        } else {
            Issue.record("expected skippedShortEpisode outcome, got \(result.outcome)")
        }
    }

    @Test("episode just under 5 min skips both gates")
    func almostFiveMinSkipsBothGates() {
        let dur: TimeInterval = 5 * 60 - 0.001 // strictly <5min
        let cands = DensityFixtures.candidates(
            episodeDuration: dur,
            nonZeroCount: 30
        )
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        if case .skippedShortEpisode = result.outcome {
            // ok
        } else {
            Issue.record("expected skippedShortEpisode at duration just under threshold, got \(result.outcome)")
        }
    }

    @Test("episode at exactly 5 min runs the gates")
    func exactlyFiveMinRunsGates() {
        // At exactly 5 min the short-episode carve-out does NOT
        // apply (the threshold is `>= 5 min runs gates`). Build a
        // candidate set dense enough that cap-and-merge must fire,
        // but rate ≤ 1/90 to avoid pathological abort.
        let dur: TimeInterval = 5 * 60
        // Cap = ceil(5/5) = 1. Floor doesn't apply (<40min). Detected
        // = 3 (1 t=0 + 2 non-zero); nonZeroCap = 1 → 1 non-zero kept
        // + t=0 = 2 retained. Pathological: rate 3/300 = 0.01 < 1/90
        // ≈ 0.011 → safe.
        // Higher-confidence non-zero (t=60, conf 0.7) wins over
        // t=200 (conf 0.5).
        let cands: [ChapterCandidate] = [
            ChapterCandidate(startTime: 0, boundaryConfidence: 1.0, triggeringSignals: []),
            ChapterCandidate(startTime: 60, boundaryConfidence: 0.7, triggeringSignals: [.musicTransition]),
            ChapterCandidate(startTime: 200, boundaryConfidence: 0.5, triggeringSignals: [.musicTransition]),
        ]
        let result = ChapterBoundaryDetector.applyDensityGates(
            candidates: cands,
            episodeDuration: dur
        )
        if case .skippedShortEpisode = result.outcome {
            Issue.record("episode at exactly 5min must NOT skip gates; gates run from threshold up")
            return
        }
        guard case let .capApplied(_, retained, _, mergeRecords) = result.outcome else {
            Issue.record("expected capApplied outcome, got \(result.outcome)")
            return
        }
        #expect(retained == 2, "cap=1 non-zero + synthetic t=0 = 2 retained")
        #expect(result.candidates.count == 2)
        #expect(result.candidates.map(\.startTime) == [0, 60],
                "higher-confidence non-zero (t=60) wins over t=200")
        #expect(mergeRecords.count == 1)
        #expect(mergeRecords[0].droppedStartTime == 200)
    }
}

@Suite("ChapterBoundaryDetector / density gates / detectRefined integration")
struct ChapterBoundaryDetectorDetectRefinedTests {

    @Test("detectRefined yields the same candidate set as detect for a sparse episode")
    func detectRefinedSparsePassthrough() {
        let detector = ChapterBoundaryDetector()
        // 60-min episode with light signal density: ~3 boundaries.
        let music = [
            ChapterMusicWindow(startTime: 100, endTime: 102, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 102, endTime: 104, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 1700, endTime: 1702, musicProbability: 0.95),
            ChapterMusicWindow(startTime: 1702, endTime: 1704, musicProbability: 0.05),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: 3600,
            musicWindows: music
        )
        let raw = detector.detect(features: snapshot)
        let refined = detector.detectRefined(features: snapshot)
        #expect(refined.candidates == raw,
                "sparse episode: detectRefined output equals detect output")
        if case .noChange = refined.outcome {
            // ok
        } else {
            Issue.record("expected noChange outcome, got \(refined.outcome)")
        }
    }

    @Test("detectRefined on a short episode skips both gates")
    func detectRefinedShortEpisodeSkips() {
        let detector = ChapterBoundaryDetector()
        let dur: TimeInterval = 240 // 4 min
        let music = [
            ChapterMusicWindow(startTime: 0, endTime: 2, musicProbability: 0.05),
            ChapterMusicWindow(startTime: 2, endTime: 4, musicProbability: 0.95),
        ]
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            musicWindows: music
        )
        let refined = detector.detectRefined(features: snapshot)
        if case .skippedShortEpisode = refined.outcome {
            // ok
        } else {
            Issue.record("expected skippedShortEpisode for 4min episode, got \(refined.outcome)")
        }
    }

    @Test("detectRefined fires the cap-and-merge gate when detect produces too many boundaries")
    func detectRefinedCapApplied() {
        // Build a 30-min snapshot with many lexical hits at distinct
        // 60s spacings — each should trigger a boundary, producing
        // ~30 raw candidates. Cap for 30min = ceil(30/5) = 6 (no
        // floor, <40min). So cap-and-merge fires and we expect
        // retainedCount ≤ 7 (6 non-zero + t=0).
        let detector = ChapterBoundaryDetector()
        let dur: TimeInterval = 30 * 60
        var hits: [ChapterLexicalHit] = []
        let categories: [LexicalPatternCategory] = [
            .sponsor, .promoCode, .urlCTA, .purchaseLanguage, .transitionMarker
        ]
        // 25 hits spaced 60s apart; rate before cap = 26/1800 ≈ 0.014
        // > 1/90 = 0.011 → would trigger pathological. So use 18
        // hits spaced 90s apart: 19/1800 = 0.0106 < 1/90.
        for index in 0..<18 {
            hits.append(ChapterLexicalHit(
                startTime: TimeInterval(index + 1) * 90.0,
                category: categories[index % categories.count]
            ))
        }
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            lexicalHits: hits
        )
        let refined = detector.detectRefined(features: snapshot)
        guard case let .capApplied(detected, retained, _, mergeRecords) = refined.outcome else {
            Issue.record("expected capApplied outcome, got \(refined.outcome)")
            return
        }
        #expect(detected > retained,
                "cap-and-merge must drop at least one candidate")
        #expect(refined.candidates.count == retained,
                "candidate list size must match retainedCount")
        #expect(mergeRecords.count == detected - retained,
                "merge-record count == drops")
        #expect(refined.candidates.first?.startTime == 0,
                "synthetic t=0 must remain at head after cap-and-merge")
    }

    @Test("detectRefined fires the pathological-rate gate when detect emits too dense candidates")
    func detectRefinedPathologicalRate() {
        // Engineer the bare detector to emit > 1 candidate per 90s on
        // a 60-min show. Lexical hits at every 60s spacing produce
        // ~60 candidates (one per hit) on the bare detector — rate
        // 61/3600 = 0.017 > 1/90 = 0.011 → pathological gate fires.
        let detector = ChapterBoundaryDetector()
        let dur: TimeInterval = 60 * 60
        var hits: [ChapterLexicalHit] = []
        let categories: [LexicalPatternCategory] = [
            .sponsor, .promoCode, .urlCTA, .purchaseLanguage, .transitionMarker
        ]
        // 60 hits at 60s spacing.
        for index in 0..<60 {
            hits.append(ChapterLexicalHit(
                startTime: TimeInterval(index + 1) * 60.0,
                category: categories[index % categories.count]
            ))
        }
        let snapshot = ChapterFeatureSnapshot(
            episodeDuration: dur,
            lexicalHits: hits
        )
        let refined = detector.detectRefined(features: snapshot)
        // The bare detector may consolidate adjacent hits within a
        // ~12s merge window, so the actual emitted count will be
        // smaller than `hits.count`. Skip if not above threshold —
        // the test fixture engineering relies on the consolidation
        // not eating the threshold; surface a clear message if not.
        let bareCount = detector.detect(features: snapshot).count
        guard Double(bareCount) / dur > 1.0 / 90.0 else {
            Issue.record("test fixture failed to engineer > 1/90s density (bare detect emitted \(bareCount) on \(dur)s)")
            return
        }
        guard case let .pathologicalRate(detected, episodeDur, perSec) = refined.outcome else {
            Issue.record("expected pathologicalRate outcome, got \(refined.outcome)")
            return
        }
        #expect(refined.candidates.isEmpty,
                "pathological-rate abort must return empty candidate list")
        #expect(detected == bareCount)
        #expect(episodeDur == dur)
        #expect(perSec > 1.0 / 90.0)
    }
}
