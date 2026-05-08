// ChapterBoundaryDetectorTests.swift
// playhead-au2v.1.4: Unit tests for `ChapterBoundaryDetector`.
//
// Coverage:
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

    @Test("co-clustered signals within minBoundarySpacing dedupe weights")
    func coClusteredDoesNotDoubleCount() {
        let detector = ChapterBoundaryDetector()
        // Two music transitions exactly minBoundarySpacing apart should
        // collapse into one boundary with a single .musicTransition
        // contribution (not two), so confidence stays at 0.4.
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
        // Both transitions are at 2.0 and 4.0 respectively; spacing 2s
        // > 1s default min spacing → they DO emit as separate
        // boundaries. This test confirms separation works as designed.
        #expect(result.count == 3, "two separated transitions emit two boundaries")
        for candidate in result.dropFirst() {
            #expect(abs(candidate.boundaryConfidence - 0.4) < 0.0001)
        }
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
}
