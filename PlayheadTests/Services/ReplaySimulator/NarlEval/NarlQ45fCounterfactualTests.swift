// NarlQ45fCounterfactualTests.swift
// playhead-q45f.3: Tests for the per-episode + per-show carryforward
// counterfactual value types that wrap `Q45fReplayGate.replay` for the
// NARL eval harness.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlQ45fCounterfactual")
struct NarlQ45fCounterfactualTests {

    // MARK: - per-episode compute(trace:)

    @Test("trace with no listenRewindEvents produces .empty counterfactual")
    func emptyTraceProducesEmpty() {
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: []
        )
        let cf = NarlQ45fCounterfactual.compute(trace: trace)
        #expect(cf == NarlQ45fCounterfactual.empty)
        #expect(cf.wouldDemote == false)
        #expect(cf.demotionTime == nil)
        #expect(cf.finalMode == SkipMode.auto.rawValue)
        #expect(cf.demotionsCount == 0)
        #expect(cf.rewindEventCount == 0)
    }

    @Test("single rewind event does not demote (need 2 to flip auto→manual)")
    func singleEventNoDemote() {
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: [
                Self.makeRewind(time: 10, windowId: "w1", podcastId: "pc-A")
            ]
        )
        let cf = NarlQ45fCounterfactual.compute(trace: trace)
        #expect(cf.wouldDemote == false)
        #expect(cf.demotionTime == nil)
        #expect(cf.finalMode == SkipMode.auto.rawValue)
        #expect(cf.demotionsCount == 0)
        #expect(cf.rewindEventCount == 1)
    }

    @Test("two rewinds flip auto → manual at event-2 time")
    func twoEventsDemoteToManual() {
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: [
                Self.makeRewind(time: 10, windowId: "w1", podcastId: "pc-A"),
                Self.makeRewind(time: 25, windowId: "w2", podcastId: "pc-A"),
            ]
        )
        let cf = NarlQ45fCounterfactual.compute(trace: trace)
        #expect(cf.wouldDemote == true)
        #expect(cf.demotionTime == 25)
        #expect(cf.finalMode == SkipMode.manual.rawValue)
        #expect(cf.demotionsCount == 1)
        #expect(cf.rewindEventCount == 2)
    }

    @Test("four rewinds flip auto → manual → shadow; demotionTime = first flip")
    func fourEventsDemoteToShadow() {
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: [
                Self.makeRewind(time: 10, windowId: "w1", podcastId: "pc-A"),
                Self.makeRewind(time: 25, windowId: "w2", podcastId: "pc-A"),
                Self.makeRewind(time: 40, windowId: "w3", podcastId: "pc-A"),
                Self.makeRewind(time: 60, windowId: "w4", podcastId: "pc-A"),
            ]
        )
        let cf = NarlQ45fCounterfactual.compute(trace: trace)
        #expect(cf.wouldDemote == true)
        #expect(cf.demotionTime == 25, "demotionTime should be the FIRST flip time")
        #expect(cf.finalMode == SkipMode.shadow.rawValue)
        #expect(cf.demotionsCount == 2)
        #expect(cf.rewindEventCount == 4)
    }

    @Test("cross-podcast rewinds are filtered to trace.podcastId before replay")
    func crossPodcastEventsFiltered() {
        // Two events on the trace's own podcast (would demote alone), plus
        // two events on a different podcast that must NOT count toward the
        // gate's running false-signal counter — those belong to a different
        // show's carryforward and the per-episode gate is single-podcast by
        // contract.
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: [
                Self.makeRewind(time: 10, windowId: "w1", podcastId: "pc-A"),
                Self.makeRewind(time: 12, windowId: "w-other", podcastId: "pc-B"),
                Self.makeRewind(time: 25, windowId: "w2", podcastId: "pc-A"),
                Self.makeRewind(time: 30, windowId: "w-other2", podcastId: "pc-B"),
            ]
        )
        let cf = NarlQ45fCounterfactual.compute(trace: trace)
        // Only the two pc-A events count: 2 events → 1 demotion at t=25.
        #expect(cf.wouldDemote == true)
        #expect(cf.demotionTime == 25)
        #expect(cf.finalMode == SkipMode.manual.rawValue)
        #expect(cf.demotionsCount == 1)
        #expect(cf.rewindEventCount == 2,
                "rewindEventCount should reflect events actually replayed (post-filter), not raw input")
    }

    // MARK: - codable round-trip

    @Test("counterfactual round-trips through JSON")
    func counterfactualCodableRoundTrip() throws {
        let original = NarlQ45fCounterfactual(
            wouldDemote: true,
            demotionTime: 25.5,
            finalMode: SkipMode.manual.rawValue,
            demotionsCount: 1,
            rewindEventCount: 3
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(NarlQ45fCounterfactual.self, from: data)
        #expect(decoded == original)
    }

    @Test("counterfactual decodes nil demotionTime when absent")
    func counterfactualDecodesNilDemotionTime() throws {
        let json = """
        {"wouldDemote":false,"demotionTime":null,"finalMode":"auto","demotionsCount":0,"rewindEventCount":0}
        """
        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(NarlQ45fCounterfactual.self, from: data)
        #expect(decoded == NarlQ45fCounterfactual.empty)
    }

    // MARK: - per-show carryforward compute(showEpisodes:)

    @Test("empty showEpisodes list produces .empty carryforward")
    func emptyShowProducesEmpty() {
        let cf = NarlQ45fCarryforwardRollup.compute(showEpisodes: [])
        #expect(cf == NarlQ45fCarryforwardRollup.empty)
        #expect(cf.episodeCount == 0)
        #expect(cf.totalRewindEventCount == 0)
        #expect(cf.totalDemotionsCount == 0)
        #expect(cf.firstDemotionEpisodeId == nil)
        #expect(cf.finalMode == SkipMode.auto.rawValue)
    }

    @Test("carryforward sorts episodes by capturedAt before threading state")
    func carryforwardSortsByCapturedAt() {
        // Episode "later" has 1 event; episode "earlier" has 1 event. If
        // processed in input order (later → earlier), the later episode's
        // single event lands first (signals=1, no demote) and the earlier
        // episode's single event makes signals=2 → demote at "earlier".
        // If processed in chronological order (earlier → later), demote
        // lands at "later". The contract is chronological; we expect the
        // chronological answer.
        let earlier = Self.makeTrace(
            episodeId: "ep-earlier",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100),
            listenRewindEvents: [
                Self.makeRewind(time: 5, windowId: "w-e", podcastId: "pc-A")
            ]
        )
        let later = Self.makeTrace(
            episodeId: "ep-later",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 9000),
            listenRewindEvents: [
                Self.makeRewind(time: 7, windowId: "w-l", podcastId: "pc-A")
            ]
        )
        // Input intentionally out-of-order.
        let cf = NarlQ45fCarryforwardRollup.compute(showEpisodes: [later, earlier])
        #expect(cf.episodeCount == 2)
        #expect(cf.totalRewindEventCount == 2)
        #expect(cf.totalDemotionsCount == 1)
        #expect(cf.firstDemotionEpisodeId == "ep-later",
                "Chronological replay puts ep-earlier first; demotion happens on ep-later's event")
        #expect(cf.finalMode == SkipMode.manual.rawValue)
    }

    @Test("carryforward state persists across episodes (no reset between episodes)")
    func carryforwardStatePersistsAcrossEpisodes() {
        // Episode A (earlier) has 2 events → demote to manual within A.
        // Episode B (later) has 2 more events → demote manual → shadow
        // within B because signals carries forward (3, 4 → ≥4 demotes).
        let epA = Self.makeTrace(
            episodeId: "ep-A",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100),
            listenRewindEvents: [
                Self.makeRewind(time: 10, windowId: "w1", podcastId: "pc-A"),
                Self.makeRewind(time: 20, windowId: "w2", podcastId: "pc-A"),
            ]
        )
        let epB = Self.makeTrace(
            episodeId: "ep-B",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 200),
            listenRewindEvents: [
                Self.makeRewind(time: 5, windowId: "w3", podcastId: "pc-A"),
                Self.makeRewind(time: 8, windowId: "w4", podcastId: "pc-A"),
            ]
        )
        let cf = NarlQ45fCarryforwardRollup.compute(showEpisodes: [epA, epB])
        #expect(cf.episodeCount == 2)
        #expect(cf.totalRewindEventCount == 4)
        #expect(cf.totalDemotionsCount == 2)
        #expect(cf.firstDemotionEpisodeId == "ep-A")
        #expect(cf.finalMode == SkipMode.shadow.rawValue)
    }

    @Test("carryforward skips episodes with zero rewind events but still counts them")
    func carryforwardSkipsEmptyEpisodes() {
        let epEmpty = Self.makeTrace(
            episodeId: "ep-empty",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100),
            listenRewindEvents: []
        )
        let epWithEvents = Self.makeTrace(
            episodeId: "ep-withEvents",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 200),
            listenRewindEvents: [
                Self.makeRewind(time: 10, windowId: "w1", podcastId: "pc-A"),
                Self.makeRewind(time: 20, windowId: "w2", podcastId: "pc-A"),
            ]
        )
        let cf = NarlQ45fCarryforwardRollup.compute(showEpisodes: [epEmpty, epWithEvents])
        #expect(cf.episodeCount == 2, "episodeCount counts every input episode, including zero-event ones")
        #expect(cf.totalRewindEventCount == 2)
        #expect(cf.totalDemotionsCount == 1)
        #expect(cf.firstDemotionEpisodeId == "ep-withEvents")
        #expect(cf.finalMode == SkipMode.manual.rawValue)
    }

    @Test("carryforward filters cross-podcast rewinds per-episode")
    func carryforwardFiltersCrossPodcast() {
        // Each episode has its own podcast. Within each, foreign-podcast
        // rewinds must be dropped before replay (gate precondition would
        // crash on mixed input). One own-podcast event per episode → no
        // demote (signals climbs 1 then 2 across episodes, demoting on
        // ep-2's event).
        let epA = Self.makeTrace(
            episodeId: "ep-A",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100),
            listenRewindEvents: [
                Self.makeRewind(time: 5, windowId: "wA", podcastId: "pc-A"),
                Self.makeRewind(time: 6, windowId: "wOther", podcastId: "pc-X"),
            ]
        )
        let epB = Self.makeTrace(
            episodeId: "ep-B",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 200),
            listenRewindEvents: [
                Self.makeRewind(time: 5, windowId: "wB", podcastId: "pc-A"),
                Self.makeRewind(time: 6, windowId: "wOther", podcastId: "pc-X"),
            ]
        )
        let cf = NarlQ45fCarryforwardRollup.compute(showEpisodes: [epA, epB])
        #expect(cf.totalRewindEventCount == 2,
                "totalRewindEventCount should count only events that were actually replayed (post-filter)")
        #expect(cf.totalDemotionsCount == 1)
        #expect(cf.firstDemotionEpisodeId == "ep-B")
        #expect(cf.finalMode == SkipMode.manual.rawValue)
    }

    @Test("carryforward round-trips through JSON")
    func carryforwardCodableRoundTrip() throws {
        let original = NarlQ45fCarryforwardRollup(
            finalMode: SkipMode.shadow.rawValue,
            totalDemotionsCount: 2,
            totalRewindEventCount: 5,
            firstDemotionEpisodeId: "ep-A",
            episodeCount: 3
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(NarlQ45fCarryforwardRollup.self, from: data)
        #expect(decoded == original)
    }

    // MARK: - helpers

    private static func makeRewind(
        time: Double,
        windowId: String,
        podcastId: String
    ) -> FrozenTrace.FrozenListenRewindEvent {
        FrozenTrace.FrozenListenRewindEvent(
            time: time,
            windowId: windowId,
            podcastId: podcastId
        )
    }

    private static func makeTrace(
        episodeId: String,
        podcastId: String,
        capturedAt: Date,
        listenRewindEvents: [FrozenTrace.FrozenListenRewindEvent]
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: episodeId,
            podcastId: podcastId,
            episodeDuration: 1800,
            traceVersion: "frozen-trace-v3",
            capturedAt: capturedAt,
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training,
            listenRewindEvents: listenRewindEvents
        )
    }
}
