// NarlQ45fCounterfactualTests.swift
// playhead-q45f.3: Tests for the per-episode + per-podcast carryforward
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

    @Test("compute(trace:) sorts out-of-order events by time before replay")
    func computeTraceSortsByTime() {
        // Out-of-order input: t=25 first, t=10 second. The gate iterates
        // events in array order and stamps `DemotionEvent.time` from the
        // threshold-crossing event's own `time`:
        //   - WITHOUT sort: replay sees [t=25, t=10]; demotion fires on
        //     the second event (t=10) because that's when the running
        //     signal counter reaches 2. Result: demotionTime = 10.
        //   - WITH sort:    replay sees [t=10, t=25]; demotion fires on
        //     the second event (t=25). Result: demotionTime = 25.
        // The contract says replay must be chronological, so demotionTime
        // must be 25. This test fails on a missing/broken sort.
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: [
                Self.makeRewind(time: 25, windowId: "w2", podcastId: "pc-A"),
                Self.makeRewind(time: 10, windowId: "w1", podcastId: "pc-A"),
            ]
        )
        let cf = NarlQ45fCounterfactual.compute(trace: trace)
        #expect(cf.wouldDemote == true)
        #expect(cf.demotionTime == 25,
                "After chronological sort, the second flip lands at t=25 (the chronologically later event)")
        #expect(cf.demotionsCount == 1)
        #expect(cf.rewindEventCount == 2)
    }

    @Test("cross-podcast rewinds are filtered to trace.podcastId before replay")
    func crossPodcastEventsFiltered() {
        // Two events on the trace's own podcast (would demote alone), plus
        // two events on a different podcast that must NOT count toward the
        // gate's running false-signal counter — those belong to a different
        // podcast's carryforward and the per-episode gate is single-podcast
        // by contract.
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
        #expect(cf.crossPodcastDroppedCount == 2,
                "crossPodcastDroppedCount must surface the dropped pc-B events on the report (M-2)")
    }

    @Test("compute(trace:) surfaces all-foreign drops as a non-.empty result with crossPodcastDroppedCount > 0")
    func computeTraceAllForeignSurfacesDropCount() {
        // Cycle-4 M-2: `.empty` is ambiguous between "no rewinds at all"
        // and "every rewind was foreign and got dropped". The fix surfaces
        // the dropped count on a non-`.empty` return value so a downstream
        // `jq` reader can distinguish the two.
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: [
                Self.makeRewind(time: 5, windowId: "wX1", podcastId: "pc-X"),
                Self.makeRewind(time: 6, windowId: "wX2", podcastId: "pc-X"),
                Self.makeRewind(time: 7, windowId: "wX3", podcastId: "pc-X"),
            ]
        )
        let cf = NarlQ45fCounterfactual.compute(trace: trace)
        #expect(cf != .empty,
                "an all-foreign trace must NOT collapse to .empty (it would be confused with a no-rewind trace)")
        #expect(cf.wouldDemote == false)
        #expect(cf.demotionTime == nil)
        #expect(cf.finalMode == SkipMode.auto.rawValue)
        #expect(cf.demotionsCount == 0)
        #expect(cf.rewindEventCount == 0,
                "no events were actually replayed — the rewindEventCount must be 0")
        #expect(cf.crossPodcastDroppedCount == 3,
                "all 3 foreign events must be reflected in the dropped count (M-2 corpus-bug telemetry)")
    }

    @Test("compute(trace:) returns truly-.empty for genuinely-empty traces")
    func computeTraceGenuinelyEmptyReturnsEmpty() {
        // Conjugate of computeTraceAllForeignSurfacesDropCount: a trace
        // with no rewinds at all must still return exactly `.empty`. The
        // ambiguity-fix from M-2 must NOT regress the no-rewind path.
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: []
        )
        let cf = NarlQ45fCounterfactual.compute(trace: trace)
        #expect(cf == .empty)
        #expect(cf.crossPodcastDroppedCount == 0)
    }

    @Test("compute(trace:) is deterministic across input shuffles when events tie on time")
    func computeTraceTieBreakDeterministic() {
        // Cycle-4 M-3: Swift's stdlib `sorted(by:)` is unstable. Without
        // a secondary key on the event sort, two events with the same
        // `time` could swap places between runs.
        //
        // **Caveat (cycle-5 L-A acknowledgement):** the value-type
        // assertion `cf1 == cf2` here is necessarily TAUTOLOGICAL for
        // pure same-`time` ties — the gate's math is order-independent
        // within a `time` tie group (each tied event contributes the
        // same penalty + signal increment, so demotion time stamped on
        // the tie group is identical regardless of within-tie order),
        // and `NarlQ45fCounterfactual` carries no `windowId` field that
        // could leak the ordering. The point of this test is *belt-and-
        // suspenders* coverage of the secondary sort key alongside the
        // carryforward variant `carryforwardCapturedAtTieBreakDeterministic`,
        // which IS observably non-tautological because
        // `firstDemotionEpisodeId` flips with order. Together they pin
        // the (time, windowId) sort against `sorted(by:)` instability;
        // the per-event variant guards against a future change that
        // adds a windowId-dependent field to the value type.
        let evA = Self.makeRewind(time: 10, windowId: "windowA", podcastId: "pc-A")
        let evB = Self.makeRewind(time: 10, windowId: "windowB", podcastId: "pc-A")
        let trace1 = Self.makeTrace(
            episodeId: "ep-1", podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: [evA, evB]
        )
        let trace2 = Self.makeTrace(
            episodeId: "ep-1", podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 1000),
            listenRewindEvents: [evB, evA]
        )
        let cf1 = NarlQ45fCounterfactual.compute(trace: trace1)
        let cf2 = NarlQ45fCounterfactual.compute(trace: trace2)
        #expect(cf1 == cf2,
                "Counterfactual must be invariant under input shuffle when events tie on time")
    }

    // MARK: - codable round-trip

    @Test("counterfactual round-trips through JSON")
    func counterfactualCodableRoundTrip() throws {
        let original = NarlQ45fCounterfactual(
            wouldDemote: true,
            demotionTime: 25.5,
            finalMode: SkipMode.manual.rawValue,
            demotionsCount: 1,
            rewindEventCount: 3,
            crossPodcastDroppedCount: 2
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(NarlQ45fCounterfactual.self, from: data)
        #expect(decoded == original)
        #expect(decoded.crossPodcastDroppedCount == 2,
                "Cycle-4: crossPodcastDroppedCount must round-trip")
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

    @Test("counterfactual decodes pre-cycle-4 artifact (missing crossPodcastDroppedCount) as 0")
    func counterfactualDecodesPreCycle4WithZeroDropCount() throws {
        // Cycle-4 codable back-compat: the new `crossPodcastDroppedCount`
        // field uses `decodeIfPresent ?? 0`. A pre-cycle-4 artifact will
        // not carry the key; it must decode to 0, not throw.
        let json = """
        {"wouldDemote":true,"demotionTime":12.5,"finalMode":"manual","demotionsCount":1,"rewindEventCount":2}
        """
        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(NarlQ45fCounterfactual.self, from: data)
        #expect(decoded.crossPodcastDroppedCount == 0,
                "pre-cycle-4 artifacts (no crossPodcastDroppedCount key) must decode the field as 0")
        #expect(decoded.wouldDemote == true)
        #expect(decoded.demotionTime == 12.5)
    }

    @Test("counterfactual decode RAISES on malformed crossPodcastDroppedCount")
    func counterfactualDecodeRaisesOnMalformedDropCount() throws {
        // Cycle-4 back-compat invariant: the new field uses
        // `decodeIfPresent` rather than `try?`, so a malformed payload
        // (string instead of int) must surface a `DecodingError` rather
        // than silently degrade to 0.
        let json = """
        {"wouldDemote":false,"demotionTime":null,"finalMode":"auto","demotionsCount":0,"rewindEventCount":0,"crossPodcastDroppedCount":"not-an-int"}
        """
        let data = Data(json.utf8)
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(NarlQ45fCounterfactual.self, from: data)
        }
    }

    // MARK: - per-podcast carryforward compute(podcastEpisodes:)

    @Test("empty podcastEpisodes list produces .empty carryforward")
    func emptyPodcastProducesEmpty() {
        let cf = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [])
        #expect(cf == NarlQ45fCarryforwardRollup.empty)
        #expect(cf.podcastId == "")
        #expect(cf.traceCount == 0)
        #expect(cf.totalRewindEventCount == 0)
        #expect(cf.totalDemotionsCount == 0)
        #expect(cf.firstDemotionEpisodeId == nil)
        #expect(cf.finalMode == SkipMode.auto.rawValue)
    }

    @Test("carryforward stamps the input traces' shared podcastId")
    func carryforwardStampsPodcastId() {
        let trace = Self.makeTrace(
            episodeId: "ep-1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100),
            listenRewindEvents: []
        )
        let cf = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [trace])
        #expect(cf.podcastId == "pc-A")
        #expect(cf.traceCount == 1)
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
        let cf = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [later, earlier])
        #expect(cf.podcastId == "pc-A")
        #expect(cf.traceCount == 2)
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
        let cf = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [epA, epB])
        #expect(cf.podcastId == "pc-A")
        #expect(cf.traceCount == 2)
        #expect(cf.totalRewindEventCount == 4)
        #expect(cf.totalDemotionsCount == 2)
        #expect(cf.firstDemotionEpisodeId == "ep-A")
        #expect(cf.finalMode == SkipMode.shadow.rawValue)
    }

    @Test("carryforward counts every input trace, including zero-event ones")
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
        let cf = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [epEmpty, epWithEvents])
        #expect(cf.traceCount == 2, "traceCount counts every input trace, including zero-event ones")
        #expect(cf.totalRewindEventCount == 2)
        #expect(cf.totalDemotionsCount == 1)
        #expect(cf.firstDemotionEpisodeId == "ep-withEvents")
        #expect(cf.finalMode == SkipMode.manual.rawValue)
    }

    @Test("carryforward filters cross-podcast EVENTS within a single-podcast trace")
    func carryforwardFiltersCrossPodcastEvents() {
        // Each trace has podcastId pc-A (so the per-trace precondition
        // holds), but the events on each trace include some pc-X events
        // (a corrupt fixture). Those event-level foreign rows must be
        // filtered out before replay — the gate's per-event precondition
        // would otherwise trap on mixed input.
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
        let cf = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [epA, epB])
        #expect(cf.totalRewindEventCount == 2,
                "totalRewindEventCount should count only events that were actually replayed (post-filter)")
        #expect(cf.totalDemotionsCount == 1)
        #expect(cf.firstDemotionEpisodeId == "ep-B")
        #expect(cf.finalMode == SkipMode.manual.rawValue)
        #expect(cf.totalCrossPodcastDroppedCount == 2,
                "Cycle-4 M-2: cross-podcast drops must aggregate across the carryforward (1 per trace × 2 traces = 2)")
    }

    @Test("carryforward surfaces all-foreign-across-all-traces drops, NOT collapsing to .empty")
    func carryforwardAllForeignAcrossAllTracesSurfacesDropCount() {
        // Cycle-5 L-D: the multi-trace mirror of the per-episode
        // `computeTraceAllForeignSurfacesDropCount`. If every trace in
        // a podcast carries only foreign events (corpus-wide capture
        // bug), `totalRewindEventCount` is 0 BUT
        // `totalCrossPodcastDroppedCount` must reflect the dropped
        // count and the rollup must NOT collapse to `.empty` — that
        // would lose the corpus alarm. Guards against a future
        // optimization that short-circuits when no events would be
        // replayed.
        let traceA = Self.makeTrace(
            episodeId: "ep-A",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100),
            listenRewindEvents: [
                Self.makeRewind(time: 5, windowId: "wA1", podcastId: "pc-X"),
                Self.makeRewind(time: 6, windowId: "wA2", podcastId: "pc-X"),
            ]
        )
        let traceB = Self.makeTrace(
            episodeId: "ep-B",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 200),
            listenRewindEvents: [
                Self.makeRewind(time: 7, windowId: "wB1", podcastId: "pc-X"),
            ]
        )
        let cf = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [traceA, traceB])
        #expect(cf != .empty,
                "all-foreign across all traces must NOT collapse to .empty (corpus alarm would be lost)")
        #expect(cf.podcastId == "pc-A",
                "rollup is stamped with the trace-level podcastId, not the foreign event podcastId")
        #expect(cf.totalRewindEventCount == 0,
                "no events were actually replayed (all foreign)")
        #expect(cf.totalDemotionsCount == 0)
        #expect(cf.firstDemotionEpisodeId == nil)
        #expect(cf.totalCrossPodcastDroppedCount == 3,
                "all 3 foreign events across both traces must be reflected in the aggregate dropped count")
        #expect(cf.traceCount == 2,
                "every input trace counts toward traceCount, even if its events all dropped")
    }

    @Test("carryforward is invariant under input shuffle when traces tie on capturedAt")
    func carryforwardCapturedAtTieBreakDeterministic() {
        // Cycle-4 M-3 (carryforward): swap order of two traces with the
        // same `capturedAt` timestamp (a real fixture pattern: batch
        // imports stamp identical times) and the rollup must come back
        // identical. The fix sorts by `(capturedAt, episodeId)`.
        let traceA = Self.makeTrace(
            episodeId: "ep-A",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100),
            listenRewindEvents: [
                Self.makeRewind(time: 10, windowId: "wA1", podcastId: "pc-A"),
                Self.makeRewind(time: 20, windowId: "wA2", podcastId: "pc-A"),
            ]
        )
        let traceB = Self.makeTrace(
            episodeId: "ep-B",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100), // tied
            listenRewindEvents: [
                Self.makeRewind(time: 5, windowId: "wB1", podcastId: "pc-A"),
                Self.makeRewind(time: 8, windowId: "wB2", podcastId: "pc-A"),
            ]
        )
        let cfAB = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [traceA, traceB])
        let cfBA = NarlQ45fCarryforwardRollup.compute(podcastEpisodes: [traceB, traceA])
        #expect(cfAB == cfBA,
                "Carryforward must be invariant under input shuffle when traces tie on capturedAt")
        // And the chronological tie-break must put ep-A first (alpha order),
        // so demotion lands within ep-A.
        #expect(cfAB.firstDemotionEpisodeId == "ep-A",
                "On capturedAt ties, episodeId is the secondary key; ep-A < ep-B alpha")
    }

    // MARK: - per-podcast convenience computePerPodcast(showEpisodes:)

    @Test("computePerPodcast emits one rollup per distinct podcastId, sorted by podcastId")
    func computePerPodcastEmitsOnePerPodcastId() {
        // A "show" with two podcastIds (the DoaC scenario: legacy form +
        // URL form). Each podcastId's trust state is independent — false
        // signals from pc-A must not bleed into pc-B's carryforward.
        let pcA1 = Self.makeTrace(
            episodeId: "ep-A1",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 100),
            listenRewindEvents: [
                Self.makeRewind(time: 5, windowId: "w1", podcastId: "pc-A"),
                Self.makeRewind(time: 10, windowId: "w2", podcastId: "pc-A"),
            ]
        )
        let pcA2 = Self.makeTrace(
            episodeId: "ep-A2",
            podcastId: "pc-A",
            capturedAt: Date(timeIntervalSince1970: 200),
            listenRewindEvents: []
        )
        let pcB1 = Self.makeTrace(
            episodeId: "ep-B1",
            podcastId: "pc-B",
            capturedAt: Date(timeIntervalSince1970: 150),
            listenRewindEvents: [
                Self.makeRewind(time: 7, windowId: "wB", podcastId: "pc-B")
            ]
        )
        let rollups = NarlQ45fCarryforwardRollup.computePerPodcast(
            showEpisodes: [pcB1, pcA1, pcA2]
        )
        #expect(rollups.count == 2)
        // Sorted by podcastId for stable rendering (pc-A before pc-B).
        #expect(rollups.map(\.podcastId) == ["pc-A", "pc-B"])
        // pc-A: two traces, two rewinds (on pcA1), demotion on ep-A1.
        let a = rollups[0]
        #expect(a.traceCount == 2)
        #expect(a.totalRewindEventCount == 2)
        #expect(a.totalDemotionsCount == 1)
        #expect(a.firstDemotionEpisodeId == "ep-A1")
        #expect(a.finalMode == SkipMode.manual.rawValue)
        // pc-B: one trace, one rewind, no demotion (need 2 to flip).
        let b = rollups[1]
        #expect(b.traceCount == 1)
        #expect(b.totalRewindEventCount == 1)
        #expect(b.totalDemotionsCount == 0)
        #expect(b.firstDemotionEpisodeId == nil)
        #expect(b.finalMode == SkipMode.auto.rawValue)
    }

    @Test("computePerPodcast on an empty show produces an empty list")
    func computePerPodcastEmptyShow() {
        let rollups = NarlQ45fCarryforwardRollup.computePerPodcast(showEpisodes: [])
        #expect(rollups.isEmpty)
    }

    // MARK: - codable back-compat (pre-q45f.3 artifacts)

    @Test("NarlReportRollup decodes pre-q45f.3 artifact (missing q45fCarryforward) as []")
    func reportRollupDecodesPreQ45f3WithEmptyCarryforward() throws {
        // Build a present-shape rollup, encode it, then strip
        // `q45fCarryforward` from the JSON payload and decode. This
        // simulates a pre-q45f.3 `report.json` artifact byte-for-byte;
        // a regression that flips `decodeIfPresent` back to a required
        // decode would fail the throw-free decode below.
        let rollup = Self.makeMinimalRollup()
        let encoded = try JSONEncoder().encode(rollup)
        let modified = try Self.removingKey("q45fCarryforward", from: encoded)
        let decoded = try JSONDecoder().decode(NarlReportRollup.self, from: modified)
        #expect(decoded.q45fCarryforward == [],
                "pre-q45f.3 artifacts (no q45fCarryforward key) must decode the field as [], not throw")
    }

    @Test("NarlReportRollup decode RAISES on malformed q45fCarryforward (decodeIfPresent over try?)")
    func reportRollupDecodeRaisesOnMalformedCarryforward() throws {
        // The decoder is intentionally `decodeIfPresent` rather than
        // `try? decode`: a malformed value (wrong type, broken shape)
        // must surface the decode error rather than silently degrade
        // to []. A regression to `try?` would swallow this case.
        let rollup = Self.makeMinimalRollup()
        let encoded = try JSONEncoder().encode(rollup)
        let modified = try Self.replacingKey(
            "q45fCarryforward",
            withScalarString: "this-is-not-an-array",
            in: encoded
        )
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(NarlReportRollup.self, from: modified)
        }
    }

    @Test("NarlReportEpisodeEntry decodes pre-q45f.3 artifact (missing q45fCounterfactual) as .empty")
    func reportEntryDecodesPreQ45f3WithEmptyCounterfactual() throws {
        let entry = Self.makeMinimalEntry()
        let encoded = try JSONEncoder().encode(entry)
        let modified = try Self.removingKey("q45fCounterfactual", from: encoded)
        let decoded = try JSONDecoder().decode(NarlReportEpisodeEntry.self, from: modified)
        #expect(decoded.q45fCounterfactual == NarlQ45fCounterfactual.empty,
                "pre-q45f.3 artifacts (no q45fCounterfactual key) must decode the field as .empty, not throw")
    }

    @Test("NarlReportEpisodeEntry decode RAISES on malformed q45fCounterfactual")
    func reportEntryDecodeRaisesOnMalformedCounterfactual() throws {
        let entry = Self.makeMinimalEntry()
        let encoded = try JSONEncoder().encode(entry)
        let modified = try Self.replacingKey(
            "q45fCounterfactual",
            withScalarString: "this-is-not-a-counterfactual-object",
            in: encoded
        )
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(NarlReportEpisodeEntry.self, from: modified)
        }
    }

    // MARK: - codable

    @Test("carryforward round-trips through JSON")
    func carryforwardCodableRoundTrip() throws {
        let original = NarlQ45fCarryforwardRollup(
            podcastId: "pc-A",
            finalMode: SkipMode.shadow.rawValue,
            totalDemotionsCount: 2,
            totalRewindEventCount: 5,
            firstDemotionEpisodeId: "ep-A",
            traceCount: 3,
            totalCrossPodcastDroppedCount: 4
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(NarlQ45fCarryforwardRollup.self, from: data)
        #expect(decoded == original)
        #expect(decoded.totalCrossPodcastDroppedCount == 4,
                "Cycle-4: totalCrossPodcastDroppedCount must round-trip")
    }

    @Test("carryforward decodes pre-cycle-4 artifact (missing totalCrossPodcastDroppedCount) as 0")
    func carryforwardDecodesPreCycle4WithZeroDropCount() throws {
        let json = """
        {"podcastId":"pc-A","finalMode":"manual","totalDemotionsCount":1,"totalRewindEventCount":2,"firstDemotionEpisodeId":"ep-A","traceCount":2}
        """
        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(NarlQ45fCarryforwardRollup.self, from: data)
        #expect(decoded.totalCrossPodcastDroppedCount == 0,
                "pre-cycle-4 artifacts (no totalCrossPodcastDroppedCount key) must decode the field as 0")
        #expect(decoded.podcastId == "pc-A")
        #expect(decoded.totalDemotionsCount == 1)
    }

    @Test("carryforward decode RAISES on malformed totalCrossPodcastDroppedCount")
    func carryforwardDecodeRaisesOnMalformedDropCount() throws {
        let json = """
        {"podcastId":"pc-A","finalMode":"auto","totalDemotionsCount":0,"totalRewindEventCount":0,"firstDemotionEpisodeId":null,"traceCount":1,"totalCrossPodcastDroppedCount":"not-an-int"}
        """
        let data = Data(json.utf8)
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(NarlQ45fCarryforwardRollup.self, from: data)
        }
    }

    // MARK: - encoder emits new keys (cycle-4 M-4)

    @Test("NarlReportEpisodeEntry encoder emits q45fCounterfactual key")
    func reportEntryEncoderEmitsQ45fCounterfactualKey() throws {
        // Cycle-4 M-4: a custom Codable that omits a new field on the
        // encode side breaks downstream readers as silently as a missing
        // decode would. Verify the wire actually carries the key by
        // round-tripping through `JSONSerialization` and inspecting the
        // top-level dictionary.
        let entry = Self.makeMinimalEntry()
        let data = try JSONEncoder().encode(entry)
        let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        let cfDict = dict?["q45fCounterfactual"] as? [String: Any]
        #expect(cfDict != nil,
                "encoded entry must carry the q45fCounterfactual key on the wire")
        // And the new cycle-4 field must be present inside the nested object.
        #expect(cfDict?["crossPodcastDroppedCount"] is Int,
                "encoded counterfactual must carry the crossPodcastDroppedCount key on the wire")
    }

    @Test("NarlReportRollup encoder emits q45fCarryforward key")
    func reportRollupEncoderEmitsQ45fCarryforwardKey() throws {
        let rollup = Self.makeMinimalRollup()
        let data = try JSONEncoder().encode(rollup)
        let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        let arr = dict?["q45fCarryforward"] as? [Any]
        #expect(arr != nil,
                "encoded rollup must carry the q45fCarryforward key on the wire")
        // Cycle-5 L-C: explicit non-empty guard so a future refactor of
        // `makeMinimalRollup()` that drops the seed entry can't turn this
        // assertion into a silent no-op (the M-4 acceptance criterion is
        // "tests must assert top-level keys exist on the wire").
        #expect(arr?.count == 1,
                "minimal rollup fixture must carry exactly 1 carryforward entry to verify the nested wire key")
        let first = try #require(arr?.first as? [String: Any])
        #expect(first["totalCrossPodcastDroppedCount"] is Int,
                "encoded carryforward must carry the totalCrossPodcastDroppedCount key on the wire")
    }

    // MARK: - sanity check on freshDefault seed (cycle-4 L-7)

    @Test("Q45fReplayGate.State.freshDefault is at or above the manual→auto trust threshold")
    func freshDefaultIsAtOrAboveManualToAutoThreshold() {
        // Cycle-4 sanity: the "best-case auto-mode steady state" seed
        // (auto, 0.90, 0) has to be at or above the trust score that
        // production would need to grant auto. If the production threshold
        // is ever raised above 0.90, this seed becomes a lie. Surface that
        // drift via a build-time test rather than a silent semantic shift.
        let threshold = TrustScoringConfig.default.manualToAutoTrustScore
        #expect(Q45fReplayGate.State.freshDefault.trustScore >= threshold,
                "freshDefault.trustScore (\(Q45fReplayGate.State.freshDefault.trustScore)) must be ≥ manualToAutoTrustScore (\(threshold)); raise the seed if production raised the bar")
        #expect(Q45fReplayGate.State.freshDefault.mode == .auto)
        #expect(Q45fReplayGate.State.freshDefault.recentFalseSkipSignals == 0)
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

    /// Minimal `NarlReportRollup` for byte-level back-compat tests.
    /// All metric fields zero — the tests don't care, they're checking
    /// the Codable shape of the new q45f field.
    private static func makeMinimalRollup() -> NarlReportRollup {
        NarlReportRollup(
            show: "TestShow",
            config: "default",
            episodeCount: 0,
            excludedEpisodeCount: 0,
            windowMetrics: [],
            secondLevel: NarlSecondLevelMetrics(
                truePositiveSeconds: 0,
                falsePositiveSeconds: 0,
                falseNegativeSeconds: 0,
                precision: 0,
                recall: 0,
                f1: 0
            ),
            totalLexicalInjectionAdds: 0,
            totalPriorShiftAdds: 0,
            totalEpisodesWithShadowCoverage: 0,
            coverageMetrics: .zero,
            pipelineCoverageFailureAssetCount: 0,
            q45fCarryforward: [
                NarlQ45fCarryforwardRollup(
                    podcastId: "pc-A",
                    finalMode: SkipMode.auto.rawValue,
                    totalDemotionsCount: 0,
                    totalRewindEventCount: 0,
                    firstDemotionEpisodeId: nil,
                    traceCount: 1
                )
            ]
        )
    }

    /// Minimal `NarlReportEpisodeEntry` for byte-level back-compat tests.
    private static func makeMinimalEntry() -> NarlReportEpisodeEntry {
        NarlReportEpisodeEntry(
            episodeId: "ep-1",
            podcastId: "pc-A",
            show: "TestShow",
            config: "default",
            isExcluded: false,
            exclusionReason: nil,
            groundTruthWindowCount: 0,
            predictedWindowCount: 0,
            windowMetrics: [],
            secondLevel: NarlSecondLevelMetrics(
                truePositiveSeconds: 0,
                falsePositiveSeconds: 0,
                falseNegativeSeconds: 0,
                precision: 0,
                recall: 0,
                f1: 0
            ),
            lexicalInjectionAdds: 0,
            priorShiftAdds: 0,
            hasShadowCoverage: false,
            q45fCounterfactual: NarlQ45fCounterfactual(
                wouldDemote: true,
                demotionTime: 12.5,
                finalMode: SkipMode.manual.rawValue,
                demotionsCount: 1,
                rewindEventCount: 2
            )
        )
    }

    /// Strip a top-level key from an encoded JSON object. Used to
    /// simulate pre-q45f.3 artifacts that lack a field the present
    /// schema carries.
    private static func removingKey(_ key: String, from data: Data) throws -> Data {
        guard var dict = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw DecodingError.dataCorrupted(
                .init(codingPath: [], debugDescription: "expected top-level JSON object")
            )
        }
        dict.removeValue(forKey: key)
        return try JSONSerialization.data(withJSONObject: dict)
    }

    /// Replace a top-level key's value with a scalar string. Used to
    /// simulate a malformed payload where a field's wire shape is
    /// wrong (the decoder must surface a `DecodingError`, not silently
    /// degrade).
    private static func replacingKey(
        _ key: String,
        withScalarString value: String,
        in data: Data
    ) throws -> Data {
        guard var dict = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw DecodingError.dataCorrupted(
                .init(codingPath: [], debugDescription: "expected top-level JSON object")
            )
        }
        dict[key] = value
        return try JSONSerialization.data(withJSONObject: dict)
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
