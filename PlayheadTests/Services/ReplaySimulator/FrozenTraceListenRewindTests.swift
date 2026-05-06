// FrozenTraceListenRewindTests.swift
// playhead-q45f.1: FrozenTrace v3 listen-rewind capture.
//
// Adds optional `listenRewindEvents: [FrozenListenRewindEvent]` to FrozenTrace
// so the q45f counterfactual gate can replay how often a user tapped
// "Listen" on an auto-skipped window. Today there is no event log of
// listen-rewinds (recordListenRewind only mutates AdWindowDecision +
// PodcastProfile), so q45f's gate is structurally unsatisfiable. This bead
// extends the schema; the persistence layer that feeds it lands in a
// follow-up commit in this same bead.
//
// Acceptance contract proven by these tests:
//   1. Field encodes and decodes round-trip with non-empty events.
//   2. Old fixtures missing `listenRewindEvents` decode with `[]` so the
//      schema stays backward-compatible (matches the additive convention
//      already used for windowScores, showLabel, terminalReason, etc.).
//   3. `traceVersion` constant has been bumped to "frozen-trace-v3" — the
//      bump signals that consumers can rely on the field being populated
//      on a fresh capture (older fixtures still decode safely).
//   4. `withHoldoutDesignation(_:)` preserves the field (canonical copy
//      path; new fields silently dropped here would corrupt eval runs).

import XCTest
@testable import Playhead

final class FrozenTraceListenRewindTests: XCTestCase {

    // MARK: - Helpers

    private func makeTrace(
        listenRewindEvents: [FrozenTrace.FrozenListenRewindEvent]
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: "ep-rew-001",
            podcastId: "pod-rew-001",
            episodeDuration: 1800,
            traceVersion: FrozenTrace.currentTraceVersion,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000),
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

    private func makeEvent(
        time: Double = 123.45,
        windowId: String = "win-A",
        podcastId: String = "pod-rew-001"
    ) -> FrozenTrace.FrozenListenRewindEvent {
        FrozenTrace.FrozenListenRewindEvent(
            time: time,
            windowId: windowId,
            podcastId: podcastId
        )
    }

    // MARK: - traceVersion bump

    func testCurrentTraceVersionIsV3() {
        XCTAssertEqual(
            FrozenTrace.currentTraceVersion,
            "frozen-trace-v3",
            "q45f.1 bumps the version constant; consumers gate on this to know listen-rewind data is reliable."
        )
    }

    // MARK: - Round-trip preservation

    func testListenRewindEventsRoundTrip() throws {
        let events = [
            makeEvent(time: 60.0, windowId: "win-A", podcastId: "pod-rew-001"),
            makeEvent(time: 1410.5, windowId: "win-B", podcastId: "pod-rew-001"),
        ]
        let trace = makeTrace(listenRewindEvents: events)

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(trace)

        let decoder = JSONDecoder()
        let decoded = try decoder.decode(FrozenTrace.self, from: data)

        XCTAssertEqual(decoded.listenRewindEvents.count, 2,
                       "listenRewindEvents must survive encode → decode round-trip")
        XCTAssertEqual(decoded.listenRewindEvents[0].time, 60.0)
        XCTAssertEqual(decoded.listenRewindEvents[0].windowId, "win-A")
        XCTAssertEqual(decoded.listenRewindEvents[0].podcastId, "pod-rew-001")
        XCTAssertEqual(decoded.listenRewindEvents[1].time, 1410.5)
        XCTAssertEqual(decoded.listenRewindEvents[1].windowId, "win-B")
    }

    func testListenRewindEventsAppearInEncodedJSON() throws {
        let events = [makeEvent(time: 42.0, windowId: "win-X", podcastId: "pod-Z")]
        let trace = makeTrace(listenRewindEvents: events)
        let data = try JSONEncoder().encode(trace)
        let str = String(data: data, encoding: .utf8) ?? ""
        XCTAssertTrue(str.contains("\"listenRewindEvents\""),
                      "listenRewindEvents key must be present in emitted JSON, got: \(str)")
        XCTAssertTrue(str.contains("\"win-X\""),
                      "windowId value must be present in emitted JSON, got: \(str)")
    }

    // MARK: - Backward compatibility (old fixtures)

    func testOldFixtureWithoutListenRewindEventsDecodesAsEmptyArray() throws {
        // An older v2 fixture missing the field. The harness's tolerance
        // contract: missing → empty array. Matches the pattern used for
        // windowScores, corrections, decisionEvents, etc.
        let legacyJSON = """
        {
          "episodeId": "legacy-ep",
          "podcastId": "legacy-pod",
          "episodeDuration": 600.0,
          "traceVersion": "frozen-trace-v2",
          "capturedAt": 1714402200.0,
          "featureWindows": [],
          "atoms": [],
          "evidenceCatalog": [],
          "corrections": [],
          "decisionEvents": [],
          "baselineReplaySpanDecisions": [],
          "holdoutDesignation": "training",
          "windowScores": []
        }
        """.data(using: .utf8)!

        let decoded = try JSONDecoder().decode(FrozenTrace.self, from: legacyJSON)
        XCTAssertEqual(decoded.listenRewindEvents, [],
                       "legacy fixtures without listenRewindEvents must decode as []")
    }

    // MARK: - withHoldoutDesignation preservation

    func testWithHoldoutDesignationPreservesListenRewindEvents() {
        let events = [makeEvent(time: 7.0, windowId: "win-H", podcastId: "pod-H")]
        let original = makeTrace(listenRewindEvents: events)

        let copy = original.withHoldoutDesignation(.holdout)

        XCTAssertEqual(copy.listenRewindEvents.count, 1,
                       "withHoldoutDesignation must carry listenRewindEvents forward")
        XCTAssertEqual(copy.listenRewindEvents.first?.windowId, "win-H")
        XCTAssertEqual(copy.holdoutDesignation, .holdout,
                       "withHoldoutDesignation must still flip the designation")
    }

    // MARK: - Existing real fixtures still decode (regression guard)

    func testRealFixturesFromCorpusStillDecode() throws {
        // Spot-check one known-good fixture from each captured date. A
        // previously captured fixture that fails to decode after this
        // schema bump means we accidentally broke back-compat.
        //
        // Path resolution uses #filePath → repoRoot like
        // NarlPipelineCoverageBucketTests does; the simulator sandbox's
        // currentDirectoryPath is NOT the repo root.
        //
        // Decoder uses iso8601 because real fixtures encode `capturedAt`
        // as ISO-8601 strings (matching the harness's encoder). The
        // default `.deferredToDate` strategy expects Double and would
        // throw silently against the wrong roots, masking back-compat
        // breakage.
        let thisFile = URL(fileURLWithPath: #filePath)
        let repoRoot = try NarlEvalHarnessTests.repoRoot(
            startingAt: thisFile.deletingLastPathComponent()
        )
        let fixtureRoots = [
            "PlayheadTests/Fixtures/NarlEval/2026-04-22",
            "PlayheadTests/Fixtures/NarlEval/2026-04-23",
            "PlayheadTests/Fixtures/NarlEval/2026-04-24",
            "PlayheadTests/Fixtures/NarlEval/2026-04-25",
        ]
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let fm = FileManager.default
        var checked = 0
        for relative in fixtureRoots {
            let url = repoRoot.appendingPathComponent(relative)
            guard fm.fileExists(atPath: url.path) else { continue }
            let entries = try fm.contentsOfDirectory(atPath: url.path)
            guard let firstJSON = entries.first(where: {
                $0.hasPrefix("FrozenTrace-") && $0.hasSuffix(".json")
            }) else { continue }
            let fileURL = url.appendingPathComponent(firstJSON)
            let data = try Data(contentsOf: fileURL)
            do {
                let trace = try decoder.decode(FrozenTrace.self, from: data)
                XCTAssertEqual(trace.listenRewindEvents, [],
                               "pre-q45f.1 fixture \(firstJSON) must decode with empty listenRewindEvents")
                checked += 1
            } catch {
                XCTFail("Existing fixture \(firstJSON) failed to decode after schema bump: \(error)")
            }
        }
        // Loud failure if no fixtures were checked — silent vacuous pass
        // would let path-resolution drift hide back-compat breakage.
        XCTAssertGreaterThan(checked, 0,
                             "no fixtures decoded — path resolution likely broken")
    }
}
