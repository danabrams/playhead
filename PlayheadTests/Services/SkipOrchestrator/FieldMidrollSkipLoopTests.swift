// FieldMidrollSkipLoopTests.swift
// playhead-lhs2e — Dan, 2026-09-08: "When it skipped the end of the midroll, it
// played the audio on a few second loop over and over until I force quit."
//
// THE ROWS ARE THE FIXTURE. Every window below is copied from the device pull
// (~/playhead-gate-artifacts/pull-2026-09-08, episode F88B3C34, the midroll he
// was listening to), so this suite asks what the shipped code does with the
// state that actually existed, not with a state that seemed plausible.
//
// WHAT IT MODELS. `PlaybackTransport.checkSkipCues(currentTime:)` is one rule:
//
//     for cue in skipCues where position >= cue.start && position < cue.end
//         -> seek to cue.end
//
// applied again after every seek, from whatever position the player reports.
// A loop is that iteration failing to TERMINATE or failing to move FORWARD, and
// both are properties of the cue list alone — so they are decidable here,
// without a player, without audio, and without waiting on a real seek. That is
// the point: a hang in a test is a bad rail (playhead-2gka), so this bounds the
// iteration and reports a cycle as a failed expectation.
//
// ELIMINATED BEFORE WRITING THIS, so the next reader does not re-derive it: the
// transport's seeks pass `toleranceBefore: .zero, toleranceAfter: .zero` at
// every site, so "the seek landed on an earlier keyframe and re-entered its own
// cue" — the obvious explanation for a few-second repeat — is NOT the mechanism.

import CoreMedia
import Foundation
import Testing

@Suite("playhead-lhs2e — the midroll cue list must settle", .timeLimit(.minutes(1)))
struct FieldMidrollSkipLoopTests {
    /// The transport's rule, as a pure function over a cue list.
    ///
    /// Returns the sequence of positions a player would visit. Bounded: a cycle
    /// is reported by the caller as a failure rather than by hanging.
    static func positionsVisited(
        from start: Double,
        cues: [(start: Double, end: Double)],
        limit: Int = 32
    ) -> [Double] {
        var visited: [Double] = [start]
        var position = start
        for _ in 0..<limit {
            guard let cue = cues.first(where: { position >= $0.start && position < $0.end }) else {
                return visited
            }
            position = cue.end
            visited.append(position)
        }
        return visited
    }

    /// The seven midroll rows of F88B3C34, verbatim from the pull.
    static let midrollWindows: [(id: String, start: Double, end: Double, state: String, boundary: String, gate: String?)] = [
        ("m-870", 870.4, 941.6, "confirmed", "userMarked", "eligible"),
        ("m-929", 929.3, 942.2, "candidate", "podContinuation", "markOnly"),
        ("m-942applied", 942.2, 1089.8, "applied", "dayZeroRediffByteExact", "eligible"),
        ("m-942user", 942.4, 970.0, "confirmed", "userMarked", "eligible"),
        ("m-970", 970.2, 979.9, "confirmed", "acousticRefined", "markOnly"),
        ("m-1005confirmed", 1005.8, 1089.9, "confirmed", "userConfirmedSuggested", nil),
        ("m-1005acoustic", 1005.8, 1089.9, "confirmed", "acousticRefined", "markOnly"),
    ]

    @Test("the transport's own rule terminates on the shipped midroll cue list")
    func theRuleTerminates() {
        // Every window the device could have cued, as the transport would see
        // it. This is deliberately the WIDEST reading — if even this settles,
        // the cue list is not the loop and the finding moves to the player.
        let cues = Self.midrollWindows.map { (start: $0.start, end: $0.end) }
        let visited = Self.positionsVisited(from: 1_085.0, cues: cues)

        #expect(
            visited.count < 32,
            """
            the skip rule did not settle from 1085 s on the shipped midroll cue list — it \
            visited \(visited.count) positions: \(visited.prefix(12)). That is Dan's loop, \
            reproduced from the rows, and it is decidable without a player.
            """
        )
        // And it must move FORWARD every time. A cue whose end is at or behind
        // the position that entered it is a seek that plays the same seconds
        // again, which is what "a few second loop" sounds like from the outside.
        for (earlier, later) in zip(visited, visited.dropFirst()) {
            #expect(later > earlier, "the rule moved from \(earlier) to \(later) — not forward")
        }
    }

    @Test("no two cues in that list share a containment that re-arms after the first skip")
    func containmentIsBounded() {
        let cues = Self.midrollWindows.map { (start: $0.start, end: $0.end) }
        // The specific shape the pull shows: a cue whose END lands inside
        // another cue. That is legal and terminating on its own — the second
        // skip carries the listener past both — but it is the shape that makes
        // the count above worth asserting, so it is named rather than assumed
        // absent.
        let endsInsideAnother = cues.filter { cue in
            cues.contains { other in
                other.start < cue.end && cue.end < other.end
            }
        }
        #expect(
            endsInsideAnother.count == 1,
            """
            expected exactly the 942.2-1089.8 window to end inside 1005.8-1089.9; found \
            \(endsInsideAnother). If this changes, re-read the loop test above — the two \
            assertions are about the same geometry.
            """
        )
    }
}
