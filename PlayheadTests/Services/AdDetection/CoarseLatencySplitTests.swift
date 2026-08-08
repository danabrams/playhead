// CoarseLatencySplitTests.swift
// playhead-rkfp / playhead-ezmv: every passA row the coarse pass produces
// carries the wait-vs-infer split — the suspending-clock twin of its latency
// and the daemon census reading at attempt start.
//
// These are WIRING tests, deliberately at the classifier's output boundary:
// `FMClockPairTests` proves the pair reads the right clocks with synthetic
// instants; what remains falsifiable here is that the coarse pass actually
// STAMPS a reading on every row it emits, success and failure alike. A pass
// that stopped stamping would regress the next device pull to exactly the
// one-number `latencyMs` that made the 1,955.6 s row unreadable.

import Foundation
import Testing

@testable import Playhead

private func seg(_ index: Int, text: String = "segment text about the episode topic") -> AdTranscriptSegment {
    AdTranscriptSegment(
        atoms: [
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "asset-split",
                    transcriptVersion: "tx-split",
                    atomOrdinal: index
                ),
                contentHash: "hash-\(index)",
                startTime: Double(index) * 5,
                endTime: Double(index) * 5 + 5,
                text: text,
                chunkIndex: index
            )
        ],
        segmentIndex: index
    )
}

@Suite("playhead-rkfp: coarse pass rows carry the latency split")
struct CoarseLatencySplitTests {

    /// On an always-awake simulator the two clocks tick together, so the twin
    /// must land within noise of the continuous number. The tolerance covers
    /// the non-atomic clock reads at each end of the span, nothing more —
    /// a twin computed from a DIFFERENT span would exceed it wildly under the
    /// parallel gate's scheduling jitter, and a dropped twin is nil.
    private static let clockSkewToleranceMs = 50.0

    @Test("a successful window's row carries a same-span suspending twin and a census reading")
    func successWindowCarriesTheSplit() async throws {
        let runtime = TestFMRuntime()
        let classifier = FoundationModelClassifier(
            runtime: runtime.runtime,
            config: .init(safetyMarginTokens: 4, maximumResponseTokens: 6)
        )

        let output = try await classifier.coarsePassA(segments: [seg(0), seg(1)])

        #expect(output.status == .success)
        #expect(!output.windows.isEmpty, "vacuity: no window was screened, nothing was asserted")
        for window in output.windows {
            let twin = try #require(
                window.suspendingLatencyMillis,
                "a screened window must carry the suspending twin — nil regresses the pull to one unreadable number"
            )
            #expect(twin >= 0)
            #expect(
                twin <= window.latencyMillis + Self.clockSkewToleranceMs,
                "suspending (\(twin)ms) exceeded continuous (\(window.latencyMillis)ms) beyond clock-read skew — the two readings are not covering the same span"
            )
            let peers = try #require(window.daemonPeersAtStart)
            #expect(peers >= 0)
        }
    }

    @Test("a failed window's row carries the split too — failures are where the 1,955.6s row lived")
    func failedWindowCarriesTheSplit() async throws {
        let runtime = TestFMRuntime(coarseFailures: [.guardrailViolation])
        let classifier = FoundationModelClassifier(
            runtime: runtime.runtime,
            config: .init(safetyMarginTokens: 4, maximumResponseTokens: 6)
        )

        let output = try await classifier.coarsePassA(segments: [seg(0)])

        let failure = try #require(
            output.failedWindows.first,
            "vacuity: the injected guardrail violation produced no failed window"
        )
        #expect(failure.status == .guardrailViolation)
        let continuous = try #require(failure.latencyMillis)
        let twin = try #require(
            failure.suspendingLatencyMillis,
            "a failure row without the twin is exactly the field defect: cost readable only as one conflated number"
        )
        #expect(twin <= continuous + Self.clockSkewToleranceMs)
        let peers = try #require(failure.daemonPeersAtStart)
        #expect(peers >= 0)
    }
}
