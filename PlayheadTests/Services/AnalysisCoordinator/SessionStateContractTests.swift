// SessionStateContractTests.swift
// playhead-gtt9.8: expand `SessionState` into distinguishable terminal
// states (`completeFull`, `completeFeatureOnly`,
// `completeTranscriptPartial`, `failedTranscript`, `failedFeature`,
// `cancelledBudget`) and a non-terminal `waitingForBackfill` that
// replaces the "stay in hotPathReady under thermal pressure" behavior.
//
// Tests exercise:
//   1. The raw-string contract — every expected case maps to the
//      stable rawValue that `analysis_assets.analysisState` and
//      `analysis_sessions.state` persist as TEXT.
//   2. The legacy `.complete` case is still decodable (deprecated,
//      only reached by legacy persisted rows on migrate).
//   3. `validTransitions` admits the new edges — `hotPathReady` can
//      fork into `waitingForBackfill`, `waitingForBackfill` drops into
//      `backfill` or `cancelledBudget`, and `backfill` terminates into
//      one of the five new terminals (not the legacy `.complete`).
//   4. All new terminal states round-trip from their rawValue.

import Foundation
import Testing

@testable import Playhead

@Suite("SessionState contract — playhead-gtt9.8")
struct SessionStateContractTests {

    // MARK: - Raw-string contract

    @Test("New SessionState cases persist under documented rawValues")
    func newCasesHaveStableRawValues() {
        // These rawValues are load-bearing: they are written to the
        // `analysis_assets.analysisState` column (TEXT NOT NULL) and
        // decoded on reload. Changing any of them would silently break
        // crash-recovery of in-flight sessions.
        #expect(SessionState.waitingForBackfill.rawValue == "waitingForBackfill")
        #expect(SessionState.completeFull.rawValue == "completeFull")
        #expect(SessionState.completeFeatureOnly.rawValue == "completeFeatureOnly")
        #expect(SessionState.completeTranscriptPartial.rawValue == "completeTranscriptPartial")
        #expect(SessionState.failedTranscript.rawValue == "failedTranscript")
        #expect(SessionState.failedFeature.rawValue == "failedFeature")
        #expect(SessionState.cancelledBudget.rawValue == "cancelledBudget")
    }

    @Test("Legacy .complete and .failed rawValues are preserved for on-disk compatibility")
    func legacyCasesRoundTrip() {
        // `.complete` is deprecated post-gtt9.8 — new pipelines route
        // through the richer terminals — but any persisted row from an
        // older build still decodes to this variant.
        #expect(SessionState(rawValue: "complete") == .complete)
        #expect(SessionState(rawValue: "failed") == .failed)
    }

    @Test(
        "Every SessionState rawValue is the exact Swift identifier",
        arguments: [
            (SessionState.queued, "queued"),
            (.spooling, "spooling"),
            (.featuresReady, "featuresReady"),
            (.hotPathReady, "hotPathReady"),
            (.waitingForBackfill, "waitingForBackfill"),
            (.backfill, "backfill"),
            (.complete, "complete"),
            (.completeFull, "completeFull"),
            (.completeFeatureOnly, "completeFeatureOnly"),
            (.completeTranscriptPartial, "completeTranscriptPartial"),
            (.failed, "failed"),
            (.failedTranscript, "failedTranscript"),
            (.failedFeature, "failedFeature"),
            (.cancelledBudget, "cancelledBudget"),
        ]
    )
    func rawValueMatchesIdentifier(state: SessionState, raw: String) {
        #expect(state.rawValue == raw)
        #expect(SessionState(rawValue: raw) == state)
    }

    // MARK: - Case cardinality

    @Test("SessionState.allCases has exactly 14 cases after gtt9.8")
    func cardinalityIsFourteen() {
        // 7 legacy cases (queued, spooling, featuresReady, hotPathReady,
        // backfill, complete, failed) + 7 new (waitingForBackfill,
        // completeFull, completeFeatureOnly, completeTranscriptPartial,
        // failedTranscript, failedFeature, cancelledBudget) = 14.
        #expect(SessionState.allCases.count == 14)
    }

    // MARK: - validTransitions

    @Test("hotPathReady may fork into waitingForBackfill or backfill")
    func hotPathReadyTransitions() {
        let next = SessionState.hotPathReady.validTransitions
        #expect(next.contains(.waitingForBackfill))
        #expect(next.contains(.backfill))
    }

    @Test("waitingForBackfill drops into backfill or cancelledBudget")
    func waitingForBackfillTransitions() {
        let next = SessionState.waitingForBackfill.validTransitions
        #expect(next.contains(.backfill))
        #expect(next.contains(.cancelledBudget))
    }

    @Test("backfill terminates into the five richer terminals (no plain .complete)")
    func backfillTerminals() {
        let next = SessionState.backfill.validTransitions
        #expect(next.contains(.completeFull))
        #expect(next.contains(.completeFeatureOnly))
        #expect(next.contains(.completeTranscriptPartial))
        #expect(next.contains(.failedTranscript))
        #expect(next.contains(.failedFeature))
        // Legacy `.complete` is no longer a valid backfill successor;
        // callers must resolve one of the richer completions.
        #expect(!next.contains(.complete))
    }

    @Test("failedTranscript and failedFeature are retryable via .queued")
    func richFailureTerminalsAreRetryable() {
        #expect(SessionState.failedTranscript.validTransitions.contains(.queued))
        #expect(SessionState.failedFeature.validTransitions.contains(.queued))
        #expect(SessionState.cancelledBudget.validTransitions.contains(.queued))
    }

    @Test("New complete terminals accept recovery re-queue")
    func newCompleteTerminalsRecover() {
        // Same recovery-shape as legacy `.complete` — if the data is
        // ever found empty, the coverage-guard recovery sweep resets
        // the session back to `.queued`.
        #expect(SessionState.completeFull.validTransitions.contains(.queued))
        #expect(SessionState.completeFeatureOnly.validTransitions.contains(.queued))
        #expect(SessionState.completeTranscriptPartial.validTransitions.contains(.queued))
    }
}
