// LexicalAnchorRefinementWireInTests.swift
// playhead-xsdz.37: tests for the LexicalAnchorBank boundary-refinement
// wire-in into `AdDetectionService.runBackfill`.
//
// Mirrors StingerRefinementWireInTests / SpanFinalizerWireInTests structure:
//   (a) Flag default / config-init plumbing — `lexicalAnchorRefinementEnabled`
//       defaults to `false` in `AdDetectionConfig.default` (this cut ships OFF
//       and is measured before any flip), and the init default matches.
//   (b) Flag-OFF byte-identity — running `runBackfill` twice on the same
//       deterministic fixture (once with the flag explicitly OFF AND a live
//       bank injected, once with the config left at its default and nothing
//       injected) produces identical persisted AdWindow rows and leaves both
//       lexical trace maps empty. Injecting the bank on the OFF arm is the
//       stronger contract: even fully wired, the flag gate alone must keep the
//       refiner unreachable.
//   (c) Flag-ON wire-up — a synthetic bank entry keyed to the runBackfill
//       `podcastId` plus a transcript carrying an exact pre-opener drives a
//       real end-to-end snap: the persisted window's start edge moves to the
//       matched-phrase position plus offset, the window count is unchanged, and
//       both trace maps are populated. A no-match arm pins the graceful no-op.

import Foundation
import Testing
@testable import Playhead

@Suite("LexicalAnchorRefinement wire-in (playhead-xsdz.37)")
struct LexicalAnchorRefinementWireInTests {

    private static let podcastId = "podcast-test"

    /// The pre-opener "we will be right back" starts at char 0 of the ad chunk
    /// (chunk 1, [30, 60)), so its interpolated onset is exactly 30.0s.
    private static let phraseOnset = 30.0

    // MARK: - Fixture

    private func makeAdSignalChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "This is the introduction to the program with our host and guest.",
            "We will be right back. This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.",
            "Now we continue our discussion about technology and the future."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeService(
        store: AnalysisStore,
        lexicalEnabled: Bool,
        lexicalBank: LexicalAnchorBank? = nil
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            lexicalAnchorRefinementEnabled: lexicalEnabled
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            lexicalAnchorBank: lexicalBank
        )
    }

    /// Bank with one show keyed by the EXACT `podcastId` — the production
    /// join-key contract — carrying a single pre-opener anchor with a caller-
    /// chosen offset. Built through `LexicalAnchorBank.decode` so the wire-in
    /// consumes a bank that went through the same validation path production
    /// uses. `phrase` lets the no-match arm plant an anchor absent from the
    /// transcript.
    private static func makeBank(
        showKey: String,
        offset: Double,
        phrase: String = "we will be right back"
    ) throws -> LexicalAnchorBank {
        let payload: [String: Any] = [
            "schemaVersion": 1,
            "shows": [
                [
                    "showKeys": [showKey],
                    "showName": "Wire-In Test Show",
                    "anchors": [
                        [
                            "phrase": phrase,
                            "side": "pre",
                            "matchPolicy": "exact",
                            "edgeOffsetSeconds": offset,
                            "confidence": 0.9,
                            "support": 3,
                        ],
                    ],
                ],
            ],
            "genericAnchors": [[String: Any]](),
        ]
        let data = try JSONSerialization.data(withJSONObject: payload)
        return try LexicalAnchorBank.decode(data)
    }

    // MARK: - (a) Config defaults

    @Test("AdDetectionConfig.default ships lexical refinement OFF")
    func configDefaultsAreOff() {
        #expect(
            AdDetectionConfig.default.lexicalAnchorRefinementEnabled == false,
            "this cut ships OFF; the production flip is measured later (out of scope)"
        )
    }

    @Test("AdDetectionConfig init carries lexicalAnchorRefinementEnabled through")
    func configInitCarriesFlag() {
        let on = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            lexicalAnchorRefinementEnabled: true
        )
        #expect(on.lexicalAnchorRefinementEnabled == true)

        let off = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1",
            lexicalAnchorRefinementEnabled: false
        )
        #expect(off.lexicalAnchorRefinementEnabled == false)

        // Omitting the arg must match `.default` (OFF).
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70, suppressionThreshold: 0.25,
            hotPathLookahead: 90.0, detectorVersion: "test-v1"
        )
        #expect(omitted.lexicalAnchorRefinementEnabled == false, "init default must match .default")
    }

    // MARK: - (b) Flag-OFF byte-identity

    @Test("Flag OFF: runBackfill is byte-identical to the default config even with a bank wired")
    func flagOffMatchesDefaultBaseline() async throws {
        let storeExplicit = try await makeTestStore()
        let storeDefault = try await makeTestStore()
        let assetId = "asset-xsdz37-off"
        try await storeExplicit.insertAsset(makeAsset(id: assetId))
        try await storeDefault.insertAsset(makeAsset(id: assetId))

        // Explicit-OFF arm: flag off but a LIVE bank keyed to this podcastId
        // and an offset that WOULD snap the start if the refiner were reached.
        let serviceExplicit = makeService(
            store: storeExplicit,
            lexicalEnabled: false,
            lexicalBank: try Self.makeBank(showKey: Self.podcastId, offset: 5.0)
        )
        // Default arm: config default (lexical OFF), nothing injected.
        let defaultConfig = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off
            // lexicalAnchorRefinementEnabled omitted → default OFF.
        )
        let serviceDefault = AdDetectionService(
            store: storeDefault,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: defaultConfig
        )

        let chunks = makeAdSignalChunks(assetId: assetId)
        try await serviceExplicit.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 90.0
        )
        try await serviceDefault.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 90.0
        )

        let windowsExplicit = try await storeExplicit.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsDefault = try await storeDefault.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        #expect(
            windowsExplicit.count == windowsDefault.count,
            "explicit-OFF \(windowsExplicit.count) vs default \(windowsDefault.count) — flag OFF must be byte-identical"
        )
        // Same persisted-field sweep the p56a/l2f.6 byte-identity tests pin.
        for (a, b) in zip(windowsExplicit, windowsDefault) {
            #expect(a.startTime == b.startTime, "startTime mismatch under flag OFF")
            #expect(a.endTime == b.endTime, "endTime mismatch under flag OFF")
            #expect(a.confidence == b.confidence, "confidence mismatch under flag OFF")
            #expect(a.decisionState == b.decisionState, "decisionState mismatch under flag OFF")
            #expect(a.eligibilityGate == b.eligibilityGate, "eligibilityGate mismatch under flag OFF")
            #expect(a.wasSkipped == b.wasSkipped, "wasSkipped mismatch under flag OFF")
            #expect(a.boundaryState == b.boundaryState, "boundaryState mismatch under flag OFF")
            #expect(a.detectorVersion == b.detectorVersion, "detectorVersion mismatch under flag OFF")
            #expect(a.metadataSource == b.metadataSource, "metadataSource mismatch under flag OFF")
            #expect(a.metadataConfidence == b.metadataConfidence, "metadataConfidence mismatch under flag OFF")
            #expect(a.evidenceStartTime == b.evidenceStartTime, "evidenceStartTime mismatch under flag OFF")
        }

        // Both lexical trace maps stay empty under flag OFF — no consult.
        let traceBySpan = await serviceExplicit.lexicalRefinementTraceBySpanIdForTesting()
        let traceByWindow = await serviceExplicit.lexicalRefinementTraceByWindowIdForTesting()
        #expect(traceBySpan.isEmpty, "flag OFF must leave the spanId trace empty")
        #expect(traceByWindow.isEmpty, "flag OFF must leave the windowId trace empty")
    }

    // MARK: - (c) Flag-ON wire-up

    @Test("Flag ON: an exact pre-opener snaps the persisted window start end-to-end and stamps both trace maps")
    func flagOnSnapsWindowStartEndToEnd() async throws {
        let storeOff = try await makeTestStore()
        let storeOn = try await makeTestStore()
        let assetId = "asset-xsdz37-on"
        try await storeOff.insertAsset(makeAsset(id: assetId))
        try await storeOn.insertAsset(makeAsset(id: assetId))
        let chunks = makeAdSignalChunks(assetId: assetId)

        // OFF baseline first, so the snap assertion is relative to what the
        // pipeline persists without refinement.
        let serviceOff = makeService(store: storeOff, lexicalEnabled: false)
        try await serviceOff.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 90.0
        )
        let windowsOff = try await storeOff.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        try #require(!windowsOff.isEmpty, "fixture must produce a window for the snap pin to be meaningful")
        let baselineStart = windowsOff[0].startTime

        // Choose an offset that drives the start a clean +5s from the baseline,
        // regardless of where the pipeline landed it. `candidate = phraseOnset
        // + offset = baselineStart + 5`, always inside the move cap.
        let targetStart = baselineStart + 5.0
        let offset = targetStart - Self.phraseOnset
        let serviceOn = makeService(
            store: storeOn,
            lexicalEnabled: true,
            lexicalBank: try Self.makeBank(showKey: Self.podcastId, offset: offset)
        )
        try await serviceOn.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 90.0
        )
        let windowsOn = try await storeOn.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        // Never split, never merge.
        #expect(
            windowsOn.count == windowsOff.count,
            "lexical refinement must never split or merge windows (\(windowsOff.count) OFF vs \(windowsOn.count) ON)"
        )

        let snapped = try #require(windowsOn.first)
        #expect(
            abs(snapped.startTime - targetStart) < 0.05,
            "window start must snap to phrase-onset + offset = \(targetStart) (got \(snapped.startTime), baseline \(baselineStart))"
        )
        #expect(
            snapped.endTime == windowsOff[0].endTime,
            "end edge has no post anchor and must not move (got \(snapped.endTime) vs \(windowsOff[0].endTime))"
        )
        #expect(snapped.endTime > snapped.startTime)

        // Both trace maps populated; the per-window entry records the start snap.
        let traceBySpan = await serviceOn.lexicalRefinementTraceBySpanIdForTesting()
        let traceByWindow = await serviceOn.lexicalRefinementTraceByWindowIdForTesting()
        #expect(!traceBySpan.isEmpty, "flag ON must populate the spanId trace map")
        #expect(!traceByWindow.isEmpty, "flag ON must populate the windowId trace map")
        let windowTrace = try #require(
            traceByWindow[snapped.id],
            "the snapped window's id must resolve a trace (ids: \(traceByWindow.keys.sorted()))"
        )
        #expect(windowTrace.startSnapped, "trace must record the start snap")
        #expect(!windowTrace.endSnapped)
        #expect(!windowTrace.revertedNoOverlap)
        #expect(windowTrace.startAnchorPhrase == "we will be right back")
        let delta = try #require(windowTrace.startDeltaSeconds)
        #expect(abs(delta - 5.0) < 0.05, "recorded delta \(delta) must equal the +5s snap")
    }

    @Test("Flag ON with an anchor absent from the transcript: bounds match OFF and the trace records the no-snap consult")
    func flagOnNoMatchDegradesToNoOp() async throws {
        let storeOff = try await makeTestStore()
        let storeOn = try await makeTestStore()
        let assetId = "asset-xsdz37-nomatch"
        try await storeOff.insertAsset(makeAsset(id: assetId))
        try await storeOn.insertAsset(makeAsset(id: assetId))
        let chunks = makeAdSignalChunks(assetId: assetId)

        let serviceOff = makeService(store: storeOff, lexicalEnabled: false)
        let serviceOn = makeService(
            store: storeOn,
            lexicalEnabled: true,
            // Anchor phrase never appears in the fixture transcript.
            lexicalBank: try Self.makeBank(
                showKey: Self.podcastId, offset: 5.0, phrase: "please subscribe on your favorite app"
            )
        )
        try await serviceOff.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 90.0
        )
        try await serviceOn.runBackfill(
            chunks: chunks, analysisAssetId: assetId, podcastId: Self.podcastId, episodeDuration: 90.0
        )

        let windowsOff = try await storeOff.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsOn = try await storeOn.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        #expect(windowsOn.count == windowsOff.count)
        for (a, b) in zip(windowsOn, windowsOff) {
            #expect(a.startTime == b.startTime, "no match ⇒ no snap ⇒ bounds match OFF baseline")
            #expect(a.endTime == b.endTime, "no match ⇒ no snap ⇒ bounds match OFF baseline")
        }

        // The refiner WAS consulted (entry resolved), so the trace records the
        // no-snap outcome — distinguishing "flag OFF" from "flag ON, no match".
        let traceBySpan = await serviceOn.lexicalRefinementTraceBySpanIdForTesting()
        #expect(!traceBySpan.isEmpty, "a consulted refiner records a trace even when nothing snapped")
        for trace in traceBySpan.values {
            #expect(trace == LexicalRefinementTrace(), "no match ⇒ pristine no-snap trace (got \(trace))")
        }
    }

    @Test("Flag ON with no bank entry for the show: refiner never consulted, trace stays empty")
    func flagOnUnknownShowLeavesTraceEmpty() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-xsdz37-unknown-show"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(
            store: store,
            lexicalEnabled: true,
            lexicalBank: try Self.makeBank(showKey: "some-other-show", offset: 5.0)
        )
        try await service.runBackfill(
            chunks: makeAdSignalChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 90.0
        )
        let traceBySpan = await service.lexicalRefinementTraceBySpanIdForTesting()
        let traceByWindow = await service.lexicalRefinementTraceByWindowIdForTesting()
        #expect(traceBySpan.isEmpty, "no bank entry for this show ⇒ refiner never consulted")
        #expect(traceByWindow.isEmpty)
    }
}
