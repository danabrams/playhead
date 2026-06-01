// SpanFinalizerWireInTests.swift
// playhead-p56a: tests for the `SpanFinalizer` wire-in into
// `AdDetectionService.runBackfill`.
//
// Three concerns:
//   (a) Flag default / config-init plumbing — `spanFinalizerEnabled`
//       defaults to `false` in `AdDetectionConfig.default`, and the init
//       carries the arg through. Asserts the OFF-by-default contract
//       documented on the field.
//   (b) Flag-OFF byte-identity — running `runBackfill` twice on the same
//       deterministic fixture (once with the flag explicitly set to OFF,
//       once with it left at its default) produces the same AdWindow row
//       count and start/end bounds. Also pins the contract that
//       `spanFinalizerConstraintsByWindowIdForTesting()` stays empty when
//       the flag is OFF — no allocation, no trace.
//   (c) Flag-ON wire-up — running `runBackfill` with the flag flipped to
//       ON does NOT throw, does NOT regress windowcount on a clean fixture,
//       and pins a CONSTRAINT-FIRES contract on a HAND-CRAFTED set of
//       overlapping / short adjacent fused spans via a direct
//       `SpanFinalizer.finalize(...)` call mirroring the wire-in's
//       `CandidateSpan` translation. Constraint #2 (`mergedWithAdjacent`)
//       is the explicit bd-4xqf target.
//
// Why both halves: the `runBackfill` end-to-end path on a synthetic
// transcript is the right shape for asserting "the flag plumbed end-to-
// end" but doesn't deterministically produce overlapping or sub-3s
// adjacent fused spans (that's a function of the live decoder + fusion
// stack which is not in scope to drive here). The direct
// `SpanFinalizer.finalize(...)` call on hand-crafted candidates is the
// right shape for asserting "constraints fire on the EXACT translation
// the wire-in uses." Together they pin both ends of the contract.

import Foundation
import Testing
@testable import Playhead

@Suite("SpanFinalizer wire-in (playhead-p56a)")
struct SpanFinalizerWireInTests {

    // MARK: - (a) Config defaults

    @Test("AdDetectionConfig.default keeps SpanFinalizer OFF")
    func configDefaultsAreOff() {
        let config = AdDetectionConfig.default
        #expect(
            config.spanFinalizerEnabled == false,
            "OFF-by-default is load-bearing: production must stay behavior-neutral until the bd-4xqf coverage measurement confirms the change is safe to enable"
        )
    }

    @Test("AdDetectionConfig init carries spanFinalizerEnabled through")
    func configInitCarriesFlag() {
        let off = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1",
            spanFinalizerEnabled: false
        )
        #expect(off.spanFinalizerEnabled == false)

        let on = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1",
            spanFinalizerEnabled: true
        )
        #expect(on.spanFinalizerEnabled == true)

        // The init's default value MUST match `.default` so callers that
        // omit the arg get the OFF path. If a future refactor flips the
        // default in either spot, this catches it on the sim.
        let omitted = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-v1"
        )
        #expect(omitted.spanFinalizerEnabled == false, "init default must match .default")
    }

    // MARK: - (b) Flag-OFF byte-identity

    /// Replicates the `BackfillFusionPipelineTests` ad-signal chunks shape
    /// (locally — the cross-test helpers are `internal` but in a different
    /// test type, so we re-declare here rather than couple the suites).
    private func makeAdSignalChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome back to the show today.",
            "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.",
            "Back to our conversation about technology and the future of podcasting."
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
        spanFinalizerEnabled: Bool
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            spanFinalizerEnabled: spanFinalizerEnabled
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    @Test("Flag OFF: runBackfill produces same AdWindow row count and bounds as the default (= flag also OFF)")
    func flagOffMatchesDefaultBaseline() async throws {
        // Two independent stores, two independent service instances, same
        // synthetic transcript. The first runs with the flag explicitly
        // set to OFF; the second leaves the flag at its (OFF) default.
        // The wire-in's OFF branch must be byte-identical to today's
        // shipping behaviour — same window count, same start/end times.

        let storeExplicit = try await makeTestStore()
        let storeDefault = try await makeTestStore()
        let assetId = "asset-p56a-off"
        try await storeExplicit.insertAsset(makeAsset(id: assetId))
        try await storeDefault.insertAsset(makeAsset(id: assetId))

        let serviceExplicit = makeService(store: storeExplicit, spanFinalizerEnabled: false)
        // For "the default arm", build the service from `.default` directly —
        // the contract is "leaving the config untouched produces today's
        // shape". `.default` doesn't take FM `.off` though, so build a
        // matching-shape config:
        let defaultConfig = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off
            // spanFinalizerEnabled omitted → init default applies → false
        )
        let serviceDefault = AdDetectionService(
            store: storeDefault,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: defaultConfig
        )

        let chunks = makeAdSignalChunks(assetId: assetId)

        try await serviceExplicit.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )
        try await serviceDefault.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let windowsExplicit = try await storeExplicit.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }
        let windowsDefault = try await storeDefault.fetchAdWindows(assetId: assetId)
            .sorted { $0.startTime < $1.startTime }

        // Same row count.
        #expect(
            windowsExplicit.count == windowsDefault.count,
            "explicit-OFF arm produced \(windowsExplicit.count) windows; default arm produced \(windowsDefault.count) — flag OFF must be byte-identical"
        )

        // playhead-p56a R1 review: byte-identity OFF was originally
        // start/end only. Tightened to assert every persisted field that
        // the wire-in could plausibly mutate when the flag flips to ON —
        // `confidence` (changes if the finalizer merges spans, taking
        // max), `decisionState` (changes if the finalizer demotes via
        // chapter penalty / action cap), `eligibilityGate` (same),
        // `wasSkipped` (downstream of the policy action). If any of these
        // diverges under flag OFF, the OFF path is not byte-identical to
        // shipping behaviour and the gate's "production behaviour is
        // unchanged" claim collapses. `confidence` is `Double`-typed; we
        // pin exact equality because the OFF path performs zero floating-
        // point math the ON path doesn't (it forwards the upstream value
        // verbatim into `buildFusionAdWindow`).
        for (a, b) in zip(windowsExplicit, windowsDefault) {
            #expect(a.startTime == b.startTime, "startTime mismatch under flag OFF")
            #expect(a.endTime == b.endTime, "endTime mismatch under flag OFF")
            #expect(a.confidence == b.confidence, "confidence mismatch under flag OFF — finalizer is OFF but the persisted skipConfidence differs")
            #expect(a.decisionState == b.decisionState, "decisionState mismatch under flag OFF — finalizer is OFF but the persisted decisionState differs")
            #expect(a.eligibilityGate == b.eligibilityGate, "eligibilityGate mismatch under flag OFF — finalizer is OFF but the persisted gate differs")
            #expect(a.wasSkipped == b.wasSkipped, "wasSkipped mismatch under flag OFF — finalizer is OFF but the persisted playback signal differs")
            #expect(a.boundaryState == b.boundaryState, "boundaryState mismatch under flag OFF")
            #expect(a.detectorVersion == b.detectorVersion, "detectorVersion mismatch under flag OFF")
            #expect(a.metadataSource == b.metadataSource, "metadataSource mismatch under flag OFF")
            #expect(a.metadataConfidence == b.metadataConfidence, "metadataConfidence mismatch under flag OFF")
            #expect(a.evidenceStartTime == b.evidenceStartTime, "evidenceStartTime mismatch under flag OFF")
        }

        // The constraint-trace map must be empty under flag OFF — no
        // allocation, no entries. Asserts the no-cost contract documented
        // on the field's docstring.
        let traceByWindow = await serviceExplicit.spanFinalizerConstraintsByWindowIdForTesting()
        let traceBySpan = await serviceExplicit.spanFinalizerConstraintsBySpanIdForTesting()
        #expect(traceByWindow.isEmpty, "flag OFF must leave the windowId trace empty")
        #expect(traceBySpan.isEmpty, "flag OFF must leave the spanId trace empty")
    }

    // MARK: - (c) Flag-ON wire-up

    @Test("Flag ON: runBackfill completes without throwing and does not produce more windows than flag-OFF on a clean fixture")
    func flagOnDoesNotRegressOnCleanFixture() async throws {
        // The wire-in path is a strict superset of the OFF path for any
        // candidate that the finalizer either keeps unchanged or drops
        // (suppressions can REDUCE the count; constraints #4/#5 may
        // DEMOTE a gate but never invent new spans). So flag ON must
        // produce <= the flag-OFF count.

        let storeOff = try await makeTestStore()
        let storeOn = try await makeTestStore()
        let assetId = "asset-p56a-clean"
        try await storeOff.insertAsset(makeAsset(id: assetId))
        try await storeOn.insertAsset(makeAsset(id: assetId))

        let chunks = makeAdSignalChunks(assetId: assetId)

        let serviceOff = makeService(store: storeOff, spanFinalizerEnabled: false)
        let serviceOn = makeService(store: storeOn, spanFinalizerEnabled: true)

        try await serviceOff.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )
        try await serviceOn.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let countOff = (try await storeOff.fetchAdWindows(assetId: assetId)).count
        let countOn = (try await storeOn.fetchAdWindows(assetId: assetId)).count

        #expect(
            countOn <= countOff,
            "flag ON produced MORE windows (\(countOn)) than flag OFF (\(countOff)) — SpanFinalizer should only keep, drop, or rewrite spans, never invent new ones"
        )
    }

    /// playhead-p56a R1 review: pins the END-TO-END trace-population
    /// contract. The hand-crafted `wireInTranslationTripsMergeConstraint`
    /// test below proves the finalizer fires the right constraints on
    /// the right inputs; this test proves the runBackfill wire-in
    /// actually populates `lastSpanFinalizerConstraintsBy{SpanId,WindowId}`
    /// and that the per-window key resolves to a non-nil trace.
    /// Constraint #6 (`policyOverrideApplied`) fires on EVERY kept span
    /// because the wire-in's `(.unknown, .unknown)` translation maps to
    /// `.detectOnly` and the finalizer's policy step always records the
    /// override when the action is not `.autoSkipEligible` (residual
    /// risk #6 from the implementer's self-review). Without this end-to-
    /// end pin, a future refactor that broke the per-window stamp inside
    /// the emission loop would only surface in the env-gated Catalyst
    /// dump path — too late.
    @Test("Flag ON end-to-end: at least one constraint fires on the clean fixture and stamps both trace maps")
    func flagOnEndToEndStampsTraceMaps() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-p56a-on-e2e"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store, spanFinalizerEnabled: true)

        let chunks = makeAdSignalChunks(assetId: assetId)
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        // The clean fixture produces at least one window (verified by the
        // OFF baseline test) — flag ON keeps that window AND tracks the
        // policy-override constraint trace on it. Both contracts must
        // hold or the wire-in is silently dropping the trace.
        let windows = try await store.fetchAdWindows(assetId: assetId)
        try #require(windows.count >= 1, "fixture must produce at least one window for the trace pin to be meaningful")

        let traceBySpan = await service.spanFinalizerConstraintsBySpanIdForTesting()
        let traceByWindow = await service.spanFinalizerConstraintsByWindowIdForTesting()
        #expect(!traceBySpan.isEmpty, "flag ON must populate the spanId trace map for kept spans")
        #expect(!traceByWindow.isEmpty, "flag ON must populate the windowId trace map for emitted windows")

        // Every entry in the spanId map must mirror in the windowId map
        // (every span that survives finalization produces exactly one
        // window per emission-loop iteration, modulo splits). The
        // mirroring is what the dump path consumes.
        for (_, trace) in traceByWindow {
            #expect(
                trace.contains(FinalizerConstraint.policyOverrideApplied.rawValue),
                "every window stamped with a trace under (.unknown, .unknown) must record policyOverrideApplied — got \(trace)"
            )
        }
    }

    /// Direct test of the same `SpanFinalizer.finalize(...)` invocation the
    /// wire-in performs. Hand-crafted candidates trip constraint #2
    /// (`mergedWithAdjacent`) — bd-4xqf's primary attribution target. The
    /// candidate translation here MUST match the runBackfill wire-in:
    /// `commercialIntent: .unknown, adOwnership: .unknown`. If a future
    /// refactor diverges either side, this test surfaces the drift.
    @Test("Wire-in translation: hand-crafted narrow-adjacent candidates trip constraint #2 (mergedWithAdjacent)")
    func wireInTranslationTripsMergeConstraint() {
        // Two non-overlapping spans with a 2-second gap (< 3s minimum
        // content gap). Constraint #1 (overlap) is NOT triggered;
        // constraint #2 fires and merges into one span.
        let assetId = "asset-p56a-merge"
        let firstSpan = DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 100, lastAtomOrdinal: 200),
            assetId: assetId,
            firstAtomOrdinal: 100,
            lastAtomOrdinal: 200,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: []
        )
        let secondSpan = DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: 300, lastAtomOrdinal: 400),
            assetId: assetId,
            firstAtomOrdinal: 300,
            lastAtomOrdinal: 400,
            startTime: 42.0,  // gap from previous endTime is 2s; below 3s minimum
            endTime: 72.0,
            anchorProvenance: []
        )
        let firstDecision = DecisionResult(
            proposalConfidence: 0.80,
            skipConfidence: 0.85,
            eligibilityGate: .eligible
        )
        let secondDecision = DecisionResult(
            proposalConfidence: 0.80,
            skipConfidence: 0.70,
            eligibilityGate: .eligible
        )

        // EXACT translation the wire-in does: (.unknown, .unknown) for
        // intent and ownership, episodeDuration from `self.episodeDuration`
        // (here a generous 3600s so the action cap never fires), empty
        // chapters so the chapter-penalty constraint never fires.
        let candidates: [CandidateSpan] = [
            CandidateSpan(
                span: firstSpan,
                decision: firstDecision,
                commercialIntent: .unknown,
                adOwnership: .unknown
            ),
            CandidateSpan(
                span: secondSpan,
                decision: secondDecision,
                commercialIntent: .unknown,
                adOwnership: .unknown
            ),
        ]
        let finalizer = SpanFinalizer(episodeDuration: 3600.0, chapters: [])
        let result = finalizer.finalize(candidates)

        // Exactly one finalized span — the two narrow-adjacent inputs
        // merged.
        #expect(result.count == 1, "expected merge to collapse two narrow-adjacent spans into one (got \(result.count))")
        let merged = result[0]
        // Spans into [10, 72] after merge.
        #expect(merged.span.startTime == 10.0)
        #expect(merged.span.endTime == 72.0)
        // Merged span takes the higher of the two confidences (0.85).
        #expect(merged.decision.skipConfidence == 0.85)
        // Constraint #2 fired.
        #expect(
            merged.constraintTrace.contains(.mergedWithAdjacent),
            "expected mergedWithAdjacent in trace; got \(merged.constraintTrace)"
        )
        // Constraint #6 ALSO fires because the unknown/unknown policy
        // matrix returns `.detectOnly` (not `.autoSkipEligible`). This
        // pins the policy-translation half of the wire-in contract: the
        // service uses the SAME (.unknown, .unknown) translation, so any
        // span the wire-in keeps will trip #6.
        #expect(
            merged.constraintTrace.contains(.policyOverrideApplied),
            "expected policyOverrideApplied; got \(merged.constraintTrace)"
        )
    }
}
