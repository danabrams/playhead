// TerminalReasonPersistenceTests.swift
// playhead-gtt9.8: the new `analysis_assets.terminalReason` column is
// written atomically with the terminal `analysisState` via the
// `updateAssetState(id:state:terminalReason:)` overload added alongside
// the migration v14 bump.
//
// These tests verify:
//   1. After inserting an asset and invoking the terminalReason
//      overload, `fetchAsset(id:)` returns an asset whose
//      `terminalReason` equals what was written.
//   2. A subsequent call with `nil` clears the field back to nil.
//   3. The plain `updateAssetState(id:state:)` overload never mutates
//      a pre-existing `terminalReason` (orthogonal writes).

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisAsset.terminalReason round-trip — gtt9.8")
struct TerminalReasonPersistenceTests {

    private func makeAsset(id: String = "asset-term-\(UUID().uuidString)") -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    @Test("updateAssetState writes terminalReason; fetchAsset round-trips")
    func terminalReasonRoundTrips() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset()
        try await store.insertAsset(asset)

        try await store.updateAssetState(
            id: asset.id,
            state: "completeFull",
            terminalReason: "full coverage: transcript 0.981, feature 0.992"
        )

        let reloaded = try await store.fetchAsset(id: asset.id)
        #expect(reloaded?.analysisState == "completeFull")
        #expect(reloaded?.terminalReason == "full coverage: transcript 0.981, feature 0.992")
    }

    @Test("terminalReason can be cleared by passing nil")
    func terminalReasonClearsWhenNil() async throws {
        let store = try await makeTestStore()
        let asset = makeAsset()
        try await store.insertAsset(asset)

        try await store.updateAssetState(
            id: asset.id,
            state: "failedTranscript",
            terminalReason: "transcript pipeline error"
        )
        var reloaded = try await store.fetchAsset(id: asset.id)
        #expect(reloaded?.terminalReason == "transcript pipeline error")

        try await store.updateAssetState(
            id: asset.id,
            state: "queued",
            terminalReason: nil
        )
        reloaded = try await store.fetchAsset(id: asset.id)
        #expect(reloaded?.terminalReason == nil)
    }

    @Test("plain updateAssetState(id:state:) preserves prior terminalReason")
    func plainUpdateDoesNotClobberTerminalReason() async throws {
        // The classifier writes (state, reason) atomically via the
        // 3-arg overload. Legacy callers using the 2-arg overload must
        // NOT nuke a previously-written reason — the column is
        // independent telemetry, not a sticky piece of state that the
        // pipeline's mid-run transitions should touch.
        let store = try await makeTestStore()
        let asset = makeAsset()
        try await store.insertAsset(asset)

        try await store.updateAssetState(
            id: asset.id,
            state: "completeTranscriptPartial",
            terminalReason: "partial transcript 600/3600s (ratio 0.167 < 0.950)"
        )
        try await store.updateAssetState(
            id: asset.id,
            state: "queued" // e.g. coverage-guard recovery sweep
        )

        let reloaded = try await store.fetchAsset(id: asset.id)
        #expect(reloaded?.analysisState == "queued")
        #expect(reloaded?.terminalReason == "partial transcript 600/3600s (ratio 0.167 < 0.950)")
    }
}
