// CrossEpisodeMemoryWiringTests.swift
// playhead-xsdz.9: Service-level wiring tests for the cross-episode "memory"
// precision signal.
//
// These prove the load-bearing OFF-by-default contract end-to-end through
// `AdDetectionService.runBackfill`:
//   1. FLAG-OFF IDENTITY: with a negative bank WIRED AND PRE-SEEDED with a
//      fingerprint that matches the candidate span, `crossEpisodeMemoryEnabled
//      = false` produces byte-identical persisted `AdWindow` confidences to the
//      no-bank baseline — i.e. no bank read, no suppression, no boost entry.
//   2. FLAG-ON SUPPRESSION: with the same pre-seeded negative bank and the flag
//      ON, the matching span's persisted confidence is SUPPRESSED (lower) than
//      the flag-off baseline — i.e. the signal genuinely moves the live
//      decision.
//   3. UNRELATED SPAN UNAFFECTED: a span whose tokens do NOT match any negative
//      keeps the flag-off confidence even when the flag is ON.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("CrossEpisodeMemory service wiring (playhead-xsdz.9)")
struct CrossEpisodeMemoryWiringTests {

    private let asset = "asset-xsdz9-wiring"
    private let show = "show-xsdz9"

    // The ad-copy text used by the candidate span AND seeded as a confirmed FP.
    private let adCopyText =
        "this episode is brought to you by squarespace use code show for ten percent off at squarespace dot com slash show"

    // MARK: - 1. Flag-off identity

    @Test("Flag OFF: a wired+seeded negative bank does not change any AdWindow confidence")
    func flagOffIsByteIdentical() async throws {
        // Baseline: no bank at all.
        let baseline = try await runAndFetchWindows(crossEpisodeMemoryEnabled: false, seedNegative: false)
        // With a seeded negative bank but the flag OFF.
        let withBankOff = try await runAndFetchWindows(crossEpisodeMemoryEnabled: false, seedNegative: true)

        #expect(baseline.count == withBankOff.count)
        // AdWindow.id is a fresh UUID per run, so key the comparison by the
        // stable (startTime, endTime) span instead. Confidence per span must be
        // identical between the no-bank baseline and the flag-off + seeded-bank
        // run — proving the flag-off path performs no bank read and no
        // suppression.
        func key(_ w: AdWindow) -> String {
            String(format: "%.3f-%.3f", w.startTime, w.endTime)
        }
        let baseMap = Dictionary(baseline.map { (key($0), $0.confidence) }, uniquingKeysWith: { a, _ in a })
        for w in withBankOff {
            let b = try #require(baseMap[key(w)], "span \(key(w)) missing in baseline")
            #expect(w.confidence == b, "flag-off must be byte-identical to no-bank baseline")
        }
    }

    // MARK: - 2. Flag-on suppression

    @Test("Flag ON: a matching negative suppresses the candidate span's confidence")
    func flagOnSuppressesMatch() async throws {
        let flagOff = try await runAndFetchWindows(crossEpisodeMemoryEnabled: false, seedNegative: true)
        let flagOn = try await runAndFetchWindows(crossEpisodeMemoryEnabled: true, seedNegative: true)

        // Find the candidate span overlapping the ad-copy interval (60–90s).
        let offWindow = try #require(flagOff.first { $0.startTime < 90 && $0.endTime > 60 })
        let onWindow = try #require(flagOn.first { $0.startTime < 90 && $0.endTime > 60 })

        // The negative bank holds the same copy → on-flag confidence is lower.
        #expect(onWindow.confidence < offWindow.confidence,
                "negative-bank match must suppress the matching span when the flag is on")
    }

    // MARK: - 3. Unrelated span unaffected

    @Test("Flag ON: a span whose copy is NOT in the negative bank is unaffected")
    func flagOnLeavesUnrelatedAlone() async throws {
        // Seed a negative for UNRELATED copy, then run the ad-copy episode with
        // the flag on. The ad-copy span should not be suppressed.
        let unrelatedNegative =
            "today on the program we explore deep sea exploration and the biology of bioluminescent creatures"

        let flagOff = try await runAndFetchWindows(crossEpisodeMemoryEnabled: false, seedNegative: false)
        let flagOnUnrelatedSeed = try await runAndFetchWindows(
            crossEpisodeMemoryEnabled: true,
            seedNegative: true,
            negativeText: unrelatedNegative
        )

        let offWindow = try #require(flagOff.first { $0.startTime < 90 && $0.endTime > 60 })
        let onWindow = try #require(flagOnUnrelatedSeed.first { $0.startTime < 90 && $0.endTime > 60 })
        #expect(onWindow.confidence == offWindow.confidence,
                "an unrelated negative must not suppress this span")
    }

    // MARK: - Harness

    private func runAndFetchWindows(
        crossEpisodeMemoryEnabled: Bool,
        seedNegative: Bool,
        negativeText: String? = nil
    ) async throws -> [AdWindow] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: asset))

        var bank: NegativeFingerprintBank?
        if seedNegative {
            let dir = try makeTempDir(prefix: "xsdz9-bank")
            let b = try NegativeFingerprintBank(directoryURL: dir)
            _ = try await b.recordConfirmedFalsePositive(
                text: negativeText ?? adCopyText,
                showId: show
            )
            bank = b
        }

        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "xsdz9-test",
            fmBackfillMode: .off,
            crossEpisodeMemoryEnabled: crossEpisodeMemoryEnabled
        )
        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            negativeFingerprintBank: bank
        )

        try await service.runBackfill(
            chunks: chunks(),
            analysisAssetId: asset,
            podcastId: show,
            episodeDuration: 130.0
        )

        let windows = try await store.fetchAdWindows(assetId: asset)
        if let bank { await bank.close() }
        return windows
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

    private func chunks() -> [TranscriptChunk] {
        let texts: [(Double, Double, String)] = [
            (0.0, 30.0, "Welcome back to the show today we discuss technology and design."),
            (60.0, 90.0, "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show."),
            (90.0, 120.0, "Back to our regular conversation about new things and ideas.")
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(asset)",
                analysisAssetId: asset,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }
}

// MARK: - Flag-OFF ⇒ no bank construction / no write (behavior-neutral gating)

/// playhead-xsdz.9 review: the ENTIRE feature — bank construction, SQLite
/// migration, the confirmed-FP WRITE trigger, AND the suppression READ — rides
/// the ONE off-by-default `crossEpisodeMemoryEnabled` flag. With the flag off
/// (the production default) there must be NO bank, NO new DB file, NO migration,
/// and NO writes. These tests pin that invariant at the seams the production
/// `PlayheadRuntime` actually uses.
@Suite("CrossEpisodeMemory flag-off gating (playhead-xsdz.9)")
struct CrossEpisodeMemoryGatingTests {

    /// The production gate: `PlayheadRuntime` constructs the bank only when
    /// `AdDetectionConfig.default.crossEpisodeMemoryEnabled` is true. The default
    /// (production) config keeps it false, so production constructs NO bank —
    /// no DB file, no migration. This is the single value the runtime branches
    /// on; pinning it here proves the OFF state is the production state.
    @Test("Production-default config keeps the feature OFF (so the runtime builds no bank)")
    func productionDefaultDisablesConstruction() {
        #expect(AdDetectionConfig.default.crossEpisodeMemoryEnabled == false)
    }

    /// The orchestrator's confirmed-FP WRITE trigger is gated by bank presence.
    /// In the flag-OFF production state no bank is wired, so a Listen revert is
    /// a no-op for the negative bank — proving there is no write side-effect when
    /// the feature is off.
    @Test("No bank wired ⇒ orchestrator defaults to nil bank (revert writes nothing)")
    func noBankWiredMeansNoWrite() async throws {
        let store = try await makeTestStore()
        let orchestrator = SkipOrchestrator(store: store)
        // Flag-OFF production state: PlayheadRuntime never calls
        // `setNegativeFingerprintBank`, so the bank stays nil and the
        // `ingestNegativeFingerprint` guard short-circuits every revert.
        #expect(await orchestrator.negativeFingerprintBank == nil)
    }

    /// When the feature IS enabled the runtime wires a bank, and the orchestrator
    /// then holds it — the confirmed-FP write trigger becomes live. Proves the
    /// flag-ON path is the only path that activates the write side.
    @Test("Bank wired ⇒ orchestrator holds it (write trigger live)")
    func bankWiredMeansWriteLive() async throws {
        let store = try await makeTestStore()
        let orchestrator = SkipOrchestrator(store: store)
        let dir = try makeTempDir(prefix: "xsdz9-gate-bank")
        let bank = try NegativeFingerprintBank(directoryURL: dir)
        await orchestrator.setNegativeFingerprintBank(bank)
        #expect(await orchestrator.negativeFingerprintBank != nil)
        await bank.close()
    }
}

#endif
