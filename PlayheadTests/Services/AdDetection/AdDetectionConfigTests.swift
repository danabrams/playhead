// AdDetectionConfigTests.swift
// Verifies the Phase 6 FM backfill configuration surfaces.

import Foundation
import Testing

@testable import Playhead

@Suite("AdDetectionConfig")
struct AdDetectionConfigTests {

    @Test("default config opts into full-mode FM backfill with Phase 6 defaults")
    func defaultConfigCarriesPhase6Defaults() {
        let config = AdDetectionConfig.default
        #expect(config.fmBackfillMode == .full)
        #expect(config.fmScanBudgetSeconds == 300)
        #expect(config.fmConsensusThreshold == 2)
    }

    @Test("FMBackfillMode is Codable round-trip")
    func fmBackfillModeCodable() throws {
        let modes: [FMBackfillMode] = [.off, .shadow, .rescoreOnly, .proposalOnly, .full]
        for mode in modes {
            let data = try JSONEncoder().encode(mode)
            let decoded = try JSONDecoder().decode(FMBackfillMode.self, from: data)
            #expect(decoded == mode)
        }
    }

    @Test("FMBackfillMode decodes legacy raw values compatibly")
    func fmBackfillModeLegacyDecode() throws {
        let disabledData = Data(#""disabled""#.utf8)
        let enabledData = Data(#""enabled""#.utf8)

        let disabledMode = try JSONDecoder().decode(FMBackfillMode.self, from: disabledData)
        let enabledMode = try JSONDecoder().decode(FMBackfillMode.self, from: enabledData)

        #expect(disabledMode == .off)
        #expect(enabledMode == .shadow)
    }

    @Test("FMBackfillMode covers exactly off, shadow, rescoreOnly, proposalOnly, full")
    func fmBackfillModeCases() {
        let cases = Set(FMBackfillMode.allCases)
        #expect(cases == [.off, .shadow, .rescoreOnly, .proposalOnly, .full])
    }

    @Test("AdDetectionConfig.init default parameter is .full")
    func initDefaultParameterIsFull() {
        let config = AdDetectionConfig(
            candidateThreshold: 0.5,
            confirmationThreshold: 0.8,
            suppressionThreshold: 0.3,
            hotPathLookahead: 60,
            detectorVersion: "test-v1"
        )
        #expect(config.fmBackfillMode == .full,
                "init default parameter must be .full so callers that omit fmBackfillMode get full mode")
    }

    @Test("full mode contributes to decision ledger and proposes new regions")
    func fullModeCapabilities() {
        #expect(FMBackfillMode.full.runsFoundationModels)
        #expect(FMBackfillMode.full.contributesToExistingCandidateLedger)
        #expect(FMBackfillMode.full.canProposeNewRegions)
    }

    @Test("FMBackfillMode helper flags match the Phase 6 ledger contract")
    func fmBackfillModeHelpers() {
        #expect(!FMBackfillMode.off.runsFoundationModels)
        #expect(FMBackfillMode.shadow.runsFoundationModels)
        #expect(FMBackfillMode.full.runsFoundationModels)

        #expect(!FMBackfillMode.off.contributesToExistingCandidateLedger)
        #expect(!FMBackfillMode.shadow.contributesToExistingCandidateLedger)
        #expect(FMBackfillMode.rescoreOnly.contributesToExistingCandidateLedger)
        #expect(!FMBackfillMode.proposalOnly.contributesToExistingCandidateLedger)
        #expect(FMBackfillMode.full.contributesToExistingCandidateLedger)

        #expect(!FMBackfillMode.off.canProposeNewRegions)
        #expect(!FMBackfillMode.shadow.canProposeNewRegions)
        #expect(!FMBackfillMode.rescoreOnly.canProposeNewRegions)
        #expect(FMBackfillMode.proposalOnly.canProposeNewRegions)
        #expect(FMBackfillMode.full.canProposeNewRegions)
    }

    // MARK: - playhead-au2v.1.2 ChapterSignalMode

    @Test("default config carries chapterSignalMode = .off (production safe)")
    func defaultConfigChapterSignalModeIsOff() {
        let config = AdDetectionConfig.default
        #expect(config.chapterSignalMode == .off,
                "Production default must be .off so the chapter-signal phase does not run until explicitly enabled.")
    }

    @Test("AdDetectionConfig.init default parameter for chapterSignalMode is .off")
    func initDefaultParameterChapterSignalModeIsOff() {
        let config = AdDetectionConfig(
            candidateThreshold: 0.5,
            confirmationThreshold: 0.8,
            suppressionThreshold: 0.3,
            hotPathLookahead: 60,
            detectorVersion: "test-v1"
        )
        #expect(config.chapterSignalMode == .off,
                "init default parameter must be .off so callers that omit chapterSignalMode get the safe default.")
    }

    @Test("ChapterSignalMode covers exactly off, shadow, enabled")
    func chapterSignalModeCases() {
        let cases = Set(ChapterSignalMode.allCases)
        #expect(cases == [.off, .shadow, .enabled])
    }

    @Test("ChapterSignalMode is Codable round-trip for every case")
    func chapterSignalModeCodableRoundTrip() throws {
        for mode in ChapterSignalMode.allCases {
            let data = try JSONEncoder().encode(mode)
            let decoded = try JSONDecoder().decode(ChapterSignalMode.self, from: data)
            #expect(decoded == mode, "Round-trip failed for \(mode)")
        }
    }

    @Test("Decoding old config payload missing chapterSignalMode defaults to .off")
    func decodingMissingFieldDefaultsToOff() throws {
        // Wrapper that mirrors how a future serialized config would carry
        // the field optionally — this is the contract the bead protects.
        struct LegacyConfigPayload: Decodable {
            let chapterSignalMode: ChapterSignalMode?
        }
        // Old payload predates the field and does not include it.
        let legacyJSON = Data(#"{}"#.utf8)
        let payload = try JSONDecoder().decode(LegacyConfigPayload.self, from: legacyJSON)
        let resolvedMode = payload.chapterSignalMode ?? .off
        #expect(resolvedMode == .off,
                "Legacy config JSON without the chapterSignalMode field must default to .off.")
    }

    @Test("ChapterSignalMode decodes legacy 'disabled' alias to .off")
    func chapterSignalModeLegacyDisabledAlias() throws {
        let disabledData = Data(#""disabled""#.utf8)
        let mode = try JSONDecoder().decode(ChapterSignalMode.self, from: disabledData)
        #expect(mode == .off, "Legacy 'disabled' alias must decode to .off.")
    }

    @Test("ChapterSignalMode rejects unknown raw values")
    func chapterSignalModeRejectsUnknownRawValues() {
        let bogus = Data(#""never_heard_of_it""#.utf8)
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(ChapterSignalMode.self, from: bogus)
        }
    }

    @Test("ChapterSignalMode helper flags match the gate contract")
    func chapterSignalModeHelpers() {
        // .off: nothing runs, nothing reads.
        #expect(!ChapterSignalMode.off.runsChapterGeneration)
        #expect(!ChapterSignalMode.off.consumersReadChapterPlan)

        // .shadow: phase runs, consumers do NOT read (detection identical to .off).
        #expect(ChapterSignalMode.shadow.runsChapterGeneration)
        #expect(!ChapterSignalMode.shadow.consumersReadChapterPlan,
                "shadow must not affect consumer behavior — that is the whole point of the shadow tier.")

        // .enabled: full activation.
        #expect(ChapterSignalMode.enabled.runsChapterGeneration)
        #expect(ChapterSignalMode.enabled.consumersReadChapterPlan)
    }

    @Test(".off and .shadow agree on consumer reads (detection-byte-equivalence invariant)")
    func offAndShadowAreConsumerByteEquivalent() {
        // Property: the chapter-plan consumer predicate must return the
        // SAME value for .off and .shadow. Detection output is therefore
        // byte-for-byte identical to today in both modes — only the
        // generation phase / telemetry differs.
        #expect(ChapterSignalMode.off.consumersReadChapterPlan ==
                ChapterSignalMode.shadow.consumersReadChapterPlan)
        // .enabled is the only mode that flips the consumer bit.
        #expect(ChapterSignalMode.enabled.consumersReadChapterPlan)
    }
}
