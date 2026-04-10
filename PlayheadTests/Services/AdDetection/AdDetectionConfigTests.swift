// AdDetectionConfigTests.swift
// Verifies the Phase 6 FM backfill configuration surfaces.

import Foundation
import Testing

@testable import Playhead

@Suite("AdDetectionConfig")
struct AdDetectionConfigTests {

    @Test("default config opts into shadow-mode FM backfill with Phase 6 defaults")
    func defaultConfigCarriesPhase6Defaults() {
        let config = AdDetectionConfig.default
        #expect(config.fmBackfillMode == .shadow)
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
}
