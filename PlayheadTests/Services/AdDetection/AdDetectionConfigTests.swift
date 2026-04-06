// AdDetectionConfigTests.swift
// Verifies the FM backfill mode toggle on AdDetectionConfig.

import Foundation
import Testing

@testable import Playhead

@Suite("AdDetectionConfig")
struct AdDetectionConfigTests {

    @Test("default config opts into shadow-mode FM backfill")
    func defaultIsShadow() {
        #expect(AdDetectionConfig.default.fmBackfillMode == .shadow)
    }

    @Test("FMBackfillMode is Codable round-trip")
    func fmBackfillModeCodable() throws {
        let modes: [FMBackfillMode] = [.disabled, .shadow, .enabled]
        for mode in modes {
            let data = try JSONEncoder().encode(mode)
            let decoded = try JSONDecoder().decode(FMBackfillMode.self, from: data)
            #expect(decoded == mode)
        }
    }

    @Test("FMBackfillMode covers exactly disabled, shadow, enabled")
    func fmBackfillModeCases() {
        let cases = Set(FMBackfillMode.allCases)
        #expect(cases == [.disabled, .shadow, .enabled])
    }
}
