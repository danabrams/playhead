// PrecheckPersonaWiringSourceCanaryTests.swift
//
// playhead-4wzh. The production ranged sampler is built once, in
// PlayheadRuntime, and nothing instantiates the runtime's rediff wiring in a
// test — so the persona it probes under is pinned here, with the one unit
// fact it rests on pinned beside it.

import Foundation
import Testing
import XCTest
@testable import Playhead

final class PrecheckPersonaWiringSourceCanaryTests: XCTestCase {

    func testTheProductionPrecheckSamplerIsNotTheDownloadPersona() throws {
        let source = try SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.loadSource(repoRelativePath: "Playhead/App/PlayheadRuntime.swift")
        )
        let occurrences = SwiftSourceInspector.occurrences(of: "URLSessionRangedAudioSampler(", in: source)
        XCTAssertEqual(occurrences, 1, "vacuous region: expected exactly one production sampler construction")
        XCTAssertFalse(
            source.contains("URLSessionRangedAudioSampler(persona: .default)"),
            ".default IS the download persona; a same-context probe on a client-pinned show reads unchanged by construction"
        )
        XCTAssertTrue(
            source.contains("RediffFetchPersona.kWayPersonasDistinct(") && source.contains("from: .download, count: 1"),
            "the precheck must select its persona with the k-way distinct-from-download logic, not a hand-picked constant"
        )
    }
}

/// The unit fact the canary rests on: the selection it names really does
/// yield a persona, and not the download one.
@Suite("playhead-4wzh: the distinct selection is non-empty and not the download persona")
struct PrecheckPersonaSelectionTests {
    @Test func selectionIsDistinct() {
        let chosen = RediffFetchPersona.kWayPersonasDistinct(from: .download, count: 1).first
        #expect(chosen != nil, "an empty selection would make the sampler persona nil — a persona-less probe")
        #expect(chosen != RediffFetchPersona.download)
        #expect(chosen != RediffFetchPersona.default)
    }
}
