// DayZeroReadinessProbeSourceCanaryTests.swift
//
// playhead-66cn. The readiness probe is a closure built inside PlayheadRuntime;
// nothing instantiates that wiring in a test, so the resolution it performs is
// pinned here with anti-vacuity checks.

import Foundation
import XCTest
@testable import Playhead

final class DayZeroReadinessProbeSourceCanaryTests: XCTestCase {

    func testTheProbeResolvesByFingerprintNotByEpisodeAlone() throws {
        let source = try SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.loadSource(repoRelativePath: "Playhead/App/PlayheadRuntime.swift")
        )
        guard let probeStart = source.range(of: "probe: {") else {
            XCTFail("could not locate the readiness probe")
            return
        }
        let body = String(source[probeStart.upperBound...].prefix(1_600))
        XCTAssertTrue(body.contains(".awaitingPinnedFile"), "vacuous region: this is not the readiness probe")
        XCTAssertTrue(body.contains(".ready(DayZeroKickoffReady("), "vacuous region: the ready branch is not here")

        XCTAssertTrue(
            body.contains("fetchAssetByEpisodeId(\n                              episodeId, assetFingerprint: fingerprint")
                || body.contains("fetchAssetByEpisodeId(episodeId, assetFingerprint: fingerprint"),
            "the probe must resolve the row by the pinned file's fingerprint"
        )
        XCTAssertTrue(body.contains("canonicalFingerprint(of: playedFileURL)"), "the fingerprint must come from the pinned file itself")
        XCTAssertFalse(
            body.contains("fetchAssetByEpisodeId(episodeId))"),
            "the bare episode fetch is back: the newest row wins, and after a re-download that is the stale one"
        )
    }
}
