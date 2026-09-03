// UserMarkCueIdentityCanaryTests.swift
//
// playhead-1mq1.2. `PlayheadRuntime.injectUserMarkedAd` mints a `windowId`,
// hands it to `AdDetectionService.recordUserMarkedAd`, and then seeds the
// orchestrator's live cue. Before this bead it seeded that cue with the id and
// span it had minted. That is correct exactly when the correction is the FIRST
// one over that ad; for a repeat, the durable write resolves to the EXISTING
// row (possibly widened), and the cue was then a twin of a row that was never
// inserted.
//
// A twin is not a cosmetic problem. `eligibilityGate`, the bounds and the id
// all feed `AdWindowMaterialIdentity.producerRevisionToken`, and playhead-o4qr
// already paid for a version of this: a veto validated the producer revision,
// found the in-memory copy and the durable row disagreed, and silently did
// nothing — so correcting a freshly marked ad worked only after relaunching
// the app.
//
// No behavioural test reaches this seam: observing it needs a runtime with a
// begun episode, because `SkipOrchestrator.injectUserMarkedAd` returns early
// unless `activeAssetId` matches. This canary pins the wiring instead, and
// carries explicit anti-vacuity assertions so that it cannot pass by matching
// an empty or wrong region — the way playhead-m8rq's canary did.
//
// XCTest, so the class is filterable through a test plan's `skippedTests`.

import Foundation
import XCTest
@testable import Playhead

final class UserMarkCueIdentityCanaryTests: XCTestCase {

    func testLiveCueIsSeededFromTheResolvedDurableIdentity() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )
        let body = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: SwiftSourceInspector.strippingComments(source),
                after: "func injectUserMarkedAd(\n        start: Double,"
            ),
            "could not locate PlayheadRuntime.injectUserMarkedAd's body"
        )

        // Anti-vacuity. Every assertion below is about text INSIDE this body,
        // so a body that does not contain the two calls under discussion would
        // let the rest pass while proving nothing.
        XCTAssertTrue(
            body.contains("adDetectionService.recordUserMarkedAd("),
            "vacuous region: the durable write is not in the extracted body"
        )
        XCTAssertTrue(
            body.contains("skipOrchestrator.injectUserMarkedAd("),
            "vacuous region: the live cue seed is not in the extracted body"
        )
        XCTAssertTrue(
            body.contains("let windowId = UUID().uuidString"),
            "vacuous region: the locally minted id is not in the extracted body"
        )

        let cueCall = try XCTUnwrap(
            callArguments(
                in: body,
                after: "skipOrchestrator.injectUserMarkedAd("
            ),
            "could not read the arguments of the live cue seed"
        )
        XCTAssertFalse(
            cueCall.isEmpty,
            "vacuous region: the cue seed's argument list is empty"
        )

        // The claim. Each argument must come from the identity the durable
        // write RESOLVED to, never from the locally minted id or the span the
        // caller asked for.
        XCTAssertTrue(
            cueCall.contains("windowId: identity.windowId"),
            "the live cue must be seeded with the durable row's id, not the minted one"
        )
        XCTAssertTrue(
            cueCall.contains("start: identity.startTime"),
            "the live cue must carry the durable row's start, which a widen moves"
        )
        XCTAssertTrue(
            cueCall.contains("end: identity.endTime"),
            "the live cue must carry the durable row's end, which a widen moves"
        )
        XCTAssertFalse(
            cueCall.contains("windowId: windowId"),
            "seeding the cue with the locally minted id re-creates the playhead-o4qr twin"
        )
    }

    /// The text between a call's opening parenthesis and its matching close.
    /// Depth-counted rather than "up to the next `)`", so a nested call in an
    /// argument cannot truncate the region and make an assertion vacuous.
    private func callArguments(in body: String, after marker: String) -> String? {
        guard let markerRange = body.range(of: marker) else { return nil }
        var depth = 1
        var index = markerRange.upperBound
        var collected = ""
        while index < body.endIndex {
            let character = body[index]
            if character == "(" {
                depth += 1
            } else if character == ")" {
                depth -= 1
                if depth == 0 { return collected }
            }
            collected.append(character)
            index = body.index(after: index)
        }
        return nil
    }
}
