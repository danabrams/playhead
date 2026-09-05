// ShadowRungSourceCanaryTests.swift
//
// playhead-zeh0. The weighted sibling of evaluatePromotion is reached only
// through a per-detector ledger entry, which the behavioural suite does not
// construct; its clause is pinned here beside the count-based one.

import Foundation
import XCTest
@testable import Playhead

final class ShadowRungSourceCanaryTests: XCTestCase {

    func testBothShadowArmsGateOnTheVetoCounter() throws {
        let source = try SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.loadSource(repoRelativePath: "Playhead/Services/TrustScoring/TrustScoringService.swift")
        )
        XCTAssertEqual(SwiftSourceInspector.occurrences(of: "fileprivate static func evaluatePromotion(", in: source), 2,
                       "vacuous region: expected the count-based and weighted overloads")
        XCTAssertTrue(source.contains("&& recentFalseSignals == 0 {\n                return .manual"),
                      "the count-based shadow arm does not gate on outstanding vetoes")
        XCTAssertTrue(source.contains("&& falseSkipWeight == 0 {\n                return .manual"),
                      "the weighted shadow arm does not gate on outstanding veto weight")
    }
}
