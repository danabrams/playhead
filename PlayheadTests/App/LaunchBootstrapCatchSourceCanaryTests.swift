// LaunchBootstrapCatchSourceCanaryTests.swift
//
// playhead-h9y6. Nothing instantiates PlayheadRuntime's launch task in a test,
// so the one claim that matters — the catch is no longer empty — is pinned
// here, with the anti-vacuity checks the m8rq canary lacked.

import Foundation
import XCTest
@testable import Playhead

final class LaunchBootstrapCatchSourceCanaryTests: XCTestCase {

    func testTheBootstrapCatchLogsAndCounts() throws {
        let source = try SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.loadSource(repoRelativePath: "Playhead/App/PlayheadRuntime.swift")
        )
        let call = "try await downloadManager.bootstrap()"
        XCTAssertEqual(SwiftSourceInspector.occurrences(of: call, in: source), 1, "vacuous region: the bootstrap call is not exactly once")
        let callRange = try XCTUnwrap(source.range(of: call))
        let catchStart = try XCTUnwrap(source.range(of: "} catch {", range: callRange.upperBound..<source.endIndex))
        let catchBody = try XCTUnwrap(
            SwiftSourceInspector.bracedBody(in: source, startingAt: source.index(before: catchStart.upperBound)),
            "could not extract the catch body"
        )
        XCTAssertFalse(catchBody.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty, "the catch is EMPTY again — a launch failure nobody can see")
        XCTAssertTrue(catchBody.contains("logger.error("), "the failure must be logged")
        XCTAssertTrue(
            catchBody.contains("LaunchHealthRecorder.shared.recordDownloadBootstrapFailure("),
            "the failure must be counted where a diagnostics bundle can read it"
        )
    }
}
