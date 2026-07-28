// AnalyticsSourceCanaryTests.swift
// playhead-jw63.3 — structural invariants a runtime test cannot express.
//
// Three claims live here:
//   1. The +30s counter has exactly two call sites and they are disjoint,
//      so one listener reach is one increment. If someone later routes the
//      runtime's skip through the transport's `skipForward`, every reach
//      would be counted twice and every runtime test would still pass —
//      this canary is what fails instead.
//   2. Ad auto-skip never touches the counter. The counter's whole meaning
//      is "the listener still had to do it themselves"; an automatic skip
//      contaminating it would make the north-star metric measure the
//      opposite of what it claims.
//   3. The analytics subsystem is inert: no BGTask, no URLSession, no
//      CloudKit read path, no episode/transcript symbols outside the one
//      file whose job is to drop them.

import Foundation
import XCTest

@testable import Playhead

final class AnalyticsSourceCanaryTests: XCTestCase {

    private func productionRoot() -> URL {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        return root.appendingPathComponent("Playhead", isDirectory: true)
    }

    private func source(_ relativePath: String) throws -> String {
        try String(
            contentsOf: productionRoot().appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }

    /// Strips whole-line comments. These canaries are about what the code
    /// does; prose that explains why a field is forbidden legitimately names
    /// the field, and a canary that fires on its own rationale is a canary
    /// people delete.
    private func codeOnly(_ text: String) -> String {
        text.split(separator: "\n", omittingEmptySubsequences: false)
            .filter { !$0.trimmingCharacters(in: .whitespaces).hasPrefix("//") }
            .joined(separator: "\n")
    }

    private func analyticsSources() throws -> [(name: String, source: String)] {
        let directory = productionRoot()
            .appendingPathComponent("Services/Analytics", isDirectory: true)
        let contents = try FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil
        )
        return try contents
            .filter { $0.pathExtension == "swift" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
            .map { ($0.lastPathComponent, codeOnly(try String(contentsOf: $0, encoding: .utf8))) }
    }

    func testManualSkipCounterHasExactlyTwoCallSites() throws {
        let productionFiles = try FileManager.default.enumerator(
            at: productionRoot(),
            includingPropertiesForKeys: nil
        )
        let enumerator = try XCTUnwrap(productionFiles)

        var callSites: [String] = []
        for case let fileURL as URL in enumerator where fileURL.pathExtension == "swift" {
            let name = fileURL.lastPathComponent
            // The recorder's own declaration is not a call site.
            guard name != "AnalyticsCohort.swift" else { continue }
            let text = codeOnly(try String(contentsOf: fileURL, encoding: .utf8))
            let occurrences = text.components(
                separatedBy: "AnalyticsRecorder.manualSkipForwardReach"
            ).count - 1
            for _ in 0..<occurrences {
                callSites.append(name)
            }
        }

        XCTAssertEqual(
            callSites.sorted(),
            ["PlaybackTransport.swift", "PlayheadRuntime.swift"],
            "the +30s counter must be incremented once per user-initiated "
                + "forward-skip path and nowhere else"
        )
    }

    func testRuntimeSkipForwardDoesNotDelegateToTransportSkipForward() throws {
        // The two instrumented functions must stay disjoint. If the runtime
        // ever calls through to the transport's `skipForward`, one tap
        // increments the counter twice and the north-star ratio doubles.
        let runtime = codeOnly(try source("App/PlayheadRuntime.swift"))
        XCTAssertFalse(
            runtime.contains("playbackService.skipForward"),
            "PlayheadRuntime must compute its own +30s target; delegating to "
                + "PlaybackService.skipForward would double-count every reach"
        )
    }

    func testAutomaticSkipPathNeverIncrementsTheManualCounter() throws {
        let orchestrator = codeOnly(
            try source("Services/SkipOrchestrator/SkipOrchestrator.swift")
        )
        XCTAssertFalse(orchestrator.contains("AnalyticsRecorder"))
        XCTAssertFalse(orchestrator.contains("manualSkipForwardReach"))

        // The transport's automatic skip machinery lives alongside the
        // instrumented remote-command function; assert the instrumentation
        // sits in `skipForward` and not in the auto-skip transition.
        let transport = codeOnly(
            try source("Services/PlaybackTransport/PlaybackTransport.swift")
        )
        let autoSkipRange = try XCTUnwrap(
            transport.range(of: "func performReservedSkipTransition")
        )
        let afterAutoSkip = String(transport[autoSkipRange.lowerBound...])
        XCTAssertFalse(
            afterAutoSkip.contains("AnalyticsRecorder"),
            "auto-skip must not increment the manual +30s counter"
        )
    }

    func testAnalyticsNeverSchedulesBackgroundWorkOrOpensASocket() throws {
        for file in try analyticsSources() {
            for forbidden in [
                "BGTaskScheduler", "BGProcessingTaskRequest", "BGAppRefreshTaskRequest",
                "URLSession", "Timer.", "DispatchSourceTimer",
                "UNUserNotificationCenter", "beginBackgroundTask",
            ] {
                XCTAssertFalse(
                    file.source.contains(forbidden),
                    "\(file.name) must not use \(forbidden) — analytics may "
                        + "never wake the device or compete with analysis work"
                )
            }
        }
    }

    func testAnalyticsTransportIsWriteOnly() throws {
        for file in try analyticsSources() {
            for forbidden in [
                "CKQuery", "CKQueryOperation", "CKSubscription", "CKFetch",
                "privateCloudDatabase", "sharedCloudDatabase",
                ".records(matching", "fetchAll", "userRecordID",
            ] {
                XCTAssertFalse(
                    file.source.contains(forbidden),
                    "\(file.name) must not read from CloudKit — the analytics "
                        + "transport is write-only by design"
                )
            }
        }
    }

    func testAnalyticsCarriesNoEpisodeOrTranscriptSymbols() throws {
        // `AnalyticsCohort.swift` is the one file allowed to name these: it
        // exists to accept them from a caller and throw them away.
        for file in try analyticsSources() where file.name != "AnalyticsCohort.swift" {
            for forbidden in [
                "episodeTitle", "showTitle", "feedURL", "transcript",
                "Transcript", "episodeId", "podcastId", "assetId",
            ] {
                XCTAssertFalse(
                    file.source.contains(forbidden),
                    "\(file.name) must not reference \(forbidden) — episode "
                        + "content may not reach an outbound record"
                )
            }
        }
    }

    func testCohortResolverReadsOnlyDuration() throws {
        let text = codeOnly(try source("Services/Analytics/AnalyticsCohort.swift"))
        let resolverStart = try XCTUnwrap(text.range(of: "enum AnalyticsCohortResolver"))
        let resolver = String(text[resolverStart.lowerBound...])
        for forbidden in [
            "context.episodeTitle", "context.showTitle", "context.feedURL",
            "context.episodeId", "context.adWindowTranscript",
        ] {
            XCTAssertFalse(
                resolver.contains(forbidden),
                "the resolver must read durationSeconds and nothing else"
            )
        }
        XCTAssertTrue(resolver.contains("context.durationSeconds"))
    }
}
