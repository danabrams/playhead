// CohortBuildContractTests.swift
// playhead-i7kvl.2: the three things a TestFlight cohort build must be true of,
// asserted rather than assumed.
//
// Two of them were VACUOUS or DEAD when this bead was picked up, and both are
// the same shape — a claim nobody had checked because checking it means asking
// whether something is CALLED, not whether it exists.

import Foundation
import Testing

@testable import Playhead

@Suite("Cohort build contract (playhead-i7kvl.2)")
struct CohortBuildContractTests {

    private static func source(_ relativePath: String) throws -> String {
        let root = try #require(
            SwiftSourceInspector.repositoryRoot(from: #filePath),
            "could not locate the repository root from \(#filePath)"
        )
        return try String(
            contentsOf: root.appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }

    // MARK: 1. No entitlement gate — asserted, not assumed

    /// THE VACUOUS CRITERION, MADE REAL. This bead's first acceptance criterion
    /// was "no paywall, unlimited analysis, no budget state reachable". Dan
    /// deferred the free tier on 2026-09-02, so there is no gate to disable and
    /// that criterion became a passing check on a mechanism that does not
    /// exist — which is worth nothing.
    ///
    /// Restated as the thing actually worth guaranteeing: **no entitlement
    /// decision reaches the analysis, download or skip paths.** If somebody
    /// later adds one without deciding to, a cohort tester silently loses
    /// analysis and the reports get blamed on detection.
    @Test("THE ACCEPTANCE: no entitlement check gates analysis, downloads or skips")
    func noEntitlementGateOnTheListeningPaths() throws {
        let root = try #require(SwiftSourceInspector.repositoryRoot(from: #filePath))
        let gatedDirectories = [
            "Playhead/Services/AdDetection",
            "Playhead/Services/PreAnalysis",
            "Playhead/Services/Downloads",
            "Playhead/Services/SkipOrchestration",
        ]
        var offenders: [String] = []
        for directory in gatedDirectories {
            let url = root.appendingPathComponent(directory)
            guard let walker = FileManager.default.enumerator(
                at: url, includingPropertiesForKeys: nil
            ) else { continue }
            for case let file as URL in walker where file.pathExtension == "swift" {
                guard let code = try? String(contentsOf: file, encoding: .utf8) else { continue }
                if code.contains("isPremium") || code.contains("PreviewBudgetStore") {
                    offenders.append(file.lastPathComponent)
                }
            }
        }
        #expect(
            offenders.isEmpty,
            """
            An entitlement check reached a listening path (\(offenders.joined(separator: ", "))). \
            The cohort build is premium-unlocked BY CONSTRUCTION — nothing gates — \
            and a gate added here would silently cost testers their analysis.
            """
        )
    }

    /// ANTI-VACUITY for the check above: it is a directory walk, and a walk that
    /// finds no files passes for the worst possible reason.
    @Test("the gate scan really walked source files")
    func gateScanIsNotVacuous() throws {
        let root = try #require(SwiftSourceInspector.repositoryRoot(from: #filePath))
        let url = root.appendingPathComponent("Playhead/Services/AdDetection")
        let walker = try #require(
            FileManager.default.enumerator(at: url, includingPropertiesForKeys: nil)
        )
        var swiftFiles = 0
        for case let file as URL in walker where file.pathExtension == "swift" {
            swiftFiles += 1
        }
        #expect(swiftFiles > 20, "found \(swiftFiles) files — the walk is not reaching the source")
    }

    // MARK: 2. The crash pipeline is actually STARTED

    /// THE DEAD ONE. `MetricKitDiagnosticsSubscriber.install` was written,
    /// tested and canaried — and had **zero production callers**, so no crash or
    /// hang report has ever been collected. `playhead-jw63.4` shipped the
    /// machinery and closed; nothing started it.
    ///
    /// The existing `MetricKitDiagnosticsWiringSourceCanaryTests` checks what
    /// the subscriber READS and that it holds a strong reference — all true, all
    /// about a component nobody constructed. This asserts the missing half.
    @Test("the crash/hang pipeline is installed on the launch path")
    func crashPipelineIsInstalled() throws {
        let app = try Self.source("Playhead/App/PlayheadApp.swift")
        #expect(
            app.contains("MetricKitDiagnosticsInstaller.install()"),
            """
            Nothing calls MetricKitDiagnosticsInstaller.install, so nothing installs the subscriber, so the crash/hang \
            pipeline collects nothing. A cohort week of crashes would be \
            invisible.
            """
        )
    }

    /// It must be on a path that runs on EVERY launch — the playhead-m8rq
    /// lesson, one bead later. A crash on a headless wake is exactly the crash
    /// worth having.
    @Test("the install is on the every-launch path, not inside the scene")
    func crashPipelineInstallIsNotSceneOnly() throws {
        let app = try Self.source("Playhead/App/PlayheadApp.swift")
        let bodyStart = try #require(app.range(of: "var body: some Scene")?.lowerBound)
        let initStart = try #require(app.range(of: "init() {")?.upperBound)

        var depth = 1
        var index = initStart
        while index < app.endIndex, depth > 0 {
            if app[index] == "{" { depth += 1 }
            if app[index] == "}" { depth -= 1 }
            if depth == 0 { break }
            index = app.index(after: index)
        }
        let initBody = app[initStart..<index]

        #expect(index < bodyStart, "init's body must close before the scene begins")
        #expect(
            initBody.contains("MetricKitDiagnosticsInstaller.install()"),
            "the install is scene-scoped, so a headless launch collects nothing"
        )
    }

    // MARK: 3. The feedback channel has somewhere to go

    @Test("the feedback recipient is a single non-empty pinned constant")
    func feedbackRecipientExists() {
        #expect(!ListenerFeedbackCopy.recipient.isEmpty)
        #expect(
            ListenerFeedbackCopy.recipient.contains("@"),
            """
            The in-app feedback channel is one of two ways a cohort tester can \
            reach Dan. A recipient that cannot receive mail turns "tell us \
            anything in ten seconds" into a silent dead end.
            """
        )
    }
}
