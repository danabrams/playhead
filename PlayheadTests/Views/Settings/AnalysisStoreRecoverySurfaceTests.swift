// AnalysisStoreRecoverySurfaceTests.swift
// The listener-facing half of "retry, then surface and let them decide"
// (playhead-wvdz).
//
// Three things are worth pinning about this surface, and only one of
// them is copy:
//
//   1. IT IS NOT REACHABLE BY ACCIDENT. "Start fresh" is the only
//      gesture in the app that moves the listener's analysis library.
//      The gate on it — a status the app only reaches after repeated
//      failures, plus a confirmation — is the entire difference between
//      this and the bug it replaces.
//   2. LAUNCH IS NOT BLOCKED. No `.alert` on a launch path, no modal, no
//      wait-for-a-person anywhere the app starts up.
//   3. THE COPY TELLS THE TRUTH ABOUT WHAT IS GONE. It has to say that
//      nothing was deleted and that playback still works, because the
//      listener's alternative reading is "the app is broken" and their
//      next action is to delete it — which really would destroy the data.
//
// Source-level assertions for the structural claims, in the same idiom
// as `DebugDiagnosticsHatchSourceCanaryTests`: `SettingsView` is a
// SwiftUI body with no seam a unit test can drive, and the properties
// under test are about which code exists where.

import Foundation
import Testing

@testable import Playhead

@Suite("Analysis-history recovery surface (playhead-wvdz)")
struct AnalysisStoreRecoverySurfaceTests {

    private static let repoRoot: URL = {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // .../Settings/
            .deletingLastPathComponent() // .../Views/
            .deletingLastPathComponent() // .../PlayheadTests/
            .deletingLastPathComponent() // .../<repo root>/
    }()

    private func read(_ relative: String) throws -> String {
        try String(contentsOf: Self.repoRoot.appendingPathComponent(relative), encoding: .utf8)
    }

    // MARK: - 1. Not reachable by accident

    /// The destructive-looking action must be gated on the status the
    /// app only reaches after repeated failures. Rendering it
    /// unconditionally would put "set my library aside" one tap from a
    /// healthy install.
    @Test("Start fresh is gated on awaitingUserDecision and behind a confirmation")
    func startFreshIsGatedAndConfirmed() throws {
        let source = try read("Playhead/Views/Settings/SettingsView.swift")
        let stripped = SwiftSourceInspector.strippingComments(source)

        let body = try #require(
            SwiftSourceInspector.firstBody(
                in: stripped, after: "private var analysisHistoryRecoveryRows"
            )
        )
        #expect(body.contains("health.status == .awaitingUserDecision"))
        #expect(body.contains("analysisHistoryStartFreshButtonLabel"))
        #expect(body.contains("showStartFreshConfirmation = true"))
        #expect(body.contains(".confirmationDialog("))
        // The button itself must not call the action directly — it sets
        // the confirmation flag, and only the dialog's destructive
        // button runs it.
        // Exactly ONE call site, then ordering. `range(of:)` finds only
        // the first occurrence, so an ordering check alone would still
        // pass if somebody added a SECOND, ungated invocation after the
        // dialog.
        #expect(
            SwiftSourceInspector.occurrences(of: "startFreshAnalysisHistory()", in: body) == 1
        )
        let dialogStart = try #require(body.range(of: ".confirmationDialog("))
        let actionCall = try #require(body.range(of: "startFreshAnalysisHistory()"))
        #expect(dialogStart.lowerBound < actionCall.lowerBound)
    }

    /// `quarantineAndRebuild` is the only thing that moves the library.
    /// Nothing but an explicit gesture may reach it — no timer, no
    /// launch path, no background task.
    @Test("The quarantine path is reachable only from the recovery hatch")
    func quarantineIsOnlyReachableFromTheHatch() throws {
        // The WHOLE production tree, not a hand-listed set of
        // subdirectories. A canary whose entire value is exhaustiveness
        // must not be able to miss a directory somebody adds later — the
        // first version of this test listed six roots and silently
        // ignored `Playhead/Models`, `Playhead/Design` and
        // `Playhead/Resources`.
        let productionRoot = Self.repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        var callSites: [String] = []
        let walker = try #require(
            FileManager.default.enumerator(at: productionRoot, includingPropertiesForKeys: nil)
        )
        for case let url as URL in walker where url.pathExtension == "swift" {
            guard let text = try? String(contentsOf: url, encoding: .utf8) else { continue }
            let stripped = SwiftSourceInspector.strippingComments(text)
            guard stripped.contains("quarantineAndRebuild") else { continue }
            callSites.append(url.lastPathComponent)
        }
        // The declaration (both overloads) plus the single hatch
        // forwarder. Anything else is a new way to reach the listener's
        // data without the listener.
        #expect(
            Set(callSites) == [
                "AnalysisStoreRecoveryCoordinator.swift",
                "AnalysisStoreRecoveryHatch.swift"
            ],
            "Unexpected `quarantineAndRebuild` call sites: \(callSites.sorted())"
        )
    }

    // MARK: - 2. Launch is not blocked

    /// Someone opening the app to play a podcast must be able to play a
    /// podcast. A modal on the launch path is the specific failure this
    /// guards against.
    @Test("The recovery surface adds no alert and lives in Settings, not on a launch path")
    func launchIsNeverBlocked() throws {
        let runtime = try read("Playhead/App/PlayheadRuntime.swift")
        let app = try read("Playhead/App/PlayheadApp.swift")
        let content = try read("Playhead/App/ContentView.swift")

        for source in [runtime, app, content] {
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
            #expect(!stripped.contains("analysisHistoryRecovery"))
            #expect(!stripped.contains("showStartFreshConfirmation"))
        }

        // And the surface itself is a confirmationDialog fired by a
        // button, never an `.alert` bound to launch state.
        let settings = SwiftSourceInspector.strippingComments(
            try read("Playhead/Views/Settings/SettingsView.swift")
        )
        let body = try #require(
            SwiftSourceInspector.firstBody(
                in: settings, after: "private var analysisHistoryRecoveryRows"
            )
        )
        #expect(!body.contains(".alert("))
    }

    // MARK: - 3. The copy tells the truth

    /// Verbatim pins. Each of these is a product decision, and the
    /// reason for each is recorded beside the string in `SettingsL274`.
    @Test("Recovery copy is verbatim")
    func copyIsVerbatim() {
        #expect(SettingsL274Copy.analysisHistoryRecoveryTitle == "Analysis history")
        #expect(
            SettingsL274Copy.analysisHistoryRecoveryBody
                == "Playhead couldn't open your analysis history. Nothing has been deleted, and playback isn't affected — only what Playhead has learned about your episodes is unavailable."
        )
        #expect(SettingsL274Copy.analysisHistoryRetryButtonLabel == "Try again")
        #expect(SettingsL274Copy.analysisHistoryStartFreshButtonLabel == "Start fresh")
        #expect(SettingsL274Copy.analysisHistoryStartFreshConfirmTitle == "Start fresh?")
        #expect(
            SettingsL274Copy.analysisHistoryStartFreshConfirmBody
                == "Your existing analysis history — including everything you've marked by hand — is set aside on this device rather than deleted, and Playhead starts over with an empty one. Playhead won't use the old one again, but it stays on your device and it isn't erased."
        )
        #expect(
            SettingsL274Copy.analysisHistoryStartFreshConfirmAction == "Set aside and start fresh"
        )
        #expect(SettingsL274Copy.analysisHistoryStartFreshCancelAction == "Cancel")
        #expect(SettingsL274Copy.analysisHistoryRecoveredCaption == "Analysis history opened.")
        #expect(
            SettingsL274Copy.analysisHistoryStillFailingCaption
                == "Still couldn't open it. Playhead will try again next time you open the app."
        )
        #expect(
            SettingsL274Copy.analysisHistorySetAsideFailedCaption
                == "Couldn't set the old history aside, so nothing was changed."
        )
        #expect(SettingsL274Copy.analysisHistorySetAsideLabel == "Set aside")
    }

    /// The two claims the listener most needs, asserted as claims rather
    /// than as byte-equality — so a rewording that quietly drops one
    /// turns this red even though the verbatim pin above was updated
    /// alongside it.
    @Test("The failure copy says nothing was deleted and that playback still works")
    func copyMakesTheTwoLoadBearingPromises() {
        let body = SettingsL274Copy.analysisHistoryRecoveryBody.lowercased()
        #expect(body.contains("nothing has been deleted"))
        #expect(body.contains("playback isn't affected"))

        let confirm = SettingsL274Copy.analysisHistoryStartFreshConfirmBody.lowercased()
        // "Start fresh" must not read as free, and must not read as
        // fatal: the reversibility is what makes it a real choice.
        #expect(confirm.contains("set aside on this device rather than deleted"))
        #expect(confirm.contains("marked by hand"))
    }

    /// No storage jargon. The listener did not ask for a storage layer.
    @Test("Recovery copy carries no implementation jargon")
    func copyHasNoJargon() {
        let strings = [
            SettingsL274Copy.analysisHistoryRecoveryTitle,
            SettingsL274Copy.analysisHistoryRecoveryBody,
            SettingsL274Copy.analysisHistoryStartFreshConfirmTitle,
            SettingsL274Copy.analysisHistoryStartFreshConfirmBody,
            SettingsL274Copy.analysisHistoryRecoveredCaption,
            SettingsL274Copy.analysisHistoryStillFailingCaption,
            SettingsL274Copy.analysisHistorySetAsideFailedCaption
        ]
        let banned = ["migrat", "database", "schema", "sqlite", "quarantin", "store"]
        for text in strings {
            let lower = text.lowercased()
            for word in banned {
                #expect(!lower.contains(word), "'\(word)' leaked into listener-facing copy: \(text)")
            }
        }
    }
}
