// AppConfigurationTests.swift
// Regression tests for app-level configuration that must survive xcodegen
// regeneration cycles. Info.plist values come from project.yml; a manual
// edit to project.yml that forgets a required entry would silently
// propagate to Info.plist on next xcodegen run.
//
// Background: commit b94a5a0 fixed a silent regression where UIBackgroundModes
// lost its 'audio' entry, breaking background podcast playback. These tests
// ensure such drift surfaces immediately at test time.

import Foundation
import Testing
@testable import Playhead

@Suite("App configuration — Info.plist required values")
struct AppConfigurationTests {

    /// Locate the Playhead app's main bundle. In test environments,
    /// `Bundle.main` returns the xctest runner's bundle (which has no
    /// Playhead Info.plist values), so we use a Playhead type as a
    /// bundle locator and fall back to `Bundle.main` if the located
    /// bundle does not contain UIBackgroundModes.
    private static func playheadBundle() -> Bundle {
        let viaType = Bundle(for: PlayheadRuntime.self)
        if viaType.infoDictionary?["UIBackgroundModes"] != nil {
            return viaType
        }
        if Bundle.main.infoDictionary?["UIBackgroundModes"] != nil {
            return Bundle.main
        }
        // Last resort: return the type-located bundle so failures are
        // self-explanatory rather than asserting against the runner.
        return viaType
    }

    @Test("UIBackgroundModes contains audio")
    func backgroundModesContainsAudio() throws {
        let bundle = Self.playheadBundle()
        let info = bundle.infoDictionary ?? [:]
        let modes = info["UIBackgroundModes"] as? [String] ?? []
        #expect(modes.contains("audio"),
                "UIBackgroundModes must contain 'audio' for background podcast playback. If this fails, project.yml lost the declaration — see commit b94a5a0.")
    }

    @Test("UIBackgroundModes contains processing")
    func backgroundModesContainsProcessing() throws {
        let bundle = Self.playheadBundle()
        let info = bundle.infoDictionary ?? [:]
        let modes = info["UIBackgroundModes"] as? [String] ?? []
        #expect(modes.contains("processing"),
                "UIBackgroundModes must contain 'processing' for BGTaskScheduler")
    }

    @Test("BGTaskSchedulerPermittedIdentifiers is non-empty")
    func backgroundTaskIdentifiersPresent() throws {
        let bundle = Self.playheadBundle()
        let info = bundle.infoDictionary ?? [:]
        let ids = info["BGTaskSchedulerPermittedIdentifiers"] as? [String] ?? []
        #expect(!ids.isEmpty,
                "BGTaskSchedulerPermittedIdentifiers must be declared for background analysis")
        #expect(ids.contains("com.playhead.app.analysis.backfill"),
                "BGTaskSchedulerPermittedIdentifiers must include the analysis backfill identifier")
    }

    @Test("BGTaskSchedulerPermittedIdentifiers includes the rediff re-fetch identifier (launch-critical since xsdz.36 activation)")
    func rediffRefetchIdentifierPermitted() throws {
        let bundle = Self.playheadBundle()
        let info = bundle.infoDictionary ?? [:]
        let ids = info["BGTaskSchedulerPermittedIdentifiers"] as? [String] ?? []
        // playhead-xsdz.36 (R4): activation registers this identifier during
        // launch, and `BGTaskScheduler.register` TRAPS on an identifier
        // missing from the permitted list — a project.yml edit that drops
        // the entry would crash every real-device launch while remaining
        // invisible on the simulator (BGTaskScheduler never fires there).
        // Pin via the code constant so the two can never drift apart.
        #expect(ids.contains(RediffRefetchService.taskIdentifier),
                "BGTaskSchedulerPermittedIdentifiers must include \(RediffRefetchService.taskIdentifier) — registration of an unpermitted id traps at launch")
    }
}
