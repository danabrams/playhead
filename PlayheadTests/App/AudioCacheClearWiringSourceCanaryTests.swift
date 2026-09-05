// AudioCacheClearWiringSourceCanaryTests.swift
//
// playhead-86sfq. `DownloadManager.clearCache()` had ZERO production callers —
// the zero-caller shape this repo found three times on 2026-09-02. The
// behavioural suite proves the view model delegates; only a source canary can
// prove the VIEW wires the actor in, because nothing constructs SettingsView.

import Foundation
import XCTest
@testable import Playhead

final class AudioCacheClearWiringSourceCanaryTests: XCTestCase {

    func testSettingsWiresTheManagerAndTheViewModelNoLongerUnlinks() throws {
        let view = try SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.loadSource(repoRelativePath: "Playhead/Views/Settings/SettingsView.swift")
        )
        XCTAssertTrue(view.contains("viewModel.audioCacheClearer = {"), "SettingsView does not wire a clearer")
        XCTAssertTrue(
            view.contains("runtime.downloadManager.clearCache()"),
            "the clearer must be DownloadManager.clearCache() — the actor is the cache's only owner"
        )

        let model = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Views/Settings/SettingsViewModel.swift"
        )
        let body = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: SwiftSourceInspector.strippingComments(model),
                after: "func clearAudioCache() async"
            ),
            "could not locate SettingsViewModel.clearAudioCache"
        )
        XCTAssertTrue(body.contains("audioCacheClearer"), "vacuous region: the delegate is not in the body")
        XCTAssertFalse(
            body.contains("removeContentsStatic") || body.contains("audioDirectories"),
            "clearAudioCache still unlinks audio directories itself, behind the manager's back"
        )
    }
}
