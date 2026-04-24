// BuildInfo.swift
// playhead-gtt9.21: Build-time provenance constant.
//
// `BuildInfo.commitSHA` is the short git SHA of the binary at build
// time. The source-of-truth is the `BuildCommitSHA` key in a
// dedicated `BuildProvenance.plist` resource, written into the app
// bundle by the `Stamp build commit SHA` build-phase script that runs
// `git rev-parse --short HEAD`. When the build is running outside a
// git context (e.g. an archive replayed without the repo, a unit-test
// runner whose host bundle has no Playhead resources in scope), the
// fallback `"unknown"` keeps callers safe — `commitSHA` must NEVER be
// empty per the contract enforced in
// `FrozenTraceProvenanceTests.testBuildInfoCommitSHAIsAccessible`.
//
// Why a SEPARATE plist (not Info.plist):
//   `ProcessInfoPlistFile` runs during the Resources phase and
//   rewrites the deployed `Info.plist` from the source-tree plist.
//   Any script-phase edits to the deployed `Info.plist` get clobbered.
//   A dedicated `BuildProvenance.plist` resource sidesteps that race.
//
// Why a Bundle-resource plist rather than a generated Swift constant:
//   1. Doesn't churn a tracked source file across rebuilds (a build
//      script that rewrites a checked-in .swift would dirty
//      `git status`).
//   2. The plist is the natural place app metadata lives — no second
//      mechanism for a tester to learn.
//   3. `Bundle.main` is fine for app builds; for unit tests we resolve
//      the bundle via a Playhead type so we read the Playhead.app
//      bundle, not the test runner's bundle (the same trick
//      `AppConfigurationTests.playheadBundle()` uses).
//
// IMPORTANT: When `BuildInfo` is queried from the unit-test target
// without a Playhead.app host bundle (PlaygroundCommandLineTool style
// invocations), the fallback fires. That's acceptable — provenance
// stamping is for production captures, not for offline harness self-
// tests.

import Foundation

enum BuildInfo {

    /// Resource basename the `Stamp build commit SHA` build phase writes.
    static let provenancePlistName: String = "BuildProvenance"

    /// Key inside `BuildProvenance.plist`.
    static let infoPlistKey: String = "BuildCommitSHA"

    /// Fallback value when no SHA can be resolved (build phase didn't
    /// run, plist absent, bundle inaccessible). Never empty by
    /// contract.
    static let unknownSHA: String = "unknown"

    /// Short git SHA of the binary that produced this build, or
    /// `"unknown"` when the build phase couldn't resolve a SHA. Never
    /// empty.
    static let commitSHA: String = {
        let bundle = playheadBundle()
        guard let url = bundle.url(forResource: provenancePlistName, withExtension: "plist"),
              let data = try? Data(contentsOf: url),
              let plist = try? PropertyListSerialization.propertyList(from: data, format: nil),
              let dict = plist as? [String: Any],
              let value = dict[infoPlistKey] as? String,
              !value.isEmpty,
              // The plist literal "$(BUILD_COMMIT_SHA)" can survive
              // unsubstituted on a misconfigured build (e.g. no script,
              // or script failed silently). Treat that as "unknown" so
              // we never persist a build-system token as if it were a
              // SHA.
              !value.hasPrefix("$(") else {
            return unknownSHA
        }
        return value
    }()

    /// Resolve the Playhead app bundle. Mirrors
    /// `AppConfigurationTests.playheadBundle()` so unit tests that load
    /// the framework against a Playhead.app host see the right resources
    /// instead of the test runner's bundle (which has no
    /// `BuildProvenance.plist`).
    private static func playheadBundle() -> Bundle {
        // `Bundle(for:)` returns the bundle that owns the given class.
        // For tests linked against Playhead, that's Playhead.app — the
        // bundle whose Resources phase the build script wrote into.
        let viaType = Bundle(for: AnchorClass.self)
        if viaType.url(forResource: provenancePlistName, withExtension: "plist") != nil {
            return viaType
        }
        if Bundle.main.url(forResource: provenancePlistName, withExtension: "plist") != nil {
            return Bundle.main
        }
        return viaType
    }

    /// Internal anchor class used purely as a `Bundle(for:)` locator.
    /// Defined here (not at file-scope) so accidental re-use elsewhere
    /// doesn't drag in a different bundle resolution.
    private final class AnchorClass {}
}
