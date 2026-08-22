// AdSkipCueSourceCanaryTests.swift
// playhead-nqwr: four properties of the SOURCE that no runtime assertion on
// this harness can observe.
//
// 1. THE SESSION IS NEVER TOUCHED BY THE CUE. The app owns exactly one
//    `AVAudioSession`. A cue that reconfigures or re-activates it can duck,
//    pause, interrupt or reroute the episode at the one moment the listener is
//    paying attention — a P0 shipped into the listening path, and strictly
//    worse than the silence it replaces. It is enforced as an ABSENCE in the
//    two cue files rather than as a behaviour because on the simulator the
//    session is a stub: a `setActive(true)` added to the cue would pass every
//    test in the tree and only misbehave on a device, in a car, once.
//
// 2. THE CUE HANGS OFF THE CUT, AND THERE IS ONE OF IT. playhead-bwxi is the
//    cautionary tale one bead back: the banner was a SECOND consumer of the
//    same event and drifted out of agreement with the skip it claimed to
//    announce. The emit site is counted here so a third consumer — a
//    "predicted skip" hook, a mark-tier chime — has to argue with this file
//    first.
//
// 3. DEFAULT #4 IS STRUCTURAL. Sound is reserved for actual CUTS; a mark or a
//    banner gets nothing audible. That is not a runtime state to assert, it is
//    the absence of a second call site anywhere in the app.
//
// 4. ON-BY-DEFAULT IS A READ, NOT A CONSTANT. `UserDefaults.bool(forKey:)`
//    answers `false` for a key nobody has written. A switch documented as
//    default-ON that is read that way ships OFF for every listener who never
//    opens Settings, and on a device that is indistinguishable from the silence
//    this bead exists to remove. `AdSkipCueSettingsTests` covers the VALUE;
//    this covers the spelling, because the value test would keep passing if the
//    read were changed to `bool(forKey:) || defaultValue` and other, worse
//    equivalents.
//
// XCTest rather than Swift Testing, matching every other source canary here:
// `xctestplan` can only filter XCTest classes (see the CLAUDE.md note on the
// Swift Testing limitation), so a canary that might one day need excluding
// stays XCTest-shaped.

import XCTest
@testable import Playhead

final class AdSkipCueSourceCanaryTests: XCTestCase {

    private static let cuePath = "Playhead/Services/PlaybackTransport/AdSkipCue.swift"
    private static let soundPath =
        "Playhead/Services/PlaybackTransport/AdSkipCueSound.swift"
    private static let transportPath =
        "Playhead/Services/PlaybackTransport/PlaybackTransport.swift"

    private func code(_ repoRelativePath: String) throws -> String {
        SwiftSourceInspector.strippingCommentsAndStrings(
            try SwiftSourceInspector.loadSource(repoRelativePath: repoRelativePath)
        )
    }

    private func occurrences(of needle: String, in haystack: String) -> Int {
        guard !needle.isEmpty else { return 0 }
        var count = 0
        var search = haystack.startIndex..<haystack.endIndex
        while let found = haystack.range(of: needle, range: search) {
            count += 1
            search = found.upperBound..<haystack.endIndex
        }
        return count
    }

    // MARK: - 1. The cue may not touch the audio session

    func testCueSourcesNeverMentionTheAudioSession() throws {
        // Every spelling that can reach the shared session or stand up a
        // second graph that would fight it for the route.
        let forbidden = [
            "AVAudioSession",
            "setActive",
            "setCategory",
            "setPreferredIOBufferDuration",
            "AVAudioEngine",
            "AudioServicesPlaySystemSound",
            "MPVolumeView"
        ]
        for path in [Self.cuePath, Self.soundPath] {
            let source = try code(path)
            for token in forbidden {
                XCTAssertFalse(
                    source.contains(token),
                    """
                    `\(path)` names `\(token)`. The ad-skip cue mixes into the \
                    session `PlaybackService.configureAudioSession` already \
                    activated — it must never configure, activate or \
                    deactivate one itself. A cue that wedges the session is a \
                    P0 in the listening path and is invisible on the \
                    simulator, which is why this is a source rail.
                    """
                )
            }
        }
    }

    /// The other half of the same claim: the cue is played through
    /// `AVAudioPlayer`, which mixes into an already-active session, and not
    /// through an API that owns a session of its own.
    func testCueIsPlayedThroughAVAudioPlayer() throws {
        let source = try code(Self.soundPath)
        XCTAssertTrue(
            source.contains("AVAudioPlayer"),
            "the cue's player is the thing this file exists to build"
        )
    }

    // MARK: - 2/3. One emit site, and it is the completed cut

    func testTheCueIsSoundedFromExactlyOneProductionSite() throws {
        guard let repoRoot = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
            XCTFail("Could not locate repo root from \(#filePath)")
            return
        }
        let productionRoot = repoRoot.appendingPathComponent("Playhead")
        guard let enumerator = FileManager.default.enumerator(
            at: productionRoot,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            XCTFail("FileManager could not enumerate \(productionRoot.path)")
            return
        }

        var callers: [String: Int] = [:]
        for case let url as URL in enumerator where url.pathExtension == "swift" {
            let relative = url.path.replacingOccurrences(
                of: repoRoot.path + "/", with: ""
            )
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(
                try String(contentsOf: url, encoding: .utf8)
            )
            let count = occurrences(of: "playAdSkipCue()", in: stripped)
            if count > 0 { callers[relative] = count }
        }

        // `AdSkipCue.swift` declares the method (protocol + implementation);
        // `PlaybackTransport.swift` calls it once. Nothing else may name it.
        XCTAssertEqual(
            Set(callers.keys), [Self.cuePath, Self.transportPath],
            """
            The ad-skip cue is sounded from an unexpected file. Sound is \
            reserved for an actual CUT (Dan's default #4): a mark or a banner \
            gets nothing audible, because nothing happened to the audio. And a \
            SECOND consumer of the skip event is exactly what playhead-bwxi \
            had to undo for the banner — it drifts out of agreement with the \
            skip it claims to announce. Observed: \(callers).
            """
        )
        XCTAssertEqual(
            callers[Self.transportPath], 1,
            "the transport must sound the cue from one site; observed \(callers)"
        )
    }

    /// The emit helper is called once, and from the transition. A second call
    /// — from `seek`, from the periodic observer, from a banner action — would
    /// announce a cut the transport did not perform.
    func testTheEmitHelperIsCalledOnceAndOnlyFromTheTransition() throws {
        let source = try code(Self.transportPath)
        XCTAssertEqual(
            occurrences(of: "emitAdSkipCue()", in: source), 2,
            "expected one declaration and exactly one call site"
        )
        XCTAssertTrue(
            source.contains("private func emitAdSkipCue()"),
            "the helper must stay private to the transport"
        )

        // The call must be the LAST statement of `duckSeekRelease`. That is
        // the one position past every early `return` in the method, and every
        // one of those returns is a skip that did NOT happen — a cancelled
        // seek, a replaced item, a reservation Listen disarmed. Anchored on
        // the enclosing function rather than on a line number so a reformat
        // cannot fail the canary.
        guard let declaration = source.range(of: "private func duckSeekRelease(") else {
            XCTFail("`duckSeekRelease` was renamed; re-derive this canary")
            return
        }
        guard let brace = source[declaration.upperBound...].firstIndex(of: "{") else {
            XCTFail("could not find the body of `duckSeekRelease`")
            return
        }
        let body = SwiftSourceInspector.bracedBody(in: source, startingAt: brace)
        XCTAssertEqual(
            occurrences(of: "emitAdSkipCue()", in: body), 1,
            "the cue must be emitted from inside the completed transition"
        )
        guard let emit = body.range(of: "emitAdSkipCue()") else {
            XCTFail("the emit call left `duckSeekRelease`")
            return
        }
        XCTAssertTrue(
            body[emit.upperBound...].trimmingCharacters(in: .whitespacesAndNewlines)
                .isEmpty,
            """
            `emitAdSkipCue()` is no longer the last statement of \
            `duckSeekRelease`. Every early `return` above it is a skip that \
            did NOT happen; a cue that moves ahead of one announces a cut \
            nobody heard, which is the failure this feature must never have. \
            Trailing text: \
            \(body[emit.upperBound...].prefix(200))
            """
        )
    }

    /// The three places a cue must be silenced. Counted rather than named
    /// individually because the failure mode is a FOURTH stop-worthy exit
    /// being added without one — or one of these three losing it.
    func testTheCueIsSilencedAtEveryTransportExit() throws {
        let source = try code(Self.transportPath)
        XCTAssertEqual(
            occurrences(of: "skipCuePlayer.stopAdSkipCue()", in: source), 3,
            """
            Expected exactly three silencing sites: `pause()` (which is what \
            BOTH the interruption handler and the vanished-route handler \
            call), `pauseAndDetachCurrentItem()`, and `tearDown()`. A cue left \
            ringing after the episode has been pulled out from under it is the \
            "alert" reading this feature exists to avoid.
            """
        )
        for anchor in ["func pause()", "func pauseAndDetachCurrentItem(", "func tearDown()"] {
            guard let start = source.range(of: anchor) else {
                XCTFail("anchor `\(anchor)` moved; re-derive this canary")
                return
            }
            guard let brace = source[start.upperBound...].firstIndex(of: "{") else {
                XCTFail("could not find the body of `\(anchor)`")
                return
            }
            let body = SwiftSourceInspector.bracedBody(in: source, startingAt: brace)
            XCTAssertTrue(
                body.contains("skipCuePlayer.stopAdSkipCue()"),
                "`\(anchor)` no longer silences a sounding cue"
            )
        }
    }

    // MARK: - 4. ON-by-default is a read, not a hope

    func testTheSwitchIsReadWithObjectForKeyAndNotBoolForKey() throws {
        let source = try code(Self.cuePath)
        XCTAssertTrue(
            source.contains("object(forKey: userDefaultsKey) as? Bool ?? defaultValue"),
            """
            `AdSkipCueSettings.isEnabled` must read the key with \
            `object(forKey:) as? Bool ?? defaultValue`. This is the same idiom \
            `UserPreferencesSnapshot.current(from:)` uses, for the same reason.
            """
        )
        XCTAssertFalse(
            source.contains("bool(forKey:"),
            """
            `UserDefaults.bool(forKey:)` answers `false` for a key nobody has \
            written, which silently ships an ON-by-default switch OFF for \
            every listener who never opens Settings.
            """
        )
        XCTAssertTrue(
            AdSkipCueSettings.defaultValue,
            "default #3 is ON; flipping this constant is how it is overturned"
        )
    }

    /// The drop-in instructions promise a specific filename in a specific
    /// place. Pin the half of that promise that lives in code, so the
    /// instructions cannot rot away from the lookup.
    func testTheDropInAssetNameMatchesTheInstructions() throws {
        XCTAssertEqual(AdSkipCueSound.resourceBaseName, "AdSkipCue")
        XCTAssertEqual(AdSkipCueSound.resourceExtensions.first, "caf")
        XCTAssertTrue(AdSkipCueSound.resourceExtensions.contains("wav"))

        let header = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.soundPath
        )
        XCTAssertTrue(
            header.contains("Playhead/Resources/AdSkipCue.caf"),
            "the header must keep naming the exact path the asset is dropped at"
        )
    }
}
