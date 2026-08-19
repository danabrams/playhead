// TraitEpisodeCountSourceCanaryTests.swift
// playhead-g7ln: two properties of the SOURCE that no runtime assertion on
// this harness can observe.
//
//   1. THE CALL SITE. `runBackfill` must forward the
//      `trust_episode_observations` claim result into `updatePriors`. A
//      literal `true` there restores the per-backfill unit exactly, and every
//      test in `TraitProfileEpisodeCountTests` still passes, because they call
//      `updatePriorsForTesting` and choose the value themselves.
//      `recordConfirmedWindowObservation` is private and `runBackfill` needs
//      a whole pipeline, so the argument is unobservable from a test — but it
//      is right there in the text.
//
//   2. THE READER SET. A unit change owes an enumeration of everyone who
//      reads the quantity, and V49's cautionary tale (playhead-scc6) is that
//      an enumeration done by hand misses the mirror. This canary makes the
//      list mechanical: it sweeps every production `.swift` file with comments
//      and string literals stripped, and fails when `episodesObserved`,
//      `isReliable` or `debugArchetypeLabel` appears in a file the
//      `ShowTraitProfile.episodesObserved` doc comment does not name.
//
// XCTest rather than Swift Testing, matching every other source canary here:
// `xctestplan` can only filter XCTest classes (see the CLAUDE.md note on the
// Swift Testing limitation), so a canary that might one day need excluding
// stays XCTest-shaped.

import XCTest
@testable import Playhead

final class TraitEpisodeCountSourceCanaryTests: XCTestCase {

    // MARK: - 1. The call site

    /// `runBackfill` binds the claim result and passes it on. Pinned as three
    /// separate facts rather than one blob match so a reformat does not fail
    /// the test and a REWIRING does.
    func testRunBackfillForwardsTheEpisodeClaimIntoUpdatePriors() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )
        let code = SwiftSourceInspector.strippingCommentsAndStrings(source)

        // (a) the claim result is BOUND rather than discarded.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"let\s+countedEpisodeObservation\s*=\s*await\s+recordConfirmedWindowObservation\("#,
                in: code
            ),
            1,
            "runBackfill must bind recordConfirmedWindowObservation's result — a discarded "
            + "@discardableResult is how this quantity became a per-backfill counter."
        )

        // (b) …and reaches `updatePriors` as the gate.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"countsAsEpisodeObservation:\s*countedEpisodeObservation"#,
                in: code
            ),
            1,
            "updatePriors must be called with the claim result, not a literal."
        )

        // (c) and NOTHING passes a literal. This is the mutant that keeps every
        // other test in this bead green.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"countsAsEpisodeObservation:\s*(true|false)"#,
                in: code
            ),
            0,
            "a literal countsAsEpisodeObservation in production restores the per-backfill unit."
        )

        // (d) the parameter has no default, so a future caller cannot omit it.
        // playhead-g7ln follows cycle-1 L3's rule for featureWindows/chunks:
        // a defaulted `true` is a silent regression waiting for a new call site.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"countsAsEpisodeObservation:\s*Bool\s*="#,
                in: code
            ),
            0,
            "countsAsEpisodeObservation must not acquire a default value."
        )
    }

    // MARK: - 2. The reader set

    /// The files allowed to name any of the three symbols, with the reason.
    /// Adding a file here is the point: it forces the author of a fourth
    /// reader to say so, and to update the enumeration on
    /// `ShowTraitProfile.episodesObserved` that this list mirrors.
    private static let licensedFiles: [String: String] = [
        "Playhead/Services/AdDetection/ShowTraitProfile.swift":
            "the declaration, `isReliable`, `debugArchetypeLabel`, and `updated(from:)`'s +1 and == 0 branch",
        "Playhead/Services/AdDetection/PriorHierarchy.swift":
            "level 2's `isReliable` gate and `traitBlendWeight(episodesObserved:)`",
        "Playhead/Services/AdDetection/AdDetectionService.swift":
            "the DEBUG no-regression assert inside updatePriors' mutate closure",
        "Playhead/Services/AdDetection/NetworkPriors.swift":
            "a DIFFERENT quantity wearing the same name: `decayedWeight(episodesObserved:)` "
            + "is fed PodcastProfile.observationCount, never the trait profile",
        "Playhead/Persistence/AnalysisStore/AnalysisStore.swift":
            "V57 constructs the repaired ShowTraitProfile; `traitProfile` decodes the column",
    ]

    func testEpisodesObservedIsReadOnlyWhereTheDocCommentSaysItIs() throws {
        let root = try XCTUnwrap(
            SwiftSourceInspector.repositoryRoot(from: #filePath),
            "could not locate the repository root"
        )
        let appRoot = root.appendingPathComponent("Playhead")

        // `\b` on both sides, so `episodesObservedWithoutSampleCount` — the
        // planner's own, unrelated counter on `podcast_planner_state` — is NOT
        // matched. That pair is exactly the confusion this canary is for.
        let patterns = [
            #"\bepisodesObserved\b"#,
            #"\bisReliable\b"#,
            #"\bdebugArchetypeLabel\b"#,
        ]
        let regexes = try patterns.map { try NSRegularExpression(pattern: $0) }

        var unlicensed: [String] = []
        var seenLicensed: Set<String> = []

        let enumerator = try XCTUnwrap(
            FileManager.default.enumerator(at: appRoot, includingPropertiesForKeys: nil)
        )
        for case let url as URL in enumerator where url.pathExtension == "swift" {
            let relative = String(url.path.dropFirst(root.path.count + 1))
            guard let text = try? String(contentsOf: url, encoding: .utf8) else { continue }
            let code = SwiftSourceInspector.strippingCommentsAndStrings(text)
            let range = NSRange(code.startIndex..<code.endIndex, in: code)
            let hits = regexes.reduce(0) { $0 + $1.numberOfMatches(in: code, range: range) }
            guard hits > 0 else { continue }
            if Self.licensedFiles[relative] != nil {
                seenLicensed.insert(relative)
            } else {
                unlicensed.append("\(relative) (\(hits) reference(s))")
            }
        }

        XCTAssertTrue(
            unlicensed.isEmpty,
            "playhead-g7ln: `episodesObserved` / `isReliable` / `debugArchetypeLabel` are read in "
            + "file(s) the reader enumeration does not name:\n"
            + unlicensed.sorted().joined(separator: "\n")
            + "\n\nA new reader is not forbidden — but the unit is EPISODES, deduped through "
            + "`trust_episode_observations`, and V57 reset every value written in the old unit. "
            + "Add the file here with its reason AND to the enumeration on "
            + "`ShowTraitProfile.episodesObserved`, so the next unit change has a list to work from."
        )

        // CLOSED IN BOTH DIRECTIONS, like `singleton-slot-allowlist.json`: a
        // licence for a file that no longer mentions the symbol is a licence
        // nobody can audit, and whatever inherits the path inherits the amnesty.
        let stale = Set(Self.licensedFiles.keys).subtracting(seenLicensed)
        XCTAssertTrue(
            stale.isEmpty,
            "playhead-g7ln: these files are licensed to read the trait episode count and no "
            + "longer mention it — delete the entry rather than leaving a standing amnesty:\n"
            + stale.sorted().joined(separator: "\n")
        )
    }

    /// The one reader that exists and has no production consumer at all, kept
    /// as a positive claim rather than an absence. If `debugArchetypeLabel`
    /// ever acquires one, the test above will not notice (its file is already
    /// licensed) — this one will.
    func testDebugArchetypeLabelStillHasNoProductionConsumer() throws {
        let root = try XCTUnwrap(SwiftSourceInspector.repositoryRoot(from: #filePath))
        let appRoot = root.appendingPathComponent("Playhead")
        let regex = try NSRegularExpression(pattern: #"\bdebugArchetypeLabel\b"#)

        var callers: [String] = []
        let enumerator = try XCTUnwrap(
            FileManager.default.enumerator(at: appRoot, includingPropertiesForKeys: nil)
        )
        for case let url as URL in enumerator where url.pathExtension == "swift" {
            guard url.lastPathComponent != "ShowTraitProfile.swift" else { continue }
            guard let text = try? String(contentsOf: url, encoding: .utf8) else { continue }
            let code = SwiftSourceInspector.strippingCommentsAndStrings(text)
            let range = NSRange(code.startIndex..<code.endIndex, in: code)
            if regex.numberOfMatches(in: code, range: range) > 0 {
                callers.append(String(url.path.dropFirst(root.path.count + 1)))
            }
        }
        XCTAssertTrue(
            callers.isEmpty,
            "playhead-g7ln: `debugArchetypeLabel` is documented as having no production reader, "
            + "and now has one: \(callers.sorted().joined(separator: ", ")). Its `>= 1` gate is a "
            + "third reader of the episode count — update the enumeration."
        )
    }
}
