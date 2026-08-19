// DetectorTrustObservationCountSourceCanaryTests.swift
// playhead-scc6: two properties of the SOURCE that no runtime assertion can
// observe, plus the READER ENUMERATION a unit change owes.
//
// THE ENUMERATION IS THE POINT, and this is the third time it has been owed.
// playhead-2qz6 (V49) enumerated the readers of `podcast_profiles
// .observationCount` by hand and missed the per-class mirror in
// `detectorTrustJSON` — the copy the skip gate actually consults. playhead-kfts
// missed `podcast_planner_state.observedEpisodeCount`. playhead-g7ln missed
// `ShowTraitProfile.episodesObserved` and, having been bitten, made its own
// enumeration mechanical. This file does the same for the per-class counter, so
// the NEXT unit change starts from a list a machine maintains.
//
// HOW THE LIST WAS PROVED COMPLETE, since "I looked" is not a method:
//
//   1. `DetectorTrustEntry.observationCount` is a STORED PROPERTY. Swift offers
//      exactly three spellings that can reach one — member access
//      (`x.observationCount`), a key path (`\.observationCount`,
//      `\DetectorTrustEntry.observationCount`) and the memberwise-init argument
//      label (`observationCount:`) — and every one of them contains the
//      identifier. So a lexical sweep with comments and string literals stripped
//      is exhaustive over the direct accesses, which is what `testEveryReadOf…`
//      below re-runs on each build.
//   2. The NON-lexical escapes are closed by inspection and are asserted here as
//      absences: there is no `Mirror(reflecting:)` over the type, and no reader
//      anywhere pulls the key out of the JSON by its string name. (The whole
//      tree contains ONE string literal `"observationCount"` and it is a test
//      fixture.) `Codable` synthesis is the remaining reflection-shaped reader
//      and it is the persistence path itself, not a policy reader.
//   3. A value of type `DetectorTrustEntry` can only be OBTAINED from
//      `DetectorTrustLedger` — `entries`, `entry(for:seededFrom:)`,
//      `seed(for:from:)`, the memberwise init — or from
//      `PodcastProfile.detectorTrustLedger`. So bounding the files that may name
//      any of those bounds where such a value can exist at all, which is the
//      second rail below and is what makes the count rails a closed statement
//      rather than a sample.
//
// WHAT THE SWEEP FOUND, on the tree this bead landed on: **40 lines** in the app
// target carry the identifier, of which **eleven** touch a `DetectorTrustEntry`.
// The other 29 are `PodcastProfile.observationCount` (the show scalar V49 fixed),
// `CrossShowSyndicationStore`'s unrelated field of the same name, or
// `SourceTrustProfile.observationCount`, a computed `Double` on a different type.
// The eleven, by what they do:
//
//   * ONE declaration (`DetectorTrustEntry.observationCount`).
//   * FIVE that READ an existing entry's count — four spelled
//     `entry.observationCount` in `TrustScoringService` (`setUserOverride` and
//     `applyFalseSkipSignal` carry it forward unchanged; `applySuccessfulObservation`
//     and `applyCorrectObservation` add one), and one key path in V58, which reads
//     it only to decide whether the row needs repairing at all.
//   * FIVE that WRITE one — `seed`'s two branches, the two `+ 1` writes, and V58's
//     literal 0.
//
// **Exactly TWO of the five reads reach policy**, and both are the same call:
// `evaluatePromotion(…observations:falseSkipWeight:)`, whose only use of the
// argument is the `.shadow -> .manual` clause (the `.manual` rung is closed by
// playhead-lqcp and `.auto` is terminal). That is why V58 changes no tier on the
// day it runs, and it is a measured claim rather than a hopeful one.
//
// THE LIMIT, stated rather than found later. The file-level rail bounds where a
// `DetectorTrustEntry` can be OBTAINED, and an unlicensed file that received one
// as a parameter would still have to NAME the type in its own signature, so it is
// caught. What would not be caught is a fully type-INFERRED route — a closure
// literal written in a licensed file and executed by a generic helper elsewhere,
// where no unlicensed source line ever spells either the type or the property.
// No such route exists today; the reason it is a limit and not a hole is that
// closing it needs type resolution, which is parsing rather than scanning — the
// same line `SwiftSourceCallGraph`'s L-6/L-7 draw.
//
// XCTest rather than Swift Testing, matching every other source canary here:
// `xctestplan` can only filter XCTest classes (see the CLAUDE.md note on the
// Swift Testing limitation), so a canary that might one day need excluding stays
// XCTest-shaped.

import XCTest
@testable import Playhead

final class DetectorTrustObservationCountSourceCanaryTests: XCTestCase {

    private static let storePath = "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
    private static let trustPath = "Playhead/Services/TrustScoring/TrustScoringService.swift"
    private static let ledgerPath = "Playhead/Services/TrustScoring/DetectorTrustLedger.swift"

    private func code(_ repoRelativePath: String) throws -> String {
        SwiftSourceInspector.strippingCommentsAndStrings(
            try SwiftSourceInspector.loadSource(repoRelativePath: repoRelativePath)
        )
    }

    // MARK: - 1. The rung is REGISTERED, in both ladders and nowhere else

    /// A migration that is declared and never called is the classic schema
    /// defect, and the mirror — a rung called from the production ladder but not
    /// from `migrateOnlyForTesting` — is how a fixture silently stops at the
    /// previous version while every test asserting `currentSchemaVersion` still
    /// passes, because the constant moved with it.
    ///
    /// Pinned as a COUNT as well as two presences: a third call site would be
    /// invisible to any runtime assertion (the rung is idempotent, so calling it
    /// twice changes nothing observable) and is exactly the kind of thing a
    /// merge resolves wrongly.
    func testV58IsRegisteredInBothLaddersExactlyOnceEach() throws {
        let store = try code(Self.storePath)
        let symbol = #"\bmigrateDetectorTrustObservationCountV58IfNeeded\b"#

        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: store), 3,
            "playhead-scc6: the V58 rung must appear exactly three times in AnalysisStore.swift — "
            + "its declaration, the `runSchemaMigration` call and the `migrateOnlyForTesting` call. "
            + "Fewer means a ladder cannot reach it; more means a call site nobody enumerated."
        )

        let production = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "private func runSchemaMigration() throws"),
            "could not isolate runSchemaMigration's body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: production), 1,
            "playhead-scc6: the production ladder must call V58 exactly once."
        )

        let testing = try XCTUnwrap(
            SwiftSourceInspector.firstBody(in: store, after: "func migrateOnlyForTesting() throws"),
            "could not isolate migrateOnlyForTesting's body — the canary's anchor has drifted"
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: symbol, in: testing), 1,
            "playhead-scc6: the ladder-only test seam must call V58 exactly once, or every "
            + "fixture-driven migration test silently stops one rung short."
        )
    }

    /// V49 stays the SHOW-SCALAR statement it always was, and the split between
    /// the two rungs stays legible. The temptation this forecloses is extending
    /// V49's `UPDATE` to reach into the JSON column — which needs SQLite's JSON1,
    /// a dependency playhead-g7ln's V57 deliberately declined in favour of
    /// rewriting the blob in Swift through the type's own `Codable`. V58 follows
    /// that shape; this rail is what stops the other one growing back.
    func testV49RemainsTheShowScalarStatementOnly() throws {
        // COMMENTS stripped, STRINGS kept — the one combination that works
        // here, and both halves are load-bearing. SQL lives inside string
        // literals, so `strippingCommentsAndStrings` would make this rail read
        // zero and pass on an empty search (the census machinery's own failure,
        // three times over: an instrument that cannot see its subject reports a
        // clean result). And the raw text would match the prose in V58's block
        // comment, which QUOTES V49's statement and names `json_set` in order to
        // say why it is not used.
        let store = SwiftSourceInspector.strippingComments(
            try SwiftSourceInspector.loadSource(repoRelativePath: Self.storePath)
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"UPDATE podcast_profiles SET observationCount = 0"#, in: store
            ),
            1,
            "playhead-scc6: V49's reset must remain exactly one statement over the show scalar."
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bjson_set\b|\bjson_extract\b|\bjson_replace\b"#, in: store),
            0,
            "playhead-scc6: no SQLite JSON1 dependency. V57 and V58 repair a JSON column ROW BY ROW "
            + "in Swift, through the type's own Codable, so the repaired population is exactly the "
            + "readable one and a blob the app cannot decode is left alone rather than half-rewritten."
        )
    }

    // MARK: - 2. The reader enumeration — where a DetectorTrustEntry can exist

    /// The files allowed to name any of the ledger identifiers, with the reason.
    /// Adding a file here is the point: it forces the author of a fourth site to
    /// say so, and to update the enumeration this file's header carries.
    private static let licensedFiles: [String: String] = [
        "Playhead/Services/TrustScoring/DetectorTrustLedger.swift":
            "the declarations, `seed`, `entry(for:seededFrom:)`, `set`, and the Codable round trip",
        "Playhead/Services/TrustScoring/TrustScoringService.swift":
            "the four writers and the two policy readers — `setUserOverride`, `applyFalseSkipSignal`, "
            + "`applySuccessfulObservation`, `applyCorrectObservation`, `materialized`, `rebuild`, "
            + "and `resolveDetectorModes`, which is what the skip gate consults",
        "Playhead/Persistence/AnalysisStore/AnalysisStore.swift":
            "the `detectorTrustJSON` column, `PodcastProfile.detectorTrustLedger`, and V58, "
            + "which rebuilds the repaired ledger",
    ]

    func testADetectorTrustEntryCanOnlyExistWhereTheEnumerationSaysItCan() throws {
        let root = try XCTUnwrap(
            SwiftSourceInspector.repositoryRoot(from: #filePath),
            "could not locate the repository root"
        )
        let appRoot = root.appendingPathComponent("Playhead")

        // Every route to a value of type `DetectorTrustEntry`. `\b` on both
        // sides so a longer identifier that merely contains one of these is not
        // matched.
        let patterns = [
            #"\bDetectorTrustEntry\b"#,
            #"\bDetectorTrustLedger\b"#,
            #"\bdetectorTrustLedger\b"#,
            #"\bdetectorTrustJSON\b"#,
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
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(text)
            let range = NSRange(stripped.startIndex..<stripped.endIndex, in: stripped)
            let hits = regexes.reduce(0) { $0 + $1.numberOfMatches(in: stripped, range: range) }
            guard hits > 0 else { continue }
            if Self.licensedFiles[relative] != nil {
                seenLicensed.insert(relative)
            } else {
                unlicensed.append("\(relative) (\(hits) reference(s))")
            }
        }

        XCTAssertTrue(
            unlicensed.isEmpty,
            "playhead-scc6: the per-detector trust ledger is reachable from file(s) the reader "
            + "enumeration does not name:\n"
            + unlicensed.sorted().joined(separator: "\n")
            + "\n\nA new site is not forbidden — but `DetectorTrustEntry.observationCount` counts "
            + "EPISODES since V58, it is NOT deduped through `trust_episode_observations` (that is "
            + "playhead-jh4y), and the value the gate reads is the STORED entry rather than the "
            + "seed. Add the file here with its reason AND to the enumeration in this file's "
            + "header, so the next unit change has a list to work from."
        )

        // CLOSED IN BOTH DIRECTIONS, like `singleton-slot-allowlist.json` and
        // g7ln's canary: a licence for a file that no longer mentions the symbol
        // is a licence nobody can audit, and whatever inherits the path inherits
        // the amnesty.
        let stale = Set(Self.licensedFiles.keys).subtracting(seenLicensed)
        XCTAssertTrue(
            stale.isEmpty,
            "playhead-scc6: these files are licensed to reach the per-detector ledger and no longer "
            + "mention it — delete the entry rather than leaving a standing amnesty:\n"
            + stale.sorted().joined(separator: "\n")
        )
    }

    /// The two NON-lexical escapes, asserted as positive claims rather than left
    /// as "I did not find any". Both would defeat the sweep above: reflection
    /// reaches a stored property without naming it, and a string key reaches the
    /// persisted value without naming the type.
    func testNothingReachesThePerClassCountByReflectionOrByStringKey() throws {
        let root = try XCTUnwrap(SwiftSourceInspector.repositoryRoot(from: #filePath))
        let appRoot = root.appendingPathComponent("Playhead")

        var reflectors: [String] = []
        var stringKeyed: [String] = []
        let enumerator = try XCTUnwrap(
            FileManager.default.enumerator(at: appRoot, includingPropertiesForKeys: nil)
        )
        for case let url as URL in enumerator where url.pathExtension == "swift" {
            let relative = String(url.path.dropFirst(root.path.count + 1))
            guard let text = try? String(contentsOf: url, encoding: .utf8) else { continue }
            // The string-key check reads the RAW text on purpose: stripping
            // string literals is exactly what would hide it.
            if text.contains(#""observationCount""#) { stringKeyed.append(relative) }
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(text)
            guard Self.licensedFiles[relative] != nil else { continue }
            if SwiftSourceInspector.regexOccurrences(of: #"\bMirror\s*\(\s*reflecting"#, in: stripped) > 0 {
                reflectors.append(relative)
            }
        }

        XCTAssertTrue(
            reflectors.isEmpty,
            "playhead-scc6: a licensed file now reflects over a value, which reaches a stored "
            + "property without naming it and defeats the lexical enumeration: "
            + reflectors.sorted().joined(separator: ", ")
        )
        XCTAssertTrue(
            stringKeyed.isEmpty,
            "playhead-scc6: production code now names `observationCount` as a STRING, i.e. reads it "
            + "out of the persisted JSON by key rather than through `DetectorTrustEntry`. That "
            + "reader is invisible to every rail in this file: "
            + stringKeyed.sorted().joined(separator: ", ")
        )
    }

    // MARK: - 3. The sites, counted

    /// The six sites that touch a `DetectorTrustEntry.observationCount`, pinned
    /// by count so a seventh has to be declared. Enumerated in the header; named
    /// again here because the failure message is where the next reader looks.
    func testEveryReadOfThePerClassCountIsOneOfTheEnumeratedSites() throws {
        let trust = try code(Self.trustPath)
        let ledger = try code(Self.ledgerPath)

        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bentry\.observationCount\b"#, in: trust), 4,
            "playhead-scc6: `TrustScoringService` reads a per-class `observationCount` at exactly "
            + "four sites — `setUserOverride` and `applyFalseSkipSignal` carry it forward unchanged, "
            + "`applySuccessfulObservation` and `applyCorrectObservation` add one. A fifth read is a "
            + "reader the enumeration does not know about; add it to this file's header first."
        )

        // The POLICY reads: the only place the number decides anything.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bobservations:\s*entryObservations\b"#, in: trust), 2,
            "playhead-scc6: a per-class count reaches policy through exactly two calls, both of them "
            + "`evaluatePromotion(config:currentMode:trustScore:observations:falseSkipWeight:)`, whose "
            + "only use of `observations` is the `.shadow -> .manual` clause. That is WHY V58 changes "
            + "no tier on the day it runs and why `.auto` entries are unaffected. A third policy read "
            + "invalidates that measurement — re-measure before adding one."
        )

        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\bobservationCount\b"#, in: ledger), 4,
            "playhead-scc6: `DetectorTrustLedger` names the per-class count four times — the stored "
            + "property, and `seed`'s two branches (literal 0 for a show-independent class, "
            + "`profile.observationCount` for a show-governed one, which is two occurrences on one "
            + "line). `seed` is the BRIDGE between the two representations: change it and the show "
            + "scalar's unit becomes the per-class unit for every class with no stored entry."
        )
    }
}
