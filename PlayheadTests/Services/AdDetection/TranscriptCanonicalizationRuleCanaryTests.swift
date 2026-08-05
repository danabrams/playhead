// TranscriptCanonicalizationRuleCanaryTests.swift
//
// playhead-iu0t R1 — the canonicalization rule, enforced instead of asserted.
//
// THE RULE (stated in `AdDetectionService.runBackfill` above `canonicalChunks`):
// any path that hands transcript chunks to `TranscriptAtomizer.atomize` or
// `TranscriptAtomizer.transcriptVersionHash` must canonicalize first, because
// `transcriptVersion` is derived from the chunks handed in and
// `BackfillJobRunner`'s job id embeds it. An uncanonicalized caller does not
// merely read a narrower transcript — it mints rows in an id space nothing else
// derives.
//
// WHY THIS FILE EXISTS RATHER THAN A PARAGRAPH. The rule has now been broken
// five separate times by the pre-hc7e collapse
// `filter { pass == "final" }.isEmpty ? chunks : filtered`, and every previous
// attempt to police it was a PROSE SURVEY of call sites, which failed:
//
//   * hc7e removed the collapse from `runBackfill` and wrote "Every consumer
//     now reads `canonicalChunks`." Two survivors were live at the time. That
//     sentence is why they went unfound for months.
//   * iu0t found two of them (`retryShadowFMPhaseForSession`,
//     `runPhase5ProjectorPhase`), fixed both, and replaced hc7e's sentence with
//     "Both are converted now" — another survey, and also wrong.
//   * iu0t's R1 review found the third live one,
//     `AnalysisCoordinator.pushEvidenceCatalog`, on the hot path. Measured on
//     the 2026-08-03 device pull: 11 of 12 assets carry final chunks, and the
//     collapsed catalog covered 7,295.6 s of 29,817.2 s of canonical transcript
//     coverage (24.5 %; worst 1.5 %, on 53FC53E3).
//
//   * iu0t's R2 review found the FIFTH, and it was not a call site at all:
//     `EpisodeSummaryBackfillCoordinator.hydrate` read the PERSISTED
//     `transcript_chunks.transcriptVersion` column as an episode summary's
//     invalidation key. That column's only writer selects `WHERE pass !=
//     'fast'` — the collapse spelled as a negation, in SQL — so it holds a
//     final-only version and nothing else. Measured on the 2026-08-03 pull:
//     all 29,247 `fast` rows NULL, all 8,251 `final` rows stamped, and on
//     53FC53E3 the stamp is `55afd3e8bb41833c004ee7d4b1be7589`, this bead's
//     own field-proof hash.
//
// A survey of call sites decays the instant somebody adds a call site, and
// nothing tells you when it did. This canary reads the source instead: it
// enumerates every `.swift` file under `Playhead/`, finds every call to the two
// functions, extracts the `chunks:` argument expression at each one, and fails
// unless that expression canonicalizes inline or appears on the allow-list
// below with a written reason. Adding an uncanonicalized caller is a red test.
//
// THREE RULES, because there are three ways to get a `transcriptVersion` and a
// call-site walk sees only one of them:
//   1. `testEveryTranscriptVersionCallSiteCanonicalizes` — the ARGUMENT at each
//      call of the two computing functions.
//   2. `testThePreHc7eFinalOnlyCollapseAppearsNowhereInProduction` — the
//      collapse SHAPE, banned by name, one step earlier than (1).
//   3. `testNoProductionConsumerReadsThePersistedChunkTranscriptVersion` — the
//      persisted COLUMN, which (1) cannot see because reading it is not a call.
//
// Plus RAILS on the walk itself, which are not rules and do not read the tree:
// `testTheArgumentParserRecordsAGuardedCallThatHasNoChunksArgument` (R4) and
// `testTheSiteFinderSeesACallSplitAcrossLines` (R5). They exercise the parser
// on synthetic source, which is the only way to test source the tree does not
// yet contain. This paragraph said "THREE tests" until R5 and there were four —
// a stale census in the census-checker's own header, which is small but is
// precisely the thing this file exists to stop being trusted.
//
// It is deliberately NARROW. It does not check that the whole program is
// correct about transcripts; it checks exactly the one predicate the five
// defects violated. Two escape hatches are honest and named: a parameter that
// its callers canonicalize, and a consumer that mints nothing.
//
// XCTest so the canary is filterable from a test plan (`xctestplan`
// `selectedTests`/`skippedTests` silently ignore Swift Testing identifiers —
// see project memory `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class TranscriptCanonicalizationRuleCanaryTests: XCTestCase {

    /// The two functions whose `chunks:` argument decides `transcriptVersion`,
    /// each as a PATTERN, a display name, and its own vacuity floor.
    ///
    /// **playhead-iu0t R5 made these patterns rather than literal substrings,
    /// and the demonstration is one line break.** They used to be the two
    /// strings `"TranscriptAtomizer.atomize("` and
    /// `"TranscriptAtomizer.transcriptVersionHash("`, searched with
    /// `String.range(of:)`. Swift lets a member call be split across lines, and
    /// splitting it made a call site VANISH from the walk — not reported, not
    /// counted against the floor below, not audited at all. Measured by
    /// planting the same uncanonicalized `atomize(chunks: chunks, …)` twice:
    ///
    ///     TranscriptAtomizer                 -> canary PASSED (invisible)
    ///         .atomize(chunks: chunks, …)
    ///
    ///     TranscriptAtomizer.atomize(        -> canary FAILED, named the site
    ///         chunks: chunks, …)
    ///
    /// That is the fail-OPEN direction, in the walk's own foundation: R4 closed
    /// the drop for a call with no `chunks:` of its own (see
    /// ``argumentListEnd(in:from:)``), and the same drop remained one level up
    /// for a call the site finder never found. It is also the last literal
    /// needle in this file — R3 and R4 replaced rule 3's and rule 2's with
    /// patterns and declared the sweep complete, and this one was never in it,
    /// which is the per-rule-instead-of-per-property sweep those rounds
    /// diagnosed, one rule further on.
    ///
    /// `\s*` around the dot and before the paren is the whole broadening: it
    /// admits line breaks and spaces and nothing else. It is deliberately NOT
    /// receiver-agnostic — a call through a typealias or a stored function
    /// reference is still invisible, and closing that needs type information
    /// this test does not have. Stated limit, not a completeness claim.
    ///
    /// **`vacuityFloor` is PER FUNCTION, and that is the point.** It used to be
    /// one `audited >= 6` summed over both, so `transcriptVersionHash`'s
    /// detector could match nothing at all and stay green off `atomize`'s hits
    /// — R3's finding 3 and R4's finding 3 verbatim, one rule over from where
    /// R4 fixed it. MEASURED 2026-08-05 by reading the counts back:
    /// `atomize` 6, `transcriptVersionHash` 2. The floors sit below those so
    /// ordinary churn does not trip them, but neither can collapse to zero. A
    /// pattern that fails to COMPILE also lands here, because
    /// ``matchRanges(of:in:)`` reports it as zero matches.
    private static let guardedCalls: [(name: String, pattern: String, vacuityFloor: Int)] = [
        (
            "TranscriptAtomizer.atomize(",
            #"\bTranscriptAtomizer\s*\.\s*atomize\s*\("#,
            4
        ),
        (
            "TranscriptAtomizer.transcriptVersionHash(",
            #"\bTranscriptAtomizer\s*\.\s*transcriptVersionHash\s*\("#,
            1
        ),
    ]

    /// Call sites that legitimately do NOT canonicalize inline, each with the
    /// reason it is safe. Keyed by the argument expression exactly as written.
    ///
    /// Two — and only two — kinds of entry belong here:
    ///
    ///   1. **A parameter whose callers canonicalize.** The obligation moves up
    ///      one frame; it does not disappear. Every such entry names the
    ///      callers, and every one of those callers is itself audited by this
    ///      canary, because they call one of the guarded functions too (or
    ///      `canonicalize` directly, which is what the check looks for).
    ///   2. **A consumer that mints nothing.** If no `backfill_jobs` row, no
    ///      `semantic_scan_results` row and no persisted `transcriptVersion`
    ///      can come out of it, a drifted version is invisible and harmless.
    ///
    /// "It happens to be fine today" is NOT one of them. If you cannot write
    /// which of the two it is, canonicalize instead — it is one call, it is
    /// idempotent, and single-pass input passes through byte-identically.
    ///
    /// Keys are `"<FileName>.swift|<enclosing declaration>|<argument
    /// expression>"`, NOT the expression alone and NOT file-plus-expression.
    ///
    /// **playhead-iu0t R2 narrowed this from file scope to declaration scope,
    /// and the reason is that file scope was demonstrably the very hole this
    /// comment warned about.** The warning was right — `chunks` is the most
    /// ordinary parameter name in the codebase, so an expression-only key
    /// pre-approves every future `atomize(chunks: chunks)` anywhere — but
    /// FILE scope did not fix it for the only file that matters. Measured:
    /// `AdDetectionService.swift` is 13,300 lines and hosted two of the four
    /// collapse instances, and the key `AdDetectionService.swift|chunks`
    /// approved the expression `chunks` in every one of its declarations. A
    /// brand-new uncanonicalized `atomize(chunks: chunks)` added anywhere in
    /// that file passed silently — verified by restoring exactly that at
    /// `runPhase5ProjectorPhase` (mutant CN05), which the file-scoped canary
    /// did not see and the declaration-scoped one kills.
    ///
    /// Declaration scope is what makes each entry's written reason true of the
    /// thing it is written about: "a parameter; audited at the CALLERS" is a
    /// claim about ONE function, and it should license exactly that function.
    private static let allowedUncanonicalizedArguments: [String: String] = [
        // (1) `runShadowFMPhase(chunks:)` — a parameter, reached only from
        // `runBackfill` (which passes `canonicalChunks`) and
        // `retryShadowFMPhaseForSession` (which passes
        // `canonicalize(chunks).chunks` since playhead-iu0t). This is the frame
        // the shipped defect lived one level above.
        "AdDetectionService.swift|runShadowFMPhase|chunks": """
            a parameter; audited at the CALLERS. `runShadowFMPhase` is reached \
            only from `runBackfill` (canonicalChunks) and \
            `retryShadowFMPhaseForSession` (canonicalize(chunks).chunks).
            """,

        // (1) `recordSemanticScanClaim(chunks:)` — the same parameter one frame
        // further in. Its own doc is explicit that the claim must name the job
        // ITS caller would have minted, which is why it takes the array rather
        // than re-reading the store.
        "AdDetectionService.swift|recordSemanticScanClaim|chunks": """
            a parameter; audited at the CALLERS. Every call site is inside \
            `runShadowFMPhase`, which is itself allow-listed on the same \
            grounds and whose two callers both canonicalize.
            """,

        // (1) `RegionShadowPhase.run(_:)` — a parameter on a value type. Its
        // sole production call site is `AdDetectionService.runBackfill`, which
        // builds `RegionShadowPhase.Input(chunks: canonicalChunks, …)`.
        "RegionShadowPhase.swift|run|input.chunks": """
            a parameter; `RegionShadowPhase.run`'s only production caller is \
            `AdDetectionService.runBackfill`, which passes `canonicalChunks`.
            """,

        // (2) A consumer that mints nothing: a human-readable text dump. Its
        // catalog is rendered into the export and discarded; no row, no job id,
        // no persisted `transcriptVersion`. It also stamps a deliberately
        // distinct `normalizationHash` of "debug-export".
        "DebugEpisodeExportService.swift|formatExport|chunks": """
            mints nothing — the catalog is rendered into a text export and \
            thrown away. No backfill_jobs row, no semantic_scan_results row, no \
            persisted transcriptVersion, so a drifted version cannot orphan \
            anything.
            """,
    ]

    /// Files exempt from the walk, with reasons.
    private static let exemptFiles: Set<String> = [
        // The canonicalizer itself, and the atomizer that defines the hash —
        // requiring them to call `canonicalize` would be circular.
        "TranscriptChunkCanonicalizer.swift",
        "TranscriptAtom.swift",
    ]

    // MARK: - The walk

    private func productionSwiftFiles() throws -> [URL] {
        guard let root = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
            throw XCTSkip("could not locate repo root from \(#filePath)")
        }
        let playhead = root.appendingPathComponent("Playhead")
        guard let walker = FileManager.default.enumerator(
            at: playhead,
            includingPropertiesForKeys: nil
        ) else {
            throw XCTSkip("could not enumerate \(playhead.path)")
        }
        return walker.compactMap { $0 as? URL }
            .filter { $0.pathExtension == "swift" }
            .filter { !Self.exemptFiles.contains($0.lastPathComponent) }
            .sorted { $0.path < $1.path }
    }

    /// Recorded as the argument of a guarded call that has NO `chunks:` label
    /// inside its own parentheses. It is not an identifier and it is not an
    /// allow-list key, so it resolves to nothing and reports as a violation.
    private static let missingChunksArgument = "<no chunks: argument>"

    /// The index of the `)` that closes the argument list opened immediately
    /// before `start`, or `endIndex` if the source is unbalanced.
    ///
    /// **playhead-iu0t R4.** This exists because the `chunks:` search used to be
    /// unbounded — `range(of: "chunks:", range: callRange.upperBound..<source
    /// .endIndex)` — and R3 recorded the consequence as "an overload lacking
    /// that label would steal a later call's argument and mis-attribute its
    /// scope". That is one of two consequences and not the worse one. The other
    /// is in the `guard … else { continue }` it fed: when NO later `chunks:`
    /// existed anywhere in the file, the call site was dropped from the walk
    /// entirely — never audited, never counted toward the `audited` floor,
    /// never reported. A canary whose stated principle is "failing closed is
    /// the right direction" (see ``enclosingDeclaration(containing:in:)``) had
    /// one branch that failed OPEN, and silently.
    ///
    /// Measured non-live at the time and still non-live: all 8 guarded call
    /// sites pass `chunks:` first, inside their own parentheses. Bounded here
    /// anyway, because "it happens to be fine today" is precisely the reasoning
    /// this file exists to replace.
    ///
    /// Stated limit: the scan is brace/bracket/paren counting and does not know
    /// about string literals, so a `)` inside a string BEFORE the `chunks:`
    /// label would end the list early and produce the sentinel. That direction
    /// is a false VIOLATION, not a false pass, and the expression scanner below
    /// has shared the same naivety since R2 — so this narrows nothing that was
    /// previously trusted.
    private static func argumentListEnd(in source: String, from start: String.Index) -> String.Index {
        var depth = 0
        var index = start
        while index < source.endIndex {
            let character = source[index]
            if character == "(" || character == "[" || character == "{" {
                depth += 1
            } else if character == ")" || character == "]" || character == "}" {
                if depth == 0 { return index }
                depth -= 1
            }
            index = source.index(after: index)
        }
        return source.endIndex
    }

    /// The `chunks:` argument expression at each guarded call site, paired with
    /// the call it belongs to and the index the call starts at.
    ///
    /// Takes the ALREADY comment-stripped source — stripping is what stops a
    /// call written inside a comment (this file's own prose, the rule's
    /// explanatory blocks) being mistaken for a real one, and taking it as a
    /// parameter rather than re-stripping internally means the returned
    /// `site` index belongs to the same string instance the caller then
    /// resolves scopes in. Two separately-produced but equal Strings would
    /// compare equal and index compatibly today; relying on that is a
    /// correctness footgun this signature removes.
    ///
    /// Every guarded call yields exactly one entry — one WITH its argument, or
    /// one carrying ``missingChunksArgument`` — so the count returned is the
    /// number of guarded calls in the file, unconditionally. That is what makes
    /// the `audited` floor a measurement of the walk rather than of the walk's
    /// successes. See ``argumentListEnd(in:from:)``.
    /// Every match of `pattern` in `source`, in source order.
    ///
    /// An uncompilable pattern yields `[]` rather than trapping, which is the
    /// same value as "matched nothing" — deliberately, because the per-function
    /// vacuity floors in
    /// ``testEveryTranscriptVersionCallSiteCanonicalizes()`` already fail on
    /// exactly that value, so both failure modes land in one place with one
    /// message instead of two.
    private static func matchRanges(of pattern: String, in source: String) -> [Range<String.Index>] {
        guard let regex = try? NSRegularExpression(
            pattern: pattern, options: [.dotMatchesLineSeparators]
        ) else { return [] }
        let full = NSRange(source.startIndex..<source.endIndex, in: source)
        return regex.matches(in: source, options: [], range: full)
            .compactMap { Range($0.range, in: source) }
    }

    private func guardedArguments(
        inStripped source: String
    ) -> [(call: String, argument: String, site: String.Index)] {
        var found: [(String, String, String.Index)] = []
        for call in Self.guardedCalls {
            for callRange in Self.matchRanges(of: call.pattern, in: source) {
                // THIS call's own argument list, not the rest of the file.
                let listEnd = Self.argumentListEnd(in: source, from: callRange.upperBound)
                guard let label = source.range(
                    of: "chunks:", range: callRange.upperBound..<listEnd
                ) else {
                    found.append((call.name, Self.missingChunksArgument, callRange.lowerBound))
                    continue
                }
                // Read to the terminator of this argument: a `,` or the closing
                // `)` at the argument's own nesting depth. Anything deeper —
                // `canonicalize(chunks).chunks`, a trailing closure, a nested
                // call — is part of the expression.
                var depth = 0
                var expression = ""
                var index = label.upperBound
                while index < source.endIndex {
                    let character = source[index]
                    if character == "(" || character == "[" || character == "{" {
                        depth += 1
                    } else if character == ")" || character == "]" || character == "}" {
                        if depth == 0 { break }
                        depth -= 1
                    } else if character == "," && depth == 0 {
                        break
                    }
                    expression.append(character)
                    index = source.index(after: index)
                }
                found.append((
                    call.name,
                    expression.trimmingCharacters(in: .whitespacesAndNewlines),
                    callRange.lowerBound
                ))
            }
        }
        return found
    }

    // MARK: - Declaration scoping

    /// The name and body of the `func`/`init` whose body contains `site`.
    ///
    /// This is what turns a file-wide question ("is there a canonicalizing
    /// `let chunks =` SOMEWHERE in these 13,000 lines?") into the question the
    /// rule actually means ("is the value handed to THIS call canonicalized?").
    /// Both of the resolver's demonstrated escapes were file-wide lookups:
    ///
    ///   * a `let x = canonicalize(…)` in one function silently vouched for an
    ///     unrelated raw parameter also called `x` in another, and
    ///   * an allow-list entry written about one function licensed the same
    ///     expression in every other function in the file.
    ///
    /// Nested and local functions are why this walks candidates in reverse
    /// rather than trusting the nearest preceding `func`: `runShadowFMPhase`
    /// declares a local `func wrap(…)` ABOVE its `atomize` call, so the nearest
    /// preceding declaration is `wrap`, whose body does not contain the call at
    /// all. The containment check rejects it and the walk continues outward,
    /// which also gives the correct answer — the innermost ENCLOSING
    /// declaration — for a call genuinely inside a nested function.
    ///
    /// Returns nil for a call at file/type scope (a property initialiser, say),
    /// which the caller treats as "no local scope to resolve in" — i.e. the
    /// argument must canonicalize inline or be allow-listed. Failing closed is
    /// the right direction for a canary.
    private func enclosingDeclaration(
        containing site: String.Index,
        in source: String
    ) -> (name: String, body: String)? {
        // Every declaration start before the call site, in source order.
        //
        // The preceding character must be whitespace (or nothing), which is
        // what separates a DECLARATION from a call: `Foo.init(`, `self.init(`
        // and `.init(` all contain "init(" and none of them opens a body. Left
        // unguarded, a `.init(` a few lines above a guarded call would name the
        // next `{` block — an `if`, a closure — as the enclosing declaration
        // and report a correct call site as a violation under the key
        // `File.swift|init|…`. Same guard for `func `: it stops `myfunc ` and
        // any identifier ending in "func".
        var starts: [(index: String.Index, keyword: String)] = []
        for keyword in ["func ", "init("] {
            var searchStart = source.startIndex
            while let range = source.range(of: keyword, range: searchStart..<site) {
                searchStart = range.upperBound
                let isDeclarationPosition = range.lowerBound == source.startIndex
                    || source[source.index(before: range.lowerBound)].isWhitespace
                if isDeclarationPosition {
                    starts.append((range.lowerBound, keyword))
                }
            }
        }
        starts.sort { $0.index < $1.index }

        for start in starts.reversed() {
            guard let brace = SwiftSourceInspector.findOpenBrace(in: source, after: start.index)
            else { continue }
            let body = SwiftSourceInspector.bracedBody(in: source, startingAt: brace)
            guard !body.isEmpty else { continue }
            // `bracedBody` returns the text between the braces; the call is
            // inside this declaration iff it lies within that span.
            let bodyStart = source.index(after: brace)
            guard let bodyEnd = source.index(
                bodyStart, offsetBy: body.count, limitedBy: source.endIndex
            ) else { continue }
            guard site >= bodyStart, site < bodyEnd else { continue }

            let nameStart = source.index(start.index, offsetBy: start.keyword == "func " ? 5 : 0)
            let name = start.keyword == "func "
                ? String(source[nameStart...].prefix { $0.isLetter || $0.isNumber || $0 == "_" })
                : "init"
            return (name.isEmpty ? "init" : name, body)
        }
        return nil
    }

    // MARK: - Local-binding resolution

    /// The text immediately following `let <name> =` in `scope`, or nil.
    ///
    /// `scope` is the enclosing declaration's body, never the whole file —
    /// see ``enclosingDeclaration(containing:in:)`` for why that distinction
    /// is the difference between enforcing the rule and appearing to.
    ///
    /// A 300-character window rather than a statement parse: the question is
    /// only "does this binding come out of `canonicalize`", and the answer is
    /// always within a line or two of the `=`. A window that is too generous
    /// can only make the canary more permissive at a site that is already
    /// naming a local it also defines, never less.
    private func definition(of name: String, in source: String) -> String? {
        guard name.allSatisfy({ $0.isLetter || $0.isNumber || $0 == "_" }),
              name.first?.isNumber == false,
              let letRange = source.range(of: "let \(name) =")
                ?? source.range(of: "let \(name):")
        else { return nil }
        let end = source.index(
            letRange.upperBound,
            offsetBy: 300,
            limitedBy: source.endIndex
        ) ?? source.endIndex
        return String(source[letRange.upperBound..<end])
    }

    /// Whether `argument` is canonicalized — inline, or through at most two
    /// local bindings.
    ///
    /// Two hops rather than one because `runBackfill`, the reference-correct
    /// site, needs exactly two: `canonicalization = canonicalize(chunks)` and
    /// then `canonicalChunks = canonicalization.chunks.sorted(…)`. Two is a
    /// deliberate ceiling, not an oversight — a chain longer than that is
    /// unreadable at the call site, which is the property this rule is really
    /// about, and the honest fix for a third hop is to canonicalize inline or
    /// to allow-list the site with a reason.
    ///
    /// Measured escapes, and which direction each fails in (playhead-iu0t R2):
    ///   * three hops — reported as a VIOLATION. Fails closed; correct.
    ///   * `var` reassigned after a canonicalizing initialiser — reported as a
    ///     VIOLATION, because only `let` bindings resolve. Fails closed.
    ///   * a same-named binding in a DIFFERENT function — used to pass. That is
    ///     what `scope` being the enclosing body now prevents.
    ///   * **canonicalize, then collapse anyway** — used to pass, and this is
    ///     playhead-iu0t R5. See ``narrowsToFinalPass(_:)``.
    private func isCanonicalized(_ argument: String, in source: String) -> Bool {
        if Self.narrowsToFinalPass(argument) { return false }
        if argument.contains("canonicalize(") { return true }
        guard let first = definition(of: argument, in: source) else { return false }
        if Self.narrowsToFinalPass(first) { return false }
        if first.contains("canonicalize(") { return true }
        let leading = first
            .drop { $0 == " " || $0 == "\n" }
            .prefix { $0.isLetter || $0.isNumber || $0 == "_" }
        guard !leading.isEmpty,
              let second = definition(of: String(leading), in: source)
        else { return false }
        if Self.narrowsToFinalPass(second) { return false }
        return second.contains("canonicalize(")
    }

    /// Whether `expression` narrows a chunk set to the final pass.
    ///
    /// **playhead-iu0t R5.** ``isCanonicalized(_:in:)`` used to answer its
    /// question by testing whether the token `canonicalize(` APPEARS. That is a
    /// different question, and the gap between the two is this bead's own
    /// defect class committed inside the check named for it — a predicate
    /// called `isCanonicalized` reporting `true` for an argument that is
    /// provably not the canonical set. Planted and measured: the canary passed
    /// on
    ///
    ///     TranscriptAtomizer.atomize(
    ///         chunks: TranscriptChunkCanonicalizer.canonicalize(chunks)
    ///             .chunks.filter { $0.pass == "final" },
    ///
    /// which canonicalizes and then throws the result away — the exact reach
    /// loss this bead exists to stop, laundered through a token the check was
    /// looking for. Rule 2 cannot cover it either: its conjunction needs the
    /// `X.isEmpty ? chunks : X` fallback, and a bare `.filter` has none.
    ///
    /// Reuses ``finalPassFilterPattern`` rather than inventing a second
    /// spelling of "the collapse", so the two rules cannot drift apart.
    ///
    /// Applied to the resolved BINDINGS as well as to the inline argument,
    /// because `let preferred = canonicalize(chunks).chunks.filter { … }` is
    /// the same defect one hop away and is the shape CN06 actually had on disk.
    /// ``definition(of:in:)`` returns a 300-character window rather than a
    /// statement, so a final-pass filter in an unrelated statement just below a
    /// canonicalizing binding would report a false VIOLATION. That direction is
    /// fail-CLOSED, and measured: zero on the clean tree.
    private static func narrowsToFinalPass(_ expression: String) -> Bool {
        SwiftSourceInspector.regexOccurrences(
            of: finalPassFilterPattern, in: expression
        ) > 0
    }

    // MARK: - The pin

    /// **The rule.** Every production caller of `atomize` /
    /// `transcriptVersionHash` canonicalizes, or is allow-listed with a reason.
    func testEveryTranscriptVersionCallSiteCanonicalizes() throws {
        var violations: [String] = []
        var auditedByCall: [String: Int] = [:]

        for file in try productionSwiftFiles() {
            let rawSource = try String(contentsOf: file, encoding: .utf8)
            // Cheap pre-filter on the RECEIVER only. It used to test the two
            // call literals, which made it a second copy of the site finder
            // and a second place a respelling could hide (playhead-iu0t R5).
            guard rawSource.contains("TranscriptAtomizer") else { continue }
            let source = SwiftSourceInspector.strippingComments(rawSource)
            for (call, argument, site) in guardedArguments(inStripped: source) {
                auditedByCall[call, default: 0] += 1
                // The declaration the call sits in is BOTH the scope a local
                // binding may be resolved in and the scope an allow-list entry
                // licenses. A call with no enclosing declaration resolves
                // nothing and is licensed by nothing — it must canonicalize
                // inline.
                let scope = enclosingDeclaration(containing: site, in: source)
                // ONE predicate, both cases. This used to be two lines — the
                // scoped `isCanonicalized(…)` and then a bare
                // `argument.contains("canonicalize(")` for the file-scope case
                // — and playhead-iu0t R5 measured the consequence: hardening
                // `isCanonicalized` against a canonicalize-then-collapse
                // argument changed nothing, because the second line still let
                // the same argument through on the presence of the token. A
                // duplicated check is a check you have to remember to fix
                // twice, which is how this bead's defects keep being
                // reintroduced BY their own fixes. `isCanonicalized` already
                // treats an empty scope as "no local binding to resolve", so
                // the file-scope case needs no line of its own.
                if isCanonicalized(argument, in: scope?.body ?? "") { continue }
                let key = "\(file.lastPathComponent)|\(scope?.name ?? "<file-scope>")|\(argument)"
                if Self.allowedUncanonicalizedArguments[key] != nil { continue }
                violations.append(
                    "\(file.lastPathComponent) in \(scope?.name ?? "<file-scope>"): " +
                    "\(call)chunks: \(argument), …)  [allow-list key: \(key)]"
                )
            }
        }

        XCTAssertTrue(violations.isEmpty, """
            \(violations.count) transcript-version call site(s) do not canonicalize \
            and are not allow-listed:

            \(violations.joined(separator: "\n            "))

            `transcriptVersion` is derived from the chunks you hand in, and \
            `BackfillJobRunner`'s job id embeds it — so an uncanonicalized \
            caller mints rows in an id space no other dispatcher derives. This \
            is the defect that discarded 2,490 s of asset 53FC53E3's transcript \
            (playhead-iu0t) and that built the banner evidence catalog over \
            24.5 % of the transcript (its R1 review).

            Fix it by wrapping the argument in \
            `TranscriptChunkCanonicalizer.canonicalize(…).chunks` — or, from raw \
            store rows, `SemanticScanClaim.transcriptVersion(forPersistedChunks:)`. \
            Canonicalization is idempotent and returns single-pass input \
            byte-identically, so it is never wrong to add.

            Only add to `allowedUncanonicalizedArguments` if you can write which \
            of its two categories the site is in.
            """)

        // Vacuity guard, PER GUARDED FUNCTION. A refactor that renamed either
        // one, a pattern that stops compiling, or a regression in the argument
        // parser would empty that half of the walk and leave this test green
        // while enforcing nothing for it. Each floor is deliberately below its
        // measured count so ordinary churn does not trip it, but neither can
        // collapse to zero.
        //
        // MEASURED 8 total, playhead-iu0t R3, by temporarily raising the floor
        // and reading the failure back: `runBackfill`, `runPhase5ProjectorPhase`,
        // `recordSemanticScanClaim`, `runShadowFMPhase`, `SemanticScanClaim
        // .transcriptVersion(forPersistedChunks:)`, `RegionShadowPhase.run`,
        // `DebugEpisodeExportService.formatExport` and
        // `AnalysisCoordinator.pushEvidenceCatalog`. R3's comment said "Six …
        // as of playhead-iu0t R1" and was never re-measured after R1 and R2
        // each converted a site — the same stale-census defect this bead keeps
        // finding, in the bead's own enforcement. Re-measure when you change it;
        // do not infer it.
        //
        // **playhead-iu0t R5 split the 8 by function: `atomize` 6,
        // `transcriptVersionHash` 2.** One summed counter with a floor of 6 was
        // satisfied by `atomize` alone, so the `transcriptVersionHash` detector
        // could go completely dark and this guard would never notice — R3's
        // finding 3 and R4's finding 3, which R4 fixed per (file, FIELD) in
        // rule 3 and did not carry across to rule 1's own summed floor. Two
        // detectors, two floors; the sum is not one of them.
        for call in Self.guardedCalls {
            let seen = auditedByCall[call.name] ?? 0
            XCTAssertGreaterThanOrEqual(seen, call.vacuityFloor, """
                this canary audited only \(seen) call site(s) of \(call.name). \
                It enforces nothing for a function it cannot find: check that \
                `\(call.pattern)` still compiles and still matches the real \
                call spelling, and that the argument parser still returns the \
                `chunks:` expression. Summing the two functions into one \
                counter is what this guard used to do, and it is why this half \
                could have gone dark unnoticed.
                """)
        }
    }

    /// **The specific shape, banned by name.** The rule above is about the
    /// argument; this is about the expression that produced four wrong ones.
    /// `X.isEmpty ? chunks : X` where `X` is a final-pass filter reads as
    /// "prefer the better transcript" and means "discard everything the final
    /// pass did not re-transcribe", because `FinalPassRetranscriptionRunner`
    /// writes `final` rows only around already-detected candidate windows.
    ///
    /// Kept separate from the call-site rule because it catches the collapse
    /// one step EARLIER — at the point the wrong array is built, before anyone
    /// hands it anywhere — and because a future instance might feed something
    /// other than the atomizer.
    ///
    /// **playhead-iu0t R4 replaced half one's two literal substrings with a
    /// pattern, and added the vacuity anchor it never had.** R3 found both of
    /// those defects in rule 3 and did not check rule 2 for them; rule 2 had
    /// both. Half one used to be
    ///
    ///     source.contains(#"filter { $0.pass == "final" }"#)
    ///         || source.contains("filter { $0.pass == TranscriptPassType.final_.rawValue }")
    ///
    /// i.e. two exact spellings out of the many a Swift author can write. The
    /// same collapse as `filter { c in c.pass == "final" }`, or written against
    /// `TranscriptChunkCanonicalizer.finalPass`, was invisible to it. That is
    /// R3's own finding 1 — "an enforcement named for a rule that checked a
    /// spelling" — and it is this bead's defect class committed twice in the
    /// same file.
    ///
    /// Measured 2026-08-05, and re-measured after R5 added the negation arm:
    /// half one matches TWO files — `FinalPassRetranscriptionRunner.swift`
    /// (filters for final rows to read back what it just wrote) and
    /// `AnalysisStore.swift` (the legacy `pass != 'fast'` backfill). Half two
    /// matches one file (`EpisodeSummaryBackfillCoordinator`'s
    /// `adFreeChunks.isEmpty ? chunks : adFreeChunks`, a correct fallback over
    /// AD ranges rather than transcript passes). Requiring BOTH matches zero
    /// files, which is why the conjunction is the rule and neither half alone
    /// is.
    ///
    /// **What is anchored, and what deliberately is not.** Half one carries a
    /// floor, because it is the half with spelling risk and it has a permanent
    /// legitimate home: `FinalPassRetranscriptionRunner` must filter for final
    /// rows. Half two carries none on purpose — its idiom is ordinary code that
    /// may legitimately leave the tree, and a floor on it would assert a fact
    /// about unrelated files that a correct edit could break. So this rule is
    /// half-anchored, said out loud rather than implied.
    func testThePreHc7eFinalOnlyCollapseAppearsNowhereInProduction() throws {
        var violations: [String] = []
        var finalFilterFiles = 0

        for file in try productionSwiftFiles() {
            let source = SwiftSourceInspector.strippingComments(
                try String(contentsOf: file, encoding: .utf8)
            )
            // Half one: a final-pass filter, in CODE. Comments are stripped
            // first because three of the four fixed sites now QUOTE the
            // collapse in the comment explaining why it is gone, and a canary
            // that fired on its own postmortem would be deleted within a week.
            let filtersFinal = SwiftSourceInspector.regexOccurrences(
                of: Self.finalPassFilterPattern, in: source
            ) > 0
            guard filtersFinal else { continue }
            finalFilterFiles += 1
            // Half two: the fallback that turns that filter into a discard —
            // `X.isEmpty ? chunks : X`, i.e. "if the subset is empty use ALL of
            // them, otherwise ONLY the subset".
            //
            // BOTH halves are required, and each half alone is legitimate
            // somewhere: `FinalPassRetranscriptionRunner` filters for final
            // rows to read back what it just wrote, and
            // `EpisodeSummaryBackfillCoordinator` has
            // `adFreeChunks.isEmpty ? chunks : adFreeChunks`, a documented and
            // correct fallback over AD ranges rather than transcript passes.
            // Measured 2026-08-05 (re-measured after R5's negation arm): with
            // both halves required this matches zero files; half one alone
            // matches two and half two alone matches one, and every one of
            // those three is fine. A canary that cries wolf once is a canary
            // nobody reads.
            guard SwiftSourceInspector.regexOccurrences(
                of: #"\.isEmpty\s*\?\s*chunks\s*:"#, in: source
            ) > 0 else { continue }
            violations.append(file.lastPathComponent)
        }

        XCTAssertTrue(violations.isEmpty, """
            the pre-hc7e final-only collapse shape reappeared in: \
            \(violations.joined(separator: ", ")).

            `finals.isEmpty ? chunks : finals` is not "prefer the final pass" — \
            it is "throw away every second of audio the final pass did not \
            re-transcribe", and the final pass only ever re-transcribes around \
            already-detected candidates. What you want is \
            `TranscriptChunkCanonicalizer.canonicalize(chunks).chunks`, which \
            uses final text in the intervals the final pass covered and keeps \
            fast text everywhere else.
            """)

        // VACUITY ANCHOR for half one (playhead-iu0t R4). A conjunction is
        // green whenever EITHER conjunct stops matching, and half one is the
        // conjunct made of a pattern that a respelling — or a bad edit to the
        // pattern, which `regexOccurrences` reports as zero matches rather than
        // as an error — can silence. Measured 2 files after R5's negation arm;
        // the floor stays 1.
        //
        // Why 1 and not 2, when rule 3's floors are exact per (file, field):
        // there, the two fields are two SEPARATE detectors and a summed counter
        // would let one go dark. Here there is ONE detector matching two files,
        // so the floor's granularity is the detector, and 1 is what "this
        // detector still fires" costs. Raising it to 2 would assert that both
        // anchor files keep their filters — a fact about unrelated code that a
        // correct edit could break.
        XCTAssertGreaterThanOrEqual(finalFilterFiles, 1, """
            the final-pass-filter detector matched NOTHING anywhere under \
            `Playhead/`, so this rule's conjunction can no longer fire whatever \
            the tree contains. `FinalPassRetranscriptionRunner` filters for \
            `pass == "final"` rows to read back what it just wrote, and \
            `AnalysisStore`'s legacy backfill filters `pass != "fast"`; both are \
            measured anchors. Check `finalPassFilterPattern` and \
            `SwiftSourceInspector.strippingComments` before concluding the tree \
            got cleaner.
            """)
    }

    /// Half one of ``testThePreHc7eFinalOnlyCollapseAppearsNowhereInProduction()``:
    /// a closure that narrows a chunk set to the final pass.
    ///
    /// Receiver-agnostic on purpose — `$0.pass`, `c.pass`, `chunk.pass` all
    /// match — because the thing being banned is the SHAPE, and pinning it to
    /// `$0` would be banning a spelling again. `[^{}]{0,120}?` keeps the match
    /// inside a single closure body rather than spanning nested braces.
    ///
    /// **playhead-iu0t R5 added the NEGATION arm, because "keep the final pass"
    /// and "drop the fast pass" are the same instruction and R4's pattern only
    /// knew the first.** This is not a hypothetical respelling: instance #5 of
    /// this bead's five is exactly the negation — `AnalysisStore
    /// .backfillLegacyTranscriptChunksPhase1IfNeeded` selects `WHERE pass !=
    /// 'fast'` — and the header of this file already says so, calling it "the
    /// collapse spelled as a negation" and noting that it is why the two greps
    /// that found the first four instances could not see it. The rule claimed
    /// to ban a SHAPE while still requiring `==` against one of three final
    /// literals. Planted `filter { $0.pass != "fast" }` with the fallback and
    /// the canary passed.
    ///
    /// MEASURED 2026-08-05, both arms, over `Playhead/` minus the exempt files:
    /// half one now matches TWO files rather than one —
    /// `FinalPassRetranscriptionRunner` (reads back the final rows it just
    /// wrote) and `AnalysisStore` (the legacy backfill above, the negation's own
    /// permanent home). Neither carries half two, so the CONJUNCTION still
    /// matches zero files and the broadening costs no false positive.
    private static let finalPassFilterPattern =
        #"filter\s*\{[^{}]{0,120}?\.pass\s*(?:"# +
        #"==\s*(?:"final"|TranscriptPassType\.final_\.rawValue|finalPass)"# +
        #"|!=\s*(?:"fast"|TranscriptPassType\.fast\.rawValue|fastPass))"#

    /// **The THIRD sink** (playhead-iu0t R2).
    ///
    /// The rule above walks call sites of `atomize` / `transcriptVersionHash`,
    /// because those are the two functions that COMPUTE a `transcriptVersion`.
    /// They are not the only two ways to obtain one. `transcript_chunks` has a
    /// persisted `transcriptVersion` column, and reading it is a third route to
    /// the same value — one that no call-site walk can see, because there is no
    /// call.
    ///
    /// That column is a final-only collapse by construction. Its only writer is
    /// `AnalysisStore.backfillLegacyTranscriptChunksPhase1IfNeeded`, whose
    /// SELECT is `WHERE pass != 'fast'` — a final-pass filter spelled as a
    /// negation, which is why neither the `TranscriptPassType` grep nor the
    /// raw-`"final"` grep that found the first four instances could see it.
    /// Measured on the 2026-08-03 device pull: 29,247 `fast` rows all NULL,
    /// 8,251 `final` rows carrying exactly one value per asset, and on 53FC53E3
    /// that value is `55afd3e8bb41833c004ee7d4b1be7589` — this bead's field
    /// proof, the hash of the 32 final chunks alone.
    ///
    /// `EpisodeSummaryBackfillCoordinator.hydrate` was reading it as an
    /// episode summary's invalidation key. That was the FIFTH instance.
    ///
    /// The ban is on the READ, not the column: the store must still hydrate and
    /// persist the field, and `TranscriptEngineService` must still carry it
    /// through when it rebuilds a row. Both are exempt by file, with reasons.
    /// Everything else derives the version instead —
    /// `SemanticScanClaim.transcriptVersion(forPersistedChunks:)` is one call.
    ///
    /// **playhead-iu0t R3 replaced this rule's three literal needles with a
    /// receiver-rooted regex, because the needles enforced a SPELLING and not a
    /// rule — and that is this bead's own defect class committed by its own
    /// fix.** The needles were
    ///
    ///     [#"\.transcriptVersion"#, "chunk.transcriptVersion", "Chunk.transcriptVersion"]
    ///
    /// matched with `source.contains(_:)`, which is a literal substring search,
    /// not a regex — so the first needle was the 19 characters
    /// `\.transcriptVersion`, i.e. the KEYPATH form alone. R3 planted instance
    /// #4's exact defect three times, in three ordinary respellings, and the
    /// whole canary stayed green (exit 0):
    ///
    ///     chunks.compactMap { $0.transcriptVersion }.last   // closure form
    ///     chunks.last?.transcriptVersion                    // optional chain
    ///     rows.last -> row.transcriptVersion                // plain receiver
    ///
    /// CN07 restores the keypath form and dies, so the mutant read as proof of
    /// a rule when it was proof of one spelling of it.
    ///
    /// Two further things measured while fixing it, both of which changed the
    /// rule rather than only its regex:
    ///
    ///   * **There was no vacuity guard.** Every `\.transcriptVersion` keypath
    ///     left in the tree sits inside a COMMENT (the two postmortems quoting
    ///     the removed line), and comments are stripped — so the old needle set
    ///     matched nothing anywhere outside the exempt files. A rule matching
    ///     zero things is green whether or not it works, and rule 1 carries an
    ///     `audited >=` floor for exactly this reason while this one carried
    ///     nothing. The guard below re-uses the exempt files as the anchor: both
    ///     genuinely contain the reads this rule is about, so if the detector
    ///     stops seeing THEM it has stopped working.
    ///   * **It enforced half the sentence it points at.** `AnalysisStore`'s
    ///     doc says "do not read `transcript_chunks.atomOrdinal` or
    ///     `.transcriptVersion` … `TranscriptCanonicalizationRuleCanaryTests`
    ///     is what enforces it". Only the version half was enforced. Both are
    ///     now, and both measure clean: 0 matches outside the exempt files, 2
    ///     inside (`AnalysisStore` binds each on insert; `TranscriptEngineService`
    ///     copies each through).
    ///
    /// **What it still cannot see, stated rather than implied.** The receiver
    /// must be chunk-shaped: the regex roots at an identifier containing
    /// `chunk`/`chunks` and walks member access, optional chaining, subscripts
    /// and closure bodies from there. A `TranscriptChunk` bound to a name with
    /// no `chunk` in it — `guard let row = someOtherArray.last` — is invisible,
    /// and closing that needs type information this test does not have. That is
    /// a stated limit, not a completeness claim; the difference is the whole
    /// lesson of hc7e's sentence.
    func testNoProductionConsumerReadsThePersistedChunkTranscriptVersion() throws {
        // Files that legitimately touch the columns, and why.
        let exempt: [String: String] = [
            "AnalysisStore.swift": "defines the columns: hydrates the row and binds them on insert",
            "TranscriptEngineService.swift":
                "field-preserving copy when an existing row is rebuilt — carries the values through, never reads their meaning",
        ]

        // Both persisted columns the `AnalysisStore` doc bans reading. Each is
        // written from the legacy `WHERE pass != 'fast'` backfill and therefore
        // describes the FINAL-ONLY chunk set, whatever its name suggests.
        let bannedFields = ["transcriptVersion", "atomOrdinal"]

        // A read of the field whose receiver chain is rooted in a chunk-shaped
        // identifier. `[^;]{0,160}?` is what carries it across the member
        // access, optional chain, subscript or closure body in between —
        // instance #4's real spelling put the receiver and both narrowing calls
        // on three separate source lines.
        func pattern(for field: String) -> String {
            #"\b\w*[Cc]hunks?\b[^;]{0,160}?\."# + field + #"\b"#
        }

        var violations: [String] = []
        var exemptAnchorHits: [String: Int] = [:]
        for file in try productionSwiftFiles() {
            let name = file.lastPathComponent
            let source = SwiftSourceInspector.strippingComments(
                try String(contentsOf: file, encoding: .utf8)
            )
            for field in bannedFields {
                let hits = SwiftSourceInspector.regexOccurrences(
                    of: pattern(for: field), in: source
                )
                guard hits > 0 else { continue }
                if exempt[name] != nil {
                    // Keyed per (file, FIELD) — see the guard below for why the
                    // per-file sum this used to accumulate could not do the job.
                    exemptAnchorHits["\(name)|\(field)", default: 0] += hits
                } else {
                    violations.append("\(name): \(hits) read(s) of chunk.\(field)")
                }
            }
        }

        XCTAssertTrue(violations.isEmpty, """
            \(violations.count) production site(s) read a PERSISTED \
            `transcript_chunks` identity column:

            \(violations.joined(separator: "\n            "))

            Those columns are written only by \
            `AnalysisStore.backfillLegacyTranscriptChunksPhase1IfNeeded`, whose \
            SELECT is `WHERE pass != 'fast'` — so they describe a FINAL-ONLY \
            chunk set, the pre-hc7e collapse in persisted form, and they are \
            stale for any asset transcribed since. Derive instead: \
            `SemanticScanClaim.transcriptVersion(forPersistedChunks: chunks)` \
            for the version, and the in-memory atom array for the ordinal.
            """)

        // VACUITY GUARD. This rule matched nothing at all before R3 and was
        // green for that reason rather than for a good one. The exempt files
        // are the anchor because they genuinely contain the reads this rule is
        // about: `AnalysisStore` binds both columns on insert and
        // `TranscriptEngineService` copies both through. If the detector stops
        // seeing those, it has stopped detecting — a bad regex (which
        // `regexOccurrences` reports as zero matches rather than as an error),
        // a broken comment-stripper, or an empty file walk all land here.
        //
        // **playhead-iu0t R4: per (file, FIELD), not per file.** R3 added this
        // guard and keyed it on the file alone, summing both fields into one
        // counter — so `atomOrdinal`'s half of `pattern(for:)` could go vacuous
        // and the guard would stay green off `transcriptVersion`'s hits. That
        // is R3's own finding 3 ("it enforced half the sentence it points at")
        // reintroduced one level up, inside the fix for R3's finding 2. The two
        // fields are separate detectors and each needs its own floor.
        //
        // MEASURED 2026-08-05, and the reason a floor of 1 per pair is exact
        // rather than generous: `AnalysisStore` 1 + 1, `TranscriptEngineService`
        // 1 + 1. Four pairs, one hit each.
        for name in exempt.keys {
            for field in bannedFields {
                XCTAssertGreaterThanOrEqual(exemptAnchorHits["\(name)|\(field)"] ?? 0, 1, """
                    the persisted-column detector found NO read of \
                    `chunk.\(field)` in \(name), which is one of the two files \
                    that certainly reads it. This rule enforces nothing for a \
                    field it cannot match — check `pattern(for:)`, \
                    `strippingComments`, and the file walk before assuming the \
                    tree got cleaner. Summing the two fields into one per-file \
                    counter is what this guard used to do, and it is why the \
                    `\(field)` half could have gone dark unnoticed.
                    """)
            }
        }
    }

    // MARK: - The walk's own rail (playhead-iu0t R4)

    /// **The argument parser fails CLOSED.**
    ///
    /// Rules 1–3 all read the tree, so their evidence is only as good as the
    /// walk that produces it — and the walk's one failure direction was
    /// invisible from the tree, because it only shows up for source the tree
    /// does not currently contain. R3 recorded the unbounded `chunks:` search
    /// as a mis-attribution risk and measured it non-live; the branch it fed
    /// also DROPPED a call site silently, which is the direction that matters
    /// in a canary. This exercises the parser on synthetic source instead of
    /// waiting for production to grow the shape.
    ///
    /// Both consequences are covered, because the finding is that R3 recorded
    /// only one of them:
    ///   * **STEAL** — a later call's `chunks:` is adopted by an earlier call
    ///     that has none. Measured against the R3 parser on the source below:
    ///     the first call reported the argument
    ///     `TranscriptChunkCanonicalizer.canonicalize(raw).chunks`, so a site
    ///     that canonicalizes NOTHING read as canonical and passed rule 1. That
    ///     is worse than a missed site — it launders a violation into a pass.
    ///   * **DROP** — with no later `chunks:` anywhere in the file, the old
    ///     `guard … else { continue }` skipped the site entirely: unaudited,
    ///     uncounted against the `audited` floor, unreported.
    ///
    /// And the sentinel that replaces both resolves to nothing and is on no
    /// allow-list, so it reaches
    /// ``testEveryTranscriptVersionCallSiteCanonicalizes()`` as a violation
    /// rather than as silence.
    func testTheArgumentParserRecordsAGuardedCallThatHasNoChunksArgument() {
        // `atomize` here takes no `chunks:` at all; the only `chunks:` in the
        // string belongs to the SECOND call, 200 characters later.
        let source = """
            func a() {
                let x = TranscriptAtomizer.atomize(atoms: someAtoms, analysisAssetId: id)
                _ = x
            }
            func b() {
                let y = TranscriptAtomizer.atomize(
                    chunks: TranscriptChunkCanonicalizer.canonicalize(raw).chunks,
                    analysisAssetId: id
                )
                _ = y
            }
            """
        let parsed = guardedArguments(inStripped: source)

        XCTAssertEqual(parsed.count, 2, """
            both guarded calls must be recorded. Before playhead-iu0t R4 the \
            first one was dropped entirely — not reported, not counted toward \
            the `audited` floor — because the `chunks:` search was unbounded and \
            its `guard … else { continue }` skipped the site.
            """)
        XCTAssertEqual(parsed.first?.argument, Self.missingChunksArgument,
                       "a call with no `chunks:` of its own must not borrow the next call's")
        XCTAssertEqual(
            parsed.last?.argument,
            "TranscriptChunkCanonicalizer.canonicalize(raw).chunks",
            "and a well-formed call must still parse exactly as before"
        )

        // The DROP consequence: nothing later in the file supplies a `chunks:`
        // for the parser to borrow, so R3's `guard … else { continue }` removed
        // this site from the walk altogether.
        let lone = """
            func c() {
                _ = TranscriptAtomizer.transcriptVersionHash(atoms: someAtoms)
            }
            """
        let parsedLone = guardedArguments(inStripped: lone)
        XCTAssertEqual(parsedLone.count, 1, """
            a guarded call with no `chunks:` anywhere after it must still be \
            recorded. Dropping it is the fail-OPEN direction: the site is \
            unaudited, uncounted against the `audited` floor, and unreported.
            """)
        XCTAssertEqual(parsedLone.first?.argument, Self.missingChunksArgument)

        // Property 3: the sentinel is inert to every escape the rule offers.
        XCTAssertFalse(Self.missingChunksArgument.contains("canonicalize("),
                       "the sentinel must not look canonicalized")
        XCTAssertFalse(isCanonicalized(Self.missingChunksArgument, in: source),
                       "and must resolve to no local binding")
        XCTAssertNil(
            Self.allowedUncanonicalizedArguments.keys
                .first { $0.hasSuffix("|\(Self.missingChunksArgument)") },
            "and must be on no allow-list, so it lands as a violation"
        )
    }

    /// **The site finder sees a call split across lines** (playhead-iu0t R5).
    ///
    /// The rail above pins what happens once a guarded call has been FOUND.
    /// This one pins the step before it, which is where the walk's remaining
    /// fail-open direction was: `guardedCalls` held literal substrings, so
    /// `TranscriptAtomizer` and `.atomize(` on two lines was not a call site at
    /// all — unaudited, uncounted, unreported, and indistinguishable from a
    /// clean tree.
    ///
    /// Synthetic source rather than a fixture file, for the same reason as the
    /// rail above: the tree contains no split-line guarded call today, and
    /// waiting for one is how this stayed invisible. The two spellings are
    /// asserted to parse IDENTICALLY, which is the property that matters — a
    /// canary that finds the site but reads a different argument out of it has
    /// only moved the hole.
    func testTheSiteFinderSeesACallSplitAcrossLines() {
        let split = """
            func a() {
                let (_, v) = TranscriptAtomizer
                    .atomize(
                        chunks: rawChunks,
                        analysisAssetId: id
                    )
                _ = v
            }
            """
        let inline = """
            func a() {
                let (_, v) = TranscriptAtomizer.atomize(
                    chunks: rawChunks,
                    analysisAssetId: id
                )
                _ = v
            }
            """
        let parsedSplit = guardedArguments(inStripped: split)
        let parsedInline = guardedArguments(inStripped: inline)

        XCTAssertEqual(parsedSplit.count, 1, """
            a guarded call split across lines must still be a call site. Before \
            playhead-iu0t R5 it was invisible: `guardedCalls` was a literal \
            substring search, so one line break removed the site from the walk \
            entirely — not reported and not counted toward the vacuity floor.
            """)
        XCTAssertEqual(parsedSplit.first?.argument, "rawChunks",
                       "and must yield the same argument the inline spelling does")
        XCTAssertEqual(parsedSplit.map(\.argument), parsedInline.map(\.argument),
                       "the two spellings are the same call and must parse alike")
        XCTAssertEqual(parsedSplit.first?.call, "TranscriptAtomizer.atomize(",
                       "and must be attributed to the function it actually calls")

        // Whitespace around the dot, the other half of the same broadening.
        let spaced = guardedArguments(
            inStripped: "func b() { _ = TranscriptAtomizer . transcriptVersionHash(chunks: raw) }"
        )
        XCTAssertEqual(spaced.count, 1)
        XCTAssertEqual(spaced.first?.argument, "raw")

        // And the argument is still judged, not merely found: `rawChunks`
        // resolves to no canonicalizing binding, so it reaches the rule as a
        // violation rather than as silence.
        XCTAssertFalse(isCanonicalized("rawChunks", in: split),
                       "a found site must still be able to FAIL the rule")
    }

    /// **A site that canonicalizes and then collapses anyway does not pass**
    /// (playhead-iu0t R5). See ``narrowsToFinalPass(_:)`` for the measurement.
    ///
    /// Inline and through a binding, because the binding form is the shape
    /// `pushEvidenceCatalog` actually had on disk (CN06's `preferred`), and a
    /// check that only saw the inline form would leave the real one open.
    func testCanonicalizingAndThenCollapsingIsNotCanonicalized() {
        let inline = "TranscriptChunkCanonicalizer.canonicalize(chunks).chunks" +
            #".filter { $0.pass == "final" }"#
        XCTAssertFalse(isCanonicalized(inline, in: ""), """
            an argument that canonicalizes and then narrows to the final pass \
            is not canonicalized. `isCanonicalized` used to answer by testing \
            whether the token `canonicalize(` appears, which is a different \
            question — and the gap between the two is this bead's own defect \
            class inside the check named for it.
            """)

        let bound = """
            func c() {
                let preferred = TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
                    .filter { chunk in chunk.pass == "final" }
                _ = preferred
            }
            """
        XCTAssertFalse(isCanonicalized("preferred", in: bound),
                       "and the same defect one hop away, through a local binding")

        // The negation spelling of the same collapse — instance #5's shape.
        let negated = """
            func d() {
                let preferred = TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
                    .filter { $0.pass != "fast" }
                _ = preferred
            }
            """
        XCTAssertFalse(isCanonicalized("preferred", in: negated),
                       "\"drop the fast pass\" is the same instruction as \"keep the final pass\"")

        // CONTROL: the reference-correct shapes must still pass, or this check
        // has bought fail-closed at the cost of failing on correct code.
        XCTAssertTrue(
            isCanonicalized("TranscriptChunkCanonicalizer.canonicalize(chunks).chunks", in: ""),
            "the plain canonical argument must still pass"
        )
        let twoHop = """
            func e() {
                let canonicalization = TranscriptChunkCanonicalizer.canonicalize(chunks)
                let canonicalChunks = canonicalization.chunks.sorted(by: canonicalTimeOrder)
                _ = canonicalChunks
            }
            """
        XCTAssertTrue(isCanonicalized("canonicalChunks", in: twoHop),
                      "and so must `runBackfill`'s two-hop binding, sort and all")
    }
}
