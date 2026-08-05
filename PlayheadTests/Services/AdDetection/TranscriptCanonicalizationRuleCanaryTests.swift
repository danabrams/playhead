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
// four separate times by the pre-hc7e collapse
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
// A survey of call sites decays the instant somebody adds a call site, and
// nothing tells you when it did. This canary reads the source instead: it
// enumerates every `.swift` file under `Playhead/`, finds every call to the two
// functions, extracts the `chunks:` argument expression at each one, and fails
// unless that expression canonicalizes inline or appears on the allow-list
// below with a written reason. Adding an uncanonicalized caller is a red test.
//
// It is deliberately NARROW. It does not check that the whole program is
// correct about transcripts; it checks exactly the one predicate the four
// defects violated. Two escape hatches are honest and named: a parameter that
// its callers canonicalize, and a consumer that mints nothing.
//
// XCTest so the canary is filterable from a test plan (`xctestplan`
// `selectedTests`/`skippedTests` silently ignore Swift Testing identifiers —
// see project memory `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class TranscriptCanonicalizationRuleCanaryTests: XCTestCase {

    /// The two functions whose `chunks:` argument decides `transcriptVersion`.
    private static let guardedCalls = [
        "TranscriptAtomizer.atomize(",
        "TranscriptAtomizer.transcriptVersionHash(",
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
    /// Keys are `"<FileName>.swift|<argument expression>"`, NOT the expression
    /// alone. That matters: `chunks` is the most ordinary parameter name in the
    /// codebase, so an expression-only key would silently pre-approve every
    /// future `atomize(chunks: chunks)` in a file nobody has looked at — which
    /// is the same "approve by category, discover the instance later" mistake
    /// the prose survey made. Scoping to the file means a new call site is
    /// flagged even when it is spelled exactly like an allowed one.
    private static let allowedUncanonicalizedArguments: [String: String] = [
        // (1) `runShadowFMPhase(chunks:)` and `recordSemanticScanClaim(chunks:)`
        // — both parameters, both reached only from `runBackfill` (which passes
        // `canonicalChunks`) and `retryShadowFMPhaseForSession` (which passes
        // `canonicalize(chunks).chunks` since playhead-iu0t). This is the frame
        // the shipped defect lived one level above.
        "AdDetectionService.swift|chunks": """
            a parameter; audited at the CALLERS. `runShadowFMPhase` and \
            `recordSemanticScanClaim` are reached only from `runBackfill` \
            (canonicalChunks) and `retryShadowFMPhaseForSession` \
            (canonicalize(chunks).chunks).
            """,

        // (1) `RegionShadowPhase.run(_:)` — a parameter on a value type. Its
        // sole production call site is `AdDetectionService.runBackfill`, which
        // builds `RegionShadowPhase.Input(chunks: canonicalChunks, …)`.
        "RegionShadowPhase.swift|input.chunks": """
            a parameter; `RegionShadowPhase.run`'s only production caller is \
            `AdDetectionService.runBackfill`, which passes `canonicalChunks`.
            """,

        // (2) A consumer that mints nothing: a human-readable text dump. Its
        // catalog is rendered into the export and discarded; no row, no job id,
        // no persisted `transcriptVersion`. It also stamps a deliberately
        // distinct `normalizationHash` of "debug-export".
        "DebugEpisodeExportService.swift|chunks": """
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

    /// The `chunks:` argument expression at each guarded call site in `source`,
    /// paired with the call it belongs to. Comments are stripped first so a
    /// call written inside a comment (this file's own prose, the rule's
    /// explanatory blocks) is never mistaken for a real one.
    private func guardedArguments(in rawSource: String) -> [(call: String, argument: String)] {
        let source = SwiftSourceInspector.strippingComments(rawSource)
        var found: [(String, String)] = []
        for call in Self.guardedCalls {
            var searchStart = source.startIndex
            while let callRange = source.range(of: call, range: searchStart..<source.endIndex) {
                searchStart = callRange.upperBound
                guard let label = source.range(
                    of: "chunks:", range: callRange.upperBound..<source.endIndex
                ) else { continue }
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
                found.append((call, expression.trimmingCharacters(in: .whitespacesAndNewlines)))
            }
        }
        return found
    }

    // MARK: - Local-binding resolution

    /// The text immediately following `let <name> =` in `source`, or nil.
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
    private func isCanonicalized(_ argument: String, in source: String) -> Bool {
        if argument.contains("canonicalize(") { return true }
        guard let first = definition(of: argument, in: source) else { return false }
        if first.contains("canonicalize(") { return true }
        let leading = first
            .drop { $0 == " " || $0 == "\n" }
            .prefix { $0.isLetter || $0.isNumber || $0 == "_" }
        guard !leading.isEmpty,
              let second = definition(of: String(leading), in: source)
        else { return false }
        return second.contains("canonicalize(")
    }

    // MARK: - The pin

    /// **The rule.** Every production caller of `atomize` /
    /// `transcriptVersionHash` canonicalizes, or is allow-listed with a reason.
    func testEveryTranscriptVersionCallSiteCanonicalizes() throws {
        var violations: [String] = []
        var audited = 0

        for file in try productionSwiftFiles() {
            let rawSource = try String(contentsOf: file, encoding: .utf8)
            guard Self.guardedCalls.contains(where: { rawSource.contains($0) }) else { continue }
            let source = SwiftSourceInspector.strippingComments(rawSource)
            for (call, argument) in guardedArguments(in: rawSource) {
                audited += 1
                if isCanonicalized(argument, in: source) { continue }
                let key = "\(file.lastPathComponent)|\(argument)"
                if Self.allowedUncanonicalizedArguments[key] != nil { continue }
                violations.append(
                    "\(file.lastPathComponent): \(call)chunks: \(argument), …)"
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

        // Vacuity guard. A refactor that renamed either guarded function, or a
        // regression in the argument parser, would empty the walk and leave
        // this test green while enforcing nothing. Six production call sites
        // exist as of playhead-iu0t R1; the floor is deliberately below that so
        // ordinary churn does not trip it, but a collapse to zero does.
        XCTAssertGreaterThanOrEqual(audited, 6, """
            this canary audited only \(audited) call site(s). It enforces nothing \
            when it finds nothing: check that `guardedCalls` still names the \
            real function paths and that the argument parser still returns the \
            `chunks:` expression.
            """)
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
    func testThePreHc7eFinalOnlyCollapseAppearsNowhereInProduction() throws {
        var violations: [String] = []

        for file in try productionSwiftFiles() {
            let source = SwiftSourceInspector.strippingComments(
                try String(contentsOf: file, encoding: .utf8)
            )
            // Half one: a final-pass filter, in CODE. Comments are stripped
            // first because three of the four fixed sites now QUOTE the
            // collapse in the comment explaining why it is gone, and a canary
            // that fired on its own postmortem would be deleted within a week.
            let filtersFinal = source.contains(#"filter { $0.pass == "final" }"#)
                || source.contains("filter { $0.pass == TranscriptPassType.final_.rawValue }")
            guard filtersFinal else { continue }
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
            // Measured 2026-08-05: with both halves required this matches zero
            // files; with either half alone it matches one, and that one is
            // fine. A canary that cries wolf once is a canary nobody reads.
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
    }
}
