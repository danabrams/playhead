// AdDetectionServiceProfileKeyingCanaryTests.swift
// playhead-2kxd — THE RULE, PLUS SOMETHING THAT ENFORCES IT.
//
// The behavioural suite next door (`AdDetectionServiceProfileKeyingTests`)
// proves that the three read sites which exist TODAY answer with the right
// show's profile. It cannot prove anything about the fourth one somebody
// writes next month, and that is exactly how this defect survived three
// review rounds: each round hardened one read site by hand, and the shape
// went on manufacturing sites.
//
// So the durable form is a rule — *the per-show profile storage is reached
// only through its two accessors, and every read names a show identity* —
// and a canary that fails the build's test phase when a line breaks it.
// Same construction as `TranscriptCanonicalizationRuleCanaryTests`
// (playhead-iu0t), and for the same stated reason: a completeness claim
// written in prose decays silently the moment someone adds a caller.
//
// All three checks run against the COMMENT- AND STRING-STRIPPED source, so
// the doc comment on `podcastProfilesByShowId` — which necessarily mentions
// the identifier several times — cannot satisfy or trip them. The stripper's
// own behaviour is pinned by the positive controls at the bottom, because a
// canary whose stripper silently ate its anchors would be green for the worst
// possible reason.

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService profile-keying source canary (playhead-2kxd)")
struct AdDetectionServiceProfileKeyingCanaryTests {

    private static let servicePath =
        "Playhead/Services/AdDetection/AdDetectionService.swift"

    /// The stored property that holds the per-show profiles. Named once here
    /// so a rename is a one-line change to this canary rather than a silent
    /// loss of coverage.
    private static let storage = "podcastProfilesByShowId"

    /// The ONLY lines allowed to mention ``storage``. Two accessors, the
    /// declaration, and the init-time seed. Anything else is a new read or
    /// write path that has not been reviewed for *whose profile is this*.
    private static let sanctionedLines: Set<String> = [
        "private var podcastProfilesByShowId: [String: PodcastProfile] = [:]",
        "return podcastProfilesByShowId[showId]",
        "podcastProfilesByShowId[profile.podcastId] = profile",
        "self.podcastProfilesByShowId = Self.seededProfileMap(podcastProfile)",
        // `#if DEBUG` only, and a READ of the key set rather than of any
        // profile: it is what lets a test tell "the empty id was never
        // stored" from "the reader refuses to look it up".
        "Set(podcastProfilesByShowId.keys)",
    ]

    /// The expressions a read is allowed to be keyed on. Both name a show
    /// identity that was handed IN — `podcastId` at every production read,
    /// `showId` inside the DEBUG accessor.
    ///
    /// This is an allow-list of NAMES rather than a shape check, and the
    /// difference is the whole point: `catalogShowId` and `analysisAssetId`
    /// are both in scope inside `runBackfill`, both are perfectly well-formed
    /// identifiers, and neither is this episode's show. A predicate that only
    /// asked "is it an identifier rather than a literal?" would wave both
    /// through — which is the same mistake as reading a value that names one
    /// thing as though it named another.
    private static let sanctionedReadArguments: Set<String> = [
        "podcastId",
        "showId",
    ]

    private func strippedService() throws -> String {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.servicePath
        )
        return SwiftSourceInspector.strippingCommentsAndStrings(source)
    }

    // MARK: - 1. The storage is reached only through its accessors

    @Test("the per-show profile storage is mentioned only by its declaration, its two accessors and the init seed")
    func theProfileStorageIsReachedOnlyThroughItsAccessors() throws {
        let stripped = try strippedService()
        let offending = Self.linesMentioningStorage(in: stripped)
            .filter { !Self.sanctionedLines.contains($0.text) }

        #expect(
            offending.isEmpty,
            """
            playhead-2kxd: `\(Self.storage)` is reached outside its accessors, \
            at line(s) \(offending.map(\.number).map(String.init).joined(separator: ", ")):

            \(offending.map { "  \($0.number): \($0.text)" }.joined(separator: "\n"))

            Every read must go through `cachedPodcastProfile(forShowId:)` and \
            every write through `cachePodcastProfile(_:)`, because those are \
            the two places that answer "whose profile is this?". A direct \
            subscript, a `.values.first`, or a `for (_, profile) in ...` \
            re-creates the singleton-standing-for-a-set this bead removed. \
            If a new access is genuinely correct, ADD ITS LINE to \
            `sanctionedLines` in this canary — deliberately, in a diff a \
            reviewer reads.
            """
        )

        // And the four sanctioned forms must all still be PRESENT. An
        // allow-list that matches nothing is the failure mode
        // `scripts/singleton-slot-allowlist.json` is closed in both
        // directions against: a licence for a line nobody can find means the
        // line moved, and whatever inherits its shape inherits the amnesty.
        let present = Set(Self.linesMentioningStorage(in: stripped).map(\.text))
        let missing = Self.sanctionedLines.subtracting(present)
        #expect(
            missing.isEmpty,
            """
            playhead-2kxd: this canary sanctions line(s) that no longer exist \
            in \(Self.servicePath):

            \(missing.sorted().map { "  \($0)" }.joined(separator: "\n"))

            The accessors were renamed, reshaped or deleted. Update this \
            canary in the same diff, or it is licensing nothing.
            """
        )
    }

    // MARK: - 2. Every read names a show identity

    @Test("every read of the per-show profile passes a show identity, never a literal and never nil")
    func everyProfileReadNamesAShowIdentity() throws {
        let stripped = try strippedService()
        let arguments = Self.readCallArguments(in: stripped)

        #expect(
            !arguments.isEmpty,
            """
            playhead-2kxd: no call to `cachedPodcastProfile(forShowId:` found \
            in \(Self.servicePath). Either the accessor was renamed (update \
            this canary) or nothing reads the profile any more — in which case \
            the storage should go, not the canary.
            """
        )

        let bad = arguments.filter { !Self.sanctionedReadArguments.contains($0.text) }
        #expect(
            bad.isEmpty,
            """
            playhead-2kxd: a profile read is keyed on something other than the \
            show identity handed in, at line(s) \
            \(bad.map(\.number).map(String.init).joined(separator: ", ")):

            \(bad.map { "  \($0.number): cachedPodcastProfile(forShowId: \($0.text))" }.joined(separator: "\n"))

            The question every read has to answer is *whose profile is this*. \
            A string literal hardcodes one show; `nil` asks for "the current \
            one", which is the question this bead deleted; and an in-scope \
            identifier from a DIFFERENT identity space — `catalogShowId`, \
            `analysisAssetId` — is the failure that looks most like success. \
            Pass the request's `podcastId`. If a new call site genuinely \
            threads the show under another name, add that name to \
            `sanctionedReadArguments`, deliberately, in a diff a reviewer \
            reads.
            """
        )
    }

    // MARK: - 3. The shadow phase's profile and its show id are the SAME show

    /// The narrowest statement of the original defect: `RegionShadowPhase`
    /// receives BOTH a `podcastProfile:` and a `podcastId:`, and they used to
    /// come from different places — the id from the request, the profile from
    /// a slot. A phase told "this is show A" while handed show B's sponsor
    /// lexicon scans A's audio for B's advertisers.
    ///
    /// Note what this catches that canary 2 cannot: `catalogShowId` is in
    /// scope at that call site and is a perfectly well-formed show identity,
    /// so `cachedPodcastProfile(forShowId: catalogShowId)` passes every other
    /// check here and still hands the phase a profile from a different
    /// identity space.
    @Test("RegionShadowPhase gets the profile of the very show it is told about")
    func theShadowPhaseGetsTheProfileOfTheShowItIsToldAbout() throws {
        let stripped = try strippedService()
        guard let call = Self.balancedCall(after: "RegionShadowPhase.Input(", in: stripped) else {
            Issue.record("Could not locate the `RegionShadowPhase.Input(` argument list")
            return
        }

        guard let profileArg = Self.firstCapture(
            of: #"podcastProfile:\s*cachedPodcastProfile\(forShowId:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)"#,
            in: call
        ) else {
            Issue.record(
                """
                playhead-2kxd: `RegionShadowPhase.Input`'s `podcastProfile:` \
                argument is no longer `cachedPodcastProfile(forShowId: <id>)`. \
                If it now comes from somewhere else, that somewhere else has \
                to answer "whose profile is this?" — and this canary has to \
                be taught the new shape.
                """
            )
            return
        }

        guard let idArg = Self.firstCapture(
            of: #"podcastId:\s*([A-Za-z_][A-Za-z0-9_]*)\s*,"#,
            in: call
        ) else {
            Issue.record("Could not read `RegionShadowPhase.Input`'s `podcastId:` argument")
            return
        }

        #expect(
            profileArg == idArg,
            """
            playhead-2kxd: `RegionShadowPhase` is told the episode belongs to \
            `\(idArg)` while being handed the profile of `\(profileArg)`. \
            Those must be the same expression — the profile becomes that \
            phase's per-show sponsor lexicon.
            """
        )
    }

    // MARK: - 4. Positive controls (prove the instrument fires)

    /// A canary that fires zero times because its pattern never matches is
    /// indistinguishable from a clean tree. These three fixtures make each
    /// predicate fire on purpose.

    @Test("control: the storage check rejects a direct subscript and ignores a comment mention")
    func storageCheckFiresOnADirectAccess() {
        let spoofed = """
        // podcastProfilesByShowId is mentioned here in a comment only
        /// and here: podcastProfilesByShowId[someShow]
        let spoof = "podcastProfilesByShowId[literal]"
        """
        let strippedSpoof = SwiftSourceInspector.strippingCommentsAndStrings(spoofed)
        #expect(
            Self.linesMentioningStorage(in: strippedSpoof).isEmpty,
            """
            the stripper let a comment/string mention of `\(Self.storage)` \
            through. Canary 1 would then be trippable by documentation, which \
            is the false-positive direction — and, worse, a real access could \
            be hidden by dressing it up next to one.
            """
        )

        let real = """
        func leak() -> PodcastProfile? {
            podcastProfilesByShowId.values.first
        }
        """
        let strippedReal = SwiftSourceInspector.strippingCommentsAndStrings(real)
        let found = Self.linesMentioningStorage(in: strippedReal)
            .filter { !Self.sanctionedLines.contains($0.text) }
        #expect(
            found.count == 1,
            "canary 1 must report a real `.values.first` back door; it found \(found.count)"
        )
    }

    @Test("control: the read check rejects nil, a literal, and a WRONG in-scope identifier")
    func readCheckFiresOnALiteralNilOrWrongIdentity() {
        // A string literal survives stripping as a blanked pair of quotes,
        // which is exactly the shape the check has to refuse.
        let literalCall = SwiftSourceInspector.strippingCommentsAndStrings(
            #"_ = cachedPodcastProfile(forShowId: "show-hardcoded")"#
        )
        let literalArgs = Self.readCallArguments(in: literalCall)
        #expect(literalArgs.count == 1, "the extractor must find the call at all")
        #expect(
            literalArgs.allSatisfy { !Self.sanctionedReadArguments.contains($0.text) },
            "a hardcoded show id must be refused; got \(literalArgs.map(\.text))"
        )

        for rejected in ["nil", "catalogShowId", "analysisAssetId", ""] {
            let call = "_ = cachedPodcastProfile(forShowId: \(rejected))"
            let args = Self.readCallArguments(in: call)
            #expect(args.count == 1, "the extractor must find `\(rejected)`")
            #expect(
                args.allSatisfy { !Self.sanctionedReadArguments.contains($0.text) },
                """
                `\(rejected)` must be refused. `nil` asks for "the current \
                show"; `catalogShowId` and `analysisAssetId` are in-scope \
                identifiers from other identity spaces, which is the \
                rejection that matters most and the one a shape-only check \
                would miss.
                """
            )
        }

        // …and the two sanctioned names must be ACCEPTED, or the check is a
        // rule nobody could ever satisfy.
        for accepted in ["podcastId", "showId"] {
            let args = Self.readCallArguments(in: "_ = cachedPodcastProfile(forShowId: \(accepted))")
            #expect(args.map(\.text) == [accepted])
            #expect(args.allSatisfy { Self.sanctionedReadArguments.contains($0.text) })
        }
    }

    @Test("control: the shadow-phase check rejects a profile keyed on a DIFFERENT identifier")
    func shadowPhaseCheckFiresOnAMismatch() {
        let fixture = """
        let regionInput = RegionShadowPhase.Input(
            priors: showPriors,
            podcastProfile: cachedPodcastProfile(forShowId: catalogShowId),
            podcastId: podcastId,
            knowledgeStore: sponsorKnowledgeStore
        )
        """
        guard let call = Self.balancedCall(after: "RegionShadowPhase.Input(", in: fixture) else {
            Issue.record("fixture did not parse")
            return
        }
        let profileArg = Self.firstCapture(
            of: #"podcastProfile:\s*cachedPodcastProfile\(forShowId:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)"#,
            in: call
        )
        let idArg = Self.firstCapture(
            of: #"podcastId:\s*([A-Za-z_][A-Za-z0-9_]*)\s*,"#,
            in: call
        )
        #expect(profileArg == "catalogShowId")
        #expect(idArg == "podcastId")
        #expect(
            profileArg != idArg,
            "canary 3 must be able to see a profile keyed on a different identity than the phase is told about"
        )
    }

    // MARK: - Helpers

    private struct SourceLine {
        let number: Int
        let text: String
    }

    /// Every line of `stripped` mentioning the storage identifier, trimmed.
    /// Word-boundary matched so a hypothetical `podcastProfilesByShowIdCache`
    /// is not silently credited as the sanctioned declaration.
    private static func linesMentioningStorage(in stripped: String) -> [SourceLine] {
        stripped
            .split(separator: "\n", omittingEmptySubsequences: false)
            .enumerated()
            .compactMap { index, raw in
                let line = String(raw)
                guard regexMatches(#"\b"# + storage + #"\b"#, in: line) else { return nil }
                return SourceLine(
                    number: index + 1,
                    text: line.trimmingCharacters(in: .whitespaces)
                )
            }
    }

    /// The argument text of every `cachedPodcastProfile(forShowId: …)` CALL.
    /// The declaration spells it `forShowId showId:` (no colon straight after
    /// the label) so it is not matched, which is deliberate: the declaration
    /// is the thing being protected, not a use of it.
    private static func readCallArguments(in stripped: String) -> [SourceLine] {
        let pattern = #"cachedPodcastProfile\(forShowId:\s*([^)]*)\)"#
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return [] }
        let ns = stripped as NSString
        let matches = regex.matches(
            in: stripped,
            range: NSRange(location: 0, length: ns.length)
        )
        return matches.compactMap { match in
            guard match.numberOfRanges > 1 else { return nil }
            let arg = ns.substring(with: match.range(at: 1))
                .trimmingCharacters(in: .whitespacesAndNewlines)
            let line = ns.substring(to: match.range.location)
                .components(separatedBy: "\n").count
            return SourceLine(number: line, text: arg)
        }
    }

    /// The balanced-paren argument text that follows `opener` in `source`.
    /// A hand-rolled walker rather than a regex because the argument list
    /// contains nested calls; `[^)]*` would stop at the first inner `)`.
    private static func balancedCall(after opener: String, in source: String) -> String? {
        guard let start = source.range(of: opener)?.upperBound else { return nil }
        var depth = 1
        var index = start
        while index < source.endIndex {
            let character = source[index]
            if character == "(" {
                depth += 1
            } else if character == ")" {
                depth -= 1
                if depth == 0 { return String(source[start..<index]) }
            }
            index = source.index(after: index)
        }
        return nil
    }

    private static func firstCapture(of pattern: String, in text: String) -> String? {
        guard let regex = try? NSRegularExpression(
            pattern: pattern,
            options: [.dotMatchesLineSeparators]
        ) else { return nil }
        let ns = text as NSString
        guard let match = regex.firstMatch(
            in: text,
            range: NSRange(location: 0, length: ns.length)
        ), match.numberOfRanges > 1 else { return nil }
        return ns.substring(with: match.range(at: 1))
    }

    private static func regexMatches(_ pattern: String, in text: String) -> Bool {
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return false }
        let ns = text as NSString
        return regex.firstMatch(
            in: text,
            range: NSRange(location: 0, length: ns.length)
        ) != nil
    }
}
