// LexicalAnchorBankTests.swift
// playhead-xsdz.37: decode/validation tests for the bundled LexicalAnchorBank
// — malformed JSON is rejected loudly with a typed, field-identifying reason —
// plus a hermetic pin of the shipped `Playhead/Resources/LexicalAnchorBank.json`
// asset (show count, slug order, per-anchor side/phrase/offset, generic set)
// so a regenerated bank that drifts fails on the simulator, not in a Catalyst
// measurement run.

import Foundation
import Testing
@testable import Playhead

@Suite("LexicalAnchorBank (playhead-xsdz.37)")
struct LexicalAnchorBankTests {

    // MARK: - Fixture builders

    /// Minimal VALID bank payload; tests mutate a copy to prove each validation
    /// rule rejects loudly.
    private static func validPayload() -> [String: Any] {
        [
            "schemaVersion": 1,
            "shows": [
                [
                    "showKeys": ["test-show", "https://feeds.example.com/test-show"],
                    "showName": "Test Show",
                    "anchors": [
                        [
                            "phrase": "we will be right back",
                            "side": "pre",
                            "matchPolicy": "exact",
                            "edgeOffsetSeconds": 2.0,
                            "confidence": 0.9,
                            "support": 3,
                        ],
                    ],
                ],
            ],
            "genericAnchors": [
                [
                    "phrase": "and now back to the show",
                    "side": "post",
                    "matchPolicy": "exact",
                    "edgeOffsetSeconds": -0.6,
                    "confidence": 0.9,
                    "support": 3,
                ],
            ],
        ]
    }

    private static func data(_ payload: [String: Any]) throws -> Data {
        try JSONSerialization.data(withJSONObject: payload)
    }

    private static func mutatedShow(
        _ mutate: (inout [String: Any]) -> Void
    ) throws -> Data {
        var payload = validPayload()
        var shows = payload["shows"] as! [[String: Any]]
        mutate(&shows[0])
        payload["shows"] = shows
        return try data(payload)
    }

    private static func mutatedAnchor(
        _ mutate: (inout [String: Any]) -> Void
    ) throws -> Data {
        try mutatedShow {
            var anchors = $0["anchors"] as! [[String: Any]]
            mutate(&anchors[0])
            $0["anchors"] = anchors
        }
    }

    private static func mutatedGeneric(
        _ mutate: (inout [String: Any]) -> Void
    ) throws -> Data {
        var payload = validPayload()
        var generics = payload["genericAnchors"] as! [[String: Any]]
        mutate(&generics[0])
        payload["genericAnchors"] = generics
        return try data(payload)
    }

    /// Asserts decode throws `.malformed` and the reason mentions `fragment`.
    private static func expectMalformed(
        _ bankData: Data,
        containing fragment: String
    ) {
        do {
            _ = try LexicalAnchorBank.decode(bankData)
            Issue.record("expected loud malformed rejection mentioning \"\(fragment)\", but decode succeeded")
        } catch let error as LexicalAnchorBankError {
            guard case .malformed(let reason) = error else {
                Issue.record("expected .malformed, got \(error)")
                return
            }
            #expect(reason.contains(fragment), "reason \"\(reason)\" must mention \"\(fragment)\"")
        } catch {
            Issue.record("expected LexicalAnchorBankError, got \(error)")
        }
    }

    // MARK: - Valid decode

    @Test("A valid payload decodes with entry lookup on every alias")
    func validPayloadDecodes() throws {
        let bank = try LexicalAnchorBank.decode(Self.data(Self.validPayload()))
        #expect(bank.schemaVersion == 1)
        #expect(bank.shows.count == 1)
        #expect(bank.genericAnchors.count == 1)

        // Join-key contract: exact match on EITHER alias; unknown/nil/empty
        // resolve to nothing.
        #expect(bank.entry(forShowKey: "test-show") != nil)
        #expect(bank.entry(forShowKey: "https://feeds.example.com/test-show") != nil)
        #expect(bank.entry(forShowKey: "other-show") == nil)
        #expect(bank.entry(forShowKey: nil) == nil)
        #expect(bank.entry(forShowKey: "") == nil)

        let entry = try #require(bank.entry(forShowKey: "test-show"))
        #expect(entry.showName == "Test Show")
        let anchor = try #require(entry.anchors.first)
        #expect(anchor.phrase == "we will be right back")
        // Normalisation must match the Python prototype: lowercase + strip
        // non-[a-z0-9].
        #expect(anchor.tokens == ["we", "will", "be", "right", "back"])
        #expect(anchor.side == .pre)
        #expect(anchor.matchPolicy == .exact)
        #expect(anchor.edgeOffsetSeconds == 2.0)
        #expect(anchor.confidence == 0.9)
        #expect(anchor.support == 3)

        // Effective anchor set = show anchors + generic anchors; unknown show
        // resolves nil (never consulted).
        let effective = try #require(bank.effectiveAnchors(forShowKey: "test-show"))
        #expect(effective.count == 2, "1 show anchor + 1 generic anchor")
        #expect(effective.contains { $0.phrase == "and now back to the show" && $0.side == .post })
        #expect(bank.effectiveAnchors(forShowKey: "other-show") == nil)
    }

    @Test("Apostrophe folding matches the prototype normaliser")
    func apostropheFolding() {
        #expect(LexicalAnchorNormalizer.normalizeWord("We'll") == "well")
        #expect(LexicalAnchorNormalizer.normalizeWord("don't") == "dont")
        #expect(LexicalAnchorNormalizer.normalizePhrase("we'll be right back")
            == ["well", "be", "right", "back"])
        // Non-[a-z0-9] (punctuation, non-ASCII) dropped; digits kept.
        #expect(LexicalAnchorNormalizer.normalizePhrase("10% OFF, now!") == ["10", "off", "now"])
    }

    // MARK: - Loud rejection of malformed payloads

    @Test("Syntactically invalid JSON is rejected loudly")
    func invalidJSONRejected() {
        Self.expectMalformed(Data("{not json".utf8), containing: "decode failed")
    }

    @Test("Unsupported schemaVersion is rejected loudly")
    func wrongSchemaVersionRejected() throws {
        var payload = Self.validPayload()
        payload["schemaVersion"] = 2
        Self.expectMalformed(try Self.data(payload), containing: "schemaVersion 2")
    }

    @Test("Missing required top-level keys are rejected loudly")
    func missingKeysRejected() throws {
        var noShows = Self.validPayload()
        noShows.removeValue(forKey: "shows")
        Self.expectMalformed(try Self.data(noShows), containing: "decode failed")

        var noGeneric = Self.validPayload()
        noGeneric.removeValue(forKey: "genericAnchors")
        Self.expectMalformed(try Self.data(noGeneric), containing: "decode failed")
    }

    @Test("Empty showKeys are rejected loudly")
    func emptyShowKeysRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow { $0["showKeys"] = [String]() },
            containing: "empty showKeys"
        )
        Self.expectMalformed(
            try Self.mutatedShow { $0["showKeys"] = ["test-show", ""] },
            containing: "empty showKeys"
        )
    }

    @Test("Duplicate showKeys across entries are rejected loudly")
    func duplicateShowKeysRejected() throws {
        var payload = Self.validPayload()
        var shows = payload["shows"] as! [[String: Any]]
        var second = shows[0]
        second["showName"] = "Other Show"
        second["showKeys"] = ["test-show"] // collides with the first entry
        shows.append(second)
        payload["shows"] = shows
        Self.expectMalformed(try Self.data(payload), containing: "duplicate showKey")
    }

    @Test("Empty showName is rejected loudly")
    func emptyShowNameRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow { $0["showName"] = "" },
            containing: "empty showName"
        )
    }

    @Test("A show with no anchors is rejected loudly")
    func anchorlessShowRejected() throws {
        Self.expectMalformed(
            try Self.mutatedShow { $0["anchors"] = [[String: Any]]() },
            containing: "no anchors"
        )
    }

    @Test("A phrase that normalises to fewer than 2 tokens is rejected loudly")
    func shortPhraseRejected() throws {
        Self.expectMalformed(
            try Self.mutatedAnchor { $0["phrase"] = "back" },
            containing: "normalized token count 1"
        )
    }

    @Test("An empty phrase is rejected loudly")
    func emptyPhraseRejected() throws {
        Self.expectMalformed(
            try Self.mutatedAnchor { $0["phrase"] = "" },
            containing: "empty anchor phrase"
        )
    }

    @Test("An unknown side is rejected loudly")
    func unknownSideRejected() throws {
        Self.expectMalformed(
            try Self.mutatedAnchor { $0["side"] = "middle" },
            containing: "unknown side"
        )
    }

    @Test("An unsupported matchPolicy is rejected loudly (exact only this cut)")
    func unsupportedMatchPolicyRejected() throws {
        Self.expectMalformed(
            try Self.mutatedAnchor { $0["matchPolicy"] = "fuzzy" },
            containing: "unsupported matchPolicy"
        )
    }

    @Test("Out-of-range confidence is rejected loudly")
    func outOfRangeConfidenceRejected() throws {
        Self.expectMalformed(
            try Self.mutatedAnchor { $0["confidence"] = 1.2 },
            containing: "confidence 1.2"
        )
        Self.expectMalformed(
            try Self.mutatedAnchor { $0["confidence"] = 0.0 },
            containing: "confidence 0.0"
        )
    }

    @Test("Insufficient support is rejected loudly")
    func insufficientSupportRejected() throws {
        Self.expectMalformed(
            try Self.mutatedAnchor { $0["support"] = 1 },
            containing: "support 1"
        )
    }

    @Test("A malformed generic anchor is rejected loudly with generic context")
    func malformedGenericAnchorRejected() throws {
        Self.expectMalformed(
            try Self.mutatedGeneric { $0["support"] = 1 },
            containing: "generic anchor"
        )
        Self.expectMalformed(
            try Self.mutatedGeneric { $0["side"] = "sideways" },
            containing: "unknown side"
        )
    }

    @Test("A bundle without the resource reports missingResource")
    func missingResourceReported() {
        do {
            _ = try LexicalAnchorBank.load(bundle: Bundle())
            Issue.record("expected missingResource, but load succeeded")
        } catch {
            #expect((error as? LexicalAnchorBankError) == .missingResource, "got \(error)")
        }
    }

    // MARK: - Hermetic pin of the bundled asset

    @Test("Bundled LexicalAnchorBank.json decodes and matches the committed generation")
    func bundledBankPins() throws {
        // The shipped bank was generated by
        // `scripts/xsdz37-emit-lexical-anchor-bank.py` from the GO'd template
        // set (playhead-baselines/xsdz37-lexical-prototype-20260716.md). These
        // pins document exactly what shipped and force any regeneration through
        // review.
        let bank = try LexicalAnchorBank.load()
        #expect(bank.schemaVersion == 1)

        // Family (a): two GO'd shows, deterministic order (sorted by showName).
        #expect(bank.shows.count == 2)
        #expect(bank.shows.map(\.showName) == ["On The Media", "Radiolab"])
        #expect(bank.shows.map { $0.showKeys[0] } == ["on-the-media", "radiolab"])

        for show in bank.shows {
            // Every entry carries the corpus slug + a production feed-URL alias.
            #expect(show.showKeys.count == 2, "\(show.showName): slug + feed URL aliases")
            #expect(show.showKeys[1].hasPrefix("https://"), "\(show.showName): non-slug alias must be a feed URL")
            // 3 entities x (4 verbs + 1 inversion) = 15 attribution templates,
            // all pre-side onset templates at the curated -1.0s offset.
            #expect(show.anchors.count == 15, "\(show.showName): 15 attribution templates")
            for anchor in show.anchors {
                #expect(anchor.side == .pre, "\(show.showName): family-a templates snap the start")
                #expect(anchor.matchPolicy == .exact)
                #expect(anchor.edgeOffsetSeconds == -1.0, "\(show.showName): curated family-a onset offset")
                #expect(anchor.tokens.count >= 2)
                #expect(anchor.confidence > 0 && anchor.confidence <= 1)
                #expect(anchor.support >= 2)
            }
        }

        // Full ordered per-show phrase pins: entities (sorted by normalised
        // key) × {brought to you by, sponsored by, supported by, presented by}
        // then the inversion. A reorder or a swapped attribution phrase fails
        // here, not in a Catalyst measurement run.
        let wnycBlock = [
            "WNYC is brought to you by",
            "WNYC is sponsored by",
            "WNYC is supported by",
            "WNYC is presented by",
            "support for WNYC comes from",
            "WNYC Studios is brought to you by",
            "WNYC Studios is sponsored by",
            "WNYC Studios is supported by",
            "WNYC Studios is presented by",
            "support for WNYC Studios comes from",
        ]
        let otm = try #require(bank.entry(forShowKey: "on-the-media"))
        #expect(otm.anchors.map(\.phrase) == [
            "On The Media is brought to you by",
            "On The Media is sponsored by",
            "On The Media is supported by",
            "On The Media is presented by",
            "support for On The Media comes from",
        ] + wnycBlock)
        let radiolab = try #require(bank.entry(forShowKey: "radiolab"))
        #expect(radiolab.anchors.map(\.phrase) == [
            "Radiolab is brought to you by",
            "Radiolab is sponsored by",
            "Radiolab is supported by",
            "Radiolab is presented by",
            "support for Radiolab comes from",
        ] + wnycBlock)

        // Family (b): the generic core pair only — the rejected 2-word / fuzzy
        // resume traps are absent. Ordered pin (pre pair, then resume pair).
        #expect(bank.genericAnchors.count == 4)
        #expect(bank.genericAnchors.map(\.phrase) == [
            "we'll be right back",
            "we will be right back",
            "and now back to the show",
            "and back to the show",
        ])
        #expect(bank.genericAnchors.map(\.side) == [.pre, .pre, .post, .post])
        let pre = bank.genericAnchors.filter { $0.side == .pre }
        let post = bank.genericAnchors.filter { $0.side == .post }
        #expect(Set(pre.map(\.phrase)) == ["we'll be right back", "we will be right back"])
        #expect(Set(post.map(\.phrase)) == ["and now back to the show", "and back to the show"])
        for anchor in pre {
            #expect(anchor.edgeOffsetSeconds == 2.0, "generic pre offset")
        }
        for anchor in post {
            #expect(anchor.edgeOffsetSeconds == -0.6, "generic resume offset")
        }
        // Normalisation pin (contracted vs uncontracted are distinct tokens).
        let contracted = try #require(pre.first { $0.phrase == "we'll be right back" })
        #expect(contracted.tokens == ["well", "be", "right", "back"])
        // The dropped traps must NOT be present anywhere.
        let allPhrases = Set(bank.genericAnchors.map(\.phrase))
        #expect(!allPhrases.contains("we're back"))
        #expect(!allPhrases.contains("welcome back"))
        #expect(!allPhrases.contains("welcome back to the show"))
    }
}
