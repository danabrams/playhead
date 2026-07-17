// SelfPromoBankTests.swift
// playhead-fl4j: decode/validation tests for the bundled SelfPromoBank —
// malformed JSON is rejected loudly with a typed, field-identifying reason —
// plus a hermetic pin of the shipped `Playhead/Resources/SelfPromoBank.json`
// asset (schema version + the exact curated phrase set and its normalisation)
// so a re-curated bank that drifts fails on the simulator, not silently.

import Foundation
import Testing
@testable import Playhead

@Suite("SelfPromoBank (playhead-fl4j)")
struct SelfPromoBankTests {

    // MARK: - Fixture builders

    /// Minimal VALID bank payload; tests mutate a copy to prove each validation
    /// rule rejects loudly. Schema v2: every phrase carries a REQUIRED
    /// `selfReference` class.
    private static func validPayload() -> [String: Any] {
        [
            "schemaVersion": 2,
            "phrases": [
                ["phrase": "rate review and subscribe", "selfReference": "selfEvident"],
                ["phrase": "follow us", "selfReference": "selfEvident"],
            ],
        ]
    }

    private static func data(_ payload: [String: Any]) throws -> Data {
        try JSONSerialization.data(withJSONObject: payload)
    }

    private static func mutatedPhrases(
        _ mutate: (inout [[String: Any]]) -> Void
    ) throws -> Data {
        var payload = validPayload()
        var phrases = payload["phrases"] as! [[String: Any]]
        mutate(&phrases)
        payload["phrases"] = phrases
        return try data(payload)
    }

    /// Asserts decode throws `.malformed` and the reason mentions `fragment`.
    private static func expectMalformed(
        _ bankData: Data,
        containing fragment: String
    ) {
        do {
            _ = try SelfPromoBank.decode(bankData)
            Issue.record("expected loud malformed rejection mentioning \"\(fragment)\", but decode succeeded")
        } catch let error as SelfPromoBankError {
            guard case .malformed(let reason) = error else {
                Issue.record("expected .malformed, got \(error)")
                return
            }
            #expect(reason.contains(fragment), "reason \"\(reason)\" must mention \"\(fragment)\"")
        } catch {
            Issue.record("expected SelfPromoBankError, got \(error)")
        }
    }

    // MARK: - Valid decode

    @Test("A valid payload decodes, normalises each phrase, and carries its class")
    func validPayloadDecodes() throws {
        let bank = try SelfPromoBank.decode(Self.data(Self.validPayload()))
        #expect(bank.schemaVersion == 2)
        #expect(bank.phrases.count == 2)
        #expect(bank.phrases.map(\.phrase) == ["rate review and subscribe", "follow us"])
        #expect(bank.phrases[0].tokens == ["rate", "review", "and", "subscribe"])
        #expect(bank.phrases[1].tokens == ["follow", "us"])
        // The self-reference class is decoded and carried through.
        #expect(bank.phrases[0].selfReference == .selfEvident)
        #expect(bank.phrases[1].selfReference == .selfEvident)
    }

    @Test("The ambiguous class decodes")
    func ambiguousClassDecodes() throws {
        var payload = Self.validPayload()
        payload["phrases"] = [
            ["phrase": "get tickets", "selfReference": "requiresCorroboration"],
            ["phrase": "follow us", "selfReference": "selfEvident"],
        ]
        let bank = try SelfPromoBank.decode(try Self.data(payload))
        #expect(bank.phrases[0].selfReference == .requiresCorroboration)
        #expect(bank.phrases[1].selfReference == .selfEvident)
    }

    // MARK: - Loud rejection of malformed payloads

    @Test("Syntactically invalid JSON is rejected loudly")
    func invalidJSONRejected() {
        Self.expectMalformed(Data("{not json".utf8), containing: "decode failed")
    }

    @Test("Unsupported schemaVersion is rejected loudly")
    func wrongSchemaVersionRejected() throws {
        var payload = Self.validPayload()
        payload["schemaVersion"] = 3
        Self.expectMalformed(try Self.data(payload), containing: "schemaVersion 3")
    }

    @Test("Missing required top-level keys are rejected loudly")
    func missingKeysRejected() throws {
        var noPhrases = Self.validPayload()
        noPhrases.removeValue(forKey: "phrases")
        Self.expectMalformed(try Self.data(noPhrases), containing: "decode failed")

        var noVersion = Self.validPayload()
        noVersion.removeValue(forKey: "schemaVersion")
        Self.expectMalformed(try Self.data(noVersion), containing: "decode failed")
    }

    @Test("A phrase missing its selfReference class is rejected loudly")
    func missingSelfReferenceRejected() throws {
        // Drop the REQUIRED class field — the non-optional decode fails loud.
        Self.expectMalformed(
            try Self.mutatedPhrases { $0[0] = ["phrase": "follow us"] },
            containing: "decode failed"
        )
    }

    @Test("A phrase with an unknown selfReference class is rejected loudly")
    func unknownSelfReferenceRejected() throws {
        Self.expectMalformed(
            try Self.mutatedPhrases {
                $0[0] = ["phrase": "follow us", "selfReference": "someday"]
            },
            containing: "unknown selfReference \"someday\""
        )
    }

    @Test("An empty phrase set is rejected loudly")
    func emptyPhraseSetRejected() throws {
        Self.expectMalformed(
            try Self.mutatedPhrases { $0 = [] },
            containing: "no phrases"
        )
    }

    @Test("An empty phrase string is rejected loudly")
    func emptyPhraseRejected() throws {
        Self.expectMalformed(
            try Self.mutatedPhrases { $0[0] = ["phrase": "", "selfReference": "selfEvident"] },
            containing: "empty phrase"
        )
    }

    @Test("A phrase that normalises to fewer than 2 tokens is rejected loudly")
    func shortPhraseRejected() throws {
        // Single word.
        Self.expectMalformed(
            try Self.mutatedPhrases { $0[0] = ["phrase": "subscribe", "selfReference": "selfEvident"] },
            containing: "normalized token count 1"
        )
        // All-punctuation folds to zero tokens.
        Self.expectMalformed(
            try Self.mutatedPhrases { $0[0] = ["phrase": "!!! ---", "selfReference": "selfEvident"] },
            containing: "normalized token count 0"
        )
    }

    @Test("Duplicate phrases (by normalised tokens) are rejected loudly")
    func duplicatePhraseRejected() throws {
        // "Rate, Review!" folds to the same tokens as the first phrase's
        // "rate review" prefix — but here we collide with the exact same tokens.
        Self.expectMalformed(
            try Self.mutatedPhrases {
                // ≡ tokens of $0[0]
                $0[1] = ["phrase": "Rate Review AND subscribe", "selfReference": "selfEvident"]
            },
            containing: "duplicate phrase"
        )
    }

    @Test("A bundle without the resource reports missingResource")
    func missingResourceReported() {
        do {
            _ = try SelfPromoBank.load(bundle: Bundle())
            Issue.record("expected missingResource, but load succeeded")
        } catch {
            #expect((error as? SelfPromoBankError) == .missingResource, "got \(error)")
        }
    }

    // MARK: - Hermetic pin of the bundled asset

    @Test("Bundled SelfPromoBank.json decodes and matches the committed curation")
    func bundledBankPins() throws {
        // The shipped bank was curated from the fl4j spike's
        // self_promo_action_phrases family (see the file header). These pins
        // document exactly what shipped and force any re-curation through review.
        let bank = try SelfPromoBank.load()
        #expect(bank.schemaVersion == 2)

        // Exact ordered phrase set. A reorder, add, drop, or re-word fails here,
        // not silently in a measurement run. Curation invariants below back it.
        #expect(bank.phrases.map(\.phrase) == [
            "rate review and subscribe",
            "rate and review",
            "rate review",
            "please rate",
            "subscribe to the show",
            "subscribe to our channel",
            "follow us",
            "find us on",
            "reach us online",
            "be a guest",
            "send us your questions",
            "you can find the podcast",
            "wherever you get your podcasts",
            "wherever fine podcasts",
            "get tickets",
            "live show",
            "live version",
            "on tour",
            "new ways to watch",
        ])

        // Every phrase is multi-word (the precision floor) and normalises to the
        // token sequence the suppressor matches.
        for phrase in bank.phrases {
            #expect(phrase.tokens.count >= 2, "\(phrase.phrase): must be a multi-word action phrase")
            #expect(
                phrase.tokens == LexicalAnchorNormalizer.normalizePhrase(phrase.phrase),
                "\(phrase.phrase): tokens must match the shared normaliser"
            )
        }
        #expect(bank.phrases.first { $0.phrase == "rate review and subscribe" }?.tokens
            == ["rate", "review", "and", "subscribe"])
        #expect(bank.phrases.first { $0.phrase == "wherever you get your podcasts" }?.tokens
            == ["wherever", "you", "get", "your", "podcasts"])

        // Curation guard: the EXCLUDED families must NOT be present. Bare sponsor
        // phrases (fire on real 3rd-party ads — spike precision 0.14) and bare
        // show-name self-reference are explicitly out per the spike verdict.
        let allTokenKeys = Set(bank.phrases.map { $0.tokens.joined(separator: " ") })
        for banned in [
            "brought to you by", "sponsored by", "supported by", "presented by",
            "in partnership with", "this episode is sponsored",
        ] {
            #expect(
                !allTokenKeys.contains(LexicalAnchorNormalizer.normalizePhrase(banned).joined(separator: " ")),
                "ambiguous sponsor phrase \"\(banned)\" must NOT ship in the self-promo bank"
            )
        }
        // Show-specific phrases the spike flagged as non-generalising must be absent.
        for showSpecific in ["subscribe to conan", "talk to conan", "visit teamcoco"] {
            #expect(
                !allTokenKeys.contains(LexicalAnchorNormalizer.normalizePhrase(showSpecific).joined(separator: " ")),
                "show-specific phrase \"\(showSpecific)\" must NOT ship"
            )
        }

        // Attention→verification class pin (schema v2). The AMBIGUOUS set — the
        // event/watch plugs that collide with THIRD-PARTY ads and so MUST be
        // corroborated by a self-reference marker before demoting — is EXACTLY
        // these five. A curator moving a phrase between classes (e.g. mis-tagging
        // "get tickets" as self-evident, which would let it demote a real
        // concert ad bare) fails here.
        let ambiguousPhrases = Set(
            bank.phrases.filter { $0.selfReference == .requiresCorroboration }.map(\.phrase)
        )
        #expect(ambiguousPhrases == [
            "get tickets", "live show", "live version", "on tour", "new ways to watch",
        ], "the AMBIGUOUS (requires-corroboration) class must be exactly the event/watch plugs")
        // Everything else is STRONG (self-corroborating self-promo CTA).
        for phrase in bank.phrases {
            let expected: SelfReferenceClass = ambiguousPhrases.contains(phrase.phrase)
                ? .requiresCorroboration
                : .selfEvident
            #expect(
                phrase.selfReference == expected,
                "\(phrase.phrase): expected class \(expected), got \(phrase.selfReference)"
            )
        }
        // Spot-pin two load-bearing classifications.
        #expect(bank.phrases.first { $0.phrase == "follow us" }?.selfReference == .selfEvident)
        #expect(bank.phrases.first { $0.phrase == "on tour" }?.selfReference == .requiresCorroboration)
    }
}
