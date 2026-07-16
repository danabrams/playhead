// PromoSuppressorTests.swift
// playhead-fl4j: unit tests for the pure `PromoSuppressor.shouldSuppress`
// evaluator — self-promo ACTION phrases fire; bare sponsor phrases and bare
// show-name self-reference do NOT (the precision guards the spike proved
// necessary); normalisation (curly apostrophes / casing / punctuation) is
// parity-identical to the shared word-stream normaliser; and a phrase OUTSIDE
// the span's time geometry is correctly excluded.

import Foundation
import Testing
@testable import Playhead

@Suite("PromoSuppressor (playhead-fl4j)")
struct PromoSuppressorTests {

    // MARK: - Fixtures

    /// Build the episode word stream the suppressor consumes, using the SAME
    /// `LexicalAnchorRefiner.buildWordStream` path production uses (so casing /
    /// punctuation / apostrophe normalisation is exercised end-to-end, not
    /// hand-normalised).
    private func words(_ text: String, start: Double = 0, end: Double = 30) -> [LexicalWord] {
        let chunk = TranscriptChunk(
            id: "c0",
            analysisAssetId: "asset-fl4j",
            segmentFingerprint: "fp0",
            chunkIndex: 0,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: text.lowercased(),
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
        return LexicalAnchorRefiner.buildWordStream(chunks: [chunk])
    }

    /// Build a bank from `(phrase, selfReference-class)` pairs through the real
    /// decode/validate path.
    private func bank(_ phrases: [(String, String)]) throws -> SelfPromoBank {
        let payload: [String: Any] = [
            "schemaVersion": 2,
            "phrases": phrases.map { ["phrase": $0.0, "selfReference": $0.1] },
        ]
        return try SelfPromoBank.decode(JSONSerialization.data(withJSONObject: payload))
    }

    /// Convenience: build a bank of STRONG (self-evident) phrases.
    private func bank(_ phrases: [String]) throws -> SelfPromoBank {
        try bank(phrases.map { ($0, "selfEvident") })
    }

    private func span(start: Double = 0, end: Double = 30) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-fl4j", firstAtomOrdinal: 0, lastAtomOrdinal: 9),
            assetId: "asset-fl4j",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 9,
            startTime: start,
            endTime: end,
            anchorProvenance: []
        )
    }

    /// The curated shipping phrase set, so the unit tests exercise the real bank.
    private func shippedBank() throws -> SelfPromoBank {
        try SelfPromoBank.load()
    }

    // MARK: - Positive: self-promo action phrases fire

    @Test("A self-promo action phrase in the span fires")
    func actionPhraseFires() throws {
        let w = words("Thanks so much for listening. Please rate review and subscribe wherever you get your podcasts.")
        #expect(PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: try shippedBank()))
    }

    @Test("An ambiguous live-show / tickets plug fires WHEN self-reference corroborates")
    func liveShowPlugFires() throws {
        // "on tour" / "get tickets" are AMBIGUOUS (they collide with third-party
        // event ads). Here the first-person "we" in the local window corroborates
        // that it is the SHOW promoting itself, so it fires.
        let w = words("We are going on tour this fall, get tickets at the box office.")
        #expect(PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: try shippedBank()))
    }

    @Test("A be-a-guest / contact-the-show plug fires")
    func beAGuestFires() throws {
        let w = words("Want to be a guest on the show? Follow us for details.")
        #expect(PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: try shippedBank()))
    }

    // MARK: - Precision guards: must NOT fire

    @Test("A bare sponsor phrase does NOT fire (fires on real 3rd-party ads)")
    func bareSponsorDoesNotFire() throws {
        // The exact ad-copy the spike showed bare-sponsor matching false-fires on.
        let w = words("This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show.")
        #expect(!PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: try shippedBank()))
    }

    @Test("Bare show-name self-reference does NOT fire (contaminated by underwriting reads)")
    func bareShowNameDoesNotFire() throws {
        // "<Show> is supported by <external brand>" — show name co-occurs with a
        // real sponsor. No self-promo ACTION verb ⇒ must not suppress.
        let w = words("WNYC Studios is supported by Proof on Broadway. Welcome back to On The Media with Brooke Gladstone.")
        #expect(!PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: try shippedBank()))
    }

    // MARK: - Attention → verification: the ambiguous class

    /// THE crux of the rework: an AMBIGUOUS phrase is a CLUE, not a verdict. The
    /// SAME phrase is NOT suppressed in a real third-party event ad (attention
    /// without verification) but IS suppressed when a first-person self-reference
    /// corroborates it (attention verified).
    @Test("Ambiguous phrase: a third-party event ad does NOT fire; self-reference DOES")
    func ambiguousPhraseRequiresSelfReference() throws {
        let b = try bank([
            ("get tickets", "requiresCorroboration"),
            ("on tour", "requiresCorroboration"),
        ])
        // Real THIRD-PARTY event ad — no first-person, no show identity. A bare
        // lexical hit that MUST NOT demote (this is the precision risk reviewers
        // flagged and the whole reason for verification).
        let thirdParty = words("Get tickets to see Taylor Swift on tour at Ticketmaster dot com.")
        #expect(
            !PromoSuppressor.shouldSuppress(span: span(), transcriptWords: thirdParty, bank: b),
            "a real third-party event ad (ambiguous phrase, no self-reference) must NOT be suppressed"
        )
        // The SAME ambiguous phrase, now with a first-person self-reference in
        // the local window ("our") — the show promoting ITSELF — DOES demote.
        let selfPromo = words("Get tickets to our live show this weekend.")
        #expect(
            PromoSuppressor.shouldSuppress(span: span(), transcriptWords: selfPromo, bank: b),
            "the same ambiguous phrase WITH a first-person self-reference must be suppressed"
        )
    }

    /// STRONG phrases carry their own self-reference — they fire with NO
    /// surrounding first-person marker and NO show identity (self-corroborating).
    @Test("Strong (self-evident) phrase fires with no surrounding self-reference")
    func strongPhraseSelfCorroborates() throws {
        let b = try bank([("rate review and subscribe", "selfEvident")])
        let w = words("Rate review and subscribe for more episodes.")
        #expect(
            PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: b),
            "a STRONG phrase is self-corroborating — it needs no external self-reference"
        )
    }

    /// An ambiguous phrase can also be corroborated by the show naming ITSELF
    /// (a show-identity token), not only by a first-person pronoun — proving the
    /// show identity is threaded into verification.
    @Test("Ambiguous phrase corroborated by a show-identity token fires")
    func ambiguousCorroboratedByShowIdentity() throws {
        let b = try bank([("on tour", "requiresCorroboration")])
        let identity = SelfPromoShowIdentity(title: "Conan O'Brien Needs a Friend")
        let w = words("Conan is going on tour this fall.")
        #expect(
            PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: b, showIdentity: identity),
            "the show naming itself ('Conan') corroborates an ambiguous plug"
        )
        // Without the show identity (and with no first-person marker) the same
        // words are a clue that FAILS verification.
        #expect(
            !PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: b),
            "without show identity and no first-person marker, the ambiguous plug is NOT suppressed"
        )
    }

    /// A self-reference OUTSIDE the local window does not corroborate — the
    /// window is genuinely local (bounds the precision claim).
    @Test("A self-reference beyond the local window does NOT corroborate an ambiguous phrase")
    func selfReferenceOutsideWindowDoesNotCorroborate() throws {
        let b = try bank([("on tour", "requiresCorroboration")])
        // "we" at token 0, then 20 filler tokens, then "on tour" — pushing the
        // pronoun beyond the ±window radius of the match.
        let filler = Array(repeating: "and", count: 20).joined(separator: " ")
        let w = words("we \(filler) going on tour")
        #expect(
            !PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: b),
            "a first-person marker beyond the local window must not corroborate"
        )
    }

    // MARK: - Normalisation parity

    @Test("Curly apostrophes, casing, and punctuation normalise to a match")
    func normalisationParity() throws {
        let b = try bank(["we're on tour"]) // tokens: were, on, tour
        // U+2019 curly apostrophe + upper-casing + trailing punctuation.
        let curly = words("We\u{2019}re ON TOUR!")
        #expect(PromoSuppressor.shouldSuppress(span: span(), transcriptWords: curly, bank: b),
                "curly-apostrophe transcript must fold to the same tokens as the bank phrase")
        // Straight ASCII apostrophe folds identically.
        let straight = words("we're on tour")
        #expect(PromoSuppressor.shouldSuppress(span: span(), transcriptWords: straight, bank: b))
    }

    // MARK: - No-op cases

    @Test("Empty word stream is a no-op")
    func emptyWordsNoOp() throws {
        #expect(!PromoSuppressor.shouldSuppress(span: span(), transcriptWords: [], bank: try shippedBank()))
    }

    @Test("A span with no self-promo phrase is a no-op")
    func noMatchNoOp() throws {
        let w = words("Today we discuss the history of aviation and the future of flight.")
        #expect(!PromoSuppressor.shouldSuppress(span: span(), transcriptWords: w, bank: try shippedBank()))
    }

    @Test("A phrase OUTSIDE the span's time geometry is excluded")
    func outOfSpanGeometryExcluded() throws {
        // The self-promo phrase lives at t≈[0,30]; the span is [40,50], so the
        // slice is empty and the phrase must NOT suppress this span.
        let w = words("Please rate review and subscribe.", start: 0, end: 30)
        let farSpan = span(start: 40, end: 50)
        #expect(!PromoSuppressor.shouldSuppress(span: farSpan, transcriptWords: w, bank: try shippedBank()))
        // Sanity: the SAME words DO fire for a span that overlaps them.
        #expect(PromoSuppressor.shouldSuppress(span: span(start: 0, end: 30), transcriptWords: w, bank: try shippedBank()))
    }

    @Test("A zero-duration span is a conservative no-op")
    func zeroDurationNoOp() throws {
        let w = words("Please rate review and subscribe.")
        #expect(!PromoSuppressor.shouldSuppress(span: span(start: 10, end: 10), transcriptWords: w, bank: try shippedBank()))
    }
}
