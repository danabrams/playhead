// SelfPromoVerifier.swift
// playhead-fl4j: the ATTENTION → VERIFICATION seam for self-promo suppression.
//
// Design principle (Dan): a lexical hit is a CLUE about where to look, NOT a
// final determination. A curated self-promo phrase matching a span's transcript
// is only ATTENTION ("look here") — it produces a CANDIDATE, never a verdict. A
// separate VERIFICATION step must independently corroborate that the segment is
// the show promoting ITSELF (first-person "we/us/our/my", or the show's own
// identity tokens) before the eligibility gate is demoted. A bare lexical match
// with no corroboration is a clue that FAILED verification and is left alone.
//
// This file owns the verification abstraction; `PromoSuppressor` owns the
// attention loop (phrase matching) and asks the verifiers to corroborate each
// candidate.
//
// EXTENSIBILITY SEAM (explicit, by request): verification is a list of
// `SelfPromoVerifier`s composed with OR — a candidate is corroborated iff ANY
// verifier confirms it. The only verifier that ships today is
// `SelfReferenceVerifier` (first-person / show-identity marker in the local
// window). A future SEMANTIC verifier (topic-continuity, bead playhead-rqu6) or
// a position/fusion corroborator conforms to `SelfPromoVerifier` and is appended
// to the verifier list — no change to the attention loop, the bank, or the wire-
// in. That is the whole point of the seam: new corroboration is additive.

import Foundation

// MARK: - Show identity

/// The show's own identity tokens — the "is this the show promoting ITSELF?"
/// corroboration source that is NOT universal (unlike first-person pronouns).
/// Derived at the call site from the podcast profile / feed metadata (title
/// words, network, known handle) and threaded into verification so an AMBIGUOUS
/// phrase can be corroborated by the show naming itself ("get tickets to see us
/// at <ShowName> live") even without a first-person pronoun.
///
/// Generic filler tokens are dropped at construction (see `genericIdentityTokens`
/// / `minIdentityTokenLength`) so a stopword in a show title ("The", "Show",
/// "Podcast") cannot over-corroborate an ambiguous phrase. Empty when no profile
/// is available — then only the universal first-person markers can corroborate.
struct SelfPromoShowIdentity: Sendable, Equatable {
    /// Distinctive normalised identity tokens (generics filtered).
    let identityTokens: Set<String>

    /// No show identity available (e.g. no podcast profile at the call site).
    static let none = SelfPromoShowIdentity(identityTokens: [])

    init(identityTokens: Set<String>) {
        self.identityTokens = identityTokens
    }

    /// Build from raw show-identity strings, normalising through the SAME
    /// `LexicalAnchorNormalizer` the bank and word stream use (so casing /
    /// punctuation / apostrophes fold identically) and dropping generic tokens
    /// that would over-corroborate.
    ///
    /// - Parameters:
    ///   - title: the human-readable show title (e.g. "Conan O'Brien Needs a Friend").
    ///   - networkId: the derived network grouping key, if any.
    ///   - handles: any known social handles / URLs for the show.
    init(title: String?, networkId: String? = nil, handles: [String] = []) {
        var tokens = Set<String>()
        let sources = ([title, networkId].compactMap { $0 }) + handles
        for source in sources {
            for token in LexicalAnchorNormalizer.normalizePhrase(source)
            where token.count >= Self.minIdentityTokenLength
                && !Self.genericIdentityTokens.contains(token) {
                tokens.insert(token)
            }
        }
        self.identityTokens = tokens
    }

    /// Minimum normalised-token length to count as a distinctive identity token.
    /// Drops short function words ("of", "to", "us", "my") that carry no show
    /// identity — first-person markers are matched separately and universally.
    static let minIdentityTokenLength = 3

    /// Generic tokens that survive the length floor but carry no show identity —
    /// common English function/filler words plus podcast-generic nouns AND the
    /// ambiguous-phrase words themselves. Filtering the ambiguous words here is
    /// deliberate: a show whose title happens to contain "Live" or "Tour" must
    /// NOT self-corroborate the "live show" / "on tour" plugs via its own name.
    static let genericIdentityTokens: Set<String> = [
        "the", "and", "for", "with", "your", "you", "our", "this", "that",
        "show", "shows", "podcast", "podcasts", "radio", "network", "networks",
        "media", "studios", "studio", "presents", "presented", "productions",
        "live", "tour", "get", "tickets", "new", "ways", "watch", "version",
    ]
}

// MARK: - Verification inputs

/// The read-only context a verifier corroborates a candidate against: the local
/// transcript token slice and the show's identity. A future semantic/position
/// verifier that needs MORE context (chapter geometry, episode position, …) adds
/// a field here — the field is inert for verifiers that don't read it.
struct SelfPromoContext: Sendable, Equatable {
    /// The span's normalised token slice (the local transcript region already
    /// scoped to the candidate ad break by `PromoSuppressor`).
    let spanTokens: [String]
    /// The show's own identity tokens.
    let showIdentity: SelfPromoShowIdentity
}

/// One ATTENTION hit: a bank phrase whose normalised tokens appear contiguously
/// in the span's token slice, plus WHERE it matched. A candidate is a clue, not
/// a verdict — a verifier must corroborate it before any demotion.
struct SelfPromoCandidate: Sendable, Equatable {
    /// The bank phrase that produced this attention hit (carries its class).
    let phrase: SelfPromoPhrase
    /// Half-open range of the contiguous match within `SelfPromoContext.spanTokens`.
    let matchRange: Range<Int>
}

// MARK: - Verifier protocol (the seam)

/// A verification step that corroborates a self-promo candidate before any
/// eligibility demotion. Conform a new type and append it to the verifier list
/// to add a corroborator (semantic topic-continuity, position/fusion, …) WITHOUT
/// re-architecting the attention→verify→decide flow. Composition is OR: a
/// candidate is demoted iff ANY verifier corroborates it.
protocol SelfPromoVerifier: Sendable {
    /// Whether this verifier independently corroborates that `candidate` is the
    /// show promoting ITSELF (not a third-party ad) within `context`.
    func corroborates(_ candidate: SelfPromoCandidate, in context: SelfPromoContext) -> Bool
}

// MARK: - SelfReferenceVerifier (the one verifier that ships today)

/// Corroborates a candidate by SELF-REFERENCE: a true self-promo is the show
/// promoting itself, marked by a first-person pronoun (we/us/our/my/…) or the
/// show's own identity tokens in the candidate's local window.
///
///   * A STRONG (`.selfEvident`) phrase carries the self-reference IN the phrase
///     itself ("follow us", "rate review and subscribe") — it is
///     self-corroborating, so this verifier confirms it unconditionally.
///   * An AMBIGUOUS (`.requiresCorroboration`) phrase ("get tickets", "on tour")
///     is NOT inherently self-referential and collides with third-party event
///     ads — it is corroborated ONLY when a first-person marker or a show-
///     identity token appears in the local window AROUND the match. A real
///     third-party ad ("get tickets to <Artist> on tour at Ticketmaster") has no
///     such marker → not corroborated → not demoted.
struct SelfReferenceVerifier: SelfPromoVerifier {

    /// Universal first-person self-reference markers. Deliberately excludes the
    /// past-tense homograph "were" (the fold of "we're") because "were" is far
    /// more often third-person past-tense ("they were on tour") than a
    /// first-person contraction, which would corrupt precision.
    static let firstPersonMarkers: Set<String> = [
        "we", "us", "our", "ours", "my", "ourselves",
    ]

    /// Tokens on EACH side of the phrase match that form the "local window" for
    /// an ambiguous phrase's corroboration. ~15 tokens ≈ a sentence or two of
    /// speech — a self-promo CTA and its self-reference ("get tickets to OUR
    /// live show") are adjacent, so a modest window captures the corroboration
    /// while excluding a first-person pronoun 80 seconds away in a long merged
    /// span. Precision-first: a window too small only costs recall (the flag is
    /// OFF and the demotion is conservative), never precision.
    static let windowRadius = 15

    func corroborates(_ candidate: SelfPromoCandidate, in context: SelfPromoContext) -> Bool {
        switch candidate.phrase.selfReference {
        case .selfEvident:
            // STRONG: the phrase IS the self-reference — self-corroborating.
            return true
        case .requiresCorroboration:
            // AMBIGUOUS: require a self-reference marker in the local window,
            // OUTSIDE the matched phrase's own tokens (the corroboration must
            // come from surrounding context, not the phrase itself).
            let tokens = context.spanTokens
            let lower = max(0, candidate.matchRange.lowerBound - Self.windowRadius)
            let upper = min(tokens.count, candidate.matchRange.upperBound + Self.windowRadius)
            var i = lower
            while i < upper {
                if !candidate.matchRange.contains(i) {
                    let token = tokens[i]
                    if Self.firstPersonMarkers.contains(token)
                        || context.showIdentity.identityTokens.contains(token) {
                        return true
                    }
                }
                i += 1
            }
            return false
        }
    }
}
