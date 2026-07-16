// SelfPromoBank.swift
// playhead-fl4j: bundled, curated, SHOW-AGNOSTIC self-promo ACTION phrase bank
// for the eligibility-side self-promo suppression signal.
//
// The bank is CURATED OFFLINE from the fl4j spike's candidate bank
// (`playhead-baselines/fl4j-negative-anchor-bank-candidate.json`,
// `self_promo_action_phrases`) and prototype report
// (`playhead-baselines/fl4j-negative-anchor-prototype-2026-07-16.md`). There is
// no on-device learning and no network fetch; the JSON ships in the app bundle
// and is versioned by `schemaVersion`.
//
// Scope of the curation (per the spike verdict + the bead brief):
//   • KEEP universal self-promo ACTION phrases — a show promoting ITSELF:
//     rate/review/subscribe, follow us, find us on, be a guest, send us your
//     questions, "wherever you get your podcasts", live-show / get-tickets / on
//     tour, "new ways to watch". These carry self-referential CALLS TO ACTION
//     and no third-party brand, so they are the lexically-distinctive subclass
//     the spike GO'd (C2 precision 0.78).
//   • DROP show-specific tokens ("subscribe to conan", "talk to conan",
//     "visit teamcoco") — those never generalise.
//   • EXCLUDE the `ambiguous_sponsor_phrases` family ("brought to you by",
//     "sponsored by", "supported by", …). The spike proved these fire on real
//     third-party ads too (C1 precision 0.14) and MUST NOT be used bare.
//   • EXCLUDE bare show-name self-reference — the "<Show/Network> is supported
//     by <external brand>" underwriting construction makes the show name
//     co-occur with genuine sponsors, so a bare self-reference gate false-fires
//     on real ads (C3/C5). The working signal is the self-promo ACTION verb.
//
// ATTENTION → VERIFICATION (schema v2): every phrase is TAGGED with a
// `selfReference` class that drives whether a bare lexical match is trusted:
//   • `.selfEvident` (STRONG) — the phrase is inherently the show promoting
//     ITSELF ("follow us", "rate review and subscribe", "be a guest", "send us
//     your questions"). It carries the self-reference in the phrase itself, so
//     the self-reference verifier corroborates it unconditionally.
//   • `.requiresCorroboration` (AMBIGUOUS) — an event/watch plug that collides
//     with THIRD-PARTY ads ("get tickets", "on tour", "live show", "live
//     version", "new ways to watch"). It is NOT inherently self-referential; a
//     verifier must find an explicit self-reference marker in the local window
//     before it can demote. A lexical hit alone is a CLUE, never a verdict.
// The tag is a REQUIRED schema field: a curator adding a phrase must classify
// it, and an unknown/missing value fails loud (never a silent default).
//
// Both the bank and the consuming `PromoSuppressor` match EXACT normalised
// token sequences only (via `LexicalAnchorNormalizer`) — no fuzzy matching.
//
// Loading follows the `LexicalAnchorBank` / `StingerBank` fail-loud precedent:
// malformed JSON throws a typed error with a field-identifying reason. The
// service-side consumer (`AdDetectionService.selfPromoSuppressionBankIfEnabled`)
// catches, logs at `.error`, and degrades to "no bank" — suppression silently
// disabled, never a crash — but the loader itself never papers over a bad
// asset.

import Foundation

// MARK: - Errors

enum SelfPromoBankError: Error, Equatable, CustomStringConvertible {
    /// `SelfPromoBank.json` is not present in the bundle.
    case missingResource
    /// The resource exists but failed to decode or validate.
    case malformed(String)

    var description: String {
        switch self {
        case .missingResource:
            return "SelfPromoBank.json missing from bundle"
        case .malformed(let reason):
            return "SelfPromoBank.json malformed: \(reason)"
        }
    }
}

// MARK: - Self-reference class

/// Whether a bank phrase carries its own self-reference (STRONG, self-evident)
/// or needs an external self-reference marker to corroborate before it can
/// demote (AMBIGUOUS). This is the schema tag that drives the
/// attention→verification split (see the file header and `SelfReferenceVerifier`).
///
/// Modelled as a `String`-backed enum like `LexicalAnchorMatchPolicy`: the case
/// set is closed, and a future class is a fail-loud schema addition rather than
/// a silent behaviour change.
enum SelfReferenceClass: String, Sendable, Equatable {
    /// STRONG. The phrase is inherently the show promoting ITSELF — a
    /// self-referential call to action ("follow us", "rate review and
    /// subscribe", "be a guest", "send us your questions"). It carries the
    /// self-reference in the phrase itself, so `SelfReferenceVerifier`
    /// corroborates it unconditionally (self-corroborating).
    case selfEvident
    /// AMBIGUOUS. The phrase is an event/watch plug that collides with
    /// THIRD-PARTY ads ("get tickets", "on tour", "live show", "live version",
    /// "new ways to watch"). It is NOT inherently self-referential, so it must
    /// be corroborated by an explicit self-reference marker in its local window
    /// before it can demote.
    case requiresCorroboration
}

// MARK: - Phrase value type

/// One curated self-promo action phrase: the display phrase (provenance + the
/// hermetic-pin key), its normalised token sequence — the exact tokens the
/// suppressor matches word-for-word — and its self-reference class.
struct SelfPromoPhrase: Sendable, Equatable {
    /// Human-readable phrase (provenance + the hermetic-pin key).
    let phrase: String
    /// `phrase` run through `LexicalAnchorNormalizer.normalizePhrase` — the
    /// exact token sequence `PromoSuppressor` matches contiguously.
    let tokens: [String]
    /// Whether this phrase is self-corroborating (STRONG) or needs an external
    /// self-reference marker to corroborate before demoting (AMBIGUOUS).
    let selfReference: SelfReferenceClass
}

// MARK: - SelfPromoBank

/// Decoded, validated self-promo action-phrase bank. Show-agnostic: one flat
/// phrase set applied to every episode (the signal is the self-referential
/// action verb, not a per-show entity — see the file header).
struct SelfPromoBank: Sendable, Equatable {
    /// The bank schema version this binary understands. Bumped 1 → 2 for the
    /// attention→verification rework: every phrase now carries a REQUIRED
    /// `selfReference` class (`.selfEvident` / `.requiresCorroboration`).
    static let supportedSchemaVersion = 2
    /// Bundle resource name (no extension).
    static let resourceName = "SelfPromoBank"

    let schemaVersion: Int
    /// The curated self-promo action phrases (never empty; validated).
    let phrases: [SelfPromoPhrase]

    // MARK: - Loading

    /// Load and validate the bundled bank. Throws `SelfPromoBankError` on a
    /// missing resource, undecodable JSON, or a payload that fails validation —
    /// malformed data is rejected loudly, never coerced.
    static func load(bundle: Bundle = .main) throws -> SelfPromoBank {
        guard let url = bundle.url(
            forResource: resourceName,
            withExtension: "json"
        ) else {
            throw SelfPromoBankError.missingResource
        }
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw SelfPromoBankError.malformed("read failed: \(error.localizedDescription)")
        }
        return try decode(data)
    }

    /// Decode + validate a bank payload. Exposed separately from `load(bundle:)`
    /// so tests can feed malformed bytes directly.
    static func decode(_ data: Data) throws -> SelfPromoBank {
        let payload: Payload
        do {
            payload = try JSONDecoder().decode(Payload.self, from: data)
        } catch {
            throw SelfPromoBankError.malformed("decode failed: \(error)")
        }
        return try SelfPromoBank(payload: payload)
    }

    // MARK: - Raw payload (JSON mirror)

    /// Direct JSON mirror; validation happens in `init(payload:)`. The
    /// `selfReference` class is decoded as a raw `String` here and validated
    /// into `SelfReferenceClass` in `init(payload:)` so an unknown value yields
    /// a field-identifying `.malformed` reason rather than a generic decode
    /// error, and a MISSING field fails loud (the property is non-optional).
    private struct Payload: Decodable {
        struct Phrase: Decodable {
            let phrase: String
            let selfReference: String
        }
        let schemaVersion: Int
        let phrases: [Phrase]
    }

    private init(payload: Payload) throws {
        guard payload.schemaVersion == Self.supportedSchemaVersion else {
            throw SelfPromoBankError.malformed(
                "schemaVersion \(payload.schemaVersion) (expected \(Self.supportedSchemaVersion))"
            )
        }
        guard !payload.phrases.isEmpty else {
            throw SelfPromoBankError.malformed("no phrases — an empty bank is inert")
        }

        // Dedupe by NORMALISED token sequence: two display phrases that fold to
        // the same tokens would match identically, so the second is a silent
        // dead entry — reject loudly instead.
        var seenTokenKeys = Set<String>()
        var validated: [SelfPromoPhrase] = []
        validated.reserveCapacity(payload.phrases.count)
        for raw in payload.phrases {
            guard !raw.phrase.isEmpty else {
                throw SelfPromoBankError.malformed("empty phrase")
            }
            let tokens = LexicalAnchorNormalizer.normalizePhrase(raw.phrase)
            // Minimum 2 tokens: a single-token self-promo match is a precision
            // liability (bare social/keyword tokens false-fired on real ads in
            // the spike), so the bank only trusts multi-word action phrases.
            guard tokens.count >= 2 else {
                throw SelfPromoBankError.malformed(
                    "phrase \"\(raw.phrase)\": normalized token count \(tokens.count) below minimum 2"
                )
            }
            let tokenKey = tokens.joined(separator: " ")
            guard seenTokenKeys.insert(tokenKey).inserted else {
                throw SelfPromoBankError.malformed(
                    "duplicate phrase \"\(raw.phrase)\" (normalises to \"\(tokenKey)\") — a second identical matcher is inert"
                )
            }
            // The self-reference class is REQUIRED and must be a known value.
            // An unknown/typo'd class fails loud with the phrase named — a
            // curator adding a phrase MUST classify it (never a silent default).
            guard let selfReference = SelfReferenceClass(rawValue: raw.selfReference) else {
                throw SelfPromoBankError.malformed(
                    "phrase \"\(raw.phrase)\": unknown selfReference \"\(raw.selfReference)\" (expected \"selfEvident\" or \"requiresCorroboration\")"
                )
            }
            validated.append(SelfPromoPhrase(
                phrase: raw.phrase,
                tokens: tokens,
                selfReference: selfReference
            ))
        }

        self.schemaVersion = payload.schemaVersion
        self.phrases = validated
    }
}
