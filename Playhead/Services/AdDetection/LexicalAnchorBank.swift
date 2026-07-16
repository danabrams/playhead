// LexicalAnchorBank.swift
// playhead-xsdz.37: bundled per-show lexical-anchor bank for ad-break
// boundary refinement.
//
// The bank is CURATED OFFLINE — `scripts/xsdz37-emit-lexical-anchor-bank.py`
// compiles the validated read-onset attribution templates (family a) and the
// generic break-edge framing phrases (family b) from the metadata + phrase
// sets the offline prototype `scripts/l2f-lexical-anchor-prototype.py` GO'd
// against gold v2 (see playhead-baselines/xsdz37-lexical-prototype-20260716.md)
// and writes `Playhead/Resources/LexicalAnchorBank.json`. There is no
// on-device learning and no network fetch; the JSON ships in the app bundle
// and is versioned by `schemaVersion`.
//
// Join-key contract (mirrors StingerBank / playhead-l2f.6): production
// identifies a show inside `AdDetectionService.runBackfill` by the opaque
// `podcastId` string it receives — the podcast's RSS feed URL in the shipping
// app, and the corpus `showSlug` on the Catalyst pipeline-dump measurement
// path. Neither representation can be derived from the other at runtime, so
// every show entry carries a `showKeys` ALIAS LIST holding both forms, and
// `entry(forShowKey:)` resolves by exact string match against any alias.
//
// Two anchor families:
//   • Family (a) — per-show read-onset ATTRIBUTION templates ("<station|show>
//     is {brought to you by|sponsored by|supported by|presented by}", plus the
//     public-radio inversion "support for <entity> comes from"). These carry
//     show-specific entity words, so they live under each show entry.
//   • Family (b) — GENERIC (all-show) break-edge FRAMING phrases (pre =
//     "we'll be right back"; resume = "and now back to the show"). One shared
//     set, folded into every registered show's effective anchor list.
//
// Both are EXACT-match only in this cut — the prototype's one false positive
// was a fuzzy "produced by" ~ "sponsored by" collision that exact matching
// kills at zero cost to edge hits, and Swift has no stdlib difflib twin, so
// fuzzy is deliberately out of scope.
//
// Loading follows the StingerBank / PromptRedactor fail-loud precedent:
// malformed JSON throws a typed error with a field-identifying reason. The
// service-side consumer (`AdDetectionService.lexicalAnchorBankIfEnabled`)
// catches, logs at `.error`, and degrades to "no bank" — refinement silently
// disabled, never a crash — but the loader itself never papers over a bad
// asset.

import Foundation

// MARK: - Errors

enum LexicalAnchorBankError: Error, Equatable, CustomStringConvertible {
    /// `LexicalAnchorBank.json` is not present in the bundle.
    case missingResource
    /// The resource exists but failed to decode or validate.
    case malformed(String)

    var description: String {
        switch self {
        case .missingResource:
            return "LexicalAnchorBank.json missing from bundle"
        case .malformed(let reason):
            return "LexicalAnchorBank.json malformed: \(reason)"
        }
    }
}

// MARK: - Normalisation

/// Token normalisation for lexical-anchor matching. MUST match the offline
/// prototype `scripts/l2f-lexical-anchor-prototype.py`: lowercase, fold+strip
/// apostrophes so "We'll" → "well", then drop everything outside `[a-z0-9]`.
///
/// The explicit apostrophe fold is subsumed by the `[a-z0-9]`-keep pass (an
/// apostrophe is non-alphanumeric, so keeping only `[a-z0-9]` both removes it
/// and joins its neighbours — exactly what the Python's two-step does), so a
/// single ASCII-alphanumeric filter over the lowercased scalars is bit-for-bit
/// equivalent to the prototype's `normalize_word`.
enum LexicalAnchorNormalizer {
    /// Lowercase, then keep only ASCII `[a-z0-9]` scalars (dropping — and
    /// joining across — everything else, including apostrophes, spaces inside
    /// a token, and non-ASCII letters, matching Python's `[^a-z0-9]` strip).
    static func normalizeWord(_ raw: String) -> String {
        var scalars = String.UnicodeScalarView()
        for scalar in raw.lowercased().unicodeScalars {
            let value = scalar.value
            if (0x61...0x7A).contains(value) || (0x30...0x39).contains(value) {
                scalars.append(scalar)
            }
        }
        return String(scalars)
    }

    /// Split on any whitespace, normalise each word, drop empties — the
    /// prototype's `normalize_phrase`.
    static func normalizePhrase(_ raw: String) -> [String] {
        raw
            .split(whereSeparator: { $0.isWhitespace })
            .map { normalizeWord(String($0)) }
            .filter { !$0.isEmpty }
    }
}

// MARK: - Anchor value types

/// Which break edge an anchor snaps. `pre` = the read/framing phrase precedes
/// the ad content, so it snaps the break START; `post` = the resume phrase
/// follows the ad content, so it snaps the break END.
enum LexicalAnchorSide: String, Sendable, Equatable {
    case pre
    case post
}

/// Match policy for an anchor. `exact` (word-for-word on normalised tokens) is
/// the only policy in this cut; the enum exists so a future fuzzy policy is a
/// fail-loud schema addition rather than a silent behaviour change.
enum LexicalAnchorMatchPolicy: String, Sendable, Equatable {
    case exact
}

/// One curated lexical anchor: a normalised phrase, the edge it snaps, and the
/// learned onset offset applied after the matched phrase position.
struct LexicalAnchor: Sendable, Equatable {
    /// Human-readable phrase / template (provenance + the hermetic-pin key).
    let phrase: String
    /// `phrase` run through `LexicalAnchorNormalizer.normalizePhrase` — the
    /// exact token sequence the refiner matches word-for-word.
    let tokens: [String]
    /// `pre` snaps the break start; `post` snaps the break end.
    let side: LexicalAnchorSide
    /// Only `.exact` in this cut.
    let matchPolicy: LexicalAnchorMatchPolicy
    /// Learned residual (seconds) between the matched-phrase position (the
    /// first matched word's start) and the true break edge. `refinedEdge =
    /// firstMatchedWordStart + edgeOffsetSeconds`.
    let edgeOffsetSeconds: Double
    /// Curated precision prior in (0, 1]. Exact matching is the precision gate
    /// in this cut, so the refiner does not yet consult this — the field ships
    /// for schema parity and future gating.
    let confidence: Double
    /// Curated support floor (>= 2). Like `confidence`, a curated prior for a
    /// template family, NOT a corpus frequency.
    let support: Int
}

extension LexicalAnchor {
    /// Build an exact-match anchor from a display phrase, normalising it to the
    /// matched token sequence. Single normalisation path shared by the bank
    /// decoder and the tests, so `tokens` can never drift from `phrase`.
    static func exact(
        phrase: String,
        side: LexicalAnchorSide,
        edgeOffsetSeconds: Double,
        confidence: Double = 1.0,
        support: Int = 2
    ) -> LexicalAnchor {
        LexicalAnchor(
            phrase: phrase,
            tokens: LexicalAnchorNormalizer.normalizePhrase(phrase),
            side: side,
            matchPolicy: .exact,
            edgeOffsetSeconds: edgeOffsetSeconds,
            confidence: confidence,
            support: support
        )
    }
}

// MARK: - Show entry

/// Per-show bank entry: the family-(a) attribution templates compiled for one
/// show, keyed by the alias list.
struct LexicalAnchorShowEntry: Sendable, Equatable {
    /// Alias list resolvable from production's `podcastId` (feed URL) or the
    /// corpus dump's `showSlug`. Exact-match join; see file header.
    let showKeys: [String]
    /// Human-readable show name (provenance/diagnostics only — never matched).
    let showName: String
    /// Family-(a) attribution templates for this show (never empty; validated).
    let anchors: [LexicalAnchor]
}

// MARK: - LexicalAnchorBank

/// Decoded, validated lexical-anchor bank.
struct LexicalAnchorBank: Sendable, Equatable {
    /// The bank schema version this binary understands.
    static let supportedSchemaVersion = 1
    /// Bundle resource name (no extension).
    static let resourceName = "LexicalAnchorBank"

    let schemaVersion: Int
    /// Per-show family-(a) attribution templates.
    let shows: [LexicalAnchorShowEntry]
    /// Family-(b) generic framing phrases, shared across every registered show.
    let genericAnchors: [LexicalAnchor]

    /// Resolve the show entry for a runtime show key (the `podcastId`
    /// `runBackfill` received). Exact match against any alias; `nil`/empty
    /// input (unknown show) resolves to no entry.
    func entry(forShowKey key: String?) -> LexicalAnchorShowEntry? {
        guard let key, !key.isEmpty else { return nil }
        return shows.first { $0.showKeys.contains(key) }
    }

    /// The EFFECTIVE anchor set for a show: its family-(a) templates plus the
    /// generic family-(b) framing phrases. `nil` for an unknown show (no entry
    /// ⇒ never consulted — mirrors the stinger entry-gated consult, so a show
    /// absent from the bank is byte-identical to flag-off). The generic set is
    /// intentionally scoped to REGISTERED shows: applying host-framing snaps to
    /// arbitrary unknown shows is exactly the false-fire surface the prototype
    /// flagged, so this cut only trusts framing phrases where the family-(a)
    /// metadata also recognises the show.
    func effectiveAnchors(forShowKey key: String?) -> [LexicalAnchor]? {
        guard let entry = entry(forShowKey: key) else { return nil }
        return entry.anchors + genericAnchors
    }

    // MARK: - Loading

    /// Load and validate the bundled bank. Throws `LexicalAnchorBankError` on a
    /// missing resource, undecodable JSON, or a payload that fails validation —
    /// malformed data is rejected loudly, never coerced.
    static func load(bundle: Bundle = .main) throws -> LexicalAnchorBank {
        guard let url = bundle.url(
            forResource: resourceName,
            withExtension: "json"
        ) else {
            throw LexicalAnchorBankError.missingResource
        }
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw LexicalAnchorBankError.malformed("read failed: \(error.localizedDescription)")
        }
        return try decode(data)
    }

    /// Decode + validate a bank payload. Exposed separately from `load(bundle:)`
    /// so tests can feed malformed bytes directly.
    static func decode(_ data: Data) throws -> LexicalAnchorBank {
        let payload: Payload
        do {
            payload = try JSONDecoder().decode(Payload.self, from: data)
        } catch {
            throw LexicalAnchorBankError.malformed("decode failed: \(error)")
        }
        return try LexicalAnchorBank(payload: payload)
    }

    // MARK: - Raw payload (JSON mirror)

    /// Direct JSON mirror; validation happens in `init(payload:)`.
    private struct Payload: Decodable {
        struct Anchor: Decodable {
            let phrase: String
            let side: String
            let matchPolicy: String
            let edgeOffsetSeconds: Double
            let confidence: Double
            let support: Int
        }

        struct Show: Decodable {
            let showKeys: [String]
            let showName: String
            let anchors: [Anchor]
        }

        let schemaVersion: Int
        let shows: [Show]
        let genericAnchors: [Anchor]
    }

    private init(payload: Payload) throws {
        guard payload.schemaVersion == Self.supportedSchemaVersion else {
            throw LexicalAnchorBankError.malformed(
                "schemaVersion \(payload.schemaVersion) (expected \(Self.supportedSchemaVersion))"
            )
        }

        var seenKeys = Set<String>()
        var entries: [LexicalAnchorShowEntry] = []
        entries.reserveCapacity(payload.shows.count)
        for show in payload.shows {
            guard !show.showKeys.isEmpty, show.showKeys.allSatisfy({ !$0.isEmpty }) else {
                throw LexicalAnchorBankError.malformed("show \(show.showName): empty showKeys")
            }
            for key in show.showKeys {
                guard seenKeys.insert(key).inserted else {
                    throw LexicalAnchorBankError.malformed(
                        "duplicate showKey \(key) — entry resolution would be ambiguous"
                    )
                }
            }
            guard !show.showName.isEmpty else {
                throw LexicalAnchorBankError.malformed(
                    "show with keys \(show.showKeys): empty showName"
                )
            }
            guard !show.anchors.isEmpty else {
                throw LexicalAnchorBankError.malformed(
                    "show \(show.showName): no anchors — an empty show entry is inert"
                )
            }
            let anchors = try show.anchors.map {
                try Self.validatedAnchor($0, context: show.showName)
            }
            entries.append(LexicalAnchorShowEntry(
                showKeys: show.showKeys,
                showName: show.showName,
                anchors: anchors
            ))
        }

        let generics = try payload.genericAnchors.map {
            try Self.validatedAnchor($0, context: "generic")
        }

        self.schemaVersion = payload.schemaVersion
        self.shows = entries
        self.genericAnchors = generics
    }

    private static func validatedAnchor(
        _ raw: Payload.Anchor,
        context: String
    ) throws -> LexicalAnchor {
        guard !raw.phrase.isEmpty else {
            throw LexicalAnchorBankError.malformed("\(context): empty anchor phrase")
        }
        let tokens = LexicalAnchorNormalizer.normalizePhrase(raw.phrase)
        guard tokens.count >= 2 else {
            throw LexicalAnchorBankError.malformed(
                "\(context) anchor \"\(raw.phrase)\": normalized token count \(tokens.count) below minimum 2"
            )
        }
        guard let side = LexicalAnchorSide(rawValue: raw.side) else {
            throw LexicalAnchorBankError.malformed(
                "\(context) anchor \"\(raw.phrase)\": unknown side \"\(raw.side)\" (expected pre|post)"
            )
        }
        guard let policy = LexicalAnchorMatchPolicy(rawValue: raw.matchPolicy) else {
            throw LexicalAnchorBankError.malformed(
                "\(context) anchor \"\(raw.phrase)\": unsupported matchPolicy \"\(raw.matchPolicy)\" (exact only in this cut)"
            )
        }
        guard raw.edgeOffsetSeconds.isFinite else {
            throw LexicalAnchorBankError.malformed(
                "\(context) anchor \"\(raw.phrase)\": non-finite edgeOffsetSeconds"
            )
        }
        guard raw.confidence.isFinite, raw.confidence > 0, raw.confidence <= 1 else {
            throw LexicalAnchorBankError.malformed(
                "\(context) anchor \"\(raw.phrase)\": confidence \(raw.confidence) outside (0, 1]"
            )
        }
        guard raw.support >= 2 else {
            throw LexicalAnchorBankError.malformed(
                "\(context) anchor \"\(raw.phrase)\": support \(raw.support) below minimum 2"
            )
        }
        return LexicalAnchor(
            phrase: raw.phrase,
            tokens: tokens,
            side: side,
            matchPolicy: policy,
            edgeOffsetSeconds: raw.edgeOffsetSeconds,
            confidence: raw.confidence,
            support: raw.support
        )
    }
}
