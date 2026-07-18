// MusicOffsetLexicalGate.swift
// playhead-eki3 (PR1): lexical ad-cue gate for the sustained-music-offset
// proposer (t1py) — the false-positive reducer for its music-ONLY banners.
//
// The problem (measured + audited, 2026-07-18): t1py's flag-on measurement
// lifted post-roll coverage 12% → 53% at ~79% true precision, but ~18% of the
// resulting `.markOnly` banners are FALSE. A 65-false-prediction audit proved
// the false banners are CONTENT at the music→speech edge (cold-open / intro
// branding, outro credits, theme music, host intros / segment transitions over
// music), while the real ads have AD-COPY at that edge. Position does NOT
// separate them (an edge-gate craters recall) and confidence barely does. The
// separator is LEXICAL: is there third-party ad-copy at the trailing edge.
//
// This gate is the Swift port of the validated prototype
// (scratchpad/measure2/lexical_prototype.py — 97% FP-kill (31/32),
// 58% ad-recall (15/26) against the audited 65). Three principled parts, NOT
// tuned to the 65 (the ~42% cue-less creative-ad residual is deliberately left
// for playhead-r2vz / FM — do NOT chase it here):
//   1. AD-cue patterns (third-party underwriting / sponsor tags / DTC-CTA /
//      bare URL / promo).
//   2. FIRST-PARTY exclusion — a show promoting ITSELF or its network
//      (ad-free / join Wondery / subscribe-to-the-show / wherever-you-get-
//      podcasts) is NOT a third-party ad and OVERRIDES an ad-cue → suppress.
//   3. Onset window — the transcript from the trailing edge onward, ~one
//      ad-read (`>= edge - 2s`, capped ~600 chars), wide enough for a
//      host-read brand cue that lands mid-read (the Chris Hayes NJM ads).
//
// SCOPING (correctness-critical): this gate ONLY ever suppresses an
// UNCORROBORATED music-ONLY proposal — a `.sustainedMusic`-origin region that
// merged with NOTHING else in `RegionProposalBuilder.build`. A music region
// that merged with FM / lexical / sponsor / fingerprint / classifier evidence
// is left UNTOUCHED (it is not a music-only banner, and music-only can never
// auto-skip anyway — see `DecisionMapper.isMusicOnlyProvenance`). Every
// non-music origin is untouched. Behind a DEFAULT-OFF flag on
// `RegionShadowPhase.Input`, so the whole seam is a byte-identical no-op until
// enabled.
//
// INJECTION POINT — this filters the POST-`build` `[ProposedRegion]` list
// rather than the pre-`build` `[ProposedSpan]` list. The merge inside
// `RegionProposalBuilder.build` is the authoritative resolver of "did this
// music run corroborate with any other signal": after it, a music-only region
// has `origins == [.sustainedMusic]` (± the bare `.acoustic` hint) and a
// corroborated one carries the corroborating origin. Filtering here reads that
// verdict directly instead of re-deriving time-overlap against every source
// (FM windows carry atom ordinals, not times), which keeps the
// "only-music-only-uncorroborated" scoping provable and avoids stripping
// `.sustainedMusic` provenance/width from a would-be-corroborated region.
//
// Design grain: pure `enum`, Sendable static helpers, no actor / persistence /
// I/O / network — mirrors `SustainedMusicOffsetProposer` and the
// `ChapterDispositionClassifier` regex-pattern idiom.

import Foundation

enum MusicOffsetLexicalGate {

    // MARK: - Onset window tuning (ported from the prototype, not fit to the 65)

    /// Seconds of lead before the trailing music→speech edge to start the onset
    /// window. Mirrors the prototype's `t >= predEnd - 2`: a cue whose read
    /// begins a beat before the detected edge is still captured.
    static let onsetWindowLeadSeconds: Double = 2.0

    /// Character cap on the onset window (~one dense ad-read). Mirrors the
    /// prototype's `[:600]`: wide enough for a host-read brand cue that lands
    /// mid-read, deliberately NOT a tight time bound tuned to the audited 65.
    static let onsetWindowCharacterCap: Int = 600

    // MARK: - Corroboration

    /// Origins that count as INDEPENDENT presence corroboration. A
    /// `.sustainedMusic` region carrying ANY of these merged with another
    /// signal in `RegionProposalBuilder.build` and is therefore NOT a
    /// music-only banner — this gate leaves it untouched. `.acoustic` is
    /// deliberately EXCLUDED: a bare acoustic break is a hint, not presence
    /// evidence. This mirrors — analogically, at the region-origin level rather
    /// than the anchor-ref level — `DecisionMapper.isMusicOnlyProvenance`, which
    /// likewise treats FM / catalog / classifier (but NOT acoustic) as the
    /// corroborating presence that lifts a span out of the music-only class.
    static let corroboratingOrigins: ProposedRegionOrigins =
        [.lexical, .sponsor, .fingerprint, .foundationModel, .classifier]

    /// True IFF `region` is an UNCORROBORATED music-ONLY proposal — the only
    /// class this gate ever considers suppressing.
    static func isUncorroboratedMusicOnly(_ region: ProposedRegion) -> Bool {
        region.origins.contains(.sustainedMusic)
            && region.origins.isDisjoint(with: corroboratingOrigins)
    }

    // MARK: - The single suppression decision (playhead-r2vz / PR2 seam)

    /// The ONE interceptable decision point. Returns `true` when `region` is an
    /// uncorroborated music-only proposal whose onset window carries NO
    /// third-party ad-cue — i.e. the cue-less content / credits / theme /
    /// host-intro false-banner class the audit isolated. playhead-r2vz (PR2)
    /// will route exactly this set to an FM recovery pass INSTEAD of dropping
    /// it; PR1 drops it.
    static func shouldSuppress(_ region: ProposedRegion, chunks: [TranscriptChunk]) -> Bool {
        guard isUncorroboratedMusicOnly(region) else { return false }
        let window = onsetWindowText(trailingEdge: region.endTime, chunks: chunks)
        return !hasAdCue(inOnsetWindow: window)
    }

    /// Drop every cue-less uncorroborated music-only proposal from `regions`.
    /// Corroborated music regions and all non-music origins pass through
    /// untouched. Deterministic — preserves input order.
    static func filter(_ regions: [ProposedRegion], chunks: [TranscriptChunk]) -> [ProposedRegion] {
        regions.filter { !shouldSuppress($0, chunks: chunks) }
    }

    // MARK: - Onset-window extraction

    /// The transcript text from `trailingEdge - onsetWindowLeadSeconds` onward,
    /// in time order, capped at `onsetWindowCharacterCap` characters. Uses raw
    /// `chunk.text` (NOT `normalizedText`) so URL dots and punctuation survive
    /// for the DTC / bare-URL patterns, which a lowercase/punctuation-stripped
    /// normalization could destroy.
    ///
    /// `trailingEdge` is the region's `endTime` — the music→speech offset for an
    /// uncorroborated music-only region. (If such a region absorbed a standalone
    /// `.acoustic` proposal in `build`'s merge its `endTime` can sit ~1 atom past
    /// the music edge, nudging the cutoff slightly later; the 2s lead plus the
    /// multi-chunk width of a real ad-read absorb that.) Chunks are ordered by
    /// `startTime` with a `chunkIndex` tiebreaker so equal-timestamp chunks keep
    /// transcript order and the 600-char truncation boundary is stable.
    static func onsetWindowText(trailingEdge: Double, chunks: [TranscriptChunk]) -> String {
        let cutoff = trailingEdge - onsetWindowLeadSeconds
        let joined = chunks
            .filter { $0.startTime >= cutoff }
            .sorted { lhs, rhs in
                lhs.startTime == rhs.startTime
                    ? lhs.chunkIndex < rhs.chunkIndex
                    : lhs.startTime < rhs.startTime
            }
            .map(\.text)
            .joined(separator: " ")
        return String(joined.prefix(onsetWindowCharacterCap))
    }

    // MARK: - Cue matcher (pure, string-only — unit-tested directly)

    /// True IFF `text` carries a third-party ad-cue AND is NOT a first-party
    /// (house / network) self-promo. Order is load-bearing and mirrors the
    /// prototype exactly:
    ///   1. FIRST-PARTY self-promo (ad-free / join Wondery / subscribe-to-the-
    ///      show / wherever-you-get-podcasts) OVERRIDES any ad-cue → not an ad.
    ///   2. else a third-party ad-cue (underwriting / sponsor tag / DTC-CTA /
    ///      bare URL / promo) → ad.
    ///   3. else → not an ad.
    static func hasAdCue(inOnsetWindow text: String) -> Bool {
        if matchesAny(firstPartyPatterns, in: text) { return false }
        return matchesAny(adPatterns, in: text)
    }

    // MARK: - Private

    private static func matchesAny(_ patterns: [NSRegularExpression], in text: String) -> Bool {
        guard !text.isEmpty else { return false }
        let range = NSRange(text.startIndex..., in: text)
        return patterns.contains { $0.firstMatch(in: text, options: [], range: range) != nil }
    }

    private static func compile(_ patterns: [String]) -> [NSRegularExpression] {
        patterns.compactMap { pattern in
            // Hardcoded literals — a compile failure is a programmer error.
            guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
                assertionFailure("MusicOffsetLexicalGate: invalid regex \(pattern)")
                return nil
            }
            return regex
        }
    }

    /// Third-party ad-cue classes, ported verbatim from the prototype's `AD`
    /// alternation. Principled cue families (underwriting / sponsor tags /
    /// DTC-CTA / bare URL / promo), NOT episode-specific tuning.
    private static let adPatterns: [NSRegularExpression] = compile([
        // underwriting / sponsor tags
        #"support (for [\w\s]+)?comes from"#,
        #"this message (is|comes) from"#,
        #"message comes from"#,
        #"brought to you by"#,
        #"sponsored by"#,
        #"this episode is sponsored"#,
        #"today'?s? sponsor"#,
        // DTC-CTA / URL
        #"\bgo to \w[\w.]*\.(com|org|io|net)"#,
        #"\bat \w[\w.]*\.(com|org|io|net)"#,
        #"\b\w[\w-]*\.(com|org|io|net)\b"#,
        #"use (promo )?code"#,
        #"promo code"#,
        #"\bcall 1[\s-]?800"#,
        #"\d+% off"#,
        #"free (trial|shipping|months?)"#,
        #"no impact on your credit"#,
        #"pre[\s-]?qualif"#,
        #"cash (back|rewards)"#,
        #"discover it at"#,
        #"sign up (now|today) at"#,
        #"download the \w+ app"#,
        #"terms (and conditions|apply)"#,
    ])

    /// First-party / house-promo markers, ported verbatim from the prototype's
    /// `FIRST_PARTY` alternation. A show promoting itself or its network is not
    /// a third-party ad, so a match here OVERRIDES any ad-cue and suppresses.
    private static let firstPartyPatterns: [NSRegularExpression] = compile([
        #"ad[\s-]free"#,
        #"join(ing)? (wondery|audible)\b"#,
        #"\b(npr|wondery|audible) ?\+"#,
        #"join (npr|wondery)\+?"#,
        #"subscribe to (the show|this (show|podcast))"#,
        #"follow (or subscribe|the show|this show)"#,
        #"wherever you get your podcasts"#,
        #"our other (podcast|show)s?"#,
        #"on the \w+ app\b"#,
    ])
}
