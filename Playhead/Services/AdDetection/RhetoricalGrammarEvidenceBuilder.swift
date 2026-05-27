// RhetoricalGrammarEvidenceBuilder.swift
// playhead-xsdz.12: Rhetorical act-sequence grammar evidence signal for
// on-device ad detection (part of epic playhead-xsdz, the multi-signal local
// ad scorer).
//
// Why this exists
// ---------------
// Ads follow a near-universal persuasion PROGRAM — roughly
//   HOOK → PROBLEM → SOLUTION → EVIDENCE → OFFER → CTA.
// No single rhetorical role is ad-specific: ordinary editorial content can ask
// a question (HOOK), name a pain point (PROBLEM), recommend a product
// (SOLUTION), cite a statistic (EVIDENCE), mention a price (OFFER), or give a
// URL (CTA). What is almost exclusively an ad is the ORDERED CO-OCCURRENCE of
// THREE OR MORE of these roles within one tight span, in (roughly) the
// canonical order. That ordered co-occurrence fires even when the existing
// sponsor / promo-code / URL lexical cues (`LexicalScanner`,
// `LexicalAutoAdEvidenceBuilder`) do NOT — e.g. a host read with no literal
// "use code …" or "brought to you by".
//
// This directly attacks the dominant FALSE-POSITIVE modes of the other
// channels:
//   • An editorial brand mention is SOLUTION-only            → 1 role  → ~0.
//   • A self-promo ("subscribe / follow the show") is CTA-only → 1 role → ~0.
//   • A product review is EVIDENCE + SOLUTION (no OFFER/CTA)  → 2 roles → ~0.
// Only the full HOOK+PROBLEM+SOLUTION(+OFFER+CTA) arc clears the >= 3-role
// gate AND scores high once the canonical-order bonus is applied.
//
// What this is NOT
// ----------------
// Deterministic rule/grammar detector ONLY. No ML model, no cloud, no network,
// no new dependency, no per-show state, no I/O, no async. A small, pure,
// `Sendable` value type that projects the span's transcript prose into at most
// ONE capped `.rhetoricalGrammar` ledger entry. It is a MODEST, CORROBORATIVE
// channel: its cap (`FusionWeightConfig.rhetoricalGrammarCap`, 0.20) has NO
// qualified promotion track, so it can never drive an auto-skip on its own —
// it only adds honest text-derived mass and bumps `distinctKinds.count` for
// the corroboration quorum. Mirrors the `.audioForensics` / `.crossEpisodeMemory`
// carve-outs (one kind, one cap, one OFF-by-default flag).
//
// Gated OFF by default (`AdDetectionConfig.rhetoricalGrammarEnabled`): with the
// flag off the builder is never called, NO entry is built, and behaviour is
// byte-identical to pre-xsdz.12 main. The classifier + scorer stay fully built
// and unit-tested, just inert in production until a corpus eval shows lift.

import Foundation

// MARK: - RhetoricalRole

/// One of the six rhetorical roles in the canonical persuasion program. The
/// `rawValue` ordering encodes the CANONICAL order an ad tends to follow; the
/// sequence scorer rewards spans whose first-appearance positions respect this
/// order and penalizes inversions.
enum RhetoricalRole: Int, Sendable, CaseIterable, Comparable {
    /// Attention grab — a question or provocative framing that opens the read.
    case hook = 0
    /// The pain point / unmet need the product is positioned against.
    case problem = 1
    /// The product / service offered as the answer to the problem.
    case solution = 2
    /// Proof: stats, testimonials, credentials, "clinically", "rated #1".
    case evidence = 3
    /// The deal: price, discount, free trial, money-back guarantee.
    case offer = 4
    /// The call to action: go to / visit / use code / sign up / link in …
    case cta = 5

    static func < (lhs: RhetoricalRole, rhs: RhetoricalRole) -> Bool {
        lhs.rawValue < rhs.rawValue
    }

    /// Stable diagnostic label recorded in the emitted `.lexical(matchedCategories:)`
    /// detail so NARL replay / decision logs can see WHICH roles formed the arc.
    var label: String {
        switch self {
        case .hook: return "hook"
        case .problem: return "problem"
        case .solution: return "solution"
        case .evidence: return "evidence"
        case .offer: return "offer"
        case .cta: return "cta"
        }
    }
}

// MARK: - RhetoricalGrammarEvidenceBuilder

struct RhetoricalGrammarEvidenceBuilder: Sendable {

    // MARK: - Config

    /// Tunable knobs for the grammar detector. Defaults are conservative
    /// (precision over recall) and exposed so tests can probe behaviour without
    /// reaching into the implementation, and so a future calibration bead can
    /// retune without editing this file.
    struct Config: Sendable, Equatable {
        /// Minimum number of DISTINCT rhetorical roles required before the span
        /// contributes any weight. Fewer than this is NOT ad-like (an editorial
        /// brand mention is SOLUTION-only; a self-promo is CTA-only; a product
        /// review is EVIDENCE+SOLUTION). Three roles is the bar at which the
        /// ordered co-occurrence becomes almost exclusively a structured ad
        /// read.
        let minDistinctRoles: Int

        /// Maximum emitted weight (before the fusion clamp to
        /// `rhetoricalGrammarCap`). A full, in-order 5+ role arc lands here; a
        /// bare 3-role arc with poor ordering lands well below. Kept MODEST —
        /// this is a corroborator, never a sole promoter.
        let maxWeight: Double

        /// Fraction of `maxWeight` carried by the distinct-role-count component
        /// (the remainder is carried by the canonical-order-consistency
        /// component). 0.6 means "having the roles" is worth a bit more than
        /// "having them in order," but order materially matters — an out-of-
        /// order arc is penalized toward (but never below) the count-only mass.
        let roleCountWeightFraction: Double

        static let `default` = Config(
            minDistinctRoles: 3,
            maxWeight: 0.20,
            roleCountWeightFraction: 0.6
        )

        init(
            minDistinctRoles: Int = 3,
            maxWeight: Double = 0.20,
            roleCountWeightFraction: Double = 0.6
        ) {
            self.minDistinctRoles = minDistinctRoles
            self.maxWeight = maxWeight
            self.roleCountWeightFraction = roleCountWeightFraction
        }
    }

    private let config: Config

    /// Compiled role-cue patterns, grouped by role. Built once per builder
    /// instance — builders are constructed per-backfill, not per-span, so this
    /// is cheap and shared across every span in an episode.
    private let rolePatterns: [RhetoricalRole: [NSRegularExpression]]

    init(config: Config = .default) {
        self.config = config
        self.rolePatterns = Self.compileRolePatterns()
    }

    // MARK: - Public API

    /// Build the (at most one) `.rhetoricalGrammar` ledger entry for a span's
    /// transcript text, or `[]` when the grammar does not fire.
    ///
    /// - Parameters:
    ///   - text: The span's joined transcript prose (e.g. the span's atom text
    ///     joined with spaces, as built at the `buildEvidenceLedger` call site).
    ///   - span: The decoded span the evidence is scored against (used only for
    ///     the entry's provenance; scoring is text-only).
    /// - Returns: A single `.rhetoricalGrammar` entry when >= `minDistinctRoles`
    ///   distinct roles appear; otherwise `[]`.
    func buildEntries(
        text: String,
        for span: DecodedSpan
    ) -> [EvidenceLedgerEntry] {
        let assessment = assess(text: text)
        guard let assessment else { return [] }

        return [EvidenceLedgerEntry(
            source: .rhetoricalGrammar,
            weight: assessment.weight,
            // Reuse `.lexical(matchedCategories:)` — the closest existing detail
            // variant — to record WHICH roles formed the arc (canonical order),
            // so diagnostics / NARL replay can see WHY the grammar fired. A
            // bespoke detail case would be churn for no consumer today; xsdz.1
            // reuses the same variant for the same reason.
            detail: .lexical(matchedCategories: assessment.orderedRoles.map(\.label))
        )]
    }

    // MARK: - Assessment

    /// The result of grammar-fitting a span's text: which roles fired, in what
    /// order, and the normalized weight. Exposed (internal) so unit tests can
    /// assert the role set + ordering without parsing the ledger entry.
    struct Assessment: Sendable, Equatable {
        /// Distinct roles present, sorted by their FIRST-appearance segment
        /// position (the order they actually occurred in the prose).
        let orderedRoles: [RhetoricalRole]
        /// Fraction in [0, 1] of canonical-order-consistent role pairs.
        let orderConsistency: Double
        /// Final normalized weight in [0, config.maxWeight].
        let weight: Double
    }

    /// Classify, sequence, and score the span text. Returns `nil` when the
    /// >= `minDistinctRoles` gate is not met (fewer roles is not ad-like).
    ///
    /// Deterministic and bounded: cost is O(segments × patterns), and both are
    /// bounded by the span text length (no backtracking-prone patterns, no
    /// unbounded loops).
    func assess(text: String) -> Assessment? {
        let segments = Self.segments(of: text)
        guard !segments.isEmpty else { return nil }

        // First-appearance segment index of each role (smaller = earlier).
        var firstSeen: [RhetoricalRole: Int] = [:]
        for (index, segment) in segments.enumerated() {
            for role in roles(in: segment) where firstSeen[role] == nil {
                firstSeen[role] = index
            }
        }

        // >= minDistinctRoles gate: fewer is not ad-like.
        guard firstSeen.count >= config.minDistinctRoles else { return nil }

        // Roles in the order they actually appeared. Ties on segment index
        // (two roles cued in the same sentence) are broken by canonical rank so
        // the ordering is deterministic and treats a same-sentence pair as
        // already canonically ordered.
        let orderedRoles = firstSeen
            .sorted { lhs, rhs in
                if lhs.value != rhs.value { return lhs.value < rhs.value }
                return lhs.key < rhs.key
            }
            .map(\.key)

        let orderConsistency = Self.orderConsistency(of: orderedRoles)
        let weight = self.weight(
            distinctRoleCount: orderedRoles.count,
            orderConsistency: orderConsistency
        )

        return Assessment(
            orderedRoles: orderedRoles,
            orderConsistency: orderConsistency,
            weight: weight
        )
    }

    // MARK: - Scoring

    /// Combine the distinct-role count and the canonical-order consistency into
    /// a single normalized weight in [0, config.maxWeight].
    ///
    /// Two components:
    ///   • role-count component — a linear ramp from the gate-clearing minimum
    ///     (`minDistinctRoles`, which earns `countBaseFraction`) up to all 6
    ///     roles (which earn the full 1.0). A bare 3-role arc therefore still
    ///     earns a meaningful but partial count share; the full arc earns all
    ///     of it.
    ///   • order component — `orderConsistency` directly scales its share, so a
    ///     perfectly canonical arc earns the full order share and a fully
    ///     inverted arc earns none.
    ///
    /// `countBaseFraction` (the share a just-gate-clearing arc gets from the
    /// count component) is fixed at 0.5: a 3-role arc gets half the count
    /// share's mass, a 6-role arc the full share. This keeps a minimum-gate
    /// arc with poor ordering meaningfully below a full in-order arc.
    private func weight(
        distinctRoleCount: Int,
        orderConsistency: Double
    ) -> Double {
        let allRoles = Double(RhetoricalRole.allCases.count)          // 6
        let minRoles = Double(config.minDistinctRoles)                // 3
        let countBaseFraction = 0.5
        // Linear ramp: minRoles → countBaseFraction, allRoles → 1.0.
        let ramp = max(allRoles - minRoles, 1)
        let rolesAboveMin = max(Double(distinctRoleCount) - minRoles, 0)
        let countFraction = min(
            countBaseFraction + (1.0 - countBaseFraction) * (rolesAboveMin / ramp),
            1.0
        )

        let countShare = config.roleCountWeightFraction
        let orderShare = 1.0 - config.roleCountWeightFraction

        let normalized = countShare * countFraction + orderShare * orderConsistency
        return min(max(normalized, 0), 1.0) * config.maxWeight
    }

    /// Fraction in [0, 1] of ALL role pairs (every i < j in first-appearance
    /// order, not just adjacent ones) whose canonical ranks are non-decreasing
    /// — a concordant-pair (Kendall-tau-style) consistency measure. 1.0 means
    /// every pair respects the canonical HOOK→…→CTA order; 0.0 means every pair
    /// is inverted.
    ///
    /// With fewer than two roles there are no pairs to judge; we return 1.0
    /// (vacuously consistent), though the >= 3-role gate means this is only
    /// ever called with >= 3 roles in the live path.
    static func orderConsistency(of orderedRoles: [RhetoricalRole]) -> Double {
        guard orderedRoles.count >= 2 else { return 1.0 }
        var consistentPairs = 0
        var totalPairs = 0
        for i in 0..<orderedRoles.count {
            for j in (i + 1)..<orderedRoles.count {
                totalPairs += 1
                // Roles are listed in actual-appearance order, so role[i]
                // appeared no later than role[j]. The pair is canonical-
                // consistent when role[i]'s canonical rank <= role[j]'s.
                if orderedRoles[i].rawValue <= orderedRoles[j].rawValue {
                    consistentPairs += 1
                }
            }
        }
        guard totalPairs > 0 else { return 1.0 }
        return Double(consistentPairs) / Double(totalPairs)
    }

    // MARK: - Role classification

    /// The distinct roles cued in a single segment. A segment can carry more
    /// than one role (e.g. "use code SAVE for 20% off" is both OFFER and CTA);
    /// we record every role whose pattern matches.
    private func roles(in segment: String) -> Set<RhetoricalRole> {
        guard !segment.isEmpty else { return [] }
        let ns = segment as NSString
        let range = NSRange(location: 0, length: ns.length)
        var found: Set<RhetoricalRole> = []
        for (role, patterns) in rolePatterns {
            for pattern in patterns where pattern.firstMatch(in: segment, range: range) != nil {
                found.insert(role)
                break
            }
        }
        return found
    }

    // MARK: - Segmentation

    /// Split text into lowercased segments on sentence-ish boundaries
    /// (terminal punctuation) so role first-appearance ORDER is meaningful.
    /// Empty / whitespace-only fragments are dropped. Deterministic and
    /// allocation-bounded by the input length.
    static func segments(of text: String) -> [String] {
        let lowered = text.lowercased()
        let separators = CharacterSet(charactersIn: ".!?;\n")
        return lowered
            .components(separatedBy: separators)
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
    }

    // MARK: - Pattern compilation

    /// Compile the per-role cue patterns. Curated, high-precision, and kept
    /// deliberately small — expanding any role's set trades precision for recall
    /// and should be done with corpus evidence. Word-boundary-anchored so cues
    /// do not match inside longer words. Patterns run against lowercased,
    /// sentence-segmented text, so they assume lowercase input.
    private static func compileRolePatterns() -> [RhetoricalRole: [NSRegularExpression]] {
        var groups: [RhetoricalRole: [NSRegularExpression]] = [:]

        // HOOK — attention grab: a question framing or "ever / tired of / what if".
        groups[.hook] = compile([
            #"\bhave you ever\b"#,
            #"\bdo you (ever|struggle|want|need|find)\b"#,
            #"\bare you (tired|struggling|looking|ready)\b"#,
            #"\bever (wish|wanted|tried|felt)\b"#,
            #"\btired of\b"#,
            #"\bwhat if\b"#,
            #"\bimagine\b"#,
            #"\bpicture this\b"#,
            #"\blet me tell you about\b"#,
        ])

        // PROBLEM — the pain point / unmet need.
        groups[.problem] = compile([
            #"\b(it|that)('s| is)? (so |really )?(hard|tough|difficult|frustrating|annoying|stressful|exhausting)\b"#,
            #"\bstruggl(e|ing|ed)\b"#,
            #"\bstruggle (with|to)\b"#,
            #"\bthe problem (is|with)\b"#,
            #"\bcan('| )?t (seem to |ever )?(find|get|sleep|focus|keep)\b"#,
            #"\bnever (enough|have time|seems to)\b"#,
            #"\b(no|not enough) (time|sleep|energy)\b"#,
            #"\bwaste (of |so much )?(time|money)\b"#,
            #"\bsick and tired\b"#,
        ])

        // SOLUTION — the product / service positioned as the answer.
        groups[.solution] = compile([
            #"\bthat('s| is) (why|where)\b"#,
            #"\bintroducing\b"#,
            #"\bmeet \w+"#,
            #"\bthe (perfect|ultimate|best) (solution|way|tool|app)\b"#,
            #"\bhelps you\b"#,
            #"\bmakes it (easy|simple)\b"#,
            #"\bdesigned to\b"#,
            #"\b(my|our|the) (favorite|go-to) \w+"#,
            #"\bswitch(ed)? to\b"#,
            #"\bi (use|recommend|love)\b"#,
        ])

        // EVIDENCE — proof: stats, testimonials, credentials, rankings.
        groups[.evidence] = compile([
            #"\b\d+ (percent|%)\b"#,
            #"\bclinically (proven|tested|shown)\b"#,
            #"\bbacked by (science|research|studies)\b"#,
            #"\b(studies|research) show(s|ed)?\b"#,
            #"\b(thousands|millions) of (people|customers|users|happy)\b"#,
            #"\b(five|5)[ -]star\b"#,
            #"\brated (number|#)? ?(one|1)\b"#,
            #"\b(award|nobel|peer)[ -]?(winning|reviewed)\b"#,
            #"\btrusted by\b"#,
            #"\b\d+ ?(million|thousand) (downloads|customers|users|reviews)\b"#,
        ])

        // OFFER — the deal: price, discount, free trial, guarantee.
        groups[.offer] = compile([
            #"\b\d+ ?(percent|%) off\b"#,
            #"\bfree trial\b"#,
            #"\bfirst (month|order) free\b"#,
            #"\bmoney[ -]back guarantee\b"#,
            #"\brisk[ -]free\b"#,
            #"\b(limited|special|exclusive) (time )?offer\b"#,
            #"\bsave (up to )?\$?\d+"#,
            #"\bfor (just|only) \$?\d+"#,
            #"\bno (commitment|contract|hidden fees)\b"#,
            #"\bfree shipping\b"#,
        ])

        // CTA — the call to action.
        groups[.cta] = compile([
            #"\buse (the )?(promo |discount )?code \w+"#,
            #"\b(promo|discount|coupon) code \w+"#,
            #"\bcode \w+ at checkout\b"#,
            #"\b(go|head) (to|over to)\b"#,
            #"\bvisit \w+"#,
            #"\bcheck out \w+"#,
            #"\bsign up (today|now)\b"#,
            #"\b(click|tap) the link\b"#,
            #"\blink in (the )?(description|show notes|bio)\b"#,
            #"\b\w+ ?\.(com|net|org|io|co|app|fm|tv)\b"#,
            #"\b\w+ dot com\b"#,
        ])

        return groups
    }

    /// Compile pattern strings into `NSRegularExpression`s. Patterns that fail
    /// to compile are skipped (compile-time-safe; the curated set is valid).
    private static func compile(_ patterns: [String]) -> [NSRegularExpression] {
        patterns.compactMap {
            try? NSRegularExpression(pattern: $0, options: [.caseInsensitive])
        }
    }
}
