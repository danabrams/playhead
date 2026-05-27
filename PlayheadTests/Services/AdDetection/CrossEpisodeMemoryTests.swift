// CrossEpisodeMemoryTests.swift
// playhead-xsdz.9: Hermetic unit tests for the cross-episode "memory" precision
// signal — the Smith-Waterman local aligner, the CrossEpisodeMemoryEvaluator
// (suppression + positive boost), and the NegativeFingerprintBank (SQLite,
// correction-gated writes, LRU + time-decay eviction).
//
// These are FULLY hermetic (no FM, no audio, no corpus). The bank tests use a
// temp-dir SQLite file and tear it down per test.

import Foundation
import Testing
@testable import Playhead

// MARK: - Smith-Waterman aligner

@Suite("SmithWatermanAligner")
struct SmithWatermanAlignerTests {

    @Test("Exact match scores 1.0 normalized")
    func exactMatch() {
        let seq = ["use", "code", "betterhelp", "for", "ten", "percent", "off"]
        let r = SmithWatermanAligner.align(seq, seq)
        #expect(r.normalizedScore == 1.0)
        #expect(r.rawScore == SmithWatermanAligner.Scoring.default.match * seq.count)
    }

    @Test("A single substitution aligns strongly across the whole sequence")
    func toleratesSubstitution() {
        let a = ["visit", "betterhelp", "dot", "com", "slash", "playhead", "today"]
        // One token substituted (com → calm) — an ASR slip mid-sequence.
        let b = ["visit", "betterhelp", "dot", "calm", "slash", "playhead", "today"]
        let r = SmithWatermanAligner.align(a, b)
        // 6 of 7 tokens match with one substitution (mismatch −1):
        // best = 6*match − mismatch = 6*2 − 1 = 11; normalized = 11/(2*7) ≈ 0.786.
        // Strongly aligned (the whole sequence participates) but below a perfect
        // match — the substitution is honestly penalized. On real ad copy
        // (20–100 tokens) a single substitution barely dents the score.
        #expect(r.normalizedScore > 0.70)
        #expect(r.normalizedScore < 1.0)
        #expect(r.rawScore == 11)
    }

    @Test("A single substitution in longer ad copy stays above the 0.80 match bar")
    func substitutionInLongCopyStaysHigh() {
        let a = SmithWatermanAligner.tokenize(
            "use promo code playhead at betterhelp dot com slash show for ten percent off your first month of therapy"
        )
        // One homophone substitution deep in the copy.
        let b = SmithWatermanAligner.tokenize(
            "use promo code playhead at betterhelp dot calm slash show for ten percent off your first month of therapy"
        )
        let r = SmithWatermanAligner.align(a, b)
        #expect(r.normalizedScore >= 0.80)
    }

    @Test("A single-token gap (ASR insertion) still aligns strongly")
    func toleratesGap() {
        let a = ["go", "to", "squarespace", "dot", "com", "slash", "show"]
        // An extra filler-ish token inserted mid-sequence.
        let b = ["go", "to", "squarespace", "dot", "com", "uh", "slash", "show"]
        let r = SmithWatermanAligner.align(a, b)
        #expect(r.normalizedScore >= 0.80)
    }

    @Test("Unrelated sequences score near zero")
    func unrelatedScoresLow() {
        let a = ["the", "quarterback", "threw", "a", "long", "touchdown", "pass"]
        let b = ["she", "studied", "marine", "biology", "in", "graduate", "school"]
        let r = SmithWatermanAligner.align(a, b)
        #expect(r.normalizedScore < 0.5)
    }

    @Test("Local containment: short sequence fully inside a longer one scores 1.0")
    func localContainment() {
        let short = ["promo", "code", "playhead"]
        let long = ["and", "now", "a", "word", "use", "promo", "code", "playhead", "at", "checkout"]
        let r = SmithWatermanAligner.align(short, long)
        // The shorter sequence appears verbatim inside the longer one — perfect
        // local alignment relative to the shorter length.
        #expect(r.normalizedScore == 1.0)
    }

    @Test("Empty / single-token edge cases are defined and bounded")
    func edgeCases() {
        #expect(SmithWatermanAligner.align([], ["a", "b"]).normalizedScore == 0.0)
        #expect(SmithWatermanAligner.align(["a", "b"], []).normalizedScore == 0.0)
        #expect(SmithWatermanAligner.align([], []) == .zero)
        // Single matching token: best possible for a length-1 shorter seq.
        #expect(SmithWatermanAligner.align(["x"], ["a", "x", "b"]).normalizedScore == 1.0)
        // Single non-matching token: no positive alignment.
        #expect(SmithWatermanAligner.align(["x"], ["a", "b"]).normalizedScore == 0.0)
    }

    @Test("Alignment is symmetric in argument order")
    func symmetry() {
        let a = ["alpha", "beta", "gamma", "delta", "epsilon"]
        let b = ["beta", "gamma", "zeta", "delta", "epsilon", "eta"]
        let ab = SmithWatermanAligner.align(a, b)
        let ba = SmithWatermanAligner.align(b, a)
        #expect(ab.rawScore == ba.rawScore)
        #expect(ab.normalizedScore == ba.normalizedScore)
    }

    @Test("tokenize shares MinHash normalization (lowercase, strip punctuation, drop filler)")
    func tokenizeNormalization() {
        let tokens = SmithWatermanAligner.tokenize("Use CODE: PLAYHEAD, um, at BetterHelp.com!")
        // Lowercased, punctuation stripped, filler "um" removed.
        #expect(tokens.contains("playhead"))
        #expect(tokens.contains("code"))
        #expect(!tokens.contains("um"))
        #expect(tokens.allSatisfy { $0 == $0.lowercased() })
    }
}

// MARK: - Evaluator

@Suite("CrossEpisodeMemoryEvaluator")
struct CrossEpisodeMemoryEvaluatorTests {

    private let evaluator = CrossEpisodeMemoryEvaluator()

    private func match(similarity: Double, decay: Double = 1.0) -> NegativeFingerprintMatch {
        NegativeFingerprintMatch(
            entry: NegativeFingerprintEntry(showId: "show", tokensJoined: "a b c d"),
            similarity: similarity,
            decayWeight: decay,
            effectiveStrength: similarity * decay
        )
    }

    @Test("nil match ⇒ no suppression (factor 1.0)")
    func nilMatchNoSuppression() {
        #expect(evaluator.suppressionFactor(for: nil) == 1.0)
        #expect(evaluator.suppress(skipConfidence: 0.9, with: nil) == 0.9)
    }

    @Test("Perfect negative match applies the full configured suppression")
    func fullSuppression() {
        // default maxSuppression 0.5 → factor 0.5 at strength 1.0.
        let factor = evaluator.suppressionFactor(for: match(similarity: 1.0))
        #expect(abs(factor - 0.5) < 1e-9)
        let suppressed = evaluator.suppress(skipConfidence: 0.82, with: match(similarity: 1.0))
        #expect(abs(suppressed - 0.41) < 1e-9)
    }

    @Test("Decayed match suppresses proportionally less")
    func decayReducesSuppression() {
        let fresh = evaluator.suppressionFactor(for: match(similarity: 1.0, decay: 1.0))
        let stale = evaluator.suppressionFactor(for: match(similarity: 1.0, decay: 0.2))
        #expect(stale > fresh) // less suppression ⇒ factor closer to 1.0
    }

    @Test("Suppression pulls a just-over-threshold span back below it")
    func suppressionCrossesThreshold() {
        // A span that just cleared the 0.80 auto-skip threshold.
        let before = 0.83
        let after = evaluator.suppress(skipConfidence: before, with: match(similarity: 1.0))
        #expect(before >= 0.80)
        #expect(after < 0.80)
    }

    @Test("Non-finite skipConfidence is left untouched")
    func nonFiniteUntouched() {
        let r = evaluator.suppress(skipConfidence: .nan, with: match(similarity: 1.0))
        #expect(r.isNaN)
    }

    @Test("Positive boost fires on a strong alignment, capped")
    func positiveBoostFires() {
        let candidate = ["use", "code", "playhead", "at", "betterhelp", "today"]
        let positives = [candidate] // a confirmed-ad bank repeat
        let entries = evaluator.buildPositiveBoostEntries(
            candidateTokens: candidate,
            positiveSequences: positives,
            cap: 0.2
        )
        #expect(entries.count == 1)
        #expect(entries.first?.source == .crossEpisodeMemory)
        #expect((entries.first?.weight ?? 0) <= 0.2)
        #expect((entries.first?.weight ?? 0) > 0)
    }

    @Test("Positive boost does NOT fire on an unrelated candidate")
    func positiveBoostQuietOnUnrelated() {
        let candidate = ["the", "interview", "covered", "marine", "biology", "research"]
        let positives = [["use", "code", "playhead", "at", "betterhelp", "today"]]
        let entries = evaluator.buildPositiveBoostEntries(
            candidateTokens: candidate,
            positiveSequences: positives,
            cap: 0.2
        )
        #expect(entries.isEmpty)
    }

    @Test("Positive boost is empty for empty inputs / zero cap")
    func positiveBoostEmptyInputs() {
        let c = ["a", "b", "c", "d"]
        #expect(evaluator.buildPositiveBoostEntries(candidateTokens: [], positiveSequences: [c], cap: 0.2).isEmpty)
        #expect(evaluator.buildPositiveBoostEntries(candidateTokens: c, positiveSequences: [], cap: 0.2).isEmpty)
        #expect(evaluator.buildPositiveBoostEntries(candidateTokens: c, positiveSequences: [c], cap: 0).isEmpty)
    }
}

// MARK: - Fusion branch (positive boost rides the additive ledger)

@Suite("CrossEpisodeMemory fusion branch")
struct CrossEpisodeMemoryFusionTests {

    private func makeSpan() -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: "a", firstAtomOrdinal: 1, lastAtomOrdinal: 2),
            assetId: "a",
            firstAtomOrdinal: 1,
            lastAtomOrdinal: 2,
            startTime: 10,
            endTime: 40,
            anchorProvenance: []
        )
    }

    private func fusion(
        crossEpisodeMemoryEntries: [EvidenceLedgerEntry],
        config: FusionWeightConfig = FusionWeightConfig()
    ) -> BackfillEvidenceFusion {
        BackfillEvidenceFusion(
            span: makeSpan(),
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            crossEpisodeMemoryEntries: crossEpisodeMemoryEntries,
            mode: .off,
            config: config
        )
    }

    @Test("Empty crossEpisodeMemoryEntries ⇒ no .crossEpisodeMemory entry in the ledger")
    func emptyProducesNone() {
        let ledger = fusion(crossEpisodeMemoryEntries: []).buildLedger()
        #expect(!ledger.contains { $0.source == .crossEpisodeMemory })
    }

    @Test("Populated entry ⇒ exactly one .crossEpisodeMemory entry, clamped to the cap")
    func populatedClampsToCap() {
        let cfg = FusionWeightConfig(crossEpisodeMemoryCap: 0.2)
        let input = [EvidenceLedgerEntry(
            source: .crossEpisodeMemory,
            weight: 0.9, // over cap — must clamp
            detail: .fingerprint(matchCount: 1, averageSimilarity: 0.95)
        )]
        let ledger = fusion(crossEpisodeMemoryEntries: input, config: cfg).buildLedger()
        let entries = ledger.filter { $0.source == .crossEpisodeMemory }
        #expect(entries.count == 1)
        #expect(entries.first?.weight == 0.2)
    }

    @Test("crossEpisodeMemory is NOT observability-only (it is a fusion input)")
    func notObservabilityOnly() {
        #expect(!EvidenceSourceType.crossEpisodeMemory.isObservabilityOnly)
    }

    @Test("crossEpisodeMemory is in the reference family (cannot self-corroborate fingerprint/catalog)")
    func referenceFamily() {
        #expect(SourceEvidenceFamily.for(.crossEpisodeMemory) == .reference)
    }

    @Test("AdDetectionConfig.default keeps crossEpisodeMemoryEnabled false")
    func defaultFlagOff() {
        #expect(AdDetectionConfig.default.crossEpisodeMemoryEnabled == false)
    }

    @Test("FusionWeightConfig.default exposes a modest crossEpisodeMemoryCap (0.20)")
    func defaultCap() {
        #expect(FusionWeightConfig().crossEpisodeMemoryCap == 0.2)
    }
}

// MARK: - Negative fingerprint bank

@Suite("NegativeFingerprintBank")
struct NegativeFingerprintBankTests {

    private func makeBank() throws -> NegativeFingerprintBank {
        let dir = try makeTempDir(prefix: "negfp-bank")
        return try NegativeFingerprintBank(directoryURL: dir)
    }

    private let adCopy = "use promo code playhead at betterhelp dot com slash show for ten percent off your first month"

    @Test("Records a confirmed FP and suppresses a near-match; not an unrelated candidate")
    func recordAndMatch() async throws {
        let bank = try makeBank()
        defer { Task { await bank.close() } }

        let wrote = try await bank.recordConfirmedFalsePositive(text: adCopy, showId: "show-A")
        #expect(wrote)
        #expect(try await bank.count() == 1)

        // A re-ASR'd near-duplicate of the same copy (one word dropped, one
        // substituted) should match.
        let nearMatchText = "use promo code playhead at betterhelp dot calm slash show for ten percent off first month"
        let nearTokens = SmithWatermanAligner.tokenize(nearMatchText)
        let hit = await bank.bestMatch(candidateTokens: nearTokens, show: "show-A")
        #expect(hit != nil)
        #expect((hit?.similarity ?? 0) >= NegativeFingerprintBank.defaultMatchThreshold)

        // An unrelated candidate should NOT match.
        let unrelated = SmithWatermanAligner.tokenize(
            "today we discuss the history of jazz and its influence on modern music composition"
        )
        let miss = await bank.bestMatch(candidateTokens: unrelated, show: "show-A")
        #expect(miss == nil)
    }

    @Test("Per-show scoping: a negative on show A does not match a candidate on show B")
    func perShowScoping() async throws {
        let bank = try makeBank()
        defer { Task { await bank.close() } }

        _ = try await bank.recordConfirmedFalsePositive(text: adCopy, showId: "show-A")
        let tokens = SmithWatermanAligner.tokenize(adCopy)
        // Querying show B should not see show A's (non-global) negative.
        let miss = await bank.bestMatch(candidateTokens: tokens, show: "show-B")
        #expect(miss == nil)
        // But show A still matches.
        let hit = await bank.bestMatch(candidateTokens: tokens, show: "show-A")
        #expect(hit != nil)
    }

    @Test("Global (nil-show) negative matches a candidate on any show")
    func globalScope() async throws {
        let bank = try makeBank()
        defer { Task { await bank.close() } }

        _ = try await bank.recordConfirmedFalsePositive(text: adCopy, showId: nil)
        let tokens = SmithWatermanAligner.tokenize(adCopy)
        let hit = await bank.bestMatch(candidateTokens: tokens, show: "any-show")
        #expect(hit != nil)
        #expect(hit?.entry.showId == nil)
    }

    @Test("Short sequences are rejected (never stored)")
    func rejectsShortSequences() async throws {
        let bank = try makeBank()
        defer { Task { await bank.close() } }

        let wrote = try await bank.recordConfirmedFalsePositive(text: "buy now", showId: "show-A")
        #expect(!wrote)
        #expect(try await bank.count() == 0)
    }

    @Test("Empty bank / empty candidate ⇒ no match, no crash")
    func emptyBankEdges() async throws {
        let bank = try makeBank()
        defer { Task { await bank.close() } }
        #expect(await bank.bestMatch(candidateTokens: SmithWatermanAligner.tokenize(adCopy), show: "x") == nil)
        #expect(await bank.bestMatch(candidateTokens: [], show: "x") == nil)
    }

    @Test("Dedup: re-recording the same copy confirms in place (no duplicate row)")
    func dedupConfirmsInPlace() async throws {
        let bank = try makeBank()
        defer { Task { await bank.close() } }

        _ = try await bank.recordConfirmedFalsePositive(text: adCopy, showId: "show-A")
        _ = try await bank.recordConfirmedFalsePositive(text: adCopy, showId: "show-A")
        #expect(try await bank.count() == 1)
        let entry = try await bank.allEntries().first
        #expect((entry?.confirmationCount ?? 0) >= 2)
    }

    @Test("Time-decay weight is 1.0 fresh, decays with age, never below floor")
    func decayCurve() {
        #expect(NegativeFingerprintBank.decayWeight(ageDays: 0) == 1.0)
        let mid = NegativeFingerprintBank.decayWeight(ageDays: 60)
        #expect(mid < 1.0 && mid > NegativeFingerprintBank.decayFloor)
        #expect(NegativeFingerprintBank.decayWeight(ageDays: 100_000) == NegativeFingerprintBank.decayFloor)
    }

    @Test("Decayed entry still matches but with reduced effective strength")
    func decayedMatchWeakerStrength() async throws {
        let bank = try makeBank()
        defer { Task { await bank.close() } }

        // Record an old negative (200 days ago, past the 120-day horizon ⇒ floor).
        let old = Date().timeIntervalSince1970 - 200 * 86_400
        _ = try await bank.recordConfirmedFalsePositive(text: adCopy, showId: "show-A", recordedAt: old)
        let tokens = SmithWatermanAligner.tokenize(adCopy)
        let hit = await bank.bestMatch(candidateTokens: tokens, show: "show-A")
        let unwrapped = try #require(hit)
        #expect(unwrapped.decayWeight == NegativeFingerprintBank.decayFloor)
        #expect(unwrapped.effectiveStrength < unwrapped.similarity)
    }

    @Test("LRU eviction caps per-show rows at maxEntriesPerShow")
    func lruEviction() async throws {
        // Use a small synthetic cap-equivalent: insert maxEntriesPerShow + a few
        // DISTINCT long negatives and assert the count never exceeds the cap.
        let bank = try makeBank()
        defer { Task { await bank.close() } }

        let cap = NegativeFingerprintBank.maxEntriesPerShow
        // Insert cap + 5 distinct sequences (each unique so dedup won't collapse).
        for i in 0..<(cap + 5) {
            let text = "negative fingerprint sequence number \(i) alpha beta gamma delta epsilon"
            _ = try await bank.recordConfirmedFalsePositive(text: text, showId: "show-evict")
        }
        let count = try await bank.count()
        #expect(count == cap)
    }
}
