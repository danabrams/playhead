// ShowMusicBedProfileEvaluator.swift
// playhead-2hpn (Plan §6 Phase 3 deliverable 4): pure helpers for the
// scoped-music-bed-generalization feature.
//
// Two responsibilities, both pure (no SwiftData, no actors):
//
//   1. `extractEpisodeJingleHashes(...)` — given an episode's persisted
//      `FeatureWindow`s + episode duration, produce a 64-bit perceptual
//      hash for the first `jingleSliceSeconds` (~10 s) and the last
//      `jingleSliceSeconds` (~10 s). Uses the existing in-tree
//      `RepeatedAdFingerprint.from(featureWindows:)` primitive — the
//      same audio-derived hash the `RepeatedAdCache` uses. This is the
//      "audio hash with Hamming-distance match" primitive the bead
//      spec calls "chromaprint-equivalent"; a literal chromaprint
//      dependency was declined per the same dependency-policy rationale
//      documented in `RepeatedAdFingerprint.swift`.
//
//   2. `apply(...)` — given the previous profile state for a show and
//      one episode's outcome, compute the next profile state. Captures
//      the bead-mandated transition rules in one auditable place:
//      • match (Hamming distance ≤ matchThreshold) advances
//        `confirmationCount` and resets `consecutiveMissCount`;
//      • no match advances `consecutiveMissCount`;
//      • 30 consecutive misses reset the profile;
//      • new unique-ish hashes accumulate up to `maxStoredHashes`
//        (FIFO eviction).
//
// Tests can exercise each rule on synthetic snapshots without ever
// touching SwiftData; the store wires the two together.

import Foundation

enum ShowMusicBedProfileEvaluator {

    // MARK: - Constants

    /// Per-cue duration cap from the bead spec ("duration ≤ 10 seconds
    /// per cue"). The episode-start slice is `[0, jingleSliceSeconds)`;
    /// the episode-end slice is `[duration - jingleSliceSeconds, duration)`.
    static let jingleSliceSeconds: Double = 10.0

    /// Hamming-distance ceiling for two 64-bit hashes to "match". Bead
    /// spec: ≤ 8 bit-diff. Equivalent to ≈12.5% bit-error tolerance over
    /// 64 bits — looser than `RepeatedAdCache`'s 3/64 ad-replay match
    /// because intro jingles ARE the same audio (and so should match
    /// tighter), but allow for episode-loudness drift + ASR-pipeline
    /// re-normalisation of feature windows across recordings.
    static let matchThreshold: Int = 8

    // MARK: - Hash extraction

    /// Outcome bundle the store consumes: the two edge hashes for one
    /// episode. Pure value type — no SwiftData, no actor.
    typealias EpisodeOutcome = ShowMusicBedEpisodeOutcome

    /// Produces the (start, end) jingle hashes for an episode.
    ///
    /// Both slices are extracted from the same persisted
    /// `[FeatureWindow]` the rest of the backfill pipeline already has,
    /// so we do NOT re-decode audio. When `episodeDuration` is too
    /// short to contain two non-overlapping `jingleSliceSeconds`
    /// slices the end slice is suppressed (returned as `.zero`).
    /// `featureWindows` are filtered to those overlapping each slice;
    /// empty filtered sets emit `.zero` for that edge.
    ///
    /// Determinism: `RepeatedAdFingerprint.from(featureWindows:)` is a
    /// pure function over the window vector, so the same input always
    /// produces the same hash bit pattern.
    static func extractEpisodeJingleHashes(
        featureWindows: [FeatureWindow],
        episodeDuration: Double
    ) -> EpisodeOutcome {
        guard episodeDuration > 0, !featureWindows.isEmpty else {
            return EpisodeOutcome(startHash: .zero, endHash: .zero)
        }

        let startSliceEnd = jingleSliceSeconds
        let startWindows = featureWindows.filter { fw in
            fw.startTime < startSliceEnd && fw.endTime > 0
        }
        let startHash = RepeatedAdFingerprint.from(featureWindows: startWindows)

        // End slice only when the episode is long enough to host a
        // non-overlapping pair. For ultra-short episodes (e.g. teasers)
        // we emit `.zero` for the end so the matching logic does not
        // double-count the same audio as both intro and outro.
        let endHash: RepeatedAdFingerprint
        if episodeDuration >= 2 * jingleSliceSeconds {
            let endSliceStart = episodeDuration - jingleSliceSeconds
            let endWindows = featureWindows.filter { fw in
                fw.startTime < episodeDuration && fw.endTime > endSliceStart
            }
            endHash = RepeatedAdFingerprint.from(featureWindows: endWindows)
        } else {
            endHash = .zero
        }

        return EpisodeOutcome(startHash: startHash, endHash: endHash)
    }

    // MARK: - Span / jingle-slice overlap

    /// Returns `true` when the half-open span `[spanStart, spanEnd)`
    /// overlaps EITHER the intro slice `[0, jingleSliceSeconds)` OR the
    /// outro slice `[episodeDuration - jingleSliceSeconds, episodeDuration)`.
    ///
    /// Strict less-than at both upper bounds keeps the comparison
    /// consistent with `extractEpisodeJingleHashes`'s slice-window filter
    /// (`fw.startTime < startSliceEnd`) so the boost path and the hash
    /// path agree on what "inside the jingle region" means.
    ///
    /// Outro suppression — symmetric with `extractEpisodeJingleHashes`:
    /// the extractor emits `.zero` for the end hash whenever
    /// `episodeDuration < 2 * jingleSliceSeconds` so the intro and outro
    /// slices cannot overlap and a short episode does not double-count
    /// the same audio in both. We MUST gate the outro overlap on the
    /// same predicate, or — for 10 s ≤ duration < 20 s episodes —
    /// `spanOverlapsJingleRegion` would claim a real outro overlap
    /// while the show profile has no corresponding outro hash to match
    /// against. R8 adversarial probe #12 fix: previously this branch
    /// used `outroStart > 0` (duration > 10), which left a silent gap
    /// where any 10–20 s span on a confirmed show received the 0.25
    /// boost from "outro overlap" even though no outro hash exists. The
    /// new predicate matches the extractor exactly.
    static func spanOverlapsJingleRegion(
        spanStart: Double,
        spanEnd: Double,
        episodeDuration: Double
    ) -> Bool {
        guard episodeDuration > 0 else { return false }
        let slice = jingleSliceSeconds
        let overlapsIntro = spanStart < slice && spanEnd > 0
        // Symmetric with `extractEpisodeJingleHashes`: outro only when
        // the episode is long enough to host non-overlapping intro/outro
        // slices. Same `>= 2 * jingleSliceSeconds` gate the extractor uses.
        let outroStart = episodeDuration - slice
        let overlapsOutro = (episodeDuration >= 2 * slice)
            && spanStart < episodeDuration
            && spanEnd > outroStart
        return overlapsIntro || overlapsOutro
    }

    // MARK: - State transition (pure)

    /// Result of applying one episode's outcome to the previous profile
    /// state. Pure value type — the store writes these fields back onto
    /// the SwiftData row.
    struct Mutation: Sendable, Equatable {
        let confirmedHashes: [RepeatedAdFingerprint]
        let confirmationCount: Int
        let consecutiveMissCount: Int
        let matched: Bool
    }

    /// Apply one episode's outcome to the prior profile snapshot.
    ///
    /// Match rule: if EITHER `startHash` or `endHash` is within
    /// `matchThreshold` Hamming bits of ANY stored hash, that episode
    /// "matches". Non-zero hashes are always recorded into the store
    /// (FIFO-bounded), so the first observation of a show starts
    /// accumulating immediately — a "match" on episode #1 is impossible
    /// (empty set) but the hashes are still recorded for episode #2 to
    /// compare against.
    ///
    /// Eviction: when `consecutiveMissCount` reaches
    /// `ShowMusicBedProfile.evictionThreshold` (30), the profile is
    /// reset: hashes cleared, counts zeroed. Next observation starts
    /// fresh.
    ///
    /// - Parameter showIdentifier: present in the signature for
    ///   logging / future per-show heuristics; not used in the math.
    ///   Swift does not warn on unused function parameters, so no
    ///   discard line is needed — the parameter is intentionally kept
    ///   in the signature as a contract / breadcrumb for future
    ///   per-show heuristics + diagnostics. (R7 cleanup: dropped the
    ///   `_ = showIdentifier` cargo-cult discard.)
    static func apply(
        outcome: EpisodeOutcome,
        toShowIdentifier showIdentifier: String,
        confirmedHashes: [RepeatedAdFingerprint],
        confirmationCount: Int,
        consecutiveMissCount: Int
    ) -> Mutation {
        // Detect a match against the previously stored hash set. Zero
        // hashes (sentinel for "no derivable signal") never match —
        // they would otherwise collide with every other zero.
        let candidateHashes = [outcome.startHash, outcome.endHash].filter { !$0.isZero }
        let matched = candidateHashes.contains { candidate in
            confirmedHashes.contains { stored in
                stored.hammingDistance(to: candidate) <= matchThreshold
            }
        }

        // Build the next hash set: keep existing, then append any
        // non-zero candidate that is not already represented (i.e. not
        // within matchThreshold of an existing entry). Cap to
        // maxStoredHashes via FIFO eviction.
        var nextHashes = confirmedHashes
        for candidate in candidateHashes {
            let alreadyKnown = nextHashes.contains { stored in
                stored.hammingDistance(to: candidate) <= matchThreshold
            }
            if !alreadyKnown {
                nextHashes.append(candidate)
            }
        }
        if nextHashes.count > ShowMusicBedProfile.maxStoredHashes {
            // FIFO: drop oldest entries.
            let overflow = nextHashes.count - ShowMusicBedProfile.maxStoredHashes
            nextHashes.removeFirst(overflow)
        }

        let nextConfirmation: Int
        let nextMiss: Int
        if matched {
            nextConfirmation = confirmationCount + 1
            nextMiss = 0
        } else {
            nextConfirmation = confirmationCount
            nextMiss = consecutiveMissCount + 1
        }

        // Eviction rule: 30 consecutive misses → reset the profile.
        if nextMiss >= ShowMusicBedProfile.evictionThreshold {
            return Mutation(
                confirmedHashes: [],
                confirmationCount: 0,
                consecutiveMissCount: 0,
                matched: false
            )
        }

        return Mutation(
            confirmedHashes: nextHashes,
            confirmationCount: nextConfirmation,
            consecutiveMissCount: nextMiss,
            matched: matched
        )
    }
}
