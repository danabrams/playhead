// EpisodeTraitSnapshotBuilder.swift
// playhead-v7v8: Production producer that derives an `EpisodeTraitSnapshot`
// from the live signals available at the end of `runBackfill` (just before
// `updatePriors`). Consumed inside `updatePriors`'s atomic `mutateProfile`
// closure to advance `PodcastProfile.traitProfileJSON` via the existing
// `ShowTraitProfile.updated(from:)` EMA path.
//
// Design notes:
//   • Pure / static. No I/O. The closure that calls this runs inside the
//     `AnalysisStore` actor; capturing `self` would require an actor hop
//     that this whole pipeline avoids.
//   • Defensive defaults. Each derivation has a "no-signal" branch that
//     returns the [0,1] mid-point (0.5 for unknown traits) or 0 for
//     additive signals like musicDensity. This keeps the resulting
//     snapshot benign — fed through EMA, it pulls the running profile
//     toward neutrality rather than corrupting it.
//   • Field semantics intentionally mirror `ShowTraitProfile` so the EMA
//     update is a one-line absorb: `profile.updated(from: snapshot)`.
//   • The bead's signal mix calls out musicDensity, speakerTurnRate,
//     structureRegularity. We additionally derive singleSpeakerDominance,
//     sponsorRecurrence, insertionVolatility (= 1 - structureRegularity),
//     and transcriptReliability so the resulting snapshot covers every
//     `EpisodeTraitSnapshot` field — the EMA path already accepts the
//     full vector and resolver maps over multiple traits.
//
// Why insertionVolatility = 1 - structureRegularity (not an independent
// signal)? In `runBackfill`'s scope, the only insertion-position signal we
// have is *this* episode's confirmed-ad slot positions vs. the persisted
// `normalizedAdSlotPriors`. That same comparison is what makes
// `structureRegularity` meaningful, so making the two derivations
// orthogonal would be cargo-cult. The EMA path will still smooth them
// toward whatever the cross-episode reality is.

import Foundation

enum EpisodeTraitSnapshotBuilder {

    /// Build a per-episode trait snapshot from the live signal observed
    /// during backfill. The result is directly consumable by
    /// `ShowTraitProfile.updated(from:)`.
    ///
    /// - Parameters:
    ///   - featureWindows: Per-window acoustic features for the episode.
    ///   - chunks: Final-pass transcript chunks (preferred when available).
    ///   - confirmedAdWindows: Non-suppressed ad windows produced by fusion.
    ///   - existingProfile: Persisted profile, used for cross-episode
    ///     comparisons (sponsor lexicon, normalized ad-slot priors).
    ///     `nil` for first-touch episodes — those derivations fall back
    ///     to neutral defaults.
    ///   - episodeDuration: Total episode duration in seconds.
    static func build(
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk],
        confirmedAdWindows: [AdWindow],
        existingProfile: PodcastProfile?,
        episodeDuration: Double
    ) -> EpisodeTraitSnapshot {
        let musicDensity = deriveMusicDensity(featureWindows: featureWindows)
        let (speakerTurnRate, singleSpeakerDominance) = deriveSpeakerSignals(
            chunks: chunks,
            episodeDuration: episodeDuration
        )
        let structureRegularity = deriveStructureRegularity(
            confirmedAdWindows: confirmedAdWindows,
            existingProfile: existingProfile,
            episodeDuration: episodeDuration
        )
        let sponsorRecurrence = deriveSponsorRecurrence(
            confirmedAdWindows: confirmedAdWindows,
            existingProfile: existingProfile
        )
        let insertionVolatility = 1.0 - structureRegularity
        let transcriptReliability: Float = 0.7

        return EpisodeTraitSnapshot(
            musicDensity: musicDensity,
            speakerTurnRate: speakerTurnRate,
            singleSpeakerDominance: singleSpeakerDominance,
            structureRegularity: structureRegularity,
            sponsorRecurrence: sponsorRecurrence,
            insertionVolatility: insertionVolatility,
            transcriptReliability: transcriptReliability
        )
    }

    // MARK: - Derivations

    /// Mean of `musicProbability` across feature windows, clamped to [0,1].
    /// Returns 0 when no windows are available — a fresh show with no
    /// acoustic features should not pretend to have music signal.
    static func deriveMusicDensity(featureWindows: [FeatureWindow]) -> Float {
        guard !featureWindows.isEmpty else { return 0 }
        var sum: Double = 0
        for w in featureWindows {
            sum += min(max(w.musicProbability, 0), 1)
        }
        let mean = sum / Double(featureWindows.count)
        return Float(mean)
    }

    /// Returns (turnsPerMinute, dominantSpeakerFraction).
    ///
    ///   • turnsPerMinute: count of consecutive `speakerId` transitions
    ///     in `chunks` divided by `episodeDuration / 60`. Chunks without
    ///     `speakerId` are skipped, so a chunk run with all-nil ids
    ///     yields 0 turns.
    ///   • dominantSpeakerFraction: count of chunks held by the most
    ///     frequent `speakerId` divided by total chunks with a speaker
    ///     id. Returns 0.5 (maximum uncertainty) when no chunks carry a
    ///     speaker id at all.
    static func deriveSpeakerSignals(
        chunks: [TranscriptChunk],
        episodeDuration: Double
    ) -> (turnsPerMinute: Float, dominantFraction: Float) {
        let speakerIds = chunks.compactMap(\.speakerId)
        guard !speakerIds.isEmpty else {
            return (0, 0.5)
        }

        // Count consecutive transitions in chunk order.
        var transitions = 0
        var previous: Int?
        for id in speakerIds {
            defer { previous = id }
            if let p = previous, p != id {
                transitions += 1
            }
        }

        let turnsPerMinute: Float
        if episodeDuration > 0 {
            let minutes = episodeDuration / 60.0
            turnsPerMinute = Float(Double(transitions) / minutes)
        } else {
            turnsPerMinute = 0
        }

        // Dominant-speaker fraction.
        var counts: [Int: Int] = [:]
        for id in speakerIds {
            counts[id, default: 0] += 1
        }
        let maxCount = counts.values.max() ?? 0
        let dominantFraction = Float(maxCount) / Float(speakerIds.count)

        return (turnsPerMinute, dominantFraction)
    }

    /// How tightly this episode's confirmed ad-slot positions match the
    /// persisted `normalizedAdSlotPriors`. The metric is bounded [0,1]:
    ///   • For each new slot, find the nearest persisted slot and
    ///     compute `1 - min(distance / windowRadius, 1)`.
    ///   • Average those per-slot scores.
    /// `windowRadius` is 0.15 (= 15% of episode duration). Slots within
    /// 0.05 of a prior land at score >= 0.66; slots more than 0.15 away
    /// score 0.
    /// Returns 0.5 when either the episode has no confirmed ads or the
    /// persisted profile carries no decoded slot priors — neutral
    /// EMA contribution rather than misleading penalty.
    static func deriveStructureRegularity(
        confirmedAdWindows: [AdWindow],
        existingProfile: PodcastProfile?,
        episodeDuration: Double
    ) -> Float {
        guard episodeDuration > 0,
              !confirmedAdWindows.isEmpty,
              let priors = decodeSlotPriors(existingProfile?.normalizedAdSlotPriors),
              !priors.isEmpty
        else {
            return 0.5
        }

        let windowRadius = 0.15
        let observed: [Double] = confirmedAdWindows.map { w in
            let center = (w.startTime + w.endTime) / 2.0
            return min(max(center / episodeDuration, 0), 1)
        }
        var scoreSum: Double = 0
        for slot in observed {
            let nearest = priors.map { abs($0 - slot) }.min() ?? windowRadius
            let normalized = min(nearest / windowRadius, 1.0)
            scoreSum += (1.0 - normalized)
        }
        let mean = scoreSum / Double(observed.count)
        return Float(min(max(mean, 0), 1))
    }

    /// Fraction of this episode's tagged advertisers that already appear
    /// in the persisted `sponsorLexicon` (case-insensitive). Returns 0
    /// when no advertisers were tagged this episode (a common case for
    /// shows where the metadata extractor produced no advertiser names),
    /// and 0 when the existing profile has no lexicon to compare against.
    static func deriveSponsorRecurrence(
        confirmedAdWindows: [AdWindow],
        existingProfile: PodcastProfile?
    ) -> Float {
        let advertisers = confirmedAdWindows
            .compactMap(\.advertiser)
            .map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
            .filter { !$0.isEmpty }
        guard !advertisers.isEmpty else { return 0 }

        let lexicon: Set<String>
        if let raw = existingProfile?.sponsorLexicon {
            lexicon = Set(
                raw.components(separatedBy: ",")
                    .map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
                    .filter { !$0.isEmpty }
            )
        } else {
            lexicon = []
        }
        guard !lexicon.isEmpty else { return 0 }

        let recurring = advertisers.filter { lexicon.contains($0) }.count
        return Float(recurring) / Float(advertisers.count)
    }

    // MARK: - Helpers

    private static func decodeSlotPriors(_ json: String?) -> [Double]? {
        guard let json,
              let data = json.data(using: .utf8),
              let decoded = try? JSONDecoder().decode([Double].self, from: data)
        else { return nil }
        return decoded
    }
}
