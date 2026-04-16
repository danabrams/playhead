// ShowTraitProfile.swift
// playhead-ef2.5.1: Continuous show characterization via trait vectors.
//
// Replaces hard archetypes with a 7-dimensional continuous trait space.
// Computed after 2-3 episodes, updated incrementally with each new episode
// via exponential moving average (alpha = 0.3).
//
// Downstream consumer guidance (wired in ef2.5.3):
//   • musicDensity + structureRegularity → musicBracketTrust default
//   • singleSpeakerDominance + low musicDensity → FM gets more budget
//   • structureRegularity → metadata trust expectations
//   • insertionVolatility → fingerprint transfer confidence

import Foundation

// MARK: - EpisodeTraitSnapshot

/// Per-episode trait measurements fed into the running ShowTraitProfile.
/// Each field mirrors the corresponding ShowTraitProfile dimension but
/// captures a single episode's observation. All [0,1] fields are clamped
/// at init; `speakerTurnRate` is clamped to non-negative.
struct EpisodeTraitSnapshot: Sendable, Codable, Equatable {
    /// Fraction of episode audio classified as music (0-1).
    let musicDensity: Float
    /// Speaker turns per minute observed in this episode.
    let speakerTurnRate: Float
    /// Fraction of episode dominated by the primary speaker (0-1).
    let singleSpeakerDominance: Float
    /// How predictable segment boundaries were in this episode (0-1).
    let structureRegularity: Float
    /// Whether known sponsors recurred from previous episodes (0-1).
    let sponsorRecurrence: Float
    /// How much ad insertion points deviated from prior episodes (0-1).
    let insertionVolatility: Float
    /// ASR transcript quality estimate for this episode (0-1).
    let transcriptReliability: Float

    init(
        musicDensity: Float,
        speakerTurnRate: Float,
        singleSpeakerDominance: Float,
        structureRegularity: Float,
        sponsorRecurrence: Float,
        insertionVolatility: Float,
        transcriptReliability: Float
    ) {
        self.musicDensity = min(max(musicDensity, 0), 1)
        self.speakerTurnRate = max(speakerTurnRate, 0)
        self.singleSpeakerDominance = min(max(singleSpeakerDominance, 0), 1)
        self.structureRegularity = min(max(structureRegularity, 0), 1)
        self.sponsorRecurrence = min(max(sponsorRecurrence, 0), 1)
        self.insertionVolatility = min(max(insertionVolatility, 0), 1)
        self.transcriptReliability = min(max(transcriptReliability, 0), 1)
    }
}

// MARK: - ShowTraitProfile

/// Continuous 7-dimensional trait vector characterizing a podcast show.
///
/// All trait values are in [0, 1] except `speakerTurnRate` which is
/// unbounded (turns per minute). A fresh profile starts at `unknown`
/// (all traits 0.5, maximum uncertainty) and converges as episodes are
/// observed.
struct ShowTraitProfile: Sendable, Codable, Equatable {

    // MARK: Trait dimensions

    /// How music-heavy the show is (0 = no music, 1 = wall-to-wall music).
    let musicDensity: Float
    /// Speaker turns per minute (unbounded; higher = more conversational).
    let speakerTurnRate: Float
    /// How much one speaker dominates (0 = balanced, 1 = monologue).
    let singleSpeakerDominance: Float
    /// How predictable the show structure is (0 = chaotic, 1 = rigid format).
    let structureRegularity: Float
    /// How often the same sponsors appear across episodes (0 = never, 1 = always).
    let sponsorRecurrence: Float
    /// How much ad insertion points vary across episodes (0 = fixed slots, 1 = random).
    let insertionVolatility: Float
    /// ASR transcript quality estimate (0 = unusable, 1 = broadcast-quality).
    let transcriptReliability: Float

    /// Number of episodes that have contributed to this profile.
    let episodesObserved: Int

    // MARK: Reliability gate

    /// True when enough episodes have been observed for the profile to be
    /// considered reliable. Downstream consumers should fall back to
    /// conservative defaults when this is false.
    var isReliable: Bool { episodesObserved >= 3 }

    // MARK: Sentinel

    /// Maximum-uncertainty default: all traits at 0.5, zero episodes observed.
    static let unknown = ShowTraitProfile(
        musicDensity: 0.5,
        speakerTurnRate: 0.5,
        singleSpeakerDominance: 0.5,
        structureRegularity: 0.5,
        sponsorRecurrence: 0.5,
        insertionVolatility: 0.5,
        transcriptReliability: 0.5,
        episodesObserved: 0
    )

    // MARK: EMA update

    /// EMA smoothing factor. 0.3 weights recent episodes ~30%, giving a
    /// responsive-but-stable profile that converges after ~5-7 episodes.
    static let emaAlpha: Float = 0.3

    /// Returns a new profile incorporating measurements from `episode`.
    ///
    /// For the first episode, the snapshot replaces the unknown sentinel
    /// directly. For subsequent episodes, each trait is updated via
    /// exponential moving average: `new = α * episode + (1 - α) * current`.
    func updated(from episode: EpisodeTraitSnapshot) -> ShowTraitProfile {
        let newCount = episodesObserved + 1

        // First episode: snapshot becomes the profile directly — no EMA
        // blending against the 0.5 sentinel, which would dilute real signal.
        if episodesObserved == 0 {
            return ShowTraitProfile(
                musicDensity: episode.musicDensity,
                speakerTurnRate: episode.speakerTurnRate,
                singleSpeakerDominance: episode.singleSpeakerDominance,
                structureRegularity: episode.structureRegularity,
                sponsorRecurrence: episode.sponsorRecurrence,
                insertionVolatility: episode.insertionVolatility,
                transcriptReliability: episode.transcriptReliability,
                episodesObserved: newCount
            )
        }

        let α = Self.emaAlpha
        return ShowTraitProfile(
            musicDensity: ema(current: musicDensity, new: episode.musicDensity, alpha: α),
            speakerTurnRate: ema(current: speakerTurnRate, new: episode.speakerTurnRate, alpha: α),
            singleSpeakerDominance: ema(current: singleSpeakerDominance, new: episode.singleSpeakerDominance, alpha: α),
            structureRegularity: ema(current: structureRegularity, new: episode.structureRegularity, alpha: α),
            sponsorRecurrence: ema(current: sponsorRecurrence, new: episode.sponsorRecurrence, alpha: α),
            insertionVolatility: ema(current: insertionVolatility, new: episode.insertionVolatility, alpha: α),
            transcriptReliability: ema(current: transcriptReliability, new: episode.transcriptReliability, alpha: α),
            episodesObserved: newCount
        )
    }

    // MARK: Debug archetype label

    /// Human-readable label for logging/QA based on dominant trait thresholds.
    /// Returns `nil` when the profile is too uncertain to label.
    var debugArchetypeLabel: String? {
        guard episodesObserved >= 1 else { return nil }

        var labels: [String] = []

        // Music characterization
        if musicDensity > 0.6 {
            labels.append("music-heavy")
        }

        // Speaker pattern
        if singleSpeakerDominance > 0.7 {
            labels.append("monologue")
        } else if speakerTurnRate > 8.0 {
            labels.append("rapid-exchange")
        } else if singleSpeakerDominance < 0.4 && speakerTurnRate > 3.0 {
            labels.append("interview")
        }

        // Structure
        if structureRegularity > 0.7 {
            labels.append("structured")
        } else if structureRegularity < 0.3 {
            labels.append("freeform")
        }

        // Content type heuristics
        if structureRegularity > 0.7 && speakerTurnRate < 2.0 && singleSpeakerDominance > 0.6 {
            labels.append("news")
        }

        if labels.isEmpty {
            return "general"
        }
        return labels.joined(separator: " ")
    }

    // MARK: Private

    private func ema(current: Float, new: Float, alpha: Float) -> Float {
        alpha * new + (1.0 - alpha) * current
    }
}
