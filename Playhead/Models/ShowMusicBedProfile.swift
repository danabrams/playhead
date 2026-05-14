// ShowMusicBedProfile.swift
// playhead-2hpn (Plan §6 Phase 3 deliverable 4): per-show jingle profile
// used by the "scoped music-bed generalization" feature to promote
// music-bed signal weight in fusion ONLY on shows where a recurring
// intro/outro jingle has been observed across multiple episodes.
//
// Scoping (per bead spec):
//   * Scope bound: intro/outro jingles + recurring music-bed cues within
//     a single subscription; per-cue duration ≤ 10 seconds.
//   * "Confirmed" state requires ≥ 3 episodes of the same show with
//     matching hashes (Hamming distance ≤ 8) at episode-start or
//     episode-end.
//   * Profile is RESET when 30 consecutive episodes of the show show no
//     match — guards against carrying stale fingerprints when a show
//     swaps its intro music.
//
// Hash storage: 64-bit perceptual hashes from `RepeatedAdFingerprint`
// (already in-tree). Bit-packed `UInt64`s would be lossless but
// SwiftData's @Model property storage is friendlier to common types;
// `Int64` carries the same bit pattern losslessly (UInt64 → Int64 via
// `Int64(bitPattern:)`) and is the same pattern used elsewhere
// (`FeedDescriptionMetadata.SourceHashes`).
//
// `versionStamp` is the bead-mandated schema-revision field. Code that
// reads a profile written by an earlier evaluator version may decide to
// discard or migrate; today we always write `currentVersionStamp` and
// the consumer compares as an additive guard.
//
// Concurrency: SwiftData @Model is main-actor-friendly; mutations of
// the profile happen inside a `ShowMusicBedProfileStore` actor that
// owns the ModelContext, so the model itself is touched only through
// that store.

import Foundation
import SwiftData

@Model
final class ShowMusicBedProfile {
    /// Canonical podcast identifier matching the show-identity used by
    /// `AdDetectionService.runBackfill(podcastId:)` and the catalog's
    /// per-show scoping (`catalogShowId`). Today this is the podcast
    /// feed URL's string form (the same value `Podcast.feedURL.absoluteString`
    /// returns); future renames flow through `Podcast.feedURL`.
    /// Unique — one profile per show.
    #Unique<ShowMusicBedProfile>([\.showIdentifier])
    var showIdentifier: String

    /// Stored bit-patterns of 64-bit perceptual hashes
    /// (`RepeatedAdFingerprint.bits`). Encoded as `Int64` so SwiftData's
    /// underlying SQLite column type matches what the rest of the
    /// codebase already does for UInt64 storage
    /// (`FeedDescriptionMetadata.SourceHashes`). Reads round-trip
    /// losslessly via `UInt64(bitPattern:)`.
    ///
    /// At most `Self.maxStoredHashes` entries — older entries are
    /// evicted FIFO to bound storage.
    var confirmedJingleHashBits: [Int64]

    /// Total number of episodes that have contributed a matching hash
    /// to this profile. Becomes "confirmed" at ≥ `confirmationThreshold`.
    /// Distinct from `confirmedJingleHashBits.count` — a single hash
    /// may be reinforced by multiple episodes.
    var confirmationCount: Int

    /// Number of consecutive episodes (most recent first) of this show
    /// for which neither the episode-start nor the episode-end slice
    /// matched any stored hash. Reaches `evictionThreshold` → the
    /// profile is reset.
    var consecutiveMissCount: Int

    /// Monotonically incremented version stamp written by the evaluator.
    /// Used by future migrations: a consumer that reads a profile with
    /// an older stamp can either accept (additive change) or discard
    /// (breaking change). Today only `currentVersionStamp` is written.
    var versionStamp: Int

    var createdAt: Date
    var updatedAt: Date

    /// Threshold required to flip the profile into "confirmed" state.
    /// Bead spec: ≥ 3 distinct episodes of the same show with matching
    /// hashes. We check this against `confirmationCount` which advances
    /// once per matching episode.
    static let confirmationThreshold: Int = 3

    /// Consecutive-miss threshold above which the profile is reset.
    /// Bead spec: 30 consecutive episodes with no match → profile evicted.
    static let evictionThreshold: Int = 30

    /// Hard cap on stored hashes. Keeps a single show's profile bounded
    /// in storage even if the evaluator records many distinct
    /// near-matches over time. FIFO eviction.
    static let maxStoredHashes: Int = 16

    /// Schema-revision stamp written on every profile mutation. Bump
    /// when the evaluator's hash-extraction or matching rule changes
    /// in a way that invalidates older stored hashes.
    static let currentVersionStamp: Int = 1

    init(
        showIdentifier: String,
        confirmedJingleHashBits: [Int64] = [],
        confirmationCount: Int = 0,
        consecutiveMissCount: Int = 0,
        versionStamp: Int = ShowMusicBedProfile.currentVersionStamp,
        createdAt: Date = .now,
        updatedAt: Date = .now
    ) {
        self.showIdentifier = showIdentifier
        self.confirmedJingleHashBits = confirmedJingleHashBits
        self.confirmationCount = confirmationCount
        self.consecutiveMissCount = consecutiveMissCount
        self.versionStamp = versionStamp
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }

    /// Convenience: true when `confirmationCount` has reached the bead-
    /// mandated 3-episode threshold AND we still hold at least one
    /// stored hash to match against. The store's eviction path zeroes
    /// the count when it resets the profile.
    var isConfirmed: Bool {
        confirmationCount >= Self.confirmationThreshold && !confirmedJingleHashBits.isEmpty
    }
}
