// ShowCapabilityProfile.swift
// playhead-h6a6 (Plan §6 Phase 3 deliverable 7): per-show capability
// profile observed by the scheduler / detection ensemble to modulate
// per-show analysis budget AFTER baseline SLIs stabilize.
//
// The profile is OBSERVED, never user-set. The Settings → Diagnostics
// row is read-only.
//
// Activation floor (enforced by `ShowCapabilityProfileEvaluator`):
//   1. ≥ 5 analysis-completed episodes for the show (`completedEpisodeCount`).
//   2. Phase-2 SLIs (playhead-d99) are within defended bounds for the
//      cohort the show maps into. The SLI gate is supplied at evaluate
//      time so this @Model carries only the raw observation counters
//      and not a snapshot of the SLI ledger.
//
// Five observed profile kinds:
//   * chapter-rich            — ≥ 80% of episodes have publisher chapters
//                                matched to detected ads.
//   * host-read-only          — ≥ 70% of episodes have host-voiced ads.
//   * music-bed-reliable      — playhead-2hpn `ShowMusicBedProfileStore`
//                                reports `isConfirmed` for the show.
//   * sponsor-declared        — ≥ 50% of episodes have RSS/show-notes
//                                pre-seed positives that resolved to ads.
//   * dynamic-insertion-heavy — ad boundaries shift episode-to-episode
//                                (high insertion volatility).
//
// Until the floor is met OR the flag is off, the profile stays
// `.unknown` and the budget modulator MUST be a no-op.
//
// V1-additive SwiftData schema: existing installs decode rows that
// don't carry this entity yet as an empty table; the evaluator
// provisions rows lazily via the store. Bumping `schemaVersion` is a
// future migration's job — this bead writes `currentSchemaVersion: 1`.
//
// Concurrency: SwiftData @Model is main-actor-friendly. Mutations
// flow exclusively through `ShowCapabilityProfileStore`, which hops
// to MainActor for every `ModelContext` operation. The model class
// itself is reachable only via the store's API.

import Foundation
import SwiftData

@Model
final class ShowCapabilityProfile {

    /// Canonical podcast identifier matching the show-identity used by
    /// `AdDetectionService.runBackfill(podcastId:)` and the catalog's
    /// per-show scoping. Today this is the podcast feed URL's string
    /// form (the same value `Podcast.feedURL.absoluteString` returns);
    /// future renames flow through `Podcast.feedURL`. Unique — one
    /// profile per show.
    #Unique<ShowCapabilityProfile>([\.showIdentifier])
    var showIdentifier: String

    /// Total number of episodes of this show that have reached an
    /// analysis-completed terminal state. The activation floor (≥ 5)
    /// is checked against this counter by
    /// `ShowCapabilityProfileEvaluator.classify(...)` reading
    /// `ShowCapabilityProfile.activationFloorEpisodeCount` (the
    /// constant lives on THIS type — see below — to keep the floor
    /// alongside the per-predicate thresholds).
    var completedEpisodeCount: Int

    /// Number of those episodes whose detected ads were matched to
    /// publisher-supplied chapter boundaries. Used by the
    /// `chapter-rich` predicate (threshold `chapterRichEpisodeRatio`).
    var chapterMatchedEpisodeCount: Int

    /// Number of those episodes whose ads were detected as
    /// host-voiced. Used by the `host-read-only` predicate (threshold
    /// `hostReadOnlyEpisodeRatio`).
    var hostVoicedEpisodeCount: Int

    /// Number of those episodes for which RSS/show-notes pre-seed
    /// produced an ad-positive that resolved to a detected ad. Used by
    /// the `sponsor-declared` predicate (threshold
    /// `sponsorDeclaredEpisodeRatio`).
    var sponsorDeclaredEpisodeCount: Int

    /// Number of those episodes whose ad boundaries shifted from the
    /// prior episode's boundaries by more than the bead's
    /// "dynamic-insertion" delta. Used by the `dynamic-insertion-heavy`
    /// predicate.
    var dynamicInsertionEpisodeCount: Int

    /// Cached profile kind raw value. Persisted so a cold launch can
    /// render the Settings → Diagnostics row without re-running the
    /// evaluator before the first backfill. Always exactly one of
    /// `ShowCapabilityProfileKind.rawValue`. Defaults to `.unknown`
    /// (no modulation) until the activation floor is reached AND the
    /// evaluator confirms a deterministic kind. Re-derived on every
    /// `recordEpisodeOutcome` call so the cached value is always
    /// authoritative as of the last write.
    var kindRawValue: String

    /// Monotonically incremented schema-revision stamp. V1 ships with
    /// `currentSchemaVersion = 1`; future migrations can decline to
    /// trust older stamps. Today only `currentSchemaVersion` is
    /// written.
    var schemaVersion: Int

    var createdAt: Date
    var updatedAt: Date

    /// Activation-floor episode count. Per the bead spec: profile is
    /// observed only after the show has ≥ 5 analysis-completed
    /// episodes. The constant lives here (alongside the per-predicate
    /// thresholds) so the floor cannot drift between the model and
    /// the evaluator.
    static let activationFloorEpisodeCount: Int = 5

    /// Per-predicate thresholds. Encoded as fractions (numerator
    /// against `completedEpisodeCount`). The bead spec values:
    ///   * chapter-rich            ≥ 80%
    ///   * host-read-only          ≥ 70%
    ///   * sponsor-declared        ≥ 50%
    ///   * dynamic-insertion-heavy ≥ 50% (boundaries shifted episode-to-episode)
    /// Music-bed-reliable does not have a ratio threshold here — it
    /// consumes the 2hpn `ShowMusicBedProfileResolving.isConfirmed`
    /// signal directly.
    static let chapterRichEpisodeRatio: Double = 0.80
    static let hostReadOnlyEpisodeRatio: Double = 0.70
    static let sponsorDeclaredEpisodeRatio: Double = 0.50
    static let dynamicInsertionEpisodeRatio: Double = 0.50

    /// Schema-revision stamp written on every profile mutation. Bump
    /// when the evaluator's predicate rules change in a way that
    /// invalidates older cached `kindRawValue` values.
    static let currentSchemaVersion: Int = 1

    init(
        showIdentifier: String,
        completedEpisodeCount: Int = 0,
        chapterMatchedEpisodeCount: Int = 0,
        hostVoicedEpisodeCount: Int = 0,
        sponsorDeclaredEpisodeCount: Int = 0,
        dynamicInsertionEpisodeCount: Int = 0,
        kindRawValue: String = ShowCapabilityProfileKind.unknown.rawValue,
        schemaVersion: Int = ShowCapabilityProfile.currentSchemaVersion,
        createdAt: Date = .now,
        updatedAt: Date = .now
    ) {
        self.showIdentifier = showIdentifier
        self.completedEpisodeCount = completedEpisodeCount
        self.chapterMatchedEpisodeCount = chapterMatchedEpisodeCount
        self.hostVoicedEpisodeCount = hostVoicedEpisodeCount
        self.sponsorDeclaredEpisodeCount = sponsorDeclaredEpisodeCount
        self.dynamicInsertionEpisodeCount = dynamicInsertionEpisodeCount
        self.kindRawValue = kindRawValue
        self.schemaVersion = schemaVersion
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }
}

// MARK: - ShowCapabilityProfileKind

/// The five observed profile kinds plus the `.unknown` sentinel.
///
/// `.unknown` is the only value any caller is permitted to assume by
/// default — every other case is meaningful ONLY when the activation
/// floor is met AND the SLI gate is satisfied. When the feature flag
/// is OFF the evaluator returns `.unknown` unconditionally, and the
/// modulator becomes a no-op (the budget multiplier is 1.0).
enum ShowCapabilityProfileKind: String, Sendable, Codable, CaseIterable, Hashable {
    case unknown
    case chapterRich = "chapter-rich"
    case hostReadOnly = "host-read-only"
    case musicBedReliable = "music-bed-reliable"
    case sponsorDeclared = "sponsor-declared"
    case dynamicInsertionHeavy = "dynamic-insertion-heavy"

    /// Human-facing display label rendered by the Settings → Diagnostics
    /// row. Verbatim — test-pinned in `SettingsL274CopyTests`.
    var displayLabel: String {
        switch self {
        case .unknown:               return "Unknown"
        case .chapterRich:           return "Chapter-rich"
        case .hostReadOnly:          return "Host-read only"
        case .musicBedReliable:      return "Music-bed reliable"
        case .sponsorDeclared:       return "Sponsor-declared"
        case .dynamicInsertionHeavy: return "Dynamic-insertion heavy"
        }
    }
}
