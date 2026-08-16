// Podcast.swift
// Core data models: Podcast, Episode, and related types.
// Uses SwiftData for persistence.

import Foundation
import SwiftData

// MARK: - Podcast

@Model
final class Podcast {
    #Unique<Podcast>([\.feedURL])

    var feedURL: URL
    var title: String
    var author: String
    var artworkURL: URL?
    @Relationship(deleteRule: .cascade, inverse: \Episode.podcast)
    var episodes: [Episode]
    var subscribedAt: Date

    /// playhead-snp: per-show toggle for new-episode local notifications.
    /// Default ON (opt-out). When `false`, the new-episode notification
    /// scheduler skips this podcast entirely, regardless of the app-wide
    /// `UserPreferences.newEpisodeNotificationsEnabled` setting.
    /// Additive optional with a Swift default — existing rows decode
    /// with `true` so an upgrade preserves the spec'd opt-out behavior
    /// for already-subscribed shows.
    var notificationsEnabled: Bool = true

    /// playhead-epii: per-show opt-out for structure-aware silence
    /// compression. Default OFF (compression-on by default). When
    /// `true`, the `SilenceCompressor` short-circuits for episodes of
    /// this podcast and never raises playback rate above the user's
    /// chosen base speed — music intros, jingles, and dead air play at
    /// full duration. Targets shows where the music IS the experience
    /// (composed scores, narrative beats).
    ///
    /// Additive optional with a Swift default — existing rows decode
    /// with `false`, preserving the spec'd "it just works" default
    /// (compression engaged) for already-subscribed shows.
    ///
    /// No global toggle exists; per-show is the only surface, in line
    /// with the bead's "peace of mind, not metrics" stance — there is
    /// no app-wide setting to disable compression entirely.
    var keepFullMusic: Bool = false

    /// playhead-5w4: per-show override for the global
    /// `SettingsL274.DownloadsSettings.autoDownloadOnSubscribe` policy
    /// (Off / Last 1 / Last 3 / All). `nil` means "inherit the global
    /// setting"; a non-nil value bypasses the global and is used as the
    /// effective policy for this podcast's auto-download decisions in
    /// the subscription auto-download path (`BackgroundFeedRefreshService`).
    ///
    /// Persisted as the rawValue of `AutoDownloadOnSubscribe` (String?).
    /// SwiftData encodes the enum directly because `AutoDownloadOnSubscribe`
    /// is `Codable`.
    ///
    /// Additive optional with no Swift default — existing rows decode
    /// with `nil`, preserving the spec'd "all subscribed shows inherit
    /// the global setting" behavior for already-subscribed podcasts.
    /// This is a SwiftData lightweight migration (new optional property).
    ///
    /// Effective policy resolution lives on the `AutoDownloadOnSubscribe`
    /// type as `effective(override:global:)` so call sites cannot drift.
    var autoDownloadOverride: AutoDownloadOnSubscribe?

    init(
        feedURL: URL,
        title: String,
        author: String,
        artworkURL: URL? = nil,
        episodes: [Episode] = [],
        subscribedAt: Date = .now,
        notificationsEnabled: Bool = true,
        keepFullMusic: Bool = false,
        autoDownloadOverride: AutoDownloadOnSubscribe? = nil
    ) {
        self.feedURL = feedURL
        self.title = title
        self.author = author
        self.artworkURL = artworkURL
        self.episodes = episodes
        self.subscribedAt = subscribedAt
        self.notificationsEnabled = notificationsEnabled
        self.keepFullMusic = keepFullMusic
        self.autoDownloadOverride = autoDownloadOverride
    }
}

// MARK: - Episode

@Model
final class Episode {
    #Unique<Episode>([\.canonicalEpisodeKey])

    var feedItemGUID: String
    var canonicalEpisodeKey: String
    var podcast: Podcast?
    var title: String
    var audioURL: URL
    /// The second durable store of a local audio path, after
    /// `analysis_assets.sourceURL` — and the ONE the playhead-b8hj audit left
    /// as-is, deliberately. It is a MIRROR, never a resolver: nothing in the
    /// tree opens it. `PlayheadRuntime` overwrites it from
    /// `DownloadManager.cachedFileURL(for:)` whenever that resolves, and the
    /// only other reader (`PodcastDiscoveryService`) tests it for NIL-NESS as a
    /// "don't overwrite a pinned episode's duration" flag. A stale container
    /// path is therefore inert as a path.
    ///
    /// It is NOT inert as a flag: it is never written back to nil, so an
    /// evicted episode reads as pinned until it is played again. That is
    /// pre-existing behaviour, out of b8hj's scope, and noted so the next
    /// reader does not mistake the value for a live location. Do NOT start
    /// dereferencing it — route through `DownloadManager`, the resolver of
    /// record, or ``AudioCacheLocation`` for a persisted string.
    var cachedAudioURL: URL?
    var downloadState: DownloadState
    // playhead-f5ao: `lastPlayedAnalysisAssetId: UUID?` and
    // `analysisSummary: AnalysisSummary?` were REMOVED here. Both were
    // declared by the schema's first commit (168b8c24, 2026-04-02) and
    // never written by anything but `init` in the whole history of the
    // repo — measured on the device library, 0 of 1594 rows carried
    // either. `analysisSummary.hasAnalysis` was the pre-playhead-cthe
    // gate for the Library ✓; cthe replaced it with
    // `derivePlaybackReadiness(coverage:anchor:)` and the three
    // stragglers that kept reading the old mirror are now on
    // `coverageSummary` below. `lastPlayedAnalysisAssetId` had zero
    // readers anywhere; the live asset id is
    // `PlayheadRuntime.currentAnalysisAssetId` (playback) or
    // `AnalysisStore.fetchAssetByEpisodeId(_:)` (browse).
    //
    // Do NOT re-add a denormalized analysis mirror without a writer at
    // the completion point AND a test that fails when that writer is
    // removed — a field that can only ever answer "no" is worse than
    // no field, because three readers believed it.
    var duration: TimeInterval?
    var publishedAt: Date?
    var playbackPosition: TimeInterval
    var isPlayed: Bool
    var feedMetadata: FeedDescriptionMetadata?

    /// Per-episode opt-in for the OptIn diagnostics bundle (playhead-ghon).
    /// Defaults to `false` so the additive SwiftData migration is
    /// non-destructive: existing rows decode with the property set to
    /// `false` and the OptIn bundle path remains a no-op until the user
    /// flips this flag through the Phase 2 Diagnostics screen
    /// (playhead-l274). Reset policy lives in
    /// ``DiagnosticsOptInResetPolicy`` — flag clears when the mail
    /// composer reports `.sent` or `.saved`, persists on `.cancelled`
    /// or `.failed`.
    var diagnosticsOptIn: Bool = false

    /// Phase 2 coverage record (playhead-cthe). JSON-encoded Codable
    /// field so the additive SwiftData migration is non-destructive —
    /// existing rows decode with the property set to `nil` and the
    /// derivation pipeline returns `PlaybackReadiness.none` until the
    /// analysis pipeline (playhead-zp5y / playhead-quh7) starts writing
    /// records.
    ///
    /// Readiness is NEVER persisted — always re-derive via
    /// `derivePlaybackReadiness(coverage:anchor:)` so multiple UI
    /// surfaces at different anchors cannot diverge.
    ///
    /// playhead-f5ao: since `analysisSummary` was removed this is the
    /// SOLE denormalized analysis mirror on `Episode`, and every
    /// "does this episode have analysis?" question in the tree now
    /// derives from it. **It has no production writer either** — that
    /// is playhead-lb26, filed rather than fixed here; f5ao's scope was
    /// the mirror cthe superseded, not cthe's own.
    var coverageSummary: CoverageSummary?

    /// Phase 2 readiness anchor (playhead-cthe). The time (seconds from
    /// episode start) from which readiness should be evaluated. Updated
    /// at the existing play-loop commit points (`PlayheadApp
    /// .persistPlaybackPosition`) alongside `playbackPosition`, so a
    /// force-quit mid-playback preserves the last persisted anchor as
    /// the spec requires ("on force-quit mid-playback, last persisted
    /// commit wins").
    ///
    /// Kept distinct from `playbackPosition` so a future scope can
    /// decouple "where the user is listening" from "where readiness is
    /// evaluated". For now the two are updated together.
    var playbackAnchor: TimeInterval?

    /// Persisted user ordering for the Activity screen's Up Next section
    /// (playhead-cjqq). `nil` means the user has never reordered this
    /// episode — it inherits the production provider's natural
    /// (scheduler-derived) ordering. After a drag, the visible Up Next
    /// rows are renumbered sequentially (0, 1, 2, …) so subsequent
    /// reorders compose deterministically and the Int domain cannot
    /// overflow.
    ///
    /// Sort rule used by `LiveActivitySnapshotProvider`:
    ///   `(queuePosition asc, nil-last, canonicalEpisodeKey tiebreak)`
    ///
    /// Additive optional field — defaults to `nil` so existing rows
    /// decode under the V1 schema without a migration stage. Do NOT
    /// promote to non-optional (would break decode of pre-cjqq rows).
    var queuePosition: Int?

    init(
        feedItemGUID: String,
        feedURL: URL,
        podcast: Podcast? = nil,
        title: String,
        audioURL: URL,
        cachedAudioURL: URL? = nil,
        downloadState: DownloadState = .notDownloaded,
        duration: TimeInterval? = nil,
        publishedAt: Date? = nil,
        playbackPosition: TimeInterval = 0,
        isPlayed: Bool = false,
        feedMetadata: FeedDescriptionMetadata? = nil,
        diagnosticsOptIn: Bool = false,
        coverageSummary: CoverageSummary? = nil,
        playbackAnchor: TimeInterval? = nil,
        queuePosition: Int? = nil
    ) {
        self.feedItemGUID = feedItemGUID
        self.canonicalEpisodeKey = Self.makeCanonicalKey(
            feedItemGUID: feedItemGUID, feedURL: feedURL
        )
        self.podcast = podcast
        self.title = title
        self.audioURL = audioURL
        self.cachedAudioURL = cachedAudioURL
        self.downloadState = downloadState
        self.duration = duration
        self.publishedAt = publishedAt
        self.playbackPosition = playbackPosition
        self.isPlayed = isPlayed
        self.feedMetadata = feedMetadata
        self.diagnosticsOptIn = diagnosticsOptIn
        self.coverageSummary = coverageSummary
        self.playbackAnchor = playbackAnchor
        self.queuePosition = queuePosition
    }

    /// Derives the canonical key from feedItemGUID + feedURL for preview budget tracking.
    static func makeCanonicalKey(feedItemGUID: String, feedURL: URL) -> String {
        "\(feedURL.absoluteString)::\(feedItemGUID)"
    }

    /// playhead-usn1: the show this episode belongs to, as a canonical
    /// identifier, WITHOUT depending on the `podcast` relationship being
    /// materialised.
    ///
    /// The relationship is the primary source and is used verbatim when it is
    /// there. When it is not, the answer is still on the row: every
    /// `canonicalEpisodeKey` is built by ``makeCanonicalKey(feedItemGUID:feedURL:)``
    /// as `feedURL.absoluteString + "::" + feedItemGUID`, and this row carries
    /// its own `feedItemGUID`. Stripping that exact suffix recovers the exact
    /// feed URL string the key was built from — no parsing, no guessing, no
    /// splitting on the first `::` (which an IPv6 host or a GUID containing
    /// `::` would defeat).
    ///
    /// The two agree by construction: `PodcastDiscoveryService.persist` matches
    /// or creates the `Podcast` from the SAME `feedURL` value it hands
    /// `Episode.init`, so the derived identifier is byte-identical to
    /// `podcast!.feedURL.absoluteString` for every row that path created.
    ///
    /// Both branches go through `RecurrenceMaterialIdentity.canonicalIdentifier`
    /// — the same canonicalisation `SkipOrchestrator.beginEpisode` applies to
    /// the value it is handed. An identifier in non-canonical spelling is not an
    /// identity we may key show-scoped evidence on, and admitting one here would
    /// retarget trust and recurrence learning into a neighbouring namespace,
    /// which is strictly worse than resolving nothing.
    var resolvedShowIdentity: String? {
        if let feedURL = podcast?.feedURL.absoluteString,
           let canonical = RecurrenceMaterialIdentity.canonicalIdentifier(feedURL) {
            return canonical
        }
        return Self.showIdentity(
            fromCanonicalEpisodeKey: canonicalEpisodeKey,
            feedItemGUID: feedItemGUID
        )
    }

    /// The inverse of ``makeCanonicalKey(feedItemGUID:feedURL:)``.
    ///
    /// Returns `nil` — never a fabricated identity — when the key does not have
    /// the shape this type writes, when nothing precedes the separator, or when
    /// what precedes it is not a canonical identifier.
    static func showIdentity(
        fromCanonicalEpisodeKey key: String,
        feedItemGUID: String
    ) -> String? {
        let suffix = "::\(feedItemGUID)"
        guard key.hasSuffix(suffix), key.count > suffix.count else { return nil }
        let feedURLString = String(key.dropLast(suffix.count))
        return RecurrenceMaterialIdentity.canonicalIdentifier(feedURLString)
    }
}

// MARK: - DownloadState

enum DownloadState: Int, Codable, Sendable {
    case notDownloaded
    case downloading
    case downloaded
    case failed
}

// MARK: - FeedDescriptionMetadata

/// Metadata from RSS description/summary fields.
/// Normalized text + source hashes for rebuild detection. Live consumers
/// decide whether to use each signal through `MetadataActivationConfig`.
struct FeedDescriptionMetadata: Codable, Sendable, Equatable {
    /// RSS `<description>` — HTML stripped, entities decoded, truncated.
    var feedDescription: String?
    /// iTunes `<itunes:summary>` or `<content:encoded>` — normalized.
    var feedSummary: String?
    /// Hashes of the raw source strings, enabling change detection without
    /// storing unbounded HTML blobs.
    var sourceHashes: SourceHashes
    /// playhead-gtt9.22: Chapter evidence parsed from RSS-feed inline
    /// `<podcast:chapter>` markers (zero-cost, deterministic). Optional
    /// so the additive Codable migration is non-destructive: existing
    /// rows decode with `nil` and the ad-detection pipeline simply sees
    /// no chapter-derived signal until the next feed refresh repopulates
    /// the field. Each entry already carries source/disposition/quality
    /// — the heavy lifting was done in `ChapterEvidenceParser`.
    /// Out of scope for this field: ID3 CHAP and Podcasting 2.0 JSON
    /// chapters (those are non-RSS sources; future beads can extend).
    var chapterEvidence: [ChapterEvidence]?
    /// playhead-gtt9.22: URL of the optional Podcasting 2.0
    /// `<podcast:chapters>` external JSON document. Captured at parse
    /// time so a future opt-in fetch path can hydrate it on demand. The
    /// parser does NOT fetch the URL (no new network calls during
    /// XMLParse); fetch is mediated by `ChapterEvidenceParser
    /// .parsePodcasting20Chapters(from:)` only when the runtime opts in.
    var chaptersFeedURL: URL?

    struct SourceHashes: Codable, Sendable, Equatable, Hashable {
        // Stored as Int64 (raw bit-pattern of the original FNV-1a 64-bit
        // UInt64). SwiftData persists `feedMetadata` as a Codable blob and
        // the NSNumber bridge traps on UInt64 values > Int64.max when the
        // getter deserializes — see the "feedMetadata UInt64 bridge" crash.
        // Consumers treat these as opaque equality tokens, so a bit-cast
        // preserves identity: equal UInt64s bit-cast to equal Int64s.
        var descriptionHash: Int64?
        var summaryHash: Int64?

        init(descriptionHash: Int64? = nil, summaryHash: Int64? = nil) {
            self.descriptionHash = descriptionHash
            self.summaryHash = summaryHash
        }

        // Custom Codable with legacy-row migration.
        //
        // Decode strategy:
        //   - New writes: Int64 numeric token — decode directly.
        //   - Legacy rows that wrote UInt64 and whose value was <= Int64.max:
        //     the JSON/plist number round-trips cleanly into an Int64 of the
        //     same numeric value; same bit pattern in that range.
        //   - Legacy rows whose UInt64 was > Int64.max: decoding as Int64
        //     overflows and throws. We swallow that error and store nil —
        //     the row simply loses its hash and gets re-synced on the next
        //     feed fetch. No user-facing data is lost.
        //   - Missing key: nil.
        //
        // Encode strategy: plain Int64 (or nil as absent key).

        private enum CodingKeys: String, CodingKey {
            case descriptionHash
            case summaryHash
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            // `try?` collapses both "key missing" and "number overflows Int64"
            // into nil. The latter is the legacy-UInt64-with-high-bit case.
            self.descriptionHash = try? container.decodeIfPresent(Int64.self, forKey: .descriptionHash)
            self.summaryHash = try? container.decodeIfPresent(Int64.self, forKey: .summaryHash)
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeIfPresent(descriptionHash, forKey: .descriptionHash)
            try container.encodeIfPresent(summaryHash, forKey: .summaryHash)
        }
    }
}
