// EpisodeSummary.swift
// playhead-jzik: data carrier for an on-device summary of a single episode.
//
// The summary is produced by `EpisodeSummaryExtractor` against a sampled
// transcript window (first/middle/last of the transcript chunk list) and
// persisted in the `episode_summaries` table keyed by `analysisAssetId`.
//
// `transcriptVersion` participates as the invalidation key — when the
// underlying transcript regenerates and produces a new version string,
// the backfill coordinator treats any pre-existing row with a stale
// `transcriptVersion` as missing and queues a fresh generation.

import Foundation

/// On-device episode summary persisted alongside an `analysis_assets`
/// row. All values are verbatim-grounded — the FM is asked to extract,
/// not paraphrase, the topics and guests it mentions in the transcript
/// window.
///
/// Field design is deliberately small:
///
///   - `summary`: 2–3 sentence editorial blurb. Surfaced verbatim in
///     the expanded episode cell. The extractor's permissive fallback
///     can land here as plain prose when the schema-bound path refuses.
///   - `mainTopics`: short keyword phrases. UI clamps render to the
///     first 3 entries. The schema bound is bigger so the FM has room
///     to produce something useful before the truncation happens.
///   - `notableGuests`: zero or more guest names. Empty for solo /
///     monologue shows is the common case.
///   - `schemaVersion`: bumped when we materially change the persisted
///     shape OR the prompt grammar. Old rows below the current version
///     are treated as invalidated by `EpisodeSummaryBackfillCoordinator`.
///   - `transcriptVersion`: identifies which transcript pass produced
///     the source text. When the transcript engine emits a new version
///     for the same asset, the row is treated as invalidated.
///   - `createdAt`: wall-clock at write time. Used purely for export /
///     diagnostics; never load-bearing.
struct EpisodeSummary: Sendable, Equatable, Hashable, Codable {
    /// Bumps when the persisted shape OR the upstream prompt grammar
    /// changes in a way that should invalidate prior rows.
    static let currentSchemaVersion: Int = 1

    let analysisAssetId: String
    let summary: String
    let mainTopics: [String]
    let notableGuests: [String]
    let schemaVersion: Int
    let transcriptVersion: String?
    let createdAt: Date

    init(
        analysisAssetId: String,
        summary: String,
        mainTopics: [String],
        notableGuests: [String],
        schemaVersion: Int = EpisodeSummary.currentSchemaVersion,
        transcriptVersion: String?,
        createdAt: Date
    ) {
        self.analysisAssetId = analysisAssetId
        self.summary = summary
        self.mainTopics = mainTopics
        self.notableGuests = notableGuests
        self.schemaVersion = schemaVersion
        self.transcriptVersion = transcriptVersion
        self.createdAt = createdAt
    }
}

extension EpisodeSummary {
    /// Maximum number of topic tags rendered in the expanded episode cell.
    /// Hard product cap from the bead spec — the backing array can be
    /// larger but the UI never shows more than this many pills.
    static let visibleTopicCap: Int = 3

    /// Trim the summary's topic and guest arrays to defensible ceilings
    /// before persisting. The FM is asked for short lists but
    /// occasionally returns runaway arrays; we don't want a single row
    /// to balloon the SQLite blob.
    static func sanitize(
        topics: [String],
        guests: [String]
    ) -> (topics: [String], guests: [String]) {
        let trimmedTopics = topics
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .prefix(8)
        let trimmedGuests = guests
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .prefix(8)
        return (Array(trimmedTopics), Array(trimmedGuests))
    }
}
