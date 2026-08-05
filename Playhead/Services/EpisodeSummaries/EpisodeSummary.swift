// EpisodeSummary.swift
// playhead-jzik: data carrier for an on-device summary of a single episode.
//
// The summary is produced by `EpisodeSummaryExtractor` against a sampled
// transcript window (first/middle/last of the transcript chunk list) and
// persisted in the `episode_summaries` table keyed by `analysisAssetId`.
//
// `transcriptVersion` is RECORDED but not yet READ (playhead-iu0t R2).
// This header used to say it "participates as the invalidation key — when
// the underlying transcript regenerates and produces a new version string,
// the backfill coordinator treats any pre-existing row with a stale
// `transcriptVersion` as missing and queues a fresh generation." No
// selector does that. `fetchEpisodeSummaryBackfillCandidates`' only
// staleness test is `s.schemaVersion < ?`; the column is written, indexed
// (`idx_episode_summaries_transcript_version`) and never compared. A
// summary therefore survives any amount of transcript growth.
//
// This is the l4i4 shape — every selector reads the STATUS, none reads the
// MEASUREMENT against it — and it is filed as its own bead rather than
// fixed here, because making the selector read it changes how much FM work
// the backfill schedules. What R2 did fix is the VALUE: it used to be the
// final-only version read back out of `transcript_chunks.transcriptVersion`
// and is now derived from the canonical chunk set, so the selector has
// something true to read when it starts reading it.

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
///   - `transcriptVersion`: the CANONICAL transcript version the source
///     text was drawn from. Recorded only — no selector compares it, so
///     it does not currently invalidate anything. See the file header.
///   - `createdAt`: wall-clock at write time. Used purely for export /
///     diagnostics; never load-bearing.
struct EpisodeSummary: Sendable, Equatable, Hashable, Codable {
    /// Bumps when the persisted shape OR the upstream prompt grammar
    /// changes in a way that should invalidate prior rows.
    ///
    /// v2 (playhead-g4dk): the summarizer now excludes confirmed-ad
    /// transcript spans from its input and the prompt explicitly instructs
    /// the model to ignore advertisements. Rows written at v1 were built
    /// from raw ad-contaminated transcripts (a car-sponsor read once
    /// crowded out a Tour de France episode), so they must regenerate.
    static let currentSchemaVersion: Int = 2

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
