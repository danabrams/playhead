// DiagnosticsBundleBuilder.swift
// Pure transform from raw inputs (work-journal entries, eligibility,
// per-episode opt-in inputs) into the support-safe diagnostics bundle.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Why a free `enum` of pure statics: the builder has no instance state
// and the legal checklist demands deterministic, audit-able transforms.
// Keeping the surface as static functions makes every code path
// reachable from a unit test without spinning up an actor.
//
// Input ordering contract: the builder does NOT assume any particular
// ordering of the `workJournalEntries` input. Both the `scheduler_events`
// and `work_journal_tail` projections sort by `timestamp` ascending
// internally before taking the most-recent tail. This keeps the builder
// correct whether the caller supplies rows oldest-first (insertion
// order, as the spec's `work_journal_tail[]` language implies) or
// newest-first (which is what the production `AnalysisStore`
// `ORDER BY timestamp DESC, rowid DESC` fetch returns). Without the
// internal sort a `.suffix(N)` on a DESC-ordered input would silently
// return the OLDEST N rows of the fetched window.
//
// Legal checklist enforcement points (per spec):
//   (a) Default bundle never carries a raw episodeId — all references
//       go through `EpisodeIdHasher.hash(installID:episodeId:)`.
//   (b) Transcript excerpts use the ±30 s window and 1000-char
//       truncation defined as `Self.transcriptExcerptHalfWindow` and
//       `Self.transcriptExcerptCharCap`.
//   (c) Hashing scheme is delegated to `EpisodeIdHasher`.
//   (d) Feature summaries are restricted to `OptInBundle.FeatureSummary`
//       (mean / max only); the input shape physically cannot carry a
//       raw feature vector.

import Foundation

// MARK: - Input shapes (test-friendly intermediates)

/// One transcript chunk, narrowed to the only fields the diagnostics
/// builder cares about. Keeping this distinct from `TranscriptChunk`
/// (which lives in `AnalysisStore.swift` and carries far more analyzer
/// state) decouples the builder from the SQLite store and makes the
/// pure-function tests cheap.
struct DiagnosticsTranscriptChunk: Sendable, Equatable {
    let startTime: Double
    let endTime: Double
    let text: String
}

/// Per-episode input for `DiagnosticsBundleBuilder.buildOptIn(...)`.
struct DiagnosticsEpisodeInput: Sendable, Equatable {
    let episodeId: String
    let episodeTitle: String
    let diagnosticsOptIn: Bool
    /// Detected ad boundary times in seconds. The builder emits one
    /// transcript excerpt per boundary.
    let adBoundaryTimes: [Double]
    /// Persisted transcript chunks for this episode, in increasing time
    /// order. The builder does not re-sort.
    let transcriptChunks: [DiagnosticsTranscriptChunk]
    /// Pre-aggregated coarse feature summary for this episode. `nil`
    /// when the episode has no completed feature pass.
    let featureSummary: OptInBundle.FeatureSummary?
}

// MARK: - Builder

enum DiagnosticsBundleBuilder {

    /// Number of `WorkJournalEntry` rows projected into
    /// `scheduler_events`. Per bead spec.
    static let schedulerEventsCap = 200

    /// Number of `WorkJournalEntry` rows preserved in
    /// `work_journal_tail`. Per bead spec.
    static let workJournalTailCap = 50

    /// Half of the transcript excerpt window around an ad boundary;
    /// total window is `2 * halfWindow` seconds. Locked in at 30 s by
    /// legal checklist item (b).
    static let transcriptExcerptHalfWindow: Double = 30

    /// Hard cap on the character length of a single transcript excerpt.
    /// Excerpts longer than this are truncated to the cap. Legal
    /// checklist item (b).
    ///
    /// Units: grapheme-cluster count via `String.prefix` (i.e.
    /// `String.count`). Byte-length of the resulting UTF-8 may exceed
    /// this for multi-byte characters (emoji, CJK text, combining
    /// marks). This is intentional — the cap is about limiting the
    /// amount of *text* shipped for legal review, not bytes on disk.
    static let transcriptExcerptCharCap = 1_000

    // MARK: - Default bundle

    /// Pure transform from raw inputs into the always-safe
    /// `DefaultBundle`.
    ///
    /// Input order is not significant: the builder sorts
    /// `workJournalEntries` by `timestamp` ascending before taking
    /// the tail for both `scheduler_events` and `work_journal_tail`,
    /// so it produces the same spec-compliant output regardless of
    /// whether the caller supplies oldest-first (insertion order) or
    /// newest-first (`ORDER BY timestamp DESC`) rows. This guards
    /// against the `.suffix(N)` inversion bug where a DESC-ordered
    /// caller would otherwise leak the OLDEST N rows into the tail.
    static func buildDefault(
        appVersion: String,
        osVersion: String,
        deviceClass: DeviceClass,
        buildType: BuildType,
        eligibility: AnalysisEligibility,
        workJournalEntries: [WorkJournalEntry],
        installID: UUID
    ) -> DefaultBundle {

        // Canonicalise: timestamp ASCENDING (oldest first). Taking the
        // suffix of this ordering is equivalent to "most recent N",
        // independent of how the caller sorted the input. Reversing
        // gives us newest-first for the `scheduler_events` projection.
        let sortedAsc = workJournalEntries.sorted { $0.timestamp < $1.timestamp }

        // scheduler_events: most-recent N by timestamp, emitted newest
        // first. Take the trailing N of the ascending list and reverse.
        let schedulerTailAsc = sortedAsc.suffix(schedulerEventsCap)
        let schedulerEvents = schedulerTailAsc.reversed().map { entry -> DefaultBundle.SchedulerEvent in
            DefaultBundle.SchedulerEvent(
                timestamp: entry.timestamp,
                eventType: entry.eventType.rawValue,
                episodeIdHash: EpisodeIdHasher.hash(
                    installID: installID, episodeId: entry.episodeId
                ),
                internalMissCause: entry.cause?.rawValue
            )
        }

        // work_journal_tail: most-recent N by timestamp, emitted in
        // ascending (insertion-equivalent) order — the spec phrases
        // this as "last 50 … by insertion order".
        let tailSlice = sortedAsc.suffix(workJournalTailCap)
        let workJournalTail = tailSlice.map { entry -> DefaultBundle.WorkJournalRecord in
            DefaultBundle.WorkJournalRecord(
                id: entry.id,
                episodeIdHash: EpisodeIdHasher.hash(
                    installID: installID, episodeId: entry.episodeId
                ),
                generationID: entry.generationID.uuidString,
                schedulerEpoch: entry.schedulerEpoch,
                timestamp: entry.timestamp,
                eventType: entry.eventType.rawValue,
                cause: entry.cause?.rawValue
            )
        }

        let reason = AnalysisUnavailableReason.derive(from: eligibility)

        return DefaultBundle(
            appVersion: appVersion,
            osVersion: osVersion,
            deviceClass: deviceClass,
            buildType: buildType,
            eligibilitySnapshot: eligibility,
            analysisUnavailableReason: reason,
            schedulerEvents: schedulerEvents,
            workJournalTail: Array(workJournalTail)
        )
    }

    // MARK: - OptIn bundle

    /// Returns an `OptInBundle` containing only opted-in episodes.
    /// Returns `nil` when no input has `diagnosticsOptIn == true` so the
    /// surrounding `DiagnosticsBundleFile` can omit the field entirely
    /// (clearer than emitting an empty `episodes: []` array).
    static func buildOptIn(episodes: [DiagnosticsEpisodeInput]) -> OptInBundle? {
        let optedIn = episodes.filter(\.diagnosticsOptIn)
        guard !optedIn.isEmpty else { return nil }

        let mapped = optedIn.map { input -> OptInBundle.Episode in
            let excerpts = input.adBoundaryTimes.map { boundary in
                makeExcerpt(boundary: boundary, chunks: input.transcriptChunks)
            }
            return OptInBundle.Episode(
                episodeId: input.episodeId,
                episodeTitle: input.episodeTitle,
                transcriptExcerpts: excerpts,
                featureSummaries: input.featureSummary.map { [$0] } ?? []
            )
        }
        return OptInBundle(episodes: mapped)
    }

    // MARK: - Transcript excerpt window + truncation

    /// Build a single excerpt for one boundary: take chunks whose
    /// `[startTime, endTime]` overlaps `[boundary - 30s, boundary + 30s]`,
    /// concatenate their text with a space separator, and truncate to
    /// the 1000-char cap.
    private static func makeExcerpt(
        boundary: Double,
        chunks: [DiagnosticsTranscriptChunk]
    ) -> OptInBundle.TranscriptExcerpt {
        let windowStart = boundary - transcriptExcerptHalfWindow
        let windowEnd = boundary + transcriptExcerptHalfWindow
        let included = chunks.filter { chunk in
            // Half-open interval semantics: the chunk overlaps the
            // window if its end is past the window start AND its start
            // is before the window end. This matches the FeatureWindow
            // overlap convention used elsewhere in the pipeline and
            // keeps boundary-aligned chunks IN the excerpt.
            chunk.endTime > windowStart && chunk.startTime < windowEnd
        }
        let raw = included.map(\.text).joined(separator: " ")
        let truncated = String(raw.prefix(transcriptExcerptCharCap))
        return OptInBundle.TranscriptExcerpt(
            boundaryTime: boundary,
            startTime: windowStart,
            endTime: windowEnd,
            text: truncated
        )
    }
}
