// ChapterMetadataEvidenceBuilder.swift
// playhead-gtt9.22: Bridges parsed `[ChapterEvidence]` into the
// `BackfillEvidenceFusion` pipeline as `EvidenceLedgerEntry` items.
//
// Design rationale (resolves bead design questions):
//
// 1. Confidence weighting (Q1):
//    Chapter markers ride the existing `metadata` channel — they are
//    one signal among many, NOT a ground-truth auto-skip vector. Even
//    a publisher-labeled "Sponsor" chapter must clear the metadata
//    corroboration gate (`DecisionMapper.metadataCorroborationGate`)
//    before contributing to a skip. Publisher metadata can be wrong,
//    imprecise, or stale — treat it like any other prior.
//
// 2. Imprecise chapter timestamps (Q2):
//    Span attachment is *interval-overlap* based, not point-anchor.
//    A chapter marked `[120s, 180s]` contributes weight to any decoded
//    span whose `[startTime, endTime]` overlaps that interval, even
//    when boundaries differ by ±a few seconds. The acoustic-snap
//    refinement still happens *upstream* of fusion (in span boundary
//    selection), so this builder receives spans whose boundaries are
//    already aligned to acoustic features when those features exist.
//    For chapters that fall fully inside or fully outside a span the
//    overlap rule is the right semantic; the ±2 s slop is absorbed
//    by the overlap test on either side.
//
// 3. Cooperative-share heuristic (Q3):
//    Out of scope for this builder. The cooperative-share decision
//    (does this show provide reliable chapters? remember per-podcast)
//    is a higher-level policy question; here we treat each `ChapterEvidence`
//    on its own merits via its `qualityScore`. A future bead can add
//    a per-show reliability multiplier without touching this file.
//
// 4. Dynamic content (Q4):
//    Out of scope; cadence-based re-fetch is handled by the existing
//    feed-refresh service. This builder is a pure projection from
//    already-persisted evidence to ledger entries — re-fetch concerns
//    live upstream.
//
// 5. Privacy/network (Q5):
//    Pure: no I/O. The builder transforms in-memory `ChapterEvidence`
//    arrays into ledger entries. Verified by the network-call regression
//    test in `ChapterEvidencePipelineRegressionTests`.

import Foundation
import OSLog

// MARK: - ChapterMetadataEvidenceBuilder

struct ChapterMetadataEvidenceBuilder: Sendable {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "ChapterMetadataEvidenceBuilder"
    )

    /// Per-chapter base weight reflecting how much trust we place in a
    /// publisher-supplied marker. Capped well below the metadata family
    /// budget (`FusionWeightConfig.metadataCap = 0.15`) so a single
    /// strong chapter cannot saturate the family on its own — we still
    /// want corroboration from in-audio signals before a skip.
    private static let baseWeight: Double = 0.10

    /// Quality-score floor below which the chapter is dropped entirely.
    /// Untitled / ambiguous chapters with no end time score near 0 and
    /// would just add noise; this filter trims them.
    private static let qualityFloor: Float = 0.30

    init() {}

    /// Build per-span chapter-derived ledger entries.
    ///
    /// - Parameters:
    ///   - chapters: All chapter evidence available for the asset.
    ///     Typically sourced from `Episode.feedMetadata.chapterEvidence`
    ///     (RSS inline) and/or PC20 JSON (opt-in path) and/or ID3 CHAP.
    ///   - span: The decoded span to score evidence against. Only
    ///     chapters that *overlap* the span's interval contribute.
    /// - Returns: Zero or more `EvidenceLedgerEntry` items with
    ///   `source: .metadata`. Returned entries are emitted as their
    ///   honest pre-clamp weights — the family clamp at
    ///   `FusionWeightConfig.metadataCap` is applied later inside
    ///   `BackfillEvidenceFusion.buildLedger()` via `FusionBudgetClamp`.
    func buildEntries(
        chapters: [ChapterEvidence],
        for span: DecodedSpan
    ) -> [EvidenceLedgerEntry] {
        guard !chapters.isEmpty else { return [] }

        // Filter to chapters whose *disposition* is actionable for ad
        // detection (`.adBreak`). `.content` and `.ambiguous` chapters
        // do not produce positive ad-evidence — `.content` is consumed
        // upstream as a soft crossing penalty by the candidate-window
        // selector, not by fusion.
        let adChapters = chapters.filter { $0.disposition == .adBreak }
        guard !adChapters.isEmpty else { return [] }

        // Restrict to chapters that overlap this span's interval.
        let spanStart = span.startTime
        let spanEnd = span.endTime
        let overlapping = adChapters.filter { chapter in
            chapterOverlapsSpan(chapter: chapter, spanStart: spanStart, spanEnd: spanEnd)
        }
        guard !overlapping.isEmpty else { return [] }

        // Apply quality-score floor and aggregate. Use the maximum
        // quality across overlapping chapters rather than summing —
        // emitting one entry per span keeps the ledger compact and
        // matches the "publisher said this region is sponsored" semantic.
        let qualifying = overlapping.filter { $0.qualityScore >= Self.qualityFloor }
        guard let bestChapter = qualifying.max(by: { $0.qualityScore < $1.qualityScore }) else {
            return []
        }

        let weight = Self.baseWeight * Double(bestChapter.qualityScore)
        guard weight > 0 else { return [] }

        let entry = EvidenceLedgerEntry(
            source: .metadata,
            weight: weight,
            detail: .metadata(
                cueCount: overlapping.count,
                sourceField: .chapter,
                // `dominantCueType: .disclosure` is the closest existing
                // semantic — chapter markers labeled "Sponsor"/"Ad break"
                // are a structured form of publisher disclosure.
                dominantCueType: .disclosure
            )
        )

        Self.logger.debug(
            "ChapterMetadataEvidenceBuilder: span=\(span.id, privacy: .public) overlap=\(overlapping.count) bestQuality=\(bestChapter.qualityScore, privacy: .public) weight=\(weight, privacy: .public)"
        )

        return [entry]
    }

    // MARK: - Private

    /// Interval-overlap test: chapter `[chStart, chEnd]` overlaps span
    /// `[spanStart, spanEnd]` when `chStart <= spanEnd && chEnd >= spanStart`.
    /// Chapters with no end time fall back to a 60-second default duration
    /// (typical mid-roll length) so the overlap test still has a meaningful
    /// upper bound — without this, a "Sponsor" chapter at 600 s with no end
    /// would only match spans starting at or after 600 s, missing the very
    /// region the publisher labeled.
    private func chapterOverlapsSpan(
        chapter: ChapterEvidence,
        spanStart: TimeInterval,
        spanEnd: TimeInterval
    ) -> Bool {
        let chStart = chapter.startTime
        let chEnd = chapter.endTime ?? (chStart + 60.0)
        return chStart <= spanEnd && chEnd >= spanStart
    }
}
