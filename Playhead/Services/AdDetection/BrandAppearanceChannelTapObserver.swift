// BrandAppearanceChannelTapObserver.swift
// playhead-brandab fire instrumentation:
//
// Observation-only sink that records, per decoded span, whether a set of
// LEDGER-ENTRY precision channels emitted an entry for that span:
//   • playhead-xsdz.12 — rhetorical act-sequence grammar (`.rhetoricalGrammar`).
//   • playhead-xsdz.13 — cross-show syndication (`.crossShowSyndication`).
//   • playhead-xsdz.8  — composite audio-forensics boundary (`.audioForensics`).
//
// The brand-appearance live A/B (`BrandAppearanceLiveABTests`) and the
// audio-forensics live A/B (`AudioForensicsTemporalRegLiveABTests`) need this so
// a NULL result is interpretable: "metrics identical to baseline" is ambiguous —
// did the channel never fire, or fire-but-no-effect? Tallying how many spans
// actually received each channel's evidence entry disambiguates the two. The
// fire site reads the SAME pre-suppression `ledger` the decision is built from,
// so a recorded fire is exactly "this channel produced an entry for this span".
//
// playhead-actempo.8: this tap is the cheapest correct fire mechanism for the
// xsdz.8 audio-forensics channel too, because `.audioForensics` is — like the two
// brand-appearance signals — a ledger entry tallied from the EXACT same
// pre-suppression `ledger` at the EXACT same fire site. No new fire site or
// production hook is needed; the channel just adds one more counted source.
//
// playhead-fbsignals.9: the same applies to the xsdz.9 cross-episode "memory"
// POSITIVE boost (`.crossEpisodeMemory`), which is also a ledger entry tallied
// from the EXACT same pre-suppression `ledger` at the EXACT same fire site. The
// channel adds one more counted source for that boost. (The HARD-NEGATIVE
// SUPPRESSION half of xsdz.9 is NOT a ledger entry — it is a post-fusion
// multiplicative penalty on `skipConfidence`, so its fire is counted by the
// separate `NegativeBankSuppressionObserver`, mirroring how the xsdz.10
// temporal-reg penalty uses `TemporalRegularizationObserver`.)
//
// Contract (mirrors `FragilityDiagnosticObserver` / `RegionShadowObserver` /
// `Phase5ProjectorObserver`):
//   • Compiled in all configurations. The fire site is a no-op when the
//     observer is `nil`, which is the production wiring: `PlayheadRuntime`
//     never constructs one (it is not even referenced there), so release builds
//     have zero footprint and byte-identical decision behavior.
//   • Behavior-neutral: the observer NEVER feeds back into the decision path.
//     It only RECORDS the inputs the real decision already computed; the
//     decision is untouched whether the observer is nil or live.
//   • Counts accumulate per asset (each `record` tallies one span), so a full
//     backfill leaves the per-channel fire totals for the asset.
//   • Actor for safe cross-concurrency-domain access from tests (backfill runs
//     on an arbitrary task executor; tests read from the main actor).

import Foundation
import OSLog

/// Per-asset fire tally for the two brand-appearance channels. Pure value type
/// so tests (and the harness JSON dump) consume it directly.
struct BrandAppearanceChannelFireCounts: Sendable, Equatable {
    /// Number of decoded spans whose ledger carried a strictly-positive
    /// `.rhetoricalGrammar` entry (xsdz.12 fired).
    var rhetoricalGrammarFiredSpans: Int = 0
    /// Number of decoded spans whose ledger carried a strictly-positive
    /// `.crossShowSyndication` entry (xsdz.13 fired).
    var crossShowSyndicationFiredSpans: Int = 0
    /// Number of decoded spans whose ledger carried a strictly-positive
    /// `.audioForensics` entry (xsdz.8 fired).
    var audioForensicsFiredSpans: Int = 0
    /// Number of decoded spans whose ledger carried a strictly-positive
    /// `.crossEpisodeMemory` entry (xsdz.9 POSITIVE-boost fired).
    var crossEpisodeMemoryFiredSpans: Int = 0
    /// Total decoded spans the observer saw for this asset (the denominator).
    var observedSpans: Int = 0
}

actor BrandAppearanceChannelTapObserver {

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "BrandAppearanceChannelTapObserver"
    )

    /// Per-asset accumulated fire counts.
    private var counts: [String: BrandAppearanceChannelFireCounts] = [:]

    init() {}

    /// Record one decoded span's brand-appearance channel firing for an asset.
    ///
    /// The fire site supplies the SAME `ledger` the decision is built from; this
    /// method tallies whether each channel produced a strictly-positive entry.
    ///
    /// - Parameters:
    ///   - assetId: analysis asset id the span belongs to.
    ///   - ledger: the evidence ledger that fed this span's decision (the
    ///     pre-suppression ledger, so the tally reflects whether the channel
    ///     EMITTED an entry, independent of any later downweight).
    func record(assetId: String, ledger: [EvidenceLedgerEntry]) {
        var tally = counts[assetId, default: BrandAppearanceChannelFireCounts()]
        tally.observedSpans += 1
        if ledger.contains(where: { $0.source == .rhetoricalGrammar && $0.weight > 0 }) {
            tally.rhetoricalGrammarFiredSpans += 1
        }
        if ledger.contains(where: { $0.source == .crossShowSyndication && $0.weight > 0 }) {
            tally.crossShowSyndicationFiredSpans += 1
        }
        if ledger.contains(where: { $0.source == .audioForensics && $0.weight > 0 }) {
            tally.audioForensicsFiredSpans += 1
        }
        if ledger.contains(where: { $0.source == .crossEpisodeMemory && $0.weight > 0 }) {
            tally.crossEpisodeMemoryFiredSpans += 1
        }
        counts[assetId] = tally
    }

    /// The accumulated fire counts for an asset (zeroed defaults if none seen).
    func fireCounts(for assetId: String) -> BrandAppearanceChannelFireCounts {
        counts[assetId, default: BrandAppearanceChannelFireCounts()]
    }
}
