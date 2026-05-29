// FbsignalsFireObservers.swift
// playhead-fbsignals FIRE instrumentation for the two FEEDBACK/MEMORY-driven
// precision signals, both gated OFF by default in production:
//
//   • playhead-xsdz.9  — cross-episode "memory" HARD-NEGATIVE suppression. The
//     POSITIVE-boost half of xsdz.9 is a `.crossEpisodeMemory` ledger entry, so
//     it is counted by the existing `BrandAppearanceChannelTapObserver`
//     (`crossEpisodeMemoryFiredSpans`). This file adds the observer for the OTHER
//     half: the negative-bank SUPPRESSION, which is NOT a ledger entry — it is a
//     post-fusion multiplicative penalty on `skipConfidence` (the same idiom as
//     the xsdz.10 temporal-reg penalty), so "did it fire?" is answered by "how
//     many spans did the suppression actually move?". `NegativeBankSuppressionObserver`
//     records exactly that.
//
//   • playhead-xsdz.11 — per-show auto-skip threshold control. The controller
//     resolves ONE per-show OFFSET per backfill and applies it (clamped) only on
//     the `.standard` track. It is NOT a ledger entry either, so "did it fire?"
//     is answered by "was the resolved offset non-zero, and on how many spans did
//     it actually shift the effective threshold?". `PerShowThresholdOffsetObserver`
//     records exactly that.
//
// The cross-episode-memory + per-show-threshold live A/B
// (`CrossEpisodeMemoryPerShowThresholdLiveABTests`) needs these so a NULL result
// is interpretable: a metric delta ≤±2 FP on the dogfood corpus is FM intra-run
// noise, so "metrics identical to baseline" is ambiguous unless we know whether
// the signal even fired. Because both signals are feedback-driven and a pure
// backfill A/B applies NO user actions, the EXPECTED fire count is 0 (an empty
// negative bank can suppress nothing; an empty controller has no corrections so
// its offset is always 0) — these observers EMPIRICALLY CONFIRM that.
//
// Contract (mirrors `TemporalRegularizationObserver` /
// `BrandAppearanceChannelTapObserver` / `FragilityDiagnosticObserver`):
//   • Compiled in all configurations. Each fire site is a no-op when the observer
//     is `nil`, which is the production wiring: `PlayheadRuntime` never constructs
//     one (they are not even referenced there), so release builds have zero
//     footprint and byte-identical decision behavior.
//   • Behavior-neutral: the observers NEVER feed back into the decision path.
//     They only RECORD what the real decision already computed; the decision is
//     untouched whether an observer is nil or live.
//   • Counts accumulate per asset, so a full backfill leaves the per-asset fire
//     total.
//   • Actor for safe cross-concurrency-domain access from tests (backfill runs on
//     an arbitrary task executor; tests read from the main actor).

import Foundation
import OSLog

// MARK: - xsdz.9 negative-bank suppression

/// Per-asset fire tally for the xsdz.9 HARD-NEGATIVE suppression pass. Pure value
/// type so tests (and the harness JSON dump) consume it directly.
struct NegativeBankSuppressionFireCounts: Sendable, Equatable {
    /// Number of candidate spans whose `skipConfidence` was actually LOWERED by
    /// the negative-bank suppression (a confirmed false-positive alignment cleared
    /// the bank's threshold AND moved the confidence). This is the signal's "fire"
    /// count. With an EMPTY bank (cold start, no user reverts) this is always 0.
    var suppressedSpans: Int = 0
    /// Number of candidate spans the suppression pass evaluated for this asset
    /// (the denominator — every span reached on the decision path while the flag
    /// was on and a bank was wired).
    var candidateSpans: Int = 0
}

actor NegativeBankSuppressionObserver {

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "NegativeBankSuppressionObserver"
    )

    /// Per-asset accumulated fire counts.
    private var counts: [String: NegativeBankSuppressionFireCounts] = [:]

    init() {}

    /// Record one candidate span's negative-bank suppression outcome for an asset.
    ///
    /// The fire site calls this once per span reached on the decision path (the
    /// denominator), with `didSuppress` true ONLY when the suppression actually
    /// changed `skipConfidence`. This method only accumulates; it derives nothing
    /// and never feeds back into the decision.
    ///
    /// - Parameters:
    ///   - assetId: analysis asset id the span belongs to.
    ///   - didSuppress: whether the negative-bank suppression moved the span's
    ///     `skipConfidence` (the EXACT condition the production decision used).
    func record(assetId: String, didSuppress: Bool) {
        var tally = counts[assetId, default: NegativeBankSuppressionFireCounts()]
        tally.candidateSpans += 1
        if didSuppress { tally.suppressedSpans += 1 }
        counts[assetId] = tally
    }

    /// The accumulated fire counts for an asset (zeroed defaults if none seen).
    func fireCounts(for assetId: String) -> NegativeBankSuppressionFireCounts {
        counts[assetId, default: NegativeBankSuppressionFireCounts()]
    }
}

// MARK: - xsdz.11 per-show threshold offset

/// Per-asset fire tally for the xsdz.11 per-show auto-skip threshold controller.
/// Pure value type so tests (and the harness JSON dump) consume it directly.
struct PerShowThresholdOffsetFireCounts: Sendable, Equatable {
    /// The per-show OFFSET the controller resolved for this backfill (the value
    /// the gate applies on the `.standard` track). With an EMPTY controller store
    /// (cold start, no corrections) this is always 0. Stored as the LAST resolved
    /// offset for the asset (a backfill resolves exactly one offset per run; a
    /// re-run would overwrite, which is fine — the live harness runs each episode
    /// once per arm).
    var resolvedOffset: Double = 0
    /// Number of `.standard`-track spans whose EFFECTIVE auto-skip threshold the
    /// offset actually shifted (offset != 0). This is the signal's "fire" count.
    /// 0 whenever the resolved offset is 0.
    var thresholdShiftedSpans: Int = 0
    /// Number of candidate spans the gate evaluated for this asset (the
    /// denominator).
    var candidateSpans: Int = 0
}

actor PerShowThresholdOffsetObserver {

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "PerShowThresholdOffsetObserver"
    )

    /// Per-asset accumulated fire counts.
    private var counts: [String: PerShowThresholdOffsetFireCounts] = [:]

    init() {}

    /// Record the per-show OFFSET the controller resolved for one backfill of an
    /// asset. Called once per backfill BEFORE the emission loop. Overwrites any
    /// prior resolved offset for the asset (one offset per run).
    func recordResolvedOffset(assetId: String, offset: Double) {
        var tally = counts[assetId, default: PerShowThresholdOffsetFireCounts()]
        tally.resolvedOffset = offset
        counts[assetId] = tally
    }

    /// Record one candidate span's per-show threshold gate outcome for an asset.
    ///
    /// Called once per span reached at the hard auto-skip gate (the denominator),
    /// with `didShiftThreshold` true ONLY when the offset actually moved the
    /// EFFECTIVE threshold the gate used (the EXACT condition the production gate
    /// used: flag on, offset != 0, `.standard` track). Accumulates only.
    func recordSpan(assetId: String, didShiftThreshold: Bool) {
        var tally = counts[assetId, default: PerShowThresholdOffsetFireCounts()]
        tally.candidateSpans += 1
        if didShiftThreshold { tally.thresholdShiftedSpans += 1 }
        counts[assetId] = tally
    }

    /// The accumulated fire counts for an asset (zeroed defaults if none seen).
    func fireCounts(for assetId: String) -> PerShowThresholdOffsetFireCounts {
        counts[assetId, default: PerShowThresholdOffsetFireCounts()]
    }
}
