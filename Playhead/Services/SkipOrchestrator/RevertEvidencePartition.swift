// RevertEvidencePartition.swift
// playhead-1mq1.2.1: which parts of a REVERTED ad window may become a
// negative label.
//
// THE DEFECT THIS EXISTS FOR
// --------------------------
// On THEMOVE the user reverted the auto window 3493.02–3536.90 because its
// WIDTH was wrong: the window opened on the show sign-off (through 3498.48),
// then silence, then a REAL ad ran 3505.74–3536.10. One tap is the only
// gesture available, so the correction seams read it as "the whole window was
// a false positive" and turned ~30s of TRUE ad into negative training
// material — a hard-negative copy row that suppresses that ad forever on this
// show, and a fuzzy catalog/repeated-ad revocation sweep keyed on a
// fingerprint taken over audio that is mostly the ad itself.
//
// THE RULE
// --------
// A revert says at least one thing reliably: *these boundaries were wrong*. It
// does NOT reliably say "there is no ad anywhere in here". So negatives are
// attributed by evidence:
//
//   • CLEAN  — no strong ad evidence localized inside the window, or evidence
//              that covers the window end to end. The user contradicted the
//              whole thing; the whole span may carry the negative. This is the
//              pre-existing behaviour and the common case, including the
//              genuine "that ad-sounding copy was not an ad" correction the
//              hard-negative bank exists to learn from.
//   • MIXED  — strong ad evidence sits in a PROPER part of the window and a
//              substantial evidence-free remainder is left over. That is the
//              width-is-wrong signature. Negatives may attach ONLY to the
//              remainder; nothing may attach to the evidenced subspan.
//
// Deliberately NOT treated as internal evidence: the reverted window's own
// window-wide provenance (`claimsCatalogMatch`, a rediff-byte-exact edge
// anchor, its fused confidence). Those are evidence for the WHOLE window —
// exactly the claim the user just contradicted — so admitting them would mark
// every catalog-matched or rediff-anchored revert as MIXED and make it
// permanently unlearnable. Internal evidence must be strictly narrower than
// the window to say anything the revert did not already overrule.

import Foundation

enum RevertEvidencePartition {

    // MARK: - Tuning

    /// Ad copy extends past the phrase that matched. A URL or promo code is a
    /// point observation inside a read that runs before and after it, so each
    /// anchor claims this much on either side before the remainder is
    /// considered evidence-free.
    static let evidenceHalo: Double = 8.0

    /// Anchors from ONE read arrive as several short entries (disclosure, then
    /// URL, then promo code). Bridging gaps up to this width keeps them a
    /// single contiguous ad region instead of manufacturing "evidence-free"
    /// holes in the middle of an ad.
    static let maximumEvidenceBridgeGap: Double = 30.0

    /// A remainder shorter than this carries no usable copy or fingerprint, so
    /// treating it as attributable would only add noise. Below this width the
    /// evidence is taken to cover the window (CLEAN, not MIXED).
    static let minimumNegativeSubspanDuration: Double = 3.0

    /// Lexical evidence categories precise enough to assert "an ad is being
    /// read here". `.ctaPhrase` and `.brandSpan` are deliberately excluded:
    /// `EvidenceCatalogBuilder` only extracts them near an anchor already, and
    /// they fire on ordinary speech often enough that admitting them would
    /// classify editorial content as ad evidence. This mirrors the strong set
    /// `AutoSkipPrecisionGate.isStrongLexicalAdPhrase` uses on the
    /// `LexicalPatternCategory` side.
    static let strongLexicalCategories: Set<EvidenceCategory> = [
        .url,
        .promoCode,
        .disclosurePhrase,
    ]

    // MARK: - Interval

    struct Interval: Sendable, Equatable {
        let startTime: Double
        let endTime: Double

        init(startTime: Double, endTime: Double) {
            self.startTime = startTime
            self.endTime = endTime
        }

        var duration: Double { endTime - startTime }

        var isWellFormed: Bool {
            startTime.isFinite && endTime.isFinite && endTime > startTime
        }

        /// Closed-interval containment. Used to decide whether a feature window
        /// may contribute to a negative fingerprint: only material lying
        /// ENTIRELY inside an attributable subspan qualifies, so audio that
        /// straddles the edge of the evidenced ad can never leak into it.
        func fullyContains(startTime other: Double, endTime otherEnd: Double) -> Bool {
            other >= startTime && otherEnd <= endTime
        }
    }

    // MARK: - Partition

    struct Partition: Sendable, Equatable {
        /// The reverted span, verbatim.
        let span: Interval
        /// Merged strong ad evidence found strictly inside `span`. Empty for a
        /// CLEAN partition.
        let adEvidence: [Interval]
        /// The spans a negative label may be attributed to. `[span]` when
        /// CLEAN; the evidence-free remainders when MIXED.
        let negativeAttributionSpans: [Interval]
        /// True when the evidence sits in a proper part of the window and a
        /// substantial remainder is left over — the width-is-wrong signature.
        let isMixed: Bool

        /// A whole-span negative label is admissible only for a CLEAN
        /// partition. MIXED defers: the gesture cannot tell us which part the
        /// user meant, and guessing costs a real ad.
        var allowsWholeSpanNegativeLabel: Bool { !isMixed }

        /// Whether material spanning `[start, end]` may contribute to a
        /// negative label derived from this revert.
        func allowsNegativeAttribution(
            startTime: Double,
            endTime: Double
        ) -> Bool {
            negativeAttributionSpans.contains {
                $0.fullyContains(startTime: startTime, endTime: endTime)
            }
        }
    }

    // MARK: - Resolution

    /// Partition a reverted span against the strong ad evidence found inside
    /// it.
    ///
    /// A malformed span resolves CLEAN over itself: the callers validate their
    /// own bounds, and failing open here keeps a corrupt row on exactly the
    /// path it had before this guard existed rather than silently disabling
    /// revocation for it.
    static func resolve(
        span: Interval,
        adEvidence: [Interval]
    ) -> Partition {
        guard span.isWellFormed else {
            return Partition(
                span: span,
                adEvidence: [],
                negativeAttributionSpans: [span],
                isMixed: false
            )
        }

        let evidence = mergedEvidence(in: span, candidates: adEvidence)
        guard !evidence.isEmpty else {
            return Partition(
                span: span,
                adEvidence: [],
                negativeAttributionSpans: [span],
                isMixed: false
            )
        }

        let remainders = complement(of: evidence, in: span)
            .filter { $0.duration >= minimumNegativeSubspanDuration }
        guard !remainders.isEmpty else {
            // Evidence covers the window. The user contradicted all of it, so
            // the whole span stays attributable — this is the case the
            // hard-negative bank was built for.
            return Partition(
                span: span,
                adEvidence: evidence,
                negativeAttributionSpans: [span],
                isMixed: false
            )
        }

        return Partition(
            span: span,
            adEvidence: evidence,
            negativeAttributionSpans: remainders,
            isMixed: true
        )
    }

    /// Halo, clip, sort, and bridge raw evidence into the contiguous ad
    /// regions inside `span`.
    static func mergedEvidence(
        in span: Interval,
        candidates: [Interval]
    ) -> [Interval] {
        let clipped: [Interval] = candidates.compactMap { candidate in
            guard candidate.startTime.isFinite,
                  candidate.endTime.isFinite,
                  candidate.endTime >= candidate.startTime,
                  candidate.startTime - evidenceHalo <= span.endTime,
                  candidate.endTime + evidenceHalo >= span.startTime
            else {
                return nil
            }
            let start = max(span.startTime, candidate.startTime - evidenceHalo)
            let end = min(span.endTime, candidate.endTime + evidenceHalo)
            guard end > start else { return nil }
            return Interval(startTime: start, endTime: end)
        }
        guard !clipped.isEmpty else { return [] }

        let sorted = clipped.sorted {
            $0.startTime == $1.startTime
                ? $0.endTime < $1.endTime
                : $0.startTime < $1.startTime
        }
        var merged: [Interval] = []
        for interval in sorted {
            guard let last = merged.last else {
                merged.append(interval)
                continue
            }
            if interval.startTime - last.endTime <= maximumEvidenceBridgeGap {
                merged[merged.count - 1] = Interval(
                    startTime: last.startTime,
                    endTime: max(last.endTime, interval.endTime)
                )
            } else {
                merged.append(interval)
            }
        }
        return merged
    }

    /// The parts of `span` not covered by `evidence`. `evidence` must already
    /// be merged, clipped to `span`, and sorted.
    static func complement(
        of evidence: [Interval],
        in span: Interval
    ) -> [Interval] {
        var gaps: [Interval] = []
        var cursor = span.startTime
        for interval in evidence {
            if interval.startTime > cursor {
                gaps.append(
                    Interval(startTime: cursor, endTime: interval.startTime)
                )
            }
            cursor = max(cursor, interval.endTime)
        }
        if cursor < span.endTime {
            gaps.append(Interval(startTime: cursor, endTime: span.endTime))
        }
        return gaps
    }
}
