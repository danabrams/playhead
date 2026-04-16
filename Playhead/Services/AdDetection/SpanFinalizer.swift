// SpanFinalizer.swift
// Phase ef2.4.2: Deterministic span finalizer — safety layer after fusion.
//
// Design:
//   • Pure, stateless struct — same input always produces same output.
//   • Applies hard constraints in fixed order as a pipeline of transforms:
//     1. Non-overlap resolution (higher confidence wins)
//     2. Minimum content gap enforcement (< 3s gap → merge)
//     3. Duration sanity (< 5s dropped, > 180s split)
//     4. Chapter penalties (.content chapters → .markOnly gate)
//     5. Action cap (no more than 50% of episode auto-skipped)
//     6. User skip-policy overrides applied last
//   • Operates on the OUTPUT of fusion, not inside it.
//   • Does NOT modify BackfillEvidenceFusion or DecisionMapper.

import Foundation
import OSLog

// MARK: - FinalizedSpan

/// A span with its fusion decision, after finalizer constraints have been applied.
/// Carries a trace of which constraints fired for diagnostics.
struct FinalizedSpan: Sendable, Equatable {
    let span: DecodedSpan
    let decision: DecisionResult
    /// Which finalizer constraints modified this span. Empty if the span passed through unchanged.
    let constraintTrace: [FinalizerConstraint]
    /// The skip-policy action assigned by the policy override step.
    let policyAction: SkipPolicyAction
}

/// Identifies which finalizer constraint fired on a span.
enum FinalizerConstraint: String, Sendable, Equatable {
    case overlapTrimmed
    case overlapSuppressed
    case mergedWithAdjacent
    case droppedBelowMinDuration
    case splitAboveMaxDuration
    case chapterPenaltyApplied
    case actionCapApplied
    case policyOverrideApplied
}

// MARK: - CandidateSpan

/// Input to the finalizer: a span paired with its fusion decision.
struct CandidateSpan: Sendable {
    let span: DecodedSpan
    let decision: DecisionResult
    /// Commercial intent + ownership for policy matrix lookup.
    let commercialIntent: CommercialIntent
    let adOwnership: AdOwnership
}

// MARK: - ChapterMarker

/// Minimal chapter info the finalizer needs to enforce chapter penalties.
struct ChapterMarker: Sendable {
    let startTime: Double
    let endTime: Double
    let isContent: Bool
}

// MARK: - SpanFinalizer

/// Deterministic safety layer applied after fusion produces decisions.
/// Enforces hard invariants regardless of upstream behavior.
struct SpanFinalizer: Sendable {
    private static let logger = Logger(subsystem: "com.playhead", category: "SpanFinalizer")

    /// Minimum seconds of content between adjacent ad spans. Below this, merge.
    static let minimumContentGapSeconds: Double = 3.0

    /// Maximum fraction of episode duration that can be auto-skipped.
    static let maxAutoSkipFraction: Double = 0.50

    /// Total episode duration in seconds (needed for the 50% cap).
    let episodeDuration: Double
    /// Content chapters for chapter-penalty enforcement.
    let chapters: [ChapterMarker]

    // MARK: - Public API

    /// Apply all finalizer constraints to the candidate spans.
    /// Returns finalized spans in time order, with constraint traces.
    func finalize(_ candidates: [CandidateSpan]) -> [FinalizedSpan] {
        // Sort by start time for deterministic processing.
        // Secondary sort by span id for determinism when start times are equal.
        let sorted = candidates.sorted { a, b in
            if a.span.startTime < b.span.startTime { return true }
            if a.span.startTime > b.span.startTime { return false }
            return a.span.id < b.span.id
        }

        // Build mutable working copies.
        var working = sorted.map { WorkingSpan(candidate: $0) }

        // Pipeline: each constraint applied in order.
        working = resolveOverlaps(working)
        working = enforceMinimumContentGap(working)
        working = enforceDurationSanity(working)
        working = applyChapterPenalties(working)
        working = enforceActionCap(working)
        working = applyPolicyOverrides(working)

        return working.map { $0.toFinalized() }
    }

    // MARK: - Constraint 1: Non-overlap

    /// Higher-confidence span wins. Loser is trimmed if possible, suppressed if fully contained.
    private func resolveOverlaps(_ spans: [WorkingSpan]) -> [WorkingSpan] {
        guard spans.count > 1 else { return spans }

        var result = spans
        var changed = true

        // Iterate until no overlaps remain (trimming can create new adjacencies).
        while changed {
            changed = false
            result.sort { a, b in
                if a.startTime < b.startTime { return true }
                if a.startTime > b.startTime { return false }
                return a.spanId < b.spanId
            }

            var i = 0
            while i < result.count - 1 {
                let a = result[i]
                let b = result[i + 1]

                guard a.endTime > b.startTime else {
                    i += 1
                    continue
                }

                // Overlap detected. Higher confidence wins.
                let aWins = a.skipConfidence >= b.skipConfidence

                if aWins {
                    // Trim b's start to a's end.
                    if a.endTime >= b.endTime {
                        // b is fully contained — suppress it.
                        Self.logger.info("Overlap: suppressing span \(b.spanId) (fully contained by \(a.spanId))")
                        result[i + 1].suppress(.overlapSuppressed)
                        result.remove(at: i + 1)
                    } else {
                        Self.logger.info("Overlap: trimming start of span \(b.spanId) from \(b.startTime) to \(a.endTime)")
                        result[i + 1].trimStart(to: a.endTime, constraint: .overlapTrimmed)
                    }
                } else {
                    // Trim a's end to b's start.
                    if b.startTime <= a.startTime && b.endTime >= a.endTime {
                        // a is fully contained — suppress it.
                        Self.logger.info("Overlap: suppressing span \(a.spanId) (fully contained by \(b.spanId))")
                        result[i].suppress(.overlapSuppressed)
                        result.remove(at: i)
                    } else {
                        Self.logger.info("Overlap: trimming end of span \(a.spanId) from \(a.endTime) to \(b.startTime)")
                        result[i].trimEnd(to: b.startTime, constraint: .overlapTrimmed)
                    }
                }
                changed = true
            }
        }

        return result
    }

    // MARK: - Constraint 2: Minimum content gap

    /// Adjacent spans with < 3s content between them are merged.
    /// Merged span takes the higher confidence of the two.
    private func enforceMinimumContentGap(_ spans: [WorkingSpan]) -> [WorkingSpan] {
        guard spans.count > 1 else { return spans }

        var result = [spans[0]]

        for i in 1..<spans.count {
            let prev = result[result.count - 1]
            let curr = spans[i]
            let gap = curr.startTime - prev.endTime

            if gap < Self.minimumContentGapSeconds {
                Self.logger.info("Merging spans \(prev.spanId) and \(curr.spanId) (gap: \(gap)s < \(Self.minimumContentGapSeconds)s)")
                result[result.count - 1].mergeWith(curr)
            } else {
                result.append(curr)
            }
        }

        return result
    }

    // MARK: - Constraint 3: Duration sanity

    /// Spans < 5s are dropped. Spans > 180s are split at 180s boundaries.
    private func enforceDurationSanity(_ spans: [WorkingSpan]) -> [WorkingSpan] {
        var result: [WorkingSpan] = []

        for span in spans {
            let duration = span.endTime - span.startTime

            if duration < DecoderConstants.minDurationSeconds {
                Self.logger.info("Dropping span \(span.spanId) (duration \(duration)s < \(DecoderConstants.minDurationSeconds)s)")
                // Dropped — don't add to result.
                continue
            }

            if duration > DecoderConstants.maxDurationSeconds {
                Self.logger.info("Splitting span \(span.spanId) (duration \(duration)s > \(DecoderConstants.maxDurationSeconds)s)")
                let splits = splitSpan(span)
                result.append(contentsOf: splits)
            } else {
                result.append(span)
            }
        }

        return result
    }

    /// Recursively split a span at maxDurationSeconds boundaries.
    private func splitSpan(_ span: WorkingSpan) -> [WorkingSpan] {
        let duration = span.endTime - span.startTime
        guard duration > DecoderConstants.maxDurationSeconds else {
            return [span]
        }

        var splits: [WorkingSpan] = []
        var cursor = span.startTime

        while cursor < span.endTime {
            let chunkEnd = min(cursor + DecoderConstants.maxDurationSeconds, span.endTime)
            let remaining = chunkEnd - cursor
            // Don't create a tiny trailing fragment.
            if remaining < DecoderConstants.minDurationSeconds && !splits.isEmpty {
                // Extend the previous split to absorb the remainder.
                splits[splits.count - 1].extendEnd(to: span.endTime, constraint: .splitAboveMaxDuration)
                break
            }
            var chunk = span.copy(startTime: cursor, endTime: chunkEnd)
            chunk.addConstraint(.splitAboveMaxDuration)
            splits.append(chunk)
            cursor = chunkEnd
        }

        return splits
    }

    // MARK: - Constraint 4: Chapter penalties

    /// Spans crossing high-quality .content chapters are capped at .markOnly eligibility.
    private func applyChapterPenalties(_ spans: [WorkingSpan]) -> [WorkingSpan] {
        let contentChapters = chapters.filter { $0.isContent }
        guard !contentChapters.isEmpty else { return spans }

        return spans.map { span in
            var s = span
            let crossesContent = contentChapters.contains { chapter in
                // Span overlaps with a content chapter if their intervals intersect.
                span.startTime < chapter.endTime && span.endTime > chapter.startTime
            }
            if crossesContent {
                Self.logger.info("Chapter penalty: span \(span.spanId) crosses content chapter → markOnly")
                s.capEligibility(.markOnly, constraint: .chapterPenaltyApplied)
            }
            return s
        }
    }

    // MARK: - Constraint 5: Action cap (50%)

    /// No more than 50% of episode duration can be auto-skip eligible.
    /// When over budget, lowest-confidence spans are demoted to .detectOnly first.
    private func enforceActionCap(_ spans: [WorkingSpan]) -> [WorkingSpan] {
        guard episodeDuration > 0 else { return spans }

        let budget = episodeDuration * Self.maxAutoSkipFraction

        // Only count spans that are actually auto-skip eligible.
        var autoSkipSpans: [(index: Int, confidence: Double, duration: Double)] = []
        for (i, span) in spans.enumerated() {
            if span.eligibilityGate == .eligible {
                autoSkipSpans.append((i, span.skipConfidence, span.endTime - span.startTime))
            }
        }

        let totalAutoSkip = autoSkipSpans.reduce(0.0) { $0 + $1.duration }
        guard totalAutoSkip > budget else { return spans }

        Self.logger.info("Action cap: \(totalAutoSkip)s exceeds \(budget)s budget (\(Self.maxAutoSkipFraction * 100)% of \(self.episodeDuration)s)")

        var result = spans
        // Sort by ascending confidence — demote lowest confidence first.
        // Secondary sort by index for determinism when confidences are equal.
        let sorted = autoSkipSpans.sorted { a, b in
            if a.confidence < b.confidence { return true }
            if a.confidence > b.confidence { return false }
            return a.index > b.index  // higher index demoted first (later in episode)
        }

        var excess = totalAutoSkip - budget
        for item in sorted {
            guard excess > 0 else { break }
            result[item.index].capEligibility(.blockedByPolicy, constraint: .actionCapApplied)
            excess -= item.duration
        }

        return result
    }

    // MARK: - Constraint 6: Policy overrides

    /// Apply SkipPolicyMatrix action based on commercial intent and ad ownership.
    private func applyPolicyOverrides(_ spans: [WorkingSpan]) -> [WorkingSpan] {
        return spans.map { span in
            var s = span
            let action = SkipPolicyMatrix.action(for: span.commercialIntent, ownership: span.adOwnership)
            s.applyPolicyAction(action)
            return s
        }
    }
}

// MARK: - WorkingSpan (private mutable intermediate)

/// Mutable intermediate used during the finalizer pipeline.
/// Tracks accumulated constraint traces and allows time/gate modifications.
private struct WorkingSpan {
    var spanId: String
    var startTime: Double
    var endTime: Double
    var skipConfidence: Double
    var proposalConfidence: Double
    var eligibilityGate: SkipEligibilityGate
    var constraintTrace: [FinalizerConstraint]
    var policyAction: SkipPolicyAction
    var commercialIntent: CommercialIntent
    var adOwnership: AdOwnership

    // Original span for identity.
    var originalSpan: DecodedSpan

    init(candidate: CandidateSpan) {
        self.spanId = candidate.span.id
        self.startTime = candidate.span.startTime
        self.endTime = candidate.span.endTime
        self.skipConfidence = candidate.decision.skipConfidence
        self.proposalConfidence = candidate.decision.proposalConfidence
        self.eligibilityGate = candidate.decision.eligibilityGate
        self.constraintTrace = []
        self.policyAction = .autoSkipEligible
        self.commercialIntent = candidate.commercialIntent
        self.adOwnership = candidate.adOwnership
        self.originalSpan = candidate.span
    }

    mutating func trimStart(to newStart: Double, constraint: FinalizerConstraint) {
        startTime = newStart
        addConstraint(constraint)
    }

    mutating func trimEnd(to newEnd: Double, constraint: FinalizerConstraint) {
        endTime = newEnd
        addConstraint(constraint)
    }

    mutating func extendEnd(to newEnd: Double, constraint: FinalizerConstraint) {
        endTime = newEnd
        addConstraint(constraint)
    }

    mutating func suppress(_ constraint: FinalizerConstraint) {
        addConstraint(constraint)
    }

    mutating func mergeWith(_ other: WorkingSpan) {
        endTime = other.endTime
        skipConfidence = max(skipConfidence, other.skipConfidence)
        proposalConfidence = max(proposalConfidence, other.proposalConfidence)
        addConstraint(.mergedWithAdjacent)
    }

    mutating func capEligibility(_ gate: SkipEligibilityGate, constraint: FinalizerConstraint) {
        // Only apply the cap if it makes the gate more restrictive (demotion).
        // Severity ordering: eligible < markOnly < blocked states.
        // A demotion (higher severity) is always allowed; a promotion (lower severity) is not.
        guard gate.severity > eligibilityGate.severity else { return }
        eligibilityGate = gate
        addConstraint(constraint)
    }

    mutating func applyPolicyAction(_ action: SkipPolicyAction) {
        policyAction = action
        if action != .autoSkipEligible {
            addConstraint(.policyOverrideApplied)
        }
    }

    mutating func addConstraint(_ constraint: FinalizerConstraint) {
        constraintTrace.append(constraint)
    }

    func copy(startTime: Double, endTime: Double) -> WorkingSpan {
        var copy = self
        copy.startTime = startTime
        copy.endTime = endTime
        return copy
    }

    func toFinalized() -> FinalizedSpan {
        // Build a modified span with adjusted times.
        let adjustedSpan = DecodedSpan(
            id: originalSpan.id,
            assetId: originalSpan.assetId,
            firstAtomOrdinal: originalSpan.firstAtomOrdinal,
            lastAtomOrdinal: originalSpan.lastAtomOrdinal,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: originalSpan.anchorProvenance
        )
        let adjustedDecision = DecisionResult(
            proposalConfidence: proposalConfidence,
            skipConfidence: skipConfidence,
            eligibilityGate: eligibilityGate
        )
        return FinalizedSpan(
            span: adjustedSpan,
            decision: adjustedDecision,
            constraintTrace: constraintTrace,
            policyAction: policyAction
        )
    }
}
