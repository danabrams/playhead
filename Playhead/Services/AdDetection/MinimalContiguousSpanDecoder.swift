// MinimalContiguousSpanDecoder.swift
// Phase 5 (playhead-4my.5.2): Converts [AtomEvidence] into [DecodedSpan].
//
// Algorithm (rule order is pinned — must stay in this order for determinism):
//   1. FORM RUNS: contiguous anchored + non-vetoed atoms → candidate spans
//   2. MERGE: adjacent candidates with gap < mergeGapSeconds (no veto, no acoustic break in gap)
//   3. SPLIT: spans > MAX_DURATION → split at longest internal gap (recurse)
//   4. USE A: boundary snap → snap edges to nearest acoustic break within ±snapRadiusSeconds
//   5. DROP: spans < MIN_DURATION → drop
//
// Precision-first invariants:
//   • No span without an upstream anchor.
//   • .userVetoed atoms are excluded from all candidate sets.
//   • .userConfirmed atoms do NOT create spans on their own (no anchor = no span).
//   • decode(atoms, id) == decode(atoms, id) (determinism guaranteed by fixed rule order).
//
// All constants are in DecoderConstants (universal, not per-show).

import Foundation
import OSLog

// MARK: - MinimalContiguousSpanDecoder

struct MinimalContiguousSpanDecoder {
    struct Configuration: Sendable, Equatable {
        let mergeGapSeconds: Double
        let snapRadiusSeconds: Double

        static let `default` = Configuration(
            mergeGapSeconds: 3.0,
            snapRadiusSeconds: 8.0
        )
    }

    enum BoundaryOwnership: Sendable {
        case legacyEvidence
        case hypothesisOwned
    }

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "MinimalContiguousSpanDecoder"
    )

    private let config: Configuration

    init(config: Configuration = .default) {
        self.config = config
    }

    // MARK: - Public API

    /// Decode a sequence of AtomEvidence records into DecodedSpans.
    ///
    /// - Parameters:
    ///   - atoms: Per-atom annotations from AtomEvidenceProjector.
    ///   - assetId: The analysis asset ID (used for DecodedSpan.id computation).
    /// - Returns: Decoded ad spans, sorted by startTime.
    ///
    // Determinism note: decode(atoms, id) == decode(atoms, id) when given identical AtomEvidence
    // input — verified by tests. Full-pipeline determinism (project → decode → re-project → decode)
    // is NOT guaranteed if span boundary ordinals are fed back as inputs to a second projection,
    // because Use A can expand boundaries beyond the original anchored range. Phase 6 must not
    // assume re-projection is deterministic across boundary changes. Tracked as known risk in the
    // Phase 5 design doc (docs/plans/2026-04-09-phase-5-design.md).
    func decode(
        atoms: [AtomEvidence],
        assetId: String,
        boundaryOwnership: BoundaryOwnership = .legacyEvidence
    ) -> [DecodedSpan] {
        guard !atoms.isEmpty else { return [] }

        let sortedAtoms = atoms.sorted { $0.atomOrdinal < $1.atomOrdinal }

        // Step 1: FORM RUNS
        let candidates = formRuns(sortedAtoms)
        guard !candidates.isEmpty else { return [] }

        // Shared lookup — built once, used by steps 2–4b.
        let atomsByOrdinal = Dictionary(uniqueKeysWithValues: sortedAtoms.map {
            ($0.atomOrdinal, $0)
        })

        // Step 2: MERGE
        let merged = mergeAdjacentCandidates(candidates, atomsByOrdinal: atomsByOrdinal)

        // Step 3: SPLIT
        let split = merged.flatMap { splitIfNeeded($0, allAtoms: sortedAtoms, atomsByOrdinal: atomsByOrdinal) }

        // Step 4: USE A — boundary snap
        let snapped: [CandidateSpan]
        switch boundaryOwnership {
        case .legacyEvidence:
            snapped = split.map { applyBoundarySnap($0, allAtoms: sortedAtoms) }
        case .hypothesisOwned:
            snapped = split
        }

        // Step 4b: Resolve overlaps introduced by boundary snap.
        // After snap, adjacent spans may overlap. Clip the later span's start
        // to the earlier span's end so downstream skip/banner logic never sees
        // overlapping time ranges.
        let resolved = resolveOverlaps(snapped, atomsByOrdinal: atomsByOrdinal)

        // Step 5: DROP — remove micro-fragments
        let kept = resolved.filter { $0.duration >= DecoderConstants.minDurationSeconds }

        // Build DecodedSpans from surviving candidates.
        let spans = kept.map { candidate -> DecodedSpan in
            let id = DecodedSpan.makeId(
                assetId: assetId,
                firstAtomOrdinal: candidate.firstOrdinal,
                lastAtomOrdinal: candidate.lastOrdinal
            )
            return DecodedSpan(
                id: id,
                assetId: assetId,
                firstAtomOrdinal: candidate.firstOrdinal,
                lastAtomOrdinal: candidate.lastOrdinal,
                startTime: candidate.startTime,
                endTime: candidate.endTime,
                anchorProvenance: candidate.anchorProvenance
            )
        }

        Self.logger.info(
            "MinimalContiguousSpanDecoder: \(sortedAtoms.count) atoms → \(candidates.count) runs → \(merged.count) merged → \(split.count) split → \(spans.count) final spans"
        )

        return spans.sorted { $0.startTime < $1.startTime }
    }

    // MARK: - Candidate spans (internal representation)

    private struct CandidateSpan {
        var firstOrdinal: Int
        var lastOrdinal: Int
        var startTime: Double
        var endTime: Double
        var anchorProvenance: [AnchorRef]

        var duration: Double { endTime - startTime }
    }

    // MARK: - Step 1: FORM RUNS

    private func formRuns(_ sortedAtoms: [AtomEvidence]) -> [CandidateSpan] {
        var result: [CandidateSpan] = []
        var current: CandidateSpan? = nil

        for atom in sortedAtoms {
            // An atom participates in a run if it is anchored AND not vetoed.
            let participates = atom.isAnchored && atom.correctionMask != .userVetoed

            if participates {
                if var span = current, atom.atomOrdinal == span.lastOrdinal + 1 {
                    // Extend the current run (ordinal is contiguous with previous atom).
                    span.lastOrdinal = atom.atomOrdinal
                    span.endTime = atom.endTime
                    // Merge anchor provenance, dedup by content.
                    span.anchorProvenance = mergeProvenance(span.anchorProvenance, atom.anchorProvenance)
                    current = span
                } else {
                    // Close current run if open (ordinal gap or first atom).
                    if let span = current {
                        result.append(span)
                    }
                    // Start a new run.
                    current = CandidateSpan(
                        firstOrdinal: atom.atomOrdinal,
                        lastOrdinal: atom.atomOrdinal,
                        startTime: atom.startTime,
                        endTime: atom.endTime,
                        anchorProvenance: atom.anchorProvenance
                    )
                }
            } else {
                // Break: close current run if open.
                if let span = current {
                    result.append(span)
                    current = nil
                }
            }
        }

        // Close any open run.
        if let span = current {
            result.append(span)
        }

        return result
    }

    // MARK: - Step 2: MERGE

    /// Merge two adjacent candidate spans if:
    ///   1. Gap (in seconds) < mergeGapSeconds
    ///   2. No .userVetoed atom in the gap
    ///   3. No atom in the gap has hasAcousticBreakHint (Use B anti-merge)
    private func mergeAdjacentCandidates(
        _ candidates: [CandidateSpan],
        atomsByOrdinal: [Int: AtomEvidence]
    ) -> [CandidateSpan] {
        guard candidates.count > 1 else { return candidates }

        var result: [CandidateSpan] = [candidates[0]]

        for i in 1 ..< candidates.count {
            let prev = result[result.count - 1]
            let next = candidates[i]

            let gapSeconds = next.startTime - prev.endTime

            if gapSeconds < config.mergeGapSeconds && gapSeconds >= 0 {
                // Check gap atoms for veto or acoustic break hint.
                let gapOrdinals = (prev.lastOrdinal + 1) ..< next.firstOrdinal
                let gapAtoms = gapOrdinals.compactMap { atomsByOrdinal[$0] }

                let hasVetoedGap = gapAtoms.contains { $0.correctionMask == .userVetoed }
                let hasAcousticBreakInGap = gapAtoms.contains { $0.hasAcousticBreakHint }  // Use B

                if !hasVetoedGap && !hasAcousticBreakInGap {
                    // Merge: extend previous span to cover next.
                    var merged = result[result.count - 1]
                    merged.lastOrdinal = next.lastOrdinal
                    merged.endTime = next.endTime
                    merged.anchorProvenance = mergeProvenance(merged.anchorProvenance, next.anchorProvenance)
                    result[result.count - 1] = merged
                    continue
                }
            }

            result.append(next)
        }

        return result
    }

    // MARK: - Step 3: SPLIT

    /// Recursively split any span above MAX_DURATION at its longest internal gap.
    private func splitIfNeeded(
        _ span: CandidateSpan,
        allAtoms: [AtomEvidence],
        atomsByOrdinal: [Int: AtomEvidence]
    ) -> [CandidateSpan] {
        guard span.duration > DecoderConstants.maxDurationSeconds else { return [span] }

        // Find atoms in this span range.
        let spanAtoms = (span.firstOrdinal ... span.lastOrdinal)
            .compactMap { atomsByOrdinal[$0] }
            .sorted { $0.atomOrdinal < $1.atomOrdinal }

        // Find the longest gap of unanchored atoms for the split point.
        // A "gap" is a maximal run of non-anchored atoms within the span.
        struct Gap {
            let firstOrdinal: Int
            let lastOrdinal: Int
            var length: Int { lastOrdinal - firstOrdinal + 1 }
            var midOrdinal: Int { (firstOrdinal + lastOrdinal) / 2 }
        }

        var gaps: [Gap] = []
        var gapStart: Int? = nil

        for atom in spanAtoms {
            let isGapAtom = !atom.isAnchored || atom.correctionMask == .userVetoed
            if isGapAtom {
                if gapStart == nil { gapStart = atom.atomOrdinal }
            } else {
                if let start = gapStart {
                    gaps.append(Gap(firstOrdinal: start, lastOrdinal: atom.atomOrdinal - 1))
                    gapStart = nil
                }
            }
        }
        if let start = gapStart, let last = spanAtoms.last {
            gaps.append(Gap(firstOrdinal: start, lastOrdinal: last.atomOrdinal))
        }

        // Pick the longest gap (leftmost on tie) as the split point.
        let splitGap: Gap?
        if let longest = gaps.max(by: { lhs, rhs in
            if lhs.length == rhs.length {
                return lhs.firstOrdinal > rhs.firstOrdinal  // leftmost wins (smaller ordinal)
            }
            return lhs.length < rhs.length
        }) {
            splitGap = longest
        } else {
            // 100%-anchored span above MAX → split at midpoint ordinal.
            let mid = (span.firstOrdinal + span.lastOrdinal) / 2
            splitGap = Gap(firstOrdinal: mid, lastOrdinal: mid)
        }

        guard let gap = splitGap else { return [span] }

        // Reconstruct left and right sub-spans around the gap.
        let leftAtoms = spanAtoms.filter { $0.atomOrdinal < gap.firstOrdinal }
        let rightAtoms = spanAtoms.filter { $0.atomOrdinal > gap.lastOrdinal }

        var results: [CandidateSpan] = []

        if let first = leftAtoms.first, let last = leftAtoms.last {
            let left = CandidateSpan(
                firstOrdinal: first.atomOrdinal,
                lastOrdinal: last.atomOrdinal,
                startTime: first.startTime,
                endTime: last.endTime,
                anchorProvenance: leftAtoms.flatMap(\.anchorProvenance).uniqued()
            )
            results.append(contentsOf: splitIfNeeded(left, allAtoms: allAtoms, atomsByOrdinal: atomsByOrdinal))
        }

        if let first = rightAtoms.first, let last = rightAtoms.last {
            let right = CandidateSpan(
                firstOrdinal: first.atomOrdinal,
                lastOrdinal: last.atomOrdinal,
                startTime: first.startTime,
                endTime: last.endTime,
                anchorProvenance: rightAtoms.flatMap(\.anchorProvenance).uniqued()
            )
            results.append(contentsOf: splitIfNeeded(right, allAtoms: allAtoms, atomsByOrdinal: atomsByOrdinal))
        }

        return results.isEmpty ? [span] : results
    }

    // MARK: - Step 4: USE A — Boundary Snap

    // Use A: snap to the OUTERMOST qualifying break within the radius window, not the nearest.
    // Left edge: earliest break (furthest left) to maximize leftward expansion.
    // Right edge: latest break (furthest right) to maximize rightward expansion.
    // This differs from Use C (nearest-break) which identifies corroboration for a specific FM hit.

    /// Snap span boundaries to nearby acoustic break atoms within the configured
    /// time radius.
    /// Use A only adjusts edges — never creates spans.
    private func applyBoundarySnap(
        _ span: CandidateSpan,
        allAtoms: [AtomEvidence]
    ) -> CandidateSpan {
        let radius = config.snapRadiusSeconds
        var result = span

        // Left edge: select the earliest (furthest left) qualifying break atom
        // in the configured time window to maximize leftward expansion.
        if let snapAtom = allAtoms.first(where: { atom in
            atom.hasAcousticBreakHint
                && atom.startTime >= span.startTime - radius
                && atom.startTime <= span.startTime + radius
        }) {
            result.firstOrdinal = snapAtom.atomOrdinal
            result.startTime = snapAtom.startTime
        }

        // Right edge: select the latest (furthest right) qualifying break atom
        // in the configured time window to maximize rightward expansion.
        if let snapAtom = allAtoms.last(where: { atom in
            atom.hasAcousticBreakHint
                && atom.endTime >= span.endTime - radius
                && atom.endTime <= span.endTime + radius
        }) {
            result.lastOrdinal = snapAtom.atomOrdinal
            result.endTime = snapAtom.endTime
        }

        return result
    }

    // MARK: - Step 4b: Overlap Resolution

    /// After boundary snap, adjacent spans may have expanded into each other's
    /// territory. This clips later spans so no two spans overlap in time.
    private func resolveOverlaps(_ spans: [CandidateSpan], atomsByOrdinal: [Int: AtomEvidence]) -> [CandidateSpan] {
        guard spans.count > 1 else { return spans }

        var sorted = spans.sorted { $0.firstOrdinal < $1.firstOrdinal }
        for i in 1 ..< sorted.count {
            if sorted[i].firstOrdinal <= sorted[i - 1].lastOrdinal {
                // Clip later span's start to one past the earlier span's end.
                sorted[i].firstOrdinal = sorted[i - 1].lastOrdinal + 1
                // Find the atom for the new start ordinal to update startTime.
                // If the clip collapses the span (firstOrdinal > lastOrdinal),
                // it will be dropped by the MIN_DURATION filter in Step 5.
                if sorted[i].firstOrdinal <= sorted[i].lastOrdinal {
                    if let atom = atomsByOrdinal[sorted[i].firstOrdinal] {
                        sorted[i].startTime = atom.startTime
                    } else {
                        sorted[i].startTime = sorted[i - 1].endTime
                    }
                }
            }
        }
        // Drop any spans that were fully consumed by clipping.
        return sorted.filter { $0.firstOrdinal <= $0.lastOrdinal }
    }

    // MARK: - Helpers

    /// Merge two anchor provenance arrays, deduplicating by value equality.
    private func mergeProvenance(_ lhs: [AnchorRef], _ rhs: [AnchorRef]) -> [AnchorRef] {
        var seen = Set<String>()
        var result: [AnchorRef] = []
        for ref in lhs + rhs {
            let key = anchorRefKey(ref)
            if seen.insert(key).inserted {
                result.append(ref)
            }
        }
        return result
    }

    private func anchorRefKey(_ ref: AnchorRef) -> String {
        switch ref {
        case .fmConsensus(let id, let s): return "fmC:\(id):\(s)"
        case .evidenceCatalog(let e): return "ev:\(e.evidenceRef):\(e.atomOrdinal)"
        case .fmAcousticCorroborated(let id, let s): return "fmA:\(id):\(s)"
        case .userCorrection(let id, let t): return "uC:\(id):\(t)"
        case .classifierSeed(let id, let s): return "cS:\(id):\(s)"
        }
    }
}

// MARK: - Array Uniquing helper

private extension Array where Element == AnchorRef {
    func uniqued() -> [AnchorRef] {
        var seen = Set<String>()
        return filter { ref in
            let key: String
            switch ref {
            case .fmConsensus(let id, let s): key = "fmC:\(id):\(s)"
            case .evidenceCatalog(let e): key = "ev:\(e.evidenceRef):\(e.atomOrdinal)"
            case .fmAcousticCorroborated(let id, let s): key = "fmA:\(id):\(s)"
            case .userCorrection(let id, let t): key = "uC:\(id):\(t)"
            case .classifierSeed(let id, let s): key = "cS:\(id):\(s)"
            }
            return seen.insert(key).inserted
        }
    }
}
