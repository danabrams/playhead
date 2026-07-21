// SpecialistScanPlanner.swift
// playhead-b6jq PR 4 (Phase B2): candidate-window selection for the specialist
// host-read scan phase.
//
// # What this is
//
// A pure, always-compiled planner (no CoreAI import) that picks the candidate
// windows the on-device specialist should classify during a backfill run. It is
// factored out — like `SpecialistFirstTokenSoftmax` in
// `SpecialistModelResources.swift` — precisely so the simulator FastTests can
// cover the candidate logic with synthetic segments/catalogs while the live
// engine that consumes the windows stays phone-gated.
//
// # Candidate gate (PR 4 MVP)
//
// The gate is the UNION of two FREE, already-trusted anchor sources the FM
// targeted phases also use:
//   * evidence-catalog anchors (`EvidenceCatalog.entries` — sponsor / promo /
//     URL / disclosure spans), and
//   * lexical-candidate spans (`LexicalScanner` over the segment text).
// Anchors are padded, merged into candidate regions, and tiled into fixed
// ~25s windows. Windows are ranked densest-cue-first and capped at `budget`
// (default 160) so a pathological episode can never scan more than ~one hour of
// windows. This deliberately caps recall at the lexical/catalog gate's recall
// (it misses cue-less host-reads); the `featureWindows` music-bed union is the
// recall lever — PR 4 ships it wired to `[]` and PR 5 evaluates turning it on
// where τ=0.7 makes precision/recall measurable.
//
// Reject by design: FM `semantic_scan_results` (circular) and full-episode
// (forbidden — the whole point is candidate narrowing).

import Foundation

/// One candidate window for the specialist to classify.
struct SpecialistScanWindow: Sendable, Equatable {
    /// Window start time in episode audio seconds.
    let startTime: Double
    /// Window end time in episode audio seconds.
    let endTime: Double
    /// `AdTranscriptSegment.segmentIndex` refs whose timespan overlaps the
    /// window — the segments whose text is joined into the classifier prompt.
    let lineRefs: [Int]
}

/// Pure, `Sendable`, always-compiled selector for specialist scan windows.
struct SpecialistScanPlanner: Sendable {
    /// Full-episode fallback CEILING, not a target: 3600s / 22.5s ≈ 160 windows
    /// is one whole hour tiled at the specialist window width. The normal
    /// candidate-gated case (2–4 breaks) lands far under this; the cap only
    /// bites pathological episodes so an unbounded lexical gate cannot fan the
    /// scan out across the entire transcript.
    static let defaultBudget = 160

    /// Fixed window width (seconds). Anchors are tiled into windows this wide.
    static let windowWidthSeconds = 25.0

    /// Padding (seconds) added on each side of an anchor before merging anchors
    /// into candidate regions. Lets nearby cues within a single ad read cluster
    /// into one region instead of fragmenting into many tiny windows.
    static let anchorPaddingSeconds = 5.0

    /// Flank (seconds) added on each side of a qualifying music-bed feature
    /// window before it becomes an anchor. Music-bed transitions mark the EDGE
    /// of a cue-less host-read, so the flank pulls the ad body into the region.
    static let musicBedFlankSeconds = 10.0

    /// Minimum `FeatureWindow.musicBedChangeScore` for a feature window to
    /// contribute a music-bed anchor. PR 4 ships `featureWindows: []`, so this
    /// gate is dormant until PR 5 evaluates the music-bed recall lever.
    static let musicBedChangeThreshold = 0.5

    /// Select the candidate windows the specialist should classify.
    ///
    /// - Parameters:
    ///   - segments: ordered transcript segments (the runner's `AssetInputs`).
    ///   - evidenceCatalog: deterministic commercial-evidence catalog.
    ///   - featureWindows: optional music-bed recall lever; PR 4 passes `[]`.
    ///   - budget: max windows to return (densest survive truncation).
    /// - Returns: candidate windows in DENSEST-CUE-FIRST order (ties → earliest
    ///   start), capped at `budget`. Empty when no anchors exist — never a
    ///   full-episode fallback.
    func selectWindows(
        segments: [AdTranscriptSegment],
        evidenceCatalog: EvidenceCatalog,
        featureWindows: [FeatureWindow] = [],
        budget: Int = defaultBudget
    ) -> [SpecialistScanWindow] {
        guard budget > 0 else { return [] }
        let ordered = segments.sorted { lhs, rhs in
            if lhs.segmentIndex == rhs.segmentIndex {
                return lhs.startTime < rhs.startTime
            }
            return lhs.segmentIndex < rhs.segmentIndex
        }
        guard let episodeStart = ordered.first?.startTime,
              let episodeEnd = ordered.last?.endTime,
              episodeStart < episodeEnd else { return [] }

        // 1. Collect UNPADDED anchor spans from every candidate source. These
        //    are kept intact for the density metric below (padded regions are a
        //    separate, coarser structure used only for tiling).
        var anchors: [(start: Double, end: Double)] = []

        // Evidence-catalog anchors (free — same anchors the FM harvester phase
        // trusts). Each entry contributes its coverage span.
        for entry in evidenceCatalog.entries {
            let lo = min(entry.coverageStartTime, entry.coverageEndTime)
            let hi = max(entry.coverageStartTime, entry.coverageEndTime)
            let clampedLo = min(max(lo, episodeStart), episodeEnd)
            let clampedHi = min(max(hi, episodeStart), episodeEnd)
            anchors.append((clampedLo, clampedHi))
        }

        // Lexical-candidate anchors (free — the same LexicalScanner pass the
        // `.scanLikelyAdSlots` phase seeds from). Direct host-read fit.
        for candidate in lexicalCandidates(orderedSegments: ordered) {
            let lo = min(candidate.startTime, candidate.endTime)
            let hi = max(candidate.startTime, candidate.endTime)
            let clampedLo = min(max(lo, episodeStart), episodeEnd)
            let clampedHi = min(max(hi, episodeStart), episodeEnd)
            anchors.append((clampedLo, clampedHi))
        }

        // Music-bed feature-window anchors (the recall lever). PR 4 passes `[]`,
        // so this loop is inert until PR 5 wires music-bed change scores in.
        for window in featureWindows where window.musicBedChangeScore >= Self.musicBedChangeThreshold {
            let lo = min(max(window.startTime - Self.musicBedFlankSeconds, episodeStart), episodeEnd)
            let hi = min(max(window.endTime + Self.musicBedFlankSeconds, episodeStart), episodeEnd)
            anchors.append((lo, hi))
        }

        guard !anchors.isEmpty else { return [] }

        // 2. Pad each anchor and merge overlapping/adjacent padded spans into
        //    coarse candidate REGIONS. Tiling happens inside a region so a
        //    single ad read yields consecutive windows, not a smear.
        let padded = anchors
            .map { anchor -> (Double, Double) in
                let lo = max(episodeStart, anchor.start - Self.anchorPaddingSeconds)
                let hi = min(episodeEnd, anchor.end + Self.anchorPaddingSeconds)
                return (lo, hi)
            }
            .sorted { $0.0 < $1.0 }
        var regions: [(Double, Double)] = [padded[0]]
        for span in padded.dropFirst() {
            let last = regions[regions.count - 1]
            if span.0 <= last.1 {
                regions[regions.count - 1] = (last.0, max(last.1, span.1))
            } else {
                regions.append(span)
            }
        }

        // 3. Tile each region into fixed-width windows; score each by the number
        //    of ORIGINAL anchors it overlaps (the density metric). Drop windows
        //    that cover no transcript — an empty prompt is useless.
        var scored: [(window: SpecialistScanWindow, density: Int)] = []
        for region in regions {
            var t = region.0
            while t < region.1 {
                let wStart = t
                let wEnd = min(t + Self.windowWidthSeconds, region.1)
                let refs = ordered
                    .filter { $0.startTime < wEnd && $0.endTime > wStart }
                    .map(\.segmentIndex)
                if !refs.isEmpty {
                    let density = anchors.reduce(into: 0) { count, anchor in
                        if anchor.start < wEnd && anchor.end > wStart {
                            count += 1
                        }
                    }
                    scored.append((
                        SpecialistScanWindow(startTime: wStart, endTime: wEnd, lineRefs: refs),
                        density
                    ))
                }
                t += Self.windowWidthSeconds
            }
        }

        // 4. Densest-cue-first (ties → earliest start) so the highest-value
        //    windows survive the budget truncation, then cap.
        scored.sort { lhs, rhs in
            if lhs.density != rhs.density {
                return lhs.density > rhs.density
            }
            return lhs.window.startTime < rhs.window.startTime
        }
        return scored.prefix(budget).map(\.window)
    }

    // MARK: - Lexical candidate derivation

    /// Run the production `LexicalScanner` over the segment text, mirroring
    /// `TargetedWindowNarrower.lexicalCandidateLineRefs`' chunk construction so
    /// the specialist gate scans the exact text the FM lexical phase does.
    private func lexicalCandidates(
        orderedSegments: [AdTranscriptSegment]
    ) -> [LexicalCandidate] {
        guard let assetId = orderedSegments.first?.atoms.first?.atomKey.analysisAssetId
        else { return [] }
        let chunks = orderedSegments.map { segment in
            TranscriptChunk(
                id: "specialist-scan-\(assetId)-\(segment.segmentIndex)",
                analysisAssetId: assetId,
                segmentFingerprint: "specialist-scan-\(segment.segmentIndex)",
                chunkIndex: segment.segmentIndex,
                startTime: segment.startTime,
                endTime: segment.endTime,
                text: segment.text,
                normalizedText: TranscriptEngineService.normalizeText(segment.text),
                pass: "final",
                modelVersion: "specialist-scan-planner",
                transcriptVersion: nil,
                atomOrdinal: segment.firstAtomOrdinal
            )
        }
        return LexicalScanner().scan(chunks: chunks, analysisAssetId: assetId)
    }
}
