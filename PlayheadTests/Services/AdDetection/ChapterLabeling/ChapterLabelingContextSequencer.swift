// ChapterLabelingContextSequencer.swift
// playhead-bahc — hermetic context-threading helper for the env-gated
// chapter-labeling harnesses.
//
// MEASUREMENT-CREDIBILITY FIX (test-only). The live labeling harnesses
// (`ChapterPlanPipelineSnapshotCaptureTests`, `ChapterFusionLiftABTests`,
// `ChapterLabelingDiagnosticTests`) used to hardcode
// `chapterIndex: 1, totalChapters: 1, previousDisposition: nil` for EVERY
// per-candidate FM call, so all prior diagnostics ran with three prompt
// priors disabled. `ChapterLabelingService.buildPrompt` already emits
// "Context: chapter {N} of {M}. Previous: {P}"; this helper threads the
// REAL values:
//   * `chapterIndex`        = the candidate's 1-based position in the
//                             start-time-sorted candidate list,
//   * `totalChapters`       = the candidate count,
//   * `previousDisposition` = the RAW disposition the labeler returned for
//                             the IMMEDIATELY PRECEDING candidate (start-time
//                             order), `nil` for the first candidate and
//                             whenever the previous label was `nil`.
//
// This file is hermetic, pure value-shuffling: it depends on no audio, no
// FoundationModels, and no live pipeline, so the bookkeeping can be
// unit-tested on the simulator. The live FM call is supplied as a closure
// by the caller. See `ChapterLabelingContextSequencerTests`.

import Foundation
@testable import Playhead

// MARK: - Region-text extraction

/// Pure region-text extraction shared by every live labeler adapter:
/// concatenate (in start-time order) the text of every transcript chunk
/// whose `startTime` falls inside `[start, end)`. `end == nil` means
/// "to the end of the episode" (the last chapter).
///
/// Hoisted out of the per-file `LiveLabelerAdapter`s so the snapshot,
/// fusion-lift, and diagnostic harnesses share ONE definition (and so it
/// can be unit-tested without FM).
enum ChapterRegionText {
    static func regionText(
        transcript: [TranscriptChunk],
        start: TimeInterval,
        end: TimeInterval?
    ) -> String {
        let upper = end ?? .greatestFiniteMagnitude
        return transcript
            .filter { $0.startTime >= start && $0.startTime < upper }
            .sorted { $0.startTime < $1.startTime }
            .map(\.text)
            .joined(separator: " ")
    }
}

// MARK: - Context sequencer

/// Pure, hermetic bookkeeping for threading real prompt context into a
/// chapter-labeling pass.
///
/// Construct it with the boundary candidates the detector produced; it
/// sorts them by start time once and exposes:
///   * `count` — the total chapter count (the `totalChapters` denominator),
///   * `makeCandidate(at:regionText:previousDisposition:)` — build the
///     `ChapterLabelingCandidate` for the i-th (0-based) sorted candidate
///     with the correct 1-based `chapterIndex`, and
///   * `run(transcript:label:)` — drive a full sequential labeling pass,
///     accumulating `previousDisposition` from the PRIOR candidate's
///     result, and return the ordered `(candidate, result)` pairs.
///
/// The `previousDisposition` fed into candidate *i* is the RAW
/// `labelDisposition` (`ChapterDispositionRaw`) the labeler returned for
/// candidate *i-1*, or `nil` when *i == 0* OR when candidate *i-1*'s result
/// was `nil` (a silent skip). This matches `ChapterLabelingCandidate`'s
/// `previousDisposition: ChapterDispositionRaw?` field, which the prompt
/// builder renders as the "Previous: <value>" hint.
struct ChapterLabelingContextSequencer: Sendable {

    /// Boundary candidates sorted by start time ascending. The 1-based
    /// position in this array is the `chapterIndex`; the array count is
    /// `totalChapters`.
    let sortedCandidates: [ChapterBoundaryCandidate]

    init(candidates: [ChapterBoundaryCandidate]) {
        self.sortedCandidates = candidates.sorted { $0.startTime < $1.startTime }
    }

    /// Total chapter count — the `totalChapters` denominator.
    var count: Int { sortedCandidates.count }

    /// Build the `ChapterLabelingCandidate` for the `index`-th (0-based)
    /// sorted boundary candidate, stamping the real 1-based `chapterIndex`,
    /// `totalChapters`, and the supplied `previousDisposition`.
    ///
    /// Precondition: `0 <= index < count`.
    func makeCandidate(
        at index: Int,
        regionText: String,
        previousDisposition: ChapterDispositionRaw?
    ) -> ChapterLabelingCandidate {
        precondition(
            index >= 0 && index < sortedCandidates.count,
            "ChapterLabelingContextSequencer: index \(index) out of range 0..<\(sortedCandidates.count)"
        )
        let candidate = sortedCandidates[index]
        return ChapterLabelingCandidate(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            regionText: regionText,
            chapterIndex: index + 1,
            totalChapters: sortedCandidates.count,
            previousDisposition: previousDisposition
        )
    }

    /// One labeled candidate plus the labeler's result, in start-time order.
    struct Labeled {
        let candidate: ChapterLabelingCandidate
        let result: LabelingResult?
    }

    /// Drive a full sequential labeling pass in start-time order. For each
    /// candidate it builds the `ChapterLabelingCandidate` (with real index /
    /// total / accumulated previous disposition), invokes `label`, and feeds
    /// the result's RAW `labelDisposition` forward as the next candidate's
    /// `previousDisposition` (`nil` if the result was `nil`).
    ///
    /// `label` receives both the built candidate and the underlying
    /// boundary candidate (the latter is what the production
    /// `ChapterLabeling.label(candidate:)` seam consumes); the live adapters
    /// route the built candidate into `ChapterLabelingService.label`.
    func run(
        transcript: [TranscriptChunk],
        label: (ChapterLabelingCandidate) async throws -> LabelingResult?
    ) async rethrows -> [Labeled] {
        var labeled: [Labeled] = []
        labeled.reserveCapacity(sortedCandidates.count)
        var previousDisposition: ChapterDispositionRaw?
        for index in sortedCandidates.indices {
            let candidate = sortedCandidates[index]
            let regionText = ChapterRegionText.regionText(
                transcript: transcript,
                start: candidate.startTime,
                end: candidate.endTime
            )
            let labelingCandidate = makeCandidate(
                at: index,
                regionText: regionText,
                previousDisposition: previousDisposition
            )
            let result = try await label(labelingCandidate)
            labeled.append(Labeled(candidate: labelingCandidate, result: result))
            previousDisposition = result?.labelDisposition
        }
        return labeled
    }
}

// MARK: - Shared live boundary detector

/// Boundary-detection adapter shared by the env-gated live harnesses.
/// Runs the real `ChapterBoundaryDetector` over a prebuilt
/// `ChapterFeatureSnapshot`, then projects the deduplicated, start-time-
/// sorted boundary starts into adjacent `[start, nextStart)` candidate
/// regions (the final region runs to `episodeDuration`).
struct LiveBoundaryDetector: ChapterBoundaryDetecting {
    let snapshot: ChapterFeatureSnapshot
    private let detector: ChapterBoundaryDetector

    init(snapshot: ChapterFeatureSnapshot) {
        self.snapshot = snapshot
        self.detector = ChapterBoundaryDetector()
    }

    func detect() async throws -> [ChapterBoundaryCandidate] {
        let starts = detector.detect(features: snapshot)
            .map(\.startTime)
            .sorted()
        return starts.enumerated().map { index, start in
            let end = index + 1 < starts.count
                ? starts[index + 1]
                : snapshot.episodeDuration
            return ChapterBoundaryCandidate(startTime: start, endTime: end)
        }
    }
}

// MARK: - Shared live labeler adapter

/// Live FM-backed labeler shared by the env-gated harnesses (snapshot
/// capture, fusion-lift A/B, diagnosis). Conforms to the production
/// `ChapterLabeling` seam, so it can be injected into a real
/// `ChapterGenerationPhase`.
///
/// The phase drives `label(candidate:)` IN PARALLEL (TaskGroup capped at
/// `ChapterGenerationPhase.maxFMConcurrency`) and OUT OF ORDER, so a naive
/// "remember the last result" accumulator would feed the wrong (or a
/// not-yet-computed) `previousDisposition` into the prompt. This actor
/// instead serializes the labeling pass in START-TIME ORDER: each
/// candidate's FM call waits until its immediate predecessor's result is
/// known, then threads that predecessor's RAW `labelDisposition` (or `nil`
/// for the first candidate / a skipped predecessor) into the prompt. The
/// per-index bookkeeping is delegated to the pure
/// `ChapterLabelingContextSequencer` so the contract is unit-tested
/// without FM.
@available(iOS 26.0, *)
actor LiveLabelerAdapter: ChapterLabeling {
    private let service: ChapterLabelingService
    private let transcript: [TranscriptChunk]
    private let sequencer: ChapterLabelingContextSequencer

    /// Index (into `sequencer.sortedCandidates`) of each candidate. Two
    /// candidates with identical bounds are indistinguishable; ties resolve
    /// to the lowest unused index, which is harmless because identical
    /// candidates produce identical prompts and contribute the same
    /// disposition forward.
    private let indexByCandidate: [ChapterBoundaryCandidate: [Int]]

    /// Resolved RAW disposition per (0-based) sorted index. `.some(nil)`
    /// means "labeled, but the result was a skip"; absent means "not yet
    /// labeled".
    private var resultDisposition: [Int: ChapterDispositionRaw?] = [:]

    /// Continuations parked waiting for index `i-1` to resolve, keyed by the
    /// awaited predecessor index.
    private var waiters: [Int: [CheckedContinuation<Void, Never>]] = [:]

    /// Next sorted index to hand out for an as-yet-unclaimed candidate, so
    /// repeated calls for identical candidates map to successive indices.
    private var claimedIndices: Set<Int> = []

    init(
        service: ChapterLabelingService,
        transcript: [TranscriptChunk],
        candidates: [ChapterBoundaryCandidate]
    ) {
        self.service = service
        self.transcript = transcript
        self.sequencer = ChapterLabelingContextSequencer(candidates: candidates)

        var map: [ChapterBoundaryCandidate: [Int]] = [:]
        for (index, candidate) in self.sequencer.sortedCandidates.enumerated() {
            map[candidate, default: []].append(index)
        }
        self.indexByCandidate = map
    }

    func label(candidate: ChapterBoundaryCandidate) async throws -> LabelingResult? {
        let index = claimIndex(for: candidate)
        // Serialize on the immediate predecessor so `previousDisposition`
        // reflects the prior candidate's RESULT even under the phase's
        // out-of-order parallel dispatch.
        if index > 0 {
            await awaitPredecessor(of: index)
        }
        let previousDisposition = index > 0 ? (resultDisposition[index - 1] ?? nil) : nil
        let regionText = ChapterRegionText.regionText(
            transcript: transcript,
            start: candidate.startTime,
            end: candidate.endTime
        )
        let labelingCandidate = sequencer.makeCandidate(
            at: index,
            regionText: regionText,
            previousDisposition: previousDisposition
        )
        let result = await service.label(labelingCandidate)
        record(disposition: result.labelDisposition, at: index)
        return result
    }

    /// Resolve a candidate to its sorted index, consuming one slot so
    /// duplicate-bounds candidates map to distinct successive indices.
    private func claimIndex(for candidate: ChapterBoundaryCandidate) -> Int {
        guard let indices = indexByCandidate[candidate] else {
            // Candidate not in the prebuilt set (should not happen when the
            // phase is fed `LiveBoundaryDetector`'s output). Treat as a lone
            // first chapter.
            return 0
        }
        for index in indices where !claimedIndices.contains(index) {
            claimedIndices.insert(index)
            return index
        }
        // All slots for these bounds already claimed: reuse the first.
        return indices[0]
    }

    private func awaitPredecessor(of index: Int) async {
        let predecessor = index - 1
        if resultDisposition.keys.contains(predecessor) { return }
        await withCheckedContinuation { continuation in
            waiters[predecessor, default: []].append(continuation)
        }
    }

    private func record(disposition: ChapterDispositionRaw, at index: Int) {
        resultDisposition[index] = disposition
        if let parked = waiters.removeValue(forKey: index) {
            for continuation in parked {
                continuation.resume()
            }
        }
    }
}
