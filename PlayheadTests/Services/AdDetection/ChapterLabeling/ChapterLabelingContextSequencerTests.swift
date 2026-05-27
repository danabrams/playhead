// ChapterLabelingContextSequencerTests.swift
// playhead-bahc — hermetic unit tests for the chapter-labeling context
// bookkeeping (1-based index, totalChapters, accumulated
// previousDisposition, start-time ordering). No audio, no FoundationModels,
// no live pipeline: the FM call is a fake closure.

import Foundation
import Testing
@testable import Playhead

@Suite("ChapterLabeling context sequencer (playhead-bahc)")
struct ChapterLabelingContextSequencerTests {

    // MARK: - Fixtures

    private static func candidate(start: Double, end: Double?) -> ChapterBoundaryCandidate {
        ChapterBoundaryCandidate(startTime: start, endTime: end)
    }

    /// Build a `LabelingResult` carrying the given RAW disposition. The
    /// sequencer only reads `labelDisposition`, so the rest is filler.
    private static func result(_ disposition: ChapterDispositionRaw) -> LabelingResult {
        LabelingResult(
            chapter: ChapterEvidence(
                startTime: 0,
                endTime: nil,
                title: nil,
                source: .inferred,
                disposition: disposition.mappedDisposition,
                qualityScore: 0
            ),
            labelDisposition: disposition,
            topicDescriptor: nil,
            failureMode: disposition == .unclear ? .semantic : nil,
            attempts: 1
        )
    }

    private static func chunk(start: Double, end: Double, text: String) -> TranscriptChunk {
        TranscriptChunk(
            id: "c-\(start)",
            analysisAssetId: "asset",
            segmentFingerprint: "",
            chunkIndex: Int(start),
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: text,
            pass: "final",
            modelVersion: "test",
            transcriptVersion: nil,
            atomOrdinal: Int(start),
            weakAnchorMetadata: nil,
            speakerId: nil,
            avgConfidence: nil
        )
    }

    // MARK: - makeCandidate: index / total

    @Test("makeCandidate stamps 1-based index and total count")
    func makeCandidate_indexAndTotal() {
        let sequencer = ChapterLabelingContextSequencer(candidates: [
            Self.candidate(start: 0, end: 100),
            Self.candidate(start: 100, end: 200),
            Self.candidate(start: 200, end: nil),
        ])
        #expect(sequencer.count == 3)

        let first = sequencer.makeCandidate(at: 0, regionText: "a", previousDisposition: nil)
        #expect(first.chapterIndex == 1)
        #expect(first.totalChapters == 3)
        #expect(first.startTime == 0)

        let third = sequencer.makeCandidate(at: 2, regionText: "c", previousDisposition: .content)
        #expect(third.chapterIndex == 3)
        #expect(third.totalChapters == 3)
        #expect(third.startTime == 200)
        #expect(third.endTime == nil)
        #expect(third.previousDisposition == .content)
        #expect(third.regionText == "c")
    }

    // MARK: - Ordering

    @Test("sequencer sorts candidates by start time")
    func sequencer_sortsByStartTime() {
        let sequencer = ChapterLabelingContextSequencer(candidates: [
            Self.candidate(start: 200, end: 300),
            Self.candidate(start: 0, end: 100),
            Self.candidate(start: 100, end: 200),
        ])
        #expect(sequencer.sortedCandidates.map(\.startTime) == [0, 100, 200])
        // Index follows sorted order, not input order.
        #expect(sequencer.makeCandidate(at: 0, regionText: "", previousDisposition: nil).startTime == 0)
        #expect(sequencer.makeCandidate(at: 1, regionText: "", previousDisposition: nil).startTime == 100)
        #expect(sequencer.makeCandidate(at: 2, regionText: "", previousDisposition: nil).startTime == 200)
    }

    // MARK: - run: previousDisposition accumulation

    @Test("run threads the prior candidate's RAW disposition; nil for first")
    func run_accumulatesPreviousDisposition() async {
        let sequencer = ChapterLabelingContextSequencer(candidates: [
            Self.candidate(start: 0, end: 100),
            Self.candidate(start: 100, end: 200),
            Self.candidate(start: 200, end: nil),
        ])
        // Disposition each candidate's label resolves to, by sorted index.
        let dispositions: [ChapterDispositionRaw] = [.intro, .hostReadAd, .content]

        var seenPrevious: [ChapterDispositionRaw?] = []
        var seenIndex: [Int] = []
        let labeled = await sequencer.run(transcript: []) { candidate in
            seenPrevious.append(candidate.previousDisposition)
            seenIndex.append(candidate.chapterIndex)
            return Self.result(dispositions[candidate.chapterIndex - 1])
        }

        // First candidate has no predecessor; each later one carries the
        // PRIOR candidate's resolved raw disposition.
        #expect(seenPrevious == [nil, .intro, .hostReadAd])
        #expect(seenIndex == [1, 2, 3])
        #expect(labeled.count == 3)
        #expect(labeled.map(\.result?.labelDisposition) == [.intro, .hostReadAd, .content])
        // Each built candidate records the same previousDisposition it saw.
        #expect(labeled.map(\.candidate.previousDisposition) == [nil, .intro, .hostReadAd])
    }

    @Test("a nil result feeds nil previousDisposition forward")
    func run_nilResultPropagatesNilPrevious() async {
        let sequencer = ChapterLabelingContextSequencer(candidates: [
            Self.candidate(start: 0, end: 100),
            Self.candidate(start: 100, end: 200),
            Self.candidate(start: 200, end: nil),
        ])

        var seenPrevious: [ChapterDispositionRaw?] = []
        _ = await sequencer.run(transcript: []) { candidate -> LabelingResult? in
            seenPrevious.append(candidate.previousDisposition)
            // The first candidate is a silent skip (nil result); the rest
            // return a concrete disposition.
            if candidate.chapterIndex == 1 { return nil }
            return Self.result(.content)
        }
        // Index 2's predecessor (index 1) skipped → nil previous. Index 3's
        // predecessor (index 2) returned .content → .content previous.
        #expect(seenPrevious == [nil, nil, .content])
    }

    @Test("previousDisposition uses the PRIOR result, never the current")
    func run_usesPriorNotCurrent() async {
        let sequencer = ChapterLabelingContextSequencer(candidates: [
            Self.candidate(start: 0, end: 50),
            Self.candidate(start: 50, end: nil),
        ])
        var observed: [(index: Int, previous: ChapterDispositionRaw?)] = []
        _ = await sequencer.run(transcript: []) { candidate in
            observed.append((candidate.chapterIndex, candidate.previousDisposition))
            // Current candidate resolves to .programmaticAd regardless of
            // index, so if the impl mistakenly used the CURRENT result the
            // second call's `previous` would be .programmaticAd from its own
            // (not-yet-known) label. It must instead be the FIRST result.
            return Self.result(candidate.chapterIndex == 1 ? .outro : .programmaticAd)
        }
        #expect(observed.count == 2)
        #expect(observed[0].previous == nil)
        #expect(observed[1].previous == .outro)
    }

    // MARK: - regionText

    @Test("regionText concatenates in-range chunks in start order")
    func regionText_concatenatesInRange() {
        let transcript = [
            Self.chunk(start: 30, end: 40, text: "third"),
            Self.chunk(start: 10, end: 20, text: "first"),
            Self.chunk(start: 20, end: 30, text: "second"),
            Self.chunk(start: 120, end: 130, text: "out-of-range"),
        ]
        let text = ChapterRegionText.regionText(transcript: transcript, start: 0, end: 100)
        #expect(text == "first second third")
    }

    @Test("regionText with nil end runs to episode end")
    func regionText_nilEndIsOpenEnded() {
        let transcript = [
            Self.chunk(start: 10, end: 20, text: "a"),
            Self.chunk(start: 500, end: 510, text: "b"),
        ]
        let text = ChapterRegionText.regionText(transcript: transcript, start: 0, end: nil)
        #expect(text == "a b")
    }

    // MARK: - run wires region text per candidate

    @Test("run slices region text per candidate from start order")
    func run_slicesRegionTextPerCandidate() async {
        let sequencer = ChapterLabelingContextSequencer(candidates: [
            Self.candidate(start: 0, end: 100),
            Self.candidate(start: 100, end: nil),
        ])
        let transcript = [
            Self.chunk(start: 5, end: 10, text: "alpha"),
            Self.chunk(start: 150, end: 160, text: "beta"),
        ]
        var seenText: [String] = []
        _ = await sequencer.run(transcript: transcript) { candidate in
            seenText.append(candidate.regionText)
            return Self.result(.content)
        }
        #expect(seenText == ["alpha", "beta"])
    }
}

// MARK: - LiveLabelerAdapter actor concurrency (playhead-bahc)
//
// The pure `ChapterLabelingContextSequencer` above proves the index /
// total / prior-disposition bookkeeping for the SEQUENTIAL path. The
// `LiveLabelerAdapter` actor re-implements that wiring for the PHASE's
// parallel, out-of-order dispatch (cap=2 TaskGroup) via its own
// `claimIndex` / `awaitPredecessor` / `record` machinery — which the
// sequencer tests do not touch. These tests drive the actor directly with
// a fake (non-FM) `ChapterLabelingService` so the highest-risk item — no
// deadlock + correct prior-disposition under out-of-order arrival — is
// locked in on the simulator. `LiveLabelerAdapter` is `@available(iOS 26.0, *)`,
// but the project's iOS deployment target IS 26.0, so it is unconditionally
// available here — no per-decl annotation is needed (and the Swift Testing
// `@Suite`/`@Test` macros reject `@available`). The suite is still `#if`-gated
// on FoundationModels because `ChapterLabel`'s schema type only exists under
// that import.
#if canImport(FoundationModels)
@Suite("LiveLabelerAdapter actor serialization (playhead-bahc)")
struct LiveLabelerAdapterConcurrencyTests {

    /// Coordinator that (a) records the prompt-context line each FM call
    /// received and (b) lets the test release calls in an arbitrary order
    /// so out-of-order completion is deterministic, not timing-dependent.
    private actor CallCoordinator {
        /// chapterIndex (parsed from the prompt) → the disposition the fake
        /// FM should return for it.
        private let dispositionByChapterIndex: [Int: ChapterDispositionRaw]
        /// Continuations parked by chapterIndex until the test releases them.
        private var gates: [Int: CheckedContinuation<Void, Never>] = [:]
        private var pendingRelease: Set<Int> = []
        /// Ordered record of (chapterIndex, totalChapters, previousRaw) each
        /// call observed in its prompt — the assertion surface.
        private(set) var observed: [(index: Int, total: Int, previous: String)] = []

        init(dispositionByChapterIndex: [Int: ChapterDispositionRaw]) {
            self.dispositionByChapterIndex = dispositionByChapterIndex
        }

        /// Called from the fake `labelCall`. Records the parsed context,
        /// then parks until the test releases this chapterIndex.
        func arrive(index: Int, total: Int, previous: String) async -> ChapterDispositionRaw {
            observed.append((index, total, previous))
            if pendingRelease.remove(index) == nil {
                await withCheckedContinuation { continuation in
                    gates[index] = continuation
                }
            }
            return dispositionByChapterIndex[index] ?? .content
        }

        /// Release the call for `index` (wake it if parked, else mark it for
        /// immediate pass-through when it arrives).
        func release(index: Int) {
            if let continuation = gates.removeValue(forKey: index) {
                continuation.resume()
            } else {
                pendingRelease.insert(index)
            }
        }

        func snapshot() -> [(index: Int, total: Int, previous: String)] { observed }
    }

    /// Parse the `Context: chapter N of M. Previous: P.` line the prompt
    /// builder emits so the fake FM can echo what context it was handed.
    private static func parseContext(_ prompt: String) -> (index: Int, total: Int, previous: String) {
        // Format (locked by ChapterLabelingService.buildPrompt):
        //   "Context: chapter \(index) of \(total). Previous: \(prev)."
        guard let range = prompt.range(of: "Context: chapter ") else {
            return (-1, -1, "PARSE-FAIL")
        }
        let tail = prompt[range.upperBound...]
        let scanner = Scanner(string: String(tail))
        var index = 0, total = 0
        _ = scanner.scanInt(&index)
        _ = scanner.scanString("of")
        _ = scanner.scanInt(&total)
        _ = scanner.scanUpToString("Previous: ")
        _ = scanner.scanString("Previous: ")
        let previous = scanner.scanUpToString(".") ?? "PARSE-FAIL"
        return (index, total, previous)
    }

    private static func candidates(_ count: Int) -> [ChapterBoundaryCandidate] {
        (0..<count).map { i in
            ChapterBoundaryCandidate(
                startTime: Double(i) * 100,
                endTime: i + 1 < count ? Double(i + 1) * 100 : nil
            )
        }
    }

    /// Build a fake `ChapterLabelingService` whose FM closure routes through
    /// the coordinator. The closure parses the prompt context and returns a
    /// `ChapterLabel` carrying the coordinator-chosen disposition.
    private static func fakeService(coordinator: CallCoordinator) -> ChapterLabelingService {
        ChapterLabelingService(labelCall: { prompt in
            let ctx = parseContext(prompt)
            let disposition = await coordinator.arrive(
                index: ctx.index,
                total: ctx.total,
                previous: ctx.previous
            )
            return ChapterLabel(disposition: disposition, confidence: 0.9, topicDescriptor: nil)
        })
    }

    /// Out-of-order arrival: dispatch candidates in REVERSE (4,3,2,1) before
    /// the predecessors, release predecessors LAST, and prove (a) the whole
    /// batch completes — no deadlock/livelock — and (b) each call still saw
    /// the PRIOR candidate's result as `previousDisposition`, not the
    /// current one. This is the cap=2 TaskGroup worst case generalized.
    @Test("actor: out-of-order arrival completes and threads prior disposition")
    func actor_outOfOrderArrivalThreadsPrior() async {
        // chapterIndex (1-based) → returned disposition.
        let byIndex: [Int: ChapterDispositionRaw] = [
            1: .intro, 2: .hostReadAd, 3: .content, 4: .programmaticAd,
        ]
        let coordinator = CallCoordinator(dispositionByChapterIndex: byIndex)
        let service = Self.fakeService(coordinator: coordinator)
        let candidates = Self.candidates(4)
        let adapter = LiveLabelerAdapter(
            service: service,
            transcript: [],
            candidates: candidates
        )

        // Dispatch every candidate concurrently in REVERSE start-time order so
        // successors arrive at the actor before their predecessors. Each
        // index>0 call parks on its predecessor; if the actor deadlocked, the
        // group would never finish and the test would hang (caught by the
        // suite timeout).
        await withTaskGroup(of: Void.self) { group in
            for candidate in candidates.reversed() {
                group.addTask {
                    _ = try? await adapter.label(candidate: candidate)
                }
            }

            // Release in REVERSE too (4,3,2,1) — index 0 (chapterIndex 1) has
            // no predecessor and runs immediately; each release lets the
            // recorded disposition cascade forward to its successor's waiter.
            // Even with this adversarial release order the chain must drain.
            for index in [4, 3, 2, 1] {
                await coordinator.release(index: index)
            }
            await group.waitForAll()
        }

        // Sort observations by chapterIndex (arrival order is nondeterministic)
        // and assert the threaded context per candidate.
        let observed = await coordinator.snapshot().sorted { $0.index < $1.index }
        #expect(observed.map(\.index) == [1, 2, 3, 4])
        #expect(observed.allSatisfy { $0.total == 4 })
        // chapter 1 → no predecessor; 2 → chapter 1's .intro; 3 → chapter 2's
        // .hostReadAd; 4 → chapter 3's .content. RAW values, prior-not-current.
        #expect(observed.map(\.previous) == ["none", "intro", "hostReadAd", "content"])
    }

    /// In-order arrival (the production cap=2 dispatcher only ever submits
    /// index i after i-1) also threads the prior result and completes.
    @Test("actor: in-order dispatch threads prior disposition")
    func actor_inOrderThreadsPrior() async {
        let byIndex: [Int: ChapterDispositionRaw] = [1: .content, 2: .hostReadAd, 3: .outro]
        let coordinator = CallCoordinator(dispositionByChapterIndex: byIndex)
        let service = Self.fakeService(coordinator: coordinator)
        let candidates = Self.candidates(3)
        let adapter = LiveLabelerAdapter(
            service: service,
            transcript: [],
            candidates: candidates
        )
        // Pre-release everything so calls pass straight through.
        for index in [1, 2, 3] { await coordinator.release(index: index) }

        var results: [LabelingResult?] = []
        for candidate in candidates {
            results.append(try? await adapter.label(candidate: candidate))
        }
        let observed = await coordinator.snapshot().sorted { $0.index < $1.index }
        #expect(observed.map(\.previous) == ["none", "content", "hostReadAd"])
        #expect(results.compactMap { $0?.labelDisposition } == [.content, .hostReadAd, .outro])
    }
}
#endif
